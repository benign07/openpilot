"""Experimental guide-paint exclusion assessor; NOT a semantic lane detector.

Green/pink paint can flag a coincident predicted boundary for exclusion review.
It never supplies a route, a replacement line, or increased confidence. The
offline proposals do not alter published model data. There is no modeld hook.
Thresholds need labeled road video (including false positives) before activation.
"""
from dataclasses import dataclass
import time

import numpy as np


DISTANCES = np.arange(8., 34., 2.)
OFFSETS = np.array([-.30, -.20, -.12, -.07, -.025, 0., .025, .07, .12, .20, .30])
CORE = slice(4, 7)
NEIGHBORS = [1, 2, 3, 7, 8, 9]
FLANKS = [0, 10]
MIN_SUPPORT = 10
MIN_OBSERVATIONS = 4
MIN_DURATION_S = .6
MAX_GAP_S = .35
SUPPORT_GROUPS = np.array([0, 5, 9])  # np.array_split(13, 3): 5 / 4 / 4
PROJECTION_PROBES = np.array([[8., -1.8, 1.2], [8., 1.8, 1.2], [32., -1.8, 1.2], [32., 1.8, 1.2]])
PROJECTION_PROBES.flags.writeable = False


@dataclass(frozen=True)
class LaneEvidence:
  color: str = "unknown"
  supported: int = 0
  visible: int = 0
  reason: str = "insufficient_support"


@dataclass(frozen=True)
class GuidePaintResult:
  evidence: tuple[LaneEvidence, ...]
  would_ignore: tuple[bool, ...]
  reason: str


def guide_colors(rgb: np.ndarray) -> np.ndarray:
  """Provisional color evidence: 0 unknown, 1 green, 2 pink. Not road semantics."""
  pixels = np.asarray(rgb, dtype=np.float32)
  r, g, b = np.moveaxis(pixels, -1, 0)
  maximum = pixels.max(axis=-1)
  return _guide_colors(r, g, b, maximum - pixels.min(axis=-1), maximum)


def _guide_colors(r, g, b, chroma, maximum):
  vivid = (chroma >= 45) & (maximum >= 75)
  green = vivid & (g >= 1.3 * r) & (g >= 1.3 * b)
  pink = vivid & (r >= 1.4 * g) & (b >= 1.25 * g) & (b >= .5 * r) & (r >= .75 * b)
  return np.where(green, 1, np.where(pink, 2, 0)).astype(np.uint8)


class RGBImage:
  def __init__(self, rgb: np.ndarray):
    if rgb.dtype != np.uint8 or rgb.ndim != 3 or rgb.shape[2] != 3:
      raise ValueError("Expected uint8 HxWx3 RGB")
    self.rgb = rgb
    self.height, self.width = rgb.shape[:2]

  def sample(self, uv: np.ndarray) -> np.ndarray:
    return self.rgb[uv[..., 1], uv[..., 0]]


class NV12Image:
  """Sparse sampling honors VisionBuf stride and UV offset; no full RGB copy."""
  def __init__(self, data, width: int, height: int, stride: int, uv_offset: int):
    if width <= 0 or height <= 0 or width % 2 or height % 2 or stride < width or stride % 2 or uv_offset < stride * height:
      raise ValueError("Invalid NV12 layout")
    self.data = np.frombuffer(data, dtype=np.uint8)
    self.data.flags.writeable = False
    if self.data.size < uv_offset + stride * (height // 2):
      raise ValueError("Short NV12 buffer")
    self.width, self.height, self.stride, self.uv_offset = width, height, stride, uv_offset

  def sample(self, uv: np.ndarray) -> np.ndarray:
    x, y = uv[..., 0], uv[..., 1]
    yy = self.data[y * self.stride + x].astype(np.float32)
    chroma_offset = self.uv_offset + (y // 2) * self.stride + (x // 2) * 2
    u = self.data[chroma_offset].astype(np.float32) - 128.
    v = self.data[chroma_offset + 1].astype(np.float32) - 128.
    # Match system/camerad/snapshot.py, not limited-range decoded video YUV.
    return np.stack((yy + 1.13983 * v, yy - .39465 * u - .58060 * v, yy + 2.03211 * u), axis=-1).clip(0, 255)


def projected_samples(line: np.ndarray, camera_from_calib: np.ndarray, width: int, height: int):
  """Model lane z already includes road height; do NOT add camera height again."""
  line = np.asarray(line, dtype=np.float64)
  if line.ndim != 2 or line.shape[1] != 3 or not 2 <= len(line) <= 64 or not np.isfinite(line).all():
    return None
  if not (np.diff(line[:, 0]) > 0).all() or line[0, 0] > DISTANCES[0] or line[-1, 0] < DISTANCES[-1]:
    return None
  center = np.column_stack((DISTANCES, np.interp(DISTANCES, line[:, 0], line[:, 1]), np.interp(DISTANCES, line[:, 0], line[:, 2])))
  if np.any((center[:, 2] < .5) | (center[:, 2] > 2.5)):
    return None
  points = np.repeat(center[:, None, :], len(OFFSETS), axis=1)
  points[:, :, 1] += OFFSETS
  proj = points @ camera_from_calib.T
  depth_ok = (proj[..., 2] > 1.).all(axis=1)
  pixels = proj[..., :2] / np.where(proj[..., 2:] > 1., proj[..., 2:], 1.)
  visible = depth_ok & ((pixels[..., 0] >= 1) & (pixels[..., 0] < width - 1) &
                        (pixels[..., 1] >= height * .45) & (pixels[..., 1] < height - 1)).all(axis=1)
  # Clamp only for safe sampling; clamped/out-of-frame samples remain invalid.
  pixels = np.rint(pixels).astype(np.int64)
  pixels[..., 0] = np.clip(pixels[..., 0], 0, width - 1)
  pixels[..., 1] = np.clip(pixels[..., 1], 0, height - 1)
  return pixels, visible, center


def _project_lanes(lines, matrix, width, height):
  """Batch regular model geometry; retain the scalar path for ragged exports."""
  try:
    geometry = np.asarray(lines, dtype=np.float64)
  except ValueError:
    geometry = None
  if geometry is None or geometry.ndim != 3 or geometry.shape[2] != 3 or not 2 <= geometry.shape[1] <= 64:
    projections = [projected_samples(line, matrix, width, height) for line in lines]
    valid = np.array([p is not None for p in projections])
    pixels = np.stack([p[0] if p is not None else np.zeros((len(DISTANCES), len(OFFSETS), 2), dtype=np.int64) for p in projections])
    visible = np.stack([p[1] if p is not None else np.zeros(len(DISTANCES), dtype=bool) for p in projections])
    return pixels, visible, valid
  valid = (np.isfinite(geometry).all(axis=(1, 2)) & (np.diff(geometry[:, :, 0], axis=1) > 0).all(axis=1) &
           (geometry[:, 0, 0] <= DISTANCES[0]) & (geometry[:, -1, 0] >= DISTANCES[-1]))
  centers = np.zeros((len(lines), len(DISTANCES), 3), dtype=np.float64)
  centers[:, :, 0] = DISTANCES
  for i in np.flatnonzero(valid):
    # Keep np.interp's arithmetic order, including its rounding at pixel ties.
    centers[i, :, 1] = np.interp(DISTANCES, geometry[i, :, 0], geometry[i, :, 1])
    centers[i, :, 2] = np.interp(DISTANCES, geometry[i, :, 0], geometry[i, :, 2])
  valid &= ((centers[:, :, 2] >= .5) & (centers[:, :, 2] <= 2.5)).all(axis=1)
  centers[~valid] = 0.
  points = np.repeat(centers[:, :, None, :], len(OFFSETS), axis=2)
  points[:, :, :, 1] += OFFSETS
  proj = points @ matrix.T
  pixels = proj[..., :2] / np.where(proj[..., 2:] > 1., proj[..., 2:], 1.)
  visible = (valid[:, None] & (proj[..., 2] > 1.).all(axis=2) &
             ((pixels[..., 0] >= 1) & (pixels[..., 0] < width - 1) &
              (pixels[..., 1] >= height * .45) & (pixels[..., 1] < height - 1)).all(axis=2))
  pixels = np.rint(pixels).astype(np.int64)
  pixels[..., 0] = np.clip(pixels[..., 0], 0, width - 1)
  pixels[..., 1] = np.clip(pixels[..., 1], 0, height - 1)
  return pixels, visible, valid


def _median_three(values):
  # Order statistics, not sum-minus-extremes: avoid cancellation/rounding changes.
  a, b, c = values[..., 0], values[..., 1], values[..., 2]
  return np.maximum(np.minimum(a, b), np.minimum(np.maximum(a, b), c))


def assess_lanes(image, lines, camera_from_calib: np.ndarray) -> tuple[LaneEvidence, ...]:
  if not len(lines):
    return ()
  uv, visible, valid = _project_lanes(lines, camera_from_calib, image.width, image.height)
  counts = visible.sum(axis=1)
  rows = np.sort(np.where(visible, uv[:, :, 5, 1], -1), axis=1)
  distinct = (rows[:, 0] != -1) + ((np.diff(rows, axis=1) != 0) & (rows[:, 1:] != -1)).sum(axis=1)
  qualified = valid & (counts >= MIN_SUPPORT) & (distinct >= 8)
  evidence = [LaneEvidence(visible=int(counts[i]), reason="insufficient_projection") if valid[i]
              else LaneEvidence(reason="invalid_geometry") for i in range(len(lines))]
  ids = np.flatnonzero(qualified)
  if not len(ids):
    return tuple(evidence)
  visible = visible[ids]
  counts = counts[ids]
  rgb = image.sample(uv[ids]).astype(np.float32, copy=False)
  names, supported_counts = _classify_numpy(rgb, visible, counts)
  for j, i in enumerate(ids):
    count = int(counts[j])
    if names[j] == 3:
      evidence[i] = LaneEvidence(visible=count, reason="ambiguous_boundary_neighbor")
    elif names[j]:
      evidence[i] = LaneEvidence("green" if names[j] == 1 else "pink", int(supported_counts[j]), count, "paint_candidate")
    else:
      evidence[i] = LaneEvidence(visible=count, reason="no_narrow_guide_support")
  return tuple(evidence)


def _classify_numpy(rgb, visible, counts):
  r, g, b = rgb[..., 0], rgb[..., 1], rgb[..., 2]
  maximum = rgb.max(axis=-1)
  chroma = maximum - rgb.min(axis=-1)
  colors = _guide_colors(r, g, b, chroma, maximum)
  core = colors[:, :, CORE]
  brightness = rgb.mean(axis=-1)
  flank_brightness = (brightness[:, :, 0] + brightness[:, :, 10]) * .5
  contrast = brightness - flank_brightness[:, :, None]
  white = (contrast >= 25) & (chroma <= np.maximum(15, .2 * maximum))
  yellow = (chroma >= 20) & (contrast >= 15) & (r >= .85 * g) & (g >= 1.35 * b)
  blue = (chroma >= 20) & (b >= 1.3 * r) & (b >= 1.1 * g)
  boundary_neighbor = visible & (white | yellow | blue)[:, :, NEIGHBORS].any(axis=2)
  ambiguous = boundary_neighbor.sum(axis=1) >= 3
  names = np.zeros(len(rgb), dtype=np.uint8)
  supported_counts = np.zeros(len(rgb), dtype=int)
  flank_fraction = (((colors[:, :, 0] != 0) + (colors[:, :, 10] != 0).astype(np.int16)) * visible).sum(axis=1) / (2 * counts)
  for color in (1, 2):
    if not (core == color).any():
      continue
    chroma = g - (r + b) / 2 if color == 1 else (r + b) / 2 - g
    contrast = _median_three(chroma[:, :, CORE]) - (chroma[:, :, 0] + chroma[:, :, 10]) * .5
    support = visible & ((core == color).sum(axis=2) >= 2) & (core[:, :, 1] == color) & (contrast >= 40)
    supported = support.sum(axis=1)
    spatial = (np.add.reduceat(support, SUPPORT_GROUPS, axis=1, dtype=np.int16) >= 2).all(axis=1)
    accepted = ~ambiguous & (names == 0) & (supported >= MIN_SUPPORT) & (supported / counts >= .8) & spatial & (flank_fraction <= .15)
    names[accepted] = color
    supported_counts[accepted] = supported[accepted]
    if ((names != 0) | ambiguous).all():
      break
  names[ambiguous] = 3
  return names, supported_counts


def assess_lane(image, line: np.ndarray, camera_from_calib: np.ndarray) -> LaneEvidence:
  return assess_lanes(image, [line], camera_from_calib)[0]


class GuidePaintAssessor:
  """Temporal proposal, never writes to an image, model message or caller array."""
  def __init__(self):
    self.reset()

  def reset(self):
    self.last_time = None
    self.last_frame = None
    self.last_camera = None
    self.last_matrix = None
    self.colors = ["unknown"] * 4
    self.counts = [0] * 4
    self.since = [0.] * 4
    self.positions = [None] * 4

  def observe(self, image, lines, probs, stds, camera_from_calib, *, frame_id: int, image_frame_id: int,
              timestamp_s: float, camera_id: str, calibrated: bool, calibration_age_s: float) -> GuidePaintResult:
    probabilities = tuple(float(p) for p in probs)
    matrix = np.asarray(camera_from_calib, dtype=np.float64)
    valid = (len(lines) == len(probabilities) == len(stds) == 4 and matrix.shape == (3, 3) and np.isfinite(matrix).all() and
             type(calibrated) is bool and type(frame_id) is int and type(image_frame_id) is int and
             np.isfinite(probabilities).all() and all(0. <= p <= 1. for p in probabilities) and
             np.isfinite(stds).all() and all(s >= 0. for s in stds) and
             calibrated and np.isfinite(timestamp_s) and np.isfinite(calibration_age_s) and
             0. <= calibration_age_s <= 2. and frame_id == image_frame_id and frame_id >= 0)
    if not valid:
      self.reset()
      return GuidePaintResult((), (False,) * 4, "invalid_input")
    if (self.last_time is not None and (timestamp_s <= self.last_time or timestamp_s - self.last_time > MAX_GAP_S or
        frame_id <= self.last_frame or camera_id != self.last_camera or
        self.projection_changed(matrix))):
      self.reset()
    evidence = []
    ignore = []
    selected = [i for i, (prob, std) in enumerate(zip(probabilities, stds, strict=True)) if prob >= .3 and std <= .15]
    measured = dict(zip(selected, assess_lanes(image, [lines[i] for i in selected], matrix), strict=True))
    for i, (line, prob, std) in enumerate(zip(lines, probabilities, stds, strict=True)):
      item = measured.get(i, LaneEvidence(reason="model_uncertain"))
      position = float(np.interp(12., np.asarray(line)[:, 0], np.asarray(line)[:, 1])) if item.color != "unknown" else None
      continuous = (item.color != "unknown" and item.color == self.colors[i] and self.positions[i] is not None and
                    abs(position - self.positions[i]) <= .15)
      self.counts[i] = self.counts[i] + 1 if continuous else int(item.color != "unknown")
      if not continuous:
        self.since[i] = timestamp_s
      self.colors[i], self.positions[i] = item.color, position
      ignore.append(self.counts[i] >= MIN_OBSERVATIONS and timestamp_s - self.since[i] >= MIN_DURATION_S - 1e-9)
      evidence.append(item)
    self.last_time, self.last_frame, self.last_camera, self.last_matrix = timestamp_s, frame_id, camera_id, matrix.copy()
    return GuidePaintResult(tuple(evidence), tuple(ignore), "shadow_proposal_only")

  def projection_changed(self, matrix):
    # Compare projection displacement in pixels, not unlike homography units.
    new = PROJECTION_PROBES @ matrix.T
    if (new[:, 2] <= 1.).any():
      return True
    if np.array_equal(matrix, self.last_matrix):
      return False
    old = PROJECTION_PROBES @ self.last_matrix.T
    if (old[:, 2] <= 1.).any():
      return True
    return bool(np.linalg.norm(new[:, :2] / new[:, 2:] - old[:, :2] / old[:, 2:], axis=1).max() > 2.)


class GuidePaintShadow:
  """Unconnected research harness, nominal 5 Hz; not called by modeld.

  Disabled by default. Wide-camera-only devices are explicitly unsupported until
  their extra extrinsic transform and independent video pairing are validated.
  Diagnostic errors disable the harness. It is not a vehicle process.
  """
  def __init__(self, enabled=False, clock=time.perf_counter):
    self.enabled = enabled
    self.clock = clock
    self.assessor = GuidePaintAssessor()
    self.next_sample = 0.
    self.last_timestamp = None
    self.last_ignore = (False,) * 4
    self.last_log = -float("inf")
    self.samples = 0
    self.proposal_samples = 0
    self.over_budget = 0

  def observe(self, buf, model, camera_from_calib, *, frame_id, calibrated, calibration_age_s, wide_camera=False, eligible=True,
              expected_size=None):
    if not self.enabled:
      return None
    try:
      if wide_camera or not calibrated or not 0. <= calibration_age_s <= 2. or not eligible:
        self.assessor.reset()
        return None
      timestamp = model.timestampEof / 1e9
      if self.last_timestamp is not None and timestamp <= self.last_timestamp:
        self.assessor.reset()
        self.next_sample = timestamp
        self.last_log = -float("inf")
      self.last_timestamp = timestamp
      if timestamp < self.next_sample:
        return None
      self.next_sample = timestamp + .19
      start = self.clock()
      if expected_size is not None and (buf.width, buf.height) != tuple(expected_size):
        self.assessor.reset()
        return None
      image = NV12Image(buf.data, buf.width, buf.height, buf.stride, buf.uv_offset)
      lines = [np.column_stack((line.x, line.y, line.z)) for line in model.laneLines]
      result = self.assessor.observe(image, lines, model.laneLineProbs, model.laneLineStds, camera_from_calib,
                                     frame_id=model.frameId, image_frame_id=frame_id, timestamp_s=timestamp,
                                     camera_id=f"road:{buf.width}x{buf.height}", calibrated=calibrated,
                                     calibration_age_s=calibration_age_s)
      elapsed = self.clock() - start
      if elapsed > .002:
        # Backoff is not a preemptive time limit: this evaluation already ran.
        self.next_sample = timestamp + 1.
        self.assessor.reset()
        self.over_budget += 1
        if timestamp - self.last_log < 1.:
          return None
        self.last_log = timestamp
        return {"event": "guide_paint_shadow_budget", "schema": 1, "active": False, "duration_ms": elapsed * 1000,
                "over_budget": self.over_budget, "samples": self.samples, "proposal_samples": self.proposal_samples}
      self.samples += 1
      self.proposal_samples += int(any(result.would_ignore))
      changed = result.would_ignore != self.last_ignore
      if timestamp - self.last_log < (1. if changed else 5.):
        return None
      self.last_log = timestamp
      self.last_ignore = result.would_ignore
      return {"event": "guide_paint_shadow", "schema": 1, "duration_ms": elapsed * 1000,
              "frame_id": int(model.frameId), "image_frame_id": int(frame_id),
              "timestamp_eof": int(model.timestampEof), "reason": result.reason, "active": False,
              "colors": [item.color for item in result.evidence], "support": [item.supported for item in result.evidence],
              "reasons": [item.reason for item in result.evidence], "visible": [item.visible for item in result.evidence],
              "would_ignore": list(result.would_ignore), "original_probs": list(model.laneLineProbs),
              "samples": self.samples, "proposal_samples": self.proposal_samples, "over_budget": self.over_budget}
    except Exception as exc:
      self.enabled = False
      self.assessor.reset()
      return {"event": "guide_paint_shadow_disabled", "active": False, "error": type(exc).__name__}
