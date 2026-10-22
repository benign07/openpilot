"""Performance refactor parity against frozen 88ffc175, not semantic accuracy.

The fixture is byte-exact previous production research code, independently
identified by SHA. It deliberately does not share the new batch helpers.
"""
import hashlib
import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace

import numpy as np
import pytest

def load(name, path):
  spec = importlib.util.spec_from_file_location(name, path)
  module = importlib.util.module_from_spec(spec)
  sys.modules[name] = module
  spec.loader.exec_module(module)
  return module


shared = load("guide_test_helpers", Path(__file__).parent / "test_guide_paint.py")
ROOT, gp, K, MATRIX, LINE, painted_image, as_nv12, GREEN, PINK = (
  getattr(shared, name) for name in ("ROOT", "gp", "K", "MATRIX", "LINE", "painted_image", "as_nv12", "GREEN", "PINK"))


REF_PATH = Path(__file__).parent / "fixtures/guide_paint_reference_88ffc175.py"
assert hashlib.sha256(REF_PATH.read_bytes()).hexdigest() == "c9d36100db948096037e80879480b2523500fa449a3dfbf93c1c75b89223bf6b"
reference = load("guide_frozen_88ffc175", REF_PATH)


def fields(item):
  return item.color, item.supported, item.visible, item.reason


@pytest.mark.parametrize("color", [GREEN, PINK, (235, 235, 235), (230, 200, 30), (40, 110, 235), (65, 65, 65)])
@pytest.mark.parametrize("nv12", [False, True])
def test_batch_scene_matches_frozen_reference(color, nv12):
  image = painted_image(color)
  if nv12:
    image = as_nv12(image)
  for n in range(1, 5):
    lines = [LINE + [0., .2 * (i % 2), 0.] for i in range(n)]
    assert [fields(e) for e in gp.assess_lanes(image, lines, MATRIX)] == [fields(reference.assess_lane(image, line, MATRIX)) for line in lines]


def test_batch_projection_matches_scalar_at_ulp_pixel_and_geometry_edges():
  for z in [.5, np.nextafter(.5, 0), np.nextafter(.5, 1), 2.5, np.nextafter(2.5, 0), np.nextafter(2.5, 3), 1e308]:
    for shift in [0., .5, np.nextafter(.5, 0), np.nextafter(.5, 1), -319., 318.5]:
      matrix = MATRIX.copy()
      matrix[0, 0] += shift
      line = LINE.copy()
      line[:, 2] = z
      uv, visible, valid = gp._project_lanes([line] * 4, matrix, 640, 480)
      expected = reference.projected_samples(line, matrix, 640, 480)
      if expected is None:
        assert not valid.any()
      else:
        assert valid.all()
        assert np.array_equal(uv, np.repeat(expected[0][None], 4, axis=0))
        assert np.array_equal(visible, np.repeat(expected[1][None], 4, axis=0))


def test_batch_random_mixed_temporal_inputs_match_frozen():
  rng = np.random.default_rng(20261022)
  white_green = painted_image((235, 235, 235)).rgb.copy()
  guide = painted_image(GREEN, offset=.2)
  mask = np.any(guide.rgb != 65, axis=-1)
  white_green[mask] = guide.rgb[mask]
  images = [painted_image(GREEN), painted_image(PINK), painted_image((235, 235, 235)), painted_image((40, 110, 235)),
            gp.RGBImage(white_green), gp.RGBImage(rng.integers(0, 256, (480, 640, 3), dtype=np.uint8))]
  images += [as_nv12(image) for image in images[:4]]
  old = reference.GuidePaintAssessor()
  batch = gp.GuidePaintAssessor()
  timestamp = 1.
  for i in range(2000):
    lines = []
    for j in range(4):
      line = LINE.copy()
      kind = int(rng.integers(0, 16))
      if kind == 0: line = line[::2]  # ragged; still covers32m
      elif kind == 1: line[5, 0] = 0.
      elif kind == 2: line[5, 1] = np.nan
      elif kind == 3: line = line[:10]
      elif kind == 4: line[:, 2] = rng.choice([.5, 2.5, 1.2, .49999999999999994, 2.5000000000000004])
      elif kind == 5: line[:, 1] += float(rng.uniform(-.3, .3))
      elif kind == 6: line = np.tile(line[:1], (65, 1))
      elif kind == 7: line[5, 0] = np.nan
      elif kind == 8: line[5, 2] = np.inf
      elif kind == 9: line[5, 2] = np.nan
      elif kind == 10: line = line.astype(np.float32)
      elif kind == 11: line = line.tolist()
      lines.append(line)
    probs = [float(v) for v in rng.choice([.3, np.nextafter(.3, 0), np.nextafter(.3, 1), .9, .1], 4)]
    stds = [float(v) for v in rng.choice([.1, .15, np.nextafter(.15, 0), np.nextafter(.15, 1), .4], 4)]
    image = images[i % len(images)]
    matrix = MATRIX.copy()
    if i % 7 == 0: matrix[0, 0] += rng.choice([.5, .1, -1., 5.])
    timestamp += float(rng.choice([.2, .2, .2, .05, .5]))
    args = dict(frame_id=i * 4, image_frame_id=i * 4, timestamp_s=timestamp, camera_id="mixed",
                calibrated=True, calibration_age_s=.1)
    expected = old.observe(image, lines, probs, stds, matrix, **args)
    actual = batch.observe(image, lines, probs, stds, matrix, **args)
    assert [fields(e) for e in actual.evidence] == [fields(e) for e in expected.evidence], i
    assert actual.would_ignore == expected.would_ignore and actual.reason == expected.reason, i


def test_batch_row_rules_match_frozen_at_float32_thresholds(monkeypatch):
  rng = np.random.default_rng(43304)
  values = np.array([0., 15., 25., 40., 45., 75., 90., 128., 160., 200., 255.], dtype=np.float32)
  values = np.concatenate((values, np.nextafter(values, np.float32(0.)), np.nextafter(values, np.float32(256.))))
  for i in range(3000):
    n = i % 4 + 1
    rgb = (rng.choice(values, (n, 13, 11, 3)) if i % 2 else rng.uniform(0., 255., (n, 13, 11, 3))).astype(np.float32)
    if i % 4 == 0:
      rgb[:] = [65, 65, 65]
      rgb[:, :, 4:7] = rng.choice([GREEN, PINK], size=(n, 13))[:, :, None, :]
    visible = rng.random((n, 13)) > .1
    visible[:, :10] = True
    codes, counts = gp._classify_numpy(rgb, visible, visible.sum(axis=1))
    for j in range(n):
      uv = np.zeros((13, 11, 2), dtype=int)
      uv[:, 5, 1] = np.arange(13)
      monkeypatch.setattr(reference, "projected_samples", lambda *args: (uv, visible[j], None))
      expected = reference.assess_lane(SimpleNamespace(width=640, height=480, sample=lambda unused: rgb[j]), LINE, MATRIX)
      code = 3 if expected.reason == "ambiguous_boundary_neighbor" else {"green": 1, "pink": 2}.get(expected.color, 0)
      assert (int(codes[j]), int(counts[j])) == (code, expected.supported), (i, j, expected)


def test_equal_matrix_does_not_bypass_invalid_probe_depth():
  assessor = gp.GuidePaintAssessor()
  assessor.last_matrix = np.zeros((3, 3))
  assert assessor.projection_changed(assessor.last_matrix)


def test_projection_probe_constant_is_readonly():
  with pytest.raises(ValueError):
    gp.PROJECTION_PROBES[0, 0] = 1.


def test_mixed_green_and_pink_batch_keeps_both_colors():
  green = painted_image(GREEN, offset=-.4).rgb.copy()
  pink = painted_image(PINK, offset=.4).rgb
  mask = np.any(pink != 65, axis=-1)
  green[mask] = pink[mask]
  image = gp.RGBImage(green)
  lines = [LINE + [0., -.4, 0.], LINE + [0., .4, 0.]]
  actual = gp.assess_lanes(image, lines, MATRIX)
  assert [e.color for e in actual] == ['green', 'pink']
  assert [fields(e) for e in actual] == [fields(reference.assess_lane(image, line, MATRIX)) for line in lines]
