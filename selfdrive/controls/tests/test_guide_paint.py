"""Synthetic contracts; these are NOT real-world guide-paint accuracy tests."""
import copy
import ast
import importlib.util
from pathlib import Path
import sys
import json
import hashlib
from types import SimpleNamespace

import numpy as np
import pytest


ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location("lx3_guide_paint", ROOT / "selfdrive/modeld/guide_paint.py")
gp = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = gp
spec.loader.exec_module(gp)

K = np.array([[800., 0, 320], [0, 800., 240], [0, 0, 1]])
VIEW = np.array([[0., 1, 0], [0, 0, 1], [1, 0, 0]])
MATRIX = K @ VIEW
LINE = np.column_stack((np.arange(0., 50.), np.zeros(50), np.full(50, 1.2)))
GREEN = (45, 190, 55)
PINK = (215, 65, 180)


def painted_image(color=GREEN, offset=0., half_width=.055, background=(65, 65, 65)):
  rgb = np.full((480, 640, 3), background, dtype=np.uint8)
  # Analytic pinhole ground-plane raster, independent of production projection.
  for row in range(265, 479):
    x = 800 * 1.2 / (row - 240)
    if 8 <= x <= 32:
      lo, hi = np.rint(320 + 800 * np.array([offset - half_width, offset + half_width]) / x).astype(int)
      rgb[row, max(0, lo):min(640, hi + 1)] = color
  return gp.RGBImage(rgb)


def observe(assessor, image=None, time_s=1., frame=1, **kwargs):
  args = dict(frame_id=frame, image_frame_id=frame, timestamp_s=time_s, camera_id="road", calibrated=True, calibration_age_s=.2)
  args.update(kwargs)
  return assessor.observe(image or painted_image(), [LINE] * 4, [.9] * 4, [.1] * 4, MATRIX, **args)


def confirmed(assessor=None, image=None):
  assessor = assessor or gp.GuidePaintAssessor()
  for i in range(4):
    result = observe(assessor, image, 1 + i * .2, i + 1)
  return assessor, result


@pytest.mark.parametrize("color,name", [(GREEN, "green"), (PINK, "pink")])
def test_exact_stripe_requires_spatial_and_temporal_support(color, name):
  assessor = gp.GuidePaintAssessor()
  image = painted_image(color)
  for i in range(4):
    result = observe(assessor, image, 1 + i * .2, i + 1)
    assert all(e.color == name for e in result.evidence)
    assert result.would_ignore == ((True,) * 4 if i == 3 else (False,) * 4)
  assert not hasattr(result, "proposed_probs")


@pytest.mark.parametrize("color", [(235, 235, 235), (230, 200, 30), (40, 110, 235), (200, 45, 30), (65, 65, 65), (80, 90, 85)])
def test_white_yellow_blue_red_asphalt_not_guides(color):
  _, result = confirmed(image=painted_image(color))
  assert not any(result.would_ignore)
  assert not hasattr(result, "proposed_probs")


@pytest.mark.parametrize("color", [GREEN, PINK])
def test_adjacent_guide_does_not_remove_real_white_line(color):
  rgb = painted_image((235, 235, 235)).rgb.copy()
  guide = painted_image(color, offset=.20)
  mask = np.any(guide.rgb != 65, axis=-1)
  rgb[mask] = guide.rgb[mask]
  _, result = confirmed(image=gp.RGBImage(rgb))
  assert not any(result.would_ignore)


def test_white_neighbor_vetoes_green_candidate():
  rgb = painted_image().rgb.copy()
  white = painted_image((235, 235, 235), offset=.12, half_width=.025)
  mask = np.any(white.rgb != 65, axis=-1)
  rgb[mask] = white.rgb[mask]
  _, result = confirmed(image=gp.RGBImage(rgb))
  assert all(e.reason == "ambiguous_boundary_neighbor" for e in result.evidence)
  assert not any(result.would_ignore)


@pytest.mark.parametrize("background", [GREEN, PINK, (150, 50, 50)])
def test_colored_road_is_not_a_narrow_guide(background):
  _, result = confirmed(image=painted_image(background, background=background))
  assert not any(result.would_ignore)


def test_small_colored_patch_or_signal_not_enough():
  rgb = painted_image().rgb.copy()
  rgb[285:, :, :] = 65
  _, result = confirmed(image=gp.RGBImage(rgb))
  assert not any(result.would_ignore)


def test_nv12_odd_coordinates_padding_and_read_only():
  data = bytearray([0] * (16 * 12))
  image = gp.NV12Image(data, 8, 6, 16, 128)
  data[3 * 16 + 5] = 100
  data[128 + 16 + 4] = 140
  data[128 + 16 + 5] = 180
  rgb = image.sample(np.array([[5, 3]]))[0]
  assert np.allclose(rgb, [100 + 1.13983 * 52, 100 - .39465 * 12 - .58060 * 52, 100 + 2.03211 * 12])
  with pytest.raises(ValueError):
    image.data[0] = 1
  assert data[0] == 0


@pytest.mark.parametrize("layout", [(8, 6, 7, 48), (7, 6, 16, 96), (8, 5, 16, 96), (8, 6, 16, 80), (8, 6, 15, 100), (8, 6, 16, 1000)])
def test_bad_nv12_layout_rejected(layout):
  with pytest.raises(ValueError):
    gp.NV12Image(bytes(200), *layout)


def test_projection_sign_and_camera_height_not_doubled():
  pixels, visible, _ = gp.projected_samples(LINE, MATRIX, 640, 480)
  assert visible.all()
  assert np.array_equal(pixels[0, 5], [320, 360])  # x=8, z=1.2
  shifted = LINE.copy()
  shifted[:, 1] = .5
  p2, _, _ = gp.projected_samples(shifted, MATRIX, 640, 480)
  assert p2[0, 5, 0] == 370  # positive y is right


@pytest.mark.parametrize("kind", ["nonmonotonic", "nan", "short", "horizon", "behind", "too_many"])
def test_bad_or_missing_geometry_has_no_proposal(kind):
  line = LINE.copy()
  if kind == "nonmonotonic":
    line[4, 0] = 0
  elif kind == "nan":
    line[4, 1] = np.nan
  elif kind == "short":
    line = line[:10]
  elif kind == "horizon":
    line[:, 2] = 0
  elif kind == "behind":
    line[:, 0] -= 100
  else:
    line = np.zeros((100, 3))
  assert gp.assess_lane(painted_image(), line, MATRIX).color == "unknown"


@pytest.mark.parametrize("change", [dict(calibrated=False), dict(calibration_age_s=2.01), dict(calibration_age_s=-1.),
                                   dict(image_frame_id=999), dict(timestamp_s=float("nan"))])
def test_invalid_inputs_reset_confirmation(change):
  assessor, _ = confirmed()
  result = observe(assessor, time_s=1.8, frame=5, **change)
  assert result.reason == "invalid_input"
  assert not any(result.would_ignore)
  assert not any(observe(assessor, time_s=2., frame=6).would_ignore)


@pytest.mark.parametrize("change", [dict(time_s=2.5), dict(time_s=1.5), dict(frame=4), dict(camera_id="different")])
def test_discontinuous_stream_resets(change):
  assessor, _ = confirmed()
  args = dict(time_s=1.8, frame=5)
  args.update(change)
  assert not any(observe(assessor, **args).would_ignore)


def test_calibration_change_and_line_identity_jump_reset():
  assessor, _ = confirmed()
  matrix = MATRIX.copy()
  matrix[0, 0] += 20  # large horizontal projection shift
  result = assessor.observe(painted_image(), [LINE] * 4, [.9] * 4, [.1] * 4, matrix, frame_id=5,
                            image_frame_id=5, timestamp_s=1.8, camera_id="road", calibrated=True, calibration_age_s=.2)
  assert not any(result.would_ignore)
  assessor, _ = confirmed()
  shifted = LINE.copy()
  shifted[:, 1] += .2
  result = assessor.observe(painted_image(offset=.2), [shifted] * 4, [.9] * 4, [.1] * 4, MATRIX, frame_id=5,
                            image_frame_id=5, timestamp_s=1.8, camera_id="road", calibrated=True, calibration_age_s=.2)
  assert not any(result.would_ignore)


def test_evidence_loss_immediately_removes_proposal():
  assessor, _ = confirmed()
  assert not any(observe(assessor, painted_image((235, 235, 235)), 1.8, 5).would_ignore)


def make_buf_model():
  buf = SimpleNamespace(data=bytes(640 * 480 * 3 // 2), width=640, height=480, stride=640, uv_offset=640 * 480)
  model = SimpleNamespace(frameId=1, timestampEof=1_000_000_000, laneLineProbs=[.9] * 4, laneLineStds=[.1] * 4,
                          laneLines=[SimpleNamespace(x=LINE[:, 0].tolist(), y=LINE[:, 1].tolist(), z=LINE[:, 2].tolist()) for _ in range(4)])
  return buf, model


def test_disabled_by_default_does_not_read_buffer():
  assert gp.GuidePaintShadow().observe(None, None, None, frame_id=0, calibrated=True, calibration_age_s=0.) is None


def test_shadow_does_not_mutate_any_model_field_or_camera_bytes():
  buf, model = make_buf_model()
  before = copy.deepcopy(model.__dict__)
  before_bytes = buf.data
  clock = iter([0., .0001]).__next__
  result = gp.GuidePaintShadow(True, clock).observe(buf, model, MATRIX, frame_id=1, calibrated=True, calibration_age_s=.1)
  assert result["active"] is False
  assert model.__dict__ == before and buf.data == before_bytes


def test_observer_error_disables_it_without_raising():
  _, model = make_buf_model()
  observer = gp.GuidePaintShadow(True)
  result = observer.observe(None, model, MATRIX, frame_id=1, calibrated=True, calibration_age_s=.1)
  assert result["event"] == "guide_paint_shadow_disabled"
  assert not observer.enabled


@pytest.mark.parametrize("kwargs", [dict(wide_camera=True), dict(eligible=False), dict(calibrated=False), dict(calibration_age_s=10.)])
def test_runtime_ineligible_resets_without_reading_buffer(kwargs):
  observer = gp.GuidePaintShadow(True)
  observer.assessor, _ = confirmed()
  args = dict(frame_id=1, calibrated=True, calibration_age_s=.1)
  args.update(kwargs)
  assert observer.observe(None, None, MATRIX, **args) is None
  assert observer.assessor.last_time is None


def test_budget_backoff():
  buf, model = make_buf_model()
  clock = iter([0., .003]).__next__
  observer = gp.GuidePaintShadow(True, clock)
  result = observer.observe(buf, model, MATRIX, frame_id=1, calibrated=True, calibration_age_s=.1)
  assert result["event"] == "guide_paint_shadow_budget"
  assert result["samples"] == 0 and result["proposal_samples"] == 0 and result["over_budget"] == 1
  assert observer.assessor.last_time is None
  model.timestampEof += 200_000_000
  model.frameId += 4
  # A backoff must skip sampling even after resetting the temporal tracker.
  assert observer.observe(None, model, MATRIX, frame_id=5, calibrated=True, calibration_age_s=.1) is None


def as_nv12(image):
  rgb = image.rgb.astype(float)
  inverse = np.linalg.inv([[1., 0., 1.13983], [1., -.39465, -.58060], [1., 2.03211, 0.]])
  yuv = rgb @ inverse.T
  stride = 768
  offset = stride * 480
  raw = np.zeros(offset + stride * 240, dtype=np.uint8)
  raw[:offset].reshape(480, stride)[:, :640] = yuv[:, :, 0].clip(0, 255).astype(np.uint8)
  chroma = yuv[:, :, 1:].reshape(240, 2, 320, 2, 2).mean(axis=(1, 3))
  raw[offset:].reshape(240, stride)[:, :640] = (chroma + 128).clip(0, 255).astype(np.uint8).reshape(240, 640)
  return gp.NV12Image(raw, 640, 480, stride, offset)


@pytest.mark.parametrize("color", [GREEN, PINK, (235, 235, 235), (230, 200, 30), (40, 110, 235)])
def test_scene_through_padded_chroma_subsampled_nv12(color):
  image = as_nv12(painted_image(color))
  _, result = confirmed(image=image)
  assert result.would_ignore == ((True,) * 4 if color in (GREEN, PINK) else (False,) * 4)


def test_dim_white_neighbor_remains_veto():
  rgb = painted_image().rgb.copy()
  white = painted_image((235, 235, 235), offset=.12, half_width=.025)
  mask = np.any(white.rgb != 65, axis=-1)
  rgb[mask] = white.rgb[mask]
  image = gp.RGBImage((rgb * .4).astype(np.uint8))
  _, result = confirmed(image=image)
  assert all(e.reason == "ambiguous_boundary_neighbor" for e in result.evidence)
  assert not any(result.would_ignore)


def test_uncertain_model_does_not_qualify_even_on_guide():
  assessor = gp.GuidePaintAssessor()
  for i in range(4):
    result = assessor.observe(painted_image(), [LINE] * 4, [.9] * 4, [.3] * 4, MATRIX, frame_id=i + 1,
                              image_frame_id=i + 1, timestamp_s=1 + i * .2, camera_id="road", calibrated=True, calibration_age_s=.2)
  assert all(e.reason == "model_uncertain" for e in result.evidence)
  assert not any(result.would_ignore)


def test_projection_reset_in_pixel_units():
  assessor, _ = confirmed()
  theta = np.radians(.05)
  yaw = np.array([[np.cos(theta), -np.sin(theta), 0], [np.sin(theta), np.cos(theta), 0], [0, 0, 1.]])
  assert not assessor.projection_changed(MATRIX @ yaw)
  theta = np.radians(.5)
  yaw = np.array([[np.cos(theta), -np.sin(theta), 0], [np.sin(theta), np.cos(theta), 0], [0, 0, 1.]])
  assert assessor.projection_changed(MATRIX @ yaw)


def test_changed_shadow_event_is_not_hidden_behind_five_second_log():
  buf, model = make_buf_model()
  observer = gp.GuidePaintShadow(True, lambda: 0.)
  assert observer.observe(buf, model, MATRIX, frame_id=1, calibrated=True, calibration_age_s=.1)
  observer.assessor.observe = lambda *a, **kw: gp.GuidePaintResult((), (True,) * 4, "shadow_proposal_only")
  model.timestampEof += 1_200_000_000
  model.frameId += 24
  result = observer.observe(buf, model, MATRIX, frame_id=model.frameId, calibrated=True, calibration_age_s=.1)
  assert result["would_ignore"] == [True] * 4 and result["proposal_samples"] == 1


def test_assessor_has_no_modeld_hook_or_model_writes():
  source = (ROOT / "selfdrive/modeld/modeld.py").read_bytes()
  assert b"guide_paint" not in source
  # Exact separately verified installed seq16 source, not merely no hook text.
  assert hashlib.sha256(source).hexdigest() == "7bb36544d72537317518e254deffe78be07ca82b519d4294833b78a3df069c86"
  guide_tree = ast.parse(Path(gp.__file__).read_text(encoding="utf8"))
  assert not any(isinstance(n, ast.Attribute) and isinstance(n.ctx, ast.Store) and n.attr in ("laneLines", "laneLineProbs") for n in ast.walk(guide_tree))
  assert not any(isinstance(n, ast.Subscript) and isinstance(n.ctx, ast.Store) and
                 isinstance(n.value, ast.Attribute) and n.value.attr in ("laneLines", "laneLineProbs") for n in ast.walk(guide_tree))
  assert not any(isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "init" and
                 n.args and isinstance(n.args[0], ast.Constant) and n.args[0].value == "laneLines" for n in ast.walk(guide_tree))


@pytest.fixture
def replay_case(tmp_path):
  from PIL import Image
  spec = importlib.util.spec_from_file_location("lx3_guide_replay", ROOT / "tools/replay_guide_paint.py")
  replay = importlib.util.module_from_spec(spec)
  spec.loader.exec_module(replay)
  image = tmp_path / "image.png"
  Image.fromarray(painted_image().rgb).save(image)
  row = dict(image=image.name, image_sha256=hashlib.sha256(image.read_bytes()).hexdigest(), frame_id=1, image_frame_id=1,
             timestamp_s=1., camera_id="road", calibrated=True, calibration_age_s=.1, intrinsics=K.tolist(),
             camera_from_calib=MATRIX.tolist(), lanes=[LINE.tolist()] * 4, probs=[.9] * 4, stds=[.1] * 4)
  return replay, tmp_path / "records.jsonl", tmp_path / "result.jsonl", row


@pytest.mark.parametrize("failure", ["frame", "hash", "duplicate", "intrinsics", "matrix_scale", "path", "nan", "bool_frame", "float_frame", "calibration_string", "bool_time", "str_time", "bool_age"])
def test_replay_rejects_unbound_or_invalid_record(replay_case, failure):
  replay, records, output, row = replay_case
  if failure == "frame": row["image_frame_id"] += 1
  elif failure == "hash": row["image_sha256"] = "0" * 64
  elif failure == "intrinsics": row["intrinsics"][0][2] = 99999
  elif failure == "path": row["image"] = "../image.png"
  elif failure == "nan": row["probs"][0] = float("nan")
  elif failure == "matrix_scale": row["camera_from_calib"] = (np.diag([4., 4., 1.]) @ MATRIX).tolist()
  elif failure == "bool_frame": row["frame_id"] = True
  elif failure == "float_frame": row["frame_id"] = 1.
  elif failure == "calibration_string": row["calibrated"] = "false"
  elif failure == "bool_time": row["timestamp_s"] = True
  elif failure == "str_time": row["timestamp_s"] = "1.0"
  elif failure == "bool_age": row["calibration_age_s"] = False
  records.write_text(json.dumps(row) + "\n" + (json.dumps(row) + "\n" if failure == "duplicate" else ""), encoding="utf8")
  with pytest.raises(ValueError):
    replay.replay(records, output)
  assert not output.exists()
  assert not output.with_name(output.name + ".summary.json").exists()


def test_offline_replay_hash_provenance_and_exclusive_output(replay_case):
  replay, records, output, row = replay_case
  rows = []
  for i in range(4):
    item = {**row, "frame_id": i + 1, "image_frame_id": i + 1, "timestamp_s": 1 + i * .2}
    rows.append(json.dumps(item))
  records.write_text("\n".join(rows) + "\n", encoding="utf8")
  result = replay.replay(records, output)
  assert result["frames"] == 4 and result["frames_with_proposal"] == 1
  assert result["records_sha256"] == hashlib.sha256(records.read_bytes()).hexdigest()
  sidecar = output.with_name(output.name + ".summary.json")
  assert json.loads(sidecar.read_bytes()) == result
  assert result["results_sha256"] == hashlib.sha256(output.read_bytes()).hexdigest()
  before = output.read_bytes()
  with pytest.raises(FileExistsError):
    replay.replay(records, output)
  assert output.read_bytes() == before


def test_replay_decodes_and_reports_the_bytes_it_hashed(replay_case, monkeypatch):
  replay, records, output, row = replay_case
  records.write_text(json.dumps(row) + "\n", encoding="utf8")
  original_records = records.read_bytes()
  original_open = replay.Image.open
  def checked_open(stream):
    import io
    assert isinstance(stream, io.BytesIO)
    (records.parent / row["image"]).write_bytes(b"file changed after hashing")
    records.write_bytes(b"records changed after reading")
    return original_open(stream)
  monkeypatch.setattr(replay.Image, "open", checked_open)
  result = replay.replay(records, output)
  assert result["records_sha256"] == hashlib.sha256(original_records).hexdigest()
  assert result["frames"] == 1


def test_empty_replay_is_not_a_success(replay_case):
  replay, records, output, _ = replay_case
  records.write_bytes(b"")
  with pytest.raises(ValueError, match="No records"):
    replay.replay(records, output)
  assert not output.exists() and not output.with_name(output.name + ".summary.json").exists()


def test_sidecar_is_not_published_if_result_link_fails(replay_case, monkeypatch):
  replay, records, output, row = replay_case
  records.write_text(json.dumps(row) + "\n", encoding="utf8")
  def refuse_link(src, dst):
    assert Path(dst) == output
    raise FileExistsError("Simulated competing output")
  monkeypatch.setattr(replay.os, "link", refuse_link)
  with pytest.raises(FileExistsError):
    replay.replay(records, output)
  assert not output.with_name(output.name + ".summary.json").exists()
