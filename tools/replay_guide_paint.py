#!/usr/bin/env python3
"""PC assessor for exact-frame, externally exported model/image records.

JSONL input rows must supply: image, image_sha256, frame_id, image_frame_id,
timestamp_s (capture EOF, not log publication), camera_id, calibrated,
calibration_age_s, intrinsics and camera_from_calib (3x3 in THIS image's pixel coordinates),
lanes (4 Nx3 arrays in model coordinates), probs and stds. No nearest-frame
matching or implicit resize. For decoded qcamera use an appropriately scaled
matrix and label resolution/codec limitations; this is not runtime NV12 parity.
No driving changes or vehicle connection. Keep inputs/results private.
"""
import argparse
import hashlib
import io
import importlib.util
import json
import os
from pathlib import Path
import sys
import uuid

import numpy as np
from PIL import Image


spec = importlib.util.spec_from_file_location("guide_paint_replay", Path(__file__).resolve().parents[1] / "selfdrive/modeld/guide_paint.py")
gp = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = gp
assessor_bytes = Path(spec.origin).read_bytes()
exec(compile(assessor_bytes, str(spec.origin), "exec"), gp.__dict__)


def replay(records: Path, output: Path):
  repo = Path(__file__).resolve().parents[1]
  if output.resolve().is_relative_to(repo) or records.resolve().is_relative_to(repo):
    raise ValueError("Keep driving records/results outside the source repository")
  if records.resolve() == output.resolve():
    raise ValueError("Output cannot overwrite input")
  assessor = gp.GuidePaintAssessor()
  count = 0
  proposals = 0
  previous = None
  summary_path = output.with_name(output.name + ".summary.json")
  if output.exists() or summary_path.exists():
    raise FileExistsError(output)
  partial = output.with_name(output.name + "." + uuid.uuid4().hex + ".partial")
  records_bytes = records.read_bytes()
  with partial.open("x", encoding="utf8") as dst:
    for raw in records_bytes.decode("utf8").splitlines():
      row = json.loads(raw)
      if type(row["calibrated"]) is not bool or type(row["frame_id"]) is not int or type(row["image_frame_id"]) is not int:
        raise ValueError("Invalid frame/calibration types")
      if type(row["timestamp_s"]) not in (int, float) or type(row["calibration_age_s"]) not in (int, float):
        raise ValueError("Invalid numeric timestamp/calibration age")
      if row["frame_id"] != row["image_frame_id"]:
        raise ValueError("Frame ID mismatch")
      if previous is not None and (row["frame_id"] <= previous[0] or row["timestamp_s"] <= previous[1]):
        raise ValueError("Non-increasing frame/time; replay different streams separately")
      previous = row["frame_id"], row["timestamp_s"]
      if Path(row["image"]).is_absolute():
        raise ValueError("Image path must be relative")
      image_path = records.parent / row["image"]
      if not image_path.resolve().is_relative_to(records.parent.resolve()):
        raise ValueError("Image outside records directory")
      if image_path.resolve() == output.resolve():
        raise ValueError("Output cannot overwrite image")
      image_bytes = image_path.read_bytes()
      if hashlib.sha256(image_bytes).hexdigest() != row["image_sha256"]:
        raise ValueError("Image hash mismatch")
      with Image.open(io.BytesIO(image_bytes)) as decoded:
        rgb = np.array(decoded.convert("RGB"), dtype=np.uint8)
      intrinsic = np.asarray(row["intrinsics"], dtype=float)
      if (intrinsic.shape != (3, 3) or not np.isfinite(intrinsic).all() or
          intrinsic[0, 0] <= 0 or intrinsic[1, 1] <= 0 or not np.allclose(intrinsic[2], [0., 0., 1.]) or
          not 0 <= intrinsic[0, 2] < rgb.shape[1] or not 0 <= intrinsic[1, 2] < rgb.shape[0]):
        raise ValueError("Invalid image intrinsics")
      matrix = np.asarray(row["camera_from_calib"], dtype=float)
      if matrix.shape != (3, 3) or not np.isfinite(matrix).all():
        raise ValueError("Invalid camera matrix")
      rotation = np.linalg.solve(intrinsic, matrix)
      if not np.allclose(rotation.T @ rotation, np.eye(3), rtol=0, atol=.002) or not np.isclose(np.linalg.det(rotation), 1., atol=.002):
        raise ValueError("Camera matrix is inconsistent with image intrinsics")
      result = assessor.observe(gp.RGBImage(rgb), row["lanes"], row["probs"], row["stds"], row["camera_from_calib"],
                                frame_id=row["frame_id"], image_frame_id=row["image_frame_id"], timestamp_s=row["timestamp_s"],
                                camera_id=row["camera_id"], calibrated=row["calibrated"], calibration_age_s=row["calibration_age_s"])
      if result.reason == "invalid_input":
        raise ValueError("Invalid model/calibration input")
      entry = {"schema": 1, "frame_id": row["frame_id"], "image_frame_id": row["image_frame_id"],
               "timestamp_s": row["timestamp_s"], "image_sha256": row["image_sha256"], "active": False,
               "evidence": [vars(e) for e in result.evidence], "would_ignore": result.would_ignore,
               "original_probs": row["probs"], "reason": result.reason}
      dst.write(json.dumps(entry, allow_nan=False) + "\n")
      count += 1
      proposals += int(any(result.would_ignore))
  if not count:
    raise ValueError("No records")
  summary = {"frames": count, "frames_with_proposal": proposals, "active": False,
          "records_sha256": hashlib.sha256(records_bytes).hexdigest(),
          "assessor_sha256": hashlib.sha256(assessor_bytes).hexdigest(),
          "results_sha256": hashlib.sha256(partial.read_bytes()).hexdigest(),
          "scope": "Offline evidence only; no labeled accuracy or road qualification implied."}
  summary_partial = partial.with_name(partial.name + ".summary")
  with summary_partial.open("x", encoding="utf8") as dst:
    json.dump(summary, dst, allow_nan=False, indent=2)
  # Each link is exclusive. Both final files are required for a complete run;
  # this is not a two-file atomic transaction. Failure never replaces a file.
  os.link(partial, output)
  os.link(summary_partial, summary_path)  # summary last: completion marker
  partial.unlink()
  summary_partial.unlink()
  return summary


if __name__ == "__main__":
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument("records", type=Path)
  parser.add_argument("output", type=Path)
  args = parser.parse_args()
  print(json.dumps(replay(args.records, args.output)))
