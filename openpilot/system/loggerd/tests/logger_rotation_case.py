"""Deterministic executable rotation witness, using the real encoding test fixture."""
import json
import os
from pathlib import Path

from openpilot.common.params import Params
from openpilot.common.prefix import OpenpilotPrefix
from openpilot.system.hardware import PC
from openpilot.system.hardware.hw import Paths
from openpilot.system.loggerd.tests.test_loggerd import TestLoggerd
from openpilot.system.manager.process_config import managed_processes
from openpilot.tools.lib.logreader import LogReader


def run_case(label, include_driver, record_front, expect_stall=False):
  assert PC, 'Synthetic camera fixtures are PC only'
  assert expect_stall == (label == 'baseline' and not include_driver)
  expected_files = {'rlog.zst', 'qlog.zst', 'qcamera.ts', 'fcamera.hevc', 'ecamera.hevc'}
  if include_driver and record_front:
    expected_files.add('dcamera.hevc')
  with OpenpilotPrefix():
    params = Params()
    params.put('RecordRoadCam', 2)
    params.put('RecordAudio', True)
    params.put('RecordFront', record_front)
    try:
      witness = TestLoggerd()._publish_camera_and_audio_messages(
        num_segs=3, segment_length=5, include_driver=include_driver)
      assert os.environ.get('LOGGERD_TEST') == '1'  # Timeout fallback cannot hide a stall.
      assert 2 in witness['road_encoder_segments'], witness
      directories = {int(path.name.rsplit('--', 1)[1]): path
                     for path in Path(Paths.log_root()).iterdir() if path.is_dir()}
      if expect_stall:
        assert set(directories) == {0}, sorted(directories)
        checked = [0]
      else:
        assert {0, 1, 2} <= set(directories), sorted(directories)
        checked = [0, 1, 2]
      max_camera_id = 0
      encoded_segments = set()
      for segment in checked:
        path = directories[segment]
        files = {file.name for file in path.iterdir() if file.is_file()}
        assert files == expected_files, (label, segment, files, expected_files)
        assert all((path / name).stat().st_size > 0 for name in expected_files)
        for name in ('rlog.zst', 'qlog.zst'):
          events = list(LogReader(str(path / name)))
          assert events[0].which() == 'initData'
          assert events[1].which() == 'sentinel'
          assert str(events[1].sentinel.type) == ('startOfRoute' if segment == 0 else 'startOfSegment')
          assert events[-1].which() == 'sentinel'
          assert str(events[-1].sentinel.type) in ('endOfRoute', 'endOfSegment')
          for event in events:
            if event.which() == 'roadCameraState':
              max_camera_id = max(max_camera_id, int(event.roadCameraState.frameId))
            elif event.which() == 'roadEncodeIdx':
              encoded_segments.add(int(event.roadEncodeIdx.segmentNum))
      # A stopped or crashed logger is not a valid negative witness. It must
      # keep recording input while encoderd advances beyond its stuck segment.
      assert max_camera_id >= 290, max_camera_id
      if expect_stall:
        assert encoded_segments == {0}, encoded_segments
      else:
        assert {0, 1, 2} <= encoded_segments, encoded_segments
      result = {'label': label, 'driver_available': include_driver, 'record_front': record_front,
                'expected_stall': expect_stall, 'observed_logger_segments': sorted(directories),
                'observed_road_encoder_segments': witness['road_encoder_segments'],
                'advertised_streams': witness['advertised_streams'],
                'sealed_encoded_segments': sorted(encoded_segments),
                'max_logged_camera_frame': max_camera_id, 'case_passed': True}
      print(json.dumps(result), flush=True)
      return result
    finally:
      managed_processes['loggerd'].stop()
      managed_processes['encoderd'].stop()
