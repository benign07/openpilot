"""Run real encoderd/loggerd rotation with road-only and full camera topology.

Uses synthetic VisionIPC frames in isolated PC prefixes, no vehicle or CAN I/O.
Missing-driver cases keep RecordFront both on and off: recording preference
must not cause a nonexistent stream to become a required rotation participant.
"""
import json
from openpilot.common.params import Params
from openpilot.common.prefix import OpenpilotPrefix
from openpilot.system.hardware import PC
from openpilot.system.loggerd.tests.test_loggerd import TestLoggerd
from openpilot.system.manager.process_config import managed_processes

assert PC, 'Run only on an isolated PC runner'
for include_driver, record_front in ((False, True), (False, False), (True, True)):
  with OpenpilotPrefix():
    params = Params()
    params.put('RecordRoadCam', 2)
    params.put('RecordAudio', True)
    params.put('RecordFront', record_front)
    try:
      # Existing test verifies sealed rlog/qlog plus videos for every segment;
      # LOGGERD_TEST disables timeout rotation, exposing missing participants.
      TestLoggerd().test_rotation(include_driver=include_driver)
      print(json.dumps(dict(driver_available=include_driver, record_front=record_front,
                            actual_encoder_rotation_passed=True)), flush=True)
    finally:
      managed_processes['loggerd'].stop()
      managed_processes['encoderd'].stop()
