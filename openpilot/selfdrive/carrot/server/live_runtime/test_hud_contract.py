"""Carrot HUD's freshness and mode fields must remain available in Carrot Web."""

import time
import unittest
from types import SimpleNamespace

from .snapshot import build_live_payload


class HudContractTest(unittest.TestCase):
  def test_fresh_vehicle_and_mode_are_separate_from_stale_vehicle(self):
    now_ns = time.monotonic_ns()
    class FakeSM:
      data = {'carState': object(), 'longitudinalPlan': object()}
      alive = {'carState': True, 'longitudinalPlan': True}
      valid = {'carState': True, 'longitudinalPlan': True}
      logMonoTime = {'carState': now_ns, 'longitudinalPlan': now_ns}
      values = {
        'carState': SimpleNamespace(vEgo=12.0, canValid=True, canTimeout=False,
                                    cruiseState=SimpleNamespace(enabled=True, speed=15.0)),
        'longitudinalPlan': SimpleNamespace(myDrivingMode=3),
      }

      def __getitem__(self, name):
        return self.values[name]

    sm = FakeSM()
    payload = build_live_payload(sm=sm, repo_flavor='carrot')
    self.assertTrue(payload['runtime']['serviceValid']['carState'])
    self.assertLess(payload['runtime']['serviceAgeMs']['carState'], 1000)
    self.assertTrue(payload['services']['carState']['canValid'])
    self.assertFalse(payload['services']['carState']['canTimeout'])
    self.assertEqual(payload['services']['longitudinalPlan']['myDrivingMode'], 3)

    sm.valid['carState'] = False
    sm.logMonoTime['carState'] = now_ns - 3_000_000_000
    payload = build_live_payload(sm=sm, repo_flavor='carrot', previous_payload=payload)
    self.assertFalse(payload['runtime']['serviceValid']['carState'])
    self.assertGreater(payload['runtime']['serviceAgeMs']['carState'], 1000)


if __name__ == '__main__':
  unittest.main()
