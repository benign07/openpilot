"""Production pause and suspension paths; no hardware or control permission setters."""
import ast
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('manual_steering_source', ROOT / 'manual_steering.py')
source = importlib.util.module_from_spec(spec)
spec.loader.exec_module(source)


class TestManualSteering(unittest.TestCase):
  def setUp(self):
    self.pause = source.ManualSteeringPause(.01)
    self.inputs = dict(enabled=True, lane_change_off=True, left_blinker=False, right_blinker=False,
                       steering_pressed=False, steering_angle=0., can_valid=True, can_timeout=False, lateral_active=True)

  def test_fade_is_one_second_and_cancellation_is_immediate(self):
    self.step(left_blinker=True)
    self.assertAlmostEqual(self.pause.scale, .99)
    self.step(49); self.assertAlmostEqual(self.pause.scale, .5)
    self.step(50); self.assertEqual(self.pause.scale, 0.)
    self.setUp();self.step(left_blinker=True);self.step(lateral_active=False)
    self.assertEqual(self.pause.scale, 0.)
    self.step(30, lateral_active=True);self.assertEqual(self.pause.scale, 0.)

  def step(self, count=1, **changes):
    self.inputs.update(changes)
    for _ in range(count):
      result = self.pause.update(**self.inputs)
    return result

  def test_either_single_signal_suspends_immediately(self):
    for left, right in ((True, False), (False, True)):
      with self.subTest(left=left, right=right):
        self.setUp()
        self.assertTrue(self.step(left_blinker=left, right_blinker=right))

  def test_hazards_alone_do_not_start_pause_but_cannot_end_one(self):
    self.assertFalse(self.step(200, left_blinker=True, right_blinker=True))
    self.assertTrue(self.step(right_blinker=False))
    self.assertTrue(self.step(200, right_blinker=True))
    self.assertTrue(self.step(99, left_blinker=False, right_blinker=False))
    self.assertFalse(self.step())

  def test_default_off_or_automatic_lane_change_keeps_stock(self):
    self.assertFalse(self.step(500, enabled=False, left_blinker=True))
    self.assertFalse(self.step(500, enabled=True, lane_change_off=False))

  def test_one_second_clear_required(self):
    self.assertTrue(self.step(left_blinker=True))
    self.assertTrue(self.step(99, left_blinker=False))
    self.assertFalse(self.step())

  def test_side_change_and_brief_signal_gaps_cannot_resume(self):
    self.step(left_blinker=True)
    for _ in range(5):
      self.assertTrue(self.step(60, left_blinker=False))
      self.assertTrue(self.step(60, right_blinker=True))
      self.assertTrue(self.step(60, left_blinker=True, right_blinker=False))

  def test_driver_steering_and_large_angle_delay_return(self):
    self.step(left_blinker=True)
    self.assertTrue(self.step(200, left_blinker=False, steering_pressed=True))
    for angle in (-90., -15., 15., 90., float('nan'), float('inf')):
      self.assertTrue(self.step(200, steering_pressed=False, steering_angle=angle))
    self.assertTrue(self.step(99, steering_angle=0.))
    self.assertFalse(self.step())

  def test_any_reasserted_input_restarts_return_timer(self):
    self.step(right_blinker=True)
    for obstruction in ({'steering_pressed':True}, {'steering_angle':20.}, {'can_valid':False}, {'can_timeout':True}):
      self.assertTrue(self.step(99, right_blinker=False, steering_pressed=False, steering_angle=0., can_valid=True, can_timeout=False))
      self.assertTrue(self.step(**obstruction))
    self.assertTrue(self.step(99, can_timeout=False))
    self.assertFalse(self.step())

  def test_setting_or_mode_changes_do_not_end_existing_pause(self):
    self.step(left_blinker=True)
    self.assertTrue(self.step(200, enabled=False, lane_change_off=False))
    self.assertTrue(self.step(99, left_blinker=False))
    self.assertFalse(self.step())
    self.assertFalse(self.step(left_blinker=True))

  def test_enable_option_with_signal_already_on_pauses(self):
    self.assertFalse(self.step(enabled=False, left_blinker=True))
    self.assertTrue(self.step(enabled=True))

  def test_can_invalidity_does_not_end_existing_pause(self):
    self.step(left_blinker=True)
    self.assertTrue(self.step(200, left_blinker=False, can_valid=False))
    self.assertTrue(self.step(200, can_valid=True, can_timeout=True))
    self.assertTrue(self.step(99, can_timeout=False))
    self.assertFalse(self.step())


class TestProductionSuspension(unittest.TestCase):
  def setUp(self):
    # Execute the complete production class with only its Params/clock imports
    # supplied locally, so this also runs on Windows without IPC/Params binaries.
    path = ROOT / 'carrot_controls.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))
    tree.body = [n for n in tree.body if isinstance(n, ast.ClassDef)]
    self.values = {'LatSuspendAngleDeg':300, 'ManualSteerWithBlinker':1, 'LaneChangeNeedTorque':-1}
    params = SimpleNamespace(get_int=lambda k:int(self.values.get(k, 0)), get_bool=lambda k:bool(self.values.get(k, 0)))
    ns = {'Params':lambda:params, 'DT_CTRL':.01, 'ManualSteeringPause':source.ManualSteeringPause,
          'uses_lx3_authority':lambda cp:True}
    exec(compile(tree, str(path), 'exec'), ns)
    self.controls = ns['CarrotControls'](SimpleNamespace())
    self.cs = SimpleNamespace(leftBlinker=False, rightBlinker=False, steeringPressed=False,
                              steeringAngleDeg=0., canValid=True, canTimeout=False)

  def test_actual_method_suspends_and_cannot_grant_lateral(self):
    self.assertTrue(self.controls.lat_suspend_control(self.cs, True))
    self.cs.leftBlinker = True
    for _ in range(99):
      self.assertTrue(self.controls.lat_suspend_control(self.cs, True))
    self.assertFalse(self.controls.lat_suspend_control(self.cs, True))
    self.cs.leftBlinker = False
    for _ in range(100):
      self.assertFalse(self.controls.lat_suspend_control(self.cs, False))
    self.assertFalse(self.controls.manual_steering.paused)
    self.assertTrue(self.controls.lat_suspend_control(self.cs, True))

  def test_legacy_angle_suspension_remains_independent(self):
    self.values['ManualSteerWithBlinker'] = 0
    self.cs.steeringPressed = True
    self.cs.steeringAngleDeg = 301.
    for _ in range(100):
      result = self.controls.lat_suspend_control(self.cs, True)
    self.assertFalse(result)
    self.cs.steeringAngleDeg = 0.
    self.cs.steeringPressed = False
    # Stock suspension counts its entry frame toward the 0.5-second hold.
    for _ in range(48):
      self.assertFalse(self.controls.lat_suspend_control(self.cs, True))
    self.assertTrue(self.controls.lat_suspend_control(self.cs, True))

  def test_disabling_auto_lane_change_alone_does_not_change_output(self):
    self.values['ManualSteerWithBlinker'] = 0
    self.cs.leftBlinker = True
    for _ in range(500):
      self.assertTrue(self.controls.lat_suspend_control(self.cs, True))

  def test_settings_refresh_does_not_unlatch_handoff(self):
    self.cs.leftBlinker = True
    for _ in range(99):
      self.assertTrue(self.controls.lat_suspend_control(self.cs, True))
    self.assertFalse(self.controls.lat_suspend_control(self.cs, True))
    self.values['ManualSteerWithBlinker'] = 0
    self.values['LaneChangeNeedTorque'] = 1
    for _ in range(200):
      self.assertFalse(self.controls.lat_suspend_control(self.cs, True))
    self.assertFalse(self.controls.manual_steering_enabled)
    self.assertFalse(self.controls.manual_lane_change_off)

  def test_other_profiles_are_unchanged_even_with_setting_on(self):
    self.controls.manual_supported=False
    self.cs.leftBlinker=True
    for _ in range(200):
      self.assertTrue(self.controls.lat_suspend_control(self.cs, True))
      self.assertEqual(self.controls.manual_steering.scale,1.)


class TestActualCeiling(unittest.TestCase):
  def test_monotonic_captured_ceiling_and_stronger_driver_yield(self):
    path=ROOT.parents[2]/'opendbc_repo/opendbc/car/hyundai/manual_steering.py'
    spec=importlib.util.spec_from_file_location('actual_ceiling',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    limit=module.ManualSteeringCeiling()
    self.assertEqual(limit.update(100.,1.,True),100.)
    for i in range(1,101):
      self.assertAlmostEqual(limit.update(250.,1-i/100.,True),100.-i)
    self.assertEqual(limit.update(250.,.5,True),0.)
    self.assertEqual(limit.update(25.,1.,True),25.)
    self.assertEqual(limit.update(25.,.99,True),24.75)
    self.assertEqual(limit.update(10.,.98,True),10.)
    self.assertEqual(limit.update(250.,.97,True),10.)
    self.assertEqual(limit.update(250.,.96,False),0.)
    self.assertEqual(limit.update(250.,.95,True),0.)

  def test_starting_during_handoff_cannot_raise_torque(self):
    path=ROOT.parents[2]/'opendbc_repo/opendbc/car/hyundai/manual_steering.py'
    spec=importlib.util.spec_from_file_location('actual_ceiling',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    limit=module.ManualSteeringCeiling()
    self.assertEqual(limit.update(250.,.99,True),0.)
    self.assertEqual(limit.update(250.,float('nan'),True),0.)


if __name__ == '__main__':
  unittest.main()
