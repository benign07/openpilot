"""Acceleration-intent preview regressions; no vehicle or CAN access."""
import unittest
from types import SimpleNamespace
from openpilot.selfdrive.controls.lib import approach_comfort as new

def lead(d=50,v=20,vr=0,a=0,track=1):
  return SimpleNamespace(status=True,radar=True,radarTrackId=track,dRel=d,vLead=v,vRel=vr,aLeadK=a)

def evaluate(module,obj,settings=(True,40,40,40,40),**kw):
  p=module.ApproachComfort()
  args=dict(eligible=True,v_ego=25.,a_ego=0.,base_tf=1.5,stop_distance=5.5,maximum_accel=1.2,cruise_speed=30.,planned_accel=.6)
  args.update(kw)
  for _ in range(100):
    request=p.update(module.ApproachSettings(*settings),[obj],**args)
  return request

class NewPreviewCases(unittest.TestCase):
  def test_matched_slow_traffic_detects_future_acceleration_closing(self):
    obj=lead(d=45,v=25,vr=0,a=0)
    after=evaluate(new,obj)
    self.assertIsNotNone(after.accel_ceiling)
    self.assertLess(after.accel_ceiling,1.2)
  def test_confirmed_braking_can_cease_positive_plan_at_strength40(self):
    obj=lead(d=12.45,v=4.42,vr=-.63,a=-1.2)
    after=evaluate(new,obj,v_ego=5.05,a_ego=.905,base_tf=.7)
    self.assertEqual(after.accel_ceiling,0)
  def test_small_extension_is_bounded_and_before_old_window(self):
    obj=lead(d=30.3,v=9,vr=-1,a=-.1)
    after=evaluate(new,obj,v_ego=10,base_tf=1.5,maximum_accel=1.2)
    self.assertIsNotNone(after.accel_ceiling)
  def test_departing_lead_releases_both_weights(self):
    p=new.ApproachComfort();s=new.ApproachSettings(True,40,40,40,40)
    args=dict(eligible=True,v_ego=2.,a_ego=-.3,base_tf=1.5,stop_distance=6.,maximum_accel=1.2)
    for _ in range(100):p.update(s,[lead(d=12,v=1,vr=-1)],**args)
    for _ in range(21):r=p.update(s,[lead(d=12,v=4,vr=2)],**args)
    self.assertIsNone(r.accel_ceiling)
  def test_other_closing_lead_prevents_fast_release(self):
    p=new.ApproachComfort();s=new.ApproachSettings(True,40,40,40,40)
    args=dict(eligible=True,v_ego=2.,a_ego=-.3,base_tf=1.5,stop_distance=6.,maximum_accel=1.2)
    objs=[lead(d=12,v=1,vr=-1),lead(d=13,v=1,vr=-1,track=2)]
    for _ in range(100):p.update(s,objs,**args)
    for _ in range(5):r=p.update(s,[lead(d=12,v=4,vr=2),objs[1]],**args)
    self.assertIsNotNone(r.accel_ceiling)
  def test_still_invalid_negative_velocity_is_not_reinterpreted(self):
    self.assertEqual(evaluate(new,lead(d=10,v=-.05,vr=-1),v_ego=1),new.ApproachRequest())
  def test_steady_following_at_cruise_target_does_not_preview(self):
    r=evaluate(new,lead(d=43,v=25,vr=0,a=0),cruise_speed=25,planned_accel=.6)
    self.assertIsNone(r.accel_ceiling)
  def test_no_positive_planning_intent_does_not_preview(self):
    for a in (0.,-.1):
      r=evaluate(new,lead(d=43,v=25,vr=0,a=0),cruise_speed=30,planned_accel=a)
      self.assertIsNone(r.accel_ceiling)
  def test_capability_cannot_replace_actual_intent(self):
    for maximum in (.3,1.2,2.5):
      r=evaluate(new,lead(d=43,v=25,vr=0,a=0),cruise_speed=30,planned_accel=0.,maximum_accel=maximum)
      self.assertIsNone(r.accel_ceiling)
  def test_preview_is_continuous_at_former_opening_cutoff(self):
    a=evaluate(new,lead(d=40,v=25.499,vr=.499),cruise_speed=30)
    b=evaluate(new,lead(d=40,v=25.501,vr=.501),cruise_speed=30)
    self.assertIsNotNone(a.accel_ceiling)
    self.assertIsNotNone(b.accel_ceiling)
    self.assertLess(abs(a.accel_ceiling-b.accel_ceiling),.01)
  def test_from_rest_has_no_new_preview_or_launch_hold(self):
    kw=dict(v_ego=0.,a_ego=0.,base_tf=1.5,cruise_speed=20.,planned_accel=.6)
    # At this distant stopped lead the pre-existing traffic constraint is off.
    self.assertIsNone(evaluate(new,lead(d=20,v=0,vr=0,a=0),**kw).accel_ceiling)
  def test_optional_intent_missing_or_invalid_does_not_add_preview(self):
    for cruise,accel in ((None,.6),(30.,None),(float('nan'),.6),(30.,float('nan'))):
      self.assertIsNone(evaluate(new,lead(d=43,v=25,vr=0,a=0),cruise_speed=cruise,planned_accel=accel).accel_ceiling)


if __name__ == "__main__":
  unittest.main()
