"""Installed-overlay behavior; no simulated grants are sent to hardware."""
import ast
from pathlib import Path
from types import SimpleNamespace as NS
import pytest
from openpilot.selfdrive.carrot.manual_steering import ManualSteeringPause
from opendbc.car.hyundai.lx3_haptic_notifications import HapticNotifications
from opendbc.car.hyundai.manual_steering import ManualSteeringCeiling


def controls():
  root=Path(__file__).resolve().parents[3]
  tree=ast.parse((root/'selfdrive/carrot/carrot_controls.py').read_text(encoding='utf-8'))
  tree.body=[n for n in tree.body if isinstance(n,ast.ClassDef)]
  params=NS(get_bool=lambda k:True,get_int=lambda k:-1 if k=='LaneChangeNeedTorque' else 300)
  ns=dict(Params=lambda:params,DT_CTRL=.01,ManualSteeringPause=ManualSteeringPause,uses_lx3_authority=lambda cp:True)
  exec(compile(tree,'carrot_controls.py','exec'),ns)
  return ns['CarrotControls'](NS())


def cs():
  return NS(leftBlinker=True,rightBlinker=False,steeringPressed=True,steeringAngleDeg=5.,canValid=True,canTimeout=False,
            lx3Authority=NS(epoch=123,lateralGeneration=4,longitudinalGeneration=0,allowed=1,armed=1,
                            lateralDecisionKey=80,longitudinalDecisionKey=0))


def test_new_accepted_lat_request_after_signal_ends_pause_despite_hands():
  c=controls();s=cs()
  for _ in range(101):c.lat_suspend_control(s,True)
  assert c.manual_steering.scale==0
  s.leftBlinker=False
  s.lx3Authority.lateralGeneration=5
  assert c.lat_suspend_control(s,True)
  assert 0 < c.manual_steering.scale < .021
  for _ in range(200):assert c.lat_suspend_control(s,True)
  s.leftBlinker=False;c.lat_suspend_control(s,True)
  s.rightBlinker=True;c.lat_suspend_control(s,True)
  assert c.manual_steering.paused


def test_scc_accepted_long_grant_can_resume_only_existing_lat_permission():
  c=controls();s=cs()
  for _ in range(101):c.lat_suspend_control(s,True)
  s.leftBlinker=False
  a=s.lx3Authority;a.longitudinalGeneration=8;a.longitudinalDecisionKey=77;a.allowed=a.armed=3
  assert c.lat_suspend_control(s,True)


def test_speed_engage_during_signal_cannot_override_manual_lane_change():
  c=controls();s=cs()
  for _ in range(101):c.lat_suspend_control(s,True)
  a=s.lx3Authority;a.longitudinalGeneration=8;a.longitudinalDecisionKey=77;a.allowed=a.armed=3
  assert not c.lat_suspend_control(s,True)


def test_explicit_resume_actual_ceiling_starts_immediately_and_ramps():
  c=controls();s=cs();ceiling=ManualSteeringCeiling()
  ceiling.update(250.,1.,True)
  for _ in range(101):
    active=c.lat_suspend_control(s,True)
    value=ceiling.update(250.,c.manual_steering.scale,active,resuming=c.manual_steering.resuming)
  assert value==0
  s.leftBlinker=False
  s.lx3Authority.lateralGeneration=5
  active=c.lat_suspend_control(s,True)
  value=ceiling.update(250.,c.manual_steering.scale,active,resuming=c.manual_steering.resuming)
  assert active and 0 < value <= 5.01
  for _ in range(49):
    active=c.lat_suspend_control(s,True)
    next_value=ceiling.update(250.,c.manual_steering.scale,active,resuming=c.manual_steering.resuming)
    assert value <= next_value <= min(250.,value+5.01)
    value=next_value
  assert value==250.
  assert ceiling.update(250.,1.,False)==0


def test_new_lat_generation_during_signal_still_preserves_manual_handoff():
  c=controls();s=cs()
  for _ in range(101):c.lat_suspend_control(s,True)
  s.lx3Authority.lateralGeneration=5
  assert not c.lat_suspend_control(s,True)
  assert c.manual_steering.scale==0 and c.manual_steering.paused


def test_scale_increase_without_explicit_resume_cannot_raise_ceiling():
  ceiling=ManualSteeringCeiling()
  assert ceiling.update(100.,1.,True)==100.
  for i in range(1,101):
    assert ceiling.update(250.,1-i/100,True)==pytest.approx(100-i)
  assert ceiling.update(250.,.5,True)==0
  assert ceiling.update(25.,1.,True)==25
  assert ceiling.update(25.,.99,True)==24.75
  assert ceiling.update(10.,.98,True)==10
  assert ceiling.update(250.,.97,True)==10
  assert ceiling.update(250.,.96,False)==0
  assert ceiling.update(250.,.95,True)==0
  assert ceiling.update(250.,.95,False,resuming=True)==0


def test_partial_fade_resume_never_steps_up_even_when_host_ramp_finishes_early():
  ceiling=ManualSteeringCeiling();ceiling.update(100.,1.,True)
  for i in range(1,41):value=ceiling.update(250.,1-i/100,True)
  assert value==pytest.approx(60.)
  for i in range(41,141):
    scale=min(1.,i/100+.2)
    next_value=ceiling.update(250.,scale,True,resuming=scale<1.)
    assert value<=next_value<=min(250.,value+5.)
    value=next_value
  assert value==250.
  assert ceiling.update(250.,.99,True)==pytest.approx(247.5)
  assert ceiling.update(250.,.98,False,resuming=True)==0


def test_new_manual_handoff_during_recovery_anchors_to_actual_partial_output():
  ceiling=ManualSteeringCeiling();ceiling.update(100.,1.,True)
  for i in range(1,41):ceiling.update(250.,1-i/100,True)
  for i in range(41,61):value=ceiling.update(250.,min(1.,i/100+.2),True,resuming=True)
  assert value==pytest.approx(160.) and ceiling.recovering
  assert ceiling.update(250.,.79,True)==pytest.approx(126.4)
  assert ceiling.fading and not ceiling.recovering


def test_resume_intent_is_serialized_and_forwarded_without_granting_authority():
  from cereal import car
  cc=car.CarControl.new_message()
  assert not cc.manualSteeringResume and cc.manualSteeringScale==1
  cc.manualSteeringResume=True;cc.manualSteeringScale=.01
  with car.CarControl.from_bytes(cc.to_bytes()) as received:
    assert received.manualSteeringResume and received.manualSteeringScale==pytest.approx(.01)
    assert not received.latActive and not received.longActive
  root=Path(__file__).resolve().parents[3]
  producer=(root/'selfdrive/controls/controlsd.py').read_text(encoding='utf-8')
  consumer=(root/'opendbc_repo/opendbc/car/hyundai/carcontroller.py').read_text(encoding='utf-8')
  assert 'CC.manualSteeringResume = self.carrot_controls.manual_steering.resuming' in producer
  assert "lat_active, resuming=getattr(CC, 'manualSteeringResume', False))" in consumer


def test_legacy_schema_disables_new_resume_without_stopping_existing_control():
  c=controls();s=cs()
  for _ in range(101):c.lat_suspend_control(s,True,explicit_resume_supported=False)
  s.leftBlinker=False;s.lx3Authority.lateralGeneration=5
  assert not c.lat_suspend_control(s,True,explicit_resume_supported=False)
  assert c.manual_steering.scale==0


@pytest.mark.parametrize('kind',['same','epoch','pending','inactive','invalid','timeout','no_key'])
def test_permission_recovery_or_bad_input_cannot_override_pause(kind):
  c=controls();s=cs()
  for _ in range(101):c.lat_suspend_control(s,True)
  a=s.lx3Authority
  if kind!='same':a.lateralGeneration=5
  if kind=='epoch':a.epoch=456
  if kind=='pending':a.allowed=a.armed=0
  if kind=='invalid':s.canValid=False
  if kind=='timeout':s.canTimeout=True
  if kind=='no_key':a.lateralDecisionKey=0
  assert not c.lat_suspend_control(s,kind!='inactive')
  assert c.manual_steering.paused and c.manual_steering.scale==0


def test_normal_automatic_resume_still_waits_clear_and_cannot_grant():
  c=controls();s=cs()
  for _ in range(101):c.lat_suspend_control(s,True)
  s.leftBlinker=False;s.steeringPressed=False
  for _ in range(49):assert not c.lat_suspend_control(s,True)
  assert c.lat_suspend_control(s,True)
  assert c.manual_steering.scale==0 and c.manual_steering.resuming
  assert not c.lat_suspend_control(s,False)


def test_haptic_pulse_bounded_retrigger_rate_and_critical_preemption():
  h=HapticNotifications()
  assert h.update('engage',eligible=True,frame=100)
  assert h.update('engage',eligible=True,frame=119)
  assert not h.update('engage',eligible=True,frame=120)
  assert not h.update('engage2',eligible=True,frame=121)
  assert h.update('engage',eligible=True,frame=230)
  assert not h.update('warningImmediate',eligible=True,frame=231)
  assert not h.update('engage2',eligible=False,frame=400)


@pytest.mark.parametrize('alert',['warningImmediate','refuse','disengage','audio1','promptRepeat','none'])
def test_unclassified_or_critical_alert_never_replaces_acoustic_warning(alert):
  assert not HapticNotifications().update(alert,eligible=True,frame=0)


def test_ccnc_bit_preserves_oem_warning_bits_and_other_profiles():
  # Execute the complete production CCNC function. Only unavailable external
  # frame packing/model drawing are supplied locally; no on-car transmission.
  root=Path(__file__).resolve().parents[3]
  tree=ast.parse((root/'opendbc_repo/opendbc/car/hyundai/hyundaicanfd.py').read_text(encoding='utf-8'))
  tree.body=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='create_ccnc_messages']
  import copy
  from enum import IntFlag
  class Flags(IntFlag):CAMERA_SCC=1
  ns=dict(copy=copy,Params=lambda:NS(get_int=lambda k:0),HyundaiFlags=Flags,
          _get_desire_and_lane_changing=lambda md:(0,0),_make_ccnc_values=lambda *a,**kw:None)
  exec(compile(tree,'hyundaicanfd.py','exec'),ns)
  s=NS(modelV2=None,lfahda_cluster=None,cruise_buttons_msg=None,adrv_0x161=None,adrv_0x200=None,adrv_0x1ea=None,
       ccnc_0x162=dict(VIBRATE=0,FAULT_LSS=1,FAULT_DAS=1),out=NS(leftBlinker=False,rightBlinker=False))
  packer=NS(make_can_msg=lambda name,bus,values,**kw:(name,bus,values.copy()))
  # lx3_buttons is a Windows import stub only for this production policy gate.
  import opendbc.car.hyundai.lx3_buttons as buttons
  old=buttons.uses_lx3_authority
  try:
    for supported in (False,True):
      buttons.uses_lx3_authority=lambda cp:supported
      result=ns['create_ccnc_messages'](NS(flags=1,carFingerprint='HYUNDAI_PALISADE_LX3_HEV' if supported else 'KIA_EV9'),packer,NS(ECAN=0),5,NS(latActive=True),s,
              NS(leadDistance=0),0,0,0,0,False,0,notification_vibrate=True)
      values=result[0][2]
      assert values['VIBRATE']==int(supported)
      assert values['FAULT_LSS']==values['FAULT_DAS']==1
      assert s.ccnc_0x162['VIBRATE']==0
  finally:buttons.uses_lx3_authority=old
