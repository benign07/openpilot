"""Execute the production driver/manual ceiling block; no CAN or native grants."""
import ast
from pathlib import Path
from types import SimpleNamespace as NS
import numpy as np
import pytest

from opendbc.car.hyundai.manual_steering import ManualSteeringCeiling
from openpilot.selfdrive.carrot.manual_steering import ManualSteeringPause

ROOT=Path(__file__).resolve().parents[3]
SOURCE=ROOT/'opendbc_repo/opendbc/car/hyundai/carcontroller.py'
BEFORE=Path(__file__).with_name('fixtures')/'lx3_driver_9b9b3d5c.txt'


def program(before=False):
  if before:
    return compile(BEFORE.read_text(encoding='utf8'),str(BEFORE),'exec')
  tree=ast.parse(SOURCE.read_text(encoding='utf8'))
  cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='CarController')
  update=next(n for n in cls.body if isinstance(n,ast.FunctionDef) and n.name=='update')
  first=next(i for i,n in enumerate(update.body) if isinstance(n,ast.Assign) and any(
    isinstance(t,ast.Name) and t.id=='lx3_driver_recovery' for t in n.targets))
  end=next(i for i,n in enumerate(update.body) if isinstance(n,ast.Assign) and any(
    isinstance(t,ast.Attribute) and t.attr=='steering_pressed_prev' for t in n.targets))
  return compile(ast.Module(body=update.body[first:end+1],type_ignores=[]),str(SOURCE),'exec')


class Driver:
  def __init__(self, *, before=False, lx3=True):
    self.program=program(before)
    self.lx3=lx3
    self.s=NS(angle_max_torque=250.,lkas_max_torque=0.,steering_pressed_prev=False,
      full_recovery_frames=0,recovering_from_override=False,repeated_override_count=0,
      override_latched=False,override_release_frames=0,driver_torque_filtered=0.,driver_torque_filtered_prev=0.,
      params=NS(STEER_THRESHOLD=250.,ANGLE_MIN_TORQUE=25.),lx3_angle_active_last=False,lx3_engagement_s=0.,
      lx3_recovery_anchor=0.,lx3_recovery_elapsed=0.,lx3_recovery_active=False,lx3_pre_override_frames=0,
      lx3_pre_override_counter=None,lx3_yield_hold=False,lx3_recovery_from_yield=False,
      manual_steering_ceiling=ManualSteeringCeiling(resume_seconds=.2 if lx3 else .5,recovery_maximum=250. if lx3 else None))
    self.counter=0

  def step(self, torque, *, pressed=False, active=True, scale=1., resuming=False, counter=None):
    self.counter=(self.counter+1)%256
    cs=NS(out=NS(steeringTorque=torque,steeringPressed=pressed),modelV2=None,
          mdps={'COUNTER':self.counter if counter is None else counter})
    scope=dict(self=self.s,CS=cs,CC=NS(manualSteeringScale=scale,manualSteeringResume=resuming),
      lx3_guard=self.lx3,angle_control=True,lat_active=active,apply_torque=0.,np=np,DT_CTRL=.01,
      DRIVER_TORQUE_FILTER_TAU=.12,PRE_OVERRIDE_PREDICTION_TIME=.15,PRE_OVERRIDE_START_RATIO=.9,
      PRE_OVERRIDE_MAX_TORQUE_DELTA=-10.,LX3_DRIVER_RELEASE_RATIO=.8)
    exec(self.program,scope)
    self.s.lx3_angle_active_last=scope['lat_active']
    return scope['steering_authority']


@pytest.mark.parametrize('torque',[100.,150.,180.,200.,220.])
def test_initial_light_hand_reaches_full_without_old_permanent_taper(torque):
  d=Driver()
  values=[d.step(torque) for _ in range(100)]
  assert values[-1]==250.
  assert all(a<=b for a,b in zip(values,values[1:]))


def test_exact_old_block_demonstrates_cold_and_post_override_failure():
  d=Driver(before=True)
  for _ in range(1000):d.step(200.)
  assert d.s.lkas_max_torque==pytest.approx(84.9365576408551)
  for _ in range(50):d.step(300.,pressed=True)
  for _ in range(1000):d.step(200.)
  assert d.s.lkas_max_torque==25.


def test_real_push_then_resting_hand_recovers_in_twenty_control_ticks():
  d=Driver()
  for _ in range(100):d.step(100.)
  for _ in range(50):assert d.step(300.,pressed=True)>=25.
  assert d.s.lkas_max_torque==25.
  values=[d.step(200.) for _ in range(20)]
  assert values[-1]==250.
  assert all(0<=b-a<=11.250001 for a,b in zip([25.]+values,values))


def test_holding_force_band_does_not_repeatedly_yield_and_recover():
  d=Driver()
  for _ in range(100):d.step(100.)
  values=[]
  for _ in range(1000):
    torque=d.s.lkas_max_torque  # A stationary hand resisting MDPS assistance.
    values.append(d.step(torque,pressed=torque>250.))
  assert max(values[-500:])-min(values[-500:])<1e-6
  for _ in range(20):value=d.step(100.)
  assert value==250.


def test_hand_noise_below_pre_yield_boundary_does_not_reduce_full_assistance():
  rng=np.random.default_rng(2742);d=Driver()
  for _ in range(100):d.step(200.)
  assert all(d.step(float(rng.uniform(180,220)))==250. for _ in range(1000))


def test_noisy_holding_hand_does_not_cycle_between_full_assistance_and_minimum_ceiling():
  d=Driver();rng=np.random.default_rng(510)
  for _ in range(100):d.step(100.)
  values=[]
  for _ in range(3000):
    torque=d.s.lkas_max_torque+float(rng.uniform(-20.,20.))
    values.append(d.step(torque,pressed=torque>250.))
  # A bounded synthetic feedback probe, not a vehicle steering stability proof.
  assert min(values[-2000:])>200.
  assert max(values[-2000:])-min(values[-2000:])<25.


def test_pre_yield_counts_distinct_mdps_updates_and_remains_before_confirmed_press():
  d=Driver()
  for _ in range(100):d.step(100.)
  for _ in range(5):assert d.step(300.,counter=55)==250.
  values=[d.step(300.,counter=c) for c in (56,57,58)]
  assert values[-1]<250.
  assert d.step(300.,pressed=True)<values[-1]


def test_permission_loss_stops_even_during_a_fast_handback():
  d=Driver()
  for _ in range(30):d.step(300.,pressed=True)
  d.step(100.)
  assert d.step(100.,active=False)==0.
  assert d.s.lkas_max_torque==0.


def test_mid_band_retains_controller_soft_ceiling_without_forcing_host_permission_off():
  d=Driver()
  for _ in range(100):d.step(100.)
  for _ in range(50):d.step(300.,pressed=True)
  for _ in range(100):assert d.step(201.)==25.
  for _ in range(20):value=d.step(200.)
  assert value==250.


def test_permission_loss_ends_handback_and_new_activation_uses_normal_ramp():
  pause=ManualSteeringPause(.01,quick_recovery=True);d=Driver()
  args=dict(enabled=True,lane_change_off=True,left_blinker=True,right_blinker=False,
    steering_pressed=False,steering_angle=0.,can_valid=True,can_timeout=False,lateral_active=True)
  for _ in range(110):pause.update(**args)
  args['left_blinker']=False;pause.update(**args)
  assert pause.resuming
  args['lateral_active']=False;pause.update(**args)
  assert not pause.resuming
  d.step(100.,active=False)
  for _ in range(20):value=d.step(100.,resuming=pause.resuming)
  assert 0.<value<250.


def test_other_angle_vehicle_matches_pinned_original_block_trace():
  before,after=Driver(before=True,lx3=False),Driver(lx3=False)
  for torque,pressed,active in [(v,p,a) for v,p in ((0.,False),(200.,False),(300.,True),(-120.,False),(-260.,True))
                               for a in (True,False,True) for _ in range(35)]:
    assert after.step(torque,pressed=pressed,active=active)==before.step(torque,pressed=pressed,active=active)


def test_blinker_host_and_real_controller_ceiling_complete_point_two_second_handback():
  pause=ManualSteeringPause(.01,quick_recovery=True);d=Driver()
  def step(signal, pressed=False):
    paused=pause.update(enabled=True,lane_change_off=True,left_blinker=signal,right_blinker=False,
      steering_pressed=pressed,steering_angle=0.,can_valid=True,can_timeout=False,lateral_active=True)
    return d.step(300. if pressed else 100.,pressed=pressed,active=not(paused and pause.scale<=0.),
                  scale=pause.scale,resuming=pause.resuming)
  for _ in range(100):step(False)
  for _ in range(110):step(True)
  assert d.s.manual_steering_ceiling.previous==0.
  for _ in range(20):assert step(False,True)==0.
  values=[step(False) for _ in range(20)]
  assert values[-1]==250.
  assert all(0<=b-a<=12.500001 for a,b in zip([0.]+values,values))


@pytest.mark.parametrize('speed,angle,rate,allowed',[(17.,25.,0.,True),(17.,25.,40.,False),(3.,90.,0.,False)])
def test_curved_road_handback_distinguishes_a_steady_curve_from_an_unfinished_turn(speed,angle,rate,allowed):
  pause=ManualSteeringPause(.01,quick_recovery=True)
  def step(signal):
    return pause.update(enabled=True,lane_change_off=True,left_blinker=signal,right_blinker=False,
      steering_pressed=False,steering_angle=angle,can_valid=True,can_timeout=False,lateral_active=True,
      speed=speed,steering_rate=rate)
  for _ in range(110):step(True)
  for _ in range(20):step(False)
  assert (pause.scale==1.) is allowed
