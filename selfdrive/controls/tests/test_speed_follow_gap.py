"""Speed comfort target ratios and actual MPC cost/constraint composition."""
from types import SimpleNamespace as NS
import numpy as np
import pytest
from openpilot.selfdrive.controls.lib.approach_comfort import speed_gap_reduction
from openpilot.selfdrive.controls.lib.flexible_gap import FlexibleGapSettings
from openpilot.selfdrive.controls.tests.test_flexible_gap_mpc import production_mpc, update


@pytest.mark.parametrize('kph,ratio', [(0,1.),(25,.975),(50,.95),(90,.91),(100,.90),(120,.90),(160,.90)])
def test_speed_ratio_is_linear_and_caps_at_100_kph(kph,ratio):
  assert 30.-speed_gap_reduction(kph/3.6,30.,5.5) == pytest.approx(30.*ratio)


@pytest.mark.parametrize('values', [(float('nan'),30.,5.5),(25.,float('inf'),5.5),
  (25.,30.,float('nan')),(-1.,30.,5.5),(25.,30.,-1.),(25.,5.5,5.5),(25.,-4.,5.5)])
def test_invalid_or_no_moving_clearance_has_no_reduction(values):
  assert speed_gap_reduction(*values) == 0.


def test_absolute_stopping_distance_is_a_floor():
  assert speed_gap_reduction(100/3.6,5.6,5.5) == pytest.approx(.1)


def equal_speed_mpc(kph=100.,tf=.7):
  m,c,r,ns=production_mpc();v=kph/3.6
  m.x0=np.array([0.,v,0.]);m.x_sol=np.tile([0.,v,0.],(13,1))
  c.get_T_FOLLOW=lambda *a:tf;c.flex_gap_nominal_tf=tf;c.comfort_brake=2.4
  c.v_cruise=45.
  # Deliberately make lead0 the limiting objective. A distant lead leaves
  # cruise as the limiting objective, where a gap preference must do nothing.
  r.leadOne=NS(status=True,radar=True,radarTrackId=1,dRel=8.,vLead=v,vRel=0.,aLeadK=0.,jLead=0.,modelProb=.99)
  m.process_lead=lambda obj,j:(np.column_stack([obj.dRel+v*ns['T_IDXS'],np.full(13,v)]),v)
  return m,c,r,ns


@pytest.mark.parametrize('kph', [0.,50.,90.,100.,120.])
def test_actual_mpc_reference_ratio_and_original_physical_parameters(kph):
  a,c,r,_=equal_speed_mpc(kph);update(a,c,r)
  b,bc,br,_=equal_speed_mpc(kph);update(b,bc,br,speed_gap_eligible=True)
  ratio=1.-.1*min(1.,kph/100.)
  assert b.desired_distance == pytest.approx(a.desired_distance*ratio)
  np.testing.assert_array_equal(a.params,b.params)
  assert b.t_follow == a.t_follow
  assert b.max_a == a.max_a and b.crash_cnt == a.crash_cnt


def test_stock_deceleration_and_early_braking_headroom_are_not_scaled_away():
  a,c,r,_=equal_speed_mpc(tf=.9);c.flex_gap_nominal_tf=.7
  update(a,c,r,approach_extra_tf=.3)
  b,bc,br,ns=equal_speed_mpc(tf=.9);bc.flex_gap_nominal_tf=.7
  update(b,bc,br,approach_extra_tf=.3,speed_gap_eligible=True)
  nominal=ns['desired_follow_distance'](100/3.6,100/3.6,2.4,5.5,.7)
  assert b.desired_distance == pytest.approx(a.desired_distance-.1*nominal)
  np.testing.assert_array_equal(a.params,b.params)


def test_flexible_gap_and_speed_scale_use_greater_reduction_not_sum_or_product():
  a,c,r,_=production_mpc();a.max_a=0.
  b,bc,br,_=production_mpc();b.max_a=0.
  for _ in range(300):
    kw=dict(flex_gap_settings=FlexibleGapSettings(True,70),flex_gap_eligible=True)
    update(a,c,r,**kw)
    update(b,bc,br,speed_gap_eligible=True,**kw)
  assert b.flex_gap.reduction > 0.
  np.testing.assert_array_equal(a.yref,b.yref)
  np.testing.assert_array_equal(a.params,b.params)
  assert b.desired_distance == a.desired_distance


@pytest.mark.parametrize('change', [dict(status=False),dict(radar=False),dict(radarTrackId=-1)])
def test_invalid_primary_never_gets_new_relaxation(change):
  a,c,r,_=equal_speed_mpc()
  for key,value in change.items():setattr(r.leadOne,key,value)
  update(a,c,r,speed_gap_eligible=True)
  assert a.speed_gap_reduction_m == 0.
  np.testing.assert_array_equal(a.yref[:,0],0.)


def test_a_limiting_secondary_lead_keeps_its_original_reference():
  a,c,r,_=equal_speed_mpc();r.leadTwo=NS(**{**vars(r.leadOne),'radarTrackId':2,'dRel':4.})
  update(a,c,r,speed_gap_eligible=True)
  assert a.speed_gap_reduction_m > 0.
  np.testing.assert_array_equal(a.yref[:,0],0.)


def test_disabled_call_preserves_legacy_reference_exactly():
  a,c,r,_=equal_speed_mpc();update(a,c,r)
  b,bc,br,_=equal_speed_mpc();update(b,bc,br,speed_gap_eligible=False)
  np.testing.assert_array_equal(a.yref,b.yref)
  np.testing.assert_array_equal(a.params,b.params)


def test_reset_and_blended_modes_do_not_use_speed_relaxation():
  for reset,mode in ((True,'acc'),(False,'blended')):
    a,c,r,_=equal_speed_mpc();a.mode=mode
    a.update(c,reset,r,35.,*[np.zeros(13) for _ in range(4)],speed_gap_eligible=True)
    assert a.speed_gap_reduction_m == 0.
    np.testing.assert_array_equal(a.yref[:,0],0.)
