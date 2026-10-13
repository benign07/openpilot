"""PC temporal gate tests. No native solver or vehicle qualification."""
from dataclasses import replace
from types import SimpleNamespace as NS
import math
import pytest
from openpilot.selfdrive.controls.lib.flexible_gap import FlexibleGap, FlexibleGapSettings


def lead(**kw):
  return NS(**(dict(status=True,radar=True,radarTrackId=1,dRel=45.,vLead=19.7,vRel=-.3,aLeadK=0.,jLead=0.,modelProb=.99)|kw))


def step(obj, count=1, leads=None, settings=None, **kw):
  args=dict(eligible=True,v_ego=20.,a_ego=0.,base_tf=2.,stop_distance=5.5,comfort_brake=2.5)|kw
  for _ in range(count):
    result=obj.update(settings or FlexibleGapSettings(True,70),leads or (lead(),),**args)
  return result


def test_three_seconds_and_ten_second_slow_acquisition():
  f=FlexibleGap()
  assert step(f,60)==0
  first=step(f)
  assert 0 < first <= .0031
  assert math.isclose(step(f,210),.6)


@pytest.mark.parametrize('tf',[.7,.8,1.,1.15,1.25])
def test_existing_short_gaps_do_not_shrink(tf):
  assert step(FlexibleGap(),500,base_tf=tf)==0


def test_minimum_timegap_and_100_percent_neutral():
  assert math.isclose(step(FlexibleGap(),500,base_tf=1.5,leads=(lead(dRel=35.),)),.25)
  assert step(FlexibleGap(),500,settings=FlexibleGapSettings(True,100))==0
  assert step(FlexibleGap(),500,settings=FlexibleGapSettings())==0


@pytest.mark.parametrize('change',[
 dict(vRel=-2.,vLead=18.),dict(aLeadK=-1.5),dict(modelProb=.1),dict(dRel=30.),
 dict(radar=False),dict(radarTrackId=9),dict(vRel=float('nan')),dict(jLead=-.8),
 dict(dRel=42.),dict(status=False),dict(vLead=4.),
])
def test_each_risk_or_identity_change_restores_reference_in_one_update(change):
  f=FlexibleGap();assert step(f,300)>0
  assert step(f,leads=(lead(**change),))==0


def test_secondary_risk_cancels_and_primary_missing_never_relaxes():
  f=FlexibleGap()
  assert step(f,300,leads=(lead(),lead(radarTrackId=2,dRel=90.)))>0
  assert step(f,leads=(lead(),lead(radarTrackId=2,radar=False)))==0
  assert step(f,500,leads=(lead(status=False),lead(radarTrackId=2)))==0


@pytest.mark.parametrize('changes',[
 dict(eligible=False),dict(v_ego=0.),dict(a_ego=-2.),dict(base_tf=float('nan')),
 dict(stop_distance=-1.),dict(comfort_brake=0.),
])
def test_ineligibility_resets_without_latched_reduction(changes):
  f=FlexibleGap();assert step(f,300)>0
  assert step(f,**changes)==0
  assert step(f,60)==0


def test_constant_speed_equal_lead_cannot_begin_shrinking():
  assert step(FlexibleGap(),500,leads=(lead(vLead=20.,vRel=0.),))==0


def test_secondary_slot_swap_needs_confirmation():
  f=FlexibleGap();pair=(lead(),lead(radarTrackId=2,dRel=90.))
  assert step(f,300,leads=pair)>0
  assert step(f,leads=pair[::-1])==0


def test_far_slow_lead_does_not_start_relaxation_or_accel_cap():
  assert step(FlexibleGap(),500,leads=(lead(dRel=200.),))==0


def test_settings_clamp_from_params():
  p=NS(get_bool=lambda k:True,get_int=lambda k:10)
  assert FlexibleGapSettings.read(p)==FlexibleGapSettings(True,70)
