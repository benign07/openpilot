"""Passive in-lane correction evidence, including sub-threshold human effort."""
from types import SimpleNamespace as NS
import copy
import pytest

from selfdrive.carrot.can_diagnostics.automatic import DriverPositionReview
from selfdrive.carrot.can_diagnostics.automatic_runtime import lateral_geometry_summary, selected_fields, FIELDS


def services(now, *, effort=200., error=1., lat=True, signal=False):
  values={
    'carState':{'vEgo':15.,'steeringTorque':effort,'steeringAngleDeg':3.+error,
      'steeringPressed':effort>250.,'gearShifter':'drive','canValid':True,'canTimeout':False,
      'leftBlinker':signal,'rightBlinker':False,'leftBlindspot':False,'rightBlindspot':True},
    'carControl':{'latActive':lat,'actuators':{'steeringAngleDeg':3.},'manualSteeringScale':1.},
    'carOutput':{'actuatorsOutput':{'steeringAngleDeg':3.,'torqueOutputCan':80.}},
    'modelV2':{'frameId':123,'laneLineProbs':[0.,.9,.95,0.],
      'lateral_geometry':{'lane_y':[[-1.8,-1.7,-1.6],[1.8,1.9,2.]],'path_y':[0.,.1,.2]}},
    'controlsState':{'curvature':.002,'desiredCurvature':.0021},
    'radarState':{'leadsLeft':[],'leadsRight':[{'radarTrackId':7,'dRel':12.,'yRel':-3.}], 'leadsCutIn':[]}}
  return {k:{'valid':True,'alive':True,'mono_ns':int(now*1e9),'data':v} for k,v in values.items()}


def test_sub_pressed_effort_and_angle_error_records_candidate_with_pre_and_after_context():
  review=DriverPositionReview()
  assert review.observe(services(100.),100.)==[]
  start=review.observe(services(100.21),100.21)
  assert start[0]['phase']=='start'
  assert 'not_confirmed' in start[0]['classification']
  assert start[0]['pre_context'][-1]['steering_pressed'] is False
  assert start[0]['pre_context'][-1]['output']['torqueOutputCan']==80.
  assert start[0]['pre_context'][-1]['nearby']['leadsRight'][0]['radarTrackId']==7
  review.observe(services(100.5,effort=0.,error=0.),100.5)
  end=review.observe(services(100.71,effort=0.,error=0.),100.71)
  assert end[0]['phase']=='end'
  after=review.observe(services(102.72,effort=0.,error=0.),102.72)
  assert after[0]['phase']=='after_2s' and after[0]['episode']==start[0]['episode']


@pytest.mark.parametrize('changes',[{'effort':100.},{'error':0.},{'lat':False},{'signal':True}])
def test_resting_hands_signal_and_inactive_control_are_not_labeled_corrections(changes):
  review=DriverPositionReview()
  rows=[]
  for i in range(15):
    now=100.+i*.21;rows+=review.observe(services(now,**changes),now)
  assert not rows and review.count==0 and len(review.pre)<=12


def test_bad_model_quality_is_retained_as_unknown_rather_than_losing_the_human_correction():
  review=DriverPositionReview()
  for now in (100.,100.21):
    rows=services(now);rows['modelV2']['valid']=False
    result=review.observe(rows,now)
  assert result[0]['phase']=='start'
  assert result[0]['pre_context'][-1]['geometry'] is None


def test_cached_samples_do_not_confirm_and_snapshots_do_not_follow_later_mutation():
  review=DriverPositionReview();row=services(100.)
  before=copy.deepcopy(row)
  assert review.observe(row,100.)==[]
  assert row==before
  assert review.observe(row,100.21)==[]
  result=review.observe(services(100.22),100.22)
  row['modelV2']['data']['lateral_geometry']['path_y'][0]=999.
  assert result[0]['pre_context'][0]['geometry']['path_y'][0]==0.


def test_geometry_is_small_interpolated_and_never_extrapolated():
  line=lambda y:NS(x=[0.,10.,20.],y=y)
  model=NS(laneLines=[line([0.,0.,0.]),line([-2.,-1.,0.]),line([2.,3.,4.])],
           roadEdges=[],position=NS(x=[5.,15.,25.],y=[0.,1.,2.]))
  row=lateral_geometry_summary(model)
  assert row['lane_y']==[[-2.,-1.,0.],[2.,3.,4.]]
  assert row['path_y']==[None,.5,1.5]
  model.laneLines[1].x=[0.,0.,20.]
  assert lateral_geometry_summary(model)['lane_y'][0] is None


def test_nearby_context_is_bounded_without_claiming_complete_free_space():
  lead=lambda i:NS(status=True,radarTrackId=i,dRel=10.+i,yRel=-3.)
  row=selected_fields(NS(leadsLeft=[],leadsRight=[lead(i) for i in range(100)],leadsCutIn=[]),FIELDS['radarState'])
  assert len(row['leadsRight'])==3
  assert [v['radarTrackId'] for v in row['leadsRight']]==[0,1,2]


def test_confirmation_retains_the_initial_peak_and_backwards_samples_do_not_advance():
  review=DriverPositionReview();review.observe(services(100.,effort=300.),100.)
  row=services(99.95,effort=300.)
  assert review.observe(row,100.15)==[]
  review.observe(services(100.21,effort=200.),100.21)
  assert review.active['peak']==300.
