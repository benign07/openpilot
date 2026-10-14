"""Diagnostic failures and comparisons cannot change the control calculation."""
import ast
import copy
import gzip
import json
from pathlib import Path
from types import SimpleNamespace as NS

import numpy as np
import pytest

from openpilot.selfdrive.controls.lib.approach_comfort import (
  ApproachComfort, ApproachSettings, LongitudinalEvidence, traffic_ceiling_comparison)
from openpilot.selfdrive.controls.tests.test_flexible_gap_mpc import production_mpc, update
from tools.lx3_drive_quality import frame_integrity, match_video, render, summarize_frames


def frame(seq=1, **changes):
  t=seq*50_000_000
  return {'schema':1,'producer':'a'*32,'sequence':seq,'cycle':seq,'mono_ns':t,'settings_generation':1,
          'comparison':{'30':{'applied_ceiling':1.105},'40':{'applied_ceiling':1.04}},
          'services':{k:{'mono_ns':t,'valid':True,'alive':True} for k in (
            'carState','carControl','controlsState','radarState','modelV2','selfdriveState')},**changes}


@pytest.mark.parametrize('traffic',[0,30,40,100])
def test_comparison_reads_filters_without_advancing_or_changing_settings(traffic):
  p=ApproachComfort();p.coast_weight=.02;p.traffic_weight=1.
  before=copy.deepcopy(vars(p));s=ApproachSettings(True,40,40,40,traffic)
  pair=traffic_ceiling_comparison(p,s,1.3,0.)
  assert pair['30']['applied_ceiling']==pytest.approx(1.105)
  assert pair['40']['applied_ceiling']==pytest.approx(1.04)
  assert vars(p)==before and s.traffic==traffic


def test_stock_slew_or_coast_can_remove_30_40_difference():
  p=ApproachComfort();p.traffic_weight=1.;p.coast_weight=1.
  pair=traffic_ceiling_comparison(p,ApproachSettings(True,40,40,40,30),1.3,0.)
  assert pair['30']['applied_ceiling']==pair['40']['applied_ceiling']
  p.coast_weight=0.
  pair=traffic_ceiling_comparison(p,ApproachSettings(True),1.3,1.3)
  assert pair['30']['applied_ceiling']==pair['40']['applied_ceiling']==pytest.approx(1.28)


def test_disabled_logger_never_builds_and_failures_are_evidence_only():
  logger=LongitudinalEvidence(False)
  assert not any(logger.due(True) for _ in range(20))
  logger.emit(lambda: (_ for _ in ()).throw(AssertionError()),lambda _:None)
  assert logger.errors==0
  logger.enabled=True
  logger.emit(lambda: {'settings':{},'bad':float('nan')},lambda _:None)
  logger.emit(lambda: {'settings':{},'huge':'x'*32768},lambda _:None)
  logger.emit(lambda: {'settings':{}},lambda _: (_ for _ in ()).throw(OSError()))
  assert logger.errors==3
  rows=[];logger.emit(lambda: {'settings':{}},rows.append)
  assert rows[0][logger.KEY]['producer_errors']==3


def test_every_low_speed_cycle_and_settings_generation():
  logger=LongitudinalEvidence(True);rows=[]
  for _ in range(8):
    if logger.due(False):logger.emit(lambda:{'settings':{'traffic':30}},rows.append)
  assert len(rows)==2
  for _ in range(3):
    assert logger.due(True)
    logger.emit(lambda:{'settings':{'traffic':40}},rows.append)
  assert rows[-1][logger.KEY]['settings_generation']==2


def test_mpc_capture_has_identical_constraints_and_references():
  baseline,c,r,_=production_mpc();update(baseline,c,r)
  captured,c,r,ns=production_mpc();captured.evidence_requested=True
  ns['time']=__import__('time')
  captured.evidence_cost_weights=[1.]*6;captured.evidence_constraint_weights=[1.]*4
  captured.u_sol=np.zeros((12,1));captured.solution_status=0;captured.solve_time=.001
  update(captured,c,r)
  np.testing.assert_array_equal(captured.params,baseline.params)
  np.testing.assert_array_equal(captured.yref,baseline.yref)
  p=captured.evidence_problem
  assert len(p['params'])==13 and p['warm_scope']=='previous_primal_solution_not_internal_dual_state'
  p['params'][0][1]=999.
  assert captured.params[0,1]==baseline.params[0,1]  # No alias to control arrays.


def test_logging_follows_actual_plan_publication():
  path=Path(__file__).resolve().parents[3]/'selfdrive/controls/lib/longitudinal_planner.py'
  cls=next(x for x in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(x,ast.ClassDef))
  publish=next(x for x in cls.body if isinstance(x,ast.FunctionDef) and x.name=='publish')
  calls=[x for x in ast.walk(publish) if isinstance(x,ast.Call)]
  send=next(x for x in calls if ast.unparse(x.func)=='pm.send')
  emit=next(x for x in calls if ast.unparse(x.func)=='self.long_evidence.emit')
  assert send.lineno<emit.lineno


@pytest.mark.parametrize('change,reason',[
  ({'sequence':4},'planner_evidence_gap'),({'cycle':5},'planner_evidence_gap'),
  ({'settings_generation':2},'producer_or_settings_changed'),
  ({'producer_errors':1},'producer_error_observed'),({'services':{}},'missing_input_validity')])
def test_missing_or_changed_evidence_never_claims_no_effect(change,reason):
  result=summarize_frames([frame(),frame(2,**change)])
  assert result['classification']=='unknown' and reason in result['integrity_issues']
  assert result['placebo_determined'] is False


def test_complete_equal_ceiling_is_not_a_placebo_or_plan_identity_claim():
  rows=[frame(i,comparison={'30':{'applied_ceiling':1.3},'40':{'applied_ceiling':1.3}}) for i in range(1,5)]
  assert summarize_frames(rows)['classification']=='same_ceiling_on_recorded_inputs'
  assert summarize_frames([])['classification']=='unknown'


def test_video_matches_presentation_timestamp_not_constant_fps():
  media={('r',0):[{'t':100_000_000,'pts':.07,'file':'media/a.mp4','frameId':3},
                  {'t':151_000_000,'pts':.125,'file':'media/a.mp4','frameId':4}]}
  match=match_video({'route':'r','t':150_000_000},media)
  assert match['pts']==.125 and match['match_error_ms']==1.
  assert match_video({'route':'r','t':1_000_000_000},media) is None


def test_report_escapes_script_end_from_untrusted_logs(tmp_path):
  render({'routes':[], 'unsafe':'</script><script>alert(1)</script>'},tmp_path)
  text=(tmp_path/'index.html').read_text(encoding='utf-8')
  assert '</script><script>alert(1)' not in text


def test_recorder_storage_and_schema_compatible_export(tmp_path):
  from selfdrive.carrot.can_diagnostics.automatic import AutoRecorder,ChunkStore
  store=ChunkStore(tmp_path,reserve=0)
  recorder=AutoRecorder(store,{'car_fingerprint':'HYUNDAI_PALISADE_LX3_HEV','boot_id':'test'})
  store.begin({'kind':'header','schema':1},1.)
  recorder.state='recording'
  row=frame(20);row['mono_ns']=1_000_000_000
  recorder.longitudinal_evidence(row,1_001_000_000,1.01)
  recorder.longitudinal_evidence({**row,'mono_ns':0},1_001_000_000,1.01)
  assert recorder.longitudinal_frames==1 and recorder.longitudinal_rejected==1
  store.seal('test')
  lines=[json.loads(line) for line in gzip.open(next(tmp_path.glob('*.jsonl.gz')),'rt')]
  assert lines[-1]['kind']=='longitudinal_evidence'
  assert lines[-1]['comparison']==row['comparison']
