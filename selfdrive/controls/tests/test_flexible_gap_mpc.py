"""Actual MPC update/cost wiring; solver calls are not numerical qualification."""
import ast
from pathlib import Path
from types import SimpleNamespace as NS
import numpy as np
import pytest
from openpilot.selfdrive.controls.lib.flexible_gap import FlexibleGap, FlexibleGapSettings, MIN_FLEX_TF
from openpilot.selfdrive.controls.lib.approach_comfort import additional_approach_tf


def production_mpc():
  root=Path(__file__).resolve().parents[3]
  path=root/'selfdrive/controls/lib/longitudinal_mpc_lib/long_mpc.py'
  tree=ast.parse(path.read_text(encoding='utf-8'))
  # Preserve all production constants/functions/classes, replacing imports
  # only. CasADi/Acados generation functions are present but not executed.
  tree.body=[n for n in tree.body if isinstance(n,(ast.Assign,ast.AnnAssign,ast.FunctionDef,ast.ClassDef))]
  ns=dict(np=np,os=__import__('os'),__file__=str(path),DT_MDL=.05,ACCEL_MIN=-3.5,
    log=NS(LongitudinalPersonality=NS(standard=1,relaxed=2,aggressive=0,moreRelaxed=3)),
    index_function=lambda idx,max_val,max_idx:max_val*(idx/max_idx)**2,
    FlexibleGap=FlexibleGap,FlexibleGapSettings=FlexibleGapSettings,MIN_FLEX_TF=MIN_FLEX_TF,
    additional_approach_tf=additional_approach_tf)
  exec(compile(tree,str(path),'exec'),ns)
  m=ns['LongitudinalMpc'].__new__(ns['LongitudinalMpc'])
  m.x0=np.array([0.,20.,0.]);m.x_sol=np.tile([0.,20.,0.],(13,1))
  m.yref=np.zeros((13,6));m.params=np.zeros((13,8));m.prev_a=np.zeros(13)
  m.mode='acc';m.j_lead=0.;m.max_a=1.2;m.cruise_min_a=-2.;m.crash_cnt=0
  m.flex_gap=FlexibleGap();m.solver=NS(set=lambda *args:None)
  m.run=lambda:None
  # Same predicted lead trajectories in each comparison; the physical lead
  # path is held fixed to isolate actual MPC cost/constraint construction.
  m.process_lead=lambda obj,j:(np.column_stack([obj.dRel+19.7*ns['T_IDXS'],np.full(13,19.7)]),19.7)
  c=NS(get_T_FOLLOW=lambda *args:2.,dynamic_t_follow=lambda tf,*args:tf,
       flex_gap_nominal_tf=2.,comfort_brake=2.5,stop_distance=5.5,
       j_lead_factor=0.,mode='acc',v_cruise=35.,stop_dist=1000.,trafficStopDistanceAdjust=0.,autoNaviSpeedDecelRate=1.)
  lead=NS(status=True,radar=True,radarTrackId=1,dRel=45.,vLead=19.7,vRel=-.3,aLeadK=0.,jLead=0.,modelProb=.99)
  absent=NS(status=False,radar=False,radarTrackId=-1,dRel=1000.,jLead=0.,modelProb=0.)
  r=NS(leadOne=lead,leadTwo=absent)
  return m,c,r,ns


def update(m,c,r,**kw):
  m.update(c,False,r,35.,*[np.zeros(13) for _ in range(4)],**kw)


def test_default_or_ineligible_is_byte_identical_to_no_flex_reference():
  a,c,r,ns=production_mpc();update(a,c,r,approach_extra_tf=.2)
  for kw in (dict(flex_gap_settings=FlexibleGapSettings(True,70),flex_gap_eligible=False),
             dict(flex_gap_settings=FlexibleGapSettings(False,70),flex_gap_eligible=True)):
    b,c,r,ns=production_mpc();update(b,c,r,approach_extra_tf=.2,**kw)
    np.testing.assert_array_equal(a.params,b.params);np.testing.assert_array_equal(a.yref,b.yref)


def test_signed_cost_target_and_original_physical_danger_parameters():
  a,c,r,ns=production_mpc();a.max_a=0.;update(a,c,r)
  b,c,r,ns=production_mpc();b.max_a=0.
  for _ in range(300):update(b,c,r,flex_gap_settings=FlexibleGapSettings(True,70),flex_gap_eligible=True)
  assert np.any(b.yref[:,0]<0.)
  np.testing.assert_array_equal(a.params,b.params)
  safe=ns['get_safe_obstacle_distance'](20.,2.,2.5,5.5)
  target=safe+b.yref[:,0]*30.
  assert np.all(target>=safe*.85-1e-9)
  assert target[0]>=safe-20.*(2.-MIN_FLEX_TF)
  assert b.desired_distance<a.desired_distance


def test_relaxation_tightens_only_positive_accel_bound_and_risk_restores():
  b,c,r,ns=production_mpc()
  for _ in range(300):
    b.max_a=1.2
    update(b,c,r,flex_gap_settings=FlexibleGapSettings(True,70),flex_gap_eligible=True)
  assert b.max_a==0 and np.all(b.params[:,0]==-3.5)
  r.leadOne.aLeadK=-2.
  update(b,c,r,flex_gap_settings=FlexibleGapSettings(True,70),flex_gap_eligible=True)
  assert b.flex_gap.reduction==0 and np.all(b.yref[:,0]==0)


def test_nominal_tf_gate_retains_stock_boost_and_secondary_unrelaxed():
  b,c,r,ns=production_mpc();c.flex_gap_nominal_tf=1.8
  for _ in range(300):update(b,c,r,flex_gap_settings=FlexibleGapSettings(True,70),flex_gap_eligible=True)
  assert np.all(b.yref[:,0]==0)
  c.flex_gap_nominal_tf=2.
  # A slower secondary remains physically limiting and gets no relaxation.
  r.leadTwo=NS(**vars(r.leadOne));r.leadTwo.radarTrackId=2;r.leadTwo.dRel=35.
  for _ in range(300):update(b,c,r,flex_gap_settings=FlexibleGapSettings(True,70),flex_gap_eligible=True)
  assert b.flex_gap.reduction>0
  assert np.all(b.yref[:,0]==0)


def test_nominal_property_calculation_matches_unmodified_applied_base():
  root=Path(__file__).resolve().parents[3]
  tree=ast.parse((root/'selfdrive/carrot/carrot_functions.py').read_text(encoding='utf-8'))
  cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='CarrotPlanner')
  method=next(n for n in cls.body if isinstance(n,ast.FunctionDef) and n.name=='get_T_FOLLOW')
  ns=dict(log=NS(LongitudinalPersonality=NS(standard=1)))
  exec(compile(ast.Module(body=[method],type_ignores=[]),'carrot_functions.py','exec'),ns)
  calls=[]
  c=NS(_get_base_t_follow=lambda *a:2.,_apply_speed_t_follow_scale=lambda tf,v:tf*.9,
    mySafeFactor=1.1,_clip_t_follow=lambda tf:tf,
    _apply_decel_hold_and_boost_t_follow=lambda tf,a:tf,
    apply_t_follow=lambda tf:calls.append(tf) or tf)
  assert ns['get_T_FOLLOW'](c,1,20,0)==c.flex_gap_nominal_tf
  assert len(calls)==1
