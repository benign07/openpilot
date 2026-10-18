"""Actual OEM DBC decoding and receive-only UI logic; no graphics/CAN TX."""
import ast
import math
import re
from types import SimpleNamespace as NS
import pytest
from openpilot.selfdrive.controls.tests.test_surround_display import ROOT,load,codec  # noqa: F401

UI=ROOT/'selfdrive/ui/onroad/augmented_road_view.py'


def overlay_scope(extra=None):
  return load(UI,{'_oem_bits','_decode_oem_surround','_oem_margin_status','_oem_comfort_status','OemSurroundOverlay'},
              {'math':math,'re':re,**(extra or {})})


@pytest.mark.parametrize('name,address', [('ADRV_0x1ea',0x1EA),('CCNC_0x162',0x162)])
@pytest.mark.parametrize('side', ['LF','RF','LR','RR'])
def test_actual_dbc_slot_distance_and_lateral_positions(codec,name,address,side):
  packer,_,checksum=codec
  data=packer.make_can_msg(name,2,{side+'_DETECT':2,side+'_DETECT_DISTANCE':12.3,side+'_DETECT_LATERAL':3.2})[1]
  values=overlay_scope()['_decode_oem_surround'](address,data)
  assert values[side]['distance'] == pytest.approx(12.3)
  assert values[side]['lateral'] == pytest.approx(3.2)
  assert values[side]['class'] == (2 if address==0x162 else 0)
  assert values[side]['white']


@pytest.mark.parametrize('name,address,code', [('ADRV_0x1ea',0x1EA,4),('CCNC_0x162',0x162,31)])
def test_hidden_or_unknown_type_suppresses_generic_fallback(codec,name,address,code):
  packer,_,checksum=codec
  data=packer.make_can_msg(name,2,{'LF_DETECT':code,'LF_DETECT_DISTANCE':12.,'LF_DETECT_LATERAL':3.})[1]
  assert overlay_scope()['_decode_oem_surround'](address,data)=={'LF':None}


@pytest.mark.parametrize('text,expected', [
 ('debug | lx3_margin_v1=3.0000,away_from_left_vehicle,0',3.),
 ('debug | lx3_margin_v1=-2.0000,away_from_right_vehicle,0',-2.),
 ('debug | lx3_margin_v1=0.0000,away_from_left_vehicle,0',None),
 ('debug | lx3_margin_v1=3.0000,away_from_left_vehicle,1',None),
 ('debug | lx3_margin_v1=-3.0000,away_from_left_vehicle,0',None),
 ('debug | lx3_margin_v1=-3.0000,toward_left_yellow,0',None),
 ('debug | lx3_margin_v1=3.0000,lane_or_curve_unqualified,0',None),
 ('debug | lx3_margin_v1=11.0000,away_from_left_vehicle,0',None),
 ('old build margin=test 3cm away_from_left_vehicle',None)])
def test_indicator_requires_actual_nonshadow_away_offset(text,expected):
  assert overlay_scope()['_oem_margin_status'](text)==expected


def render_fixture(codec,**changes):
  packer,_,checksum=codec;clock=NS(now=2.)
  raw=packer.make_can_msg('ADRV_0x1ea',2,{'LF_DETECT':1,'LF_DETECT_DISTANCE':12.,'LF_DETECT_LATERAL':3.})[1]
  packet=NS(src=2,address=0x1EA,dat=raw)
  event=NS(logMonoTime=1_990_000_000,valid=True,can=[packet]);queue=[event];draws=[]
  class SM(dict):pass
  sm=SM(carState=NS(canValid=True,canTimeout=False),carControl=NS(latActive=True,longActive=True),longitudinalPlan=NS(),
    lateralPlan=NS(mpcSolutionValid=True,latDebugText='debug | lx3_margin_v1=3.0000,away_from_left_vehicle,0'))
  sm.valid={k:True for k in sm};sm.alive=dict(sm.valid);sm.logMonoTime={k:1_990_000_000 for k in sm}
  ui=NS(sm=sm,CP=NS(carFingerprint='HYUNDAI_PALISADE_LX3_HEV'),params=NS(get_int=lambda _:1))
  rl=NS(Rectangle=lambda x,y,width,height:NS(x=x,y=y,width=width,height=height),Color=lambda *v:v,
    WHITE=1,SKYBLUE=2,GREEN=3,LIGHTGRAY=4,
    draw_rectangle_rounded=lambda *a:draws.append(('panel',a)),draw_rectangle_lines_ex=lambda *a:None,
    draw_line=lambda *a:None)
  env=overlay_scope({'ui_state':ui,'time':NS(monotonic=lambda:clock.now,monotonic_ns=lambda:int(clock.now*1e9)),'rl':rl,
    'messaging':NS(recv_one_or_none=lambda _:queue.pop(0) if queue else None),
    'draw_text_ui_style':lambda text,*a,**kw:draws.append(('text',text))})
  obj=env['OemSurroundOverlay']();obj.socket=object();obj.bus=2;obj.checksum=checksum
  return obj,sm,ui,clock,event,packet,queue,draws,rl.Rectangle(0,0,1920,1080)


def test_actual_receive_path_shows_generic_target_and_applied_indicator(codec):
  obj,sm,ui,clock,event,packet,queue,draws,rect=render_fixture(codec)
  obj.render(rect)
  assert any(kind=='text' and '12m'==text for kind,text in draws)
  assert any(kind=='text' and '거리두기 적용 중 → 3.0cm'==text for kind,text in draws)


@pytest.mark.parametrize('condition', ['stale','future','bad_crc','wrong_bus','invalid_CAN'])
def test_invalid_source_never_draws_an_object(codec,condition):
  obj,sm,ui,clock,event,packet,queue,draws,rect=render_fixture(codec)
  sm['carControl'].latActive=False
  if condition=='stale':event.logMonoTime=1_000_000_000
  elif condition=='future':event.logMonoTime=2_100_000_000
  elif condition=='bad_crc':packet.dat=packet.dat[:8]+bytes([packet.dat[8]^1])+packet.dat[9:]
  elif condition=='wrong_bus':packet.src=128
  else:sm['carState'].canValid=False
  obj.render(rect)
  assert not draws


@pytest.mark.parametrize('condition', ['control_off','plan_stale','plan_invalid','shadow','zero'])
def test_target_visible_does_not_imply_away_adjustment(codec,condition):
  obj,sm,ui,clock,event,packet,queue,draws,rect=render_fixture(codec)
  if condition=='control_off':sm['carControl'].latActive=False
  elif condition=='plan_stale':sm.logMonoTime['lateralPlan']=1_000_000_000
  elif condition=='plan_invalid':sm['lateralPlan'].mpcSolutionValid=False
  elif condition=='shadow':sm['lateralPlan'].latDebugText='lx3_margin_v1=3.0000,away_from_left_vehicle,1'
  else:sm['lateralPlan'].latDebugText='lx3_margin_v1=0.0000,away_from_left_vehicle,0'
  obj.render(rect)
  assert ('text','12m') in draws
  assert ('text','거리두기 미적용') in draws


def test_slots_expire_without_reusing_receive_time(codec):
  obj,sm,ui,clock,event,packet,queue,draws,rect=render_fixture(codec)
  sm['carControl'].latActive=False;obj.render(rect);assert draws
  draws.clear();clock.now=2.3
  sm.logMonoTime={k:2_290_000_000 for k in sm}
  obj.render(rect);assert not draws


def test_overlay_has_no_transmit_calls_and_alerts_draw_after_it():
  tree=ast.parse(UI.read_text(encoding='utf-8'))
  cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='OemSurroundOverlay')
  calls=[ast.unparse(n.func) for n in ast.walk(cls) if isinstance(n,ast.Call)]
  assert not any('PubMaster' in s or s.endswith('.send') or 'put_' in s for s in calls)
  view=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='AugmentedRoadView')
  draw=ast.unparse(next(n for n in view.body if isinstance(n,ast.FunctionDef) and n.name=='_render'))
  assert draw.index('oem_surround_overlay.render') < draw.index('alert_renderer.render')


@pytest.mark.parametrize('stamp,mask,expected',[(1_990_000_000,7,7),(1_990_000_000,0,0),
  (1_000_000_000,7,0),(2_000_000_001,7,0)])
def test_comfort_indicator_expires_and_never_uses_future_or_zero_status(stamp,mask,expected):
  fn=overlay_scope()['_oem_comfort_status']
  assert fn(f'debug | lx3_comfort_v1={stamp},{mask}',2_000_000_000)==expected


def test_comfort_can_show_without_surround_display_or_lateral_control(codec):
  obj,sm,ui,clock,event,packet,queue,draws,rect=render_fixture(codec)
  ui.params.get_int=lambda _:0;sm['carControl'].latActive=False
  sm['lateralPlan'].latDebugText='lx3_comfort_v1=1990000000,3'
  obj.render(rect)
  assert ('text','부드러운 감속 적용 중') in draws
  assert ('text','타력 · 미리 감속') in draws
  assert ('text','12m') not in draws


@pytest.mark.parametrize('change,expected', [({},1),(dict(enabled=False),0),(dict(eligible=False),0),(dict(active=False),0),
  (dict(mode='blended'),0),(dict(reset=True),0),(dict(after=1.2),0),(dict(final=.1),0),
  (dict(after=1.2,final=1.2,margin=2.),2),(dict(after=1.2,final=1.2,jerk=1.2),4),
  (dict(margin=2.,jerk=1.2),7)])
def test_passive_producer_marks_only_a_binding_approach_ceiling(change,expected):
  values=dict(enabled=True,eligible=True,active=True,mode='acc',reset=False,after=.5,final=.5)
  values.update(change)
  path=ROOT/'selfdrive/controls/lib/longitudinal_planner.py'
  tree=ast.parse(path.read_text(encoding='utf-8'))
  cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='LongitudinalPlanner')
  update=next(n for n in cls.body if isinstance(n,ast.FunctionDef) and n.name=='update')
  start=next(i for i,n in enumerate(update.body) if isinstance(n,ast.Assign) and ast.unparse(n.targets[0])=='comfort_mask')
  nodes=update.body[start:start+3]
  carrot=NS(jerk_factor_apply=.5)
  scope={'self':NS(approach_supported=True,approach_settings=NS(enabled=values['enabled']),
    mpc=NS(mode=values['mode'],max_a=values['final'],approach_margin_applied_m=values.get('margin',0.))),
    'approach_eligible':values['eligible'],'sm':{'carControl':NS(longActive=values['active'])},
    'reset_state':values['reset'],'accel_limits_turns':[-2.,values['after']],'evidence_maximum':1.2,
    'approach_request':NS(jerk_factor=values.get('jerk',1.)),'carrot':carrot,'time':NS(monotonic_ns=lambda:2_000_000_000)}
  exec(compile(ast.Module(body=nodes,type_ignores=[]),'actual-comfort-ui-producer','exec'),scope)
  assert carrot.lx3_comfort_ui==(2_000_000_000,expected)
