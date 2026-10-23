"""Actual OEM DBC decoding and receive-only UI logic; no graphics/CAN TX."""
import ast
import math
import re
from types import SimpleNamespace as NS
import pytest
from openpilot.selfdrive.controls.tests.test_surround_display import ROOT,load,codec,display_scope,lane_model,lead  # noqa: F401

UI=ROOT/'selfdrive/ui/onroad/augmented_road_view.py'


def overlay_scope(extra=None):
  return load(UI,{'_oem_bits','_decode_oem_surround','_oem_margin_status','_oem_comfort_status','_oem_native_clock_offset_ns','_render_driving_status','OemSurroundOverlay'},
              {'math':math,'re':re,'CALIBRATED':1,**(extra or {})})


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
  raw=packer.make_can_msg('CCNC_0x162',2,{'LF_DETECT':4,'LF_DETECT_DISTANCE':12.,'LF_DETECT_LATERAL':3.})[1]
  packet=NS(src=2,address=0x162,dat=raw)
  event=NS(logMonoTime=1_990_000_000,valid=True,can=[packet]);queue=[event];draws=[]
  class SM(dict):pass
  sm=SM(carState=NS(canValid=True,canTimeout=False),carControl=NS(latActive=True,longActive=True),longitudinalPlan=NS(),
    modelV2=NS(position=NS(x=[0.,60.],z=[0.,0.])),liveCalibration=NS(calStatus=1,rpyCalib=[0.,0.,0.]),
    roadCameraState=NS(),wideRoadCameraState=NS(),
    lateralPlan=NS(mpcSolutionValid=True,latDebugText='debug | lx3_margin_v1=3.0000,away_from_left_vehicle,0'))
  sm.valid={k:True for k in sm};sm.alive=dict(sm.valid);sm.logMonoTime={k:1_990_000_000 for k in sm}
  ui=NS(sm=sm,CP=NS(carFingerprint='HYUNDAI_PALISADE_LX3_HEV'),params=NS(get_int=lambda _:1))
  rl=NS(Rectangle=lambda x,y,width,height:NS(x=x,y=y,width=width,height=height),Color=lambda *v:v,
    WHITE=1,SKYBLUE=2,GREEN=3,LIGHTGRAY=4,
    draw_rectangle_rounded=lambda *a:draws.append(('panel',a)),draw_rectangle_lines_ex=lambda *a:draws.append(('icon',a)),
    draw_line=lambda *a:None)
  env=overlay_scope({'ui_state':ui,'time':NS(monotonic=lambda:clock.now,monotonic_ns=lambda:int(clock.now*1e9)),'rl':rl,
    'messaging':NS(recv_one_or_none=lambda _:queue.pop(0) if queue else None),
    'draw_text_ui_style':lambda text,*a,**kw:draws.append(('text',text))})
  obj=env['OemSurroundOverlay']();obj.socket=object();obj.bus=2;obj.echo_bus=128;obj.checksum=checksum
  renderer=NS(_car_space_transform=[[960.,1000.,0.],[540.,0.,1000.],[1.,0.,0.]],_path_offset_z=1.22)
  render=obj.render;obj.render=lambda rect,**kw:render(rect,renderer,**kw)
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
  elif condition=='wrong_bus':packet.src=129
  else:sm['carState'].canValid=False
  obj.render(rect)
  assert not any(kind=='icon' or kind=='text' and text=='12m' for kind,text in draws)


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
  assert not any(kind=='text' and '거리두기 적용 중' in text for kind,text in draws)


def test_slots_expire_without_reusing_receive_time(codec):
  obj,sm,ui,clock,event,packet,queue,draws,rect=render_fixture(codec)
  sm['carControl'].latActive=False;obj.render(rect);assert draws
  draws.clear();clock.now=2.3
  sm.logMonoTime={k:2_290_000_000 for k in sm}
  obj.render(rect);assert not any(kind=='icon' or kind=='text' and text=='12m' for kind,text in draws)


def test_optional_overlay_is_detached_from_live_road_ui():
  tree=ast.parse(UI.read_text(encoding='utf-8'))
  cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='OemSurroundOverlay')
  calls=[ast.unparse(n.func) for n in ast.walk(cls) if isinstance(n,ast.Call)]
  assert not any('PubMaster' in s or s.endswith('.send') or 'put_' in s for s in calls)
  view=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='AugmentedRoadView')
  init=ast.unparse(next(n for n in view.body if isinstance(n,ast.FunctionDef) and n.name=='__init__'))
  draw=ast.unparse(next(n for n in view.body if isinstance(n,ast.FunctionDef) and n.name=='_render'))
  assert 'OemSurroundOverlay' not in init
  assert 'oem_surround_overlay' not in draw
  assert draw.index('model_renderer.render') < draw.index('alert_renderer.render')
  assert draw.index('_render_driving_status') < draw.index('alert_renderer.render')


def test_lightweight_status_retains_feedback_without_consuming_can(codec):
  obj,sm,ui,clock,event,packet,queue,draws,rect=render_fixture(codec)
  sm['lateralPlan'].latDebugText += ' | lx3_comfort_v1=1990000000,3'
  status=overlay_scope({'ui_state':ui,'time':NS(monotonic=lambda:clock.now,monotonic_ns=lambda:int(clock.now*1e9)),
    'rl':NS(GREEN=1,SKYBLUE=2),
    'draw_text_ui_style':lambda text,*a,**kw:draws.append(('text',text))})['_render_driving_status']
  status(rect)
  assert len(queue)==1  # Existing CAN queue was not read to draw plan status.
  assert any('거리두기 적용 중' in text for kind,text in draws)
  assert any('부드러운 감속 적용 중' in text for kind,text in draws)
  draws.clear();sm['carControl'].latActive=sm['carControl'].longActive=False
  status(rect);assert not draws


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
  assert ('text','부드러운 감속 적용 중 · 가속 억제 · 미리 감속') in draws
  assert ('text','12m') not in draws


def test_both_controls_off_still_show_objects_without_claiming_active_comfort(codec):
  obj,sm,ui,clock,event,packet,queue,draws,rect=render_fixture(codec)
  sm['carControl'].enabled=False
  sm['carControl'].latActive=sm['carControl'].longActive=False
  sm['lateralPlan'].latDebugText='lx3_comfort_v1=1990000000,7'
  obj.render(rect)
  assert ('text','12m') in draws
  assert not any(kind=='text' and ('부드러운 감속' in text or '거리두기 적용 중' in text) for kind,text in draws)


def test_traffic_only_acceleration_limit_does_not_claim_physical_coasting(codec):
  # A recorded traffic-only positive ceiling (1.29025m/s2, coast_weight=0)
  # produces bit1 too. It means a binding acceleration limit, not freewheel.
  obj,sm,ui,clock,event,packet,queue,draws,rect=render_fixture(codec)
  sm['carControl'].latActive=False
  sm['lateralPlan'].latDebugText='lx3_comfort_v1=1990000000,1'
  obj.render(rect)
  assert ('text','부드러운 감속 적용 중 · 가속 억제') in draws
  assert not any(kind=='text' and '타력' in text for kind,text in draws)


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


def test_actual_tx_echo_can_place_a_known_car_on_the_camera(codec):
  obj,sm,ui,clock,event,packet,queue,draws,rect=render_fixture(codec)
  packet.src=128;sm['carControl'].latActive=False
  obj.render(rect)
  body=next(value[0] for kind,value in draws if kind=='icon')
  assert body.x+body.width/2==pytest.approx(710.)
  assert body.y+body.height==pytest.approx(540.+1000.*1.22/12.)
  assert not any(kind=='panel' for kind,value in draws)  # No minimap.


def test_fresh_empty_tx_clears_original_objects_instead_of_merging_them_back(codec):
  packer,_,_=codec
  obj,sm,ui,clock,event,packet,queue,draws,rect=render_fixture(codec)
  empty=packer.make_can_msg('CCNC_0x162',0,{})[1]
  queue.append(NS(logMonoTime=1_995_000_000,valid=True,can=[NS(src=128,address=0x162,dat=empty)]))
  obj.render(rect)
  assert not any(kind=='icon' for kind,value in draws)


@pytest.mark.parametrize('code,visible',[(1,False),(2,False),(3,True),(4,True),(5,True),(6,True),
  (7,False),(8,False),(9,True),(10,True),(11,True),(12,True),(13,False),(14,False),(31,False)])
def test_tx_only_unknown_boxes_people_and_cones_are_not_camera_cars(codec,code,visible):
  packer,_,_=codec
  obj,sm,ui,clock,event,packet,queue,draws,rect=render_fixture(codec)
  packet.src=128
  packet.dat=packer.make_can_msg('CCNC_0x162',0,{'LF_DETECT':code,'LF_DETECT_DISTANCE':12.,'LF_DETECT_LATERAL':3.})[1]
  obj.render(rect)
  assert any(kind=='icon' for kind,value in draws) is visible


def test_original_oem_colour_may_draw_only_a_neutral_marker(codec):
  packer,_,_=codec
  obj,sm,ui,clock,event,packet,queue,draws,rect=render_fixture(codec)
  packet.address=0x1EA
  packet.dat=packer.make_can_msg('ADRV_0x1ea',2,{'LF_DETECT':2,'LF_DETECT_DISTANCE':12.,'LF_DETECT_LATERAL':3.})[1]
  obj.render(rect)
  assert len([value for kind,value in draws if kind=='icon'])==1  # No invented car silhouette.
  assert ('text','12m') in draws


@pytest.mark.parametrize('condition',['uncalibrated','old_calib','bad_rpy','stale_model','stale_camera','future_camera'])
def test_camera_icons_require_real_calibration_model_and_camera_freshness(codec,condition):
  obj,sm,ui,clock,event,packet,queue,draws,rect=render_fixture(codec)
  if condition=='uncalibrated':sm['liveCalibration'].calStatus=0
  elif condition=='old_calib':sm.logMonoTime['liveCalibration']=500_000_000
  elif condition=='bad_rpy':sm['liveCalibration'].rpyCalib=[0.,float('nan'),0.]
  elif condition=='stale_model':sm.logMonoTime['modelV2']=1_500_000_000
  elif condition=='stale_camera':sm.logMonoTime['roadCameraState']=1_500_000_000
  else:sm.logMonoTime['roadCameraState']=2_100_000_000
  obj.render(rect)
  assert not any(kind=='icon' for kind,value in draws)


def test_native_boot_time_is_normalized_without_renewing_old_packets(codec):
  obj,sm,ui,clock,event,packet,queue,draws,rect=render_fixture(codec)
  env=obj.project_icon.__globals__
  env['time'].CLOCK_BOOTTIME=7
  env['time'].clock_gettime_ns=lambda _:int(clock.now*1e9)+5_000_000_000
  event.logMonoTime+=5_000_000_000
  sm.logMonoTime['roadCameraState']+=5_000_000_000
  obj.render(rect);assert ('text','12m') in draws
  draws.clear();clock.now=2.3
  sm.logMonoTime={k:2_290_000_000 for k in sm};sm.logMonoTime['roadCameraState']+=5_000_000_000
  queue.append(event)  # Same old source timestamp, received again.
  obj.render(rect);assert not any(kind=='icon' for kind,value in draws)


def test_projection_moves_with_distance_and_never_places_rear_or_off_camera_objects():
  obj=overlay_scope()['OemSurroundOverlay'];model=NS(position=NS(x=[0.,60.],z=[0.,0.]))
  renderer=NS(_car_space_transform=[[960.,1000.,0.],[540.,0.,1000.],[1.,0.,0.]],_path_offset_z=1.22)
  rect=NS(x=0.,y=0.,width=1920.,height=1080.)
  a=obj.project_icon('LF',{'distance':12.,'lateral':3.,'class':4},renderer,model,rect)
  b=obj.project_icon('LF',{'distance':24.,'lateral':3.,'class':4},renderer,model,rect)
  assert a[0]<b[0]<960. and a[1]>b[1]  # Receding target approaches vanishing point.
  for side,target in [('LR',{'distance':12.,'lateral':3.,'class':4}),
                      ('LF',{'distance':1.,'lateral':3.,'class':4}),
                      ('LF',{'distance':12.,'lateral':30.,'class':4})]:
    assert obj.project_icon(side,target,renderer,model,rect) is None
  renderer._car_space_transform=[[0.,0.,0.]]*3
  assert obj.project_icon('LF',{'distance':12.,'lateral':3.,'class':4},renderer,model,rect) is None


def test_wide_camera_requires_its_own_valid_calibration(codec):
  obj,sm,ui,clock,event,packet,queue,draws,rect=render_fixture(codec)
  obj.render(rect,camera_service='wideRoadCameraState')
  assert not any(kind=='icon' for kind,value in draws)
  sm['liveCalibration'].wideFromDeviceEuler=[0.,0.,0.]
  draws.clear();obj.render(rect,camera_service='wideRoadCameraState')
  assert ('text','12m') in draws


@pytest.mark.parametrize('condition',['ok','static','slow','bad_lane','stale_radar','stale_model',
  'old_origin','radar_error','position_mismatch','hidden_source','source_geometry','missing_source','old_helper'])
def test_generic_echo_needs_fresh_matching_occupancy_and_truly_empty_sources(codec,condition):
  packer,_,_=codec
  obj,sm,ui,clock,event,packet,queue,draws,rect=render_fixture(codec)
  queue.clear();sm['carControl'].latActive=False
  md=lane_model();md.position=NS(x=[0.,60.],z=[0.,0.]);sm['modelV2']=md
  target=lead(dRel=12.,yRel=3.,dPath=3.)
  sm['radarState']=NS(mdMonoTime=1_990_000_000,leadsLeft=[target],leadsRight=[],
    radarErrors=NS(to_dict=lambda:{'canError':condition=='radar_error'}))
  sm.valid['radarState']=sm.alive['radarState']=True;sm.logMonoTime['radarState']=1_990_000_000
  obj.filter_lead=display_scope()['LX3SurroundDisplay'].eligible
  for name,address in (('ADRV_0x1ea',0x1EA),('CCNC_0x162',0x162)):
    source={}
    if condition=='hidden_source' and address==0x1EA:source={'LF_DETECT':4,'LF_DETECT_DISTANCE':12.,'LF_DETECT_LATERAL':3.}
    if condition=='source_geometry' and address==0x162:source={'LF_DETECT_DISTANCE':12.,'LF_DETECT_LATERAL':3.}
    if condition=='missing_source' and address==0x162:continue
    queue.append(NS(logMonoTime=1_990_000_000,valid=True,can=[NS(src=2,address=address,dat=packer.make_can_msg(name,2,source)[1])]))
  echo=packer.make_can_msg('CCNC_0x162',0,{'LF_DETECT':1,'LF_DETECT_DISTANCE':12.,'LF_DETECT_LATERAL':3.})[1]
  queue.append(NS(logMonoTime=1_995_000_000,valid=True,can=[NS(src=128,address=0x162,dat=echo)]))
  if condition=='static':target.vLead=0.
  elif condition=='slow':target.vLead=3.
  elif condition=='bad_lane':md.laneLineProbs[1]=.1
  elif condition=='stale_radar':sm.logMonoTime['radarState']=1_000_000_000
  elif condition=='stale_model':sm.logMonoTime['modelV2']=1_000_000_000
  elif condition=='old_origin':sm['radarState'].mdMonoTime=1_000_000_000
  elif condition=='position_mismatch':target.dRel=30.
  elif condition=='old_helper':obj.filter_lead=None
  obj.render(rect)
  visible=condition in ('ok','static','slow','bad_lane')
  assert len([x for kind,x in draws if kind=='icon'])==int(visible)
  assert ('text','12m') in draws if visible else ('text','12m') not in draws


@pytest.mark.parametrize('name,address',[('ADRV_0x1ea',0x1EA),('CCNC_0x162',0x162)])
@pytest.mark.parametrize('side',['LF','RF','LR','RR'])
def test_empty_slot_check_uses_all_three_actual_DBC_fields(codec,name,address,side):
  packer,_,_=codec;fn=overlay_scope()['_decode_oem_surround']
  assert fn(address,packer.make_can_msg(name,2,{})[1],empty_slots=True)[side]
  for suffix,value in (('_DETECT',1),('_DETECT_DISTANCE',12.),('_DETECT_LATERAL',3.)):
    assert not fn(address,packer.make_can_msg(name,2,{side+suffix:value})[1],empty_slots=True)[side]
