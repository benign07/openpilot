"""Production display paths on PC; no native IPC, actuator TX or vehicle I/O.

AST loading only avoids Linux Params imports. Formatter/packer/DBC functions
themselves are production code, including byte encoding and checksum callbacks.
"""
from __future__ import annotations

import ast
import copy
import dataclasses
import functools
import json
import math
import os
import re
import time
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

ROOT = Path(__file__).resolve().parents[3]
HYUNDAI = ROOT / 'opendbc_repo/opendbc/car/hyundai/hyundaicanfd.py'
LX3 = 'HYUNDAI_PALISADE_LX3_HEV'


def load(path, names, scope=None):
  scope = dict(scope or {})
  scope.setdefault('__name__', __name__)
  nodes = [n for n in ast.parse(path.read_text(encoding='utf8')).body
           if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name in names]
  assert {n.name for n in nodes} == set(names)
  exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), 'exec',
               flags=__import__('__future__').annotations.compiler_flag), scope)
  return scope


def display_scope():
  return load(HYUNDAI, {'LX3SurroundDisplay', '_apply_lx3_surround_display',
    '_clip_int', '_get_desire_and_lane_changing', '_apply_lane_desire',
    '_apply_radar_blink', '_make_ccnc_values', 'create_ccnc_messages'},
    {'math': math, 'copy': copy, 'HyundaiFlags': NS(CAMERA_SCC=NS(value=1)),
     'Params': lambda: NS(get_int=lambda name: 0)})


def lead(side=0, slot=1, **changes):
  sign = 1 if side == 0 else -1
  values = dict(status=True, radar=True, radarTrackId=slot, dRel=15.,
                yRel=3.2*sign, dPath=3.2*sign, vRel=0., vLead=10., vLat=0.)
  values.update(changes)
  return NS(**values)


def radar(left=None, right=None, error=False):
  return NS(leadsLeft=[lead()] if left is None else left,
            leadsRight=[] if right is None else right,
            radarErrors=NS(to_dict=lambda: {'canError': error}))


def prime(h, r=None, count=21, start=1_000_000_000):
  result = {}
  for i in range(count):
    stamp = start+i*50_000_000
    result = h.update((r or radar(), stamp), stamp, True)
  return result


def test_stable_tracks_are_generic_objects_without_an_engage_requirement():
  h = display_scope()['LX3SurroundDisplay']()
  assert not prime(h, count=20)
  assert h.update((radar(), 2_000_000_000), 2_000_000_000, True) == {'LF': (15., 3.2)}
  assert prime(h, radar([], [lead(1, 2, vLead=0.)]), start=3_000_000_000) == {'RF': (15., 3.2)}


@pytest.mark.parametrize('change', [
  {'status': False}, {'radar': False}, {'radarTrackId': -1},
  {'dRel': .5}, {'dRel': 50.}, {'yRel': -3.2}, {'dPath': -3.2},
  {'yRel': 2.}, {'yRel': 5.}, {'dPath': 2.}, {'dPath': 5.},
  {'yRel': 4.1}, {'vLead': -2.}, {'vLat': 1.}, {'vLat': -1.},
  *[{key: float('nan')} for key in ('dRel','yRel','dPath','vRel','vLead','vLat')],
  {'dRel': float('inf')},
])
def test_unsupported_or_uncertain_tracks_never_become_icons(change):
  h = display_scope()['LX3SurroundDisplay']()
  assert not prime(h, radar([lead(**change)]))


@pytest.mark.parametrize('event', ['missing', 'disabled', 'error', 'old', 'future', 'zero', 'reverse', 'dropout'])
def test_invalid_context_removes_visible_objects_immediately(event):
  h = display_scope()['LX3SurroundDisplay']()
  assert prime(h)
  t = 2_050_000_000
  args = ((radar(), t), t, True)
  if event == 'missing': args = (None, t, True)
  elif event == 'disabled': args = ((radar(), t), t, False)
  elif event == 'error': args = ((radar(error=True), t), t, True)
  elif event == 'old': args = ((radar(), 2_000_000_000), 2_200_000_001, True)
  elif event == 'future': args = ((radar(), t+1), t, True)
  elif event == 'zero': args = ((radar(), 0), t, True)
  elif event == 'reverse': args = ((radar(), 1_990_000_000), t, True)
  elif event == 'dropout': args = ((radar([]), t), t, True)
  assert h.update(*args) == {}
  assert h.objects == {}


def test_repeated_packets_cannot_age_a_track_into_a_visible_object():
  h = display_scope()['LX3SurroundDisplay']()
  for _ in range(100):
    assert not h.update((radar(), 1_000_000_000), 1_150_000_000, True)
  assert h.tracks[(0,1)][1] == 1_000_000_000


@pytest.mark.parametrize('change', [{'dRel': 25.}, {'yRel': 4., 'dPath': 4.},
                                    {'vLead': 20.}, {'radarTrackId': 9}])
def test_reused_slot_or_discontinuity_must_qualify_again(change):
  h = display_scope()['LX3SurroundDisplay']()
  assert prime(h)
  assert not h.update((radar([lead(**change)]), 2_050_000_000), 2_050_000_000, True)
  assert prime(h, radar([lead(**change)]), start=2_100_000_000)


def test_long_gap_and_duplicate_slot_do_not_inherit_stability():
  h = display_scope()['LX3SurroundDisplay']()
  assert prime(h)
  assert not h.update((radar(), 2_250_000_000), 2_250_000_000, True)
  assert not prime(h, radar([lead()], [lead(1, 1)]), start=3_000_000_000)
  assert not h.tracks


def test_nearest_qualified_target_and_bounded_state():
  h = display_scope()['LX3SurroundDisplay']()
  r = radar([lead(slot=i, dRel=30.-i) for i in range(20)],
            [lead(1, 100+i, dRel=40.-i) for i in range(20)])
  assert prime(h, r) == {'LF': (15., 3.2), 'RF': (25., 3.2)}
  assert len(h.tracks) == 32


def empty_slots():
  return {side+suffix: 0 for side in ('LF','RF','LR','RR')
          for suffix in ('_DETECT','_DETECT_DISTANCE','_DETECT_LATERAL')}


@pytest.mark.parametrize('source', [0,1])
@pytest.mark.parametrize('suffix,value', [('_DETECT',1),('_DETECT',31),
  ('_DETECT_DISTANCE',12.),('_DETECT_LATERAL',3.2),('_DETECT_LATERAL',float('nan'))])
def test_oem_type_or_hidden_geometry_in_either_format_has_priority(source, suffix, value):
  sources = [empty_slots(), empty_slots()]
  sources[source]['LF'+suffix] = value
  cs = NS(adrv_0x1ea=sources[0], ccnc_0x162=sources[1])
  values = empty_slots(); before = values.copy()
  display_scope()['_apply_lx3_surround_display'](values, cs, {'LF': (15.,3.2)})
  assert values == before


def test_fill_changes_only_six_empty_forward_side_fields():
  fn = display_scope()['_apply_lx3_surround_display']
  cs = NS(adrv_0x1ea=empty_slots(), ccnc_0x162=empty_slots())
  values = {**empty_slots(), 'FAULT_LSS': 1, 'FAULT_DAS': 1, 'HDA_MODE2': 0,
            'FF_DETECT': 5, 'FF_DISTANCE': 18., 'LR_DETECT': 3, 'COUNTER': 27}
  before = values.copy()
  fn(values, cs, {'LF': (15.,3.2), 'RF': (25.,3.5), 'LR': (1.,3.)})
  changed = {key for key in values if values[key] != before[key]}
  assert changed == {side+suffix for side in ('LF','RF')
                     for suffix in ('_DETECT','_DETECT_DISTANCE','_DETECT_LATERAL')}
  assert values['LF_DETECT'] == values['RF_DETECT'] == 1
  assert cs.adrv_0x1ea == cs.ccnc_0x162 == empty_slots()
  cs.ccnc_0x162 = None
  other = before.copy(); fn(other, cs, {'LF': (15.,3.2)})
  assert other == before


def test_card_context_requires_both_valid_fresh_services_and_model_origin():
  fn = load(ROOT/'selfdrive/car/card.py', {'surround_display_context'})['surround_display_context']
  class SM(dict): pass
  sm = SM(radarState=NS(mdMonoTime=1_950_000_000), modelV2=NS())
  sm.valid = sm.alive = {'radarState': True, 'modelV2': True}
  sm.logMonoTime = {'radarState': 2_000_000_000, 'modelV2': 1_950_000_000}
  assert fn(sm, 2_050_000_000) == (sm['radarState'], 2_000_000_000)
  for attr in ('valid','alive'):
    for service in sm:
      old = getattr(sm,attr)
      setattr(sm,attr,{**old,service:False})
      assert fn(sm,2_050_000_000) is None
      setattr(sm,attr,old)
  for service in sm:
    old = sm.logMonoTime[service]
    for stamp in (0,2_050_000_001,1_849_999_999):
      sm.logMonoTime[service] = stamp
      assert fn(sm,2_050_000_000) is None
    sm.logMonoTime[service] = old
  for stamp in (0,2_050_000_001,1_849_999_999):
    sm['radarState'].mdMonoTime = stamp
    assert fn(sm,2_050_000_000) is None


def test_incomplete_oem_source_does_not_imply_an_empty_slot():
  values = empty_slots(); original = values.copy()
  cs = NS(adrv_0x1ea=empty_slots(),ccnc_0x162={})
  display_scope()['_apply_lx3_surround_display'](values,cs,{'LF':(15.,3.2)})
  assert values == original


@pytest.mark.parametrize('valid,timeout,setting,frame,should_call', [
  (True,False,1,100,True),(False,False,1,100,True),
  (True,True,1,100,True),(True,False,0,100,True),(True,False,1,101,False),
])
def test_actual_controller_display_gate_uses_can_health_without_granting_control(
    valid,timeout,setting,frame,should_call):
  tree=ast.parse((ROOT/'opendbc_repo/opendbc/car/hyundai/carcontroller.py').read_text(encoding='utf8'))
  cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='CarController')
  update=next(n for n in cls.body if isinstance(n,ast.FunctionDef) and n.name=='update')
  node=next(n for n in update.body if isinstance(n,ast.If) and
            'self.lx3_surround_display' in ast.unparse(n.test))
  calls=[]
  selector=NS(update=lambda *args: (calls.append(args),{'LF':(15.,3.2)})[1])
  context=object(); cs=NS(out=NS(canValid=valid,canTimeout=timeout),lx3_surround_context=context)
  controller=NS(lx3_surround_display=selector,frame=frame,enable_corner_radar=setting)
  scope={'self':controller,'CS':cs,'now_nanos':2_000_000_000,'surround_objects':{}}
  exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-controller-display-gate','exec'),scope)
  assert bool(calls) is should_call
  if should_call:
    assert calls==[(context,2_000_000_000,bool(setting and valid and not timeout))]
    assert scope['surround_objects']=={'LF':(15.,3.2)}


def test_apply_extension_keeps_actuators_and_non_lx3_call_contract():
  path = ROOT/'opendbc_repo/opendbc/car/interfaces.py'
  cls = next(n for n in ast.parse(path.read_text(encoding='utf8')).body
             if isinstance(n,ast.ClassDef) and n.name=='CarInterfaceBase')
  node = next(n for n in cls.body if isinstance(n,ast.FunctionDef) and n.name=='apply')
  scope = {'time':time}
  exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec',
               flags=__import__('__future__').annotations.compiler_flag),scope)
  for fingerprint in (LX3,'KIA_EV9'):
    calls = []; result = (object(), [object()])
    ci = NS(CP=NS(carFingerprint=fingerprint),CS=NS(),
            CC=NS(update=lambda *args: (calls.append(args),result)[1]))
    command, model, context = object(), object(), (radar(),2_000_000_000)
    assert scope['apply'](ci,command,2_000_000_000,model,display_context=context) is result
    assert calls == [(command,ci.CS,2_000_000_000)] and ci.CS.modelV2 is model
    assert hasattr(ci.CS,'lx3_surround_context') is (fingerprint==LX3)
    assert scope['apply'](ci,command,2_010_000_000,model) is result
    if fingerprint==LX3: assert ci.CS.lx3_surround_context is None


def codec_scope():
  """Actual DBC/packer source and generated-name checksum selection, no native modules."""
  path = ROOT/'opendbc_repo/opendbc/can/dbc.py'
  crc_path = ROOT/'opendbc_repo/opendbc/car/crc.py'
  crc_scope = {}; exec(compile(crc_path.read_text(encoding='utf8'),str(crc_path),'exec'),crc_scope)
  checksum = load(HYUNDAI, {'hkg_can_fd_checksum'},crc_scope)['hkg_can_fd_checksum']
  scope = {'__name__': __name__, 're':re,'os':os,'Callable':Callable,
           'dataclass':dataclasses.dataclass,'cache':functools.cache,
           'DBC_PATH':str(path.parent),'hkg_can_fd_checksum':checksum}
  nodes = [n for n in ast.parse(path.read_text(encoding='utf8')).body
           if not isinstance(n,(ast.Import,ast.ImportFrom))]
  exec(compile(ast.Module(body=nodes,type_ignores=[]),str(path),'exec'),scope)
  def unknown(*args,**kwargs): raise AssertionError((args,kwargs))
  scope.update(math=math,carlog=NS(error=unknown))
  scope.update(load(ROOT/'opendbc_repo/opendbc/can/packer.py',{'CANPacker','set_value'},scope))
  scope.update(load(ROOT/'opendbc_repo/opendbc/can/parser.py',{'get_raw_value'},scope))
  return scope


@pytest.fixture
def codec(tmp_path):
  scope = codec_scope()
  dbc = tmp_path/'hyundai_canfd_lx3_hev_generated.dbc'
  source = ROOT/'opendbc_repo/opendbc/dbc/generator/hyundai/hyundai_canfd_lx3_hev.dbc'
  dbc.write_bytes(source.read_bytes())
  packer = scope['CANPacker'](str(dbc))
  def decode(name, dat):
    return {key:scope['get_raw_value'](dat,sig)*sig.factor+sig.offset
            for key,sig in packer.dbc.name_to_msg[name].sigs.items()}
  return packer, decode, scope['hkg_can_fd_checksum']


def minimal_cs():
  return NS(modelV2=None,lfahda_cluster={'HDA_CntrlModSta':0,'HDA_LFA_SymSta':0},
            cruise_buttons_msg=None,adrv_0x161=None,adrv_0x200=None,
            adrv_0x1ea=empty_slots(),ccnc_0x162=empty_slots(),
            out=NS(steeringAngleDeg=0.,leftBlinker=False,rightBlinker=False))


@pytest.mark.parametrize('fingerprint,setting,enabled,expected',[
  (LX3,1,False,1),(LX3,1,True,1),(LX3,2,False,1),
  (LX3,0,True,0),('KIA_EV9',1,True,0),
])
def test_real_formatter_packs_empty_oem_fallback_with_crc_and_preserves_other_fields(
    codec,fingerprint,setting,enabled,expected):
  packer,decode,checksum = codec
  cs = minimal_cs(); cs.ccnc_0x162.update(FAULT_LSS=1,FAULT_DAS=1,FF_DETECT=3,FF_DISTANCE=12.)
  scope = display_scope(); fn = scope['create_ccnc_messages']
  args = (NS(carFingerprint=fingerprint,flags=1),packer,NS(ECAN=0,CAM=2),100,
          NS(enabled=enabled,latActive=enabled),cs,NS(leadDistance=0.),0.,False,False,setting,False,0)
  before = fn(*args)
  # Reset only the packer's counters so byte comparison doesn't confuse sequence with payload.
  packer.counters.clear()
  after = fn(*args,surround_objects={'LF':(15.2,3.4),'RF':(21.3,3.1)})
  assert len(before)==len(after)==2
  for old,new in zip(before,after):
    assert old[0]==new[0] and old[2]==new[2]==0
    name = packer.dbc.addr_to_msg[new[0]].name
    original,values = decode(name,old[1]),decode(name,new[1])
    assert values['LF_DETECT']==values['RF_DETECT']==expected
    assert int.from_bytes(new[1][:2],'little')==checksum(new[0],None,bytearray(new[1]))
    allowed = {'CHECKSUM'} | ({s+k for s in ('LF','RF')
                              for k in ('_DETECT','_DETECT_DISTANCE','_DETECT_LATERAL')} if expected else set())
    assert all(values[k]==v for k,v in original.items() if k not in allowed)
    if expected:
      assert values['LF_DETECT_DISTANCE']==pytest.approx(15.2)
      assert values['LF_DETECT_LATERAL']==pytest.approx(3.4)
      assert values['RF_DETECT_DISTANCE']==pytest.approx(21.3)
      assert values['RF_DETECT_LATERAL']==pytest.approx(3.1)


def test_new_menu_uses_existing_native_switch_once_and_preserves_range_default():
  from openpilot.selfdrive.controls.lib.approach_comfort import DRIVING_PREFERENCES
  fn = load(ROOT/'selfdrive/carrot/server/services/settings.py',{'inject_driving_preferences'},
            {'DRIVING_PREFERENCES':DRIVING_PREFERENCES})['inject_driving_preferences']
  original = json.loads((ROOT/'selfdrive/carrot_settings.json').read_text(encoding='utf8'))
  data = fn(copy.deepcopy(original)); second = fn(copy.deepcopy(data))
  assert data == second
  old = next(p for p in original['params'] if p['name']=='EnableCornerRadar')
  new = next(p for p in data['params'] if p['name']=='EnableCornerRadar')
  assert all(new[k]==old[k] for k in ('min','max','default','unit'))
  assert not new.get('virtual',False)
  def entries(nodes):
    return [key for n in nodes for key in [*n.get('params',[]),*entries(n.get('groups',[]))]]
  assert entries(data['menu']).count('EnableCornerRadar')==1
  assert 'EnableCornerRadar' in entries(data['menu'][:1])
