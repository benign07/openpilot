"""The donor feature layer must not silently discard original haptics or S0."""
from pathlib import Path
import ast,importlib.util,json,subprocess,sys
import pytest
from types import SimpleNamespace as NS
import math,numpy as np

ROOT=Path(__file__).resolve().parents[3]
spec=importlib.util.spec_from_file_location('original_parity_profile',ROOT/'tools/prepare_original_lx3_feature_parity.py')
profile=importlib.util.module_from_spec(spec);spec.loader.exec_module(profile)

def test_verified_source_overlay_has_complete_registry_and_preserves_existing_paths(tmp_path):
  result=profile.prepare(tmp_path/'candidate')
  assert result['keys_before']==324 and result['keys_after']==333
  assert result['steering_MDPS_builder_AST_unchanged']
  assert result['schema_only_appended_optional_resume_boolean']
  assert not result['device_installed'] and not result['physical_or_road_qualified']
  assert not result['native_extension_included']
  assert not set(result['protected'])&set(result['after'])

@pytest.mark.parametrize('key',['CruiseStartCurrentSpeed','ApproachComfortEnabled','ApproachCoastStrength',
 'ApproachBrakeMargin','ApproachStopSmooth','ApproachTrafficCalm','ApproachFlexibleGap','ApproachFlexibleGapMin','C3xLiteBuzzerMute'])
def test_previously_unsupported_feature_key_is_in_actual_union(key):
  p,b,a=profile.sources();old=profile.declarations(b[profile.KEYS]);new=profile.declarations(a[profile.KEYS])
  assert key not in old and key in new

def test_controller_keeps_original_configurable_haptic_helper():
  p,b,a=profile.sources();source=a['opendbc_repo/opendbc/car/hyundai/carcontroller.py'].decode()
  assert 'self.haptic_notifications.configure_from_params(params)' in source
  helper='opendbc_repo/opendbc/car/hyundai/lx3_haptic_notifications.py'
  assert helper not in a and profile.sha(profile.committed(p['source_commit'],helper))==p['protected'][helper]
  for signal in ('self.haptic_notifications.update(', 'notification_vibrate=notification_vibrate'):
    assert signal in source

def test_actual_current_speed_reset_is_accepted_enable_edge_and_not_lfa_grant():
  _,before,after=profile.sources();name='selfdrive/car/cruise.py'
  old=before[name].decode();new=after[name].decode()
  assert 'CruiseStartCurrentSpeed' not in old
  assert "self._start_current_speed = self.params.get_bool('CruiseStartCurrentSpeed')" in new
  assert 'CC.enabled and not self.enabled_last' in new
  assert 'self.CP.openpilotLongitudinalControl' in new
  assert 'CS.canValid and not CS.canTimeout and start_speed_kph is not None' in new

def production_engage_block():
  # Execute the production assignment/guard/body and speed validation method,
  # excluding unrelated UI/native imports. This is a branch test, not a replay
  # of the complete engagement state machine or proof of road behaviour.
  _,_,after=profile.sources();tree=ast.parse(after['selfdrive/car/cruise.py'])
  helper=next(n for n in ast.walk(tree) if isinstance(n,ast.FunctionDef) and n.name=='_current_start_speed')
  helper.decorator_list=[]
  update=next(n for n in ast.walk(tree) if isinstance(n,ast.FunctionDef) and n.name=='update_v_cruise' and
              any(isinstance(s,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='start_speed_kph' for t in s.targets) for s in n.body))
  index=next(i for i,n in enumerate(update.body) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='start_speed_kph' for t in n.targets))
  ns={'math':math,'np':np,'CV':NS(MS_TO_KPH=3.6)}
  exec(compile(ast.fix_missing_locations(ast.Module(body=[helper],type_ignores=[])),'production_speed_helper','exec'),ns)
  body=compile(ast.fix_missing_locations(ast.Module(body=update.body[index:index+2],type_ignores=[])),'production_accepted_engage_block','exec')
  return ns,body

@pytest.mark.parametrize('cluster,measured,expected',[(113.,112.8975,113.),(0.,112.8975,113.),(float('nan'),112.8975,113.),(200.,112.8975,113.),(0.,0.,5.)])
def test_production_reset_replaces_previous_130_with_current_plausible_speed(cluster,measured,expected):
  ns,block=production_engage_block()
  state=NS(lx3_authority=True,_start_current_speed=True,CP=NS(openpilotLongitudinalControl=True,pcmCruise=False),
    enabled_last=False,v_cruise_kph=130.,v_cruise_cluster_kph=130.,_cruise_speed_min=5.,_cruise_speed_max=145.,_add_log=lambda _:None,
    _current_start_speed=ns['_current_start_speed'])
  ns.update(self=state,CS=NS(vEgo=measured/3.6,vEgoCluster=cluster/3.6,canValid=True,canTimeout=False),CC=NS(enabled=True))
  exec(block,ns)
  assert state.v_cruise_kph==expected and state.v_cruise_cluster_kph==expected
  assert state._pause_auto_speed_up and state._v_cruise_kph_at_brake==0

@pytest.mark.parametrize('change',['already_enabled','lfa_only','invalid_can','timeout','pcm','no_long','disabled_option','invalid_speed'])
def test_production_reset_never_resets_without_valid_new_long_engagement(change):
  ns,block=production_engage_block()
  state=NS(lx3_authority=True,_start_current_speed=True,CP=NS(openpilotLongitudinalControl=True,pcmCruise=False),
    enabled_last=False,v_cruise_kph=130.,v_cruise_cluster_kph=130.,_cruise_speed_min=5.,_cruise_speed_max=145.,_add_log=lambda _:None,
    _current_start_speed=ns['_current_start_speed'])
  cs=NS(vEgo=112.8975/3.6,vEgoCluster=113./3.6,canValid=True,canTimeout=False);cc=NS(enabled=True)
  if change=='already_enabled':state.enabled_last=True
  elif change=='lfa_only':cc.enabled=False
  elif change=='invalid_can':cs.canValid=False
  elif change=='timeout':cs.canTimeout=True
  elif change=='pcm':state.CP.pcmCruise=True
  elif change=='no_long':state.CP.openpilotLongitudinalControl=False
  elif change=='disabled_option':state._start_current_speed=False
  elif change=='invalid_speed':cs.vEgo=float('nan')
  ns.update(self=state,CS=cs,CC=cc);exec(block,ns)
  assert state.v_cruise_kph==130. and state.v_cruise_cluster_kph==130.

def test_registry_descriptor_mutation_fails_closed():
  p,b,a=profile.sources();changed=dict(a)
  changed[profile.KEYS]=changed[profile.KEYS].replace(b'{"DisableDM", {PERSISTENT, INT, "0"}}',b'{"DisableDM", {PERSISTENT, INT, "1"}}')
  assert changed[profile.KEYS]!=a[profile.KEYS]
  with pytest.raises(ValueError):profile.verify(p,b,changed)

def test_mdps_mutation_fails_closed():
  p,b,a=profile.sources();changed=dict(a);name='opendbc_repo/opendbc/car/hyundai/hyundaicanfd.py'
  changed[name]=changed[name].replace(b'values["STEERING_COL_TORQUE"] += 220',b'values["STEERING_COL_TORQUE"] += 221')
  assert changed[name]!=a[name]
  with pytest.raises(ValueError):profile.verify(p,b,changed)

def test_schema_nonappend_change_fails_closed():
  p,b,a=profile.sources();changed=dict(a)
  changed[profile.SCHEMA]=changed[profile.SCHEMA].replace(b'manualSteeringScale @19 :Float32 = 1',b'manualSteeringScale @19 :Float32 = 0')
  with pytest.raises(ValueError):profile.verify(p,b,changed)

def test_donor_surround_display_includes_its_interface_dependency():
  _,_,after=profile.sources()
  assert b'display_context=surround_display_context(' in after['selfdrive/car/card.py']
  assert b'*, display_context=None' in after['opendbc_repo/opendbc/car/interfaces.py']
  assert b'self.CS.lx3_surround_context = display_context' in after['opendbc_repo/opendbc/car/interfaces.py']

def test_wire_append_reads_with_old_schema_and_preserves_authority(tmp_path):
  pytest.importorskip('capnp')
  _,before,after=profile.sources()
  for name,raw in [('before',before[profile.SCHEMA]),('after',after[profile.SCHEMA])]:
    folder=tmp_path/name;folder.mkdir();(folder/'car.capnp').write_bytes(raw)
    include=folder/'include';include.mkdir()
    (include/'c++.capnp').write_bytes((ROOT/'opendbc_repo/opendbc/car/include/c++.capnp').read_bytes())
  data=tmp_path/'control.bin'
  writer="import capnp,sys; s=capnp.load(sys.argv[1]); m=s.CarControl.new_message(); m.enabled=True; m.latActive=True; m.longActive=False; m.manualSteeringScale=.75; m.manualSteeringResume=True; m.lx3Authority.epoch=42; m.lx3Authority.intent=1; open(sys.argv[2],'wb').write(m.to_bytes())"
  reader="import capnp,sys,json; s=capnp.load(sys.argv[1]); f=open(sys.argv[2],'rb'); m=s.CarControl.read(f); assert m.enabled and m.latActive and not m.longActive and m.manualSteeringScale==.75 and m.lx3Authority.epoch==42 and m.lx3Authority.intent==1; print('PASS old-schema reader retained all authority fields')"
  subprocess.run([sys.executable,'-c',writer,str(tmp_path/'after/car.capnp'),str(data)],check=True,capture_output=True)
  subprocess.run([sys.executable,'-c',reader,str(tmp_path/'before/car.capnp'),str(data)],check=True,capture_output=True)
