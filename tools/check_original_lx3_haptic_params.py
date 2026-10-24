"""Build-profile/registry evidence for original LX3; never touch device Params."""
import argparse
import importlib.util
import hashlib
import json
import platform
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
PROFILE=ROOT/'tools/original_lx3_haptics'
BASE=json.loads((PROFILE/'params_baseline.json').read_text())
OUT=ROOT/'original-haptic-arm-evidence'
KEY='C3xLiteHapticNotifications'
spec=importlib.util.spec_from_file_location('haptic_profile_options',ROOT/'opendbc_repo/opendbc/car/hyundai/lx3_haptic_notifications.py')
options=importlib.util.module_from_spec(spec);spec.loader.exec_module(options)
EXTRA_KEYS=[x[0] for x in options.HAPTIC_EVENT_OPTIONS]+[options.HAPTIC_PATTERN_KEY]
sha=lambda raw:hashlib.sha256(raw).hexdigest()


def header(after):
 raw=(PROFILE/'params_keys_80acfe79.h').read_bytes()
 assert sha(raw)==BASE['original_header_sha256']
 if after:
  raw=raw.replace(b'    {"HardwareC3xLite", {PERSISTENT, INT, "0"}},\n',
    b'    {"HardwareC3xLite", {PERSISTENT, INT, "0"}},\n'
    b'    {"C3xLiteHapticNotifications", {PERSISTENT, BOOL, "0"}},\n'+options.haptic_param_declarations().encode())
  assert raw.count(b'"C3xLiteHapticNotifications"')==1
 return raw


def main():
 parser=argparse.ArgumentParser(description=__doc__)
 parser.add_argument('action',choices=['prepare-before','prepare-after','verify-before','verify-after'])
 args=parser.parse_args();after=args.action.endswith('after');OUT.mkdir(exist_ok=True)
 for name,expected in BASE['paired_sources'].items():
  assert sha((ROOT/name).read_bytes())==expected,name
 if args.action.startswith('prepare'):
  (ROOT/'common/params_keys.h').write_bytes(header(after))
  print('Prepared original Params registry profile:', str(BASE['original_key_count']+1+len(EXTRA_KEYS))+' keys' if after else '316 keys')
  return
 assert platform.machine()=='aarch64' and sys.version_info[:2]==(3,12)
 assert (ROOT/'common/params_keys.h').read_bytes()==header(after)
 from openpilot.common.params import Params,ParamKeyFlag,ParamKeyType,UnknownKeyName
 with tempfile.TemporaryDirectory(prefix='original-haptic-params-') as temp:
  p=Params(temp)
  keys=sorted(k.decode() if isinstance(k,bytes) else k for k in p.all_keys())
  assert len(keys)==BASE['original_key_count']+(1+len(EXTRA_KEYS) if after else 0)
  descriptor={k:{'type':int(p.get_type(k)),'default':p.get_default_value(k)} for k in keys}
  if not after:
   assert KEY not in keys
   legacy_helper=options.HapticNotifications();legacy_helper.configure_from_params(p)
   assert legacy_helper.enabled_events==legacy_helper.ROUTINE and legacy_helper.pattern==0
   try:p.get_bool(KEY)
   except UnknownKeyName:pass
   else:raise AssertionError('Old native registry unexpectedly accepts new haptic key')
   (OUT/'before-registry.json').write_text(json.dumps(descriptor,indent=2,default=str))
  else:
   before=json.loads((OUT/'before-registry.json').read_text())
   assert {k:v for k,v in descriptor.items() if k not in [KEY]+EXTRA_KEYS}==before
   assert descriptor[KEY]=={'type':int(ParamKeyType.BOOL),'default':False}
   default_helper=options.HapticNotifications();default_helper.configure_from_params(p)
   assert default_helper.enabled_events==default_helper.ROUTINE and default_helper.pattern==0
   for key,default,_,_ in options.HAPTIC_EVENT_OPTIONS:
    assert descriptor[key]=={'type':int(ParamKeyType.BOOL),'default':default}
    assert p.get(key,return_default=True)==default
    p.put_bool(key,False);assert not p.get_bool(key)
   assert descriptor[options.HAPTIC_PATTERN_KEY]=={'type':int(ParamKeyType.INT),'default':0}
   p.put_int(options.HAPTIC_PATTERN_KEY,2);assert p.get_int(options.HAPTIC_PATTERN_KEY)==2
   selected_helper=options.HapticNotifications();selected_helper.configure_from_params(p)
   assert not selected_helper.enabled_events and selected_helper.pattern==2
   assert not p.get_bool(KEY)
   p.put_bool(KEY,True);assert p.get_bool(KEY)
   for flag in (ParamKeyFlag.CLEAR_ON_MANAGER_START,ParamKeyFlag.CLEAR_ON_ONROAD_TRANSITION,
                ParamKeyFlag.CLEAR_ON_OFFROAD_TRANSITION,ParamKeyFlag.CLEAR_ON_IGNITION_ON):
    p.clear_all(flag);assert p.get_bool(KEY)
    assert p.get_int(options.HAPTIC_PATTERN_KEY)==2
    assert all(not p.get_bool(key) for key,_,_,_ in options.HAPTIC_EVENT_OPTIONS)
   p.put_bool(KEY,False);assert not p.get_bool(KEY)
   assert not any(k.startswith('Calibration') and p.get(k) is not None for k in keys)
 binary=ROOT/'common/params_pyx.so'
 elf=subprocess.check_output(['readelf','-h',str(binary)],text=True);assert 'AArch64' in elf
 dynamic=subprocess.check_output(['readelf','-d',str(binary)],text=True)
 assert 'RPATH' not in dynamic and 'RUNPATH' not in dynamic
 ldd=subprocess.check_output(['ldd',str(binary)],text=True);assert 'not found' not in ldd
 versions=subprocess.check_output(['readelf','--version-info',str(binary)],text=True)
 tags=set(re.findall(r'GLIBC(?:XX)?_[0-9]+(?:\.[0-9]+)+|CXXABI_[0-9]+(?:\.[0-9]+)+|GCC_[0-9]+(?:\.[0-9]+)+',versions))
 assert tags<=set(BASE['required_symbol_versions']),sorted(tags-set(BASE['required_symbol_versions']))
 result={'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
  'original_profile':True,'phase':'after' if after else 'before','machine':platform.machine(),
  'python':sys.version,'key_count':len(keys),'original316_descriptors_preserved':after,
  'new_key_registered':after,'module_sha256':sha(binary.read_bytes()),'module_bytes':binary.stat().st_size,
  'registry_header_sha256':sha(header(after)),'required_symbol_versions':sorted(tags),
  'ABI_subset_of_original_backup':True,'live_target_import_verified':False,'motor_response_verified':False,
  'native_params_profile_only_not_Panda_or_controls':True,'paired_sources':BASE['paired_sources']}
 name='after' if after else 'before'
 (OUT/(name+'-elf.txt')).write_text(elf+'\n'+dynamic+'\n'+ldd+'\n'+versions)
 (OUT/(name+'-manifest.json')).write_text(json.dumps(result,indent=2))
 print(json.dumps(result))


if __name__=='__main__':main()
