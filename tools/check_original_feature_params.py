"""Isolated ARM 324->333 Params build; no live device or firmware access."""
from pathlib import Path
import argparse,hashlib,importlib.util,json,platform,re,subprocess,sys,tempfile
from prepare_original_lx3_feature_parity import ROOT,KEYS,sources,verify,sha

OUT=ROOT/'original-feature-arm-evidence'
BASE=json.loads((ROOT/'tools/original_lx3_haptics/params_baseline.json').read_text())

def main():
  p=argparse.ArgumentParser(description=__doc__)
  p.add_argument('action',choices=['prepare-before','prepare-after','verify-before','verify-after'])
  args=p.parse_args();after=args.action.endswith('after');OUT.mkdir(exist_ok=True)
  profile,before,desired=sources();verify(profile,before,desired)
  for n,d in BASE['paired_sources'].items():
    if sha((ROOT/n).read_bytes())!=d:raise ValueError('Original ABI source changed: '+n)
  header=desired[KEYS] if after else before[KEYS]
  if args.action.startswith('prepare'):
    (ROOT/KEYS).write_bytes(header)
    print('Prepared original-compatible',333 if after else 324,'key registry');return
  assert platform.machine()=='aarch64' and sys.version_info[:2]==(3,12)
  assert (ROOT/KEYS).read_bytes()==header
  from openpilot.common.params import Params,UnknownKeyName
  with tempfile.TemporaryDirectory(prefix='original-feature-registry-') as temp:
    params=Params(temp);keys=sorted(k.decode() if isinstance(k,bytes) else k for k in params.all_keys())
    assert keys==profile['registered_keys_after' if after else 'registered_keys_before']
    attrs={k:{'type':int(params.get_type(k)),'default':params.get_default_value(k)} for k in keys}
    if not after:
      for k in profile['new_keys']:
        try:params.get(k)
        except UnknownKeyName:pass
        else:raise AssertionError('Original unexpectedly supports '+k)
      (OUT/'before-registry.json').write_text(json.dumps(attrs,indent=2))
    else:
      old=json.loads((OUT/'before-registry.json').read_text())
      assert {k:v for k,v in attrs.items() if k not in profile['new_keys']}==old
      assert params.get_bool('CruiseStartCurrentSpeed',return_default=True)
      assert not params.get_bool('ApproachComfortEnabled',return_default=True)
      params.put_bool('ApproachComfortEnabled',True);assert params.get_bool('ApproachComfortEnabled')
      params.put_int('ApproachTrafficCalm',30);assert params.get_int('ApproachTrafficCalm')==30
      params.put_int('ApproachFlexibleGapMin',70);assert params.get_int('ApproachFlexibleGapMin')==70
      params.put_bool('C3xLiteHapticEngage',False);assert not params.get_bool('C3xLiteHapticEngage')
      params.put_int('C3xLiteHapticPattern',2);assert params.get_int('C3xLiteHapticPattern')==2
      assert not any((Path(temp)/k).exists() for k in ('CalibrationParams','PandaSignatures','DongleId'))
  binary=ROOT/'common/params_pyx.so'
  read=lambda *args:subprocess.check_output(args,text=True)
  elf=read('readelf','-h',str(binary));assert 'AArch64' in elf
  dynamic=read('readelf','-d',str(binary));assert 'RPATH' not in dynamic and 'RUNPATH' not in dynamic
  ldd=read('ldd',str(binary));assert 'not found' not in ldd
  versions=read('readelf','--version-info',str(binary))
  tags=set(re.findall(r'GLIBC(?:XX)?_[0-9]+(?:\.[0-9]+)+|CXXABI_[0-9]+(?:\.[0-9]+)+|GCC_[0-9]+(?:\.[0-9]+)+',versions))
  assert tags<=set(BASE['required_symbol_versions'])
  phase='after' if after else 'before'
  result={'builder_commit':read('git','rev-parse','HEAD').strip(),'runtime_source_commit':profile['source_commit'],
    'original_324_attributes_preserved':after,'key_count':len(keys),'new_keys':profile['new_keys'] if after else [],
    'machine':'aarch64','python':sys.version,'module_sha256':sha(binary.read_bytes()),'module_bytes':binary.stat().st_size,
    'registry_header_sha256':sha(header),'required_symbol_versions':sorted(tags),'ABI_subset_of_original_backup':True,
    'phase':phase,'target_import_verified':False,'device_installed':False,'Panda_firmware_changed':False}
  (OUT/(phase+'-manifest.json')).write_text(json.dumps(result,indent=2)+'\n')
  (OUT/(phase+'-elf.txt')).write_text(elf+'\n'+dynamic+'\n'+ldd+'\n'+versions)
  print(json.dumps(result))

if __name__=='__main__':main()
