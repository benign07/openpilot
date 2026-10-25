"""Compose the verified donor feature layer over original S0. Never install.

The donor source, original before bytes, original haptics and D1/S0 diagnostics
are independent inputs. This is a separate schema/Params deployment, NOT a HUD
source OTA. A generated directory alone is not deployment qualification.
"""
from pathlib import Path
import argparse,ast,gzip,hashlib,io,json,re,subprocess,tarfile

ROOT=Path(__file__).resolve().parents[1]
PROFILE=ROOT/'tools/original_lx3_feature_parity'
KEYS='common/params_keys.h'
SCHEMA='opendbc_repo/opendbc/car/car.capnp'

def sha(data):return hashlib.sha256(data).hexdigest()

def committed(commit,name):
  return subprocess.check_output(['git','show',commit+':'+name],cwd=ROOT)

def declarations(raw):
  result={}
  for name,body in re.findall(rb'^[ \t]*\{"([^"\n]+)",\s*\{([^{}\n]+)\}[ \t]*\},?[ \t]*(?://[^\n]*)?$',raw,re.M):
    key=name.decode()
    if key in result:raise ValueError('Duplicate Params declaration: '+key)
    result[key]=re.sub(rb'\s+',b'',body).decode()
  return result

def sources():
  profile=json.loads((PROFILE/'profile.json').read_text())
  blob=(PROFILE/'before_sources.tar.gz').read_bytes()
  if sha(blob)!=profile['source_archive_sha256']:raise ValueError('Original source archive hash')
  before={}
  with tarfile.open(fileobj=io.BytesIO(blob)) as tar:
    for item in tar:
      name=item.name
      if not item.isfile() or name not in profile['files'] or '..' in Path(name).parts or name in before:
        raise ValueError('Unsafe original source entry')
      raw=tar.extractfile(item).read()
      if sha(raw)!=profile['files'][name]['before_sha256']:raise ValueError('Original source hash: '+name)
      before[name]=raw
  if set(before)!={n for n,r in profile['files'].items() if r['before_sha256'] is not None}:
    raise ValueError('Incomplete original source archive')
  after={}
  for name,row in profile['files'].items():
    original_name='cereal/car.capnp' if name==SCHEMA else name
    raw=committed(profile['source_commit'],original_name)
    if sha(raw)!=row['after_sha256']:raise ValueError('Feature source mismatch: '+name)
    after[name]=raw
  return profile,before,after

def verify(profile,before,after):
  old,new=declarations(before[KEYS]),declarations(after[KEYS])
  if set(old)!=set(profile['registered_keys_before']) or set(new)!=set(profile['registered_keys_after']):
    raise ValueError('Params registry key set')
  if len(old)!=324 or len(new)!=333 or any(new[k]!=v for k,v in old.items()):
    raise ValueError('Existing Params attributes changed')
  if sorted(set(new)-set(old))!=profile['new_keys']:raise ValueError('Unexpected new keys')
  # Only append @20, preserving all prior wire fields and their defaults.
  schema=after[SCHEMA].decode()
  reduced=schema.replace('  # Smooth actuator handback only; never grants lateral control permission.\n','').replace('  manualSteeringResume @20 :Bool;\n','')
  if reduced!=before[SCHEMA].decode():raise ValueError('Schema change beyond appended resume boolean')
  factory='opendbc_repo/opendbc/car/hyundai/hyundaicanfd.py'
  tree=lambda raw:{f.name:ast.dump(f,include_attributes=False) for f in ast.parse(raw).body if isinstance(f,(ast.FunctionDef,ast.ClassDef))}
  a,b=tree(before[factory]),tree(after[factory])
  for name in ('create_steering_messages_camera_scc','create_steering_messages','create_adrv_messages'):
    if a[name]!=b[name]:raise ValueError('Protected steering/MDPS builder changed: '+name)
  for name,raw in after.items():
    if name.endswith('.py'):compile(raw,name,'exec')
    if name.endswith('.json'):json.loads(raw)
  forbidden={'selfdrive/pandad/pandad','panda/board/obj/panda_h7.bin.signed',
    'selfdrive/carrot/can_diagnostics/automatic.py','selfdrive/carrot/can_diagnostics/automatic_runtime.py',
    'opendbc_repo/opendbc/car/hyundai/lx3_haptic_notifications.py'}
  if forbidden&set(after):raise ValueError('Protected native/diagnostic/haptic replacement')
  return {'keys_before':len(old),'keys_after':len(new),'preserved_key_descriptors':True,
    'steering_MDPS_builder_AST_unchanged':True,'schema_only_appended_optional_resume_boolean':True,
    'Panda_firmware_replaced':False,'pandad_S0_replaced':False,'D1_diagnostics_replaced':False,
    'original_haptic_helper_replaced':False,'source_files':len(after),'device_installed':False}

def prepare(out):
  profile,before,after=sources();evidence=verify(profile,before,after)
  out.mkdir(parents=True,exist_ok=False)
  for name,raw in after.items():
    p=out/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(raw)
  record={**evidence,'builder_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
    'runtime_source_commit':profile['source_commit'],'before_release':profile['before_release'],
    'target_serial':profile['target_serial'],'source_donor_serial':profile['donor_serial'],
    'before':{n:r['before_sha256'] for n,r in profile['files'].items()},
    'after':{n:sha(raw) for n,raw in after.items()},'protected':profile['protected'],
    'native_Params_build_required':True,'native_extension_included':False,
    'all_donor_settings_applied':False,'physical_or_road_qualified':False,
    'limits':['No device access or runtime mutations','Retain target calibration, identity and camera/model assets',
      'Registry extension requires original ARM/Python ABI verification','Append-only schema needs old/new serialization checks',
      'Separate signed parked deployment; unsupported by shared HUD source OTA']}
  (out/'FEATURE_PROFILE.json').write_text(json.dumps(record,indent=2)+'\n')
  return record

if __name__=='__main__':
  p=argparse.ArgumentParser(description=__doc__);p.add_argument('--out',type=Path,required=True)
  a=p.parse_args();print(json.dumps(prepare(a.out)))
