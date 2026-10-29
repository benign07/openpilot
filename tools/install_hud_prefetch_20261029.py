"""Separate signed parked-only updater bootstrap. Never ordinary source OTA.

Adds only prefetch.py and replaces service.py while comma.service is stopped.
The hash-pinned existing gate/atomic helper is supplied with the signed package.
Core/boot, user configuration, profiles, receipts and existing releases stay intact.
"""
import argparse
import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from urllib.request import urlopen

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

PUBLIC_SHA='56d7f915ed4d802bccf46db83f0d7b147579466ed060abdf1dab96759babd223'
ORDER=('selfdrive/carrot/hud_update/prefetch.py','selfdrive/carrot/hud_update/service.py')
PROTECTED=('selfdrive/carrot/hud_update/core.py','selfdrive/carrot/hud_update/boot_apply.py',
           'selfdrive/carrot/server/services/setting_safety.py')
INACTIVE={'idle','complete','cancelled','rolled_back','failed','health_warning'}
sha=lambda b:hashlib.sha256(b).hexdigest()


def checked(root,name):
  if name not in ORDER+PROTECTED:raise ValueError('Not a bootstrap path')
  path=root/name
  if not path.resolve().is_relative_to(root.resolve()) or path.is_symlink():raise ValueError('Unsafe source path')
  return path


def digest(path):return sha(path.read_bytes()) if path.is_file() else None


def require_vehicle():
  from openpilot.common.params import Params
  from cereal import car
  raw=Params().get('CarParams')
  if not raw:raise ValueError('Missing vehicle identity')
  with car.CarParams.from_bytes(raw) as cp:
    if cp.carFingerprint!='HYUNDAI_PALISADE_LX3_HEV':raise ValueError('Not LX3 HEV')


def updater_stopped(proc=Path('/proc')):
  for entry in proc.iterdir():
    if not entry.name.isdigit() or int(entry.name)==os.getpid():continue
    try:
      argv=(entry/'cmdline').read_bytes().split(b'\0')
      if any(arg.endswith(b'/carrot_server.py') or arg in (b'selfdrive.carrot.carrot_server',b'carrot_server') for arg in argv):
        raise ValueError('carrot_server still running')
    except (FileNotFoundError,ProcessLookupError,PermissionError):pass


def verify(folder,root,state_root):
  config_path=state_root/'config.json'
  public=base64.b64decode(json.loads(config_path.read_bytes())['public_key'],validate=True)
  if sha(public)!=PUBLIC_SHA:raise ValueError('Unrecognized installed release public key')
  envelope=json.loads((folder/'bootstrap.signed.json').read_bytes())
  raw=base64.b64decode(envelope['payload'],validate=True)
  Ed25519PublicKey.from_public_bytes(public).verify(base64.b64decode(envelope['signature'],validate=True),raw)
  manifest=json.loads(raw)
  if (manifest.get('schema')!=1 or manifest.get('kind')!='HudPrefetchBootstrap' or
      manifest.get('installer_sha256')!=sha(Path(__file__).read_bytes()) or
      list(manifest.get('files',{}))!=list(ORDER) or set(manifest.get('protected',{}))!=set(PROTECTED) or
      manifest.get('automatic_enabled') is not False):raise ValueError('Bootstrap scope mismatch')
  helper=folder/'parked_atomic.py'
  if sha(helper.read_bytes())!=manifest['helper_sha256']:raise ValueError('Gate helper changed')
  spec=importlib.util.spec_from_file_location('prefetch_parked_atomic',helper)
  core=importlib.util.module_from_spec(spec);spec.loader.exec_module(core)
  for name,row in manifest['files'].items():
    blob=base64.b64decode(row['data'],validate=True)
    if len(blob)!=row['bytes'] or sha(blob)!=row['sha256']:raise ValueError('Bootstrap source damaged')
    compile(blob,name,'exec')
    checked(root,name)
  for name,value in manifest['protected'].items():
    if digest(checked(root,name))!=value:raise ValueError('Protected source differs: '+name)
  return manifest,core,sha(raw)


def originals_match(root,manifest):
  for name,row in manifest['files'].items():
    if digest(checked(root,name))!=row['before']:raise ValueError('Source differs: '+name)


def idle_state(state_root):
  state=json.loads((state_root/'state.json').read_bytes()) if (state_root/'state.json').exists() else {'phase':'idle'}
  if state.get('phase') not in INACTIVE:raise ValueError('Finish/cancel the current HUD update first')


def stage(folder,root,state_root,manifest,core,bundle):
  idle_state(state_root);originals_match(root,manifest)
  if (folder/'transaction.json').exists():raise ValueError('Existing bootstrap transaction; inspect or restore it')
  record={'schema':1,'phase':'staged','bundle_sha256':bundle,'root':str(root.resolve()),
          'state_root':str(state_root.resolve()),'files':{},'automatic_before':None}
  for name,row in manifest['files'].items():
    target=checked(root,name)
    record['files'][name]={'before':row['before'],'sha256':row['sha256'],'mode':(target.stat().st_mode&0o777) if target.exists() else 0o644}
    if row['before'] is not None:core.atomic(folder/'original'/name,target.read_bytes())
    core.atomic(folder/'new'/name,base64.b64decode(row['data'],validate=True))
  policy=state_root/'automatic.json'
  if policy.exists():
    record['automatic_before']=sha(policy.read_bytes());core.atomic(folder/'automatic.original.json',policy.read_bytes())
  core.save(folder/'transaction.json',record)
  return record


def smoke(folder,root,manifest):
  smoke_root=folder/'smoke'
  package=smoke_root/'selfdrive/carrot/hud_update';package.mkdir(parents=True,exist_ok=True)
  for directory in (smoke_root/'selfdrive',smoke_root/'selfdrive/carrot',package):
    (directory/'__init__.py').write_bytes(b'')
  for name in ('core.py','service.py','prefetch.py'):
    relative='selfdrive/carrot/hud_update/'+name
    source=(folder/'new'/relative) if relative in manifest['files'] else checked(root,relative)
    (package/name).write_bytes(source.read_bytes())
  subprocess.run([sys.executable,'-c','from selfdrive.carrot.hud_update import service,prefetch; assert callable(service.UpdateService.prefetch_tick)'],
                 cwd=smoke_root,env={**os.environ,'PYTHONPATH':str(smoke_root)},check=True,timeout=30)


def restored(folder,root,state_root,record,core):
  # Restore importer first, then remove its optional dependency if it was absent.
  for name in reversed(ORDER):
    row=record['files'][name];target=checked(root,name)
    if digest(target) not in (row['before'],row['sha256']):raise ValueError('Foreign source during recovery')
    if row['before'] is None:
      if target.exists():target.unlink()
    else:
      raw=(folder/'original'/name).read_bytes()
      if sha(raw)!=row['before']:raise ValueError('Damaged bootstrap backup')
      core.atomic(target,raw,row['mode'])
  policy=state_root/'automatic.json'
  if record['automatic_before'] is None:
    if policy.exists():policy.unlink()
  else:
    raw=(folder/'automatic.original.json').read_bytes()
    if sha(raw)!=record['automatic_before']:raise ValueError('Damaged policy backup')
    core.atomic(policy,raw)


def start_service(folder,record,core,original=None):
  try:subprocess.run(['sudo','-n','systemctl','start','comma.service'],check=True,timeout=10)
  except BaseException as exc:
    record.update(phase='recovery_required',service_start_error=type(exc).__name__,
                  original_error=type(original).__name__+': '+str(original) if original else None)
    core.save(folder/'transaction.json',record)
    raise RuntimeError('comma.service start failed; original='+str(record['original_error'])) from exc


def verify_service(expected_sha,deadline_seconds=30):
  deadline=time.monotonic()+deadline_seconds;since=None;pid=None
  url='http://127.0.0.1:7000/api/hud_update/local_health'
  while time.monotonic()<deadline:
    try:
      with urlopen(url,timeout=1) as response:
        if response.geturl()!=url:raise ValueError('Unexpected health redirect')
        chunks=[];size=0
        while True:
          if time.monotonic()>=deadline:raise ValueError('Health read deadline')
          chunk=response.read1(min(4097-size,1024))
          if not chunk:break
          chunks.append(chunk);size+=len(chunk)
          if size>4096:raise ValueError('Health response too large')
        raw=b''.join(chunks)
      if len(raw)>4096:raise ValueError('Health response too large')
      row=json.loads(raw)
      good=(row.get('ok') is True and row.get('configured') is True and row.get('policy_valid') is True and
            row.get('prefetch_version')==1 and row.get('service_sha256')==expected_sha and
            type(row.get('automatic_enabled')) is bool and type(row.get('process_id')) is int and row['process_id']>0)
      if good:
        if pid!=row['process_id']:pid=row['process_id'];since=time.monotonic()
        if time.monotonic()-since>=3.:return row
      else:since=pid=None
    except Exception:since=pid=None
    time.sleep(.5)
  raise RuntimeError('New carrot_server local health not stable')


def finish_service(folder,root,state_root,manifest,core,bundle):
  record=json.loads((folder/'transaction.json').read_bytes())
  if record.get('phase') not in ('installed_pending_service_verification','activation_pending_restart','complete'):
    raise ValueError('Bootstrap not awaiting service verification')
  try:
    health=verify_service(manifest['files'][ORDER[1]]['sha256'])
  except Exception as original:
    if record.get('phase') in ('complete','activation_pending_restart'):
      raise RuntimeError('Previously verified service differs; no automatic restore attempted') from original
    record.update(service_verification_error=type(original).__name__+': '+str(original))
    core.save(folder/'transaction.json',record)
    try:
      # The manager is running again: reacquire the full fresh parked/Panda
      # gate before stopping it and restoring, never reuse the old lease.
      restored_record=transaction(folder,root,state_root,manifest,core,bundle,True)
      restored_record['phase']='restored_after_service_failure'
      restored_record['service_verification_error']=record['service_verification_error']
      core.save(folder/'transaction.json',restored_record)
    except Exception as recovery:
      record.update(phase='recovery_required',recovery_error=type(recovery).__name__+': '+str(recovery))
      core.save(folder/'transaction.json',record)
      raise RuntimeError('Service failed; parked recovery also failed: '+str(recovery)) from original
    raise RuntimeError('Service failed; original updater restored under fresh parked gate') from original
  record.update(phase='complete',service_verified=True,service_process_id=health['process_id'],
                automatic_active=health['automatic_enabled'])
  core.save(folder/'transaction.json',record)
  return record


def activate(folder,root,state_root,manifest,core):
  record=json.loads((folder/'transaction.json').read_bytes())
  if record.get('phase')!='complete' or record.get('service_verified') is not True:
    raise ValueError('Verify the installed service before activation')
  health=verify_service(manifest['files'][ORDER[1]]['sha256'])
  if health['automatic_enabled']:
    record['automatic_active']=True;core.save(folder/'transaction.json',record);return record
  idle_state(state_root)
  gate=core.parked()
  require_vehicle()
  idle_state(state_root)
  for name,row in manifest['files'].items():
    if digest(checked(root,name))!=row['sha256']:raise ValueError('Installed service changed before activation')
  policy=json.loads((state_root/'automatic.json').read_bytes())
  if not isinstance(policy,dict) or policy.get('schema')!=1:raise ValueError('Review automatic policy')
  previous_policy=dict(policy)
  policy['enabled']=True;core.save(state_root/'automatic.json',policy)
  record.update(phase='activation_pending_restart',activation_at=time.monotonic(),automatic_active=False)
  core.save(folder/'transaction.json',record)
  # No source change: restart once to load the opt-in only AFTER verified
  # startup. This keeps failed bootstrap recovery free of automatic queues.
  try:
    if time.monotonic()-gate['mono']>=1:raise ValueError('Fresh parked restart required')
    subprocess.run(['sudo','-n','reboot'],check=True,timeout=5)
  except BaseException as exc:
    core.save(state_root/'automatic.json',previous_policy)
    record.update(phase='complete',automatic_active=False,activation_error=type(exc).__name__)
    core.save(folder/'transaction.json',record)
    raise
  return record


def transaction(folder,root,state_root,manifest,core,bundle,restore=False):
  record=json.loads((folder/'transaction.json').read_bytes())
  if (record.get('bundle_sha256')!=bundle or record.get('root')!=str(root.resolve()) or
      record.get('state_root')!=str(state_root.resolve()) or set(record.get('files',{}))!=set(ORDER)):
    raise ValueError('Staging identity mismatch')
  for name,row in manifest['files'].items():
    saved=record['files'][name]
    if (saved.get('before')!=row['before'] or saved.get('sha256')!=row['sha256'] or
        type(saved.get('mode')) is not int or not 0<=saved['mode']<=0o777):
      raise ValueError('Staging file record mismatch')
  if not restore:
    if record.get('phase')!='staged':raise ValueError('Not staged')
    originals_match(root,manifest);smoke(folder,root,manifest)
    if digest(state_root/'automatic.json')!=record['automatic_before']:raise ValueError('Policy changed since staging')
    for name,row in manifest['files'].items():
      if digest(folder/'new'/name)!=row['sha256']:raise ValueError('Staged bytes changed')
  idle_state(state_root)
  # The qualified helper obtains10 continuous seconds of fresh P/zero/inactive
  # host and Panda state, never an old saved approval or a CLI bypass flag.
  gate=core.parked()
  require_vehicle()
  record.update(phase='stopping',parked_gate=gate);core.save(folder/'transaction.json',record)
  changed=False
  try:
    subprocess.run(['sudo','-n','systemctl','stop','comma.service'],check=True,timeout=10)
    core.stopped();updater_stopped();idle_state(state_root)
    if time.monotonic()-gate['mono']>=15:raise ValueError('Expired parked transaction window')
    preserved={n:digest(state_root/n) for n in ('config.json','state.json','installed.json','history.json')}
    for name,value in manifest['protected'].items():
      if digest(checked(root,name))!=value:raise ValueError('Protected file changed')
    if restore:
      changed=True;restored(folder,root,state_root,record,core);record['phase']='restored'
    else:
      originals_match(root,manifest)
      record['phase']='applying';core.save(folder/'transaction.json',record);changed=True
      for name in ORDER:
        core.atomic(checked(root,name),(folder/'new'/name).read_bytes(),record['files'][name]['mode'])
      for name,row in manifest['files'].items():
        if digest(checked(root,name))!=row['sha256']:raise ValueError('Installed bootstrap mismatch')
      policy=json.loads((state_root/'automatic.json').read_bytes()) if (state_root/'automatic.json').exists() else {}
      if not isinstance(policy,dict):raise ValueError('Existing automatic policy needs review')
      policy.update(schema=1,enabled=manifest['automatic_enabled'])
      core.save(state_root/'automatic.json',policy)
      record['phase']='installed_pending_service_verification'
    if time.monotonic()-gate['mono']>=15:raise ValueError('Stopped transaction exceeded15seconds')
    if preserved!={n:digest(state_root/n) for n in preserved}:raise ValueError('Existing updater records changed')
    core.save(folder/'transaction.json',record)
  except BaseException as original:
    if changed:
      try:
        restored(folder,root,state_root,record,core);record['phase']='restored_after_failure';core.save(folder/'transaction.json',record)
      except BaseException:
        record['phase']='recovery_required';core.save(folder/'transaction.json',record);raise
    start_service(folder,record,core,original)
    raise
  start_service(folder,record,core)
  return record


def main():
  parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('phase',choices=('verify','stage','install','restore','verify-service','activate'))
  parser.add_argument('--folder',type=Path,required=True);parser.add_argument('--root',type=Path,default=Path('/data/openpilot'))
  parser.add_argument('--state-root',type=Path,default=Path('/data/community/hud_updates'))
  a=parser.parse_args();manifest,core,bundle=verify(a.folder,a.root,a.state_root)
  if a.phase=='verify':originals_match(a.root,manifest);result={'verified':True,'installed':False}
  elif a.phase=='stage':result=stage(a.folder,a.root,a.state_root,manifest,core,bundle)
  elif a.phase=='verify-service':result=finish_service(a.folder,a.root,a.state_root,manifest,core,bundle)
  elif a.phase=='activate':result=activate(a.folder,a.root,a.state_root,manifest,core)
  else:
    result=transaction(a.folder,a.root,a.state_root,manifest,core,bundle,a.phase=='restore')
    if a.phase=='install':result=finish_service(a.folder,a.root,a.state_root,manifest,core,bundle)
  print(json.dumps(result,ensure_ascii=True))


if __name__=='__main__':main()
