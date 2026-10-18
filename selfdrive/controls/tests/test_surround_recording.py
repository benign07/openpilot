"""Real display helpers and bounded diagnostic storage; no CAN/control I/O."""
import ast
import gzip
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

from openpilot.selfdrive.carrot.can_diagnostics.automatic import AutoRecorder, ChunkStore

ROOT=Path(__file__).resolve().parents[3]

def formatter():
  path=ROOT/'opendbc_repo/opendbc/car/hyundai/hyundaicanfd.py'
  names={'_clip_int','_apply_lane_desire','_apply_radar_blink','_make_ccnc_values'}
  scope={}
  exec(compile(ast.Module(body=[n for n in ast.parse(path.read_text(encoding='utf8')).body
    if isinstance(n,ast.FunctionDef) and n.name in names],type_ignores=[]),str(path),'exec'),scope)
  return scope['_make_ccnc_values']

@pytest.mark.parametrize('typ',range(32))
def test_ccnc_preserves_every_oem_corner_class_and_geometry(typ):
  values={key:value for side in ('LF','RF','LR','RR') for key,value in
    ((side+'_DETECT',typ),(side+'_DETECT_DISTANCE',20.),(side+'_DETECT_LATERAL',3.2))}
  original=values.copy()
  formatter()(values,NS(),True,100,NS(),lane_line=False,preserve_corner_types=True,
              blink_pairs=[('LR_DETECT','LR_DETECT_DISTANCE'),('RR_DETECT','RR_DETECT_DISTANCE')])
  assert values==original

def test_empty_oem_slots_are_not_invented_and_legacy_format_stays_legacy():
  values={key:0 for side in ('LF','RF','LR','RR') for key in (side+'_DETECT',side+'_DETECT_DISTANCE')}
  expected=values.copy();formatter()(values,NS(),False,0,NS(),lane_line=False,preserve_corner_types=True)
  assert values==expected
  for side in ('LF','RF','LR','RR'):
    values[side+'_DETECT']=4;values[side+'_DETECT_DISTANCE']=10.
  formatter()(values,NS(),False,0,NS(),lane_line=False)
  assert all(values[s+'_DETECT']==1 for s in ('LF','RF','LR','RR'))

def test_real_caller_uses_ccnc_flag_only_for_lx3_162_not_1ea():
  tree=ast.parse((ROOT/'opendbc_repo/opendbc/car/hyundai/hyundaicanfd.py').read_text(encoding='utf8'))
  fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='create_ccnc_messages')
  for attr,expected in (('ccnc_0x162',True),('adrv_0x1ea',False)):
    branch=next(n for n in ast.walk(fn) if isinstance(n,ast.If) and
      isinstance(n.test,ast.Compare) and isinstance(n.test.left,ast.Attribute) and n.test.left.attr==attr)
    call=next(n for n in ast.walk(branch) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='_make_ccnc_values')
    arg=next((k.value for k in call.keywords if k.arg=='preserve_corner_types'),None)
    if expected:
      for fingerprint,wanted in (('HYUNDAI_PALISADE_LX3_HEV',True),('KIA_EV9',False)):
        assert eval(compile(ast.Expression(arg),'actual-call-flag','eval'),{'CP':NS(carFingerprint=fingerprint)}) is wanted
    else:assert arg is None

def sealed(root,index):
  # Bounded but incompressible data gives pressure without giant fixtures.
  store=ChunkStore(root,reserve=0,chunk_bytes=1024)
  assert store.begin({'route':'r'},float(index))
  store.append({'kind':'sample','padding':os.urandom(700).hex()},float(index))
  store.seal('test')
  path=next(p for p in root.glob('*.manifest.json') if p.name not in sealed.seen)
  sealed.seen.add(path.name)
  os.utime(path,ns=(index*1_000_000_000,index*1_000_000_000))
  return json.loads(path.read_text())
sealed.seen=set()

def archive(tmp_path,count=8):
  sealed.seen=set()
  return [sealed(tmp_path,i+1) for i in range(count)]

def test_explicit_rolling_makes_room_oldest_first_and_preserves_newest(tmp_path):
  rows=archive(tmp_path)
  store=ChunkStore(tmp_path,reserve=0,chunk_bytes=1024,rolling=True)
  store.quota=store.usage+1024+65536-1000
  assert store.begin({'route':'new'},100.)
  assert store.retired_chunks>0
  assert not (tmp_path/(rows[0]['id']+'.manifest.json')).exists()
  for row in rows[-2:]:assert (tmp_path/(row['id']+'.jsonl.gz')).exists()
  assert store.stream is not None

def test_legacy_preserve_policy_and_corrupt_or_open_records_are_not_deleted(tmp_path):
  rows=archive(tmp_path)
  bad=tmp_path/(rows[0]['id']+'.jsonl.gz');bad.write_bytes(b'preserve evidence')
  (tmp_path/'recovery_quarantine').mkdir();protected=tmp_path/'recovery_quarantine'/'original';protected.write_bytes(b'bad original')
  store=ChunkStore(tmp_path,reserve=0,chunk_bytes=1024)
  partial=tmp_path/'active.partial';partial.write_bytes(b'{"kind":"header","schema":1}\n')
  store.quota=store.usage
  before={p.name for p in tmp_path.glob('*.jsonl.gz')}
  assert not store.begin({},100.)
  assert before=={p.name for p in tmp_path.glob('*.jsonl.gz')}
  store.rolling=True;store.quota+=1024+65536-1000
  assert store.begin({},101.)
  assert bad.read_bytes()==b'preserve evidence'
  assert protected.read_bytes()==b'bad original'
  assert partial.read_bytes()==b'{"kind":"header","schema":1}\n'
  assert store.retention_skipped>=1


def test_many_corrupt_old_records_do_not_permanently_starve_valid_retention(tmp_path):
  rows=archive(tmp_path,70)
  for row in rows[:65]:
    (tmp_path/(row['id']+'.jsonl.gz')).write_bytes(b'preserve corrupt original')
  store=ChunkStore(tmp_path,reserve=0,chunk_bytes=1024,rolling=True)
  store.quota=store.usage+1024+65536-1000
  # Each scan is bounded; later scans must advance past unverified old files.
  for now in range(100,401,31):
    if store.begin({},float(now)):break
  assert store.stream is not None
  assert store.retention_skipped>=65 and store.retired_chunks>0
  for row in rows[:65]:assert (tmp_path/(row['id']+'.jsonl.gz')).read_bytes()==b'preserve corrupt original'
  for row in rows[-2:]:assert (tmp_path/(row['id']+'.jsonl.gz')).exists()


def test_failed_delete_restores_advertised_manifest(tmp_path,monkeypatch):
  rows=archive(tmp_path,3)
  path=tmp_path/(rows[0]['id']+'.jsonl.gz');unlink=Path.unlink
  def fail_data_delete(self,*args,**kwargs):
    if self==path:raise PermissionError('open download')
    return unlink(self,*args,**kwargs)
  store=ChunkStore(tmp_path,reserve=0,chunk_bytes=1024,rolling=True)
  store.quota=store.usage
  monkeypatch.setattr(Path,'unlink',fail_data_delete)
  assert not store.begin({},100.)
  assert (tmp_path/(rows[0]['id']+'.manifest.json')).exists() and path.exists()
  assert not list(tmp_path.glob('*.retiring'))


def test_matching_manifest_with_corrupt_gzip_tail_is_preserved(tmp_path):
  rows=archive(tmp_path,3)
  row=rows[0];path=tmp_path/(row['id']+'.jsonl.gz')
  bad=bytearray(path.read_bytes());bad[-8]^=0xff;path.write_bytes(bad)
  row['sha256']=hashlib.sha256(bad).hexdigest()
  manifest=tmp_path/(row['id']+'.manifest.json');manifest.write_text(json.dumps(row))
  os.utime(manifest,ns=(1_000_000_000,)*2)
  store=ChunkStore(tmp_path,reserve=0,chunk_bytes=1024,rolling=True);store.quota=store.usage
  assert not store.begin({},100.)
  assert path.read_bytes()==bad and store.retention_skipped==1

def test_retirement_crash_recovery_restores_surviving_data(tmp_path):
  rows=archive(tmp_path,3)
  p=tmp_path/(rows[0]['id']+'.manifest.json');p.replace(p.with_suffix('.retiring'))
  store=ChunkStore(tmp_path,reserve=0)
  assert len(store.list_chunks())==3
  assert not list(tmp_path.glob('*.retiring'))

def test_lateral_evidence_uses_existing_event_and_rejects_stale_rows(tmp_path):
  store=ChunkStore(tmp_path,reserve=0)
  recorder=AutoRecorder(store,{'car_fingerprint':'HYUNDAI_PALISADE_LX3_HEV','boot_id':'b'})
  recorder.state='recording';recorder.recording_route='route';recorder.route_observed_ns=99_000_000_000
  assert store.begin({},100.)
  row={'schema':1,'mono_ns':100_000_000_000,'reason':'corridor_unqualified','applied':0.}
  recorder.lateral_evidence(row,100_001_000_000,100.)
  recorder.lateral_evidence(row,100_001_000_000,100.1)
  recorder.lateral_evidence({**row,'mono_ns':90_000_000_000},100_001_000_000,100.6)
  store.seal('test')
  records=[json.loads(x) for x in gzip.decompress(next(tmp_path.glob('*.jsonl.gz')).read_bytes()).decode().splitlines()]
  evidence=[r for r in records if r.get('name')=='lateral_margin_evidence']
  assert len(evidence)==1
  assert evidence[0]['kind']=='event' and evidence[0]['route']=='route'
  assert evidence[0]['evidence']==row
