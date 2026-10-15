"""Diagnostic failures and comparisons cannot change the control calculation."""
import ast
import copy
import gzip
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace as NS

import numpy as np
import pytest

from openpilot.selfdrive.controls.lib.approach_comfort import (
  ApproachComfort, ApproachSettings, LongitudinalEvidence, traffic_ceiling_comparison)
from openpilot.selfdrive.controls.tests.test_flexible_gap_mpc import production_mpc, update
from tools.lx3_drive_quality import frame_integrity, match_video, render, summarize_frames


def frame(seq=1, **changes):
  t=seq*50_000_000
  return {'schema':1,'producer':'a'*32,'sequence':seq,'cycle':seq,'mono_ns':t,'settings_generation':1,
          'settings':{'traffic':30},'actual_arm':30,'bounds':{'after_approach':1.105,'mpc_max':1.105},
          'comparison':{'30':{'applied_ceiling':1.105},'40':{'applied_ceiling':1.04}},
          'services':{k:{'mono_ns':t,'valid':True,'alive':True} for k in (
            'carState','carControl','controlsState','radarState','modelV2','selfdriveState')},**changes}


@pytest.mark.parametrize('traffic',[0,30,40,100])
def test_comparison_reads_filters_without_advancing_or_changing_settings(traffic):
  p=ApproachComfort();p.coast_weight=.02;p.traffic_weight=1.
  before=copy.deepcopy(vars(p));s=ApproachSettings(True,40,40,40,traffic)
  pair=traffic_ceiling_comparison(p,s,1.3,0.)
  assert pair['30']['applied_ceiling']==pytest.approx(1.105)
  assert pair['40']['applied_ceiling']==pytest.approx(1.04)
  assert vars(p)==before and s.traffic==traffic


def test_stock_slew_or_coast_can_remove_30_40_difference():
  p=ApproachComfort();p.traffic_weight=1.;p.coast_weight=1.
  pair=traffic_ceiling_comparison(p,ApproachSettings(True,40,40,40,30),1.3,0.)
  assert pair['30']['applied_ceiling']==pair['40']['applied_ceiling']
  p.coast_weight=0.
  pair=traffic_ceiling_comparison(p,ApproachSettings(True),1.3,1.3)
  assert pair['30']['applied_ceiling']==pair['40']['applied_ceiling']==pytest.approx(1.28)


def test_disabled_logger_never_builds_and_failures_are_evidence_only():
  logger=LongitudinalEvidence(False)
  assert not any(logger.due(True) for _ in range(20))
  logger.emit(lambda: (_ for _ in ()).throw(AssertionError()),lambda _:None)
  assert logger.errors==0
  logger.enabled=True
  logger.emit(lambda: {'settings':{},'bad':float('nan')},lambda _:None)
  logger.emit(lambda: {'settings':{},'huge':'x'*32768},lambda _:None)
  logger.emit(lambda: {'settings':{}},lambda _: (_ for _ in ()).throw(OSError()))
  assert logger.errors==3
  rows=[];logger.emit(lambda: {'settings':{}},rows.append)
  assert rows[0][logger.KEY]['producer_errors']==3


def test_every_low_speed_cycle_and_settings_generation():
  logger=LongitudinalEvidence(True);rows=[]
  for _ in range(8):
    if logger.due(False):logger.emit(lambda:{'settings':{'traffic':30}},rows.append)
  assert len(rows)==2
  for _ in range(3):
    assert logger.due(True)
    logger.emit(lambda:{'settings':{'traffic':40}},rows.append)
  assert rows[-1][logger.KEY]['settings_generation']==2


def test_mpc_capture_has_identical_constraints_and_references():
  baseline,c,r,_=production_mpc();update(baseline,c,r)
  captured,c,r,ns=production_mpc();captured.evidence_requested=True
  ns['time']=__import__('time')
  captured.evidence_cost_weights=[1.]*6;captured.evidence_constraint_weights=[1.]*4
  captured.u_sol=np.zeros((12,1));captured.solution_status=0;captured.solve_time=.001
  update(captured,c,r)
  np.testing.assert_array_equal(captured.params,baseline.params)
  np.testing.assert_array_equal(captured.yref,baseline.yref)
  p=captured.evidence_problem
  assert len(p['params'])==13 and p['warm_scope']=='previous_primal_solution_not_internal_dual_state'
  p['params'][0][1]=999.
  assert captured.params[0,1]==baseline.params[0,1]  # No alias to control arrays.


def test_logging_follows_actual_plan_publication():
  path=Path(__file__).resolve().parents[3]/'selfdrive/controls/lib/longitudinal_planner.py'
  cls=next(x for x in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(x,ast.ClassDef))
  publish=next(x for x in cls.body if isinstance(x,ast.FunctionDef) and x.name=='publish')
  calls=[x for x in ast.walk(publish) if isinstance(x,ast.Call)]
  send=next(x for x in calls if ast.unparse(x.func)=='pm.send')
  emit=next(x for x in calls if ast.unparse(x.func)=='self.long_evidence.emit')
  assert send.lineno<emit.lineno


@pytest.mark.parametrize('change,reason',[
  ({'sequence':4},'planner_evidence_gap'),({'cycle':5},'planner_evidence_gap'),
  ({'settings_generation':2},'producer_or_settings_changed'),
  ({'producer_errors':1},'producer_error_observed'),({'services':{}},'missing_input_validity'),
  ({'actual_arm':20},'comparison_excludes_actual_setting')])
def test_missing_or_changed_evidence_never_claims_no_effect(change,reason):
  result=summarize_frames([frame(),frame(2,**change)])
  assert result['classification']=='unknown' and reason in result['integrity_issues']
  assert result['placebo_determined'] is False


def test_complete_equal_ceiling_is_not_a_placebo_or_plan_identity_claim():
  rows=[frame(i,comparison={'30':{'applied_ceiling':1.3},'40':{'applied_ceiling':1.3}},bounds={'after_approach':1.3,'mpc_max':1.3}) for i in range(1,5)]
  assert summarize_frames(rows)['classification']=='same_ceiling_on_recorded_inputs'
  assert summarize_frames([])['classification']=='unknown'


def test_video_matches_presentation_timestamp_not_constant_fps():
  media={('r',0):[{'t':100_000_000,'pts':.07,'file':'media/a.mp4','frameId':3},
                  {'t':151_000_000,'pts':.125,'file':'media/a.mp4','frameId':4}]}
  match=match_video({'route':'r','t':150_000_000},media)
  assert match['pts']==.125 and match['match_error_ms']==1.
  assert match_video({'route':'r','t':1_000_000_000},media) is None


def test_report_escapes_script_end_from_untrusted_logs(tmp_path):
  render({'routes':[], 'unsafe':'</script><script>alert(1)</script>'},tmp_path)
  text=(tmp_path/'index.html').read_text(encoding='utf-8')
  assert '</script><script>alert(1)' not in text


def test_recorder_storage_and_schema_compatible_export(tmp_path):
  from selfdrive.carrot.can_diagnostics.automatic import AutoRecorder,ChunkStore
  store=ChunkStore(tmp_path,reserve=0)
  recorder=AutoRecorder(store,{'car_fingerprint':'HYUNDAI_PALISADE_LX3_HEV','boot_id':'test'})
  store.begin({'kind':'header','schema':1},1.)
  recorder.state='recording'
  row=frame(20);row['mono_ns']=1_000_000_000
  recorder.longitudinal_evidence(row,1_001_000_000,1.01)
  recorder.longitudinal_evidence({**row,'mono_ns':0},1_001_000_000,1.01)
  assert recorder.longitudinal_frames==1 and recorder.longitudinal_rejected==1
  store.seal('test')
  lines=[json.loads(line) for line in gzip.open(next(tmp_path.glob('*.jsonl.gz')),'rt')]
  assert lines[-1]['kind']=='longitudinal_evidence'
  assert lines[-1]['comparison']==row['comparison']


def test_local_video_server_supports_range_seeking(tmp_path):
  from functools import partial
  from http.server import ThreadingHTTPServer
  import threading
  from urllib.request import Request,urlopen
  from tools.serve_lx3_quality import Handler
  (tmp_path/'a.mp4').write_bytes(b'0123456789')
  server=ThreadingHTTPServer(('127.0.0.1',0),partial(Handler,directory=str(tmp_path)))
  thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
  try:
    with urlopen(Request(f'http://127.0.0.1:{server.server_port}/a.mp4',headers={'Range':'bytes=3-6'})) as r:
      assert r.status==206 and r.headers['Content-Range']=='bytes 3-6/10' and r.read()==b'3456'
  finally:server.shutdown();thread.join();server.server_close()


def test_media_sync_keeps_active_segment_and_external_targets_out():
  from tools.sync_lx3_quality_media import checked_base,completed_segments
  assert checked_base('http://100.71.19.14:7000')=='http://100.71.19.14:7000'
  for url in ('http://example.com:7000','http://127.0.0.1:7000','http://100.71.19.14:7000/evil','http://u@100.71.19.14:7000'):
    with pytest.raises(ValueError):checked_base(url)
  rows=['r--0','r--1','../r--2','other--3']
  assert completed_segments('r',rows,'r')==['r--0']
  assert completed_segments('r',rows,'next')==['r--0','r--1']


@pytest.mark.parametrize('truncate',[False,True])
def test_media_sync_uses_actual_catalog_and_never_finalizes_truncation(tmp_path,monkeypatch,truncate):
  import io
  from types import SimpleNamespace
  from tools import sync_lx3_quality_media as media
  (tmp_path/'a.jsonl.gz').write_bytes(gzip.compress(b'{"kind":"header","route":"r","device_dongle_id":"device"}\n'))
  called=[]
  def reply(request,timeout):
    url=request.full_url;called.append(url)
    if 'params_bulk' in url:raw=b'{"values":{"CurrentRoute":"r","DongleId":"device"}}'
    elif '/segments/' in url:raw=b'{"ok":true,"route":"r","segments":["r--0","r--1"]}'
    else:raw=b'123' if truncate else b'123456'
    response=io.BytesIO(raw);response.url=url;response.status=200
    name='qlog.zst' if url.endswith('/qlog') else 'qcamera.ts'
    response.headers={'Content-Length':'6','Content-Disposition':f'attachment; filename="r--0--{name}"'}
    return response
  monkeypatch.setattr(media.OPENER,'open',reply)
  monkeypatch.setattr(media.shutil,'disk_usage',lambda _:SimpleNamespace(free=4*1024**3))
  if truncate:
    with pytest.raises(ValueError):media.sync('http://100.71.19.14:7000',tmp_path)
    assert not (tmp_path/'video_segments/r--0/qcamera.ts').exists()
  else:
    assert media.sync('http://100.71.19.14:7000',tmp_path)['downloaded']==['r--0']
    assert (tmp_path/'video_segments/r--0/qcamera.ts').read_bytes()==b'123456'
    assert (tmp_path/'video_segments/r--0/qlog.zst').read_bytes()==b'123456'
    assert media.sync('http://100.71.19.14:7000',tmp_path)['downloaded']==[]
  assert not any('/download/r--1/' in path for path in called)


def test_conflicting_and_incomplete_video_identities_are_not_aligned():
  from tools.lx3_drive_quality import qualified_indexes
  row={'segmentId':0,'frameId':10,'native_sof':100}
  assert qualified_indexes([row,row],1)==[row]
  with pytest.raises(ValueError):qualified_indexes([row,{**row,'native_sof':101}],1)
  with pytest.raises(ValueError):qualified_indexes([row],2)


def test_cached_videos_do_not_repeat_remux_or_probe(tmp_path,monkeypatch):
  from tools import lx3_drive_quality as quality
  videos={};indexes={};(tmp_path/'media').mkdir()
  for segment in range(50):
    source=tmp_path/f'{segment}.ts';source.write_bytes(b'input');videos['r',segment]=source
    target=tmp_path/'media'/(hashlib.sha256(b'r').hexdigest()[:12]+f'-{segment}.mp4');target.write_bytes(b'output')
    target.with_suffix('.cache.json').write_text(json.dumps({'source_sha256':quality.sha(source),'mp4_sha256':quality.sha(target),'pts':[0.]}))
    indexes['r',segment]=[{'segmentId':0,'native_sof':100,'frameId':segment}]
  monkeypatch.setattr(quality.subprocess,'run',lambda *a,**kw: (_ for _ in ()).throw(AssertionError('cached video was reprocessed')))
  media,errors=quality.media_prepare(videos,indexes,{'r':0},tmp_path)
  assert len(media)==50 and not errors


def test_offline_video_source_still_produces_report(tmp_path,monkeypatch):
  import sys
  from tools import can_auto_sync
  monkeypatch.syspath_prepend(str(Path(can_auto_sync.__file__).parent))
  import sync_lx3_quality_media
  monkeypatch.setattr(sync_lx3_quality_media,'sync',lambda *a: (_ for _ in ()).throw(OSError('OP offline')))
  monkeypatch.setattr(sys,'argv',['can_auto_sync','--out',str(tmp_path),'--analyze-only','--quality','--video','--media-url','http://100.71.19.14:7000'])
  can_auto_sync.main()
  assert (tmp_path/'quality/index.html').is_file()


def test_failed_videos_do_not_starve_valid_new_video(tmp_path,monkeypatch):
  import subprocess
  from tools import lx3_drive_quality as quality
  videos={};indexes={};calls=[]
  for segment in range(5):
    path=tmp_path/f'{"bad" if segment<4 else "valid"}{segment}.ts';path.write_bytes(b'input');videos['r',segment]=path
    indexes['r',segment]=[{'segmentId':0,'frameId':segment,'native_sof':100}]
  def run(command,**kwargs):
    calls.append(command)
    if command[0]=='ffmpeg':
      if 'bad' in command[command.index('-i')+1]:raise subprocess.CalledProcessError(1,command)
      Path(command[-1]).write_bytes(b'output')
    return NS(stdout='{"frames":[{"best_effort_timestamp_time":"0.0"}]}')
  monkeypatch.setattr(quality.subprocess,'run',run)
  monkeypatch.setattr(quality.shutil,'disk_usage',lambda _:NS(free=4*1024**3))
  media,errors=quality.media_prepare(videos,indexes,{'r':0},tmp_path)
  assert len(errors)==4 and ('r',4) in media
  calls.clear();media,errors=quality.media_prepare(videos,indexes,{'r':0},tmp_path)
  assert not calls and len(errors)==4 and ('r',4) in media


def test_video_remux_leaves_primary_receiver_reserve(tmp_path,monkeypatch):
  from tools import lx3_drive_quality as quality
  path=tmp_path/'a.ts';path.write_bytes(b'input')
  free=2*1024**3+64*1024**2
  monkeypatch.setattr(quality.shutil,'disk_usage',lambda _:NS(free=free))
  monkeypatch.setattr(quality.subprocess,'run',lambda *a,**kw: (_ for _ in ()).throw(AssertionError('must not consume primary reserve')))
  media,errors=quality.media_prepare({('r',0):path},{},{'r':0},tmp_path)
  assert not media and 'primary chunk space preserved' in errors[0]['error']
  assert free > 2*1024**3 + 4*1024**2


def test_route_change_seals_before_new_sample_and_keeps_same_boot_trace_pre(tmp_path):
  from selfdrive.carrot.can_diagnostics.automatic import AutoRecorder,ChunkStore
  from selfdrive.carrot.can_diagnostics.test_automatic import services
  from tools.lx3_drive_quality import collect
  metadata={'car_fingerprint':'HYUNDAI_PALISADE_LX3_HEV','route':'old_route','boot_id':'test'}
  recorder=AutoRecorder(ChunkStore(tmp_path,reserve=0),metadata)
  recorder.update(services(100.,long=True),100.)
  trip=recorder.trip
  recorder.trace_pre.append((100_000_000_000,'rx',0,0x10B,b'old',None,None))
  metadata['route']='new_route'
  data=services(100.3,long=True);data['carState']['data']['vEgo']=10.
  recorder.update(data,100.3)
  assert recorder.trip==trip and recorder.trace_pre
  recorder.begin_trace(100_350_000_000,100.35,'test')
  recorder.close()
  chunks=[[json.loads(line) for line in gzip.open(p,'rt')] for p in tmp_path.glob('*.jsonl.gz')]
  assert {chunk[0]['route'] for chunk in chunks}=={'old_route','new_route'}
  assert all(row['route']==chunk[0]['route'] for chunk in chunks for row in chunk if row['kind']=='sample')
  streams,*_=collect([tmp_path],tmp_path)
  assert [row['vEgo'] for row in streams['old_route']['carState']]==[20.]
  assert [row['vEgo'] for row in streams['new_route']['carState']]==[10.]
  start=next(r for c in chunks for r in c if r.get('name')=='button_trace_start')
  assert start['pre_window_before_route_observation'] is True


@pytest.mark.parametrize('stamp_new_route',[True,False])
def test_mixed_legacy_chunk_rejected_without_breaking_unstamped_compatibility(tmp_path,stamp_new_route):
  from tools.lx3_drive_quality import collect
  rows=[{'kind':'header','route':'old_route'},
        {'kind':'sample','mono_ns':100,'route':'old_route','services':{'carState':{'mono_ns':100,'valid':True,'data':{'vEgo':20.}}}},
        {'kind':'sample','mono_ns':200,'services':{'carState':{'mono_ns':200,'valid':True,'data':{'vEgo':10.}}}}]
  if stamp_new_route:rows[-1]['route']='new_route'
  (tmp_path/'chunk.jsonl.gz').write_bytes(gzip.compress(('\n'.join(json.dumps(row) for row in rows)+'\n').encode()))
  streams,frames,_,indexes,_,hashes,errors=collect([tmp_path],tmp_path)
  assert hashes
  if stamp_new_route:
    assert not streams and not frames and not indexes
    assert any('mixed route' in row['error'] for row in errors)
  else:
    assert not errors and [row['vEgo'] for row in streams['old_route']['carState']]==[20.,10.]


def test_new_route_rejects_buffered_previous_route_evidence(tmp_path):
  from selfdrive.carrot.can_diagnostics.automatic import AutoRecorder,ChunkStore
  from selfdrive.carrot.can_diagnostics.test_automatic import services
  metadata={'car_fingerprint':'HYUNDAI_PALISADE_LX3_HEV','route':'old_route'}
  recorder=AutoRecorder(ChunkStore(tmp_path,reserve=0),metadata)
  recorder.update(services(100.,long=True),100.)
  metadata['route']='new_route';recorder.update(services(100.3,long=True),100.3)
  recorder.longitudinal_evidence(frame(20,mono_ns=100_100_000_000),100_100_000_000,100.35)
  assert recorder.longitudinal_rejected==1 and recorder.longitudinal_frames==0
  recorder.longitudinal_evidence(frame(21,mono_ns=100_340_000_000),100_340_000_000,100.35)
  assert recorder.longitudinal_frames==1
  recorder.close()


@pytest.mark.parametrize('source',['can','panda'])
def test_new_route_preserves_fresh_same_boot_native_pre_event_evidence(tmp_path,source):
  from selfdrive.carrot.can_diagnostics.automatic import AutoRecorder,ChunkStore
  from selfdrive.carrot.can_diagnostics.test_automatic import services
  metadata={'car_fingerprint':'HYUNDAI_PALISADE_LX3_HEV','route':'old_route'}
  recorder=AutoRecorder(ChunkStore(tmp_path,reserve=0),metadata)
  recorder.update(services(100.,long=True),100.)
  metadata['route']='new_route';recorder.update(services(100.3,long=True),100.3)
  def send(t):
    if source=='can':recorder.can_frame(0,0x12A,bytes(16),t,100.35)
    else:recorder.panda_snapshot([{'controls_allowed':False}],t,100.35)
  send(100_100_000_000)
  assert recorder.stale_packets==0
  send(100_340_000_000)
  recorder.close()
  chunks=[[json.loads(line) for line in gzip.open(p,'rt')] for p in tmp_path.glob('*.jsonl.gz')]
  new_chunk=next(c for c in chunks if c[0]['route']=='new_route')
  native=[r for r in new_chunk if r['kind']==('can_sample' if source=='can' else 'panda_snapshot')]
  # Native pre-event evidence is useful and is not a route-qualified report
  # input. Source timestamps expose that it predates the observed boundary.
  assert native[0]['mono_ns']==100_100_000_000 < new_chunk[0]['route_observed_mono_ns']
  assert recorder.stale_packets==0


@pytest.mark.parametrize('off_ticks,unknown_after_off',[(1,False),(4,False),(1,True)])
def test_previous_trip_route_never_labels_next_trip_before_new_route(tmp_path,off_ticks,unknown_after_off):
  from selfdrive.carrot.can_diagnostics.automatic import AutoRecorder,ChunkStore
  from selfdrive.carrot.can_diagnostics.test_automatic import services
  from tools.lx3_drive_quality import collect
  metadata={'car_fingerprint':'HYUNDAI_PALISADE_LX3_HEV','route':'old_route'}
  recorder=AutoRecorder(ChunkStore(tmp_path,reserve=0),metadata)
  recorder.update(services(100.,long=True),100.)
  for step in range(off_ticks):recorder.update(services(100.1+step*.02,started=False),100.1+step*.02)
  if unknown_after_off:recorder.update({},100.2)
  data=services(100.3,long=True);data['carState']['data']['vEgo']=10.
  recorder.update(data,100.3);new_trip=recorder.trip
  assert recorder.recording_route is None
  # Raw metadata may still carry old_route. It must not leak through headers,
  # sample rows, detailed rows, or native video forwarding.
  recorder.longitudinal_evidence(frame(21,mono_ns=100_340_000_000),100_340_000_000,100.35)
  recorder.video_index({'timestampSof':100_340_000_000,'segmentNum':0},100_350_000_000,100.35,'old_route',100_350_000_000,0)
  assert recorder.video_index_route_mismatch==1
  metadata['route']='new_route';data=services(100.6,long=True);data['carState']['data']['vEgo']=9.
  recorder.update(data,100.6);recorder.close()
  chunks=[[json.loads(line) for line in gzip.open(p,'rt')] for p in tmp_path.glob('*.jsonl.gz')]
  current=[c for c in chunks if c[0]['trip_id']==new_trip]
  assert {c[0]['route'] for c in current}=={None,'new_route'}
  assert all(r.get('route')==c[0]['route'] for c in current for r in c if r['kind'] in ('sample','longitudinal_evidence'))
  streams,*_=collect([tmp_path],tmp_path)
  assert [r['vEgo'] for r in streams['old_route']['carState']]==[20.]
  assert [r['vEgo'] for r in streams['new_route']['carState']]==[9.]


def test_unknown_ignition_recovers_the_same_live_route(tmp_path):
  from selfdrive.carrot.can_diagnostics.automatic import AutoRecorder,ChunkStore
  from selfdrive.carrot.can_diagnostics.test_automatic import services
  metadata={'car_fingerprint':'HYUNDAI_PALISADE_LX3_HEV','route':'same_route'}
  recorder=AutoRecorder(ChunkStore(tmp_path,reserve=0),metadata)
  recorder.update(services(100.,long=True),100.)
  recorder.update({},104.)
  recorder.update(services(104.1,long=True),104.1)
  assert recorder.recording_route=='same_route'
  recorder.close()
  chunks=[[json.loads(line) for line in gzip.open(p,'rt')] for p in tmp_path.glob('*.jsonl.gz')]
  assert len(chunks)==2 and {c[0]['route'] for c in chunks}=={'same_route'}


@pytest.mark.parametrize('prior_trip',[False,True])
def test_new_route_visible_before_worker_started_is_preserved(tmp_path,prior_trip):
  from selfdrive.carrot.can_diagnostics.automatic import AutoRecorder,ChunkStore
  from selfdrive.carrot.can_diagnostics.test_automatic import services
  metadata={'car_fingerprint':'HYUNDAI_PALISADE_LX3_HEV','route':'old_route' if prior_trip else None}
  recorder=AutoRecorder(ChunkStore(tmp_path,reserve=0),metadata)
  if prior_trip:recorder.update(services(100.),100.)
  recorder.update(services(100.1,started=False),100.1)
  metadata['route']='new_route';recorder.update(services(100.2,started=False),100.2)
  recorder.update(services(100.3),100.3)
  assert recorder.recording_route=='new_route'
  recorder.close()


@pytest.mark.parametrize('condition',['accepted','old_sof','old_envelope','changed_route','unknown_route','storage_full'])
def test_video_forwarding_uses_effective_route_and_native_clock(tmp_path,condition):
  from selfdrive.carrot.can_diagnostics.automatic import AutoRecorder,ChunkStore
  from selfdrive.carrot.can_diagnostics.test_automatic import services
  route=None if condition=='unknown_route' else 'route'
  recorder=AutoRecorder(ChunkStore(tmp_path,reserve=0),{'car_fingerprint':'HYUNDAI_PALISADE_LX3_HEV','route':route})
  recorder.update(services(100.),100.)
  offset=2_000_000_000
  index={'timestampSof':102_350_000_000,'timestampEof':102_360_000_000,'segmentNum':0,'segmentId':0,'frameId':10}
  source_ns=102_400_000_000
  observed=route
  if condition=='old_sof':index['timestampSof']=101_950_000_000
  if condition=='old_envelope':source_ns=101_800_000_000
  if condition=='changed_route':observed='different'
  if condition=='storage_full':recorder.store.seconds=0;recorder.store.quota=0
  recorder.video_index(index,source_ns,100.4,observed,100_400_000_000,offset)
  recorder.close()
  rows=[json.loads(line) for p in tmp_path.glob('*.jsonl.gz') for line in gzip.open(p,'rt')]
  forwarded=[r for r in rows if r['kind']=='video_index']
  assert len(forwarded)==(1 if condition=='accepted' else 0)
  assert recorder.video_index_stale==(1 if condition=='old_envelope' else 0)
  assert recorder.video_index_route_mismatch==(1 if condition in ('old_sof','changed_route','unknown_route') else 0)
  assert recorder.video_index_dropped==(1 if condition=='storage_full' else 0)
  if forwarded:
    assert forwarded[0]['route']==route and forwarded[0]['native_clock_offset_ns']==offset


def test_pre_window_annotation_excludes_old_rows_outside_copied_window(tmp_path):
  from selfdrive.carrot.can_diagnostics.automatic import AutoRecorder,ChunkStore
  from selfdrive.carrot.can_diagnostics.test_automatic import services
  metadata={'car_fingerprint':'HYUNDAI_PALISADE_LX3_HEV','route':'old_route'}
  recorder=AutoRecorder(ChunkStore(tmp_path,reserve=0),metadata)
  recorder.update(services(100.),100.)
  metadata['route']='new_route';recorder.update(services(100.3),100.3)
  recorder.can_frame(0,0x12A,bytes(16),100_100_000_000,100.35)
  # No more selected CAN arrives, so the old deque is not pruned. A later
  # host/panda-triggered trace must describe only the pre-rows actually copied.
  recorder.begin_trace(104_000_000_000,104.,'quiet_bus_trigger')
  recorder.close()
  rows=[json.loads(line) for p in tmp_path.glob('*.jsonl.gz') for line in gzip.open(p,'rt')]
  start=next(r for r in rows if r.get('name')=='button_trace_start')
  assert not start['pre_window_before_route_observation']
  assert not any(r['kind']=='button_trace_can' for r in rows)
