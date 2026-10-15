"""Use real serialized qlogs to catch encoder/log segment-number confusion."""
import gzip
import json
from pathlib import Path

import pytest

from tools.lx3_drive_quality import collect
from tools import sync_lx3_quality_media as media


def test_incremental_archive_cache_preserves_counts_and_rejects_changed_source(tmp_path):
  from tools.can_auto_sync import analyze
  import hashlib
  for i in range(2):
    ident=str(i)*32
    raw=gzip.compress(('\n'.join(json.dumps(r) for r in [
      {'kind':'header','schema':1,'boot_id':'boot','trip_id':'trip'},
      {'kind':'event','name':'test'}])+'\n').encode())
    (tmp_path/(ident+'.jsonl.gz')).write_bytes(raw)
    (tmp_path/(ident+'.manifest.json')).write_text(json.dumps(
      {'id':ident,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}))
  pending=analyze(tmp_path,budget_seconds=0)
  assert pending['analysis_pending_chunks']==2 and pending['events']=={}
  fresh=analyze(tmp_path);cached=analyze(tmp_path,budget_seconds=0)
  assert fresh['events']==cached['events']=={'test':2}
  assert cached['cached_chunks']==2 and cached['analysis_pending_chunks']==0
  (tmp_path/('0'*32+'.jsonl.gz')).write_bytes(b'corrupt')
  broken=analyze(tmp_path)
  assert broken['events']=={'test':1} and broken['coverage']['corrupt_chunks']==1


def fixture(tmp_path, segments=(5,), boots=('boot',)):
  capnp = pytest.importorskip('capnp')
  zstandard = pytest.importorskip('zstandard')
  capnp.remove_import_hook()
  schema = Path(__file__).resolve().parents[3] / 'cereal'
  event = capnp.load(str(schema / 'log.capnp'), imports=[str(schema)]).Event
  for i, boot in enumerate(boots):
    header = {'kind': 'header', 'schema': 1, 'route': 'r', 'boot_id': boot,
              'native_clock_offset_ns': 2, 'device_dongle_id': 'device'}
    (tmp_path / (str(i)+'.jsonl.gz')).write_bytes(gzip.compress((json.dumps(header)+'\n').encode()))
  for i, encoder_segment in enumerate(segments):
    directory = tmp_path / ('r--'+str(i)); directory.mkdir()
    message = event.new_message(); message.logMonoTime = 100+i*10; message.valid = True
    idx = message.init('qRoadEncodeIdx')
    idx.segmentNum = encoder_segment; idx.segmentId = 0; idx.frameId = i+10
    idx.timestampSof = 90+i*10; idx.timestampEof = 95+i*10
    (directory / 'qlog.zst').write_bytes(zstandard.ZstdCompressor().compress(message.to_bytes()))
    (directory / 'qcamera.ts').write_bytes(b'video')
  return collect([tmp_path], schema)


def test_qlog_indexes_follow_directory_when_encoder_started_earlier(tmp_path):
  _, _, videos, indexes, offsets, _, errors = fixture(tmp_path, (5, 6))
  assert not errors and set(videos) == {('r', 0), ('r', 1)}
  assert set(indexes) == set(videos) and ('r', 5) not in indexes
  assert indexes['r', 0][0]['frameId'] == 10 and offsets['r'] == 2


def test_changed_encoder_offset_rejects_matching_counts(tmp_path):
  _, _, _, indexes, _, _, errors = fixture(tmp_path, (5, 7))
  assert not indexes and any('offset changed' in row['error'] for row in errors)


def test_multiple_boots_or_missing_boot_never_qualify_video(tmp_path):
  _, _, _, _, offsets, _, errors = fixture(tmp_path, boots=('first', 'second'))
  assert offsets['r'] is None and any('boot identity' in row['error'] for row in errors)


def test_media_sync_does_not_read_an_unidentified_or_different_device(tmp_path, monkeypatch):
  (tmp_path / 'a.jsonl.gz').write_bytes(gzip.compress(b'{"kind":"header","route":"r","device_dongle_id":"other"}\n'))
  calls = []
  def fetch(base, path):
    calls.append(path)
    return {'values': {'CurrentRoute': 'r', 'DongleId': 'device'}}
  monkeypatch.setattr(media, 'fetch', fetch)
  result = media.sync('http://100.71.19.14:7000', tmp_path)
  assert result['pending_identity'] == ['r'] and not result['downloaded']
  assert len(calls) == 1 and not list(tmp_path.glob('video_segments/*'))
