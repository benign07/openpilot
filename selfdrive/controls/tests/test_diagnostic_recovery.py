"""Power-loss evidence cannot poison the immutable upload queue."""
import gzip
import json
from pathlib import Path

import pytest

from openpilot.selfdrive.carrot.can_diagnostics.automatic import ChunkStore


@pytest.mark.parametrize('suffix,payload', [
  ('.partial', b''), ('.partial', b'{"kind":"hea'),
  ('.jsonl', b''), ('.jsonl', b'{"kind":"header"}'),
])
def test_unrecoverable_first_header_is_preserved_without_manifest(tmp_path, suffix, payload):
  source = tmp_path / ('a' * 32 + suffix)
  source.write_bytes(payload)
  store = ChunkStore(tmp_path, reserve=0)
  assert store.list_chunks() == []
  assert not list(tmp_path.glob('*.jsonl.gz'))
  quarantined = list((tmp_path / 'recovery_quarantine').glob('*'))
  assert any(p.read_bytes() == payload for p in quarantined if p.is_file())
  assert store.recovery_quarantined == 1
  assert store.usage >= len(payload)


def test_completed_prefix_is_recovered_and_next_record_is_usable(tmp_path):
  header = b'{"kind":"header","schema":1}\n'
  (tmp_path / ('a' * 32 + '.partial')).write_bytes(header + b'{"unfinished":')
  store = ChunkStore(tmp_path, reserve=0)
  rows = store.list_chunks()
  assert len(rows) == 1 and rows[0]['reason'] == 'power_loss_recovered'
  assert gzip.decompress((tmp_path / (rows[0]['id'] + '.jsonl.gz')).read_bytes()) == header
  assert store.begin({'boot_id': 'b'}, 1.)
  store.append({'kind': 'sample', 'mono_ns': 1}, 1.1)
  store.seal('test')
  assert len(store.list_chunks()) == 2


def test_orphan_empty_gzip_does_not_get_a_manifest(tmp_path):
  payload = gzip.compress(b'', mtime=0)
  (tmp_path / ('b' * 32 + '.jsonl.gz')).write_bytes(payload)
  store = ChunkStore(tmp_path, reserve=0)
  assert store.list_chunks() == []
  assert not list(tmp_path.glob('*.jsonl.gz'))
  assert any(p.read_bytes() == payload for p in (tmp_path / 'recovery_quarantine').iterdir())


@pytest.mark.parametrize('payload',[b'bad gzip',b'\x1f\x8b\x08',gzip.compress(b'header')[:11]])
def test_corrupt_orphan_gzip_is_preserved_without_a_restart_loop(tmp_path,payload):
  source=tmp_path/('c'*32+'.jsonl.gz');source.write_bytes(payload)
  store=ChunkStore(tmp_path,reserve=0)
  assert store.list_chunks()==[] and store.recovery_quarantined==1
  assert next((tmp_path/'recovery_quarantine').iterdir()).read_bytes()==payload
  assert ChunkStore(tmp_path,reserve=0).recovery_quarantined==0


def test_header_is_durable_before_first_periodic_flush(tmp_path):
  store = ChunkStore(tmp_path, reserve=0)
  assert store.begin({'boot_id': 'b'}, 1.)
  # Separate reader, before the one-second periodic data flush.
  raw = store.path.read_bytes()
  assert raw.endswith(b'\n') and json.loads(raw)['kind'] == 'header'
  store.seal('test')


def test_quarantined_bytes_still_consume_quota(tmp_path):
  directory = tmp_path / 'recovery_quarantine'
  directory.mkdir()
  (directory / 'previous.partial').write_bytes(b'x' * 200_000)
  store = ChunkStore(tmp_path, quota=210_000, chunk_bytes=10_000, reserve=0)
  assert not store.begin({}, 1.)
  assert store.error == 'storage_full_preserving_records'
