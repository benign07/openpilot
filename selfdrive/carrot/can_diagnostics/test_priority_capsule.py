"""Inert quota/first-fault evidence regression. Never opens a Panda or CAN publisher."""
import binascii
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from .automatic import AutoRecorder, ChunkStore, FirstFaultCapsule, PriorityCapsuleWriter


def packet(lss=0):
  data=bytearray((lss<<219).to_bytes(32,'little'))
  data[:2]=(binascii.crc_hqx(data[2:]+b'\x62\x01',0)^0x9f5b).to_bytes(2,'little')
  return bytes(data)


def services(now):
  return {'deviceState':{'mono_ns':int(now*1e9),'valid':True,'data':{'started':True}}}


def full_store_regression(module, root):
  store=module.ChunkStore(root,quota=0,reserve=0)
  rec=module.AutoRecorder(store,{'boot_id':'inert','car_fingerprint':'HYUNDAI_PALISADE_LX3_HEV'})
  try:
    rec.update(services(100),100)
    assert rec.state=='storage_full_preserving_records'
    rec.can_frame(2,0x162,packet(),100_000_000_000,100)
    rec.can_frame(2,0x162,packet(1),100_010_000_000,100.01)
    rec.update(services(108.1),108.1)
  finally:rec.close()
  manifests=[json.loads(p.read_text()) for p in Path(root).glob('*.manifest.json')]
  assert sum(m.get('priority_capsule') is True for m in manifests)==2, 'ordinary quota suppressed first-fault evidence'
  return manifests


def urgent_retention_regression(module,root):
  store=module.ChunkStore(root,quota=1000000,reserve=0,rolling=True,chunk_bytes=4096)
  for n in range(6):
    assert store.begin({'boot_id':'retention','trip_id':'t'},n*10)
    store.append({'kind':'sample','text':'x'*100},n*10);store.seal('test')
  store.quota=store.usage+store.chunk_bytes+65536-1
  store.last_retention_scan=100
  before=store.retired_chunks
  store.make_room(101)
  assert store.retired_chunks>before,'30-second throttle suppresses urgent retirement'


class Sink:
  queue_dropped=errors=0
  def __init__(self):self.items=[]
  def submit(self,*args):self.items.append(args);return True


class CapsuleTests(unittest.TestCase):
  def make_capsule(self):
    sink=Sink();cap=FirstFaultCapsule(sink)
    cap.begin('trip1',{'car_fingerprint':'HYUNDAI_PALISADE_LX3_HEV','route':'route1'})
    return cap,sink

  def send(self,cap,lss=0,now=100,bus=2,direction='rx',data=None):
    cap.can_frame(bus,0x162,packet(lss) if data is None else data,int(now*1e9),now,direction)

  def test_actual_full_chunk_store_keeps_first_fault(self):
    import sys
    with tempfile.TemporaryDirectory() as root:
      full_store_regression(sys.modules[AutoRecorder.__module__],root)

  def test_urgent_retention_can_progress_one_second_after_previous_scan(self):
    import sys
    with tempfile.TemporaryDirectory() as root:
      urgent_retention_regression(sys.modules[AutoRecorder.__module__],root)

  def test_main_append_io_error_does_not_restart_or_disarm_fault_capture(self):
    import errno
    with tempfile.TemporaryDirectory() as root:
      store=ChunkStore(root,reserve=0)
      rec=AutoRecorder(store,{'boot_id':'inert','car_fingerprint':'HYUNDAI_PALISADE_LX3_HEV'})
      try:
        rec.update(services(100),100)
        with patch.object(store,'append',side_effect=OSError(errno.ENOSPC,'injected')):
          rec.can_frame(2,0x162,packet(),100_000_000_000,100)
        self.assertTrue(rec.storage_disabled);self.assertGreater(rec.storage_io_errors,0)
        rec.can_frame(2,0x162,packet(1),100_100_000_000,100.1)
        self.assertIsNotNone(rec.priority.trigger)
        rec.update(services(109),109)
      finally:
        rec.close()
        if store.stream is not None:store.stream.close()
      self.assertEqual(sum(json.loads(p.read_text()).get('priority_capsule') is True for p in Path(root).glob('*.manifest.json')),2)

  def test_first_fault_has_zero_history_and_short_pulse_is_not_sampled_away(self):
    cap,sink=self.make_capsule()
    self.send(cap,now=100);self.send(cap,1,100.001)
    self.assertEqual(len(sink.items),1)
    self.assertEqual(cap.trigger['before'],(0,0,0,0,0,0))
    self.assertFalse(cap.trigger['initial_fault_present'])
    self.assertEqual(len(sink.items[0][1]),2)
    cap.context({},109);self.assertEqual(len(sink.items),2)
    self.send(cap,2,110);self.assertEqual(len(sink.items),2)

  def test_storage_disabled_trace_cannot_prevent_next_trip_capsule(self):
    with tempfile.TemporaryDirectory() as root:
      rec=AutoRecorder(ChunkStore(root,reserve=0),{'car_fingerprint':'HYUNDAI_PALISADE_LX3_HEV'})
      try:
        rec.update(services(100),100);old=rec.trip
        rec.storage_disabled=True;rec.trace={'inert':True}
        off=services(101);off['deviceState']['data']['started']=False
        with patch.object(rec,'close_trace',side_effect=OSError('persistent disk failure')) as close:
          rec.update(off,101);close.assert_not_called()
        self.assertIsNone(rec.trip);self.assertIsNone(rec.trace)
        rec.update(services(102),102);self.assertNotEqual(rec.trip,old)
        self.assertEqual(rec.priority.trip,rec.trip)
        rec.can_frame(2,0x162,packet(),102_000_000_000,102)
        rec.can_frame(2,0x162,packet(1),102_100_000_000,102.1)
        self.assertIsNotNone(rec.priority.trigger)
      finally:
        rec.close()
        if rec.store.stream is not None:rec.store.stream.close()

  def test_initial_nonzero_does_not_claim_onset(self):
    cap,sink=self.make_capsule();self.send(cap,1)
    self.assertTrue(sink.items[0][0]['trigger']['initial_fault_present'])
    self.assertIsNone(sink.items[0][0]['trigger']['before'])
    self.assertIsNone(cap.trigger)
    self.send(cap,now=101);self.send(cap,1,101.1)
    self.assertEqual(len(sink.items),2)
    self.assertIsNotNone(cap.trigger)

  def test_echo_host_other_bus_bad_crc_and_other_car_do_not_trigger(self):
    cap,sink=self.make_capsule()
    for bus,direction in ((0,'rx'),(1,'rx'),(130,'tx_echo'),(2,'host')):
      self.send(cap,1,bus=bus,direction=direction)
    data=bytearray(packet(1));data[0]^=1;self.send(cap,data=bytes(data))
    self.assertEqual(cap.bad_crc,1)
    cap.metadata['car_fingerprint']='KIA_EV9';self.send(cap,1)
    self.assertEqual(sink.items,[])

  def test_stale_reordered_and_capacity_limits_are_explicit(self):
    cap,sink=self.make_capsule()
    cap.can_frame(2,0x162,packet(1),97_000_000_000,100,'rx')
    self.send(cap,now=100);self.send(cap,1,99.99)
    self.assertEqual((cap.stale,cap.out_of_order),(1,1))
    self.assertEqual(sink.items,[])
    cap.MAX_ROWS=3;self.send(cap,1,100.1)
    for n in range(20):self.send(cap,1,100.2+n*.1)
    self.assertEqual(len(cap.rows),3);self.assertGreater(cap.dropped,0)

  def test_checkpoint_freezes_metadata_and_rows(self):
    cap,sink=self.make_capsule();self.send(cap);self.send(cap,1,100.1)
    cap.metadata['route']='route2';cap.rows[0]['data']='changed'
    self.assertEqual(sink.items[0][0]['route'],'route1')
    self.assertNotEqual(sink.items[0][1][0]['data'],'changed')

  def test_no_fault_never_submits_disk_work(self):
    cap,sink=self.make_capsule()
    for n in range(200):self.send(cap,now=100+n*.01)
    cap.finish(103,'stop');self.assertEqual(sink.items,[])

  def test_unknown_other_fault_enum_does_not_consume_lss_incident(self):
    cap,sink=self.make_capsule()
    other=(2<<216).to_bytes(32,'little');other=bytearray(other)
    other[:2]=(binascii.crc_hqx(other[2:]+b'\x62\x01',0)^0x9f5b).to_bytes(2,'little')
    self.send(cap,data=bytes(other));self.assertEqual(sink.items,[])
    self.send(cap,1,100.1);self.assertEqual(len(sink.items),1)

  def test_source_gap_is_not_reported_as_exact_fault_onset(self):
    cap,sink=self.make_capsule();self.send(cap,now=100);self.send(cap,1,109)
    trigger=sink.items[0][0]['trigger']
    self.assertEqual(trigger['mono_ns']-trigger['previous_source_observation_ns'],9_000_000_000)
    self.assertEqual(len(sink.items[0][1]),1)  # Expired pre-window cannot be invented.

  def test_other_keys_cannot_starve_source_fault_key(self):
    cap,sink=self.make_capsule()
    for n in range(64):cap.can_frame(n,0x4B9,b'\x00'*8,100_000_000_000,100,'rx')
    self.send(cap,1);self.assertEqual(len(sink.items),1)
    self.assertEqual(len(cap.last),64);self.assertEqual(cap.dropped,1)

  def test_native_spi_log_requires_exact_bounded_counter_set(self):
    from .automatic_runtime import spi_host_fields
    text='SPI_HOST_STATS '+' '.join(k+'=0' for k in ('mono_ms','attempts','retries','header_io','header_nack',
      'header_timeout','data_io','data_nack','data_timeout','read_io','bad_length','bad_checksum',
      'recovery_failed','failed','success','window_max_call_attempts','window_max_charged_us','window_max_budget_permille'))
    self.assertEqual(spi_host_fields(text)['failed'],0)
    for bad in (text+' failed=1',text.replace('attempts=0','attempts=-1'),text.replace('retries=0','retries='+str(2**64)),
                text.replace('attempts=0 ',''),{'msg':text}):self.assertIsNone(spi_host_fields(bad))

  def test_profile_parser_matches_actual_generated_cpp_format(self):
    import re
    from tools.original_lx3_spi_variant import SUPPORT
    from .automatic_runtime import spi_profile_fields
    fmt=re.search(r'LOGW\("SPI_PROFILE_STATS ([^"]+)"',SUPPORT).group(1)
    message='SPI_PROFILE_STATS '+fmt.replace('%s','observed',1).replace('%s','a'*64,1).replace('%llu','0')
    parsed=spi_profile_fields(message)
    self.assertEqual(parsed['profile'],'observed');self.assertEqual(parsed['call_wall_max_us'],0)

  def test_ten_millisecond_state_pulse_survives_post_window_sampling(self):
    cap,sink=self.make_capsule();self.send(cap);self.send(cap,1,100.1)
    for now,active in ((102,0),(102.01,1),(102.02,0)):
      cap.can_frame(0,0xEA,(active<<48).to_bytes(24,'little'),int(now*1e9),now,'rx')
    pulses=[r for r in cap.rows if r.get('address')==0xEA]
    self.assertEqual(len(pulses),3)
    self.assertEqual(pulses[1]['mono_ns']-pulses[0]['mono_ns'],10_000_000)

  def test_producer_lag_is_recorded_without_claiming_fresh_full_rate(self):
    cap,sink=self.make_capsule()
    cap.can_frame(2,0x162,packet(1),100_000_000_000,101.5,'rx')
    self.assertEqual(sink.items[0][1][0]['producer_age_ns'],1_500_000_000)
    self.assertEqual(sink.items[0][0]['capsule_phase'],'initial_present')

  def test_factory_emergency_alert_edge_survives_after_full_rate_window(self):
    cap,sink=self.make_capsule();self.send(cap);self.send(cap,1,100.1)
    for now,value in ((102,0),(102.01,11),(102.02,0)):
      cap.can_frame(2,0x161,(value<<128).to_bytes(32,'little'),int(now*1e9),now,'rx')
    cap.context({},109)
    rows=[r for r in sink.items[-1][1] if r.get('address')==0x161]
    self.assertEqual(len(rows),3)
    self.assertEqual((int.from_bytes(bytes.fromhex(rows[1]['data']),'little')>>128)&63,11)

  def test_capsules_are_not_counted_as_additional_driving(self):
    from tools.can_auto_sync import chunk_summary
    result=chunk_summary([{'kind':'header','priority_capsule':True,'boot_id':'b','trip_id':'t'},
                          {'kind':'event','name':'overlapping'}])
    self.assertTrue(result['quality_excluded'])
    self.assertEqual((result['events'],result['histograms']),({},{}))


class WriterTests(unittest.TestCase):
  def test_own_failed_temp_removed_foreign_temp_and_renamed_gzip_preserved(self):
    with tempfile.TemporaryDirectory() as root:
      foreign=Path(root)/'.priority-foreign.writing';foreign.write_bytes(b'preserve')
      writer=PriorityCapsuleWriter(root)
      with patch('os.fsync',side_effect=OSError('injected fsync')):
        with self.assertRaises(OSError):writer.write({'capsule_phase':'onset'},[],{})
      self.assertEqual(list(Path(root).glob('.priority-*.writing')),[foreign])
      writer.write({'capsule_phase':'onset'},[],{})
      self.assertEqual(writer.written,1);self.assertEqual(foreign.read_bytes(),b'preserve')

  def test_oversized_optional_settings_are_omitted_without_losing_capsule(self):
    with tempfile.TemporaryDirectory() as root:
      writer=PriorityCapsuleWriter(root)
      writer.write({'capsule_phase':'onset','settings':{'large':'x'*100000}},[],{})
      with gzip.open(next(Path(root).glob('*.jsonl.gz')),'rt') as stream:header=json.loads(stream.readline())
      self.assertNotIn('settings',header);self.assertIn('settings',header['header_omitted'])
      self.assertEqual(writer.header_trimmed,1)

  def test_manifest_failure_leaves_valid_gzip_for_recovery(self):
    import sys
    with tempfile.TemporaryDirectory() as root:
      writer=PriorityCapsuleWriter(root)
      with patch.object(sys.modules[PriorityCapsuleWriter.__module__],'atomic_json',side_effect=OSError('interrupted')):
        with self.assertRaises(OSError):writer.write({'capsule_phase':'onset'},[],{})
      self.assertEqual(len(list(Path(root).glob('*.jsonl.gz'))),1)
      store=ChunkStore(root,reserve=0)
      self.assertTrue(store.list_chunks()[0]['priority_capsule'])

  def test_stopped_writer_restarts_without_leaking_threads(self):
    with tempfile.TemporaryDirectory() as root:
      writer=PriorityCapsuleWriter(root)
      for n in range(3):
        self.assertTrue(writer.submit({'capsule_phase':str(n)},[],{}));writer.close()
        self.assertIsNone(writer.thread)
      self.assertEqual(writer.written,3)

  def test_atomic_export_marker_survives_recovery_and_normal_retention(self):
    with tempfile.TemporaryDirectory() as root:
      store=ChunkStore(root,reserve=0,rolling=True,quota=100000)
      writer=PriorityCapsuleWriter(root)
      writer.write({'capsule_phase':'onset'},[],{})
      manifest=next(Path(root).glob('*.manifest.json'));meta=json.loads(manifest.read_text())
      data=Path(root)/(meta['id']+'.jsonl.gz')
      self.assertEqual(hashlib.sha256(data.read_bytes()).hexdigest(),meta['sha256'])
      manifest.unlink()  # Power loss after gzip rename, before manifest publication.
      store=ChunkStore(root,reserve=0,rolling=True,quota=0)
      self.assertTrue(json.loads(manifest.read_text())['priority_capsule'])
      store.make_room(100);self.assertTrue(data.exists())

  def test_own_file_limit_retires_only_verified_priority_files(self):
    with tempfile.TemporaryDirectory() as root:
      foreign=Path(root)/'do_not_delete';foreign.write_text('unrelated')
      writer=PriorityCapsuleWriter(root);writer.MAX_FILES=2
      for n in range(5):writer.write({'capsule_phase':str(n)},[],{})
      self.assertEqual(len(list(Path(root).glob('*.manifest.json'))),2)
      self.assertEqual(foreign.read_text(),'unrelated')

  def test_corrupt_priority_is_preserved_without_poisoning_future_captures(self):
    with tempfile.TemporaryDirectory() as root:
      writer=PriorityCapsuleWriter(root);writer.write({'capsule_phase':'onset'},[],{})
      data=next(Path(root).glob('*.jsonl.gz'));data.write_bytes(b'bad')
      writer.write({'capsule_phase':'final'},[],{})
      self.assertEqual(data.read_bytes(),b'bad')
      self.assertEqual(writer.written,2);self.assertEqual(writer.unverified_records,1)

  def test_slow_disk_worker_never_blocks_bounded_submit_queue(self):
    with tempfile.TemporaryDirectory() as root:
      entered=threading.Event();release=threading.Event()
      class SlowWriter(PriorityCapsuleWriter):
        def write(self,*args):entered.set();release.wait(3)
      writer=SlowWriter(root)
      try:
        self.assertTrue(writer.submit({},[],{}));self.assertTrue(entered.wait(2))
        self.assertTrue(writer.submit({},[],{}));self.assertTrue(writer.submit({},[],{}))
        self.assertFalse(writer.submit({},[],{}));self.assertEqual(writer.queue_dropped,1)
      finally:release.set();writer.close()

  def test_disk_reserve_and_background_errors_are_reported(self):
    with tempfile.TemporaryDirectory() as root:
      writer=PriorityCapsuleWriter(root);writer.RESERVE=10**30
      self.assertTrue(writer.submit({'capsule_phase':'onset'},[],{}));writer.close()
      self.assertEqual((writer.errors,writer.written),(1,0))
      self.assertEqual(list(Path(root).glob('*.manifest.json')),[])


if __name__=='__main__':unittest.main()
