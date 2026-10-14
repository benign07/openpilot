"""Record actual wire-format service readers, including native enum values."""
import gzip
import json

from cereal import log
from .automatic import AutoRecorder, ChunkStore
from .automatic_runtime import FIELDS, selected_fields


def actual_services(now, sound='none'):
  services = {}
  for name, fields in FIELDS.items():
    message = log.Event.new_message()
    payload = message.init(name)
    if name == 'deviceState':
      payload.started = True
      payload.cpuTempC = [78., 80.]
      payload.gpuTempC = [75.]
      payload.pmicTempC = [68.]
      payload.cpuUsagePercent = [20, 70]
      payload.thermalStatus = 'yellow'
    if name == 'selfdriveState':
      payload.alertSound = sound
    with log.Event.from_bytes(message.to_bytes()) as event:
      data = selected_fields(getattr(event, name), fields)
    services[name] = {'mono_ns': int(now*1e9), 'valid': True, 'alive': True, 'data': data}
  return services


def test_all_runtime_fields_can_be_recorded_from_real_schema(tmp_path):
  recorder = AutoRecorder(ChunkStore(tmp_path, reserve=0, seconds=1),
                          {'boot_id': 'ci-schema', 'car_fingerprint': 'HYUNDAI_PALISADE_LX3_HEV'})
  for now, sound in ((100., 'none'), (101.1, 'refuse')):
    services = actual_services(now, sound)
    # Conversion must preserve the actual warning; replacing it with an empty
    # value or stringifying the whole payload would hide the recording fault.
    assert services['selfdriveState']['data']['alertSound'] == sound
    assert services['deviceState']['data']['cpuTempC'] == [78.,80.]
    assert services['deviceState']['data']['thermalStatus'] == 'yellow'
    json.dumps(services, allow_nan=False)
    recorder.update(services, now)
  recorder.close()
  rows = []
  for p in tmp_path.glob('*.jsonl.gz'):
    with gzip.open(p, 'rt', encoding='utf-8') as stream:
      rows.extend(json.loads(line) for line in stream)
  samples = [r for r in rows if r['kind'] == 'sample']
  assert len(samples) == 2
  assert [r['services']['selfdriveState']['data']['alertSound'] for r in sorted(samples,key=lambda r:r['mono_ns'])] == ['none','refuse']
  assert len([r for r in rows if r['kind']=='header']) == 2


def test_wire_video_index_passes_effective_route_and_native_clock_gate(tmp_path):
  recorder=AutoRecorder(ChunkStore(tmp_path,reserve=0),{'car_fingerprint':'HYUNDAI_PALISADE_LX3_HEV','route':'actual_schema'})
  recorder.update(actual_services(100.),100.)
  message=log.Event.new_message()
  message.logMonoTime=102_400_000_000
  index=message.init('qRoadEncodeIdx')
  index.timestampSof=102_350_000_000;index.timestampEof=102_360_000_000
  index.segmentNum=0;index.segmentId=0;index.frameId=10
  with log.Event.from_bytes(message.to_bytes()) as event:
    recorder.video_index(event.qRoadEncodeIdx.to_dict(),int(event.logMonoTime),100.4,'actual_schema',100_400_000_000,2_000_000_000)
  recorder.close()
  rows=[json.loads(line) for p in tmp_path.glob('*.jsonl.gz') for line in gzip.open(p,'rt',encoding='utf-8')]
  forwarded=[r for r in rows if r['kind']=='video_index']
  assert len(forwarded)==1 and forwarded[0]['index']['frameId']==10
  assert forwarded[0]['route']=='actual_schema' and forwarded[0]['source_log_mono_ns']==102_400_000_000
