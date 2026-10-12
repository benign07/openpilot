import ast
from pathlib import Path
from openpilot.selfdrive.carrot.can_diagnostics.automatic import AutoRecorder


class Store:
  def __init__(self):self.rows=[]
  def append(self,row,now):self.rows.append(row)
  def full(self,now):return False


def recorder():
  s=Store();r=AutoRecorder(s,{'boot_id':'boot-A','car_fingerprint':'HYUNDAI_PALISADE_LX3_HEV'})
  r.state='recording'
  return r,s


def events(store,name):return [r for r in store.rows if r.get('name')==name]


def test_oem_fault_without_button_opens_pre_post_and_includes_trigger_frame():
  r,s=recorder()
  r.can_frame(2,0xCB,bytes(24),100_000_000_000,100.)
  fault=(1<<219).to_bytes(32,'little')
  r.can_frame(130,0x162,fault,100_050_000_000,100.05)
  assert r.trace is None
  r.can_frame(2,0x162,fault,100_100_000_000,100.1)
  assert r.diagnostic_trace_count==1
  assert {x['address'] for x in s.rows if x['kind']=='button_trace_can'}=={0xCB,0x162}
  assert events(s,'diagnostic_trace_trigger')[0]['reason']=='oem_fault_edge'
  assert not events(s,'physical_button_edge')


def test_spi_interval_saturation_decrease_and_epoch_are_not_false_rates():
  r,s=recorder()
  state=dict(safety_model='hyundaiCanfd',safety_param=2238,spi_checksum_errors=65530,
             lx3_authority_state={'epoch':1},controls_allowed=False)
  r.panda_snapshot([state],100_000_000_000,100.)
  state['spi_checksum_errors']=65535;r.panda_snapshot([state.copy()],100_100_000_000,100.1)
  changes=events(s,'diagnostic_trace_trigger')[-1]['changes']['spi_checksum_errors']
  assert changes['delta']==5 and changes['rate_per_s'] is None and changes['status']=='saturated_lower_bound'
  for i in range(1,10):r.panda_snapshot([state.copy()],int((100.1+i)*1e9),100.1+i)
  assert len([x for x in events(s,'diagnostic_trace_trigger') if x['reason']=='transport_counter_change'])==1
  state['spi_checksum_errors']=1;r.panda_snapshot([state.copy()],110_000_000_000,110.)
  changes=events(s,'diagnostic_trace_trigger')[-1]['changes']['spi_checksum_errors']
  assert changes['delta'] is None and changes['rate_per_s'] is None
  state['lx3_authority_state']={'epoch':2};state['spi_checksum_errors']=900
  r.panda_snapshot([state.copy()],111_000_000_000,111.)
  assert events(s,'transport_counter_baseline')[-1]['counters']['spi_checksum_errors']==900


def test_transport_storm_is_bounded_and_does_not_use_button_quota():
  r,s=recorder()
  for i in range(12001):
    now=100+i*.05
    r.panda_snapshot([dict(controls_allowed=False,safety_param=2238,tx_blocked=i)],int(now*1e9),now)
  assert r.diagnostic_trace_count==8
  assert len(events(s,'diagnostic_trace_trigger'))<=121
  # 600 s at ordinary 2 Hz plus at most 8 windows × 8 s × extra 8 Hz.
  assert len([x for x in s.rows if x['kind']=='panda_snapshot'])<=1713
  r.close_trace(701.,'test')
  r.can_frame(0,0x10B,bytes(16),701_000_000_000,701.)
  press=bytearray(16);press[10]=0x80
  r.can_frame(0,0x10B,press,701_100_000_000,701.1)
  assert r.trace and r.trace_count-r.diagnostic_trace_count==1


def test_transport_storm_cannot_exhaust_steering_fault_capture():
  r,s=recorder()
  r.diagnostic_trace_count=r.DIAGNOSTIC_TRACE_MAX_PER_TRIP
  r.last_diagnostic_trace=699.
  r.diagnostic_trigger('lane_departure',700_000_000_000,700.,left=True,right=False)
  assert r.trace and r.steering_trace_count==1 and r.diagnostic_trace_count==9
  assert events(s,'diagnostic_trace_trigger')[-1]['reason']=='lane_departure'
  r.close_trace(709.,'test')
  r.diagnostic_trigger('transport_counter_change',740_000_000_000,740.)
  assert r.trace is None


def test_haptic_request_is_passive_not_echo_or_actuation_proof():
  r,s=recorder();pulse=bytearray(32);pulse[6]=0x10
  r.can_frame(128,0x162,pulse,100_000_000_000,100.)
  assert r.trace is None
  r.can_frame(0,0x162,pulse,100_100_000_000,100.1,direction='tx_requested')
  assert r.trace and r.steering_trace_count==1
  event=events(s,'diagnostic_trace_trigger')[-1]
  assert event['reason']=='haptic_request' and event['meaning']=='host_bit_request_not_ecu_ack_or_motor_response'
  r.can_frame(0,0x162,pulse,100_200_000_000,100.2,direction='tx_requested')
  assert len([x for x in events(s,'diagnostic_trace_trigger') if x['reason']=='haptic_request'])==1


def test_steering_windows_are_bounded_separately_from_physical_buttons():
  r,s=recorder()
  for i in range(20):
    now=100+i*40.
    r.diagnostic_trigger('lane_departure',int(now*1e9),now)
    if r.trace:r.close_trace(now+9.,'test')
  assert r.steering_trace_count==8 and r.diagnostic_trace_count==8
  assert r.trace_count-r.diagnostic_trace_count==0
  r.diagnostic_trigger('transport_counter_change',901_000_000_000,901.)
  assert r.trace and r.diagnostic_trace_count==9


def test_second_steering_event_can_open_after_ten_seconds_without_transport_delay():
  r,s=recorder()
  r.diagnostic_trigger('haptic_request',100_000_000_000,100.)
  assert r.trace and r.steering_trace_count==1
  r.close_trace(108.1,'test')
  r.diagnostic_trigger('oem_fault_edge',109_000_000_000,109.)
  assert r.trace is None
  r.diagnostic_trigger('lane_departure',110_000_000_000,110.)
  assert r.trace and r.steering_trace_count==2


def test_counter_decrease_then_recovery_inside_report_interval_cannot_create_false_rate():
  r,s=recorder()
  for now,count in ((100.,1),(100.1,4),(100.2,0),(105.2,9)):
    r.panda_snapshot([dict(safety_param=2238,spi_checksum_errors=count)],int(now*1e9),now)
  change=events(s,'diagnostic_trace_trigger')[-1]['changes']['spi_checksum_errors']
  assert change['status']=='decreased_reset_or_wrap' and change['rate_per_s'] is None and change['delta'] is None


def test_first_short_counter_interval_and_missing_keys_do_not_create_rates():
  r,s=recorder()
  for now,state in ((100.,dict(spi_checksum_errors=10)),(100.1,dict(spi_checksum_errors=15)),
                    (105.2,{}),(110.3,dict(spi_checksum_errors=25))):
    r.panda_snapshot([dict(safety_param=2238,**state)],int(now*1e9),now)
  changes=[x['changes']['spi_checksum_errors'] for x in events(s,'diagnostic_trace_trigger') if x['reason']=='transport_counter_change']
  assert changes[0]['delta']==5 and changes[0]['rate_per_s'] is None
  assert changes[1]['status']=='counter_missing' and changes[2]['status']=='counter_returned'
  assert changes[2]['delta'] is None and changes[2]['rate_per_s'] is None


def test_counter_missing_and_returning_inside_summary_window_is_not_silent():
  r,s=recorder()
  for now,state in ((100.,dict(tx_blocked=1)),(100.1,dict(tx_blocked=2)),(100.2,{}),(105.2,dict(tx_blocked=2))):
    r.panda_snapshot([dict(safety_param=2238,**state)],int(now*1e9),now)
  change=events(s,'diagnostic_trace_trigger')[-1]['changes']['tx_blocked']
  assert change['status']=='counter_returned' and change['rate_per_s'] is None


def test_native_clock_offset_preserves_raw_can_and_python_sendcan_alignment():
  from types import SimpleNamespace as NS
  from openpilot.selfdrive.carrot.can_diagnostics.automatic_runtime import native_clock_offset_ns,packet_evidence_time
  fake=NS(CLOCK_BOOTTIME=7,monotonic_ns=lambda:100_000_000_000,
          clock_gettime_ns=lambda kind:160_000_000_000)
  offset=native_clock_offset_ns(fake);assert offset==60_000_000_000
  r,s=recorder()
  raw=160_000_000_000
  r.can_frame(2,0xCB,bytes(24),packet_evidence_time('can',raw,offset),100.,source_mono_ns=raw)
  r.observer_tick(100.);r.observer_tick(100.56)
  pre=[x for x in s.rows if x['kind']=='button_trace_can']
  assert pre[0]['mono_ns']==100_000_000_000 and pre[0]['source_log_mono_ns']==raw
  assert packet_evidence_time('sendcan',100_100_000_000,offset)==100_100_000_000
  raw=160_600_000_000
  r.panda_snapshot([dict(safety_param=2238)],packet_evidence_time('pandaStates',raw,offset),100.6,source_mono_ns=raw)
  row=next(x for x in s.rows if x['kind']=='panda_snapshot')
  assert row['mono_ns']==100_600_000_000 and row['source_log_mono_ns']==raw
  assert r.stale_packets==0


def test_first_authority_cause_is_recorded_before_pre_window_fills_small_chunk():
  r,s=recorder()
  for i in range(30):r.can_frame(2,0xCB,bytes(24),100_000_000_000+i*10_000_000,100+i*.01)
  r.panda_snapshot([dict(controls_allowed=False,safety_param=2238)],100_300_000_000,100.3)
  r.panda_snapshot([dict(controls_allowed=True,safety_param=2238)],100_400_000_000,100.4)
  edge=next(i for i,x in enumerate(s.rows) if x.get('name')=='panda_state_edge' and x['field']=='controls_allowed')
  pre=next(i for i,x in enumerate(s.rows) if x['kind']=='button_trace_can')
  assert edge<pre


def test_other_car_and_stale_health_never_open_diagnostic_trace():
  r,s=recorder()
  r.metadata['car_fingerprint']='other'
  r.diagnostic_trigger('oem_fault_edge',100_000_000_000,100.)
  assert not s.rows and r.trace is None
  r.metadata['car_fingerprint']='HYUNDAI_PALISADE_LX3_HEV'
  r.panda_snapshot([dict(tx_blocked=10)],99_000_000_000,100.)
  assert r.stale_packets==1 and not s.rows


def test_observer_gap_is_distinct_and_runtime_has_no_direct_panda_or_can_sender():
  r,s=recorder();r.observer_tick(100.);r.observer_tick(100.56)
  assert events(s,'diagnostic_trace_trigger')[0]['meaning']=='collector_gap_not_producer_outage_proof'
  root=Path(__file__).resolve().parents[3]
  tree=ast.parse((root/'selfdrive/carrot/can_diagnostics/automatic_runtime.py').read_text(encoding='utf-8'))
  calls=[n.func for n in ast.walk(tree) if isinstance(n,ast.Call)]
  assert not any(isinstance(n,ast.Name) and n.id in ('Panda','SpiDevice') for n in calls)
  assert not any(isinstance(n,ast.Attribute) and n.attr in ('pub_sock','can_send','can_send_many','put_bool') for n in calls)
