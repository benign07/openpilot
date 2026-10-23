"""Worker-owned read-only msgq subscriptions, independent of HUD connections."""
import hashlib
import json
from itertools import islice
from pathlib import Path
import shutil
import threading
import time
from collections import deque

from .automatic import AutoRecorder, ChunkStore

FIELDS = {
  'deviceState': ('started', 'startedMonoTime', 'cpuTempC', 'gpuTempC', 'memoryTempC', 'pmicTempC',
                  'maxTempC', 'thermalStatus', 'cpuUsagePercent', 'gpuUsagePercent',
                  'memoryUsagePercent', 'fanSpeedPercentDesired', 'powerDrawW', 'somPowerDrawW'),
  'carState': ('vEgo', 'aEgo', 'standstill', 'gearShifter', 'canValid', 'canTimeout', 'brakePressed',
               'vCruise', 'softHoldActive',
               'gasPressed', 'steeringPressed', 'steeringAngleDeg', 'steeringTorque', 'leftBlinker',
               'rightBlinker', 'leftBlindspot', 'rightBlindspot', 'cruiseState', 'latEnabled',
               'steerFaultTemporary', 'steerFaultPermanent', 'buttonEvents', 'lx3Authority', 'lx3SteeringLimited'),
  'carControl': ('enabled', 'latActive', 'longActive', 'actuators', 'lx3Authority',
                 'manualSteeringScale', 'manualSteeringResume', 'hudControl'),
  'modelV2': ('frameId', 'timestampEof', 'meta'),
  'selfdriveState': ('enabled', 'active', 'state', 'alertText1', 'alertText2', 'alertType', 'alertSound', 'lx3LongRequest', 'lx3LongRefused', 'lx3RefuseAfterSequence'),
  'controlsState': ('longControlState', 'forceDecel'),
  'radarState': ('leadOne', 'leadTwo', 'errors'),
  'longitudinalPlan': ('hasLead', 'longitudinalPlanSource', 'fcw', 'shouldStop', 'speeds', 'accels',
                       'aTarget', 'vTargetNow', 'jTargetNow', 'modelMonoTime', 'cruiseTarget',
                       'jerks', 'tFollow', 'desiredDistance', 'myDrivingMode'),
  'carrotMan': ('leftSec', 'desiredSpeed'),
}
# These installed publishers use Python cereal.messaging.new_message,
# including modeld.py. This tags the envelope, not timestamps inside payloads.
SERVICE_CLOCKS = {name: 'monotonic' for name in FIELDS}
PARAMS = ('MyDrivingMode', 'MyDrivingModeAuto', 'LongitudinalPersonality', 'TFollowGap1', 'TFollowGap2',
          'TFollowGap3', 'TFollowGap4', 'LaneChangeNeedTorque', 'AlwaysLateral', 'TurnSpeedControlMode',
          'ModelTurnSpeedFactor', 'AutoNaviSpeedCtrlMode', 'EnableRadarTracks', 'EnableCornerRadar',
          'HardwareC3xLite', 'DisableDM', 'SoundVolumeAdjust', 'C3xLiteBuzzerMute', 'C3xLiteHapticNotifications',
          'CanfdDebug', 'HapticFeedbackWhenSpeedCamera', 'HDPuse',
          'ApproachComfortEnabled', 'ApproachCoastStrength', 'ApproachBrakeMargin', 'ApproachStopSmooth',
          'ApproachTrafficCalm', 'ApproachFlexibleGap', 'ApproachFlexibleGapMin',
          'EnableSpeedTF', 'TFollowDecelBoost', 'DynamicTFollow', 'DynamicTFollowLC', 'StopDistanceCarrot', 'ManualSteerWithBlinker',
          'CruiseStartCurrentSpeed')
PARAMS += tuple('CruiseMaxVals'+str(i) for i in range(7)) + (
  'AChangeCostStarting', 'CruiseEcoControl', 'LongActuatorDelay', 'VEgoStopping', 'StoppingAccel',
  'LongTuningKpV', 'LongTuningKiV', 'LongTuningKf')
PARAMS += ('UseLaneLineSpeed', 'UseLaneLineCurveSpeed', 'PathOffset', 'AdjustLaneOffset',
           'LatMpcInputOffset', 'LaneLineCheck')


def recording_storage_status(root, usage=None):
  """Mirror stock loggerd retention thresholds; never change or bypass them."""
  try:
    capacity = usage if usage is not None else shutil.disk_usage(root)
    minimum_bytes, minimum_percent = 5 * 1024**3, 30
    required = max(minimum_bytes, (capacity.total * minimum_percent + 99) // 100)
    return {'status': 'low_space' if capacity.free < required else 'ok',
            'free_bytes': capacity.free, 'total_bytes': capacity.total,
            'required_free_bytes': required, 'needed_bytes': max(0, required - capacity.free),
            'meaning': 'stock_loggerd_may_delete_completed_segments_not_encoder_failure_proof'}
  except OSError:
    return {'status': 'unknown', 'meaning': 'filesystem_capacity_unavailable'}


def native_clock_offset_ns(clock=time):
  """Installed C++ CAN/pandaStates use BOOTTIME; Python sendcan uses MONOTONIC.

  Keep evidence on the collector's monotonic axis and retain each native raw
  logMonoTime. Re-read after suspend instead of assuming both clocks coincide.
  """
  boot = getattr(clock, 'CLOCK_BOOTTIME', None)
  if boot is None:
    return 0
  before = clock.monotonic_ns()
  native = clock.clock_gettime_ns(boot)
  after = clock.monotonic_ns()
  return max(0, native - (before + after) // 2)


def packet_evidence_time(stream, source_mono_ns, native_offset_ns):
  return source_mono_ns - native_offset_ns if stream in ('can', 'pandaStates') else source_mono_ns


def read_param(params, key):
  # This device's typed Params API does not accept the legacy encoding keyword.
  value = params.get(key)
  return value.decode('utf-8', errors='replace') if isinstance(value, bytes) else value


def selected_fields(reader, fields):
  result = {}
  for key in fields:
    value = getattr(reader, key, None)
    if key in ('gearShifter', 'state', 'longitudinalPlanSource', 'alertSound', 'longControlState', 'thermalStatus') and value is not None:
      value = str(value)
    elif key in ('speeds', 'accels', 'jerks', 'cpuTempC', 'gpuTempC', 'pmicTempC', 'cpuUsagePercent') and value is not None:
      value = list(value)
    elif key == 'errors' and value is not None:
      value = [str(error) for error in value]
    elif key == 'buttonEvents' and value is not None:
      value = [{'type': str(button.type), 'pressed': bool(button.pressed),
                **({k:getattr(button,k) for k in ('physical','physicalKey','durationMs','observedMonoTime')}
                   if getattr(button,'physical',False) else {})} for button in value]
    elif hasattr(value, 'to_dict'):
      value = value.to_dict()
    result[key] = value
  return result


def selected_tx_identity(frame):
  identity = getattr(frame, 'lx3Identity', None)
  if identity is None or not identity.valid:
    return None
  # Intended host identity, not a received MCU admission stamp or ECU ACK.
  return {key: int(getattr(identity, key)) for key in ('epoch', 'generation', 'axis')}


def panda_summary(panda):
  buses = (panda.canState0, panda.canState1, panda.canState2)
  result = {
    'safety_model': str(panda.safetyModel), 'safety_param': int(panda.safetyParam),
    'controls_allowed': bool(panda.controlsAllowed),
    'tx_blocked': int(panda.safetyTxBlocked),
    'rx_overflow': int(panda.rxBufferOverflow), 'tx_overflow': int(panda.txBufferOverflow),
    'spi_checksum_errors': int(panda.spiChecksumErrorCount),
    'rx_checks_invalid': bool(panda.safetyRxChecksInvalid),
    'faults': [str(fault) for fault in panda.faults],
    'buses': [{'rx': int(bus.totalRxCnt), 'rx_lost': int(bus.totalRxLostCnt),
              'errors': int(bus.totalErrorCnt), 'bus_off': bool(bus.busOff)} for bus in buses],
  }
  a = getattr(panda, 'lx3Authority', None)
  if a is not None and a.version:
    result['lx3_authority'] = a.to_dict()
    # Do not promote heartbeat age/sequence and cumulative counters into edges.
    result['lx3_authority_state'] = selected_fields(a, ('version','profile','epoch','allowed','armed','reason',
      'inputReady','config','lateralGeneration','longitudinalGeneration','lateralRevision','longitudinalRevision',
      'pendingGeneration','longPendingGeneration','oemEmergency'))
  return result


class AutomaticController:
  def __init__(self, root='/data/community/automatic_drive'):
    self.root = Path(root)
    self.lock = threading.RLock()
    self.shutdown = threading.Event()
    self.worker = None
    self.recorder = None
    self.error = None
    self.restart_count = 0
    self.retry_after_seconds = 0
    self.thermal_last = None
    self.thermal_errors = 0

  def start(self):
    if self.worker is None:
      self.worker = threading.Thread(target=self.run, name='passive_drive_recorder', daemon=True)
      self.worker.start()

  def status(self):
    with self.lock:
      result = self.recorder.status() if self.recorder else {'state': 'starting'}
      if self.error:
        result.update(state='error', error=self.error, retry_after_seconds=self.retry_after_seconds)
      result['recorder_restart_count'] = self.restart_count
      result['thermal_last'] = self.thermal_last
      result['thermal_evidence_errors'] = self.thermal_errors
      if self.recorder:
        result['recording_retention'] = self.recorder.metadata.get('recording_retention', {'status': 'unknown'})
      return result

  def close(self):
    self.shutdown.set()
    if self.worker:
      self.worker.join(timeout=5)

  def chunks(self):
    with self.lock:
      store = self.recorder.store if self.recorder else None
    # Manifests are atomically published and never modified or deleted by HTTP.
    # A large download index must not hold up the CAN reader's lock.
    return store.list_chunks() if store else []

  def run(self):
    delay = 5
    while not self.shutdown.is_set():
      started = time.monotonic()
      try:
        self.live_loop()
      except Exception as exc:
        if time.monotonic() - started >= 60:
          delay = 5
        with self.lock:
          self.error = f'{type(exc).__name__}: {str(exc)[:160]}'
          self.restart_count += 1
          self.retry_after_seconds = delay
      finally:
        with self.lock:
          if self.recorder:
            try:
              self.recorder.close()
            except Exception as exc:
              self.error = f'close: {type(exc).__name__}: {str(exc)[:160]}'
      if self.shutdown.wait(delay):
        break
      delay = min(60, delay * 2)

  def live_loop(self):
    from cereal import car, messaging
    from openpilot.common.params import Params
    from openpilot.selfdrive.controls.lib.approach_comfort import DrivingPreferences

    params = Params()
    driving_preferences = DrivingPreferences()
    sm = messaging.SubMaster(list(FIELDS))
    sockets = {name: messaging.sub_sock(name, timeout=0, conflate=False) for name in ('can', 'sendcan')}
    panda_socket = messaging.sub_sock('pandaStates', timeout=0, conflate=True)
    evidence_socket = messaging.sub_sock('logMessage', timeout=0, conflate=False)
    # loggerd writes qRoadEncodeIdx into qlog/rlog, but does not publish it.
    # The PC imports that completed log alongside qcamera; no dead live socket.
    boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    metadata = {'boot_id': boot, 'route': None, 'car_fingerprint': None,
                'device_dongle_id': read_param(params, 'DongleId'),
                'rate_hz': 5, 'can_per_key_max_hz': 10, 'panda_max_hz': 20, 'panda_periodic_max_hz': 2,
                'host_tx_identity_scope': 'sendcan_intent_not_native_admission_or_ecu_ack',
                'panda_state_changes': 'received_valid_lx3_snapshots_bypass_periodic_limit',
                'button_trace': {'physical_input': 'bus0/0x10B', 'pre_s': 2, 'post_s': 8,
                                 'max_window_s': 30, 'max_traces_per_trip': 24,
                                 'max_frames_per_trace': 20000, 'can_source': 'selected_unthrottled',
                                 'host_context_max_hz': 20, 'panda_max_hz': 20, 'panda_periodic_max_hz': 10},
                'physical_ecu_origin': 'not_inferred_from_bus'}
    metadata['diagnostic_trace'] = {'triggers': ['oem_fault_edge', 'panda_authority_edge',
                                                'transport_counter_change', 'observer_gap', 'blindspot_warning_candidate',
                                                'adas_setting_candidate_edge'],
                                    'max_extra_traces_per_trip': 8, 'min_new_window_interval_s': 30,
                                    'spi_scope': 'existing_health_aggregate_only_not_stage_reasons',
                                    'echo_scope': 'panda_tx_confirmation_not_receiving_ecu_ack'}
    metadata['adas_state_evidence'] = {
      'schema': 1, 'source': 'selected_raw_CAN_fields_in_installed_LX3_DBC',
      'addresses': [f'0x{address:03X}' for address in AutoRecorder.ADAS_STATE_FIELDS],
      'maximum_observations_per_second': AutoRecorder.ADAS_OBSERVATIONS_PER_SECOND,
      'settings_windows_per_trip': AutoRecorder.SETTINGS_TRACE_MAX_PER_TRIP,
      'settings_window_interval_s': AutoRecorder.SETTINGS_TRACE_INTERVAL,
      'classification': 'dbc_candidate_not_setting_ack_or_cause',
      'integrity': 'not_validated_by_recorder',
      'ordering': 'equal_packet_timestamps_keep_collector_frame_order_not_per_frame_hardware_time',
      'coverage': 'changed_fields_not_every_CAN_frame; raw_rlog_remains_independent_source'}
    metadata['longitudinal_evidence'] = {'schema': 1, 'producer_clock': 'monotonic',
                                       'source': 'existing_logMessage_and_rlog', 'maximum_frame_bytes': 32768,
                                       'comparison': 'same_filter_state_ceiling_not_alternative_command_or_placebo_proof'}
    metadata['blindspot_trace'] = {'pre_s': 2, 'post_s': 8, 'max_extra_windows_per_trip': 4,
                                 'min_new_window_interval_s': 30, 'raw_rear_indicator_address': '0x1BA',
                                 'trigger': 'fresh_same_side_blinker_and_object_not_confirmed_warning_or_haptic',
                                 'unknown_address_search': 'full_rlog_on_PC_not_all_bus_python_capture'}
    metadata['clock_contract'] = {'version': 2, 'evidence': 'host_monotonic',
                                 'native_raw': 'source_log_mono_ns',
                                 'normalized_native_streams': ['can', 'pandaStates'],
                                 'nested_service_timestamps': 'producer_logMonoTime_unchanged_source_clock',
                                 'sendcan': 'python_monotonic_unchanged'}
    metadata['native_clock_offset_ns'] = native_clock_offset_ns()
    repo = Path(__file__).resolve().parents[3]
    sources = ('selfdrive/carrot/can_diagnostics/automatic.py', 'selfdrive/carrot/can_diagnostics/automatic_runtime.py',
               'opendbc_repo/opendbc/car/hyundai/carcontroller.py', 'opendbc_repo/opendbc/car/hyundai/carstate.py',
               'opendbc_repo/opendbc/car/hyundai/hyundaicanfd.py',
               'opendbc_repo/opendbc/car/hyundai/radar_interface.py', 'selfdrive/controls/controlsd.py',
               'selfdrive/carrot/manual_steering.py', 'cereal/car.capnp',
               'selfdrive/carrot/carrot_functions.py', 'selfdrive/controls/lib/approach_comfort.py',
               'selfdrive/controls/lib/flexible_gap.py', 'selfdrive/controls/lib/longitudinal_planner.py',
               'selfdrive/controls/lib/longitudinal_mpc_lib/long_mpc.py', 'selfdrive/controls/beep.py',
               'selfdrive/controls/lib/lateral_planner.py', 'selfdrive/controls/lib/lane_planner_2.py')
    metadata['source_hashes'] = {rel: hashlib.sha256((repo / rel).read_bytes()).hexdigest()
                               for rel in sources if (repo / rel).is_file()}
    dbc = repo / 'opendbc_repo/opendbc/dbc/generator/hyundai/hyundai_canfd_lx3_hev.dbc'
    if dbc.exists():
      metadata['dbc_sha256'] = hashlib.sha256(dbc.read_bytes()).hexdigest()
    with self.lock:
      self.recorder = AutoRecorder(ChunkStore(self.root, rolling=True), metadata)
      self.error = None
      self.retry_after_seconds = 0
    last_metadata = last_context = last_storage = -100
    last_thermal = -100
    thermal_recent = deque(maxlen=120)
    thermal_at_start = None
    thermal_emitted = False
    thermal_started_at_first_sample = None
    services = {}
    while not self.shutdown.is_set():
      sm.update(0)
      now = time.monotonic()
      native_offset_ns = native_clock_offset_ns()
      # Route identity must not wait for the five-second settings refresh.
      # This tmpfs read runs in the existing passive worker, never controlsd.
      current_route = read_param(params, 'CurrentRoute') or None
      try:
        if now - last_thermal >= 1 and sm.valid['deviceState'] and sm.alive['deviceState'] and 0 <= now - sm.logMonoTime['deviceState'] / 1e9 < 3:
          sample = {'mono_ns': int(sm.logMonoTime['deviceState']), **selected_fields(sm['deviceState'], FIELDS['deviceState'])}
          json.dumps(sample, allow_nan=False)
          if thermal_started_at_first_sample is None:
            thermal_started_at_first_sample = sample['started']
          thermal_recent.append(sample)
          with self.lock:
            self.thermal_last = sample
          if sample['started'] and thermal_at_start is None:
            thermal_at_start = list(thermal_recent)
          last_thermal = now
      except Exception:
        self.thermal_errors += 1  # No additional fsync or failure of the CAN reader.
      with self.lock:
        metadata['route'] = current_route
        if abs(native_offset_ns - metadata['native_clock_offset_ns']) > 2_000_000:
          before = metadata['native_clock_offset_ns']
          metadata['native_clock_offset_ns'] = native_offset_ns
          if self.recorder.state == 'recording':
            self.recorder.event('native_clock_offset_changed', now, before_ns=before, after_ns=native_offset_ns,
                                meaning='clock_mapping_changed_not_producer_outage_proof')
        self.recorder.observer_tick(now)
      if now - last_metadata >= 5:
        metadata['settings'] = {key: read_param(params, key) for key in PARAMS}
        metadata['settings']['driving_preferences'] = driving_preferences.read()
        if metadata['car_fingerprint'] is None:
          raw = params.get('CarParams')
          if raw:
            with car.CarParams.from_bytes(raw) as cp:
              metadata['car_fingerprint'] = cp.carFingerprint
        last_metadata = now
      if now - last_storage >= 30:
        metadata['recording_retention'] = recording_storage_status(self.root)
        with self.lock:
          if self.recorder.state == 'recording':
            self.recorder.event('recording_retention', now, **metadata['recording_retention'])
        last_storage = now
      if now - last_context >= (.05 if self.recorder.trace_active(now) else .2):
        # Read only required fields at the recorded rate. Full carState/deviceState
        # conversion on every CAN drain needlessly copies unrelated payloads.
        for name, fields in FIELDS.items():
          services[name] = {'mono_ns': int(sm.logMonoTime[name]), 'valid': bool(sm.valid[name]),
                            'source_clock': SERVICE_CLOCKS[name],
                            'alive': bool(sm.alive[name]),
                            'data': selected_fields(sm[name], fields)}
        last_context = now
      with self.lock:
        self.recorder.update(services, now)
        if not thermal_emitted and thermal_at_start is not None and self.recorder.state == 'recording':
          try:
            self.recorder.event('boot_thermal_samples', now, samples=thermal_at_start,
                                started_at_first_sample=thermal_started_at_first_sample, restart_count=self.restart_count,
                                scope='collector_start_to_first_onroad_not_kernel_boot_or_causal_proof')
            thermal_emitted = True
          except Exception:
            self.thermal_errors += 1
      # Bounded drains; lag is recorded explicitly and stale CAN is discarded.
      for name, sock in sockets.items():
        for _ in range(32):
          raw = sock.receive(non_blocking=True)
          if raw is None:
            break
          event = messaging.log_from_bytes(raw)
          if not event.valid:
            continue
          with self.lock:
            for frame in getattr(event, name):
              if frame.address in self.recorder.ADDRESSES:
                source_ns = int(event.logMonoTime)
                self.recorder.can_frame(frame.src, frame.address, bytes(frame.dat),
                                        packet_evidence_time(name, source_ns, native_offset_ns), time.monotonic(),
                                        'tx_requested' if name == 'sendcan' else 'rx', source_mono_ns=source_ns,
                                        identity=selected_tx_identity(frame) if name == 'sendcan' else None)
      raw = panda_socket.receive(non_blocking=True)
      if raw is not None:
        event = messaging.log_from_bytes(raw)
        if event.valid and event.which() == 'pandaStates':
          with self.lock:
            source_ns = int(event.logMonoTime)
            self.recorder.panda_snapshot([panda_summary(p) for p in islice(event.pandaStates, 4)],
                                         packet_evidence_time('pandaStates', source_ns, native_offset_ns),
                                         time.monotonic(), source_mono_ns=source_ns)
      # Existing logmessaged remains the sole publisher. This reader neither
      # changes the CAN drain budget nor conflates away short launch decisions.
      for _ in range(16):
        raw = evidence_socket.receive(non_blocking=True)
        if raw is None:
          break
        event = messaging.log_from_bytes(raw)
        if not event.valid or event.which() != 'logMessage':
          continue
        text = str(event.logMessage)
        if len(text) > 65536 or not any(key in text for key in ('lx3_longitudinal_evidence', 'lx3_lateral_margin')):
          continue
        try:
          message = json.loads(text).get('msg', {})
          row = message.get('lx3_longitudinal_evidence')
          lateral = message.get('lx3_lateral_margin')
        except (ValueError, AttributeError):
          continue
        with self.lock:
          if row is not None:
            self.recorder.longitudinal_evidence(row, int(event.logMonoTime), time.monotonic())
          if lateral is not None:
            self.recorder.lateral_evidence(lateral, int(event.logMonoTime), time.monotonic())
      self.shutdown.wait(.05)
