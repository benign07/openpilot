"""Worker-owned read-only msgq subscriptions, independent of HUD connections."""
import hashlib
from itertools import islice
from pathlib import Path
import threading
import time

from .automatic import AutoRecorder, ChunkStore

FIELDS = {
  'deviceState': ('started', 'thermalStatus', 'cpuUsagePercent', 'cpuTempC', 'memoryUsagePercent'),
  'carState': ('vEgo', 'aEgo', 'standstill', 'gearShifter', 'canValid', 'canTimeout', 'brakePressed',
               'gasPressed', 'steeringPressed', 'steeringAngleDeg', 'steeringTorque', 'leftBlinker',
               'rightBlinker', 'leftBlindspot', 'rightBlindspot', 'cruiseState', 'latEnabled',
               'steerFaultTemporary', 'steerFaultPermanent', 'buttonEvents', 'lx3Authority', 'lx3SteeringLimited'),
  'carControl': ('enabled', 'latActive', 'longActive', 'actuators', 'lx3Authority', 'manualSteeringScale'),
  'selfdriveState': ('enabled', 'active', 'state', 'alertText1', 'alertText2', 'alertType', 'lx3LongRequest', 'lx3LongRefused', 'lx3RefuseAfterSequence'),
  'radarState': ('leadOne', 'leadTwo', 'errors'),
  'longitudinalPlan': ('hasLead', 'longitudinalPlanSource', 'fcw', 'shouldStop', 'speeds', 'accels'),
  'onroadEvents': (),
}
HEALTH_SERVICES = ('pandaStates', 'peripheralState', 'managerState')
PARAMS = ('MyDrivingMode', 'MyDrivingModeAuto', 'LongitudinalPersonality', 'TFollowGap1', 'TFollowGap2',
          'TFollowGap3', 'TFollowGap4', 'LaneChangeNeedTorque', 'ManualSteerWithBlinker', 'AlwaysLateral', 'TurnSpeedControlMode',
          'AutoNaviSpeedCtrlMode', 'EnableRadarTracks', 'EnableCornerRadar', 'HardwareC3xLite', 'RecordRoadCam', 'RecordAudio')


def read_param(params, key):
  # This device's typed Params API does not accept the legacy encoding keyword.
  value = params.get(key)
  return value.decode('utf-8', errors='replace') if isinstance(value, bytes) else value


def selected_fields(reader, fields):
  result = {}
  for key in fields:
    value = getattr(reader, key, None)
    if key in ('gearShifter', 'state', 'longitudinalPlanSource', 'thermalStatus') and value is not None:
      value = str(value)
    elif key in ('cpuUsagePercent', 'cpuTempC') and value is not None:
      value = list(islice(value, 16))
    elif key in ('speeds', 'accels') and value is not None:
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


def service_snapshot(sm, name, fields):
  # These are this passive reader's observations (20 Hz maximum), not the
  # control process's frequency decision. The latter is captured as events.
  tracker = sm.freq_tracker[name]
  recent_dt = tracker.recent_avg_dt.get_average() if tracker.recent_avg_dt.count else 0
  if name == 'onroadEvents':
    flags = ('enable', 'noEntry', 'warning', 'userDisable', 'softDisable', 'immediateDisable', 'preEnable', 'permanent', 'overrideLateral', 'overrideLongitudinal')
    data = {'events': [{'name': str(event.name), **{key: bool(getattr(event, key, False)) for key in flags}}
                       for event in islice(sm[name], 64)], 'truncated': len(sm[name]) > 64}
  elif name == 'managerState':
    processes = sm[name].processes
    data = {'processes': [{'name': str(p.name)[:64], 'pid': int(p.pid), 'running': bool(p.running),
                           'shouldBeRunning': bool(p.shouldBeRunning), 'exitCode': int(p.exitCode)}
                          for p in islice(processes, 64)], 'truncated': len(processes) > 64}
  else:
    data = selected_fields(sm[name], fields)
  return {'mono_ns': int(sm.logMonoTime[name]), 'valid': bool(sm.valid[name]),
          'alive': bool(sm.alive[name]), 'observer_freq_ok': bool(sm.freq_ok[name]),
          'observer_recent_hz': 1 / recent_dt if recent_dt > 0 else None, 'data': data}


def source_metadata(app_root):
  """Stable logical keys for both flat and nested openpilot checkouts."""
  dbc_root = app_root if (app_root / 'opendbc_repo').is_dir() else app_root.parent
  sources = {rel: app_root / rel for rel in (
    'selfdrive/carrot/can_diagnostics/automatic.py', 'selfdrive/carrot/can_diagnostics/automatic_runtime.py',
    'system/manager/manager.py')}
  sources.update({rel: dbc_root / rel for rel in (
    'opendbc_repo/opendbc/car/hyundai/carcontroller.py', 'opendbc_repo/opendbc/car/hyundai/carstate.py',
    'opendbc_repo/opendbc/car/hyundai/radar_interface.py', 'opendbc_repo/opendbc/car/hyundai/hyundaicanfd.py')})
  result = {'source_hashes': {rel: hashlib.sha256(path.read_bytes()).hexdigest()
                            for rel, path in sources.items() if path.is_file()},
            'source_hashes_missing': [rel for rel, path in sources.items() if not path.is_file()]}
  dbc = dbc_root / 'opendbc_repo/opendbc/dbc/generator/hyundai/hyundai_canfd_lx3_hev.dbc'
  if dbc.is_file():
    result['dbc_sha256'] = hashlib.sha256(dbc.read_bytes()).hexdigest()
  return result


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
    from openpilot.cereal import car, messaging
    from openpilot.common.params import Params

    params = Params()
    sm = messaging.SubMaster([*FIELDS, *HEALTH_SERVICES], frequency=20)
    sockets = {name: messaging.sub_sock(name, timeout=0, conflate=False) for name in ('can', 'sendcan')}
    panda_socket = messaging.sub_sock('pandaStates', timeout=0, conflate=True)
    boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    metadata = {'boot_id': boot, 'route': None, 'car_fingerprint': None,
                'rate_hz': 5, 'can_per_key_max_hz': 10, 'panda_max_hz': 20, 'panda_periodic_max_hz': 2,
                'panda_state_changes': 'received_valid_lx3_snapshots_bypass_periodic_limit',
                'button_trace': {'physical_input': 'bus0/0x10B', 'pre_s': 2, 'post_s': 8,
                                 'max_window_s': 30, 'max_traces_per_trip': 24,
                                 'max_frames_per_trace': 20000, 'can_source': 'selected_unthrottled',
                                 'host_context_max_hz': 20, 'panda_max_hz': 20, 'panda_periodic_max_hz': 10},
                'physical_ecu_origin': 'not_inferred_from_bus',
                'sample_extensions': ['service_health_observer_v1', 'onroad_events_v1', 'device_resources_v1', 'manager_processes_v1'],
                'service_health_observer': 'passive_reader_max_20hz_not_selfdrived_frequency_check'}
    metadata.update(source_metadata(Path(__file__).resolve().parents[3]))
    with self.lock:
      self.recorder = AutoRecorder(ChunkStore(self.root), metadata)
      self.error = None
      self.retry_after_seconds = 0
    last_metadata = last_context = -100
    services = {}
    while not self.shutdown.is_set():
      sm.update(0)
      now = time.monotonic()
      if now - last_metadata >= 5:
        metadata['route'] = read_param(params, 'CurrentRoute') or None
        metadata['settings'] = {key: read_param(params, key) for key in PARAMS}
        if metadata['car_fingerprint'] is None:
          raw = params.get('CarParams')
          if raw:
            with car.CarParams.from_bytes(raw) as cp:
              metadata['car_fingerprint'] = cp.carFingerprint
        last_metadata = now
      if now - last_context >= (.05 if self.recorder.trace_active(now) else .2):
        # Read only required fields at the recorded rate. Full carState/deviceState
        # conversion on every CAN drain needlessly copies unrelated payloads.
        for name, fields in FIELDS.items():
          services[name] = service_snapshot(sm, name, fields)
        for name in HEALTH_SERVICES:
          services[name] = service_snapshot(sm, name, ())
        last_context = now
      with self.lock:
        self.recorder.update(services, now)
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
                self.recorder.can_frame(frame.src, frame.address, bytes(frame.dat), event.logMonoTime,
                                        time.monotonic(), 'tx_requested' if name == 'sendcan' else 'rx')
      raw = panda_socket.receive(non_blocking=True)
      if raw is not None:
        event = messaging.log_from_bytes(raw)
        if event.valid and event.which() == 'pandaStates':
          with self.lock:
            self.recorder.panda_snapshot([panda_summary(p) for p in islice(event.pandaStates, 4)],
                                         event.logMonoTime, time.monotonic())
      self.shutdown.wait(.05)
