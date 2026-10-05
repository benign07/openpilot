"""Worker-owned read-only msgq subscriptions, independent of HUD connections."""
import hashlib
from itertools import islice
from pathlib import Path
import threading
import time

from .automatic import AutoRecorder, ChunkStore

FIELDS = {
  'deviceState': ('started',),
  'carState': ('vEgo', 'aEgo', 'standstill', 'gearShifter', 'canValid', 'canTimeout', 'brakePressed',
               'gasPressed', 'steeringPressed', 'steeringAngleDeg', 'steeringTorque', 'leftBlinker',
               'rightBlinker', 'leftBlindspot', 'rightBlindspot', 'cruiseState', 'latEnabled',
               'steerFaultTemporary', 'steerFaultPermanent', 'buttonEvents'),
  'carControl': ('enabled', 'latActive', 'longActive', 'actuators'),
  'selfdriveState': ('enabled', 'active', 'state', 'alertText1', 'alertText2', 'alertType'),
  'radarState': ('leadOne', 'leadTwo', 'errors'),
  'longitudinalPlan': ('hasLead', 'longitudinalPlanSource', 'fcw', 'shouldStop', 'speeds', 'accels'),
}
PARAMS = ('MyDrivingMode', 'MyDrivingModeAuto', 'LongitudinalPersonality', 'TFollowGap1', 'TFollowGap2',
          'TFollowGap3', 'TFollowGap4', 'LaneChangeNeedTorque', 'AlwaysLateral', 'TurnSpeedControlMode',
          'AutoNaviSpeedCtrlMode', 'EnableRadarTracks', 'EnableCornerRadar')


def read_param(params, key):
  # This device's typed Params API does not accept the legacy encoding keyword.
  value = params.get(key)
  return value.decode('utf-8', errors='replace') if isinstance(value, bytes) else value


def selected_fields(reader, fields):
  result = {}
  for key in fields:
    value = getattr(reader, key, None)
    if key in ('gearShifter', 'state', 'longitudinalPlanSource') and value is not None:
      value = str(value)
    elif key in ('speeds', 'accels') and value is not None:
      value = list(value)
    elif key == 'errors' and value is not None:
      value = [str(error) for error in value]
    elif key == 'buttonEvents' and value is not None:
      value = [{'type': str(button.type), 'pressed': bool(button.pressed)} for button in value]
    elif hasattr(value, 'to_dict'):
      value = value.to_dict()
    result[key] = value
  return result


def panda_summary(panda):
  buses = (panda.canState0, panda.canState1, panda.canState2)
  return {
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
    sm = messaging.SubMaster(list(FIELDS))
    sockets = {name: messaging.sub_sock(name, timeout=0, conflate=False) for name in ('can', 'sendcan')}
    panda_socket = messaging.sub_sock('pandaStates', timeout=0, conflate=True)
    boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    metadata = {'boot_id': boot, 'route': None, 'car_fingerprint': None,
                'rate_hz': 5, 'can_per_key_max_hz': 10, 'panda_max_hz': 2,
                'button_trace': {'physical_input': 'LX3 bus0/0x10B; EV9 ECAN bus0-or-1/0x1CF-or-0x1AA', 'pre_s': 2, 'post_s': 8,
                                 'max_window_s': 30, 'max_traces_per_trip': 24,
                                 'max_frames_per_trace': 20000, 'can_source': 'selected_unthrottled',
                                 'host_context_max_hz': 20, 'panda_max_hz': 10},
                'physical_ecu_origin': 'not_inferred_from_bus'}
    repo = Path(__file__).resolve().parents[4]
    sources = ('openpilot/selfdrive/carrot/can_diagnostics/automatic.py', 'openpilot/selfdrive/carrot/can_diagnostics/automatic_runtime.py',
               'opendbc_repo/opendbc/car/hyundai/carcontroller.py', 'opendbc_repo/opendbc/car/hyundai/carstate.py',
               'opendbc_repo/opendbc/car/hyundai/radar_interface.py')
    metadata['source_hashes'] = {rel: hashlib.sha256((repo / rel).read_bytes()).hexdigest()
                               for rel in sources if (repo / rel).is_file()}
    dbc = repo / 'opendbc_repo/opendbc/dbc/generator/hyundai/hyundai_canfd.dbc'
    if dbc.exists():
      metadata['dbc_sha256'] = hashlib.sha256(dbc.read_bytes()).hexdigest()
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
          services[name] = {'mono_ns': int(sm.logMonoTime[name]), 'valid': bool(sm.valid[name]),
                            'data': selected_fields(sm[name], fields)}
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
