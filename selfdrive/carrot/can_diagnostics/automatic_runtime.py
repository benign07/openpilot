"""Worker-owned read-only msgq subscriptions, independent of HUD connections."""
import hashlib
from pathlib import Path
import threading
import time

from .automatic import AutoRecorder, ChunkStore

FIELDS = {
  'deviceState': ('started',),
  'carState': ('vEgo', 'aEgo', 'standstill', 'gearShifter', 'canValid', 'canTimeout', 'brakePressed',
               'gasPressed', 'steeringPressed', 'steeringAngleDeg', 'steeringTorque', 'leftBlinker',
               'rightBlinker', 'leftBlindspot', 'rightBlindspot', 'cruiseState', 'seatbeltUnlatched',
               'steerFaultTemporary', 'steerFaultPermanent', 'latEnabled', 'buttonEvents', 'datetime'),
  'carControl': ('enabled', 'latActive', 'longActive', 'actuators'),
  'selfdriveState': ('enabled', 'active', 'state', 'lx3EngagementMode', 'alertText1', 'alertText2', 'alertType'),
  'pandaStates': ('controlsAllowed', 'safetyModel', 'safetyParam', 'safetyRxChecksInvalid', 'safetyRxInvalid',
                  'safetyTxBlocked', 'faults', 'alternativeExperience'),
  'radarState': ('leadOne', 'leadTwo', 'errors'),
  'longitudinalPlan': ('hasLead', 'longitudinalPlanSource', 'fcw', 'shouldStop', 'speeds', 'accels',
                       'myDrivingMode', 'tFollow'),
}
PARAMS = ('MyDrivingMode', 'MyDrivingModeAuto', 'LongitudinalPersonality', 'TFollowGap1', 'TFollowGap2',
          'TFollowGap3', 'TFollowGap4', 'LaneChangeNeedTorque', 'AlwaysLateral', 'TurnSpeedControlMode',
          'ModelTurnSpeedFactor', 'AutoNaviSpeedCtrlMode', 'EnableRadarTracks', 'EnableCornerRadar',
          'EnableSpeedTF')


def read_param(params, key):
  # This device's typed Params API does not accept the legacy encoding keyword.
  value = params.get(key)
  return value.decode('utf-8', errors='replace') if isinstance(value, bytes) else value


def selected_fields(reader, fields):
  result = {}
  for key in fields:
    value = getattr(reader, key, None)
    if key in ('gearShifter', 'state', 'longitudinalPlanSource', 'safetyModel') and value is not None:
      value = str(value)
    elif key in ('speeds', 'accels') and value is not None:
      value = list(value)
    elif key in ('errors', 'faults') and value is not None:
      value = [str(error) for error in value]
    elif key == 'buttonEvents' and value is not None:
      value = [{'type': str(button.type), 'pressed': bool(button.pressed)} for button in value]
    elif hasattr(value, 'to_dict'):
      value = value.to_dict()
    result[key] = value
  return result


class AutomaticController:
  def __init__(self, root='/data/community/automatic_drive'):
    self.root = Path(root)
    self.lock = threading.RLock()
    self.shutdown = threading.Event()
    self.worker = None
    self.recorder = None
    self.error = None

  def start(self):
    if self.worker is None:
      self.worker = threading.Thread(target=self.run, name='passive_drive_recorder', daemon=True)
      self.worker.start()

  def status(self):
    with self.lock:
      result = self.recorder.status() if self.recorder else {'state': 'starting'}
      if self.error:
        result.update(state='error', error=self.error)
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
    try:
      self.live_loop()
    except Exception as exc:
      with self.lock:
        self.error = f'{type(exc).__name__}: {str(exc)[:160]}'
    finally:
      with self.lock:
        if self.recorder:
          try:
            self.recorder.close()
          except Exception as exc:
            self.error = f'close: {type(exc).__name__}: {str(exc)[:160]}'

  def live_loop(self):
    from cereal import car, messaging
    from openpilot.common.params import Params

    params = Params()
    sm = messaging.SubMaster(list(FIELDS))
    sockets = {name: messaging.sub_sock(name, timeout=0, conflate=False) for name in ('can', 'sendcan')}
    boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    metadata = {'boot_id': boot, 'route': None, 'car_fingerprint': None,
                'rate_hz': 5, 'can_per_key_max_hz': 10, 'physical_ecu_origin': 'not_inferred_from_bus'}
    repo = Path(__file__).resolve().parents[3]
    sources = ('selfdrive/carrot/can_diagnostics/automatic.py', 'selfdrive/carrot/can_diagnostics/automatic_runtime.py',
               'opendbc_repo/opendbc/car/hyundai/carcontroller.py', 'opendbc_repo/opendbc/car/hyundai/carstate.py',
               'opendbc_repo/opendbc/car/hyundai/radar_interface.py', 'opendbc_repo/opendbc/car/hyundai/lx3_inputs.py',
               'selfdrive/selfdrived/lx3_engagement.py', 'opendbc_repo/opendbc/safety/safety/safety_hyundai_canfd.h')
    metadata['source_hashes'] = {rel: hashlib.sha256((repo / rel).read_bytes()).hexdigest()
                               for rel in sources if (repo / rel).is_file()}
    dbc = repo / 'opendbc_repo/opendbc/dbc/generator/hyundai/hyundai_canfd_lx3_hev.dbc'
    if dbc.exists():
      metadata['dbc_sha256'] = hashlib.sha256(dbc.read_bytes()).hexdigest()
    with self.lock:
      self.recorder = AutoRecorder(ChunkStore(self.root), metadata)
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
      if now - last_context >= .2:
        # Read only required fields at the recorded rate. Full carState/deviceState
        # conversion on every CAN drain needlessly copies unrelated payloads.
        for name, fields in FIELDS.items():
          data = ([selected_fields(panda, fields) for panda in sm[name]] if name == 'pandaStates'
                  else selected_fields(sm[name], fields))
          services[name] = {'mono_ns': int(sm.logMonoTime[name]), 'valid': bool(sm.valid[name]), 'data': data}
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
      self.shutdown.wait(.05)
