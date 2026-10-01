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
               'steerFaultTemporary', 'steerFaultPermanent', 'latEnabled', 'buttonEvents', 'datetime',
               'lx3InputState', 'lx3InputReason', 'lx3PhysicalCounter', 'lx3PhysicalCounterValid', 'lx3InputResetCount'),
  'carControl': ('enabled', 'latActive', 'longActive', 'actuators', 'lx3IdentityValid', 'lx3Generation',
                 'lx3PhysicalCounter', 'lx3Mode', 'lx3TransportEpoch'),
  'selfdriveState': ('enabled', 'active', 'state', 'lx3EngagementMode', 'lx3AckMode', 'lx3AckGeneration',
                    'lx3AckPhysicalCounter', 'lx3AckValid', 'lx3AckTransportEpoch', 'lx3AcceptedGeneration',
                    'lx3AcceptedPhysicalCounter', 'lx3AcceptedTransportEpoch', 'alertText1', 'alertText2', 'alertType'),
  'pandaStates': ('controlsAllowed', 'safetyModel', 'safetyParam', 'safetyRxChecksInvalid', 'safetyRxInvalid',
                  'safetyTxBlocked', 'faults', 'alternativeExperience', 'lx3PermissionVersion', 'lx3RequestedMode',
                  'lx3AcceptedMode', 'lx3PhysicalCounter', 'lx3RequestGeneration', 'lx3RequestAgeMs',
                  'lx3ControlsAllowed', 'lx3PermissionPhase', 'lx3TransportEpoch'),
  'radarState': ('leadOne', 'leadTwo', 'errors'),
  'longitudinalPlan': ('hasLead', 'longitudinalPlanSource', 'fcw', 'shouldStop', 'speeds', 'accels',
                       'myDrivingMode', 'tFollow'),
}
PARAMS = ('MyDrivingMode', 'MyDrivingModeAuto', 'LongitudinalPersonality', 'TFollowGap1', 'TFollowGap2',
          'TFollowGap3', 'TFollowGap4', 'LaneChangeNeedTorque', 'AlwaysLateral', 'TurnSpeedControlMode',
          'ModelTurnSpeedFactor', 'AutoNaviSpeedCtrlMode', 'EnableRadarTracks', 'EnableCornerRadar',
          'EnableSpeedTF', 'DisengageOnAccelerator')


def read_param(params, key):
  # This device's typed Params API does not accept the legacy encoding keyword.
  value = params.get(key)
  return value.decode('utf-8', errors='replace') if isinstance(value, bytes) else value


def selected_fields(reader, fields):
  result = {}
  for key in fields:
    value = getattr(reader, key, None)
    if key in ('gearShifter', 'state', 'longitudinalPlanSource', 'safetyModel', 'lx3InputState') and value is not None:
      value = str(value)
    elif key in ('speeds', 'accels') and value is not None:
      value = list(value)
    elif key in ('errors', 'faults') and value is not None:
      value = [str(error) for error in value]
    elif key == 'buttonEvents' and value is not None:
      value = [{'type': str(button.type), 'pressed': bool(button.pressed),
                'lx3PhysicalCounter': getattr(button, 'lx3PhysicalCounter', None),
                'lx3PhysicalValid': bool(getattr(button, 'lx3PhysicalValid', False))} for button in value]
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
      attempt_started = time.monotonic()
      failed = False
      try:
        self.live_loop()
      except Exception as exc:
        failed = True
        if time.monotonic() - attempt_started >= 60:
          delay = 5  # A stable minute resets previous transient-failure backoff.
        with self.lock:
          self.error = f'{type(exc).__name__}: {str(exc)[:160]}'
          self.restart_count += 1
          self.retry_after_seconds = delay
      finally:
        with self.lock:
          if self.recorder:
            try:
              self.recorder.close("recorder_restart" if failed else "server_shutdown")
            except Exception as exc:
              self.error = f'close: {type(exc).__name__}: {str(exc)[:160]}'
              try:
                self.recorder.store.abandon_open_stream()
              except Exception:
                pass  # Keep partial files; retry recovery once storage works.
      if self.shutdown.wait(delay):
        break
      delay = min(60, delay * 2)

  def consume_can(self, raw, source, decoder, now):
    direction = 'tx_requested' if source == 'sendcan' else 'rx'
    try:
      event = decoder(raw)
    except Exception:
      with self.lock:
        self.recorder.transport_event(direction, now, malformed=True)
      return
    with self.lock:
      self.recorder.transport_event(direction, now, valid=bool(event.valid))
      for frame in getattr(event, source):
        if frame.address in self.recorder.ADDRESSES:
          self.recorder.can_frame(frame.src, frame.address, bytes(frame.dat), event.logMonoTime,
                                  now, direction, event_valid=bool(event.valid))

  def live_loop(self):
    from cereal import car, messaging
    from openpilot.common.params import Params

    params = Params()
    sm = messaging.SubMaster(list(FIELDS))
    sockets = {name: messaging.sub_sock(name, timeout=0, conflate=False) for name in ('can', 'sendcan')}
    boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    metadata = {'boot_id': boot, 'route': None, 'car_fingerprint': None,
                'recorder_restart_count': self.restart_count,
                'rate_hz': 5, 'can_per_key_max_hz': 10, 'physical_ecu_origin': 'not_inferred_from_bus',
                'physical_10b_capture': 'all_received_subject_to_drain_chunk_quota_limits',
                'permission_transition_worker_max_hz': 20,
                'permission_transition_coverage': 'conflated_publications_not_all_firmware_transitions'}
    repo = Path(__file__).resolve().parents[3]
    sources = ('selfdrive/carrot/can_diagnostics/automatic.py', 'selfdrive/carrot/can_diagnostics/automatic_runtime.py',
               'opendbc_repo/opendbc/car/hyundai/carcontroller.py', 'opendbc_repo/opendbc/car/hyundai/carstate.py',
               'opendbc_repo/opendbc/car/hyundai/radar_interface.py', 'opendbc_repo/opendbc/car/hyundai/lx3_inputs.py',
               'selfdrive/selfdrived/lx3_engagement.py', 'opendbc_repo/opendbc/safety/safety/safety_hyundai_canfd.h',
               'selfdrive/car/card.py', 'selfdrive/selfdrived/selfdrived.py', 'selfdrive/controls/controlsd.py',
               'selfdrive/pandad/panda.cc', 'selfdrive/pandad/pandad.cc', 'panda/board/main_comms.h',
               'opendbc_repo/opendbc/safety/lx3_permission.h', 'opendbc_repo/opendbc/safety/safety.h')
    metadata['source_hashes'] = {rel: hashlib.sha256((repo / rel).read_bytes()).hexdigest()
                               for rel in sources if (repo / rel).is_file()}
    dbc = repo / 'opendbc_repo/opendbc/dbc/generator/hyundai/hyundai_canfd_lx3_hev.dbc'
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
      if now - last_context >= .2:
        # Read only required fields at the recorded rate. Full carState/deviceState
        # conversion on every CAN drain needlessly copies unrelated payloads.
        for name, fields in FIELDS.items():
          data = ([selected_fields(panda, fields) for panda in sm[name]] if name == 'pandaStates'
                  else selected_fields(sm[name], fields))
          services[name] = {'mono_ns': int(sm.logMonoTime[name]), 'valid': bool(sm.valid[name]), 'data': data}
        last_context = now
      else:
        # Copy just these small messages on receipt, so the 5Hz full context
        # sample does not discard a 100ms pending/accepted publication pair.
        # The worker still conflates faster SS messages; report that limitation.
        for name in AutoRecorder.PERMISSION_FIELDS:
          if sm.updated[name]:
            fields = FIELDS[name]
            data = ([selected_fields(panda, fields) for panda in sm[name]] if name == 'pandaStates'
                    else selected_fields(sm[name], fields))
            services[name] = {'mono_ns': int(sm.logMonoTime[name]), 'valid': bool(sm.valid[name]), 'data': data}
      with self.lock:
        self.recorder.update(services, now)
      # Bounded drains; lag is recorded explicitly and stale CAN is discarded.
      for name, sock in sockets.items():
        for _ in range(32):
          raw = sock.receive(non_blocking=True)
          if raw is None:
            break
          self.consume_can(raw, name, messaging.log_from_bytes, time.monotonic())
      self.shutdown.wait(.05)
