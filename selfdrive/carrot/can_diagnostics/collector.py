"""Single-owner subscriber thread; this module never opens a CAN publisher."""
from __future__ import annotations

import threading
import time
from pathlib import Path

from .analysis import DbcIndex
from .core import Capture
from .validation import DrivingValidator

CAR_FIELDS = ('vEgo', 'vEgoRaw', 'aEgo', 'standstill', 'gearShifter', 'canValid', 'canTimeout',
              'brakePressed', 'brake', 'gasPressed', 'gas', 'steeringAngleDeg', 'steeringTorque',
              'leftBlinker', 'rightBlinker', 'leftBlindspot', 'rightBlindspot', 'cruiseState')


def installed_dbc_paths():
  dbc = Path(__file__).resolve().parents[3] / 'opendbc_repo/opendbc/dbc'
  generated = dbc / 'hyundai_canfd_lx3_hev_generated.dbc'
  # The source definitions are useful annotations when generation hasn't run.
  return [generated] if generated.exists() else sorted((dbc / 'generator/hyundai').glob('*lx3*.dbc'))


def runtime_database():
  """Load definitions and checksum callbacks from the installed parser, without a CarInterface."""
  paths = installed_dbc_paths()
  index = DbcIndex(paths)
  try:
    from opendbc.car.structs import CarParams
    from opendbc.car.hyundai.hyundaicanfd import CanBus
    from opendbc.car.hyundai.values import DBC as PLATFORM_DBC
    from opendbc.car import Bus
    from opendbc.can.dbc import DBC
    from opendbc import DBC_PATH
    raw = Path('/data/params/d/CarParams').read_bytes()
    with CarParams.from_bytes(raw) as cp:
      if cp.carFingerprint != 'HYUNDAI_PALISADE_LX3_HEV':
        index.runtime_info = {'status': 'unsupported_vehicle', 'vehicle': cp.carFingerprint,
                              'note': 'LX3 HEV 정의를 다른 차량에 확정 매칭하지 않습니다.'}
        index.messages.clear()
        return index
      buses = CanBus(cp)
      pt_name = PLATFORM_DBC[cp.carFingerprint][Bus.pt]
      radar_name = PLATFORM_DBC[cp.carFingerprint].get(Bus.radar)
      pt_path = Path(DBC_PATH) / (pt_name + '.dbc')
      paths = [pt_path]
      if radar_name and (Path(DBC_PATH) / (radar_name + '.dbc')).exists():
        paths.append(Path(DBC_PATH) / (radar_name + '.dbc'))
      index = DbcIndex(paths)
      index.bus_bindings = {b: [pt_path.name] for b in (buses.ECAN, buses.ACAN, buses.CAM)}
      if len(paths) > 1:
        index.bus_bindings[buses.ACAN].append(paths[1].name)
      index.runtime_info = {'status': 'runtime_configured', 'vehicle': cp.carFingerprint,
                            'bus_scope': {'ECAN': buses.ECAN, 'ACAN': buses.ACAN, 'CAM': buses.CAM},
                            'note': '버스 범위는 현재 CarParams와 CanBus 설정 기준이며 개별 신호의 의미 검증과는 별개입니다.'}
    for path in paths:
      native = DBC(str(path))
      for address, definitions in index.messages.items():
        for definition in definitions:
          if definition['dbc'] != path.name or address not in native.msgs:
            continue
          for signal in definition['signals']:
            spec = native.msgs[address].sigs.get(signal['name'])
            if spec is None:
              continue
            signal['counter_configured'] = spec.type == 1
            signal['checksum_configured'] = spec.calc_checksum is not None
            if spec.calc_checksum:
              index.checksum_functions[path.name, address, signal['name']] = (
                lambda payload, addr=address, sig=spec: sig.calc_checksum(addr, sig, bytearray(payload)))
  except Exception as exc:
    index.runtime_info = {'status': 'runtime_binding_unavailable', 'reason': str(exc)[:200],
                          'note': '현재 버스 바인딩을 확인할 수 없어 DBC 정의는 참고 정보로만 표시합니다.'}
  return index


class Controller:
  def __init__(self, root='/data/community/can_diagnostics', demo=False):
    self.capture = Capture(root, DbcIndex(installed_dbc_paths()))
    self.capture.metadata = {'demo': demo}
    self.lock = threading.RLock()
    self.thread = None
    self.shutdown = threading.Event()
    self.demo = demo

  def ensure_worker(self):
    with self.lock:
      self.capture.last_client = time.monotonic()
      if self.shutdown.is_set():
        return
      if not self.thread or not self.thread.is_alive():
        self.thread = threading.Thread(target=self._run, name='carrot-can-diagnostics', daemon=True)
        self.thread.start()

  def status(self):
    self.ensure_worker()
    with self.lock:
      result = self.capture.status()
      result['demo'] = self.demo
      return result

  def action(self, kind, test_id=None):
    self.ensure_worker()
    with self.lock:
      if kind == 'start':
        self.capture.start(test_id)
      elif kind == 'mark':
        self.capture.mark()
      elif kind == 'stop':
        driving = self.capture.active() and self.capture.session['test_id'] == 'drive'
        self.capture.finish('사용자가 기록을 마쳤습니다.', completed=driving)
      elif kind == 'marker':
        self.capture.marker(test_id)
      result = self.capture.status()
      result['demo'] = self.demo
      return result

  def close(self):
    self.shutdown.set()
    if self.thread:
      self.thread.join(timeout=3)
    with self.lock:
      self.capture.finish('웹 서버가 종료되어 진단을 중단했습니다.', completed=False)

  def _run(self):
    try:
      if self.demo:
        self._demo_loop()
      else:
        self._live_loop()
    except Exception as exc:
      with self.lock:
        self.capture.error = f'CAN 수집기 오류: {type(exc).__name__}: {str(exc)[:180]}'
        try:
          self.capture.finish('수집 오류로 중단했습니다. 원시 기록은 보관됩니다.', completed=False)
        except Exception:
          for stream in (self.capture.raw, self.capture.events):
            if stream:
              try:
                stream.close()
              except OSError:
                pass
          self.capture.raw = self.capture.events = None
          if self.capture.session:
            self.capture.session['state'] = 'stopped'

  def _live_loop(self):
    from cereal import messaging
    database = runtime_database()
    with self.lock:
      self.capture.dbc = database
      self.capture.validator = DrivingValidator(database)
      boot = Path('/proc/sys/kernel/random/boot_id')
      if boot.exists():
        self.capture.metadata['boot_id'] = boot.read_text().strip()
    # All msgq sockets are created, read and destroyed in this worker thread.
    sockets = {name: messaging.sub_sock(name, conflate=name != 'can', timeout=0)
               for name in ('can', 'carState', 'selfdriveState', 'radarState', 'carControl', 'longitudinalPlan', 'modelV2')}
    self.capture.error = None
    last_context_read = 0.0
    while not self.shutdown.is_set():
      now = time.monotonic()
      with self.lock:
        if not self.capture.active() and now - self.capture.last_client > 30:
          return
      context_services = ('carState', 'selfdriveState', 'radarState', 'carControl', 'longitudinalPlan', 'modelV2') if now - last_context_read >= .05 else ()
      if context_services:
        last_context_read = now
      for service in context_services:
        raw = sockets[service].receive(non_blocking=True)
        if raw is None:
          continue
        event = messaging.log_from_bytes(raw)
        timestamp = event.logMonoTime / 1e9
        reader = getattr(event, service)
        if service == 'modelV2':
          leads = [lead.to_dict() for lead in list(event.modelV2.leadsV3)[:3]]
          data = {'leadsV3': [{k: lead.get(k) for k in ('prob', 'x', 'y', 'v', 'a', 't')} for lead in leads]}
        elif service == 'carState':
          data = {key: getattr(reader, key) for key in CAR_FIELDS if key not in ('gearShifter', 'cruiseState')}
          data.update(gearShifter=str(reader.gearShifter), cruiseState=reader.cruiseState.to_dict())
        elif service == 'selfdriveState':
          data = {'enabled': reader.enabled, 'active': reader.active, 'state': str(reader.state)}
        elif service == 'radarState':
          data = {lead: {k: getattr(getattr(reader, lead), k) for k in ('status', 'dRel', 'yRel', 'vRel', 'aRel')}
                  for lead in ('leadOne', 'leadTwo')}
        elif service == 'carControl':
          actuators = reader.actuators.to_dict()
          data = {'enabled': reader.enabled, 'latActive': reader.latActive, 'longActive': reader.longActive,
                  'actuators': {key: actuators.get(key) for key in ('accel', 'torque', 'steeringAngleDeg', 'longControlState')}}
        elif service == 'longitudinalPlan':
          data = {'speeds': list(reader.speeds), 'accels': list(reader.accels), 'hasLead': reader.hasLead,
                  'longitudinalPlanSource': str(reader.longitudinalPlanSource), 'fcw': reader.fcw, 'shouldStop': reader.shouldStop}
        with self.lock:
          self.capture.telemetry.update({service: data, service + '_received': timestamp,
                                         service + '_received_ns': event.logMonoTime,
                                         service + '_valid': bool(event.valid)})
      with self.lock:
        self.capture.tick()
      backlog = True
      for packet in range(16):
        raw = sockets['can'].receive(non_blocking=True)
        if raw is None:
          backlog = False
          break
        event = messaging.log_from_bytes(raw)
        if not event.valid:
          continue
        with self.lock:
          if self.capture.active() and time.monotonic() - event.logMonoTime / 1e9 > .5:
            self.capture.finish('CAN 수집 지연이 0.5초를 넘어 기록을 중단했습니다.', completed=False)
          for frame in event.can:
            self.capture.receive_frame(frame.src, frame.address, bytes(frame.dat), event.logMonoTime)
      with self.lock:
        self.capture.tick()
      # Refresh decoded context between bounded batches, even while catching up.
      self.shutdown.wait(0 if backlog else .01)

  def _demo_loop(self):
    # Explicit demo data for UI development. Never available through production routes.
    counter = 0
    while not self.shutdown.wait(.02):
      now = time.monotonic()
      with self.lock:
        if not self.capture.active() and now - self.capture.last_client > 30:
          return
        active = self.capture.active() and self.capture.phase()[1] == 'active'
        self.capture.telemetry = {
          'carState': {'vEgo': 0.0, 'standstill': True, 'gearShifter': 'park', 'canValid': True,
                       'brakePressed': active, 'leftBlinker': active, 'rightBlinker': active},
          'selfdriveState': {'enabled': False, 'active': False},
          'carState_valid': True, 'selfdriveState_valid': True,
          'carState_received': now, 'selfdriveState_received': now,
        }
        self.capture.receive_frame(1, 0x123, bytes([int(active) << 2, counter % 256]), int(now * 1e9))
        self.capture.tick()
        counter += 1
