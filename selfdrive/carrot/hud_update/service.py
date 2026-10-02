"""Authenticated phone requests persist; installation is armed only while parked."""
import asyncio
import hmac
import ipaddress
import json
import time
from pathlib import Path
from urllib.request import Request, urlopen

from aiohttp import web
from . import core

ACTIVE = {'waiting_parked', 'downloading', 'countdown', 'armed', 'applying', 'verifying', 'rolling_back'}
REQUIRED = {'card', 'controlsd', 'selfdrived', 'plannerd', 'radard', 'modeld', 'carrot_server'}
CONTROL_SOURCES = {'selfdrive/selfdrived/selfdrived.py', 'selfdrive/controls/controlsd.py',
                   'selfdrive/carrot/carrot_controls.py',
                   'opendbc_repo/opendbc/car/hyundai/carcontroller.py',
                   'opendbc_repo/opendbc/car/hyundai/hyundaicanfd.py'}


def requires_onroad_verification(release):
  return any(row['path'] in CONTROL_SOURCES for row in release.get('files', []))


def fetch(url, limit):
  with urlopen(Request(url, headers={'User-Agent': 'CarrotHUD-DeviceUpdater', 'Cache-Control': 'no-cache'}), timeout=20) as response:
    if not response.geturl().startswith(core.RAW): raise ValueError('Unexpected release redirect')
    raw = response.read(limit + 1)
  if len(raw) > limit: raise ValueError('Release response too large')
  return raw


class UpdateService:
  def __init__(self, app, root=core.ROOT, state_root=core.STATE_ROOT):
    self.app, self.root, self.folder = app, root, state_root
    self.config = core.load(state_root / 'config.json')
    self.state = core.load(state_root / 'state.json', {'phase': 'idle', 'message': '업데이트 확인 대기'})
    self.history = core.load(state_root / 'history.json', [])
    self.latest = core.load(state_root / 'latest.json')
    self.lock = asyncio.Lock()
    self.parked_since = None
    self.countdown_since = None
    self.health_sm = None
    self.boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    if self.state.get('phase') in ('downloading', 'countdown', 'armed'):
      self.state.update(phase='waiting_parked', message='연결 서비스 재시작 · P 정차 확인 대기')
      core.save(self.folder / 'state.json', self.state)

  def authorize(self, request):
    try:
      peer = ipaddress.ip_address(request.remote or '')
      allowed = self.config and str(peer) == self.config['phone_ip']
      token = request.headers.get('Authorization', '')
      if not allowed or not hmac.compare_digest(token, 'Bearer ' + self.config['token']):
        raise web.HTTPUnauthorized(text='기기 업데이트 연결 등록이 필요합니다.')
    except (ValueError, KeyError):
      raise web.HTTPUnauthorized(text='기기 업데이트 연결 등록이 필요합니다.')

  def public(self):
    installed = core.load(self.folder / 'installed.json', {})
    return {'ok': True, 'configured': bool(self.config), 'installed': installed, 'latest': self.latest,
            'phase': self.state.get('phase', 'idle'), 'message': self.state.get('message', ''),
            'requested_release': (self.state.get('index') or {}).get('release_id'),
            'history': self.history[-30:], 'cancel_available': self.state.get('phase') in ('waiting_parked', 'countdown')}

  def change(self, phase, message, **extra):
    self.state.update(phase=phase, message=message, **extra)
    core.save(self.folder / 'state.json', self.state)
    self.history.append({'at': time.time(), 'phase': phase, 'message': message,
                         'release_id': (self.state.get('index') or {}).get('release_id')})
    self.history = self.history[-100:]
    core.save(self.folder / 'history.json', self.history)

  async def check(self):
    index = core.validate_index(json.loads(await asyncio.to_thread(fetch, core.CHANNEL, 65536)))
    self.latest = index
    core.save(self.folder / 'latest.json', index)
    return index

  async def parked(self):
    from ..server.services.setting_safety import parked_state_error
    broker, lock = self.app.get('realtime_broker'), self.app.get('realtime_broker_poll_lock')
    if broker is None or lock is None: return False
    async with lock:
      await asyncio.to_thread(broker.poll, 0)
      return parked_state_error(broker.sm) is None

  def vehicle_matches(self):
    from cereal import car
    from openpilot.common.params import Params
    raw = Params().get('CarParams')
    if not raw: return False
    with car.CarParams.from_bytes(raw) as cp:
      return cp.carFingerprint == 'HYUNDAI_PALISADE_LX3_HEV'

  async def healthy(self):
    # The web broker does not subscribe to managerState. Own this tiny health
    # subscription on the event-loop thread, only during post-boot verification.
    if self.health_sm is None:
      from cereal import messaging
      self.health_sm = messaging.SubMaster(['managerState', 'deviceState', 'carState'])
    sm = self.health_sm
    sm.update(0)
    for name in ('managerState', 'deviceState'):
      if not sm.alive.get(name) or not sm.valid.get(name) or not 0 <= time.monotonic() - sm.logMonoTime[name] / 1e9 < 3:
        return False
    running = {p.name for p in sm['managerState'].processes if p.running}
    if not sm['deviceState'].started:
      if requires_onroad_verification(self.state.get('release') or {}):
        return None  # Control processes must actually run before claiming success.
      return {'carrot_server', 'ui'} <= running
    return (REQUIRED <= running and sm.alive.get('carState') and sm.valid.get('carState') and
            0 <= time.monotonic() - sm.logMonoTime['carState'] / 1e9 < .5 and sm['carState'].canValid)

  async def tick(self):
    async with self.lock:
      phase = self.state.get('phase')
      if phase == 'verifying':
        release = self.state['release']
        matched = all(core.sha(core.checked_path(self.root, row['path']).read_bytes()) == row['sha256'] for row in release['files'])
        health = await self.healthy()
        if matched and health is True:
          self.change('complete', '업데이트 완료 · 적용 파일과 기기 프로세스 확인됨')
        elif matched and health is None:
          if self.state.get('message') != '제어 소스 적용됨 · 차량 시작 후 프로세스·CAN 확인 대기':
            self.change('verifying', '제어 소스 적용됨 · 차량 시작 후 프로세스·CAN 확인 대기')
        else:
          if requires_onroad_verification(release) and 'onroad_verify_started_at' not in self.state:
            self.state['onroad_verify_started_at'] = time.time()
            core.save(self.folder / 'state.json', self.state)
          started = self.state.get('onroad_verify_started_at', self.state.get('applied_at', time.time()))
          if time.time() - started <= 180:
            return
          self.change('health_warning', '파일 적용됨 · 기기 정상 실행 확인 실패, PC 점검 필요')
        return
      if phase not in ('waiting_parked', 'countdown'): return
      if not await self.parked():
        self.parked_since = self.countdown_since = None
        if phase == 'countdown': self.change('waiting_parked', '차량 상태 변경 · 업데이트 예약 유지')
        return
      now = time.monotonic()
      if self.parked_since is None: self.parked_since = now
      if now - self.parked_since < 10: return
      if not self.vehicle_matches():
        self.change('failed', '차량 식별이 일치하지 않아 중단했습니다')
        return
      if phase == 'waiting_parked':
        index = self.state['index']  # Exact user-selected release, never a moving latest pointer.
        self.change('downloading', '선택한 버전 다운로드·서명·호환성 검사 중')
        raw = await asyncio.to_thread(fetch, core.bundle_url(index), core.LIMIT)
        current = core.load(self.folder / 'installed.json', {}).get('sequence', 0)
        release = core.verify_bundle(raw, index, self.config['public_key'], current)
        # Disconnect or gear change during download leaves all production files untouched.
        if not await self.parked():
          self.parked_since = None
          self.change('waiting_parked', '차량 상태 변경 · P 정차 후 다시 준비합니다')
          return
        await asyncio.to_thread(core.stage, self.root, self.folder, release)
        self.state['release'] = release
        self.state['previous_installed'] = core.load(self.folder / 'installed.json', {})
        self.countdown_since = time.monotonic()
        self.change('countdown', '검증·백업 완료 · P 상태가 유지되면 10초 뒤 재부팅합니다')
        return
      if self.countdown_since is None: self.countdown_since = now
      if now - self.countdown_since < 10: return
      await asyncio.to_thread(core.validate_staged, self.root, self.folder, self.state['release'])
      if not await self.parked():
        self.parked_since = self.countdown_since = None
        self.change('waiting_parked', '차량 상태 변경 · 업데이트 예약 유지')
        return
      self.change('armed', '재부팅 요청 · 시작 전에 준비된 파일을 적용합니다', armed_boot=self.boot, armed_at=time.time())
      proc = await asyncio.create_subprocess_exec('sudo', '-n', 'reboot', stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
      try:
        rc = await asyncio.wait_for(proc.wait(), timeout=5)
      except asyncio.TimeoutError:
        proc.kill(); await proc.wait(); rc = -1
      if rc != 0: self.change('failed', '재부팅 요청 실패 · 실행 중인 코드는 그대로입니다')

  async def run(self):
    if not self.config: return
    while True:
      try: await self.tick()
      except asyncio.CancelledError: raise
      except Exception as exc:
        if self.state.get('phase') == 'verifying':
          self.change('health_warning', '파일 적용 후 실행 확인 실패 · PC 점검 필요 (' + type(exc).__name__ + ')')
        else:
          detail = str(exc)[:240] if isinstance(exc, ValueError) else type(exc).__name__
          self.change('failed', '업데이트 준비 실패 · 기존 실행 코드 유지: ' + detail)
      await asyncio.sleep(2)


async def status(request):
  service = request.app['hud_update_service']
  service.authorize(request)
  return web.json_response(service.public())


async def action(request):
  service = request.app['hud_update_service']; service.authorize(request)
  try: body = await request.json()
  except Exception: raise web.HTTPBadRequest(text='Invalid JSON')
  if not isinstance(body, dict): raise web.HTTPBadRequest(text='Invalid JSON')
  # Do not hold a long download/reboot transaction open behind a second button press.
  if service.lock.locked(): raise web.HTTPConflict(text='업데이트 처리 중입니다. 상태를 다시 확인하세요.')
  async with service.lock:
    command = body.get('action')
    if command == 'check':
      try: await service.check()
      except Exception: raise web.HTTPBadGateway(text='GitHub 버전을 확인하지 못했습니다. 다시 시도하세요.')
    elif command == 'cancel':
      if service.state.get('phase') not in ('waiting_parked', 'countdown'):
        raise web.HTTPConflict(text='현재 단계에서는 취소할 수 없습니다.')
      service.change('cancelled', '사용자가 업데이트 예약을 취소했습니다')
      service.parked_since = service.countdown_since = None
    elif command == 'queue':
      index = service.latest
      if index is None or body.get('release_id') != index['release_id'] or body.get('bundle_sha256') != index['bundle_sha256']:
        raise web.HTTPConflict(text='버전·변경점을 다시 확인하세요.')
      if service.state.get('phase') in ACTIVE:
        if (service.state.get('index') or {}).get('bundle_sha256') == index['bundle_sha256']:
          return web.json_response(service.public())
        raise web.HTTPConflict(text='이미 다른 업데이트가 예약되어 있습니다.')
      if index['sequence'] <= core.load(service.folder / 'installed.json', {}).get('sequence', 0):
        raise web.HTTPConflict(text='이미 설치된 버전입니다.')
      service.state = {'index': index, 'requested_at': time.time()}
      service.parked_since = service.countdown_since = None
      service.change('waiting_parked', '예약됨 · P 정차·속도 0·제어 해제 10초를 기다립니다')
    else: raise web.HTTPBadRequest(text='Unknown action')
  return web.json_response(service.public())


async def lifecycle(app):
  service = UpdateService(app)
  app['hud_update_service'] = service
  task = asyncio.create_task(service.run())
  yield
  task.cancel()
  try: await task
  except asyncio.CancelledError: pass


def register(app):
  app.cleanup_ctx.append(lifecycle)
  app.router.add_get('/api/hud_update/status', status)
  app.router.add_post('/api/hud_update/action', action)
