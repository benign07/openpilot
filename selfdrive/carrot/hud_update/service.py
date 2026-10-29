"""Prefetch signed updates independently; installation is armed only while parked."""
import asyncio
import hmac
import ipaddress
import json
import os
import time
import uuid
from pathlib import Path
from urllib.request import Request, urlopen

from aiohttp import web
from . import core, prefetch

ACTIVE = {'waiting_parked', 'downloading', 'countdown', 'armed', 'applying', 'verifying', 'rolling_back'}
REQUIRED = {'card', 'controlsd', 'selfdrived', 'plannerd', 'radard', 'modeld', 'carrot_server'}
ONROAD_HEALTH_SECONDS = 30
ONROAD_HEALTH_DEADLINE = 180
PREFETCH_INTERVAL = 15*60
PREFETCH_RETRY = 60


def requires_onroad_verification(release):
  # Only static web assets can be verified while parked. All permitted Python
  # code may affect the driving process tree, including future Carrot modules.
  return any(not (row['path'].startswith('selfdrive/carrot/web/') and
                  row['path'].endswith(('.js', '.css', '.html', '.json'))) for row in release.get('files', []))


def fetch(url, limit):
  # read1 performs at most one underlying read, so a trickling peer cannot
  # hide an unbounded loop inside read(n). One socket operation is <=10s.
  deadline = time.monotonic()+min(90., 20.+limit/131072.)
  chunks, size = [], 0
  with urlopen(Request(url, headers={'User-Agent': 'CarrotHUD-DeviceUpdater', 'Cache-Control': 'no-cache'}), timeout=10) as response:
    if not response.geturl().startswith(core.RAW): raise ValueError('Unexpected release redirect')
    while True:
      if time.monotonic() >= deadline: raise ValueError('Release download deadline exceeded')
      chunk = response.read1(min(65536, limit+1-size))
      if not chunk: break
      chunks.append(chunk); size += len(chunk)
      if size > limit: raise ValueError('Release response too large')
  return b''.join(chunks)


class UpdateService:
  def __init__(self, app, root=core.ROOT, state_root=core.STATE_ROOT):
    self.app, self.root, self.folder = app, root, state_root
    self.config = core.load(state_root / 'config.json')
    self.state = core.load(state_root / 'state.json', {'phase': 'idle', 'message': '업데이트 확인 대기'})
    self.history = core.load(state_root / 'history.json', [])
    self.latest = core.load(state_root / 'latest.json')
    self.lock = asyncio.Lock()
    self.cache = prefetch.BundleCache(state_root)
    self.prefetch_block = prefetch.validate_block(prefetch.read_metadata(state_root/'prefetch_block.json'))
    self.auto_policy = prefetch.read_metadata(state_root/'automatic.json')
    self.policy_invalid = self.auto_policy is None and (state_root/'automatic.json').exists()
    if (not isinstance(self.auto_policy, dict) or self.auto_policy.get('schema') != 1 or
        type(self.auto_policy.get('enabled',False)) is not bool or
        type(self.auto_policy.get('skipped_sequence',0)) is not int or self.auto_policy.get('skipped_sequence',0)<0):
      self.auto_policy = {}
      self.policy_invalid = (state_root/'automatic.json').exists()
    self.prefetch_state = {'phase': 'idle', 'message': '미리 다운로드 대기', 'ready': False}
    self.prefetch_generation = 0
    self.prefetched_digest = None
    self.prefetch_due = 0.
    self.wifi_was_connected = False
    self.prefetch_not_before = 0.
    self.auto_driven = False
    self.drive_session_onroad = False
    self.drive_state_at = 0.
    self.parked_since = None
    self.countdown_since = None
    self.health_sm = None
    self.health_identity = None
    self.health_mode = 'unknown'
    self.onroad_observed = False
    self.onroad_verify_started_mono = None
    self.onroad_healthy_since = None
    self.onroad_healthy_identity = None
    self.boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    if self.state.get('phase') == 'verifying' and self.state.get('onroad_verify_boot') == self.boot:
      started = self.state.get('onroad_verify_started_mono')
      if type(started) in (int, float) and 0 <= started <= time.monotonic():
        self.onroad_verify_started_mono = started
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
    rollback = {'available': False}
    if self.config and self.state.get('phase') not in ACTIVE:
      try:
        candidate = core.prepare_rollback(self.root, self.folder, installed, self.config['public_key'])
        rollback = {'available': True, 'installed_release_id': installed['release_id'],
                    'target_release_id': candidate['restored_release_id']}
      except Exception:
        pass
    return {'ok': True, 'configured': bool(self.config), 'installed': installed, 'latest': self.latest,
            'phase': self.state.get('phase', 'idle'), 'message': self.state.get('message', ''),
            'requested_release': (self.state.get('index') or {}).get('release_id'),
            'requested_bundle_sha256': (self.state.get('index') or {}).get('bundle_sha256'),
            'automatic_request': self.state.get('automatic') is True,
            'history': self.history[-30:], 'rollback': rollback,
            'automatic': {'supported': True, 'enabled': self.auto_policy.get('enabled') is True,
                          'wifi_connected': self.wifi_was_connected, **self.prefetch_state},
            'cancel_available': self.state.get('phase') in ('waiting_parked', 'downloading', 'countdown')}

  def save_auto_policy(self, **values):
    policy = {**self.auto_policy, **values, 'schema': 1}
    if self.policy_invalid:
      original=self.folder/'automatic.json'
      core.atomic(self.folder/('automatic.invalid-'+uuid.uuid4().hex+'.json'),original.read_bytes())
    core.save(self.folder/'automatic.json', policy)
    self.auto_policy = policy
    self.policy_invalid=False

  def request_identity(self):
    return self.state.get('request_id') or core.sha(core.canonical({
      'index': self.state.get('index'), 'requested_at': self.state.get('requested_at'),
      'operation': self.state.get('operation')}))

  def cancelled(self, generation):
    return (generation != self.prefetch_generation or
            self.auto_policy.get('cancelled_request') == self.request_identity() or
            (self.state.get('automatic') and self.auto_policy.get('enabled') is not True) or
            self.state.get('phase') == 'cancelled')

  def set_automatic(self, enabled):
    if type(enabled) is not bool: raise web.HTTPBadRequest(text='enabled must be boolean')
    self.save_auto_policy(enabled=enabled)
    self.prefetch_generation += 1
    self.prefetch_due = 0.
    if not enabled and self.state.get('automatic') and self.state.get('phase') in ('waiting_parked','downloading','countdown'):
      self.change('cancelled', '자동 업데이트 꺼짐 · 자동 설치 예약 취소')
      self.parked_since = self.countdown_since = None

  def cancel(self):
    if self.state.get('phase') not in ('waiting_parked', 'downloading', 'countdown'):
      raise web.HTTPConflict(text='현재 단계에서는 취소할 수 없습니다.')
    self.save_auto_policy(suppressed_bundle=(self.state.get('index') or {}).get('bundle_sha256'),
                          skipped_sequence=max(self.auto_policy.get('skipped_sequence',0),
                                               (self.state.get('index') or {}).get('sequence',0)),
                          cancelled_request=self.request_identity())
    self.prefetch_generation += 1
    self.change('cancelled', '사용자가 업데이트 예약을 취소했습니다 · 같은 버전 자동 재예약 안 함')
    self.parked_since = self.countdown_since = None

  def observe_drive(self, sm):
    try:
      now = time.monotonic()
      if not (sm.alive['deviceState'] and sm.valid['deviceState'] and 0 <= now-sm.logMonoTime['deviceState']/1e9 < 3.):
        self.drive_state_at = 0.
        self.auto_driven = self.drive_session_onroad = False
        return
      self.drive_state_at = now
      if not sm['deviceState'].started:
        self.auto_driven = self.drive_session_onroad = False
        return
      if not self.drive_session_onroad:
        self.auto_driven = False
        self.drive_session_onroad = True
      if (sm.alive['carState'] and sm.valid['carState'] and 0 <= now-sm.logMonoTime['carState']/1e9 < .5 and
          sm['carState'].canValid and sm['carState'].vEgo > 1.):
        self.auto_driven = True
    except (AttributeError, KeyError, TypeError, ValueError):
      self.drive_state_at = 0.
      self.auto_driven = self.drive_session_onroad = False

  async def wifi(self):
    broker, lock = self.app.get('realtime_broker'), self.app.get('realtime_broker_poll_lock')
    if broker is None or lock is None: return False
    async with lock:
      await asyncio.to_thread(broker.poll, 0)
      self.observe_drive(broker.sm)
      return prefetch.wifi_connected(broker.sm, time.monotonic())

  async def prefetch_tick(self):
    # This loop owns all network downloads. Never hold the installation lock
    # across network I/O, and never stage/backup/arm from this worker.
    pending = (self.state.get('phase') == 'waiting_parked' and self.state.get('operation') != 'rollback')
    automatic = (self.auto_policy.get('enabled') is True and
                 self.state.get('phase', 'idle') in ('idle', 'complete', 'cancelled'))
    if not pending and not automatic: return
    if automatic:
      connected = await self.wifi()
      rising = connected and not self.wifi_was_connected
      self.wifi_was_connected = connected
      if not connected:
        self.prefetch_state.update(message='Wi-Fi 연결 대기')
        return
      if rising: self.prefetch_due = max(self.prefetch_not_before, min(self.prefetch_due, time.monotonic()))
    index = dict(self.state['index']) if pending else None
    if pending and self.prefetched_digest == index.get('bundle_sha256'): return
    if time.monotonic() < self.prefetch_due: return
    generation = self.prefetch_generation
    self.prefetch_due = time.monotonic()+PREFETCH_INTERVAL
    self.prefetch_not_before = time.monotonic()+PREFETCH_RETRY
    self.prefetch_state = {'phase': 'checking', 'message': '게시 버전 확인 중', 'ready': False}
    if index is None:
      index = core.validate_index(json.loads(await asyncio.to_thread(fetch, core.CHANNEL, 65536)))
    previous = core.load(self.folder/'installed.json', {})
    sequence = previous.get('sequence', 0)
    if index['sequence'] <= sequence:
      self.prefetch_state = {'phase': 'current', 'message': '게시된 최신 버전 사용 중', 'ready': False}
      return
    if automatic and (self.auto_policy.get('suppressed_bundle') == index['bundle_sha256'] or
                      index['sequence'] <= self.auto_policy.get('skipped_sequence',0)):
      self.prefetch_state = {'phase': 'cancelled', 'message': '취소한 버전 · 자동 재예약 안 함', 'ready': False}
      return
    try:matches=self.vehicle_matches()
    except Exception:matches=False
    if not matches:
      self.prefetch_due=time.monotonic()+PREFETCH_RETRY
      self.prefetch_state={'phase':'vehicle_wait','ready':False,'message':'LX3 차량 식별 확인 대기 · 번들은 아직 받지 않습니다'}
      return
    blocked=self.prefetch_block
    if blocked and blocked['bundle_sha256']==index['bundle_sha256']:
      if blocked['kind']=='validation':
        await self.block_prefetch(index,blocked['message'],pending,generation,'validation')
        return
      cached=await asyncio.to_thread(self.cache.load,index,self.config['public_key'],sequence)
      if cached is not None:
        context=await asyncio.to_thread(prefetch.source_context,self.root,cached[1])
        if context==blocked['source_context']:
          await self.block_prefetch(index,blocked['message'],pending,generation,'source',context)
          return
    self.prefetch_state = {'phase': 'downloading', 'message': '미리 다운로드·서명 검증 중', 'ready': False,
                           'release_id': index['release_id']}
    cached = await asyncio.to_thread(self.cache.load, index, self.config['public_key'], sequence)
    if cached is None:
      raw = await asyncio.to_thread(fetch, core.bundle_url(index), index['bundle_bytes'])
      if len(raw)!=index['bundle_bytes'] or core.sha(raw)!=index['bundle_sha256']:
        # EOF/truncation/stale transport bytes can recover on the next fetch.
        # Only validation of the exact pinned bytes can permanently block it.
        raise OSError('다운로드 바이트가 완전하지 않습니다 · 연결 복구 후 재시도')
      try:
        release = await asyncio.to_thread(core.verify_bundle, raw, index, self.config['public_key'], sequence)
      except Exception as exc:
        await self.block_prefetch(index,'서명·호환성 검증 거절 ('+type(exc).__name__+')',pending,generation,'validation')
        return
    else:
      raw, release = cached
    async with self.lock:
      if generation != self.prefetch_generation: return
      if index['sequence'] <= core.load(self.folder/'installed.json', {}).get('sequence', 0): return
      if pending:
        if (self.state.get('phase') != 'waiting_parked' or self.state.get('operation') == 'rollback' or
            (self.state.get('index') or {}).get('bundle_sha256') != index['bundle_sha256']): return
      elif (self.auto_policy.get('enabled') is not True or
            self.state.get('phase', 'idle') not in ('idle', 'complete', 'cancelled') or
            self.auto_policy.get('suppressed_bundle') == index['bundle_sha256']):
        return
      try:
        await asyncio.to_thread(self.cache.save, index, raw)
      except OSError as exc:
        if generation!=self.prefetch_generation:return
        self.mark_prefetch_block(index,'캐시 저장 실패 · '+type(exc).__name__,pending,'validation')
        return
      if generation != self.prefetch_generation: return
      # Preserve valid signed bytes even when this device needs a source audit.
      # An unchanged before-hash mismatch must not redownload8MB every minute.
      try:
        await asyncio.to_thread(prefetch.check_sources,self.root,release)
      except prefetch.SourceMismatch as exc:
        if generation!=self.prefetch_generation:return
        self.mark_prefetch_block(index,str(exc),pending,'source',exc.context)
        return
      if generation != self.prefetch_generation:return
      if self.prefetch_block is not None:
        self.prefetch_block=None;core.save(self.folder/'prefetch_block.json',None)
      self.prefetched_digest = index['bundle_sha256']
      self.prefetch_state = {'phase': 'ready', 'message': '다운로드·서명·현재 파일 확인 완료 · P 정차 대기',
                             'ready': True, 'release_id': index['release_id']}
      if not pending:
        self.latest = index
        core.save(self.folder/'latest.json', index)
        self.state = {'index': index, 'automatic': True, 'request_id': uuid.uuid4().hex, 'requested_at': time.time()}
        self.parked_since = self.countdown_since = None
        self.change('waiting_parked', '자동 준비 완료 · P 정차·속도 0·제어 해제 후 설치합니다')

  def mark_prefetch_block(self,index,message,pending,kind,context=None):
    block={'schema':1,'bundle_sha256':index['bundle_sha256'],'sequence':index['sequence'],
           'kind':kind,'source_context':context,'message':message[:240]}
    changed=block!=self.prefetch_block
    self.prefetch_block=block;self.prefetched_digest=None
    self.prefetch_state={'phase':'blocked','ready':False,'release_id':index['release_id'],
                         'message':'PC 점검 필요 · '+block['message']}
    if changed:
      try:core.save(self.folder/'prefetch_block.json',block)
      except OSError:self.prefetch_state['message']+=' · 차단 기록 저장 실패'
    if pending:self.change('failed',self.prefetch_state['message'])

  async def block_prefetch(self,index,message,pending,generation,kind,context=None):
    async with self.lock:
      if generation!=self.prefetch_generation:return
      if pending:
        if (self.state.get('phase')!='waiting_parked' or self.state.get('operation')=='rollback' or
            (self.state.get('index') or {}).get('bundle_sha256')!=index['bundle_sha256']):return
      elif self.auto_policy.get('enabled') is not True or self.state.get('phase','idle') not in ('idle','complete','cancelled'):
        return
      self.mark_prefetch_block(index,message,pending,kind,context)

  async def prefetch_run(self):
    while True:
      try:
        await self.prefetch_tick()
      except asyncio.CancelledError:
        raise
      except Exception as exc:
        self.prefetch_due = time.monotonic()+PREFETCH_RETRY
        detail = str(exc)[:200] if isinstance(exc, ValueError) else type(exc).__name__
        self.prefetch_state = {'phase': 'retry_wait', 'ready': False,
                               'message': '미리 받기 대기 · 기존 실행 코드 유지: '+detail}
      await asyncio.sleep(2)

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
      self.observe_drive(broker.sm)
      return parked_state_error(broker.sm) is None

  async def safe_parked(self):
    try:return await self.parked()
    except Exception:return False

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
    self.observe_drive(sm)
    self.health_identity = None
    def fresh(name):
      return (sm.alive.get(name) and sm.valid.get(name) and
              0 <= time.monotonic() - sm.logMonoTime[name] / 1e9 < 3)
    if not fresh('deviceState'):
      self.health_mode = 'onroad' if self.onroad_observed else 'unknown'
      return False if self.onroad_observed else None
    self.onroad_observed = bool(sm['deviceState'].started)
    if not self.onroad_observed:
      self.health_mode = 'offroad'
      if requires_onroad_verification(self.state.get('release') or {}):
        return None  # A real offroad state does not start the onroad timer.
      if not fresh('managerState'): return None
      running = {p.name: p for p in sm['managerState'].processes if p.running}
      return {'carrot_server', 'ui'} <= running.keys()
    self.health_mode = 'onroad'
    if not fresh('managerState'):
      return False  # Started vehicle with missing manager health is a failure.
    running = {p.name: p for p in sm['managerState'].processes if p.running}
    healthy = bool(REQUIRED <= running.keys() and sm.alive.get('carState') and sm.valid.get('carState') and
                   0 <= time.monotonic() - sm.logMonoTime['carState'] / 1e9 < .5 and sm['carState'].canValid)
    self.health_identity = tuple(sorted((name, running[name].pid) for name in REQUIRED)) if healthy else None
    return healthy

  async def tick(self):
    async with self.lock:
      phase = self.state.get('phase')
      generation = self.prefetch_generation
      if (phase in ('waiting_parked','countdown') and
          (self.auto_policy.get('cancelled_request') == self.request_identity() or
           (self.state.get('automatic') and self.auto_policy.get('enabled') is not True))):
        self.change('cancelled', '취소 기록 확인 · 같은 요청을 설치하지 않습니다')
        return
      if phase == 'verifying':
        release = self.state['release']
        matched = all(core.sha(core.checked_path(self.root, row['path']).read_bytes()) == row['sha256'] for row in release['files'])
        if not matched:
          self.change('health_warning', '적용 파일 무결성 확인 실패 · PC 점검 필요')
          return
        health = await self.healthy()
        critical = requires_onroad_verification(release)
        if critical and health is None:
          self.onroad_healthy_since = self.onroad_healthy_identity = None
          if self.health_mode == 'offroad' and self.onroad_verify_started_mono is not None:
            self.onroad_verify_started_mono = None
            for name in ('onroad_verify_started_at', 'onroad_verify_started_mono', 'onroad_verify_boot'):
              self.state.pop(name, None)
            core.save(self.folder / 'state.json', self.state)
          elif (self.health_mode == 'unknown' and self.onroad_verify_started_mono is not None and
                time.monotonic() - self.onroad_verify_started_mono > ONROAD_HEALTH_DEADLINE):
            self.change('health_warning', '차량 시작 후 상태 정보가 끊겨 실행 확인 실패 · PC 점검 필요')
            return
          if self.state.get('message') != '업데이트 파일 적용됨 · 차량 시작 후 프로세스·CAN 확인 대기':
            self.change('verifying', '업데이트 파일 적용됨 · 차량 시작 후 프로세스·CAN 확인 대기')
          return
        if critical:
          if self.onroad_verify_started_mono is None:
            self.onroad_verify_started_mono = time.monotonic()
            self.state['onroad_verify_started_at'] = time.time()
            self.state['onroad_verify_started_mono'] = self.onroad_verify_started_mono
            self.state['onroad_verify_boot'] = self.boot
            core.save(self.folder / 'state.json', self.state)
          if health is True:
            identity = self.health_identity
            if identity is None or identity != self.onroad_healthy_identity:
              self.onroad_healthy_since = time.monotonic()
              self.onroad_healthy_identity = identity
            if time.monotonic() - self.onroad_healthy_since >= ONROAD_HEALTH_SECONDS:
              self.change('complete', '업데이트 완료 · 적용 파일과 차량 시작 후 연속 프로세스·CAN 확인됨')
              return
          else:
            self.onroad_healthy_since = self.onroad_healthy_identity = None
          deadline = self.onroad_verify_started_mono + ONROAD_HEALTH_DEADLINE
          if health is True and self.onroad_healthy_since is not None:
            deadline = max(deadline, min(self.onroad_healthy_since + ONROAD_HEALTH_SECONDS,
                                         self.onroad_verify_started_mono + ONROAD_HEALTH_DEADLINE + ONROAD_HEALTH_SECONDS))
          if time.monotonic() > deadline:
            self.change('health_warning', '파일 적용됨 · 기기 정상 실행 확인 실패, PC 점검 필요')
        elif health is True:
          self.change('complete', '업데이트 완료 · 적용 파일과 기기 프로세스 확인됨')
        elif time.time() - self.state.get('applied_at', time.time()) > ONROAD_HEALTH_DEADLINE:
          self.change('health_warning', '파일 적용됨 · 기기 정상 실행 확인 실패, PC 점검 필요')
        return
      if phase not in ('waiting_parked', 'countdown'): return
      parked = await self.safe_parked()
      if self.cancelled(generation): return
      if not parked:
        self.parked_since = self.countdown_since = None
        if phase == 'countdown': self.change('waiting_parked', '차량 상태 변경 · 업데이트 예약 유지')
        return
      if self.state.get('automatic') and (not self.auto_driven or not self.drive_session_onroad or
                                         not 0 <= time.monotonic()-self.drive_state_at < 3.):
        self.parked_since = self.countdown_since = None
        if self.state.get('message') != '자동 준비 완료 · 이번 시동의 주행 후 P 정차를 기다립니다':
          self.change('waiting_parked', '자동 준비 완료 · 이번 시동의 주행 후 P 정차를 기다립니다')
        return
      now = time.monotonic()
      if self.parked_since is None: self.parked_since = now
      if now - self.parked_since < 10: return
      try: matches = self.vehicle_matches()
      except Exception: matches = False
      if not matches: return
      if phase == 'waiting_parked':
        index = self.state['index']  # Exact user-selected release, never a moving latest pointer.
        previous = core.load(self.folder / 'installed.json', {})
        restoring = self.state.get('operation') == 'rollback'
        if restoring:
          release = self.state['release']
        else:
          cached = await asyncio.to_thread(self.cache.load, index, self.config['public_key'], previous.get('sequence', 0))
          if self.cancelled(generation): return
          if cached is None:
            self.prefetched_digest = None
            self.prefetch_due = min(self.prefetch_due,max(time.monotonic(),self.prefetch_not_before))
            self.prefetch_state = {'phase':'retry_wait','ready':False,'message':'캐시 확인 필요 · 다시 받기 대기'}
            return  # Worker retries independently; no network wait in the parked tick.
          raw, release = cached
          await asyncio.to_thread(prefetch.check_sources, self.root, release)
        # Disconnect or gear change during download leaves all production files untouched.
        if not await self.safe_parked():
          self.parked_since = None
          self.change('waiting_parked', '차량 상태 변경 · P 정차 후 다시 준비합니다')
          return
        if self.cancelled(generation): return
        self.change('downloading', '준비 파일 재검증·백업 중 · 다운로드 대기 없음')
        directory = await asyncio.to_thread(core.stage, self.root, self.folder, release)
        if self.cancelled(generation): return
        if not restoring:
          core.atomic(directory / 'signed_bundle.json', raw)
          core.save(directory / 'index.json', index)
          core.save(directory / 'previous_installed.json', previous)
        self.state['release'] = release
        self.state['previous_installed'] = previous
        self.countdown_since = time.monotonic()
        self.change('countdown', '검증·백업 완료 · P 상태가 유지되면 10초 뒤 재부팅합니다')
        return
      if self.countdown_since is None: self.countdown_since = now
      if now - self.countdown_since < 10: return
      await asyncio.to_thread(core.validate_staged, self.root, self.folder, self.state['release'])
      if self.cancelled(generation): return
      if not await self.safe_parked():
        self.parked_since = self.countdown_since = None
        self.change('waiting_parked', '차량 상태 변경 · 업데이트 예약 유지')
        return
      if self.cancelled(generation): return
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


async def local_health(request):
  # Bootstrap process check only. No token/config/notes, no mutations, and no
  # forwarding-header trust. Normal phone endpoints keep their existing auth.
  if request.remote not in ('127.0.0.1','::1'):raise web.HTTPUnauthorized()
  service=request.app['hud_update_service']
  return web.json_response({'ok':True,'prefetch_version':1,'configured':bool(service.config),
    'policy_valid':service.auto_policy.get('schema')==1 and not service.policy_invalid,
    'automatic_enabled':service.auto_policy.get('enabled') is True,'process_id':os.getpid(),
    'service_sha256':core.sha(Path(__file__).read_bytes())})


async def action(request):
  service = request.app['hud_update_service']; service.authorize(request)
  try: body = await request.json()
  except Exception: raise web.HTTPBadRequest(text='Invalid JSON')
  if not isinstance(body, dict): raise web.HTTPBadRequest(text='Invalid JSON')
  # Cancellation must be accepted while stage validation or the final parked
  # poll is awaiting. Durable intent is checked immediately before arming.
  if body.get('action') == 'cancel':
    service.cancel()
    return web.json_response(service.public())
  if body.get('action') == 'automatic' and body.get('enabled') is False:
    service.set_automatic(False)
    return web.json_response(service.public())
  # Do not hold a long download/reboot transaction open behind a second button press.
  if service.lock.locked(): raise web.HTTPConflict(text='업데이트 처리 중입니다. 상태를 다시 확인하세요.')
  async with service.lock:
    command = body.get('action')
    if command == 'check':
      try: await service.check()
      except Exception: raise web.HTTPBadGateway(text='GitHub 버전을 확인하지 못했습니다. 다시 시도하세요.')
    elif command == 'queue':
      index = service.latest
      if index is None or body.get('release_id') != index['release_id'] or body.get('bundle_sha256') != index['bundle_sha256']:
        raise web.HTTPConflict(text='버전·변경점을 다시 확인하세요.')
      if service.state.get('phase') in ACTIVE:
        if (service.state.get('index') or {}).get('bundle_sha256') == index['bundle_sha256']:
          if service.state.get('automatic') and service.state.get('phase') == 'waiting_parked':
            service.state['automatic'] = False
            service.prefetch_generation += 1
            service.change('waiting_parked', '사용자 설치 요청 · P 정차·제어 해제 확인 후 적용')
          return web.json_response(service.public())
        raise web.HTTPConflict(text='이미 다른 업데이트가 예약되어 있습니다.')
      if index['sequence'] <= core.load(service.folder / 'installed.json', {}).get('sequence', 0):
        raise web.HTTPConflict(text='이미 설치된 버전입니다.')
      service.prefetch_generation += 1
      service.prefetch_due = 0.
      service.prefetch_block=None
      core.save(service.folder/'prefetch_block.json',None)
      service.state = {'index': index, 'request_id': uuid.uuid4().hex, 'requested_at': time.time()}
      service.parked_since = service.countdown_since = None
      service.change('waiting_parked', '예약됨 · P 정차·속도 0·제어 해제 10초를 기다립니다')
    elif command == 'rollback':
      if service.state.get('phase') in ACTIVE:
        raise web.HTTPConflict(text='업데이트 처리 중에는 복원을 예약할 수 없습니다.')
      installed = core.load(service.folder / 'installed.json', {})
      if body.get('release_id') != installed.get('release_id'):
        raise web.HTTPConflict(text='설치 버전이 달라졌습니다. 상태를 다시 확인하세요.')
      try:
        release = await asyncio.to_thread(core.prepare_rollback, service.root, service.folder,
                                         installed, service.config['public_key'])
      except Exception:
        raise web.HTTPConflict(text='직전 버전 백업을 확인하지 못했습니다. 기존 파일을 유지합니다.')
      service.prefetch_generation += 1
      service.state = {'operation': 'rollback', 'release': release,
                       'request_id': uuid.uuid4().hex,
                       'index': {k: release[k] for k in ('release_id', 'sequence', 'notes')},
                       'requested_at': time.time()}
      service.parked_since = service.countdown_since = None
      service.change('waiting_parked', '직전 버전 복원 예약됨 · P 정차·제어 해제 후 적용합니다')
    elif command == 'automatic':
      service.set_automatic(body.get('enabled'))
    else: raise web.HTTPBadRequest(text='Unknown action')
  return web.json_response(service.public())


async def lifecycle(app):
  service = UpdateService(app)
  app['hud_update_service'] = service
  task = asyncio.create_task(service.run())
  prefetch_task = asyncio.create_task(service.prefetch_run()) if service.config else None
  yield
  task.cancel()
  if prefetch_task is not None: prefetch_task.cancel()
  try: await task
  except asyncio.CancelledError: pass
  if prefetch_task is not None:
    try: await prefetch_task
    except asyncio.CancelledError: pass


def register(app):
  app.cleanup_ctx.append(lifecycle)
  app.router.add_get('/api/hud_update/status', status)
  app.router.add_get('/api/hud_update/local_health', local_health)
  app.router.add_post('/api/hud_update/action', action)
