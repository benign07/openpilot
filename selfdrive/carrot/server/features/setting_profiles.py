import asyncio
import hashlib
import json
import re
from aiohttp import web
from ..services import setting_profiles as tuning
from ..services.setting_safety import require_parked, settings_write_lock, settled_setting_write, parked_write_proof, check_parked_write_proof, serialize_settings_mutation

from ..services.setting_profiles import (
  apply_setting_profile,
  create_setting_profile,
  delete_setting_profile,
  preview_setting_profile,
  read_setting_profiles,
  update_setting_profile,
)


async def get_setting_profiles(request: web.Request) -> web.Response:
  return web.json_response({"ok": True, **read_setting_profiles()})


async def create_setting_profile_route(request: web.Request) -> web.Response:
  try:
    body = await request.json()
  except Exception:
    body = {}
  try:
    profile = create_setting_profile(body.get("name", ""))
    return web.json_response({"ok": True, "profile": profile, **read_setting_profiles()})
  except Exception as e:
    return web.json_response({"ok": False, "error": str(e)}, status=400)


async def update_setting_profile_route(request: web.Request) -> web.Response:
  try:
    body = await request.json()
  except Exception:
    body = {}
  profile_id = str(body.get("id") or "").strip()
  if not profile_id:
    return web.json_response({"ok": False, "error": "missing profile id"}, status=400)
  try:
    profile = update_setting_profile(profile_id, body)
    return web.json_response({"ok": True, "profile": profile, **read_setting_profiles()})
  except KeyError as e:
    return web.json_response({"ok": False, "error": str(e)}, status=404)
  except Exception as e:
    return web.json_response({"ok": False, "error": str(e)}, status=400)


async def delete_setting_profile_route(request: web.Request) -> web.Response:
  try:
    body = await request.json()
  except Exception:
    body = {}
  profile_id = str(body.get("id") or "").strip()
  if not profile_id:
    return web.json_response({"ok": False, "error": "missing profile id"}, status=400)
  try:
    delete_setting_profile(profile_id)
    return web.json_response({"ok": True, **read_setting_profiles()})
  except KeyError as e:
    return web.json_response({"ok": False, "error": str(e)}, status=404)
  except Exception as e:
    return web.json_response({"ok": False, "error": str(e)}, status=400)


async def preview_setting_profile_route(request: web.Request) -> web.Response:
  try:
    body = await request.json()
  except Exception:
    body = {}
  profile_id = str(body.get("id") or "").strip()
  if not profile_id:
    return web.json_response({"ok": False, "error": "missing profile id"}, status=400)
  try:
    preview = preview_setting_profile(profile_id, body.get("values") if isinstance(body, dict) else None)
    return web.json_response({"ok": True, "preview": preview})
  except KeyError as e:
    return web.json_response({"ok": False, "error": str(e)}, status=404)
  except Exception as e:
    return web.json_response({"ok": False, "error": str(e)}, status=400)


async def apply_setting_profile_route(request: web.Request) -> web.Response:
  try:
    body = await request.json()
  except Exception:
    body = {}
  profile_id = str(body.get("id") or "").strip()
  if not profile_id:
    return web.json_response({"ok": False, "error": "missing profile id"}, status=400)
  try:
    result = apply_setting_profile(profile_id, body.get("values") if isinstance(body, dict) else None)
    return web.json_response({"ok": True, **result})
  except KeyError as e:
    return web.json_response({"ok": False, "error": str(e)}, status=404)
  except Exception as e:
    return web.json_response({"ok": False, "error": str(e)}, status=400)


def register(app: web.Application) -> None:
  app.router.add_get('/api/tuning_profiles', get_tuning_profiles)
  app.router.add_get('/api/tuning_profiles/receipt', get_tuning_receipt)
  app.router.add_post('/api/tuning_profiles/preview', preview_tuning_route)
  app.router.add_post('/api/tuning_profiles/apply', apply_tuning_route)
  app.router.add_post('/api/tuning_profiles/recover', recover_tuning_route)
  app.router.add_post('/api/tuning_profiles/archive', archive_tuning_route)
  app.router.add_get("/api/setting_profiles", get_setting_profiles)
  app.router.add_post("/api/setting_profiles", create_setting_profile_route)
  app.router.add_post("/api/setting_profiles/update", update_setting_profile_route)
  app.router.add_post("/api/setting_profiles/delete", delete_setting_profile_route)
  app.router.add_post("/api/setting_profiles/preview", preview_setting_profile_route)
  app.router.add_post("/api/setting_profiles/apply", serialize_settings_mutation(apply_setting_profile_route))


async def _tuning_body(request):
  if request.content_length is not None and request.content_length > 32768:
    raise ValueError('프로필 요청이 너무 큽니다.')
  raw = await request.text()
  if len(raw.encode('utf8')) > 32768: raise ValueError('프로필 요청이 너무 큽니다.')
  body = tuning.strict_tuning_json(raw)
  if not isinstance(body, dict):
    raise ValueError('JSON 객체가 필요합니다.')
  return body


async def get_tuning_profiles(request):
  try:
    return web.json_response(_tuning_progress(request, tuning.tuning_snapshot()))
  except Exception as error:
    return web.json_response({'ok': False, 'error': str(error)}, status=409)


async def preview_tuning_route(request):
  try:
    body = await _tuning_body(request)
    async with settings_write_lock(request):
      await _acknowledge_tuning_archive()
      return web.json_response(tuning.preview_tuning_profile(body.get('profile')))
  except Exception as error:
    return web.json_response({'ok': False, 'error': str(error)}, status=400)


async def _restore_tuning_transaction(request, state):
  """Best-effort conditional restore. Never overwrite another writer's value."""
  errors = []
  try:
    if tuning._tuning_context() != state['context']:
      raise ValueError('기기/차량 식별이 바뀌어 복원하지 않았습니다.')
    for key in reversed(state['attempted']):
      await require_parked(request)
      current = tuning._tuning_values()[key]
      if current == state['before'][key]:
        continue
      if current != state['after'][key]:
        errors.append(key + ': 외부 변경값을 보존했습니다.')
        continue
      # Revalidate against this firmware; no migration/clamp on rollback.
      contract = tuning.tuning_contract()['fields'].get(key)
      old = state['before'][key]
      if contract is None or not contract['min'] <= old <= contract['max']:
        raise ValueError(key + ': 현재 펌웨어에서 복원할 수 없는 값입니다.')
      proof = await parked_write_proof(request)
      await settled_setting_write(_checked_tuning_write, proof, key, old)
      if tuning._tuning_values()[key] != old:
        raise ValueError(key + ': 복원 읽기 검증 실패')
  except Exception as error:
    errors.append(str(error))
  state['phase'] = 'recovery_required' if errors else 'rolled_back'
  state['error'] = '; '.join(errors)
  await settled_setting_write(tuning.save_tuning_state, state)
  return not errors


async def apply_tuning_route(request):
  try:
    body = await _tuning_body(request)
    profile = body.get('profile')
    values = tuning.validate_tuning_profile(profile)
    request_id = body.get('request_id')
    if not isinstance(request_id, str) or not re.fullmatch('[a-f0-9]{32}', request_id):
      raise ValueError('적용 요청 식별자가 올바르지 않습니다.')
    profile_hash = tuning.tuning_profile_digest(profile)
    async with settings_write_lock(request):
      await require_parked(request)
      await _acknowledge_tuning_archive()
      snapshot = tuning.tuning_snapshot()
      old_state = snapshot['receipt']
      if old_state and old_state.get('request_id') == request_id:
        if old_state.get('profile_hash') != profile_hash:
          raise ValueError('동일 요청에 다른 프로필을 보낼 수 없습니다.')
        return web.json_response({'ok': old_state['phase'] == 'complete', 'receipt': old_state, 'snapshot': snapshot})
      if snapshot['recovery_required']:
        raise ValueError('이전 적용 복구가 필요합니다. 새 적용을 중단했습니다.')
      if (body.get('revision') != snapshot['revision'] or body.get('serial') != snapshot['context']['serial'] or
          body.get('profile_hash') != profile_hash):
        raise ValueError('기기·설정·프로필이 미리보기 이후 바뀌었습니다. 다시 확인하세요.')
      changes = sorted((k for k, v in values.items() if snapshot['values'][k] != v), key=lambda k: k == 'LaunchFollowStrength')
      if not changes and not (snapshot['unverified_previous'] and set(values)==set(tuning.TUNING_UNITS)):
        return web.json_response({'ok': True, 'phase': 'unchanged', 'snapshot': snapshot})
      state = {'schema': 1, 'phase': 'applying', 'request_id': request_id, 'profile_hash': profile_hash,
               'profile_id': profile['id'], 'profile_name': profile['name'], 'context': snapshot['context'],
               'contract_revision': snapshot['contract_revision'], 'units': profile['units'],
               'before': {k: snapshot['values'][k] for k in values}, 'after': values,
               'archive_warning_id': snapshot['archive_warning_id'], 'attempted': [], 'saved_at': tuning._now_iso(), 'server_epoch': tuning._TUNING_SERVER_EPOCH}
      request.app['tuning_active_request'] = request_id
      try:
        await settled_setting_write(tuning.save_tuning_state, state)
        try:
          for key in changes:
            await require_parked(request)
            if tuning._tuning_context() != state['context']:
              raise ValueError('기기/차량 식별이 바뀌었습니다.')
            state['attempted'].append(key)
            await settled_setting_write(tuning.save_tuning_state, state)
            proof = await parked_write_proof(request)
            await settled_setting_write(_checked_tuning_write, proof, key, values[key])
            if tuning._tuning_values()[key] != values[key]:
              raise ValueError(key + ': 저장 읽기 검증 실패')
          await require_parked(request)
          final = tuning.tuning_snapshot()
          if final['context'] != state['context'] or any(final['values'][k] != v for k, v in values.items()):
            raise ValueError('프로필 저장값이 일치하지 않습니다.')
        except asyncio.CancelledError:
          state['phase'] = 'recovery_required'
          await settled_setting_write(tuning.save_tuning_state, state, terminal=True)
          raise
        except Exception as error:
          restored = await _restore_tuning_transaction(request, state)
          return web.json_response({'ok': False, 'error': str(error), 'restored': restored, 'receipt': state}, status=409)
        # Native values are verified. Receipt/response failures must not undo them.
        state['phase'] = 'complete'
        try:
          await settled_setting_write(tuning.save_tuning_state, state, terminal=True)
        except Exception as error:
          state['phase'] = 'recovery_required'
          state['error'] = '값은 읽기 확인됐으나 완료 기록을 저장하지 못했습니다: ' + str(error)
          try: await settled_setting_write(tuning.save_tuning_state, state, terminal=True)
          except Exception: pass
          return web.json_response({'ok': False, 'error': state['error'], 'receipt': state}, status=503)
        await _acknowledge_tuning_archive(terminal=True)
        return web.json_response({'ok': True, 'phase': 'complete', 'receipt': state, 'snapshot': None})
      finally:
        if request.app.get('tuning_active_request') == request_id:
          request.app.pop('tuning_active_request', None)
  except web.HTTPException:
    raise
  except Exception as error:
    return web.json_response({'ok': False, 'error': str(error)}, status=409)


async def recover_tuning_route(request):
  try:
    body = await _tuning_body(request)
    async with settings_write_lock(request):
      await require_parked(request)
      state = tuning._tuning_state()
      if not state or state['phase'] not in ('applying', 'recovery_required') or body.get('request_id') != state['request_id']:
        raise ValueError('복구할 미완료 적용 기록을 확인하세요. 손상 기록은 자동 삭제하지 않습니다.')
      restored = await _restore_tuning_transaction(request, state)
      return web.json_response({'ok': restored, 'receipt': state, 'snapshot': tuning.tuning_snapshot()}, status=200 if restored else 409)
  except web.HTTPException:
    raise
  except Exception as error:
    return web.json_response({'ok': False, 'error': str(error)}, status=409)


def _checked_tuning_write(proof, key, value):
  check_parked_write_proof(proof)
  tuning.tuning_write(key, value)


async def get_tuning_receipt(request):
  try:
    async with settings_write_lock(request):
      await _acknowledge_tuning_archive()
      result = _tuning_progress(request, tuning.tuning_receipt())
    wanted = request.query.get('request_id')
    if wanted is not None:
      if not re.fullmatch('[a-f0-9]{32}', wanted):
        raise ValueError('요청 식별자를 확인하세요.')
      result['found'] = bool(result['receipt'] and result['receipt'].get('request_id') == wanted)
    return web.json_response(result)
  except Exception as error:
    return web.json_response({'ok': False, 'error': str(error)}, status=409)


async def archive_tuning_route(request):
  try:
    body = await _tuning_body(request)
    lock = settings_write_lock(request)
    if lock.locked(): raise ValueError('설정 작업이 진행 중입니다. 보존 작업을 다시 확인하세요.')
    async with lock:
      await require_parked(request)
      archived = await settled_setting_write(tuning.archive_tuning_journal, body.get('journal_sha256'), body.get('serial'))
      return web.json_response({'ok': True, 'archived': archived, 'receipt': tuning.tuning_receipt(),
                               'note': '원본 기록을 보존했습니다. Params는 변경하지 않았으며 이전 적용 결과를 확인한 것은 아닙니다. 현재값을 다시 미리보기하세요. 이전 결과 미확인 표시는 전체 프로필 저장 확인 전까지 유지됩니다.'})
  except web.HTTPException:
    raise
  except Exception as error:
    return web.json_response({'ok': False, 'error': str(error)}, status=409)


def _tuning_progress(request, result):
  receipt = result.get('receipt')
  live = bool(receipt and receipt.get('phase') == 'applying' and
              receipt.get('server_epoch') == tuning._TUNING_SERVER_EPOCH and
              request.app.get('tuning_active_request') == receipt.get('request_id'))
  result['in_progress'] = live
  if live: result['recovery_required'] = False
  return result


async def _acknowledge_tuning_archive(terminal=False):
  # A failed metadata rename retains the warning, never undoes native values.
  try: await settled_setting_write(tuning.acknowledge_tuning_archive_warning,terminal=terminal)
  except Exception: pass
