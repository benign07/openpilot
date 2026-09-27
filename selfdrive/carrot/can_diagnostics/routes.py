from __future__ import annotations

import asyncio
import json
from pathlib import Path
import re
from urllib.parse import urlsplit

from aiohttp import web

from .collector import Controller
from .core import json_safe

KEY = web.AppKey('can_diagnostics', Controller)
SESSION_RE = re.compile(r'^\d{8}T\d{6}Z-[a-f0-9]{10}$')
DOWNLOADS = {'report.json', 'candidates.csv', 'raw_can.jsonl.gz', 'events.jsonl', 'catalog.json'}


def response(value, status=200):
  return web.json_response(json_safe(value), status=status, headers={'Cache-Control': 'no-store'})


async def status(request):
  return response(request.app[KEY].status())


async def action(request):
  # Custom header + JSON require a same-origin request; no CORS is enabled.
  if request.headers.get('X-Carrot-Diagnostics') != '1':
    return response({'error': '앱에서 진단을 실행해 주세요.'}, 403)
  origin = request.headers.get('Origin')
  if origin and urlsplit(origin).netloc != request.host:
    return response({'error': '다른 사이트에서 보낸 요청은 허용하지 않습니다.'}, 403)
  if request.content_type != 'application/json':
    return response({'error': 'JSON 요청이 필요합니다.'}, 415)
  if request.content_length and request.content_length > 1024:
    return response({'error': '요청이 너무 큽니다.'}, 413)
  try:
    raw = await request.content.read(1025)
    if len(raw) > 1024:
      return response({'error': '요청이 너무 큽니다.'}, 413)
    payload = json.loads(raw)
    if not isinstance(payload, dict):
      raise ValueError('잘못된 요청입니다.')
    result = request.app[KEY].action(request.match_info['action'], payload.get('code') if request.match_info['action'] == 'marker' else payload.get('test_id'))
    return response(result)
  except (ValueError, TypeError, json.JSONDecodeError) as exc:
    return response({'error': str(exc)}, 409)
  except OSError:
    return response({'error': '진단 파일을 저장할 수 없습니다.'}, 507)


async def sessions(request):
  root = request.app[KEY].capture.root
  items = []
  # Incomplete crash captures remain available as raw data without fabricated results.
  for folder in sorted(root.glob('*'), reverse=True) if root.exists() else []:
    if not folder.is_dir() or not SESSION_RE.fullmatch(folder.name):
      continue
    report = folder / 'report.json'
    row = {'id': folder.name, 'completed': False, 'reason': '미완료 원시 기록', 'candidate_count': 0}
    if report.is_file():
      try:
        saved = json.loads(report.read_text(encoding='utf-8'))
        row.update({k: saved.get(k) for k in ('test_id', 'completed', 'reason', 'frame_count')})
        row['candidate_count'] = len(saved.get('candidates', []))
      except (ValueError, OSError):
        row['reason'] = '결과 파일 확인 필요'
    row['files'] = sorted(p.name for p in folder.iterdir() if p.name in DOWNLOADS and p.is_file())
    items.append(row)
    if len(items) >= 50:
      break
  return response({'sessions': items})


async def catalog(request):
  controller = request.app[KEY]
  with controller.lock:
    result = controller.capture.validator.catalog()
  return response(result)


async def download(request):
  session_id, filename = request.match_info['session'], request.match_info['file']
  if not SESSION_RE.fullmatch(session_id) or filename not in DOWNLOADS:
    raise web.HTTPNotFound()
  controller = request.app[KEY]
  with controller.lock:
    if controller.capture.active() and controller.capture.session['id'] == session_id:
      return response({'error': '수집을 마친 후 내려받아 주세요.'}, 409)
  root = controller.capture.root.resolve()
  target = root / session_id / filename
  if not target.resolve().is_relative_to(root) or not target.is_file():
    raise web.HTTPNotFound()
  return web.FileResponse(target, headers={'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff',
                          'Content-Disposition': f'attachment; filename="{session_id}-{filename}"'})


async def cleanup(app):
  await asyncio.to_thread(app[KEY].close)


def register(app, *, controller=None):
  app[KEY] = controller or Controller()
  prefix = '/api/can_diagnostics'
  app.router.add_get(prefix + '/status', status)
  app.router.add_post(prefix + '/{action:start|mark|stop|marker}', action)
  app.router.add_get(prefix + '/sessions', sessions)
  app.router.add_get(prefix + '/catalog', catalog)
  app.router.add_get(prefix + '/download/{session}/{file}', download)
  app.on_cleanup.append(cleanup)
