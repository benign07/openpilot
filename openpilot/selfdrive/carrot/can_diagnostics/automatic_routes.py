import asyncio
import re

from aiohttp import web

from .automatic_runtime import AutomaticController

KEY = web.AppKey('automatic_drive', AutomaticController)
ID = re.compile(r'^[a-f0-9]{32}$')


async def status(request):
  return web.json_response(request.app[KEY].status(), headers={'Cache-Control': 'no-store'})


async def chunks(request):
  rows = await asyncio.to_thread(request.app[KEY].chunks)
  return web.json_response({'chunks': rows}, headers={'Cache-Control': 'no-store'})


async def download(request):
  chunk = request.match_info['chunk']
  if not ID.fullmatch(chunk):
    raise web.HTTPNotFound()
  root = request.app[KEY].root.resolve()
  path = root / (chunk + '.jsonl.gz')
  if not (root / (chunk + '.manifest.json')).is_file() or not path.is_file() or not path.resolve().is_relative_to(root):
    raise web.HTTPNotFound()
  return web.FileResponse(path, headers={'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff'})


async def startup(app):
  app[KEY].start()


async def cleanup(app):
  await asyncio.to_thread(app[KEY].close)


def register(app, controller=None):
  app[KEY] = controller or AutomaticController()
  app.router.add_get('/api/automatic_drive/status', status)
  app.router.add_get('/api/automatic_drive/chunks', chunks)
  app.router.add_get('/api/automatic_drive/chunks/{chunk}', download)
  app.on_startup.append(startup)
  app.on_cleanup.append(cleanup)
