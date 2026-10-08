"""Standalone diagnostic UI for local verification; --demo is explicitly labeled."""
import argparse
from pathlib import Path

from aiohttp import web

from .collector import Controller
from .routes import register


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--host', default='127.0.0.1')
  parser.add_argument('--port', type=int, default=7010)
  parser.add_argument('--demo', action='store_true')
  parser.add_argument('--data-dir', default='/data/community/can_diagnostics')
  args = parser.parse_args()
  app = web.Application(client_max_size=1024)
  register(app, controller=Controller(args.data_dir, demo=args.demo))
  directory = Path(__file__).resolve().parents[1] / 'web'
  app.router.add_get('/', lambda request: web.HTTPFound('/diagnostics.html'))
  app.router.add_static('/', directory, show_index=False)
  web.run_app(app, host=args.host, port=args.port)


if __name__ == '__main__':
  main()
