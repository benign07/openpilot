from aiohttp import web

from ...can_diagnostics import routes as can_diagnostics

from . import (
  cars,
  dashcam,
  params,
  screenrecord,
  settings,
  setting_favorites,
  setting_popular_values,
  setting_profiles,
  ssh_keys,
  static,
  stream,
  system,
  terminal,
  tools,
  vision_diag,
  vision_test,
  web_settings,
  ws,
)


def register_all(app: web.Application) -> None:
  can_diagnostics.register(app)
  static.register(app)
  stream.register(app)
  ws.register(app)
  settings.register(app)
  params.register(app)
  setting_favorites.register(app)
  setting_popular_values.register(app)
  setting_profiles.register(app)
  web_settings.register(app)
  ssh_keys.register(app)
  cars.register(app)
  system.register(app)
  terminal.register(app)
  dashcam.register(app)
  screenrecord.register(app)
  tools.register(app)
  vision_test.register(app)
  vision_diag.register(app)
