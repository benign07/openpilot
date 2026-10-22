"""Real projection geometry, inert drawing: output parity, not GPU/FPS testing."""
import ast
from pathlib import Path
from types import SimpleNamespace as NS

import numpy as np
import pytest


HERE = Path(__file__).parent
CURRENT = HERE.parents[2] / 'selfdrive/ui/onroad/model_renderer.py'
FROZEN = HERE / 'fixtures/lane_renderer_reference_88ffc175.py'
METHODS = {'_get_path_length_idx', '_map_line_to_polygon', '_draw_lane_lines_carrot'}


def renderer_class(source, draws):
  tree = ast.parse(source)
  cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'ModelRenderer')
  cls.bases = []
  cls.body = [n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name in METHODS]
  scope = {'np': np, 'rl': NS(Color=lambda *rgba: rgba), 'MIN_DRAW_DISTANCE': 10., 'MAX_DRAW_DISTANCE': 100.,
           'draw_polygon': lambda rect, vertices, color: draws.append(('fill', vertices.copy(), color, 0.))}
  exec(compile(ast.Module(body=[cls], type_ignores=[]), '<production rendering methods>', 'exec'), scope)
  return scope['ModelRenderer']


def make_case(left=11, show=1, empty=False, valid=True):
  x = np.linspace(0., 83., 33) if not empty else np.array([])
  lines = [NS(x=x, y=np.full_like(x, y) + .0001 * x ** 2, z=np.full_like(x, 1.2)) for y in [-5.4, -1.8, 1.8, 5.4]]
  model = NS(laneLines=lines, roadEdges=lines[::3], laneLineProbs=[.9, .2, .9, .4], roadEdgeStds=[.1, 1.8])
  class SM(dict):
    pass
  sm = SM(modelV2=model, carState=NS(leftLaneLine=left, rightLaneLine=31))
  sm.valid = {'modelV2': valid, 'carState': valid}
  return show, sm


def make_renderer(cls, draws, show):
  ui = cls()
  ui._rect = None
  ui._carrot_show_lane_info = show
  ui._car_space_transform = np.array([[320., 800., 0.], [240., 0., 800.], [1., 0., 0.]])
  ui._clip_region = NS(x=0., y=0., width=640., height=480.)
  ui._draw_polygon_outline_carrot = lambda vertices, color, stroke: draws.append(('outline', vertices.copy(), color, stroke))
  return ui


@pytest.mark.parametrize('left', [0, 1, 4, 14, 24, 34, 151, -1])
@pytest.mark.parametrize('show', [0, 1, 2])
def test_full_draw_commands_match_frozen_production_geometry(left, show):
  outputs = []
  for source in [FROZEN, CURRENT]:
    commands = []
    cls = renderer_class(source.read_text(encoding='utf8'), commands)
    ui = make_renderer(cls, commands, show)
    ui._draw_lane_lines_carrot(make_case(left, show)[1])
    outputs.append(commands)
  assert len(outputs[0]) == len(outputs[1])
  for old, new in zip(*outputs, strict=True):
    assert old[0] == new[0] and old[2:] == new[2:]
    np.testing.assert_array_equal(old[1], new[1])


@pytest.mark.parametrize('empty,valid', [(True, True), (False, False)])
def test_early_return_still_draws_nothing(empty, valid):
  for source in [FROZEN, CURRENT]:
    commands = []
    ui = make_renderer(renderer_class(source.read_text(encoding='utf8'), commands), commands, 2)
    ui._draw_lane_lines_carrot(make_case(empty=empty, valid=valid)[1])
    assert commands == []


def test_lane_only_mode_does_not_read_unused_malformed_edges():
  commands = []
  cls = renderer_class(CURRENT.read_text(encoding='utf8'), commands)
  sm = make_case()[1]
  sm['modelV2'].roadEdges = []
  sm['modelV2'].roadEdgeStds = []
  make_renderer(cls, commands, 1)._draw_lane_lines_carrot(sm)
  assert commands
  with pytest.raises(IndexError):
    make_renderer(cls, commands, 2)._draw_lane_lines_carrot(sm)


@pytest.mark.parametrize('case', ['offscreen_double', 'short_path', 'right_codes', 'missing_edges'])
def test_projection_edges_match_frozen(case):
  outputs = []
  for source in [FROZEN, CURRENT]:
    commands = []
    ui = make_renderer(renderer_class(source.read_text(encoding='utf8'), commands), commands, 2)
    sm = make_case(left=24)[1]
    if case == 'offscreen_double':
      sm['modelV2'].laneLines[1].y[:] = -100.
    elif case == 'short_path':
      for line in sm['modelV2'].laneLines:
        line.x = line.x[:3]
        line.y = line.y[:3]
        line.z = line.z[:3]
    elif case == 'right_codes':
      for right in [0, 21, 24, 34, 151, -1]:
        sm['carState'].rightLaneLine = right
        ui._draw_lane_lines_carrot(sm)
    else:
      sm['modelV2'].roadEdges = []
      with pytest.raises(IndexError):
        ui._draw_lane_lines_carrot(sm)
      continue
    ui._draw_lane_lines_carrot(sm)
    outputs.append(commands)
  if outputs:
    assert len(outputs[0]) == len(outputs[1])
    for old, new in zip(*outputs, strict=True):
      assert old[0] == new[0] and old[2:] == new[2:]
      np.testing.assert_array_equal(old[1], new[1])
