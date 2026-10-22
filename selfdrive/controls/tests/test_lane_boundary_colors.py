"""Exercise the production lane renderer with an inert drawing surface."""
import ast
from pathlib import Path
from types import SimpleNamespace as NS

import numpy as np
import pytest


SOURCE = Path(__file__).resolve().parents[3] / 'selfdrive/ui/onroad/model_renderer.py'


def render(left=11, right=11, probabilities=None, valid=True):
  tree = ast.parse(SOURCE.read_text(encoding='utf8'))
  cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'ModelRenderer')
  method = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == '_draw_lane_lines_carrot')
  draws, mapped, outlines = [], [], []
  scope = {'np': np, 'rl': NS(Color=lambda *rgba: rgba), 'MIN_DRAW_DISTANCE': 10., 'MAX_DRAW_DISTANCE': 100.,
           'draw_polygon': lambda rect, polygon, color: draws.append((float(polygon[0, 0]), color))}
  exec(compile(ast.Module(body=[method], type_ignores=[]), str(SOURCE), 'exec'), scope)

  def polygon(line, width, *args):
    index = float(line[0, 1])
    double = len(args) > 3
    mapped.append((index, width, double))
    return np.array([[index + (0.1 if double else 0), 0], [index, 1], [index + 1, 1]])

  ui = NS(_carrot_show_lane_info=1, _rect=None, _map_line_to_polygon=polygon,
          _get_path_length_idx=lambda xs, limit: len(xs) - 1,
          _draw_polygon_outline_carrot=lambda vertices, color, stroke: outlines.append((float(vertices[0, 0]), color, stroke)))
  lines = [NS(x=[0., 10., 20., 30.], y=[float(i)] * 4, z=[0.] * 4) for i in range(4)]
  model = NS(laneLines=lines, roadEdges=lines[:2], laneLineProbs=probabilities or [.9] * 4, roadEdgeStds=[.1, .1])
  class SM(dict):
    pass
  sm = SM(modelV2=model, carState=NS(leftLaneLine=left, rightLaneLine=right))
  sm.valid = {'modelV2': valid, 'carState': valid}
  scope['_draw_lane_lines_carrot'](ui, sm)
  return draws, mapped, outlines


@pytest.mark.parametrize('code,rgb', [(0, (255, 255, 255)), (11, (255, 255, 255)),
                                    (21, (218, 202, 37)), (31, (50, 150, 255)),
                                    (30, (50, 150, 255)), (35, (50, 150, 255)),
                                    (41, (255, 255, 255)), (51, (255, 255, 255)),
                                    (-1, (255, 255, 255)), (151, (255, 255, 255))])
@pytest.mark.parametrize('side,index', [('left', 1.), ('right', 2.)])
def test_each_inner_boundary_uses_its_actual_color(code, rgb, side, index):
  draws, _, _ = render(**{side: code})
  assert dict(draws)[index] == (*rgb, 220)
  assert dict(draws)[0.] == (255, 255, 255, 220)
  assert dict(draws)[3.] == (255, 255, 255, 220)


def test_display_fix_only_tints_the_existing_model_boundaries():
  draws, _, _ = render(left=31, right=21)
  assert len(draws) == 4
  assert dict(draws)[1.] == (50, 150, 255, 220)
  assert dict(draws)[2.] == (218, 202, 37, 220)


def test_low_model_confidence_keeps_color_transparent():
  draws, _, _ = render(left=31, right=21, probabilities=[.9, .2, .1, .9])
  assert dict(draws)[1.][-1] == 0
  assert dict(draws)[2.][-1] == 0


def test_invalid_model_or_car_state_draws_nothing():
  assert render(left=31, valid=False) == ([], [], [])


@pytest.mark.parametrize('code,rgb', [(24, (218, 202, 37)), (34, (50, 150, 255))])
def test_double_line_and_existing_colored_style_remain(code, rgb):
  draws, mapped, outlines = render(left=code, right=31)
  assert len(draws) == 5
  assert dict(draws)[1.] == dict(draws)[1.1] == (*rgb, 220)
  assert next(width for index, width, double in mapped if index == 1. and not double) == .05
  assert len(outlines) == 2


def test_unknown_color_does_not_inherit_yellow_width_or_outline():
  _, mapped, outlines = render(left=41)
  assert next(width for index, width, double in mapped if index == 1. and not double) == .025
  assert outlines == []
