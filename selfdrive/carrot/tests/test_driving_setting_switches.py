"""Execute production parameter/speed-selection blocks without vehicle I/O."""
import ast
import enum
import re
import unittest
from pathlib import Path
from types import SimpleNamespace as NS

ROOT = Path(__file__).resolve().parents[3]

def method_nodes(path, name):
  tree=ast.parse((ROOT/path).read_text(encoding='utf-8'))
  return next(n for n in ast.walk(tree) if isinstance(n,ast.FunctionDef) and n.name==name)

def execute(nodes, env):
  exec(compile(ast.fix_missing_locations(ast.Module(body=nodes,type_ignores=[])),'production-code-block','exec'),env)

class Mode(enum.IntEnum): Eco=1; Safe=2; Normal=3; High=4

class TestDrivingSettingSwitches(unittest.TestCase):
  def test_manual_override_then_explicit_auto_off_on(self):
    node=method_nodes('selfdrive/carrot/carrot_functions.py','_params_update')
    env={'DrivingMode':Mode}; execute([node],env)
    values={'MyDrivingMode':3,'MyDrivingModeAuto':1}
    planner=NS(frame=0,params_count=0,myDrivingMode_last=Mode.Normal,myDrivingMode_disable_auto=False,
      drivingModeDetector=NS(get_mode=lambda:Mode.Safe),params=NS(get_int=lambda k:values.get(k,0),get_float=lambda k:float(values.get(k,0))))
    def tick():
      for _ in range(10): env['_params_update'](planner)
    tick(); self.assertEqual(planner.myDrivingMode,Mode.Safe)
    values['MyDrivingMode']=4; tick(); self.assertEqual(planner.myDrivingMode,Mode.High)
    tick(); self.assertEqual(planner.myDrivingMode,Mode.High)
    values['MyDrivingModeAuto']=0; tick(); self.assertEqual(planner.myDrivingMode,Mode.High)
    values['MyDrivingModeAuto']=1; tick(); self.assertEqual(planner.myDrivingMode,Mode.Safe)
    self.assertFalse(planner.myDrivingMode_disable_auto)

  def candidates(self, navi_mode, turn_mode, source_type=0):
    method=method_nodes('selfdrive/carrot/carrot_serv.py','update_navi')
    start=next(i for i,n in enumerate(method.body) if isinstance(n,ast.If) and isinstance(n.test,ast.Compare) and
               isinstance(n.test.left,ast.Attribute) and n.test.left.attr=='autoNaviSpeedCtrlMode' and ast.unparse(n.test)=='self.autoNaviSpeedCtrlMode == 0')
    end=next(i for i,n in enumerate(method.body) if isinstance(n,ast.Assign) and ast.unparse(n.targets[0])=='(desired_speed, source)')
    env={'self':NS(autoNaviSpeedCtrlMode=navi_mode,autoTurnControl=0,turnSpeedControlMode=turn_mode,
      mapTurnSpeedFactor=1,autoCurveSpeedLowerLimit=45,xSpdType=source_type,xDistToTurn=0),
      'atc_desired':250,'atc_desired_next':250,'sdi_speed':40,'hda_active':True,'limit_speed':200,
      'route_speed':100,'vturn_speed':80,'sm':{'modelV2':NS(meta=NS(modelTurnSpeed=50))}}
    execute(method.body[start:end+1],env)
    return env['desired_speed'],env['source']

  def test_off_ignores_oem_waze_camera_and_model_candidates(self):
    for source_type in (0,4,22,100,101): self.assertEqual(self.candidates(0,0,source_type),(200,'road'))

  def test_enabled_navigation_still_limits_speed(self): self.assertEqual(self.candidates(1,0),(40,'hda'))
  def test_enabled_turn_control_keeps_model_candidate(self): self.assertEqual(self.candidates(0,1),(50,'model'))

if __name__=='__main__': unittest.main()
