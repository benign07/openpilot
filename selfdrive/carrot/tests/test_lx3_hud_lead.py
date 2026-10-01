"""Execute the real controlsd HUD selection block, without vehicle I/O."""
import ast
import math
from pathlib import Path
from types import SimpleNamespace as NS
import unittest

ROOT = Path(__file__).resolve().parents[3]


class TestLx3HudLead(unittest.TestCase):
  def lead(self, distance, status=True, **fields):
    return NS(dRel=distance, status=status, yRel=0.3, vRel=-1.0,
              radar=True, dPath=0.2, **fields)

  def hud(self, one, two, lx3=True, alive=True, valid=True, target=None):
    tree = ast.parse((ROOT / 'selfdrive/controls/controlsd.py').read_text(encoding='utf-8'))
    method = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == 'publish')
    start = next(i for i, n in enumerate(method.body) if isinstance(n, ast.Assign)
                 and any(isinstance(t, ast.Name) and t.id == 'radarState' for t in n.targets))
    end = next(i for i in range(start + 1, len(method.body)) if isinstance(method.body[i], ast.Assign)
               and any(isinstance(t, ast.Name) and t.id == 'meta' for t in method.body[i].targets))
    sm = type('Messages', (dict,), {})(radarState=NS(leadOne=one, leadTwo=two))
    sm.alive, sm.valid = {'radarState': alive}, {'radarState': valid}
    if target is None:
      target = NS(leadVisible=True)
    env = dict(self=NS(CP=NS(carFingerprint='HYUNDAI_PALISADE_LX3_HEV' if lx3 else 'OTHER'), sm=sm),
               hudControl=target)
    # Load only the small production selector, when the implementation imports it.
    helper = ROOT / 'selfdrive/carrot/hud_lead.py'
    if helper.exists():
      import runpy
      env['select_display_lead'] = runpy.run_path(str(helper))['select_display_lead']
    exec(compile(ast.Module(body=method.body[start:end], type_ignores=[]), 'production-hud-block', 'exec'), env)
    return target

  def test_nearer_second_lead_supplies_all_hud_fields(self):
    first, second = self.lead(40), self.lead(12)
    second.vRel, second.dPath, second.radar = -3.0, -0.8, False
    hud = self.hud(first, second)
    self.assertEqual((hud.leadVisible, hud.leadDistance, hud.leadRelSpeed, hud.leadRadar, hud.leadDPath),
                     (True, 12, -3.0, 0, -0.8))

  def test_missing_first_lead_keeps_valid_second_visible(self):
    hud = self.hud(self.lead(0, False), self.lead(18))
    self.assertTrue(hud.leadVisible)
    self.assertEqual(hud.leadDistance, 18)

  def test_equal_distance_retains_first_lead(self):
    first, second = self.lead(18), self.lead(18)
    first.dPath, second.dPath = -1, 1
    self.assertEqual(self.hud(first, second).leadDPath, -1)

  def test_invalid_second_lead_does_not_replace_first(self):
    for bad in (self.lead(5, False), self.lead(0), self.lead(-1), self.lead(math.nan), self.lead(math.inf)):
      self.assertEqual(self.hud(self.lead(18), bad).leadDistance, 18)

  def test_nonfinite_lateral_or_relative_speed_is_not_a_valid_display_lead(self):
    for field in ('yRel', 'vRel'):
      bad = self.lead(5)
      setattr(bad, field, math.nan)
      self.assertEqual(self.hud(bad, self.lead(18)).leadDistance, 18)

  def test_no_valid_lead_clears_previous_hud_fields(self):
    hud = self.hud(self.lead(0, False), self.lead(0, False))
    self.assertEqual((hud.leadVisible, hud.leadDistance, hud.leadRelSpeed, hud.leadRadar, hud.leadDPath),
                     (False, 0, 0, 0, 0))

  def test_unhealthy_radar_publication_cannot_replay_last_visible_target(self):
    for alive, valid in ((False, True), (True, False), (False, False)):
      hud = self.hud(self.lead(18), self.lead(12), alive=alive, valid=valid)
      self.assertFalse(hud.leadVisible)
      self.assertEqual(hud.leadDistance, 0)

  def test_other_platforms_keep_existing_first_lead_and_visibility_behavior(self):
    for status in (False, True):
      first, second = self.lead(18, status), self.lead(12)
      hud = self.hud(first, second, lx3=False, alive=False)
      self.assertTrue(hud.leadVisible)
      self.assertEqual(hud.leadDistance, 18 if status else 0)
      self.assertEqual(hud.leadRadar, 1)

  def test_selection_does_not_mutate_control_leads(self):
    first, second = self.lead(18), self.lead(12)
    before = (vars(first).copy(), vars(second).copy())
    self.hud(first, second)
    self.assertEqual(before, (vars(first), vars(second)))

  def test_scc_hud_selection_changes_no_actuator_or_original_object_fields(self):
    from selfdrive.selfdrived.tests.test_lx3_cluster_transport import TestLx3ClusterTransport
    from selfdrive.carrot.tests.test_lx3_can_time import DBC_FILE, definitions
    case = TestLx3ClusterTransport()
    case.setUp()
    definitions(ROOT / 'opendbc_repo/opendbc/car/hyundai/hyundaicanfd.py', case.env, {'create_acc_control_scc2'})
    case.cs.scc_control = dict.fromkeys(case.packer.dbc.name_to_msg['SCC_CONTROL'].sigs, 0)
    case.cs.softHoldActive = case.cs.paddle_button_prev = 0
    case.cs.out.gasPressed, case.cs.out.vEgo, case.cs.out.aEgo = False, 20, -1
    case.cs.scc_control.update(ACC_ObjDist=30, ACC_ObjRelSpd=-1.0, ACC_ObjLatPos=0.5, HUD_LEAD_INFO=1)
    jerk = NS(carrot_cruise=0, jerk_u=3.0, jerk_l=3.0)
    no_lead = self.hud(self.lead(0, False), self.lead(0, False))
    nearer = self.hud(self.lead(30), self.lead(12))
    no_lead.leadDistanceBars = nearer.leadDistanceBars = 2
    for enabled in (False, True):
      for stopping in (False, True):
        for override in (False, True):
          messages = [case.env['create_acc_control_scc2'](
              case.env['CANPacker'](str(DBC_FILE)), case.can, enabled, -0.9, -1.0, stopping, override, 80,
              hud, jerk, case.cs, lx3_guard=True) for hud in (no_lead, nearer)]
          self.assertEqual(messages[0], messages[1])
          raw = messages[0][1]
          for name in ('ACC_ObjDist', 'ACC_ObjRelSpd', 'ACC_ObjLatPos', 'HUD_LEAD_INFO'):
            signal = case.packer.dbc.name_to_msg['SCC_CONTROL'].sigs[name]
            value = case.env['get_raw_value'](raw, signal) * signal.factor + signal.offset
            self.assertAlmostEqual(value, case.cs.scc_control[name])


if __name__ == '__main__':
  unittest.main()
