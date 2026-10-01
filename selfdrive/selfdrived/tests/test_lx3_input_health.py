"""Input-health gates in production controlsd/card bodies; no sockets or CAN TX."""
import ast
from pathlib import Path
import runpy
from types import SimpleNamespace as NS
import unittest

from selfdrive.selfdrived.tests import test_lx3_engagement as host

ROOT = Path(__file__).resolve().parents[3]
READY = host.MODULE['lx3_input_ready']
TRANSPORT = runpy.run_path(str(ROOT / 'selfdrive/selfdrived/lx3_transport.py'))


class TestInputHealthConsumers(unittest.TestCase):
  def setUp(self):
    self.cs = NS(canValid=True, lx3InputState='ready', lx3PhysicalCounterValid=True,
                 steerFaultTemporary=False, steerFaultPermanent=False)
    self.ss = NS(lx3EngagementMode=2, enabled=True, active=True, lx3AcceptedGeneration=12,
                 lx3AcceptedPhysicalCounter=40, lx3AcceptedTransportEpoch=0x123456789ABCDEF0)

  def control(self):
    # Execute the exact production LX3 branch, including axis AND identity
    # gates, against a still-accepted selfdriveState during CarState input loss.
    path = ROOT / 'selfdrive/controls/controlsd.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))
    branch = next(node for node in ast.walk(tree) if isinstance(node, ast.If) and
                  ast.unparse(node.test) == "self.CP.carFingerprint == 'HYUNDAI_PALISADE_LX3_HEV'")
    sm = host.SubMaster(selfdriveState=self.ss, onroadEvents=[])
    cc = NS()
    context = NS(CP=NS(carFingerprint='HYUNDAI_PALISADE_LX3_HEV', openpilotLongitudinalControl=True),
                 sm=sm, carrot_controls=NS(lat_suspend_control=lambda cs, active: active))
    env = dict(self=context, CC=cc, CS=self.cs, driving_gear=True, standstill=False,
               lx3_input_ready=READY, lx3_control_permissions=host.permissions,
               stamp_control_identity=TRANSPORT['stamp_control_identity'])
    exec(compile(ast.Module(body=[branch], type_ignores=[]), str(path), 'exec'), env)
    return cc

  def test_input_loss_blocks_both_axes_and_identity_before_host_state_catches_up(self):
    cc = self.control()
    self.assertTrue(cc.latActive and cc.longActive and cc.enabled and cc.lx3IdentityValid)
    for state, valid in (('warmingUp', False), ('requalifying', False), ('integrityFault', False),
                         ('notApplicable', False), ('ready', False)):
      with self.subTest(state=state):
        self.cs.lx3InputState, self.cs.lx3PhysicalCounterValid = state, valid
        cc = self.control()
        self.assertFalse(cc.latActive or cc.longActive or cc.enabled or cc.lx3IdentityValid)

  def test_genuine_eps_fault_still_blocks_control(self):
    self.cs.steerFaultTemporary = True
    cc = self.control()
    self.assertFalse(cc.latActive or cc.longActive or cc.enabled)

  def card(self, fingerprint='HYUNDAI_PALISADE_LX3_HEV'):
    messages = [(address, bytes(32), 0) for address in TRANSPORT['GUARDED_ADDRESSES']] + [(0x730, bytes(8), 1)]
    sent = []
    sm = host.SubMaster(modelV2=None)
    sm.valid = sm.alive = {'modelV2': False}
    sm.all_alive = lambda services: True
    context = NS(initialized_prev=True, sm=sm, CP=NS(carFingerprint=fingerprint),
                 CI=NS(apply=lambda *args: (None, messages)), pm=NS(send=lambda *args: sent.append(args)))
    cc = NS(lx3IdentityValid=True, lx3Generation=12, lx3PhysicalCounter=40,
            lx3Mode=2, lx3TransportEpoch=self.ss.lx3AcceptedTransportEpoch)
    env = dict(car=NS(CarState=NS, CarControl=NS), REPLAY=False, time=NS(monotonic=lambda: 1.0),
               prepare_sendcan=TRANSPORT['prepare_sendcan'], lx3_input_ready=READY,
               can_list_to_can_capnp=lambda frames, **kwargs: (frames, kwargs))
    host.load_definitions(ROOT / 'selfdrive/car/card.py', env, {'controls_update'})
    env['controls_update'](context, self.cs, cc)
    return sent[0][1]

  def test_card_rechecks_current_input_health_and_filters_owned_only(self):
    frames, metadata = self.card()
    self.assertEqual(len(frames), 11)
    self.assertIsNotNone(metadata['lx3_identity'])
    self.cs.lx3InputState, self.cs.lx3PhysicalCounterValid = 'integrityFault', False
    frames, metadata = self.card()
    self.assertEqual([frame[0] for frame in frames], [0x730])
    self.assertIsNone(metadata['lx3_identity'])

  def test_other_car_keeps_its_existing_producer_path_with_default_fields(self):
    del self.cs.lx3InputState
    del self.cs.lx3PhysicalCounterValid
    self.assertFalse(READY(self.cs))
    frames, metadata = self.card('HYUNDAI_SORENTO')
    self.assertEqual(len(frames), 11)
    self.assertIsNone(metadata['lx3_identity'])


if __name__ == '__main__':
  unittest.main()
