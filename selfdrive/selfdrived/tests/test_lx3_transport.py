"""Real producer helpers/schema serializer, no vehicle/device access."""
import copy
from pathlib import Path
import runpy
import shutil
import tempfile
from types import SimpleNamespace as NS
import unittest

from selfdrive.selfdrived.tests import test_lx3_engagement as host

ROOT = Path(__file__).resolve().parents[3]
TRANSPORT = runpy.run_path(str(ROOT / 'selfdrive/selfdrived/lx3_transport.py'))
stamp = TRANSPORT['stamp_control_identity']
prepare = TRANSPORT['prepare_sendcan']
OWNED = TRANSPORT['GUARDED_ADDRESSES']


class TestProducerIdentity(unittest.TestCase):
  def setUp(self):
    self.ss = NS(enabled=True, active=True, lx3EngagementMode=2, lx3AcceptedGeneration=12,
                 lx3AcceptedPhysicalCounter=254, lx3AcceptedTransportEpoch=0x123456789ABCDEF0,
                 lx3AckValid=False, lx3AckGeneration=0, lx3AckPhysicalCounter=0)
    self.cc = NS(latActive=True, longActive=True)
    self.messages = [(addr, bytes(8), 0) for addr in sorted(OWNED)] + [(0x730, b'\x02\x3e\x80' + bytes(5), 1)]

  def test_committed_identity_survives_cleared_ack(self):
    stamp(self.cc, self.ss, True)
    self.assertTrue(self.cc.lx3IdentityValid)
    self.assertEqual(self.cc.lx3Generation, 12)

  def test_old_control_is_never_retagged_after_new_session(self):
    stamp(self.cc, self.ss, True)
    old_cc = copy.deepcopy(self.cc)
    self.ss.lx3AcceptedGeneration += 2
    self.ss.lx3AcceptedPhysicalCounter = 4
    self.ss.lx3AcceptedTransportEpoch ^= 1
    stamp(self.cc, self.ss, True)
    _, old = prepare(self.messages, old_cc)
    _, new = prepare(self.messages, self.cc)
    self.assertEqual((old['generation'], old['counter'], old['epoch']), (12, 254, 0x123456789ABCDEF0))
    self.assertNotEqual(old, new)

  def test_pre_enable_has_no_committed_identity(self):
    self.ss.active = False
    self.ss.lx3AcceptedGeneration = 0
    self.ss.lx3AckValid = True
    self.ss.lx3AckGeneration = 99
    stamp(self.cc, self.ss, True)
    kept, identity = prepare(self.messages, self.cc)
    self.assertIsNone(identity)
    self.assertEqual([m[0] for m in kept], [0x730])

  def test_inactive_axes_do_not_erase_accepted_session(self):
    self.cc.latActive = self.cc.longActive = False
    self.ss.active = False
    stamp(self.cc, self.ss, True)
    _, identity = prepare(self.messages, self.cc)
    self.assertIsNotNone(identity)

  def test_invalid_source_off_and_malformed_identity_remove_all_owned_frames(self):
    for field, value in (('enabled', False), ('lx3EngagementMode', 0), ('lx3AcceptedGeneration', 0),
                         ('lx3AcceptedPhysicalCounter', 256), ('lx3AcceptedTransportEpoch', 0)):
      with self.subTest(field=field):
        ss = copy.deepcopy(self.ss); setattr(ss, field, value)
        stamp(self.cc, ss, True)
        kept, identity = prepare(self.messages, self.cc)
        self.assertIsNone(identity); self.assertEqual([m[0] for m in kept], [0x730])
    stamp(self.cc, self.ss, False)
    self.assertIsNone(prepare(self.messages, self.cc)[1])

  def test_card_invalid_control_or_can_drops_owned_only(self):
    stamp(self.cc, self.ss, True)
    kept, identity = prepare(self.messages, self.cc, fresh=False)
    self.assertIsNone(identity); self.assertEqual([m[0] for m in kept], [0x730])
    self.assertTrue(self.cc.lx3IdentityValid)  # No mutation of the old producer.

  def test_old_producer_defaults_to_no_identity(self):
    kept, identity = prepare(self.messages, NS(latActive=True, longActive=True))
    self.assertIsNone(identity); self.assertEqual([m[0] for m in kept], [0x730])

  def test_serializer_preserves_old_identity_and_stamps_owned_only(self):
    import capnp
    capnp.remove_import_hook()
    # Git's symlink stubs on Windows must be resolved without editing checkout.
    with tempfile.TemporaryDirectory() as directory:
      destination = Path(directory)
      for source in (ROOT / 'cereal').glob('*.capnp'):
        text = source.read_text(encoding='utf-8')
        actual = (source.parent / text.strip()).resolve() if text.startswith('../') else source
        shutil.copyfile(actual, destination / source.name)
      shutil.copytree(ROOT / 'cereal/include', destination / 'include')
      schema = capnp.SchemaParser().load(str(destination / 'log.capnp'))
    env = dict(log=schema, time=__import__('time'), _cached_writer_fields=None,
               GUARDED_ADDRESSES=OWNED, identity_valid=TRANSPORT['identity_valid'])
    host.load_definitions(ROOT / 'selfdrive/pandad/pandad_api_impl.py', env,
                     {'can_list_to_can_capnp', '_get_writer_fields'})
    stamp(self.cc, self.ss, True)
    msgs, identity = prepare(self.messages, self.cc)
    data = env['can_list_to_can_capnp'](msgs, msgtype='sendcan', lx3_identity=identity)
    with schema.Event.from_bytes(data) as event:
      for item in event.sendcan:
        self.assertEqual(item.lx3IdentityValid, item.address in OWNED)
        if item.address in OWNED:
          self.assertEqual((item.lx3Generation, item.lx3PhysicalCounter, item.lx3TransportEpoch),
                           (12, 254, 0x123456789ABCDEF0))
    data = env['can_list_to_can_capnp'](msgs, msgtype='can', lx3_identity=identity)
    with schema.Event.from_bytes(data) as event:
      self.assertTrue(all(not item.lx3IdentityValid for item in event.can))
    data = env['can_list_to_can_capnp'](msgs, msgtype='sendcan')
    with schema.Event.from_bytes(data) as event:
      self.assertTrue(all(not item.lx3IdentityValid for item in event.sendcan))


class TestEpochSession(unittest.TestCase):
  def setUp(self):
    self.case = host.TestLx3Session()
    self.case.setUp()

  def test_epoch_change_revokes_accepted_even_with_same_other_identity(self):
    self.case.engage()
    self.case.panda.lx3TransportEpoch ^= 1
    self.assertEqual(self.case.step(), (False, False))
    self.assertFalse(self.case.ctx.enabled)

  def test_epoch_change_during_pending_never_commits(self):
    self.case.pending(1, 40)
    self.case.step(host.button('lfaButton', counter=40))
    self.case.panda.lx3TransportEpoch ^= 1
    self.case.accept()
    self.assertEqual(self.case.step(), (False, False))

  def test_v1_companion_no_longer_grants_v2_transport(self):
    self.case.panda.lx3PermissionVersion = 1
    self.assertEqual(self.case.engage(), (False, False))


class TestInputHealthSchema(unittest.TestCase):
  def test_additive_carstate_defaults_and_enum_roundtrip_are_recorded(self):
    import capnp
    capnp.remove_import_hook()
    from selfdrive.carrot.can_diagnostics.automatic_runtime import selected_fields, FIELDS
    with tempfile.TemporaryDirectory() as directory:
      destination = Path(directory)
      shutil.copyfile(ROOT / 'opendbc_repo/opendbc/car/car.capnp', destination / 'car.capnp')
      shutil.copytree(ROOT / 'opendbc_repo/opendbc/car/include', destination / 'include')
      schema = capnp.SchemaParser().load(str(destination / 'car.capnp'))
    old = schema.CarState.new_message(canValid=True)
    self.assertEqual(str(old.lx3InputState), 'notApplicable')
    self.assertFalse(old.lx3PhysicalCounterValid)
    self.assertEqual(old.lx3InputResetCount, 0)
    self.assertFalse(host.MODULE['lx3_input_ready'](old))
    for state in ('notApplicable', 'warmingUp', 'ready', 'requalifying', 'integrityFault'):
      msg = schema.CarState.new_message(lx3InputState=state, lx3PhysicalCounter=254,
                                        lx3PhysicalCounterValid=state == 'ready', lx3InputReason='checksum',
                                        lx3InputResetCount=4294967295)
      with schema.CarState.from_bytes(msg.to_bytes()) as parsed:
        sample = selected_fields(parsed, FIELDS['carState'])
        self.assertEqual(sample['lx3InputState'], state)
        self.assertEqual(sample['lx3PhysicalCounter'], 254)
        self.assertEqual(sample['lx3InputReason'], 'checksum')
        self.assertEqual(sample['lx3InputResetCount'], 4294967295)
        self.assertEqual(sample['lx3PhysicalCounterValid'], state == 'ready')
        self.assertEqual(host.MODULE['lx3_input_ready'](parsed), state == 'ready')


if __name__ == '__main__':
  unittest.main()
