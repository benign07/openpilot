"""Immutable producer identity for guarded LX3 CAN; no hardware access."""
GUARDED_ADDRESSES = frozenset((0xCB, 0x12A, 0x1A0, 0x161, 0x162, 0x1E0, 0x1EA, 0x200, 0x362, 0x2A4))


def identity_valid(generation, counter, mode, epoch):
  return (1 <= generation <= 65535 and 0 <= counter <= 255 and mode in (1, 2)
          and 1 <= epoch <= 2**64 - 1)


def stamp_control_identity(cc, ss, fresh):
  """Use the same selfdriveState as axes, never a current Panda/ACK substitute."""
  generation = getattr(ss, 'lx3AcceptedGeneration', 0)
  counter = getattr(ss, 'lx3AcceptedPhysicalCounter', 0)
  epoch = getattr(ss, 'lx3AcceptedTransportEpoch', 0)
  mode = ss.lx3EngagementMode
  valid = bool(fresh and ss.enabled and identity_valid(generation, counter, mode, epoch))
  cc.lx3IdentityValid = valid
  cc.lx3Generation = generation if valid else 0
  cc.lx3PhysicalCounter = counter if valid else 0
  cc.lx3Mode = mode if valid else 0
  cc.lx3TransportEpoch = epoch if valid else 0


def prepare_sendcan(messages, cc, fresh=True):
  """Copy the originating CarControl identity. No newer state can retag it."""
  identity = dict(generation=getattr(cc, 'lx3Generation', 0), counter=getattr(cc, 'lx3PhysicalCounter', 0),
                  mode=getattr(cc, 'lx3Mode', 0), epoch=getattr(cc, 'lx3TransportEpoch', 0))
  valid = fresh and getattr(cc, 'lx3IdentityValid', False) and identity_valid(**identity)
  if not valid:
    # OFF: keep diagnostic/non-owned traffic, let original owned frames pass.
    return [msg for msg in messages if msg[0] not in GUARDED_ADDRESSES], None
  return messages, identity
