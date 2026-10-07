"""Physical LX3 switch observations, independent of Carrot's button settings.

Only received bus-0 0x10B frames belong here. Returned/rejected host frames are
not physical input. A release is identified by its first neutral wire counter,
including the MAIN debounce; processing the same cached value is not an edge.
"""
from dataclasses import dataclass
import binascii

LX3_AUTHORITY_FLAG = 2048
BUTTON_TIMEOUT_NS = 200_000_000
HOST_CONTINUITY_NS = 1_000_000_000
MAIN_RELEASE_NS = 300_000_000
LFA = 128
MAIN = 8
CANCEL = 4


def uses_lx3_authority(CP) -> bool:
  return CP.carFingerprint == "HYUNDAI_PALISADE_LX3_HEV" and any(
    int(c.safetyParam) & LX3_AUTHORITY_FLAG for c in CP.safetyConfigs)


@dataclass(frozen=True)
class PhysicalButton:
  button: int
  pressed: bool
  counter: int
  mono_ns: int
  held_ns: int = 0


class PhysicalButtons:
  def __init__(self):
    self.last_ns = None
    self.counter = 0
    self.neutral = 0
    self.ready = False
    self.held = 0
    self.raw_key = 0
    self.press_ns = 0
    self.main_held = False
    self.main_last_ns = 0
    self.main_press_ns = 0
    self.main_release_counter = 0
    self.main_neutral = False
    self.events: list[PhysicalButton] = []

  def invalidate(self):
    self.ready = False
    self.neutral = 0
    self.held = 0
    self.main_held = False
    self.main_neutral = False
    # A rejected batch must not manufacture a release/enable from a lost press.
    self.events[:] = [e for e in self.events if e.button == CANCEL and e.pressed]

  def healthy(self, now_ns: int) -> bool:
    return self.ready and self.stream_healthy(now_ns)

  def stream_healthy(self, now_ns: int) -> bool:
    return self.last_ns is not None and 0 <= now_ns - self.last_ns <= BUTTON_TIMEOUT_NS

  def feed(self, now_ns: int, address: int, data: bytes, bus: int):
    if address != 0x10B or bus != 0:
      return
    if (len(data) != 16 or
        int.from_bytes(data[:2], "little") != (binascii.crc_hqx(data[2:] + b"\x0b\x01", 0) ^ 0x041D)):
      self.invalidate()
      self.last_ns = None
      return
    raw = data[10]
    key = LFA if raw & 128 else raw & 15
    # Simultaneous LFA/cruise or an unknown switch value has no grant meaning.
    valid_key = key in (0, 1, 2, 3, CANCEL, MAIN, LFA) and not (raw & 128 and raw & 15)
    continuous = (self.last_ns is not None and 0 <= now_ns - self.last_ns <= HOST_CONTINUITY_NS
                  and ((data[2] - self.counter) & 255) == 2)
    if not continuous or not valid_key:
      self.invalidate()
    self.last_ns, self.counter = now_ns, data[2]
    if not valid_key:
      return
    if key == CANCEL:
      self.events.append(PhysicalButton(CANCEL, True, self.counter, now_ns))
    if not self.ready:
      self.neutral = self.neutral + 1 if key == 0 else 0
      self.ready = self.neutral >= 3
      self.raw_key = key
      return
    if key and self.raw_key and key != self.raw_key:
      self.invalidate()
      self.raw_key = key
      return
    self.raw_key = key

    if key == MAIN:
      if not self.main_held:
        self.main_held = True
        self.main_press_ns = now_ns
        self.events.append(PhysicalButton(MAIN, True, self.counter, now_ns))
      self.main_last_ns = now_ns
      self.main_neutral = False
    elif self.main_held:
      if not self.main_neutral:
        self.main_release_counter = self.counter
        self.main_neutral = True
      if now_ns - self.main_last_ns >= MAIN_RELEASE_NS:
        self.events.append(PhysicalButton(MAIN, False, self.main_release_counter, now_ns,
                                         self.main_last_ns - self.main_press_ns))
        self.main_held = False

    other = 0 if key == MAIN else key
    if other != self.held:
      # Direct switch-to-switch transitions do not prove a physical release.
      if self.held and other:
        self.invalidate()
        return
      if self.held and other == 0:
        self.events.append(PhysicalButton(self.held, False, self.counter, now_ns, now_ns - self.press_ns))
      if other and self.held == 0:
        if other != CANCEL:
          self.events.append(PhysicalButton(other, True, self.counter, now_ns))
        self.press_ns = now_ns
      self.held = other

  def update(self, can_packets):
    self.events.clear()
    for now_ns, frames in can_packets:
      for address, data, bus in frames:
        self.feed(now_ns, address, data, bus)
    return self.events
