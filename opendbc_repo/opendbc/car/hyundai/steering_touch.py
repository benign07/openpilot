"""Receive-profile wheel touch, independent of vehicle name and ADAS forwarding."""
import math

from opendbc.car.crc import CRC8J1850, mk_crc8_fun
from opendbc.car.hyundai.hyundaicanfd import hkg_can_fd_checksum

TOUCH_ADDR = 0x2AF
TOUCH_MSG = 'STEER_TOUCH_2AF'
TOUCH_TIMEOUT_NS = 250_000_000
_crc = mk_crc8_fun(CRC8J1850)


def touch_checksum(data: bytes) -> int:
  # Empirical receive profile: polynomial 0x1D, zero initial register, residual
  # 0x32 over bytes 1..7. Verified on independent Ioniq 5 PE logs; this is not
  # an OEM protocol specification or the existing transmit checksum function.
  return _crc(data) ^ 0x32


class HyundaiSteeringTouch:
  def __init__(self):
    self.last_timestamp = 0
    self.last_counter = None
    self.frame_valid = False

  def update(self, cp) -> dict:
    # Scope the address to the named DBC message on original ECAN. A matching
    # numeric ID in another vehicle protocol is not touch evidence.
    message = cp.dbc.name_to_msg.get(TOUCH_MSG)
    if message is None or message.address != TOUCH_ADDR or message.size != 8:
      return {}
    # Observe even after the startup fingerprint window. Register only once a
    # real frame was seen; absence/dropout must not create a CAN-validity fault.
    if TOUCH_ADDR not in cp.addresses and TOUCH_ADDR in cp.seen_addresses:
      cp._add_message(TOUCH_MSG, math.nan)
    timestamp = cp.ts_nanos.get(TOUCH_MSG, {}).get('TOUCH_DETECT', 0)
    data = cp.dat.get(TOUCH_ADDR, b'')
    now = cp._last_update_nanos
    fresh = timestamp > 0 and 0 <= now - timestamp <= TOUCH_TIMEOUT_NS and not cp.bus_timeout
    if timestamp != self.last_timestamp:
      previous_timestamp = self.last_timestamp
      previous_counter = self.last_counter
      self.last_timestamp = timestamp
      layout_ok = (len(data) == 8 and data[1] & 0x0f == 0 and data[1] >> 4 < 15 and
                   data[2] <= 4 and data[3] == 1 and data[6:] == b'\x01\x00')
      integrity = layout_ok and data[0] == touch_checksum(data[1:])
      counter = data[1] >> 4 if integrity else None
      self.frame_valid = (fresh and integrity and previous_counter is not None and
                          0 < timestamp - previous_timestamp <= TOUCH_TIMEOUT_NS and
                          counter == (previous_counter + 1) % 15)
      self.last_counter = counter if fresh else None
    if not fresh:
      self.frame_valid = False
      self.last_counter = None
    valid = fresh and self.frame_valid
    return dict(available=timestamp > 0, valid=valid, touched=bool(valid and data[2] >= 1),
                sampleMonoTime=timestamp, rawStatus=data[2] if len(data) == 8 else 0,
                rawTouch1=data[4] if len(data) == 8 else 0, rawTouch2=data[5] if len(data) == 8 else 0)


class LX3SteeringTouch:
  """Read-only LX3 receive profile, separate from the other Hyundai 0x2AF profile."""

  ADDR = 0x208
  MSG = "STEER_TOUCH_LX3"
  TIMEOUT_NS = 250_000_000  # The observed original-bus stream is about 5 Hz.

  def __init__(self):
    self.last_timestamp = 0
    self.last_counter = None
    self.frame_valid = False

  def update(self, cp) -> dict:
    message = cp.dbc.name_to_msg.get(self.MSG)
    if message is None or message.address != self.ADDR or message.size != 16:
      return {}
    # Optional receive-only registration after an original-bus frame appears.
    # Absence of this sensor must never make the whole CAN parser invalid.
    if self.ADDR not in cp.addresses and self.ADDR in cp.seen_addresses:
      cp._add_message(self.MSG, math.nan)

    timestamp = cp.ts_nanos.get(self.MSG, {}).get("TOUCH_RAW1", 0)
    data = cp.dat.get(self.ADDR, b'')
    now = cp._last_update_nanos
    fresh = timestamp > 0 and 0 <= now - timestamp <= self.TIMEOUT_NS and not cp.bus_timeout

    if timestamp != self.last_timestamp:
      previous_timestamp = self.last_timestamp
      previous_counter = self.last_counter
      self.last_timestamp = timestamp
      # Only the observed LX3 layout is accepted. Byte 2 advanced by two in
      # 1,498 consecutive samples; an unknown counter jump revokes evidence.
      layout_ok = (len(data) == 16 and data[3:10] == b'\x00' * 7 and
                   data[10] <= 4 and data[11] == 1 and data[14:] == b'\x01\x00')
      integrity = layout_ok and int.from_bytes(data[:2], 'little') == hkg_can_fd_checksum(self.ADDR, None, bytearray(data))
      counter = data[2] if integrity else None
      self.frame_valid = (fresh and integrity and previous_counter is not None and
                          0 < timestamp - previous_timestamp <= self.TIMEOUT_NS and
                          counter == (previous_counter + 2) % 256)
      self.last_counter = counter if fresh else None

    if not fresh:
      self.frame_valid = False
      self.last_counter = None

    valid = fresh and self.frame_valid
    return dict(available=timestamp > 0, valid=valid,
                # Byte 12 is an observed contact magnitude, not a bit field:
                # a recorded continuous grip reaches 64 (bit 5 clears).
                touched=bool(valid and data[10] in (3, 4) and data[12] >= 32),
                sampleMonoTime=timestamp, rawStatus=data[10] if len(data) == 16 else 0,
                rawTouch1=data[12] if len(data) == 16 else 0,
                rawTouch2=data[13] if len(data) == 16 else 0)
