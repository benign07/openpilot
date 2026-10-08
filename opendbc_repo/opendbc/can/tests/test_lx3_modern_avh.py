"""Stock Carrot AVH decoder names while retaining the LX3 legacy bit alias.

Actual CAN pack/parser integration, not verification of the physical lamp.
"""
import unittest
from opendbc.can.packer import CANPacker
from opendbc.can.parser import CANParser


class TestLx3ModernAvh(unittest.TestCase):
  def test_stock_fields_and_legacy_alias_decode_the_same_wire_bytes(self):
    packer = CANPacker('hyundai_canfd_lx3_hev_generated')
    parser = CANParser('hyundai_canfd_lx3_hev_generated', [('ESP_STATUS', 100)], 0)
    for counter, state in enumerate(range(4)):
      frame = packer.make_can_msg('ESP_STATUS', 0, {'AVH_Sta': state, 'AVH_I_LAMP': 1, 'AVH_LAMP': 2})
      parser.update([(1_000_000_000 + counter * 10_000_000, [frame])])
      decoded = parser.vl['ESP_STATUS']
      self.assertEqual(decoded['AVH_Sta'], state)
      self.assertEqual(decoded['AVH_I_LAMP'], 1)
      self.assertEqual(decoded['AVH_LAMP'], 2)
      self.assertEqual(decoded['AUTO_HOLD'], state & 1)


if __name__ == '__main__': unittest.main()
