// Offline ctypes bridge to the same production RX/STATE/TX policy as H7.
// Only clock and physical bytes are supplied; no setter grants authority.
#define main native_regression_main
#include "tests/test_lx3_authority_native.c"
#undef main

void fixture_init(void) {
  timer.CNT = 2000000U; sequence = 0U;
  assert(set_safety_hooks(SAFETY_HYUNDAI_CANFD, LX3_AUTHORITY_PROFILE) == 0);
  lx3_native_control(LX3_EPOCH_HIGH_REQUEST, (uint16_t)(epoch >> 48U), (uint16_t)(epoch >> 32U));
  lx3_native_control(LX3_EPOCH_LOW_REQUEST, (uint16_t)(epoch >> 16U), (uint16_t)epoch);
}
void fixture_time(uint32_t us) { timer.CNT=us; }
int fixture_rx(unsigned address, const uint8_t *data, unsigned length) {
  CANPacket_t p=packet(address,0U,length); memcpy(p.data,data,length);
  return safety_rx_hook(&p);
}
void fixture_status(lx3_status_t *s) { *s=lx3_native_status(); }
void fixture_state(unsigned intent, unsigned key, unsigned generation, unsigned automatic, unsigned revision, unsigned config, unsigned refuse, unsigned refuse_after) {
  state_config=config; state_refuse=refuse != 0U; state_refuse_after=refuse_after;
  state(intent,key,generation,automatic != 0U,revision);
}
int fixture_tx(unsigned address, const uint8_t *data, unsigned length, unsigned generation) {
  CANPacket_t p=packet(address,0U,length); memcpy(p.data,data,length);
  if (!send(&p,generation)) return 0;
  // The policy final gate is exercised separately from the transport test.
  lx3_queue_stamp_t stamp=lx3_current_tx_stamp;
  return lx3_native_final_tx(&p,&stamp);
}

int fixture_oem_lateral_replacement(void) {
  CANPacket_t p=cb(2U,90U,0); p.bus=2U;
  const int bus=safety_fwd_hook(&p);
  if (bus!=0 || lx3_forward_stamp.origin!=1U || !lx3_native_final_tx(&p,&lx3_forward_stamp)) return -1;
  return (((p.data[3] >> 4U) & 3U) << 8U) | p.data[6];
}

int fixture_oem_lateral_replaced_by_inactive(void) {
  return fixture_oem_lateral_replacement()==256;
}
