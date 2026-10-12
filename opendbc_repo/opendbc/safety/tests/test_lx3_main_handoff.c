// Same production policy and physical-input fixture as the native suite.
// This reproduces a possible handoff, not the exact MCU timing of a road log.
#define main existing_native_suite_main
#include "test_lx3_authority_native.c"
#undef main

int main(void) {
  reset(); main_button(); cite(LX3_ALL);
  assert(lx3_auth.allowed == LX3_ALL);
  const uint16_t generation = lx3_auth.lateral_generation;
  CANPacket_t active = cb(2U, 25U, 0);
  assert(send(&active, generation));
  lx3_queue_stamp_t stamp = lx3_current_tx_stamp;

  // Physical MAIN changes native permission before a lagging host publishes OFF.
  rx_tick(8U, false, false);
  // MAIN release is deliberately debounced for eight neutral wire samples.
  // Keep the existing host heartbeat during that tail; no permission shortcut.
  for (unsigned i = 0; i < 8; i++) {
    rx_tick(0U, false, false);
    if (i < 7) state(LX3_ALL, 0U, 0U, false, lx3_auth.longitudinal_revision);
  }
  assert(lx3_auth.allowed == 0U && lx3_auth.reason == LX3_REASON_CANCEL);
  // This old queued stamp is also beyond its age window: final rejection here
  // does not isolate permission from age. Neither path counts as LIMIT failure.
  assert(!lx3_native_final_tx(&active, &stamp));
  assert(lx3_limit_rejections[1] == 0U);
  active = cb(2U, 250U, 0);
  for (unsigned i = 0; i < 3; i++) {
    assert(!send(&active, generation));
    assert(lx3_auth.allowed == 0U && lx3_auth.reason == LX3_REASON_CANCEL);
    assert(lx3_limit_rejections[0] == 0U);
  }
  assert(!lx3_native_final_tx(&active, &stamp));
  assert(lx3_limit_rejections[1] == 0U);
  CANPacket_t scc = packet(0x1A0, 0U, 32U);
  scc.data[8] = 1U << 4U; hyundai_canfd_update_checksum(&scc);
  assert(!send(&scc, generation));
  CANPacket_t release = cb(1U, 0U, 0);
  assert(send(&release, generation));
  assert(lx3_auth.allowed == 0U);
  puts("PASS physical MAIN revoke rejects queued/late active 250 and SCC without a LIMIT revoke; inactive release does not grant permission");
  return 0;
}
