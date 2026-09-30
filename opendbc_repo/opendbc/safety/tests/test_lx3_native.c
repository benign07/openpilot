// Compile the real safety implementation; this program never opens a CAN device.
#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

// Firmware-owned symbols absent from the desktop libsafety harness.
bool safety_tx_buffered_for_fwd = false;
void putui(uint32_t value) { (void)value; }
#include "libsafety/safety.c"

static CANPacket_t packet(unsigned int addr, unsigned int bus, unsigned int dlc) {
  CANPacket_t p = {0};
  p.fd = 1;
  p.addr = addr;
  p.bus = bus;
  p.data_len_code = dlc;
  return p;
}

static void reset(uint16_t param) {
  assert(set_safety_hooks(SAFETY_HYUNDAI_CANFD, param) == 0);
  init_tests();
  set_timer(1000000U);
  safety_tx_buffered_for_fwd = false;
}

static CANPacket_t angle_command(bool active) {
  CANPacket_t p = packet(0xCB, 0, 12);  // 24 bytes; LX3 DBC bit 29, Motorola 2-bit.
  p.data[3] = active ? 0x20U : 0U;
  p.data[6] = active ? 25U : 0U;
  return p;
}

static CANPacket_t accel_command(void) {
  CANPacket_t p = packet(0x1A0, 0, 13);
  p.data[8] = 0x10U;  // ACCMode=1, raw and value acceleration=0.
  p.data[16] = 0xFFU;
  p.data[17] = 0xF3U;
  p.data[18] = 0x3FU;
  return p;
}

static void assert_rejected_without_side_effects(CANPacket_t *p) {
  assert(!safety_tx_hook(p));
  assert(!controls_allowed);
  assert(!safety_tx_buffered_for_fwd);
  for (int i = 0; canfd_bfwd[i].addr > 0; i++) {
    assert(canfd_bfwd[i].count == 0U);
    assert(!canfd_bfwd[i].has_last_pkt);
  }
}

static void rejected_tx_regressions(void) {
  for (uint16_t param = 0; param < 256U; param += 4U) {
    reset(param);
    int count = current_safety_config.tx_msgs_len;
    for (int i = 0; i < count; i++) {
      reset(param);
      const CanMsg msg = current_safety_config.tx_msgs[i];
      unsigned int dlc = 0;
      while (dlc < 16U && dlc_to_len[dlc] != msg.len) dlc++;
      assert(dlc < 16U);
      CANPacket_t p = packet(msg.addr, msg.bus, 0);
      assert_rejected_without_side_effects(&p);  // Zero DLC, payload not readable.
      p.data_len_code = dlc;
      p.bus = 3;
      assert_rejected_without_side_effects(&p);
      p.bus = msg.bus;
      set_relay_malfunction(true);
      assert_rejected_without_side_effects(&p);
    }
  }
  // Also exercise meaningful ACCMode data, not only zero-filled invalid frames.
  reset(190);
  CANPacket_t p = accel_command();
  p.bus = 3;
  assert_rejected_without_side_effects(&p);
  puts("PASS: rejected CAN-FD TX has no buffer or authorization side effects across 64 configurations");
}

static void compatibility_checks(void) {
  // HDA1/HDA2, stock/OP longitudinal, camera/radar, alternate buttons/steering.
  // Validates configuration selection and transparent unknown-frame forwarding,
  // not the complete steering/longitudinal behavior of every vehicle.
  for (uint16_t param = 0; param < 256U; param += 4U) {
    reset(param);
    assert(hyundai_canfd_buffered_fwd == ((param & 8U) != 0U));
    CANPacket_t p = packet(0x555, 2, 12);
    p.data[5] = 0xA5U;
    CANPacket_t original = p;
    assert(safety_fwd_hook(&p) == 0);
    assert(memcmp(&p, &original, sizeof(p)) == 0);
    p.bus = 0;
    assert(safety_fwd_hook(&p) == 2);
    p.bus = 1;
    assert(safety_fwd_hook(&p) == -1);
  }
  reset(190);
  set_controls_allowed(true);
  CANPacket_t command = angle_command(false);
  command.data[10] = 0xA5U;
  assert(safety_tx_hook(&command));
  assert(safety_tx_buffered_for_fwd);
  CANPacket_t stock = packet(0xCB, 2, 12);
  stock.data[2] = 123U;
  assert(safety_fwd_hook(&stock) == 0);
  assert(stock.data[10] == 0xA5U);
  assert(hyundai_canfd_get_counter(&stock) == 123U);
  assert(hyundai_canfd_get_checksum(&stock) == hyundai_common_canfd_compute_checksum(&stock));
  reset(190);
  assert(canfd_bfwd_find(0xCB, 0)->count == 0U);
  assert(!canfd_bfwd_find(0xCB, 0)->has_last_pkt);
  puts("PASS: 64 configuration combinations and buffered counter/checksum/reset compatibility");
}

static unsigned int blockers = 0;
static void check(const char *name, bool safe) {
  printf("AUDIT %s %s\n", safe ? "PASS" : "BLOCKER", name);
  blockers += safe ? 0U : 1U;
}

static void release_audit(void) {
  reset(190);
  CANPacket_t p = angle_command(true);
  p.data_len_code = 13;  // Wrong DLC must neither send nor enter forwarding queue.
  bool accepted = safety_tx_hook(&p);
  check("rejected_DLC_does_not_enqueue", !accepted && canfd_bfwd_find(0xCB, 0)->count == 0U);

  reset(190);
  set_relay_malfunction(true);
  p = angle_command(true);
  accepted = safety_tx_hook(&p);
  check("relay_fault_does_not_enqueue", !accepted && canfd_bfwd_find(0xCB, 0)->count == 0U);

  reset(190);
  p = accel_command();
  p.bus = 3;  // Rejected bus must not authorize future controls.
  accepted = safety_tx_hook(&p);
  check("rejected_bus_does_not_authorize", !accepted && !controls_allowed);

  reset(190);
  p = accel_command();
  (void)safety_tx_hook(&p);
  check("ACC_TX_does_not_self_authorize", !controls_allowed);

  reset(190);
  p = angle_command(true);
  accepted = safety_tx_hook(&p);
  check("active_angle_requires_controls", !accepted && canfd_bfwd_find(0xCB, 0)->count == 0U);

  reset(190);
  set_controls_allowed(true);
  p = angle_command(true);
  assert(safety_tx_hook(&p));
  set_controls_allowed(false);
  CANPacket_t stock = angle_command(false);
  stock.bus = 2;
  int destination = safety_fwd_hook(&stock);
  check("cancel_does_not_replay_active_angle", destination != 0 || (stock.data[3] & 0x30U) != 0x20U);

  reset(190);
  set_controls_allowed(true);
  p = angle_command(true);
  assert(safety_tx_hook(&p));
  stock = angle_command(false);
  stock.bus = 2;
  assert(safety_fwd_hook(&stock) == 0);  // Consume, then test last-packet reuse after 1s.
  set_timer(2000000U);
  stock = angle_command(false);
  stock.bus = 2;
  destination = safety_fwd_hook(&stock);
  check("expired_active_angle_not_reused", destination != 0 || (stock.data[3] & 0x30U) != 0x20U);

  reset(190);
  set_controls_allowed(true);
  p = angle_command(true);
  assert(safety_tx_hook(&p));
  assert(safety_tx_hook(&p));
  p = angle_command(false);
  assert(safety_tx_hook(&p));
  bool inactive_seen = false;
  for (int i = 0; i < 5; i++) {
    stock = angle_command(true);  // Distinguish fallback OEM data from accepted OFF.
    stock.bus = 2;
    destination = safety_fwd_hook(&stock);
    inactive_seen |= destination == 0 && (stock.data[3] & 0x30U) == 0U;
  }
  check("full_queue_retains_new_OFF_command", inactive_seen);
  printf("RELEASE_AUDIT blockers=%u; this is not vehicle qualification\n", blockers);
}

int main(int argc, char **argv) {
  if (argc == 2 && strcmp(argv[1], "--release-audit") == 0) {
    release_audit();
    return blockers == 0U ? 0 : 1;
  }
  compatibility_checks();
  rejected_tx_regressions();
  return 0;
}
