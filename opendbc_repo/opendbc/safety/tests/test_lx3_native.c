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

static void fresh_mdps(int angle, int torque) {
  CANPacket_t p = packet(0xEA, 0, 12);
  const unsigned int encoded_torque = (unsigned int)(torque + 4095);
  p.data[10] = encoded_torque & 0xFFU;
  p.data[11] = (encoded_torque >> 8U) & 0x1FU;
  p.data[16] = (unsigned int)angle & 0xFFU;
  p.data[17] = ((unsigned int)angle >> 8U) & 0xFFU;
  hyundai_canfd_update_checksum(&p);
  // Isolated RX function fixture; full RX configuration is tested separately.
  hyundai_canfd_rx_hook(&p);
}

static void reset(uint16_t param) {
  assert(set_safety_hooks(SAFETY_HYUNDAI_CANFD, param) == 0);
  init_tests();
  set_timer(1000000U);
  safety_tx_buffered_for_fwd = false;
  if ((param & HYUNDAI_PARAM_LX3_ENGAGEMENT_GUARD) != 0U) fresh_mdps(0, 0);
}

// Explicit fixture permission for isolated actuator tests, not physical evidence.
static void grant_controls(void) {
  if (hyundai_canfd_lx3_guard) {
    lx3_mode = 2;
    lx3_button_seen = true;
    lx3_button_ready = true;
    lx3_button_us = microsecond_timer_get();
  }
  set_controls_allowed(true);
}

static void physical_button(unsigned int raw) {
  set_timer(microsecond_timer_get() + 40000U);
  fresh_mdps(0, 0);
  CANPacket_t p = packet(0x10B, 0, 10);
  p.data[2] = lx3_button_seen ? (uint8_t)(lx3_button_counter + 2U) : 250U;
  p.data[10] = raw;
  hyundai_canfd_update_checksum(&p);
  assert(safety_rx_hook(&p));
}

static void physical_baseline(void) {
  for (unsigned int i = 0; i < 3U; i++) physical_button(0);
  assert(lx3_button_ready && !controls_allowed);
}

static CANPacket_t angle_command(bool active) {
  CANPacket_t p = packet(0xCB, 0, 12);  // 24 bytes; LX3 DBC bit 29, Motorola 2-bit.
  p.data[3] = active ? 0x20U : 0U;
  p.data[6] = active ? 25U : 0U;
  return p;
}

static void set_angle(CANPacket_t *p, int angle) {
  unsigned int bits = (unsigned int)angle & 0x3FFFU;
  p->data[4] = bits & 0xFFU;
  p->data[5] = (p->data[5] & 0xC0U) | ((bits >> 8) & 0x3FU);
}

static CANPacket_t stock_angle(bool active) {
  CANPacket_t p = angle_command(active);
  p.bus = 2;
  hyundai_canfd_update_checksum(&p);
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

static uint16_t lx3_param(void) {
  return 190U | HYUNDAI_PARAM_LX3_ENGAGEMENT_GUARD;
}

static void assert_rejected_without_side_effects(CANPacket_t *p);

static void set_accel(CANPacket_t *p, int raw, int val) {
  unsigned int raw_bits = (unsigned int)(raw + 1023);
  unsigned int val_bits = (unsigned int)(val + 1023);
  p->data[16] = raw_bits & 0xFFU;
  p->data[17] = ((val_bits & 0xFU) << 4) | ((raw_bits >> 8) & 0x7U);
  p->data[18] = (val_bits >> 4) & 0xFFU;
}

static CANPacket_t forward(CANPacket_t source, int bus) {
  source.bus = bus;
  hyundai_canfd_update_checksum(&source);
  const int destination = safety_fwd_hook(&source);
  if (destination != (bus == 2 ? 0 : 2)) {
    printf("Unexpected forwarding: addr=%x bus=%d destination=%d timer=%u\n", source.addr, bus, destination, microsecond_timer_get());
    fflush(stdout);
  }
  assert(destination == (bus == 2 ? 0 : 2));
  return source;
}

static void guarded_regressions(void) {
  // Unsupported guarded configurations do not accept even neutral actuation.
  for (uint16_t param = 2; param < 256U; param += 4U) {
    if ((param & 30U) != 30U) {  // Hybrid, LONG, camera SCC and HDA2 required.
      reset(param | HYUNDAI_PARAM_LX3_ENGAGEMENT_GUARD);
      CANPacket_t p = angle_command(false);
      assert_rejected_without_side_effects(&p);
    }
  }

  // Each guarded active actuator needs permission, including nonzero accel
  // which the fork's shared helper would otherwise use to self-authorize.
  for (int raw = -400; raw <= 250; raw += 50) {
    reset(lx3_param());
    CANPacket_t p = accel_command();
    set_accel(&p, raw, raw);
    assert_rejected_without_side_effects(&p);
  }

  reset(lx3_param());
  grant_controls();
  CANPacket_t p = accel_command();
  assert(safety_tx_hook(&p));
  set_controls_allowed(false);
  CANPacket_t oem = accel_command();
  oem.data[9] = 0xA5U;
  CANPacket_t out = forward(oem, 2);
  assert(out.data[9] == 0xA5U);  // Original OEM fallback is not rewritten.
  assert(canfd_bfwd_find(0x1A0, 0)->count == 0U);

  reset(lx3_param());
  grant_controls();
  p = accel_command();
  assert(safety_tx_hook(&p));
  gas_pressed_prev = true;  // Alternative experience may leave controls enabled.
  oem.data[9] = 0x5AU;
  out = forward(oem, 2);
  assert(out.data[9] == 0x5AU);
  assert(canfd_bfwd_find(0x1A0, 0)->count == 0U);

  for (int state = 0; state <= 1; state++) {
    reset(lx3_param());
    grant_controls();
    p = angle_command(true);
    assert(safety_tx_hook(&p));
    assert(safety_tx_hook(&p));
    p = angle_command(false);
    p.data[3] = (unsigned int)state << 4;
    assert(safety_tx_hook(&p));
    out = forward(angle_command(true), 2);
    assert(((out.data[3] >> 4) & 3U) == (unsigned int)state);
    assert(out.data[6] == 0U);
    assert(hyundai_canfd_get_checksum(&out) == hyundai_common_canfd_compute_checksum(&out));
  }

  // A pop/reuse at 29ms must not grant another 30ms of lifetime.
  for (uint32_t start = 1000000U; ; start = UINT32_MAX - 10000U) {
    reset(lx3_param());
    set_timer(start);
    fresh_mdps(0, 0);
    grant_controls();
    p = angle_command(true);
    assert(safety_tx_hook(&p));
    set_timer(start + 29000U);
    out = forward(angle_command(false), 2);
    assert(hyundai_canfd_actuator_active(&out));
    set_timer(start + 30000U);
    out = forward(angle_command(false), 2);
    assert(!hyundai_canfd_actuator_active(&out));
    if (start != 1000000U) break;
  }

  // Revocation clears every queue/cache before permission can rise again.
  reset(lx3_param());
  grant_controls();
  p = angle_command(true);
  assert(safety_tx_hook(&p));
  set_controls_allowed(false);
  CANPacket_t harmless = packet(0x555, 0, 12);
  (void)forward(harmless, 0);
  grant_controls();
  out = forward(angle_command(false), 2);
  assert(!hyundai_canfd_actuator_active(&out));

  reset(lx3_param());
  grant_controls();
  p = angle_command(true);
  assert(safety_tx_hook(&p));
  CANPacket_t invalid_stock = stock_angle(false);
  invalid_stock.data[0] ^= 1U;
  CANPacket_t before = invalid_stock;
  assert(safety_fwd_hook(&invalid_stock) == 0);
  assert(memcmp(&invalid_stock, &before, sizeof(before)) == 0);
  assert(canfd_bfwd_find(0xCB, 0)->count == 1U);
  invalid_stock = stock_angle(false);
  invalid_stock.data_len_code = 13;
  before = invalid_stock;
  assert(safety_fwd_hook(&invalid_stock) == 0);
  assert(memcmp(&invalid_stock, &before, sizeof(before)) == 0);
  assert(canfd_bfwd_find(0xCB, 0)->count == 1U);
  out = forward(angle_command(false), 2);
  assert(hyundai_canfd_actuator_active(&out));

  reset(lx3_param());
  set_timer(0U);
  CANPacket_t unsent = packet(0x161, 2, 13);
  (void)forward(unsent, 2);  // No TX must be transparent even at timestamp zero.
  canfd_record_tx_time(0, 0x161, true);
  assert(safety_fwd_hook(&unsent) == -1);  // A real TX at zero must still block.
  set_timer(70000U);
  (void)forward(unsent, 2);

  // Invalid RX cannot consume or accept buffered OP control.
  reset(lx3_param());
  grant_controls();
  p = angle_command(true);
  assert(safety_tx_hook(&p));
  safety_rx_checks_invalid = true;
  assert(!safety_tx_hook(&p));
  out = forward(angle_command(false), 2);
  assert(!hyundai_canfd_actuator_active(&out));

  // A full active queue keeps the newest command; an OFF clears cached active.
  reset(lx3_param());
  grant_controls();
  p = angle_command(true);
  for (unsigned int marker = 1; marker <= 3; marker++) {
    p.data[10] = marker;
    assert(safety_tx_hook(&p));
  }
  out = forward(angle_command(false), 2);
  assert(out.data[10] == 2U);
  out = forward(angle_command(false), 2);
  assert(out.data[10] == 3U);
  p = angle_command(false);
  assert(safety_tx_hook(&p));
  out = forward(angle_command(true), 2);
  assert(!hyundai_canfd_actuator_active(&out));

  // The development guard cannot be authorized by legacy enable messages.
  reset(lx3_param());
  CANPacket_t buttons = packet(0x1AA, 0, 10);
  buttons.data[4] = HYUNDAI_BTN_RESUME << 4;
  hyundai_canfd_rx_hook(&buttons);
  buttons.data[4] = 0;
  hyundai_canfd_rx_hook(&buttons);
  assert(!controls_allowed);

  reset(lx3_param());
  p = angle_command(false);
  p.data[6] = 1U;
  assert_rejected_without_side_effects(&p);
  p = angle_command(true);
  p.data[3] = 0x30U;
  assert_rejected_without_side_effects(&p);
  for (unsigned int addr_index = 0; addr_index < 2; addr_index++) {
    p = packet(addr_index == 0 ? 0xEA : 0x175, 2, 12);
    assert_rejected_without_side_effects(&p);
  }

  for (int sign = -1; sign <= 1; sign += 2) {
    reset(lx3_param());
    grant_controls();
    p = angle_command(false);
    set_angle(&p, sign * 1750);
    assert(safety_tx_hook(&p));
    set_angle(&p, sign * 1751);
    unsigned int queued = canfd_bfwd_find(0xCB, 0)->count;
    assert(!safety_tx_hook(&p));
    assert(canfd_bfwd_find(0xCB, 0)->count == queued);
    p = angle_command(true);
    p.data[6] = 251U;
    assert(!safety_tx_hook(&p));
    assert(canfd_bfwd_find(0xCB, 0)->count == queued);
  }
  puts("PASS: LX3 guarded authorization, neutral supersession, expiry, timer wrap and RX failure regressions");
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

static void physical_permission_regressions(void) {
  for (int alternative = 0; alternative <= 1; alternative++) {
    reset(lx3_param());
    set_alternative_experience(alternative);
    CANPacket_t gas = packet(0x105, 0, 13);
    gas.data[12] = 0x80U;
    assert(safety_rx_hook(&gas));
    physical_baseline();
    physical_button(8);
    for (int i = 0; i < 8; i++) physical_button(0);
    assert(controls_allowed == (alternative == ALT_EXP_DISABLE_DISENGAGE_ON_GAS));
    if (controls_allowed) {
      CANPacket_t angle = angle_command(true);
      assert(safety_tx_hook(&angle));
      CANPacket_t accel = accel_command();
      assert(!safety_tx_hook(&accel));  // Mode 1 cannot automate gas while overridden.
      accel.data[8] = 0x20U;
      assert(safety_tx_hook(&accel));  // Qualified combined session, zero-accel override.
      CANPacket_t stock = accel; stock.bus = 2;
      hyundai_canfd_update_checksum(&stock);
      assert(safety_fwd_hook(&stock) == 0);
      accel.data[23] = 1U;  // StopReq would conflict with the driver's gas input.
      assert(!safety_tx_hook(&accel));
      accel.data[23] = 0U;
      set_accel(&accel, 10, 0);
      assert(!safety_tx_hook(&accel));
      set_accel(&accel, 0, -10);
      assert(!safety_tx_hook(&accel));
      CANPacket_t cancel = packet(0x10B, 0, 10);
      cancel.data[2] = (uint8_t)(lx3_button_counter + 2U);
      cancel.data[10] = 4U;
      set_timer(microsecond_timer_get() + 40000U);
      hyundai_canfd_update_checksum(&cancel);
      assert(safety_rx_hook(&cancel));
      set_accel(&accel, 0, 0);
      assert(!controls_allowed && !safety_tx_hook(&accel));
    }
  }
  set_alternative_experience(0);
  reset(lx3_param());
  // A held button at boot, including its release, cannot obtain permission.
  physical_button(128);
  physical_button(0);
  assert(!controls_allowed);
  physical_button(0);
  physical_button(0);
  assert(lx3_button_ready);
  physical_button(128);
  assert(!controls_allowed);
  physical_button(0);
  assert(controls_allowed && lx3_mode == 1);
  CANPacket_t angle = angle_command(true);
  assert(safety_tx_hook(&angle));
  CANPacket_t acc = accel_command();
  assert(!safety_tx_hook(&acc));  // LFA-only never permits active longitudinal.
  physical_button(128);
  physical_button(0);
  assert(!controls_allowed && lx3_mode == 0);

  reset(lx3_param());
  physical_baseline();
  physical_button(8);
  for (unsigned int i = 0; i < 5U; i++) {
    physical_button(0);
    assert(!controls_allowed);
    physical_button(8);
  }
  for (unsigned int i = 0; i < 7U; i++) { physical_button(0); assert(!controls_allowed); }
  physical_button(0);
  assert(controls_allowed && lx3_mode == 2);
  assert(safety_tx_hook(&acc));
  physical_button(132);  // Physical CANCEL and LFA together: cancel wins.
  assert(!controls_allowed && lx3_mode == 0);
  assert(canfd_bfwd_find(0x1A0, 0)->count == 0U);

  for (unsigned int raw = 1; raw <= 2U; raw++) {
    reset(lx3_param());
    physical_baseline();
    physical_button(raw);
    assert(!controls_allowed);
    physical_button(0);
    assert(controls_allowed && lx3_mode == 2);
  }

  for (unsigned int damage = 0; damage < 5U; damage++) {
    reset(lx3_param());
    physical_baseline();
    physical_button(128);
    physical_button(0);
    assert(controls_allowed);
    set_timer(microsecond_timer_get() + 40000U);
    CANPacket_t p = packet(0x10B, 0, 10);
    p.data[2] = (uint8_t)(lx3_button_counter + 2U);
    if (damage == 1U) p.data[2] = lx3_button_counter;  // duplicate
    if (damage == 2U) p.data[2] += 2U;  // missing frame
    if (damage == 3U) set_timer(microsecond_timer_get() + 200001U);
    if (damage == 4U) p.data_len_code = 9;
    hyundai_canfd_update_checksum(&p);
    if (damage == 0U) p.data[0] ^= 1U;
    (void)safety_rx_hook(&p);
    assert(!controls_allowed && !lx3_button_ready);
    physical_button(128);  // A recovery held press must not recreate permission.
    physical_button(0);
    assert(!controls_allowed);
  }

  reset(lx3_param());
  physical_baseline();
  physical_button(128);
  physical_button(0);
  assert(safety_tx_hook(&angle));
  set_timer(microsecond_timer_get() + 200001U);
  CANPacket_t inactive = stock_angle(false);
  (void)safety_fwd_hook(&inactive);  // Missing buttons expire queued control too.
  assert(!controls_allowed && !hyundai_canfd_actuator_active(&inactive));

  reset(lx3_param());
  physical_baseline();
  physical_button(128);
  brake_pressed = true;
  physical_button(0);
  assert(!controls_allowed);

  reset(lx3_param());
  physical_baseline();
  physical_button(129);  // Ambiguous simultaneous LFA/RES cannot enable.
  physical_button(0);
  assert(!controls_allowed);

  reset(lx3_param());
  physical_baseline();
  CANPacket_t echo = packet(0x10B, 130, 10);
  echo.data[10] = 128;
  hyundai_canfd_update_checksum(&echo);
  (void)safety_rx_hook(&echo);
  echo.data[10] = 0;
  hyundai_canfd_update_checksum(&echo);
  (void)safety_rx_hook(&echo);
  assert(!controls_allowed);

  reset(lx3_param());
  physical_baseline();
  angle.data[6] = 0;  // Active-but-zero-force is still an active request.
  assert_rejected_without_side_effects(&angle);
  angle.data[3] = 0x10U;
  assert(safety_tx_hook(&angle));  // Host's inactive keepalive remains allowed.
  puts("PASS: physical CRC/counter/gestures, LFA-only authority, cancel, held recovery, stale and zero-force active rejection");
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
  grant_controls();
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

static void angle_envelope_regressions(void) {
  for (int sign = -1; sign <= 1; sign += 2) {
    reset(lx3_param());
    grant_controls();
    CANPacket_t p = angle_command(true);
    set_angle(&p, sign * 22);
    assert(!safety_tx_hook(&p));
    assert(!lx3_angle_active_prev);  // Rejected goals never become the reference.
    set_angle(&p, sign * 21);
    assert(safety_tx_hook(&p));
    set_angle(&p, sign * 25);  // No elapsed time: cannot spend another rate budget.
    assert(!safety_tx_hook(&p));
    set_timer(1010000U);
    set_angle(&p, sign * 42);
    assert(safety_tx_hook(&p));
  }
  reset(lx3_param());
  fresh_mdps(300, 0);
  grant_controls();
  CANPacket_t p = angle_command(true);
  set_angle(&p, 300);
  assert(safety_tx_hook(&p));  // First active frame starts from measured angle.
  reset(lx3_param());
  grant_controls();
  set_timer(1050001U);
  p = angle_command(true);
  assert(!safety_tx_hook(&p));
  reset(lx3_param());
  grant_controls();
  fresh_mdps(0, 500);
  p = angle_command(true);
  p.data[6] = 26U;
  assert(!safety_tx_hook(&p));
  p.data[6] = 25U;
  assert(safety_tx_hook(&p));  // Matches the host's minimum override authority.
  reset(lx3_param());
  grant_controls();
  p = angle_command(true);
  p.data[6] = 200U;
  assert(safety_tx_hook(&p));
  fresh_mdps(0, 500);
  CANPacket_t output = forward(angle_command(false), 2);
  assert(!hyundai_canfd_actuator_active(&output));  // Recheck before buffering handoff.
  reset(lx3_param());
  grant_controls();
  lx3_mdps_fault = true;
  p = angle_command(true);
  assert(!safety_tx_hook(&p));
  puts("PASS: LX3 angle rate, measured initial target, stale EPS, driver override and forwarding rechecks");
}

static void release_audit(uint16_t param) {
  reset(param);
  CANPacket_t p = angle_command(true);
  p.data_len_code = 13;  // Wrong DLC must neither send nor enter forwarding queue.
  bool accepted = safety_tx_hook(&p);
  check("rejected_DLC_does_not_enqueue", !accepted && canfd_bfwd_find(0xCB, 0)->count == 0U);

  reset(param);
  set_relay_malfunction(true);
  p = angle_command(true);
  accepted = safety_tx_hook(&p);
  check("relay_fault_does_not_enqueue", !accepted && canfd_bfwd_find(0xCB, 0)->count == 0U);

  reset(param);
  p = accel_command();
  p.bus = 3;  // Rejected bus must not authorize future controls.
  accepted = safety_tx_hook(&p);
  check("rejected_bus_does_not_authorize", !accepted && !controls_allowed);

  reset(param);
  p = accel_command();
  (void)safety_tx_hook(&p);
  check("ACC_TX_does_not_self_authorize", !controls_allowed);

  reset(param);
  p = angle_command(true);
  accepted = safety_tx_hook(&p);
  check("active_angle_requires_controls", !accepted && canfd_bfwd_find(0xCB, 0)->count == 0U);

  reset(param);
  grant_controls();
  p = angle_command(true);
  assert(safety_tx_hook(&p));
  set_controls_allowed(false);
  CANPacket_t stock = stock_angle(false);
  int destination = safety_fwd_hook(&stock);
  check("cancel_does_not_replay_active_angle", destination != 0 || (stock.data[3] & 0x30U) != 0x20U);

  reset(param);
  grant_controls();
  p = angle_command(true);
  assert(safety_tx_hook(&p));
  stock = stock_angle(false);
  assert(safety_fwd_hook(&stock) == 0);  // Consume, then test last-packet reuse after 1s.
  set_timer(2000000U);
  stock = stock_angle(false);
  destination = safety_fwd_hook(&stock);
  check("expired_active_angle_not_reused", destination != 0 || (stock.data[3] & 0x30U) != 0x20U);

  reset(param);
  grant_controls();
  p = angle_command(true);
  assert(safety_tx_hook(&p));
  assert(safety_tx_hook(&p));
  p = angle_command(false);
  assert(safety_tx_hook(&p));
  bool inactive_seen = false;
  for (int i = 0; i < 5; i++) {
    stock = stock_angle(true);  // Distinguish fallback OEM data from accepted OFF.
    destination = safety_fwd_hook(&stock);
    inactive_seen |= destination == 0 && (stock.data[3] & 0x30U) == 0U;
  }
  check("full_queue_retains_new_OFF_command", inactive_seen);

  if ((param & HYUNDAI_PARAM_LX3_ENGAGEMENT_GUARD) != 0U) {
    // Explicit release blockers beyond the original buffer/permission bugs.
    // A controlled test may set permission; actual firmware must obtain it
    // from a qualified physical-button RX path, without legacy TX shortcuts.
    reset(param);
    physical_baseline();
    physical_button(8);
    for (unsigned int i = 0; i < 8U; i++) physical_button(0);
    check("physical_main_release_has_qualified_permission_path", controls_allowed);

    reset(param);
    grant_controls();
    p = angle_command(true);
    assert(safety_tx_hook(&p));
    set_angle(&p, 1000);  // Instant 100-degree step, inside absolute range.
    set_timer(1010000U);
    check("active_angle_step_rate_is_enforced", !safety_tx_hook(&p));
  }
  printf("RELEASE_AUDIT blockers=%u; this is not vehicle qualification\n", blockers);
}

int main(int argc, char **argv) {
  if (argc == 2 && strcmp(argv[1], "--release-audit") == 0) {
    release_audit(lx3_param());
    return blockers == 0U ? 0 : 1;
  }
  if (argc == 2 && strcmp(argv[1], "--legacy-audit") == 0) {
    release_audit(190);
    return blockers == 0U ? 0 : 1;
  }
  compatibility_checks();
  rejected_tx_regressions();
  guarded_regressions();
  angle_envelope_regressions();
  physical_permission_regressions();
  return 0;
}
