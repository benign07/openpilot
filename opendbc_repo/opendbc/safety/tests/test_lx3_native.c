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
  // Explicit desktop boot-incarnation fixture; no real board configuration.
  if (safety_lx3_transport_epoch() == 0U) {
    assert(safety_lx3_set_transport_epoch(true, 0x1234U, 0x5678U));
    assert(safety_lx3_set_transport_epoch(false, 0x9ABCU, 0xDEF0U));
  }
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
  fresh_mdps(0, 0);
  grant_controls();
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

// Explicit host entry-check acknowledgement. Physical input alone must not
// grant; this helper is only used after a test has established a real pending.
static void acknowledge_request(void) {
  assert(lx3_pending && !controls_allowed);
  const uint16_t value = LX3_HEARTBEAT_TAG | 1U | ((uint16_t)lx3_requested_mode << 1U) |
                         ((uint16_t)lx3_request_counter << 8U);
  safety_host_heartbeat(value, lx3_request_generation);
  assert(controls_allowed && !lx3_pending && heartbeat_engaged);
}

static void physical_permission_regressions(void) {
  reset(lx3_param());
  CANPacket_t inactive_acc = accel_command();
  inactive_acc.data[8] = 0U;
  assert(safety_tx_hook(&inactive_acc));  // OFF/zero acceleration keepalive.
  for (unsigned int stop = 1U; stop <= 3U; stop++) {
    inactive_acc.data[23] = stop;
    assert(!safety_tx_hook(&inactive_acc));  // OFF must not carry a stop/error request.
  }
  for (int alternative = 0; alternative <= 1; alternative++) {
    reset(lx3_param());
    set_alternative_experience(alternative);
    CANPacket_t gas = packet(0x105, 0, 13);
    gas.data[12] = 0x80U;
    hyundai_canfd_update_checksum(&gas);
    assert(safety_rx_hook(&gas));
    physical_baseline();
    physical_button(8);
    for (int i = 0; i < 8; i++) physical_button(0);
    if (alternative == ALT_EXP_DISABLE_DISENGAGE_ON_GAS) acknowledge_request();
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
  acknowledge_request();
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
  acknowledge_request();
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
    acknowledge_request();
    assert(controls_allowed && lx3_mode == 2);
  }

  for (unsigned int damage = 0; damage < 5U; damage++) {
    reset(lx3_param());
    physical_baseline();
    physical_button(128);
    physical_button(0);
    acknowledge_request();
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
  acknowledge_request();
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
  // Real MAIN-neutral-RES input from route172 used to revoke a healthy stream.
  // The neutral witness confirms MAIN's release; preserve its toggle identity.
  for (unsigned int next = 1U; next <= 3U; next++) {
    reset(lx3_param()); physical_baseline(); physical_button(8);
    physical_button(0);
    const uint8_t main_counter = lx3_button_counter;
    physical_button(next);
    assert(lx3_button_ready && lx3_pending && lx3_requested_mode == 2);
    assert(lx3_request_counter == main_counter && !controls_allowed);
    const uint16_t generation = lx3_request_generation;
    physical_button(0);
    assert(lx3_pending && lx3_request_generation == generation);
    acknowledge_request(); assert(controls_allowed && lx3_mode == 2);
  }
  reset(lx3_param()); physical_baseline(); physical_button(8);
  physical_button(0); physical_button(128);
  assert(lx3_pending && lx3_requested_mode == 2 && lx3_button_ready);
  physical_button(0);
  assert(!controls_allowed && !lx3_pending && lx3_button_ready);

  reset(lx3_param()); physical_baseline(); physical_button(1); physical_button(0);
  acknowledge_request(); assert(controls_allowed && lx3_mode == 2);
  physical_button(8); physical_button(0);
  physical_button(1);
  assert(!controls_allowed && !lx3_pending && lx3_button_ready);  // MAIN OFF is never discarded.
  const uint16_t off_generation = lx3_request_generation;
  physical_button(0);
  assert(!controls_allowed && lx3_pending && lx3_request_generation != off_generation);
  acknowledge_request(); assert(controls_allowed && lx3_mode == 2);  // New RES needs a new ACK.

  reset(lx3_param()); physical_baseline(); physical_button(8); physical_button(1);
  assert(!lx3_button_ready && !lx3_pending && !controls_allowed);
  reset(lx3_param()); physical_baseline(); physical_button(8); physical_button(0);
  physical_button(8); physical_button(1);
  assert(!lx3_button_ready && !lx3_pending && !controls_allowed);
  reset(lx3_param()); physical_baseline(); physical_button(1); physical_button(2);
  assert(!lx3_pending && !controls_allowed);
  physical_button(0);
  assert(lx3_pending && lx3_request_counter == lx3_button_counter);
  puts("PASS: physical CRC/counter/gestures, MAIN neutral witness, OFF priority, LFA-only authority, cancel, held recovery, stale and zero-force active rejection");
}

static uint16_t ack_value(unsigned int mode, bool enabled, uint8_t counter) {
  return LX3_HEARTBEAT_TAG | (enabled ? 1U : 0U) | (uint16_t)(mode << 1U) | ((uint16_t)counter << 8U);
}

static void pending_lateral(void) {
  reset(lx3_param());
  physical_baseline();
  physical_button(128);
  physical_button(0);
  assert(lx3_pending && lx3_requested_mode == 1 && !controls_allowed && lx3_mode == 0);
}

static void transaction_regressions(void) {
  assert(sizeof(lx3_permission_t) == 20U);
  pending_lateral();
  lx3_permission_t s = safety_lx3_permission();
  assert(lx3_permission_valid(&s, sizeof(s)));
  for (int len = -1; len <= 21; len++) {
    if (len != 20) assert(!lx3_permission_valid(&s, len));
  }
  assert(!lx3_permission_valid(NULL, 12));
  for (unsigned int field = 0; field < 11U; field++) {
    lx3_permission_t invalid = s;
    if (field == 0U) invalid.version = 0;
    if (field == 1U) invalid.requested_mode = 3;
    if (field == 2U) invalid.accepted_mode = 1;
    if (field == 3U) invalid.generation = 0;
    if (field == 4U) invalid.age_ms = 500;
    if (field == 5U) invalid.controls_allowed = 1;
    if (field == 6U) invalid.phase = 3;
    if (field == 7U) invalid.reserved = 1;
    if (field == 8U) invalid.controls_allowed = 2;
    if (field == 9U) invalid.transport_epoch = 0;
    if (field == 10U) invalid.version = 1;  // Previous companion has no epoch.
    assert(!lx3_permission_valid(&invalid, sizeof(invalid)));
  }
  for (unsigned int counter = 0; counter < 256U; counter++) {
    const uint16_t encoded = lx3_heartbeat_value(true, 2, 1, (uint8_t)counter);
    assert((encoded & 0xF8U) == LX3_HEARTBEAT_TAG && (encoded >> 8U) == counter && (encoded & 7U) == 5U);
    assert(lx3_heartbeat_value(false, 2, 0, (uint8_t)counter) == LX3_HEARTBEAT_TAG);
  }
  assert(s.version == 2U && s.phase == 1U && s.requested_mode == 1U && s.accepted_mode == 0U);
  assert(s.physical_counter == lx3_button_counter && s.generation != 0U && s.age_ms == 0U);
  CANPacket_t angle = angle_command(true);
  angle.data[6] = 0U;
  assert(!safety_tx_hook(&angle));  // Zero-force active still cannot bypass pending.
  CANPacket_t accel = accel_command();
  assert(!safety_tx_hook(&accel));
  const uint16_t g = s.generation;
  const uint16_t value = ack_value(1, true, s.physical_counter);
  safety_host_heartbeat(value, (uint16_t)(g + 1U));
  assert(lx3_pending && !controls_allowed);
  safety_host_heartbeat(ack_value(2, true, s.physical_counter), g);
  assert(lx3_pending && !controls_allowed);
  safety_host_heartbeat(ack_value(1, true, (uint8_t)(s.physical_counter + 2U)), g);
  assert(lx3_pending && !controls_allowed);
  safety_host_heartbeat(ack_value(1, false, s.physical_counter), g);
  assert(lx3_pending && !controls_allowed);
  // A disabled heartbeat sent before this request cannot reject it.
  safety_host_heartbeat(ack_value(0, false, 0), 0);
  assert(lx3_pending && !controls_allowed);
  heartbeat_engaged_mismatches = 2U;
  safety_host_heartbeat(value, g);
  assert(controls_allowed && lx3_mode == 1 && !lx3_pending && heartbeat_engaged);
  assert(heartbeat_engaged_mismatches == 0U);
  s = safety_lx3_permission();
  assert(s.phase == 2U && s.accepted_mode == 1U && s.controls_allowed == 1U);
  safety_host_heartbeat(value, g);  // Idempotent ACK does not reset motion queues.
  assert(controls_allowed);
  safety_host_heartbeat(ack_value(0, false, 0), 0);
  assert(!controls_allowed && lx3_mode == 0);
  s = safety_lx3_permission();
  assert(s.phase == 0U && s.requested_mode == 0U && s.accepted_mode == 0U);
  safety_host_heartbeat(value, g);
  assert(!controls_allowed);  // No pending remains to consume.

  pending_lateral();
  s = safety_lx3_permission();
  safety_host_heartbeat(ack_value(0, false, s.physical_counter), s.generation);
  assert(!lx3_pending && !controls_allowed && lx3_button_ready);
  // Rejection preserves the healthy input baseline; a new gesture is required.
  physical_button(128); physical_button(0);
  assert(lx3_pending && lx3_request_generation != s.generation);
  safety_host_heartbeat(ack_value(1, true, s.physical_counter), s.generation);
  assert(lx3_pending && !controls_allowed);
  acknowledge_request();

  pending_lateral();
  s = safety_lx3_permission();
  physical_button(128); physical_button(0);  // Rapid second toggle cancels pending.
  assert(!lx3_pending && !controls_allowed);
  safety_host_heartbeat(ack_value(1, true, s.physical_counter), s.generation);
  assert(!controls_allowed);
  assert(lx3_button_ready);
  physical_button(128); physical_button(0);  // OFF does not invent a 120ms fault holdoff.
  assert(lx3_pending && !controls_allowed);
  acknowledge_request();

  for (int fault = 0; fault < 6; fault++) {
    pending_lateral();
    s = safety_lx3_permission();
    if (fault == 0) brake_pressed = true;
    if (fault == 1) regen_braking = true;
    if (fault == 2) gas_pressed = true;
    if (fault == 3) relay_malfunction = true;
    if (fault == 4) safety_rx_checks_invalid = true;
    if (fault == 5) lx3_mdps_fault = true;
    (void)safety_lx3_permission();
    assert(!lx3_pending && !controls_allowed);
    brake_pressed = false; regen_braking = false; gas_pressed = false;
    relay_malfunction = false; safety_rx_checks_invalid = false; lx3_mdps_fault = false;
    safety_host_heartbeat(ack_value(1, true, s.physical_counter), s.generation);
    assert(!controls_allowed);
  }

  pending_lateral();
  s = safety_lx3_permission();
  // Keep RX and MDPS fresh while the original request expires.
  for (int i = 0; i < 13; i++) physical_button(0);
  assert(!lx3_pending && lx3_button_ready && !controls_allowed);
  safety_host_heartbeat(ack_value(1, true, s.physical_counter), s.generation);
  assert(!controls_allowed);

  pending_lateral();
  s = safety_lx3_permission();
  safety_host_heartbeat(1U, s.generation);  // Legacy bool cannot grant in guard.
  assert(!controls_allowed && !heartbeat_engaged && !lx3_pending);
  pending_lateral();
  s = safety_lx3_permission();
  reset(lx3_param());  // Mode reset invalidates the previous generation.
  assert(lx3_request_generation != s.generation);
  safety_host_heartbeat(ack_value(1, true, s.physical_counter), s.generation);
  assert(!controls_allowed);

  // Never reuse generation in this boot; exhausted identity requires reboot.
  reset(lx3_param()); physical_baseline();
  const uint16_t fixture_generation = lx3_request_generation;
  lx3_request_generation = UINT16_MAX;
  physical_button(128); physical_button(0);
  assert(!lx3_pending && !controls_allowed && lx3_generation_exhausted);
  reset(lx3_param()); physical_baseline(); physical_button(128); physical_button(0);
  assert(!lx3_pending && !controls_allowed && lx3_request_generation == UINT16_MAX);
  // Only this synthetic fixture restores state for unrelated later tests.
  lx3_generation_exhausted = false;
  lx3_request_generation = fixture_generation;

  // RES/SET in combined is speed adjustment, not another permission transaction.
  reset(lx3_param()); physical_baseline();
  physical_button(1); physical_button(0); acknowledge_request();
  const uint16_t combined_g = lx3_request_generation;
  physical_button(2); physical_button(0);
  assert(controls_allowed && lx3_mode == 2 && lx3_request_generation == combined_g && !lx3_pending);
  // Upgrade from LAT suspends active commands until a new, matching COMB ACK.
  pending_lateral(); acknowledge_request();
  const uint16_t lat_g = lx3_request_generation;
  physical_button(1); physical_button(0);
  assert(!controls_allowed && lx3_pending && lx3_requested_mode == 2 && lx3_request_generation != lat_g);
  assert(!safety_tx_hook(&accel));
  acknowledge_request(); assert(lx3_mode == 2);

  // Main's identity is the FIRST neutral counter, not delayed 300ms decision.
  reset(lx3_param()); physical_baseline(); physical_button(8);
  const uint8_t anchor = (uint8_t)(lx3_button_counter + 2U);
  for (int i = 0; i < 8; i++) physical_button(0);
  assert(lx3_pending && lx3_request_counter == anchor && lx3_request_counter != lx3_button_counter);
  acknowledge_request();

  // ABI and exact policy scope: old flags 190 and NOOUTPUT use legacy bools.
  reset(190);
  assert(safety_lx3_permission().version == 0U);
  safety_host_heartbeat(1U, 0U); assert(heartbeat_engaged);
  safety_host_heartbeat(ack_value(1, true, 0), 0U); assert(!heartbeat_engaged);
  assert(set_safety_hooks(SAFETY_NOOUTPUT, 0U) == 0);
  assert(safety_lx3_permission().version == 0U);
  safety_host_heartbeat(1U, 0U); assert(heartbeat_engaged);
  puts("PASS: LX3 physical request/host ACK transaction, expiry, fault/cancel, stale ACK, counter/generation wrap, legacy ABI");
}

static void snapshot_state_regressions(void) {
  reset(lx3_param());
  physical_baseline();
  uint32_t seed = 0x1057C0DEU;
  unsigned int phases[3] = {0};
  for (unsigned int i = 0; i < 20000U; i++) {
    seed = seed * 1664525U + 1013904223U;
    const unsigned int action = (seed >> 24U) % 16U;
    if (action < 5U) physical_button(0);
    else if (action == 5U) physical_button(128);
    else if (action == 6U) physical_button(8);
    else if (action == 7U) physical_button(4);
    else if (action == 8U) physical_button(1);
    else if (action == 9U) {
      const uint8_t mode = (seed >> 8U) % 4U;
      const uint16_t g = (seed & 0x1000U) ? lx3_request_generation : (uint16_t)(lx3_request_generation + 1U);
      safety_host_heartbeat(lx3_heartbeat_value(true, mode, g, lx3_request_counter), mode <= 2U ? g : 0U);
    } else if (action == 10U) safety_host_heartbeat(lx3_heartbeat_value(false, 0, 0, 0), 0);
    else if (action == 11U) {
      safety_host_heartbeat(lx3_heartbeat_value(false, 0, lx3_request_generation, lx3_request_counter), lx3_request_generation);
    } else if (action == 12U) {
      brake_pressed = !brake_pressed;
    } else if (action == 13U) {
      set_timer(microsecond_timer_get() + 600000U);
    } else if (action == 14U) {
      fresh_mdps(0, 0);
    } else {
      reset(lx3_param());
    }
    const lx3_permission_t state = safety_lx3_permission();
    assert(lx3_permission_valid(&state, sizeof(state)));
    assert(!(lx3_pending && controls_allowed));
    phases[state.phase]++;
  }
  printf("PASS: 20000 deterministic mixed C input/heartbeat/fault/reset states have coherent companions (idle=%u pending=%u accepted=%u)\n",
         phases[0], phases[1], phases[2]);
  assert(phases[0] > 0U && phases[1] > 0U && phases[2] > 0U);
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
    acknowledge_request();
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

static void camera_suppression_permission_regressions(void) {
  for (unsigned int variant = 0; variant < 4U; variant++) {
    const unsigned int address = (variant & 1U) != 0U ? 0x2A4U : 0x362U;
    const unsigned int bus = variant / 2U;
    CANPacket_t p = packet(address, bus, address == 0x362U ? 13U : 12U);
    hyundai_canfd_update_checksum(&p);
    reset(190);
    assert(!controls_allowed && safety_tx_hook(&p));  // Existing platforms unchanged.
    reset(lx3_param());
    assert(!safety_tx_hook(&p));
    physical_baseline();
    physical_button(128);
    physical_button(0);
    assert(lx3_pending && !safety_tx_hook(&p));
    acknowledge_request();
    assert(lx3_mode == 1 && safety_tx_hook(&p));
    safety_host_heartbeat(lx3_heartbeat_value(false, 0, 0, 0), 0);
    assert(!controls_allowed && !safety_tx_hook(&p));
    for (unsigned int fault = 0; fault < 3U; fault++) {
      reset(lx3_param());
      physical_baseline();
      physical_button(128);
      physical_button(0);
      acknowledge_request();
      if (fault == 0U) safety_rx_checks_invalid = true;
      if (fault == 1U) set_relay_malfunction(true);
      if (fault == 2U) set_timer(microsecond_timer_get() + 200001U);
      assert(!safety_tx_hook(&p));
    }
    reset(lx3_param());
    CANPacket_t original = p;
    original.bus = 1U;
    original.data[7] = 0x33U;
    CANPacket_t saved = original;
    assert(safety_fwd_hook(&original) == -1);  // Bus1 is local, never OP replacement.
    assert(memcmp(original.data, saved.data, GET_LEN(&original)) == 0);
  }
  puts("PASS: LX3 camera suppression requires accepted permission; OFF/pending/fault rejected and legacy preserved");
}

static void angle_buffer_delivery_rate_regressions(void) {
  // Explicit isolated permission and fresh MDPS fixtures. Accepted USB goals
  // are not evidence that any command has been emitted on the vehicle bus.
  for (int sign = -1; sign <= 1; sign += 2) {
    reset(lx3_param());
    grant_controls();
    CANPacket_t p = angle_command(true);
    for (unsigned int step = 1; step <= 10U; step++) {
      set_timer(1000000U + step * 10000U);
      fresh_mdps(0, 0);
      lx3_button_us = microsecond_timer_get();
      set_angle(&p, sign * (int)step * 20);
      assert(safety_tx_hook(&p));
    }
    CANPacket_t emitted = forward(stock_angle(false), 2);
    const int emitted_angle = to_signed(GET_BYTES(&emitted, 4, 2) & 0x3FFFU, 14);
    assert(!hyundai_canfd_actuator_active(&emitted) || ABS(emitted_angle) <= 21);
    assert(!controls_allowed && lx3_mode == 0 && !lx3_pending && lx3_button_ready);
    assert(!lx3_angle_active_prev && !lx3_angle_forwarded_active_prev);
    lx3_permission_t idle = safety_lx3_permission();
    assert(lx3_permission_valid(&idle, sizeof(idle)) && idle.phase == 0U);
    assert(!safety_tx_hook(&p));  // Cannot silently resume from another USB goal.
  }
  puts("PASS: buffered angle rate budget follows emitted commands, not unsent USB goals");
}

static void display_session_permission_regressions(void) {
  const unsigned int addresses[] = {0x161U, 0x162U, 0x1E0U, 0x1EAU, 0x200U};
  const unsigned int dlcs[] = {13U, 13U, 10U, 13U, 8U};
  for (unsigned int i = 0U; i < sizeof(addresses) / sizeof(addresses[0]); i++) {
    CANPacket_t p = packet(addresses[i], 0, dlcs[i]);
    p.data[3] = 0xA5U;
    reset(190);
    assert(!controls_allowed && safety_tx_hook(&p));
    reset(lx3_param());
    assert(!safety_tx_hook(&p));
    physical_baseline();
    physical_button(128);
    physical_button(0);
    assert(lx3_pending && !safety_tx_hook(&p));
    acknowledge_request();
    assert(lx3_mode == 1 && safety_tx_hook(&p));
    CANPacket_t original = p;
    original.bus = 2U;
    original.data[3] = 0x5AU;
    assert(safety_fwd_hook(&original) == -1);
    safety_host_heartbeat(lx3_heartbeat_value(false, 0, 0, 0), 0);
    // OFF releases original ownership immediately, without another USB TX.
    assert(!controls_allowed && safety_fwd_hook(&original) == 0);
    assert(original.data[3] == 0x5AU);
    const CanfdTxState* tx = find_canfd_tx_state(0, (int)addresses[i]);
    assert(tx != NULL && !tx->tx_active);
    assert(!safety_tx_hook(&p));
    assert(!tx->tx_active);
    // Old display ownership must not reappear on the next grant before TX.
    physical_button(128);
    physical_button(0);
    acknowledge_request();
    assert(controls_allowed && safety_fwd_hook(&original) == 0);
    assert(safety_tx_hook(&p));
    safety_rx_checks_invalid = true;
    assert(!safety_tx_hook(&p));
    assert(safety_fwd_hook(&original) == 0);
    assert(!tx->tx_active);
    // main.c can clear controls_allowed outside these hooks (e.g. heartbeat
    // mismatch). The next delayed USB TX must itself release old ownership.
    reset(lx3_param());
    physical_baseline();
    physical_button(128);
    physical_button(0);
    acknowledge_request();
    assert(safety_tx_hook(&p));
    assert(find_canfd_tx_state(0, (int)addresses[i])->tx_active);
    set_controls_allowed(false);
    assert(!safety_tx_hook(&p));
    assert(!find_canfd_tx_state(0, (int)addresses[i])->tx_active);
    assert(safety_fwd_hook(&original) == 0);
  }
  puts("PASS: five LX3 display IDs require accepted permission; OFF/fault releases ownership and legacy preserved");
}

static void angle_delivery_continuity_regressions(void) {
  for (int sign = -1; sign <= 1; sign += 2) {
    // Two queued, still-fresh goals can be delivered one tick apart. The EPS
    // measurement may lag: continuous output uses the previous inserted goal.
    reset(lx3_param());
    grant_controls();
    CANPacket_t p = angle_command(true);
    set_timer(1010000U);
    set_angle(&p, sign * 20);
    assert(safety_tx_hook(&p));
    set_timer(1020000U);
    set_angle(&p, sign * 40);
    assert(safety_tx_hook(&p));
    CANPacket_t out = forward(stock_angle(false), 2);
    assert(hyundai_canfd_actuator_active(&out) && lx3_angle_forwarded == sign * 20);
    set_timer(1030000U);
    out = forward(stock_angle(false), 2);
    assert(hyundai_canfd_actuator_active(&out) && lx3_angle_forwarded == sign * 40);
    assert(controls_allowed);

    // A queue overflow loses the first step. Do not enlarge the output budget
    // or silently clamp the remaining goal: revoke the accepted session.
    reset(lx3_param());
    grant_controls();
    p = angle_command(true);
    for (unsigned int step = 1; step <= 3U; step++) {
      set_timer(1000000U + step * 10000U);
      set_angle(&p, sign * (int)step * 20);
      assert(safety_tx_hook(&p));
    }
    out = forward(stock_angle(false), 2);
    assert(!hyundai_canfd_actuator_active(&out));
    assert(!controls_allowed && lx3_mode == 0 && lx3_button_ready);

    // The same timestamp cannot buy a second output budget from the FIFO.
    reset(lx3_param());
    grant_controls();
    p = angle_command(true);
    set_timer(1010000U);
    set_angle(&p, sign * 20);
    assert(safety_tx_hook(&p));
    set_timer(1020000U);
    set_angle(&p, sign * 40);
    assert(safety_tx_hook(&p));
    out = forward(stock_angle(false), 2);
    assert(hyundai_canfd_actuator_active(&out));
    out = forward(stock_angle(false), 2);
    assert(!hyundai_canfd_actuator_active(&out) && !controls_allowed);

    // OFF is a real stream boundary. The next first active target is measured
    // angle +/- 2 degrees, even if the previous inserted goal was near zero.
    reset(lx3_param());
    grant_controls();
    p = angle_command(true);
    set_angle(&p, sign * 20);
    assert(safety_tx_hook(&p));
    out = forward(stock_angle(false), 2);
    assert(hyundai_canfd_actuator_active(&out));
    p = angle_command(false);
    assert(safety_tx_hook(&p));
    out = forward(stock_angle(true), 2);
    assert(!hyundai_canfd_actuator_active(&out) && !lx3_angle_forwarded_active_prev);
    fresh_mdps(sign * 100, 0);
    p = angle_command(true);
    set_angle(&p, sign * 120);
    assert(safety_tx_hook(&p));
    out = forward(stock_angle(false), 2);
    assert(hyundai_canfd_actuator_active(&out) && lx3_angle_forwarded == sign * 120);

    // Original fallback, malformed originals, or a >=30ms output gap each
    // break continuity. Replaying an old goal must then recheck measured EPS.
    for (unsigned int boundary = 0; boundary < 4U; boundary++) {
      reset(lx3_param());
      grant_controls();
      p = angle_command(true);
      for (unsigned int step = 1; step <= 5U; step++) {
        set_timer(1000000U + step * 10000U);
        fresh_mdps(0, 0);
        set_angle(&p, sign * (int)step * 20);
        assert(safety_tx_hook(&p));
        out = forward(stock_angle(false), 2);
        assert(hyundai_canfd_actuator_active(&out));
      }
      if (boundary == 0U) {
        // Two reuses retain the acceptance deadline. The third uses OEM data.
        for (unsigned int reuse = 1; reuse <= 3U; reuse++) {
          set_timer(1050000U + reuse * 1000U);
          out = forward(stock_angle(false), 2);
          assert(hyundai_canfd_actuator_active(&out) == (reuse <= 2U));
        }
        assert(!lx3_angle_forwarded_active_prev);
      } else if (boundary <= 2U) {
        set_timer(1051000U);
        CANPacket_t malformed = stock_angle(false);
        if (boundary == 1U) malformed.data[0] ^= 1U;
        else malformed.data_len_code = 13U;
        const CANPacket_t saved = malformed;
        assert(safety_fwd_hook(&malformed) == 0);
        assert(memcmp(&saved, &malformed, sizeof(saved)) == 0);
        assert(!lx3_angle_forwarded_active_prev);
      }
      set_timer(boundary == 3U ? 1080000U : 1060000U);
      fresh_mdps(0, 0);
      assert(safety_tx_hook(&p));  // USB reference has not moved: still accepted.
      out = forward(stock_angle(false), 2);
      assert(!hyundai_canfd_actuator_active(&out));
      assert(!controls_allowed && lx3_mode == 0 && lx3_button_ready);
      assert(canfd_bfwd_find(0xCB, 0)->count == 0U);
      assert(!safety_tx_hook(&p));
    }

    // Unsigned time subtraction preserves the same envelope across MCU wrap.
    reset(lx3_param());
    const uint32_t start = UINT32_MAX - 5000U;
    set_timer(start);
    fresh_mdps(0, 0);
    grant_controls();
    p = angle_command(true);
    set_angle(&p, sign * 20);
    assert(safety_tx_hook(&p));
    out = forward(stock_angle(false), 2);
    assert(hyundai_canfd_actuator_active(&out));
    set_timer(start + 10000U);
    fresh_mdps(0, 0);
    set_angle(&p, sign * 40);
    assert(safety_tx_hook(&p));
    out = forward(stock_angle(false), 2);
    assert(hyundai_canfd_actuator_active(&out) && controls_allowed);
  }
  puts("PASS: output FIFO, overflow, same-tick budget, OFF/OEM/gap boundaries and timer wrap");
}

static void mdps_receive_boundary_regressions(void) {
  // Exercise the real outer RX dispatcher. Unlisted frame lengths may reach
  // a mode RX hook, but must never replace a qualified LX3 driver sample.
  for (int sign = -1; sign <= 1; sign += 2) {
    reset(lx3_param());
    CANPacket_t original = packet(0xEA, 0, 12);
    const unsigned int encoded = (unsigned int)(4095 + sign * 450);
    original.data[10] = encoded & 0xFFU;
    original.data[11] = (encoded >> 8U) & 0x1FU;
    for (unsigned int i = 0; i < 6U; i++) {
      original.data[2] = (uint8_t)(20U + i);
      hyundai_canfd_update_checksum(&original);
      assert(safety_rx_hook(&original));
    }
    assert(!lx3_angle_context_valid(26));
    const struct sample_t saved = torque_driver;
    const uint32_t mdps_us = lx3_mdps_us;
    // Start with 12 bytes: both torque bytes really exist on this wire shape,
    // so the before-fix failure does not depend on memory beyond GET_LEN.
    const unsigned int malformed_dlcs[] = {9U, 0U, 1U, 2U, 3U, 4U, 5U, 6U,
                                          7U, 8U, 10U, 11U, 13U, 14U, 15U};
    for (unsigned int j = 0U; j < sizeof(malformed_dlcs) / sizeof(malformed_dlcs[0]); j++) {
      CANPacket_t malformed = packet(0xEA, 0, malformed_dlcs[j]);
      malformed.data[10] = 0xFFU;  // Encodes zero driver torque, when read.
      malformed.data[11] = 0x0FU;
      for (unsigned int i = 0; i < 6U; i++) {
        (void)safety_rx_hook(&malformed);
      }
      if (memcmp(&torque_driver, &saved, sizeof(saved)) != 0) {
        fprintf(stderr, "MDPS boundary failure: len=%u driver_min=%d driver_max=%d context26=%d\n",
                GET_LEN(&malformed), torque_driver.min, torque_driver.max, lx3_angle_context_valid(26));
      }
      assert(memcmp(&torque_driver, &saved, sizeof(saved)) == 0);
      assert(lx3_mdps_us == mdps_us);
      assert(!lx3_angle_context_valid(26));
      if (GET_LEN(&malformed) >= 2U) {
        // The wrong length is rejected even with a checksum correct for that
        // length; this separately tests shape and CRC qualification.
        hyundai_canfd_update_checksum(&malformed);
        assert(hyundai_canfd_get_checksum(&malformed) == hyundai_common_canfd_compute_checksum(&malformed));
        for (unsigned int i = 0; i < 6U; i++) {
          (void)safety_rx_hook(&malformed);
        }
        assert(memcmp(&torque_driver, &saved, sizeof(saved)) == 0);
        assert(lx3_mdps_us == mdps_us);
        assert(!lx3_angle_context_valid(26));
      }
    }
    // A CRC failure at the correct length is also rejected by the dispatcher.
    CANPacket_t corrupt = original;
    corrupt.data[10] ^= 1U;
    assert(!safety_rx_hook(&corrupt));
    assert(memcmp(&torque_driver, &saved, sizeof(saved)) == 0);
    // The same local qualification protects direct-hook callers as well.
    hyundai_canfd_rx_hook(&corrupt);
    assert(memcmp(&torque_driver, &saved, sizeof(saved)) == 0);
    assert(lx3_mdps_us == mdps_us);
    set_timer(mdps_us + 50001U);
    assert(!lx3_angle_context_valid(0));
    CANPacket_t stale = packet(0xEA, 0, 9);
    stale.data[10] = 0xFFU;
    stale.data[11] = 0x0FU;
    hyundai_canfd_update_checksum(&stale);
    (void)safety_rx_hook(&stale);
    assert(lx3_mdps_us == mdps_us && !lx3_angle_context_valid(0));
    // Fresh, correctly shaped, CRC-valid originals can replace the sample.
    original.data[10] = 0xFFU;
    original.data[11] = 0x0FU;
    set_timer(mdps_us + 60000U);
    for (unsigned int i = 0; i < 6U; i++) {
      original.data[2] = (uint8_t)(26U + i);
      hyundai_canfd_update_checksum(&original);
      assert(safety_rx_hook(&original));
      assert(lx3_angle_context_valid(26) == (i == 5U));
    }
    assert(torque_driver.min == 0 && torque_driver.max == 0);
    assert(lx3_mdps_us == mdps_us + 60000U);
    assert(lx3_angle_context_valid(26));
  }
  // Preserve the explicitly non-LX3 Carrot receive behavior.
  reset(190U);
  CANPacket_t legacy = packet(0xEA, 0, 9);
  legacy.data[10] = (4095U + 300U) & 0xFFU;
  legacy.data[11] = ((4095U + 300U) >> 8U) & 0x1FU;
  for (unsigned int i = 0; i < 6U; i++) (void)safety_rx_hook(&legacy);
  assert(torque_driver.min == 300 && torque_driver.max == 300);
  puts("PASS: LX3 driver torque ignores all malformed MDPS lengths and CRC errors, valid RX recovers");
}

static void physical_input_receive_boundary_regressions(unsigned int addr) {
  reset(lx3_param());
  physical_baseline();
  const bool gas = addr == 0x105U;
  const bool brake = addr == 0x175U;
  const unsigned int expected_dlc = gas ? 13U : 12U;
  CANPacket_t original = packet(addr, 0U, expected_dlc);
  original.data[2] = 9U;
  if (gas) original.data[13] = 1U;
  else if (brake) original.data[10] = 2U;
  else original.data[8] = 100U;
  hyundai_canfd_update_checksum(&original);
  assert(safety_rx_hook(&original));
  assert(gas ? gas_pressed : (brake ? brake_pressed : vehicle_moving));
  const struct sample_t speed = vehicle_speed;
  const bool context = lx3_request_context_valid();
  if (gas || brake) assert(!context);
  // First use 16 bytes: the actual brake/pedal/wheel bytes fit in this shape.
  const unsigned int bad_dlcs[] = {10U, 0U, 1U, 2U, 3U, 4U, 5U, 6U,
                                  7U, 8U, 9U, 11U, 12U, 13U, 14U, 15U};
  for (unsigned int j = 0U; j < sizeof(bad_dlcs) / sizeof(bad_dlcs[0]); j++) {
    if (bad_dlcs[j] == expected_dlc) continue;
    CANPacket_t malformed = packet(addr, 0U, bad_dlcs[j]);
    if (GET_LEN(&malformed) >= 2U) hyundai_canfd_update_checksum(&malformed);
    (void)safety_rx_hook(&malformed);
    const bool retained = gas ? gas_pressed : (brake ? brake_pressed : vehicle_moving);
    if (!retained) {
      fprintf(stderr, "Physical input boundary failure: addr=%x len=%u context=%d\n",
              addr, GET_LEN(&malformed), lx3_request_context_valid());
    }
    assert(retained);
    assert(memcmp(&vehicle_speed, &speed, sizeof(speed)) == 0);
    assert(lx3_request_context_valid() == context);
  }
  {
    CANPacket_t corrupt = original;
    if (gas) corrupt.data[13] = 0U;
    else if (brake) corrupt.data[10] = 0U;
    else corrupt.data[8] = 0U;
    assert(!safety_rx_hook(&corrupt));
    hyundai_canfd_rx_hook(&corrupt);
    assert(gas ? gas_pressed : (brake ? brake_pressed : vehicle_moving));
    assert(memcmp(&vehicle_speed, &speed, sizeof(speed)) == 0);
  }
  // Correctly shaped input can release the pedal/brake or update real speed.
  original.data[2] = 10U;
  if (gas) original.data[13] = 0U;
  else if (brake) original.data[10] = 0U;
  else original.data[8] = 0U;
  hyundai_canfd_update_checksum(&original);
  assert(safety_rx_hook(&original));
  assert(!(gas ? gas_pressed : (brake ? brake_pressed : vehicle_moving)));
  const struct sample_t released_speed = vehicle_speed;
  CANPacket_t spurious = packet(addr, 0U, 10U);
  if (gas) spurious.data[13] = 1U;
  else if (brake) spurious.data[10] = 2U;
  else spurious.data[8] = 100U;
  hyundai_canfd_update_checksum(&spurious);
  (void)safety_rx_hook(&spurious);
  assert(!(gas ? gas_pressed : (brake ? brake_pressed : vehicle_moving)));
  assert(memcmp(&vehicle_speed, &released_speed, sizeof(released_speed)) == 0);
  spurious.data_len_code = expected_dlc;
  spurious.bus = 1U;  // The actual LX3 physical input is ECAN bus0.
  hyundai_canfd_update_checksum(&spurious);
  (void)safety_rx_hook(&spurious);
  assert(!(gas ? gas_pressed : (brake ? brake_pressed : vehicle_moving)));
  assert(memcmp(&vehicle_speed, &released_speed, sizeof(released_speed)) == 0);
  printf("PASS: LX3 physical input%03x shape boundary, CRC policy and valid release\n", addr);
}

static void legacy_physical_input_receive_regressions(void) {
  const unsigned int addresses[] = {0x105U, 0x175U, 0xA0U};
  for (unsigned int i = 0U; i < sizeof(addresses) / sizeof(addresses[0]); i++) {
    reset(190U);
    CANPacket_t input = packet(addresses[i], 0U, 10U);
    if (i == 0U) input.data[13] = 1U;
    else if (i == 1U) input.data[10] = 2U;
    else input.data[8] = 100U;
    (void)safety_rx_hook(&input);
    assert(i == 0U ? gas_pressed : (i == 1U ? brake_pressed : vehicle_moving));
  }
  puts("PASS: non-LX3 physical input receive behavior preserved");
}

static void refresh_physical_rx_except(unsigned int missing, uint8_t counter) {
  for (int i = 0; i < current_safety_config.rx_checks_len; i++) {
    RxCheck *check = &current_safety_config.rx_checks[i];
    int index = check->status.msg_seen ? check->status.index : 0;
    if (!check->status.msg_seen && (check->msg[0].addr == 0x35)) index = 2U;  // Actual hybrid0x105.
    const unsigned int addr = check->msg[index].addr;
    if (addr == missing) continue;
    unsigned int dlc = 0U;
    while (dlc_to_len[dlc] != check->msg[index].len) { dlc++; assert(dlc < 16U); }
    CANPacket_t input = packet(addr, (unsigned int)check->msg[index].bus, dlc);
    hyundai_canfd_set_counter(&input, counter);
    if (addr == 0xEAU) { input.data[10] = 0xFFU; input.data[11] = 0x0FU; }
    hyundai_canfd_update_checksum(&input);
    assert(safety_rx_hook(&input));
  }
  // A neutral physical stream maintains the existing button lease, without
  // asking for a new session or creating any host ACK/control TX.
  CANPacket_t neutral = packet(0x10B, 0U, 10U);
  neutral.data[2] = (uint8_t)(lx3_button_counter + 2U);
  hyundai_canfd_update_checksum(&neutral);
  assert(safety_rx_hook(&neutral));
}

static void physical_input_health_timeout_regressions(void) {
  const unsigned int addresses[] = {0x105U, 0x175U, 0xA0U};
  for (unsigned int i = 0U; i < sizeof(addresses) / sizeof(addresses[0]); i++) {
    reset(lx3_param());
    physical_baseline();
    set_timer(microsecond_timer_get() + 10000U);
    refresh_physical_rx_except(0U, 1U);
    safety_tick_current_safety_config();
    assert(safety_config_valid() && !safety_rx_checks_invalid);
    assert(lx3_button_ready);
    // Isolated permission fixture solely to observe real tick revocation.
    grant_controls();
    const uint32_t start = microsecond_timer_get();
    const int target = get_addr_check_index(&(CANPacket_t){.addr=addresses[i], .bus=0U,
      .data_len_code=addresses[i] == 0x105U ? 13U : 12U}, current_safety_config.rx_checks, current_safety_config.rx_checks_len);
    assert(target >= 0);
    for (unsigned int step = 1U; step <= 101U; step++) {
      set_timer(start + step * 10000U);
      refresh_physical_rx_except(addresses[i], (uint8_t)(step + 1U));
      CANPacket_t malformed = packet(addresses[i], 0U, 10U);
      hyundai_canfd_update_checksum(&malformed);
      (void)safety_rx_hook(&malformed);
      assert(current_safety_config.rx_checks[target].status.last_timestamp == start);
    }
    assert(controls_allowed && !safety_rx_checks_invalid && lx3_mode == 2);
    safety_tick_current_safety_config();
    assert(current_safety_config.rx_checks[target].status.lagging);
    assert(safety_rx_checks_invalid && !controls_allowed);
    lx3_permission_maintenance();
    assert(lx3_mode == 0);
    for (unsigned int step = 102U; step <= 104U; step++) {
      set_timer(start + step * 10000U);
      refresh_physical_rx_except(0U, (uint8_t)(step + 1U));
    }
    safety_tick_current_safety_config();
    assert(safety_config_valid() && !safety_rx_checks_invalid);
    assert(!controls_allowed && lx3_mode == 0);  // Recovery never self-engages.
  }
  puts("PASS: malformed physical inputs cannot refresh RX health; tick revokes and fresh RX recovers without engagement");
}

static void rejected_rx_recovery_order_regression(void) {
  const unsigned int addresses[] = {0x175U, 0xEAU};
  for (unsigned int scenario = 0U; scenario < 4U; scenario++) {
  const unsigned int target = scenario / 2U;
  const bool counter_fault = (scenario % 2U) != 0U;
  reset(lx3_param()); physical_baseline();
  set_timer(microsecond_timer_get() + 10000U);
  refresh_physical_rx_except(0U, 1U); safety_tick_current_safety_config();
  physical_button(128U); physical_button(0U);
  assert(lx3_pending && !controls_allowed);
  const uint16_t generation = lx3_request_generation;
  const uint16_t ack = LX3_HEARTBEAT_TAG | 1U | ((uint16_t)lx3_requested_mode << 1U) |
                       ((uint16_t)lx3_request_counter << 8U);
  CANPacket_t input = packet(addresses[target], 0U, 12U);
  if (target == 1U) { input.data[10] = 0xFFU; input.data[11] = 0x0FU; }
  bool rejected = false;
  for (unsigned int sample = 0U; sample < (counter_fault ? 5U : 1U); sample++) {
    set_timer(microsecond_timer_get() + 1000U);
    hyundai_canfd_set_counter(&input, counter_fault ? 1U : 2U); hyundai_canfd_update_checksum(&input);
    if (!counter_fault) input.data[0] ^= 1U;
    rejected = !safety_rx_hook(&input);
  }
  assert(rejected);
  // A valid replacement arrives before snapshot, heartbeat or an explicit
  // maintenance call. Rejecting only the current context loses this history.
  bool recovered = false;
  for (uint8_t counter = counter_fault ? 2U : 3U; counter <= (counter_fault ? 7U : 3U); counter++) {
    set_timer(microsecond_timer_get() + 1000U);
    hyundai_canfd_set_counter(&input, counter); hyundai_canfd_update_checksum(&input);
    recovered = safety_rx_hook(&input);
  }
  assert(recovered && !safety_rx_checks_invalid);
  safety_host_heartbeat(ack, generation);  // Isolated API fixture, never CAN I/O.
  if (controls_allowed || lx3_pending) {
    printf("RX recovery ordering failure: controls=%d pending=%d\n", controls_allowed, lx3_pending); fflush(stdout);
  }
  assert(!controls_allowed && !lx3_pending && lx3_mode == 0);
  }
  puts("PASS: valid RX replacement before maintenance cannot revive the rejected LX3 request");
}

static void rejected_rx_pending_regressions(void) {
  for (unsigned int counter_fault = 0U; counter_fault < 2U; counter_fault++) {
    reset(lx3_param()); physical_baseline();
    set_timer(microsecond_timer_get() + 10000U);
    refresh_physical_rx_except(0U, 1U); safety_tick_current_safety_config();
    assert(!safety_rx_checks_invalid && safety_config_valid());
    physical_button(128U); physical_button(0U);
    assert(lx3_pending && !controls_allowed && lx3_request_context_valid());
    const uint16_t generation = lx3_request_generation;
    const uint16_t ack = LX3_HEARTBEAT_TAG | 1U | ((uint16_t)lx3_requested_mode << 1U) |
                         ((uint16_t)lx3_request_counter << 8U);
    CANPacket_t invalid = packet(0x175U, 0U, 12U);
    bool rejected = false;
    for (unsigned int sample = 0U; sample < (counter_fault ? 5U : 1U); sample++) {
      set_timer(microsecond_timer_get() + 1000U);
      hyundai_canfd_set_counter(&invalid, counter_fault ? 1U : 2U);
      hyundai_canfd_update_checksum(&invalid);
      if (!counter_fault) invalid.data[0] ^= 1U;
      rejected = !safety_rx_hook(&invalid);
    }
    assert(rejected && !safety_rx_checks_invalid);  // No intervening 1Hz tick.
    if (lx3_pending || lx3_request_context_valid()) {
      printf("RX pending invalidation failure: counter=%u pending=%d context=%d\n",
             counter_fault, lx3_pending, lx3_request_context_valid()); fflush(stdout);
    }
    assert(!lx3_pending && !lx3_request_context_valid() && !controls_allowed);
    // Existing host API fixture only; no CAN ACK, transport or real device.
    safety_host_heartbeat(ack, generation);
    assert(!controls_allowed && lx3_mode == 0);
    // A new gesture while the known RX status is still invalid cannot request.
    physical_baseline(); physical_button(128U); physical_button(0U);
    assert(!lx3_pending && !controls_allowed);
    for (uint8_t counter = 2U; counter <= 7U; counter++) {
      set_timer(microsecond_timer_get() + 1000U);
      hyundai_canfd_set_counter(&invalid, counter); hyundai_canfd_update_checksum(&invalid);
      (void)safety_rx_hook(&invalid);
    }
    const int rx_index = get_addr_check_index(&invalid, current_safety_config.rx_checks, current_safety_config.rx_checks_len);
    assert(rx_index >= 0 && current_safety_config.rx_checks[rx_index].status.valid_checksum &&
           current_safety_config.rx_checks[rx_index].status.wrong_counters < MAX_WRONG_COUNTERS);
    safety_host_heartbeat(ack, generation);  // Old identity cannot revive after recovery.
    assert(!controls_allowed && !lx3_pending);
    physical_baseline(); physical_button(128U); physical_button(0U);
    assert(lx3_pending && lx3_request_generation != generation);
    acknowledge_request();
    // Accepted authority is also revoked by a rejected input, without tick.
    hyundai_canfd_set_counter(&invalid, 8U); hyundai_canfd_update_checksum(&invalid);
    invalid.data[0] ^= 1U;
    assert(!safety_rx_hook(&invalid));
    assert(!controls_allowed && !lx3_pending && lx3_mode == 0);
  }
  reset(lx3_param()); physical_baseline();
  physical_button(128U); physical_button(0U);
  assert(lx3_pending);  // Unseen RX keeps the existing common tick policy.
  CANPacket_t hybrid = packet(0x105U, 0U, 13U);
  hyundai_canfd_update_checksum(&hybrid); hybrid.data[0] ^= 1U;
  assert(!safety_rx_hook(&hybrid));
  assert(!lx3_pending && !lx3_request_context_valid());  // Qualified LX3 CRC requirement.
  reset(190U); grant_controls();
  CANPacket_t legacy = packet(0x175U, 0U, 12U);
  hyundai_canfd_update_checksum(&legacy); legacy.data[0] ^= 1U;
  assert(!safety_rx_hook(&legacy) && !controls_allowed);
  puts("PASS: rejected RX invalidates LX3 pending and old identity before tick; fresh gesture and legacy CRC policy preserved");
}

static void hybrid_crc_receive_regressions(void) {
  reset(lx3_param()); physical_baseline();
  set_timer(microsecond_timer_get() + 10000U);
  refresh_physical_rx_except(0U, 1U); safety_tick_current_safety_config();
  physical_button(128U); physical_button(0U);
  assert(lx3_pending && !controls_allowed);
  CANPacket_t gas = packet(0x105U, 0U, 13U);
  gas.data[2] = 3U; gas.data[12] = 0x80U;
  hyundai_canfd_update_checksum(&gas); gas.data[0] ^= 1U;
  const bool accepted = safety_rx_hook(&gas);
  if (accepted) {
    printf("LX3 hybrid CRC failure: accepted=%d gas=%d pending=%d\n", accepted, gas_pressed, lx3_pending); fflush(stdout);
  }
  assert(!accepted && !gas_pressed && !lx3_pending && !controls_allowed);
  // A direct mode-hook call must enforce the same integrity boundary.
  hyundai_canfd_rx_hook(&gas); assert(!gas_pressed);
  const int index = get_addr_check_index(&gas, current_safety_config.rx_checks, current_safety_config.rx_checks_len);
  assert(index >= 0 && !current_safety_config.rx_checks[index].status.valid_checksum);
  for (uint8_t header = 5U; header <= 13U; header += 4U) {
    gas.data[2] = header; hyundai_canfd_update_checksum(&gas);
    assert(safety_rx_hook(&gas) && gas_pressed);
    assert(current_safety_config.rx_checks[index].status.wrong_counters == 0);
  }
  gas.data[12] = 0U;  // Corrupt release cannot clear a pressed pedal.
  assert(!safety_rx_hook(&gas) && gas_pressed);
  hyundai_canfd_rx_hook(&gas); assert(gas_pressed);
  hyundai_canfd_update_checksum(&gas);
  assert(safety_rx_hook(&gas) && !gas_pressed && !controls_allowed && !lx3_pending);
  physical_baseline(); physical_button(128U); physical_button(0U);
  assert(lx3_pending); acknowledge_request();
  gas.data[0] ^= 1U;
  assert(!safety_rx_hook(&gas) && !controls_allowed && lx3_mode == 0);

  for (unsigned int alt = 0U; alt < 2U; alt++) {
    reset(lx3_param() - (alt ? 0U : 32U));
    assert(current_safety_config.rx_checks_len == 5);
    const CanMsgCheck *pedal = &current_safety_config.rx_checks[0].msg[0];
    assert(pedal->addr == 0x105 && pedal->bus == 0 && pedal->len == 32);
    assert(!pedal->ignore_checksum && pedal->ignore_counter && pedal->max_counter == 0U);
    assert(current_safety_config.rx_checks[0].msg[1].addr == 0);
    assert(current_safety_config.rx_checks[4].msg[0].addr == (alt ? 0x1AA : 0x1CF));
    for (int group = 1; group < 4; group++) {
      const CanMsgCheck *selected = &current_safety_config.rx_checks[group].msg[0];
      const CanMsgCheck *original = &hyundai_canfd_hda2_long_alt_buttons_rx_checks_scc2[group].msg[0];
      assert(selected->addr == original->addr && selected->bus == original->bus && selected->len == original->len);
      assert(selected->ignore_checksum == original->ignore_checksum && selected->ignore_counter == original->ignore_counter);
      assert(selected->max_counter == original->max_counter && selected->quality_flag == original->quality_flag);
      assert(selected->frequency == original->frequency);
    }
    assert(!current_safety_config.rx_checks[0].status.msg_seen);
    CANPacket_t other = packet(0x35U, 0U, 13U); hyundai_canfd_update_checksum(&other);
    assert(safety_rx_hook(&other));
    other.addr = 0x100U; hyundai_canfd_update_checksum(&other);
    assert(safety_rx_hook(&other));
    assert(!current_safety_config.rx_checks[0].status.msg_seen);
    gas.data[12] = 0U; hyundai_canfd_update_checksum(&gas);
    assert(safety_rx_hook(&gas));
    assert(current_safety_config.rx_checks[0].status.msg_seen && current_safety_config.rx_checks[0].status.index == 0);
  }
  // Switching away from LX3 must not leak CRC metadata through shared arrays.
  const uint16_t legacy_params[] = {190U, 1210U};
  for (unsigned int i = 0U; i < 2U; i++) {
    reset(legacy_params[i]);
    gas.data[12] = 0x80U; hyundai_canfd_update_checksum(&gas); gas.data[0] ^= 1U;
    assert(safety_rx_hook(&gas) && gas_pressed);
    assert(current_safety_config.rx_checks[0].msg[2].ignore_checksum);
    assert(current_safety_config.rx_checks[0].msg[2].ignore_counter);
  }
  reset(lx3_param());
  assert(!current_safety_config.rx_checks[0].status.msg_seen);
  assert(!safety_rx_hook(&gas));
#ifndef LX3_EXACT_6714_FIXTURE
  const uint16_t required_flags[] = {1024U, 16U, 8U, 4U, 2U};  // Guard/HDA2/camera/long/hybrid.
  for (unsigned int i = 0U; i < sizeof(required_flags) / sizeof(required_flags[0]); i++) {
    reset(lx3_param() & (uint16_t)~required_flags[i]);
    assert(!lx3_hybrid_crc_required);
    assert(current_safety_config.rx_checks != hyundai_canfd_lx3_rx_checks);
    assert(current_safety_config.rx_checks != hyundai_canfd_lx3_alt_buttons_rx_checks);
    // Unsupported configurations need not arm the LX3 neutral detector.
    for (unsigned int neutral = 0U; neutral < 3U; neutral++) physical_button(0U);
    physical_button(128U); physical_button(0U);
    assert(!lx3_pending);
  }
  reset(lx3_param()); assert(lx3_hybrid_crc_required);
#endif
  puts("PASS: LX3 hybrid pedal CRC rejects corrupt press and release, revokes requests, and preserves header/legacy policy");
}

int main(int argc, char **argv) {
  if (argc == 2) {
    if (strcmp(argv[1], "--hybrid-crc") == 0) { hybrid_crc_receive_regressions(); return 0; }
    if (strcmp(argv[1], "--rx-recovery-order") == 0) { rejected_rx_recovery_order_regression(); return 0; }
    if (strcmp(argv[1], "--rx-pending") == 0) { rejected_rx_pending_regressions(); return 0; }
    if (strcmp(argv[1], "--physical-health") == 0) { physical_input_health_timeout_regressions(); return 0; }
    if (strcmp(argv[1], "--gas-boundary") == 0) { physical_input_receive_boundary_regressions(0x105U); return 0; }
    if (strcmp(argv[1], "--brake-boundary") == 0) { physical_input_receive_boundary_regressions(0x175U); return 0; }
    if (strcmp(argv[1], "--speed-boundary") == 0) { physical_input_receive_boundary_regressions(0xA0U); return 0; }
  }
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
  transaction_regressions();
  snapshot_state_regressions();
  camera_suppression_permission_regressions();
  display_session_permission_regressions();
  angle_buffer_delivery_rate_regressions();
  angle_delivery_continuity_regressions();
  mdps_receive_boundary_regressions();
  physical_input_receive_boundary_regressions(0x105U);
  physical_input_receive_boundary_regressions(0x175U);
  physical_input_receive_boundary_regressions(0xA0U);
  legacy_physical_input_receive_regressions();
  physical_input_health_timeout_regressions();
  rejected_rx_pending_regressions();
  rejected_rx_recovery_order_regression();
  hybrid_crc_receive_regressions();
  return 0;
}
