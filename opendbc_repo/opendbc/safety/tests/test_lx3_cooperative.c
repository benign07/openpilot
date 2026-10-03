// Production C policy, isolated desktop fixtures. No vehicle/CAN I/O.
#define main lx3_prior_suite_main
#include "test_lx3_native.c"
#undef main

static CANPacket_t cooperative_camera(bool active) {
  CANPacket_t p = stock_angle(active);
  p.data[3] = active ? 0x20U : 0x10U;  // Observed LX3 inactive mode is 1.
  hyundai_canfd_update_checksum(&p);
  return p;
}

static CANPacket_t forward_raw(CANPacket_t source, int bus) {
  source.bus = (unsigned int)bus;
  const int destination = safety_fwd_hook(&source);
  assert(destination == (relay_malfunction ? -1 : (bus == 0 ? 2 : 0)));
  return source;  // Do not repair malformed/CRC-negative fixtures.
}

static void effort_window(void) {
  for (int sign = -1; sign <= 1; sign += 2) {
    reset(lx3_param());
    grant_controls();
    CANPacket_t p = angle_command(true);
    p.data[6] = 200U;
    for (unsigned int i = 0U; i < 5U; i++) {
      fresh_mdps(0, sign * 500);
      assert(safety_tx_hook(&p));
    }
    fresh_mdps(0, sign * 500);
    assert(!safety_tx_hook(&p));
    p.data[6] = 25U;
    assert(safety_tx_hook(&p) && controls_allowed);
    fresh_mdps(0, 0);
    assert(lx3_angle_context_valid(200));
    reset(lx3_param());
    for (unsigned int i = 0U; i < 6U; i++) fresh_mdps(0, sign * 250);
    assert(lx3_angle_context_valid(250));
  }
  reset(lx3_param());
  const int history[] = {-300, 0, 0, 0, 0, 300};
  for (unsigned int i = 0U; i < sizeof(history) / sizeof(history[0]); i++) fresh_mdps(0, history[i]);
  assert(lx3_angle_context_valid(200));  // Both extrema are large; sustained effort is false.
  for (unsigned int i = 0U; i < 6U; i++) fresh_mdps(0, (i % 2U == 0U) ? 300 : -300);
  assert(!lx3_angle_context_valid(26));  // Sign changes alone do not restore high authority.
  puts("PASS: cooperative driver effort uses all six qualified samples and preserves 25 ceiling");
}

static void prepare_owned(void) {
  reset(lx3_param());
  grant_controls();
  CANPacket_t p = angle_command(true);
  assert(safety_tx_hook(&p));
  const CANPacket_t out = forward(cooperative_camera(false), 2);
  assert(hyundai_canfd_actuator_active(&out));
}

static CANPacket_t feedback_host(void) {
  CANPacket_t p = packet(0xEA, 2, 12);
  memset(p.data, 0xA5, 24);  // No arbitrary host fields may reach the camera.
  p.data[18] = 1U;
  hyundai_canfd_update_checksum(&p);
  return p;
}

static CANPacket_t feedback_original(void) {
  CANPacket_t p = packet(0xEA, 0, 12);
  for (unsigned int i = 0U; i < 24U; i++) p.data[i] = (uint8_t)(i * 7U + 3U);
  p.data[2] = 255U;
  p.data[6] &= 0xBFU;  // LKA_FAULT
  p.data[18] = (p.data[18] & 0xDCU) | 2U;  // LFA2_FAULT=0, real LFA2_ACTIVE=2
  hyundai_canfd_update_checksum(&p);
  return p;
}

static void feedback_mediation(void) {
  // No queued feedback means an OEM MDPS frame must pass through byte-for-byte,
  // including malformed input; no host authority is inferred from that frame.
  reset(lx3_param());
  CANPacket_t idle_mdps = feedback_original();
  idle_mdps.data[0] ^= 1U;
  const CANPacket_t idle_saved = idle_mdps;
  CANPacket_t idle_out = forward_raw(idle_mdps, 0);
  assert(memcmp(idle_out.data, idle_saved.data, 24U) == 0);
  assert(canfd_bfwd_find(0xEA, 2)->count == 0U && !controls_allowed);

  for (unsigned int camera_state = 1U; camera_state <= 2U; camera_state++) {
    prepare_owned();
    CANPacket_t camera = cooperative_camera(camera_state == 2U);
    const CANPacket_t ignored = forward(camera, 2);
    (void)ignored;
    CANPacket_t host = feedback_host();
    host.data[18] = (uint8_t)camera_state;
    assert(safety_tx_hook(&host));
    const CanfdTxState* feedback_tx_state = find_canfd_tx_state(2, 0xEA);
    assert(feedback_tx_state != NULL && !feedback_tx_state->tx_active);
    CANPacket_t original = feedback_original();
    original.data[18] = (original.data[18] & 0xFCU) | (3U - camera_state);
    hyundai_canfd_update_checksum(&original);
    const CANPacket_t saved = original;
    CANPacket_t expected = original;
    expected.data[18] = (expected.data[18] & 0xFCU) | camera_state;
    hyundai_canfd_update_checksum(&expected);
    CANPacket_t out = forward(original, 0);
    if (memcmp(out.data, expected.data, 24) != 0) {
      fprintf(stderr, "feedback state=%u out=%u expected=%u permission=%d wire_active=%d fault54=%d fault149=%d\n",
              camera_state, out.data[18], expected.data[18], controls_allowed,
              lx3_angle_forwarded_active_prev, GET_BIT(&original, 54U), GET_BIT(&original, 149U));
    }
    assert(memcmp(out.data, expected.data, 24) == 0);
    assert(memcmp(original.data, saved.data, 24) == 0);
    assert(out.data[2] == original.data[2]);
    assert(hyundai_canfd_get_checksum(&out) == hyundai_common_canfd_compute_checksum(&out));
    hyundai_canfd_rx_hook(&original);  // fdcan separately delivers the untouched RX object.
    const int actual_torque = (int)(((original.data[11] & 0x1FU) << 8U) | original.data[10]) - 4095;
    assert(torque_driver.values[0] == actual_torque);
  }
  // Every boundary must pass the physical source without changing a byte.
  for (unsigned int boundary = 0U; boundary < 10U; boundary++) {
    prepare_owned();
    CANPacket_t host = feedback_host();
    assert(safety_tx_hook(&host));
    CANPacket_t original = feedback_original();
    if (boundary == 0U) set_controls_allowed(false);
    if (boundary == 1U) safety_rx_checks_invalid = true;
    if (boundary == 2U) set_relay_malfunction(true);
    if (boundary == 3U) set_timer(1030000U);
    if (boundary == 4U) original.data[0] ^= 1U;
    if (boundary == 5U) original.data_len_code = 10U;
    if (boundary == 6U) { original.data[6] |= 0x40U; hyundai_canfd_update_checksum(&original); }
    if (boundary == 7U) { original.data[18] |= 0x20U; hyundai_canfd_update_checksum(&original); }
    if (boundary == 8U) {
      CANPacket_t camera = cooperative_camera(false);
      camera.data[0] ^= 1U;
      (void)forward_raw(camera, 2);
    }
    if (boundary == 9U) {
      // The host mirror predates a new camera request, even though OP owns CB.
      (void)forward(cooperative_camera(true), 2);
    }
    const CANPacket_t saved = original;
    CANPacket_t out = forward_raw(original, 0);
    assert(memcmp(out.data, saved.data, GET_LEN(&saved)) == 0);
  }
  reset(lx3_param());
  CANPacket_t host = feedback_host();
  assert(!safety_tx_hook(&host));
  grant_controls();
  assert(safety_tx_hook(&host));
  assert(!find_canfd_tx_state(2, 0xEA)->tx_active);
  CANPacket_t original = feedback_original();
  CANPacket_t out = forward(original, 0);  // Accepted session but no OP CB ever inserted.
  assert(memcmp(out.data, original.data, 24) == 0);
  reset(190U);
  host = feedback_host();
  assert(safety_tx_hook(&host));
  out = forward(original, 0);
  assert(out.data[10] == 0xA5U);  // Other vehicles retain the existing full-payload path.
  // An accepted blinker/large-angle suspension keeps neutral OP ownership:
  // OEM active2 may not silently resume assistance, but camera state mediation
  // still follows its original request. No torque/touch/fault data is replaced.
  prepare_owned();
  for (unsigned int i = 0U; i < 60U; i++) {
    set_timer(1000000U + i * 10000U);
    lx3_button_us = microsecond_timer_get();
    fresh_mdps(0, 0);
    CANPacket_t neutral = angle_command(false);
    neutral.data[3] = 0x10U;
    assert(safety_tx_hook(&neutral));
    out = forward(cooperative_camera(true), 2);
    assert(!hyundai_canfd_actuator_active(&out) && out.data[6] == 0U);
    host = feedback_host();
    host.data[18] = 2U;
    assert(safety_tx_hook(&host));
    original = feedback_original();
    original.data[2] = (uint8_t)(255U + i);
    original.data[18] = (original.data[18] & 0xFCU) | 1U;
    hyundai_canfd_update_checksum(&original);
    out = forward_raw(original, 0);
    assert((out.data[18] & 3U) == 2U && out.data[2] == original.data[2]);
    assert(controls_allowed);
  }
  // Reusing feedback cannot refresh the original host acceptance deadline.
  prepare_owned();
  host = feedback_host();
  assert(safety_tx_hook(&host));
  original = feedback_original();
  out = forward_raw(original, 0);
  assert((out.data[18] & 3U) == 1U);
  out = forward_raw(original, 0);
  assert((out.data[18] & 3U) == 1U);
  set_timer(1030000U);
  fresh_mdps(0, 0);
  CANPacket_t active = angle_command(true);
  assert(safety_tx_hook(&active));
  (void)forward(cooperative_camera(false), 2);
  out = forward_raw(original, 0);
  assert(memcmp(out.data, original.data, 24) == 0);
  for (unsigned int dlc = 0U; dlc < 16U; dlc++) {
    if (dlc == 12U) continue;
    prepare_owned();
    host = feedback_host();
    host.data_len_code = dlc;
    assert(!safety_tx_hook(&host));
    assert(canfd_bfwd_find(0xEA, 2)->count == 0U);
  }
  puts("PASS: MDPS state mediation retains original counter/effort/angle/fault bytes and requires live OP ownership");
}

static void fallback_continuity(void) {
  for (int sign = -1; sign <= 1; sign += 2) {
    prepare_owned();
    set_timer(1010000U);
    fresh_mdps(0, 0);
    CANPacket_t p = angle_command(true);
    p.data[6] = 200U;
    set_angle(&p, sign * 20);
    assert(safety_tx_hook(&p));
    for (unsigned int i = 0U; i < 6U; i++) fresh_mdps(sign * 30, sign * 497);
    (void)forward(cooperative_camera(false), 2);  // The queued excessive cap falls back to OEM.
    assert(controls_allowed && !lx3_angle_forwarded_active_prev);
    set_timer(1020000U);
    fresh_mdps(sign * 100, sign * 497);
    p.data[6] = 25U;
    set_angle(&p, sign * 100);
    assert(safety_tx_hook(&p));
    const CANPacket_t out = forward(cooperative_camera(false), 2);
    assert(hyundai_canfd_actuator_active(&out) && controls_allowed);
    // A rejected USB goal alone must not erase the accepted rate reference.
    set_angle(&p, sign * 400);
    assert(!safety_tx_hook(&p));
    assert(lx3_angle_active_prev && lx3_angle_accepted == sign * 100);
  }
  puts("PASS: real OEM fallback resets both angle histories; rejected USB alone preserves history");
}

static void emergency_template_fallback(void) {
  prepare_owned();
  CANPacket_t emergency = cooperative_camera(true);
  set_angle(&emergency, 500);  // OEM request far from current measured angle.
  emergency.data[6] = 200U;
  hyundai_canfd_update_checksum(&emergency);
  CANPacket_t host_template = emergency;
  host_template.bus = 0;
  assert(!safety_tx_hook(&host_template));
  // Exhaust the prior accepted frame/reuses: the unchanged original OEM
  // request then takes over. This is an isolated forwarding scenario, not
  // an assertion that a real OEM emergency command has this magnitude.
  CANPacket_t out = forward(emergency, 2);
  out = forward(emergency, 2);
  out = forward(emergency, 2);
  assert(memcmp(out.data, emergency.data, 24) == 0);
  assert(controls_allowed && !lx3_angle_active_prev);
  assert(!safety_tx_hook(&host_template));
  assert(!controls_allowed && lx3_mode == 0);
  out = forward(emergency, 2);
  assert(memcmp(out.data, emergency.data, 24) == 0);
  puts("PASS: far OEM template remains original while invalid host reentry visibly ends permission");
}

int main(int argc, char **argv) {
  if (argc == 1 || strcmp(argv[1], "--effort") == 0) effort_window();
  if (argc == 1 || strcmp(argv[1], "--feedback") == 0) feedback_mediation();
  if (argc == 1 || strcmp(argv[1], "--fallback") == 0) fallback_continuity();
  if (argc == 1) emergency_template_fallback();
  return 0;
}
