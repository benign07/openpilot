// Desktop fixtures for actual C/production-host scheduling; no hardware I/O.
#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

static int quiet_printf(const char *format, ...) { (void)format; return 0; }
#define printf quiet_printf
bool safety_tx_buffered_for_fwd = false;
void putui(uint32_t value) { (void)value; }
#include "libsafety/safety.c"
#undef printf

#ifdef _WIN32
#define LX3_EXPORT __declspec(dllexport)
#else
#define LX3_EXPORT __attribute__((visibility("default")))
#endif

static uint8_t fixture_counters[16];
static uint32_t fixture_last_us[16];

static CANPacket_t fixture_packet(int address, int bus, int length) {
  CANPacket_t p = {0};
  p.fd = 1;
  p.addr = (uint32_t)address;
  p.bus = (uint8_t)bus;
  for (unsigned int dlc = 0; dlc < 16U; dlc++) {
    if (dlc_to_len[dlc] == length) { p.data_len_code = dlc; break; }
  }
  return p;
}

LX3_EXPORT void lx3_test_reset(void) {
  assert(set_safety_hooks(SAFETY_HYUNDAI_CANFD, 190U | HYUNDAI_PARAM_LX3_ENGAGEMENT_GUARD) == 0);
  init_tests();
  set_timer(1000000U);
  memset(fixture_counters, 0, sizeof(fixture_counters));
  memset(fixture_last_us, 0, sizeof(fixture_last_us));
}

LX3_EXPORT void lx3_test_sensors(uint32_t us, bool brake, bool gas) {
  set_timer(us);
  assert(current_safety_config.rx_checks_len <= 16);
  for (int i = 0; i < current_safety_config.rx_checks_len; i++) {
    const RxCheck *check = &current_safety_config.rx_checks[i];
    const CanMsgCheck *msg = &check->msg[0];
    // Hybrid pedal variant; don't latch the ICE/EV alternative by accident.
    for (unsigned int j = 0; j < MAX_ADDR_CHECK_MSGS; j++) {
      if (check->msg[j].addr == 0x105) msg = &check->msg[j];
    }
    if (fixture_last_us[i] != 0U && us - fixture_last_us[i] < 1000000U / msg->frequency) continue;
    fixture_last_us[i] = us;
    CANPacket_t p = fixture_packet(msg->addr, msg->bus, msg->len);
    const uint8_t counter = fixture_counters[i]++;
    hyundai_canfd_set_counter(&p, counter);
    if (msg->addr == 0xEA) { p.data[10] = 0xFFU; p.data[11] = 0xFU; }  // zero driver torque, zero angle
    if (msg->addr == 0x175 && brake) p.data[10] |= 2U;
    if (msg->addr == 0x105 && gas) p.data[12] |= 0x80U;
    hyundai_canfd_update_checksum(&p);
    assert(safety_rx_hook(&p));
  }
}

LX3_EXPORT bool lx3_test_button(uint32_t us, uint8_t raw, uint8_t counter, uint8_t *data) {
  set_timer(us);
  CANPacket_t p = fixture_packet(0x10B, 0, 16);
  p.data[2] = counter;
  p.data[10] = raw;
  hyundai_canfd_update_checksum(&p);
  memcpy(data, p.data, 16U);
  return safety_rx_hook(&p);
}

LX3_EXPORT void lx3_test_time(uint32_t us) { set_timer(us); }

LX3_EXPORT void lx3_test_tick(void) { safety_tick_current_safety_config(); }

LX3_EXPORT bool lx3_test_rx_valid(void) {
  return safety_config_valid() && !safety_rx_checks_invalid && !relay_malfunction;
}

LX3_EXPORT void lx3_test_state(lx3_permission_t *state) { *state = safety_lx3_permission(); }

LX3_EXPORT void lx3_test_heartbeat(bool enabled, uint8_t mode, uint16_t generation, uint8_t counter) {
  // Same encoder used by production pandad, same decoder used by firmware.
  safety_host_heartbeat(lx3_heartbeat_value(enabled, mode, generation, counter), generation);
}

LX3_EXPORT bool lx3_test_active_tx(uint8_t mode) {
  CANPacket_t p = fixture_packet(mode == 2U ? 0x1A0 : 0xCB, 0, mode == 2U ? 32 : 24);
  if (mode == 2U) {
    p.data[8] = 0x10U;
    p.data[16] = 0xFFU; p.data[17] = 0xF3U; p.data[18] = 0x3FU;
  } else {
    p.data[3] = 0x20U; p.data[6] = 25U;
  }
  return safety_tx_hook(&p);
}

LX3_EXPORT bool lx3_test_packet_tx(int address, int bus, int length, const uint8_t *data) {
  assert(length >= 0 && length <= 64);
  CANPacket_t p = fixture_packet(address, bus, length);
  memcpy(p.data, data, (size_t)length);
  return safety_tx_hook(&p);
}

LX3_EXPORT int lx3_test_packet_fwd(int address, int bus, int length, const uint8_t *data, uint8_t *output) {
  assert(length >= 0 && length <= 64);
  CANPacket_t p = fixture_packet(address, bus, length);
  memcpy(p.data, data, (size_t)length);
  const int destination = safety_fwd_hook(&p);
  memcpy(output, p.data, (size_t)length);
  return destination;
}
