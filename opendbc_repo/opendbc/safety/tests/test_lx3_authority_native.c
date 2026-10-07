// Actual production RX/STATE/TX/forwarding policy. Mock clock and input bytes;
// no assignment of controls_allowed or accepted permission as a test shortcut.
#define CANFD
#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include "fake_stm.h"
void putui(uint32_t n) { printf("%u", n); }
#ifndef LX3_BOARD_TEST
bool safety_tx_buffered_for_fwd;
#else
extern bool safety_tx_buffered_for_fwd;
#endif
#include "can.h"
#include "faults.h"
#include "safety.h"
_Static_assert(sizeof(lx3_status_t) == LX3_STATUS_SIZE, "paired status layout");

static uint8_t counters[6];
static uint32_t sequence;
static const uint64_t epoch = 0x3141592653589793ULL;

static CANPacket_t packet(int address, unsigned bus, unsigned length) {
  CANPacket_t p = {0}; p.addr = address; p.bus = bus; p.fd = 1U;
  p.data_len_code = length == 16U ? 10U : length == 24U ? 12U : 13U;
  return p;
}

static void rx_tick(uint8_t key, bool brake, bool gas) {
  timer.CNT += 40000U;
  for (unsigned i = 0U; i < 6U; i++) {
    CANPacket_t p = packet(lx3_rx_addresses[i], 0U, lx3_rx_lengths[i]);
    counters[i] += i == 4U ? 2U : 1U; p.data[2] = counters[i];
    if (i == 0U && gas) p.data[12] = 128U;
    if (i == 1U && brake) p.data[10] = 2U;
    if (i == 4U) p.data[10] = key;
    hyundai_canfd_update_checksum(&p);
    assert(safety_rx_hook(&p));
  }
}

static void state(uint8_t intent, uint16_t key, uint16_t generation, bool automatic, uint16_t revision) {
  uint8_t button = key >> 8U;
  uint8_t kind = button == 128U ? 1U : button == 8U ? 2U : button == 1U ? 3U : button == 2U ? 4U : 0U;
  const uint16_t value = (uint16_t)((key & 255U) << 8U) | (kind << 4U) | intent | (automatic ? 4U : 0U);
  const uint16_t config = 5U; // AlwaysLateral + armed automatic resume.
  const uint64_t binding = lx3_state_binding(epoch, ++sequence, value, generation, config, 700U, revision);
  lx3_native_control(LX3_CONFIG_REQUEST, config, 700U);
  lx3_native_control(LX3_SEQUENCE_REQUEST, sequence >> 16U, sequence);
  lx3_native_control(LX3_REVOKE_ACK_REQUEST, revision, 0U);
  lx3_native_control(LX3_BINDING_HIGH_REQUEST, binding >> 48U, binding >> 32U);
  lx3_native_control(LX3_BINDING_LOW_REQUEST, binding >> 16U, binding);
  lx3_native_control(LX3_STATE_REQUEST, value, generation);
  lx3_status_t s = lx3_native_status();
  assert(lx3_status_valid(&s, sizeof(s)) && s.sequence == sequence);
}

static void cite(uint8_t intent) {
  const lx3_status_t s = lx3_native_status();
  state(intent, s.pending_key, s.pending_generation, false, s.longitudinal_revision);
}

static void reset(void) {
  timer.CNT = 2000000U; sequence = 0U; memset(counters, 0, sizeof(counters));
  assert(set_safety_hooks(SAFETY_HYUNDAI_CANFD, LX3_AUTHORITY_PROFILE) == 0);
  lx3_native_control(LX3_EPOCH_HIGH_REQUEST, (uint16_t)(epoch >> 48U), (uint16_t)(epoch >> 32U));
  lx3_native_control(LX3_EPOCH_LOW_REQUEST, (uint16_t)(epoch >> 16U), (uint16_t)epoch);
  rx_tick(0U, false, false); rx_tick(0U, false, false); rx_tick(0U, false, false);
  assert(lx3_native_healthy());
  state(0U, 0U, 0U, false, 0U);
  assert(!controls_allowed && lx3_auth.allowed == 0U);
}

static void lfa(void) { rx_tick(128U, false, false); rx_tick(0U, false, false); }
static void main_button(void) {
  rx_tick(8U, false, false);
  for (unsigned i = 0U; i < 8U; i++) { rx_tick(0U, false, false); state(0U, 0U, 0U, false, lx3_auth.longitudinal_revision); }
}

static CANPacket_t cb(unsigned active, unsigned torque, int angle) {
  CANPacket_t p = packet(0xCB, 0U, 24U);
  p.data[3] = active << 4U; const unsigned raw = (unsigned)angle & 0x3FFFU;
  p.data[4] = raw; p.data[5] = raw >> 8U; p.data[6] = torque;
  hyundai_canfd_update_checksum(&p); return p;
}

static bool send(CANPacket_t *p, uint16_t generation) {
  lx3_incoming_identity = (lx3_tx_identity_t){epoch, generation, lx3_packet_axis(p), true};
  bool ok = safety_tx_hook(p);
  lx3_incoming_identity = (lx3_tx_identity_t){0};
  return ok;
}

static void physical_grant_and_final_revoke(void) {
  reset(); state(LX3_ALL, 0U, 0U, false, 0U); assert(lx3_auth.allowed == 0U);
  CANPacket_t active = cb(2U, 25U, 0);
  assert(!send(&active, 0U)); assert(!controls_allowed);
  lfa(); cite(LX3_LAT); assert(lx3_auth.allowed == LX3_LAT && !controls_allowed);
  uint16_t generation = lx3_auth.lateral_generation;
  assert(send(&active, generation));
  CANPacket_t original = cb(2U, 90U, 0); original.bus = 2U; original.data[2] = 123U;
  hyundai_canfd_update_checksum(&original);
  assert(safety_fwd_hook(&original) == 0);
  assert(original.data[6] == 25U && original.data[2] == 123U);
  lx3_queue_stamp_t stamp = lx3_forward_stamp;
  assert(lx3_native_final_tx(&original, &stamp));
  state(0U, 0U, 0U, false, lx3_auth.longitudinal_revision);
  assert(!lx3_native_final_tx(&original, &stamp)); // Actual pre-TXBAR policy.
  assert(!send(&active, generation));
  CANPacket_t off = cb(1U, 0U, 0);
  assert(send(&off, generation)); // Epoch-valid inactive release across revoke.
  assert(lx3_auth.allowed == 0U);
  puts("PASS actual physical RX -> STATE -> guarded TX -> OEM-triggered replacement -> final revoke");
}

static void separate_pending_and_main_tail(void) {
  reset(); lfa();
  const uint16_t lat_key = lx3_auth.pending_key, lat_gen = lx3_auth.pending_generation;
  rx_tick(2U, false, false); rx_tick(0U, false, false);
  assert(lx3_auth.pending_axes == LX3_ALL);
  state(LX3_LAT, lat_key, lat_gen, false, 0U);
  cite(LX3_ALL); assert(lx3_auth.allowed == LX3_ALL && controls_allowed);
  reset(); rx_tick(8U, false, false); rx_tick(0U, false, false);
  rx_tick(1U, false, false); rx_tick(0U, false, false); cite(LX3_LONG);
  const uint16_t long_gen = lx3_auth.longitudinal_generation;
  for (unsigned i = 0U; i < 5U; i++) { rx_tick(0U, false, false); state(LX3_LONG, 0U, 0U, false, 0U); }
  assert(lx3_auth.allowed == LX3_LONG && lx3_auth.longitudinal_generation == long_gen);
  cite(LX3_ALL); assert(lx3_auth.allowed == LX3_ALL);
  puts("PASS independent LFA/SET pending and separately released RES during MAIN debounce tail");
}

static void input_and_transport_failures(void) {
  reset(); main_button(); cite(LX3_ALL); assert(lx3_auth.allowed == LX3_ALL);
  rx_tick(1U, false, false); rx_tick(2U, false, false);
  assert(!lx3_switches.ready && lx3_auth.allowed == LX3_ALL && lx3_auth.pending_axes == 0U);
  rx_tick(4U, false, false); assert(lx3_auth.allowed == LX3_LAT && !controls_allowed);
  CANPacket_t bad = packet(0xEA, 0U, 24U); bad.data[2] = ++counters[3];
  hyundai_canfd_update_checksum(&bad); bad.data[16] ^= 1U;
  assert(!safety_rx_hook(&bad)); assert(lx3_auth.allowed == 0U);
  reset(); lfa(); cite(LX3_LAT);
  lx3_native_control(LX3_CONFIG_REQUEST, 0U, 700U);
  lx3_native_control(LX3_STATE_REQUEST, 0U, 0U); // Partial stage cannot alter config or renew heartbeat.
  assert(lx3_auth.allowed == LX3_LAT && lx3_native_status().config == 5U);
  for (unsigned i = 0U; i < 7U; i++) rx_tick(0U, false, false);
  assert(lx3_auth.allowed == 0U && lx3_auth.reason == LX3_REASON_HEARTBEAT);
  puts("PASS input ambiguity preserves continuation, raw CANCEL revokes, CRC and stale heartbeat revoke");
}

static void pedal_revision_and_epoch(void) {
  reset(); main_button(); cite(LX3_ALL);
  const uint16_t old_revision = lx3_auth.longitudinal_revision;
  rx_tick(0U, true, false); assert(lx3_auth.allowed == LX3_LAT);
  state(LX3_LAT, 0U, 0U, false, old_revision); // Pre-revoke OFF is not an acknowledgement.
  rx_tick(0U, false, false); state(LX3_ALL, 0U, 0U, true, old_revision);
  assert(lx3_auth.allowed == LX3_LAT);
  const uint16_t revision = lx3_auth.longitudinal_revision;
  state(LX3_LAT, 0U, 0U, false, revision); state(LX3_ALL, 0U, 0U, true, revision);
  assert(lx3_auth.allowed == LX3_ALL);
  assert(set_safety_hooks(SAFETY_HYUNDAI_CANFD, LX3_AUTHORITY_PROFILE) == 0);
  assert(lx3_auth.epoch == 0U && lx3_auth.allowed == 0U);
  puts("PASS actual brake -> post-revoke OFF acknowledgement -> armed resume; mode reset clears epoch");
}

static void emergency_and_corrupt_original(void) {
  reset(); lfa(); cite(LX3_LAT);
  CANPacket_t active = cb(2U, 25U, 0); assert(send(&active, lx3_auth.lateral_generation));
  CANPacket_t original = cb(2U, 90U, 0); original.bus = 2U; original.data[0] ^= 1U;
  CANPacket_t before = original;
  assert(safety_fwd_hook(&original) == 0 && !memcmp(original.data, before.data, 24U));
  assert(canfd_bfwd_find(0xCB, 0)->count == 1U);
  CANPacket_t emergency = packet(0x161, 2U, 32U); emergency.data[16] = 21U;
  hyundai_canfd_update_checksum(&emergency); (void)safety_fwd_hook(&emergency);
  assert(lx3_oem_emergency && canfd_bfwd_find(0xCB, 0)->count == 0U);
  assert(!send(&active, lx3_auth.lateral_generation));
  original = cb(2U, 90U, 0); original.bus = 2U;
  assert(safety_fwd_hook(&original) == 0 && original.data[6] == 90U);
  assert(lx3_oem_lat_count == 1U);
  puts("PASS corrupt OEM source not repaired; actual emergency source flushes OP replacements and stays OEM");
}

static void stale_head_and_limit_recovery(void) {
  reset(); lfa(); cite(LX3_LAT);
  CANPacket_t active = cb(2U,25U,0), off = cb(1U,0U,0);
  const uint16_t generation = lx3_auth.lateral_generation;
  assert(send(&active,generation));
  rx_tick(0U,false,false); rx_tick(0U,false,false);
  assert(send(&off,generation)); rx_tick(0U,false,false);
  CANPacket_t source = cb(2U,90U,0); source.bus=2U;
  assert(safety_fwd_hook(&source)==0 && source.data[6]==0U);
  assert(lx3_forward_stamp.origin==1U && lx3_native_final_tx(&source,&lx3_forward_stamp));
  reset(); lfa(); cite(LX3_LAT);
  CANPacket_t bad = cb(2U,26U,0);
  for (unsigned i=0U;i<3U;i++) assert(!send(&bad,lx3_auth.lateral_generation));
  assert(lx3_auth.allowed==0U && lx3_auth.reason==LX3_REASON_LIMIT);
  puts("PASS stale buffered head retains fresh inactive follower; three envelope failures revoke with reason");
}

int main(void) {
  physical_grant_and_final_revoke(); separate_pending_and_main_tail(); input_and_transport_failures();
  pedal_revision_and_epoch(); emergency_and_corrupt_original(); stale_head_and_limit_recovery();
  return 0;
}
