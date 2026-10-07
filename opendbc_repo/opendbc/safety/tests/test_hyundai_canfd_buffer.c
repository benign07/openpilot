// Offline regression: the actual Hyundai policy, with only a mock MCU clock.
// No CAN interface, host permission grant, device access or road qualification.
#define CANFD
#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include "fake_stm.h"
#ifndef BUFFER_FAKE_STM_HAS_PUTUI
void putui(uint32_t n) { printf("%u", n); }
#endif
bool safety_tx_buffered_for_fwd = false; // board TX glue, not a permission fixture
#include "can.h"
#include "faults.h"
#include "safety.h"

static CANPacket_t command(int addr, int bus, unsigned active, unsigned marker) {
  CANPacket_t p = {0};
  p.addr = addr;
  p.bus = bus;
  p.fd = 1U;
  p.data_len_code = 12U;
  p.data[3] = (uint8_t)(active << 4U);
  p.data[6] = active == 2U ? 50U : 0U;
  p.data[8] = (uint8_t)marker;
  hyundai_canfd_update_checksum(&p);
  return p;
}

static CanfdBufferedFwd *reset(unsigned param) {
  timer.CNT = 2000000U;
  assert(set_safety_hooks(SAFETY_HYUNDAI_CANFD, param) == 0);
  return canfd_bfwd_find(0xCB, 0);
}

static void overflow_retains_release(void) {
  CanfdBufferedFwd *st = reset(190U);
  for (unsigned i = 0U; i < CANFD_BFWD_MAX_QUEUE; i++) {
    CANPacket_t p = command(0xCB, 0, 2U, i);
    assert(safety_tx_hook(&p));
  }
  CANPacket_t release = command(0xCB, 0, 1U, CANFD_BFWD_MAX_QUEUE);
  assert(safety_tx_hook(&release));
  assert(st->count == CANFD_BFWD_MAX_QUEUE);
  CANPacket_t out;
  for (unsigned i = 1U; i <= CANFD_BFWD_MAX_QUEUE; i++) {
    assert(canfd_bfwd_pop(st, &out));
    // Old code drops the newly accepted release, and fails here with marker 0.
    assert(out.data[8] == i);
  }
  assert(out.data[3] == 0x10U && out.data[6] == 0U);
  assert(!controls_allowed);  // Queue policy is not an authority fix.
  puts("PASS overflow retains newest release and remaining FIFO order");
}

static void lone_command_is_not_delayed(void) {
  CanfdBufferedFwd *st = reset(190U);
  CANPacket_t on = command(0xCB, 0, 2U, 1U), off = command(0xCB, 0, 1U, 2U), out;
  canfd_bfwd_push(st, &on);
  assert(canfd_bfwd_pop(st, &out));
  assert(out.data[3] == 0x20U && st->count == 0U);
  assert(st->has_last_pkt && st->reuse_left == 2U);
  canfd_bfwd_push(st, &off);
  // Waiting for two commands after a drain would reuse the prior active command.
  assert(canfd_bfwd_pop(st, &out));
  assert(out.data[3] == 0x10U && out.data[6] == 0U);
  puts("PASS startup and refill consume a single pending command immediately");
}

static void refill_does_not_reuse_active(void) {
  CanfdBufferedFwd *st = reset(190U);
  CANPacket_t on = command(0xCB, 0, 2U, 1U), off = command(0xCB, 0, 1U, 2U), out;
  canfd_bfwd_push(st, &on);
  canfd_bfwd_push(st, &on); // enough to start both restored and upstream queues
  assert(canfd_bfwd_pop(st, &out));
  assert(canfd_bfwd_pop(st, &out));
  assert(st->count == 0U && st->has_last_pkt && st->reuse_left == 2U);
  canfd_bfwd_push(st, &off);
  CANPacket_t original = command(0xCB, 2, 1U, 99U);
  assert(safety_fwd_hook(&original) == 0);
  // Exact upstream START_COUNT=2 reuses the active last packet despite queued off.
  assert(original.data[3] == 0x10U && original.data[6] == 0U && original.data[8] == 2U);
  puts("PASS actual forwarding consumes pending release before reusing last active packet");
}

static void reuse_and_reset(void) {
  CanfdBufferedFwd *st = reset(190U);
  CANPacket_t p = command(0xCB, 0, 1U, 1U), out;
  canfd_bfwd_push(st, &p);
  assert(canfd_bfwd_pop(st, &out));
  for (unsigned i = 0U; i < CANFD_BFWD_REUSE_MAX; i++) {
    assert(!canfd_bfwd_pop(st, &out));
    assert(canfd_bfwd_reuse_last(st, &out));
    assert(out.data[6] == 0U);
  }
  assert(!canfd_bfwd_reuse_last(st, &out));
  canfd_bfwd_push(st, &p);
  reset(190U);
  assert(st->count == 0U && !st->started && !st->has_last_pkt && st->reuse_left == 0U);
  assert(!canfd_bfwd_reuse_last(st, &out));
  puts("PASS bounded reuse and safety reinitialization clear stored commands");
}

static void burst_ring_order(void) {
  CanfdBufferedFwd *st = reset(190U);
  CANPacket_t out;
  for (unsigned round = 0U; round < 50U; round++) {
    // Exercise repeated head/tail wrap with both overflow and normal consumption.
    for (unsigned i = 0U; i < 7U; i++) {
      CANPacket_t p = command(0xCB, 0, 1U, i);
      canfd_bfwd_push(st, &p);
    }
    for (unsigned i = 7U - CANFD_BFWD_MAX_QUEUE; i < 7U; i++) {
      assert(canfd_bfwd_pop(st, &out));
      assert(out.data[8] == i);
    }
    assert(st->count == 0U);
  }
  puts("PASS repeated bursts retain FIFO order across ring wrap");
}

static void invalid_target_and_copy(void) {
  CanfdBufferedFwd *st = reset(190U);
  CANPacket_t wrong = command(0xCB, 2, 1U, 1U), out;
  canfd_bfwd_push(NULL, &wrong);
  canfd_bfwd_push(st, &wrong);
  assert(st->count == 0U);
  st->enabled = false;
  wrong.bus = 0U;
  canfd_bfwd_push(st, &wrong);
  assert(!canfd_bfwd_pop(st, &out) && st->count == 0U);
  st->enabled = true;
  wrong.returned = 1U;
  wrong.rejected = 1U;
  canfd_bfwd_push(st, &wrong);
  wrong.data[8] = 42U;
  assert(canfd_bfwd_pop(st, &out));
  assert(out.data[8] == 1U && out.returned == 0U && out.rejected == 0U);
  puts("PASS invalid targets ignored; packet copy is independent and clears echo flags");
}

static void actual_forwarding(void) {
  reset(190U);
  // Fill with active commands, then enqueue an inactive command under pressure.
  for (unsigned i = 0U; i <= CANFD_BFWD_MAX_QUEUE; i++) {
    CANPacket_t p = command(0xCB, 0, i < CANFD_BFWD_MAX_QUEUE ? 2U : 1U, i);
    assert(safety_tx_hook(&p));
  }
  for (unsigned i = 1U; i <= CANFD_BFWD_MAX_QUEUE; i++) {
    CANPacket_t original = command(0xCB, 2, 1U, 99U);
    original.data[2] = (uint8_t)(100U + i);
    hyundai_canfd_update_checksum(&original);
    CANPacket_t fwd = original;
    assert(safety_fwd_hook(&fwd) == 0);
    assert(original.data[8] == 99U);
    assert(fwd.data[8] == i && fwd.data[2] == (uint8_t)(100U + i));
    assert(hyundai_canfd_get_checksum(&fwd) == hyundai_common_canfd_compute_checksum(&fwd));
    if (i == CANFD_BFWD_MAX_QUEUE) assert(fwd.data[3] == 0x10U && fwd.data[6] == 0U);
  }
  puts("PASS real forwarding preserves original counter/CRC and reaches retained release");
}

static void rejected_tx_does_not_evict(void) {
  CanfdBufferedFwd *st = reset(190U);
  for (unsigned i = 0U; i < CANFD_BFWD_MAX_QUEUE; i++) {
    CANPacket_t p = command(0xCB, 0, 2U, i);
    assert(safety_tx_hook(&p));
  }
  CANPacket_t bad = command(0xCB, 2, 1U, 99U);
  assert(!safety_tx_hook(&bad)); // destination outside the TX allowlist
  bad.bus = 0U;
  bad.data_len_code = 10U;
  assert(!safety_tx_hook(&bad)); // wrong length
  CANPacket_t out;
  for (unsigned i = 0U; i < CANFD_BFWD_MAX_QUEUE; i++) {
    assert(canfd_bfwd_pop(st, &out));
    assert(out.data[8] == i);
  }
  puts("PASS rejected TX cannot evict previously accepted commands");
}

static void rejected_tx_has_no_deferred_send(void) {
  CanfdBufferedFwd *st = reset(190U);
  CANPacket_t bad = command(0xCB, 0, 2U, 42U);
  bad.data_len_code = 10U; // valid storage, invalid CB wire length (16 instead of 24)
  assert(!safety_tx_hook(&bad));
  CANPacket_t original = command(0xCB, 2, 1U, 99U);
  assert(safety_fwd_hook(&original) == 0);
  // A rejected packet must not become a later replacement via skip-TX forwarding.
  assert(GET_LEN(&original) == 24U && original.data[6] == 0U && original.data[8] == 99U);
  assert(st->count == 0U && !st->has_last_pkt);

  reset(190U);
  CANPacket_t valid = command(0xCB, 0, 2U, 43U);
  relay_malfunction = true;
  assert(!safety_tx_hook(&valid));
  assert(st->count == 0U && !st->has_last_pkt);
  relay_malfunction = false; // isolate a future permitted-forward event in the fixture
  original = command(0xCB, 2, 1U, 99U);
  assert(safety_fwd_hook(&original) == 0 && original.data[6] == 0U);
  puts("PASS rejected length/relay frames never enter deferred forwarding");
}

static void rejected_scc_cannot_grant(void) {
  reset(190U);
  CANPacket_t p = {0};
  p.addr = 0x1A0;
  p.bus = 2U; // wrong host destination, but meaningful SCC content
  p.data_len_code = 13U;
  p.data[8] = 0x10U;
  p.data[16] = 0xFFU;
  p.data[17] = 0xF3U;
  p.data[18] = 0x3FU; // zero raw/value acceleration
  assert(!safety_tx_hook(&p));
  assert(!controls_allowed);
  p.bus = 0U;
  relay_malfunction = true;
  assert(!safety_tx_hook(&p) && !controls_allowed);
  relay_malfunction = false;
  // A valid existing Carrot SCC request keeps its current (separate) grant policy.
  assert(safety_tx_hook(&p) && controls_allowed);
  puts("PASS outer-rejected SCC cannot grant; allowed SCC retains existing policy");
}

static void special_policy_exceptions(void) {
  CANPacket_t p = command(0x321, 0, 1U, 1U);
  assert(set_safety_hooks(SAFETY_ALLOUTPUT, 0U) == 0);
  assert(safety_tx_hook(&p));
  relay_malfunction = true;
  assert(!safety_tx_hook(&p));
  assert(set_safety_hooks(SAFETY_ELM327, 0U) == 0);
  p.addr = 0x7DF;
  p.data_len_code = 8U;
  assert(safety_tx_hook(&p));
  p.data_len_code = 10U;
  assert(!safety_tx_hook(&p));
  assert(set_safety_hooks(SAFETY_SILENT, 0U) == 0);
  assert(!safety_tx_hook(&p));
  puts("PASS existing all-output/ELM327 exceptions and silent policy retained");
}

static void profile_compatibility(void) {
  const unsigned profiles[] = {0U, 2U, 4U, 8U, 12U, 16U, 20U, 24U, 28U, 32U, 40U, 56U, 60U, 144U, 156U, 188U, 190U};
  for (unsigned i = 0U; i < sizeof(profiles) / sizeof(profiles[0]); i++) {
    CanfdBufferedFwd *st = reset(profiles[i]);
    assert(!controls_allowed && st->count == 0U && !st->has_last_pkt);
    assert(hyundai_canfd_buffered_fwd == ((profiles[i] & 8U) != 0U));
    // Each of the six stock replacement queues has the same bounded ring contract.
    for (unsigned q = 0U; canfd_bfwd[q].addr != 0; q++) {
      CanfdBufferedFwd *s = &canfd_bfwd[q];
      for (unsigned n = 0U; n <= CANFD_BFWD_MAX_QUEUE; n++) {
        CANPacket_t p = command(s->addr, s->dst_bus, 1U, n);
        canfd_bfwd_push(s, &p);
      }
      CANPacket_t out;
      for (unsigned n = 1U; n <= CANFD_BFWD_MAX_QUEUE; n++) {
        assert(canfd_bfwd_pop(s, &out));
        assert(out.data[8] == n);
      }
    }
  }
  puts("PASS 17 profile initializations and six queue contracts (not per-car road validation)");
}

int main(int argc, char **argv) {
  if (argc > 1 && strcmp(argv[1], "refill") == 0) { refill_does_not_reuse_active(); return 0; }
  if (argc > 1 && strcmp(argv[1], "admission") == 0) { rejected_tx_has_no_deferred_send(); return 0; }
  if (argc > 1 && strcmp(argv[1], "grant") == 0) { rejected_scc_cannot_grant(); return 0; }
  overflow_retains_release();
  lone_command_is_not_delayed();
  refill_does_not_reuse_active();
  reuse_and_reset();
  burst_ring_order();
  invalid_target_and_copy();
  actual_forwarding();
  rejected_tx_does_not_evict();
  rejected_tx_has_no_deferred_send();
  rejected_scc_cannot_grant();
  special_policy_exceptions();
  profile_compatibility();
  puts("ALL BUFFER REGRESSIONS PASS; AUTHORITY AND OEM FAULTS ARE NOT QUALIFIED");
  return 0;
}
