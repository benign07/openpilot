// Pure production state-machine regression. Board/RX/IPC integration is tested
// separately; healthy=true here is a dependency fixture, never vehicle proof.
#include <assert.h>
#include <stdio.h>
#include "lx3_authority.h"

static uint32_t now;
static uint8_t counter;
static lx3_buttons_t buttons;
static lx3_authority_t auth;

static void heartbeat(uint8_t intent, bool cite, bool resume, bool brake) {
  (void)lx3_authority_heartbeat(&auth, now, auth.epoch, intent,
    cite ? auth.pending_generation : 0U, cite ? auth.pending_key : 0U,
    resume, true, brake, false, auth.longitudinal_revision);
}

static void frame(uint8_t key) {
  now += 40000U;
  counter += 2U;
  lx3_buttons_feed(&buttons, now, counter, key);
  for (uint8_t i = 0U; i < buttons.count; i++) {
    lx3_authority_button(&auth, &buttons.events[i], now, buttons.ready);
  }
}

static void reset(void) {
  now = 1000000U;
  counter = 248U;  // Include physical counter wrap in every test.
  buttons = (lx3_buttons_t){0};
  auth = (lx3_authority_t){.epoch=123456789U,
    .config={.always_lateral=true, .auto_resume=true, .lfa_long_us=700000U}};
  frame(0U); frame(0U); frame(0U);
  assert(buttons.ready);
}

static void lfa(void) { frame(128U); frame(128U); frame(0U); }

static void main_button(void) {
  frame(8U); frame(0U);
  const uint8_t first_neutral = counter;
  for (unsigned i = 0U; i < 7U; i++) frame(0U);
  assert(auth.pending_key == ((8U << 8U) | first_neutral));
}

static void physical_citation_and_axes(void) {
  reset();
  heartbeat(3U, false, false, false);
  assert(auth.allowed == 0U);  // Host enable/accel is not a physical grant.
  lfa();
  heartbeat(3U, true, false, false);
  assert(auth.allowed == 0U);  // LFA cannot authorize longitudinal control.
  heartbeat(1U, true, false, false);
  assert(auth.allowed == 1U);
  const uint16_t lat = auth.lateral_generation;
  // Long remains off; lateral intent persists through zero-output intervals.
  for (unsigned i = 0U; i < 100U; i++) { now += 100000U; heartbeat(1U, false, false, false); }
  assert(auth.allowed == 1U && auth.lateral_generation == lat);
  heartbeat(0U, false, false, false);
  heartbeat(1U, true, false, false);
  assert(auth.allowed == 0U);  // Consumed event cannot re-enable after host off.
  puts("PASS physical citation, separate axes, lat-only heartbeat, consumed-event replay");
}

static void main_and_resume_barrier(void) {
  reset(); main_button(); heartbeat(3U, true, false, false);
  assert(auth.allowed == 3U && auth.longitudinal_armed);
  const uint16_t lat = auth.lateral_generation, old_long = auth.longitudinal_generation;
  lx3_authority_pedal(&auth, true, false);
  assert(auth.allowed == 1U && auth.resume_needs_off);
  heartbeat(3U, false, true, true);
  heartbeat(3U, false, true, false);
  assert(auth.allowed == 1U);  // Brake release alone cannot re-use old intent.
  heartbeat(1U, false, false, false);
  heartbeat(3U, false, true, false);
  assert(auth.allowed == 3U && auth.lateral_generation == lat);
  assert(auth.longitudinal_generation != old_long);
  assert(!lx3_authority_identity(&auth, auth.epoch, LX3_LONG, old_long));
  frame(4U);
  assert(auth.allowed == 1U && !auth.longitudinal_armed);
  heartbeat(1U, false, false, false); heartbeat(3U, false, true, false);
  assert(auth.allowed == 1U);
  puts("PASS MAIN debounce identity, pedal resume barrier, independent generations, cancel disarm");
}

static void bad_context_and_old_epoch(void) {
  reset(); lfa();
  const uint16_t generation = auth.pending_generation, key = auth.pending_key;
  lx3_authority_maintain(&auth, now, false);
  assert(!lx3_authority_heartbeat(&auth, now, auth.epoch, 1U, generation, key, false, true, false, false, 0U));
  assert(auth.allowed == 0U);
  lfa(); heartbeat(1U, true, false, false);
  assert(auth.allowed == 1U);
  lx3_authority_maintain(&auth, now + LX3_HEARTBEAT_TIMEOUT_US + 1U, true);
  assert(auth.allowed == 0U && !auth.longitudinal_armed);
  lfa();
  assert(!lx3_authority_heartbeat(&auth, now, auth.epoch + 1U, 1U, auth.pending_generation,
    auth.pending_key, false, true, false, false, 0U));
  assert(auth.allowed == 0U);
  puts("PASS invalid context destroys pending grant; timeout and wrong epoch revoke");
}

static void physical_edges(void) {
  reset();
  frame(128U);
  for (unsigned i = 0U; i < 20U; i++) frame(128U);
  frame(0U);
  assert(auth.pending_axes == LX3_LAT);
  heartbeat(0U, false, false, false);  // Carrot long press gives no enable citation.
  assert(auth.allowed == 0U);
  lx3_authority_revoke(&auth, LX3_ALL, LX3_REASON_INPUT, true);
  frame(1U); frame(128U); frame(128U); frame(0U);
  assert(!buttons.ready && auth.pending_axes == 0U);
  frame(0U); frame(0U);
  assert(buttons.ready);
  // Same MCU timestamp is permitted for queued neutral packets, not duplicates.
  lx3_buttons_feed(&buttons, now, (uint8_t)(counter + 2U), 0U);
  assert(buttons.ready);
  lx3_buttons_feed(&buttons, now, (uint8_t)(counter + 2U), 0U);
  assert(!buttons.ready);
  buttons = (lx3_buttons_t){0};
  for (unsigned i = 0U; i < 10U; i++) frame(128U);
  frame(0U);
  assert(!buttons.ready && buttons.count == 0U);
  puts("PASS long press, direct-switch ambiguity, coalesced FIFO, duplicate and boot-held input");
}

int main(void) {
  physical_citation_and_axes(); main_and_resume_barrier(); bad_context_and_old_epoch(); physical_edges();
  return 0;
}
