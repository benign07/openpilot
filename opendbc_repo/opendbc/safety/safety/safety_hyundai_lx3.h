#pragma once
// Included after the stock Carrot forwarding ring. Other Hyundai profiles do
// not enter this adapter; MDPS payload construction remains in stock host code.
#include "lx3_authority.h"
#include "lx3_protocol.h"

static lx3_authority_t lx3_auth;
static lx3_buttons_t lx3_switches;
static bool lx3_profile_supported;
static uint32_t lx3_rx_good_us[6];
static bool lx3_rx_good[6];
static bool lx3_mdps_fault;
static int lx3_angle_measured;
#define LX3_ANGLE_HISTORY_SIZE 16U
static struct { int angle; uint32_t us; bool valid; } lx3_angle_history[LX3_ANGLE_HISTORY_SIZE];
static unsigned lx3_angle_history_next;
static uint32_t lx3_oem_lat_count;
static uint32_t lx3_oem_long_count;
static bool lx3_oem_emergency;
static lx3_tx_identity_t lx3_incoming_identity;
static inline void lx3_protocol_reset(void);

static const int lx3_rx_addresses[6] = {0x105, 0x175, 0xA0, 0xEA, 0x10B, 0x1AA};
static const int lx3_rx_lengths[6] = {32, 24, 24, 24, 16, 16};
static RxCheck lx3_rx_checks[] = {
  {.msg = {{0x105, 0, 32, .frequency=100U, .ignore_counter=true}, {0}, {0}}},
  {.msg = {{0x175, 0, 24, .frequency=50U, .max_counter=255U}, {0}, {0}}},
  {.msg = {{0xA0, 0, 24, .frequency=100U, .max_counter=255U}, {0}, {0}}},
  {.msg = {{0xEA, 0, 24, .frequency=100U, .max_counter=255U}, {0}, {0}}},
  // Physical counter +2 is checked by the switch decoder, never the +1 parser.
  {.msg = {{0x10B, 0, 16, .frequency=25U, .ignore_counter=true}, {0}, {0}}},
  {.msg = {{0x1AA, 0, 16, .frequency=50U, .max_counter=255U, .ignore_checksum=true}, {0}, {0}}},
};

static bool lx3_native_active(void) {
  return (current_safety_mode == 28U) && ((current_safety_param & LX3_AUTHORITY_PARAM) != 0U);
}

static bool lx3_native_healthy(void) {
  const uint32_t now = microsecond_timer_get();
  bool healthy = lx3_profile_supported && !relay_malfunction && !safety_rx_checks_invalid && !lx3_mdps_fault &&
                   lx3_switches.seen && ((now - lx3_switches.last_us) <= LX3_BUTTON_TIMEOUT_US);
  for (unsigned i = 0U; i < 6U; i++) {
    healthy = healthy && lx3_rx_good[i] && ((now - lx3_rx_good_us[i]) <= LX3_BUTTON_TIMEOUT_US);
  }
  return healthy;
}

typedef struct {
  bool active;
  int angle;
  int torque;
  uint32_t us;
  uint32_t angle_credit;   // raw angle * 1000
  uint32_t torque_credit; // torque units * 1000
} lx3_envelope_t;
static lx3_envelope_t lx3_admit_envelope;
static lx3_envelope_t lx3_final_envelope;
static bool lx3_packet_active(const CANPacket_t *p);
static unsigned lx3_limit_rejections[2];

static void lx3_purge_active_slot(CanfdBufferedFwd *st) {
  CANPacket_t retained[CANFD_BFWD_MAX_QUEUE];
  lx3_queue_stamp_t stamps[CANFD_BFWD_MAX_QUEUE];
  uint8_t count = 0U;
  for (uint8_t i = 0U; i < st->count; i++) {
    const unsigned at = (st->head + i) % CANFD_BFWD_MAX_QUEUE;
    if (!lx3_packet_active(&st->q[at])) { retained[count] = st->q[at]; stamps[count++] = st->stamps[at]; }
  }
  st->head = 0U; st->tail = count % CANFD_BFWD_MAX_QUEUE; st->count = count; st->started = count != 0U;
  for (uint8_t i = 0U; i < count; i++) { st->q[i] = retained[i]; st->stamps[i] = stamps[i]; }
  if (st->has_last_pkt && lx3_packet_active(&st->last_pkt)) { st->has_last_pkt = false; st->reuse_left = 0U; }
}

static void lx3_native_purge(uint8_t axes) {
  for (unsigned i = 0U; canfd_bfwd[i].addr != 0; i++) {
    const int addr = canfd_bfwd[i].addr;
    if ((((axes & LX3_LAT) != 0U) && ((addr == 0xCB) || (addr == 0x12A))) ||
        (((axes & LX3_LONG) != 0U) && (addr == 0x1A0))) lx3_purge_active_slot(&canfd_bfwd[i]);
  }
  if ((axes & LX3_LAT) != 0U) {
    lx3_admit_envelope = (lx3_envelope_t){0};
    lx3_final_envelope = (lx3_envelope_t){0};
    lx3_limit_rejections[0] = 0U;
    lx3_limit_rejections[1] = 0U;
  }
}

static void lx3_native_maintain(void) {
  const uint8_t previous = lx3_auth.allowed;
  lx3_authority_maintain(&lx3_auth, microsecond_timer_get(), lx3_native_healthy());
  const uint8_t removed = previous & (uint8_t)~lx3_auth.allowed;
  if (removed != 0U) lx3_native_purge(removed);
  // Standard health/engaged heartbeat continues to mean longitudinal enable.
  // LFA-only permission is explicit in the versioned companion, not hidden in
  // a union bit that would conceal a missing longitudinal grant.
  controls_allowed = (lx3_auth.allowed & LX3_LONG) != 0U;
}

static void lx3_native_init(uint16_t param) {
  lx3_protocol_reset();
  lx3_auth = (lx3_authority_t){0};
  lx3_switches = (lx3_buttons_t){0};
  lx3_profile_supported = param == LX3_AUTHORITY_PROFILE;
  for (unsigned i = 0U; i < 6U; i++) { lx3_rx_good[i] = false; lx3_rx_good_us[i] = 0U; }
  lx3_mdps_fault = false;
  lx3_angle_measured = 0;
  lx3_angle_history_next = 0U;
  for (unsigned i = 0U; i < LX3_ANGLE_HISTORY_SIZE; i++) lx3_angle_history[i].valid = false;
  lx3_oem_lat_count = 0U;
  lx3_oem_long_count = 0U;
  lx3_oem_emergency = false;
  lx3_incoming_identity = (lx3_tx_identity_t){0};
  lx3_native_purge(LX3_ALL);
}

// Called even when the common RX checker rejected the frame, including a
// monitored address with the wrong DLC (which common address lookup misses).
static bool lx3_native_observe(const CANPacket_t *p, bool accepted) {
  if (!lx3_native_active()) return accepted;
  const uint32_t now = microsecond_timer_get();
  if (GET_BUS(p) == 0) {
    for (unsigned i = 0U; i < 6U; i++) {
      if (GET_ADDR(p) != lx3_rx_addresses[i]) continue;
      accepted = accepted && (GET_LEN(p) == lx3_rx_lengths[i]) && (p->returned == 0U) && (p->rejected == 0U);
      if (!accepted) {
        lx3_rx_good[i] = false;
        lx3_authority_revoke(&lx3_auth, LX3_ALL, LX3_REASON_INPUT, true);
        lx3_buttons_invalidate(&lx3_switches);
        lx3_switches.seen = false;
        lx3_native_purge(LX3_ALL);
        break;
      }
      lx3_rx_good[i] = true;
      lx3_rx_good_us[i] = now;
      if (GET_ADDR(p) == 0xEA) {
        const unsigned raw = GET_BYTE(p, 16) | (GET_BYTE(p, 17) << 8U);
        lx3_angle_measured = raw >= 32768U ? (int)raw - 65536 : (int)raw;
        lx3_angle_history[lx3_angle_history_next].angle = lx3_angle_measured;
        lx3_angle_history[lx3_angle_history_next].us = now;
        lx3_angle_history[lx3_angle_history_next].valid = true;
        lx3_angle_history_next = (lx3_angle_history_next + 1U) % LX3_ANGLE_HISTORY_SIZE;
        lx3_mdps_fault = GET_BIT(p, 54U) || GET_BIT(p, 149U);
      } else if (GET_ADDR(p) == 0x10B) {
        lx3_buttons_feed(&lx3_switches, now, GET_BYTE(p, 2), GET_BYTE(p, 10));
        if (!lx3_switches.ready) lx3_authority_pending_clear(&lx3_auth);
        // CANCEL is a revoke even during resynchronization; it can never grant.
        if ((GET_BYTE(p, 10) & 0x8FU) == 4U) {
          const lx3_button_event_t cancel = {.button=LX3_BUTTON_CANCEL, .pressed=true};
          lx3_authority_button(&lx3_auth, &cancel, now, false);
        }
        if (((GET_BYTE(p, 10) & 0x8FU) == 8U) && !lx3_switches.ready) {
          lx3_authority_revoke(&lx3_auth, LX3_ALL, LX3_REASON_CANCEL, true);
        }
        for (uint8_t j = 0U; j < lx3_switches.count; j++) {
          lx3_authority_button(&lx3_auth, &lx3_switches.events[j], now, lx3_native_healthy() && lx3_switches.ready);
        }
      }
      break;
    }
  }
  lx3_native_maintain();
  return accepted;
}

static void lx3_native_pedals(void) {
  const uint8_t previous = lx3_auth.allowed;
  lx3_authority_pedal(&lx3_auth, brake_pressed && !brake_pressed_prev, gas_pressed && !gas_pressed_prev);
  brake_pressed_prev = brake_pressed;
  gas_pressed_prev = gas_pressed;
  if (previous != lx3_auth.allowed) lx3_native_purge(previous & (uint8_t)~lx3_auth.allowed);
  lx3_native_maintain();
}

static bool lx3_angle_envelope(const CANPacket_t *p, lx3_envelope_t *s, bool commit, uint32_t anchor_us) {
  const int raw = GET_BYTE(p, 4) | ((GET_BYTE(p, 5) & 63U) << 8U);
  const int angle = raw >= 8192 ? raw - 16384 : raw;
  const int torque = GET_BYTE(p, 6);
  const uint32_t now = microsecond_timer_get();
  if ((ABS(angle) > 1750) || (torque > 250)) return false;
  lx3_envelope_t next = *s;
  if (!next.active) {
    // Match the first command to recent *measured* angles, like other angle
    // policies. card's sample can be 1-3 ticks behind the current MCU sample.
    // No host inactive target, stale sample or larger command rate is trusted.
    int lo = 0, hi = 0;
    bool found = false;
    // At final TX use the measurement window at admission, not a shifted
    // window after the command waited in a software queue (at most 100 ms).
    for (unsigned i = 0U; i < LX3_ANGLE_HISTORY_SIZE; i++) {
      if (lx3_angle_history[i].valid && (anchor_us - lx3_angle_history[i].us <= 50000U)) {
        const int measured = lx3_angle_history[i].angle;
        lo = found ? MIN(lo, measured) : measured; hi = found ? MAX(hi, measured) : measured;
        found = true;
      }
    }
    if (!found) return false;
    next.angle = MIN(MAX(angle, lo), hi);
    next.torque = 0;
    next.angle_credit = 21000U;
    // Stock host clamps the first active command to ANGLE_MIN_TORQUE = 25.
    next.torque_credit = 25000U;
  } else {
    const uint32_t elapsed = MIN(now - next.us, 20000U);
    next.angle_credit = MIN(40000U, next.angle_credit + elapsed * 2U);
    // Host maximum positive torque ramp is 22.5 per 10 ms.
    next.torque_credit = MIN(46000U, next.torque_credit + (elapsed * 23U) / 10U);
  }
  const uint32_t angle_cost = (uint32_t)ABS(angle - next.angle) * 1000U;
  const uint32_t torque_cost = (uint32_t)MAX(torque - next.torque, 0) * 1000U;
  if ((angle_cost > next.angle_credit) || (torque_cost > next.torque_credit)) return false;
  next.angle_credit -= angle_cost;
  next.torque_credit -= torque_cost;
  next.angle = angle; next.torque = torque; next.us = now; next.active = true;
  if (commit) *s = next;
  return true;
}

static uint8_t lx3_packet_axis(const CANPacket_t *p) {
  return GET_ADDR(p) == 0x1A0 ? LX3_LONG : LX3_LAT;
}

static bool lx3_packet_active(const CANPacket_t *p) {
  if (GET_ADDR(p) == 0xCB) return (((GET_BYTE(p, 3) >> 4U) & 3U) >= 2U) || (GET_BYTE(p, 6) != 0U);
  if (GET_ADDR(p) == 0x12A) return GET_BIT(p, 52U) || (((GET_BYTE(p, 9) >> 4U) & 3U) != 0U) || (GET_BYTE(p, 12) != 0U);
  if (GET_ADDR(p) == 0x1A0) {
    const unsigned mode = (GET_BYTE(p, 8) >> 4U) & 7U;
    const int raw = (int)(((GET_BYTE(p, 17) & 7U) << 8U) | GET_BYTE(p, 16)) - 1023;
    const int val = (int)((GET_BYTE(p, 18) << 4U) | (GET_BYTE(p, 17) >> 4U)) - 1023;
    return (mode == 1U) || (mode == 2U) || (raw != 0) || (val != 0) || ((GET_BYTE(p, 23) & 3U) != 0U);
  }
  return false;
}

static bool lx3_packet_shape(const CANPacket_t *p, bool permitted) {
  if (GET_ADDR(p) == 0xCB) {
    const unsigned active = (GET_BYTE(p, 3) >> 4U) & 3U;
    return (active <= 2U) && ((active == 2U) ? permitted : GET_BYTE(p, 6) == 0U);
  }
  if (GET_ADDR(p) == 0x12A) {
    const unsigned raw_torque = ((GET_BYTE(p, 6) & 15U) << 7U) | (GET_BYTE(p, 5) >> 1U);
    return (raw_torque == 0U) && !GET_BIT(p, 52U) && (((GET_BYTE(p, 9) >> 4U) & 3U) == 0U) && (GET_BYTE(p, 12) == 0U);
  }
  if (GET_ADDR(p) == 0x1A0) {
    const unsigned mode = (GET_BYTE(p, 8) >> 4U) & 7U;
    const int raw = (int)(((GET_BYTE(p, 17) & 7U) << 8U) | GET_BYTE(p, 16)) - 1023;
    const int val = (int)((GET_BYTE(p, 18) << 4U) | (GET_BYTE(p, 17) >> 4U)) - 1023;
    const unsigned stop = GET_BYTE(p, 23) & 3U;
    if (stop > 1U) return false;
    if ((mode != 0U) && (mode != 1U) && (mode != 2U) && (mode != 4U)) return false;
    if ((raw < HYUNDAI_LONG_LIMITS.min_accel) || (raw > HYUNDAI_LONG_LIMITS.max_accel) ||
        (val < HYUNDAI_LONG_LIMITS.min_accel) || (val > HYUNDAI_LONG_LIMITS.max_accel)) return false;
    if ((mode == 0U) || (mode == 4U) || (mode == 2U)) {
      if ((raw != 0) || (val != 0) || (stop != 0U)) return false;
    }
    return permitted || !lx3_packet_active(p);
  }
  return true;
}

static bool lx3_native_packet(const CANPacket_t *p, const lx3_queue_stamp_t *stamp, bool final, bool commit) {
  lx3_native_maintain();
  if (!lx3_profile_supported || !lx3_tx_guarded_address(GET_ADDR(p))) return false;
  const uint8_t axis = lx3_packet_axis(p);
  if ((axis == LX3_LAT) && lx3_oem_emergency) return false;
  const uint16_t generation = axis == LX3_LAT ? lx3_auth.lateral_generation : lx3_auth.longitudinal_generation;
  const bool tagged = (stamp->origin == 1U) && (stamp->epoch != 0U) && (stamp->epoch == lx3_auth.epoch) &&
                      (stamp->axis == axis) && (!lx3_packet_active(p) || stamp->generation == generation) &&
                      ((microsecond_timer_get() - stamp->admitted_us) <= 100000U);
  const bool permitted = tagged && lx3_native_healthy() && ((lx3_auth.allowed & axis) != 0U);
  if (!tagged || !lx3_packet_shape(p, permitted)) return false;
  if ((GET_ADDR(p) == 0xCB) && lx3_packet_active(p)) {
    const bool ok = lx3_angle_envelope(p, final ? &lx3_final_envelope : &lx3_admit_envelope, commit, stamp->admitted_us);
    if (commit || !ok) {
      const unsigned at = final ? 1U : 0U;
      lx3_limit_rejections[at] = ok ? 0U : lx3_limit_rejections[at] + 1U;
      if (lx3_limit_rejections[at] >= 3U) {
        lx3_authority_revoke(&lx3_auth, LX3_LAT, LX3_REASON_LIMIT, true);
        lx3_native_purge(LX3_LAT);
        lx3_native_maintain();
      }
    }
    return ok;
  }
  if ((GET_ADDR(p) == 0xCB) && commit) {
    *(final ? &lx3_final_envelope : &lx3_admit_envelope) = (lx3_envelope_t){0};
    lx3_limit_rejections[final ? 1U : 0U] = 0U;
  }
  return true;
}

static bool lx3_native_admit(const CANPacket_t *p) {
  const uint8_t axis = lx3_packet_axis(p);
  lx3_current_tx_stamp = (lx3_queue_stamp_t){.epoch=lx3_incoming_identity.epoch,
    .admitted_us=microsecond_timer_get(), .generation=lx3_incoming_identity.generation,
    .axis=lx3_incoming_identity.axis, .origin=1U};
  if (!lx3_incoming_identity.valid || (lx3_incoming_identity.axis != axis)) return false;
  return lx3_native_packet(p, &lx3_current_tx_stamp, false, true);
}

static void lx3_native_oem(const CANPacket_t *p) {
  if (GET_BUS(p) == 2 && lx3_packet_active(p)) {
    if (GET_ADDR(p) == 0xCB) lx3_oem_lat_count++;
    if (GET_ADDR(p) == 0x1A0) lx3_oem_long_count++;
  }
}

static bool lx3_native_oem_source(const CANPacket_t *p) {
  const int addr = GET_ADDR(p);
  const int length = addr == 0xCB ? 24 : addr == 0x12A ? 16 : 32;
  return GET_BUS(p) == 2 && GET_LEN(p) == length && !p->returned && !p->rejected &&
    hyundai_canfd_get_checksum(p) == hyundai_common_canfd_compute_checksum(p);
}

static void lx3_native_emergency_observe(const CANPacket_t *p) {
  if (GET_ADDR(p) == 0x161 && lx3_native_oem_source(p)) {
    const unsigned alert = GET_BYTE(p, 16) & 63U;
    const bool emergency = ((alert >= 11U) && (alert <= 15U)) || ((alert >= 21U) && (alert <= 26U));
    if (emergency || lx3_oem_emergency) {
      lx3_native_purge(LX3_LAT);
      // OEM emergency owns these slots, including inactive host metadata.
      canfd_bfwd_reset(canfd_bfwd_find(0xCB, 0));
      canfd_bfwd_reset(canfd_bfwd_find(0x12A, 0));
    }
    // Invalid or missing clear frames cannot resurrect queued OP commands.
    lx3_oem_emergency = emergency;
  }
}

#include "lx3_native_protocol.h"

static inline bool lx3_native_final_tx(const CANPacket_t *p, const lx3_queue_stamp_t *stamp) {
  if (stamp->origin == 0U) return true;  // Genuine OEM or an unchanged legacy profile.
  return lx3_native_active() && lx3_native_packet(p, stamp, true, true);
}
