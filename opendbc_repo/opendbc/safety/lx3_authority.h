#pragma once
#include <stdbool.h>
#include <stdint.h>
#include "lx3_buttons.h"

// Version 3 is deliberately incompatible with the October 1 experiment.
// This is host/MCU state, not a vehicle CAN message or a replacement MDPS reply.
#define LX3_AUTHORITY_VERSION 3U
#define LX3_AUTHORITY_PARAM 2048U
#define LX3_AUTHORITY_PROFILE (LX3_AUTHORITY_PARAM | 190U)
#define LX3_LAT 1U
#define LX3_LONG 2U
#define LX3_ALL 3U
#define LX3_HEARTBEAT_TIMEOUT_US 250000U
#define LX3_EVENT_TIMEOUT_US 600000U

enum {
  LX3_REASON_NONE = 0,
  LX3_REASON_INPUT = 1,
  LX3_REASON_HEARTBEAT = 2,
  LX3_REASON_CANCEL = 3,
  LX3_REASON_HOST_OFF = 4,
  LX3_REASON_PEDAL = 5,
  LX3_REASON_IDENTITY = 6,
  LX3_REASON_LIMIT = 7,
  LX3_REASON_EXHAUSTED = 8,
};

typedef struct {
  bool always_lateral;
  bool disengage_on_gas;
  bool auto_resume;
  uint8_t lfa_mode;
  uint8_t cancel_mode;
  uint32_t lfa_long_us;
} lx3_authority_config_t;

typedef struct {
  uint64_t epoch;
  uint16_t sequence;
  uint16_t lateral_generation;
  uint16_t longitudinal_generation;
  uint16_t lateral_revision;
  uint16_t longitudinal_revision;
  uint16_t pending_generation;
  uint16_t pending_key;
  uint16_t lateral_key;
  uint16_t longitudinal_key;
  uint32_t pending_us;
  uint16_t pending_axis_generation[2];
  uint16_t pending_axis_key[2];
  uint32_t pending_axis_us[2];
  uint16_t main_press_sequence;
  uint32_t heartbeat_us;
  uint8_t pending_axes;
  uint8_t allowed;
  uint8_t last_intent;
  uint8_t reason;
  bool heartbeat_seen;
  bool longitudinal_armed;
  bool lateral_armed;
  bool resume_needs_off;
  bool exhausted;
  lx3_authority_config_t config;
} lx3_authority_t;

static inline void lx3_authority_pending_refresh(lx3_authority_t *s) {
  s->pending_axes = 0U;
  for (unsigned i = 0U; i < 2U; i++) if (s->pending_axis_generation[i] != 0U) s->pending_axes |= 1U << i;
  const unsigned first = s->pending_axis_generation[0] != 0U ? 0U : 1U;
  s->pending_generation = s->pending_axis_generation[first];
  s->pending_key = s->pending_axis_key[first];
  s->pending_us = s->pending_axis_us[first];
}

static inline void lx3_authority_pending_clear(lx3_authority_t *s) {
  for (unsigned i = 0U; i < 2U; i++) s->pending_axis_generation[i] = 0U;
  lx3_authority_pending_refresh(s);
}

static inline uint16_t lx3_authority_next(lx3_authority_t *s) {
  if (s->sequence == UINT16_MAX) {
    s->exhausted = true;
    s->allowed = 0U;
    s->lateral_generation = 0U;
    s->longitudinal_generation = 0U;
    lx3_authority_pending_clear(s);
    s->longitudinal_armed = false;
    s->lateral_armed = false;
    s->reason = LX3_REASON_EXHAUSTED;
    return 0U;
  }
  return ++s->sequence;
}

static inline void lx3_authority_revoke(lx3_authority_t *s, uint8_t axes, uint8_t reason, bool disarm) {
  if ((((s->allowed | s->pending_axes) & axes) != 0U) ||
      (((axes & LX3_LONG) != 0U) && s->longitudinal_armed) || (((axes & LX3_LAT) != 0U) && s->lateral_armed)) {
    const uint16_t revision = lx3_authority_next(s);
    if ((axes & LX3_LAT) != 0U) s->lateral_revision = revision;
    if ((axes & LX3_LONG) != 0U) s->longitudinal_revision = revision;
  }
  s->allowed &= (uint8_t)~axes;
  if ((axes & LX3_LAT) != 0U) { s->lateral_generation = 0U; s->lateral_key = 0U; }
  if ((axes & LX3_LONG) != 0U) { s->longitudinal_generation = 0U; s->longitudinal_key = 0U; }
  if (disarm && ((axes & LX3_LONG) != 0U)) s->longitudinal_armed = false;
  if (disarm && ((axes & LX3_LAT) != 0U)) s->lateral_armed = false;
  // No gesture observed before a revocation can subsequently restore an axis.
  lx3_authority_pending_clear(s);
  s->reason = reason;
}

static inline void lx3_authority_maintain(lx3_authority_t *s, uint32_t now, bool input_healthy) {
  if (!input_healthy || s->exhausted) {
    lx3_authority_revoke(s, LX3_ALL, s->exhausted ? LX3_REASON_EXHAUSTED : LX3_REASON_INPUT, true);
  } else if (s->heartbeat_seen && ((now - s->heartbeat_us) > LX3_HEARTBEAT_TIMEOUT_US)) {
    lx3_authority_revoke(s, LX3_ALL, LX3_REASON_HEARTBEAT, true);
  }
  for (unsigned i = 0U; i < 2U; i++) {
    if ((s->pending_axis_generation[i] != 0U) && ((now - s->pending_axis_us[i]) >= LX3_EVENT_TIMEOUT_US)) s->pending_axis_generation[i] = 0U;
  }
  lx3_authority_pending_refresh(s);
}

static inline void lx3_authority_button(lx3_authority_t *s, const lx3_button_event_t *e,
                                        uint32_t now, bool healthy) {
  if ((e->button == LX3_BUTTON_MAIN) && e->pressed) s->main_press_sequence = s->sequence;
  if ((e->button == LX3_BUTTON_CANCEL) && e->pressed) {
    lx3_authority_revoke(s, s->config.cancel_mode == 1U ? LX3_ALL : LX3_LONG, LX3_REASON_CANCEL, true);
    return;
  }
  if (e->pressed || !healthy || (s->epoch == 0U) || s->exhausted) return;
  uint8_t axes = 0U;
  if ((e->button == LX3_BUTTON_LFA) && (s->config.lfa_mode == 0U)) {
    // Carrot classifies short/long presses using its settings. MCU ISR time and
    // host batch time are different clocks: native validates the event citation,
    // not a second independently timed interpretation of the same gesture.
    axes = LX3_LAT;
  } else if (e->button == LX3_BUTTON_MAIN) {
    // MAIN is debounced for 300 ms. A later, separately released RES/LFA may
    // already have granted permission during that tail; do not undo it.
    uint8_t older = 0U;
    if (s->lateral_generation <= s->main_press_sequence) older |= LX3_LAT;
    if (s->longitudinal_generation <= s->main_press_sequence) older |= LX3_LONG;
    lx3_authority_revoke(s, older, LX3_REASON_CANCEL, true);
    axes = LX3_ALL;
  } else if ((e->button == 1U) || (e->button == 2U)) {
    axes = LX3_LONG | (s->lateral_armed ? LX3_LAT : 0U);
  }
  if (axes != 0U) {
    const uint16_t generation = lx3_authority_next(s);
    if (generation != 0U) {
      for (unsigned i = 0U; i < 2U; i++) {
        if ((axes & (1U << i)) != 0U) {
          s->pending_axis_generation[i] = generation;
          s->pending_axis_key[i] = ((uint16_t)e->button << 8U) | e->counter;
          s->pending_axis_us[i] = now;
        }
      }
      lx3_authority_pending_refresh(s);
    }
  }
}

static inline void lx3_authority_pedal(lx3_authority_t *s, bool brake_edge, bool gas_edge) {
  if (brake_edge || (gas_edge && s->config.disengage_on_gas)) {
    // Lateral-only has no longitudinal session to resume after pedal release.
    if (!s->config.always_lateral && !s->longitudinal_armed) s->lateral_armed = false;
    lx3_authority_revoke(s, s->config.always_lateral ? LX3_LONG : LX3_ALL, LX3_REASON_PEDAL, false);
    s->resume_needs_off = true;
  }
}

// A STATE is accepted only after the transport validates its epoch/binding.
// Continuation never grants; additions require a cited physical event or an
// explicitly armed automatic resume after the host observed the pedal revoke.
static inline bool lx3_authority_heartbeat(lx3_authority_t *s, uint32_t now, uint64_t epoch,
                                           uint8_t intent, uint16_t generation, uint16_t key,
                                           bool auto_resume, bool healthy, bool brake_down, bool gas_down,
                                           uint16_t observed_long_revision) {
  lx3_authority_maintain(s, now, healthy);
  if ((epoch == 0U) || (epoch != s->epoch) || (intent > LX3_ALL)) {
    lx3_authority_revoke(s, LX3_ALL, LX3_REASON_IDENTITY, true);
    return false;
  }
  s->heartbeat_seen = true;
  s->heartbeat_us = now;
  if (!healthy || s->exhausted) return false;

  const uint8_t removed = s->allowed & (uint8_t)~intent;
  // Host-off does not consume a *new* physical event while we wait for the
  // normal selfdrived state transition. Native cancellations invalidate it.
  if ((removed & LX3_LAT) != 0U) { s->allowed &= (uint8_t)~LX3_LAT; s->lateral_generation = 0U; s->lateral_key = 0U; }
  if ((removed & LX3_LONG) != 0U) { s->allowed &= (uint8_t)~LX3_LONG; s->longitudinal_generation = 0U; s->longitudinal_key = 0U; }
  if (removed != 0U) s->reason = LX3_REASON_HOST_OFF;
  if ((removed & LX3_LAT) != 0U) s->lateral_armed = false;
  for (unsigned i = 0U; i < 2U; i++) if ((removed & (1U << i)) != 0U) s->pending_axis_generation[i] = 0U;
  lx3_authority_pending_refresh(s);
  if (((intent & LX3_LONG) == 0U) && (observed_long_revision == s->longitudinal_revision)) s->resume_needs_off = false;
  const uint8_t additions = intent & (uint8_t)~s->allowed;
  bool granted = additions == 0U;
  bool cited = generation != 0U;
  for (unsigned i = 0U; i < 2U; i++) {
    if ((additions & (1U << i)) != 0U) cited = cited && generation == s->pending_axis_generation[i] &&
      key == s->pending_axis_key[i] && (now - s->pending_axis_us[i]) < LX3_EVENT_TIMEOUT_US;
  }
  const bool pedals_clear = !brake_down && (!gas_down || !s->config.disengage_on_gas);
  if ((additions != 0U) && cited &&
      (((additions & LX3_LONG) == 0U) || pedals_clear) &&
      (((additions & LX3_LAT) == 0U) || s->config.always_lateral || pedals_clear)) {
    if ((additions & LX3_LAT) != 0U) { s->lateral_generation = generation; s->lateral_key = key; s->lateral_armed = true; }
    if ((additions & LX3_LONG) != 0U) {
      s->longitudinal_generation = generation;
      s->longitudinal_key = key;
      s->longitudinal_armed = true;
      s->resume_needs_off = false;
    }
    s->allowed |= additions;
    for (unsigned i = 0U; i < 2U; i++) if ((additions & (1U << i)) != 0U) s->pending_axis_generation[i] = 0U;
    lx3_authority_pending_refresh(s);
    granted = true;
  } else if (((additions == LX3_LONG) || ((additions == LX3_ALL) && s->lateral_armed)) && auto_resume && s->config.auto_resume &&
             s->longitudinal_armed && !s->resume_needs_off && ((s->last_intent & LX3_LONG) == 0U) &&
             !brake_down && (!gas_down || !s->config.disengage_on_gas)) {
    const uint16_t next = lx3_authority_next(s);
    if (next != 0U) {
      s->longitudinal_generation = next;
      s->longitudinal_key = 0U;  // Explicit auto-resume; never labelled a button.
      s->allowed |= LX3_LONG;
      if ((additions & LX3_LAT) != 0U) {
        s->lateral_generation = next;
        s->lateral_key = 0U;
        s->allowed |= LX3_LAT;
      }
      granted = true;
    }
  }
  s->last_intent = intent;
  if (granted && (s->allowed != 0U)) s->reason = LX3_REASON_NONE;
  return granted;
}

static inline bool lx3_authority_identity(const lx3_authority_t *s, uint64_t epoch, uint8_t axis,
                                          uint16_t generation) {
  if ((epoch == 0U) || (epoch != s->epoch) || (generation == 0U) || ((s->allowed & axis) == 0U)) return false;
  return ((axis == LX3_LAT) && (generation == s->lateral_generation)) ||
         ((axis == LX3_LONG) && (generation == s->longitudinal_generation));
}
