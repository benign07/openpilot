#pragma once

#include <stdint.h>
#include <stdbool.h>

// Additive companion to health v16. No legacy health layout/version changes.
#define LX3_PERMISSION_VERSION 1U
#define LX3_PERMISSION_REQUEST 0xC7U
#define LX3_HEARTBEAT_TAG 0x80U
#define LX3_REQUEST_TIMEOUT_US 500000U

typedef struct __attribute__((packed)) {
  uint8_t version;
  uint8_t requested_mode;
  uint8_t accepted_mode;
  uint8_t physical_counter;
  uint16_t generation;
  uint16_t age_ms;
  uint8_t controls_allowed;
  uint8_t phase;  // 0 idle, 1 pending, 2 accepted
  uint16_t reserved;
} lx3_permission_t;

// Shared decoder checks the length before inspecting fields. Old firmware,
// partial reads and unknown future versions default to no guarded permission.
static inline bool lx3_permission_valid(const lx3_permission_t *state, int bytes) {
  if ((bytes != (int)sizeof(*state)) || (state == 0)) return false;
  if ((state->version != LX3_PERMISSION_VERSION) || (state->reserved != 0U) ||
      (state->generation == 0U) || (state->requested_mode > 2U) || (state->accepted_mode > 2U) ||
      (state->phase > 2U) || (state->controls_allowed > 1U)) return false;
  const bool idle = (state->phase == 0U) && (state->requested_mode == 0U) &&
                    (state->accepted_mode == 0U) && (state->controls_allowed == 0U);
  const bool pending = (state->phase == 1U) && (state->requested_mode != 0U) &&
                       (state->accepted_mode == 0U) && (state->controls_allowed == 0U) &&
                       (state->age_ms < (LX3_REQUEST_TIMEOUT_US / 1000U));
  const bool accepted = (state->phase == 2U) && (state->accepted_mode != 0U) &&
                        (state->requested_mode == state->accepted_mode) && (state->controls_allowed == 1U);
  return idle || pending || accepted;
}

static inline uint16_t lx3_heartbeat_value(bool enabled, uint8_t mode, uint16_t generation, uint8_t counter) {
  uint16_t value = LX3_HEARTBEAT_TAG | (enabled ? 1U : 0U);
  if ((mode <= 2U) && (generation != 0U)) {
    value |= (uint16_t)mode << 1U;
    value |= (uint16_t)counter << 8U;
  }
  return value;
}
