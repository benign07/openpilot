#pragma once
// Used inside the board's control-transfer critical section. None of these
// messages are sent to the vehicle. A partial STATE is never applied.
static uint64_t lx3_epoch_staged;
static bool lx3_epoch_high_seen;
static uint64_t lx3_binding_staged;
static uint32_t lx3_sequence_staged;
static uint32_t lx3_sequence_accepted;
static uint16_t lx3_config_staged;
static uint16_t lx3_long_ms_staged;
static uint16_t lx3_config_accepted;
static uint16_t lx3_long_ms_accepted;
static uint16_t lx3_revision_staged;
static uint8_t lx3_stage_parts;
static uint32_t lx3_stage_us;

static inline void lx3_protocol_reset(void) {
  lx3_epoch_staged = 0U;
  lx3_epoch_high_seen = false;
  lx3_binding_staged = 0U;
  lx3_sequence_staged = 0U;
  lx3_sequence_accepted = 0U;
  lx3_config_staged = 0U;
  lx3_long_ms_staged = 0U;
  lx3_config_accepted = 0U;
  lx3_long_ms_accepted = 0U;
  lx3_revision_staged = 0U;
  lx3_stage_parts = 0U;
  lx3_stage_us = 0U;
}

static inline lx3_status_t lx3_native_status(void) {
  lx3_status_t s = {.version=LX3_PROTOCOL_VERSION};
  if (!lx3_native_active()) return s;
  lx3_native_maintain();
  s.profile = lx3_profile_supported ? 1U : 2U;
  s.allowed = lx3_auth.allowed;
  s.armed = lx3_auth.longitudinal_armed;
  s.reason = lx3_auth.reason;
  s.pending_key = lx3_auth.pending_key;
  s.pending_generation = lx3_auth.pending_axes != 0U ? lx3_auth.pending_generation : 0U;
  s.pending_age_ms = MIN((microsecond_timer_get() - lx3_auth.pending_us) / 1000U, UINT16_MAX);
  s.lateral_generation = lx3_auth.lateral_generation;
  s.longitudinal_generation = lx3_auth.longitudinal_generation;
  s.epoch = lx3_auth.epoch;
  s.sequence = lx3_sequence_accepted;
  s.config = lx3_config_accepted;
  s.lfa_long_ms = lx3_long_ms_accepted;
  s.oem_lateral_passthrough = lx3_oem_lat_count;
  s.oem_longitudinal_passthrough = lx3_oem_long_count;
  s.heartbeat_age_ms = lx3_auth.heartbeat_seen ? MIN((microsecond_timer_get() - lx3_auth.heartbeat_us) / 1000U, UINT16_MAX) : UINT16_MAX;
  s.input_ready = lx3_native_healthy();
  s.lateral_revision = lx3_auth.lateral_revision;
  s.longitudinal_revision = lx3_auth.longitudinal_revision;
  s.oem_emergency = lx3_oem_emergency;
  return s;
}

static inline void lx3_native_control(uint8_t request, uint16_t value, uint16_t index) {
  if (!lx3_native_active() || !lx3_profile_supported) return;
  const uint32_t now = microsecond_timer_get();
  if (request == LX3_EPOCH_HIGH_REQUEST) {
    lx3_epoch_high_seen = lx3_auth.epoch == 0U;
    lx3_epoch_staged = ((uint64_t)value << 48U) | ((uint64_t)index << 32U);
    return;
  }
  if (request == LX3_EPOCH_LOW_REQUEST) {
    if (lx3_epoch_high_seen && (lx3_auth.epoch == 0U)) {
      lx3_auth.epoch = lx3_epoch_staged | ((uint32_t)value << 16U) | index;
    }
    lx3_epoch_high_seen = false;
    return;
  }
  if (request == LX3_CONFIG_REQUEST) {
    // Start ONE transaction. Each later field is mandatory and consumed once.
    lx3_stage_parts = 1U;
    lx3_stage_us = now;
    lx3_config_staged = value;
    lx3_long_ms_staged = index;
  } else if (request == LX3_SEQUENCE_REQUEST) {
    lx3_sequence_staged = ((uint32_t)value << 16U) | index;
    lx3_stage_parts |= 2U;
  } else if (request == LX3_REVOKE_ACK_REQUEST) {
    lx3_revision_staged = value;
    lx3_stage_parts |= index == 0U ? 4U : 0U;
  } else if (request == LX3_BINDING_HIGH_REQUEST) {
    lx3_binding_staged = ((uint64_t)value << 48U) | ((uint64_t)index << 32U);
    lx3_stage_parts |= 8U;
  } else if (request == LX3_BINDING_LOW_REQUEST) {
    lx3_binding_staged |= ((uint32_t)value << 16U) | index;
    lx3_stage_parts |= 16U;
  } else if (request == LX3_STATE_REQUEST) {
    if ((lx3_stage_parts != 31U) || ((now - lx3_stage_us) >= 100000U) || (lx3_sequence_staged <= lx3_sequence_accepted)) {
      lx3_stage_parts = 0U;
      lx3_native_maintain(); // Missing/replayed stages cannot refresh the lease.
      return;
    }
    const bool valid = (lx3_long_ms_staged >= 100U) &&
      (lx3_long_ms_staged <= 10000U) && ((lx3_config_staged & 0xFFC0U) == 0U) &&
      ((value & 0x0008U) == 0U) &&
      (lx3_binding_staged == lx3_state_binding(lx3_auth.epoch, lx3_sequence_staged, value, index,
                                              lx3_config_staged, lx3_long_ms_staged, lx3_revision_staged));
    lx3_stage_parts = 0U;
    if (!valid) {
      lx3_authority_revoke(&lx3_auth, LX3_ALL, LX3_REASON_IDENTITY, true);
      lx3_native_purge(LX3_ALL);
      lx3_native_maintain();
      return;
    }
    lx3_sequence_accepted = lx3_sequence_staged;
    if ((value & 0x80U) != 0U) {
      lx3_authority_revoke(&lx3_auth, LX3_ALL, LX3_REASON_HEARTBEAT, true);
      lx3_native_purge(LX3_ALL);
      lx3_native_maintain();
      return;
    }
    lx3_config_accepted = lx3_config_staged;
    lx3_long_ms_accepted = lx3_long_ms_staged;
    lx3_auth.config = (lx3_authority_config_t){
      .always_lateral=(lx3_config_staged & 1U) != 0U,
      .disengage_on_gas=(lx3_config_staged & 2U) != 0U,
      .auto_resume=(lx3_config_staged & 4U) != 0U,
      .lfa_mode=(lx3_config_staged >> 3U) & 3U,
      .cancel_mode=(lx3_config_staged >> 5U) & 1U,
      .lfa_long_us=lx3_long_ms_staged * 1000U,
    };
    const uint8_t kind = (value >> 4U) & 7U;
    const uint8_t button = kind == 1U ? 128U : kind == 2U ? 8U : kind == 3U ? 1U : kind == 4U ? 2U : 0U;
    const uint16_t key = ((uint16_t)button << 8U) | (value >> 8U);
    const uint8_t previous = lx3_auth.allowed;
    (void)lx3_authority_heartbeat(&lx3_auth, now, lx3_auth.epoch, value & 3U, index, key,
      (value & 4U) != 0U, lx3_native_healthy(), brake_pressed, gas_pressed, lx3_revision_staged);
    if (previous != lx3_auth.allowed) lx3_native_purge(previous & (uint8_t)~lx3_auth.allowed);
    lx3_native_maintain();
  }
}
