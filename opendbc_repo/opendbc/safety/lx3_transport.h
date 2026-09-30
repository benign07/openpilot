#pragma once
#include "lx3_permission.h"

// Two USB/SPI-only classic-sized packets precede ONE vehicle CAN packet.
// Bus 7 cannot reach a physical CAN bus. Vehicle payload/header ABI stays v4.
#define LX3_TX_PREFIX_ADDR 0x1FFFFFFFU
#define LX3_TX_EPOCH_ADDR 0x1FFFFFFEU
#define LX3_TX_MARKER_BUS 7U

typedef struct {
  uint64_t epoch;
  uint16_t generation;
  uint8_t counter;
  uint8_t mode;
  bool valid;
} lx3_tx_identity_t;

static inline bool lx3_tx_guarded_address(uint32_t addr) {
  return (addr == 0xCBU) || (addr == 0x12AU) || (addr == 0x1A0U) ||
         (addr == 0x161U) || (addr == 0x162U) || (addr == 0x1E0U) ||
         (addr == 0x1EAU) || (addr == 0x200U) || (addr == 0x362U) || (addr == 0x2A4U);
}

static inline bool lx3_tx_identity_valid(const lx3_tx_identity_t *id) {
  return id->valid && (id->epoch != 0U) && (id->generation != 0U) &&
         ((id->mode == 1U) || (id->mode == 2U));
}

static inline uint16_t lx3_tx_crc_step(uint16_t crc, uint8_t byte) {
  crc ^= (uint16_t)byte << 8U;
  for (unsigned int bit = 0U; bit < 8U; bit++) {
    crc = (uint16_t)((crc << 1U) ^ (((crc & 0x8000U) != 0U) ? 0x1021U : 0U));
  }
  return crc;
}

// Detect accidental pair/fragment mix-up, not malicious-host authentication.
static inline uint16_t lx3_tx_binding(const uint8_t prefix[8], const uint8_t epoch[8],
                                    const uint8_t *packet, uint32_t length) {
  uint16_t crc = 0xFFFFU;
  for (unsigned int i = 0U; i < 6U; i++) crc = lx3_tx_crc_step(crc, prefix[i]);
  for (unsigned int i = 0U; i < 8U; i++) crc = lx3_tx_crc_step(crc, epoch[i]);
  for (uint32_t i = 0U; i < length; i++) crc = lx3_tx_crc_step(crc, packet[i]);
  return crc;
}

static inline void lx3_tx_encode(const lx3_tx_identity_t *id, const uint8_t *packet, uint32_t length,
                                 uint8_t prefix[8], uint8_t epoch[8]) {
  prefix[0] = 0x4CU; prefix[1] = 0x32U;  // L2: companion/transport v2
  prefix[2] = id->mode; prefix[3] = id->counter;
  prefix[4] = (uint8_t)id->generation; prefix[5] = (uint8_t)(id->generation >> 8U);
  for (unsigned int i = 0U; i < 8U; i++) epoch[i] = (uint8_t)(id->epoch >> (i * 8U));
  const uint16_t crc = lx3_tx_binding(prefix, epoch, packet, length);
  prefix[6] = (uint8_t)crc; prefix[7] = (uint8_t)(crc >> 8U);
}

static inline lx3_tx_identity_t lx3_tx_decode(const uint8_t prefix[8], const uint8_t epoch[8]) {
  lx3_tx_identity_t id = {0};
  id.valid = (prefix[0] == 0x4CU) && (prefix[1] == 0x32U);
  id.mode = prefix[2]; id.counter = prefix[3];
  id.generation = (uint16_t)prefix[4] | ((uint16_t)prefix[5] << 8U);
  for (unsigned int i = 0U; i < 8U; i++) id.epoch |= (uint64_t)epoch[i] << (i * 8U);
  return id;
}

static inline bool lx3_tx_matches(const lx3_tx_identity_t *id, const lx3_permission_t *state) {
  return lx3_tx_identity_valid(id) && lx3_permission_valid(state, sizeof(*state)) &&
         (state->phase == 2U) && (state->controls_allowed == 1U) &&
         (id->epoch == state->transport_epoch) && (id->generation == state->generation) &&
         (id->counter == state->physical_counter) && (id->mode == state->accepted_mode);
}
