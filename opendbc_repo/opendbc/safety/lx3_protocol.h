#pragma once
#include <stdbool.h>
#include <stdint.h>

#define LX3_PROTOCOL_VERSION 3U
#define LX3_STATUS_SIZE 50U
#define LX3_STATUS_REQUEST 0xC7U
#define LX3_EPOCH_HIGH_REQUEST 0xC8U
#define LX3_EPOCH_LOW_REQUEST 0xC9U
#define LX3_BINDING_HIGH_REQUEST 0xCAU
#define LX3_BINDING_LOW_REQUEST 0xCBU
#define LX3_SEQUENCE_REQUEST 0xCCU
#define LX3_CONFIG_REQUEST 0xCDU
#define LX3_REVOKE_ACK_REQUEST 0xCEU
#define LX3_STATE_REQUEST 0xF4U

// Additive USB/SPI companion. Universal health v16 and CAN packet v4 stay intact.
typedef struct __attribute__((packed)) {
  uint8_t version;
  uint8_t profile;  // 0 capability only, 1 supported LX3, 2 rejected combination
  uint8_t allowed;
  uint8_t armed;
  uint16_t reason;
  uint16_t pending_key;
  uint16_t pending_generation;
  uint16_t lateral_generation;
  uint16_t longitudinal_generation;
  uint16_t pending_age_ms;
  uint64_t epoch;
  uint32_t sequence;
  uint16_t config;
  uint16_t lfa_long_ms;
  uint32_t oem_lateral_passthrough;
  uint32_t oem_longitudinal_passthrough;
  uint16_t heartbeat_age_ms;
  uint16_t input_ready;
  uint16_t lateral_revision;
  uint16_t longitudinal_revision;
  uint16_t oem_emergency;
} lx3_status_t;

static inline bool lx3_status_valid(const lx3_status_t *s, int bytes) {
  return (s != 0) && (bytes == (int)sizeof(*s)) && (s->version == LX3_PROTOCOL_VERSION) &&
    (s->profile <= 2U) && (s->allowed <= 3U) && (s->armed <= 1U) && (s->input_ready <= 1U) && (s->oem_emergency <= 1U) &&
    (((s->allowed & 1U) == 0U) == (s->lateral_generation == 0U)) &&
    (((s->allowed & 2U) == 0U) == (s->longitudinal_generation == 0U));
}

// Detect accidental cross-session/partial-transfer mix-ups, not authentication
// against a malicious host. A monotonically increasing sequence rejects replay.
static inline uint64_t lx3_state_binding(uint64_t epoch, uint32_t sequence, uint16_t value, uint16_t generation,
                                         uint16_t config, uint16_t long_ms, uint16_t revision) {
  uint64_t mix = epoch ^ ((uint64_t)value << 48U) ^ ((uint64_t)generation << 32U) ^ sequence;
  mix = (mix << 17U) | (mix >> 47U);
  return mix ^ ((uint64_t)config << 32U) ^ ((uint64_t)long_ms << 16U) ^ revision;
}

#define LX3_TX_PREFIX_ADDR 0x1FFFFFFFU
#define LX3_TX_EPOCH_ADDR 0x1FFFFFFEU
#define LX3_TX_MARKER_BUS 7U
typedef struct {
  uint64_t epoch;
  uint16_t generation;
  uint8_t axis;
  bool valid;
} lx3_tx_identity_t;

static inline bool lx3_tx_guarded_address(uint32_t address) {
  return (address == 0xCBU) || (address == 0x1A0U) || (address == 0x12AU);
}

static inline uint16_t lx3_tx_crc(uint16_t crc, uint8_t byte) {
  crc ^= (uint16_t)byte << 8U;
  for (unsigned bit = 0U; bit < 8U; bit++) crc = (uint16_t)((crc << 1U) ^ ((crc & 0x8000U) != 0U ? 0x1021U : 0U));
  return crc;
}

static inline uint16_t lx3_tx_binding(const uint8_t prefix[8], const uint8_t epoch[8], const uint8_t *packet, uint32_t size) {
  uint16_t crc = 0xFFFFU;
  for (unsigned i = 0U; i < 6U; i++) crc = lx3_tx_crc(crc, prefix[i]);
  for (unsigned i = 0U; i < 8U; i++) crc = lx3_tx_crc(crc, epoch[i]);
  for (uint32_t i = 0U; i < size; i++) crc = lx3_tx_crc(crc, packet[i]);
  return crc;
}

static inline void lx3_tx_encode(const lx3_tx_identity_t *id, const uint8_t *packet, uint32_t size,
                                 uint8_t prefix[8], uint8_t epoch[8]) {
  prefix[0] = 0x4CU; prefix[1] = 0x33U;
  prefix[2] = id->axis; prefix[3] = 0U;
  prefix[4] = (uint8_t)id->generation; prefix[5] = (uint8_t)(id->generation >> 8U);
  for (unsigned i = 0U; i < 8U; i++) epoch[i] = (uint8_t)(id->epoch >> (8U * i));
  const uint16_t crc = lx3_tx_binding(prefix, epoch, packet, size);
  prefix[6] = (uint8_t)crc; prefix[7] = (uint8_t)(crc >> 8U);
}

static inline lx3_tx_identity_t lx3_tx_decode(const uint8_t prefix[8], const uint8_t epoch[8]) {
  lx3_tx_identity_t id = {0};
  id.valid = (prefix[0] == 0x4CU) && (prefix[1] == 0x33U) && (prefix[3] == 0U);
  id.axis = prefix[2];
  id.generation = (uint16_t)prefix[4] | ((uint16_t)prefix[5] << 8U);
  for (unsigned i = 0U; i < 8U; i++) id.epoch |= (uint64_t)epoch[i] << (8U * i);
  return id;
}

// Firmware-internal queue stamp, stored alongside (not inside) CANPacket_t.
// A software queue entry from an old grant cannot become current after re-arm.
typedef struct {
  uint64_t epoch;
  uint32_t admitted_us;
  uint16_t generation;
  uint8_t axis;
  uint8_t origin;  // 0 legacy/OEM, 1 host replacement (including inactive)
} lx3_queue_stamp_t;
