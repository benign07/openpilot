#pragma once

#include <stdint.h>

// Optional, read-only transport evidence. Existing health/CAN/authority ABI stays unchanged.
#define SPI_DIAGNOSTICS_REQUEST 0xb3U
#define SPI_DIAGNOSTICS_VERSION 1U
#define SPI_DIAGNOSTICS_SIZE 64U
#define SPI_DIAGNOSTICS_STATE_UNKNOWN 0xffU

typedef struct {
  uint8_t version;
  uint8_t size;
  uint8_t last_unexpected_rx_state;
  uint8_t last_unexpected_tx_state;
  uint16_t legacy_checksum_errors;
  uint16_t reserved2;
  uint32_t header_ok;
  uint32_t data_ok;
  uint32_t header_checksum_bad;
  uint32_t header_sync_bad;
  uint32_t data_checksum_bad;
  uint32_t unexpected_rx_state;
  uint32_t unexpected_tx_state;
  uint32_t can_backpressure_nack;
  uint32_t other_nack;
  uint32_t version_requests;
  uint32_t bad_length;
  uint32_t header_fill_11;
  uint32_t header_fill_13;
  uint32_t header_fill_14;
} spi_diagnostics_t;

static inline void spi_diagnostics_increment(uint32_t *value) {
  if (*value < UINT32_MAX) *value += 1U;
}
