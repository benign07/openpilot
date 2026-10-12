#pragma once
#include <cstdint>
extern double spi_mcu_fixture_time_ms;
inline double millis_since_boot() { return spi_mcu_fixture_time_ms; }
inline uint64_t nanos_since_boot() { return static_cast<uint64_t>(spi_mcu_fixture_time_ms * 1000000.0); }
