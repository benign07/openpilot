#pragma once
extern double spi_mcu_fixture_time_ms;
inline double millis_since_boot() { return spi_mcu_fixture_time_ms; }
