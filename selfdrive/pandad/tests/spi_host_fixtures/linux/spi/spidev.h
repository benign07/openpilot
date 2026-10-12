#pragma once
#include <cstdint>
struct spi_ioc_transfer {uint64_t tx_buf=0,rx_buf=0;uint32_t len=0;};
#define SPI_MODE_0 0
#define SPI_IOC_WR_MODE 1
#define SPI_IOC_WR_MAX_SPEED_HZ 2
#define SPI_IOC_WR_BITS_PER_WORD 3
#define SPI_IOC_MESSAGE(n) 4
