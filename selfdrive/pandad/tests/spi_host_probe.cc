// Compile the actual SPI driver with inert I/O, never connect to a Panda.
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cmath>
#include <cstring>
#include <iomanip>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <cstdio>
#include "linux/spi/spidev.h"

enum class Scenario { GOOD, OVERSIZED, ABOVE_REQUEST, RECOVERY_STUCK, HEADER_NACK, BAD_CHECKSUM };
static Scenario scenario = Scenario::GOOD;
static double fake_ms = 0;
static unsigned calls = 0;
static bool excessive_read = false;
static unsigned response_length = 12;
static unsigned request_max = 12;
static unsigned flow_nacks = 0;
namespace util {
int safe_ioctl(int, unsigned long request, void *argument, const char *) {
  if (request != SPI_IOC_MESSAGE(1)) return 0;
  auto *transfer = static_cast<spi_ioc_transfer *>(argument);
  auto *tx = reinterpret_cast<uint8_t *>(transfer->tx_buf);
  auto *rx = reinterpret_cast<uint8_t *>(transfer->rx_buf);
  fake_ms += 10;
  if (++calls > 500) throw std::runtime_error("unbounded SPI recovery/retry");
  if (tx[0] == 0x5A && transfer->len == 7) {
    request_max = uint16_t(tx[4]) | uint16_t(tx[5]) << 8;
    response_length = request_max;
    if (scenario == Scenario::RECOVERY_STUCK) return -1;
  } else if (tx[0] == 0x11 && transfer->len == 1) {
    rx[0] = 0x79;
  } else if (tx[0] == 0x13 && transfer->len == 3) {
    if (scenario == Scenario::HEADER_NACK && flow_nacks++ < 12U) {
      rx[0] = 0x1F;
      return static_cast<int>(transfer->len);
    }
    response_length = scenario == Scenario::OVERSIZED ? 2047 : scenario == Scenario::ABOVE_REQUEST ? request_max + 1 : request_max;
    rx[0] = 0x85; rx[1] = response_length & 255; rx[2] = response_length >> 8;
  } else if (tx[0] == 0x14) {
    rx[0] = scenario == Scenario::RECOVERY_STUCK ? 0 : 0x1F;
  } else if (transfer->len == response_length + 1) {
    // Detect the old driver's out-of-bounds request without actually corrupting
    // the fixture's memory. The production code must reject before this call.
    if (response_length > 2044 || response_length > request_max) {
      excessive_read = true;
      return -1;
    }
    std::memset(rx, 0, transfer->len);
    rx[response_length] = 0xAB ^ 0x85 ^ (response_length & 255) ^ (response_length >> 8);
    if (scenario == Scenario::BAD_CHECKSUM) rx[response_length] ^= 1;
  }
  return static_cast<int>(transfer->len);
}
}

#include SPI_PRODUCTION_SOURCE

int main(int argc, char **argv) {
  try {
    PandaSpiHandle panda("");  // Stubbed UID read, no device open or transfer.
    calls = 0; fake_ms = 0; excessive_read = false;
    std::string test = argc > 1 ? argv[1] : "good";
    scenario = test == "oversized" ? Scenario::OVERSIZED : test == "request" ? Scenario::ABOVE_REQUEST :
               test == "recovery" ? Scenario::RECOVERY_STUCK : test == "nack" ? Scenario::HEADER_NACK :
               test == "checksum" ? Scenario::BAD_CHECKSUM : Scenario::GOOD;
    unsigned char buffer[16]; std::memset(buffer, 0xCC, sizeof(buffer));
    int result = scenario == Scenario::HEADER_NACK ? panda.bulk_write(3, buffer, sizeof(buffer), 1) :
                 panda.control_read(0x42, 0, 0, buffer, sizeof(buffer), 1);
    if (scenario == Scenario::HEADER_NACK) {
      assert(result == 0 && panda.comms_healthy && flow_nacks >= 12U && fake_ms > 120);
    } else if (scenario == Scenario::GOOD) {
      assert(result == 16 && buffer[0] == 0);
    } else {
      assert(result < 0 && !excessive_read && calls < 500);
      assert(std::all_of(std::begin(buffer), std::end(buffer), [](uint8_t b) { return b == 0xCC; }));
      scenario = Scenario::GOOD; calls = 0;
      assert(panda.control_read(0x42, 0, 0, buffer, sizeof(buffer), 1) == 16);
    }
#ifdef EXPECT_SPI_DIAGNOSTICS
    if (test == "oversized" || test == "request") assert(spi_diagnostics.get(SpiStage::BAD_LENGTH) > 0);
    if (test == "recovery") assert(spi_diagnostics.get(SpiStage::RECOVERY_FAILED) > 0);
    if (test == "nack") assert(spi_diagnostics.get(SpiStage::DATA_NACK) >= 12U);
    if (test == "checksum") assert(spi_diagnostics.get(SpiStage::BAD_CHECKSUM) > 0);
    SpiDiagnostics report;
    assert(!report.report_due(4999) && report.report_due(5000));
    assert(!report.report_due(5001) && report.report_due(10000));
#endif
    std::printf("PASS actual spi.cc %s: returned, preserved output on failure, recovered on valid next read\n", test.c_str());
    return 0;
  } catch (const std::exception &error) {
    std::printf("FAIL actual spi.cc: %s\n", error.what());
    return 42;
  }
}
