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
#include <functional>
#include "linux/spi/spidev.h"

enum class Scenario { GOOD, OVERSIZED, ABOVE_REQUEST, RECOVERY_STUCK, DATA_BACKPRESSURE, HEADER_NACK, BAD_CHECKSUM,
                      HEADER_STUCK, CONTROL_DATA_STUCK, CAN_HEADER_STUCK, MIXED_NACK, DISCONNECT_BACKPRESSURE };
static Scenario scenario = Scenario::GOOD;
static double fake_ms = 0;
static unsigned calls = 0;
static bool excessive_read = false;
static unsigned response_length = 12;
static unsigned request_max = 12;
static unsigned flow_nacks = 0;
static unsigned header_nacks = 0;
static std::function<void()> on_transfer;
static constexpr unsigned CALL_LIMIT = 20000;
static constexpr unsigned BACKPRESSURE_NACKS = 100;
namespace util {
int safe_ioctl(int, unsigned long request, void *argument, const char *) {
  if (request != SPI_IOC_MESSAGE(1)) return 0;
  auto *transfer = static_cast<spi_ioc_transfer *>(argument);
  auto *tx = reinterpret_cast<uint8_t *>(transfer->tx_buf);
  auto *rx = reinterpret_cast<uint8_t *>(transfer->rx_buf);
  // Inert transfer time, not a claim about real device timing. Advancing the
  // separate usleep stub also tests charged backoff, which the old fixture hid.
  fake_ms += 0.2;
  if (++calls > CALL_LIMIT) throw std::runtime_error("unbounded SPI recovery/retry");
  if (on_transfer) on_transfer();
  if (tx[0] == 0x5A && transfer->len == 7) {
    request_max = uint16_t(tx[4]) | uint16_t(tx[5]) << 8;
    response_length = request_max;
    if (scenario == Scenario::RECOVERY_STUCK) return -1;
  } else if (tx[0] == 0x11 && transfer->len == 1) {
    const bool stuck = scenario == Scenario::HEADER_STUCK || scenario == Scenario::CAN_HEADER_STUCK;
    const bool short_header = scenario == Scenario::HEADER_NACK && header_nacks++ < 4U;
    const bool mixed_header = scenario == Scenario::MIXED_NACK && flow_nacks >= BACKPRESSURE_NACKS && header_nacks++ < 2U;
    rx[0] = stuck || short_header || mixed_header ? 0x1F : 0x79;
  } else if (tx[0] == 0x13 && transfer->len == 3) {
    if (scenario == Scenario::CONTROL_DATA_STUCK || scenario == Scenario::DISCONNECT_BACKPRESSURE ||
        ((scenario == Scenario::DATA_BACKPRESSURE || scenario == Scenario::MIXED_NACK) && flow_nacks++ < BACKPRESSURE_NACKS)) {
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
               test == "recovery" ? Scenario::RECOVERY_STUCK : test == "nack" ? Scenario::DATA_BACKPRESSURE :
               test == "header_nack" ? Scenario::HEADER_NACK :
               test == "header_stuck" ? Scenario::HEADER_STUCK :
               test == "control_data_stuck" ? Scenario::CONTROL_DATA_STUCK :
               test == "can_header_stuck" ? Scenario::CAN_HEADER_STUCK :
               test == "mixed_nack" ? Scenario::MIXED_NACK :
               test == "disconnect_backpressure" ? Scenario::DISCONNECT_BACKPRESSURE :
               test == "checksum" ? Scenario::BAD_CHECKSUM : Scenario::GOOD;
    if (scenario == Scenario::DISCONNECT_BACKPRESSURE) on_transfer = [&panda]() { if (calls >= 80U) panda.connected = false; };
    unsigned char buffer[16]; std::memset(buffer, 0xCC, sizeof(buffer));
    const bool can_write = scenario == Scenario::DATA_BACKPRESSURE || scenario == Scenario::CAN_HEADER_STUCK ||
                           scenario == Scenario::MIXED_NACK || scenario == Scenario::DISCONNECT_BACKPRESSURE;
    const unsigned timeout = 1U;
    int result = can_write ? panda.bulk_write(3, buffer, sizeof(buffer), timeout) :
                            panda.control_read(0x42, 0, 0, buffer, sizeof(buffer), timeout);
    if (scenario == Scenario::DATA_BACKPRESSURE || scenario == Scenario::MIXED_NACK) {
      assert(result == 0 && panda.comms_healthy && flow_nacks >= BACKPRESSURE_NACKS && fake_ms > 120);
    } else if (scenario == Scenario::GOOD || scenario == Scenario::HEADER_NACK) {
      assert(result == 16 && buffer[0] == 0);
    } else {
      assert(result < 0 && !excessive_read && calls < CALL_LIMIT);
      assert(std::all_of(std::begin(buffer), std::end(buffer), [](uint8_t b) { return b == 0xCC; }));
      assert(panda.comms_healthy == !can_write);
      if (scenario == Scenario::HEADER_STUCK || scenario == Scenario::CONTROL_DATA_STUCK || scenario == Scenario::CAN_HEADER_STUCK) {
        assert(result == SpiError::NACK);  // Private header classification never escapes.
        assert(fake_ms >= 120 && fake_ms < 125);  // Includes NACK backoff sleep.
      }
      on_transfer = {}; panda.connected = true; scenario = Scenario::GOOD; calls = 0;
      assert(panda.control_read(0x42, 0, 0, buffer, sizeof(buffer), 1) == 16);
    }
#ifdef EXPECT_SPI_DIAGNOSTICS
    if (test == "oversized" || test == "request") assert(spi_diagnostics.get(SpiStage::BAD_LENGTH) > 0);
    if (test == "recovery") assert(spi_diagnostics.get(SpiStage::RECOVERY_FAILED) > 0);
    if (test == "nack") assert(spi_diagnostics.get(SpiStage::DATA_NACK) >= BACKPRESSURE_NACKS);
    if (test == "header_nack") assert(spi_diagnostics.get(SpiStage::HEADER_NACK) >= 4U);
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
