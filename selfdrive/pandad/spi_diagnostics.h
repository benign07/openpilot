#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>

// Host transport evidence only. No CAN authority, wire layout or MCU counters.
enum class SpiStage : unsigned {
  ATTEMPTS, RETRIES, HEADER_IO, HEADER_NACK, HEADER_TIMEOUT,
  DATA_IO, DATA_NACK, DATA_TIMEOUT, READ_IO, BAD_LENGTH, BAD_CHECKSUM,
  RECOVERY_FAILED, TRANSFER_FAILED, SUCCESS, COUNT,
};

class SpiDiagnostics {
public:
  static constexpr unsigned SIZE = static_cast<unsigned>(SpiStage::COUNT);
  void count(SpiStage stage) { counts[static_cast<unsigned>(stage)].fetch_add(1, std::memory_order_relaxed); }
  uint64_t get(SpiStage stage) const { return counts[static_cast<unsigned>(stage)].load(std::memory_order_relaxed); }
  bool report_due(uint64_t now_ms) {
    uint64_t previous = last_report_ms.load(std::memory_order_relaxed);
    return now_ms >= previous && now_ms - previous >= 5000U &&
           last_report_ms.compare_exchange_strong(previous, now_ms, std::memory_order_relaxed);
  }
private:
  std::array<std::atomic<uint64_t>, SIZE> counts{};
  std::atomic<uint64_t> last_report_ms{0};
};

inline unsigned spi_retry_budget_ms(unsigned timeout) {
  // Keep the existing ACK timeout range and six-timeout retry envelope, but
  // bound non-timeout I/O/checksum/NACK failures as well. A blocking kernel
  // ioctl may still overrun the userspace deadline.
  return 6U * std::clamp(timeout == 0U ? 500U : timeout, 20U, 500U);
}

inline bool spi_within_budget(double started_ms, double now_ms, unsigned budget_ms, bool connected) {
  return connected && now_ms >= started_ms && now_ms - started_ms < budget_ms;
}
