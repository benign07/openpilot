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
  void observe_call(uint64_t attempts, uint64_t charged_us, unsigned budget_ms) {
    update_max(max_call_attempts, attempts);
    update_max(max_charged_us, charged_us);
    update_max(max_budget_permille, charged_us / budget_ms);
  }
  uint64_t call_attempts_max() const { return max_call_attempts.load(std::memory_order_relaxed); }
  uint64_t charged_us_max() const { return max_charged_us.load(std::memory_order_relaxed); }
  uint64_t budget_permille_max() const { return max_budget_permille.load(std::memory_order_relaxed); }
  uint64_t take_call_attempts_max() { return max_call_attempts.exchange(0, std::memory_order_relaxed); }
  uint64_t take_charged_us_max() { return max_charged_us.exchange(0, std::memory_order_relaxed); }
  uint64_t take_budget_permille_max() { return max_budget_permille.exchange(0, std::memory_order_relaxed); }
  bool report_due(uint64_t now_ms) {
    uint64_t previous = last_report_ms.load(std::memory_order_relaxed);
    return now_ms >= previous && now_ms - previous >= 5000U &&
           last_report_ms.compare_exchange_strong(previous, now_ms, std::memory_order_relaxed);
  }
private:
  static void update_max(std::atomic<uint64_t> &value, uint64_t candidate) {
    uint64_t previous = value.load(std::memory_order_relaxed);
    while (previous < candidate && !value.compare_exchange_weak(previous, candidate, std::memory_order_relaxed)) {}
  }
  std::array<std::atomic<uint64_t>, SIZE> counts{};
  std::atomic<uint64_t> last_report_ms{0};
  std::atomic<uint64_t> max_call_attempts{0}, max_charged_us{0}, max_budget_permille{0};
};

inline unsigned spi_retry_budget_ms(unsigned timeout) {
  // Keep the existing ACK timeout range and six-timeout retry envelope, but
  // bound non-timeout I/O/checksum/protocol-NACK failures as well. Only endpoint-3
  // data NACK (possibly CAN TX backpressure) retains the existing retry exemption.
  // Charged attempts include their backoff sleep. A blocking kernel
  // ioctl may still overrun the userspace deadline.
  return 6U * std::clamp(timeout == 0U ? 500U : timeout, 20U, 500U);
}

inline bool spi_within_budget(double started_ms, double now_ms, unsigned budget_ms, bool connected) {
  return connected && now_ms >= started_ms && now_ms - started_ms < budget_ms;
}
