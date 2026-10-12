#pragma once

#include <cstdint>

// Claim at most one execution at the current monotonic time. Missed slots are
// discarded, never replayed as queued heartbeats/old control requests.
class PandaStateDeadline {
public:
  explicit PandaStateDeadline(uint64_t period_ns) : period_ns_(period_ns) {}

  bool due(uint64_t now_ns) {
    if (next_ns_ == 0U) next_ns_ = now_ns;
    if (now_ns < next_ns_) return false;
    next_ns_ += ((now_ns - next_ns_) / period_ns_ + 1U) * period_ns_;
    return true;
  }

  void reset() { next_ns_ = 0U; }

private:
  const uint64_t period_ns_;
  uint64_t next_ns_ = 0U;
};

// Keep other vehicles' stock frame schedule. Guarded LX3 adds synchronous
// authority transactions: its status deadlines must not count idle loop ticks.
class PandaStateSchedule {
public:
  void set_guarded(bool guarded) {
    if (guarded != guarded_) {
      peripheral_.reset(); state_.reset(); publish_.reset(); serial_.reset();
      guarded_ = guarded;
    }
  }
  bool peripheral(uint64_t frame, uint64_t now) { return guarded_ ? peripheral_.due(now) : frame % 5U == 0U; }
  bool state(uint64_t frame, uint64_t now) { return guarded_ ? state_.due(now) : frame % 10U == 0U; }
  bool publish(uint64_t frame, uint64_t now) { return guarded_ ? publish_.due(now) : frame % 50U == 0U; }
  bool serial(uint64_t frame, uint64_t now) { return guarded_ ? serial_.due(now) : frame % 10U == 0U; }

private:
  bool guarded_ = false;
  PandaStateDeadline peripheral_{50000000ULL};
  PandaStateDeadline state_{100000000ULL};
  PandaStateDeadline publish_{500000000ULL};
  PandaStateDeadline serial_{100000000ULL};
};
