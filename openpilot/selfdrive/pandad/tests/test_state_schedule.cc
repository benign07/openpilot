#include <cassert>
#include <cmath>
#include <cstdio>
#include <vector>

#include "common/ratekeeper.h"
#include "selfdrive/pandad/state_schedule.h"

// Linked with the actual common/ratekeeper.cc; only time/sleep/log dependencies
// are replaced by the test runner. No control policy or CAN hardware is mocked.
uint64_t simulated_ns = 0U;

static double simulate(bool guarded, unsigned cost_ms, unsigned peripheral_ms, unsigned serial_ms, bool one_stall = false) {
  simulated_ns = 1000000000ULL;
  RateKeeper rk("test", 100);
  PandaStateSchedule schedule;
  schedule.set_guarded(guarded);
  std::vector<uint64_t> states;
  bool injected = false;
  const uint64_t finish = simulated_ns + 30000000000ULL;
  while (simulated_ns < finish) {
    if (schedule.peripheral(rk.frame(), simulated_ns)) {
      simulated_ns += peripheral_ms * 1000000ULL;
      if (one_stall && !injected && states.size() >= 20U) {
        simulated_ns += 95000000ULL;
        injected = true;
      }
    }
    if (schedule.state(rk.frame(), simulated_ns)) {
      states.push_back(simulated_ns);
      simulated_ns += cost_ms * 1000000ULL;
    }
    schedule.publish(rk.frame(), simulated_ns);
    if (schedule.serial(rk.frame(), simulated_ns)) simulated_ns += serial_ms * 1000000ULL;
    rk.keepTime();
  }
  for (size_t i = 1; i < states.size(); ++i) {
    assert(states[i] > states[i - 1]);
    assert(states[i] - states[i - 1] >= cost_ms * 1000000ULL);
    size_t window_count = 1U;
    for (size_t j = i; j > 0U && states[i] - states[j - 1] <= 1000000000ULL; --j) ++window_count;
    assert(window_count <= 11U);
  }
  return (states.size() - 1) * 1e9 / (states.back() - states.front());
}

static void simulate_transitions() {
  simulated_ns = 1000000000ULL;
  RateKeeper rk("transitions", 100);
  PandaStateSchedule schedule;
  bool guarded = false, entering = false;
  uint64_t transition_at = 0U;
  for (unsigned i = 0; i < 120U; ++i) {
    schedule.set_guarded(guarded);
    const bool run = schedule.state(rk.frame(), simulated_ns);
    if (!guarded) assert(run == (rk.frame() % 10U == 0U));
    if (entering) {
      assert(run);
      assert(simulated_ns - transition_at <= 10000001U);
      entering = false;
    }
    if (run) simulated_ns += 60000000ULL;
    // Like configureSafetyMode: switch after a slow state batch, before keepTime.
    if (i == 20U || i == 70U) {
      guarded = true; entering = true; transition_at = simulated_ns;
    }
    if (i == 50U || i == 100U) guarded = false;
    rk.keepTime();
  }
}

int main() {
  PandaStateSchedule legacy;
  for (uint64_t frame = 0; frame < 10000U; ++frame) {
    const uint64_t jittered_time = frame * 17777777ULL;
    assert(legacy.peripheral(frame, jittered_time) == (frame % 5U == 0U));
    assert(legacy.state(frame, jittered_time) == (frame % 10U == 0U));
    assert(legacy.publish(frame, jittered_time) == (frame % 50U == 0U));
    assert(legacy.serial(frame, jittered_time) == (frame % 10U == 0U));
  }
  for (uint64_t phase = 0; phase < 10U; ++phase) {
    PandaStateSchedule s;
    uint64_t now = 1000000000ULL + phase * 10000000ULL;
    s.set_guarded(true);
    assert(s.state(phase, now));
    assert(!s.state(phase, now));
    assert(!s.state(phase, now + 99999999U));
    assert(s.state(phase, now + 100000000U));
    // A long stall emits one current execution, never ten missed ones.
    assert(s.state(phase, now + 1000000000U));
    assert(!s.state(phase, now + 1000000000U));
    s.set_guarded(false);
    assert(s.state(phase, now) == (phase % 10U == 0U));
    s.set_guarded(true);
    assert(s.state(phase, now + 1010000000U));
    assert(s.peripheral(phase, now + 1010000000U));
    assert(s.publish(phase, now + 1010000000U));
    assert(s.serial(phase, now + 1010000000U));
  }
  const double before = simulate(false, 60, 0, 0);
  const double after = simulate(true, 60, 0, 0);
  assert(before > 6.4 && before < 6.9);
  assert(after > 9.8 && after < 10.2);
  std::printf("PASS actual RateKeeper: 60ms status work old %.3fHz -> deadline %.3fHz\n", before, after);
  for (unsigned cost : {0U, 30U, 60U, 90U}) {
    for (unsigned peripheral : {0U, 1U}) {
      for (unsigned serial : {0U, 2U}) {
        const double hz = simulate(true, cost, peripheral, serial);
        assert(hz > 9.5 && hz < 10.5);
      }
    }
  }
  const double overloaded = simulate(true, 160, 1, 2);
  assert(overloaded < 8.0);
  assert(simulate(true, 60, 1, 2, true) > 9.5);
  simulate_transitions();
  std::printf("PASS overload remains observable %.3fHz; transitions, legacy cadence and skipped slots\n", overloaded);
}
