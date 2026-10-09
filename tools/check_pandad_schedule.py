"""Compile actual deadline policy + RateKeeper with deterministic time only."""
import argparse
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
p = argparse.ArgumentParser()
p.add_argument('--compiler', default='g++')
p.add_argument('--zig', action='store_true')
a = p.parse_args()
with tempfile.TemporaryDirectory(prefix='pandad-schedule-') as tmp:
  directory = Path(tmp)
  common = directory / 'common'
  common.mkdir()
  (common / 'timing.h').write_text('#pragma once\n#include <cstdint>\nextern uint64_t simulated_ns;\ninline double seconds_since_boot() { return simulated_ns * 1e-9; }\n')
  (common / 'util.h').write_text('#pragma once\n#include <cmath>\n#include <cstdint>\nextern uint64_t simulated_ns;\nnamespace util { inline void sleep_for(double ms) { simulated_ns += static_cast<uint64_t>(std::ceil(ms * 1e6)); } }\n')
  (common / 'swaglog.h').write_text('#pragma once\n#define LOGW(...) do {} while (0)\n')
  exe = directory / 'schedule-test.exe'
  command = [a.compiler] + (['c++'] if a.zig else [])
  command += ['-std=c++17', '-Wall', '-Wextra', '-Werror', '-Wno-error=reorder', '-I' + str(directory), '-I' + str(ROOT / 'openpilot'),
              str(ROOT / 'openpilot/common/ratekeeper.cc'),
              str(ROOT / 'openpilot/selfdrive/pandad/tests/test_state_schedule.cc'), '-o', str(exe)]
  subprocess.run(command, check=True)
  subprocess.run([str(exe)], check=True)
