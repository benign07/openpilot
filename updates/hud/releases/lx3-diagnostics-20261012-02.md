# LX3 passive diagnostics 20261012-02

Source: `a381f7d6c7d469d391a8e45121718279bcaf9d61`. Signed HUD sequence 6. Intended only for the matching
additional LX3 installation `lx3-integrated-20261011-01`.

The source bundle replaces **only** automatic.py and automatic_runtime.py.
It rotates full chunks before fresh selected CAN/prebuffer/Panda evidence is
discarded, preserves host-requested TX identity, separates stale CAN and Panda
observations and storage loss, and reports stock loggerd retention thresholds.
Quota/reserve/time/frame limits and existing reader/API fields remain.
Storage cleanup is still required; this update does not delete files.

Exact CI: [five successful jobs](https://github.com/benign07/openpilot/actions/runs/38189545985). Both Python versions passed
91 recording/API/signed-updater/rollback tests. Actual unchanged C safety
policy and MAIN handoff suites passed. Seven actual-source SPI mock scenarios,
MCU VERSION/CRC/saturation fixtures, H7 development firmware and real Linux
pandad builds passed. Original recorder fixture stored 6/80 selected frames;
the corrected rotation fixture stores all 80. These are PC/CI checks.

Actual Claude Fable5.1/high same-session review completed three rounds.
Corrections preserve normal CAN TX NACK backpressure. The final minor recorder
counter and HACK-NACK test suggestions are covered by the final exact CI.

Native SPI length/recovery/diagnostic changes and MCU VERSION counter fix are
separate candidates and are **not delivered by this source bundle**. The H7
artifact is a development build, not a vehicle-qualified flash image. No ARM
vehicle runtime binary or installed MCU provenance is claimed by the Linux
build. Existing startup capability/NO_OUTPUT checks remain. Verify installed
paired support hashes and health/status sizes before native deployment.

OEM LSS warning cause and actual SPI error-rate attribution remain unresolved.
Normal VERSION false increments are reproduced, but do not explain the actual
parked rate by themselves. MAIN late-command rejection can be correct; road
records do not establish a LIMIT fault. No control ramps, MDPS, driver
coexistence, safety policy, DisableDM, schema, updater or HUD app changes.

No vehicle installation/reboot/deletion was performed during this preparation.
Application remains signed, before-hash-bound and parked through the existing
HUD updater. Original-device and EV9 compatibility is not permission to apply
this additional-device release; different before hashes must be rejected.
