# LX3 diagnostics and SPI boundary candidate

This branch contains selected verified installed sources over a Git build
context. It is **not** a complete image of the installed mixed legacy runtime.
Do not install the whole checkout. Use only hash-bound reviewed replacements
onto the verified `lx3-integrated-20261011-01` runtime.

The passive Python update fixes rotation loss, preserves sendcan identity,
separates collector loss sources, and reports low recording retention space.
The native SPI update validates response lengths before reading/copying,
avoids an unaligned length load, bounds failure recovery and non-NACK failure
retries, and emits
five-second host stage totals without adding Panda connections or transfers.
The firmware change stops counting a valid VERSION response as a checksum error.
It preserves bad-header/data checksum rejection and the health wire layout.

Native and firmware changes require their own build and verified parked
deployment. A HUD source bundle contains only the two passive Python files.
GitHub publication, device installation, and road qualification are distinct.
No changed native/firmware component has been installed by preparing this branch.
Normal CAN TX NACK backpressure retains stock retries. Previously endless
non-NACK failures can now return a negative result; existing health reads return
an empty optional on failure, not a valid zero-filled health record. No sticky
transport-health override was added. Before native deployment, verify the
installed host/MCU health struct sizes and paired support-file hashes.
The existing startup capability check can select NO_OUTPUT if its status read
fails; its existing "capability missing" message also covers transport failure.
This candidate does not bypass that check or add automatic permission retries.
Verify startup on hardware; a failed startup remains unusable until corrected.
The paired `state_schedule.h` is copied unchanged from the prior installed-build
support. Its hash, like `panda.cc`, must be checked before native deployment.

The existing MDPS handling, driver coexistence, safety authority and angle/torque
limits are preserved. The OEM steering warning and the measured SPI error rate
are not established as solved. MAIN handoff rejection can be correct safety
behaviour; counts alone do not justify changing steering ramps or bypassing it.

Recording needs a storage maintenance step before the next complete capture.
Keep stock loggerd deletion thresholds. Protect the current runtime, original
working baseline, current rollback, Params, and untransferred drive evidence.
`tools/lx3_storage_guard.py` refuses a storage-ready result below the stock
threshold plus staging size. It is read-only and does not delete any directory.

PC mocks and CI builds do not establish physical SPI timing, MCU firmware
provenance, the OEM warning cause, or closed-loop vehicle behaviour.
