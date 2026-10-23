# LX3 passive ADAS settings diagnostics

Source: https://github.com/benign07/openpilot/commit/774669299672f425bffd61bfe725a3fd9fd4f463

Exact CI: https://github.com/benign07/openpilot/actions/runs/39902792542 — Python 3.11 and 3.12 each 119 tests, zero failures/errors/skips.
PC: 119 tests. Original CI logs, XML and artifact hashes retained privately.
Claude: same persistent Opus 5.5 session, review22 max and final review23/24 high;
the root inspected the final result. Review-result SHA256: b9cf1d58fbb409e2e07f2aca72854933ac00de62e2ba8911a29e9a555af836f1.

This is a diagnostic release. It does not claim to fix the OEM service warning,
SPI transport, physical haptic motor or a particular real driving stop.
The manual's automatic safety activation, refused toggle, normal off, blockage
and fault conditions remain competing observation categories, not proven causes.

Only automatic.py and automatic_runtime.py are deployed through the existing
source allowlist. Before hashes come from a hash-verified sealed actual LX3
record with the installed notification-controller hash, not an assumed branch.
This is last verified inventory, not a fresh online device check. Staging must
still reject any current before-hash mismatch. No bypass is permitted.

RX state records have an independent 20/s budget; host/echo/reject records have
20/s. The settings-window quota is six per trip with 30 s minimum separation.
Raw DBC candidates have no recorder CRC validation and cannot prove an AVN ACK,
ECU origin or actuator reaction. Missing fingerprint, wrong length, stale data,
rate loss and per-chunk baselines are explicit. 0x160 is not full-rate traced.

PC report changes are in the source repository, not this device bundle. No new
phone APK, schema, parameter, control/Panda/MDPS/DM/planner/native/model change.
No full native build, vehicle installation, bus acknowledgement, ECU acceptance,
physical vibration or road qualification is claimed. Existing P/zero/inactive,
original signing identity, hashes, boot transaction and rollback are retained.
