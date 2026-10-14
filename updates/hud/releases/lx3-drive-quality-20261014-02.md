# LX3 diagnostic source OTA10: drive-quality evidence route boundaries

Source: 2bd232be118008f497037ad0272ab35a5632647b
Release: lx3-drive-quality-20261014-02, sequence10.
This is a new immutable signed source bundle. Sequence9 remains preserved.

Changes: the passive worker observes CurrentRoute each tick. A changed route
seals the old chunk, and all qualified sample/detail/video stamps use one
effective route. After a fresh explicit ignition-off, a previous route left in
Params cannot label the next trip. An unknown/stale deviceState does not suppress
the same still-running route on recovery. The PC rejects an old chunk whose
explicit route stamps conflict. Unstamped old records remain compatible.

Native CAN/Panda pre-event evidence retains the original freshness window and
source timestamps. It is not qualified per-route input to the drive-quality
report. The trace annotation describes selected pre-window rows accurately;
storage/frame-limit losses remain separately counted. No new CAN, buttons,
responses or actuator commands are synthesized.

Exact source CI38537192612: all3jobs successful, Python3.11/3.12 each214 tests,
real Cap'n Proto schema2 tests, linked Acados20 capture-on/off cycles identical,
actual CarrotPlanner builder and diagnostic-failure isolation verified.
Windows214 tests passed. These are scoped PC/native solver tests, not complete
vehicle closed-loop or whole-repository safety qualification.

Actual Claude Fable5.1 cross-review used the same persistent session and shared
source paths: max for the route/identity questions, high for counterproposal and
implementation review, medium for the final annotation correction. Claude's
unconditional stale-route proposal was rejected by a deviceState-blackout
counterexample, and refined to explicit ignition-off only. An unnecessary
CAN/Panda boundary gate was removed, preserving original behavior. An initial
large review timed out before the owner requested removing the time limit;
that attempt was not counted as a review pass. Completed later reviews and
their file-read receipts are retained privately. Current review has no timeout.

HUD companion source cb607d00daafc21c21aadcfd38f5ad5c0b0cb588 adds a persisted
identity-recheck flag only after manual IP/port edits. Known identity is retained;
a token-free selected-endpoint observation and active-device/snapshot compare
must qualify it before an update. A different observed LX3 cannot inherit the
original device's pairing. Older stored JSON and old device APIs remain usable.
Exact Android branchCI38536566658 and mainCI38537208393 succeeded with61 JVM
tests and13 PC receiver tests. Phone runtime/SharedPreferences/network glue
and actual APK installation have not been verified on the owner's Xiaomi.

Deployment scope: seven existing Python/web files, with before-hashes from the
last verified installed sequence8. No native/updater/bootstrap/OS/model/schema/
DBC/Panda/H7 files are in this source OTA. The separate native SPI candidate
remains uninstalled. Existing controls, MDPS handling, driver coexistence,
DisableDM, thermal thresholds and the owner's current Traffic30 are unchanged.
The PC analysis tool is updated on the PC separately; it is not a vehicle file.
HUD1.0.35's API is compatible; the new HUD APK adds manual-edit target protection.

Limits: route observation is not loggerd's exact start time. A worker restart
before observing a completed route, or an ignition cycle hidden by a deviceState
blackout with no explicit off sample, cannot be inferred perfectly. Initial
detail/video frames before the observed boundary can be omitted. A mixed legacy
chunk can lose up to its60s span from quality evaluation. Boot thermal events
can be retained in an unknown-route chunk and skipped by per-route graphs.
Device camera-start timing, runtime overhead, actual transfer/video coverage,
OEM steering warning, native SPI behavior, hot-reboot cause and physical haptics
remain unverified. No new real drive-quality frames have been collected yet.

Publication and vehicle installation are separate. This work does not connect
to, install, restart or transmit CAN to a vehicle, and does not install the phone
APK. Existing signed updater checks for fingerprint, file hashes, continuous
P/zero/inactive control, backup and boot transaction remain mandatory.
