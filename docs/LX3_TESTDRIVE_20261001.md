# 2026-10-01 installed candidate test drive

Installed source/firmware: `1d3367bb5d1c58a62af055989b586fd47ca94658`.
Route: `00000172--ffd54d216f`, closed segments 0–9. The PC holds
177,479,680 bytes of rlog/qlog and sealed automatic diagnostics (48 files).
Archive and every extracted file match the device SHA-256 manifests. Active
recording segment 10 was excluded from this immutable snapshot.

## Confirmed runtime failure

The lateral MPC Cython extension was linked with an absolute solver path:

```
/data/community/lx3_engagement/1d3367bb/prepared/selfdrive/controls/lib/lateral_mpc_lib/c_generated_code/libacados_ocp_solver_lat.so
```

Staged compilation succeeded at this path. Deployment renamed `prepared` to
`/data/openpilot`, leaving that ELF `DT_NEEDED` dependency unavailable.
The original build directory was not retained as a compatibility alias.
Qlog contains 1,337 matching `plannerd` import crashes. Its
`longitudinalPlan` and `driverAssistance` publications are absent; selfdrived
reports `commIssue` and refuses engagement. The other absent services listed
in the same startup log (`driverMonitoringState`, `alertDebug`) are explicitly
ignored by existing selfdrived checks and are not established causes here.

Across the captured qlog samples, `selfdriveState.enabled/active`,
`carControl.latActive/longActive`, and Panda `controlsAllowed` are always false.
CAN validity remains true. Stock cruise state is enabled in 511 samples.
This supports the user's observation of stock ASCC; radar display is not
evidence that openpilot gained actuator authority.

There are also 77 carState samples with temporary steering faults, no permanent
steering fault samples, and `steerUnavailable` / `steerTempUnavailable` events.
The OEM warning's exact trigger and the hands-on warning are not explained by
the import failure alone. Physical input/CAN/MDPS timing must be examined
separately using the copied rlogs and automatic diagnostics. Qlog sampling can
miss short button edges and permission handshakes.

The full rlogs additionally confirm 77,773 carControl messages with neither
axis active and no valid originating control identity. Original MDPS frames
(0xEA, bus 0) all have `LKA_FAULT=0` and `LFA2_FAULT=0`; camera health frames
(0x162, bus 2) all have the four decoded fault fields zero. Original camera
steering commands and matching MDPS active states are present, consistent with
stock lateral assistance rather than openpilot authority.

Two short host temporary-fault episodes begin at boot-relative 342.248s and
489.976s. At each, a valid neutral CAN frame after MAIN is followed by RES
before the existing 300ms MAIN release qualification finishes. The host input
code marks that sequence `ambiguous_gesture`, resets readiness and contributes
to `steerFaultTemporary`. All 18,169 original physical-input frames have valid
CRC. This input-ordering concern needs its own native/host regression and
remedy; the MPC packaging fix does not resolve it. The absence of the decoded
MDPS/camera fault flags does not prove the user's cluster warning was absent
or identify its ECU source.

## Source remedy and verification gap

The lateral SCons build now uses a named solver library, local library search
path and `$ORIGIN` runtime resolution, matching the existing longitudinal
strategy. Its solver resolves acados dependencies relative to its installed
location. The Darwin install-name path follows the longitudinal build.
No actuator policy, safety limit or communication fault check is weakened.

CI must import actual `plannerd` and run both built MPC extensions from a copied
installation tree in fresh processes. The relocation check rejects absolute
`DT_NEEDED` entries and build-root loader paths, clears `LD_LIBRARY_PATH`, then
creates/resets and reads/writes both native solvers. It does not publish CAN or
qualify vehicle behavior. Linux executable evidence and Claude review results
must be recorded separately from this design description.

Claude's source review confirmed the linking mechanism and challenged false
passes through previously loaded/build-tree libraries. The stronger regression
requires origin-relative loader entries and checks `/proc/self/maps` in each
child process to prove vendored MPC/acados dependencies came from the copied
tree. The ARM vendored dependency scan is static inspection, not execution of
the corrected device MPC extension. Actual ARM build/relocation verification
remains necessary at the next authorized device preparation.

The earlier postboot verification checked source hashes, firmware signature,
profile, CAN, error flags and process presence. A repeatedly restarted process
can appear present. That verification did not establish planner health, and
the successful staged build did not establish relocation. The next installation
must additionally check planner imports at the final location and fresh valid
`longitudinalPlan`/`driverAssistance` publications over time before reporting
readiness for driving. A missing planner must retain the engagement refusal.

Vehicle software is not changed again during this collection/PC task. Any
follow-up installation requires the completed candidate build and a fresh
parked/inactive deployment opportunity. No new OTA release is published.
