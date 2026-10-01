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

# Additional PC review — 2026-10-01 afternoon

All ten copied rlogs contain 67,682 carState samples, including 767 temporary
steering-fault samples. A replay ordered by logMonoTime (logger subscriber file
order is not time order) attributes 40 driving-period samples (22 and 18 in the
two episodes) to ambiguous-gesture/neutral-requalification/warm-up. Another 12
startup samples have sequence/warm-up contributions. No driving-period
CCNC stale/fault or MDPS fault contribution appears in that replay. Exact card
consumption batches are not recorded, so this is input/health attribution rather
than an executable reproduction of the installed complete CarState process.

All 727 startup samples are at 79.736–87.125 seconds; 715 have no raw-health or
input contribution in the upfront-parser model. Raw healthy camera
and MDPS data is already available. CarState previously read its optional
`ccnc_0x162` display cache for health; that cache is populated only after
ControlsReady and fingerprint-monitor count122. Missing that cache therefore
reported a steering fault even with valid received camera health. The PC fix
registers required CCNC health from parser creation and reads its live parsed
values for the health check; the optional display cache and other vehicle paths
retain their existing initialization. Missing/stale data and invalid CRC continue
to deny health. The cache defect is reproduced independently; the installed
cache's exact registration time has not been pinned from logs, so its attribution
to all 715 samples remains an inference. This does not establish the cause of the
OEM cluster warning.

The gesture fix instead completes MAIN on a distinct supported physical button
after a validated fully neutral frame, before processing the new press. It
preserves MAIN's toggle and first-neutral counter, including OFF, and retains
the 300ms debounce for MAIN/neutral flicker alone. An initial proposal to discard
unfinished MAIN was rejected because it could lose OFF. Direct MAIN→other with
no neutral and damaged/stale/ambiguous-bit input still revoke. The enum cannot
prove that two mechanical buttons were pressed simultaneously.

Host RES/SET/LFA release events now require fully neutral physical input, matching
native; direct button changes no longer manufacture release identity. Replaying
18,169 actual original button frames changes only four event frames in the two
MAIN→RES episodes; the other gestures, including the slow MAIN→LFA cases, stay
unchanged. Both ambiguous-gesture episodes disappear in the candidate input
replay; observed installed carState/control logs are not rewritten.

Local production-host/native tests cover the new gestures and transport batching.
A very short MAIN OFF→RES retry delivered in a delayed batch can still be denied
when native OFF precedes host consumption of MAIN; the mismatched counter/mode
cannot grant. That availability limitation is retained explicitly in a golden
schedule, not reported as a successful retry. A held RES release after the OFF
publication has a separately tested new-generation request path. Fixed ARM
runtime and actual vehicle/cluster qualification remain unproven.

Follow-up review also gates the LX3 CCNC display/object cache on a received,
fresh timestamp: the pre-registered parser's zero dictionary is never treated
as a received object. Health remains false on missing, bad-CRC or stale data.
An isolated actual camera parser replay, with its construction clock set to the
recorded route clock, reports all 66,786 observed CAN-event ticks valid, including
startup. This measures the newly required CCNC health stream; it is not a
complete dynamically discovered CarState canValid replay.

The initial CI of 05caa42e failed two fixtures that manually registered CCNC
again. Their registration assumptions have been updated to assert startup
registration; checksum/freshness and forwarding assertions are retained. The
native Linux UBSan physical/transport/legacy tests and 848 host/native schedules
passed before that duplicate-registration fixture failure. The complete CI must
be rerun on the final follow-up commit.

Actual original-RX native replay uses 4,184,048 timestamp-ordered records and the
single recorded Panda boot epoch, with no host ACK or actuator TX. It returns
zero rejected RX, zero reordered records and zero accepted-control rows. The
legacy uint16-address adapter excludes 132,490 extended-address frames; all
18,169 physical button frames are included. Its timer uses pandad publication
times, so it does not prove actual native wire-reception timing.

Pinned 354b1764 policy and the candidate were replayed through the same recorded
RX/epoch adapter. In addition to the two MAIN→RES request edges, the native
comparison adds MAIN/LFA-OFF edges near519s, where the host already completed
MAIN but the older native timer had not. This is a timing-dependent divergence
fix in the publication-timestamp model; it is not proof of installed wire timing.
Retained physical counters and additional requests shift subsequent companion
rows, so row differences are not counted as separate gesture defects.

LX3 corner fusion now also ignores stale optional ADRV0x1EA data, rather than
allowing it to override fresh CCNC distances. The legacy TCS0x175 rewrite
producer returns no frames for LX3; its Sorento/default CAMERA_SCC behavior is
retained and native rejection of synthesized feedback remains unchanged. Real
DBC builder tests verify both the LX3 suppression and the default rewrite path.
The later offline CI failure on57cffc8b was another two generic display-fixture
loops attempting to re-register CCNC. Those fixtures now preserve the startup
registration and continue testing all five CRC/header/unknown-bit round trips.

PC candidate08ae5cc9 completed all five exact-commit CI jobs in run36818895063:
full runtime/C++ schema build, relocated x86_64 lateral and longitudinal solver
execution, real IPC smoke, actual C++/native transport, native UBSan/compatibility,
H7 firmware build, and229 offline tests per Python3.11/3.12. ARM runtime on the
device and OEM warning acceptance remain unverified.

Further PC changes separate physical button readiness/integrity from EPS faults.
CarState adds input state, retained integrity reason and per-tick physical
counter/validity. Old/default producers read notApplicable/false and cannot grant
guarded LX3 control. MDPS/CCNC faults retain their existing EPS barrier. Input
loss has its own NO_ENTRY/IMMEDIATE_DISABLE event; controlsd denies both axes and
identity immediately, and card independently removes all ten owned messages
against its current input health. Neither path supplies an enable ACK on input
loss. Other vehicles retain their original paths.

Validated cancellation now matches native three-neutral requalification. A short
RES attempt during this recovery cannot latch an enable or manufacture a release.
The normal cancel alert takes priority, rather than reporting an EPS failure.
Dead streams during startup/requalification are diagnosed as stale after200ms;
integrity cause survives later neutral/warmup samples until full requalification.
Local production Python tests232 and separate real schema serializer12 pass;
actual C plus production host956 scheduling cases include cancel/short retry and
qualified retry. Full build and real msgq/new-enum publishing must pass on this
later exact commit before its CI is described as green. Old recorded LX3 logs
have absent input fields and intentionally cannot grant in this new host path;
raw CAN replay must regenerate input health, rather than infer it from defaults.

The preceding input-health commit1586ff1a passed all five jobs in run36820553496,
including244 offline tests per Python version and956 joint schedules. The C++
transport executable reports8483 assertions, not the previously reported8489.

The next PC candidate preserves a bounded interpretation window for delayed
physical releases after native permission changes. It retains only the host's
own previous accepted intent, physical consumption counter, reset serial and
epoch. It never imports a requested mode from Panda or restores an accepted ACK.
A real released event must produce a candidate which matches a fresh native
mode/counter/generation/epoch and enters the normal PRE_ENABLE transition.
All barriers, input resets, epoch changes and500ms expiry discard the window.
CarState's new reset serial also detects a fault and requalification which both
occur inside a single published batch; old producers default to zero and cannot
use this window. An independent same-batch OFF/retry defect is fixed by recording
whether a session existed before interpreting buttons, forcing disable before
the new PRE_ENABLE/ACK. The native permission policy is unchanged.

Local evidence is258 Python/schema tests and1116 actual-C/production-host joint
schedules, including54 schedules each for MAIN-OFF/short-RES, MAIN-OFF/short-LFA
and LFA-OFF/short-RES. Claude Opus5.5/high round33 found no authority defect in
source review; it did not execute these tests. Broad NO_ENTRY barriers deliberately
reduce replay availability. Native-only sub10ms revocation is outside the fixed
25Hz joint schedules; a candidate without a matching native request still cannot
obtain authority. Capturing a changed native identity retains the existing
controlsMismatch alert; this does not establish an OEM warning fix. Exact-commit
full build and extended real msgq IPC are pending for this next candidate.
Vehicle ARM execution, EPS acceptance and OEM cluster warnings remain unverified.
