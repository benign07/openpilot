# LX3 driver response integrity review, 2026-10-27

This remains a working 0627/September27 LX3 integration, with reviewed Carrot
improvements. It is not a wholesale migration to the current upstream branch.
The review follows the changed runtime files and their authority, CAN, schema,
planner, settings, logging and display dependencies. It does not certify every
upstream subsystem or replace vehicle qualification.

## Compared sources

| Source | Exact revision | Role |
| --- | --- | --- |
| Published candidate before this cleanup | `29f0d86f2533cb80945ab3c85d67386f49c311cb` | Behavioral comparison |
| Stock Carrot `carrot-wip` | `f29102fa8346ca3c83eace15ee2915eed90015c9` | Current upstream intent |
| LX3 0627 reference | `6a3d7783b50490370e4a58f3f9a9ce683fd94845` | Working vehicle compatibility |
| September27 device source snapshot | `61fd1f262a7bca79dbc7868188fa0adac29f1741` | Device baseline |

The stock tree uses `openpilot/` prefixes for many planner/runtime paths. Source
mapping, rather than a missing same-path file, determines availability.

## Changes at the original decision points

* Filter signed LX3 driver torque before computing its magnitude, following
  stock Carrot's intent. Require a filtered magnitude above65% of the original
  threshold for predictive yielding, while retaining the distinct MDPS counter
  check. Alternating +/-230 synthetic noise used to accumulate into a reduction
  from250 to25; it no longer does. This is a reproducible synthetic defect, not
  evidence of the cause of an observed cluster warning.
* Reuse `override_latched` for the predictive-yield hold instead of a second
  equivalent latch. Preserve other vehicles' inherited adaptive-recovery state.
  Definite `steeringPressed` still yields immediately, bounded by the existing
  20-per-control-tick reduction. Fast pushes can start yielding at confirmed
  press, rather than predictively. Ramps of0.1/0.3/0.5/1s, both directions and
  repeated MDPS samples exercise the production press-confirmation method.
* Share one pure projected corridor envelope between lateral-margin and manual
  position preferences. During withdrawal, a manual-position preference now
  obeys the current finite corridor bounds just as lateral margins do. If the
  base path is already outside that envelope the preference becomes zero;
  normal qualified3s hold/5s return remains unchanged. It may therefore end
  early on a bad-quality frame. This is not obstacle avoidance or a guarantee
  of measured free space.
* Remove the unqualified alternative departure-threshold state machine from
  the live longitudinal planner. Keep the original `vEgoStopping` decision.
  PC-only `tools/lx3_departure_review.py` compares captured inputs, rejects
  missing data, verifies the original stop predicate and resets across gaps.
  The additive `stop_decision` sub-contract2 records update time separately
  from publication time, both velocity samples and the MPC/e2e decisions.
  Logger envelope schema1 and existing consumers remain compatible.
* Write final settings descriptions directly, eliminating text replacement
  and append passes. The complete emitted settings JSON equals the preceding
  candidate, including menu ordering and option values.
* For LX3, a green lateral indication now also requires a fresh prepared
  controller output and no controller angle limit. It represents prepared
  assistance, not a Panda send receipt, ECU ACK or measured physical torque.
* Capture the existing `LatSuspendAngleDeg` setting in passive diagnostics.
  Its inherited300-degree default and independent angle/manual checks remain;
  the0.2s recovery does not override those checks or missing authority.

## Deliberately retained compatibility

Host permission, Panda heartbeat, TX checks, MDPS processing, CAN buses and
button semantics are unchanged in this cleanup. No synthesized buttons/ACKs,
fault masking, torque-generated vibration or driver-monitoring policy is added.
Stock `DisableDM` behavior remains. Native Params registry, DBC/schema, Panda
firmware, updater and boot transaction helpers are protected by package hashes.

The host pause, controller yielding and final manual ceiling enforce different
contracts; replacing all three with a new monolithic controller was rejected.
Likewise, separate JSON preference namespaces and profile exclusions preserve
older readers/rollback behavior. They are not removed for superficial brevity.
Upstream's new CANFD jerk/stopping experiments and steering-handover mode3 are
not transplanted without LX3 data and qualification. Other-car traces retain
the inherited code path. Carrot HUD1.0.27 API compatibility is preserved.

## Verification and limits

Regression coverage includes the authority/C host integration, deployment
validation, settings byte equality, original/new controller traces, signed
noise, force ramps, fresh UI output, corridor withdrawal and PC comparison
contracts. Linked native lateral MPC comparisons require byte-identical normal
paths and solutions against the exact published candidate. Local Linux
contracts and linked native comparisons passed. The first local response
check could not import the longitudinal generated binding; SCons built both
bindings and the same checker then passed. The workflow is configured to build
both native MPCs and Params/msgq bindings and run the real evidence producer.
That exact-commit CI result is pending at the time this source note is written;
the separate signed release verification must record its actual result.

These are synthetic, source and native-runtime checks. They do not establish
physical launch delay, closed-loop handling, full Panda/MISRA coverage, OEM
warning resolution or SPI fault resolution. No car access, installation or
reboot is part of this review. The signed sixteen-Python-file deployment stays
separate from the shared HUD source OTA channel; it must not be described as
an automatically installable HUD release.
