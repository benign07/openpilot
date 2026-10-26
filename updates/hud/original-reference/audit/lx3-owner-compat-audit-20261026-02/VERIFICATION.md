# Original LX3 owner-compatibility audit layer

Release: `lx3-owner-compat-audit-20261026-02`. Preparation, publication,
installation and road verification are separate records. This file describes
the PC candidate; it is not proof of a vehicle installation.

## Seven production files

- `lateral_planner.py`: preserve the explicitly requested actual-margin opt-in
  to stock lane following from 30 km/h when both the stored setting and the
  runtime lane selection are zero. Respect positive thresholds and the existing
  LFA long-press runtime selection. Log stored/runtime/effective selection
  separately. Existing model slowdown, lane confidence, curve and margin vetoes
  remain. No new steering algorithm or increased torque/authority.
- `lane_planner_2.py`: report and reset an actual margin when lane selection is
  disabled; the default argument preserves existing direct callers. Shadow
  candidate calculation and corridor/occupancy/freshness guards remain.
- `longitudinal_planner.py`: fix processingDelay nanosecond-to-second arithmetic;
  this changes diagnostic timing, not acceleration or braking trajectories.
- `server/features/system.py`: maintenance first uses the existing strict fresh
  P/zero/all-axes-inactive guard. Power-only offroad maintenance requires fresh
  device OFF and independent fresh Panda ignition-OFF/controls-OFF evidence,
  manager Params agreement, and stopped control producers. The dedicated Panda
  reader is created once, reused under a lock and polled only after offroad
  preconditions pass. Rejected driving requests do not open it. This avoids
  msgq's non-reclaimed subscriber-slot accumulation. It does not add HUD streams
  or a monitoring process. Separate MONOTONIC and BOOTTIME clocks are respected.
  Missing/stale/unknown Panda or contradictory state is rejected. No driving
  setting or installation gate is relaxed.
- `ui/soundd.py`: update loop/offset on every buffer iteration and recognize exact
  completion, preventing finite sounds from replaying past their configured end.
- `can_diagnostics/automatic_runtime.py`: record the separate cruise-button
  preference and its observed/default/error status, plus authority-source hashes.
- `server/services/settings.py`: preserve the requested lane-mode opt-in while
  describing the forward radar's actual scope and all conditions accurately.

## Preserved behavior and limits

The original Params333, H7, native pandad/SPI profile, MDPS/CAN builder,
controller/authority, haptic helper, stock DisableDM=1 and existing MAIN
keep-lateral option remain protected. Owner settings are not rewritten.
Rollback preserves the owner's current settings rather than old install-time
values. The shared HUD updater allowlist and latest index remain unchanged;
this is a separately signed parked source layer.

The prior dec19356 candidate removed an owner-authorized feature. That removal
has been withdrawn. Its signed package remains historical and must not be
installed. Review52's approval of that removal is superseded by review55.

The examined road samples had zero additional margin applied. The recorded
13–18 cm difference was between planned paths, not measured vehicle movement or
proof of actual clearance. Retaining the opt-in does not prove a margin was
applied or that adjacent/rear space is free. Stock thresholds and vetoes are not
weakened to force visible activation.

LSS/stock steering-warning, factory HUD lane style/colour and MCU SPI/CAN1
root causes remain unresolved. No warning suppression, synthetic control
authorization, forged MDPS/button/ACK, driver-monitor policy or ECU diagnostic
probe is included. Actual vehicle behavior requires later driving evidence.

## Validation records

The local Linux contract run records the parent commit plus SHA256 of every
overlaid source file, uses real past Git objects for recovery fixtures, and
executes the coupled host/native C tests. The source snapshot, XML and full log
are retained outside Git. All 623 tests and 151 unittest subtests passed before
preparation; no skipped cases or failed cases in that run. Three pytest
configuration warnings reflect optional test plugins not installed in that
isolated contract environment.

Reproduction records retain the old owner-opt-in failure, runtime LFA mode
failure, and missing-Panda offroad-permission failure before their fixes.
Focused Windows checks passed 92 tests/147 subtests before the final IPC/native
checker adjustments; Linux checks cover the final source snapshot, including
the 45-request single-subscriber and driving-rejection tests.

Actual x86 Params/msgq bindings and lateral-MPC solver were built from the
isolated source snapshot. The native comparison passed: 160 initial lateral
cycles; OFF/shadow path and solver bytes equal the baseline; bounded active
margin; native-zero opt-in, LFA runtime OFF, reset, curvature veto and non-LX3
isolation. Explicit --isolated-local runs require an x86 PC without /TICI and
use temporary Params. Exact source and native library hashes are in the local
binding record. An earlier local run set the CI admission environment flag;
the final run instead uses the explicit local flag with GITHUB_ACTIONS unset.
Neither is a GitHub Actions run or an ARM device build.

Persistent Claude review55/max corrected review52's intent assumption and found
the LFA runtime-setting and ignition-proof issues. Review56/high identified
msgq subscription accumulation; source inspection and failing-before tests
confirmed it. Review57/high passed the corrected maintenance path. Reviews
were read-only, not physical tests. A native solver check is not closed-loop
vehicle qualification, and neither PC check is installation.

## Deployment

Verify the signed exact seven-file package and protected inventory. Obtain a
new device connection and fresh continuous P/zero/all-controls-inactive evidence
before staging/applying/reboot. Do not reuse the earlier parked sample. Restore
this layer before the older MAIN/parity layers. Retain old immutable packages,
raw private evidence and the current owner preferences. Public publication is
pending the owner's explicit approval requested after the previous automatic
approval rejection; private keys, raw routes, location and preference backups
are excluded from publication.
