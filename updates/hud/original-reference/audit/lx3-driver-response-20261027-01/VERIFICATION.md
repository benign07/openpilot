# LX3 driver-response candidate — 2026-10-27

Source: `29f0d86f2533cb80945ab3c85d67386f49c311cb`.
Separate signed fixed-scope update for original LX3 serial `80acfe79` only.
Not installed and not road qualified. This is not shared HUD source OTA.

## Changes

- LX3-only steering authority recovery seeds the first driver-torque filter
  from the current sample. A resting 200-raw hand no longer causes the old
  permanent recovery taper/25 ceiling. Definite driver input yields; confirmed
  release recovers over 0.2s. Predictive yielding requires four distinct MDPS
  updates above 225; the 200 release boundary retains a middle holding band.
  These raw units are not a calibrated distinction between touch and intent.
  Existing maximum authority 250 and native angle/torque/permission limits stay.
- Manual-blinker handback uses the same 0.2s recovery after CarState confirms
  signals OFF and confirmed driver force ends. Existing lamp debounce may add
  delay after the physical lever ends. A steady curve may qualify at >=8m/s,
  angle <85deg and rate <=12deg/s for 0.2s. Low-speed large turns still wait.
  A new permission after a real loss uses the normal initial activation ramp.
- `DriverPositionHold` is a separate default-OFF experimental JSON preference.
  A confirmed manual within-lane displacement can hold the lane-relative position
  for 3s then return to the normal path over 5s with a quintic transition.
  It changes a bounded native-MPC path, never fixes steering angle/torque.
  It requires fresh inputs, 30..110km/h, high lane quality and corridor clearance.
  Pre-press and last six end-of-press observations must be stable within 4cm;
  at least 0.3s of native confirmed press and >=5cm/uncertainty-qualified movement
  are required. Additional offset <=10cm and existing total <=40cm are preserved.
  Occupancy/maneuver/control loss veto immediately. Routine quality loss discards
  the capture and withdraws at 5cm/s. It may not capture a moving release or
  predictive yielding below native steeringPressed. It is not collision avoidance.
- Passive review records manual correction candidates with before/after context,
  requested/output steering, lane/path/road-edge samples, curvature, side tracks
  and video/route monotonic times. It does not learn or change settings online.
  Buffers, rates and per-trip episode caps are bounded. Ground truth remains unknown.
- Capture adds real carOutput, radarErrors, launch CarParams and Panda bus counters.
  LAT-only UI follows fresh actual carControl independently of SCC state updates.
- Includes previously uninstalled owner-compat audit fixes for lane-mode selection,
  diagnostic clock units, audio buffer boundaries and parked/offroad maintenance.

## Explicitly withheld

`DepartureThresholdReview` computes a diagnostic-only candidate. The actual
`get_accel_from_plan(vEgoStopping=vEgoStopping)` and stop policy are unchanged.
There is no owner option to apply the candidate: transient quality loss could
cause a low-speed re-stop. Its record explicitly says `applied_to_control=false`.
Morning raw SCC echo already released StopReq about 1s before wheel motion;
lowering a planner threshold does not establish the cause or fix that delay.

## Evidence

[Exact GitHub Actions 40444888003](https://github.com/benign07/openpilot/actions/runs/40444888003):
all three jobs successful, head SHA/attempt 1 bound and 3 job logs/5 artifacts
downloaded with SHA256 verification. Python 3.11 and 3.12 each passed
624 existing cases +151 subtests and 131 response cases; no errors/failures/skips.

Native Params/msgq and both actual MPC solvers built. Actual lateral solver:
161 manual-position cycles verified 3s hold/5s return, other-car isolation and
OFF/shadow path/solver identity. Actual longitudinal solver: 160 equal input
cycles preserve params/costs/solutions/acceleration; candidate is shadow only.
Real longitudinal planner/Carrot builder: 120 OFF-baseline cycles byte-identical,
passive evidence failure does not abort planning, snapshot and replay checked.

Same persistent Claude session `b673ecb6-e097-4252-9eb7-14a36ba669e5`, actual
authorized Opus 5.5 fallback. Reviews 61/62/64 max and 63/65/66 high examined
the same issues. Parent reproduced/fixed the confirmed host/ramp cases,
strengthened noisy-position rejection, and withheld the unqualified launch
candidate. Review66's remaining test rename was corrected and final CI passed.
Claude read source; it did not execute tests or validate the vehicle.

Earlier failures are preserved locally: incomplete old Linux namespace snapshot,
AST test globals missing new preferences, test-only optional feature import,
rename expectation, float32 fixture tolerance and noisy capture counterexample.
They were not counted as successful evidence or hidden by control changes.

## Deployment and limits

16 fixed Python runtime paths, exact installed-before hashes, Ed25519 pin,
installer/helper hashes, protected firmware/native/schema hashes and continuous
fresh P/zero/inactive control checks remain mandatory. New JSON is preserved
through installation/rollback. UI staging is syntax-only because full import
creates global UI/hardware instances. Actual rendering needs postboot checks.

No Panda/native firmware, MDPS processing, driver-monitoring policy, torque
maximum, CAN button/ACK synthesis or authority bypass is included. No OEM fault
masking. DisableDM stock Carrot policy and HUD API contract are retained.
No new upstream Carrot feature transplant is claimed for this update.

OEM LSS/steering warning, SPI root cause, physical launch delay, real lane comfort
and road thermal cost remain unresolved/unqualified. Full safety pytest/MISRA,
vehicle closed-loop validation and handset interaction were not performed.
Local/CI synthetic timings are not vehicle CPU/thermal measurements.
