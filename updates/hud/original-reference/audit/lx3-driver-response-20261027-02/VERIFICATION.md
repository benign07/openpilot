# LX3 response and structural integrity candidate02 — 2026-10-27

Source: `5c29ed90a8167540702a1e9ac111d71619b7603e`.
Separate signed sixteen-Python-file update for original LX3 serial `80acfe79`.
Not installed or road qualified. This is not shared HUD source OTA.
Supersedes the uninstalled `lx3-driver-response-20261027-01`; all of its intended
driver-response, position-capture and owner-compatibility changes are retained.

## Retained behavior

* LX3 resting-hand recovery no longer stays in the old taper/minimum forever.
  Confirmed driver input yields and release restores authority over0.2s.
  Native authority, angle checks and original maximum250 remain independent.
  Raw thresholds do not perfectly distinguish a person's touch from intent.
* Blinker handback takes0.2s after confirmed CarState OFF and confirmed force
  release, subject to existing angle/rate/permission checks. Native lamp debounce
  can add delay after the physical lever. Real authority loss resets handback;
  new approval uses normal initial activation rather than an old quick ramp.
* Default-OFF `DriverPositionHold` in the development menu uses a confirmed
  lane-relative movement:3s hold then5s quintic central return. It requires
  high-quality fresh lanes, actual lane mode,30..110km/h, at least0.3s confirmed
  steeringPressed and stable pre/end six-sample windows with <=4cm spread and
  >=5cm/uncertainty-qualified movement. Extra offset <=10cm and total <=40cm.
  It is a bounded path preference, not obstacle avoidance or free-space proof.
* Bounded manual-correction episodes record before/after context, output/request,
  lane/path/curvature/side tracks and video clocks for later offline tuning.
  No online learning or automatic setting change. Real carOutput, radarErrors,
  launch CarParams and Panda counters remain captured. HUD1.0.27 contract stays.
* Prior owner-compatibility fixes for native lane-mode selection, diagnostic
  time units, sound buffer bounds and parked/offroad maintenance remain included.

## Simplification and confirmed corrections versus01

* At the existing controller decision point, filter signed LX3 torque before
  taking magnitude, following stock Carrot intent; add its65% filtered-magnitude
  minimum to predictive yielding. Retain four distinct MDPS observations above
  the225 raw predictive boundary, release at200 and middle-band hold. Synthetic
  alternating +/-230 formerly reduced the cap250->25 and now retains250.
  Sustained force and confirmed steeringPressed still yield. Fast0.1s pushes
  yield at confirmed press; slower tested ramps can yield before it. No new
  minimum-rise-rate requirement. Other-vehicle legacy traces are preserved.
* Reuse the existing override latch instead of a duplicate LX3 yield latch.
  Host pause, controller yield and final output ceiling are retained because
  their contracts differ. A wholesale handover rewrite was rejected.
* One pure projected corridor-envelope helper replaces three duplicated
  calculations. Position withdrawal now obeys current finite bounds too; an
  out-of-envelope base path immediately removes the preference, matching lateral
  margin withdrawal. This can end a hold early. Qualified normal behavior is
  byte-identical to01, including its3s/5s timing.
* Remove the unqualified departure-threshold comparison state machine entirely
  from live planning. The actual `vEgoStopping` policy is unchanged. PC-only
  `tools/lx3_departure_review.py` uses recorded stop samples, states and the
  exact update clock under additive `stop_decision` sub-contract2. Missing older
  fields, changed baseline predicates and logging gaps are rejected/reset.
  There is no setting to enable that unqualified candidate for control.
* Replace runtime settings-description rewriting with literal final strings;
  the entire emitted settings JSON is byte-identical. Keep separate JSON
  namespaces and profile exclusions for old-reader/rollback compatibility.
* LX3 green lateral status requires fresh prepared controller output and no
  controller angle limit as well as the host request. It is not a TX receipt,
  ECU ACK or physical torque measurement. Capture existing LatSuspendAngleDeg
  for later diagnosis without changing its inherited300-degree default.

## Compared sources and scope

Exact published01 `29f0d86f2533cb80945ab3c85d67386f49c311cb`, current stock
Carrot `f29102fa8346ca3c83eace15ee2915eed90015c9`, working0627 LX3
`6a3d7783b50490370e4a58f3f9a9ce683fd94845`, September27 snapshot
`61fd1f262a7bca79dbc7868188fa0adac29f1741`.

43 changed-runtime/dependency paths were inventoried; stock paths were mapped
under its openpilot/ prefix where required. This is a0627-based supported LX3
integration, not a wholesale latest-Carrot port. New upstream CANFD jerk/stopping
experiments and steering-handover mode3 were not imported without LX3 evidence.
Native/Panda authority, MDPS processing, buses, buttons, schema, Params registry,
DM policy and the actual protected updater/boot helpers remain unchanged.

## Exact verification

[GitHub Actions40450289678](https://github.com/benign07/openpilot/actions/runs/40450289678)
completed all three jobs successfully at the exact source SHA, attempt1.
Three job logs and five artifacts were downloaded, identity and SHA256 verified.
Python3.11 and3.12 each:624 cases +151 subtests and162 response/integrity cases,
with no failures/errors/skips. The preceding code-identical492e2f64 CI also
completed successfully; it does not replace this exact final-head result.

Native Params/msgq and both MPC solvers built. Actual lateral paths/solver
solutions match published01 byte-for-byte for160 margin cycles and161 qualified
manual hold/return cycles; OFF/shadow and other-car isolation also pass.
The actual longitudinal solver has160 identical input/solution/acceleration
cycles while the unqualified candidate remains PC-only. The actual
CarrotPlanner/LongitudinalPlanner producer verifies sub-contract2, snapshot
immutability, diagnostic exception isolation, original OFF baseline120cycles
and actual launch/turn/feasibility/approach precedence.

Production press-confirmation ramps cover0.1/0.3/0.5/1s, both directions and
one/two-tick MDPS samples, immediate yielding by confirmed press, <=20cap drop
per control tick and0.2s release. Settings bytes, normal controller traces,
projected envelope equivalence, deliberate stricter withdrawal, signed-noise
before/after, prepared UI freshness and PC gap rejection are tested.

Claude same persistent session `b673ecb6-e097-4252-9eb7-14a36ba669e5`, authorized
Opus5.5 fallback: review67 max and68/69 high. Source-only reciprocal review
challenged inherited-versus-new code, fast-push timing, current withdrawal
bounds, data timestamps and truthful evidence. Parent ran the tests, corrected
the document's pending-CI wording and recorded the setup failure. Claude did not
run tests or validate a vehicle. Its approximate0.3s ramp timing was superseded
by the executed production-method trace.

Local first response check failed before execution because its fresh snapshot
lacked the longitudinal generated binding. SCons built both bindings and the
same checks passed. Setup failure and earlier candidate failures remain recorded
locally; they are not counted as passes or hidden by production changes.

## Deployment and unresolved limits

Signed fixed scope, installed-before hashes, Ed25519 pin, installer/helper hashes,
protected33-file inventory, owner JSON/Params preservation and continuous fresh
P/zero/inactive checks remain mandatory. No installer-gate relaxation. The
base evidence is the last verified installed release; it must be rechecked on
the actual device before staging/install. No vehicle contacted in this review.
No install record is claimed. No shared HUD latest change or automatic HUD
installation eligibility is claimed for this separate package.

No synthesized buttons/ACKs, torque-generated vibration, fault masking or
monitoring changes. Stock DisableDM behavior remains. No full safety pytest or
MISRA/closed-loop vehicle qualification is claimed. OEM LSS warning, SPI root
cause, physical launch delay, actual driver intent, curve feel and device thermal
cost remain unresolved. A synthetic250->25 fix is not evidence that LSS or SPI
has been resolved. Local/CI timings are not vehicle CPU/heat measurements.
