# LX3 lead approach preview refinement

The optional legacy LX3 approach modifier now considers the planner's existing
positive acceleration intent when a confirmed radar lead is slower than the
adjusted cruise target. A distant lead no longer needs to be closing already
before this additional preview can apply. This is a bounded planning preference,
not a guarantee of physical coasting or a measured road comfort improvement.

- Margin 40 changes the lookahead from 4.0 to 4.2 seconds. Extra following-time
  headroom retains its existing bound and slew.
- Coast 40 can reach a zero positive planning ceiling with a sufficiently strong
  approach cue. Existing negative braking bounds and MPC entry feasibility remain.
- Preview fades continuously with lead opening speed, target-speed difference,
  and low vehicle speed. It does not add a launch hold at standstill.
- Both comfort weights release faster only when all selected valid leads open.
- Final stopping/hold, LongControl/PID, Panda, MDPS, lateral control and DisableDM
  are unchanged. The earlier OEM LSS warning and separate reason-7 lateral-limit
  refusal are not fixed or explained by this change.

## Verification and limits

32 public stdlib helper regressions passed. The actual installed native MPC and
Carrot planner passed disabled-exact and unsupported-EV9-exact comparisons and
48 mode/personality/headway combinations. Actual native MPC/full planner and
unchanged LongControl/PID also ran against a synthetic flat-road lag plant for
distant slow lead, cutout, steady noise, and stop/go (60 seconds per version).
All solutions were finite with solver status zero. An additional matched-speed
lead/noise case with cruise above lead speed passed without an increased command
sign-crossing count (11 in both versions, last-15-second command range about
0.048 m/s^2). This does not establish real-vehicle stability or OEM behavior.

In the distant-slow synthetic case, first negative command moved only from
2.70 to 2.65 seconds. Positive command samples while closing fell from 7 to 5.
The synthetic difference is small; a road impression must not be presented as
already measured. Stop/go minimum gap was about 3.93/3.95 m despite a 5.5 m
configured reference, a limitation shared by the baseline plant/controller.
No vehicle brake-pressure, grade, emergency-response or closed-loop qualification
was performed by these offline checks. Source review used the owner's requested
actual Claude Fable 5.1/high session; its counterarguments prompted the noise test.

## Signed source deployment

Only the two existing control Python files are deployed. The release binds exact
installed before-hashes, preserves signature/path/fingerprint checks, and uses
the existing parked boot transaction and rollback originals. Native binaries,
schemas, models, firmware, updater code and other vehicles are outside this OTA.

The top-level settings catalog is outside the source updater's allowlist and is
left unchanged. Its old coast description mentioning zero at 100 is incomplete:
40 can now also reach zero for a strong cue. The precise behavior is documented
in the HUD update changelog; no new setting keys, defaults or ranges are introduced.
GitHub publication, actual installation, startup health and road qualification
must be recorded separately.
