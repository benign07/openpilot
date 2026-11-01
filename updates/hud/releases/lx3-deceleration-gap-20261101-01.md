# LX3 mild deceleration and gradual following-gap recovery

Source: `48008a1a4626eea5fd13888233947c0eff08afc8`. Sequence23. Target: existing LX3 source22 only, with exact installed before-file hashes. Three existing Python files; no native solver/model generation, Panda/H7, MDPS, DM, DBC/schema or extra inference changes.

## Behavior

- Uses the existing ApproachComfortEnabled and ApproachFlexibleGap settings. Same schema1 units and all27 numerical preset values are retained. Current ratio80 permits up to20% relaxation of the moving timegap component, not20% of the whole physical distance.
- Begins only after ego and the same qualified radar lead are already mildly decelerating. It does not deliberately advance braking onset. Both selected leads, identity/continuity, probability, relative motion and braking limits are checked.
- Acquisition clearance and ongoing mild-maneuver holding are separate so an ordinary preference boundary does not cause sudden catch-up braking. Severe braking, rapid closing, cut-in/unreliable observations or ineligibility cancel the comfort offset.
- Normal recovery follows a smooth30second curve. Renewed traffic braking pauses/restarts recovery from the remaining offset; risk cancellation is immediate. Recovery alone does not zero the positive acceleration ceiling. The existing40km/h departure-assist cutoff is unchanged.
- Physical obstacles, braking/standstill terms, negative braking bounds and danger constraint parameters remain unchanged. Only the existing primary-lead comfort cost reference is relaxed. The secondary/stop/cruise obstacle can still win. Additional relaxation is bounded outside the danger-cost margin and does not add to the existing speed-gap reduction.
- At zero ego speed the new offset contributes zero across the prediction horizon, even with a warm future-speed trajectory. No new request for standstill creep is introduced.
- Existing user profile files, Params and transaction journal are not rewritten or reapplied. Preset behavior markers and explanatory text are synchronized with the new source.

## Exact validation

- [Longitudinal evidence/native CI](https://github.com/benign07/openpilot/actions/runs/41266842812): all3jobs successful at this exact source. Each Python version629evidence tests plus42handoff/authority tests. Native runtime schema2 tests. Existing linked evidence, approach, lateral preference and launch checks passed.
- [Portable profile CI](https://github.com/benign07/openpilot/actions/runs/41266842798): both versions successful,170main tests plus151subtests each. Immutable schema1 contract unchanged.
- [Host/driver/lateral regressions](https://github.com/benign07/openpilot/actions/runs/41266842788) and [signed updater/passive diagnostics](https://github.com/benign07/openpilot/actions/runs/41266842819): successful at the exact source.
- New linked-MPC comparison uses immutable source22 Python files and the unchanged model/OCP. Eight synthetic scenarios, both old/candidate,1400cycles each:22,400solver calls, no nonzero solve status and no simulated collision. Opted-out/far-lead numerical traces are identical. Severe-braking and identity changes cancel relaxation. This is an ideal bounded-acceleration plant, not vehicle or actuator qualification.
- High mild-case peak deceleration:0.417625→0.402775m/s²; low mild-case:0.260377→0.256367m/s². These modest simulation results are not predictions of road comfort. The low case is slow-following, not complete-stop actuator qualification. Reduced minimum gaps are expected; original braking constraints are not a formal safety guarantee.
- HUD/Windows source `70bff36301379871449d4526b12650b20dd8400a`: [CI](https://github.com/benign07/carrot-hud/actions/runs/41266670411) successful;135JVM tests and Windows3069checks/480production static references. Public HUD1.0.63 and Windows package: [v63](https://github.com/benign07/carrot-hud/releases/tag/v63). Runtime source hashes match this firmware; the catalog source label f3df26e7 refers to the identical production code before test/workflow-only fixes.

## Limits and review

No road-quality improvement, fuel saving, engine-off behavior or regeneration/friction allocation is certified. Hybrid powertrain signals remain unvalidated and no motor/engine control was added. OEM steering-assist warnings and SPI diagnosis remain unresolved and are outside this change.

Early unpublished prototypes either stayed dormant or worsened mild peak deceleration after abrupt clearance withdrawal. They were rejected; source22/candidate comparison and final guards are retained. The first broader CI failures were obsolete help/state assertions and an unfetched historical native reference; those tests/dependency fetches were corrected without weakening vehicle guards. Exact final runs above all passed.

The same persistent Claude Fable session returned organization OAuth403 before inference; no Claude review pass is claimed. The local helper supplied a post-recovery identity boundary-test idea, adapted and independently executed. Its claim that the30second completion test was absent was rejected because that test already existed.

Publication, actual installation, post-boot process verification and road testing are separate states. Installation/reboot remains fresh P, zero speed and controls inactive, with existing signature/hash/identity/transaction checks.
