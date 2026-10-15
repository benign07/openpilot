# LX3 launch and lateral preferences — source publication verification

Source: `c54d763b2cfe13a080056634504a7774155ca82a`.

This release offers optional launch-following capability and bounded within-lane preferences, and repairs diagnostic recovery. All amounts default to0 and lateral shadow defaults on. It is published for the LX3 additional-device lineage currently verified at OTA10. Publication is not installation or road qualification.

## Verified PC evidence

[Exact source CI](https://github.com/benign07/openpilot/actions/runs/38739424812): all three jobs succeeded. Python3.11 and3.12 each passed302 tests, with0failures/errors/skips. Real cereal schema passed2 tests. Actual longitudinal and lateral MPC libraries and runtime bindings built successfully. Job logs and three artifacts were downloaded and checked against GitHub artifact SHA256 values.

Actual linked checks include120 longitudinal cycles with byte-identical problem/solution and identical stop/target outputs against the exact OTA10 planner fixture with new preferences off;160 lateral cycles with byte-identical path/solver for off and shadow against the exact OTA10 lateral fixture; bounded active preference; shadow observation with native lane mode0; owner-authorized transient30km/h lane-mode pairing and restoration; production controlsd curvature-sign veto; different-vehicle isolation; existing20-cycle evidence noninterference and approach/flexible-gap comparisons. These are synthetic linked checks, not a closed-loop vehicle validation, H7 firmware build, MISRA run or full upstream test suite.

Same persistent Claude Code session reviewed design, implementation and corrections using actual Opus5.5 (Fable usage limit fallback authorized by the owner), max for difficult initial review and high for corrections. Source review found no remaining blocker for default-off/shadow publication. It did not install or run on a car. Independent source/artifact verification and road evidence must not be conflated.

Concrete source counterexamples reproduce queue omission, radar-slot reuse, launch jitter and hard occupancy/cut-in/corridor veto failures in the exact prior revisions; the same cases pass in this revision. Headerless/corrupt diagnostic recovery, parked setting/reset behavior, invalid-settings preservation, encoder segment/clock boundaries and updater/rollback contracts are covered.

On two original morning rlog segments, fixed-input launch-cap replay at strength10/20 qualified89 frames each; the largest optional positive-ceiling increases were approximately0.129/0.254m/s². Strength0 added nothing. The lateral shadow replay yielded0 nonzero candidates across2399 frames. No reduced road gap, improved clearance or subjective comfort is claimed; the original laneless path is not a counterfactual new30km/h lane-mode path.

## User behavior and deployment boundary

HUD → LX3 driving preferences. `LaunchFollowStrength`0..20%; `YellowLineBiasCm` and `NearbyVehicleBiasCm`0..10cm; `LateralMarginShadow` defaults on. With shadow off and a lateral amount set, stock lane following is temporarily selected from30km/h, including its existing±2km/h hysteresis. Native lane-mode settings are never overwritten. Off, shadow and rollback restore the original setting. Changes/reset require P, zero speed and inactive control.

The front radar and OEM occupancy are inputs, not the drawn HUD icons, vehicle class or measured vehicle width. The assumed2.24m lateral envelope is a design guard, not verified mirror clearance. Alongside/rear free-space coverage and independent lane-color age are unavailable. Moving toward yellow can reduce separation from opposing traffic. Confidence, curve, corridor, occupancy, cut-in and driver/maneuver guards take precedence.

Only13 existing allowlisted Python/web files are bundled. Before hashes come from the last verified installed original-clone archive and signed OTA10 plus postboot hashes. No fresh live-device hash read is claimed. Actual drift must still be rejected by the updater. Native Panda/H7/SPI, MDPS processing, driver coexistence, DisableDM, DBC/schema, OS/model, updater and boot code are byte-identical to the verified baseline. The protected original and EV9 are excluded.

The HUD phone and PC receiver/tools are separate deployments. The phone preserves and reports empty/format-invalid sources, closes streams on gzip-constructor failures and continues bounded batches without treating backlog as a network error. PC source hashing and incremental aggregate caching preserve pending/error coverage; the automatic quality window is400 files by receipt/file modification time, not necessarily latest driving date. Original recordings remain intact. There is no added vehicle video-analysis process.

OEM lane-assistance dashboard warnings and SPI transport errors remain unresolved. This is not their repair release. Signed source bundle, immutable index and tag are verified separately from installation. Existing parked/fingerprint/hash/signature/backup/boot-transaction checks and signed rollback remain enforced.
