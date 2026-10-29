LX3 departure following: low-speed-only allowance

Source: 2ae6ad862247c00b1abf61e5c05d8b50b308194f. Production implementation was committed in b644a27f384e1138114f0742a4e5d82db1fb9250; subsequent commits change shared preset metadata and the integrity test's explicit allowance for the reviewed LaunchFollow help text only.

Behavior
- Preserve the current configured Carrot acceleration curve and the 27 portable-profile numeric values.
- Optional departure ceiling capability remains unchanged below20km/h, fades linearly20→40km/h, and is exactly absent at40km/h and above.
- Reaching40 clears departure eligibility. Merely slowing below40 cannot reenable it until the existing low-speed-stop observation occurs again.
- Preserve the existing25-second observation window and v<=0.15m/s stop observation. Below40, soft withdrawal/reentry inside that window remains possible; this is not a newly qualified standstill/hold-release detector.
- Bound residual allowance before soft withdrawal paths, and retire the departure on invalid limits.
- Stock MPC, turn/feasibility checks, speed-dependent distance correction and approach/braking helpers remain active. This does not promise to eliminate every acceleration-then-braking event, shorten first-motion delay or improve road comfort.

Evidence
- Exact source profile CI https://github.com/benign07/openpilot/actions/runs/40809680632 : Python3.11/3.12 XML each321 cases, zero failures/errors/skips (including subtest cases).
- Exact source host/native CI https://github.com/benign07/openpilot/actions/runs/40809680648 : each Python version audit815 and response162 XML cases, zero failures/errors/skips. Native lateral, response and evidence component checks passed.
- Local Linux related tests260 passed plus151 subtests. Initial new boundary tests on the unchanged preceding source failed8, passed1. Initial local dependency/snapshot setup errors and stale presentation-test expectations were corrected; they are not concealed as passes.
- Added realistic1.5m/s² taper, invalid-input retirement,40km/h boundary chatter, no-reentry without stop, and actual production planner block/MPC setter binary equality checks. The planner test extracts the production block; it is not a full IPC execution.
- Actual x86 linked MPC18 fixed-input solves: at40,50,60km/h, new complete solver trajectories equal strength0;20km/h retains old capability,30/39 taper. Same production acceleration table and unchanged solver generation/binary. Fixed component comparisons do not measure physical actuator response, hold release, perception or road comfort.
- App source887b231baf94f86c7b74dfe69d891a4ada73eb60, build56 https://github.com/benign07/carrot-hud/actions/runs/40809489523 passed Android95 JVM/UI tests and Windows static-reference/import/export tests. Public APK/shared fixture bytes and Windows ZIP hashes verified. Existing user profiles remain importable; new builtins' metadata identifies new semantics without changing their27 numeric settings.
- Actual Claude CLI continued the same persistent review session, Opus5.5 fallback: review90 max identified wording, invalid-input rearming, metadata and planner-test requirements; review91 high found no must-fix after changes. Its narrative mistakenly said preset IDs were unchanged; actual revision IDs were advanced and verified by the primary agent. A local Qwen bounded review completed but misidentified the governing function and was not adopted.

Deployment scope
- Signed source OTA modifies only existing approach_comfort.py and server/services/settings.py. Before hashes match the last verified source baseline; the unchanged approach source hash was observed in the prior device inventory, and settings matches verified sequence19 installed bytes.
- No vehicle source write, reboot or actual phone installation was performed for this release. No Panda/native/DBC/model/DM/steering code or acceleration-table setting is deployed by this bundle.
- Preserve signed immutable source, exact before-file hashes, target-car binding, parked/inactive checks, boot transaction and rollback. Do not bypass a mismatch.
- Existing profile API20 server files are unchanged and are not included in this two-file bundle. This is not an API19→20 server migration; older installations without the profile API still need that separately versioned update.
- Preserve user profiles, receipts, logs and parameters; no automatic profile reapplication.

Limits: vehicle installation and functional/road qualification are pending. First-motion delay, cluster steering warning and SPI are unresolved by this change.
