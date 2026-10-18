# LX3 OEM camera overlay and blinker handback correction

Source: 0fa97b88c985d5bc1f9ddfc2bbd8ec248833ff8d.
Baseline: verified installed source80b875f0, HUD sequence13.
Sequence14. PC, exact CI, parked installation and postboot evidence are separated below.

Confirmed mechanisms
- Before-source formatter revived hidden1EA code4 and hidden rear geometry.
- Before-source empty-slot fill accepted stable unclassified stationary tracks.
- An8-second read-only actualCAN sample had no original side objects but9
  host-supplemented RF slots on TXecho128. The old UI listened only to CAM2.
- The old handoff waited1second after clear and then set its scale directly1.
  The identical new timing test produced5failures/7passes before the correction.
  One failure concerns an internal scale while disabled, not actual permission.

Corrections
- Stop live radar empty-slot fills; preserve LX3 OEM types and hidden states.
- Replace the minimap with calibrated real camera projection using actual echo.
- Unknown OEM-visible types remain neutral markers; unclassified radar fills
  never become camera vehicle icons. Existing DBC type definitions still need
  LX3/video correlation; no new object-class/confidence mapping is claimed.
- Reduce clear wait1→0.5seconds, use0.5second fade-in on ordinary and newly
  accepted resumes. Existing1second fade-out, signal-on handoff, grants and
  driver/angle/torque limitations remain. No full-torque deadline is forced.
- Preserve existing parameter values and update HUD descriptions/menu.

PC evidence
-510 main pytest cases and39separate handoff/authority cases pass (549distinct).
- The latter include13new timing/menu cases. No hardware grant/CAN TX.
- Protected controls/authority/Panda/MDPS/DBC/native paths remain unchanged.
  The controller AST outside display-selection removal is unchanged; the two
  manual-steering helpers intentionally change only the reviewed envelope.
- ActualCAN original/echo, prior counterexamples, before/after timing logs,
  source hashes and testXML stay outsideGit. An initial runner import issue
  and stale fixture/description expectations are saved separately.
- Original radar fields PROB/VALID/VALID2/IN_MYLANE were sampled read-only.
  Their names/values are not validated confidence or object-class semantics.
- Claude's recovered persistent session is blocked by its weekly usage limit;
  no fresh same-topic cross-review pass is claimed for this correction.

Release/install limits
- First CI10059e4c failed because importing the handoff helpers loaded real
  schemas before the copied-schema qlog fixture. The final source changes only
  CI process isolation from10059e4c; all production/test Python blobs are equal.
- Exact-source final GitHubCI is separate and recorded by run/headSHA.
- Five signed allowlisted source files; one separately signed unsupported UI
  bootstrap. The allowlist is unchanged. Publish/queue only after parked UI
  bootstrap and exact-source device verification; keep all parking/hash gates.
- Parked SSH UI bootstrap and signed source boot transaction completed; public HUD publication is the next separate step.
- Parked ARM module/import tests passed; real UI process is stable. Actual camera-pixel and road handback validation remain for the next drive.
- OEM steering warning and MCU/SPI root cause remain unresolved.
- Additional LX3 only. Original LX3 and EV9 stay unchanged. No phone APK change.


## Exact-source and installed verification

Exact source: 0fa97b88c985d5bc1f9ddfc2bbd8ec248833ff8d. CI39113991298 has3/3successful jobs; Python3.11 and3.12 each510main+39handoff/authority cases. Native linked solvers/schema comparisons passed. API archive SHA digests and inner files verified.

Parked ARM correction tests: 177PASS, actual UI module imported under a private prefix. No CAN TX by tests. A first scratch mirror import path was rejected; corrected scratch source provenance passed without any production-source change.

Before both installation steps, fresh continuous P/zero/inactive controls were verified for10seconds. Existing key/signature, fingerprint, strict before hashes and updater transaction gates retained. One unsupported UI file was deployed separately before the five-file source transaction; the source updater allowlist is unchanged.

Real new boot confirmed: 3595bb6c-c303-4af0-a0b6-8a3759c84bfd. All14cumulative runtime source hashes match. Native pandad, Panda safety profile hyundaiCanfd/2238 and selected persistent settings were preserved. CANvalid=true; Panda faults=[]; eight required processes continuously healthy for30.1seconds; updater independently reached complete. No OEM overlay runtime warning appeared in this boot journal.

The reboot SSH command timed out during the connection loss; no second reboot or second installation was issued. The actual new boot, signed installed metadata and runtime hashes established the outcome.

Actual moving-image alignment, object classification correctness, handback feel, OEM steering warning and MCU/SPI improvements are not claimed. Original LX3/EV9/phone APK and native firmware are unchanged. The recovered Claude session remains usage-blocked; no fresh review PASS.
