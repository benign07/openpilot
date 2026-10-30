LX3 installed19 cumulative update preparation —2026-10-30

Source dd81cf729baf6316cc8517b377dc39f6aaa137b6 (unchanged production Git blobs).
Sequence22 combines already-published20(profile API,5existing Python files) and21
(departure low-speed restriction,2existing Python files). Every after-file SHA is
exactly equal to the corresponding published20/21 after SHA. No new algorithm,
parameter range/unit, profile numeric value, lateral/DM/Panda/model/DBC/core/boot
implementation is introduced by this packaging.

All7 before hashes were read from the actual installed19 baseline on2026-10-30 and
matched their previous publication before hashes. The exact existing-files guard
remains mandatory. This is for the full19 baseline; partially updated20/21 or other
different cohorts must reject rather than changing the before rules to fit.

Behavior included
- HUD/Windows schema1 longitudinal27-key profiles, preview/apply/readback and
  durable recovery contract. User data/Params/receipts are preserved. No automatic
  profile application after updating. Parked-state/type/unit/revision checks stay.
- Optional post-departure following allowance full below20km/h, taper20→40,
  absent40+, retired after40until the original low-speed stop detector observes
  a stop again. Existing25s window and normal acceleration table/approach limits
  remain. This is not proof of faster first movement or physical road comfort.
- The automatic-updater bootstrap remains a SEPARATE signed two-file deployment.
  This source bundle never modifies the updater/core/boot. Complete bootstrap
  verification/activation independently before applying this bundle, since it
  advances protected setting_safety.py from API19 to API20.

Evidence reused at exact immutable source (no reasonless global test rerun)
https://github.com/benign07/openpilot/actions/runs/40820903428
  updater/rollback/passive diagnostics Python3.11/3.12 each153pass.
https://github.com/benign07/openpilot/actions/runs/40820903349
  profile/API/launch Python3.11/3.12 each321pass.
https://github.com/benign07/openpilot/actions/runs/40820903314
  eachPython815audit+162response plus native-lateral-audit successful.
https://github.com/benign07/carrot-hud/actions/runs/40820979665
  HUDv59 Android98/PCreceiver16/Windows3069checks+480reference cases successful.
The publisher verifies exact source/client CI, immutable schema1 contract,
five common fixtures and Windows source bindings before publication.

Primary verifies the new signed cumulative bytes/sequence/compile/before hashes,
private staging/validation without changing the temporary before source, and
public immutable bundle/index/signature after publication. Actual vehicle work
so far is private package transfer and read-only verification; no live replacement,
service stop, CAN transmission or reboot has occurred during this preparation.

Same-topic prior source and bootstrap Claude reviews were preserved. Additional
packaging review97 continued the SAME session, but CLI returned authentication
403 before any reasoning/read. It is NOT counted as a review pass. Primary
independently checks the seven-file provenance and package scope. No fallback
account/key, local session reset or administrator setting change was attempted.

Limits: no installation or road qualification yet. Source files require parked
P/zero/inactive and original boot/rollback health verification. Steering cluster
warning, SPI and actual first-motion latency remain unresolved by this packaging.
