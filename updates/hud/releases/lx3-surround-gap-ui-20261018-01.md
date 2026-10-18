# LX3 surround, speed-gap and applied-state UI — sequence 13

Source: `80b875f0456c297be0023af22daa29767dd8f05f`.
Release: `lx3-surround-gap-ui-20261018-01`.
Signed bundle SHA-256: `6ac788bf6f6d3c15f4c1083693950cf4aace118fbdb17457b667ae6ac7bec98e`.

This publication contains exactly the immutable signed bundle already installed
and reboot-verified on the additional LX3 development device. Publishing does
not perform a second vehicle installation. Source lineage, publication,
installation and physical driving validation are separate records.

## Changes

- Nominal following comfort reference scales linearly from the prior 100% at
  rest to 90% at 100 km/h, capped at 90% above 100 km/h. It is gated to fresh
  valid LX3 ACC/actual longitudinal control. Absolute stopping floor, physical
  obstacles, braking/danger constraints and stock deceleration/early-braking
  headroom remain. Qualified flexible-gap relaxation uses the greater reduction,
  without summing or multiplying the two reductions.
- OEM surrounding-object display preserves valid source slots and classifications.
  Stable, fresh front-side radar tracks can fill completely empty display slots
  with generic targets. It does not invent vehicle types, rear coverage or free space.
- The openpilot screen adds a receive-only relative-position minimap from OEM
  CAN, with CRC, bus and source-age validation. Unknown classes and stale objects
  are hidden. Existing EnableCornerRadar controls this display.
- Nearby clearance and smooth-approach indicators require the corresponding
  actual planned-path or binding comfort calculation. Shadow/zero/stale/inactive
  conditions do not claim active adjustment. The indicator is not proof of
  physical vehicle movement or comfort.
- Pending diagnostic/recording changes include preserving OEM 0x162 types,
  bounded diagnostic storage and oldest-completed-first rotation. Protected
  recent/open/unverified records are preserved. Old completed diagnostics may
  rotate without a PC receipt; this is not guaranteed lossless archival.

## Completed verification

- Windows PC: 483 integrated pytest items plus 15 separately executed authority
  items passed; seven authority subcases are additional.
- Isolated ARM Linux: 332 integrated pytest items plus 15 separate authority
  items passed; seven authority subcases are additional. Production source was
  untouched during these tests and test CAN bytes were not transmitted.
- Real native IPC with typed Cap'n Proto data and the real CarInterface.apply
  optional display-context API passed using separate IPC/Params namespaces.
- The actual ARM linked longitudinal solver ran 800 iterations per OFF/ON case
  at 100 km/h in an ideal equal-speed kinematic plant. Solver status remained 0,
  results were finite and this synthetic plant had positive separation. Mean
  final gaps were 31.3745 m OFF and 28.3751 m ON. These are numerical test results,
  not measured road gaps or a physical safety/comfort guarantee.
- The real UI module imported and the installed UI process ran normally. No OEM
  overlay warning appeared in the checked boot journal. Moving-object visual
  placement and actual applied-state behavior still require a road comparison.
- After a fresh continuous P/zero-speed/inactive-control check, three separate
  API/UI bootstrap files were installed with backups and service stopped. Then
  the existing signed source boot transaction applied nine source-OTA files.
  A new boot, all twelve exact runtime hashes, unchanged native pandad, valid
  vehicle CAN, no Panda faults and 30 continuous seconds of stable required
  processes were verified. The existing updater independently reached complete.
- An initial test mirror imported the production checkout; the harness package
  binding was corrected. A separate copied-schema suite initially collided with
  loaded cereal schema IDs; it was separated into its own process. Failed logs
  were preserved and neither failure was counted as a pass. Production safety
  conditions were not weakened to make these tests pass.

## Deployment boundaries and remaining limitations

The source bundle contains nine existing allowlisted Python files.
`opendbc_repo/opendbc/car/interfaces.py`, `selfdrive/car/card.py` and
`selfdrive/ui/onroad/augmented_road_view.py` are outside that allowlist and need
the separately verified parked bootstrap first. This was completed on the
target additional LX3. A source bundle alone must not be presented as installing
those three files. No allowlist, signature, before-hash, identity, parking,
authenticated HUD API or recoverable boot checks were relaxed.

Native pandad/Panda/H7, model, DBC, MDPS and DisableDM are unchanged. The original
LX3 and EV9 are excluded from this deployment. The cluster steering-assistance
warning and SPI issue are not fixed by this release. No new road qualification
or fresh Claude cross-review PASS is claimed: the recovered continuous Fable
and Opus review session was blocked by its weekly usage limit. Exact-source
GitHub CI is tracked separately and does not replace these recorded checks.
The exact-source CI was started as
[run 39109978669](https://github.com/benign07/openpilot/actions/runs/39109978669);
its outcome is not included in the completed checks above.

Automatic diagnostics were recording without a worker error or restart after
boot, and rotation retired old completed chunks. Storage-drop and stale-source
counters were nonzero, so complete diagnostic coverage is not claimed. Those
counters alone do not establish native SPI loss or delayed actuator commands.

The immutable signed change notes were authored during candidate preparation.
Their references to then-pending authentication, Linux/native tests and device
installation are historical progress statements. The completed checks and
actual installation state above supersede those progress statements. The
signed payload was deliberately preserved byte-for-byte; no release ID was
reused with different contents.
