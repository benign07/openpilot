# LX3 owner workflow

The owner requests that tested LX3 updates be managed on GitHub and offered
through Carrot HUD, rather than stopping after a source-branch push.

- Keep the current September 27 restored 0627 controls, MDPS handling and Panda
  firmware unless a separately reviewed control change is explicitly in scope.
  Passive diagnostics and the recording overlay are independently versioned.
- After testing a source change, use `tools/publish_hud_release.py` to publish
  its signed immutable bundle, release notes, verification/limits, incremented
  sequence, pinned `updates/hud/latest.json`, and release tag on
  `hud-device-updates-20260928`. Verify the public bytes and signature. A review
  branch push alone is not completion of the requested release workflow.
- Derive the spec's `before` hashes from verified installed files or the last
  verified installed release. Never weaken the updater's hash, signature,
  fingerprint, parked-state, or boot-transaction checks to make an update fit.
- Keep signing keys, pairing tokens, Params backups, original CAN/rlog and
  location records outside Git. Use the existing private key/public-key pairing.
- HUD source OTA supports its explicit Python/web allowlist. Native programs,
  Panda firmware, model, DBC/schema, additions/deletions and updater/boot code
  need a separately verified deployment. Do not claim a source bundle covers
  unsupported changes. The initial updater restoration is a parked SSH action.
- Installation/reboot requires fresh continuous P, zero speed and inactive
  control. A request while driving may be queued; do not replace executing
  control source or restart the device during driving.
- Record GitHub publication, actual installation and functional/road
  verification as separate states. Keep Carrot HUD 1.0.27's download/API contract.
  The EV9 work is on hold; LX3 releases must not be installed on it.
- When a new same-topic cross-review is useful, use the installed Claude Fable
  CLI with high effort and save the actual model/result. A usage-limit failure
  is not a review pass, and historical Opus reviews are not Fable reviews.

The 2026-10-06 source release uses diagnostic commit 013fcfdb with updater
integration d4cf0377; the annotated release `palisade-20261006-01` identifies
the publication. Actual device verification belongs in the release record.
