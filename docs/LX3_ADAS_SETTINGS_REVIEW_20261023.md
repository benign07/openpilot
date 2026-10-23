# LX3 HEV ADAS settings evidence

This is a diagnostic update, not a claimed fix for the OEM lane-assistance
warning, SPI errors or the physical haptic motor.

The [2026 LX3 HEV manual](https://ownersmanual.hyundai.com/full_webhelp/LX3HEV/2026/ko_KR/adas.html)
distinguishes departure prevention (LKA) from lane centering (LFA). With LFA
and SCC both enabled, or HDA enabled, FCA/LKA automatically turn on and a
request to turn them off can be refused. Afterward LKA restores its previous
setting; FCA remains on. The orange LKA indicator can mean off, obstruction
or a fault. A refusal toast, a service message and an ECU fault observation
must not be treated as the same event.

## Static source comparison

Compared installed-source `3fa2ef3d6f89690419e971b2a6380fd4ee191b73`, the real
September 27 backup `61fd1f262a7bca79dbc7868188fa0adac29f1741`, and observed
upstream `carrot-wip` `214e7c24f7635201e19e970b465413a2a6ea10b2`.

All retain compatibility fields including `HAS_LANE_SAFETY=0`, LKA mode and
icon replacements. Those fields are not a proven AVN setting readback or a
newly introduced fault. The installed camera-SCC path periodically requests
LFA when the received symbol is off, even while OP control is inactive.
That mechanism can affect the automatic-on conditions but does not prove
it caused the reported service warning.

Latest upstream also has a different ALT2 button-request/Panda-overlay path.
Its Python side must not be copied without reviewing the paired firmware,
LX3 physical input, timing and authority contract. This update changes none
of those control paths.

## Diagnostic changes

- Capture LX3-only `0x160` and `0x1E0`, plus full-rate `0x1AA` inside existing
  bounded windows. Preserve raw bytes and original/normalized packet clocks.
- Preserve selected state changes between ordinary 10 Hz samples, including
  short host button press/release, settings candidates, modes/icons and faults.
- Keep physical input, host request, Panda TX echo and TX rejection separate.
  A raw receive bus is not an ECU identity. The MCU creates the echo when
  submitting to the controller FIFO; it proves neither bus ACK nor ECU acceptance.
- Do not decode checksums as validity. State fields are explicitly unvalidated
  DBC candidates. Retain raw bytes and the DBC/source hashes for PC validation.
- Initial observations and gaps longer than one second are baselines, not
  proof of a setting change. Equal packet timestamps preserve collector order.
- Bound added state records to 20/s RX plus a separate 20/s host/echo/reject
  budget so host traffic cannot exhaust RX capacity. Expose losses, unknown
  fingerprint frames and wrong lengths, including RX/host loss totals. Repeat
  baselines in each new chunk, including rapid recovery after storage failures.
  Keep high-rate 0x160 out of the full trace; its samples and changed fields
  remain available. Pre-window/frame caps and eviction counters still apply.
  Settings have
  six separate 2 s before/8 s after windows per trip, at least 30 s apart;
  automatic button pulses cannot create/extend them or consume physical quotas.
- Retain `CanfdDebug`, `HDPuse` and speed-camera haptic preferences in the
  existing five-second configuration polling/header snapshot.
- The PC trace report includes observations outside button windows and orders
  them within a boot by monotonic time, not potentially adjusted wall time.
  Its 10,000-record display cap and missing observations remain explicit. A
  truncated manifest-scan prefix is explicitly not a contiguous time window.

## Verification and limits

Tests cover the previously missing setting messages, a 40 ms host pulse and
release missed by 10 Hz samples, bus/direction isolation, installed DBC bit
positions, stale/old timestamps, burst limits, quota isolation, gaps, storage
loss, real compressed chunks, route/trip changes and the signed HUD updater.

No production steering, braking planner, MDPS, Panda, DM, DBC/schema, native
program, phone APK or new parameter is changed. The source update uses existing
allowed Python paths and the original signing identity. Installation and actual
road/ECU verification remain separate from publication.

Next drive evidence should distinguish an AVN setting request from the observed
state and OP's periodic button requests. The manual supplies behavior rules,
not CAN identities. No dangerous departure or collision stimulus is required.
