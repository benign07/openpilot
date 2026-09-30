# LX3 request-specific permission transaction — development

This branch is a PC verification candidate. It is not OTA or vehicle qualified;
the LX3 dashcamOnly interlock remains. Production remains e0747fca. Normal host
engagement integration is the next step; the old host cannot grant this policy.

## Confirmed defects motivating the change

- After a host NO_ENTRY, the former physical-button policy could retain Panda
  permission and reverse the meaning of the next toggle. The actual C/production
  host reproduction is recorded in the September 30 archive review.
- pandad timestamps CAN batches, not individual hardware arrivals. In 78 unique
  extracts (132,341 physical 0x10B frames), four timestamps contain two physical
  frames with a valid +2 counter step; two more consecutive valid steps arrive
  less than 10ms apart. The former host integrity check rejected these delivery
  patterns. These are transport observations, not hardware period measurements.

## Firmware/transport contract, version 1

The legacy universal health packet stays version16/58bytes. The new companion
is control read 0xC7, packed12bytes, little-endian:

| offset | field | type |
| --- | --- | --- |
| 0 | version=1 on guarded policy, 0 elsewhere | uint8 |
| 1 | requested mode: OFF0, LAT1, COMBINED2 | uint8 |
| 2 | accepted mode | uint8 |
| 3 | physical gesture counter | uint8 |
| 4 | nonzero request generation | uint16 |
| 6 | request age, clamped at65535 | uint16 milliseconds |
| 8 | controlsAllowed from the same snapshot | uint8 boolean |
| 9 | phase: IDLE0, PENDING1, ACCEPTED2 | uint8 |
| 10 | reserved=0 | uint16 |

Snapshot maintenance and copy use a firmware critical section. pandad requires
the exact length, version and internally coherent phase/mode/permission fields;
unknown or short responses publish default protocol0 and cannot grant control.
Do not combine companion controlsAllowed with the separately sampled health bit.
Policy scope is safetyModel28 AND explicit guard1024; flags190 retain legacy ABI.
Non-CANFD boards retain their existing heartbeat behavior.

Heartbeat 0xF3 retains legacy `(param1 == 1, param2 == 0)` outside this policy.
Guarded encoding:

- param1 low byte: tag0x80, enabled bit0, ACK mode bits1..2, other low bits0.
- param1 high byte: physical gesture counter.
- param2: request generation; zero means no request ACK.

Physical RX creates a PENDING request, with no accepted mode or active actuator
permission. Only an enabled, tagged ACK matching generation, counter and mode
can consume that pending, before500ms and with fresh physical input/MDPS, healthy
RX/relay and permitted pedals. The USB grant resets heartbeat mismatch count
inside the same critical section as heartbeat_engaged. An ACK cannot create a
request or resurrect one after cancel, timeout or a hardware fault.

Tagged enabled0 immediately removes accepted permission. It only cancels a new
pending when it is an explicit matching OFF ACK: an earlier disabled heartbeat
in transit must not erase an unobserved new request. Host rejection/timeout does
not classify a valid physical input stream as corrupt. Explicit main/LFA OFF
also preserves the validated baseline, so a quick new press is not discarded
for an invented120ms recovery. Faults/cancel retain the
existing neutral-requalification behavior.

LFA or main during PENDING means OFF. RES/SET during pending is ignored; RES/SET
in accepted COMBINED changes speed without re-handshaking. A LAT→COMBINED request
currently suspends both actuator permissions pending independent host entry
checks. This interruption is an explicit vehicle qualification item, not a
proved smooth transition.

Main-button debounce remains300ms. Counter identity is the first neutral frame
after the last raw8 frame, independent of the delayed decision frame. Host
metadata is attached separately to each emitted event, never to the batch's
last counter. Host permits shared/fast transport timestamps with valid counter
steps; Panda retains its10ms minimum on actual physical RX timing.

Generation increments across safety resets and skips0 at wrap. RAM boot restart
is not a persistent epoch. Host ACK must be bounded to a fresh matching pending
and cleared outside it. Board restart, delivery latency and ISR behavior remain
qualification items; this is not an adversarial replay-proof protocol claim.

## Mutual review decisions

Six same-topic rounds used the actual installed Claude Code session. Accepted:
tag-aware heartbeat decoding, rechecking NO_ENTRY/SOFT_DISABLE during preEnabled,
atomic accepted-mode telemetry, and stable main gesture identity. Rejected:
8-bit-only nonce, unconditional pending cancellation on disabled heartbeat, and
the claim that a second ACK eliminates asynchronous disable races. Round6 also
identified OFF's unnecessary requalification and explicit-cancel loss after a
later bad batch frame; both corrected. Its claimed missing init-generation
increment was disproved by the actual init and safety-reset native regression.
A condition
can change immediately after any confirmation; tests must measure bounded
revocation and absence of new host active commands, not assert zero latency.

## Verification at this checkpoint

- 162 Python regressions pass, including six new actual-parser batching/counter
 tests. Existing host tests are still the previous host adapter, not proof of
 the new end-to-end handshake.
- Actual Panda C, compiled with strict warnings and undefined-behavior sanitizer,
 passes existing forwarding/angle/physical tests plus transaction tests for
 pending active rejection, wrong generation/mode/counter, old/disabled heartbeat,
 immediate revoke, rejected-request retry, rapid second toggle, six fault cases,
 timeout, safety reset, wrap, combined speed adjustments and upgrade.
  A deterministic20,000-action state/heartbeat/fault/reset check also passes
  companion decoding in all observed phases (idle19,821; pending176; accepted3).
  Shared C/C++ decoder rejects short packets and incoherent fields; the encoder
  preserves all256 physical counter values and reserves generation0.
- Linux full runtime and H7 build must be checked on the resulting commit. These
 do not establish EPS reception, real steering, LFA icon or warning removal.

## Required next work

1. Integrate production SelfdriveD with the fresh request-specific companion,
   normal preEnabled, per-frame entry barriers, and ACK lifetime limited to
   pending. Legacy/default ButtonEvent producers cannot enable this path.
2. Exercise actual C plus production host logic with reversed delivery orders,
   rapid toggles, denial/retry, after-ACK faults and request expiry; expand real
   msgq/Capnp smoke. Preserve other-car state-machine paths.
3. Recheck latest complete CI. Compare current protocol against recorded physical
   RX separately from synthetic host acknowledgements; old logs have no such ACK.
4. Keep the original-camera fault gate until bit meanings/ownership are verified.
   The historical LSS/DAS onset association is not root-cause proof.
