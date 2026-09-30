# LX3 request-specific permission transaction — development

This branch is a PC verification candidate. It is not OTA or vehicle qualified;
the LX3 dashcamOnly interlock remains. Production remains e0747fca. Normal host
engagement integration now uses the normal StateMachine PRE_ENABLE path and
the request-specific companion. An old host cannot grant this policy.

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

Ten same-topic rounds used the actual installed Claude Code session. Accepted:
tag-aware heartbeat decoding, rechecking NO_ENTRY/SOFT_DISABLE during preEnabled,
atomic accepted-mode telemetry, and stable main gesture identity. Rejected:
8-bit-only nonce, unconditional pending cancellation on disabled heartbeat, and
the claim that a second ACK eliminates asynchronous disable races. Round6 also
identified OFF's unnecessary requalification and explicit-cancel loss after a
later bad batch frame; both corrected. Its claimed missing init-generation
increment was disproved by the actual init and safety-reset native regression.
Rounds7/8 also found and corrected the upgrade's lost USER_DISABLE alert,
unbound refusal arriving before the companion, accepted transactions being
timed out while another normal PRE_ENABLE condition holds, and invalid guarded
companion data falling back to the independently sampled health permission.
Round9 corrected a duplicate denied gesture overwriting its OFF ACK and the
observer's lost publication gap when its chunk was full. Same-counter replay
is denied; normal IPC retry uses a different physical counter. Rotated chunks
now include permission baselines and per-service omitted-observation attempts.
Round10 corrected duplicated LX3 cluster producers and OEM warning masking.
Actual DBC packing also exposed the missing0x161 checksum/counter definitions;
see [cluster transport review](LX3_CLUSTER_TRANSPORT_REVIEW_20261001.md).
A condition
can change immediately after any confirmation; tests must measure bounded
revocation and absence of new host active commands, not assert zero latency.

## Host integration

Each adapter frame executes StateMachine.update exactly once. New physical
requests need valid counter metadata; default/old ButtonEvent producers cannot
enable. A physical request received before its companion waits while disabled.
After binding the matching pending generation/counter/mode, normal ENABLE plus
the LX3 PRE_ENABLE event makes enabled true but active false and publishes the
matching ACK. Only the matching accepted companion completes the transaction.
Independent health controlsAllowed is not used as a companion substitute.

Pending rechecks NO_ENTRY, USER_DISABLE, SOFT_DISABLE and IMMEDIATE_DISABLE each
frame. A refusal received before its nonce is remembered only to send a later
matching OFF ACK; it cannot latch an enable. Normal StateMachine alerts and
other-car paths remain. An upgrade first publishes one normal disable frame
with USER_DISABLE, then enters the new pending on the next frame.

An accepted transaction can remain normally preEnabled while a separate
PRE_ENABLE condition holds. It has no pending timeout or repeated ACK and sends
no active actuator commands. Its entry barriers are still rechecked. This is
the existing enabled/inactive StateMachine distinction, not a new self-grant.

The passive recorder keeps physical bus0 RX 0x10B frames without its previous
worker-wall-time sampling cap (25Hz CAN can be drained in one50ms batch). It
also records received companion/host ACK changes, with source timestamps,
sequence and publication gaps, between the existing5Hz full context samples.
The20Hz worker uses conflated subscriptions; these records do not prove every
firmware transition was captured. Chunk/drain/quota limits still apply. Missing
guarded protocol data is unknown, not permission inferred from universal health.

## Verification at this checkpoint

- 192 Python regressions pass, including actual-parser batching/counter and
 production host adapter/StateMachine tests. New cases cover delayed refusals,
 499/500ms request boundaries, generation wrap, unrelated PRE_ENABLE, retained
 disable alerts, one update per frame, and bounded transition recording.
- Actual Panda C, compiled with strict warnings and undefined-behavior sanitizer,
 passes existing forwarding/angle/physical tests plus transaction tests for
 pending active rejection, wrong generation/mode/counter, old/disabled heartbeat,
 immediate revoke, rejected-request retry, rapid second toggle, six fault cases,
 timeout, safety reset, wrap, combined speed adjustments and upgrade.
  A deterministic20,000-action state/heartbeat/fault/reset check also passes
  companion decoding in all observed phases (idle19,821; pending176; accepted3).
  Shared C/C++ decoder rejects short packets and incoherent fields; the encoder
  preserves all256 physical counter values and reserves generation0.
- Actual Panda C and production Python host logic pass360 synthetic delivery
 schedules (CAN0/10/30/90ms, batching,10Hz companion phases and read/host/HB
 order). All65,160 host frames call StateMachine.update exactly once;6,600
 heartbeats and32,216 host active TX checks pass.391 host commands after a
 physical revocation but before its publication are blocked by the actual C
 policy. This counts synthetic schedules, not measured vehicle latency.
- A strict/UBSan STM32F4-conditional desktop compile/run confirms the classic
 8byte CAN packet, unchanged health16/58byte and legacy heartbeat behavior.
 It is a protocol compatibility test, not an STM32F4 firmware build.
- Host follow-up commita64e454d has all5 CI jobs successful (run36748114010),
 including full Linux runtime/import/production publisher IPC/AlertManager and
 H7. Round10 cluster/DBC/parser changes and actual module blinker tests require
 their resulting commit's full CI before being recorded as build-verified. Neither
 build establishes EPS reception, real steering, LFA icon or warning removal.

## Required next work

1. Recheck latest complete CI including the expanded real msgq/Capnp production
   publisher smoke, full runtime, classic compatibility and360 joint schedules.
2. Compare current protocol against recorded physical
   RX separately from synthetic host acknowledgements; old logs have no such ACK.
3. Keep the original-camera fault gate until bit meanings/ownership are verified.
   The historical LSS/DAS onset association is not root-cause proof.

The updated actual-C archive replay covers78 unique saved streams under both
accelerator policies (156 cases), without any synthesized ACK. All observed
physical-button rows retain mode0/no accepted permission;684/937 pending rows
are observed for alternativeExperience0/1. Each policy rejects all121,620 old
active0xCB requests. Historical inactive/display TX may still be accepted.
Recorded batch timestamps do not validate hardware RX timing; original logs do
not contain the new protocol, so this is a no-self-grant replay, not evidence
that the new handshake was used successfully on a vehicle.
