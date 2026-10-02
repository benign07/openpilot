# LX3 accepted session and short CarState IPC gaps

The October 3 route `00000183--47c7917cb2` on installed `b6bfe9b3`
had three `controlsMismatch` events near 22.45–25.84 ms CarState publication
gaps. Panda still reported the matching accepted identities, no RX errors,
and no TX blocks. The log does not contain the selfdrived socket return value;
a receive timeout is a supported explanation, not direct field proof.

The production socket waits 20 ms. Pending requests already tolerate missing
samples bounded by the last valid publication's age. The accepted path instead
immediately rejected a single missing sample. An offline production-adapter
test reproduces that asymmetry against the installed source.

The fix preserves only an already enabled **and active** session for a missing
sample at most 50 ms old. It requires the exact accepted mode, generation,
physical counter and epoch, healthy Panda and vehicle configuration, qualified
input, and no fault or barrier. Invalid received messages are still rejected.
No cached button is replayed, new ACK issued or PRE_ENABLE promoted. A longer
gap or permission loss still disengages, and recovery never auto-engages.
The strict fresh-sample condition remains in the separate intent replay path.

`lx3_active_carstate_gap` and `lx3_active_reject` record the actual branch and
input ages to distinguish future field failures. Other vehicle paths, native
safety limits, steering output, DBC and HUD contracts are unchanged.

Tests cover both modes, observed publication gaps, the 50 ms boundary, repeated
misses without renewing the age, invalid input, all accepted identity components,
Panda failure, normal barriers, cached buttons, recovery and inactive sessions.
The Linux smoke also exercises the actual receive method on a real empty 20 ms
IPC socket, followed by the production LX3 adapter. Its judgement clock is fixed
to test the age deterministically; this is not a scheduler latency measurement
or a complete `SelfdriveD.step()`/vehicle closed-loop test.

This does **not** resolve the separate steering-yield or OEM warning questions.
During the first accepted LFA session, `latActive` stayed false and no host 0xCB
was produced while driver-torque yield remained asserted. During a later combined
session, camera FCA then LSS/DAS faults appeared, with no Panda TX blocks. Their
cause, and any indirect contribution from host/OEM ownership changes, remains
unconfirmed. Build success is not permission to claim those faults are fixed.
