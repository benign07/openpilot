# Passive EV9/LX3 comparison capture

The automatic recorder shares the LX3 diagnostic row and chunk format. It
starts with the Carrot web server and records only while `deviceState.started`
is current. It subscribes to existing `can`, `sendcan`, Panda, and vehicle-state
messages; it does not publish CAN or change vehicle settings.

On EV9, a received CAN-FD `CRUISE_BUTTONS` frame (`0x1CF`, 8 bytes) or its
alternative layout (`0x1AA`, 16 bytes) on E-CAN bus 0 or 1 can open a bounded
button trace. The decoder follows this branch's `hyundai_canfd.dbc` definitions.
Only physical RX bus numbers 0/1 can trigger a trace; `sendcan`, Panda echo,
and rejected-TX frames remain observations. When the device is not attached to
the EV9, the vehicle fingerprint is unavailable and button triggers are idle.

The recorder stores sealed, hash-described chunks under
`/data/community/automatic_drive`. Read-only endpoints are
`/api/automatic_drive/status`, `/api/automatic_drive/chunks`, and
`/api/automatic_drive/chunks/{id}`. On the PC, `tools/can_auto_sync.py` downloads
and checks them; `python -m openpilot.selfdrive.carrot.can_diagnostics.button_trace_report
<chunk-directory>` produces a button timeline. Match each device's source
hashes, car fingerprint, boot ID, and monotonic times before comparing a run.
EV9 forwarding pairs are deliberately not inferred from LX3 bus conventions.

This port preserves the installed EV9 control source. The EV9 and LX3 control
builds have different Git commits; matching diagnostic schema does not make
their vehicle-control implementations equivalent. Road behavior and button
wiring still require a real EV9 recording to validate.
