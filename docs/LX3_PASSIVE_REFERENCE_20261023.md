# LX3 original-device passive reference

The original device (serial 80acfe79) is a separate observation reference.
The additional device's signed ADAS diagnostic release 17 remains unchanged.
Different native binaries, schemas and devices mean this is not a controlled
single-variable A/B comparison.

Use the existing OpenpilotEnabledToggle=0 while offroad, then restart. The
installed card reads it during initialization and sets CarParams.passive.
It skips controls_update, including CarInterface.init and regular vehicle
control/display generation. Do not replace controller, Panda, DBC, schema,
native programs or driver-monitoring policy for this experiment.

Offroad Panda noOutput is expected. Onroad it may remain in the startup ELM327
mode awaiting ControlsReady; inspect the actual installed behavior rather than
claiming noOutput from the host CarParams alone. Fingerprinting may generate
VIN/FW diagnostic requests before passive selection. Distinguish these from
actuator/display traffic and inspect post-identification sendcan and echoes.
Do not infer the physical transmitting ECU from bus number alone. With relay
bypass, raw traffic may move/duplicate between observed buses and the normal
camera parser can become invalid even when raw recording works.

The two diagnostic Python files retain bounded capture and add compatibility
for the original native key registry and absent approach_comfort module.
Unsupported settings are recorded explicitly, never created or bypassed.
An internal import failure in an installed preference module remains an error.
Metadata records the host passive/safety configuration as host intent only;
Panda observations are separate evidence of its actual reported mode.

Enable the existing road-camera recording option and keep native logging on.
Retain stock oldest-completed-first log/video deletion. The diagnostic store
also bounds its own size and retires only verified completed chunks. Pull logs
promptly; retention is not an archival guarantee. Office execution cannot
verify camera encoding, ignition recording, physical relay or vehicle response.

Before a vehicle test: stationary, confirm passive mode, choose the vehicle's
desired lane-safety/haptic settings, then use ordinary manual driving and stock
assistance. Record naturally occurring warnings/vibration by timestamp after
stopping. Do not deliberately cross lanes, release the wheel, change assistance
settings while driving, or synthesize CAN commands to provoke a response.
An observed VIBRATE field is a candidate correlation, not proof of motor output.
Fault state can persist across restarts; clearing it is not part of this patch.

Deployment to the original device is serial- and before-hash-bound, separately
signed, and performed only offroad with ignition absent and control processes
stopped. Save both source originals and changed settings for rollback. Reboot
and verify hashes, native preservation, passive setting and recorder health.
Do not move the additional-device shared HUD release pointer for this separate
reference installation.
