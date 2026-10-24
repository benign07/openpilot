# LX3 routine haptic options (candidate)

The owner confirmed a physical steering-wheel vibration at engage on the
original LX3 on 2026-10-24 after `lx3-original-haptic-20261023-02`. This is
functional feedback for that event, not qualification of every alert/pattern.

The first Development features / Steering-wheel haptics menu contains the
existing master switch followed by six event switches and a pattern selector.
The switches select existing audible events; they do not create events or
grant control permission. Defaults preserve the previous ten-event set.
An active controller, valid CAN and no blinkers remain prerequisites.
Consequently a lane-change alert while blinkers are on is deliberately muted.
Reverse and navigation events also do not request haptics with control inactive.

Pattern 0 retains the verified engage request: one 0.8 s pulse. Pattern 1 is
0.3 s on, 0.2 s off, 0.3 s on. Pattern 2 is three 0.2 s pulses with 0.1 s gaps.
All end within the original 0.8 s window. Patterns 1/2 are experimental: a
motor/controller may not reproduce a short binary pulse as expected.
Changing settings cancels the active routine request without replaying it.
The original speed-camera setting and its independent pattern are unchanged.

The confirmed DBC field is a one-bit `CCNC_0x162.VIBRATE`. There is no verified
amplitude/intensity command. These options control timing, not strength.
No torque/angle oscillation, unknown CAN-bit write, fault masking or authority
override is added. Critical acoustic alerts and monitoring are unchanged.

The original matching ARM Params profile grows from 317 installed keys to
324. The original 316 descriptors and the master switch descriptor are
preserved; six persistent BOOL defaults are true, one INT pattern defaults 0.
Existing 317-key modules use previous defaults but cannot expose the new keys.
Native deployment requires its own verified signed/parked transaction; this
is not covered by a Python-only HUD source OTA. No device installation is
claimed by this document or by a source-branch push.

Steering-system cluster warnings and SPI transport errors remain independent
open investigations. The latest original-device read showed the legacy SPI
counter at its 65535 saturation limit; a constant saturated value cannot
measure the current error rate. No MCU firmware or pandad replacement belongs
to this settings candidate.
