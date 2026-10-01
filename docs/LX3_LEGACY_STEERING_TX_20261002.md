# LX3 steering format boundary

The supported LX3 uses LFA_ALT0xCB for active steering. Exactb60 native C still
accepted shared legacy LKAS0x50/0x110 TX with controls_allowed=false, and an
active0x12A +1023 torque request with isolated granted permission. Those formats
were outside the guarded0xCB angle envelope. This is a desktop policy finding,
not evidence that these packets were emitted by the vehicle's host or caused
the earlier LSS/DAS warning.

LX3 now rejects host0x50/0x110. The existing passive0x12A companion remains:
STEER_REQ0, LKAS_ANGLE_ACTIVE0, LKAS_ANGLE_MAX_TORQUE0, torque0 or the existing
Carrot-1024 sentinel. Other vehicle policies stay unchanged. Host emergency
templates with active0x12A fields are not duplicated. Original OEM steering
frames retain their bytes and are not replaced by a queued passive companion.
The normal0xCB command, physical request/host ACK, and steering bounds are unchanged.

An initial alternative removed all0x12A host output. The existing raw-bit79
compatibility test caught that regression. That alternative was discarded;
the final policy preserves passive companion behavior and original bit79.

The same C test fails on exactb60 and passes on the candidate:24 guarded active
TX cases,12legacy190 cases,640 passive-field combinations, neutral companion
acceptance and original forwarding checks. The full native suite passes too.
Permission grants used by this isolated test are explicit fixtures, not physical
button/host qualification. Final Python and native host schedule results are
recorded separately. New exact GitHub CI/H7 and vehicle qualification are pending.
