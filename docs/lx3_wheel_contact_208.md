# LX3 steering-wheel contact receive profile

The additional LX3 device recorded bus 0 CAN-FD `0x208` (16 bytes) while the
driver narrated several wheel-contact and release transitions on a watch.
Across nine stable labeled intervals, byte 12 was 38–62 for all 375 held
frames and zero for all 145 released frames. A recorded continuous re-grip
briefly reached 64, so byte 12 must be treated as a magnitude rather than
testing bit 5. The 1,500 captured frames all
matched the Hyundai CAN-FD CRC16/address calculation. At a parked, lateral-
inactive transition, byte 10 moved from 0 through 3 to 4 and bytes 12/13 rose
with contact. The hand-off transition reversed these values. The vehicle's
original `0x2AF` wheel-touch profile was absent from the captured route.

`STEER_TOUCH_LX3` is registered on the original powertrain CAN parser only for
`HYUNDAI_PALISADE_LX3_HEV`. It does not transmit or alter a CAN frame. The
receiver accepts only a 16-byte frame with the observed fixed fields, valid
CRC, a fresh timestamp, and the observed byte-2 sequence increment of two.
Missing, malformed, stale, or out-of-sequence frames revoke contact evidence;
the optional message does not invalidate the vehicle's CAN health. A valid
byte-10 status of 3 or 4 together with byte-12 magnitude at least 32 populates the existing
`CarState.steeringTouch` input. Other Hyundai vehicles retain their `0x2AF`
receive profile. Steering torque and `steeringPressed` are unchanged.

The existing DM2 camera-unavailable path uses a fresh, valid held touch as
continuous response. It still handles release, loss of signal, alerts, and
lockout through its normal policy. A touch after a dropout is not a new
interaction edge until a valid release is observed. This change does not
disable driver monitoring or relax Panda steering authority.

The signal's OEM field names and behavior under all environmental conditions
are not independently documented. The observed 5 Hz / `+2` profile is strict:
a different cadence will fail closed and require new validation before changing
the parser. The archived watch/rlog comparison and CRC evidence are local under
`can_inventory_work/lx3_touch_research_20261010`; private route data is not
included in the repository.
