# SPI NACK candidate, 2026-10-14

The installed driver at `32ff4a1101fc522ce43d44e0989cd52862abc203`
exempts every NACK from its failed-attempt time budget. The same inert-I/O
production-driver probe reproduces an unbounded retry on persistent header
NACK, endpoint-0 data NACK, and CAN-write header NACK. This candidate charges
those failures, including their backoff, and normalizes the private header
classification back to the existing public error code -2.

Only endpoint-3 **data** NACK remains exempt. In the exact retained H7 source,
`panda/board/drivers/spi.h::spi_rx_done` rejects invalid header sync, checksum
or lengths before data handling; CAN queue flow control exists only in the
endpoint-3 data branch via `spi_can_tx_ready`. Its wire NACK also covers data
checksum failure, so the host cannot safely distinguish those causes.

That legacy endpoint-3 data retry remains unbounded: production SPI never
clears `connected`. The disconnect fixture explicitly simulates a false flag;
it does not prove automatic physical-disconnect detection. A blocking kernel
ioctl can exceed the userspace budget. No physical timing guarantee is made.

The existing lock, wire protocol, recovery clocks, ACK timeout, SPI speed,
H7 application, Panda safety, MDPS, driver coexistence, DM policy, schemas,
longitudinal behavior/settings and HUD API are unchanged. USB and Python SPI
paths are untouched. The C++ driver is shared by SPI cars, not LX3 alone.

Control-write callers still use existing behavior: LX3 profile/session/state
readback, MCU heartbeat/watchdog revocation and host safety-config mismatch
checks. Some write return values remain ignored; this is not a new atomic
control-write guarantee. No monitoring or authority policy is added.

Tests compile the actual driver, with inert syscalls and simulated 0.2 ms I/O
plus backoff time. Twelve situations cover normal traffic, length/checksum
rejection, recovery, short and persistent NACK, mixed queue/header rejection,
legacy CAN backpressure beyond the failure budget, and simulated disconnect.
The same probe on the exact baseline must show three specific unbounded-loop
sentinel failures; Linux/ARM additionally run UBSAN. These are PC tests.

The recorded 2026-10-13 drive has zero final SPI transfer failures/timeouts.
This is a latent liveness correction, **not** proof of the field retry/CRC root
cause or the OEM lane-system warning being fixed. Counter categories overlap.

Deploy only through the separately signed native host package with prebuilt
AArch64/TICI pandad, fresh parked checks and exact installed-file hashes.
Ordinary HUD Python source OTA cannot install this binary. Preserve the
verified installed H7 image and satisfactory e09364d4 longitudinal setup.
Do not combine this binary trial with the separate optional-haptic-OFF
experiment: that experiment uses the current binaries and one existing
setting only. LSS/pulse/engagement timing remains confounded; a single clean
drive does not establish cause or resolution.
