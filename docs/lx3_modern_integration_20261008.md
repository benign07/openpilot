# LX3 modern Carrot development candidate

This branch starts from stock Carrot `e74e6938ddc4574b1ba19ee7405ff146f3c1728e`
and carries the LX3 authority implementation from `75b5d824bd13f1a5d4225f36edfd769d4b52677b`.
The latter remains the separately pinned vehicle rollback reference. This development
branch is not a claim that the intermittent OEM steering/cluster warning is resolved.

## Paired native and host contract

Stock schema field numbers, multi-Panda abstractions, cluster forwarding and combined
angle handover are preserved. LX3 schema fields are appended rather than reusing stock
ordinals. LX3 uses safety bit 16384, exact profile 16574 and protocol 5. Stock cluster
direct transmission retains bit 2048. Protocol-4 firmware is not compatible with this
candidate. Firmware, schemas, model resources and runtime must be built and installed
as one checked baseline; this is not a source-only phone update.

Physical LX3 button identity, generations, native permission, heartbeat and actual
transmission checks remain linked. Host overlay copies of physical button 0x10B cannot
create a grant. No synthetic button, native ACK, fault masking or forced permission is
added. Model/camera validity and driver monitoring remain stock. Current Carrot retired
DisableDM and has its own no-camera monitoring fallback; this branch does not turn off
monitoring as a compatibility migration.

## Carrot HUD compatibility

The existing app's JSON/live-payload, saved driving-mode, automatic diagnostic archive,
transfer and `/api/hud_update/` request contracts are retained. Effective driving mode
is shown only with fresh, valid service information. Old backends that omit that
information produce an unknown effective mode rather than a guessed value.

The modern layout has a separate source-update channel, keeping the original layout
and rollback-reference channel intact. Signed updates still validate every before hash,
reject native/schema/updater files, and apply before manager starts. Startup retains the
stock repository lock/recovery behavior, while stock overlay updates are excluded when
the signed HUD updater owns the device. Phone source rollback restores only its signed
source transaction; it cannot revert a firmware/schema generation change.

A full baseline installation must archive previous updater staging and install metadata
before assigning the new baseline identity. Do not reuse an old queued release or claim
an old installed release represents this new firmware. Preserve the previous complete
baseline for full rollback separately.

## Passive recording and remaining validation

Automatic diagnostic chunks rotate at capacity instead of dropping input while waiting
for the slower status writer. Storage quota/free-space protection retains existing
records. The logger counts advertised camera streams at rotation boundaries, retaining
the road/wide-only clone fix without inventing an absent camera stream.

Focused desktop native/header/register-model and host/passive checks do not establish
Linux IPC, ARM firmware/runtime, model execution, live phone compatibility, vehicle bus
timing or closed-loop behavior. The dedicated workflow exercises actual Linux runtime,
Panda firmware, linked serializers/MPC, updater/rollback and camera-topology recording.
Bench installation remains a power-only development validation. OEM warning resolution
requires subsequent matched vehicle data. Driver-style fitting must exclude assisted
driving as training evidence and cannot be declared learned without sufficient data.
