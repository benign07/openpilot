# Original-device transport comparison

This sealed archive contains source files from the owner's original-device
backup, paired with installed pandad SHA256
02302e2a712168d4ed6532b7f2f589ccf91abacebc63e26b79ee034763019d64.
The manifest binds every file and the original ELF's required ABI versions.
No recordings, location history, actual CAN captures, device Params values,
credentials or installed binary are included. Existing source licenses apply.

The original native 316-key Params header is preserved. The independently
installed Python 324-key haptic extension/header are not replaced by this
native candidate. The original car schema, Panda host state machine, authority
payloads, CAN packet protocol and firmware remain unchanged.

Profiles:

- baseline: archived original source. A reproducible source baseline, not a
  claim of binary-identical reproduction of the installed compiler/linker
  environment, and not a replacement to deploy.
- observed: original native profile with guarded SPI response bounds, bounded
  recovery/retries, two-per-call malformed-length fast failure, stage and
  endpoint counters. Lock waiting is separated from failed-I/O retry budget.
- turnaround: the same generated source with a compile-time ready profile:
  HACK-to-DATA 400us, CAN bulk transaction spacing 400us, post-0xdc 1000us.
  There is no control-to-control 1ms rule. This is an experimental artifact,
  blocked from automatic installation pending original-device cadence evidence.

All other native object files are compared after stripping debug information.
No Panda firmware is built or flashed. None of these PC/ARM tests qualify road
operation or establish the cause or resolution of the OEM LSS warning.

The 13-call inert control-mix test quantifies added policy delay, not real
hardware latency or scheduler contention. Real CAN/sendcan cadence, endpoint
lock wait, final failures, response length errors and unsaturated MCU counter
slope must be checked while parked before a ready profile can be selected.
