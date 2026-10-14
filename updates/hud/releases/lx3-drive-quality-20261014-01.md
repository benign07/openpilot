# LX3 detailed evidence and video review source release

This release adds passive observations and prepares PC video/log evaluation.
It preserves the installed approach settings and control calculation. It does
not claim to resolve the OEM steering warning or the native SPI problem.

## Exact source and validation

- Source: `f67c953cabf1a8d2970b13705a65f6f8f7b83a13`.
- [Exact source CI](https://github.com/benign07/openpilot/actions/runs/38506557927): all three jobs succeeded.
- Python 3.11 and 3.12: 195 tests each. Windows: the same 195 checks passed.
- Real wire-schema test includes nonempty thermal arrays and the thermal enum.
- Linked Acados: capture on/off produced identical arrays and solutions over
  20 synthetic cycles. The actual CarrotPlanner/LongitudinalPlanner builder,
  immutable snapshots, diagnostic exception isolation and qualified single
  problem replay were exercised. This is PC evidence, not road qualification.
- [HUD exact main build](https://github.com/benign07/carrot-hud/actions/runs/38506070393):
  54 JVM tests and 13 PC receiver tests passed; APK build and publication passed.
- [HUD 1.0.35](https://github.com/benign07/carrot-hud/releases/tag/v35): public APK
  bytes match the exact CI artifact. Its fixed certificate, v2 signed-data
  signature and content digest were checked; modified content was rejected.
- Actual Claude CLI Opus 5.5, xhigh effort, reviewed this same topic and the
  counterproposals. Thermal serialization, video route identity/coverage,
  failure caching, reserved primary storage, pairing races and LAN/Tailscale
  scheduling objections were resolved or recorded as limitations.

## Installation boundary

The signed bundle contains six existing Python files and one existing web JS
file. Python before-hashes come from the verified sequence-8 publication. The
unchanged web before-hash comes from the retained installed-source manifest and
matches the current branch base. A different installed file causes rejection.

No new source file, native program, H7/Panda, SPI binary, model, DBC/schema,
updater/bootstrap, OS, MDPS handling, monitoring or driving parameter is included.
The separately prepared native SPI candidate remains uninstalled.

Signed/hash-bound installation, fresh continuous P/zero speed/inactive control,
backup, boot transaction and post-boot checks remain unchanged. A queued request
is not an installation result. App source rollback is not an OS/firmware rollback.
Publication, installation and road verification are separate states. This record
confirms publication preparation and PC validation only, not vehicle installation.
Only the additional LX3 with the sequence-8 file hashes is the target; EV9 is on hold.

## Recording and interpretation limits

Snapshots and MPC-array copies are timed within the existing planner cycle;
serialization follows plan publication. The combined 5 ms budget skips evidence
and backs off when exceeded. This is not a hard real-time timing guarantee.
There is no second on-device solver, CAN command or video-analysis process.

The first-onroad thermal snapshot covers the collector's own observed samples,
at most 120 seconds at 1 Hz, and records whether collection began already onroad
and whether the worker restarted. It does not cover kernel boot, every hot
restart, or permanent offroad followed by loss of power. Thermal thresholds are
unchanged. Aggregate CPU utilization does not identify a process as the cause.

Video requires a consistent clock mapping, matching route stamps, no conflicting
encoder identities and complete frame-index coverage. Missing coverage stays
unverified, including possible startup segment loss. Recorded rlog indexes may
provide coverage absent from sampled diagnostic chunks. No interpolation fills
gaps. Model lane center and acceleration-derived jerk are observational proxies.

PC retrieval uses a fixed Tailscale endpoint and local chunk route names. The
current route's newest video waits until a subsequent route. Offline retrieval
does not prevent a report from existing records. New media work is bounded;
source/MP4 SHA caches avoid repeat decoding, and unchanged failures are skipped.
Secondary media retains a 3 GiB reserve, above the configured 2 GiB receiver
reserve. Slow links may leave videos pending. Old data are preserved. Video
hashes identify received local bytes; they are not remote signed hashes.

The evaluator separates lead motion, stop release, positive command and actual
movement. 30/40 ceiling equality concerns the recorded input only. Earlier state
divergence, missing records, baseline mismatch or interventions prevent broader
causal claims. Neither subjective improvement nor placebo is automatically
determined. Real-vehicle new logging, phone transfer and installation remain to
be verified after the owner's normal parked update.

Original CAN, rlog, video, pairing files, keys and location data stay outside GitHub.
