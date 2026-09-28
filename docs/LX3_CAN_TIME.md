# LX3 HEV CAN calendar validation — 2026-09-28

실차 기록에서는 기존 DBC의 `0x4EB` / `0x4F0`가 관측되지 않았다. 다른 주소를 전체 비트 탐색한 결과 `0x41B`에서 한국 날짜·시각을 확인했다. 아래 변경은 소스 준비 상태이며, 실기기가 오프라인이라 설치·재부팅 검증은 아직 하지 않았다.

## Evidence

- Deduplicated 159 rlog/qlog files by SHA-256, preferring rlog over the qlog in the same segment directory. Eight files have truncated tails; their intact prefixes were retained and the errors recorded.
- Read 48,973,491 receive-log frames, including Panda echo copies; 30,544,944 were on physical bus indices below 128. There were 372 distinct addresses. This is recorded coverage, not proof of visibility into every vehicle network.
- Inventoried every captured ID without relying on its DBC name. Discovery searched keys with at least 40 sampled frames in the first 156 files, using binary and BCD bit fields in both byte orders, time-of-day/epoch comparisons and independent second-by-second progression checks. Three additional rlogs were included in the full-frame candidate validation. GPS fixes were preferred as wall-time anchors; valid `clocks` records were the fallback.
- `0x41B`: 18,569 physical receive frames in 90 files; all valid calendars, all Hyundai CAN FD CRC matches, all within 2 seconds of their time anchor. No `sendcan` frames for this ID. Most samples are from full rlogs; qlogs provide sparse supplemental coverage.
- Two additional real diagnostic captures contained 1,396 `0x41B` frames; all passed CRC and matched their contemporaneous wall-time anchors within 2 seconds. Demo captures were excluded.
- Recorded dates span April and September 2026. Year encoding is consistent with the independently carried full UTC year; future year/month rollover behavior is tested synthetically, not claimed as observed on the car.
- Original raw CAN, route identifiers, GPS positions, private manifests and replay caches remain on the PC and are not published here.

## Mappings

Offsets below are zero-based Intel/LSB bit positions. Byte fields are binary, not BCD.

| ID / DLC | Fields | Use |
| --- | --- | --- |
| `0x41B` / 16 | H `72:8`, M `80:8`, S `88:8`, month `98:4`, year-minus-2000 `104:8`, day `112:8` | Running Korean calendar (UTC+09:00) |
| `0x417` / 32 | full year `64:16`, month `80:8`, day `88:8`, H `96:8`, M `104:8`, S `112:8` | UTC snapshot for offline corroboration; not a runtime clock dependency |
| `0x367` / 32 | H `203:5`, M `208:8`, S `216:8` | Supporting local time of day; date not established |

All three use checksum bytes 0–1 and an observed rolling byte at byte 2. Unknown flag bits are intentionally left unnamed. ECU identity is not inferred solely from the observed bus.

`0x417` has valid CRCs but its calendar can lag by roughly **303 seconds** while fresh CAN frames continue to arrive. It must not be used as a continuously current clock. `0x367` is a 32-byte CAN FD message here; the legacy 8-byte `LVR12` definition at that address does not fit this vehicle.

## Implementation

- The LX3-specific DBC defines these messages. Existing legacy calendar definitions are retained for other vehicles.
- Only the LX3 HEV parser subscribes to the local calendar. It is optional (`ignore_alive`) with CRC binding explicitly limited to this message. Its raw rolling byte cannot invalidate driving CAN when packets are missed.
- The owner confirmed that this vehicle is used only in Korea. This personal LX3 profile therefore uses fixed UTC+09:00, independent of the absent old `HDA_INFO_4A3` country-code signal and delayed UTC snapshot. A local clock must advance twice before `carState.datetime` becomes available. Missing/frozen local time or a discontinuity clears validation and requires new progression. This profile is not a general timezone implementation for other markets.
- `timed` may recover an invalid boot clock only for this vehicle, with fresh valid CAN, P gear, standstill and all control states inactive. It never uses CAN to override an already valid system clock or current-boot NTP synchronization. Usable GPS remains the preferred source. All epoch-to-date conversions and comparisons use UTC.
- The recovery is once per `timed` process lifetime; after success the valid system-clock guard also suppresses recovery if `timed` restarts. It does not restore elapsed power-off time from a saved date.
- Charger-only operation needs no car connection: AGNOS Wi-Fi/NTP synchronization remains active. Missing car telemetry simply disables the optional CAN fallback; it does not block the time service or internet synchronization.
- Automatic phone-bound diagnostic chunks also include these three raw addresses and decoded `carState.datetime`, so later PC analysis can compare clock sources without manual diagnostic activation. Existing retention/rate limits remain in force.
- No CAN messages are transmitted. Steering, acceleration, braking parameters and the updater's NTP/expiry requirements are unchanged. The CAN fallback starts with `card`/`timed`; it is not available during the earlier pre-manager updater phase.

## Validation and deployment

- 66 offline unit/regression tests cover CRC rejection, missing messages, counter gaps, frozen clocks, date rollovers, KST-to-UTC date conversion, parked gates, GPS priority, charger-only operation and existing HUD/updater behavior. The no-GPS loop is limited to at most 1 Hz even when car telemetry arrives at 100 Hz.
- Production parser/clock logic was replayed against 7,605 sampled bus-0 clock frames from 90 files. It accepted 7,374, all within 2 seconds of the reference; it withheld the others during warm-up and gaps. Each file starts with a new validation state, including sparse qlogs. The six recent September rlogs all produced accepted samples. This is not a full vehicle-process replay.
- The existing GitHub workflow runs these tests under Linux Python 3.11 and 3.12.
- This change includes a new Python module, generated DBC and `system/timed.py`, outside the existing HUD source-bundle allowlist. It requires a parked SSH installation, an atomic backup, regeneration/copy of the LX3 generated DBC, and native startup verification. It is not advertised as installed through `updates/hud/latest.json`.
- On 2026-09-29 the source at `d98782d4` was installed through SSH after a fresh continuous P/inactive gate and a verified private PC backup. Seven runtime file hashes and 154 settings were checked; 20 tests and actual native parser construction passed before reboot. After reboot all ten required processes ran, CAN was valid and the recorder was recording. During a 12-second check, 1,194 calendar samples lagged internet time by 0.15–1.16 seconds and GPS by 0.1–1.1 seconds, consistent with the whole-second field. No steering faults or current alert appeared during that parked check.
- A follow-up bounds the no-GPS service loop and adds its regression test. A controlled next cold boot before NTP while parked remains untested. The verified reboot had NTP available and does not establish that CAN set the system clock. Do not force the system clock backward merely to simulate a cold boot on a live vehicle.

Local analysis: `can_inventory/time_research_20260928/validated_candidates.json` and `implementation_replay.json`. Reproduction helpers are in the PC workspace's `can_inventory_work` folder.
