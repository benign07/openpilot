LX3 acceleration-suppression status clarity, sequence16.
Source: cadd97de0d690db8540d1d35adb34e3e9af6516b
Changes: UI bit1 now says acceleration suppression rather than physical coasting.
Traffic-only positive ceilings can bind at standstill. The status is not a freewheel sensor.
Existing coast/traffic setting descriptions are retained with an additive, idempotent explanation.
Parameter identities, defaults, ranges, saved values and CarrotHUD API remain unchanged.
No control calculation, start/hold permission, limits, native binary, Panda, MDPS, DBC or DM change.
Object display still works without lateral/longitudinal engagement; freshness/calibration/camera gates remain.
PC577 + separate39 =616 PASS. Exact-source CI39239080335 all3 success; artifact API/inner SHA verified.
Actual isolated ARM tests: 272 PASS, real UI import, no test CAN TX or real-window proof.
One unsupported UI path was separately deployed while parked and verified before one signed allowlisted settings source file.
Existing key, signature, hash, vehicle, continuous parking and boot transaction checks were preserved.
Captured initial apply boot: 1f1a6377-11e6-4bd0-b4ef-a4dead188c82. All14 cumulative runtime hashes verified.
Current independent health-check boot: 9c2d5e23-a02a-46ac-ad07-a926ac329413.
Initial apply-boot continuous health was interrupted by loss of connection; current-boot health and actual updater complete are separately verified.
Native pandad unchanged; CAN valid/Panda faults empty/eight processes stable for30.2seconds.
Actual updater independently reached complete; no OEM-overlay warning in this boot journal.
Physical screen pixels, freewheeling, initial launch delay, OEM steering warning and SPI resolution remain unverified or unresolved.
No fresh Claude PASS: prior persistent-session weekly quota block remains; it is not counted as review.
Original LX3, EV9 and phone APK were not changed.
