LX3 portable tuning profile API — source-only release verification

Source commit:2305c7598850265f40789a7f26be760a5d242795

Exact native profile CI: https://github.com/benign07/openpilot/actions/runs/40677546434

Exact existing driver regression CI: https://github.com/benign07/openpilot/actions/runs/40677546346

Local backend:94 tests plus151 existing audit subtests; actual CI logs/XML are authoritative.

Client APK and Windows source: https://github.com/benign07/carrot-hud/commit/c1f7469fedadc915b0b02ea8e21043660b4009a0

Scope:five existing Python settings-server files only. No controller, Panda, MDPS, DM, model, CAN generation, tuning value or personal preference changes.

Portable schema1:longitudinal27-key domain; five built-in candidates, explicit preview, exact device and current-value binding, conditional readback and recovery; no retry after uncertain POST.

Profile journal/archived evidence live outside source at /data/community/tuning_profiles and survive source updates. Unverified archive warning persists until full-domain completion is acknowledged.

Forward OTA publication requires exact native/client CI success, immutable v1 unit/range/default/type digest, identical golden fixtures, and public Windows source hashes for ten fixed production paths.

Tests use isolated Params/files and a test parked guard; they do not qualify real Android, actual Params pyx, vehicle integration, longitudinal outcomes or road safety.

The unit test Request initially lacked query; fixed the fixture. An intermediate Android source truncation caused topic CI40672976692 to fail; corrected, and ce320c92 and98f0930 builds passed. Final release uses the later exact successful source.

Source release expected before hashes:four match previously verified installed sequence19; the existing feature/setting_profiles.py expected hash is pinned to Git37cc and is not freshly device-verified. The unchanged source installer MUST verify all exact before bytes, target fingerprint and fresh P/0/inactive before replacement. A differing device must refuse this bundle.

No device installation/reboot performed in this task; devices offline. Steering cluster warning, SPI and left-curve tracking incident remain unresolved; this API update does not claim to solve them.

Candidate native/ideal comparison is an engineering comparison, not a physical or road qualification. The Windows illustration is not a vehicle dynamics simulator.
