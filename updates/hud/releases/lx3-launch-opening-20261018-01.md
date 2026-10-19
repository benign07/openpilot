LX3 launch opening / surrounding display: installed sequence15
Source: cbc8c393a8cdaecb3d41287d2a22f7b61694e69a

Launch allowance starts above target+1m rather than target+2m. Sustained
opening relative-speed growth can use the full configured allowance through
existing slew/speed/cap gates. This is an upper bound, not a forced actuator
command. Physical obstacles, brake/hold, turn limits and control authority
retain priority. Existing setting identity/range/default/API are unchanged.

Previous stable unclassified radar occupancy display is restored, including
stationary/slow targets; walls/barriers can reappear. OEM hidden/type priority
and actual-TX camera projection/neutral markers are retained. This is not a
vehicle classifier or proof of a separately filtered CAN object list.

PC574 plus separate39 =613 PASS; exact-source CI39130496922 all3 success,
Python3.11/3.12 each574+39. Three artifact API digests and contained SHA hashes
verified. Actual native MPC comparisons passed; fixed/ideal test cases do
not qualify physical brake release, ECU behavior, driving comfort or safety.

Recorded older morning inputs changed allowance onset5.20s to2.59s, not
physical launch timing. Actual linked MPC changes were small; ideal peak gap
changed by only about0.043m and ideal start time was unchanged. Real launch
delay and perceived following improvement remain unverified.

Actual ARM correction tests: 269PASS; real UI
module imported in a private scratch prefix, no test CAN transmission.
Two initial test attempts were interrupted when parking was lost; production
source had not changed. The third unchanged-source scratch attempt passed.

Fresh continuous P/zero/inactive controls were checked before deployment.
One unsupported UI path was deployed separately; four allowlisted Python
files used the existing-key-signed boot transaction. Hash/signature/vehicle/
parking/boot gates and OTA allowlist were not relaxed.
New boot: 5371a6ef-8a23-4982-b67c-b882cf7b93e8. All14 cumulative runtime hashes match.
Native pandad, hyundaiCanfd/2238, selected Params and driving preferences
were preserved. CANvalid=true, Panda faults=[]; eight required processes
held stable for30.1seconds.
A later owner reboot was verified separately from the original apply boot.
Current subsequent boot: 6937bfd1-58fa-4410-a1d9-9af2da568b7a.
Subsequent stable process/CAN health: 30.1seconds.
A passive15s sample with control disabled and HDA_MODE2=0 observed valid
original camera and actual bus128 TX-echo display frames. This proves data
availability, not drawn screen pixels or the factory HDA display rule.
The actual updater independently reached complete. No OEM-overlay warning
appeared in the boot journal. Physical cluster/HUD rendering and camera
pixel placement still require the next drive.

OEM steering warning, SPI and front-icon color changes remain unresolved.
No fresh Claude review PASS: recorded same-session weekly quota block.
Original LX3, EV9, phone APK, DBC, native programs and firmware are unchanged.
Publication is a separate state from this installed receipt; publish and
verify the immutable bundle/index/tag/record through the existing helper.
