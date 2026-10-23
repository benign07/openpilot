# Original LX3 passive reference

Source ed00cdf412e24eafbb91191de2ce5fdf8bfd6549

CI https://github.com/benign07/openpilot/actions/runs/39945466819 : Python3.11/3.12 each125PASS. PC125PASS. Actual ARM isolated office runtime:15 seconds, no restart/error. Same-session Claude Opus5.5 high26/medium27 cross-review; no fresh blocking defect. Existing missing-CarParams metadata issue corrected with regression.

Original serial80acfe79 only. Independent signed two-file deployment; shared additional-device HUD pointer17 remains unchanged. Existing control/Panda/native/MDPS/DM/DBC/schema retained. Existing stock toggle selects passive mode; road logging is enabled through registered stock settings. Private source/settings backup required. First car test must verify actual mode, raw bus coverage, no actuator/display send after initial fingerprint queries, rlog/video creation. Office smoke is not road/relay/motor validation.

See docs/LX3_PASSIVE_REFERENCE_20261023.md. No deliberate hazardous warning provocation. No steering torque haptic synthesis. No fault suppression.
