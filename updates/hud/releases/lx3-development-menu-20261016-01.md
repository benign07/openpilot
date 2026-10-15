# Development menu source update

Source `d991d0b9ca8b1a60d0754819c692877566074f32`. Only `selfdrive/carrot/server/services/settings.py` changes on the device. Its before hash `e39c6705fe1ba0f19ab5eca47faa3ace42a7c1d1f2e9e38e89d391a4a149e78c` was read and verified after actual OTA11 installation, along with all thirteen OTA11 files/backups and unchanged native pandad. Installed OTA11 reached complete.

[Exact CI](https://github.com/benign07/openpilot/actions/runs/38822536726) all3 success, Python3.11/3.12 each303 passed; schema2 passed. Actual native regression job passed; no planner or native production change since OTA11. Logs/three artifacts were downloaded and matched GitHub API SHA256 values.

The first settings category is Development features (개발 중 기능), group New features (신기능 모아보기), containing launch following, within-lane margin and prior approach/flexible-gap tuning. Parameter names, defaults, limits and stored values remain identical. Existing menu items have no omissions/duplicates; repeated injection is idempotent. Profile/favorite lookup remains by the same parameter names. Existing group id lx3_preferences_group is retained.

New amounts still default0 and shadow1. Menu movement never enables controls. Owner-requested10percent/5cm/5cm/shadow0 activation is a separate parked-only action. Existing P/zero/inactive, source signature/hash, fingerprint, backup/boot and rollback checks are unchanged. No direct running-source replacement or forced reboot.

Same persistent Claude session was requested for targeted medium review, but actual CLI returned weekly usage limit without reading files. It is not counted as a new review pass. The menu change was directly reviewed and covered by metadata-contract/regression tests. Prior control review belongs to OTA11, not a fresh menu review.

OEM steering-assistance warnings and SPI remain unresolved. No road qualification is claimed. This is for the additional LX3 OTA11 lineage, not the protected original or EV9.
