# Hyundai Palisade LX3 HEV (2026MY) Port — Phase 2

**Base**: Carrot2-v9 (upstream commit `ce355250be`, 2025-03-07)
**Branch**: `lx3_hev`
**Vehicle**: 2026 Hyundai Palisade Hybrid LX3 (VIN KMHRK81ADTU019104)
**Device**: comma C3X-lite (no driver camera)

## Phase 1 → Phase 2 동기

기존 작업 브랜치 `benign07/openpilot tree/210_c1`에는 v1~v24 누적 patch 24개. 코드 추적 어렵고 carrot upstream의 새 기능(monitor_fingerprint 등) 부족. Phase 2는 **Carrot2-v9 최신 base에 LX3_HEV 차량 차이만 minimal patch**로 깔끔히 재구성.

## 적용된 minimal patches

### 1. `opendbc_repo/opendbc/car/hyundai/values.py`
- `HYUNDAI_PALISADE_LX3_HEV` HyundaiCanFDPlatformConfig 추가
- flags: HYBRID | ANGLE_CONTROL | CANFD_ALT_BUTTONS | ALT_LIMITS
- CarSpecs: mass=2215, wheelbase=2.97, steerRatio=16

### 2. `opendbc_repo/opendbc/safety/safety/safety_hyundai_canfd.h` — 0x105 ignore
- `HYUNDAI_CANFD_COMMON_RX_CHECKS`의 0x105 RxCheck를 `max_counter=0 + ignore_counter=true + ignore_checksum=true`로 변경
- LX3_HEV의 0x105(ACCELERATOR_ALT_HEV)는 byte[2] 카운터가 +2 step 증가 (다른 차량은 +1). panda가 +1 기대해서 50/sec invalid → controlsAllowed False loop 발생 → 우회

### 3. `opendbc_repo/opendbc/safety/safety/safety_hyundai_canfd.h` — 0x2af bus 0
- HDA2_TX_MSGS와 HDA1_TX_MSGS에 `{687, 0, 8}` (STEER_TOUCH_2AF on ECAN) 추가
- carrot upstream commit `fafdb3e`가 STEER_TOUCH_2AF를 ECAN으로 송출하는데 panda는 bus 2(CAM)만 허용 → 모든 0x2af TX 차단(TxBlocked 9.97/s) → 1행 추가로 해소

### 4. `opendbc_repo/opendbc/car/hyundai/carstate.py` — LFA_ICON 분기
- L570: `cp_cam.vl["LFAHDA_CLUSTER"]["HDA_LFA_SymSta"]` → CCNC_0x161 fingerprint 분기
- LX3_HEV는 LFA_ICON이 ADRV_0x161에 있고 LFAHDA_CLUSTER는 항상 0. fingerprint 분기로 다른 차량 영향 없음

### 5. (TODO) `opendbc_repo/opendbc/car/hyundai/fingerprints.py` — FW + CAN signature
- LX3_HEV의 정확한 FW signature는 디바이스 SSH로 추출 후 추가 예정
- 지금은 platform_config만 있고 FW 매칭 entry 미작성 → 디바이스에서 매칭 실패 가능
- 임시 우회: CarFingerprint param에 "HYUNDAI_PALISADE_LX3_HEV" 강제 설정 또는 FW skip 모드

## Phase 1 누적 patch 자연 처리

carrot upstream Carrot2-v9에 이미 있어 **별도 patch 불필요**:

- v7 (CRUISE_BUTTONS_ALT msgs sub) — `monitor_fingerprint` 동적 등록으로 처리
- v19 (CRUISE_BUTTONS_ALT2 + 분기) — carrot 원본에 이미 보유 (carstate L255, L559, L669, L696)
- v19/v22 carcontroller (`if not CC.latActive`) — carrot 원본 L258에 이미 있음
- v20 controlsd (`lateral_enabled = driving_gear`) — carrot 원본
- v24 stock LFA suppress (`create_suppress_lfa`) — carrot 원본 L242 (단 호출 조건 확인 필요. carrot은 `not camera_scc`만 호출 → LX3_HEV는 camera_scc=3 모드라 미호출)

**Phase 1 누적 patch 중 Phase 2 추가 검토 항목**:
- v24 stock LFA suppress 호출 조건 확장 (Phase 1과 동일 fix 필요할 수 있음)
- v23 main_button alt2 분기 제거 (LX3_HEV의 0x10B byte 10 = 0x88 mismatch — carrot upstream에 영향)

## 검증 단계 (디바이스 적용 시)

1. fingerprints.py에 LX3_HEV FW 매칭 entry 추가
2. 디바이스에 새 브랜치 push + checkout
3. scons 빌드 (특히 panda 재플래시 필요)
4. 정차 시동 ON → check_v24.py 같은 부작용 모니터링
5. 짧은 운행 → controlsAllowed/engage 안정성 확인
6. 사용자 보고 비교 — Phase 1 v24 운행과 동등 또는 더 안정 여부

## 알려진 위험

- HDA2 + camera_scc=3 동시 모드는 carrot upstream도 명시적 가정 안 함 → 일부 분기에서 LX3_HEV 추가 분기 필요할 수 있음
- 새 base에 monitor_fingerprint 메커니즘 있어 동적 메시지 등록되지만 LX3_HEV의 비표준 메시지(0x161 LFA_ICON 등)는 carrot upstream이 모름 → patch 4가 정확
- panda safety가 stock과 다른 hyundai_canfd_alt_buttons + camera_scc 동시 동작 검증 안 됨
