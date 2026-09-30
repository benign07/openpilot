# LX3 전체 빌드·과거 CAN 재검토 — 2026-09-30

현재 개발 브랜치는 `fix/lx3-engagement-state`이다. 개발 코드와 실제 설치된 코드는 별개다. 이번 검토 중 차량 설치·재부팅·OTA 배포를 하지 않았다. 생산 OTA 기준은 `hud-device-updates-20260928`의 `e0747fcad651d273a55b4dc157195d37adffe9cf`이며 개발 후보의 `dashcamOnly` 인터록은 유지한다.

**현재 후보를 정상 인게이지·계기판 경고 해결 완료로 판정하지 않는다.** 가장 큰 미확정 사항은 순정 카메라 `0x162`의 `FAULT_LSS` 의미이다. 현재 호스트 판정은 과거 활성화 버튼 해제 119회 중 113회를 차단한다. Panda 물리 권한 검사가 통과하는 것과 호스트가 실제 세션을 받아들이는 것은 서로 다른 검사다.

## 1. 확보 자료의 범위와 한계

PC의 `주행데이터`, `device_backups`, `scc_button_route_00000042`에서 rlog 79개를 발견했다. SHA-256 중복 1개를 제외한 78개를 검토했다. carState가 관측된 구간 길이 합계는 5,249.11초(약 87.5분)다. 6개는 끝부분이 잘려 있어 읽을 수 있는 완전한 앞부분만 사용했다. 누락 구간을 정상 동작으로 간주하지 않는다.

| 항목 | 관측 수량 |
|---|---:|
| 전체 RX CAN(전달 echo 포함) | 50,332,576 |
| sendcan 요청 | 2,409,513 |
| 원본 물리 버튼 `bus 0, 0x10B` | 132,341 |
| main 해제 | 70 |
| LFA 해제 | 28 |
| RES / SET 해제 | 9 / 12 |
| 물리 CANCEL raw=4 | 0 |
| CRC 유효 원본 MDPS `bus 0, 0xEA` | 529,263 |
| CRC 유효 원본 카메라 `bus 2, 0x162` | 105,867 |

물리 입력 helper에서는 132,106개가 정상, 초기 중립 확인 204개, 연속성 이상 24개, 모호한 동시 입력 1개, 중립 재확인 2개, 기록 순서 역전 4개였다. 과거 파일의 publisher 간 기록 순서는 단조 증가하지 않아, C 재생에서는 기록 시각으로 안정 정렬했다. 초기 정렬 전 결과는 최신 근거로 사용하지 않는다.

MDPS `STEERING_ANGLE_2`의 DBC 계수와 호스트 부호 처리를 교차 확인했다. 30ms 이내 carState 대응 508,375개 중 1도 이내 일치는 493,918개, 5도 이내는 506,920개다. 이는 단위·부호의 대규모 교차 근거이며 모든 움직임·고속 경계·EPS 응답의 실차 검증을 대신하지 않는다.

## 2. 발견하여 수정한 권한·설정 충돌

### 가속페달 설정과 Panda 정책 불일치

기존 card는 LX3 `alternativeExperience=0`을 고정했지만 호스트는 실시간 `DisengageOnAccelerator=false` 설정을 사용했다. 이 조합에서는 가속페달을 밟을 때 호스트가 세션을 유지하면서 Panda가 권한을 철회할 수 있다.

LX3는 시작 시 설정을 표준 `ALT_EXP_DISABLE_DISENGAGE_ON_GAS=1`로 CP/Panda에 전달하고, selfdrived도 동일 CP 정책을 사용하게 했다. 설정 변경은 다음 제어 시작에 적용한다고 UI에 설명했다. 가속페달 해제 설정이 꺼져 있으면 lateral 세션은 유지하지만 longitudinal 자동 제어는 override 이벤트로 중단한다. 다른 차종의 기존 정책은 유지한다.

### ACC 보조 상태가 세션을 되살리는 경로

`create_acc_control_scc2`의 기존 softHold/Carrot cruise 보조 상태가 disabled 또는 lateral-only 상태에서도 ACCMode/가속을 만들 수 있었다. LX3 전용 guard에서는 accepted 세션의 enabled 값만 사용하고, 비활성·가속페달 override 시 두 가속값과 StopReq를 0으로 만든다. 다른 차량의 softHold/Carrot 동작은 변경하지 않았다.

Panda에서는 물리 combined 권한이 있고 가속페달이 눌렸으며 두 가속값·StopReq가 모두 0인 `ACCMode=2`만 override 유지 메시지로 허용한다. 가속·정지 요청이 남거나 cancel 뒤 재사용되는 메시지는 거절한다.

추가 C 재현에서 `ACCMode=0`, 두 가속값 0이지만 StopReq가 1/2/3인 메시지가 비활성 keepalive로 허용되는 문제를 확인했다. `StopReq=0`까지 비활성 조건에 포함해 수정했다. 정상 OFF 유지 메시지를 허용하는 검사와 잔여 StopReq 거절 검사를 함께 통과했다. 수정 전 실패 로그는 PC의 `can_inventory_work/lx3-stopreq-repro-20260930.log`에 보관했다.

## 3. 실제 C 정책과 과거 기록 대조

실제 Panda C `safety_rx_hook`, `safety_tick` 및 `safety_tx_hook`에 정렬된 과거 CAN을 넣었다. `controls_allowed`를 fixture로 켜거나 버튼·MDPS 표본을 합성하지 않았다. 각 파일 시작은 초기화하고, 실제 CAN 무결성과 실제 물리 해제를 사용했다.

| 정책 | 허용 과거 TX | 거절 과거 TX | 권한 없이 허용한 활성 TX |
|---|---:|---:|---:|
| alternativeExperience=1 | 514,860 | 992,976 | 0 |
| alternativeExperience=0 | 505,082 | 1,002,754 | 0 |

현재 코드로 재생한 132,341개 물리 문맥에서 alt=1의 권한 허용 표본은 11,164개였다. alt=0은 6,538개다. 가속페달 정책 차이가 반영된다. 이 재생은 **과거 컨트롤러가 요청한 TX를 새 Panda 정책에 넣은 검사**이며 새 컨트롤러 출력·heartbeat·forwarding 전체·액추에이터 응답을 검증한 것은 아니다.

과거 설치 버전에서는 250ms 이내 Panda 문맥이 controlsAllowed=false인데 active `0xCB` 송신 echo가 관측된 경우가 48,028개였다. 요청 프레임은 46,837개다. 기존 우회·버퍼 권한 경로를 의심할 근거지만 비동기 상태 시차가 있으므로 EPS에 실제 도착했다거나 계기판 경고의 단일 원인이라고 단정하지 않는다. 새 guarded 정책은 실제 물리 RX 권한을 요구하고 TX가 자가 허용하지 않는다.

## 4. 미해결: 카메라 fault 판정

현재 `opendbc_repo/opendbc/car/hyundai/lx3_state.py:lateral_fault`는 신선한 카메라 0x162에서 `FAULT_LSS/LFA/DAS`가 모두 0이어야 정상으로 판정한다. 과거 원본 105,867개는 모두 CRC가 유효하지만 LSS=1이 94,559개이고 LFA는 모두 0, DAS=1은 1,485개다.

실제 활성화 해제 시점과 가장 가까운 신선한 카메라 상태를 대조하면 다음과 같다.

| 입력 | 전체 해제 | 현재 fault 기준으로 차단 |
|---|---:|---:|
| main | 70 | 68 |
| LFA | 28 | 24 |
| RES | 9 | 9 |
| SET | 12 | 12 |
| 합계 | 119 | 113 |

이 결과는 현재 후보가 과거의 대다수 상황에서 인게이지를 거절할 수 있음을 뜻한다. LSS=1이 실제 고장인지, 기능 비활성·가용성·기존 가상 입력의 결과인지, LX3 DBC 의미가 다른지는 아직 입증하지 못했다. 과거 carState fault=0은 기존 코드가 이 비트를 사용하지 않았기 때문에 반증이 아니다. 비트를 무시하거나 카메라 건강 검사를 없애서 테스트를 통과시키지 않는다.

Claude와 같은 경고 주제를 상호 검토하며 원본 CAN 타임라인을 추가 대조했다. LSS 0→1 전이는 5개이며, 한 파일만 봤을 때 예외였던 r34는 다음 segment 2에서 동일 변화가 확인됐다. 첫 OP active 요청부터 전이까지는 약 0.70/0.70/4.48/8.72/12.55초였다. 모든 전이에서 원본 카메라 CB는 active=1/cap=0, OP 송신 echo는 active=2/cap=25 또는250, 원본 MDPS LFA2_ACTIVE=2였지만 카메라로 가는 MDPS echo는 1로 바뀌어 있었다. 당시 carControl은 latActive=true, enabled=false이며 Panda 허용은 4개에서 false, 1개에서 true였다. LSS 뒤 약150ms에 DAS가 켜져 약14.82~14.87초 뒤 꺼졌다. MDPS의 두 fault는 모두0이었다.

이는 기존 명령 대체와 MDPS 변조가 카메라의 명령·응답 정합성 확인에 영향을 줬다는 가설과 맞는다. 다만 두 변경이 동시에 존재했고 송신 echo는 EPS 도착·토크 실행의 증거가 아니므로 단일 원인을 확정하지 않는다. 후보에서 MDPS 변조를 제거한 것만으로 경고 해결을 예측하지 않는다. camera 원본이 전이 전에도 비활성인 r15b/r31과, 처음에는 active=2였다가 바뀐 r32/r33/r34를 구분한다. segment 시작을 점화 시작으로 해석하지 않는다.

## 4-1. 상호 검토로 재현한 호스트 거절 뒤 모드 불일치

호스트 NO_ENTRY로 LFA 인게이지를 거절하면 호스트 accepted mode는 OFF지만 Panda는 물리 입력으로 LATERAL 권한을 가진다. 펌웨어의 heartbeat 불일치 철회 전 다음 LFA를 누르면 Panda는 OFF로, 호스트는 LATERAL 요청으로 해석한다. 실제 C 물리 RX와 생산 호스트 StateMachine을 연결한 소프트웨어 재현에서 첫 해제의 `(Panda1, host0)`과 다음 해제의 `(Panda0, host pending1)`을 확인했다. 유효한 합성 MDPS/버튼 fixture이며 실제 차량 조작 증거는 아니다. 타임아웃 뒤 호스트가 자동으로 살아나지 않는 것도 확인했다.

Claude의 첫 heartbeat bool ACK 패치는 이전 true를 새 세션 승인으로 오인하고 빠른 OFF 입력 의미를 바꾸는 반례가 있어 적용하지 않았다. 재검토에서 Claude도 철회했다. 실제 heartbeat 송신은 pandad10Hz이며, 보드의 불일치 누적 판정이1Hz다. 권한 부여·전환을 확인하려면 세대와 모드를 구분하는 protocol이 필요하며 즉흥 타이머로 해결했다고 보고하지 않는다. OFF/CANCEL 즉시 철회, 이전 ACK 재사용 금지, lateral→combined 별도 승인, 늦은 ACK 무효를 설계 기준으로 남긴다.

별도로 비활성 대기 상태에서 controlsMismatch를 상시 추가해 engageable을 잘못 표시하는 문제는 수정했다. 실제 요청·활성 중 권한 철회·잘못된 firmware·stale에 대한 거절은 유지하며 3개 회귀로 확인했다. 이 수정은 카메라 fault 또는 위 모드 불일치를 해결한 것이 아니다.

기존 LX3 자동 진단의 5Hz 표본에 관측 전용 `host_disabled_panda_allowed`를 추가했다. 정상 pending ACK와 메시지 시차도 true로 관측될 수 있으므로 오류·승인 근거로 쓰지 않는다. 두 서비스의 원본 `mono_ns`는 기존 sample에 함께 저장한다. 250ms freshness, 정확한 bool과 active Panda 모델을 요구하고 stale/missing/silent/noOutput-only는 unknown(None)으로 남긴다. 제어 권한을 변경하거나 수집 빈도를 늘리지 않는다.

## 5. 전체 빌드에서 확인한 누락

- 고정된 raylib 5.5.0.4의 LoadFontData 인자 수와 폰트 생성 코드가 달랐다. native CFFI 시그니처를 확인해 6/7개 양쪽 API를 지원하도록 수정했다.
- 저장소 driving ONNX가 133-byte LFS pointer여서 모델 컴파일이 실패했다. PC의 동일 원본 모델 60,792,584바이트를 기존 chunk 포맷으로 복원했다. SHA-256은 `f73a9e535523d5e9acb9e642c64e33d631825dc8ba74123757d107cedd047bb5`이며 Git blob을 재결합해도 동일하다. 모델 내용 변경은 없다.
- loggerd의 두 C++ 지정 초기화가 구조체 선언 순서와 달라 GCC 빌드가 실패했다. 필드 값을 바꾸지 않고 순서를 수정했다.
- Linux x86_64 런타임 전체 의존 그래프의 `scons --minimal` 컴파일과 card/selfdrived/controlsd/automatic diagnostics의 실제 import가 완료됐다. H7 ARM firmware는 별도 job에서 컴파일했다. 기기용 호스트 전체 빌드·보드 실행은 별도 확인 대상이다.
- 실제 msgq/Capnp/StateMachine smoke를 추가했다. 기존 테스트 fixture가 Panda ACK를 너무 빠르게 보내 정상 frequency 검사를 실패시킨 문제를 수정하고 실제 10Hz cadence를 사용한다. 이 검사는 전체 프로세스·차량 폐루프 시험이 아니다. 최신 CI 결과는 아래 검증 기록에서 관리한다.

`549214ca`의 [GitHub 실행 36727214294](https://github.com/benign07/openpilot/actions/runs/36727214294)는 전체 런타임 빌드·실제 IPC·H7·C·Python3.11/3.12의 5개 job 모두 성공했다. 이후 idle 표시와 관측 필드 수정에 대해서는 새 커밋의 CI 결과를 별도로 확인한다.

호스트/입력/HUD/진단/시간 테스트는 후속 회귀 포함 156개이며, Windows strict C/undefined-behavior sanitizer 회귀와 guarded software audit10개도 통과했다. 기존64개 flag 조합의 구성·기본 전달·CRC/카운터·거절 부작용 검사를 유지한다. 테스트 수나 audit 성공은 실차 준비 완료를 뜻하지 않는다.

## 6. 직접 버튼 조작 없는 다음 정차에서 할 수 있는 일

시동 후 신선한 P·속도 0·비활성 상태를 확인하고, 원본 bus0/2 CAN과 Panda/호스트 문맥을 자동 기록한다. 0x10B CRC/+2 counter/주기, 0xEA 측정 각도·토크·고장, 0x162 LSS/LFA/DAS 상태와 전환, 원본 0xCB active/angle/max-torque, 원본 0x1A0 ACC 상태를 설치 소스·CP·firmware signature와 함께 대조할 수 있다. 버튼·조향·가속 명령을 합성해서 송신하지 않는다.

이 수집으로 새 부팅의 기본 fault 상태와 신호 정합성·진단 자동 저장을 검증할 수 있다. 실제 LFA/SCC/CANCEL 조작이 없으면 새 모드 전환이나 최초 계기판 경고 소멸은 확인할 수 없다. 우선 카메라 상태 의미를 좁히고, firmware/host 대응, 속도별 제한, OEM 긴급 fallback/제어 소유권과 실제 버튼·EPS 응답 검증을 마친 뒤 OTA 후보를 결정한다.

## 근거 파일

PC `주행데이터/engagement_audit_20260930/all_raw/`의 `summary.json`, `native-summary-alt1.json`, `native-summary-alt0.json`, `camera-gate-summary.json`, `camera-onset-summary.json`, `host-panda-denial-repro.json`에 파일별 수량·해제 시점·재생 결과와 범위를 남겼다. 재현 스크립트는 PC `can_inventory_work/`의 `audit_all_lx3_archives_20260930.py`, `replay_all_lx3_native_archives_20260930.py`, `audit_lx3_camera_gate_20260930.py`, `audit_lx3_camera_onset_20260930.py`, `repro_lx3_denied_host_panda_20260930.py`다. 원본 자료·수집 개인정보·인증값은 Git에 넣지 않는다.

실제 Claude Code CLI의 동일 세션으로 3차 상호 검토를 완료했다. Codex의 수정·근거 → Claude의 반론 → 실제 C/CAN 재현 → 양쪽 재검토 순서로 진행했다. heartbeat 패치·과거 기록 해석·neutral-gas 설명의 반례를 교정했고, 확정된 idle 표시·관측 필드를 검증했다. raw 응답 JSON은 PC `can_inventory_work/claude_lx3_*result_20260930.json`에 보관했다. 계정·인증 자료는 포함하지 않는다.
