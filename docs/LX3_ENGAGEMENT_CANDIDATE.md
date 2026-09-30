# LX3 정상 engage 전환 수정안 — 실차 배포 금지

2026-09-30, 개발 브랜치 `fix/lx3-engagement-state`.

이 변경은 **호스트 제어 상태와 CAN 송신 경로의 검증용 구현**이다. Panda 펌웨어와 순정 ADAS 제어 주체 전환까지 완성한 포팅이 아니다. LX3 `dashcamOnly=True` 인터록을 포함한다. 이 브랜치를 기기에 설치하면 주행 제어가 비활성화되므로 운행용 업데이트로 배포하지 않는다. 기존 HUD 배포 브랜치와 릴리스 목록을 갱신하지 않는다.

### 9월 30일 후속 검증 상태

새 **LX3 전용 safetyParam bit 1024**를 추가했다. 기존 190 조합만으로 LX3를 판별하지 않는다. 현재 LX3 후보는 190|1024=1214를 요청하며, 구형 Panda의 190 보고는 호스트 승인을 통과하지 못한다. `dashcamOnly`는 유지한다.

아래의 기존 버퍼·권한 검사 8개는 새 전용 경로에서 모두 통과한다. 이번에 추가한 물리 버튼 허용 경로와 각도 변화율 검사는 2개 모두 실패한다. **검사 8개 통과가 운행 가능 판정은 아니다.** 기존 정책의 `--legacy-audit`는 여전히 5개 실패하며, 다른 차종의 기존 동작을 유지했음을 기존 정책의 안전성 해결로 표현하지 않는다.

## 바꾼 구조

기존에는 `AlwaysLateral && CS.latEnabled`가 전체 engage와 무관하게 조향을 켤 수 있었다. 이번 LX3 경로는 물리 버튼의 요청을 저장하되 실제 활성은 기존 `StateMachine.update(events)`로 결정한다.

```text
물리 버튼 → 요청 모드 → 기존 StateMachine의 진입/해제 검사
                     ↓ 정상 진입했을 때만
        selfdriveState.enabled / active / lx3EngagementMode
                     ↓ 통신·기어·고장 조건 재확인
                 controlsd → CC.latActive / CC.longActive
```

새 `lx3EngagementMode` 값은 0=OFF, 1=LATERAL, 2=COMBINED다. 이 값만으로 토크를 허용하지 않는다. `selfdriveState.enabled`는 조향 단독을 포함한 전체 세션이므로 운전자 모니터링과 Panda heartbeat도 세션에 연결된다. 기존 Carrot/ACC 소비자 호환을 위해 LX3 `CarControl.enabled`는 종방향 허용으로 유지한다. 실제 조향은 `CarControl.latActive`로 판단한다.

버튼 release보다 Panda 상태 보고가 늦을 수 있으므로, 다른 진입 조건이 정상일 때 해당 요청의 확인만 최대 0.5초 기다린다. 대기 중에는 StateMachine이 disabled이고 두 축 출력도 비활성이다. 시간 초과·취소·고장 시 요청을 버리며, 이후 늦게 온 확인으로 자동 활성화하지 않는다. 이미 활성인 상태에서 Panda 허용이 사라지면 대기 없이 해제한다.

| 조작 | 수정안의 요청 |
|---|---|
| OFF에서 LFA release | 조향 단독 진입 |
| OFF 또는 조향 단독에서 SCC main release | 조향+종방향 진입 |
| COMBINED에서 SCC main release | 양쪽 해제 |
| RES/SET release | 양쪽 진입/유지. LFA를 껐던 상태도 복원 |
| 활성 세션에서 LFA release | 세션 전체 해제 |
| cancel press/release | 양쪽 해제. 같은 배치의 enable보다 우선 |
| 고장/통신 실패로 해제 | 요청 소거. 조건이 회복되어도 새 물리 입력 없이 재진입하지 않음 |

안전한 초기 정책으로 LFA OFF와 SCC/cancel OFF는 모두 세션 종료다. SCC를 유지한 채 lateral만 OFF인 종방향 단독 모드는 이번 구현에 없다. 종전 LfaButtonMode/CancelButtonMode/AutoEngage 설정으로 이 정책을 우회하지 않는다. 길게 누름 등 추가 UX 정책은 실차 배포 전 별도로 확정·검증한다.

## 코드 변경

- `selfdrive/selfdrived/lx3_engagement.py`: 요청 모드와 축별 권한, Panda 상태 일치 검사. 아이콘, 순정 ACC 응답, 자동 크루즈 요청으로 engage하지 않는다.
- `selfdrive/selfdrived/selfdrived.py`: 기존 상태 머신 연결. `wrongCarMode`의 순정 main 상태 의존을 LX3에서 대체하고 나머지 고장·운전자·통신 이벤트는 유지한다. 이전 carState의 버튼을 재사용하지 않는다. Panda 모델/파라미터/대체 경험/RX 상태/고장/controlsAllowed가 맞아야 진입한다. 승인 없는 초기 enabled 상태도 OFF로 돌린다.
- `selfdrive/controls/controlsd.py`: LX3에서 AlwaysLateral 우회 제거. 새 세션 상태와 최신 입력으로 두 축을 결정한다. 기존 깜빡이 일시 중단은 한 번만 적용한다.
- `selfdrive/car/card.py`, `cruise.py`: lateral 상태의 소유권을 cruise helper에서 selfdrived로 옮긴다. helper의 available edge/AutoEngage/LFA toggle이 조향 권한을 만들지 않는다.
- `carstate.py`: 순정 MainMode_ACC로 main OFF를 되살리는 경로 제거. 순정 0x162 FAULT_LSS/LFA/DAS를 조향 사용불가에 반영한다. CRC 검증된 카메라 건강 정보가 없거나 250ms를 넘으면 정상으로 인정하지 않는다.
- `hyundaicanfd.py`, `carcontroller.py`: LX3의 주기적 가상 LFA/크루즈 버튼과 관련 구형 mode 2/3 toggle 경로를 제거한다. 카메라 방향의 수정된 MDPS/터치 메시지 송신을 중단한다. CanfdDebug로 LSS/DAS 경고를 숨기지 않는다. 다른 차종의 기존 동작은 해당 분기에서 유지한다.
- `cereal/log.capnp`, live snapshot: 요청 모드와 latEnabled를 기록·HUD 전달한다. 조향 단독을 종방향 활성으로 표시하던 경로 표시도 분리한다.
- `hyundai/interface.py`: 검증 완료 전 LX3 실차 제어 인터록.

## 검증

로컬 Windows Python 3.11에서 다음을 확인했다.

- 신규 36개 테스트 통과: 정상 StateMachine 함수 본문과 실제 이벤트 종류 정의를 이용한 상태 전환, cancel 우선권, 진입 거절 후 재요청, 조향→복합 전환 검사, soft/immediate disable, CAN/Panda 이상, 확인 지연/시간 초과/대기 중 취소·고장, 가상 버튼 및 피드백 송신 제거, fault/CRC/오래된 값 차단.
- 기존 HUD/설정/시간 관련 66개 테스트 통과. Windows의 기본 CP949 대신 `python -X utf8`로 실행해야 DBC 파일을 읽을 수 있다.
- 실제 pycapnp로 새 SelfdriveState의 세 모드 직렬화/역직렬화 통과. Windows에서는 Git 심볼릭 링크 대신 체크아웃된 경로 텍스트를 별도 스키마 검증 디렉터리에서 올바른 원본 파일로 해석했다. 원본 저장소 링크는 변경하지 않았다.
- 9월 27일 경고 구간과 9월 29일 구간에서 원본 0x162 2,902개 모두 현재 Hyundai CRC와 일치. 0x10B 3,628개와 0x1AA 7,254개도 일치했다. 이는 checksum 후보 확인이며 버튼 counter/debounce와 safety 허용의 완전한 검증은 아니다.
- Python 문법과 `git diff --check` 통과.

호스트 테스트는 메시지 facade와 실제 함수 본문을 사용한다. 아래 추가 검증에서 Panda C 소스의 데스크톱 빌드까지 진행했다. 전체 프로세스 IPC 실행, Panda 보드용 펌웨어 빌드/실행, 완전한 closed-loop replay, 계기판 경고 해소, 실차 조향 응답을 통과했다는 뜻은 아니다. GitHub 전용 workflow는 배포 권한 없이 테스트와 스키마 검증을 실행한다.

## 호환성 검토와 실제 C 추가 검증

불필요해 보인다는 이유로 공통 코드를 삭제하지 않는다. 현재 safetyParam 190은 hybrid/longitudinal/camera-SCC/HDA2/alternate-button/alternate-steering 플래그 조합이지 LX3 전용 식별자가 아니다. 이를 LX3 판별자로 사용하여 다른 차량까지 제어 정책을 바꾸면 안 된다. 저장소에서 확인 가능한 해당 Panda 파일 변경 이력은 초기 스냅샷으로 이어지므로, 원 제작자의 모든 변경 의도를 확인했다고 주장하지 않는다.

| 경로 | 확인된 용도·영향 | 이번 처리 |
|---|---|---|
| Camera-SCC buffered forwarding | 순정 수신 카운터에 맞춰 OP 명령의 counter/CRC를 다시 구성. Panda `can_send`도 해당 버퍼 플래그를 인식하여 직접 송신을 생략 | 구조 유지. 정상 counter/CRC·초기화 검사 추가 |
| HDA1/HDA2, camera/radar SCC, alternate buttons/steering | RX/TX 목록·버스·조향 주소 선택 | 분기 유지. 관련 64개 플래그 조합에서 기본 전달과 거절 경계 검사 |
| 비-LX3 가상 SCC/LFA 버튼, MDPS/touch, 토크 조향 | 다른 포팅의 기존 프로토콜 경로 | 기존 동작 보존을 호스트 테스트로 확인. 동작 보존이 안전성 인증을 뜻하지 않음 |
| 순정 ALERTS_1 긴급조향 | 비활성 OP 상태에서도 순정 각도·토크를 보존하는 기존 경로 | 11개 alert 값과 두 활성 상태의 보존 검사. 실차 제어 주체 전환은 미검증 |
| 공통 `longitudinal_accel_checks`, `aol_allowed` | 다른 safety 모델에도 영향을 주는 변경 | 일괄 수정하지 않음. LX3 전용 권한 경로와 별도 검증 필요 |
| 거절된 Hyundai CAN-FD TX의 hook 호출 | 반환값은 실패여도 hook이 이미 버퍼와 controls_allowed를 바꿈 | 해당 safety 모델에 한해 hook 전에 거절. 다른 모델의 hook 호출 순서는 유지 |

새 `opendbc_repo/opendbc/safety/tests/test_lx3_native.c`는 실제 `libsafety/safety.c`를 포함하고 Linux GCC `-Wall -Werror` 및 undefined-behavior sanitizer로 빌드한다. PC 테스트용 timer와 firmware 소유 심볼만 제공한다. 기존 Python CFFI 선언은 `safety_fwd_hook(int, int)`인데 현재 C 함수는 `safety_fwd_hook(CANPacket_t *)`이므로, 이번 검증은 잘못된 ABI를 사용하지 않고 C 패킷 포인터로 직접 호출한다. 기존 Python safety 전체 테스트를 통과했다고 보고하지 않는다.

검증 전 `5e2befe7`에서는 8개 release-audit 항목 모두 실패했다. 수정 `a499830d`에서는 다음 3개가 통과했다. 이 3개는 하나의 거절 처리 순서 문제를 서로 다른 조건으로 재현한 것이며 독립적인 3개 실차 고장을 의미하지 않는다.

- 잘못된 DLC의 0xCB가 forwarding 버퍼에 들어가지 않음.
- relay malfunction에서 거절한 명령이 버퍼에 들어가지 않음.
- 잘못된 버스로 보낸 ACC 명령이 controls_allowed를 켜지 않음.

64개 설정 조합의 모든 TX 목록 항목에 대해 잘못된 길이·버스·relay fault의 버퍼/권한 부작용 부재를 확인했다. 정상 forwarding 기본 동작, LX3 비활성 0xCB의 순정 counter와 checksum 재생성, 모드 재초기화도 통과했다. 이는 구성 선택·거절 경계·일부 정상 경로 검사이며 64개 차량의 전체 호환성 인증이 아니다.

남은 native release-audit 실패는 5개다.

1. 허용 목록에 있는 ACC TX가 controls_allowed를 스스로 켬.
2. controls_allowed 없이 활성 0xCB를 허용함.
3. 허용 해제 뒤 큐에 남은 활성 0xCB를 재전송함.
4. 마지막 활성 명령을 1초 뒤에도 재사용함.
5. 큐가 가득 차면 뒤늦게 도착한 OFF 명령이 유실됨.

관측용 C 테스트의 OFF 패킷은 active=0, torque=0이다. 호스트의 실제 비활성 송신은 active=1, torque=0이므로 이 값의 차이와 OEM 전환 프로토콜도 최종 검증해야 한다. native 테스트는 OP 활성 명령의 잔존을 확인하며 순정 fallback까지 무조건 무토크라고 가정하지 않는다.

Workflow의 release-audit는 실패를 숨기지 않고 job을 실패시킨다. 검사 로그는 `native-release-audit` artifact와 check annotations에 남는다. `f684ce7c`의 첫 실행은 파이프 종료코드가 전파되지 않아 성공 표시되었으므로 release 근거로 사용하지 않는다. `5e2befe7`에서 명시적인 bash pipefail 실행으로 수정했다. 호스트 테스트는 호환성 2개 추가 후 38개이며 기존 HUD/설정/시간 66개와 별개다. 실차 인터록과 OTA 미배포 방침은 그대로 유지한다.

## 9월 30일 후속: LX3 전용 권한·버퍼 정리

- `HyundaiSafetyFlags.LX3_ENGAGEMENT_GUARD=1024`를 Python 설정과 C 정책에 연결했다. 현재 전용 정책은 hybrid/HDA2/camera-SCC/longitudinal 구성을 요구하며, 다른 구성에서는 OP 송신을 거절한다. 기존 64개 설정 조합에는 이 비트를 추가하지 않는다.
- ACC TX의 상태 비트와 가속값이 권한을 스스로 켜지 못하게 했다. 공유 `longitudinal_accel_checks`는 호출하지 않고, 운전자/Panda 권한과 기존 -4.0~2.5m/s² 경계를 독립적으로 검사한다. 권한이 없는 활성 ACC/0xCB는 전달 버퍼에도 들어가지 않는다.
- MDPS/TCS/터치·가상 버튼 송신은 전용 정책에서 거절한다. 기존 0x1AA RES/SET으로 전용 정책을 켜지 못하게 했다. **물리 0x10B의 유효성·counter·debounce·LFA/SCC 정책이 완성될 때까지 실제 RX에서 제어 허용을 만들지 않는다.** 취소는 권한을 내릴 수 있다.
- 해제나 RX 이상이 확인되면 OP actuator 큐와 재사용 캐시를 소거한다. gas override가 종방향 권한을 없앴을 때도 이전 ACC 명령을 재사용하지 않는다. 순정 fallback 프레임은 원본대로 전달한다.
- 활성/비활성 명령을 받아들인 시각을 저장한다. 0xCB/LFA 30ms, ACC 40ms(기존 메시지 주기+20ms)의 전달 유효 기간을 적용한다. pop/reuse 시각으로 수명을 연장하지 않는다. 이 기간은 개발 정책의 소프트웨어 경계이며 OEM 타이밍 허용을 입증하지 않는다.
- 중립 명령은 대기 중인 활성 명령과 마지막 활성 캐시를 즉시 대체한다. 활성 큐가 가득 차면 가장 오래된 것을 버리고 새 명령을 보관한다. 호스트가 쓰는 `active=1, torque=0`과 관측용 `active=0, torque=0`을 모두 검사했다. 비활성 토크와 예약된 active=3은 거절한다.
- 0xCB의 기존 호스트 절대 경계인 ±175도, max-torque 250을 C에서도 검사한다. 급격한 각도 변화·운전자 토크·측정 각도 추종 경계는 아직 구현되지 않았으며, 별도 실패 검사로 남겼다.
- 타이머 overflow 검사에서 추가로 발견한 초기 타임스탬프 0 오판을 고쳤다. 실제 송신 여부를 별도 기록하여, 송신하지 않은 프레임은 0 근처에서도 전달하고 실제 0시각 송신은 차단 시간에 반영한다. 이 변경도 LX3 전용 비트에 한정했다.
- 순정 actuator 프레임의 DLC나 CRC가 잘못되면 OP 명령을 끼워 넣거나 큐를 소비하지 않는다. 원본을 보존하고, 유효 기간 안에 정상 순정 프레임이 들어올 때만 counter를 맞춰 전달한다. 지원되지 않는 전용 flag 조합에서도 중립 actuator 송신까지 거절되는지 검사했다.

Windows에서 실제 C 소스를 Zig/Clang `-Wall -Werror` 및 undefined-behavior sanitizer로 빌드·실행했다. 기존 64개 조합의 구성·기본 전달·counter/CRC/초기화, 거절된 TX 부작용 검사와 새 권한/비활성 우선/timeout/overflow/RX 이상/gas override/절대 경계 회귀가 통과했다. 호스트 39개, 기존 HUD·설정·시간 66개도 통과했다. 테스트에서 직접 `set_controls_allowed(true)`를 사용하는 것은 버퍼/검사를 시험하기 위한 fixture이며 실제 물리 버튼 허용의 증거가 아니다.

새 native release audit는 기존 8개 항목 통과, 물리 버튼 허용 및 각도 변화율 2개 미통과를 보고한다. Linux GCC sanitizer 결과와 보드 펌웨어 빌드/실행, 전체 IPC, 실제 제어 전환은 각각 별도 확인 대상이다. Workflow는 release 실패를 그대로 유지하며 기존 정책 결과도 별도 artifact에 보관한다.

## 연결 후 먼저 할 정차 검증

1. 현재 `/data/openpilot`의 실제 수정 파일과 실행 Panda signature를 백업·대조한다. Git HEAD만으로 설치 소스를 판단하지 않는다. 호스트 safetyParam과 펌웨어 정책 대응도 확인한다.
2. 현재 시점의 신선한 P·속도 0·조향/가감속 비활성 상태에서 수신만 하는 기록으로 0x10B 물리 LFA/SCC/cancel의 press/release, counter, CRC, 누락·중복·main flicker를 수집한다. 기기에서 버튼·조향·가속 명령을 합성 송신하지 않는다.
3. MDPS 실제 각도·운전자 토크·조향 fault, 순정 0xCB angle/active/max-torque, 0x162 fault/ACK의 시각과 값도 함께 확보한다. 이를 바탕으로 물리 RX 권한과 각도 변화율·운전자 개입 경계를 구현·벤치 재생한다.
4. 아래 인터록 해제 조건과 firmware/host 대응이 완료된 뒤 제어 시험 범위를 정한다. 계기판 점선·커브·아이콘의 표시 확인은 새 조향 후보의 제어 검증과 별도로 기록한다.

## 남은 필수 작업 — 인터록 해제 조건

1. 실행 중인 Panda signature/소스 대응 확인. 새 전용 경로의 TX 자기허용·권한 없는 0xCB는 차단했지만, 각도 변화율·운전자 개입·실제 firmware 제어 전환을 추가 구현·검증해야 한다. 기존 공통 helper의 `aol_allowed`는 전용 경로의 권한 근거로 사용하지 않는다.
2. 물리 0x10B의 checksum/counter/주기/중복·누락·main debounce를 검증하고 LFA/SCC/cancel의 운전자 의도와 Panda 허용을 연결. 현재 호스트가 요구하는 Panda 승인을 만들기 위해 TX에서 강제로 허용해서는 안 된다.
3. 실제 0xCB 각도·변화율·토크 제한과 비활성 명령을 검증. Python 측 목표값 제한만으로 Panda 검사를 대체하지 않기.
4. buffered forwarding의 대기열·재사용·타임아웃 중 과거 활성 명령이 취소 후 나가지 않는지, 순정 LFA와 OP의 제어 주체가 어떻게 전환되는지 벤치에서 확인.
5. 순정 긴급조향/제동 ALERTS_1에서 원본 명령을 보존하는 기존 분기는 임의로 삭제하지 않았다. 이 분기의 소유권/허용과 순정 AEB 기능 보존을 함께 검증해야 한다.
6. 카메라에 합성 버튼/MDPS/터치 변조 없이 필요한 정상 프로토콜 상태를 만들 수 있는지 순정 ACK/DTC로 확인. 아이콘을 ACK로 대체하지 않기.
7. 새로운 cereal 스키마와 selfdrived/controlsd/card/CAN 코드를 한 버전으로 빌드·배포. 기존 HUD 소스 업데이트의 일부 파일 교체 대상으로 추가하지 않기.

이번 후보 코드는 이미 보고된 경고 원인을 완전히 해결했다고 주장하지 않는다. 호스트 상태 전환을 정상화한 검토 가능한 변경과 회귀 테스트를 제공하며, 실차 제어 허용은 명시적으로 차단한다.
