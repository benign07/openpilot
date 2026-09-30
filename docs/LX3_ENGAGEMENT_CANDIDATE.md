# LX3 정상 engage 전환 수정안 — 실차 배포 금지

2026-09-30, 개발 브랜치 `fix/lx3-engagement-state`.

이 변경은 **호스트 제어 상태와 CAN 송신 경로의 검증용 구현**이다. Panda 펌웨어와 순정 ADAS 제어 주체 전환까지 완성한 포팅이 아니다. LX3 `dashcamOnly=True` 인터록을 포함한다. 이 브랜치를 기기에 설치하면 주행 제어가 비활성화되므로 운행용 업데이트로 배포하지 않는다. 기존 HUD 배포 브랜치와 릴리스 목록을 갱신하지 않는다.

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

- 신규 32개 테스트 통과: 정상 StateMachine 함수 본문과 실제 이벤트 종류 정의를 이용한 상태 전환, cancel 우선권, 진입 거절 후 재요청, 조향→복합 전환 검사, soft/immediate disable, CAN/Panda 이상, 가상 버튼 및 피드백 송신 제거, fault/CRC/오래된 값 차단.
- 기존 HUD/설정/시간 관련 66개 테스트 통과. Windows의 기본 CP949 대신 `python -X utf8`로 실행해야 DBC 파일을 읽을 수 있다.
- 실제 pycapnp로 새 SelfdriveState의 세 모드 직렬화/역직렬화 통과. Windows에서는 Git 심볼릭 링크 대신 체크아웃된 경로 텍스트를 별도 스키마 검증 디렉터리에서 올바른 원본 파일로 해석했다. 원본 저장소 링크는 변경하지 않았다.
- 9월 27일 경고 구간과 9월 29일 구간에서 원본 0x162 2,902개 모두 현재 Hyundai CRC와 일치. 0x10B 3,628개와 0x1AA 7,254개도 일치했다. 이는 checksum 후보 확인이며 버튼 counter/debounce와 safety 허용의 완전한 검증은 아니다.
- Python 문법과 `git diff --check` 통과.

테스트는 메시지 facade와 실제 함수 본문을 사용한다. 전체 프로세스 IPC 실행, Panda C 빌드/테스트, 완전한 closed-loop replay, 계기판 경고 해소, 실차 조향 응답을 통과했다는 뜻은 아니다. GitHub 전용 workflow는 배포 권한 없이 동일 테스트와 스키마 검증을 실행한다.

## 남은 필수 작업 — 인터록 해제 조건

1. 실행 중인 Panda signature/소스 대응 확인. 기존 `controls_allowed` TX 자기허용, 검사 위반 무시, 상수 `aol_allowed`에 기대지 않는 전용 안전 경로 구현·C 테스트.
2. 물리 0x10B의 checksum/counter/주기/중복·누락·main debounce를 검증하고 LFA/SCC/cancel의 운전자 의도와 Panda 허용을 연결. 현재 호스트가 요구하는 Panda 승인을 만들기 위해 TX에서 강제로 허용해서는 안 된다.
3. 실제 0xCB 각도·변화율·토크 제한과 비활성 명령을 검증. Python 측 목표값 제한만으로 Panda 검사를 대체하지 않기.
4. buffered forwarding의 대기열·재사용·타임아웃 중 과거 활성 명령이 취소 후 나가지 않는지, 순정 LFA와 OP의 제어 주체가 어떻게 전환되는지 벤치에서 확인.
5. 순정 긴급조향/제동 ALERTS_1에서 원본 명령을 보존하는 기존 분기는 임의로 삭제하지 않았다. 이 분기의 소유권/허용과 순정 AEB 기능 보존을 함께 검증해야 한다.
6. 카메라에 합성 버튼/MDPS/터치 변조 없이 필요한 정상 프로토콜 상태를 만들 수 있는지 순정 ACK/DTC로 확인. 아이콘을 ACK로 대체하지 않기.
7. 새로운 cereal 스키마와 selfdrived/controlsd/card/CAN 코드를 한 버전으로 빌드·배포. 기존 HUD 소스 업데이트의 일부 파일 교체 대상으로 추가하지 않기.

이번 후보 코드는 이미 보고된 경고 원인을 완전히 해결했다고 주장하지 않는다. 호스트 상태 전환을 정상화한 검토 가능한 변경과 회귀 테스트를 제공하며, 실차 제어 허용은 명시적으로 차단한다.
