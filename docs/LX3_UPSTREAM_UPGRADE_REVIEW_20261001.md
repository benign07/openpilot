# LX3 최신 당근 개선사항 검토 및 일부 반영

원본 기준은 사용자가 지정한 ajouatom/openpilot Wiki의 carrot-wip,
`abe1a232d81fd5b0f8db4b7212587952514e0274`이다. 순정 당근의 차량 목록과
CAN 정의가 LX3 실차 검증을 대신하지 않는다. LX3 전용 DBC, 물리 버튼,
정상 호스트/Panda 제어권한 경로를 유지하며 확인된 개선을 이식한다.

## 사용자 Carrot HUD 앱 호환성

사용자는 직접 만든 Carrot HUD와의 호환성 유지도 명시했다. 기준은
앱1.0.27/f9ccfc4d이며, 최신 당근의 웹/서버로 통째 교체하지 않는다.
현재 차량 설치 소스687e16f6 대비 서버, 웹, 설정 정의, 자동 진단,
업데이트 서비스와 cereal 스키마의 차이를 확인한다. 앱은 장치의
`/hud.html`을 WebView로 사용하며 다음 기존 계약을 유지한다.

- `/api/heartbeat_status`, `/api/live_runtime`: HTTP 연결 가능 여부와
  실제 CAN/서비스 유효성·수신 시각을 구분하고 기존 단위/키를 유지한다.
- `/api/params_bulk`, `/api/param_set`: MyDrivingMode1..4 저장 확인과
  실제 longitudinalPlan.myDrivingMode를 구분한다. 운행 중 모드 변경을 유지한다.
- `/api/automatic_drive/chunks` 및 개별 다운로드: id/bytes/sha256,
  압축 기록 형식과 휴대폰→PC 전송 무결성 계약을 유지한다.
- `/api/hud_update/status`, `/api/hud_update/action`: schema1,
  release_id/bundle_sha256, 인증과 기존 진행 상태를 유지한다.

4b9137d8의 정확 CI36863500293은 서버/HUD/설정/진단108검사를 포함해
각 Python 버전277검사가 통과했다. 앱 소스는 변경하지 않았으며 해당
정확 f9ccfc4d CI36835805935의 JVM49검사 및 PC수신10검사 근거를 유지한다.
이는 PC 수준 호환성 근거이며 새 후보를 휴대폰/차량에서 연동 시험한
것으로 주장하지 않는다. 외부GPU 지원 여부도 앱 통신 계약과 구분한다.

## 반영한 표시 개선

원본 `hyundaicanfd._display_lead`의 가까운 유효 대상 선택을
`selfdrive/carrot/hud_lead.py`로 가져왔다. LX3 controlsd의 HUD만 이
선택을 사용한다. 첫 번째와 두 번째 후보 중 상태가 유효하고 거리,
횡위치, 상대속도가 유한한 대상을 선택한다. 같은 거리이면 첫 번째를
유지한다. 수신 상태가 무효이거나 끊기거나 대상이 없으면 표시를 지운다.
다른 차량의 기존 leadOne/longitudinalPlan 표시 계약은 유지한다.

radarState를 새로 CarState에 주입하거나 ACC_ObjDist/ACC_ObjRelSpd를
덮어쓸 필요가 없는 이식이다. radarState의 두 후보와 플래너의 제어
선택, 조향/가속 요청과 세션 권한은 바꾸지 않는다. 실제 SCC 생산
builder/DBC/packer 검사는 enabled/stopping/override의 8조합에서
전체 payload가 같음을 확인한다. 비교하는 packer의
초기 counter 상태를 같게 유지한다.

Claude54 검토는 기존 SCC의 카메라 객체 거리/속도/횡위치를 복사하면서
HUD_LEAD_INFO만 별도 표시 대상의 유무로 덮어쓰는 혼합을 지적했다.
LX3에서는 네 필드 모두 원본 카메라 값으로 함께 유지하도록 수정했다.
따라서 CCNC의 가까운 대상 표시를 SCC의 다른 원본 객체에 적용하지
않는다. 다른 차량의 기존 SCC HUD 처리는 유지한다. 최신 원본의
`_apply_scc_lead` 전체와 횡위치 필터를 이식한 것으로 주장하지 않는다.
이는999의planner기반HUD_LEAD_INFO덮어쓰기와동작이다르다. 카메라가
대상을보고하지않으면SCC객체표시는없을수있고,CCNC는별도로유효한
radarState대상을표시한다. 동일객체가아닌두출처를섞어표시하지않는다.

생산 controlsd의 기존 블록에서 9검사 중 5실패를 재현했고, 새 블록과
SCC payload 회귀의 10검사가 통과했다. Linux 실구성 검사는 실제
RadarState/CarControl 생성 스키마와 생산 블록의 직렬화도 확인한다.
이는 표시 선택의 검증이며 계기판 차종/앞차 깜빡임의 모든 원인을
해결했다거나 ECU 오류를 제거했다는 증거가 아니다.

원본 오후7시의 14개 rlog를 파일 SHA 확인 후 새 표시 질문으로
분석했다. radarState 20,205건 중 invalid 15건을 제외한 20,190건에서
두 번째 후보가 선택되는 경우가 414건이었다. 그중 첫 후보가 없는
경우 21건, 더 가깝거나 첫 후보가 유효하지 않은 경우 393건이었다.
이 기록 대부분은 OP 제어 비활성이므로 표시 개선과 실제 가감속
개선을 혼동하지 않는다.

## 아직 필요한 원본 개선 검증

| 최신 당근 항목 | LX3 실기록/의존성 검토 |
| --- | --- |
| 레이더 상태/identity 개선 후보 | 실제19시 bus1에는 0x3A5..0x3C4의24바이트32슬롯이 각각약20,293건 있다. 이는주소/길이관측이며최신Group2 payload레이아웃일치의증거는아니다. Group3용0x400..0x41D24바이트 뱅크는 이 bus에 없다. 다른 bus의 같은 주소/다른 길이를 Group3로 취급하지 않는다. 상태 전달은 별도 스키마/소비자까지 검토한다. |
| CAN steering-touch/DM fallback | 이14구간의0x2AF는 관측되지 않았다. 기존 DBC의 주소만으로 터치 프로파일을 인정하지 않는다. 새 parser raw-cache, CRC helper, schema와 모니터 소비자가 필요하다. 운전자 카메라 부재만으로 원본의 interaction fallback을 불가능하다고 단정하지 않는다. |
| 정지 준비/감속 유지/재시도 | ESP_STATUS의기존AUTO_HOLD bit는0이89,507건,1이12,060건이며CRC-valid101,567건이다. 이 분포는AVH_Sta/AVH_LAMP enum이나실제hold권한을증명하지 않는다. 실제정지interlock 정의를 검증하기 전 원본의감속/재시도 상태를 이식했다고 주장하지 않는다. |
| steering_handover | 기존mode0/각도목표/운전자override/native토크상한을 유지해야 한다. source의capture/회복과실제native상한을같은입력으로대조하며 옵션을강제로0에묶거나권한을완화하는것을완료로판단하지 않는다. |
| MDPS/TCS TX counter 개선 | 원본은송신feedback재작성도포함한다. LX3는실제MDPS/물리버튼의동일바이트전달을보존하고feedback위조생산을제외한다. 단순cherry-pick으로그경로를복원하지 않는다. |
| eGPU/Cinque/Jetson | 사용자질문의기능설명으로확인했다. 새하드웨어나모델변경을요청한것은아니므로이번LX3후보의주행모델은유지한다. |

원본 입력 감사는 PC 자료
`can_inventory_work/upstream_signal_prerequisites_1900_20261001.json`에
원본 파일 SHA, 주소/길이, 표시 후보 예시와 기존 hold bit 분포를 저장했다.
Claude Code 동일 쟁점 검토는 읽기 전용이며 독립 테스트 실행과 구별한다.
최신 소스를 가져왔다는 이유로 실차 정상 ownership 인계/OEM 경고 해결이
증명되는 것은 아니다. 차량 설치·재부팅·CAN 명령·OTA 게시를 하지 않았다.

## LX3 물리 입력 CRC 확인과 호환성 범위

추가 원본 감사 `wheel_and_hybrid_crc_1900_20261001.json`은 14개 rlog의
원본 SHA를 확인하고 bus0 WHEEL_SPEEDS 101,568건(24B) 및 하이브리드
ACCELERATOR_ALT 50,781건(32B)의 공통 CAN-FD CRC가 모두 일치함을
확인했다. 기존 하이브리드 CRC 예외는 다른 차량을 위한 정책이며 LX3의
CRC 부재를 뜻하지 않는다. 관측은 차량 한 대/이 주행 기록의 근거다.

LX3 전용 host DBC에 ACCELERATOR_ALT의 CHECKSUM0|16만 추가하고
기존 LX3 수신 callback에 연결한다. 페달103|10/scale0.25는 유지한다.
byte2의 증가분은 +2가50,777건/+4가3건이나 의미를 확정하지 않으며
COUNTER 신호나 엄격 counter 검증을 추가하지 않는다. CHECKSUM type은
DEFAULT로 유지되어 parser callback만 사용하고 packer의 자동 CRC 송신
동작을 새로 만들지 않는다. parser와 packer는 캐시된 동일 DBC를 공유하므로
두 신호 집합의 단순 비교를 독립 검증 근거로 삼지 않는다.

Panda는 guard+HDA2+camera-SCC+long+hybrid 조합에서만 별도 RX 배열을
선택한다. gas 그룹은 이 기록에서 확인된 bus0/32B/0x105만 CRC 필수로
검증하고 counter 예외는 유지한다. TCS/wheel/MDPS와 기존1AA/1CF 버튼
검사는 원래 메타데이터와 같다. 공통 매크로/배열, 다른 차량과 미지원
guard 조합은 기존 정책을 유지한다. 선택한 배열 자신의 길이를 사용하고
init에서 flag를 재계산하며 set_safety_hooks가 RX 상태를 초기화한다.
원래100Hz 설정은 변경하지 않았으며
실제 관측 페달 주기는 약50Hz다. 공통 lag 하한1초는 동일하다.

정확6714 소스와 같은 최종 테스트로 host의 CRC 불변 페달bit103 손상이
0.25로 반영되는 실패, native가 불량 프레임을 수용하고 gas를 바꾸는
실패를 먼저 재현했다. 이전 native pending은 가짜 페달 상승으로 이미
취소됐으므로 권한 우회/불량 CRC grant를 입증한 것으로 주장하지 않는다.
수정 후 host press/release의 값·시각 보존 및 정상 복구, native의 outer와
direct hook 거부, pending/accepted 권한 해제, 새 조작 후 복구, +2/+4
header 예외, private 두 배열 및 기존190/미지원1210 경로를 확인한다.
정확 근거는 `lx3_hybrid_crc_validation_20261001.json`과 전후 로그다.
추가 배열/flag 단언은 이전 소스에 정의가 없어 before 컴파일에만 명시적인
fixture define(LX3_EXACT_6714_FIXTURE)으로 제외한다. 일반 빌드에서는
해당 단언이 항상 컴파일되며 각 구성 조건 제거 후 물리 조작도 pending을
만들지 못함을 확인한다. 핵심 전후 손상 입력은 양쪽에서 동일하게 실행한다.

CRC 실패 한 번으로 native 권한이 해제되면 기존 중립3회와 새 물리 조작이
필요하다. host의 한 프레임 거부가 즉시 can_valid=False를 뜻하지 않으며
초기 동적 주기 학습 전 timeout도 그대로다. native 공통 RX 건강 판정과
정상 권한 거래를 완화하지 않는다. 당시 원본 CRC는 모두 정상이므로 이
보완을 오후7시 OEM 경고 원인이나 해결 증거로 취급하지 않는다.

Claude64의 배열 길이/공유 DBC 비교/host 불량 release 반론과65의 테스트
전처리/조건 부재 조작/초기화 주체 구분 반론을 반영했다. 로컬70/12검사
실행은 도구 출력에 있으며 지속 로그를 남긴 새 정확 CI로 전체 근거를 확정한다.
테스트가 부족한 초기 fixture의 CRC0 실패와 비교용 이전 header에 새 flag를
직접 참조해 생긴 컴파일 실패는 fixture를 수정한 과정이며 생산 결함의
전후 실패 증거와 구분한다. 전체 Linux/H7/IPC/양MPC/actual C 검증은 새
커밋의 정확 CI 결과를 사용한다. 이 workflow는 범용 safety pytest 전체나
MISRA/cppcheck를 수행한 것으로 주장하지 않는다. 다른 차량 호환성 근거는
실행한 실제 C 구성/전달 회귀와 공통 코드 불변 범위다.
