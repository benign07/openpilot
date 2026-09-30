# LX3 계기판 프레임·순정 경고 검토 — 2026-10-01

개발 후보의 계기판 송신 경로를 추가로 수정했다. 실차 설치·OTA는 하지
않았으며, 최초 주행보조 경고가 해결됐다고 판정하지 않는다. 정상 인게이지
거래의 현재 근거는 [권한 프로토콜 검토](LX3_PERMISSION_TRANSACTION_20261001.md)에 있다.

## 코드로 재현한 문제

1. LX3에서 `camera_scc_params=3`, OP longitudinal 사용, `frame%5=0`이면
   `create_lfa_icon_non_camera_scc`와 `create_ccnc_messages`가 각각
   ECAN `ADRV_0x161`을 만들었다. 전자는 수정된 표시 payload의 CRC를
   다시 계산하지 않았다. 후자는 LX3 전용 CRC 보정 helper를 사용한다.
2. 두 builder 모두 일부 `ALERTS_*`, `SOUNDS_*`, FCA/DAW 상태를 지웠다.
   뒤 builder가 차선·0x162 fault를 보존하는 것과 0x161 경고 보존은
   별개였다. 실제 DBC packer/parser 재현에서 원본 `ALERTS_2=1`이0이 됐다.
3. 전용 LX3 DBC의0x161에는 checksum/counter header 정의가 없었다.
   실제 packer 결과의 byte2가0으로 남고 parser에 COUNTER가 없었다.
   counter 값이 있다는 전제로 작성한 dictionary fixture는 이 결함을
   발견하지 못한다. 단순히 CRC만 보정해도 헤더 누락은 해결되지 않는다.

수정 전 실패를 PC `can_inventory_work/lx3-cluster-before-fix-20261001.log`에
보관했다. 테스트용 메시지 외에 실제 차량·CAN 장치에 접근하지 않았다.

## 수정 범위

- LX3는 완전한 CCNC builder에서만0x161을 생성한다. 다른 차종의 기존
  두 builder 경로와 경고 처리 목록은 유지했다. 지원하지 않는 LX3
  stock-long 구성이나 아직 캐시되지 않은0x161을 새로 합성하지 않는다.
- LX30x161의 순정 경고·소리·FCA/DAW 상태를 보존한다. 검증되지 않은 다른
  플랫폼의 경고 번호를 LX3에 적용해 숨기지 않는다. OP의 모드·거리·차선
  표시 기능과 기존 순정 emergency 조향 분기는 별도 경로로 유지한다.
- 전용 DBC에0x161의16bit checksum과8bit counter를 추가했다. cluster
  helper는 원본 counter를 명시적으로 기록하고 payload CRC를 재계산한다.
  다른 차종이나 버퍼링되는 actuator의 counter 정책은 변경하지 않았다.
- LX3 카메라 parser에서0x161 CRC를 확인하도록 연결했다. 잘못된 checksum의
  경고/emergency 표시가 기존 캐시와 시각을 갱신하지 않는지 검증했다.

## 근거와 한계

생산 builder 및 호출 분기의 실제 함수 본문과 실제 DBC packer/parser로
송신 개수, CRC, 원본 counter, 경고·소리·fault·차선 보존을 검사했다. 이
검사는 전체 CarController100Hz 루프나 실제 계기판의 수신 검증이 아니다.
기존 수동 깜빡이 양보 검사는 전체 Linux runtime 환경에서 추가 실행한다.

과거 LSS onset5건과 r34 앞구간을 포함한7개 segment에서 원본0x161
10,161개는 모두 CRC가 맞았고 `ALERTS_1=0`이었다. 이 자료에서 상속된
emergency 조향 목록이 실행됐다는 근거는 없다. 원본 MDPS의 active2와
카메라향 echo의 active1이 가까운 시각에10,426회 달랐다. 다른 payload가
같고 driver torque만+220인 대응도351회 관측했다. 일반적인 서로 다른
시각의 torque 차이를 모두 변조로 분류하지 않는다.

경고 onset의 명령/응답 불일치 가설과 맞지만, echo는 EPS 전달 증거가 아니며
최초 차이는 경고보다 먼저 존재하기도 했다. 카메라 `FAULT_LSS`의 LX3
의미, EPS 응답, 긴급 조향 주체 전환과 경고 해소는 실차 확인 항목이다.
현재 fault gate는 유지한다. 경고를 보이게 하는 것은 원인 해결과 다르다.

원본·파생 관측은 PC
`주행데이터/engagement_audit_20260930/all_raw/oem-ownership-context-20261001.json`,
재현 코드는 `can_inventory_work/audit_lx3_oem_ownership_context_20261001.py`에
보관한다. 개인정보가 포함될 수 있는 원본 기록은 Git에 포함하지 않는다.

## 12차 상호검토 후 표시 소유권 보완

실제 Claude와 같은 코드를 다시 검토했다. COUNTER 헤더 추가가 자동으로
+1 검증을 켠다는 11차 우려는 실제 signal type과 parser 재현으로 철회했다.
전용 DBC 이름에는 checksum-state가 없으므로 COUNTER는 정보 필드다.
checksum callback 연결과 counter 검증은 서로 다른 조건이며 테스트로 고정했다.

수용한 문제는 오래된 표시 원본의 반복 송신과 비활성 중 표시 덮어쓰기다.
LX3 전용 `Lx3ClusterTransport`를 각 CarController에 두었다. 실제로 active인
SelfdriveState에서 온 latEnabled, CC.latActive 또는 CC.longActive일 때만 표시를
소유한다. lateral-only의 CC.enabled=False는 정상이고 PRE_ENABLE은 소유권이 없다.
일시적인 깜빡이 양보는 준비 아이콘을 표시할 수 있으나 조향 활성 아이콘으로
꾸미지 않는다. 이 표시는 조향 권한을 부여하지 않는다.

5개 원본 표시 메시지(0x161/162/1E0/1EA/200)는 CRC가 맞는 parser 원본과 같은
캐시에서만 가져온다. 수신 timestamp/counter 쌍마다 한 번, age0..100ms 이내에
생성한다. 비활성 기간의 이미 전달된 원본은 기준으로 소비하며 parser 교체나
clock 역행 후에는 새 원본이 필요하다. 다른 차종의 기존 builder는 유지한다.

50ms마다 claim하면 원본과 OP 주기의 위상 차이로 간격이100ms까지 늘어나
Panda의70ms 표시 차단 창이 끝날 수 있다는 Claude의 반례를 실제 C forwarding
hook으로 재현했다. 그래서 LX3는10ms tick마다 새 원본이 있으면 생성한다.
송신 주파수를100Hz로 만드는 것이 아니라 원본 publication에 맞춘다.
40개 합성 위상·47~53ms jitter 조건에서 기존50ms claim은 활성 안정 구간에
순정 원본이74회 섞였다. 개선안은0회였고 다른 payload의 동일 counter는
인게이지 시작 시각의 원본 이미 전달1회/조건만 남았다. 이 최초 중복과 실제
계기판의 처리 방식은 실차 검증 항목이며 완전한 ECU 교체라고 주장하지 않는다.

CRC를 바로잡기 전에도 순정 비트 보존이 필요하다. 5개 메시지에는 총255bit의
정의되지 않은 영역이 있었다. LX3 DBC에 `RAW_UNMAPPED_*` 정보 필드를 추가해
의미를 추측하지 않고 원본 비트를 보존한다. 원본 값을 바꿔 사용하는 신호
정의가 아니다. 실제 DBC parser/packer로500개 전체 바이트 round-trip이 일치했다.

과거7개 segment의 r15b에서 OP가 요청한0x161 및1E0/1EA/200은 각각1450개 모두
CRC가 틀렸다. 다른4개 LSS onset은 다른 dirty 소스에서0x161 sendcan이 없었고
정상 원본이 전달됐다. 따라서 표시 CRC 결함만으로5건 경고를 설명할 수 없다.
역사적 bus1 CAM362 OP 송신8127개/echo8117개는 모두 CRC가 맞았다. 현재 후보의
카메라 억제와 당시 코드가 같은 동작이라는 전제 없이 별도로 검토한다.

직전96e82f00의5개 CI job은 성공했으며 full-runtime 로그에서 실제
msgq/Capnp/production publisher+AlertManager와 실제 깜빡이9tests를 확인했다.
이번 소유권 보완은14개 display 테스트, 전체200개 PC Python 회귀 및 실제
Panda C schedule 검증을 추가한다. 정확한 새 커밋의 CI 결과는 별도 기록한다.

## 16·17차 상호검토와 지연 표시 권한 보완

추가 스케줄은 실제 sensor RX와 물리 LFA 버튼 CRC/counter/ACK로 Panda 권한을
얻는다. 처음 만든 표시 지연 모델에는 native grant가 없었으므로 이를 보완한
뒤 같은 문제를 다시 확인했다. 해제 후 USB로 늦게 도착한 표시108개가 이전
C에서 수락됐다. 새 accepted-permission 회귀는 이전 DLL에서 실제로 실패했다.

LX3 guard의5개 표시 ID는 accepted native 권한이 있을 때만 TX를 허용한다.
권한 해제 때 이5개의 `tx_active/last_tx_us`도 정리해, 다음 USB 메시지 없이도
순정 원본으로 복귀한다. 새 grant 전에 남은 표시 차단 창도 재사용하지 않는다.
거절된 패킷 자체는 활성 세션의 차단 marker를 변경하지 않는다. 실제 OFF/fault
처리에서만 소유권을 끝낸다. 다른 차종의190 정책은 유지한다.

actual C native 검사는5개 ID 각각 OFF/pending/accepted/OFF 직후 원본 복귀,
새 grant 전의 소유권 초기화, RX 이상, hook 밖에서 `controls_allowed`만
해제한 뒤 첫 지연 TX, 기존190 허용을 포함한다. 새 지연 표시108개는 모두
거부됐으며 actuator/host414개 일정도 통과했다.

표시 스케줄은 이상적인40개 일정 외에 CAN/USB0·10·30ms 고정/가변 지연,
일부 host tick 누락480개와300ms 표시 producer 정지40개를 포함한다.
기존70ms 표시 차단 창에서 가변 지연 때문에 활성 안정 구간에 순정이 섞인
경우60개는 남는다. producer 정지 중 순정 통과는 liveness 동작으로 구분한다.
정지40개 일정에서 순정200개가 통과했고, 재개 때 동일 counter의 원본과 OP
표시가 함께 보이는 경우도 있다. 완전한 표시 대체나 경고 해결로 주장하지 않는다.

Claude가 제안한110ms는 desktop fixture에서만 같은520개 조건으로 비교했다.
가변 지연의60개 혼입은0으로 줄었으나 producer 정지 중 원본 통과도200개에서
160개로 늦어졌다. 원본 위상이 이전 publication에 묶인 점을 빠뜨린 Claude의
110ms 반례는17차에 철회했다. 실제 차량의 최대 지연·계기판 timeout 근거는
없으므로 production70ms는 바꾸지 않았다. 권한이 유지되는 동안 순정을
무기한 차단하는 제안도 새 순정 경고/FAULT/곡률이 늦어질 수 있어 채택하지 않았다.

원본을 복사한 host 메시지는 이전 시점의 정보다. 현재 시각의 긴급 경고까지
보존한다는 보장은 아니다. USB CAN 패킷 자체에 세션 generation이 없으므로,
아주 늦은 이전 세션 패킷이 새 accepted 세션에 도착하는 문제도 이 gate만으로
해결한 것으로 표시하지 않는다. 실측 지연과 세션 경계 검증이 필요하다.

PC 자료: `can_inventory_work/lx3-display-delay-timeout-sensitivity-20261001.json`,
`lx3-display-before-gate-regression-20261001.log`. 긴 지연 직후
`safety_tx_blocked` 증가를 해제 후 정상 거부와 지속 fault로 구분해야 한다.

## SCC 대기 플래그와 설정 속도 표시

표시 builder도 기존 `cruiseState.available`에 종속돼 있었다. LX3의 새
RES/SET 요청은 이 순정 대기 플래그와 독립적으로 combined 권한을 얻을 수
있지만, builder는 `CC.enabled=True`일 때도 SETSPEED/SETSPEED_HUD를0으로
만들었다. 실제 parser/packer 회귀를 먼저 실패시켜 확인했다.

LX3에서는 `stock available or CC.enabled`를 표시 대기 기준으로 사용한다.
controlsd의 LX3 `CC.enabled`는 정상 거래로 승인된 실제 longitudinal 권한이다.
LFA 단독일 때는 False이므로 이 변경으로 SCC 활성 표시를 만들지 않는다.
다른 차종은 이전 순정 availability 계약을 유지한다. 별도2개 회귀와 전체
PC Python204개 검사가 통과했다. 차량 아이콘 enum의 실제 의미와 최초
경고 해소를 이 표시 수정으로 입증했다고 주장하지 않는다.

19차의 추가 반례는 순정 main latch가 True인 LFA-only에서 ready 표시1이
남는 경우다. 이 ready 표시를 실제 combined 권한으로 바꾸지 않는다.
해당 상태에서 SETSPEED/HUD/HDA가1이고 LFA가2인 기존 계약을 회귀로 고정했다.
main latch를 삭제해 다른 기능까지 바꾸는 정책은 채택하지 않았다. gas override
중 active SETSPEED3이 순정 계기판에서 갖는 의미도 차량 확인 전에는 미확정이다.
