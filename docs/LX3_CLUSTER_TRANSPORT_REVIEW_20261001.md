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
