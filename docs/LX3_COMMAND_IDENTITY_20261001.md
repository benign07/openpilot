# LX3 명령 식별자와 부팅 경계 — PC 검증 후보

2026-10-01 오전 후속 작업. 기존 정상 상태 머신·물리 요청/ACK 구조에
명령 자체의 식별자를 연결했다. 이후 사용자가 주차 상태에서 **주행 제어 활성화와
기기 설치**를 명시적으로 요청했다. 정확한1214/angle/long profile만 dashcamOnly를
해제하며 다른 조합은 passive를 유지한다. physical request/ACK·epoch·native
제한은 그대로 필수다. 설치는 최종 빌드 통과·새 P/속도0/비활성 확인 후 진행한다.
공개 production OTA는 별도이며 이 요청으로 자동 갱신하지 않는다.
아래 운행 중 관측 버전은 설치 전 production e0747fca/safetyParam190이다.

## 확인한 결함과 변경

기존 generation/counter/mode는 권한 ACK를 묶었지만 CAN payload에는 없었다.
OFF 이후 새 권한이 생기면 USB에 늦게 도착한 옛 명령이 새 권한을 사용할
수 있었다. 6d052f76의 실제 `can_comms.h`와 현재 native 정책을 결합한
desktop 재현에서 옛 raw 표시 패킷이 수락됐다. 전체 과거 커밋을 다시 빌드한
재현은 아니며, 수신 decoder의 식별자 누락을 고립시킨 검사다.

지금은 호스트가 commit한 `(boot epoch, generation, physical counter, mode)`를
SelfdriveState에 ACK와 별도로 저장한다. controlsd는 제어 축을 계산한 같은
SelfdriveState에서 CarControl에 복사한다. card는 그 CarControl의 식별자만
CanData에 복사하며 더 최신 Panda/ACK/상태로 옛 명령을 다시 표시하지 않는다.
OFF·잘못된 식별자·오래된 carControl·CAN invalid에서는 LX3가 소유하는10개
주소만 걸러내고 tester/진단 등 나머지 트래픽은 보존한다.

Panda C++ packer는 식별자가 있는 owned CAN 앞에8바이트 marker 두 개를
붙인다. 이들은 USB/SPI 전용 bus7의 확장 주소0x1fffffff/0x1ffffffe이며
차량 CAN이나 거절 echo 큐에 들어가지 않는다. 첫 marker는 mode/counter/
generation과 CRC16, 두 번째는 epoch64다. CRC16은 식별자와 대상 CAN의
전체6바이트 헤더·payload를 함께 묶는다. marker와 대상의 USB XOR checksum도
검사한다. 차량 CAN header ABI v4와 원래 payload는 바꾸지 않는다.

실제 firmware decoder는 조각난 전송을 조립하고, 두 marker 다음의 일반
CAN 한 개에서 식별자를 소모한다. 대상이 무관하거나 거절돼도 소모하며,
새 prefix나 communications reset으로 이전 staging을 재사용하지 못한다.
native의 accepted 식별자 대조와 실제 can_send 호출은 한 critical section에
있다. 현재 accepted와 불일치한 명령은 기존 driver 거절 경로를 사용한다.

## 부팅·heartbeat 계약

companion은 version2/20바이트다. 공통 health-v16/58바이트는 유지한다.
구 version1이나 짧은 응답은 guarded engage를 승인하지 않는다. 호스트와
board firmware가 함께 맞아야 한다. 읽기 실패는 pandad 오류 로그에 남긴다.

Linux getrandom으로 만든0이 아닌64비트 incarnation을 C8/C9 요청으로 board
부팅당 한 번 봉인한다. 제어권한이 없을 때만 초기 봉인을 받으며, safety-mode
변경·comms reset에서는 보존된다. 봉인 전에 물리 요청이 grant될 수 없다.
실제 board 재부팅은 새 난수를 요구한다. 16비트 generation은 같은 부팅에서
wrap하지 않고 소진 시 권한을 OFF로 유지한다. 재부팅 전까지 재사용하지 않는다.

CA/CB와 F3 heartbeat도 epoch 및 실제 value/generation에 묶으며 한번 소모한다.
없는/틀린 binding의 enabled heartbeat는 기존 권한 회수 경로로 간다.
disabled heartbeat와 다른 차종의 기존 bool heartbeat 계약은 유지한다.

Claude21차가 **새 구현에서 발견한 결함**도 수정했다. commit 뒤 ACK를 지우면서
정상10Hz heartbeat의 epoch까지0이 되어 첫 heartbeat에서 권한을 회수하는
문제였다. 지금은 pending에서는 ACK epoch, accepted 정상 상태에서는 호스트가
commit한 accepted epoch를 보낸다. 최신 Panda epoch로 대체하지 않는다.
이 결함은 PC 후보 검토 중 수정했으며 현재 운행 중인 기존 버전의 원인은 아니다.

64비트 난수 충돌은 확률적이다. CRC/binding은 신뢰된 호스트의 우발적 혼입·
옛 세션 재사용 격리이며 악성 호스트 인증이 아니다. getrandom은 초기 entropy
준비까지 기다릴 수 있다. 약한 난수나 추정 hardware RNG 대체 경로는 없다.

## 검증과 정확한 범위

- 기존 Python206개와 새 producer/schema/epoch11개, 합계217개가 PC에서 통과했다.
- 실제 native C와 `can_comms.h`를 strict warnings/UBSan으로 실행했다. 전송 분할,
  모든 분할 위치,10개 owned 주소, checksum/CRC/magic/혼입/reset/단회 소모,
  OFF 후 새 grant·같은 나머지 식별자에 다른 boot epoch·legacy ABI를 검사했다.
- 실제 C++ packer와 generated schema 테스트가40개 owned CAN과 chunk 경계를
  검사하고 그 바이트를 실제 native decoder 테스트에 넘기도록 Linux CI에 연결했다.
- 실제 send_heartbeat가 사용하는 production pack 함수의 제어 요청을 generated
  SelfdriveState로 만든다. grant ACK→ACK 삭제→정상 heartbeat20회 유지→식별자
  없는 enabled heartbeat 회수 및 잘못된 epoch의 grant 거절을 native 함수에 연결한다.
  main_comms hardware IRQ나 실제 SPI control transfer를 실행하는 검사는 아니다.
- 실제 msgq smoke에서 SelfdriveD의 production publisher→CarControl→sendcan의
  accepted 식별자 보존과 OFF 초기화를 검사한다. 전체 실행 loop나 EPS 검사는 아니다.
- 기존 C/호스트414개 및 표시40개 기본·520개 지연 일정도 통과했다. 이 일정은
  safety policy에 직접 들어가는 unit fixture이며 새 USB marker 통합 검사로 세지 않는다.
- Windows compiler의 MS bitfield 배치는 CAN header를9바이트로 만들 수 있어,
  serialization 검사는 `-mno-ms-bitfields`로 실행하며 data offset6을 assert한다.
  Linux/H7의6바이트 ABI를 Windows 기본 struct 결과로 대신 입증하지 않는다.

GitHub의 최신 커밋별 전체 runtime·실제 C++/IPC·H7·native·Python3.11/3.12
결과를 모두 확인한 뒤 설치 대상으로 삼는다. 정확한 SHA/run/job/artifact와
Claude 원본 검토 및 로컬 검사 경로는 PC의 candidate_validation.json에 남긴다.
과거 커밋의 녹색 결과를 이 변경의 성공으로 사용하지 않는다.

Claude22차는 immutable producer chain, marker 소모/CRC, epoch와 호환성 경로를
읽고 추가 권한 결함을 찾지 못했다. 실행 검증은 하지 않았고 C transport 테스트,
packer flush 끝부분과 can_reject는 그 라운드에서 읽지 않았다. 제안한 pending
epoch 잔류는 실제 reject가0으로 지우므로 그대로 남는 반례가 아니었다.
23차에서 flush·can_reject·실제 C decoder 테스트도 읽고 그 지적을 철회했다.
동 라운드의 extras 빌드 위험은 실제 CI에서 SCons mutation 옵션 오류로 나타났다.
이 테스트 target만 명시적으로 minimal 모드에서 정의·빌드하도록 고쳤으며
테스트 실행과 C++→native 연결 검사는 생략하지 않는다.

## 오전 운행 중 수신만 한 대조

설치된9개 핵심 파일은 production e0747fca와 hash가 일치했다. snapshot에서
AlwaysLateral=1, selfdriveState.enabled=false, Panda.controlsAllowed=false인데
CarControl.latActive=true였다. 이것은 기존 독립 lateral 우회 경로와 일치한다.
이 값만으로 EPS가 제어를 허용했다고 판단하지 않는다.

뒤이은45초에52,656개 프레임·781개 host context를 받았다. 이 구간에는 host
조향/종방향 비활성, controlsAllowed=false, CAN invalid/TxBlocked 증가가 없었다.
0x10B 물리 입력은0, 원본0x1AA bus0도0인데 전달 echo src130에는 약2초마다
LFA 펄스가 있었다. 기존 create_ccnc_messages의 `frame%200` 버튼 합성 형태와
일치하며 후보 LX3에서는 이 경로를 제외하고 native 0x1AA TX도 거절한다.
사용자 조작으로 재해석하지 않는다. 경고와의 인과관계는 확인되지 않았다.

0xCB의 active raw값은1이지만 max-torque는0이었다. MDPS fault raw값0,
CCNC FAULT_LSS raw값1이 유지됐고 새 경고 전환은 없었다. 실제 조향 힘이나
계기판 문구를 echo 또는 상속 DBC enum만으로 확정하지 않는다. 기존1E0 송신
echo900개는 CRC가 맞지 않고 원본900개는 맞았다. 후보가 이미 수정한 LX3
cluster checksum 경로와 대조할 추가 근거이며 새 경고 원인 증명은 아니다.
원본 CAN과 상세 분석은 PC에만 보존한다.

## 남은 실차 확인

epoch는 **같은 세션 안에서 지연된 명령의 나이**를 해결하지 않는다. 단일
SPI/USB stream writer를 전제로 하며 동시 writer는 검증하지 않았다. guard의
actuator 수명·각도 제한·70ms 표시 제한은 유지하며 임의로 늘리지 않았다.
정상 버튼→권한→실제 CAN선→EPS/순정 카메라 주체 전환 및 최초 계기판 경고
해결은 실차 근거가 필요하다. PC 빌드 성공과 이 운행 중 읽기 관측을 정상
실차 engage/OTA 준비 완료로 표현하지 않는다.
