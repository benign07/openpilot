LX3 CAN 명령–응답 진단 (PC, 읽기 전용)

목적
오파가 정상 경로로 요청한 명령, Panda의 실제 송신 에코, ECU 원본 응답을
시간순으로 비교한다. CAN 송신, 차량 접속, 버튼/응답 합성 기능은 없다.
기존 HUD 자동 진단 수집은 그대로 유지한다. 이 도구는 PC 분석 단계이며,
HUD에 새 실행 메뉴나 전송 기능을 추가한 것은 아니다.

실행 (저장된 같은 부팅의 로그만 함께 지정)
  python tools/lx3_can_response.py input.json --out reports/run-001
  python tools/lx3_can_response.py local/rlog.zst --schema cereal/log.capnp --out reports/run-002

rlog은 pycapnp 및 압축 형식에 따라 zstandard가 필요하다. 실제 로그와 같은
버전의 로컬 log.capnp 및 그 import 파일들을 사용한다. JSON/JSONL은 표준
Python만 필요하다. --window 1.0은 요청 이후 관측 범위(초), --max-events
2000은 요청 표 최대 행 수다. 기존 결과 폴더를 덮어쓰지 않는다.

입력 JSON 형식
  {"rows": [{"t": 1.23, "kind": "can", "bus": 128,
             "address": 203, "hex": "..."},
            {"t": 1.23, "kind": "carControl",
             "data": {"latActive": true, "longActive": false}}]}
bus는 rlog의 src 값 그대로이며 128/130을 0/2로 사전에 바꾸지 않는다.
carState, selfdriveState, pandaStates의 data도 선택적으로 입력 가능하다.
서로 다른 부팅을 섞으면 시간 축이 같지 않으므로 비교할 수 없다.

출력
  report.html: 모드 필터, ECU 상태/경고 변화, 요청–에코–MDPS 변화 표.
  report.json: 입력 및 프로그램 SHA256, 전체 계수, CRC/길이 문제,
               주소별 TX 거절, 표본 공백, 조향 오차 분포, 세션 중립 구간,
               각 구간의 순정 카메라/물리 MDPS/카메라행 MDPS 상태.
신호 범위: 0xCB 조향, 0xEA MDPS, 0x1A0 SCC, 0x162 경고,
            0x10B 물리 버튼. 매핑 밖 프레임은 주소 계수만 제공한다.

해석
sendcan은 요청이고 can.src128/130은 송신 에코다. 에코는 순정 전달일 수도
있으며 ECU가 명령을 수락했다는 증거는 아니다. 같은 값의 호스트 요청과
일치하는지는 표시하지만 발신 주체나 인과관계를 확정하지 않는다.
ECU/에코는 길이와 CRC가 맞는 프레임만 신호 분석에 쓴다. 호스트 CRC는
Panda에서 완성하기 전 값일 수 있으므로 별도 집계한다.
MDPS는 요청 이전 기준값과 이후 첫 관측 변화(응답 여부 미확정)를 비교한다.
분석 기준값이20ms보다 오래됐거나 같은 배치 안에서 이미 변했다면 변화
시각을 계산하지 않고 baseline_stale/same_batch_unordered로 표시한다.
각도1도·열 토크 원시값50의 변화는 단순 관측이며, 상태/고장 변화 후보와
별도 표시한다. 창은 다음 active/neutral·보조0/비0 또는 SCC mode/stop
전환에서 끝낸다. 계속 변하는 목표각·도로 영향의 인과 추정은 하지 않는다.
SCC 명령에는 주변 ECU 경고/상태를 연결하며 가감속 원인을 추정하지 않는다.
로그 시간은 배치 단위다. 공백, 첫 관측, 부분 로그는 미응답/발생시점으로
해석하지 않는다. 조향 오차 분포는 요청 이전 50ms 내 MDPS와의 차이다.
raw torque/cap은 Nm가 아니다. 경고 raw 값은 DTC 조회 결과가 아니다.
선택된 요청 표는 상태 변화와 누적 크기 변화만 포함하며 전체 빈도는
별도 계수에 보존한다. 상한을 넘으면 truncated=true로 명시한다.
모든 CB/SCC/카메라행 MDPS 요청의50ms 내 같은 제어값 에코 관측 수를
주소·세션별로 집계한다. SCC 경고 비트는 최신 순정값이 유지되므로 명령
일치 조건에서 제외하되 에코에 보존한다. MDPS 일치는 LFA 상태만 본다.
에코 미관측은 미송신 확정이 아니고, 거절 프레임0은 전체 송신 성공이
아니다. Panda 버퍼 안의 폐기/만료는 거절 프레임에 나타나지 않을 수 있다.

검증
  python -m unittest tools.tests.test_lx3_can_response -v
실제 생산 CRC 함수와 DBC packer 비교, 원본 CRC 벡터, 양쪽 각도 부호,
잘못된 CRC/길이, 요청/에코/응답 분리, 누락 기록, 모드 만료, 중립 세션,
주소별 거절 및 HTML escape를 검사한다. 차량 제어 검증을 대신하지 않는다.
