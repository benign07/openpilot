# LX3 순정 제어 주체·경고 전이의 시간 대조

6시간 검토 중의 중간 결과다. 경고의 원인이나 실차 해결을 확정하지 않는다.
소프트웨어 권한 거래와 차량 ECU의 제어 주체 정합성은 별개의 검증 항목이다.

## 경고 전이 5건

원본 MDPS(bus0)와 카메라 방향 송신 echo(bus130)를 비교했다. 서로 가까운
기록은 동일 원본을 전달했다는 뜻이 아니므로 counter가 같은 대응도 구분한다.
연속 에피소드의 기록 간격 기준은30ms이며 실제 ECU의 timeout 값이 아니다.

| 기록 | 가까운 MDPS 원본2 / echo1 연속 시작→LSS 전이 | 같은 counter 대응의 연속 시작→전이 | bus1 억제 |
|---|---:|---:|---|
| r15b | 658.84ms | 87.71ms, 중간 대응 누락 | 송신·echo 없음 |
| r31 | 681.51ms | 670.91ms | 39.12초 전부터 기록 |
| r32 | 649.44ms | 649.44ms | 33.76초 전부터 기록 |
| r33 | 642.61ms | 642.61ms | 13.86초 전부터 기록 |
| r34 seg2 | 649.30ms | 649.30ms | 앞 segment부터 존재 |

r34는 같은 route의 seg1에서 OP 활성 명령이 먼저 기록됐다. seg2의 첫 프레임을
시동이나 노출 시작으로 해석하지 않는다. 위 약650ms 정렬은 사건 후보를
좁히는 상관 근거다. FAULT_LSS와 MDPS enum 의미는 상속 DBC의 해석이다.

전체7개 구간에서 MDPS의 같은 counter·30ms 이내 대응45,456개 중9,188개는
원본 LFA2_ACTIVE2와 echo1이 달랐다. 카메라 자체 명령이1인 상태에서 echo도1이면
카메라 명령과 표시된 응답은 일치한다. 다른 EPS 동작 신호나 섞여 전달된
원본, 또는 다른 제어 주체 상태가 경고에 영향을 줬을 가능성은 남는다.
따라서 MDPS 변조 제거만으로 새 후보의 경고 해소를 예측하지 않는다.

약700ms 이상 같은 불일치가 지속되면서 카메라가 계속 LSS0을 보고한 반례를
찾는다. 전이 이후 시간까지 포함한 전체 에피소드 길이를 정상 유지 시간으로
계산하지 않는다. segment 첫 상태가 이미 fault면 경고 시작 시각을 알 수 없다.
echo가 없는 counter도 원본이 실제 카메라에 도달했다는 증거가 아니다.

## 카메라 차선 억제의 별도 문제

CAM362 bus1 송신은 원본을 교체하는 forwarding이 아니라 같은 버스의 추가
송신이다. 과거 echo8,117개는 모두 CRC가 맞았고8,116개는150ms 이내 같은
counter 원본과 대응했다.5,443개는 payload가 달랐고4,809개는 host lateral이
비활성인데도 차선을0으로 만든 프레임이었다. 정상 r34seg1에도935개 변화
대응이 있었으므로 억제만으로 경고 발생을 설명할 수 없다. r15b에 억제가
없어도 경고가 있었으므로5건의 공통 필요조건도 아니다.

LX3의 기존 억제 분기는 accepted active 세션의 CS.out.latEnabled일 때만
송신한다. CC.latActive로 제한하면 깜빡이·정지 시 억제가 반복 중단되므로
13차 상호검토의 반례를 받아 세션 기준으로 정리했다. Panda도 LX3 guard일 때
OFF/pending/권한 없음/RX 이상/relay fault에서는362/2A4를 거부한다. 일반190
정책은 유지한다. 기존 활성 억제의 CRC를 새로 보정하거나 효과를 입증했다고
주장하지 않는다. 제거 여부는 순정 카메라 제어 주체 검증과 함께 판단한다.

마지막 실기기 확인(9월30일)은 HyundaiCameraSCC1, CanfdHDA2=2,
openpilotLongitudinalControl=True, safetyParam190이었다. camera_scc_params3에
붙은 억제 경로가 현재 설정에서도 실행된다고 혼동하지 않는다. 새 Linux
runtime 검사는 실제 Params/CarInterface/generated DBC/CarController로 이
설정의 guarded1214 생성과 지원하지 않는 stock-long 설정을 따로 확인한다.

## 검증 기록

b68db657의5개 GitHub CI job은 모두 성공했다(run36754256291). 실제 msgq,
Capnp, production host publisher/AlertManager, 실제 깜빡이9tests, full Linux
runtime과 H7 빌드를 포함한다. PC Python200회귀와 실제 Panda C의40개 표시
위상·jitter 조건도 통과했다. 추가 억제 정책·runtime 설정 검사는 그 후속
커밋의 결과로 따로 기록한다.

실제 Claude 동일 세션13차까지 주장을 교환했다. 마지막 회차는 표시 스케줄
테스트 본문과 일부 원본 전이 문맥을 읽었다. 전체 데이터나 모든 테스트를
독립 검증했다고 표기하지 않는다. 원본은 PC의
`주행데이터/engagement_audit_20260930/all_raw/oem-ownership-context-20261001.json`에
보관하고 Git에는 넣지 않는다. OTA·차량 설치·재부팅은 하지 않았다.

## 전체78개 기록의 후속 대조

관측된 false 상태나 같은 counter 대응 누락을 즉시 에피소드 종료로 처리한
추가 검토에서도 LSS 전이는5건이었다. CRC가 유효하고150ms 이내 카메라가
LSS0을 보고한 MDPS 불일치 에피소드는31개였고, 가장 긴 것은670.775ms였다.
700ms 이상 정상 유지 반례는 없었다. 이 결과는 약650ms 가설을 반증하지
못한 것일 뿐, 실제 ECU timer나 경고 원인에 대한 증명이 아니다.

5개 전이 직전700ms에서 원본 MDPS와30ms 이내 같은 counter echo가 대응하지
않은 프레임은 각각0/0/3/0/2개였다. 이 누락으로 원본이 카메라에 도달했다고
판단하지 않는다. 전체504,685개 같은 counter 대응 중24,614개는 원본2에서
echo1로 달랐다. 비활성 host에서 zero-lane CAM362 echo는44,611개였다.

원본 actuator의 정의되지 않은 bit도 조사했다. 수집한 원본0xCB529,337개와
SCC264,664개에서는 해당 bit들이 모두0이었다. 해당 수집 범위에 없는
LFA0x12A는0 또는 부재라고 결론 내리지 않는다. 가상의 원인에 맞춰 actuator
DBC 필드를 추가하지 않는다.

별도로 실제 C에서 확인한 버퍼 전달 각도 제한 결함과 수정은
[LX3_BUFFERED_ANGLE_DELIVERY_20261001.md](LX3_BUFFERED_ANGLE_DELIVERY_20261001.md)에
기록했다. 이 소프트웨어 결함과 순정 계기판 경고의 인과관계는 확인하지 않았다.
