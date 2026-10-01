# LX3 최초 수신 전 제어 요청 보완

기존 LX3 request context는 이미 관측된 수신의 오류를 검사했지만 아직 한 번도
받지 않은 입력은 공통 1Hz 건강 검사에 맡겼다. 최초 tick 전에는 페달·브레이크·
차속 등의 초기값을 정상 관측값과 구분하지 못할 수 있었다.

정확99c981b6 C와 동일 시험에서 profile1214, LFA, 페달0x105 미수신 상태에
정상 물리 버튼 프레임과 일치하는 오프라인 heartbeat API를 주면 pending1,
controls1이 되는 첫 실패를 재현했다. 이는 실제 생산 host가 승인했다거나
차량에서 그 입력이 누락됐다는 증거가 아니다. 특히 오후7시 OEM 경고의
원인이나 해결 증거로 사용하지 않는다.

LX3 request context는 구성된 다섯 RX 항목이 모두 관측되고 checksum,
quality, counter 판정을 통과한 상태를 요구한다. 원래의 MDPS50ms, 버튼
200ms 조건, 공통 lag 하한·1Hz 검사, 다른 차량 정책은 유지한다. 기존
pending/ACK 유효성 검사에도 같은 context를 사용한다. 미수신 상태에서
거절된 요청은 이후 입력이 도착하더라도 자동으로 살아나지 않는다.

실제 outer RX와 물리0x10B 입력을 사용한 새 C회귀는 두 지원 구성1214/1182와
LFA/RES 두 요청 모드에서 입력 하나씩 누락한20경우, 정상4경우, 입력 복구 후
오래된 승인 거절·새 물리 조작·CANCEL을 확인한다. 이 새 시험에는
`grant_controls`나 RX valid 상태 직접 대입이 없다. 승인 API 호출은 PC
fixture이며 차량 버튼/IPC/폐루프 검증을 뜻하지 않는다.

기존 버튼 단위시험의 정상 차량 가정은 helper가 실제 RX를 한 번 공급하도록
명시했다. 새 누락 시험에서는 그 helper를 사용하지 않는다. 처음 만든
1182 fixture는8B 1CF의byte1 counter를16bit CRC로 덮어써 실패했다.
8B checksum-ignore 계약에 맞게 고쳤고 최종 이전/이후 시험에는 같은
수정된 시험을 사용했다. 생산 checksum 정책을 바꾼 것이 아니다.

로컬 전체 native회귀 및 SCC 최신 경고8192쌍/16384전달 검사를 통과했다.
정확 근거는 PC의 `lx3_required_rx_validation_20261002.json`과 연결된
전후/전체 로그다. 새 정확 Linux 전체CI/H7와 기기 검증은 별도로 필요하다.
기존20ab 성공 빌드와 패키지는 이 소스를 포함하지 않는다.
