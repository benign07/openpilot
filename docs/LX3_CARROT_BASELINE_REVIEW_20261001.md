# LX3 수정 기준과 순정 당근 신호 보존 검토

사용자가 지정한 기준은 [ajouatom/openpilot 위키](https://github.com/ajouatom/openpilot/wiki)와 그 위키가 명시한 `carrot-wip` 코드다. 2026-10-01에 확인한 원본은 `abe1a232d81fd5b0f8db4b7212587952514e0274`이다. 설치본의 기반과 최신 원본은 별도로 비교하며 최신 브랜치를 통째로 합치지 않는다.

정상 제어 흐름과 다른 차량 호환성을 유지한다. 원본 당근 코드의 결함 또는 LX3 포팅 과정의 누락을 재현한 경우에만 최소 수정한다. 오류 표시를 숨기거나 MDPS 응답·물리 버튼·Panda 권한을 위조하여 정상으로 보이게 하는 방법은 해결안에서 제외한다.

## 원본 LFA 비트의 재생성 손실

LX3 전용 DBC의 16바이트 `LFA`(0x12A)에 bit79 정의가 없었다. 기존 당근의 범용 DBC에도 같은 위치가 미정의였다. 생산 parser는 정의된 신호만 읽고 packer는 정의된 신호만 0으로 초기화한 payload에 쓰므로 `copy(CS.lfa)`만으로는 원본 bit79를 보존할 수 없었다.

순정으로 분류된 두 route의 CRC-valid camera LFA93샘플과 19시 운행 오류 주변 원본1490샘플에서 이 비트는 모두1이었다. 실제 활성 OP echo76개에서는0으로 소실됐다. 대표 원본 `a4f29500800108000081000400640000`은 단순 decode/re-encode만으로도 byte9의 `81`이 `01`이 되었다. 드문 qlog 표본의 촬영 연도와 연속 상태 전이 순서는 확정하지 않는다.

LX3 전용 DBC에 `RAW_UNMAPPED_79` 한 비트만 추가한다. 원본 값0과1을 모두 보존하며 의미나 고정값을 추측하지 않는다. 다른 차량의 DBC, 조향 요청값, 상태 머신, Panda ACK/각도·출력 제한은 변경하지 않는다. 기존 당근의 `TORQUE_REQUEST=-1024`, `NEW_SIGNAL_1=10` 등은 순정 차량 표본 분포와 다르다는 이유만으로 바꾸지 않는다.

원본 fixture 왕복·생산 camera-SCC builder의 active/inactive/emergency 경로·원본 bit0 반례·필드 비중첩을 검사한다. 수정 전에는 byte 손실 검사가 실패했고 수정 후에는 통과했다. 이 비트의 OEM 의미와 계기판 오류의 인과관계는 미확정이다.

## 포팅에서 빠진 카메라 템플릿 CRC 검사 복원

범용 당근 CAN-FD DBC 이름에는 생산 checksum binding이 적용된다. LX3 전용 파일명은 이 조건에 포함되지 않아 `LFA`, `LFA_ALT`, `SCC_CONTROL` 수신 템플릿의 CHECKSUM callback이 없었다. CRC가 깨진 프레임도 값과 수신 시각을 갱신하는 결함을 실제 parser로 재현했다.

기존 LX3 clock/health/display와 같은 방식으로 세 카메라 템플릿의 CRC 검사를 복원한다. 관측된 +2 counter를 일반 parser의 +1 검사에 억지로 맞추지 않으며 기존 counter 정책은 유지한다. 불량 CRC가 값·시각을 갱신하지 않고 정상 CRC/+2 counter가 다시 갱신하는지 검사한다. native는 여전히 전달 시 원본 CRC·counter와 기존 제어권·출력 제한을 검사한다.

19시 운행의 camera bus2 원본 CRC는 CB101570개, LFA101570개, SCC50783개가 모두 정상이었으므로 이 누락을 당시 오류의 직접 원인이라고 주장하지 않는다. 관련 PC168검사와 CI처럼 프로세스를 분리한 오프라인267검사가 통과했다. 생성 DBC와 실제 Linux 구성 검사는 새 정확 후보 CI에서 따로 확인한다.

## 남은 제어 주체 문제

19시 원본에서는 SCC MAIN 뒤 카메라 SCC와 실제 TCS ACC 요청이 이미 활성으로 바뀌었지만 카메라의 조향 요청은 inactive1이었다. 그 상태에서 OP 요청과 실제 MDPS는 active2가 되었고 이후 카메라측 LSS/DAS 오류가 발생했다. 오류 전 물리 버튼373쌍과 MDPS1490쌍의 원본/전달 echo는 같은 counter와30ms 이내에서 모두 같은 바이트였다. Echo만으로 카메라 ECU 수신이나 내부 판정을 입증하지 않는다.

단순히 카메라 요청2를 OP 조향의 새 필요조건으로 추가하면 원하는 SCC→LFA 동시 인게이지가 실행되지 않을 수 있다. 이것을 정상 해결로 분류하지 않는다. 정확한 ownership 인계·OEM 감시 조건은 아직 증명되지 않았다. 원본 카메라 활성 요청·MDPS 실제 응답·물리 LFA 전이를 연속 기록과 대조하고, 필요하면 원본 DTC/프로토콜 근거로 확인해야 한다. 아직 ECU 비활성화 주소나 요청을 추측하여 변경하지 않는다.

실제 Claude CLI Opus5.5/high와 같은 원본/생산 소스를 상호 검토했다. 잘못된 flags 해석과 자동 LFA 가정은 원본 반례로 철회했다. 독립 실차 실험을 한 것으로 기록하지 않는다. 이번 PC 작업에는 차량 설치·재부팅·OTA 게시가 없다.

## 추가 수신 피드백 CRC 검토

같은 LX3 전용 DBC 이름의 자동 바인딩 누락은 MDPS/TCS에도 남아 있었다.
실제 생산 parser에서 정상 MDPS 다음 CRC를 깨뜨린 프레임이 값과
CHECKSUM 수신 시각을 갱신하는 1FAIL을 재현했다. 기존 LX3 패턴대로
두 메시지의 CRC callback을 복원했으며, 정보용 counter와 최종 Panda
전송 경로는 유지했다. 부정 입력 거부와 정상 입력 복구를 포함한
25 cluster 검사가 통과했다. Linux 생성 DBC의 callback/counter 상태도
실구성 검사 대상이다. 19시 MDPS101,563/TCS50,782 원본은 CRC가 정상이므로
이 누락을 당시 OEM 경고의 원인으로 주장하지 않는다.

440..455초 원본 MDPS의 1,490연속차이와 TCS의744연속차이는 모두+1이었다.
물리0x10B/일부카메라 템플릿의+2와구분한다. Host의counter는정보용으로
유지하고 실제native수신counter/CRC검사를완화하지않는다. Native의
주소/bus/24바이트일치RX검사는그검사대상범위에한정되며, 다른길이의
0xEA를동일하게검사한다거나이코드리뷰로ECU응답을증명한다고하지않는다.

## Native MDPS 길이 경계 수정

실제 outer `safety_rx_hook`은 RX 표에 없는 길이도 mode hook으로 넘긴다.
이때 기존 LX3 hook은 각도/고장/시각을 검증하나 driver torque는 먼저
갱신했다. 정확43b1cb10 정책과 새 동일 C 검사로, 정상24바이트의
운전자토크450 이력 뒤12바이트의중립토크6개가 min/max를0으로지워
`lx3_angle_context_valid(26)`을true로만드는실패를재현했다.12바이트에는
토크바이트10/11이실제로있어길이밖메모리를가정한실패가아니다.

LX3 guard에서만 토크도 동일24바이트+CRC 조건 안에서 갱신한다.
권한을 우회하거나 요청/ACK/토크상한을 완화하지 않는다. 다른차량의
guardfalse 경로는그대로다. outerRX가unknown길이에true를반환하는
공통정책전체를바꾼것이아니며 LX3 상태갱신만거부한다.

양부호/15가지잘못된DLC, 그중CRC헤더를담을수있는13가지의자기길이
CRC정상프레임, 정상길이불량CRC outer/direct hook,50ms초과시각,
정상수신1..5개override유지/6번째복구, non-LX3기존토크수신 회귀가
기존native검사와함께통과했다. Claude56의길이와CRC를독립검증하라는
반론을반영했다. Linux undefined-behavior 검사와 H7 전체빌드는
새정확commit CI에서별도확인해야한다.

원본19시 bus0 MDPS101,563건과 bus2 MDPS680건은모두24바이트였다.
이경계결함은실차에서관측한원인을확정한것이아니며OEMownership 및
계기판경고 인과관계는여전히미해결이다. PC파일만사용했고실차CAN
전송·설치·OTA는없다.

## 추가 물리 입력 길이 경계

같은 outer RX 경계가0x105(가속),0x175(브레이크),0xA0(차속)에도
남아 있었다. 정확aab 정책을 같은 새 C 검사로 각각 실행하면 정상
입력 뒤16바이트 해제/정차 입력이상태를덮어썼다. 가속·브레이크는
요청context까지false에서true로바뀌었고 차속은vehicle_moving 및
vehicle_speed 이력이바뀌었다. LX3각도상한이차속기반이라는주장은
하지않는다. 읽는각signal byte는첫실패의16바이트안에있다.

LX3 전용 입력 helper로 각값을읽기전에기존 native RX표와같은
길이·CRC조건을확인한다. MDPS의앞선조건을동일helper로정리했다.

| 입력 | LX3 적용 조건 |
| --- | --- |
| MDPS/브레이크/차속 |24바이트 및 기존 CAN-FD CRC |
| EV/ICE 가속 분기 |32바이트 및 기존 CAN-FD CRC; 이 분기로 LX3 미지원 조합을 지원했다고 주장하지 않는다 |
| 실제 LX3 하이브리드0x105 |32바이트; 기존 ignore_checksum/ignore_counter 정책 유지 |

0x105의CRC예외는기존정책이며해당차량의32바이트에CRC가없다는
실차증명이아니다. 이를임의로새CRC필수조건으로바꾸지않는다.
다른차량의guardfalse는즉시통과해기존수신동작을유지한다.
버튼/ACK/nativecounter/권한상한/공통generic RX 판정은바꾸지않았다.

각15wrongDLC에서pressed/moving이유지되고, 정상길이badCRC의
outer/direct판독거부,정상해제,가짜pressed/moving,wrongbus,
non-LX3기존수신 회귀가통과했다. 별도실제safety_tick검사는다른RX와
중립물리스트림을갱신하면서한입력만wronglength로유지한다.
원본timestamp는갱신되지않고1.01초후호출한tick에서해당입력lagging,
RX무효/권한회수를확인했다. 이후정상RX로건강판정은회복하지만
자동재인게이지하지않는다. 생산tick은1Hz이고기존lag기준은최소1초라
실제즉시차단또는정확1.01초주기라고주장하지않는다.
이timeout검사는이전정책과새정책모두에서통과하는기존기간상한
회귀이며수정효과의증거는앞선세개의16바이트실패대조다.
Claude59가첫중립스트림의동일시각입력과tick전권한단언누락을
지적해첫refresh전10ms진행/button_ready유지와tick직전mode2·권한
활성·RX정상단언을추가했다. 강제permission fixture가lease오류를
가려통과하는것으로판정하지않는다.

Claude58은생산분기의상호배타성/0x105예외/다른차호환을검토했고,
tick와추적되지않은생산파일확인반론을반영했다. 이전정책의세별도
실행은각첫16바이트단언에서중단됐으므로뒤하위검사도모두이전에서
실패한것처럼표기하지않는다. 원본19시의gas50,781건은32바이트,
brake50,782건과speed101,568건은24바이트였다. 이경계결함도당시
계기판경고원인으로확정하지않는다. 새정확LinuxUBSan/H7/전체CI는
커밋후별도검증하며차량설치·OTA는없다.

## RX 거절과 인게이지 대기 요청의 연결

정확 a540 정책에서 정상 all-RX와 실제 C tick 판정 뒤 LFA press/release로
pending을 만들고, 정상24B TCS0x175의 CRC만 손상시켜 outer safety_rx_hook에
넣었다. RX는거절/controls=false가되지만 pending/context는1로남고
safety_rx_checks_invalid는다음1Hz tick전까지false였다. 첫탐색의press만
넣은fixture실패는폐기하고release까지수신한경로만결함근거로사용했다.

LX3요청context에서이미수신한감시입력의현재CRC/quality/counter상태를
부작용없이확인한다. 아직수신하지않은입력과lag는기존공통tick처리를
유지한다. unseen입력이모두검증됐다고주장하지않는다. 공통dispatcher가
감시입력을거절하면 exact guarded LX3에서기존revoke함수를호출해
pending/accepted권한과버튼재적격을함께폐기한다. 다른mode와다른차량은
기존경로이며CRC예외/counter허용치/주기/상한은변경없다.

Claude60은현재status를request에반영하는순수helper만제안했다.
정상대체프레임이maintenance보다먼저도착하면status가회복돼oldpending이
살아남는반례를제시했고61이인정했다. 실제같은test를 a540 safety.h와
새helper header로컴파일한context-only비교에서 CRC거절→정상대체→
old host heartbeat가controls=1/pending=0이되는실패를별도로재현했다.
최종dispatcher수정후같은test가통과한다. baseline첫pending/context실패와
context-only첫CRC수락실패뒤의하위항목도실패했다고확장해주장하지않는다.

회귀는CRC/누적5wrongcounter즉시pending폐기, known-invalid상태의새gesture
거절, 정상복구뒤oldidentity거절, 새neutral/gesture/generation의정상수락,
accepted중거절시mode0, 0x105기존CRC예외와190legacy경로를포함한다.
별도정상대체먼저순서의TCS/MDPS 각각CRC와counter4조합도검사한다.
host heartbeat API는오프라인fixture이며실차ACK/물리버튼/ECU반응증거가
아니다. 정확전후production2파일/testSHA/실패위치/전체로그SHA는
can_inventory_work/lx3_rejected_rx_pending_validation_20261001.json에있다.

감시대상한프레임의실제거절도재적격neutral3회와새gesture를요구하는
가용성변화다. badcounter한번으로거절하지않으며기존MAX_WRONG_COUNTERS를
유지한다. 현재fdcan 드라이버는forward/send뒤 safety_rx_hook를호출한다.
따라서RX에서폐기한것을이미전송된프레임까지취소하는것으로표기하지않고,
그RX이후의대기수락과권한유지를차단하는범위로한정한다. 새동시성문맥을
만들지않고기존RX버튼revoke와같은함수를같은RX문맥에서쓴다.
19시원본TCS/MDPS CRC는정상이므로이수정역시계기판경고원인/해결증거가
아니다. 정확전체Linux/H7 CI는커밋후별도확인하며OTA/차량접속없음.
