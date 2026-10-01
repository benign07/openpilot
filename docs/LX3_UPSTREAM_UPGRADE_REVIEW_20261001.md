# LX3 최신 당근 개선사항 반영

원본 기준은 사용자가 지정한 ajouatom/openpilot Wiki의 carrot-wip,
`abe1a232d81fd5b0f8db4b7212587952514e0274`이다. 순정 당근의 차량 목록과
CAN 정의가 LX3 실차 검증을 대신하지 않는다. LX3 전용 DBC, 물리 버튼,
정상 호스트/Panda 제어권한 경로를 유지하며 확인된 개선을 이식한다.

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
| Group2 radar-native 상태와 Group3 object ID | 실제19시 bus1에는 0x3A5..0x3C4의24바이트32슬롯이 각각약20,293건 있다. Group3용0x400..0x41D24바이트 뱅크는 이 bus에 없다. 다른 bus의 같은 주소/다른 길이를 Group3로 취급하지 않는다. Group2 상태 전달은 별도 스키마/소비자까지 검토한다. |
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
