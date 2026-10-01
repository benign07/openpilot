# 정차 이후 진단·영상·HUD 후속 후보 (2026-10-01)

차량에 설치해 정차 검증한 버전은999df40f입니다. 이 문서의 후속 수정은 PC/GitHub 후보이며 차량 설치·OTA 게시를 수행하지 않습니다.

## 원본과 비교한 결함
- 현재 부팅5fc0baa5-026b-45f1-b549-5105eba186cc의 12closed qlog,약870초에서 제어4프로세스PID가 유지됐으며CANinvalid/EPSfault가없었습니다. 실제버튼과OEM계기판 경고 해결은 검증전입니다.
- 영상 저장은loggerd가모든설정카메라의4encoder를기다리지만encoderd는VisionIPC에실제광고된ROAD/WIDE만선택해3encoder만송신하여72초timeoutrotation과queue누락을일으킵니다. DRIVER가없고RecordFrontfalse인실기기관측으로확인했습니다. loggerd가첫encode publication에서동일광고stream집합을확인해active encoder개수를고정합니다. RecordFront설정만으로DRIVER유무를추론하지않습니다. 카메라가있지만프레임송신이멈추는경우기존timeoutfallback을유지합니다.
- 자동진단worker의한번의예외가운행기록을영구중단시켰습니다.5/10/20/40/60초boundedretry와brokenpartial streamclose후기존sealed/partial복구를사용합니다. quota초과는기존자료를삭제하지않고error/retry로남깁니다.
- invalid CAN/sendcan publication을버려무결성실패맥락이사라졌습니다. invalid/malformed카운터와event_valid를기록하며유효성변화첫프레임을샘플링에서보존합니다. 이필드는msgq publication.valid이며CAN CRC나EPS전달성공증거가아닙니다. tx_requested/tx_echo구분과boundedcapture는유지합니다.
- 전화HUD에서는 stale속도가현재값처럼보이거나서버HTTP503·폰저장오류가차량offline으로보일수있었습니다. 별도전화후보는서버reachable/freshcarState/각서비스age를구분하고실서버compact vCruiseCluster km/h 및legacy cruise m/s단위를맞춥니다.

## 검증
- localautomaticdiagnostics41검사PASS(깨진partial복구/invalidraw유지/재시도/무결성다운로드/쿼터포함).
- 전화PC receiver10검사PASS. 전화JVM/APK및Linux전체빌드결과는정확candidate CI에서확인해야합니다.
- full-runtime-build에실제encoderd/loggerd+VisionIPC로missingDRIVER/RecordFronttrue,false및전체카메라3경우를추가합니다. LOGGERD_TEST로timeoutfallback이꺼진상태에서정상rotation/영상파일+sealedqlog/rlog를검사하므로timeout만으로통과할수없습니다.
- 기존인게이지/Panda C·host/다른차량호환성/msgq/양MPCrelocation/H7전체5CI를유지합니다. 이후정확commit과job로그를외부PC보고서에보존합니다.

## 남은 관측 한계
실제SCC/LFA 버튼인게이지, 조향명령의EPS수락, OEM경고원인은PC검사로완료주장하지않습니다. /data/stats는10000파일quota에도달하여통계저장이멈췄으며원본삭제없이별도보관정책을검토해야합니다. startupUDS response에서invalid consecutive frameindex1회가있지만차량인식성공/CANhealthy/프로세스지속과구분합니다.
