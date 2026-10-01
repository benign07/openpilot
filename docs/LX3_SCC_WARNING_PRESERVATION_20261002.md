# LX3 SCC 원본 경고 보존

카메라 SCC 메시지를 복사하는 기존 당근 경로는 `SysFailState`,
`TakeOverReq`, `DriverAlert`를 0으로 덮어썼다. LX3에서는 세 원본 필드를
그대로 유지한다. 다른 차량은 기존 경로를 유지한다. 이는 정상으로 보이게
오류를 숨기지 않는다는 포팅 원칙에 따른 변경이다.

호스트에서 복사한 시점과 실제 CAN 전달 시점은 다르다. 정상 상태에서 만든
호스트 메시지가 대기·재사용되는 동안 새 카메라 경고가 오면 오래된 0이
최신 경고를 덮어쓸 수 있었다. 실제20ab Panda C의 첫 반례는 queued0,
fresh63, reuse1에서 출력0이었다. 이 숫자63은 세2bit필드를 합친 시험값이며
OEM 경고 코드63이라는 뜻이 아니다.

LX3 native0x1A0 전달은 기존32바이트/CRC검사를 통과한 현재 원본에서
byte8 mask0x03, byte9 mask0x63을 보존한다. 검증된 호스트 명령을 복사한
뒤 이 비트만 복구하고 원본counter와 CRC를 적용한다. 원본의0도 그대로
반영하며 새 고장 의미를 추측하거나 경고를 고정하지 않는다. 권한, 제어
요청/ACK, 가감속·조향 제한과 나머지 payload는 바꾸지 않는다.

수신 원본CRC가 잘못되면 기존대로 그대로 전달하고 OP queue를 소비하지
않는다. 잘못된 원본의CRC를 정상화해 재전송하는 수정이 아니다. `fdcan`의
forward가RX검사보다 먼저 실행되는 기존 순서도 바꾸지 않았다.

로컬 검증:

- 실제 builder/DBC/packer: LX3 64경고×8출력상태=512조합 보존. 다른차
  512조합 및 경고0의 LX3 8조합은20ab와 전체payload동일. 입력 객체 불변.
- Python282검사 통과. 새3개 cluster검사는 기존 CI에 포함된 파일에 있다.
- 실제C: LX3와legacy190의8192원본/호스트쌍, 초회/재사용16384전달,
  counter/CRC/나머지payload 보존과 불량원본 거절 후 정상복구. 전체native
  회귀도 Windows Zig0.13.0에서 통과했다. 명시적인 격리 permission fixture를
  사용하는 payload시험이며 물리버튼/hostACK/실차 허가 증거가 아니다.

오후7시 원본14rlog의 카메라SCC50,783개는 해당3필드가 모두0이었다.
따라서 이 수정은 그때의 OEM LSS/DAS 경고 원인이나 해결 증거가 아니다.
원본 신호의 전체 OEM 의미와 실제 ECU 반응은 실차 확인이 필요하다.

기존20ab의CI/H7/설치패키지는 새수정을 포함하지 않는다. 새로운 정확
커밋의 전체CI와H7빌드가 끝나기 전에는 새버전을 검증된 설치본으로
표시하지 않는다. 이 PC검토에서는 차량 설치/재부팅/OTA게시가 없다.

PC근거: `can_inventory_work/lx3_scc_warning_fields_1900_20261002.json`,
`lx3_scc_warning_preservation_validation_20261002.json`(호스트 수정 단계),
`lx3_scc_native_before_20261002.json`, `lx3_scc_native_after_20261002.json`,
`lx3_scc_native_full_20261002.json`, `lx3_two_hour_python_regressions_20261002.json`.
