LX3 수신 전용 자동진단 보완 / sequence7

현재 추가 LX3의 검증된 lx3-spi-cause-test-20261012-01 설치 파일을 기준으로 진단 Python2파일만 교체합니다.
새 감속 후보 및 제어/MDPS/Panda/DBC/schema/OS/DisableDM/부저/진동 명령은 배포하지 않습니다.

확인한 결함: 실제 selfdriveState.alertSound enum을 그대로 JSON에 넣어 표본 저장이 실패합니다.
정확32ff installed 두 소스의 manifest SHA와 같은 log schema SHA로 실패를 재현했습니다.
수정본은 알림 이름을 유지해 직렬화하며 실제 모든 수집 서비스의 wire reader에서 표본2개와 파일회전을 검사했습니다.
회수한22개 닫힌 자동진단은 헤더뿐이므로 기존 분석 근거 중 qlog/별도 live 관측과 구분합니다.
후보0da369a4의 정확 CI38345911634 양Python103검사, 로그/산출물SHA를 검증했습니다.
https://github.com/benign07/openpilot/actions/runs/38345911634

동측 깜빡이+물체감지는 경고 후보일 뿐입니다. 경고음/접근속도/실제 진동은 미확정이며 전체rlog와 대조해야 합니다.
0x1BA 원본 수집과 전2초/후8초 창은 최대4개/운행·새창간격30초입니다. 기존 조향/통신/물리버튼 할당량은 유지합니다.
휴대폰 앱 업데이트는 이 진단을 위해 필수는 아닙니다. HUD1.0.31은 별도의 위젯설명 업데이트입니다.
차량 설치와 수집 정상화 확인은 별도이며 P/속도0/제어 비활성·before해시·기존서명/boot transaction 검사를 유지합니다.
기존 기기와 EV9에는 설치하지 않습니다. Claude Fable5.1/high는 사용량한도로 새 검토를 완료하지 못했습니다.
