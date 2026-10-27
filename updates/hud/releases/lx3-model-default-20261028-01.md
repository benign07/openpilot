LX3의 기본 경로 선택을 순정 모델 모드로 맞춥니다. 차로 추종 사용 속도와 실제 선택이0이면, 좌우 미세조절·수동 위치 유지 옵션이 차선 모드를 자동으로 켜지 않습니다.

기존 미세조절 값은 보존하지만 모델 모드에서는 실제 위치 편향이0으로 대기합니다. 기록 전용 후보 계산은 유지하며 HUD 설명도 실제 동작에 맞춥니다. 네이티브 설정 기본값0은 기존과 같고 Params·개인 JSON을 변경하지 않습니다.

명시적인 양수 차로 추종 설정과 기존 LFA 길게 누르기 전환은 남습니다. 차선 모드에서 보고된 위험 장면의 근본 수정이나 도로 검증 완료를 의미하지 않으며, 모델 기본 모드를 유지하는 기준판입니다.

제어 권한·Panda·MDPS·DBC·감시 정책·운전자 양보 및0.2초 복귀·크루즈·부드러운 감속·모델 파일·영상 기록은 변경하지 않습니다. 기존 Python2파일만 정차·부팅 트랜잭션으로 교체합니다.

정확 소스 CI40509317042의 Python3.11·3.12 각각664검사+151하위검사 및162검사, 실제 런타임/양MPC·수집 생산자 검사를 통과했습니다. 옵션36조합과 비대칭 차로의 실제 MPC 바이트 비교를 포함합니다.

약50km/h 문제 구간을 고정 관측으로 재생한976개 제어 주기에서 최신 순정 모델 모드와 목표 곡률·요청각이 일치했습니다. 다른 속도·곡선·실제 차량 궤적의 안전성을 입증한 시험은 아닙니다. 최신 고속 곡률 제한·보조 함수 등 전체 조향 모듈의 일괄 이식은 이번 수정에 포함하지 않습니다.

CarrotHUD1.0.27 API와 기존 서명·이전파일 해시·차종·정차·롤백 검사를 유지합니다. EV9에는 적용하지 않습니다. 계기판 조향 경고와 SPI는 미해결입니다.

Source commit: 37cc7b5e545f2f6f7ae3a3aecac3c91bb6eb2959
Exact CI: https://github.com/benign07/openpilot/actions/runs/40509317042


2026-10-28 publication: lx3-model-default-20261028-01 / sequence 19. Supersedes the sequence18 publication for the original device, whose previous device-local passive diagnostic metadata already used sequence18. Identical source37cc7b5e and two runtime file hashes; source CI40509317042 applies unchanged. Actual before hashes and native UseLaneLineSpeed=0 re-read from serial80acfe79. No anti-rollback, signature or parked checks relaxed. Road qualification remains unperformed.
