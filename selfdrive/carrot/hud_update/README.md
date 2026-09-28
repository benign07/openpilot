# HUD에서 선택하는 기기 소스 업데이트

버전 태그, 변경 이력, CI와 실기기 적용 상태는 [GitHub 업데이트 관리](../../../updates/hud/README.md)에서 함께 추적한다.

PC에서 검증한 소스 커밋을 서명해 GitHub에 게시하고, 사용자가 HUD의 **오파 업데이트**에서 변경점을 읽은 뒤 해당 버전을 예약한다. 예약은 운행 중에도 가능하다. 기기는 P·속도 0·유효한 최신 CAN·조향과 가감속 제어 해제가 10초간 유지된 뒤 다운로드/서명 검증/원본 백업을 준비하고, 다시 10초 대기와 직전 상태 검사를 거쳐 재부팅한다. 다운로드와 백업은 실행 중인 파일을 바꾸지 않는다.

실제 파일 교체는 `launch_chffrplus.sh`의 manager 시작 전 단계에서 수행한다. 순정 Git 자동 reset/pull 및 overlay 업데이트는 이 관리 방식의 설정 파일이 있을 때 중지한다. 설정 파일이 없는 기존 기기의 동작은 바꾸지 않는다.

AGNOS의 초기 부팅 시각이 이미지 제작일로 돌아가는 기기에서는, 예약된 설치에 한해 현재 부팅의 NTP 동기화 표시를 최대 90초 기다린다. 그 뒤 기존 120초 예약 유효시간과 부트 ID를 검사한다. 동기화 실패·예약 만료는 파일을 유지하고 재예약을 요구하며 자동 재부팅을 반복하지 않는다. 중단된 파일 교체의 복구는 시계나 네트워크를 기다리지 않는다.

## 사용 화면

- HUD 버튼 줄 또는 기기 목록 메뉴 → **오파 업데이트**
- 현재 설치 버전, GitHub 게시 버전, 변경점, 예약/취소, 시간별 진행/실패 이력
- 오파 연결이 없어도 GitHub 변경점 미리보기 가능
- 요청 응답 유실은 완료로 표시하지 않으며 POST를 자동 반복하지 않음
- 기기의 설정/연결 파일 최초 등록 전에는 업데이트 예약이 불가능함

## 배포 범위

첫 버전은 기존 Python/JavaScript/HTML/CSS/JSON 파일의 제한된 소스 업데이트다. 허용 디렉터리는 core.checked_path에 명시했다. 새 파일 추가, 삭제, updater 자체, 부트로더, 펌웨어, 모델, C++/네이티브 바이너리, DBC/schema는 이 경로로 배포하지 않는다. 해당 변경은 별도의 검토·빌드·기기 설치가 필요하다.

서명은 Ed25519이며 기기에 고정된 공개키로 검증한다. GitHub 인덱스는 실제 bundle 커밋 SHA와 파일 SHA-256를 고정한다. 화면에 표시한 release_id/변경점과 서명된 내용이 일치해야 한다. 현재 실기기 파일의 before 해시가 다르면 덮어쓰지 않고 PC 검토를 요구한다. `/data/params`와 주행 기록에는 쓰지 않는다.

다운로드/서명/차량 식별/저장 공간/원본 해시 검증은 소스 교체 전에 끝낸다. 부팅 중 중단된 교체는 다음 시작 때 백업으로 복구한다. 백업이 손상됐거나 알 수 없는 파일 변경이 끼어들면 manager 시작을 중단하고 recovery 서버를 열어 PC 복구를 요구한다. **새 코드의 기능 회귀나 기기 프로세스 시작 실패를 자동으로 치료하는 기능은 아니다.** 파일 교체 후 프로세스 검증 실패는 별도 경고로 표시한다. 실제 차량의 주행 안전성을 검증했다는 의미도 아니다.

이전 정상 코드와 updater bootstrap은 반드시 별도 보관한다. updater는 자신의 코드를 교체할 수 없으므로 최초 기기 설치/업데이트는 SSH로 수행한다. 이 저장소를 작성한 현재 시점에는 **오파가 사무실에 있는 폰과 분리되어 있어 bootstrap의 실기기 설치·재부팅 검증은 아직 수행하지 않았다.**

## PC에서 새 버전 만들기

1. 기기 상태/원본 해시를 확보하고 원본 소스·Params를 비공개로 백업한다.
2. 코드 변경을 테스트하고 GitHub에 올릴 소스 커밋을 만든다.
3. `release_id`, 증가하는 `sequence`, 변경점 `notes`, 대상 `car_fingerprint`, 각 파일의 `path`/`before`를 spec JSON에 기록한다.
4. 저장소 밖 비공개 디렉터리에 서명 키를 보관한다. 처음에만 `python tools/hud_release.py init-key --private-dir <private-dir>`를 사용한다. 키를 GitHub, APK, 일반 배포 ZIP에 넣지 않는다.
5. `python tools/hud_release.py build --spec <spec.json> --private-key <private-dir>/release-signing.key --commit <source-sha> --out updates/hud/bundles`로 커밋된 Git blob에서 배포 파일을 만든다.
6. bundle 파일을 커밋한다. `python tools/hud_release.py index --template updates/hud/bundles/<id>.index-template.json --bundle-commit <bundle-sha> --out updates/hud/latest.json`으로 인덱스를 고정한 뒤 커밋/푸시한다.
7. 공개 주소에서 내려받은 bundle의 해시·서명을 다시 검증한다. 실기기 적용은 HUD에서 사용자가 예약한다.

`config.json`은 `/data/community/hud_updates`에 0600으로 저장하며 phone_ip, 무작위 token, public_key를 가진다. HUD의 `op-update-pairing.json`은 schema=1, Tailscale URL, 동일 token만 갖고 앱 noBackupFilesDir에 보관한다. PC 전송 연결 파일과 별개다. API는 등록된 폰의 Tailscale IP와 Bearer token을 함께 확인한다.

## 검증

모의 파일/차량 상태를 사용한 테스트는 서명·변조·잘못된 경로·중복/이전 릴리스·실기기 변경·백업 손상·교체 도중 오류·중단 후 복구·오래된 정차 승인·운행 중 예약·다운로드/재부팅 직전 상태 변화·취소·중복 요청을 포함한다. 기본 주행 설정 테스트를 포함한 41개가 Windows에서 통과했다. Linux 3.11/3.12 CI도 등록했으며 기기 설치 전 통과 결과를 확인한다.

폰 APK의 JVM 테스트는 21개이며 PC 수신기 10개 테스트와 함께 CI에서 통과했다. 실폰에서는 1.0.20 설치를 확인했다. 차량에 updater를 설치한 뒤 실제 상태 확인·예약·재부팅·복구를 검증해야 한다.
