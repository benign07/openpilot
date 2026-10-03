(() => {
  'use strict';
  const host = document.getElementById('automaticDriveStatus');
  if (!host) return;
  const labels = {
    recording: '자동 기록 중 · 별도 조작이 필요 없습니다',
    waiting_for_ignition: '시동을 기다리는 중',
    ignition_off: '운행 종료 · 기록 보관 중',
    ignition_unknown: '차량 상태 수신 대기 · 기록 일시 중단',
    storage_full_preserving_records: '저장 공간 부족 · 기존 기록을 보존하고 수집을 멈췄습니다',
    starting: '자동 기록기 시작 중',
    stopped: '자동 기록 중단',
    error: '자동 기록 오류 · 확인이 필요합니다',
  };
  async function poll() {
    try {
      const response = await fetch('/api/automatic_drive/status', {cache:'no-store', signal:AbortSignal.timeout(5000)});
      if (!response.ok) throw new Error('unavailable');
      const data = await response.json();
      host.textContent = (labels[data.state] || '기록 상태 확인 중') +
        (typeof data.bytes === 'number' ? ` · ${(data.bytes / 1048576).toFixed(1)} MB` : '');
    } catch (_) {
      host.textContent = '자동 기록 상태를 확인할 수 없습니다 · 기기 연결 또는 업데이트를 확인하세요';
    } finally {
      setTimeout(poll, document.hidden ? 15000 : 5000);
    }
  }
  poll();
})();
