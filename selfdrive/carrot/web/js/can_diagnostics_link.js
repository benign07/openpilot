(() => {
  'use strict';
  if (document.getElementById('canDiagnosticsLink')) return;
  const link = document.createElement('a');
  link.id = 'canDiagnosticsLink'; link.href = '/diagnostics.html';
  const title = document.createElement('strong'); title.textContent = '자동 주행 기록 · CAN 진단';
  const subtitle = document.createElement('span'); subtitle.textContent = '자동 기록 상태 / 선택 조작 검사';
  subtitle.style.cssText = 'display:block;font-size:11px;font-weight:500;margin-top:4px;';
  link.append(title, subtitle);
  link.setAttribute('aria-label', '주행 평가와 정차 조작 검사 열기');
  link.style.cssText = 'position:fixed;right:max(12px,env(safe-area-inset-right));bottom:calc(82px + env(safe-area-inset-bottom));z-index:2147483000;max-width:calc(100vw - 24px);background:#83e5bd;color:#082a20;border:1px solid #b0f7da;border-radius:14px;padding:13px 16px;font:600 16px/1.3 system-ui;text-decoration:none;box-shadow:0 3px 15px #0006;';
  document.body.append(link);
})();
