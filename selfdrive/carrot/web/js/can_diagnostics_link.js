(() => {
  'use strict';
  if (document.getElementById('canDiagnosticsLink')) return;
  const link = document.createElement('a');
  link.id = 'canDiagnosticsLink'; link.href = '/diagnostics.html'; link.textContent = 'CAN 진단';
  link.setAttribute('aria-label', 'CAN 진단 열기');
  link.style.cssText = 'position:fixed;right:12px;bottom:calc(76px + env(safe-area-inset-bottom));z-index:2147483000;background:#172a31;color:#a0efce;border:1px solid #477768;border-radius:10px;padding:9px 13px;font:600 13px system-ui;text-decoration:none;box-shadow:0 3px 15px #0006;';
  document.body.append(link);
})();
