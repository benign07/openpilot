(() => {
  'use strict';
  if (document.getElementById('canDiagnosticsLink')) return;
  const link = document.createElement('a');
  link.id = 'canDiagnosticsLink'; link.href = '/diagnostics.html';
  const title = document.createElement('strong'); title.textContent = '주행 평가 · CAN 진단';
  const subtitle = document.createElement('span'); subtitle.textContent = '신호 정합성 기록 / 정차 조작 검사';
  subtitle.style.cssText = 'display:block;font-size:11px;font-weight:500;margin-top:4px;';
  link.append(title, subtitle);
  link.setAttribute('aria-label', '주행 평가와 CAN 신호 정합성 진단 열기');
  const base = 'box-sizing:border-box;background:#83e5bd;color:#082a20;border:1px solid #b0f7da;border-radius:14px;padding:12px 16px;font:600 15px/1.3 system-ui;text-decoration:none;';
  function place() {
    const groups = document.getElementById('groupList');
    const hud = document.getElementById('wf');
    if (groups) {
      // Part of the settings menu layout; never covers a setting control.
      if (link.parentElement !== groups) groups.prepend(link);
      link.style.cssText = base + 'display:block;grid-column:1/-1;width:100%;margin-bottom:8px;';
    } else if (hud) {
      if (link.parentElement !== hud) hud.append(link);
      link.style.cssText = base + 'display:block;text-align:center;margin-top:8px;flex-shrink:0;';
    }
  }
  place();
  const groups = document.getElementById('groupList');
  if (groups) new MutationObserver(place).observe(groups, {childList:true});
})();
