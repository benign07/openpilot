(() => {
  'use strict';
  const $ = id => document.getElementById(id), prefix = '/api/can_diagnostics';
  let last = null, busy = false, catalog = null, initialized = false, shownResult = '';
  const activeStates = new Set(['driving', 'awaiting_action', 'settling', 'recording']);
  const names = {'report.json':'결과 JSON','catalog.json':'신호 사전','raw_can.jsonl.gz':'원시 CAN','events.jsonl':'표식·주행 문맥','candidates.csv':'비트 후보 CSV'};
  // A live installation can use a temporary standalone server until the
  // manager's preimported web modules are refreshed by the next normal boot.
  if (location.port === '7010') {
    const hud = document.querySelector('a[href="/?view=hud"]');
    if (hud) { const url = new URL(hud.href); url.port = '7000'; hud.href = url.href; }
  }
  function el(tag, text, cls) { const node = document.createElement(tag); if (text != null) node.textContent = text; if (cls) node.className = cls; return node; }
  function message(text, error = false) { $('message').textContent = text; $('message').className = error ? 'error' : ''; }
  function clock(s) { return String(Math.floor(s / 60)).padStart(2, '0') + ':' + String(Math.floor(s % 60)).padStart(2, '0'); }
  async function api(path, payload) {
    const options = {cache: 'no-store'};
    if (payload !== undefined) Object.assign(options, {method:'POST', headers:{'Content-Type':'application/json','X-Carrot-Diagnostics':'1'}, body:JSON.stringify(payload)});
    const response = await fetch(prefix + path, {...options, signal: AbortSignal.timeout(8000)});
    if (response.status === 404 && path === '/status' && location.port === '7000') {
      const standalone = new URL(location.href);
      standalone.port = '7010';
      location.replace(standalone.href);
      throw new Error('진단 서버로 연결하고 있습니다.');
    }
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || '요청을 완료하지 못했습니다.');
    return data;
  }
  async function act(kind, payload = {}) {
    if (busy) return;
    busy = true;
    try { const data = await api('/' + kind, payload); render(data); message(kind === 'marker' ? '이 순간을 기록했습니다.' : ''); }
    catch (error) { message(error.message, true); }
    finally { busy = false; }
  }
  function links(host, id, files) {
    host.replaceChildren();
    for (const file of files) {
      const link = el('a', names[file] || file);
      link.href = prefix + '/download/' + encodeURIComponent(id) + '/' + encodeURIComponent(file);
      link.download = id + '-' + file;
      host.append(link);
    }
  }
  function render(data) {
    last = data;
    $('demo').hidden = !data.demo;
    $('connection').textContent = data.error ? '수집기 확인' : '연결됨';
    $('connection').className = 'badge good';
    const session = data.session, active = session && activeStates.has(session.state), driving = active && session.test_id === 'drive';
    $('startDrive').disabled = busy || active || !data.ready;
    $('stop').disabled = !active;
    $('startGuided').disabled = active || !!data.guided_block_reason;
    $('preflight').textContent = data.error || (driving ? '주행 CAN과 해석·계획 값을 기록하고 있습니다.' : (data.block_reason || 'CAN 수신 중 · 주행 상태에서도 기록할 수 있습니다.'));
    $('guidedReason').textContent = data.guided_block_reason || '정차 조작 검증을 시작할 수 있습니다.';
    $('captureState').textContent = active ? (driving ? '기록 중' : '조작 검증 중') : '대기';
    $('captureState').className = 'badge' + (active ? ' live' : '');
    $('elapsed').textContent = clock(session ? session.elapsed_seconds : 0);
    $('frames').textContent = (session ? session.frame_count : 0).toLocaleString();
    const car = data.car || {};
    $('vehicle').textContent = data.car_state_fresh && typeof car.vEgo === 'number' ? (car.vEgo * 3.6).toFixed(0) + ' km/h' : '—';
    $('gear').textContent = data.car_state_fresh ? ({park:'P단',drive:'D단',reverse:'R단',neutral:'N단'}[car.gearShifter] || '기어 미확인') : '차량 해석 신호 대기';
    const v = data.drive_validation || {};
    $('seen').textContent = v.observed_messages || 0; $('unknown').textContent = v.unknown_messages || 0;
    $('length').textContent = v.length_mismatches || 0; $('counter').textContent = v.counter_discontinuities || 0;
    $('checksum').textContent = v.checksum_checks ? `${v.checksum_failures} / ${v.checksum_checks.toLocaleString()}` : '미검사';
    $('dbStatus').textContent = v.runtime_database?.note || '수신한 주소와 길이를 DBC 정의에 대조합니다.';
    if (!initialized) {
      for (const test of data.tests) { const option = el('option', test.title); option.value = test.id; $('test').append(option); }
      for (const [code, title] of Object.entries(data.markers)) { const button = el('button', title); button.dataset.marker = code; button.addEventListener('click', () => act('marker', {code})); $('markers').append(button); }
      initialized = true;
    }
    document.querySelectorAll('[data-marker]').forEach(button => { button.disabled = !driving; });
    const guided = active && !driving;
    $('guide').hidden = !guided;
    if (guided) {
      $('step').textContent = `${session.step + 1} / 9 · ${session.cycle}번째 반복`;
      $('progress').value = session.step;
      $('instruction').textContent = session.prompt;
      $('feedback').textContent = session.decoded_feedback == null ? '기존 해석값 없음' : (session.decoded_feedback === session.expected_feedback ? '변화 감지됨' : '변화 대기 중');
      $('countdown').textContent = session.state === 'awaiting_action' ? '안내대로 조작한 후 아래 버튼을 눌러 주세요.' : `${session.state === 'settling' ? '안정화' : '측정'} ${session.remaining_seconds.toFixed(1)}초 · 현재 조작을 유지해 주세요.`;
      $('mark').disabled = session.state !== 'awaiting_action';
    }
    if (data.report && data.report.session_id !== shownResult) {
      shownResult = data.report.session_id; $('result').hidden = false;
      $('resultText').textContent = data.report.reason + ' 후보 신호는 반복 검증 후 확정할 수 있습니다.';
      links($('resultLinks'), shownResult, Object.keys(names));
      $('candidates').replaceChildren();
      for (const c of data.report.candidates.slice(0, 12)) $('candidates').append(el('p', `BUS ${c.bus} · ${c.address_hex} · ${c.dlc}B · 비트 ${c.bit_lsb0} · ${c.repeat_count}회 반복 · 일치도 ${(c.score * 100).toFixed(0)}%`, 'fine'));
    }
  }
  async function poll() {
    try { render(await api('/status')); }
    catch (error) { $('connection').textContent = '연결 끊김'; $('connection').className = 'badge'; $('startDrive').disabled = true; $('startGuided').disabled = true; $('mark').disabled = true; $('preflight').textContent = '연결을 확인하고 있습니다. 진행 중인 주행 기록은 기기에서 계속됩니다.'; }
    finally { setTimeout(poll, 1000); }
  }
  function renderCatalog() {
    const host = $('catalogRows'); host.replaceChildren();
    if (!catalog) return;
    const query = $('search').value.trim().toLowerCase();
    const rows = catalog.messages.filter(row => !query || JSON.stringify(row).toLowerCase().includes(query));
    $('catalogInfo').textContent = `관측 ${catalog.messages.length}개 · 검색 ${rows.length}개 · 원본 값 통계는 최대 10Hz 표본 · 비트 번호는 LSB0`;
    if (!rows.length) { host.append(el('div', '아직 관측된 신호가 없습니다. 주행 기록을 시작해 주세요.', 'empty')); return; }
    for (const row of rows.slice(0, 100)) {
      const card = el('details', null, 'catalogItem'), title = el('summary');
      title.append(el('h3', `BUS ${row.bus} · ${row.address_hex} · ${row.dlc}B`));
      title.append(el('span', row.definitions.map(d => d.name).join(' / ') || '아직 정의되지 않은 주소', 'muted'));
      const tags = el('div', null, 'tags');
      tags.append(el('span', `${row.observed_hz == null ? '—' : row.observed_hz} Hz`, 'tag'));
      tags.append(el('span', `${row.frame_count.toLocaleString()} frames`, 'tag'));
      if (row.length_mismatch) tags.append(el('span', '길이 불일치', 'tag warn'));
      if (row.checksum_failures) tags.append(el('span', `체크섬 실패 ${row.checksum_failures}`, 'tag warn'));
      if (row.counter_discontinuities) tags.append(el('span', `카운터 불연속 ${row.counter_discontinuities}`, 'tag warn'));
      tags.append(el('span', '의미 검증 전', 'tag'));
      title.append(tags); card.append(title);
      const text = [];
      for (const definition of row.definitions) for (const signal of definition.signals) {
        const seen = row.signal_observations[signal.name];
        text.push(`${signal.name}\n  bit ${signal.start_bit} / ${signal.size}bit / ${signal.byte_order}${signal.signed ? ' / signed' : ''}\n  배율 ${signal.factor} + 오프셋 ${signal.offset} ${signal.unit || ''}\n  관측 ${seen ? `${seen.minimum_observed} … ${seen.maximum_observed} (최근 ${seen.last_value})` : '없음'}${signal.validation_note ? '\n  ' + signal.validation_note : ''}`);
      }
      card.append(el('pre', text.join('\n\n') || '원시 CAN과 사건 표식을 바탕으로 정의를 추가할 후보입니다.'));
      host.append(card);
    }
  }
  async function refreshCatalog() { try { catalog = await api('/catalog'); renderCatalog(); } catch (error) { message(error.message, true); } }
  async function refreshHistory() {
    try {
      const data = await api('/sessions'), host = $('historyRows'); host.replaceChildren();
      if (!data.sessions.length) host.append(el('div', '저장된 진단이 없습니다.', 'empty'));
      for (const session of data.sessions) { const card = el('div', null, 'card'); card.append(el('h3', session.test_id === 'drive' ? '주행 정합성 기록' : '조작 검증 기록')); card.append(el('p', session.id, 'fine')); card.append(el('p', session.reason)); const dl = el('div', null, 'downloads'); links(dl, session.id, session.files); card.append(dl); host.append(card); }
    } catch (error) { message(error.message, true); }
  }
  document.querySelectorAll('[data-tab]').forEach(button => button.addEventListener('click', () => {
    document.querySelectorAll('.panel').forEach(panel => { panel.hidden = panel.id !== button.dataset.tab; });
    document.querySelectorAll('[data-tab]').forEach(tab => tab.classList.toggle('selected', tab === button));
    if (button.dataset.tab === 'catalog') refreshCatalog(); if (button.dataset.tab === 'history') refreshHistory();
  }));
  $('startDrive').addEventListener('click', () => act('start', {test_id:'drive'}));
  $('stop').addEventListener('click', () => act('stop'));
  $('startGuided').addEventListener('click', () => act('start', {test_id:$('test').value}));
  $('mark').addEventListener('click', () => act('mark'));
  $('refreshCatalog').addEventListener('click', refreshCatalog);
  $('refreshHistory').addEventListener('click', refreshHistory);
  $('search').addEventListener('input', renderCatalog);
  poll();
})();
