/* ============================================================
   widget_pwa.js — phone "widget"/focus mode for the Carrot web app.
   Loaded LAST (after app.js). In ?view=hud it overlays a SELF-CONTAINED
   HUD (its own DOM + polling of /api/live_runtime), so the layout is fully
   controlled and fits any viewport — full screen OR the small floating
   overlay window. It does NOT rely on the SPA's card/surfaces.
   ============================================================ */
(function () {
  "use strict";

  var FOCUS = "hud";
  var pollTimer = null;

  function getView() {
    try { return new URLSearchParams(location.search).get("view") || ""; }
    catch (e) { return ""; }
  }
  function setUrlView(on) {
    try {
      var u = new URL(location.href);
      if (on) u.searchParams.set("view", FOCUS); else u.searchParams.delete("view");
      history.replaceState(history.state || {}, "", u.toString());
    } catch (e) {}
  }

  // ---------- self-contained HUD ----------
  function buildHud() {
    if (document.getElementById("wfHud")) return;
    var host = document.createElement("div");
    host.id = "wfHud";
    host.innerHTML =
      '<div class="wfCard">' +
      '  <div class="wfHead"><span class="wfDot" id="wfDot"></span>' +
      '    <span class="wfHeadTxt" id="wfHeadTxt">연결 중…</span></div>' +
      '  <div class="wfMain">' +
      '    <div class="wfSpeed" id="wfSpeed">--</div>' +
      '    <div class="wfUnit" id="wfUnit">km/h</div>' +
      '  </div>' +
      '  <div class="wfRow">' +
      '    <div class="wfCell"><div class="wfVal wfSet" id="wfSet">--</div><div class="wfLbl">설정</div></div>' +
      '    <div class="wfCell"><div class="wfVal" id="wfGap">--</div><div class="wfLbl">차간</div></div>' +
      '    <div class="wfCell"><div class="wfVal wfLimit" id="wfLimit">--</div><div class="wfLbl">제한</div></div>' +
      '  </div>' +
      '</div>';
    document.body.appendChild(host);
  }

  function num(v) { return (typeof v === "number" && isFinite(v)) ? v : null; }
  function setTxt(id, t) {
    var e = document.getElementById(id);
    if (e && e.textContent !== t) e.textContent = t;
  }

  function updateHud(data) {
    var dot = document.getElementById("wfDot");
    var head = document.getElementById("wfHeadTxt");
    if (!data) {
      if (dot) dot.style.background = "#E5534B";
      if (head) head.textContent = "오프라인 (기기·WiFi 확인)";
      return;
    }
    var services = data.services || {};
    var params = (data.runtime && data.runtime.params) || {};
    var metric = String(params.IsMetric == null ? "1" : params.IsMetric) === "1";
    var f = metric ? 3.6 : 2.2369363;
    var cs = services.carState || null;
    var cm = services.carrotMan || null;

    var speed = null;
    if (cs) {
      var v = num(cs.vEgoCluster); if (v == null) v = num(cs.vEgo);
      if (v != null) speed = Math.max(0, Math.round(v * f));
    }
    var set = null;
    if (cs && cs.cruiseState) { var sv = num(cs.cruiseState.speed); if (sv != null && sv > 0) set = Math.round(sv * f); }
    if (set == null && cm) { var dv = num(cm.desiredSpeed); if (dv != null && dv > 0) set = Math.round(dv); }
    var gap = null;
    if (params.LongitudinalPersonality != null) {
      var g = parseInt(params.LongitudinalPersonality, 10);
      if (!isNaN(g)) gap = (g + 1);
    }
    var limit = null;
    if (cm) {
      var l = num(cm.nRoadLimitSpeed); if (l == null || l <= 0) l = num(cm.xSpdLimit);
      if (l != null && l > 0) limit = Math.round(l);
    }

    setTxt("wfSpeed", speed == null ? "--" : String(speed));
    setTxt("wfUnit", metric ? "km/h" : "mph");
    setTxt("wfSet", set == null ? "--" : String(set));
    setTxt("wfGap", gap == null ? "--" : String(gap));
    setTxt("wfLimit", limit == null ? "--" : String(limit));
    if (dot) dot.style.background = "#3DDC84";
    if (head) head.textContent = "현재속도";
  }

  function poll() {
    fetch("/api/live_runtime", { cache: "no-store" })
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (j) { updateHud(j && j.ok !== false ? j : (j || null)); })
      .catch(function () { updateHud(null); });
  }
  function startPoll() { if (pollTimer) return; poll(); pollTimer = setInterval(poll, 1000); }
  function stopPoll() { if (pollTimer) { clearInterval(pollTimer); pollTimer = null; } }

  // ---------- focus mode ----------
  function applyFocus(on) {
    document.body.setAttribute("data-view", on ? FOCUS : "");
    if (on) { buildHud(); startPoll(); }
    else {
      stopPoll();
      var h = document.getElementById("wfHud");
      if (h && h.parentNode) h.parentNode.removeChild(h);
    }
  }
  function enterFocus() { setUrlView(true); applyFocus(true); }
  function exitToSettings() {
    setUrlView(false);
    applyFocus(false);
    try { if (typeof showPage === "function") showPage("setting", true); } catch (e) {}
  }

  async function togglePip() {
    if (!("documentPictureInPicture" in window)) {
      alert("PiP(웹 떠있는 창)은 보안 연결(HTTPS)에서만 됩니다. 앱의 '오버레이'를 이용하세요.");
      return;
    }
    try {
      var pip = await documentPictureInPicture.requestWindow({ width: 320, height: 240 });
      document.querySelectorAll('link[rel="stylesheet"], style').forEach(function (s) {
        try { pip.document.head.appendChild(s.cloneNode(true)); } catch (e) {}
      });
      pip.document.body.setAttribute("data-view", FOCUS);
      pip.document.body.style.margin = "0";
      var host = document.getElementById("wfHud");
      var ph = document.createComment("wfhud");
      if (host) { host.parentNode.insertBefore(ph, host); pip.document.body.appendChild(host); }
      pip.addEventListener("pagehide", function () { if (ph.parentNode) ph.parentNode.insertBefore(host, ph); });
    } catch (e) { alert("PiP 실패: " + (e && e.message ? e.message : e)); }
  }

  function buildControls() {
    if (document.getElementById("wfBtns")) return;
    var bar = document.createElement("div");
    bar.id = "wfBtns";
    var setBtn = document.createElement("button");
    setBtn.type = "button"; setBtn.id = "wfSetBtn"; setBtn.title = "설정"; setBtn.textContent = "⚙";
    setBtn.addEventListener("click", exitToSettings);
    var pipBtn = document.createElement("button");
    pipBtn.type = "button"; pipBtn.id = "wfPip"; pipBtn.title = "PiP"; pipBtn.textContent = "▣";
    pipBtn.addEventListener("click", togglePip);
    bar.appendChild(setBtn); bar.appendChild(pipBtn);
    document.body.appendChild(bar);

    var launch = document.createElement("button");
    launch.type = "button"; launch.id = "wfLaunch"; launch.title = "위젯"; launch.textContent = "▤";
    launch.addEventListener("click", enterFocus);
    document.body.appendChild(launch);
  }

  function markStandalone() {
    try {
      var s = (window.matchMedia && window.matchMedia("(display-mode: standalone)").matches) ||
        window.navigator.standalone === true;
      if (s) document.body.classList.add("wf-standalone");
    } catch (e) {}
  }

  function init() {
    buildControls();
    markStandalone();
    applyFocus(getView() === FOCUS);
  }

  if (document.readyState === "complete") setTimeout(init, 0);
  else window.addEventListener("load", function () { setTimeout(init, 0); });
})();
