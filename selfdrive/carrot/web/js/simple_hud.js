"use strict";
let requestedMode = 0, effectiveMode = 0, modeAvailable = false, saveBusy = false, runtimeBusy = false, lastRuntimeAt = 0;
const MODES = {1:"연비", 2:"완만", 3:"일반", 4:"고속"};
function num(v) { return typeof v === "number" && Number.isFinite(v) ? v : null; }
function setTxt(id, text) { const node = document.getElementById(id); if (node) node.textContent = text; }
function serviceFresh(data, name, maxAge = 1000) {
  const runtime = data.runtime || {}, age = runtime.serviceAgeMs?.[name];
  return runtime.serviceAlive?.[name] === true && runtime.serviceValid?.[name] === true &&
    num(age) !== null && age >= 0 && age < maxAge;
}
function gearText(cs) {
  const gear = String(cs.gearShifter || "").toLowerCase();
  return ({park:"P", drive:"D", reverse:"R", neutral:"N"})[gear] || "--";
}
function renderMode() {
  const button = document.getElementById("mode");
  button.className = "mode" + (MODES[effectiveMode] ? " m" + effectiveMode : "");
  button.disabled = !modeAvailable || saveBusy;
  setTxt("modeTxt", MODES[effectiveMode] || "--");
  const requested = MODES[requestedMode];
  setTxt("modeHint", (effectiveMode ? "실제 모드" : "실제 모드 확인 중") +
    (requested && requestedMode !== effectiveMode ? ` · 저장값: ${requested}` : "") +
    (effectiveMode === 4 ? " · 신호 정지 OFF" : "") + " · 탭하여 운행모드 변경");
}
function markOffline(message = "차량 데이터 끊김") {
  modeAvailable = false; effectiveMode = 0;
  setTxt("speed", "--"); setTxt("gear", "--");
  document.getElementById("dot").style.background = "#E5534B";
  setTxt("stat", message); renderMode();
}
function acceptRuntime(data) {
  if (!data || data.ok !== true || num(data.snapshotAgeMs) === null || data.snapshotAgeMs < 0 ||
      data.snapshotAgeMs >= 1500 || !serviceFresh(data, "carState")) {
    markOffline(); return false;
  }
  const services = data.services || {}, cs = services.carState || {};
  if (cs.canValid !== true || num(cs.vEgo) === null) { markOffline(); return false; }
  lastRuntimeAt = Date.now();
  const metric = ![false, 0, "0"].includes(data.runtime?.params?.IsMetric);
  const speed = num(cs.vEgoCluster) ?? num(cs.vEgo);
  setTxt("speed", String(Math.max(0, Math.round(speed * (metric ? 3.6 : 2.2369363)))));
  setTxt("unit", metric ? "km/h" : "mph"); setTxt("gear", gearText(cs));
  effectiveMode = serviceFresh(data, "longitudinalPlan") ? Number(services.longitudinalPlan?.myDrivingMode || 0) : 0;
  modeAvailable = true;
  document.getElementById("dot").style.background = "#3DDC84";
  setTxt("stat", "차량 데이터 수신 중"); renderMode(); return true;
}
async function fetchJson(url, options = {}) {
  const response = await fetch(url, {...options, signal: AbortSignal.timeout(2500)});
  const data = await response.json();
  if (!response.ok || data.ok !== true) throw new Error(data.error || `HTTP ${response.status}`);
  return data;
}
async function pollRuntime() {
  if (runtimeBusy) return;
  runtimeBusy = true;
  try { acceptRuntime(await fetchJson("/api/live_runtime", {cache:"no-store"})); }
  catch (_) { markOffline(); }
  finally { runtimeBusy = false; }
}
async function pollMode() {
  try {
    const data = await fetchJson("/api/params_bulk?names=MyDrivingMode", {cache:"no-store"});
    requestedMode = Number(data.values?.MyDrivingMode || 0); renderMode();
  } catch (_) { /* Saved preference must never replace the actual runtime mode. */ }
}
async function changeMode() {
  if (!modeAvailable || saveBusy) return;
  saveBusy = true; renderMode();
  try {
    const next = requestedMode >= 1 && requestedMode <= 4 ? requestedMode % 4 + 1 : 1;
    const saved = await fetchJson("/api/param_set", {method:"POST", headers:{"Content-Type":"application/json"},
      body:JSON.stringify({name:"MyDrivingMode", value:next})});
    requestedMode = Number(saved.value); await pollRuntime();
  } catch (error) { setTxt("stat", error.message); }
  finally { saveBusy = false; renderMode(); }
}
document.getElementById("mode").addEventListener("click", changeMode);
pollRuntime(); pollMode();
setInterval(pollRuntime, 500);
setInterval(pollMode, 2000);
setInterval(() => { if (Date.now() - lastRuntimeAt > 1500) markOffline(); }, 250);
