const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const source = fs.readFileSync(path.join(__dirname, '../web/js/simple_hud.js'), 'utf8');
const elements = Object.fromEntries(['mode','modeTxt','modeHint','speed','gear','dot','stat','unit'].map(n=>[n,{style:{}}]));
const context = vm.createContext({document:{getElementById:n=>elements[n]}, Date, AbortSignal});
vm.runInContext(source.slice(0, source.indexOf('document.getElementById("mode").addEventListener')), context);
const run = code => vm.runInContext(code, context);
function snapshot() {
  const services = ['carState','carControl','selfdriveState','longitudinalPlan'];
  return {ok:true,snapshotAgeMs:10,runtime:{params:{IsMetric:true},
    serviceAlive:Object.fromEntries(services.map(n=>[n,true])),serviceValid:Object.fromEntries(services.map(n=>[n,true])),
    serviceAgeMs:Object.fromEntries(services.map(n=>[n,20]))}, services:{
    carState:{canValid:true,vEgo:0,vEgoCluster:0,gearShifter:'park'},
    carControl:{enabled:false,latActive:false,longActive:false},selfdriveState:{enabled:false,active:false},
    longitudinalPlan:{myDrivingMode:2}}};
}
let count=0;
function test(name, fn) { fn(); count++; console.log('PASS', name); }
test('actual runtime mode wins over stored preference',()=>{
  context.data=snapshot(); run('requestedMode=4; acceptRuntime(data)');
  assert.equal(elements.modeTxt.textContent,'완만'); assert.match(elements.modeHint.textContent,/저장값: 고속/);
  assert.equal(elements.mode.disabled,false);
});
test('manual movement prevents mode changes',()=>{
  context.data=snapshot(); context.data.services.carState.vEgo=10; run('acceptRuntime(data)');
  assert.equal(elements.mode.disabled,true);
});
test('lateral-only control prevents changes',()=>{
  context.data=snapshot(); context.data.services.carControl.latActive=true; run('acceptRuntime(data)');
  assert.equal(elements.mode.disabled,true);
});
test('fresh HTTP with stale CAN clears old speed and green state',()=>{
  context.data=snapshot(); context.data.runtime.serviceAgeMs.carState=2000; run('acceptRuntime(data)');
  assert.equal(elements.speed.textContent,'--'); assert.equal(elements.gear.textContent,'--'); assert.equal(elements.dot.style.background,'#E5534B');
});
test('invalid CAN and missing freshness both fail closed',()=>{
  for (const mutate of [d=>d.services.carState.canValid=false,d=>delete d.runtime.serviceAgeMs,d=>d.runtime.serviceValid.carState=false]) {
    context.data=snapshot(); mutate(context.data); assert.equal(run('acceptRuntime(data)'),false);
    assert.equal(elements.mode.disabled,true);
  }
});
test('missing planner never labels stored mode as actual',()=>{
  context.data=snapshot(); context.data.runtime.serviceAlive.longitudinalPlan=false; run('acceptRuntime(data)');
  assert.equal(elements.modeTxt.textContent,'--');
});
(async()=>{
  context.data=snapshot(); run('requestedMode=4; acceptRuntime(data)');
  context.fetch=async()=>({ok:false,status:409,json:async()=>({ok:false,error:'정차/P 필요'})});
  await run('changeMode()');
  assert.equal(run('requestedMode'),4); assert.equal(elements.modeTxt.textContent,'완만');
  assert.equal(elements.stat.textContent,'정차/P 필요'); count++;
  console.log(`PASS rejected save retains both actual mode and stored preference\n${count} HUD regressions passed`);
})().catch(error=>{console.error(error);process.exitCode=1;});
