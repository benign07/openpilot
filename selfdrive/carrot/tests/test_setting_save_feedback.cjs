const fs=require('node:fs'), vm=require('node:vm'), assert=require('node:assert/strict'), path=require('node:path');
const source=fs.readFileSync(path.join(__dirname,'../web/js/pages/setting.js'),'utf8');
function block(marker) {
  const start=source.indexOf(marker); assert(start>=0); const brace=source.indexOf('{',start); let depth=0;
  for(let i=brace;i<source.length;i++) {
    if(source[i]==='{') depth++;
    if(source[i]==='}' && --depth===0) return source.slice(start,i+1);
  }
  throw Error('missing block');
}
const toasts=[], displayed=[];
const ctx=vm.createContext({profile:null,p:{min:0,max:1,default:1},name:'ShowLaneInfo',val:{dataset:{committedValue:'0',rawValue:'1'}},defaultBtn:{},
  unavailable:false,saveBusy:false,el:{},group:'x',originGroup:'x',refreshSettingAvailability:()=>{},cacheSettingValue:()=>{},
  syncSettingControlState:(el,v)=>displayed.push(v),clamp:(v,l,h)=>Math.max(l,Math.min(h,v)),
  setParam:async()=>{throw Error('write rejected');},showAppToast:text=>toasts.push(text),UI_STRINGS:{ko:{set_failed:'실패: '}},LANG:'ko',
  getUIText:(k,f)=>f,formatSettingDisplayValue:(p,x)=>String(x),appConfirm:async()=>true});
for(const marker of ['function normalizeSettingValue(','async function commitSettingValue(','defaultBtn.onclick = async (event) =>']) vm.runInContext(block(marker),ctx);
(async()=>{
  await ctx.defaultBtn.onclick({stopPropagation:()=>{}});
  assert.deepEqual(toasts,['실패: write rejected']); assert.deepEqual(displayed,['0']);
  assert.equal(ctx.val.dataset.committedValue,'0'); assert.equal(ctx.saveBusy,false);
  ctx.setParam=async()=>({ok:true,value:1,restart_required:false});
  assert.equal(await vm.runInContext('commitSettingValue(0)',ctx),true);
  assert.equal(ctx.val.dataset.committedValue,'1'); assert.equal(displayed.at(-1),1);
  console.log('2 setting-save regressions passed (failed reset, canonical saved value)');
})().catch(e=>{console.error(e);process.exitCode=1;});
