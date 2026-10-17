const test=require('node:test'),assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs');
const src=fs.readFileSync(require('node:path').join(__dirname,'../static/app-shell.js'),'utf8');
const start=src.indexOf('(function () {',src.indexOf('// Session activity heartbeat'));
const code=src.slice(start,src.indexOf('\n})();',start)+6);
const flush=()=>new Promise(r=>setImmediate(r));
function setup(){
 const events={},timers=new Map(),requests=[],redirects=[];let id=0;
 const ctx={Date,AbortSignal,setTimeout:(fn,ms)=>{timers.set(++id,{fn,ms});return id;},clearTimeout:i=>timers.delete(i),setInterval:()=>1,clearInterval(){},
 window:{location:{pathname:'/stars',search:'?q=test',replace:p=>redirects.push(p)},addEventListener:(n,f)=>events[n]=f},
 document:{readyState:'loading',visibilityState:'visible',documentElement:{style:{}},addEventListener:(n,f)=>events[n]=f},
 fetch:async url=>{requests.push(url);return ctx.response;}};
 ctx.response={ok:true,status:200,json:async()=>({password_set:true,authenticated:true,remaining_seconds:60})};
 vm.createContext(ctx);vm.runInContext(code,ctx);return {ctx,events,timers,requests,redirects};
}
test('server expiry schedules a password check without renewing idle session',async()=>{
 const s=setup();s.events.pageshow({persisted:false});await flush();
 const timer=[...s.timers.values()][0];assert.equal(timer.ms,60100);
 s.ctx.response={status:401,ok:false};timer.fn();await flush();
 assert.match(s.redirects[0],/^\/login\?next=/);assert.equal(s.ctx.document.documentElement.style.visibility,'hidden');
 assert.deepEqual(s.requests,['/api/auth/status','/api/auth/status']);
});
test('open access does not schedule password lock',async()=>{
 const s=setup();s.ctx.response={ok:true,status:200,json:async()=>({password_set:false})};
 s.events.pageshow({persisted:false});await flush();assert.equal(s.timers.size,0);assert.equal(s.redirects.length,0);
});
test('activity renews the deadline from server response',async()=>{
 const s=setup();s.events.pointerdown();await flush();
 assert.deepEqual(s.requests,['/api/auth/heartbeat']);assert.equal([...s.timers.values()][0].ms,60100);
});
