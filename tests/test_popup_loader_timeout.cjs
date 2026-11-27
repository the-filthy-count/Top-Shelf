const test=require('node:test'), assert=require('node:assert/strict'), vm=require('node:vm'), fs=require('node:fs');
test('stalled viewer scripts expire and can be retried',async()=>{
 const timers=new Map(), scripts=[],events=[];let seq=0;
 const window={addEventListener(){},requestIdleCallback(){},dispatchEvent:e=>events.push(e)};
 const document={addEventListener(){},querySelector:()=>null,head:{appendChild:s=>scripts.push(s)},createElement:()=>({dataset:{},listeners:{},setAttribute(){},addEventListener(k,f){this.listeners[k]=f;},removeEventListener(k){delete this.listeners[k];},remove(){this.removed=true;}})};
 const context={window,document,CustomEvent:class{constructor(type,opts){this.type=type;this.detail=opts.detail;}},setTimeout:f=>{timers.set(++seq,f);return seq;},clearTimeout:id=>timers.delete(id)};
 vm.runInNewContext(fs.readFileSync('static/popup-loader.js','utf8'),context);
 const first=window.openPerformerPopup({});assert.equal(scripts.length,2);
 for(const fire of [...timers.values()])fire();await first;
 assert(scripts.every(s=>s.removed));assert.equal(events[0].type,'ts-popup-load-error');
 const retry=window.openPerformerPopup({});assert.equal(scripts.length,4);
 let opened=false;window.openPerformerPopup=()=>{opened=true;};
 for(const s of scripts.slice(2))s.listeners.load();await retry;
 assert(opened);assert.equal(timers.size,0);
});
