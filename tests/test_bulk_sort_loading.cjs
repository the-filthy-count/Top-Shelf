const assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs');
const source=fs.readFileSync('static/queue.js','utf8');
const code=source.match(/  async function _bulkFolderPopulateAsync\(items\) \{[\s\S]*?\n  \}/)[0];
let notify;const started=[];const observed=[];const body={};
const context={_bulkFolderState:{},_bulkFolderRecordedDate:f=>f.parsed?.file_mtime||'',document:{getElementById:()=>({closest:()=>body}),querySelectorAll:()=>[{dataset:{strip:'0'}},{dataset:{strip:'1'}}]},
 IntersectionObserver:class{constructor(fn,opts){notify=fn;assert.equal(opts.root,body);}observe(el){observed.push(el);}unobserve(){}},
 window:{tsModalFetch:()=>new Promise(()=>{})},_bulkFolderLoadFilmstrip:(idx,name)=>started.push(name)};
vm.createContext(context);vm.runInContext(code,context);
context._bulkFolderPopulateAsync([{filename:'one'},{filename:'two'}]);
assert.equal(started.length,0);
notify([{target:observed[0],isIntersecting:true},{target:observed[1],isIntersecting:false}]);
assert.deepEqual(started,['one'],'visible frames start even when all date requests are stalled');
context._bulkFolderState={};notify([{target:observed[1],isIntersecting:true}]);
assert.deepEqual(started,['one'],'old folder observer cannot load into new folder');
console.log('PASS: visible strips load independently of dates; stale folder callbacks ignored');

const helper=source.match(/  function _bulkFolderRecordedDate\(file\) \{[\s\S]*?\n  \}/)[0];
vm.runInContext(helper,context);
assert.equal(context._bulkFolderRecordedDate({parsed:{file_mtime:'2026-11-28'}}),'2026-11-28');
let requests=0;context.window.tsModalFetch=()=>{requests++;throw new Error('Unexpected date lookup');};
context._bulkFolderPopulateAsync([{filename:'cached',parsed:{file_mtime:'2026-11-28'}}]).then(()=>assert.equal(requests,0));
