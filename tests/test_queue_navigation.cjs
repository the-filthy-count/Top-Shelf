const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(require('node:path').join(__dirname, '../static/queue.js'), 'utf8');
const extract = name => source.match(new RegExp('  (?:async )?function '+name+'\\([^]*?\\n  }'))[0];
function setup(fetch) {
  const ctx = {fetch, AbortController, setTimeout, clearTimeout, _queuePayloadReads:new Map()};
  vm.createContext(ctx);
  vm.runInContext(['_readQueuePayload','_cancelQueuePayloadReads'].map(extract).join('\n'),ctx);
  return ctx;
}
test('concurrent foreground and prefetch reads share one request',async()=>{
  let calls=0,resolve;
  const ctx=setup(()=>{calls++;return new Promise(r=>resolve=r);});
  const first=ctx._readQueuePayload('/api/queue');
  const second=ctx._readQueuePayload('/api/queue');
  assert.equal(first,second);assert.equal(calls,1);
  resolve({ok:true,json:async()=>({files:[]})});await first;
  assert.equal(ctx._queuePayloadReads.size,0);
});
test('leaving while JSON loads cancels it without clearing a replacement',async()=>{
  let body,respond;
  const ctx=setup(async()=>({ok:true,json:()=>new Promise(r=>body=r)}));
  const first=ctx._readQueuePayload('/api/queue');await new Promise(resolve => setImmediate(resolve));
  ctx._cancelQueuePayloadReads();
  ctx.fetch=()=>new Promise(r=>respond=r);
  const second=ctx._readQueuePayload('/api/queue');
  body({files:['stale']});await assert.rejects(first,{name:'AbortError'});
  assert.equal(ctx._queuePayloadReads.size,1);
  respond({ok:true,json:async()=>({files:['fresh']})});
  assert.deepEqual((await second).files,['fresh']);
});
test('HTTP failures release the read for retry',async()=>{
  const ctx=setup(async()=>({ok:false,status:503}));
  await assert.rejects(ctx._readQueuePayload('/api/queue'),/503/);
  assert.equal(ctx._queuePayloadReads.size,0);
});
for(const [name,payload] of [['loadQueue','_sceneQueuePayload'],['loadMovieQueue','_movieQueuePayload']]) {
  test(name+' retains usable results when refresh fails',async()=>{
    let notices=0;
    const ctx={_queueLoadGen:1,queueMode:'scenes',[payload]:{files:['existing']},
      _refreshViceSlugs(){},_refreshLibraryStudios(){},
      _readQueuePayload:async()=>{throw Error('offline');},
      document:{getElementById(){throw Error('grid must not be replaced');}},
      window:{showToast(){notices++;}}};
    vm.createContext(ctx);vm.runInContext(extract(name),ctx);
    await ctx[name]({force:true,preserveView:true});assert.equal(notices,1);
  });
}
