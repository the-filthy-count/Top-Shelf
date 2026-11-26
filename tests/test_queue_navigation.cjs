const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(require('node:path').join(__dirname, '../static/queue.js'), 'utf8');
const extract = name => source.match(new RegExp('  (?:async )?function '+name+'\\([^]*?\\n  }'))[0];
function setup(fetch) {
  const ctx = {_renderQueueActivity(){}, fetch, AbortController, setTimeout, clearTimeout, _queuePayloadReads:new Map()};
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

test('successful loading polls renew the request deadline instead of timing out the build',async()=>{
  let elapsed=0, nextId=0, calls=0;
  const timers=new Map();
  const ctx=setup(async()=>{
    elapsed+=10000;
    for(const [id,timer] of [...timers]) {
      if(timer.at<=elapsed){ timers.delete(id);timer.fn(); }
    }
    calls++;
    return {ok:true,json:async()=>calls<5?{loading:true}:{files:['ready']}};
  });
  ctx.setTimeout=(fn,delay)=>{
    if(delay===500){fn();return 0;}
    const id=++nextId;timers.set(id,{fn,at:elapsed+delay});return id;
  };
  ctx.clearTimeout=id=>timers.delete(id);
  assert.deepEqual((await ctx._readQueuePayload('/api/queue')).files,['ready']);
  assert.equal(calls,5);
  assert.equal(timers.size,0);
});

function activity(data, elapsed = null, offline = false) {
  const ctx = {}; vm.createContext(ctx);
  vm.runInContext(extract('_queueActivityView'), ctx);
  return ctx._queueActivityView(data, elapsed, offline);
}
test('queue loading is separate from idle matching, with a long wait explanation', () => {
  const view = activity({running:false}, 45);
  assert.equal(view.title, 'Matching idle');
  assert.match(view.detail, /Loading queue snapshot · 45s/);
  assert.match(view.detail, /Still waiting for the server/);
  assert.equal(view.percent, null);
});
test('matching shows its phase, current file and real completed count', () => {
  const view = activity({running:true,pipeline_phase:'Searching fingerprints',current_file:'example.mp4',pipeline_done:2,pipeline_total:8});
  assert.equal(view.title, 'Searching fingerprints');
  assert.match(view.detail, /example.mp4/);
  assert.match(view.detail, /2 \/ 8 files processed/);
  assert.equal(view.percent, 25);
});
test('provider waits are explicit and stale waits do not make idle work look active', () => {
  const data = {running:true,provider_waits:[{provider:'stashdb.org',seconds:42}]};
  assert.match(activity(data).detail, /stashdb.org: retry in 42s/);
  assert.equal(activity(data).title, 'Waiting for search services');
  assert.equal(activity({...data,running:false}).title, 'Matching idle');
  assert.equal(activity(data,null,true).title, 'Status unavailable — reconnecting…');
});
test('inactive queue reads cannot replace the active queue loading status', () => {
  const nodes = Object.fromEntries(['queueActivity','queueActivityTitle','queueActivityDetail','queueActivityProgress'].map(id=>[id,{}]));
  const ctx={document:{getElementById:id=>nodes[id],querySelector:()=>null},queueMode:'scenes',_queuePayloadReads:new Map([['/api/movies/queue',{started:0}]]),_queueActivity:{running:false},_queueActivityError:false};
  vm.createContext(ctx);
  vm.runInContext(extract('_queueActivityView')+'\n'+extract('_renderQueueActivity'),ctx);
  ctx._renderQueueActivity();
  assert.doesNotMatch(nodes.queueActivityDetail.textContent,/Loading queue/);
});

test('queue failures retain server reason and HTTP status', async () => {
  const ctx = setup(async () => ({ok:false,status:503,json:async()=>({error:'Queue scan failed. Check the server log and retry.'})}));
  await assert.rejects(ctx._readQueuePayload('/api/queue'), /HTTP 503: Queue scan failed/);
});
test('expired sessions explain that signing in is required', async () => {
  const ctx = setup(async () => ({ok:false,status:401,json:async()=>({error:'Unauthorised'})}));
  await assert.rejects(ctx._readQueuePayload('/api/queue'), /Your session has expired/);
});
