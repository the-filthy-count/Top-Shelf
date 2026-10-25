const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const read = name => fs.readFileSync(path.join(__dirname, '../static/', name), 'utf8');
const extract = (src,name) => src.match(new RegExp('  (?:async )?function '+name+'\\([^]*?\\n  }'))[0];
function loader() {
  const scripts=[]; const events={}; const docEvents={}; const errors=[];
  const window={addEventListener:(n,f)=>events[n]=f,requestIdleCallback(){},toast:m=>errors.push(m)};
  const document={addEventListener:(n,f)=>docEvents[n]=f,
    querySelector:s=>scripts.find(e=>s.includes(e.src)) || null,
    createElement:()=>({dataset:{},setAttribute(){},remove(){scripts.splice(scripts.indexOf(this),1);}}),
    head:{appendChild:s=>scripts.push(s)}};
  vm.runInNewContext(read('popup-loader.js'),{window,document,Promise});
  return {window,scripts,events,docEvents,errors};
}
test('lazy popup opens only the latest requested record', async()=>{
  const ctx=loader(); const stub=ctx.window.openPerformerPopup;
  const one=stub({name:'first'}),two=stub({name:'second'}); const opened=[];
  ctx.window.openPerformerPopup=opts=>opened.push(opts.name);
  ctx.scripts.forEach(s=>s.onload());
  await Promise.all([one,two]); assert.deepEqual(opened,['second']);
});
test('failed popup scripts are removed and can be retried', async()=>{
  const ctx=loader(); const stub=ctx.window.openStudioPopup;
  const first=stub({name:'one'});
  const broken=ctx.scripts.find(s=>s.src.includes('studio-popup'));
  broken.onerror(); ctx.scripts.forEach(s=>s.onload()); await first;
  assert.equal(ctx.errors.length,1); assert(!ctx.scripts.includes(broken));
  const next=stub({name:'two'});const opened=[];
  ctx.window.openStudioPopup=o=>opened.push(o.name);
  ctx.scripts.forEach(s=>s.onload()); await next; assert.deepEqual(opened,['two']);
});
for(const cancel of ['pagehide','Escape']) test('pending popup respects '+cancel,async()=>{
  const ctx=loader();const promise=ctx.window.openPerformerPopup({name:'one'});const opened=[];
  if(cancel==='pagehide')ctx.events.pagehide(); else ctx.docEvents.keydown({key:'Escape'});
  ctx.window.openPerformerPopup=o=>opened.push(o.name);ctx.scripts.forEach(s=>s.onload());
  await promise; assert.equal(opened.length,0);
});
test('slow queue poll is not repeatedly aborted by polling ticks',async()=>{
  let calls=0;const src=read('queue.js');
  const ctx={document:{visibilityState:'visible'},_queueLoadGen:1,_queueStatusAbort:null,
    AbortController,setTimeout,clearTimeout,fetch:(url,{signal})=>{calls++;return new Promise((resolve,reject)=>signal.addEventListener('abort',()=>reject(new DOMException('cancelled','AbortError'))));}};
  vm.createContext(ctx);vm.runInContext(extract(src,'pollQueueStatus'),ctx);
  const first=ctx.pollQueueStatus();const controller=ctx._queueStatusAbort;
  await ctx.pollQueueStatus();assert.equal(calls,1);assert.equal(controller.signal.aborted,false);
  controller.abort();await first;assert.equal(ctx._queueStatusAbort,null);
});
test('Home keeps mounted cards when refresh data is unchanged',async()=>{
  const src=read('home.html');let renders=0;
  const ctx={_homeRequestPending:false,_homeLoadController:null,_homeRenderSignature:null,
    AbortController,setTimeout,clearTimeout,_lastUpdatedAt:0,
    _allScenes:[],_allPerformers:[],_allMovies:[],_allJav:[],_allDiscovered:[],
    _favScenes:[],_favPerformers:[],_favMovies:[],_favJav:[],_scenesByKind:{},
    document:{getElementById:()=>null},fetch:async()=>({ok:true,json:async()=>({scenes:[{id:1}]})}),
    _mergeScenesByKind:()=>({}),_renderAll:()=>renders++,_startBgPoll(){},_stopBgPoll(){}};
  vm.createContext(ctx);vm.runInContext(extract(src,'loadHomeData'),ctx);
  await ctx.loadHomeData();await ctx.loadHomeData();assert.equal(renders,1);
  ctx.fetch=async()=>({ok:true,json:async()=>({scenes:[{id:2}]})});
  await ctx.loadHomeData();assert.equal(renders,2);
});
test('Back restores queue polling once and ordinary pageshow does not duplicate it',()=>{
  const src=read('queue.js');let polls=0;const timers=new Set();let next=0;
  const ctx={TS_DL_PAGE:'queue',_queuePollTimer:null,_queuePollIntervalMs:5000,
    loadQueue(){},loadQueueStats(){},pollQueueStatus:()=>polls++,setInterval:()=>{timers.add(++next);return next;},clearInterval:id=>timers.delete(id),
    window:{addEventListener:(name,fn)=>ctx.restore=fn}};
  vm.createContext(ctx);
  vm.runInContext(extract(src,'startQueuePolling')+'\n'+extract(src,'stopQueuePolling')+'\n'+src.match(/window.addEventListener\('pageshow', function \(event\) \{[^]*?\n  \}\);/)[0],ctx);
  ctx.restore({persisted:false});assert.equal(polls,0);
  ctx.restore({persisted:true});ctx.restore({persisted:true});assert.equal(polls,2);assert.equal(timers.size,1);
});
test('library cancellation does not overwrite the existing grid data',async()=>{
  const src=read('favourites.js');const old=[{id:1}];
  const ctx={_kindLoadGenerations:new Map(),_kindControllers:new Map(),_libraryRevision:0,
    _rowsPerf:old,_loadedKinds:new Set(),AbortController,
    fetch:(url,{signal})=>new Promise((resolve,reject)=>signal.addEventListener('abort',()=>reject(new DOMException('cancelled','AbortError'))))};
  vm.createContext(ctx);vm.runInContext(extract(src,'_fetchKind'),ctx);
  const request=ctx._fetchKind('performer');ctx._kindControllers.get('performer').abort();
  await assert.rejects(request,{name:'AbortError'});
  assert.equal(ctx._rowsPerf,old);assert.equal(ctx._kindControllers.size,0);assert.equal(ctx._loadedKinds.size,0);
});
test('header polling resumes safely while an old cancelled request settles',async()=>{
  const src=read('app-shell.js');let calls=0;
  const ctx={dlStatsController:null,dlStatsTimer:1,AbortController,setTimeout,clearTimeout,clearInterval(){},
    fetch:(url,{signal})=>{calls++;return new Promise((resolve,reject)=>signal.addEventListener('abort',()=>reject(new DOMException('cancelled','AbortError'))));}};
  vm.createContext(ctx);vm.runInContext(extract(src,'pollDlStats')+'\n'+extract(src,'stopDlStatsPolling'),ctx);
  const first=ctx.pollDlStats();await ctx.pollDlStats();assert.equal(calls,1);
  ctx.stopDlStatsPolling();const second=ctx.pollDlStats();const current=ctx.dlStatsController;
  await first;assert.equal(ctx.dlStatsController,current);assert.equal(calls,2);
  ctx.stopDlStatsPolling();await second;assert.equal(ctx.dlStatsController,null);
});
