const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(require('node:path').join(__dirname, '../static/queue.js'), 'utf8');
const extract = name => source.match(new RegExp('  (?:async )?function '+name+'\\([^]*?\\n  }'))[0];
function setup() {
  const elements = new Map();
  const element = id => {
    if (!elements.has(id)) elements.set(id, {value:'old search', style:{},classList:{remove(){},add(){},toggle(){}},setAttribute(k,v){this[k]=v;}});
    return elements.get(id);
  };
  const ctx = { document:{getElementById:element,querySelector:()=>null,body:element('body')},
    localStorage:{setItem(){}}, queueMode:'movies', _queueStatusFilter:'error|no_dir',
    _queueErrorsOnly:true,_queueMatchedOnly:true,_queueVicesOnly:true,_queueCurrentFolder:'@studio:Example',
    _queuePage:4,_queueLoadGen:2,_lastRenderedSig:'old',_queueStatsPayload:null,
    closeMovieSearchPanel(){},clearSelectedFile(){},loadQueue(){},_prefetchInactiveQueue(){},loadQueueStats(){},
    _renderQueueStatusChip(){},_recomputeQueueFilter(){},_renderQueuePage(){},
  };
  vm.createContext(ctx);
  vm.runInContext(['_resetQueueFilters','setQueueMode','clearAllQueueFilters','filterQueueByStatus'].map(extract).join('\n'),ctx);
  return {ctx,element};
}
test('Scenes counter clears every filter, folder and page together',()=>{
  const {ctx,element}=setup(); ctx.setQueueMode('scenes');
  assert.equal(ctx.queueMode,'scenes');
  for(const key of ['_queueErrorsOnly','_queueMatchedOnly','_queueVicesOnly']) assert.equal(ctx[key],false);
  assert.equal(ctx._queueStatusFilter,'');assert.equal(ctx._queueCurrentFolder,'');assert.equal(ctx._queuePage,0);
  assert.equal(element('btnQueueErrorsOnly')['aria-pressed'],'false');assert.equal(element('queueFilter').value,'');
  assert.equal(ctx._queueLoadGen,3);
});
test('Total clears folder and secondary filters',()=>{
  const {ctx}=setup();ctx.clearAllQueueFilters();
  assert.equal(ctx._queueCurrentFolder,'');assert.equal(ctx._queueMatchedOnly,false);assert.equal(ctx._queueVicesOnly,false);
});
test('Errors remains in Movies',()=>{
  const {ctx}=setup();ctx.filterQueueByStatus('error|no_dir');assert.equal(ctx.queueMode,'movies');
  assert.equal(ctx._queueErrorsOnly,true);assert.equal(ctx._queueCurrentFolder,'');
});
test('late Movies response cannot overwrite newly selected Scenes',async()=>{
  const {ctx}=setup();let resolve;
  Object.assign(ctx,{_movieQueuePayload:null,fetch:()=>new Promise(r=>resolve=r),_applyMovieQueuePayload(){throw Error('stale payload applied');}});
  vm.runInContext(extract('loadMovieQueue'),ctx);
  const pending=ctx.loadMovieQueue({});ctx.setQueueMode('scenes');
  resolve({json:async()=>({files:[]})});await pending;
  assert.equal(ctx.queueMode,'scenes');
});
