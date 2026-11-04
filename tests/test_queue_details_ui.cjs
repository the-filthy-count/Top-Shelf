const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const source = fs.readFileSync(path.join(__dirname, '../static/queue.js'), 'utf8');
const code = source.slice(source.indexOf('  let _qSearchRecord = null;'), source.indexOf('  async function openManualSearch'));

function setup() {
  const nodes = {};
  for (const id of ['srchTitle','srchStudio','srchDate','srchTerm','srchSceneNumber','srchPerformer','mPlot','srchTag','srchExtraKind','qDetailsStatus','qDetailsTechnical']) {
    nodes[id] = {value:'', textContent:'', dataset:{}};
  }
  let perfs = [], tags = [];
  const context = {document:{getElementById:id=>nodes[id]},
    _qOpenSearchSeq:1,
    _perfPillsValue:()=>perfs.join(', '), _perfPillsSet:n=>{perfs=n},
    _tagPillsValue:()=>tags.join(', '), _tagPillsSet:n=>{tags=n},
    window:{}, URLSearchParams};
  vm.createContext(context);
  vm.runInContext(code, context);
  const record = {file_id:'stable',filename:'sample.mp4',revision:0,edited:false,technical:{phash:'abc'},
    fields:{title:'Original', performers:['Jane Doe'],studio:'',date:'',scene_number:null,term:'',description:'',tags:[],extra_kind:'__auto__'}};
  context._qApplySearchRecord(record);
  return {context,nodes,record};
}

test('searching unchanged defaults does not save them as manual overrides', async () => {
  const {context,nodes} = setup();
  context.window.tsModalFetch = ()=>{ throw Error('No save expected'); };
  assert.equal(await context.saveQueueSearchDetails(), true);
  assert.match(nodes.qDetailsTechnical.textContent, /abc/);
});

test('corrections save with stable identity and revision', async () => {
  const {context,nodes,record} = setup();
  nodes.srchTitle.value = 'Correction';
  let sent;
  context.window.tsModalFetch = async (owner,url,options) => {
    sent = JSON.parse(options.body);
    return {ok:true,json:async()=>({...record,revision:1,edited:true,fields:sent.fields})};
  };
  assert.equal(await context.saveQueueSearchDetails(), true);
  assert.equal(sent.file_id, 'stable');
  assert.equal(sent.revision, 0);
  assert.equal(sent.fields.title, 'Correction');
  assert.match(nodes.qDetailsStatus.textContent, /^Saved/);
});

test('typing during a save keeps new text and defers searching', async () => {
  const {context,nodes,record} = setup();
  nodes.srchTitle.value = 'First edit';
  context.window.tsModalFetch = async (owner,url,options) => {
    const sent = JSON.parse(options.body);
    nodes.srchTitle.value = 'Newer edit';
    return {ok:true,json:async()=>({...record,revision:1,edited:true,fields:sent.fields})};
  };
  assert.equal(await context.saveQueueSearchDetails(), false);
  assert.equal(nodes.srchTitle.value, 'Newer edit');
  assert.match(nodes.qDetailsStatus.textContent, /latest edits/);
});

test('failed writes retain edits and stop search', async () => {
  const {context,nodes} = setup();
  nodes.srchTitle.value = 'Unsaved';
  context.window.tsModalFetch = async ()=>({ok:false,json:async()=>({error:'Revision changed'})});
  assert.equal(await context.saveQueueSearchDetails(), false);
  assert.equal(nodes.srchTitle.value, 'Unsaved');
  assert.equal(nodes.qDetailsStatus.dataset.error, 'true');
});
