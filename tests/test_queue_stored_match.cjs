const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const src = fs.readFileSync('static/queue.js', 'utf8');
const start = src.indexOf('  async function useThisScene(');
const end = src.indexOf('\n  /* IAFD scene-picker', start);
const elements = new Map();
const el = id => { if (!elements.has(id)) elements.set(id, {value:'', style:{}}); return elements.get(id); };
let names;
const context = vm.createContext({
  window:{_qSearchResults:[], toast(){}}, document:{getElementById:el},
  _qSelectedFile:'file.mp4', _qConfirmScene:null, _qConfirmPerformers:[],
  _mPerfLoadLibrary:async()=>{}, _perfPillsSet:v=>{names=v;}, _tagPillsSet:()=>{},
  mThumbPreview:()=>{}, _mRenderMatchBanner:()=>{}, _setManualExtrasOpen:()=>{}, console,
});
vm.runInContext(src.slice(start,end), context);
(async()=>{
 const performer = {id:'stable-id',name:'Provider Name'};
 const row = {source:'StashDB',_raw:{id:'scene-id',title:'Matched title',release_date:'2020-01-02',_source:'StashDB',performers:[{performer}]}};
 await context.useThisScene(-1,undefined,{row,isCurrent:()=>true,preserveFields:()=>false});
 assert.equal(context._qConfirmPerformers[0].id,'stable-id');
 assert.equal(el('srchTitle').value,'Matched title');
 assert.equal(names[0],'Provider Name');
 el('srchTitle').value='My correction';
 await context.useThisScene(-1,undefined,{row,isCurrent:()=>true,preserveFields:()=>true});
 assert.equal(el('srchTitle').value,'My correction');
 assert.equal(context._qConfirmPerformers[0].id,'stable-id');
 context._qConfirmScene=null;
 await context.useThisScene(-1,undefined,{row,isCurrent:()=>false,preserveFields:()=>false});
 assert.equal(context._qConfirmScene,null);
 console.log('Stored match hydration: ID, edits, and stale response checks passed');
})().catch(e=>{console.error(e);process.exitCode=1;});
