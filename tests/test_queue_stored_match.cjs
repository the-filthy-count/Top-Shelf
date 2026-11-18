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
  _qSceneSelectionSeq:0, _qOpenSearchSeq:1, _qSelectedFile:'file.mp4', _qConfirmScene:null, _qConfirmPerformers:[],
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
 let release;
 context._mPerfLoadLibrary=()=>new Promise(resolve=>release=resolve);
 context.window._qSearchResults=[row];
 context._qConfirmScene=null;
 const pending=context.useThisScene(0);
 context._qSelectedFile='other.mp4'; context._qOpenSearchSeq++;
 release(); await pending;
 assert.equal(context._qConfirmScene,null, 'must not attach the old scene to a new file');
 const waits=[];
 context._mPerfLoadLibrary=()=>new Promise(resolve=>waits.push(resolve));
 const first=context.useThisScene(0);
 context.window._qSearchResults=[{...row,_raw:{...row._raw,id:'new-scene'}}];
 const second=context.useThisScene(0);
 waits[1](); await second; waits[0](); await first;
 assert.equal(context._qConfirmScene.id,'new-scene');
 const helperStart=src.indexOf('  function _qScenePerformerForName(');
 const submitEnd=src.indexOf('\n  async function generateThumb',helperStart);
 vm.runInContext(src.slice(helperStart,submitEnd),context);
 context._qsNormalizeMatchSource=s=>s;
 context._mPerfLibrary=[{name:'Library Alias',aliases:['Other Alias'],match_stashdb_id:'stable-id'}];
 context._qConfirmPerformers=[performer]; context._qConfirmScene=row._raw;
 assert.equal(context._qScenePerformerForName('Other Alias').id,'stable-id');
 context._mPerfLibrary.push({name:'Library Alias',aliases:['Other Alias'],match_stashdb_id:'different'});
 assert.equal(context._qScenePerformerForName('Other Alias').id,'', 'ambiguous aliases must not guess');
 context._mPerfLibrary.pop();
 let payload;
 context._perfPillsNames=()=>['Library Alias']; context._tagPillsNames=()=>[];
 context._postManualFileWithRetry=async p=>{payload=p;return {error:'test only'};};
 context.mThumbMode=null;
 el('srchStudio').value='Corrected Studio'; el('srchDate').value='2024-02-03';
 await context._submitScenePick(null,'');
 assert.equal(payload.scene.performers[0].performer.id,'stable-id');
 assert.equal(payload.scene.studio.name,'Corrected Studio');
 assert.equal(payload.scene.studio.id,undefined);
 assert.equal(payload.scene.release_date,'2024-02-03');
 console.log('Stored match hydration, selection races, aliases, and edited filing metadata passed');
})().catch(e=>{console.error(e);process.exitCode=1;});
