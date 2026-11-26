const test = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const source = fs.readFileSync(require('node:path').join(__dirname, '../static/queue.js'), 'utf8');
const code = source.slice(source.indexOf('  window.openQueueUrlImport ='), source.indexOf('  function openQueueSearchDetails('));
function setup(fetch) {
  const elements = new Map();
  const get = id => {if (!elements.has(id)) elements.set(id,{value:'existing'}); return elements.get(id);};
  const parts = {'input':{value:'https://example.com/scene',focus(){}},'form':{},'[type=button]':{},'[type=submit]':{},'[role=status]':{}};
  const dialog = {querySelector:s=>parts[s],addEventListener(t,fn){this[t+'Handler']=fn;},showModal(){this.open=true;},close(){this.open=false;this.closeHandler();},remove(){}};
  const ctx = {window:{},AbortController,fetch,document:{createElement:()=>dialog,body:{appendChild(){}},getElementById:get},
    _qSelectedFile:'file.mp4',_qSearchRecord:{},_qDetailsLoading:false,
    openQueueSearchDetails:async()=>{},_perfPillsSet:n=>ctx.names=n,_tagPillsSet:n=>ctx.tags=n,
    _qDetailsStatus:t=>ctx.status=t,mThumbPreview(){}};
  vm.createContext(ctx); vm.runInContext(code,ctx);
  ctx.window.openQueueUrlImport('file.mp4',{file_id:'stable-id'});
  return {ctx,parts,dialog,get,submit:()=>parts.form.onsubmit({preventDefault(){}})};
}
test('URL data populates editor without blanking absent fields or automatically saving',async()=>{
 const s=setup(async()=>({ok:true,json:async()=>({title:'Imported',performers:['Liz Jordan'],provider:'Webpage metadata'})}));
 await s.submit(); assert.equal(s.get('srchTitle').value,'Imported');assert.equal(s.get('srchStudio').value,'existing');
 assert.deepEqual(s.ctx.names,['Liz Jordan']);assert.match(s.ctx.status,/review and save/);assert.equal(s.dialog.open,false);
});
test('closing popup while scrape is pending cannot update editor',async()=>{
 let resolve;const s=setup(()=>new Promise(r=>resolve=r));const pending=s.submit();s.dialog.close();
 resolve({ok:true,json:async()=>({title:'Stale'})});await pending;
 assert.equal(s.get('srchTitle').value,'existing');
});
test('scrape failure stays in popup and allows retry',async()=>{
 const s=setup(async()=>({ok:false,json:async()=>({error:'Blocked webpage'})}));await s.submit();
 assert.equal(s.parts['[role=status]'].textContent,'Blocked webpage');assert.equal(s.parts['[type=submit]'].disabled,false);
 assert.equal(s.dialog.open,true);
});
