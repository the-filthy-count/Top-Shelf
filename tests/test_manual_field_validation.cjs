const test = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const source = fs.readFileSync(require('node:path').join(__dirname, '../static/queue.js'),'utf8');
const code = source.slice(source.indexOf('  function _validateManualFields()'), source.indexOf('  async function submitManual()'));
test('missing date blocks both submit paths and highlights field',()=>{
 const nodes = Object.fromEntries(['srchTitle','srchStudio','srchDate'].map(id=>[id,{value:id==='srchDate'?'':'Value',classList:{toggle(k,v){this.invalid=v;},remove(){this.invalid=false;}},setAttribute(){},removeAttribute(){},scrollIntoView(){},focus(){this.focused=true;}}]));
 const ctx={document:{getElementById:id=>nodes[id]},_qDetailsStatus(){}};vm.createContext(ctx);vm.runInContext(code,ctx);
 assert.equal(ctx._validateManualFields(),false);assert.equal(nodes.srchDate.classList.invalid,true);assert.equal(nodes.srchDate.focused,true);
 nodes.srchDate.value='2026-02-30';assert.equal(ctx._validateManualFields(),false);
 nodes.srchDate.value='2026-11-27';assert.equal(ctx._validateManualFields(),true);
 const submit=source.slice(source.indexOf('  async function submitManual()'));
 assert.ok(submit.indexOf('if (!_validateManualFields()) return;') < submit.indexOf('await saveQueueSearchDetails()'));
 assert.ok(submit.indexOf('if (!_validateManualFields()) return;') < submit.indexOf('await _submitScenePick('));
});
