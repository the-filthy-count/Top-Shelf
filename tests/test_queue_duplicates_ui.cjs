const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('static/queue.js', 'utf8');
function setup(groups, response) {
  const status = {textContent:''};
  const controls = {};
  const calls = [];
  const ctx = { _queueDupGroups:groups, document:{getElementById:id => id === 'queueDupStatus' ? status : (controls[id] ||= {}), querySelectorAll:()=>[]},
    confirm:()=>true, fetch:async (url, opts)=>{calls.push(JSON.parse(opts.body));return {ok:response.ok,json:async()=>response};}, refreshQueueDuplicatesModal:async()=>{} };
  vm.createContext(ctx);
  vm.runInContext(source.slice(source.indexOf('  let _queueDupRemoving'),source.indexOf('  // ── Suggestions modal logic')),ctx);
  return {ctx,calls,status};
}
const group = {files:[{current_filename:'keep.mp4'},{current_filename:'remove.mp4'}]};
test('bulk removal protects the final copy',async()=>{
  const {ctx,calls,status}=setup([group],{ok:true,deleted:1});
  await ctx.queueDupRemoveNames(new Set(['keep.mp4','remove.mp4']));
  assert.equal(calls.length,0);assert.match(status.textContent,/Keep at least one/);
});
test('bulk removal uses one keeper per group and reports success',async()=>{
  const {ctx,calls,status}=setup([group],{ok:true,deleted:1});
  await ctx.queueDupRemoveNames(new Set(['remove.mp4']));
  assert.deepEqual(calls,[{keep_filename:'keep.mp4',delete_filenames:['remove.mp4']}]);
  assert.match(status.textContent,/Moved 1/);
});
test('partial failure is visible and preserves actual removal count',async()=>{
  const {ctx,status}=setup([group],{ok:false,deleted:1,error:'File is busy'});
  await ctx.queueDupRemoveNames(new Set(['remove.mp4']));
  assert.match(status.textContent,/Moved 1/);assert.match(status.textContent,/File is busy/);
});
test('ignored groups cannot be removed by bulk action',async()=>{
  const {ctx,calls}=setup([{...group,ignored:true}],{ok:true});
  await ctx.queueDupRemoveNames(new Set(['remove.mp4']));assert.equal(calls.length,0);
});
