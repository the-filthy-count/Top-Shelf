const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(require('node:path').join(__dirname, '../static/queue.js'), 'utf8');
const code = source.match(/  function _qCall\([^]*?\n  }/)[0];

test('delete action retains the clicked file ID across a queue refresh', () => {
  const calls = [];
  const context = {queueMode:'scenes', window:{_queueFiles:[{filename:'other.mp4', file_id:'other-id'}],
    confirmDeleteQueueFile:(...args)=>calls.push(args)}};
  vm.createContext(context); vm.runInContext(code, context);
  const row = {getAttribute:key=>({'data-filename':'old-name.mp4','data-file-id':'clicked-id'})[key]};
  context._qCall('confirmDeleteQueueFile', {closest:()=>row});
  assert.equal(calls[0][0], 'old-name.mp4');
  assert.equal(calls[0][1].file_id, 'clicked-id');
  assert.equal(calls[0][1].queue_kind, 'scene');
});

test('movie deletion carries the movie scope and stable ID', () => {
  let received;
  const context = {queueMode:'movies', window:{_queueFiles:[{filename:'movie.mp4',file_id:'movie-id'}],
    confirmDeleteQueueFile:(...args)=>received=args}};
  vm.createContext(context); vm.runInContext(code, context);
  context._qCall('confirmDeleteQueueFile', 0);
  assert.equal(received[1].file_id,'movie-id');
  assert.equal(received[1].queue_kind,'movie');
});

test('manual scene and vice actions retain the clicked identity', () => {
  for (const name of ['openManualSearch', 'openManual', 'openVice']) {
    let received;
    const context = {queueMode:'scenes', window:{_queueFiles:[], [name]:(...args)=>received=args}};
    vm.createContext(context); vm.runInContext(code, context);
    const row = {getAttribute:key=>({'data-filename':'old.mp4','data-file-id':'original'})[key]};
    context._qCall(name, {closest:()=>row});
    assert.equal(received[1].file_id, 'original');
  }
});
