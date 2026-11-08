const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('static/queue.js', 'utf8');
const preview = {removeAttribute(name) { delete this[name]; }};
const slots = Array.from({length: 5}, () => ({classList: {toggle(_, value) {this.picked = value;}}}));
const context = vm.createContext({document: {
  getElementById: () => preview,
  querySelectorAll: () => slots,
}});
const start = source.indexOf('  let _qsPickedFrame = null;');
const end = source.indexOf('  function _clearSearchFrames()', start);
vm.runInContext("let _searchFrameSources = ['frame0', 'frame1'];\n" + source.slice(start, end), context);
vm.runInContext('_showFileFrame(0)', context);
assert.equal(preview.src, 'frame0');
vm.runInContext("_showScenePreview('scene-poster')", context);
assert.equal(preview.src, 'scene-poster');
assert.ok(slots.every(s => !s.classList.picked));
// Late filmstrip responses must not replace the selected database image.
vm.runInContext('_showFileFrame(1)', context);
assert.equal(preview.src, 'scene-poster');
// Explicit frame selection relinquishes the database image override.
const pickStart = source.indexOf('  function _pickFrameAsThumb(i)');
const pickEnd = source.indexOf('    const pct =', pickStart);
vm.runInContext(source.slice(pickStart, pickEnd) + '\n}', context);
vm.runInContext('_pickFrameAsThumb(1)', context);
assert.equal(preview.src, 'frame1');
assert.equal(slots[1].classList.picked, true);
assert.match(source.slice(source.indexOf('  function mThumbPreview(url)'), source.indexOf('  // openManual is now')), /_showScenePreview\(url\)/);
console.log('Scene preview selection and polling regression checks passed');
