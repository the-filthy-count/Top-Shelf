const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const source = fs.readFileSync(path.join(__dirname, '../static/ts-utils.js'), 'utf8');
const fn = source.match(/  function _releaseKeywordMatches\([^]*?\n  }/)[0];
const ctx = {}; vm.createContext(ctx); vm.runInContext(fn, ctx);
const matches = ctx._releaseKeywordMatches;
test('keywords ignore separators, order, accents and case', () => {
  assert.equal(matches('Lea.Pam.Outdoor-Shower.1080p', 'shower outdoor'), true);
  assert.equal(matches('Léa_Pam_Outdoor', 'lea pam'), true);
  assert.equal(matches('Don’t.Stop', "dont stop"), true);
  assert.equal(matches('東京.Outdoor', '東京 outdoor'), true);
});
test('all keywords are required and substrings do not match inside words', () => {
  assert.equal(matches('Outdoor Shower 1080p', 'out shower 1080'), true);
  assert.equal(matches('Outdoor Shower', 'outdoor studio'), false);
  assert.equal(matches('Canal Trip', 'anal'), false);
  assert.equal(matches(null, 'shower'), false);
  assert.equal(matches('Any release', '   '), true);
});
test('inline filter applies typing and Enter, and clears without closing the profile', () => {
  const popup = fs.readFileSync(path.join(__dirname, '../static/performer-popup.js'), 'utf8');
  const wire = popup.match(/  function wireReleaseKeywordFilter\([^]*?\n  }/)[0];
  const input = {value:'',handlers:{},addEventListener(k,fn){this.handlers[k]=fn;},focus(){this.focused=true;}};
  const clear = {handlers:{},addEventListener(k,fn){this.handlers[k]=fn;}};
  const host = {dataset:{},querySelector:s=>s==='[data-release-keyword]'?input:clear};
  const applied=[];
  const context={window:{filterEmbeddedProwlarrKeyword:(h,q)=>applied.push(q)}};
  vm.createContext(context);vm.runInContext(wire,context);context.wireReleaseKeywordFilter(host);
  input.value='outdoor';input.handlers.input();assert.equal(applied.at(-1),'outdoor');
  let prevented=false,stopped=false;
  input.handlers.keydown({key:'Enter',preventDefault(){prevented=true;},stopPropagation(){stopped=true;}});
  assert.ok(prevented && stopped);assert.equal(applied.at(-1),'outdoor');
  clear.handlers.click();assert.equal(applied.at(-1),'');assert.equal(clear.disabled,true);assert.equal(input.focused,true);
  input.value='test';input.handlers.keydown({key:'Escape',preventDefault(){},stopPropagation(){}});
  assert.equal(applied.at(-1),'');
});
