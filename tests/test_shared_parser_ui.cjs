const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(require('node:path').join(__dirname, '../static/queue.js'), 'utf8');
const code = source.match(/  function _qApplyParseToManualSearch\([^]*?\n  }/)[0];

test('shared parse replaces the old studio guess without cleaning the title twice', () => {
  const fields = {srchTitle:{value:''},srchDate:{value:''},srchStudio:{value:'demi_hawks'},srchPerformer:{value:'Demi Hawks'}};
  const context = {document:{getElementById:id=>fields[id]},_stripTokensFromTitle:()=>{throw Error('Shared title must not be parsed twice');}};
  vm.createContext(context);vm.runInContext(code,context);
  context._qApplyParseToManualSearch({parser_version:3,title:'Summer Fling In Saratoga Springs',site:'FuckPassVR',date:'2023-10-13',performers:'Demi Hawks'},'demi_hawks',['Demi Hawks']);
  assert.equal(fields.srchTitle.value,'Summer Fling In Saratoga Springs');
  assert.equal(fields.srchStudio.value,'FuckPassVR');
  fields.srchTitle.value='My title';fields.srchStudio.value='My studio';
  context._qApplyParseToManualSearch({parser_version:3,title:'Late response',site:'Other'},'demi_hawks',['Demi Hawks']);
  assert.equal(fields.srchTitle.value,'My title');assert.equal(fields.srchStudio.value,'My studio');
});
