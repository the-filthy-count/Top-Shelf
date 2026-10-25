const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const src=fs.readFileSync(require('node:path').join(__dirname,'../static/theme-init.js'),'utf8');
async function setup(serverAccent, fail=false) {
  const styles=new Map(), values=new Map([['ts_ui_accent_override','#ff0000']]);
  const head={appendChild(el){el.parentNode=head;styles.set(el.id,el);},removeChild(el){styles.delete(el.id);}};
  const window={location:{pathname:'/'},addEventListener(){},localStorage:{getItem:k=>values.get(k),setItem:(k,v)=>values.set(k,v),removeItem:k=>values.delete(k)}};
  const document={documentElement:{setAttribute(){},removeAttribute(){}},head,getElementById:id=>styles.get(id),createElement:()=>({})};
  const ctx={window,document,fetch:async()=>({ok:!fail,json:async()=>({accent:serverAccent})})};
  vm.createContext(ctx);
  const body=src.slice(src.indexOf('  function _hexToRgbTriple'),src.lastIndexOf('})();'));
  vm.runInContext("var ACCENT_STORAGE_KEY='ts_ui_accent_override';\n"+body,ctx);
  await new Promise(resolve=>setImmediate(resolve));
  return {styles,values,window};
}
test('saved server accent replaces stale browser colour',async()=>{
 const {styles,values}=await setup('#008080');
 assert.match(styles.get('accent-override-style').textContent,/--accent: #008080/);
 assert.match(styles.get('accent-override-style').textContent,/0, 128, 128/);
 assert.equal(values.get('ts_ui_accent_override'),'#008080');
});
test('cleared server override removes stale local style',async()=>{
 const {styles,values}=await setup('');
 assert.equal(styles.size,0);assert.equal(values.size,0);
});
test('unavailable server keeps cached colour',async()=>{
 const {styles}=await setup('',true);
 assert.match(styles.get('accent-override-style').textContent,/#ff0000/);
});
