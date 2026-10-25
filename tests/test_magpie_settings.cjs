const {test}=require('node:test'),assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs');
const source=fs.readFileSync(require('node:path').join(__dirname,'../static/settings-page.js'),'utf8').split('/* Magpie Settings controls — loaded with the main Settings page. */')[1];
const flush=()=>new Promise(r=>setImmediate(r));
function setup(fail=false){
 const els={},calls=[];let release;
 const el=id=>els[id]??={value:'',innerHTML:'Test connection',classList:{remove(){},add(){}},addEventListener(n,f){this[n]=f;}};
 const cfg={url:'http://nas',token_set:true,categories:{},category_labels:{stars:'Stars'}};
 const ctx={AbortController,AbortSignal,setTimeout,clearTimeout,window:{addEventListener(){}},document:{getElementById:el,querySelectorAll:()=>[{dataset:{magpieCategory:'stars'},value:'photos'}],addEventListener(){}},fetch:async(url,o)=>{calls.push({url,o});if(url.endsWith('/test'))return new Promise(r=>release=()=>r({ok:!fail,json:async()=>fail?{detail:'Invalid token'}:[]}));return {ok:true,json:async()=>url.endsWith('/config')?cfg:[]};}};
 vm.runInNewContext(source,ctx);return {el,calls,ctx,release:()=>release()};
}
test('test shows pending and success, restores button, never saves',async()=>{const s=setup();await flush();const button=s.el('magpieSave');const p=button.click({currentTarget:button});assert.equal(button.disabled,true);assert.equal(s.el('magpieTestStatus').textContent,'Connecting…');s.release();await p;assert.equal(button.disabled,false);assert.equal(s.el('magpieTestStatus').textContent,'Connected');assert.equal(s.calls.filter(x=>x.o.method==='POST').length,1);assert.ok(s.calls.at(-1).url.endsWith('/test'));});
test('test failure is visible beside button',async()=>{const s=setup(true);await flush();const b=s.el('magpieSave'),p=b.click({currentTarget:b});s.release();await p;assert.equal(s.el('magpieTestStatus').textContent,'Invalid token');assert.equal(b.disabled,false);});
test('main save persists token and mappings',async()=>{const s=setup();await flush();s.el('magpieToken').value='magpie_new';await s.ctx.window.saveMagpieSettings();const writes=s.calls.filter(x=>x.o.method==='POST');assert.deepEqual(writes.map(x=>x.url),['/api/magpie/config','/api/magpie/categories']);assert.equal(JSON.parse(writes[1].o.body).categories.stars,'photos');assert.equal(s.el('magpieToken').value,'');});
