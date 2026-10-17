const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const source=fs.readFileSync(require('node:path').join(__dirname,'../static/downloads.html'),'utf8').split('<script id="downloadsMagpieControls">')[1].split('</script>')[0];
const flush=()=>new Promise(r=>setImmediate(r));
function setup(override){
 const elements=new Map(),requests=[],events={};
 const el=id=>{if(id==='magpieSettings')return null;if(!elements.has(id))elements.set(id,{value:'',listeners:{},replaceChildren(){},addEventListener(n,f){this.listeners[n]=f;}});return elements.get(id);};
 const cfg={url:'http://nas',token_set:true,categories:{stars:'photos'},category_labels:{stars:'Stars',jav:'JAV'}};
 const ctx={AbortController,AbortSignal,setTimeout,clearTimeout,window:{addEventListener:(n,f)=>events[n]=f,loadDownloads(){}},document:{getElementById:el,visibilityState:'visible',addEventListener(){}},fetch:async(url,options)=>{requests.push({url,options});if(override){const r=override(url,options);if(r)return r;}return {ok:true,json:async()=>cfg};}};
 vm.createContext(ctx);vm.runInContext(source,ctx);return {ctx,el,requests,events};
}
test('submission selector enables mapped categories only',async()=>{
 const s=setup();await flush();assert.equal(s.el('magpieCategory').value,'stars');assert.equal(s.el('magpieSubmit').disabled,false);
 assert.match(s.el('magpieCategory').innerHTML,/value="jav" disabled/);
});
test('page exit cancels configuration read',async()=>{
 let signal;
 const s=setup((url,options)=>new Promise((resolve,reject)=>{signal=options.signal;signal.addEventListener('abort',()=>reject(new DOMException('aborted','AbortError')));}));
 s.events.pagehide();await flush();assert.equal(signal.aborted,true);
});
test('category submission ignores duplicate clicks while pending',async()=>{
 let release;
 const s=setup((url,options)=>options.method==='POST'?new Promise(r=>release=()=>r({ok:true,json:async()=>({})})):null);
 await flush();s.el('magpieUrl').value='https://example.com/gallery';
 const event={preventDefault(){}};s.el('magpieForm').listeners.submit(event);s.el('magpieForm').listeners.submit(event);
 assert.equal(s.requests.filter(r=>r.options.method==='POST').length,1);
 assert.deepEqual(JSON.parse(s.requests.at(-1).options.body),{url:'https://example.com/gallery',category:'stars'});
 release();await flush();assert.equal(s.el('magpieUrl').value,'');
});
const html=fs.readFileSync(require('node:path').join(__dirname,'../static/downloads.html'),'utf8');
const extract=name=>html.match(new RegExp('  (?:async )?function '+name+'\\([^]*?\\n  }'))[0];
test('combined counters exclude failures/cancellations from completed and include all sources',()=>{
 const els={};const ctx={document:{getElementById:id=>els[id]??=( {textContent:''})}};vm.createContext(ctx);vm.runInContext(extract('_paintDlStats'),ctx);
 ctx._paintDlStats([{source:'nzb',queue:'history',status:'success',progress_pct:100},{source:'torrent',queue:'active',status:'seeding',progress_pct:100},{source:'magpie',queue:'active',status:'downloading',progress_pct:100,completed:false},{source:'magpie',queue:'history',status:'failed',failed:true},{source:'magpie',queue:'history',status:'cancelled'}],{bandwidth_down_bps:2048});
 for(const [key,value]of Object.entries({Total:5,Active:1,Complete:2,Nzb:1,Torrent:1,Magpie:3,Bandwidth:2}))assert.equal(els['dlStat'+key].textContent,String(value));
});
test('empty combined download page renders without missing helpers',async()=>{
 const els={};const el=id=>els[id]??={value:'',innerHTML:'',insertAdjacentHTML(){}};
 const ctx={document:{getElementById:el},URLSearchParams,_downloadsFetchGen:0,getDlSearchMode:()=> 'downloads',_refreshLibraryStudios:async()=>{},fetch:async()=>({ok:true,json:async()=>({items:[]})}),_paintDlStats(){},esc:String};
 vm.createContext(ctx);vm.runInContext(extract('magpieProgressRings')+'\n'+extract('_dlEmptyHtml')+'\n'+extract('loadDownloads'),ctx);await ctx.loadDownloads();
 assert.match(el('rows').innerHTML,/>nada</);
});
test('Magpie renders inside the original grid with its own logo and safe controls',async()=>{
 const els={};const el=id=>els[id]??={value:'',innerHTML:'',insertAdjacentHTML(){},querySelectorAll(){return[];}};
 const items=[{source:'magpie',id:'a'.repeat(32),name:'https://example.com/gallery',status:'downloading',queue:'active',magpie_action:'cancel',magpie_files:2,destination_name:'Photos'},
 {source:'magpie',id:'b'.repeat(32),name:'Finished',status:'completed',queue:'history',completed:true},
 {source:'nzb',id:'nzb1',name:'Original NZB',queue:'history',status:'success',progress_pct:100}];
 const ctx={document:{getElementById:el},URLSearchParams,_downloadsFetchGen:0,getDlSearchMode:()=> 'downloads',_refreshLibraryStudios:async()=>{},fetch:async()=>({ok:true,json:async()=>({items})}),_paintDlStats(){},_hydrateDownloadHeadshots(){},spaceifyReleaseName:s=>s,esc:String};
 vm.createContext(ctx);vm.runInContext(extract('magpieProgressRings')+'\n'+extract('_dlEmptyHtml')+'\n'+extract('loadDownloads'),ctx);await ctx.loadDownloads();
 const rendered=el('rows').innerHTML;assert.doesNotMatch(rendered,/>Error:/);assert.match(rendered,/magpie_logo\.webp/);assert.match(rendered,/nzb\.webp/);
 assert.match(rendered,/data-magpie-action="cancel"/);assert.match(rendered,/data-magpie-action="remove"/);assert.match(rendered,/Remove from history/);assert.doesNotMatch(rendered,new RegExp("chooseDownloadRemoval\\('"+'a'.repeat(32)));
});

test('completed Magpie rings show overall completion despite cleared totals',()=>{
 const ctx={};vm.createContext(ctx);vm.runInContext(extract('magpieProgressRings'),ctx);
 const rendered=ctx.magpieProgressRings({status:'completed',file_progress_pct:null,album_progress_pct:null},'');
 assert.equal((rendered.match(/aria-valuenow="100"/g)||[]).length,2);
 assert.match(rendered,/dl-tile-pct-num"[^>]*>100</);
 assert.doesNotMatch(rendered,/inner|outer|Done/);
 assert.doesNotMatch(rendered,/is-unknown/);
});

test('failed Magpie shows a right-aligned warning without a percent sign',()=>{
 const ctx={};vm.createContext(ctx);vm.runInContext(extract('magpieProgressRings'),ctx);
 const rendered=ctx.magpieProgressRings({status:'failed',failed:true,album_progress_pct:30},'');
 assert.match(rendered,/dl-tile-pct-num"[^>]*>  !</);
 assert.doesNotMatch(rendered,/dl-tile-pct-sym|is-running/);
});

 test('Magpie uses file percentage when album total is unknown',()=>{
 const ctx={};vm.createContext(ctx);vm.runInContext(extract('magpieProgressRings'),ctx);
 const rendered=ctx.magpieProgressRings({status:'downloading',file_progress_pct:42,album_progress_pct:null},'');
 assert.match(rendered,/title="File progress"> 42</);
 assert.match(rendered,/dl-tile-pct-sym/);
 const unknown=ctx.magpieProgressRings({status:'downloading'},'');
 assert.match(unknown,/dl-magpie-unknown"[^>]*>Unknown</);
 assert.doesNotMatch(unknown,/dl-tile-pct-sym/);
 });
