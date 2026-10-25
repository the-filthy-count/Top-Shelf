const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');const vm=require('node:vm');const path=require('node:path');
const read=n=>fs.readFileSync(path.join(__dirname,'../static',n),'utf8');
const extract=(s,n)=>s.match(new RegExp('  (?:async )?function '+n+'\\([^]*?\\n  }'))[0];
function requests(fetcher){
 const signals=[];let observer;
 const el={style:{},classList:{contains:()=>true},addEventListener(){},removeEventListener(){},querySelectorAll:()=>[]};
 const window={addEventListener(){}};
 const ctx={window,document:{getElementById:()=>el},AbortController,AbortSignal,DOMException,URL,
  location:{href:'http://fixture.test/'},MutationObserver:class{constructor(fn){observer=fn;}observe(){}disconnect(){}},
  fetch:async(url,opts)=>{signals.push(opts.signal);return fetcher(url,opts);}};
 vm.runInNewContext(read('ts-utils.js').split('// Return keyboard users')[0],ctx);
 return {window,signals,el,close(){el.classList.contains=()=>false;observer();}};
}
test('completed popup reads release their cancellation controllers',async()=>{
 const ctx=requests(async()=>({json:async()=>({id:1})}));
 for(let i=0;i<20;i++) await (await ctx.window.tsModalFetch('modal','/read/'+i)).json();
 ctx.window.tsCancelModalLoads('modal');assert(ctx.signals.every(s=>!s.aborted));
});
test('closing a popup still cancels an unfinished response body',async()=>{
 let finish;const ctx=requests(async()=>({json:()=>new Promise(r=>finish=r)}));
 const response=await ctx.window.tsModalFetch('modal','/read');const body=response.json();
 ctx.close();finish({stale:true});await assert.rejects(body,{name:'AbortError'});
});
test('failed old request cleanup does not detach its replacement',async()=>{
 const ctx=requests(async()=>({json:async()=>({})}));
 const old=await ctx.window.tsModalFetch('modal','/read');
 const fresh=await ctx.window.tsModalFetch('modal','/read');
 await assert.rejects(old.json(),{name:'AbortError'});
 ctx.window.tsCancelModalLoads('modal');assert.equal(ctx.signals[1].aborted,true);
 await assert.rejects(fresh.json(),{name:'AbortError'});
});
test('body parse failures release requests as well',async()=>{
 const ctx=requests(async()=>({json:async()=>{throw new SyntaxError('bad JSON');}}));
 await assert.rejects((await ctx.window.tsModalFetch('modal','/read')).json(),{name:'SyntaxError'});
 ctx.window.tsCancelModalLoads('modal');assert.equal(ctx.signals[0].aborted,false);
});
test('popup focus returns without scrolling and does not steal from another dialog',()=>{
 let returned=0,focused=0;const trigger={isConnected:true,getClientRects:()=>[{}],focus:opts=>{assert.equal(opts.preventScroll,true);returned++;}};
 const inside={};const body={};const document={activeElement:trigger,body};
 const overlay={contains:el=>el===inside,querySelector:()=>({focus:()=>{focused++;document.activeElement=inside;}})};
 const window={};
 vm.runInNewContext(read('ts-utils.js').split('// Return keyboard users')[1].split('// HTML escaping')[0].replace(/^[\s\S]*?\(function/,'(function'),{window,document});
 window.tsPopupOpened(overlay);window.tsPopupOpened(overlay);assert.equal(focused,1);
 window.tsPopupClosed(overlay);assert.equal(returned,1);
 document.activeElement=trigger;window.tsPopupOpened(overlay);document.activeElement={};
 window.tsPopupClosed(overlay);assert.equal(returned,1);
});
test('history restore resets exit guard and keeps an already loaded record mounted',()=>{
 const s=read('entity.html');let opens=0;let open=true;
 const ctx={_navigatingAway:true,_reopenOnRestore:false,path:'/performer/42',
 document:{getElementById:()=>({classList:{contains:()=>open},style:{}})},dispatch:()=>opens++,window:{addEventListener:(n,f)=>ctx.restore=f}};
 vm.createContext(ctx);vm.runInContext(s.match(/window.addEventListener\('pageshow', function \(event\) \{[^]*?\n  \}\);/)[0],ctx);
 ctx.restore({persisted:true});assert.equal(ctx._navigatingAway,false);assert.equal(opens,0);
 open=false;ctx.restore({persisted:true});assert.equal(opens,1);
 open=true;ctx._reopenOnRestore=true;ctx.restore({persisted:true});assert.equal(opens,2);
});
test('entity nested-modal dismissal excludes only its own root record',()=>{
 let exclusions;
 const ctx={path:'/movie/12',window:{tsCloseTopModal:ids=>{exclusions=ids;return true;}}};
 vm.createContext(ctx);vm.runInContext(extract(read('entity.html'),'_closeTopmostModal'),ctx);
 assert.equal(ctx._closeTopmostModal(),true);assert.deepEqual(Array.from(exclusions),['movieDetailOverlay']);
});
test('generic class-based dialog dismissal does not leave an inline display trap',()=>{
 let cancelled;const classes=new Set(['open']);
 const el={id:'customModal',getAttribute:()=>null,style:{},classList:{contains:c=>classes.has(c),remove:(...names)=>names.forEach(c=>classes.delete(c))}};
 const ctx={window:{tsCancelModalLoads:id=>cancelled=id},ID_CLOSE_FN:{}};
 vm.createContext(ctx);vm.runInContext(extract(read('app-shell.js'),'closeTop'),ctx);
 assert.equal(ctx.closeTop(el),true);assert.equal(cancelled,'customModal');assert.equal(classes.size,0);assert.equal(el.style.display,undefined);
});
