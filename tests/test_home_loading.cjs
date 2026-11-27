const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const html = fs.readFileSync('static/home.html', 'utf8');
const code = html.slice(html.indexOf('  const _homeSections ='), html.indexOf('\n  function _startBgPoll()'));
const nodes = new Map();
function node() { return {textContent:'Loading', innerHTML:'', appendChild(child){this.child=child;}}; }
const requests = [];
const painted = [];
const timers = [];
const ctx = {AbortController, Map, JSON, Promise, console:{error(){}},
 document:{getElementById(id){if(!nodes.has(id))nodes.set(id,node());return nodes.get(id);},createElement:node},
 window:{addEventListener(){}},setTimeout(fn){timers.push(fn);return timers.length;},clearTimeout(){},
 fetch(url){return new Promise((resolve,reject)=>requests.push({url,resolve,reject}));},
 _visibleCount:()=>10,SCENES_MIN_COL:220,SCENES_ROWS:2,LIB_MIN_COL:140,LIB_ROWS:2,
 _mergeScenesByKind:(b,s)=>b||{all:s},_startBgPoll(){},_stopBgPoll(){},_lastUpdatedAt:0,
};
for(const [section,variable,renderer,arr] of [
 ['scenes','_allScenes','renderScenes','_sceneArr'],['performers','_allPerformers','renderPerformers','_perfArr'],
 ['discovered','_allDiscovered','renderDiscovered','_discoveredArr'],['movies','_allMovies','renderMovies','_movieArr'],['jav','_allJav','renderJav','_javArr']]){
 ctx[variable]=[];ctx[renderer]=items=>painted.push(section);ctx[arr]=()=>ctx[variable];
}
vm.createContext(ctx);vm.runInContext(code,ctx);
const tick=()=>new Promise(resolve=>setImmediate(resolve));
(async()=>{
 const loading=ctx.loadHomeData();
 assert.equal(requests.length,5,'all sections start independently');
 requests.find(r=>r.url.includes('performers')).resolve({ok:true,json:async()=>({performers:[{id:1}]})});
 await tick();assert.deepEqual(painted,['performers'],'stars paint before slow movie response');
 requests.find(r=>r.url.includes('movies')).resolve({ok:false,status:500});
 await tick();assert.match(nodes.get('homeMoviesMeta').textContent,/500/);assert(nodes.get('homeMoviesMeta').child);
 requests.find(r=>r.url.includes('scenes')).resolve({ok:true,json:async()=>({scenes:[]})});
 requests.find(r=>r.url.includes('discovered')).resolve({ok:true,json:async()=>({discovered:[]})});
 requests.find(r=>r.url.includes('jav')).resolve({ok:true,json:async()=>({jav:[]})});
 await loading;
 const retry=nodes.get('homeMoviesMeta').child.onclick();
 assert.equal(requests.length,6,'retry fetches only the failed section');
 requests[5].resolve({ok:true,json:async()=>({movies:[{id:2}]})});await retry;
 assert.equal(nodes.get('homeMoviesMeta').textContent,'1 movie');
 const failure=ctx._loadHomeSection('movies');requests[6].reject(new Error('Network unavailable'));await failure;
 assert.equal(painted.filter(s=>s==='movies').length,1,'failed refresh preserves cards');
 console.log('PASS: independent section loading, failure feedback, retry and preserved cards');
})().catch(e=>{console.error(e);process.exitCode=1;});
