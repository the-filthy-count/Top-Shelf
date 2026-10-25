(() => {
  let data, controller, selectedGroup, selectedMethod='median';
  let selectedFolders=[],selectedStudios=[];
  try{const saved=JSON.parse(localStorage.getItem('ts_insights_filters')||'{}');selectedFolders=Array.isArray(saved.folders)?saved.folders:[];selectedStudios=Array.isArray(saved.studios)?saved.studios:[];}catch(_){}
  const $ = id => document.getElementById(id);
  const format = value => value == null ? 'Not recorded' : Number(value).toLocaleString(undefined,{maximumFractionDigits:3});
  function node(tag,text){const el=document.createElement(tag);el.textContent=text;return el;}
  function rankings(id,rows){const root=$(id);root.replaceChildren();if(!rows.length){root.append(node('p','No matching metadata recorded yet.'));return;}const max=rows[0].count;rows.forEach(row=>{const li=node('li','');li.style.setProperty('--bar',`${row.count/max*100}%`);li.append(node('span',row.name),node('strong',format(row.count)));root.append(li);});}
  function profile(){if(!data)return;const p=data.profiles?.[selectedGroup] || {population:0,numeric:{},categorical:{}},method=selectedMethod;window.renderInsightsBody?.(selectedGroup,p,method);const left=$('traits-left'),right=$('traits-right');left.replaceChildren();right.replaceChildren();$('traits-bio').replaceChildren();$('traits-ratios').replaceChildren();function trait(root,label,value,samples,note,field,kind){const box=node('div','');box.dataset.field=field;if(['age','career_start_year','gender','country','ethnicity'].includes(field))root=$('traits-bio');else if(field.includes('ratio')||field==='weight')root=$('traits-ratios');const heading=node('dt',label+' ');const help=node('button','');help.type='button';help.className='insights-help';help.style.cssText='border:0!important;background:transparent!important;box-shadow:none!important;border-radius:0!important';help.setAttribute('aria-label',label+' sample information');help.dataset.tip=`${format(samples)} recorded${note ? ' · '+note : ''}`;const icon=node('i','');icon.className='fa-solid fa-circle-info';icon.setAttribute('aria-hidden','true');help.append(icon);heading.append(help);const detail=node('dd','');const action=node('a',value);action.href='#';action.setAttribute('role','button');action.className='trait-analysis';action.setAttribute('aria-label','Analyse '+label);action.addEventListener('click',event=>{event.preventDefault();closeTip();window.openInsightsAnalysis?.({profile:p,group:selectedGroup,field,kind,label,method});});detail.append(action);box.append(heading,detail);root.append(box);}
    [['age','Age',' years'],['height','Height',' cm'],['weight','Weight',' kg'],['waist','Waist',' cm'],['hips','Hips',' cm'],['band','Band size',' in'],['career_start_year','Career start',''],['waist_height_ratio','Waist / height',''],['waist_hip_ratio','Waist / hips','']].forEach(([field,label,unit])=>{const v=p.numeric[field] || {samples:0};trait(left,label,v[method]==null?(method==='mode'&&v.samples?(v.modes?.length?v.modes.map(format).join(' / ')+unit:'No unique mode'):'Not recorded'):(field==='career_start_year'?String(v[method]):format(v[method]))+unit,v.samples,method,field,'numeric');});
    Object.entries(p.categorical).forEach(([field,v])=>{trait(right,field==='ethnicity'?'Ethnicity (mode)':field.replaceAll('_',' '),v.values.join(' / ')||'Not recorded',v.samples,v.samples?`${Math.round(v.count/v.samples*100)}% each`:'',field,'categorical');});
    requestAnimationFrame(drawBodyCallouts);
    $('profile-note').dataset.tip=`Based on ${format(p.population)} saved performers in this group. ${format(data.unknown_gender || 0)} library performers have no recognised gender recorded. Missing or invalid traits are excluded; recorded adult birth dates supply age. This is a composite, not a real person. Numbers use the selected mode or median. Tied numeric modes are listed together; the body uses template defaults where no single mode exists; categories use the most common value (mode). Ties are shown together. Each figure selects only that recorded gender group.`;
  }
  function drawBodyCallouts(){
    const layout=document.querySelector('.composite-layout'),svg=layout.querySelector('.body-callouts');
    svg.replaceChildren();if(matchMedia('(max-width:800px)').matches){layout.querySelectorAll('[data-field]').forEach(el=>el.style.top='');return;}
    const body=$('body-view').hidden?$('composite-figure'):$('body-view');
    const base=layout.getBoundingClientRect(),r=body.getBoundingClientRect();if(!r.width||!r.height)return;
    svg.setAttribute('viewBox',`0 0 ${base.width} ${base.height}`);
    const ns='http://www.w3.org/2000/svg';
    const defs=document.createElementNS(ns,'defs'),marker=document.createElementNS(ns,'marker');
    marker.id='body-arrow';marker.setAttribute('viewBox','0 0 8 8');marker.setAttribute('refX','7');marker.setAttribute('refY','4');marker.setAttribute('markerWidth','6');marker.setAttribute('markerHeight','6');marker.setAttribute('orient','auto');
    const arrow=document.createElementNS(ns,'path');arrow.setAttribute('d','M1 1L7 4L1 7');marker.append(arrow);defs.append(marker);svg.append(defs);

    const points={height:[.38,.06],waist:[.43,.43],hips:[.42,.53],band:[.43,.31],hair_color:[.53,.06],eye_color:[.53,.12],cup_size:[.57,.31]};
    const occupied=new Map();
    const boxes=[...layout.querySelectorAll('[data-field]')].sort((a,b)=>(points[a.dataset.field]?.[1]||0)-(points[b.dataset.field]?.[1]||0));
    for(const box of boxes){
      const point=points[box.dataset.field];if(!point)continue;
      const parent=box.parentElement,parentRect=parent.getBoundingClientRect();
      const scale=base.height/layout.offsetHeight||1;
      const desired=(r.top+r.height*point[1]-parentRect.top)/scale-box.offsetHeight/2;
      const top=Math.max(desired,occupied.get(parent)||0);
      box.style.top=top+'px';occupied.set(parent,top+box.offsetHeight+22);
      const rect=box.getBoundingClientRect(),left=box.closest('#traits-left');
      const x=(left?rect.right:rect.left)-base.left,y=rect.top+rect.height/2-base.top;
      const tx=r.left-base.left+r.width*point[0],ty=r.top-base.top+r.height*point[1];
      const path=document.createElementNS(ns,'path');path.setAttribute('marker-end','url(#body-arrow)');path.setAttribute('d',`M${x} ${y} H${x+(left?16:-16)} L${tx} ${ty}`);svg.append(path);
      const dot=document.createElementNS(ns,'circle');dot.setAttribute('cx',tx);dot.setAttribute('cy',ty);dot.setAttribute('r',3);svg.append(dot);
    }
  }
  const bodyLayout=document.querySelector('.composite-layout');
  new ResizeObserver(()=>requestAnimationFrame(drawBodyCallouts)).observe(bodyLayout);
  new MutationObserver(()=>requestAnimationFrame(drawBodyCallouts)).observe($('body-view'),{attributes:true,attributeFilter:['hidden']});
  function groups(){
    const available=data.profiles || {};const keys=Object.keys(available);
    if(!available[selectedGroup])selectedGroup=keys[0];
    const host=document.querySelector('.figure-wrap .figure-options');host.replaceChildren();
    const styles={male:['person','Male'],female:['person-dress','Female'],mix:['person-half-dress','Transgender / mixed']};
    function update(){const icon=styles[selectedGroup];$('composite-figure').hidden=!icon;$('composite-figure').className='fa-solid fa-'+(icon?.[0]||'person');document.querySelector('.figure-wrap>span').textContent=icon?icon[1]+' composite':'No recorded gender groups';host.querySelectorAll('button').forEach(b=>b.setAttribute('aria-pressed',String(b.dataset.group===selectedGroup)));profile();}
    keys.forEach(key=>{const style=styles[key];if(!style)return;const b=node('button','');b.type='button';b.dataset.group=key;b.title=style[1];b.setAttribute('aria-label',style[1]+' library performers');const icon=node('i','');icon.className='fa-solid fa-'+style[0];icon.setAttribute('aria-hidden','true');b.append(icon);b.addEventListener('click',()=>{selectedGroup=key;update();});host.append(b);});update();
  }
  function filterOptions(){
    for(const [id,values,selected] of [['folder',data.filters?.folders||[],selectedFolders],['studio',data.filters?.studios||[],selectedStudios]]){
      const root=$(id+'-options');root.replaceChildren();
      [...new Set([...values,...selected])].forEach(value=>{const label=node('label','');const input=document.createElement('input');input.type='checkbox';input.setAttribute('aria-label',value);input.value=value;input.checked=selected.includes(value);input.addEventListener('change',()=>{const list=Array.from(root.querySelectorAll('input:checked'),el=>el.value);if(id==='folder')selectedFolders=list;else selectedStudios=list;try{localStorage.setItem('ts_insights_filters',JSON.stringify({folders:selectedFolders,studios:selectedStudios}));}catch(_){}load(true);});label.append(input,node('span',value));root.append(label);});
      if(!values.length&&!selected.length)root.append(node('p','No options recorded'));
      $(id+'-count').textContent=selected.length?String(selected.length):'All';
    }
  }
  function releaseChart(){
    const grouped=new Map();for(const row of data.release_months||[]){const label=$('release-group').value==='year'?row.month.slice(0,4):row.month;grouped.set(label,(grouped.get(label)||0)+row.count);}
    const root=$('release-months');root.replaceChildren();const max=Math.max(1,...grouped.values());
    for(const [label,count] of grouped){const box=node('div','');box.className='month';const bar=node('div','');bar.className='bar';bar.style.setProperty('--height',`${count/max*130}px`);box.append(node('strong',format(count)),bar,node('span',label));root.append(box);}
    requestAnimationFrame(()=>{root.scrollLeft=root.scrollWidth;});
    if(!grouped.size)root.append(node('p','No release dates recorded for this selection.'));
    $('release-coverage').dataset.tip=`${format(data.coverage.release_dates||0)} of ${format(data.totals.scenes)} library scenes have a release date. Uses saved scene metadata or the date encoded in canonical filenames; unknown dates are omitted. This is separate from download/import dates.`;
  }
  function render(){filterOptions();releaseChart();const t=data.totals;Object.keys(t).forEach(key=>{const el=$('total-'+key);if(el){el.textContent=padSkyCounter(t[key]);el.style.setProperty('--counter-length',el.textContent.length);}});groups();['performers','studios','tags'].forEach(id=>rankings(id,data[id]));$('tag-coverage').dataset.tip=`Tags available for ${format(data.coverage.tags)} of ${format(t.scenes)} scenes. Each tag counts once per scene.`;profile();$('months').replaceChildren();const max=Math.max(1,...data.months.map(x=>x.count));data.months.forEach(row=>{const box=node('div','');box.className='month';const bar=node('div','');bar.className='bar';bar.style.setProperty('--height',`${row.count/max*130}px`);box.append(node('strong',format(row.count)),bar,node('span',row.month));$('months').append(box);});if(!data.months.length)$('months').append(node('p','No scene import dates recorded yet.'));$('stats-content').hidden=false;}
  const cacheKey='ts_insights_snapshot_v3';
  const selectionKey=()=>JSON.stringify([selectedFolders.slice().sort(),selectedStudios.slice().sort()]);
  function validSnapshot(value){return value && value.totals && value.coverage && value.profiles && ['performers','studios','tags','months'].every(key=>Array.isArray(value[key]));}
  function restoreSnapshot(){
    try{const saved=JSON.parse(localStorage.getItem(cacheKey));if(saved?.selection!==selectionKey()||!validSnapshot(saved.data))return;data=saved.data;render();}
    catch(_){data=null;try{localStorage.removeItem(cacheKey);}catch(_){} }
  }
  async function load(refresh=false){
    controller?.abort();const request=new AbortController();controller=request;
    $('refresh').disabled=true;
    $('status').textContent=data?'Showing saved statistics from '+new Date(data.generated_at).toLocaleString()+' · Refreshing…':'Loading library statistics…';
    try{
      const params=new URLSearchParams();if(refresh)params.set('refresh','true');selectedFolders.forEach(value=>params.append('folder',value));selectedStudios.forEach(value=>params.append('studio',value));
      const response=await fetch('/api/library/insights?'+params,{signal:request.signal,cache:'no-store'});
      if(response.status===401){data=null;$('stats-content').hidden=true;try{localStorage.removeItem(cacheKey);}catch(_){}throw Error('Please sign in to view statistics.');}
      if(!response.ok)throw Error('Could not refresh statistics. Try Refresh.');
      const next=await response.json();if(!validSnapshot(next))throw Error('Invalid statistics response. Try Refresh.');
      if(request!==controller)return;
      data=next;render();
      try{localStorage.setItem(cacheKey,JSON.stringify({selection:selectionKey(),data}));}catch(_){} // Storage being unavailable must not block the page.
      $('status').textContent='Updated '+new Date(data.generated_at).toLocaleString();
    }catch(error){if(error.name!=='AbortError'&&request===controller)$('status').textContent=(data?'Showing saved statistics from '+new Date(data.generated_at).toLocaleString()+' · ':'')+error.message;}
    finally{if(request===controller)$('refresh').disabled=false;}
  }
  const tip=node('div','');tip.id='insights-tooltip';tip.className='insights-tooltip';tip.setAttribute('role','tooltip');tip.setAttribute('popover','auto');document.body.append(tip);let tipOwner;
  function closeTip(){if(tip.matches(':popover-open'))tip.hidePopover();tipOwner?.removeAttribute('aria-describedby');tipOwner=null;}
  function showTip(button){if(!button.dataset.tip)return;closeTip();tipOwner=button;tip.textContent=button.dataset.tip;button.setAttribute('aria-describedby',tip.id);tip.showPopover();const r=button.getBoundingClientRect(),box=tip.getBoundingClientRect();const scale=Number.parseFloat(getComputedStyle(document.documentElement).zoom)||1;tip.style.left=Math.max(8,Math.min(r.left,innerWidth-box.width-8))/scale+'px';tip.style.top=Math.max(8,(r.bottom+box.height+12>innerHeight?r.top-box.height-8:r.bottom+8))/scale+'px';}
  document.addEventListener('pointerover',e=>{const b=e.target.closest('.insights-help');if(b&&b!==tipOwner&&e.pointerType!=='touch')showTip(b);});
  document.addEventListener('pointerout',e=>{if(e.target.closest('.insights-help')&&!e.relatedTarget?.closest('.insights-help')&&e.relatedTarget!==tip&&!tip.contains(e.relatedTarget))closeTip();});
  tip.addEventListener('pointerleave',closeTip);
  document.addEventListener('focusin',e=>{if(e.target.matches('.insights-help'))showTip(e.target);});
  document.addEventListener('focusout',e=>{if(e.target.matches('.insights-help'))closeTip();});
  document.addEventListener('click',e=>{const b=e.target.closest('.insights-help');if(b)showTip(b);});
  document.addEventListener('keydown',e=>{if(e.key==='Escape')closeTip();});
  window.addEventListener('resize',closeTip);window.addEventListener('scroll',closeTip,true);
  $('release-group').addEventListener('change',()=>{if(data)releaseChart();});
  $('clear-filters').addEventListener('click',()=>{selectedFolders=[];selectedStudios=[];try{localStorage.removeItem('ts_insights_filters');}catch(_){}load(true);});
  $('method').addEventListener('click',e=>{const button=e.target.closest('[data-method]');if(!button)return;selectedMethod=button.dataset.method;$('method').querySelectorAll('button').forEach(b=>b.setAttribute('aria-pressed',String(b===button)));profile();});$('refresh').addEventListener('click',()=>load(true));$('logout').addEventListener('click',()=>{try{localStorage.removeItem(cacheKey);}catch(_){}fetch('/api/auth/logout',{method:'POST'}).then(()=>location.href='/login');});window.addEventListener('pagehide',()=>{controller?.abort();window.closeInsightsAnalysis?.();});restoreSnapshot();load(true);
})();
