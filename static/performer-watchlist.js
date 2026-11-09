(() => {
  const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  let items = [], timer, mounted = false;
  const host = () => document.getElementById('performerWatchlist');
  async function api(path='', method='GET', body) {
    const response = await fetch('/api/performer-watchlist'+path, {method,headers:{'Content-Type':'application/json'},body:body ? JSON.stringify(body):undefined});
    const result = await response.json();
    if (!response.ok) throw Error(result.detail || 'Watchlist request failed');
    return result;
  }
  function sourceUrl(hit) {
    const bases={TPDB:'https://theporndb.net/performers/',StashDB:'https://stashdb.org/performers/',FansDB:'https://fansdb.cc/performers/',JAVStash:'https://javstash.org/performers/'};
    return (bases[hit.source] || '') + encodeURIComponent(hit.id);
  }
  function profile(hit) {
    const fields = {TPDB:'tpdbId',StashDB:'stashId',FansDB:'fansdbId',JAVStash:'javstashId'};
    window.tsGotoEntity({kind:'performer',name:hit.name,[fields[hit.source]]:hit.id});
  }
  function status(row) {
    if(row.confirmed) return 'Identified';
    if(row.checking || !row.checked_at) return 'Checking…';
    if(row.candidates.length) return 'Needs review';
    if(Object.values(row.checks).some(s => s !== 'Checked')) return 'Check incomplete';
    return 'Not found';
  }
  const sourceLogos = {TPDB:'tpdb', StashDB:'stashdb', FansDB:'fansdb', JAVStash:'javstash'};
  function checkSummary(row) {
    const label = status(row);
    const state = row.confirmed ? 'identified' : row.checking || !row.checked_at ? 'checking' : row.candidates.length ? 'review' : 'quiet';
    const icons = {identified:'fa-check', checking:'fa-circle-notch fa-spin', review:'fa-user-check', quiet:'fa-magnifying-glass'};
    const databases = Object.entries(sourceLogos).map(([source, logo]) => {
      const result = row.checks[source];
      const pending = !!row.checking || !row.checked_at;
      const tone = pending ? 'pending' : result === 'Checked' ? 'ok' : result === 'Not configured' ? 'off' : 'error';
      const icon = {pending:'fa-clock',ok:'fa-check',off:'fa-minus',error:'fa-triangle-exclamation'}[tone];
      const count = row.candidates.filter(hit => hit.source === source).length;
      const resultLabel = result === 'Checked' ? '✓' : (result || 'Not checked');
      const detail = pending ? 'Check pending; previous result: ' + resultLabel : resultLabel;
      const tooltip = `${source}: ${detail}${count ? ` · ${count} candidate${count === 1 ? '' : 's'}` : ''}`;
      return `<span class="watch-db watch-db--${tone}" title="${esc(tooltip)}" aria-label="${esc(tooltip)}" tabindex="0"><img src="/static/logos/${logo}.webp" alt="${esc(source)}"><i class="fa-solid ${icon}" aria-hidden="true"></i>${count ? `<span class="watch-db-count">${count}</span>` : ''}</span>`;
    }).join('');
    return `<div class="watch-checks"><div class="watch-check-heading"><span class="watch-state watch-state--${state}"><i class="fa-solid ${icons[state]}" aria-hidden="true"></i>${label}</span><span class="watch-checked-at">${row.checked_at ? '<i class="fa-regular fa-clock" aria-hidden="true"></i> ' + esc(new Date(row.checked_at*1000).toLocaleString(undefined,{day:'numeric',month:'short',hour:'2-digit',minute:'2-digit'})) : 'Awaiting first check'}</span></div><div class="watch-databases">${databases}</div></div>`;
  }
  async function load() {
    if(!host() || host().hidden) return;
    try {
      items = (await api()).items;
      host().querySelector('.watch-status').textContent = `${items.length} watched · Unidentified entries checked daily`;
      host().querySelector('.watch-list').innerHTML = items.length ? items.map(row => `<article class="watch-row${row.confirmed ? ' watch-row--identified' : row.candidates.length ? ' watch-row--review' : ''}">
        <div class="watch-identity"><span class="watch-avatar" aria-hidden="true"><i class="fa-solid ${row.confirmed ? 'fa-user-check' : 'fa-user'}"></i></span><div class="watch-name"><strong>${esc(row.name)}</strong>${row.aliases?.length ? `<small>${esc(row.aliases.join(', '))}</small>`:''}${row.notes ? `<small>${esc(row.notes)}</small>`:''}${row.source_url ? `<a href="${esc(row.source_url)}" target="_blank" rel="noopener noreferrer">Source ↗</a>`:''}</div></div>
        ${checkSummary(row)}
        <div class="watch-actions">${row.confirmed ? `<button data-action="profile" data-id="${row.id}">Open profile / Add</button>` : `<button data-action="review" data-id="${row.id}" ${row.candidates.length?'':'disabled'}>Review ${row.candidates.length||''}</button><button data-action="check" data-id="${row.id}" title="Check databases" aria-label="Check databases" ${row.checking?'disabled':''}><i class="fa-solid fa-rotate"></i></button>`}<button data-action="edit" data-id="${row.id}" title="Edit entry" aria-label="Edit entry"><i class="fa-solid fa-pen"></i></button><button data-action="delete" data-id="${row.id}" title="Remove from watchlist" aria-label="Remove from watchlist"><i class="fa-solid fa-xmark"></i></button></div>
        </article>`).join('') : '<div class="watch-empty"><i class="fa-solid fa-eye" aria-hidden="true"></i><strong>Keep an eye out</strong><p>Add a name above. We’ll check the databases for you.</p></div>';
    } catch(e) {host().querySelector('.watch-status').textContent=e.message;}
  }
  function edit(row) {
    const form=host().querySelector('form'); form.dataset.id=row?.id||'';
    for(const key of ['name','notes','source_url']) form.elements[key].value=row?.[key]||'';
    form.elements.aliases.value=(row?.aliases||[]).join(', ');
    const submit = form.querySelector('[type=submit]');
    submit.innerHTML = `<i class="fa-solid ${row ? 'fa-floppy-disk' : 'fa-circle-plus'}" aria-hidden="true"></i>`;
    submit.title = row ? 'Save changes' : 'Add to watchlist';
    submit.setAttribute('aria-label', submit.title);
    form.elements.name.focus();
  }
  function review(row) {
    const dialog=document.createElement('dialog');dialog.className='watch-review';
    dialog.innerHTML=`<header><h2>Matches for ${esc(row.name)}</h2><button data-close aria-label="Close">×</button></header><p>Review the profile before confirming. Shared aliases can belong to different people.</p><div class="watch-candidates">${row.candidates.map((hit,i)=>`<article><img src="${esc(hit.image || '/static/img/logo.webp')}" alt="" loading="lazy"><div><strong>${esc(hit.name)}</strong><small><a href="${esc(sourceUrl(hit))}" target="_blank" rel="noopener noreferrer">${esc(hit.source)} ↗</a></small><small>${esc(Array.isArray(hit.aliases)?hit.aliases.join(', '):hit.aliases)}</small><button data-open="${i}">View profile</button> <button data-confirm="${i}">This is the performer</button></div></article>`).join('')}</div><p role="status"></p>`;
    document.body.appendChild(dialog);dialog.showModal();
    dialog.querySelector('[data-close]').onclick=()=>dialog.close();
    dialog.addEventListener('close',()=>dialog.remove(),{once:true});
    dialog.addEventListener('click',async event=>{
      const open=event.target.closest('[data-open]');if(open){profile(row.candidates[+open.dataset.open]);return;}
      const confirm=event.target.closest('[data-confirm]');if(!confirm)return;
      confirm.disabled=true;
      try {await api(`/${row.id}/confirm`,'POST',row.candidates[+confirm.dataset.confirm]);dialog.close();await load();}
      catch(e){dialog.querySelector('[role=status]').textContent=e.message;confirm.disabled=false;}
    });
  }
  function mount() {
    if(!mounted){
      mounted=true;
      host().innerHTML=`<form class="watch-form"><label class="watch-field"><span>Name / alias</span><input name="name" placeholder="Name or alias" aria-label="Name or alias" maxlength="150" required></label><label class="watch-field"><span>Other aliases</span><input name="aliases" placeholder="Other aliases, comma separated" aria-label="Additional aliases"></label><label class="watch-field"><span>Source link</span><input name="source_url" type="url" placeholder="Source URL (optional)" aria-label="Source URL" maxlength="2000"></label><label class="watch-field"><span>Notes</span><input name="notes" placeholder="Notes (optional)" aria-label="Notes" maxlength="2000"></label><button type="submit" class="watch-form-icon" title="Add to watchlist" aria-label="Add to watchlist"><i class="fa-solid fa-circle-plus" aria-hidden="true"></i></button><button type="button" class="watch-form-icon" data-cancel title="Clear form" aria-label="Clear form"><i class="fa-solid fa-eraser" aria-hidden="true"></i></button></form><div class="watch-tools"><span class="watch-status" role="status"></span><button data-all><i class="fa-solid fa-rotate"></i> Check all</button></div><div class="watch-list"></div>`;
      const form=host().querySelector('form');
      form.onsubmit=async event=>{event.preventDefault();const button=form.querySelector('[type=submit]');button.disabled=true;try{const data=Object.fromEntries(new FormData(form));data.aliases=data.aliases.split(',').map(s=>s.trim()).filter(Boolean);await api(form.dataset.id?'/'+form.dataset.id:'',form.dataset.id?'PUT':'POST',data);edit(null);await load();}catch(e){host().querySelector('.watch-status').textContent=e.message;}finally{button.disabled=false;}};
      host().querySelector('[data-cancel]').onclick=()=>edit(null);
      host().querySelector('[data-all]').onclick=async()=>{try{await api('/check','POST');await load();}catch(e){host().querySelector('.watch-status').textContent=e.message;}};
      host().querySelector('.watch-list').onclick=async event=>{
        const button=event.target.closest('[data-action]');if(!button)return;
        const row=items.find(r=>r.id===+button.dataset.id);if(!row)return;
        const action=button.dataset.action;
        if(action==='edit')return edit(row);
        if(action==='profile')return profile(row.confirmed);
        if(action==='review')return review(row);
        if(action==='delete'&&!confirm(`Remove ${row.name} from the watchlist? This does not delete library files.`))return;
        button.disabled=true;
        try{await api(`/${row.id}${action==='check'?'/check':''}`,action==='check'?'POST':'DELETE');await load();}catch(e){host().querySelector('.watch-status').textContent=e.message;button.disabled=false;}
      };
    }
    clearInterval(timer);load();timer=setInterval(load,5000);
  }
  window.PerformerWatchlist={mount};
})();
