(() => {
  const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  function linkNotes(value) {
    return String(value).split(/((?:https?:\/\/|www\.)[^\s<>"']+)/gi).map((part, index) => {
      if (!(index % 2)) return esc(part);
      const url = part.replace(/[.,;!?)]+$/, '');
      const suffix = part.slice(url.length);
      return `<a href="${esc(/^www\./i.test(url) ? 'https://' + url : url)}" target="_blank" rel="noopener noreferrer">${esc(url)}</a>${esc(suffix)}`;
    }).join('');
  }
  let items = [], timer, mounted = false, listFilter = '', databaseFilter = '';
  const reviewCandidates = row => row.candidates.filter(hit => !['Coomer', 'IAFD', 'Babepedia'].includes(hit.source));
  function visibleRow(row) {
    const matches = reviewCandidates(row);
    return (!databaseFilter || matches.some(hit => hit.source === databaseFilter)) &&
      (!listFilter || (listFilter === 'review' && !row.confirmed && matches.length > 0) ||
       (listFilter === 'confirmed' && !!row.confirmed) ||
       (listFilter === 'unmatched' && !row.confirmed && !matches.length));
  }
  const host = () => document.getElementById('performerWatchlist');
  async function api(path='', method='GET', body) {
    const response = await fetch('/api/performer-watchlist'+path, {method,headers:{'Content-Type':'application/json'},body:body ? JSON.stringify(body):undefined});
    const result = await response.json();
    if (!response.ok) throw Error(result.detail || 'Watchlist request failed');
    return result;
  }
  function sourceUrl(hit) {
    if (hit.source === 'Coomer' && /^https:\/\/coomer\.st\//i.test(hit.url || '')) return hit.url.replace(/^https:\/\/coomer\.st/i, 'https://site1.coomer.com.co');
    if (['Coomer','IAFD'].includes(hit.source) && /^https?:\/\//i.test(hit.url || '')) return hit.url;
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
    if(reviewCandidates(row).length) return 'Needs review';
    if(Object.values(row.checks).some(s => s !== 'Checked')) return 'Check incomplete';
    return 'Not found';
  }
  const sourceLogos = {TPDB:'tpdb', StashDB:'stashdb', FansDB:'fansdb', JAVStash:'javstash', Coomer:'coomer', IAFD:'iafd'};
  const libraryBadge = '<img class="watch-library-logo" src="/static/img/logo.webp" alt="In library" title="Already in your Top-Shelf library">';
  function checkSummary(row) {
    const label = status(row);
    const state = row.confirmed ? 'identified' : row.checking || !row.checked_at ? 'checking' : reviewCandidates(row).length ? 'review' : 'quiet';
    const icons = {identified:'fa-check', checking:'fa-circle-notch fa-spin', review:'fa-user-check', quiet:'fa-magnifying-glass'};
    const databases = Object.entries(sourceLogos).map(([source, logo]) => {
      const linked = row.linked_profiles?.[source] || (row.confirmed?.source === source ? row.confirmed : null);
      const result = linked ? 'Checked' : row.checks[source];
      const pending = !linked && !row.confirmed && (!!row.checking || !row.checked_at);
      const tone = row.confirmed ? (result === 'Checked' ? 'ok' : 'off') : pending ? 'pending' : result === 'Checked' ? 'ok' : result === 'Not configured' ? 'off' : 'error';
      const icon = {pending:'fa-clock',ok:'fa-check',off:'fa-minus',error:'fa-triangle-exclamation'}[tone];
      const count = row.candidates.filter(hit => hit.source === source).length;
      const resultLabel = result === 'Checked' ? '✓' : (result || 'Not checked');
      const detail = row.confirmed ? 'Performer identified; further checks stopped' : pending ? 'Check pending; previous result: ' + resultLabel : resultLabel;
      const locked = !!linked || !!row.confirmed;
      const tooltip = `${source}: ${linked ? 'Matched — open linked profile' : row.confirmed ? 'Unmatched — further checks stopped' : detail}${!locked && count ? ` · ${count} candidate${count === 1 ? '' : 's'}` : ''}`;
      return `<${linked ? 'a' : 'span'} ${linked ? `href="${esc(sourceUrl(linked))}" target="_blank" rel="noopener noreferrer"` : ''} class="watch-db watch-db--${tone}" title="${esc(tooltip)}" aria-label="${esc(tooltip)}" tabindex="0"><img src="/static/logos/${logo}.webp" alt="${esc(source)}">${locked ? '<span aria-hidden="true"></span>' : `<i class="fa-solid ${icon}" aria-hidden="true"></i>`}<span class="watch-db-count">${locked ? `<i class="fa-solid fa-lock watch-db-lock${linked ? ' watch-db-lock--matched' : ''}" aria-hidden="true"></i>` : String(Math.min(count, 99)).padStart(2, '0')}</span></${linked ? 'a' : 'span'}>`;
    }).join('');
    return `<div class="watch-checks"><div class="watch-check-heading"><span class="watch-state watch-state--${state}"><i class="fa-solid ${icons[state]}" aria-hidden="true"></i>${label}</span><span class="watch-checked-at">${row.checked_at ? '<i class="fa-regular fa-clock" aria-hidden="true"></i> ' + esc(new Date(row.checked_at*1000).toLocaleString(undefined,{day:'numeric',month:'short',hour:'2-digit',minute:'2-digit'})) : 'Awaiting first check'}</span></div><div class="watch-databases">${databases}</div></div>`;
  }
  async function load() {
    if(!host() || host().hidden) return;
    try {
      items = (await api()).items;
      render();
    } catch(e) {host().querySelector('.watch-status').textContent=e.message;}
  }
  function render() {
      const shown = items.filter(visibleRow);
      host().querySelector('.watch-status').textContent = `${shown.length} / ${items.length} watched`;
      host().querySelectorAll('[data-list-filter]').forEach(button => button.setAttribute('aria-pressed', String(button.dataset.listFilter === listFilter)));
      host().querySelectorAll('[data-db-filter]').forEach(button => button.setAttribute('aria-pressed', String(button.dataset.dbFilter === databaseFilter)));
      host().querySelector('.watch-list').innerHTML = shown.length ? shown.map(row => `<article class="watch-row${row.confirmed ? ' watch-row--identified' : reviewCandidates(row).length ? ' watch-row--review' : ''}">
        <div class="watch-identity"><span class="watch-avatar${row.confirmed ? ' watch-avatar--confirmed' : ''}" aria-hidden="true"><img src="${esc(row.profile_image || '/static/img/profile.webp')}" alt="" loading="lazy" onerror="this.onerror=null;this.src='/static/img/profile.webp'"></span><div class="watch-name"><div class="watch-name-heading"><strong>${row.source_url ? `<a class="watch-name-link" href="${esc(row.source_url)}" target="_blank" rel="noopener noreferrer" title="Open source link">${esc(row.name)}</a>` : esc(row.name)}</strong>${row.in_library ? libraryBadge : ''}</div>${row.aliases?.length ? `<small>${esc(row.aliases.join(', '))}</small>`:''}${row.notes ? `<small>${linkNotes(row.notes)}</small>`:''}</div></div>
        <div>${checkSummary(row)}</div>
        <div class="watch-actions">${row.confirmed ? `<button data-action="profile" data-id="${row.id}" title="Open profile / Add to library" aria-label="Open profile / Add to library"><i class="fa-solid fa-circle-plus" aria-hidden="true"></i></button>` : `<button data-action="review" data-id="${row.id}" title="Review matches" aria-label="Review ${reviewCandidates(row).length} candidate matches" ${row.candidates.length?'':'disabled'}><i class="fa-solid fa-user-check" aria-hidden="true"></i> <span class="watch-db-count">${String(Math.min(reviewCandidates(row).length, 99)).padStart(2, '0')}</span></button><button data-action="check" data-id="${row.id}" title="Check databases" aria-label="Check databases" ${row.checking?'disabled':''}><i class="fa-solid fa-rotate"></i></button>`}<button data-action="edit" data-id="${row.id}" title="Edit entry" aria-label="Edit entry"><i class="fa-solid fa-pen"></i></button><button data-action="delete" data-id="${row.id}" title="Remove from watchlist" aria-label="Remove from watchlist"><i class="fa-solid fa-xmark"></i></button></div>
        </article>`).join('') : '<div class="watch-empty"><i class="fa-solid fa-eye" aria-hidden="true"></i><strong>Keep an eye out</strong><p>Add a name above. We’ll check the databases for you.</p></div>';
      if (items.length && !shown.length) host().querySelector('.watch-list').innerHTML = '<div class="watch-empty">No entries match these filters.</div>';
  }
  function edit(row) {
    const form=host().querySelector('form'); form.dataset.id=row?.id||'';
    for(const key of ['name','notes','source_url']) form.elements[key].value=row?.[key]||'';
    form.elements.aliases.value=(row?.aliases||[]).join(', ');
    const submit = form.querySelector('[type=submit]');
    submit.innerHTML = `<i class="fa-solid ${row ? 'fa-floppy-disk' : 'fa-circle-plus'}" aria-hidden="true"></i>`;
    submit.title = row ? 'Save changes' : 'Add to watchlist';
    submit.setAttribute('aria-label', submit.title);
    form.elements.name.focus({preventScroll:true});
  }
  function editDetails(row) {
    const fields = [
      ['gender','Gender'],['birthdate','Date of birth','date'],['death_date','Date of death','date'],
      ['country','Country'],['ethnicity','Ethnicity'],['hair_color','Hair colour'],['eye_color','Eye colour'],
      ['height','Height (cm)'],['weight','Weight (kg)'],['measurements','Measurements'],
      ['career_start_year','Career start year'],['career_end_year','Career end year'],
      ['tattoos','Tattoos'],['piercings','Piercings'],['biography','Biography','textarea'],
      ['image_url','Photo URL','url'],['urls','Profile links — one URL per line','textarea'],
      ['evidence','Sources / evidence for these details','textarea']
    ];
    const dialog = document.createElement('dialog');
    dialog.className = 'watch-review watch-editor';
    const countries = Object.values(window.COUNTRY_FLAG_LOOKUP || {});
    const choices = {
      gender:['Female','Male','Transgender Female','Transgender Male','Non-binary','Intersex','Other'],
      country:[...new Set(countries.map(v=>v.code.toUpperCase()))].sort((a,b)=>countryLabel(a).localeCompare(countryLabel(b))),
      ethnicity:['Asian','Black','Caucasian','Hispanic','Indian','Middle Eastern','Mixed','Native American','Pacific Islander','Other'],
      hair_color:['Black','Brown','Blonde','Red','Auburn','Grey','White','Bald','Other'],
      eye_color:['Brown','Blue','Green','Hazel','Grey','Amber','Other']
    };
    function countryLabel(value) {return window.COUNTRY_FLAG_LOOKUP?.[value.toLowerCase()]?.name || value;}
    const input = (key,label,value,type='text',detail=false) => {
      let control;
      if (choices[key]) {
        const values=[...new Set([value,...choices[key]].filter(Boolean))];
        control=`<select name="${key}" data-detail><option value="">Not set</option>${values.map(v=>`<option value="${esc(v)}" ${v===value?'selected':''}>${esc(key==='country'?countryLabel(v):v)}</option>`).join('')}</select>`;
      } else if(type==='textarea') control=`<textarea name="${key}" rows="3" ${detail?'data-detail':''}>${esc(value)}</textarea>`;
      else control=`<input name="${key}" type="${type}" value="${esc(value)}" ${detail?'data-detail':''} ${key==='name'?'required maxlength="150"':''}>`;
      return `<label class="watch-field${type==='textarea'?' watch-editor-wide':''}"><span>${label}</span>${control}</label>`;
    };
    dialog.innerHTML = `<form><header><h2>Edit ${esc(row.name)}</h2><button type="button" data-close aria-label="Close">×</button></header><div class="watch-editor-fields">${input('name','Name / alias',row.name)}${input('aliases','Other aliases, comma separated',(row.aliases||[]).join(', '))}${input('source_url','Source link',row.source_url,'url')}${input('notes','Notes',row.notes)}${fields.map(([key,label,type])=>input(key,label,row.details?.[key]||'',type,true)).join('')}</div><footer><span role="status" aria-live="polite"></span><button type="submit" title="Save details" aria-label="Save details"><i class="fa-solid fa-floppy-disk" aria-hidden="true"></i></button></footer></form>`;
    dialog.querySelector('[data-close]').onclick=()=>dialog.close();
    dialog.addEventListener('close',()=>dialog.remove(),{once:true});
    dialog.querySelector('form').onsubmit=async event=>{
      event.preventDefault();
      const form=event.currentTarget, button=form.querySelector('[type=submit]');
      const values=Object.fromEntries(new FormData(form));
      const details=Object.fromEntries(fields.map(([key])=>[key,values[key]]));
      button.disabled=true;
      try {
        await api('/'+row.id,'PUT',{name:values.name,aliases:values.aliases.split(',').map(v=>v.trim()).filter(Boolean),source_url:values.source_url,notes:values.notes,details});
        dialog.close();await load();
      } catch(e){form.querySelector('[role=status]').textContent=e.message;button.disabled=false;}
    };
    document.body.appendChild(dialog);dialog.showModal();
    const form=dialog.querySelector('form');
    const statusEl=form.querySelector('[role=status]');
    const actions=document.createElement('div');actions.className='watch-submit-actions';
    actions.innerHTML=`<select data-destination aria-label="Library destination"><option value="">Choose library directory…</option></select><span data-submit-options hidden><button type="button" disabled title="StashDB submission disabled" aria-label="StashDB submission disabled"><img src="/static/logos/stashdb.webp" alt="StashDB"></button>${['FansDB','JAVStash'].filter(source=>!new Set([...(row.origin_sources||[]),row.confirmed?.source]).has(source)).map(source=>`<button type="button" data-submit-db="${source}" title="Submit draft to ${source} and add to library" aria-label="Submit draft to ${source} and add to library"><img src="/static/logos/${sourceLogos[source]}.webp" alt="${source}"></button>`).join('')}</span>`;
    form.querySelector('footer').appendChild(actions);
    const eligible=()=>['image_url','gender','evidence'].every(key=>form.elements[key].value.trim()) && !!(form.elements.urls.value.trim()||form.elements.source_url.value.trim());
    const updateOptions=()=>{actions.querySelector('[data-submit-options]').hidden=!eligible();};
    form.addEventListener('input',updateOptions);updateOptions();
    fetch('/api/metadata/dirs?kind=performer').then(r=>{if(!r.ok)throw Error('Could not load library directories');return r.json();}).then(data=>{
      actions.querySelector('select').innerHTML='<option value="">Choose library directory…</option>'+(data.dirs||[]).map(d=>`<option value="${esc(d.path)}">${esc(d.label)}</option>`).join('');
    }).catch(e=>statusEl.textContent=e.message);
    actions.addEventListener('click',async event=>{
      const button=event.target.closest('[data-submit-db]');if(!button)return;
      if(!form.reportValidity())return;
      const destination=actions.querySelector('select').value;
      if(!destination){statusEl.textContent='Choose a library directory first.';return;}
      const values=Object.fromEntries(new FormData(form));
      const details=Object.fromEntries(fields.map(([key])=>[key,values[key]]));
      const buttons=Array.from(form.querySelectorAll('button'));buttons.forEach(b=>b.disabled=true);
      try {
        await api('/'+row.id,'PUT',{name:values.name,aliases:values.aliases.split(',').map(v=>v.trim()).filter(Boolean),source_url:values.source_url,notes:values.notes,details});
        const result=await api('/'+row.id+'/submit','POST',{source:button.dataset.submitDb,dest_dir:destination});
        statusEl.textContent=`Added to library. ${button.dataset.submitDb} draft ${result.id} submitted for review.`;
        await load();
      } catch(e){statusEl.textContent=e.message;}
      finally {buttons.forEach(b=>b.disabled=false);actions.querySelector('button:not([data-submit-db])').disabled=true;}
    });
    if(row.confirmed || Object.keys(row.linked_profiles||{}).length){
      statusEl.textContent='Loading linked profile details…';
      const edited=new Set();form.addEventListener('input',event=>edited.add(event.target.name));
      api('/'+row.id+'/details','POST').then(result=>{
        if(!dialog.isConnected)return;
        for(const [key,value] of Object.entries(result.details||{}))if(form.elements[key]&&!edited.has(key)){
          const field=form.elements[key];
          if(field.tagName==='SELECT' && value && !Array.from(field.options).some(option=>option.value===value))field.add(new Option(key==='country'?countryLabel(value):value,value));
          field.value=value;
        }
        statusEl.textContent='';updateOptions();
      }).catch(e=>{statusEl.textContent='Could not import profile details: '+e.message;});
    }
  }
  function review(row) {
    const dialog=document.createElement('dialog');dialog.className='watch-review';
    dialog.innerHTML=`<header><h2>Matches for ${esc(row.name)}</h2><button data-close aria-label="Close">×</button></header><p>Review the profile before confirming. Shared aliases can belong to different people.</p><div class="watch-review-filters"><div class="watch-source-filters" role="group" aria-label="Filter by database"><button type="button" data-source="" aria-pressed="true">All</button>${Object.entries(sourceLogos).map(([source,logo])=>`<button type="button" data-source="${source}" aria-pressed="false" title="${source}" aria-label="${source}" ${row.candidates.some(hit=>hit.source===source)?'':'disabled'}><img src="/static/logos/${logo}.webp" alt="${source}"></button>`).join('')}</div><div class="watch-review-search"><input type="search" data-match-query placeholder="Filter names or aliases…" aria-label="Filter names or aliases"><button type="button" data-photo-filter aria-pressed="false" title="Only candidates with photos" aria-label="Only candidates with photos"><i class="fa-solid fa-image" aria-hidden="true"></i></button><button type="button" data-reset-filters title="Reset filters" aria-label="Reset filters"><i class="fa-solid fa-rotate-left" aria-hidden="true"></i></button></div><span class="watch-match-count" aria-live="polite"></span></div><div class="watch-candidates">${row.candidates.map((hit,i)=>`<article data-candidate="${i}"><img src="${esc(hit.image || '/static/img/profile.webp')}" alt="" loading="lazy" onerror="this.onerror=null;this.src='/static/img/profile.webp'"><div><strong>${esc(hit.name)}</strong>${hit.in_library ? libraryBadge : ''}<small>${esc(Array.isArray(hit.aliases)?hit.aliases.join(', '):hit.aliases)}</small><button class="watch-profile-button" data-open="${i}" title="View ${esc(hit.source)} profile" aria-label="View ${esc(hit.source)} profile"><img src="/static/logos/${sourceLogos[hit.source] || 'stashdb'}.webp" alt="${esc(hit.source)}"></button> ${`<button data-confirm="${i}" title="${hit.external_only ? 'Save supplementary link' : 'Confirm performer'}" aria-label="${hit.external_only ? 'Save supplementary link' : 'Confirm performer'}"><i class="fa-solid ${hit.external_only ? 'fa-link' : 'fa-check'}" aria-hidden="true"></i></button>`}</div></article>`).join('')}</div><p class="watch-no-matches" hidden>No candidates match these filters.</p><p role="status"></p>`;
    let selectedSource = '', photosOnly = false;
    const search = dialog.querySelector('[data-match-query]');
    const applyFilters = () => {
      const words = search.value.toLocaleLowerCase().trim().split(/\s+/).filter(Boolean);
      let visible = 0;
      dialog.querySelectorAll('[data-candidate]').forEach(card => {
        const hit = row.candidates[Number(card.dataset.candidate)];
        const text = [hit.name, ...(Array.isArray(hit.aliases) ? hit.aliases : [hit.aliases || ''])].join(' ').toLocaleLowerCase();
        const show = (!selectedSource || hit.source === selectedSource) && (!photosOnly || !!hit.image) && words.every(word => text.includes(word));
        card.hidden = !show;
        if (show) visible++;
      });
      dialog.querySelectorAll('[data-source]').forEach(button => button.setAttribute('aria-pressed',String(button.dataset.source === selectedSource)));
      dialog.querySelector('[data-photo-filter]').setAttribute('aria-pressed',String(photosOnly));
      dialog.querySelector('.watch-match-count').textContent = `${visible} / ${row.candidates.length} matches`;
      dialog.querySelector('.watch-no-matches').hidden = visible > 0;
    };
    search.addEventListener('input',applyFilters);
    dialog.querySelectorAll('[data-source]').forEach(button => button.onclick = () => {selectedSource = button.dataset.source; applyFilters();});
    dialog.querySelector('[data-photo-filter]').onclick = () => {photosOnly = !photosOnly; applyFilters();};
    dialog.querySelector('[data-reset-filters]').onclick = () => {selectedSource = ''; photosOnly = false; search.value = ''; applyFilters();};
    applyFilters();
    document.body.appendChild(dialog);dialog.showModal();
    dialog.querySelector('[data-close]').onclick=()=>dialog.close();
    dialog.addEventListener('close',()=>dialog.remove(),{once:true});
    dialog.addEventListener('click',async event=>{
      const open=event.target.closest('[data-open]');if(open){window.open(sourceUrl(row.candidates[+open.dataset.open]), '_blank', 'noopener,noreferrer');return;}
      const confirm=event.target.closest('[data-confirm]');if(!confirm)return;
      confirm.disabled=true;
      try {await api(`/${row.id}/confirm`,'POST',row.candidates[+confirm.dataset.confirm]);dialog.close();await load();}
      catch(e){dialog.querySelector('[role=status]').textContent=e.message;confirm.disabled=false;}
    });
  }
  function mount() {
    if(!mounted){
      mounted=true;
      host().innerHTML=`<div class="watch-sticky"><form class="watch-form"><label class="watch-field"><span>Name / alias</span><input name="name" placeholder="Name or alias" aria-label="Name or alias" maxlength="150" required></label><label class="watch-field"><span>Other aliases</span><input name="aliases" placeholder="Other aliases, comma separated" aria-label="Additional aliases"></label><label class="watch-field"><span>Source link</span><input name="source_url" type="url" placeholder="Source URL (optional)" aria-label="Source URL" maxlength="2000"></label><label class="watch-field"><span>Notes</span><input name="notes" placeholder="Notes (optional)" aria-label="Notes" maxlength="2000"></label><button type="submit" class="watch-form-icon" title="Add to watchlist" aria-label="Add to watchlist"><i class="fa-solid fa-circle-plus" aria-hidden="true"></i></button><button type="button" class="watch-form-icon" data-cancel title="Clear form" aria-label="Clear form"><i class="fa-solid fa-eraser" aria-hidden="true"></i></button></form><div class="watch-tools"><div class="watch-list-filters" role="group" aria-label="Filter watchlist">${[['','fa-list','All entries'],['review','fa-user-check','Needs review'],['confirmed','fa-circle-check','Confirmed matches'],['unmatched','fa-user-slash','No matches']].map(([value,icon,label])=>`<button type="button" data-list-filter="${value}" title="${label}" aria-label="${label}" aria-pressed="${value === listFilter}"><i class="fa-solid ${icon}" aria-hidden="true"></i></button>`).join('')}${Object.entries(sourceLogos).filter(([source])=>!['Coomer','Babepedia'].includes(source)).map(([source,logo])=>`<button type="button" data-db-filter="${source}" title="Matches on ${source}" aria-label="Matches on ${source}" aria-pressed="false"><img src="/static/logos/${logo}.webp" alt=""></button>`).join('')}</div><span class="watch-status" role="status"></span><button data-all title="Check all" aria-label="Check all"><i class="fa-solid fa-rotate" aria-hidden="true"></i></button></div></div><div class="watch-list"></div>`;
      host().querySelectorAll('[data-list-filter]').forEach(button => button.onclick = () => {listFilter = button.dataset.listFilter; render();});
      host().querySelectorAll('[data-db-filter]').forEach(button => button.onclick = () => {databaseFilter = databaseFilter === button.dataset.dbFilter ? '' : button.dataset.dbFilter; render();});
      const form=host().querySelector('form');
      form.onsubmit=async event=>{event.preventDefault();const button=form.querySelector('[type=submit]');button.disabled=true;try{const data=Object.fromEntries(new FormData(form));data.aliases=data.aliases.split(',').map(s=>s.trim()).filter(Boolean);await api(form.dataset.id?'/'+form.dataset.id:'',form.dataset.id?'PUT':'POST',data);edit(null);await load();}catch(e){host().querySelector('.watch-status').textContent=e.message;}finally{button.disabled=false;}};
      host().querySelector('[data-cancel]').onclick=()=>edit(null);
      host().querySelector('[data-all]').onclick=async()=>{try{await api('/check','POST');await load();}catch(e){host().querySelector('.watch-status').textContent=e.message;}};
      host().querySelector('.watch-list').onclick=async event=>{
        const button=event.target.closest('[data-action]');if(!button)return;
        const row=items.find(r=>r.id===+button.dataset.id);if(!row)return;
        const action=button.dataset.action;
        if(action==='edit')return editDetails(row);
        if(action==='profile')return row.confirmed?.external_only ? editDetails(row) : profile(row.confirmed);
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
