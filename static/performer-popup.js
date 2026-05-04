/* Universal performer popup — opened from any page, always renders the
 * same 80vw × 80vh four-cell layout:
 *   ┌─────────────────────────┬─────────────────────────┐
 *   │  Headshot + Bio         │  Filmography            │
 *   ├─────────────────────────┼─────────────────────────┤
 *   │  Scene-image carousel   │  Performer image gallery│
 *   └─────────────────────────┴─────────────────────────┘
 *
 * Public API:
 *   window.openPerformerPopup({ stashId, libraryRowId, name })
 *
 * At least one of stashId / libraryRowId / name must be set. The backend
 * resolves whichever is given into the unified popup payload.
 *
 * Click-anywhere wiring: any element with `data-performer-link` and one
 * or more of `data-stash-id` / `data-library-row-id` / `data-name` will
 * open the popup when clicked. A single delegated handler on document.body
 * picks them up — no per-render wiring required.
 */
(function () {
  if (window._performerPopupInited) return;
  window._performerPopupInited = true;

  const ESC = (s) =>
    String(s ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');

  let _galleryFullScreenIdx = -1;
  let _activeOpts = null;
  let _activeData = null;
  let _headshotPollHandle = null;
  let _stageTimer = null;
  // Cross-modal coordination: image picker / upload / ext-link modal
  // check this against their saved row id to decide whether to refresh
  // an open popup. Mirrors the role _bioPopupRowId served before.
  window._performerPopupActiveId = null;

  function ensureModal() {
    if (document.getElementById('performerPopupModal')) return;
    const div = document.createElement('div');
    div.id = 'performerPopupModal';
    div.className = 'performer-popup-overlay';
    div.innerHTML = `
      <div class="performer-popup-shell" role="dialog" aria-modal="true" aria-label="Performer">
        <div class="performer-popup-bg" id="performerPopupBg" aria-hidden="true"></div>
        <div class="performer-popup-bg-overlay" aria-hidden="true"></div>
        <header class="performer-popup-header" id="performerPopupHeader">
          <!-- Toolbar lives inside the header as a flex sibling of the
               name and pill row so its column is reserved by the
               layout — no absolute-overlay overlap with the pills. -->
          <div class="performer-popup-toolbar">
            <button type="button" class="performer-popup-tool" id="performerPopupRefresh"
                    title="Fill missing headshot / posters">
              <i class="fa-solid fa-arrows-rotate"></i>
            </button>
            <button type="button" class="performer-popup-tool performer-popup-close"
                    title="Close" aria-label="Close">
              <i class="fa-solid fa-xmark"></i>
            </button>
          </div>
        </header>
        <div class="performer-popup-grid">
          <section class="performer-popup-cell pp-cell-bio">
            <div class="pp-loading">Loading…</div>
          </section>
          <section class="performer-popup-cell pp-cell-films">
            <div class="pp-loading">Loading filmography…</div>
          </section>
          <section class="performer-popup-cell pp-cell-carousel">
            <div class="pp-loading">Loading scenes…</div>
          </section>
          <section class="performer-popup-cell pp-cell-gallery">
            <div class="pp-loading">Loading gallery…</div>
          </section>
          <aside class="pp-cell-posters" aria-label="Posters">
            <div class="pp-poster-slot pp-poster-slot--primary is-empty" data-poster-role="primary">
              <span class="pp-poster-slot-empty">—</span>
            </div>
            <div class="pp-poster-slot pp-poster-slot--secondary is-empty" data-poster-role="secondary">
              <span class="pp-poster-slot-empty">—</span>
            </div>
          </aside>
        </div>
      </div>`;
    document.body.appendChild(div);
    // Inject the #spotlight-bloom filter if the host page hasn't
    // already defined it. Lets the secondary poster slot use the
    // same tritone+airbrush composite as the /scenes spotlight tiles.
    if (!document.getElementById('spotlight-bloom')) {
      const sb = document.createElement('div');
      sb.innerHTML = `
        <svg width="0" height="0" style="position:absolute;width:0;height:0;pointer-events:none" aria-hidden="true">
          <defs>
            <filter id="spotlight-bloom" color-interpolation-filters="sRGB">
              <feComponentTransfer in="SourceGraphic" result="ts-bright">
                <feFuncR type="linear" slope="2.6" intercept="-1.3"/>
                <feFuncG type="linear" slope="2.6" intercept="-1.3"/>
                <feFuncB type="linear" slope="2.6" intercept="-1.3"/>
              </feComponentTransfer>
              <feGaussianBlur in="ts-bright" stdDeviation="9" result="ts-bloom-full"/>
              <feComponentTransfer in="ts-bloom-full" result="ts-bloom">
                <feFuncR type="linear" slope="0.5"/>
                <feFuncG type="linear" slope="0.5"/>
                <feFuncB type="linear" slope="0.5"/>
              </feComponentTransfer>
              <feBlend in="SourceGraphic" in2="ts-bloom" mode="screen"/>
            </filter>
          </defs>
        </svg>`;
      document.body.appendChild(sb.firstElementChild);
    }
    div.addEventListener('click', (e) => {
      if (e.target === div) closePerformerPopup();
    });
    div.querySelector('.performer-popup-close').addEventListener('click', closePerformerPopup);
    div.querySelector('#performerPopupRefresh').addEventListener('click', refreshImagesFromCurrent);
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && div.classList.contains('open')) closePerformerPopup();
    });
  }

  async function refreshImagesFromCurrent() {
    if (!_activeOpts || !window._performerPopupActiveId) return;
    const btn = document.getElementById('performerPopupRefresh');
    if (btn) {
      btn.disabled = true;
      btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>';
    }
    try {
      await fetch('/api/performers/enrich-headshot', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ row_id: window._performerPopupActiveId }),
      });
      setTimeout(() => openPerformerPopup({ ..._activeOpts, _refresh: true }), 2500);
    } catch (e) {
      alert(e.message || 'Failed');
    } finally {
      if (btn) {
        btn.disabled = false;
        btn.innerHTML = '<i class="fa-solid fa-arrows-rotate"></i>';
      }
    }
  }
  window.refreshPerformerPopup = function () {
    if (_activeOpts) openPerformerPopup({ ..._activeOpts, _refresh: true });
  };

  function closePerformerPopup() {
    const m = document.getElementById('performerPopupModal');
    if (m) m.classList.remove('open');
    if (_headshotPollHandle) {
      clearTimeout(_headshotPollHandle);
      _headshotPollHandle = null;
    }
    if (_stageTimer) {
      clearTimeout(_stageTimer);
      _stageTimer = null;
    }
    _activeOpts = null;
    _activeData = null;
    window._performerPopupActiveId = null;
  }
  window.closePerformerPopup = closePerformerPopup;

  // ── Add-to-library modal ────────────────────────────────────────────
  // Small in-place modal that appears on top of the performer popup
  // when the user clicks the "+" button on an unmatched performer.
  // Mirrors /discover's create flow (POST /api/metadata/create) without
  // navigating away.
  function ensureAddModal() {
    if (document.getElementById('ppAddToLibraryModal')) return;
    const div = document.createElement('div');
    div.id = 'ppAddToLibraryModal';
    div.className = 'pp-add-overlay';
    div.innerHTML = `
      <div class="pp-add-shell" role="dialog" aria-modal="true" aria-label="Add to library">
        <div class="pp-add-header">
          <span class="pp-add-title">Add to library</span>
          <button type="button" class="pp-add-close" aria-label="Close" title="Close">
            <i class="fa-solid fa-xmark"></i>
          </button>
        </div>
        <div class="pp-add-body">
          <div class="pp-add-row">
            <span class="pp-add-name" id="ppAddName">—</span>
          </div>
          <div class="pp-add-row">
            <label class="pp-add-label" for="ppAddDest">Directory</label>
            <select class="pp-add-select" id="ppAddDest">
              <option value="">Loading…</option>
            </select>
          </div>
          <input type="text" class="pp-add-custom" id="ppAddCustom"
                 placeholder="Custom path…" style="display:none">
          <div class="pp-add-msg" id="ppAddMsg"></div>
        </div>
        <div class="pp-add-footer">
          <button type="button" class="pp-add-btn pp-add-btn--cancel" id="ppAddCancel">Cancel</button>
          <button type="button" class="pp-add-btn pp-add-btn--primary" id="ppAddSubmit">
            <i class="fa-solid fa-plus"></i> Add
          </button>
        </div>
      </div>`;
    document.body.appendChild(div);
    div.addEventListener('click', (e) => {
      if (e.target === div) closeAddToLibraryModal();
    });
    div.querySelector('.pp-add-close').addEventListener('click', closeAddToLibraryModal);
    div.querySelector('#ppAddCancel').addEventListener('click', closeAddToLibraryModal);
    div.querySelector('#ppAddDest').addEventListener('change', () => {
      const sel = div.querySelector('#ppAddDest');
      const custom = div.querySelector('#ppAddCustom');
      custom.style.display = (sel.value === '__custom__') ? 'block' : 'none';
    });
    div.querySelector('#ppAddSubmit').addEventListener('click', submitAddToLibrary);
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && div.classList.contains('open')) closeAddToLibraryModal();
    });
  }

  async function openAddToLibraryModal(data) {
    ensureAddModal();
    const div = document.getElementById('ppAddToLibraryModal');
    const id = (data && data.identity) || {};
    const name = id.canonical_name || (_activeOpts && _activeOpts.name) || '';
    if (!name) { alert('No performer name available'); return; }
    // Pick the source / id pair to send to /api/metadata/create. Stash
    // sources are preferred when present (richer data) — TPDB is the
    // fallback when the click came from a TPDB-sourced cast.
    let source = '', sid = '';
    if (id.stash_id)       { source = 'StashDB'; sid = id.stash_id; }
    else if (id.fansdb_id) { source = 'FansDB';  sid = id.fansdb_id; }
    else if (id.tpdb_id)   { source = 'TPDB';    sid = id.tpdb_id; }
    div._submitCtx = { source, id: sid, name };
    div.querySelector('#ppAddName').textContent = name;
    const msg = div.querySelector('#ppAddMsg');
    msg.textContent = '';
    msg.className = 'pp-add-msg';
    const submitBtn = div.querySelector('#ppAddSubmit');
    submitBtn.disabled = false;
    submitBtn.innerHTML = '<i class="fa-solid fa-plus"></i> Add';
    div.classList.add('open');
    // Load directory list (cached after first call by browser).
    try {
      const r = await fetch('/api/metadata/dirs', { credentials: 'same-origin' });
      const d = await r.json();
      const sel = div.querySelector('#ppAddDest');
      const opts = ['<option value="">Choose directory…</option>']
        .concat((d.dirs || []).map(dir => `<option value="${ESC(dir.path)}">${ESC(dir.label)}</option>`))
        .concat(['<option value="__custom__">Custom path…</option>']);
      sel.innerHTML = opts.join('');
    } catch (e) {
      div.querySelector('#ppAddDest').innerHTML = '<option value="">(failed to load)</option>';
    }
  }

  function closeAddToLibraryModal() {
    const div = document.getElementById('ppAddToLibraryModal');
    if (div) div.classList.remove('open');
  }

  async function submitAddToLibrary() {
    const div = document.getElementById('ppAddToLibraryModal');
    if (!div) return;
    const ctx = div._submitCtx || {};
    const sel = div.querySelector('#ppAddDest');
    const customEl = div.querySelector('#ppAddCustom');
    const dest = (sel && sel.value === '__custom__'
      ? (customEl.value || '').trim()
      : (sel.value || '')) || '';
    const msg = div.querySelector('#ppAddMsg');
    if (!dest) {
      msg.textContent = 'Choose a destination directory.';
      msg.className = 'pp-add-msg pp-add-msg--err';
      return;
    }
    if (!ctx.name) {
      msg.textContent = 'Missing performer name.';
      msg.className = 'pp-add-msg pp-add-msg--err';
      return;
    }
    const submitBtn = div.querySelector('#ppAddSubmit');
    submitBtn.disabled = true;
    submitBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Adding…';
    try {
      const r = await fetch('/api/metadata/create', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          type:     'performer',
          source:   ctx.source || '',
          id:       ctx.id || '',
          name:     ctx.name,
          dest_dir: dest,
        }),
      });
      const d = await r.json();
      if (!r.ok || d.success === false) {
        throw new Error(d.error || ('HTTP ' + r.status));
      }
      // Refresh the underlying performer popup so the heart/lock icons
      // appear in place of the + button (server now knows the row).
      if (_activeOpts) {
        openPerformerPopup({ ..._activeOpts, _refresh: true });
      }
      closeAddToLibraryModal();
    } catch (e) {
      msg.textContent = 'Error: ' + (e.message || 'Failed');
      msg.className = 'pp-add-msg pp-add-msg--err';
      submitBtn.disabled = false;
      submitBtn.innerHTML = '<i class="fa-solid fa-plus"></i> Add';
    }
  }

  async function openPerformerPopup(opts) {
    ensureModal();
    const m = document.getElementById('performerPopupModal');
    m.classList.add('open');
    _activeOpts = { libraryRowId: opts.libraryRowId, stashId: opts.stashId, name: opts.name };

    // Reset cells to skeleton state (unless this is a soft refresh).
    // The skeleton scaffolds match each cell's real layout so the
    // transition to populated content is a swap rather than a relayout.
    if (!opts._refresh) {
      paintSkeletons(m);
      paintStageBanner(m);
    }

    const params = new URLSearchParams();
    if (opts.libraryRowId) params.set('row_id', String(opts.libraryRowId));
    if (opts.stashId) params.set('stash_id', String(opts.stashId));
    if (opts.tpdbId)  params.set('tpdb_id', String(opts.tpdbId));
    if (opts.name) params.set('name', String(opts.name));
    if (opts._refresh) params.set('refresh', '1');

    let data;
    try {
      const r = await fetch('/api/performer/popup?' + params.toString(), { credentials: 'same-origin' });
      data = await r.json();
      if (!r.ok || data.error) throw new Error(data.error || 'Load failed');
    } catch (e) {
      clearStageBanner(m);
      m.querySelector('.pp-cell-bio').innerHTML = `<div class="pp-error">${ESC(e.message || 'Error')}</div>`;
      return;
    }

    // Update active id for cross-modal coordination + paint background hero
    window._performerPopupActiveId = (data.library_status && data.library_status.row_id) || null;
    paintBgHero(data);

    // Headshot enrich-on-miss: trigger a background fetch + poll
    if (window._performerPopupActiveId && !data.headshot_url) {
      fetch('/api/performers/enrich-headshot', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ row_id: window._performerPopupActiveId }),
      }).then(r => r.json()).then(r => {
        if (r && r.started) startHeadshotPoll(window._performerPopupActiveId, 0);
      }).catch(() => {});
    }

    renderHeader(m.querySelector('#performerPopupHeader'), data);
    renderBio(m.querySelector('.pp-cell-bio'), data);
    renderFilmography(m.querySelector('.pp-cell-films'), data);
    renderCarousel(m.querySelector('.pp-cell-carousel'), data);
    renderGallery(m.querySelector('.pp-cell-gallery'), data);
    renderPosters(m.querySelector('.pp-cell-posters'), data);
    clearStageBanner(m);

    // Stash the loaded payload so the inline Add modal can grab the
    // identity (source / id / name) without re-fetching.
    _activeData = data;

    // Inline action wiring (lock/favourite/upload/alias/add-to-library)
    // — re-bound on every render. .pp-name-action and .pp-cta-btn are
    // both eligible.
    m.querySelectorAll('.pp-action-btn[data-action], .pp-name-action[data-action], .pp-cta-btn[data-action]').forEach((btn) => {
      btn.addEventListener('click', async (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        const action = btn.dataset.action;
        if (action === 'add-to-library') {
          openAddToLibraryModal(_activeData);
          return;
        }
        const rowId = parseInt(btn.dataset.rowId, 10);
        if (!rowId) return;
        try {
          if (action === 'favourite') {
            const on = !btn.classList.contains('is-on');
            await fetch('/api/favourites/star', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              credentials: 'same-origin',
              body: JSON.stringify({ id: rowId, is_favourite: on }),
            });
            btn.classList.toggle('is-on', on);
            btn.title = on ? 'Unfavourite' : 'Favourite';
            fetch(`/api/performer/popup?row_id=${rowId}&refresh=1`, { credentials: 'same-origin' }).catch(() => {});
          } else if (action === 'lock') {
            const on = !btn.classList.contains('is-locked');
            await fetch('/api/favourites/lock', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              credentials: 'same-origin',
              body: JSON.stringify({ id: rowId, matches_locked: on }),
            });
            btn.classList.toggle('is-locked', on);
            btn.title = on ? 'Unlock matches' : 'Lock matches';
            const ic = btn.querySelector('i');
            if (ic) ic.className = `fa-solid fa-${on ? 'lock' : 'lock-open'}`;
            fetch(`/api/performer/popup?row_id=${rowId}&refresh=1`, { credentials: 'same-origin' }).catch(() => {});
          } else if (action === 'upload') {
            uploadCustomImage(rowId);
          } else if (action === 'alias') {
            editAliases(rowId);
          }
        } catch (e) {
          alert(e.message || 'Failed');
        }
      });
    });

    // Profile-pill action wiring — change link / remove link
    m.querySelectorAll('.pp-profile-action-btn').forEach((btn) => {
      btn.addEventListener('click', (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        const rowId = parseInt(btn.dataset.ppRow, 10);
        const site = btn.dataset.ppSite;
        if (btn.dataset.ppRemove) {
          removeExtLink(rowId, site);
        } else {
          // Edit/change — site lookup uses /library's openSearchForRow
          // for DB sources or openExtLinkModal for ext sources.
          editExtLink(btn);
        }
      });
    });
    // Missing pills (clicking the button itself, not the action sub-button)
    m.querySelectorAll('.pp-profile-pill.is-missing[data-pp-edit-action]').forEach((btn) => {
      btn.addEventListener('click', (ev) => {
        ev.preventDefault();
        editExtLink(btn);
      });
    });

    // Headshot click → opens the Manage Posters modal (when on /library
    // where openPosterRolePicker exists). Elsewhere, navigate to library
    // with ?focus= so the user lands on the editor.
    const headshotWrap = m.querySelector('.pp-headshot-wrap');
    if (headshotWrap && data.library_status && data.library_status.in_library) {
      const rid = data.library_status.row_id;
      headshotWrap.classList.add('is-clickable');
      headshotWrap.title = 'Manage images';
      headshotWrap.onclick = (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        if (typeof window.openPosterRolePicker === 'function') {
          window.openPosterRolePicker(rid);
        } else {
          window.location.href = `/library?focus=${encodeURIComponent(rid)}&manage=images`;
        }
      };
    }
  }

  /* ── Image upload + alias editing (modal forms) ───────────────── */

  function uploadCustomImage(rowId) {
    // File input via dynamic <input type="file">
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = 'image/*';
    input.onchange = async () => {
      if (!input.files || !input.files[0]) return;
      const fd = new FormData();
      fd.append('row_id', String(rowId));
      fd.append('file', input.files[0]);
      try {
        const r = await fetch('/api/performers/upload-image', {
          method: 'POST',
          credentials: 'same-origin',
          body: fd,
        });
        const d = await r.json().catch(() => ({}));
        if (!r.ok) { alert(d.error || 'Upload failed'); return; }
        if (window._performerPopupActiveId === rowId && _activeOpts) {
          openPerformerPopup({ ..._activeOpts, _refresh: true });
        }
      } catch (e) { alert(e.message || 'Failed'); }
    };
    input.click();
  }

  async function editAliases(rowId) {
    let cur = '';
    try {
      const r = await fetch('/api/performer/popup?row_id=' + rowId, { credentials: 'same-origin' });
      const d = await r.json();
      cur = ((d && d.identity && d.identity.aliases) || []).join(', ');
    } catch (e) { /* fall through */ }
    const v = window.prompt('Aliases (comma-separated):', cur);
    if (v === null) return;
    const list = v.split(',').map(s => s.trim()).filter(Boolean);
    try {
      const r = await fetch('/api/performers/aliases', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ row_id: rowId, aliases: list }),
      });
      if (!r.ok) { const d = await r.json().catch(() => ({})); alert(d.error || 'Save failed'); return; }
      if (window._performerPopupActiveId === rowId && _activeOpts) {
        openPerformerPopup({ ..._activeOpts, _refresh: true });
      }
    } catch (e) { alert(e.message || 'Failed'); }
  }

  async function removeExtLink(rowId, site) {
    if (!rowId || !site) return;
    if (!window.confirm(`Remove ${site} link?`)) return;
    try {
      await fetch('/api/favourites/clear-ext-link', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ row_id: rowId, site: site.toLowerCase() }),
      });
      if (window._performerPopupActiveId === rowId && _activeOpts) {
        openPerformerPopup({ ..._activeOpts, _refresh: true });
      }
    } catch (e) { alert(e.message || 'Failed'); }
  }

  function editExtLink(btn) {
    const rowId = parseInt(btn.dataset.ppRow, 10);
    const site = btn.dataset.ppSite;
    const action = btn.dataset.ppEditAction;
    if (!rowId || !site) return;
    // Use /library's existing search modals when on /library; elsewhere
    // fall back to a simple prompt asking for the URL directly.
    if (action === 'db' && typeof window.openSearchForRow === 'function') {
      window.openSearchForRow(rowId, site);
    } else if (action === 'ext' && typeof window.openExtLinkModal === 'function') {
      const name = btn.dataset.ppName || '';
      window.openExtLinkModal(rowId, site, name);
    } else {
      const url = window.prompt(`Set ${site} URL:`, '');
      if (!url) return;
      fetch('/api/favourites/ext-link', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ row_id: rowId, site: site.toLowerCase(), url: url.trim() }),
      }).then((r) => {
        if (!r.ok) { r.json().then(d => alert(d.error || 'Failed')); return; }
        if (window._performerPopupActiveId === rowId && _activeOpts) {
          openPerformerPopup({ ..._activeOpts, _refresh: true });
        }
      }).catch((e) => alert(e.message || 'Failed'));
    }
  }
  window.openPerformerPopup = openPerformerPopup;

  /* ── Skeleton + staged loader ─────────────────────────────────── */

  const PP_STAGES = [
    { delay: 0,    text: 'Connecting',          ico: 'fa-tower-broadcast' },
    { delay: 350,  text: 'Resolving identity',  ico: 'fa-fingerprint' },
    { delay: 850,  text: 'Pulling biography',   ico: 'fa-id-card' },
    { delay: 1500, text: 'Loading filmography', ico: 'fa-film' },
    { delay: 2200, text: 'Fetching gallery',    ico: 'fa-images' },
    { delay: 3000, text: 'Polishing',           ico: 'fa-wand-sparkles' },
  ];

  function paintSkeletons(m) {
    m.querySelector('.pp-cell-bio').innerHTML = `
      <div class="pp-skel pp-skel-bio">
        <div class="pp-skel-headshot"></div>
        <div class="pp-skel-stack">
          <div class="pp-skel-line pp-skel-line--lg" style="width:55%"></div>
          <div class="pp-skel-line" style="width:35%"></div>
          <div class="pp-skel-grid">
            <div class="pp-skel-line" style="width:80%"></div>
            <div class="pp-skel-line" style="width:60%"></div>
            <div class="pp-skel-line" style="width:70%"></div>
            <div class="pp-skel-line" style="width:50%"></div>
            <div class="pp-skel-line" style="width:65%"></div>
            <div class="pp-skel-line" style="width:75%"></div>
          </div>
          <div class="pp-skel-line" style="width:90%;margin-top:8px"></div>
          <div class="pp-skel-line" style="width:85%"></div>
        </div>
      </div>`;
    m.querySelector('.pp-cell-films').innerHTML = `
      <div class="pp-skel pp-skel-films">
        <div class="pp-skel-line pp-skel-line--lg" style="width:32%;margin-bottom:14px"></div>
        ${Array.from({ length: 7 }).map((_, i) => `
          <div class="pp-skel-row">
            <div class="pp-skel-line" style="width:${48 + (i * 7) % 40}%"></div>
            <div class="pp-skel-pill"></div>
          </div>`).join('')}
      </div>`;
    m.querySelector('.pp-cell-carousel').innerHTML = `
      <div class="pp-skel pp-skel-scenes">
        <div class="pp-skel-line pp-skel-line--lg" style="width:38%;margin-bottom:14px"></div>
        <div class="pp-skel-thumbs">
          ${Array.from({ length: 6 }).map(() => '<div class="pp-skel-thumb"></div>').join('')}
        </div>
      </div>`;
    m.querySelector('.pp-cell-gallery').innerHTML = `
      <div class="pp-skel pp-skel-gallery">
        <div class="pp-skel-polaroid pp-skel-polaroid--3"></div>
        <div class="pp-skel-polaroid pp-skel-polaroid--2"></div>
        <div class="pp-skel-polaroid pp-skel-polaroid--1"></div>
      </div>`;
  }

  function paintStageBanner(m) {
    let banner = m.querySelector('.pp-stage-banner');
    if (!banner) {
      banner = document.createElement('div');
      banner.className = 'pp-stage-banner';
      banner.innerHTML = `
        <div class="pp-stage-pulse" aria-hidden="true">
          <span></span><span></span><span></span>
        </div>
        <i class="fa-solid fa-tower-broadcast pp-stage-ico"></i>
        <span class="pp-stage-text">Connecting</span>
        <span class="pp-stage-dots"><i></i><i></i><i></i></span>
      `;
      // Insert above the grid (or inside header if it's empty)
      const grid = m.querySelector('.performer-popup-grid');
      grid.parentNode.insertBefore(banner, grid);
    }
    banner.classList.add('is-active');
    if (_stageTimer) {
      clearTimeout(_stageTimer);
      _stageTimer = null;
    }
    const startedAt = performance.now();
    const advance = () => {
      const elapsed = performance.now() - startedAt;
      // Pick the highest stage whose delay has passed
      let stage = PP_STAGES[0];
      for (const s of PP_STAGES) {
        if (elapsed >= s.delay) stage = s;
      }
      const txt = banner.querySelector('.pp-stage-text');
      const ico = banner.querySelector('.pp-stage-ico');
      if (txt && txt.textContent !== stage.text) {
        // Tiny crossfade so the message change feels intentional
        txt.classList.remove('is-fresh');
        void txt.offsetWidth;
        txt.textContent = stage.text;
        txt.classList.add('is-fresh');
      }
      if (ico) ico.className = `fa-solid ${stage.ico} pp-stage-ico`;
      _stageTimer = setTimeout(advance, 350);
    };
    advance();
  }

  function clearStageBanner(m) {
    if (_stageTimer) { clearTimeout(_stageTimer); _stageTimer = null; }
    const banner = m && m.querySelector ? m.querySelector('.pp-stage-banner') : null;
    if (banner) {
      banner.classList.remove('is-active');
      // Remove from DOM after the fade so it doesn't take grid space
      setTimeout(() => banner.remove(), 350);
    }
  }

  /* ── Helpers ──────────────────────────────────────────────────── */

  function paintBgHero(data) {
    const bg = document.getElementById('performerPopupBg');
    if (!bg) return;
    // Prefer secondary poster (full-body / promo) → primary → headshot
    const imgs = data.images || [];
    const sec = imgs.find(i => /secondary/.test(i.url || '')) || null;
    const pri = imgs.find(i => /primary/.test(i.url || '')) || null;
    const hero = (sec && sec.url) || (pri && pri.url) || data.headshot_url || '';
    if (!hero) {
      bg.style.backgroundImage = '';
      bg.style.opacity = '0';
      return;
    }
    const probe = new Image();
    probe.onload = () => {
      bg.style.backgroundImage = `url(${hero})`;
      bg.style.opacity = '';
    };
    probe.src = hero;
  }

  function startHeadshotPoll(rowId, attempt) {
    if (attempt >= 8) return;
    if (_headshotPollHandle) clearTimeout(_headshotPollHandle);
    _headshotPollHandle = setTimeout(async () => {
      if (window._performerPopupActiveId !== rowId) return;
      try {
        const r = await fetch(`/api/performer/popup?row_id=${rowId}&refresh=1`, { credentials: 'same-origin' });
        const d = await r.json();
        if (d && d.headshot_url) {
          const slot = document.querySelector('#performerPopupModal .pp-headshot-wrap');
          if (slot) {
            slot.innerHTML = `<img class="pp-headshot" src="${ESC(d.headshot_url)}&v=${Date.now()}" alt="" onerror="this.parentElement.innerHTML='<div class=pp-headshot-fallback><i class=fa-solid fa-person></i></div>'">`;
          }
          paintBgHero(d);
          return;
        }
      } catch (e) { /* swallow */ }
      startHeadshotPoll(rowId, attempt + 1);
    }, 4000);
  }

  // Site-logo lookup for ext-link pills. Falls back to a text label
  // when no logo is available for the source.
  const SOURCE_LOGOS = {
    stashdb:   '/static/logos/stashdb.png',
    fansdb:    '/static/logos/fansdb.png',
    tpdb:      '/static/logos/tpdb.png',
    tmdb:      '/static/logos/tmdb.png',
    iafd:      '/static/logos/iafd.png',
    freeones:  '/static/logos/freeones.png',
    babepedia: '/static/logos/babepedia.png',
    coomer:    '/static/logos/coomer.png',
  };
  function srcLogoHtml(key, label) {
    const url = SOURCE_LOGOS[key];
    if (!url) return ESC(label);
    return `<img class="pp-src-logo" src="${ESC(url)}" alt="${ESC(label)}" title="${ESC(label)}" onerror="this.replaceWith(document.createTextNode('${ESC(label)}'))">`;
  }

  /* ── Cell renderers ───────────────────────────────────────────── */

  // Shared pill builder — rendered both in the header (centered row
  // under the name) and (historically) in the bio cell. Now lives in
  // the header only.
  function buildProfilePillsHtml(data) {
    const id = data.identity || {};
    const lib = data.library_status || {};
    const links = data.ext_links || {};
    const PILLS = [
      { key: 'tpdb',      label: 'ThePornDB',  kind: 'db',  url: links.tpdb && links.tpdb.url },
      { key: 'stashdb',   label: 'StashDB',    kind: 'db',  url: links.stashdb && links.stashdb.url },
      { key: 'fansdb',    label: 'FansDB',     kind: 'db',  url: links.fansdb && links.fansdb.url },
      { key: 'tmdb',      label: 'TMDB',       kind: 'ext', url: links.tmdb && links.tmdb.url },
      { key: 'iafd',      label: 'IAFD',       kind: 'ext', url: links.iafd && links.iafd.url },
      { key: 'freeones',  label: 'Freeones',   kind: 'ext', url: null },
      { key: 'babepedia', label: 'Babepedia',  kind: 'ext', url: null },
      { key: 'coomer',    label: 'Coomer',     kind: 'ext', url: null },
    ];
    const rowId = lib.in_library ? lib.row_id : null;
    return PILLS.map((p) => {
      const logo = srcLogoHtml(p.key, p.label);
      if (p.url) {
        const editAction = rowId && p.kind === 'db'
          ? `data-pp-edit-action="db" data-pp-site="${ESC(p.label)}" data-pp-row="${ESC(rowId)}"`
          : (rowId && p.kind === 'ext'
              ? `data-pp-edit-action="ext" data-pp-site="${ESC(p.label)}" data-pp-row="${ESC(rowId)}" data-pp-name="${ESC(id.canonical_name)}"`
              : '');
        const removeAction = rowId
          ? `data-pp-remove="1" data-pp-site="${ESC(p.label)}" data-pp-row="${ESC(rowId)}"`
          : '';
        return `<a class="pp-profile-pill is-linked" href="${ESC(p.url)}" target="_blank" rel="noopener noreferrer" title="${ESC(p.label)} — click to open">
          <i class="fa-solid fa-check pp-profile-check"></i>
          ${logo}
          ${(editAction || removeAction) ? `<span class="pp-profile-actions">
            ${editAction ? `<button type="button" class="pp-profile-action-btn" ${editAction} title="Change link"><i class="fa-solid fa-pen"></i></button>` : ''}
            ${removeAction ? `<button type="button" class="pp-profile-action-btn is-remove" ${removeAction} title="Remove link"><i class="fa-solid fa-xmark"></i></button>` : ''}
          </span>` : ''}
        </a>`;
      }
      const linkAction = rowId
        ? `data-pp-edit-action="${p.kind}" data-pp-site="${ESC(p.label)}" data-pp-row="${ESC(rowId)}" data-pp-name="${ESC(id.canonical_name)}"`
        : '';
      return `<button type="button" class="pp-profile-pill is-missing" ${linkAction} title="${ESC(p.label)} — click to search and link">
        ${logo}
        <i class="fa-solid fa-magnifying-glass" style="font-size:9px;opacity:0.7"></i>
      </button>`;
    }).join('');
  }

  function renderHeader(el, data) {
    if (!el) return;
    const id = data.identity || {};
    const bio = data.bio || {};
    const lib = data.library_status || {};
    const flag = (bio.country && window.countryFlagHtml)
      ? window.countryFlagHtml(bio.country, 'pp-flag')
      : '';
    let nameIcons = '';
    if (lib.in_library) {
      const fav = lib.is_favourite;
      const locked = lib.matches_locked;
      nameIcons = `
        <button type="button" class="pp-name-action${fav ? ' is-on' : ''}"
                data-action="favourite" data-row-id="${ESC(lib.row_id)}"
                title="${fav ? 'Unfavourite' : 'Favourite'}" aria-label="Favourite">
          <i class="fa-solid fa-heart"></i>
        </button>
        <button type="button" class="pp-name-action${locked ? ' is-locked' : ''}"
                data-action="lock" data-row-id="${ESC(lib.row_id)}"
                title="${locked ? 'Unlock matches' : 'Lock matches'}" aria-label="Lock matches">
          <i class="fa-solid fa-${locked ? 'lock' : 'lock-open'}"></i>
        </button>`;
    } else if (id.canonical_name) {
      // Not yet in library — open the inline add-to-library modal
      // (directory picker + Add). Mirrors /discover's add flow but
      // doesn't navigate away from the popup.
      nameIcons = `
        <button type="button" class="pp-name-action pp-name-action--add"
                data-action="add-to-library"
                title="Add to library"
                aria-label="Add to library">
          <i class="fa-solid fa-plus"></i>
        </button>`;
    }
    const pillsHtml = buildProfilePillsHtml(data);
    // Don't blow away the toolbar (which is now a permanent flex
    // child of the header). Replace only the name + pills slots,
    // creating them on first render.
    let nameEl = el.querySelector('.pp-name');
    if (!nameEl) {
      nameEl = document.createElement('h2');
      nameEl.className = 'pp-name';
      el.insertBefore(nameEl, el.firstChild);
    }
    nameEl.innerHTML = `
      <span class="pp-name-text">${ESC(id.canonical_name || 'Unknown')}</span>
      ${flag}
      ${nameIcons}`;
    let pillsEl = el.querySelector('.pp-profiles-grid--header');
    if (!pillsEl) {
      pillsEl = document.createElement('div');
      pillsEl.className = 'pp-profiles-grid pp-profiles-grid--header';
      // Insert before the toolbar so it sits between name and toolbar.
      const toolbar = el.querySelector('.performer-popup-toolbar');
      if (toolbar) el.insertBefore(pillsEl, toolbar);
      else el.appendChild(pillsEl);
    }
    pillsEl.innerHTML = pillsHtml;
  }

  function renderBio(el, data) {
    const id = data.identity || {};
    const bio = data.bio || {};
    const lib = data.library_status || {};
    const links = data.ext_links || {};

    const headshot = data.headshot_url || '';
    // Name + flag + fav/lock icons now live in the popup header (built
    // by renderHeader). Bio cell starts straight at the headshot.

    const stat = (label, value, opts = {}) => {
      if (!value && value !== 0) return '';
      return `<div class="pp-stat-row"><span class="pp-stat-label">${ESC(label)}</span><span class="pp-stat-value${opts.wrap ? ' wrap' : ''}">${ESC(value)}</span></div>`;
    };

    // Aliases now carry an inline edit pencil at the end so the Manage
    // CTA row is gone — headshot click opens image manager (replacing
    // upload button) and the alias-edit pencil sits with the names it
    // edits.
    const aliasEditBtn = lib.in_library
      ? `<button type="button" class="pp-alias-edit"
                 data-action="alias" data-row-id="${ESC(lib.row_id)}"
                 title="Edit aliases" aria-label="Edit aliases">
           <i class="fa-solid fa-pen"></i>
         </button>`
      : '';
    const aliasHtml = (id.aliases && id.aliases.length)
      ? `<div class="pp-aliases"><span>${id.aliases.map(ESC).join(', ')}</span>${aliasEditBtn}</div>`
      : (lib.in_library
          ? `<div class="pp-aliases pp-aliases--empty"><span style="opacity:0.5">no aliases yet</span>${aliasEditBtn}</div>`
          : '');

    let ctaHtml = '';
    if (lib.in_library) {
      // Headshot click opens image manager; alias edit moved inline.
      // Nothing left for the CTA row when in library.
      ctaHtml = '';
    } else if (id.canonical_name) {
      // Not in library yet — adding happens inline via the Add modal
      // (mirrors /discover's create flow without navigating away).
      const iafd = data.iafd_search_url || '';
      ctaHtml = `<div class="pp-cta">
        <button type="button" class="pp-cta-btn" data-action="add-to-library"
           title="Add to library">
          <i class="fa-solid fa-plus"></i> Add to library
        </button>
        ${iafd ? `<a class="pp-cta-btn" href="${ESC(iafd)}" target="_blank" rel="noopener" title="Search IAFD">
          <i class="fa-brands fa-searchengin"></i> IAFD
        </a>` : ''}
      </div>`;
    }

    el.innerHTML = `
      <div class="pp-bio-layout">
        <div class="pp-headshot-wrap">
          ${headshot
            ? `<img class="pp-headshot" src="${ESC(headshot)}" alt="${ESC(id.canonical_name)}" onerror="this.parentElement.innerHTML='<div class=pp-headshot-fallback><i class=fa-solid fa-person></i></div>'">`
            : `<div class="pp-headshot-fallback"><i class="fa-solid fa-person"></i></div>`}
        </div>
        <div class="pp-bio-text">
          ${aliasHtml}
          <div class="pp-stats">
            ${stat('Gender', bio.gender)}
            ${stat('Born', bio.birthdate)}
            ${stat('Country', bio.country)}
            ${stat('Ethnicity', bio.ethnicity)}
            ${stat('Hair', bio.hair_color)}
            ${stat('Eyes', bio.eye_color)}
            ${stat('Height', bio.height)}
            ${stat('Stats', bio.measurements, { wrap: true })}
            ${stat('Active', bio.career_start_year)}
            ${stat('Tattoos', bio.tattoos, { wrap: true })}
            ${stat('Piercings', bio.piercings, { wrap: true })}
          </div>
          ${bio.biography ? `<div class="pp-biography">${ESC(bio.biography)}</div>` : ''}
          ${ctaHtml}
          ${data.is_stub ? '<div class="pp-stub-note">Limited info — not yet matched in any database.</div>' : ''}
        </div>
      </div>`;
  }

  function renderFilmography(el, data) {
    const films = data.filmography || [];
    if (!films.length) {
      el.innerHTML = `<div class="pp-empty pp-empty--silhouette"><img src="/static/img/silhouette5.png" alt="" class="pp-empty-silhouette"/>${data.iafd_search_url ? `<a href="${ESC(data.iafd_search_url)}" target="_blank" rel="noopener" class="pp-empty-link">Search IAFD →</a>` : ''}</div>`;
      return;
    }
    // Group by year (films arrive ordered year DESC, title ASC). Insert
    // a year separator each time the year changes — matches the bespoke
    // bio popup's filmography list.
    const parts = [];
    let curYear = '__init__';
    for (const f of films) {
      const y = (f.year || '').toString().trim();
      const yLabel = y || 'Year unknown';
      if (yLabel !== curYear) {
        parts.push(`<div class="pp-films-year">${ESC(yLabel)}</div>`);
        curYear = yLabel;
      }
      const studio = (f.studio || '').toString().trim();
      const title = (f.title || '').toString().trim() || '(untitled)';
      const href = (f.url || '').toString().trim();
      const titleAttr = ESC(title + (studio ? ' — ' + studio : ''));
      const inner = `<span class="pp-films-title">${ESC(title)}</span>${studio ? `<span class="pp-films-studio">${ESC(studio)}</span>` : ''}`;
      parts.push(href
        ? `<a class="pp-films-row" href="${ESC(href)}" target="_blank" rel="noopener noreferrer" title="${titleAttr}">${inner}</a>`
        : `<div class="pp-films-row" title="${titleAttr}">${inner}</div>`);
    }
    el.innerHTML = `<div class="pp-film-list">${parts.join('')}</div>`;
  }

  async function renderCarousel(el, data) {
    // Mirrors /discover's performer-scenes layout: hits /api/scenes/recent
    // with the same source/id/type/slug/name params and renders the
    // results as scene-card tiles (thumb + title + date · studio + studio
    // logo hover overlay).
    const id = data.identity || {};
    const stashId = id.stash_id || id.fansdb_id || '';
    const sourceLabel = id.stash_id ? 'StashDB' : (id.fansdb_id ? 'FansDB' : 'TPDB');
    const params = new URLSearchParams({
      source: sourceLabel,
      id:     stashId,
      type:   'performer',
      slug:   '',
      name:   id.canonical_name || '',
    });
    el.innerHTML = `<div class="pp-loading">Loading scenes…</div>`;
    let scenes = [];
    try {
      const r = await fetch('/api/scenes/recent?' + params.toString(), { credentials: 'same-origin' });
      const d = await r.json();
      scenes = (d && d.scenes) || [];
    } catch (e) {
      el.innerHTML = `<div class="pp-error">${ESC(e.message || 'Error')}</div>`;
      return;
    }
    // Cap at the most recent 12 — /api/scenes/recent returns date-DESC.
    scenes = scenes.slice(0, 12);
    // Pad to a minimum of 9 so the grid never looks half-empty. Empty
    // slots render as "NO SIGNAL" static-noise tiles, mirroring the
    // /discover layout.
    const MIN_TILES = 9;
    const realCount = scenes.length;
    const padded = scenes.slice();
    while (padded.length < MIN_TILES) padded.push({ __static: true });
    const renderTile = (s) => {
      if (s && s.__static) {
        return `
          <div class="scene-card scene-card--static discover-info-scene-card discover-info-scene-card--performer" aria-hidden="true">
            <div class="img-load">
              <div class="scene-static-noise" aria-hidden="true"></div>
              <div class="scene-static-bands" aria-hidden="true"></div>
              <div class="scene-static-label">NO SIGNAL</div>
            </div>
            <div class="scene-meta" style="padding:6px 4px">
              <div class="scene-title" style="font-size:11px;color:rgba(255,255,255,0.35);line-height:1.3;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">— — —</div>
              <div style="font-size:10px;color:rgba(255,255,255,0.25)">CH-00 · STATIC</div>
            </div>
          </div>`;
      }
      const thumb  = s.thumb || s.image || '/static/img/missing.jpg';
      const title  = s.title || '';
      const date   = s.date || '';
      const studio = s.studio || s.site_name || '';
      const studioLogo = (studio || title)
        ? `<img class="scene-studio-logo" src="/api/studio-logo?name=${encodeURIComponent(studio)}&q=${encodeURIComponent(title)}" alt="" loading="lazy" onload="this.closest('.scene-card')?.classList.add('has-studio-logo')" onerror="this.remove()">`
        : '';
      return `
        <div class="scene-card discover-info-scene-card discover-info-scene-card--performer" tabindex="0" title="${ESC(title)}">
          <div class="img-load">
            <div class="img-spin" aria-hidden="true"></div>
            <img class="scene-thumb" src="${ESC(thumb)}" loading="lazy" onload="this.closest('.img-load')?.classList.add('ready')" onerror="this.onerror=null;this.src='/static/img/missing.jpg';this.closest('.img-load')?.classList.add('ready');">
            <div class="duo-tint" aria-hidden="true"></div>
            ${studioLogo}
          </div>
          <div class="scene-meta" style="padding:6px 4px">
            <div class="scene-title" style="font-size:11px;color:var(--text);line-height:1.3;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${ESC(title)}</div>
            <div style="font-size:10px;color:var(--dim)">${ESC(date)}${studio ? ' · ' + ESC(studio) : ''}</div>
          </div>
        </div>`;
    };
    const cards = padded.map(renderTile).join('');
    el.innerHTML = `
      <div class="pp-scenes-grid">${cards}</div>`;
  }

  /* ── Posters rail ─────────────────────────────────────────────
   * Picks the primary + secondary poster URLs out of `data.images`
   * (matches by url substring, same heuristic as paintBgHero) and
   * paints them into the right-rail slots. Click → full-screen via
   * the existing showFullScreenImage helper. */
  function renderPosters(el, data) {
    if (!el) return;
    const imgs = data.images || [];
    const findKind = (rx) =>
      imgs.find(i => rx.test(i.kind || '')) ||
      imgs.find(i => rx.test(i.url || '')) ||
      null;
    const slots = [
      { role: 'primary',   img: findKind(/primary/i) },
      { role: 'secondary', img: findKind(/secondary/i) },
    ];
    slots.forEach(({ role, img }) => {
      const slot = el.querySelector(`.pp-poster-slot[data-poster-role="${role}"]`);
      if (!slot) return;
      slot.innerHTML = '';
      if (img && img.url) {
        slot.classList.remove('is-empty');
        const im = document.createElement('img');
        im.src = img.url;
        im.alt = '';
        im.loading = 'lazy';
        slot.appendChild(im);
        slot.onclick = () => {
          const idx = imgs.indexOf(img);
          showFullScreenImage(imgs, idx >= 0 ? idx : 0);
        };
        // Secondary slot uses the /scenes spotlight composite —
        // the ::before/::after pseudos read --tile-bg to paint
        // the tritone + airbrush layers over the same image.
        if (role === 'secondary') {
          slot.style.setProperty(
            '--tile-bg',
            `url('${img.url.replace(/'/g, "\\'")}')`,
          );
        }
      } else {
        slot.classList.add('is-empty');
        slot.onclick = null;
        const empty = document.createElement('span');
        empty.className = 'pp-poster-slot-empty';
        empty.textContent = '—';
        slot.appendChild(empty);
        if (role === 'secondary') slot.style.removeProperty('--tile-bg');
      }
    });
  }

  function renderGallery(el, data) {
    // Primary + secondary live in the right-rail poster tiles, so
    // strip them from the polaroid deck to avoid duplicates. Match
    // by `kind` first, then by url substring (same heuristic as
    // renderPosters).
    const allImgs = data.images || [];
    const isPosterRole = (i) =>
      /primary|secondary/i.test(i.kind || '') ||
      /primary|secondary/i.test(i.url || '');
    const imgs = allImgs.filter(i => !isPosterRole(i));
    const sil = data.cat_gay ? 'silhouette7.png' : 'silhouette6.png';
    if (!imgs.length) {
      el.innerHTML = `<div class="pp-empty pp-empty--silhouette pp-empty--corner"><img src="/static/img/${sil}" alt="" class="pp-empty-silhouette pp-empty-silhouette--corner"/></div>`;
      return;
    }
    // Show up to 6 polaroids stacked. Top one is interactive; the rest
    // are decorative until shuffled forward. The silhouette sits BEHIND
    // the deck (always visible, 50% opacity, centred, full-width).
    // Container starts in `is-loading` so polaroid tiles stay hidden
    // (CSS sets opacity: 0). The flag is cleared once all six images
    // have decoded — see waitForDeckLoad() below.
    const STACK_DEPTH = Math.min(6, imgs.length);
    el.innerHTML = `
      <div class="pp-gallery is-loading" data-stack-size="${STACK_DEPTH}" data-total="${imgs.length}">
        <img class="pp-gallery-bg-silhouette" src="/static/img/${sil}" alt="" aria-hidden="true">
        <div class="pp-gallery-spinner" aria-hidden="true"><i class="fa-solid fa-spinner fa-spin"></i></div>
        <span class="pp-gallery-counter"><span class="pp-gallery-cur">1</span> / ${imgs.length}</span>
        <button type="button" class="pp-gallery-nav pp-gallery-prev" title="Previous photo" aria-label="Previous photo">
          <i class="fa-solid fa-chevron-left"></i>
        </button>
        <button type="button" class="pp-gallery-nav pp-gallery-next" title="Next photo" aria-label="Next photo">
          <i class="fa-solid fa-chevron-right"></i>
        </button>
      </div>`;
    const stage = el.querySelector('.pp-gallery');
    let order = imgs.map((_, i) => i); // current top-to-back order
    // First-paint guard — the load-wait + reveal only runs once.
    // Subsequent next/prev re-paints skip the gate entirely (images
    // are already cached by the browser).
    let initialRevealDone = false;

    // Deterministic per-position rotations / offsets — looks intentional
    // rather than randomly noisy. Position 0 is the front (most prominent),
    // each step back is more rotated and slightly offset.
    const STACK_PRESETS = [
      { rot:  -3, x:  -4, y:  -2, z: 50 },
      { rot:   5, x:  18, y:   8, z: 40 },
      { rot:  -8, x: -22, y:  10, z: 30 },
      { rot:   9, x:  10, y: -14, z: 20 },
      { rot: -12, x:  -8, y:  18, z: 10 },
      { rot:  14, x:  26, y:  -6, z:  5 },
    ];

    function paint() {
      // Remove all existing tiles
      stage.querySelectorAll('.pp-gallery-tile').forEach(t => t.remove());
      // Render back-to-front so DOM order matches stacking
      const visible = order.slice(0, STACK_DEPTH).slice().reverse();
      visible.forEach((imgIdx, i) => {
        const stackPos = visible.length - 1 - i; // 0 = top
        const preset = STACK_PRESETS[stackPos] || STACK_PRESETS[STACK_PRESETS.length - 1];
        const img = imgs[imgIdx];
        const tile = document.createElement('button');
        tile.type = 'button';
        tile.className = 'pp-gallery-tile' + (stackPos === 0 ? ' is-top' : '');
        tile.style.setProperty('--pp-rot', preset.rot + 'deg');
        tile.style.setProperty('--pp-x', preset.x + 'px');
        tile.style.setProperty('--pp-y', preset.y + 'px');
        tile.style.setProperty('--pp-z', preset.z);
        tile.dataset.imgIdx = imgIdx;
        tile.innerHTML = `
          <img src="${ESC(img.url)}" alt="" loading="lazy" onerror="this.closest('.pp-gallery-tile').style.display='none'">
          ${img.kind ? `<span class="pp-gallery-kind">${ESC(img.kind)}</span>` : ''}`;
        tile.addEventListener('click', (ev) => {
          if (stackPos === 0) {
            // Top polaroid: click → view full-screen
            ev.preventDefault();
            showFullScreenImage(imgs, parseInt(tile.dataset.imgIdx, 10));
          } else {
            // Background polaroid: click → bring to front
            ev.preventDefault();
            bringToFront(imgIdx);
          }
        });
        stage.insertBefore(tile, stage.querySelector('.pp-gallery-counter'));
      });
      const cur = stage.querySelector('.pp-gallery-cur');
      if (cur && order.length) cur.textContent = String(order[0] + 1);
      // First paint: keep the deck hidden until every polaroid image
      // has decoded, so the user never sees them appear one by one.
      // Subsequent paints (next/prev shuffles) skip the gate — the
      // browser already has the images cached.
      if (!initialRevealDone) {
        const tiles = stage.querySelectorAll('.pp-gallery-tile img');
        const total = tiles.length;
        if (!total) {
          stage.classList.remove('is-loading');
          initialRevealDone = true;
          return;
        }
        let pending = total;
        const decrement = () => {
          pending -= 1;
          if (pending <= 0) {
            stage.classList.remove('is-loading');
            initialRevealDone = true;
          }
        };
        // Safety net: reveal anyway after 8s in case some images stall.
        const safety = setTimeout(() => {
          if (!initialRevealDone) {
            stage.classList.remove('is-loading');
            initialRevealDone = true;
          }
        }, 8000);
        tiles.forEach((im) => {
          if (im.complete && im.naturalWidth > 0) {
            decrement();
            return;
          }
          im.addEventListener('load',  () => { clearTimeout(safety); decrement(); }, { once: true });
          im.addEventListener('error', () => { clearTimeout(safety); decrement(); }, { once: true });
        });
      }
    }

    function nextOnce() {
      // Synchronous swap — earlier version queued a 280ms setTimeout
      // before the order shifted, which raced against the 550ms CSS
      // transition and dropped clicks when users tapped the arrow
      // quickly. Painting immediately makes the deck always reflect
      // the latest click.
      order.push(order.shift());
      paint();
    }

    function prevOnce() {
      // Pull the back-most polaroid forward to the top — visual inverse
      // of next.
      order.unshift(order.pop());
      paint();
    }

    function bringToFront(imgIdx) {
      const at = order.indexOf(imgIdx);
      if (at <= 0) return;
      order = [imgIdx, ...order.filter((i) => i !== imgIdx)];
      paint();
    }

    stage.querySelector('.pp-gallery-next').addEventListener('click', (ev) => {
      ev.preventDefault();
      nextOnce();
    });
    stage.querySelector('.pp-gallery-prev').addEventListener('click', (ev) => {
      ev.preventDefault();
      prevOnce();
    });

    paint();
  }

  // Keyboard handler is module-level so the same listener can be
  // attached / detached as the full-screen viewer opens / closes —
  // re-binding inside showFullScreenImage would leak listeners on
  // every reopen.
  let _fsKeyHandler = null;

  function showFullScreenImage(imgs, startIdx) {
    let overlay = document.getElementById('performerPopupGalleryFs');
    if (!overlay) {
      overlay = document.createElement('div');
      overlay.id = 'performerPopupGalleryFs';
      overlay.className = 'pp-fs-overlay';
      overlay.innerHTML = `
        <button type="button" class="pp-fs-close" aria-label="Close"><i class="fa-solid fa-xmark"></i></button>
        <button type="button" class="pp-fs-nav pp-fs-prev" aria-label="Previous"><i class="fa-solid fa-chevron-left"></i></button>
        <button type="button" class="pp-fs-nav pp-fs-next" aria-label="Next"><i class="fa-solid fa-chevron-right"></i></button>
        <img class="pp-fs-img" alt="">`;
      document.body.appendChild(overlay);
      overlay.addEventListener('click', (e) => {
        if (e.target === overlay) closeFs();
      });
      overlay.querySelector('.pp-fs-close').addEventListener('click', closeFs);
    }
    _galleryFullScreenIdx = startIdx;
    const img = overlay.querySelector('.pp-fs-img');
    const set = (n) => {
      _galleryFullScreenIdx = (n + imgs.length) % imgs.length;
      img.src = imgs[_galleryFullScreenIdx].url;
    };
    function closeFs() {
      overlay.classList.remove('open');
      if (_fsKeyHandler) {
        document.removeEventListener('keydown', _fsKeyHandler);
        _fsKeyHandler = null;
      }
    }
    set(startIdx);
    overlay.querySelector('.pp-fs-prev').onclick = () => set(_galleryFullScreenIdx - 1);
    overlay.querySelector('.pp-fs-next').onclick = () => set(_galleryFullScreenIdx + 1);
    // Detach any previous keyboard listener (re-opening the viewer
    // with a different image set) before binding the current one.
    if (_fsKeyHandler) document.removeEventListener('keydown', _fsKeyHandler);
    _fsKeyHandler = (e) => {
      if (!overlay.classList.contains('open')) return;
      if (e.key === 'ArrowLeft') {
        e.preventDefault();
        set(_galleryFullScreenIdx - 1);
      } else if (e.key === 'ArrowRight') {
        e.preventDefault();
        set(_galleryFullScreenIdx + 1);
      } else if (e.key === 'Escape') {
        e.preventDefault();
        closeFs();
      }
    };
    document.addEventListener('keydown', _fsKeyHandler);
    overlay.classList.add('open');
  }

  /* ── Delegated click handler ──────────────────────────────────── */

  // Attach to documentElement so it works even when this script is
  // loaded in <head> before <body> exists. Listening on document
  // would also work; using the html element keeps the chain short.
  // capture: false = phase 3 (bubbling) so per-element handlers still
  // run first if they want to.
  const _delegatedClick = (e) => {
    const el = e.target.closest && e.target.closest('[data-performer-link]');
    if (!el) return;
    // Don't trap clicks on inner buttons / links inside the linked element
    if (e.target.closest('button, a:not([data-performer-link])')) return;
    e.preventDefault();
    e.stopPropagation();
    openPerformerPopup({
      stashId: el.dataset.stashId || null,
      libraryRowId: el.dataset.libraryRowId ? parseInt(el.dataset.libraryRowId, 10) : null,
      tpdbId: el.dataset.tpdbId || null,
      name: el.dataset.name || el.textContent.trim() || null,
    });
  };
  // documentElement is always available in script-execution context
  // (unlike document.body which is null when this script is loaded
  // synchronously inside <head>).
  document.documentElement.addEventListener('click', _delegatedClick);
})();
