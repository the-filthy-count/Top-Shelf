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

  const PP_SCROLL_SELECTORS = [
    '.pp-films-prowlarr-embed .ts-prowlarr-popup-results',
    '.pp-film-list',
    '.pp-scenes-grid',
  ];

  /** Bottom fade + “Scroll” hint when films / Prowlarr / scenes overflow. */
  function wirePpScrollAffordance(root) {
    if (!root) return;
    PP_SCROLL_SELECTORS.forEach(function (sel) {
      root.querySelectorAll(sel).forEach(function (el) {
        if (el.dataset.ppScrollWired) return;
        el.dataset.ppScrollWired = '1';
        let wrap = el.parentElement;
        if (!wrap || !wrap.classList.contains('pp-scroll-wrap')) {
          wrap = document.createElement('div');
          wrap.className = 'pp-scroll-wrap';
          el.parentNode.insertBefore(wrap, el);
          wrap.appendChild(el);
        }
        if (!wrap.querySelector('.pp-scroll-hint')) {
          const hint = document.createElement('div');
          hint.className = 'pp-scroll-hint';
          hint.setAttribute('aria-hidden', 'true');
          hint.innerHTML =
            '<span class="pp-scroll-hint__label">Scroll</span>' +
            '<i class="fa-solid fa-chevron-down" aria-hidden="true"></i>';
          wrap.appendChild(hint);
        }
        function sync() {
          const can = el.scrollHeight > el.clientHeight + 6;
          const atEnd = el.scrollTop + el.clientHeight >= el.scrollHeight - 10;
          wrap.classList.toggle('pp-scroll-wrap--can-scroll', can);
          wrap.classList.toggle('pp-scroll-wrap--at-end', atEnd);
        }
        el.addEventListener('scroll', sync, { passive: true });
        if (typeof ResizeObserver !== 'undefined') {
          new ResizeObserver(sync).observe(el);
        }
        if (typeof MutationObserver !== 'undefined') {
          new MutationObserver(sync).observe(el, { childList: true, subtree: true });
        }
        sync();
      });
    });
  }

  let _galleryFullScreenIdx = -1;
  let _activeOpts = null;
  let _activeData = null;
  /** Folder name chosen for add-to-library (canonical or alias chip). */
  let _selectedFolderName = '';

  function ppFolderNameOptions(identity) {
    const id = identity || {};
    const canonical = String(id.canonical_name || '').trim();
    const out = [];
    const seen = new Set();
    const add = (value, kind) => {
      const t = String(value || '').trim();
      if (!t) return;
      const key = t.toLowerCase();
      if (seen.has(key)) return;
      seen.add(key);
      out.push({ value: t, label: t, kind });
    };
    if (canonical) add(canonical, 'canonical');
    for (const a of id.aliases || []) add(a, 'alias');
    return out;
  }

  function ppAliasesForFolder(folderName, identity) {
    const id = identity || {};
    const folderKey = String(folderName || '').trim().toLowerCase();
    const seen = new Set();
    const out = [];
    const add = (s) => {
      const t = String(s || '').trim();
      if (!t || t.toLowerCase() === folderKey) return;
      const k = t.toLowerCase();
      if (seen.has(k)) return;
      seen.add(k);
      out.push(t);
    };
    add(id.canonical_name);
    for (const a of id.aliases || []) add(a);
    return out;
  }

  function ppEnsureFolderNameSelection(identity) {
    const opts = ppFolderNameOptions(identity);
    if (!opts.length) {
      _selectedFolderName = '';
      return opts;
    }
    const cur = String(_selectedFolderName || '').trim();
    const match = opts.find((o) => o.value === cur)
      || opts.find((o) => o.value.toLowerCase() === cur.toLowerCase());
    _selectedFolderName = match ? match.value : opts[0].value;
    return opts;
  }

  function ppSelectFolderName(name, rootEl) {
    const t = String(name || '').trim();
    if (!t) return;
    _selectedFolderName = t;
    const scope = rootEl || document.getElementById('performerPopupModal');
    if (scope) {
      scope.querySelectorAll('.pp-alias-chip').forEach((chip) => {
        chip.classList.toggle('is-selected', chip.dataset.folderName === t);
      });
    }
    const addSel = document.getElementById('ppAddFolderName');
    if (addSel && addSel.options.length) addSel.value = t;
  }

  function ppRenderFolderNameChips(identity, selected) {
    const opts = ppFolderNameOptions(identity);
    if (!opts.length) return '';
    const sel = String(selected || opts[0].value).trim();
    const chips = opts.map((o) => {
      const cls = 'pp-alias-chip' + (o.value === sel ? ' is-selected' : '');
      return `<button type="button" class="${cls}" data-folder-name="${ESC(o.value)}" title="${ESC(o.label)}">${ESC(o.label)}</button>`;
    }).join('');
    return `<div class="pp-aliases pp-aliases--pickable"><div class="pp-alias-chips">${chips}</div></div>`;
  }
  let _headshotPollHandle = null;
  let _stageTimer = null;
  let _refreshReopenTimer = null;
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
            <button type="button" class="performer-popup-tool performer-popup-close"
                    title="Close" aria-label="Close">
              <i class="fa-solid fa-xmark"></i>
            </button>
          </div>
        </header>
        <div class="performer-popup-grid">
          <section class="performer-popup-cell pp-cell-bio">
            <div class="pp-loading"><span class="loader" role="status" aria-label="Loading"></span></div>
          </section>
          <section class="performer-popup-cell pp-cell-films">
            <div class="pp-loading"><span class="loader" role="status" aria-label="Loading"></span></div>
          </section>
          <section class="performer-popup-cell pp-cell-carousel">
            <div class="pp-loading"><span class="loader" role="status" aria-label="Loading"></span></div>
          </section>
          <section class="performer-popup-cell pp-cell-gallery">
            <div class="pp-loading"><span class="loader" role="status" aria-label="Loading"></span></div>
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
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && div.classList.contains('open')) closePerformerPopup();
    });
  }

  async function refreshImagesFromCurrent(triggerEl) {
    if (!_activeOpts || !window._performerPopupActiveId) return;
    const btn = triggerEl || null;
    const icon = btn && btn.querySelector('i');
    let loaderEl = null;
    if (btn) {
      btn.disabled = true;
      if (icon) icon.style.display = 'none';
      loaderEl = document.createElement('span');
      loaderEl.className = 'loader loader--btn';
      loaderEl.setAttribute('role', 'status');
      loaderEl.setAttribute('aria-label', 'Loading');
      btn.appendChild(loaderEl);
    }
    const startedFor = window._performerPopupActiveId;
    try {
      await fetch('/api/performers/enrich-headshot', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ row_id: window._performerPopupActiveId }),
      });
      if (_refreshReopenTimer) clearTimeout(_refreshReopenTimer);
      _refreshReopenTimer = setTimeout(() => {
        _refreshReopenTimer = null;
        if (!_activeOpts || window._performerPopupActiveId !== startedFor) return;
        openPerformerPopup({ ..._activeOpts, _refresh: true });
      }, 2500);
    } catch (e) {
      window.toast(e.message || 'Failed');
    } finally {
      if (btn) btn.disabled = false;
      if (loaderEl && loaderEl.parentNode) loaderEl.remove();
      if (icon) {
        icon.style.display = '';
        icon.className = 'fa-solid fa-arrows-rotate';
      }
    }
  }
  window.refreshPerformerImagesFromPopup = refreshImagesFromCurrent;
  window.refreshPerformerPopup = function () {
    if (!_activeOpts) return;
    const o = { ..._activeOpts, _refresh: true };
    if (window._performerPopupActiveId) o.libraryRowId = window._performerPopupActiveId;
    openPerformerPopup(o);
  };

  function closePerformerPopup() {
    const m = document.getElementById('performerPopupModal');
    if (m) {
      m.classList.remove('open');
      m.removeAttribute('lang');
      // Defensively clear any stale inline display set by an earlier
      // fallback close path. With performerPopupModal registered in
      // app-shell.js's ID_CLOSE_FN this should not happen anymore, but
      // keeping the cleanup symmetric with openPerformerPopup() means a
      // future regression elsewhere can't bring back the invisible-but-
      // blocking state that broke /library originally.
      if (m.style.display) m.style.display = '';
    }
    if (_headshotPollHandle) {
      clearTimeout(_headshotPollHandle);
      _headshotPollHandle = null;
    }
    if (_stageTimer) {
      clearTimeout(_stageTimer);
      _stageTimer = null;
    }
    if (_refreshReopenTimer) {
      clearTimeout(_refreshReopenTimer);
      _refreshReopenTimer = null;
    }
    _activeOpts = null;
    _activeData = null;
    window._performerPopupActiveId = null;
    // Sweep stuck sub-modals. The popup hosts modal-over-modal layers
    // (Manage Posters, Image picker, Crop, Image upload, Search, Ext
    // link, Add-to-library, Gallery fullscreen, Performer-Prowlarr) —
    // any of these still ".open" / ".is-open" after the popup closes
    // will sit invisibly above the page (z=1600+ stack) and silently
    // block every click. Clearing the class also lets the host page
    // (/library) clean up its own state via the existing close
    // functions when those exist.
    [
      'imgUploadModal', 'posterRoleModal', 'imagePickModal',
      'searchModal', 'extLinkModal', 'cropModal',
      'tsLinkSearchModal', 'tsLinkExtModal',
      'ppAddToLibraryModal', 'performerPopupGalleryFs',
    ].forEach((id) => {
      const el = document.getElementById(id);
      if (!el) return;
      if (el.classList.contains('open')) el.classList.remove('open');
      // Clear inline display set by app-shell.js's fallback close path
      // for any of these that were dismissed via Escape before being
      // registered in ID_CLOSE_FN. Once cleared, the next open via
      // classList.add('open') will display correctly.
      if (el.style.display) el.style.display = '';
    });
    // Performer-Prowlarr search uses .is-open instead of .open.
    const ppl = document.getElementById('ppPerfProwlarrModal');
    if (ppl && ppl.classList.contains('is-open')) ppl.classList.remove('is-open');
    document.dispatchEvent(new CustomEvent('performer-popup-close'));
  }
  window.closePerformerPopup = closePerformerPopup;

  // ── Add-to-library modal ────────────────────────────────────────────
  // Small in-place modal that appears on top of the performer popup
  // when the user clicks the "+" button on an unmatched performer.
  // Mirrors /discover's create flow (POST /api/metadata/create) without
  // navigating away.
  function ensureAddModal() {
    const existing = document.getElementById('ppAddToLibraryModal');
    if (existing && !existing.querySelector('#ppAddFolderName')) {
      existing.remove();
    }
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
            <label class="pp-add-label" for="ppAddFolderName">Select Alias</label>
            <select class="pp-add-select" id="ppAddFolderName">
              <option value="">—</option>
            </select>
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

  function ppPopulateAddFolderSelect(div, identity) {
    const sel = div.querySelector('#ppAddFolderName');
    if (!sel) return;
    const opts = ppEnsureFolderNameSelection(identity);
    if (!opts.length) {
      sel.innerHTML = '<option value="">(no name)</option>';
      return;
    }
    sel.innerHTML = opts.map((o) => {
      const tag = '';
      return `<option value="${ESC(o.value)}">${ESC(o.label)}${tag}</option>`;
    }).join('');
    sel.value = _selectedFolderName;
    sel.onchange = () => {
      ppSelectFolderName(sel.value, document.getElementById('performerPopupModal'));
    };
  }

  async function openAddToLibraryModal(data) {
    ensureAddModal();
    const div = document.getElementById('ppAddToLibraryModal');
    const id = (data && data.identity) || {};
    const name = id.canonical_name || (_activeOpts && _activeOpts.name) || '';
    if (!name) { window.toast('No performer name available'); return; }
    // Pick the source / id for /api/metadata/create. When javstash_id is
    // known, always create from JAVStash so tvshow.nfo gets a javstash uniqueid.
    let source = '', sid = '';
    const javId = String(id.javstash_id || '').trim();
    const stashId = String(id.stash_id || '').trim();
    const fansId = String(id.fansdb_id || '').trim();
    const tpdbId = String(id.tpdb_id || '').trim();
    if (javId) {
      source = 'JAVStash';
      sid = javId;
    } else if (stashId && !javId) {
      source = 'StashDB';
      sid = stashId;
    } else if (fansId) {
      source = 'FansDB';
      sid = fansId;
    } else if (tpdbId) {
      source = 'TPDB';
      sid = tpdbId;
    }
    ppEnsureFolderNameSelection(id);
    div._submitCtx = { source, id: sid, name, identity: id };
    ppPopulateAddFolderSelect(div, id);
    const msg = div.querySelector('#ppAddMsg');
    msg.textContent = '';
    msg.className = 'pp-add-msg';
    const submitBtn = div.querySelector('#ppAddSubmit');
    submitBtn.disabled = false;
    submitBtn.innerHTML = '<i class="fa-solid fa-plus"></i> Add';
    div.classList.add('open');
    // Load directory list (cached after first call by browser).
    try {
      const r = await fetch('/api/metadata/dirs?kind=performer', { credentials: 'same-origin' });
      const d = await r.json();
      const sel = div.querySelector('#ppAddDest');
      const raw = (d.dirs || []).slice();
      const opts = ['<option value="">Choose directory…</option>']
        .concat(raw.map(dir => `<option value="${ESC(dir.path)}">${ESC(dir.label)}</option>`))
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
  // Exposed so app-shell.js's universal Escape handler can close this
  // via ID_CLOSE_FN. Otherwise the fallback sets inline display:none on
  // the overlay, which sticks across reopens (same bug pattern as the
  // performer popup itself).
  window.closeAddToLibraryModal = closeAddToLibraryModal;

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
    const folderSel = div.querySelector('#ppAddFolderName');
    const folderName = (folderSel && folderSel.value || _selectedFolderName || ctx.name || '').trim();
    if (!folderName) {
      msg.textContent = 'Choose a folder name.';
      msg.className = 'pp-add-msg pp-add-msg--err';
      return;
    }
    if (!ctx.name && !(ctx.identity && ctx.identity.canonical_name)) {
      msg.textContent = 'Missing performer name.';
      msg.className = 'pp-add-msg pp-add-msg--err';
      return;
    }
    if (!ctx.id) {
      msg.textContent = 'No linked database id for this performer (need StashDB, FansDB, TPDB, or JAVStash).';
      msg.className = 'pp-add-msg pp-add-msg--err';
      return;
    }
    const submitBtn = div.querySelector('#ppAddSubmit');
    submitBtn.disabled = true;
    submitBtn.innerHTML = (typeof loaderHtml === 'function' ? loaderHtml('loader--btn') : '<span class="loader loader--btn" role="status" aria-label="Loading"></span>') + ' Adding…';
    try {
      const r = await fetch('/api/metadata/create', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          type:        'performer',
          source:      ctx.source || '',
          id:          ctx.id || '',
          javstash_id: (ctx.identity && ctx.identity.javstash_id) || '',
          name:        (ctx.identity && ctx.identity.canonical_name) || ctx.name,
          folder_name: folderName,
          aliases:     ppAliasesForFolder(folderName, ctx.identity || {}),
          dest_dir:    dest,
        }),
      });
      const d = await r.json();
      if (!r.ok || d.success === false) {
        throw new Error(d.error || ('HTTP ' + r.status));
      }
      // Notify hosting pages (e.g. /library) so the new row appears
      // without a manual refresh. /library's listener wipes the cached
      // performer list and re-renders the active tab.
      try {
        document.dispatchEvent(new CustomEvent('library-row-added', {
          detail: { kind: 'performer', rowId: d.row_id || null, name: d.name || '' },
        }));
      } catch (_) { /* noop */ }
      // Re-open by row id when indexed — required when folder_name is an
      // alias (popup opened with canonical name / stash id won't resolve).
      const reopen = { _refresh: true };
      if (d.row_id) {
        reopen.libraryRowId = d.row_id;
      } else if (_activeOpts) {
        Object.assign(reopen, _activeOpts);
        if (d.name) reopen.name = d.name;
      }
      openPerformerPopup(reopen);
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
    // Clear any stale inline display set by a fallback close path
    // (e.g. app-shell.js's generic Escape closer) — without this the
    // class-based `.open { display: flex }` is overridden by the
    // inline rule and the popup opens invisibly while still
    // intercepting pointer events on its overlay.
    if (m.style.display) m.style.display = '';
    m.classList.add('open');
    m.setAttribute('lang', 'en');
    // Warm the shared wanted-keys cache so each tile's eye paints its
    // `.is-wanted` state on first render.
    if (typeof window.tsLoadWantedKeys === 'function') {
      window.tsLoadWantedKeys();
    }
    _activeOpts = { libraryRowId: opts.libraryRowId, stashId: opts.stashId, tpdbId: opts.tpdbId, name: opts.name, standalone: !!opts.standalone };
    if (!opts._refresh) _selectedFolderName = '';

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
    if (opts.standalone) params.set('standalone', '1');

    let data;
    try {
      const r = await fetch('/api/performer/popup?' + params.toString(), { credentials: 'same-origin' });
      data = await r.json();
      if (!r.ok || data.error) throw new Error(data.error || 'Load failed');
    } catch (e) {
      clearStageBanner(m);
      m.setAttribute('lang', 'en');
      m.querySelector('.pp-cell-bio').innerHTML = `<div class="pp-error">${ESC(e.message || 'Error')}</div>`;
      return;
    }

    // Soft refresh: the popup payload is server-cache-busted via
    // ?refresh=1, but image URLs are stable (`/api/favourites/
    // performer-thumb?row_id=N`, etc.) so the browser would happily
    // serve the prior bytes from its own cache. Append a one-shot
    // query param to every image URL so the IMG re-fetches when the
    // user just changed the headshot / poster from the manage-posters
    // modal.
    if (opts._refresh) {
      const buster = '__t=' + Date.now();
      const bust = (u) => (u && typeof u === 'string')
        ? u + (u.includes('?') ? '&' : '?') + buster
        : u;
      if (data.headshot_url) data.headshot_url = bust(data.headshot_url);
      if (Array.isArray(data.images)) {
        data.images = data.images.map((im) => {
          if (im && im.url) return { ...im, url: bust(im.url) };
          return im;
        });
      }
    }

    // Browser translate: Japanese subtree when JAVStash resolved the
    // performer and StashDB/FansDB did not (mixed profiles stay English).
    const src = data.sources || {};
    const wantJa = !!(src.javstash && !src.stashdb && !src.fansdb);
    m.setAttribute('lang', wantJa ? 'ja' : 'en');

    // Update active id for cross-modal coordination
    window._performerPopupActiveId = (data.library_status && data.library_status.row_id) || null;

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

    // Performers not yet in the library don't have user-chosen
    // primary/secondary posters. Pick two random images and tag them
    // so both renderPosters (right rail) and renderGallery (polaroid
    // deck) treat them consistently — the deck filter excludes anything
    // tagged primary/secondary, so this also keeps the same image from
    // appearing twice. Run before paintBgHero so the background can
    // also use the secondary pick instead of falling back to headshot.
    pickRandomPostersIfNeeded(data);
    paintBgHero(data);

    renderHeader(m.querySelector('#performerPopupHeader'), data);
    renderBio(m.querySelector('.pp-cell-bio'), data);
    applyBioCellFanart(m.querySelector('.pp-cell-bio'),
      (data && data.library_status && data.library_status.row_id) || null);
    renderFilmsPanel(m.querySelector('.pp-cell-films'), data);
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
    m.querySelectorAll('.pp-action-btn[data-action], .pp-name-action[data-action], .pp-cta-btn[data-action], .pp-alias-edit[data-action], .pp-bio-search-btn[data-action], .pp-alias-chip[data-folder-name]').forEach((btn) => {
      btn.addEventListener('click', async (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        if (btn.classList.contains('pp-alias-chip') && btn.dataset.folderName) {
          ppSelectFolderName(btn.dataset.folderName, m);
          return;
        }
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
          window.toast(e.message || 'Failed');
        }
      });
    });

    // Group-badge click → open the members modal so the user can drill
    // into individual members and edit their DB links separately. Re-
    // bound on every render since renderHeader rewrites the name HTML.
    m.querySelectorAll('[data-pp-open-members]').forEach((btn) => {
      btn.addEventListener('click', (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        const lib = (_activeData && _activeData.library_status) || {};
        const ident = (_activeData && _activeData.identity) || {};
        if (!lib.row_id) return;
        if (typeof window.openPerformerGroupMembersModal === 'function') {
          window.openPerformerGroupMembersModal(
            lib.row_id,
            ident.canonical_name || '',
            ident.group_ids || {},
          );
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

    // Headshot click → manage-posters modal in place (no /library redirect).
    const headshotWrap = m.querySelector('.pp-headshot-wrap');
    if (headshotWrap && data.library_status && data.library_status.in_library) {
      const rid = data.library_status.row_id;
      const perfName = (data.identity && data.identity.canonical_name) || '';
      headshotWrap.classList.add('is-clickable');
      headshotWrap.title = 'Manage images';
      headshotWrap.onclick = async (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        try {
          if (typeof window.ensurePosterRolePicker === 'function') {
            await window.ensurePosterRolePicker();
          }
          if (typeof window.openPosterRolePicker === 'function') {
            await window.openPosterRolePicker(rid, { title: perfName });
            return;
          }
        } catch (err) {
          console.error('Manage posters:', err);
        }
        window.location.href = `/library?focus=${encodeURIComponent(rid)}&manage=images`;
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
        if (!r.ok) { window.toast(d.error || 'Upload failed'); return; }
        if (window._performerPopupActiveId === rowId && _activeOpts) {
          openPerformerPopup({ ..._activeOpts, _refresh: true });
        }
      } catch (e) { window.toast(e.message || 'Failed'); }
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
      if (!r.ok) { const d = await r.json().catch(() => ({})); window.toast(d.error || 'Save failed'); return; }
      if (window._performerPopupActiveId === rowId && _activeOpts) {
        openPerformerPopup({ ..._activeOpts, _refresh: true });
      }
    } catch (e) { window.toast(e.message || 'Failed'); }
  }

  async function removeExtLink(rowId, site) {
    if (!rowId || !site) return;
    if (!window.confirm(`Remove ${site} link?`)) return;
    try {
      await fetch('/api/favourites/clear-ext-link', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          row_id: rowId,
          site: String(site || '').toLowerCase().replace(/\s+/g, ''),
        }),
      });
      if (window._performerPopupActiveId === rowId && _activeOpts) {
        openPerformerPopup({ ..._activeOpts, _refresh: true, libraryRowId: rowId });
      }
    } catch (e) { window.toast(e.message || 'Failed'); }
  }

  function editExtLink(btn) {
    const rowId = parseInt(btn.dataset.ppRow, 10);
    const site = btn.dataset.ppSite;
    const action = btn.dataset.ppEditAction;
    if (!rowId || !site) return;
    const name = btn.dataset.ppName || '';
    // /library has richer modals (year, gender filters, source picker for
    // movies/studios) — use them when on /library. Everywhere else, the
    // universal performer link-search modal handles change/remove.
    if (action === 'db' && typeof window.openSearchForRow === 'function') {
      window.openSearchForRow(rowId, site);
    } else if (action === 'ext' && typeof window.openExtLinkModal === 'function') {
      window.openExtLinkModal(rowId, site, name);
    } else if (typeof window.openPerformerLinkSearch === 'function') {
      window.openPerformerLinkSearch({ rowId, site, name, kind: action });
    } else {
      const url = window.prompt(`Set ${site} URL:`, '');
      if (!url) return;
      fetch('/api/favourites/ext-link', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          row_id: rowId,
          site: String(site || '').toLowerCase().replace(/\s+/g, ''),
          url: url.trim(),
        }),
      }).then((r) => {
        if (!r.ok) { r.json().then(d => window.toast(d.error || 'Failed')); return; }
        window.toast('Link saved');
        if (window._performerPopupActiveId === rowId && _activeOpts) {
          openPerformerPopup({ ..._activeOpts, _refresh: true, libraryRowId: rowId });
        }
      }).catch((e) => window.toast(e.message || 'Failed'));
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
        <div class="pp-skel-line pp-skel-line--lg" style="width:55%;margin-bottom:12px"></div>
        <div class="pp-skel-thumbs" style="grid-template-columns:repeat(2,minmax(0,1fr));gap:8px">
          ${Array.from({ length: 4 }).map(() => '<div class="pp-skel-thumb"></div>').join('')}
        </div>
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

  //: Local background.jpg / fanart.jpg / backdrop.jpg backdrop for the
  //: top-left bio cell. Probes /api/favourites/folder-fanart with a
  //: throwaway Image so a 404 doesn't show up as a console error and
  //: doesn't trigger the CSS ::before. Mirrors `applyCellFanart` in
  //: studio-popup.js — same CSS rules in app-shell.css drive both.
  function applyBioCellFanart(cell, rowId) {
    if (!cell) return;
    if (!rowId) {
      cell.removeAttribute('data-has-fanart');
      cell.style.removeProperty('--ts-cell-fanart');
      return;
    }
    const url = '/api/favourites/folder-fanart?row_id=' + encodeURIComponent(rowId);
    const probe = new Image();
    probe.onload = () => {
      cell.style.setProperty('--ts-cell-fanart', `url("${url}")`);
      cell.setAttribute('data-has-fanart', '1');
    };
    probe.onerror = () => {
      cell.removeAttribute('data-has-fanart');
      cell.style.removeProperty('--ts-cell-fanart');
    };
    probe.src = url;
  }

  function paintBgHero(data) {
    const bg = document.getElementById('performerPopupBg');
    if (!bg) return;
    // Prefer secondary poster (full-body / promo) → primary → headshot
    const imgs = data.images || [];
    const sec = imgs.find(i => /secondary/.test(i.kind || '') || /secondary/.test(i.url || '')) || null;
    const pri = imgs.find(i => /primary/.test(i.kind || '') || /primary/.test(i.url || '')) || null;
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
            slot.innerHTML = `<img class="pp-headshot" src="${ESC(d.headshot_url)}&v=${Date.now()}" alt="" loading="lazy" onerror="this.parentElement.innerHTML='<div class=pp-headshot-fallback><i class=fa-solid fa-person></i></div>'">`;
          }
          paintBgHero(d);
          return;
        }
      } catch (e) { /* swallow */ }
      startHeadshotPoll(rowId, attempt + 1);
    }, 4000);
  }

  // Social link row: rendered under the bio (not in the header). Uses
  // Google's s2 favicon endpoint so each pill gets the actual site
  // glyph — same approach the /scenes detail panel takes via
  // `renderLinks()` in scenes-common.js.
  function buildSocialLinksHtml(data) {
    const items = Array.isArray(data && data.social_links) ? data.social_links : [];
    if (!items.length) return '';
    const pills = items.map((s) => {
      const url = (s && s.url) || '';
      if (!url) return '';
      const label = s.label || '';
      let host = '';
      try { host = new URL(url).hostname.replace(/^www\./, ''); } catch { host = ''; }
      const iconHtml = host
        ? `<img src="https://www.google.com/s2/favicons?domain=${ESC(host)}&sz=64" alt="${ESC(label || host)}" onerror="this.replaceWith(Object.assign(document.createElement('i'),{className:'fa-solid fa-globe'}))">`
        : `<i class="fa-solid fa-globe"></i>`;
      return `<a class="pp-social-pill" href="${ESC(url)}" target="_blank" rel="noopener noreferrer" title="${ESC(label || url)}" aria-label="${ESC(label || url)}">${iconHtml}</a>`;
    }).filter(Boolean).join('');
    if (!pills) return '';
    return `<div class="pp-social-grid">${pills}</div>`;
  }

  // Site-logo lookup for ext-link pills. Falls back to a text label
  // when no logo is available for the source.
  const SOURCE_LOGOS = {
    stashdb:   '/static/logos/stashdb.webp',
    javstash:  '/static/logos/javstash.webp',
    fansdb:    '/static/logos/fansdb.webp',
    tpdb:      '/static/logos/tpdb.webp',
    tmdb:      '/static/logos/tmdb.webp',
    iafd:      '/static/logos/iafd.webp',
    freeones:  '/static/logos/freeones.webp',
    babepedia: '/static/logos/babepedia.webp',
    coomer:    '/static/logos/coomer.webp',
    javdatabase: '/static/logos/javdatabase.webp',
  };
  function srcLogoHtml(key, label) {
    const url = SOURCE_LOGOS[key];
    if (!url) return ESC(label);
    return `<img class="pp-src-logo" src="${ESC(url)}" alt="${ESC(label)}" title="${ESC(label)}" onerror="this.replaceWith(document.createTextNode('${ESC(label)}'))" loading="lazy">`;
  }

  /* ── Cell renderers ───────────────────────────────────────────── */

  // Shared pill builder — rendered both in the header (centered row
  // under the name) and (historically) in the bio cell. Now lives in
  // the header only.
  function buildProfilePillsHtml(data) {
    const id = data.identity || {};
    const lib = data.library_status || {};
    const links = data.ext_links || {};
    // `site` matches the API source key used by /api/metadata/search
    // (TPDB / StashDB / FansDB) and /api/favourites/clear-ext-link
    // (lowercase iafd / freeones / etc.). We pass it as data-pp-site so
    // both the universal link-modals.js and /library's openSearchForRow
    // can filter results without a label-vs-key mismatch.
    const PILLS = [
      { key: 'tpdb',      label: 'ThePornDB',  site: 'TPDB',     kind: 'db',  url: links.tpdb && links.tpdb.url },
      { key: 'stashdb',   label: 'StashDB',    site: 'StashDB',  kind: 'db',  url: links.stashdb && links.stashdb.url },
      { key: 'fansdb',    label: 'FansDB',     site: 'FansDB',   kind: 'db',  url: links.fansdb && links.fansdb.url },
      { key: 'javstash',  label: 'JAVStash',   site: 'JAVStash', kind: 'db',  url: links.javstash && links.javstash.url },
      { key: 'tmdb',      label: 'TMDB',       site: 'TMDB',     kind: 'ext', url: links.tmdb && links.tmdb.url },
      { key: 'iafd',      label: 'IAFD',       site: 'IAFD',     kind: 'ext', url: links.iafd && links.iafd.url },
      { key: 'freeones',  label: 'Freeones',   site: 'Freeones', kind: 'ext', url: links.freeones && links.freeones.url },
      { key: 'babepedia', label: 'Babepedia',  site: 'Babepedia',kind: 'ext', url: links.babepedia && links.babepedia.url },
      { key: 'coomer',    label: 'Coomer',     site: 'Coomer',   kind: 'ext', url: links.coomer && links.coomer.url },
      { key: 'javdatabase', label: 'JAV Database', site: 'JAV Database', kind: 'ext', url: links.javdatabase && links.javdatabase.url },
    ];
    const rowId = lib.in_library ? lib.row_id : null;
    return PILLS.map((p) => {
      const logo = srcLogoHtml(p.key, p.label);
      const siteKey = p.site || p.label;
      if (p.url) {
        const editAction = rowId && p.kind === 'db'
          ? `data-pp-edit-action="db" data-pp-site="${ESC(siteKey)}" data-pp-row="${ESC(rowId)}"`
          : (rowId && p.kind === 'ext'
              ? `data-pp-edit-action="ext" data-pp-site="${ESC(siteKey)}" data-pp-row="${ESC(rowId)}" data-pp-name="${ESC(id.canonical_name)}"`
              : '');
        const removeAction = rowId
          ? `data-pp-remove="1" data-pp-site="${ESC(siteKey)}" data-pp-row="${ESC(rowId)}"`
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
        ? `data-pp-edit-action="${p.kind}" data-pp-site="${ESC(siteKey)}" data-pp-row="${ESC(rowId)}" data-pp-name="${ESC(id.canonical_name)}"`
        : '';
      return `<button type="button" class="pp-profile-pill is-missing" ${linkAction} title="${ESC(p.label)} — click to search and link">
        ${logo}
        <i class="fa-solid fa-magnifying-glass" style="font-size:9px;opacity:0.7"></i>
      </button>`;
    }).join('');
  }

  //: Compute age from a YYYY-MM-DD birthdate string. Returns null if
  //: missing or unparseable. Same heuristic as /library's tile overlay.
  function _ppComputeAge(birthdate) {
    const bd = (birthdate || '').trim();
    const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(bd);
    if (!m) return null;
    const y = +m[1], mo = +m[2], d = +m[3];
    const now = new Date();
    let age = now.getFullYear() - y;
    const past = (now.getMonth() + 1 > mo) || (now.getMonth() + 1 === mo && now.getDate() >= d);
    if (!past) age -= 1;
    if (age < 0 || age > 130) return null;
    return age;
  }

  function renderHeader(el, data) {
    if (!el) return;
    const id = data.identity || {};
    const bio = data.bio || {};
    const lib = data.library_status || {};
    const flag = (bio.country && window.countryFlagHtml)
      ? window.countryFlagHtml(bio.country, 'pp-flag')
      : '';
    //: Age coin — white circle with red outline, sits between the
    //: country flag and the favourite / lock icons. Same visual
    //: language as the hover-revealed age icon on /library tiles, so
    //: the user reads "age" the same way across both surfaces.
    const ppAge = _ppComputeAge(bio.birthdate);
    const ageHtml = (ppAge != null)
      ? `<span class="pp-name-age" aria-label="Age ${ppAge}" title="Age ${ppAge}">${ppAge}</span>`
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
          <i class="fa-regular fa-bookmark"></i>
        </button>`;
    }
    // Group glyph sits next to the name when this row was promoted to
    // a group folder (is_group=1). Title attribute calls out the
    // member count so the reader doesn't need to open the hamburger.
    const isGroup = !!lib.is_group;
    // Groups don't carry their own DB identity — the per-source pill
    // row would always be "missing" because matches live on individual
    // members. Skip it entirely; the group badge → members modal owns
    // the per-member DB link workflow.
    const pillsHtml = isGroup ? '' : buildProfilePillsHtml(data);
    let groupBadge = '';
    if (isGroup) {
      // Collapse group_ids by name so the count reflects logical
      // members, not raw entries — a single person with TPDB + StashDB
      // ids is one tile in the modal, so it should show "1" here.
      const gids = id.group_ids || {};
      const memberCount = deriveGroupMembers(gids).length;
      const tip = memberCount
        ? `Open ${memberCount} member${memberCount === 1 ? '' : 's'}`
        : 'Group folder · no members yet — click to add';
      // Clickable so the user can drill into individual members. Even
      // an empty group surfaces the modal (which shows "no members yet
      // — add one" with the picker as the only tile) so the entry
      // point stays consistent regardless of group state.
      groupBadge = `<button type="button" class="pp-name-group" data-pp-open-members="1"
              aria-label="${ESC(tip)}" title="${ESC(tip)}">
        <i class="fa-solid fa-users"></i>
        <span class="pp-name-group-count">${memberCount}</span>
      </button>`;
    }
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
      <span class="pp-lib-entity-actions"></span>
      <span class="pp-name-text">${ESC(id.canonical_name || 'Unknown')}</span>
      ${groupBadge}
      ${flag}
      ${ageHtml}
      ${nameIcons}`;
    const actionsEl = nameEl.querySelector('.pp-lib-entity-actions');
    if (actionsEl) {
      actionsEl.innerHTML = '';
      if (lib.in_library && window.LibEntityActions && lib.row_id) {
        LibEntityActions.mountMenuButton(actionsEl, {
          id: Number(lib.row_id),
          kind: 'performer',
          name: id.canonical_name || 'this performer',
          viceId: '',
          canChangeDirectory: Number(lib.row_id) > 0,
          isGroup: !!lib.is_group,
        }, (action) => {
          // Rename leaves the row in place; refresh so the new name
          // and aliases reflect immediately. Everything else (delete,
          // remove, move) tears the entity out from under us.
          if (action === 'rename' && typeof window.refreshPerformerPopup === 'function') {
            window.refreshPerformerPopup();
          } else {
            closePerformerPopup();
          }
        });
      }
    }
    let pillsEl = el.querySelector('.pp-profiles-grid--header');
    if (isGroup) {
      // Pull the empty pill row from the DOM so the header tightens up
      // without a dead band where the pills used to live.
      if (pillsEl) pillsEl.remove();
    } else {
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

    // The group-members modal owns member visibility now — drop any
    // legacy chip row left behind by an earlier render.
    const legacyChips = el.querySelector('.pp-group-links');
    if (legacyChips) legacyChips.remove();
  }

  /** Collapse group_ids_json entries into one logical row per member.
   * Entries are keyed by name (case-insensitive); legacy bare-ID rows
   * have no name so each becomes its own "orphan" member. The result
   * is what the members modal renders.
   *
   * Each member ends up with:
   *   { name, image, ids: { tpdb, stashdb, fansdb, javstash } }
   * where ids[src] is the FIRST id seen for that source under the
   * member name. Multi-id-per-source is rare and the popup only needs
   * one to resolve a profile, so we don't keep extras.
   */
  function deriveGroupMembers(groupIds) {
    const byKey = new Map();
    const orphans = [];
    ['tpdb', 'stashdb', 'fansdb', 'javstash'].forEach((src) => {
      (groupIds[src] || []).forEach((entry) => {
        const isObj = entry && typeof entry === 'object';
        const xid   = isObj ? String(entry.id || '')    : String(entry || '');
        const nm    = isObj ? String(entry.name || '')  : '';
        const img   = isObj ? String(entry.image || '') : '';
        if (!xid) return;
        if (nm) {
          const k = nm.trim().toLowerCase();
          let m = byKey.get(k);
          if (!m) {
            m = { name: nm, image: img, ids: { tpdb: '', stashdb: '', fansdb: '', javstash: '' } };
            byKey.set(k, m);
          }
          if (!m.ids[src]) m.ids[src] = xid;
          if (!m.image && img) m.image = img;
        } else {
          // Orphan — surface as its own member tile so the user can
          // still drill into it.
          orphans.push({
            name: '', image: '',
            ids: { tpdb: '', stashdb: '', fansdb: '', javstash: '' },
            _source: src, _id: xid,
          });
          orphans[orphans.length - 1].ids[src] = xid;
        }
      });
    });
    return [...byKey.values(), ...orphans];
  }

  function renderBio(el, data) {
    const id = data.identity || {};
    const bio = data.bio || {};
    const lib = data.library_status || {};
    const links = data.ext_links || {};

    const headshot = data.headshot_url || '';
    // Name + flag + fav/lock icons now live in the popup header (built
    // by renderHeader). Bio cell starts straight at the headshot.

    // Auto-span: short values pack 1 cell, medium spill into 2, long
    // values claim the whole row. Combined with `grid-auto-flow: dense`
    // on .pp-stats, short fields backfill any holes left by spanning
    // ones — so "Height" + "Stats" can sit beside "Active" instead of
    // each forcing its own line. Caller can still pin via opts.span.
    const stat = (label, value, opts = {}) => {
      if (!value && value !== 0) return '';
      const v = String(value);
      const span = opts.span
        ? opts.span
        : (v.length > 38 ? 3 : (v.length > 18 ? 2 : 1));
      const spanCls = span >= 3 ? ' span-3' : (span === 2 ? ' span-2' : '');
      return `<div class="pp-stat-row${spanCls}"><span class="pp-stat-label">${ESC(label)}</span><span class="pp-stat-value">${ESC(value)}</span></div>`;
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
    const usePickableAliases = !lib.in_library && (id.canonical_name || (id.aliases && id.aliases.length));
    let aliasHtml = '';
    if (lib.in_library) {
      aliasHtml = (id.aliases && id.aliases.length)
        ? `<div class="pp-aliases"><span>${id.aliases.map(ESC).join(', ')}</span>${aliasEditBtn}</div>`
        : `<div class="pp-aliases pp-aliases--empty"><span style="opacity:0.5">no aliases yet</span>${aliasEditBtn}</div>`;
    } else if (id.canonical_name || (id.aliases && id.aliases.length)) {
      ppEnsureFolderNameSelection(id);
      aliasHtml = ppRenderFolderNameChips(id, _selectedFolderName);
    }

    const ctaHtml = '';

    const isDeceased = !!(bio.death_date && String(bio.death_date).trim());
    // Switched from `<wa-icon name="skull-crossbones">` to a plain
    // FontAwesome glyph: WebAwesome's bundle doesn't ship the
    // `skull-crossbones` icon by default and the element rendered
    // empty. FontAwesome solid (`fa-skull-crossbones`) is already
    // loaded site-wide via `font-awesome/css/all.min.css`.
    const skullHtml = isDeceased
      ? `<span class="pp-headshot-skull" aria-label="Deceased" title="Deceased"><i class="fa-solid fa-skull-crossbones"></i></span>`
      : '';
    el.innerHTML = `
      <div class="pp-bio-layout${usePickableAliases ? ' pp-bio-layout--pick-name' : ''}">
        <div class="pp-headshot-wrap${isDeceased ? ' is-deceased' : ''}">
          ${headshot
            ? `<img class="pp-headshot" src="${ESC(headshot)}" alt="${ESC(id.canonical_name)}" loading="lazy" onerror="this.outerHTML='<div class=&quot;pp-headshot-fallback&quot;><i class=&quot;fa-solid fa-person&quot;></i></div>'">`
            : `<div class="pp-headshot-fallback"><i class="fa-solid fa-person"></i></div>`}
          ${skullHtml}
        </div>
        <div class="pp-bio-text">
          ${aliasHtml}
          <div class="pp-stats">
            ${stat('Gender', bio.gender)}
            ${stat('Born', bio.birthdate)}
            ${stat('Died', bio.death_date)}
            ${stat('Country', bio.country)}
            ${stat('Ethnicity', bio.ethnicity)}
            ${stat('Hair', bio.hair_color)}
            ${stat('Eyes', bio.eye_color)}
            ${stat('Height', bio.height)}
            ${stat('Stats', bio.measurements)}
            ${stat('Active', bio.career_start_year)}
            ${stat('Tattoos', bio.tattoos)}
            ${stat('Piercings', bio.piercings)}
          </div>
          ${bio.biography ? `<div class="pp-biography">${ESC(bio.biography)}</div>` : ''}
          ${buildSocialLinksHtml(data)}
          ${ctaHtml}
          ${data.is_stub ? '<div class="pp-stub-note">Limited info — not yet matched in any database.</div>' : ''}
        </div>
      </div>`;
  }

  function _ppFilmsPosterUrls(data) {
    const rowId = (data && data.library_status && data.library_status.row_id) || null;
    return {
      posterUrl: rowId ? `/api/favourites/performer-thumb?prefer=secondary&row_id=${rowId}` : '',
      headshotUrl: (data && data.headshot_url) || '',
    };
  }

  async function _fetchPerformerProwlarrReleases(name) {
    const trimmed = (name || '').trim();
    if (!trimmed) return { error: 'No performer name', releases: [], totalBeforeDedupe: 0 };
    const params = new URLSearchParams();
    params.append('q', trimmed);
    if (/\s/.test(trimmed)) params.append('q', trimmed.replace(/\s+/g, '.'));
    const r = await fetch('/api/prowlarr/search?' + params.toString(), { credentials: 'same-origin' });
    const d = await r.json();
    if (d && d.error) return { error: d.error, releases: [], totalBeforeDedupe: 0 };
    const releases = (d && d.results) || [];
    const seen = new Set();
    const unique = [];
    for (const rel of releases) {
      const norm = _ppNormReleaseTitle(rel.title);
      if (!norm || seen.has(norm)) continue;
      seen.add(norm);
      unique.push(rel);
    }
    return { releases: unique, totalBeforeDedupe: releases.length };
  }

  function _setFilmsPanelTab(panel, tab) {
    panel.querySelectorAll('[data-pp-films-tab]').forEach((btn) => {
      const on = btn.dataset.ppFilmsTab === tab;
      btn.classList.toggle('active', on);
      btn.setAttribute('aria-selected', on ? 'true' : 'false');
    });
    panel.querySelectorAll('[data-pp-films-pane]').forEach((pane) => {
      const on = pane.dataset.ppFilmsPane === tab;
      pane.hidden = !on;
      pane.classList.toggle('is-active', on);
    });
  }

  function renderFilmsPanel(el, data) {
    const name = ((data && data.identity && data.identity.canonical_name) || '').trim();
    el.innerHTML = `
      <div class="pp-films-panel">
        <div class="pp-films-panel-head">
          <div class="ts-seg ts-seg--2 pp-films-source-toggle" role="tablist" aria-label="Release sources">
            <button type="button" class="active" data-pp-films-tab="prowlarr" role="tab" aria-selected="true" title="Prowlarr" aria-label="Prowlarr">
              <img src="/static/logos/prowlarr2.webp" alt="Prowlarr" class="pp-films-tab-logo" onerror="this.style.display='none'">
            </button>
            <button type="button" data-pp-films-tab="iafd" role="tab" aria-selected="false" title="IAFD filmography" aria-label="IAFD">
              <img src="/static/logos/iafd.webp" alt="IAFD" class="pp-films-tab-logo" onerror="this.style.display='none'">
            </button>
          </div>
        </div>
        <div class="pp-films-panel-body">
          <div class="pp-films-tab pp-films-tab--prowlarr is-active" data-pp-films-pane="prowlarr" role="tabpanel">
            <div class="ts-prowlarr-embed pp-films-prowlarr-embed">
              <div class="ts-prowlarr-embed-status ts-prowlarr-popup-status">${name ? 'Searching Prowlarr…' : 'No performer name to search.'}</div>
              <div class="ts-prowlarr-embed-filters ts-prowlarr-popup-filters ts-prowlarr-filter-bar" hidden></div>
              <div class="ts-prowlarr-embed-results ts-prowlarr-popup-results"></div>
            </div>
          </div>
          <div class="pp-films-tab pp-films-tab--iafd" data-pp-films-pane="iafd" role="tabpanel" hidden>
            <div class="pp-films-iafd-inner"></div>
          </div>
        </div>
      </div>`;
    const panel = el.querySelector('.pp-films-panel');
    const iafdInner = el.querySelector('.pp-films-iafd-inner');
    renderFilmography(iafdInner, data);
    panel.querySelectorAll('[data-pp-films-tab]').forEach((btn) => {
      btn.addEventListener('click', () => _setFilmsPanelTab(panel, btn.dataset.ppFilmsTab));
    });
    const embedHost = el.querySelector('.ts-prowlarr-embed');
    if (embedHost && typeof window.unmountEmbeddedProwlarrSearch === 'function') {
      window.unmountEmbeddedProwlarrSearch(embedHost);
    }
    if (name && embedHost && typeof window.mountEmbeddedProwlarrSearch === 'function') {
      void window.mountEmbeddedProwlarrSearch(embedHost, {
        title: name,
        dotVariant: true,
        dedupe: true,
        kind: 'scene',
      }).then(function () {
        wirePpScrollAffordance(el);
      });
    }
    wirePpScrollAffordance(el);
  }

  function renderFilmography(el, data) {
    const films = data.filmography || [];
    if (!films.length) {
      el.innerHTML = `<div class="pp-empty pp-empty--silhouette"><img src="/static/img/silhouette5.webp" alt="" class="pp-empty-silhouette" loading="lazy"/>${data.iafd_search_url ? `<a href="${ESC(data.iafd_search_url)}" target="_blank" rel="noopener" class="pp-empty-link">Search IAFD →</a>` : ''}</div>`;
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
    wirePpScrollAffordance(el);
  }

  function dedupePerformerPopupScenes(list) {
    // Cross-source dedupe: the same scene returned by TPDB, StashDB,
    // FansDB and JAVStash has *different* IDs in each, so a single
    // id-keyed pass misses them. Build up to three keys per row —
    // `id:{source}:{id}` (same-source dupes), `td:{normTitle}|{date}`
    // (same performer + title + date is the same scene even when
    // studio strings differ, e.g. "NUBILES" vs "NUBILES.NET"), and
    // `th:{thumbUrl}` (identical CDN URL is conclusive). A row is
    // a duplicate if ANY of its keys was seen on a previous row.
    const seen = new Set();
    const out = [];
    const norm = (v) => String(v || '').toLowerCase()
      // Drop common URL TLD-ish suffixes embedded in studio names
      // ("NUBILES.NET" → "nubiles", "BangBros.com" → "bangbros").
      .replace(/\.(net|com|org|cc|tv|xxx|co|io)\b/g, '')
      // Collapse punctuation / whitespace to single spaces.
      .replace(/[^a-z0-9]+/g, ' ')
      .trim();
    for (const s of list || []) {
      if (!s) continue;
      const keys = [];
      const id = String(s.id || '').trim();
      const srcKey = String(s.source || '').toLowerCase();
      if (id) {
        keys.push('id:' + srcKey + ':' + id);
      }
      const date  = String(s.date || '').trim().slice(0, 10);
      const title = norm(s.title);
      if (date && title) {
        keys.push('td:' + title + '|' + date);
      } else if (title) {
        // Date missing on one side — fall back to title+studio.
        keys.push('ts:' + title + '|' + norm(s.studio || s.site_name));
      }
      const thumb = String(s.thumb || s.image || '').trim();
      if (thumb && /^https?:\/\//i.test(thumb)) {
        keys.push('th:' + thumb);
      }
      if (!keys.length) {
        // Pathological row with no id/title/thumb — keep it but skip
        // dedupe to avoid collapsing distinct empty rows together.
        out.push(s);
        continue;
      }
      if (keys.some(k => seen.has(k))) continue;
      keys.forEach(k => seen.add(k));
      out.push(s);
    }
    return out;
  }

  async function renderCarousel(el, data) {
    // Hits /api/scenes/recent for each distinct (source, id) pair the
    // popup identity carries. For solo performers that's the primary
    // tpdb/stash/fans/jav id; for group folders ("Fox Twins") we also
    // fan out across every linked id in `identity.group_ids`, then
    // merge + dedupe so the grid surfaces scenes from every member.
    // Strict-ID only — name-search would cross-pollute groups with
    // unrelated performers who happen to share a token.
    const id = data.identity || {};
    const groupIds = id.group_ids || {};
    // Build a per-source set of ids: primary first, then group entries.
    // Stripped + deduped per source — a primary id duplicated in
    // group_ids_json shouldn't cost an extra round-trip.
    const idSets = { tpdb: new Set(), stashdb: new Set(), fansdb: new Set(), javstash: new Set() };
    const pushId = (src, v) => {
      // ``v`` may be a bare string (legacy) or ``{id, name, image}`` —
      // both shapes coexist in group_ids_json since the "Link DB
      // profile" picker started writing rich entries. Coerce to the id.
      const raw = (v && typeof v === 'object') ? (v.id || '') : v;
      const s = String(raw || '').trim();
      if (s) idSets[src].add(s);
    };
    pushId('tpdb',     id.tpdb_id);
    pushId('stashdb',  id.stash_id);
    pushId('fansdb',   id.fansdb_id);
    pushId('javstash', id.javstash_id);
    ['tpdb', 'stashdb', 'fansdb', 'javstash'].forEach((src) => {
      (groupIds[src] || []).forEach((v) => pushId(src, v));
    });
    // Flat list of (source, id) tuples. Each becomes one /api/scenes/recent
    // call — small N (1-4 for solo, ~4-12 for a 2-3 member group).
    const fetchPlan = [];
    const SOURCE_LABEL = { tpdb: 'TPDB', stashdb: 'StashDB', fansdb: 'FansDB', javstash: 'JAVStash' };
    Object.entries(idSets).forEach(([src, set]) => {
      set.forEach((extId) => fetchPlan.push({ source: SOURCE_LABEL[src], srcKey: src, id: extId }));
    });
    const hasAnyId = fetchPlan.length > 0;
    el.innerHTML = `<div class="pp-loading"><span class="loader" role="status" aria-label="Loading"></span></div>`;
    let merged = [];
    if (hasAnyId) {
      try {
        // Fire all plan items in parallel and union the buckets. Each
        // call sends only the one id pair it knows so the backend can
        // strict-match without cross-source leakage.
        const responses = await Promise.all(fetchPlan.map((p) => {
          const params = new URLSearchParams({
            source:      p.source,
            id:          p.id,
            type:        'performer',
            slug:        '',
            name:        '',
            tpdb_id:     p.srcKey === 'tpdb'     ? p.id : '',
            stashdb_id:  p.srcKey === 'stashdb'  ? p.id : '',
            fansdb_id:   p.srcKey === 'fansdb'   ? p.id : '',
            javstash_id: p.srcKey === 'javstash' ? p.id : '',
          });
          return fetch('/api/scenes/recent?' + params.toString(), { credentials: 'same-origin' })
            .then((r) => r.json())
            .catch(() => ({}));
        }));
        for (const d of responses) {
          const buckets = (d && d.sources) || {};
          for (const k of ['tpdb', 'stashdb', 'fansdb', 'javstash']) {
            if (Array.isArray(buckets[k])) merged = merged.concat(buckets[k]);
          }
          if (!merged.length && Array.isArray(d && d.scenes)) merged = merged.concat(d.scenes);
        }
      } catch (e) {
        el.innerHTML = `<div class="pp-error">${ESC(e.message || 'Error')}</div>`;
        return;
      }
    }
    // Defensive client-side filter: drop any row the backend tagged as
    // a name match. Strict-ID mode shouldn't produce them, but a stale
    // cache entry from before this guard could still carry them.
    merged = merged.filter(s => s && s.match !== 'name');
    // Dedupe across buckets (id-first; falls back to date+thumb, then
    // title+studio+date — works cross-source).
    let scenes = dedupePerformerPopupScenes(merged);
    // Sort by date DESC. yyyy-mm-dd strings sort lexicographically.
    scenes.sort((a, b) => String(b.date || '').localeCompare(String(a.date || '')));
    // Cap at the most recent 12.
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
      const thumb  = s.thumb || s.image || '/static/img/missing.webp';
      const title  = s.title || '';
      const date   = s.date || '';
      const studio = s.studio || s.site_name || '';
      const studioLogo = (studio || title)
        ? `<img class="scene-studio-logo" src="/api/studio-logo?name=${encodeURIComponent(studio)}&q=${encodeURIComponent(title)}" alt="" loading="lazy" onload="this.closest('.scene-card')?.classList.add('has-studio-logo')" onerror="this.remove()">`
        : '';
      // Per-scene hover actions: link out to the source DB and search
      // Prowlarr by title. Replaces the recent-scenes popup which used
      // to host these actions on each row. `s.source` is set per-row
      // by the backend (TPDB / StashDB / FansDB / JAVStash) so the
      // hover link routes to the right DB even when the carousel
      // mixes buckets.
      const sid = String(s.id || '').trim();
      const rawRowSource = String(s.source || '').toLowerCase();
      const rowSource = rawRowSource === 'stashdb' ? 'stashdb'
        : rawRowSource === 'fansdb' ? 'fansdb'
        : rawRowSource === 'javstash' ? 'javstash'
        : 'tpdb';
      const sceneUrl = !sid ? ''
        : rowSource === 'stashdb' ? `https://stashdb.org/scenes/${encodeURIComponent(sid)}`
        : rowSource === 'fansdb' ? `https://fansdb.cc/scenes/${encodeURIComponent(sid)}`
        : rowSource === 'javstash' ? `https://javstash.org/scenes/${encodeURIComponent(sid)}`
        : `https://theporndb.net/scenes/${encodeURIComponent(sid)}`;
      const sourceUiLabel = rowSource === 'stashdb' ? 'StashDB' : rowSource === 'fansdb' ? 'FansDB' : rowSource === 'javstash' ? 'JAVStash' : 'TPDB';
      // Backend tags `match="name"` when the row came from the
      // last-ditch name-search path (no per-source IDs known). Badge
      // those tiles so the user can spot a same-name collision.
      const isNameMatch = (s.match === 'name');
      const nameBadge = isNameMatch
        ? `<div class="pp-scene-match-badge" title="Matched by name only — performer had no ${sourceUiLabel} ID">name match</div>`
        : '';
      // HTML-escape the JSON literal so inline onclick="..." attribute
      // parsing doesn't terminate at the first " from JSON.stringify.
      // ESC handles & " < > ' so the browser decodes back to clean
      // JSON when the attribute is read.
      const titleJson = ESC(JSON.stringify(title));
      const studioJson = ESC(JSON.stringify(studio));
      // Two action surfaces over the tile thumbnail:
      //   • source DB logo, no chrome, top-right corner
      //   • Prowlarr search, large round button, centered on the tile
      // Both fade in on tile hover via the existing
      // `.pp-cell-carousel .scene-card:hover` rules.
      const sourceLogoFile = rowSource + '.png';
      const sourceLink = sceneUrl ? `
        <a class="pp-scene-action pp-scene-action--source pp-scene-action--${rowSource}" href="${ESC(sceneUrl)}" target="_blank" rel="noopener noreferrer" title="Open on ${ESC(sourceUiLabel)}" aria-label="Open on ${ESC(sourceUiLabel)}" onclick="event.stopPropagation()">
          <img src="/static/logos/${ESC(sourceLogoFile)}" alt="" onerror="this.replaceWith(Object.assign(document.createElement('i'),{className:'fa-solid fa-arrow-up-right-from-square'}))">
        </a>` : '';
      const prowlarrBtn = `
        <button type="button" class="pp-scene-prowlarr-center" onclick="event.stopPropagation(); window.openProwlarrSearchPopup({title:${titleJson},studio:${studioJson},kind:'scene'})" title="Search Prowlarr" aria-label="Search Prowlarr">
          <i class="fa-solid fa-download" aria-hidden="true"></i>
        </button>`;
      const blacklistBtn = `<button type="button" class="scene-card-blacklist-btn" data-title="${ESC(title || '')}" onclick="event.stopPropagation();sceneCardBlacklist(this)" title="Blacklist this title" aria-label="Blacklist title"><i class="fa-solid fa-ban"></i></button>`;
      const wantedBtn = (typeof window.tsBuildWantedBtnHtml === 'function')
        ? window.tsBuildWantedBtnHtml(s, 'scene')
        : '';
      return `
        <div class="scene-card discover-info-scene-card discover-info-scene-card--performer${isNameMatch ? ' is-name-match' : ''}" tabindex="0" title="${ESC(title)}">
          <div class="img-load">
            <span class="loader loader--tile" aria-hidden="true"></span>
            <img class="scene-thumb" src="${ESC(thumb)}" loading="lazy" onload="this.closest('.img-load')?.classList.add('ready')" onerror="this.onerror=null;this.src='/static/img/missing.webp';this.closest('.img-load')?.classList.add('ready');">
            <div class="duo-tint" aria-hidden="true"></div>
            ${studioLogo}
            ${nameBadge}
            ${sourceLink}
            ${prowlarrBtn}
            ${wantedBtn}
            ${blacklistBtn}
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
    wirePpScrollAffordance(el);
  }

  /* ── Random poster picks for unsaved performers ───────────────
   * The right-rail primary/secondary slots are populated from images
   * tagged kind='primary'/'secondary' (or matching the URL pattern
   * for legacy /api/favourites/performer-thumb endpoints). Performers
   * who aren't in the library yet have no such tags, so the rail
   * shows two blank slots. Pick two random images and tag them so
   * the rail has something to show — the gallery filter then strips
   * the same images so they don't appear twice. */
  function pickRandomPostersIfNeeded(data) {
    const lib = data.library_status || {};
    if (lib.in_library) return;
    const imgs = data.images || [];
    if (!imgs.length) return;
    const hasTagged = (rx) =>
      imgs.some(i => rx.test(i.kind || '') || rx.test(i.url || ''));
    const need = {
      primary: !hasTagged(/primary/i),
      secondary: !hasTagged(/secondary/i),
    };
    if (!need.primary && !need.secondary) return;
    // Don't reuse an already-tagged image for the missing slot.
    const taken = new Set();
    imgs.forEach((i, idx) => {
      if (/primary|secondary/i.test(i.kind || '') ||
          /primary|secondary/i.test(i.url || '')) {
        taken.add(idx);
      }
    });
    const pool = imgs.map((_, idx) => idx).filter(idx => !taken.has(idx));
    // Fisher–Yates shuffle so the two picks don't lean toward the
    // start of the list and feel different across opens.
    for (let i = pool.length - 1; i > 0; i -= 1) {
      const j = Math.floor(Math.random() * (i + 1));
      [pool[i], pool[j]] = [pool[j], pool[i]];
    }
    if (need.primary && pool.length) {
      const idx = pool.shift();
      imgs[idx] = { ...imgs[idx], kind: 'primary' };
    }
    if (need.secondary && pool.length) {
      const idx = pool.shift();
      imgs[idx] = { ...imgs[idx], kind: 'secondary' };
    }
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
      el.innerHTML = `<div class="pp-empty pp-empty--silhouette pp-empty--corner"><img src="/static/img/${sil}" alt="" class="pp-empty-silhouette pp-empty-silhouette--corner" loading="lazy"/></div>`;
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
        <img class="pp-gallery-bg-silhouette" src="/static/img/${sil}" alt="" aria-hidden="true" loading="lazy">
        <div class="pp-gallery-spinner" aria-hidden="true"><span class="loader loader--tile"></span></div>
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
    // Randomise which photo lands on top of the deck — keeps repeat
    // popup opens visually fresh instead of always leading with the
    // same headshot. Rest of the stack stays in its natural order; we
    // only swap the front index with a random other one.
    if (order.length > 1) {
      const pick = Math.floor(Math.random() * order.length);
      if (pick !== 0) {
        [order[0], order[pick]] = [order[pick], order[0]];
      }
    }
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
          // Any polaroid: click → view full-screen. Earlier behaviour
          // shuffled background tiles forward on click, but the deck
          // animates over 550ms so the polaroid moved out from under
          // the cursor and felt fiddly to hit. Fullscreen-on-click is
          // a stable target; the next/prev arrows still drive the
          // shuffle for users who want to flip through the deck in
          // place.
          ev.preventDefault();
          showFullScreenImage(imgs, parseInt(tile.dataset.imgIdx, 10));
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

  // Module-level close so app-shell.js's universal Escape handler can
  // dismiss the gallery via ID_CLOSE_FN. Mirrors the inner closeFs()
  // (class toggle + key handler detach) without depending on closure.
  window.closePerformerGalleryFs = function () {
    const overlay = document.getElementById('performerPopupGalleryFs');
    if (overlay) overlay.classList.remove('open');
    if (_fsKeyHandler) {
      document.removeEventListener('keydown', _fsKeyHandler);
      _fsKeyHandler = null;
    }
  };

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
    // Bail only when the interactive element is INSIDE the linked
    // element (e.g. an edit/remove button rendered inside a performer
    // name pill). If the linked element is itself nested inside an
    // anchor — like the green name spans inside `<a class="news-tile">`
    // — the anchor surrounds `el` rather than living inside it, and we
    // DO want to intercept the click and open the popup instead of
    // navigating away. So check containment, not bare proximity.
    const interactive = e.target.closest('button, a:not([data-performer-link])');
    if (interactive && el.contains(interactive)) return;
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

  /* ── Performer-Prowlarr search popup ─────────────────────────────
   * Triggered by the bio-area "Search Prowlarr" button. Searches
   * Prowlarr by performer name, dedupes by normalised title (first
   * occurrence wins), and renders the unique releases as 2:3 tiles
   * styled to mirror the /downloads Performers tab. */

  let _ppPerfProwlarrReleases = [];

  function _ppNormReleaseTitle(t) {
    return String(t || '').toLowerCase()
      .replace(/[\[\(].*?[\]\)]/g, '')
      .replace(/\b(720p|1080p|2160p|4k|uhd|sd|480p|hdtv|webrip|web-dl|webdl|web|bdrip|brrip|bluray|x264|x265|h264|h265|hevc|aac|mp3|xvid|divx|imageset|imgset)\b/gi, '')
      .replace(/[\W_]+/g, ' ')
      .trim();
  }

  function ensurePerfProwlarrModal() {
    if (document.getElementById('ppPerfProwlarrModal')) return;
    const div = document.createElement('div');
    div.id = 'ppPerfProwlarrModal';
    div.className = 'pp-perf-prowlarr-overlay';
    div.innerHTML = `
      <div class="pp-perf-prowlarr-shell" role="dialog" aria-modal="true" aria-label="Prowlarr search">
        <header class="pp-perf-prowlarr-head">
          <h2 id="ppPerfProwlarrTitle">Search Prowlarr</h2>
          <div id="ppPerfProwlarrStatus" class="pp-perf-prowlarr-status"></div>
          <button type="button" class="pp-perf-prowlarr-close" title="Close" aria-label="Close">
            <i class="fa-solid fa-xmark"></i>
          </button>
        </header>
        <div class="pp-perf-prowlarr-body">
          <div id="ppPerfProwlarrGrid" class="pp-perf-prowlarr-grid"></div>
        </div>
      </div>`;
    document.body.appendChild(div);
    div.addEventListener('click', (ev) => {
      if (ev.target === div) closePerfProwlarrPopup();
    });
    div.querySelector('.pp-perf-prowlarr-close').addEventListener('click', closePerfProwlarrPopup);
    // Delegated grab handler — clicking anywhere on a tile sends the
    // release. The grab pill itself has `pointer-events: none` so the
    // tile-level closest() hit-test always wins.
    // Keyboard activation: Enter / Space on a focused tile reroutes
    // through the same click handler.
    div.querySelector('#ppPerfProwlarrGrid').addEventListener('keydown', (ev) => {
      if (ev.key !== 'Enter' && ev.key !== ' ') return;
      const tile = ev.target.closest('.pp-pr-tile');
      if (!tile) return;
      ev.preventDefault();
      tile.click();
    });
    div.querySelector('#ppPerfProwlarrGrid').addEventListener('click', async (ev) => {
      const tile = ev.target.closest('.pp-pr-tile');
      if (!tile) return;
      ev.preventDefault();
      ev.stopPropagation();
      const idx = parseInt(tile.dataset.relIdx, 10);
      await _ppProwlarrGrabRelease(_ppPerfProwlarrReleases[idx], tile);
    });
  }

  function closePerfProwlarrPopup() {
    const m = document.getElementById('ppPerfProwlarrModal');
    if (m) m.classList.remove('is-open');
    _ppPerfProwlarrReleases = [];
  }
  // Exposed for app-shell.js's universal Escape handler. Note this
  // modal uses .is-open (not .open) so its visibility is controlled
  // exclusively by class — the inline-display fallback would not even
  // toggle it, but registering a real close still lets Escape work.
  window.closePerfProwlarrPopup = closePerfProwlarrPopup;

  async function openPerformerProwlarrSearchPopup(opts) {
    // Backwards-compat shim: a bare string still works as the name.
    if (typeof opts === 'string') opts = { name: opts };
    opts = opts || {};
    const name = opts.name || '';
    if (!name) return;
    ensurePerfProwlarrModal();
    const overlay = document.getElementById('ppPerfProwlarrModal');
    const titleEl = document.getElementById('ppPerfProwlarrTitle');
    const statusEl = document.getElementById('ppPerfProwlarrStatus');
    const gridEl = document.getElementById('ppPerfProwlarrGrid');
    titleEl.textContent = name;
    statusEl.textContent = 'Searching Prowlarr…';
    gridEl.innerHTML = '';
    _ppPerfProwlarrReleases = [];
    overlay.classList.add('is-open');
    let result;
    try {
      result = await _fetchPerformerProwlarrReleases(name);
    } catch (e) {
      statusEl.textContent = 'Error: ' + (e.message || e);
      return;
    }
    if (result.error) {
      statusEl.textContent = result.error;
      return;
    }
    const unique = result.releases;
    _ppPerfProwlarrReleases = unique;
    if (!unique.length) {
      statusEl.textContent = 'No releases found.';
      return;
    }
    statusEl.innerHTML = `<strong>${unique.length}</strong> unique release${unique.length === 1 ? '' : 's'} <span class="pp-perf-prowlarr-status-dim">(${result.totalBeforeDedupe} total before dedupe)</span>`;
    let posterUrl = opts.posterUrl;
    if (posterUrl === undefined) {
      const urls = _ppFilmsPosterUrls(_activeData);
      posterUrl = urls.posterUrl;
    }
    let headshotUrl = opts.headshotUrl;
    if (headshotUrl === undefined) {
      headshotUrl = (_activeData && _activeData.headshot_url) || '';
    }
    const showCenterPh = opts.showCenterPlaceholder !== false;
    gridEl.innerHTML = unique.map((rel, i) => buildPerfProwlarrTile(rel, i, posterUrl, headshotUrl, showCenterPh)).join('');
  }
  // Expose globally so the studio entity panel (and anywhere else)
  // can open the same popup without going through the performer popup.
  window.openPerformerProwlarrSearchPopup = openPerformerProwlarrSearchPopup;

  function buildPerfProwlarrTile(rel, i, posterUrl, headshotUrl, showCenterPh) {
    const isTor = rel.type === 'torrent';
    const m = rel.match || {};
    const studio = (m.studio || '').trim();
    const studioLogo = studio
      ? `<img class="pp-pr-tile-studio-logo" src="/api/studio-logo?name=${encodeURIComponent(studio)}&q=${encodeURIComponent(rel.title || '')}" alt="" loading="lazy" onerror="this.remove()">`
      : '';
    const sizeMb = rel.size_mb || (rel.size ? rel.size / 1024 / 1024 : 0);
    const sizeLabel = sizeMb >= 1024
      ? (sizeMb / 1024).toFixed(2) + ' GB'
      : Math.round(sizeMb) + ' MB';
    const ageLabel = rel.age != null ? Math.round(rel.age / 24) + 'd' : '';
    const meta = [ageLabel, sizeLabel].filter(Boolean).join(' · ');
    const typeLogo = `<img class="pp-pr-tile-type-logo" src="/static/logos/${isTor ? 'torrent' : 'nzb'}.webp" alt="${isTor ? 'Torrent' : 'NZB'}" title="${isTor ? 'Torrent' : 'NZB'}">`;
    //: Image-only resolution / VR / 3D / uncensored badges — same
    //: shared renderer the main Prowlarr overlay uses, so the legacy
    //: performer-tile popup picks up new badge types automatically.
    const qualityBadges = (typeof window._tsProwlarrQualityBadges === 'function')
      ? window._tsProwlarrQualityBadges(rel.title || '', 'row')
      : '';
    const indexer = (rel.indexer || '').trim();
    const titleAttr = ESC(`Send to download client — ${rel.title || 'Untitled'}`);
    return `<div class="pp-pr-tile" data-rel-idx="${i}" role="button" tabindex="0" title="${titleAttr}" aria-label="${titleAttr}">
      ${posterUrl ? `<img class="pp-pr-tile-bg" src="${ESC(posterUrl)}" alt="" loading="lazy" onerror="this.remove()">` : ''}
      <div class="pp-pr-tile-veil"></div>
      <div class="pp-pr-tile-content">
        <div class="pp-pr-tile-top">${studioLogo}</div>
        ${headshotUrl
          ? `<img class="pp-pr-tile-headshot" src="${ESC(headshotUrl)}" alt="" loading="lazy" onerror="this.style.display='none'">`
          : (showCenterPh ? `<div class="pp-pr-tile-headshot pp-pr-tile-headshot--ph"><i class="fa-solid fa-person"></i></div>` : '<div class="pp-pr-tile-headshot-spacer"></div>')}
        <div class="pp-pr-tile-bottom">
          <div class="pp-pr-tile-title" title="${ESC(rel.title || '')}">${ESC(rel.title || 'Untitled')}</div>
          <div class="pp-pr-tile-meta">${qualityBadges}${ESC(meta)}${indexer ? ` <span class="pp-pr-tile-indexer">· ${ESC(indexer)}</span>` : ''}</div>
        </div>
      </div>
      <span class="pp-pr-tile-type-badge">${typeLogo}</span>
      <span class="pp-pr-tile-grab" aria-hidden="true">
        <i class="fa-solid fa-download"></i>
      </span>
    </div>`;
  }

  /* ── "Link DB profile" search modal ─────────────────────────────
   * Mirrors studio-popup.js's openStudioLinkSearchModal. Lets the user
   * search TPDB / StashDB / FansDB / JAVStash for additional performer
   * profiles and append each pick to the row's group_ids_json. The
   * popup carousel then fans out scenes across the new id, and /queue
   * scenes carrying that id route here automatically via the library
   * index lookup. */
  let _perfLinkSearchToken  = 0;
  let _perfLinkSearchRowId  = null;
  let _perfLinkSearchName   = '';
  let _perfLinkSearchResults = [];
  // Set of "{source}:{id}" already linked on the active row, refreshed
  // every time the modal opens (and after each pick) so the result list
  // can grey-out already-linked entries instead of double-linking.
  let _perfLinkSearchLinkedSet = new Set();

  function ensurePerformerLinkSearchModal() {
    if (document.getElementById('performerLinkSearchModal')) return;
    const div = document.createElement('div');
    div.id = 'performerLinkSearchModal';
    div.className = 'modal-overlay';
    // Sit above the performer popup (which is z=1500ish).
    div.style.setProperty('z-index', '1800', 'important');
    div.innerHTML = `
      <div class="modal-box ts-link-modal-box performer-link-search-box"
           style="max-width:760px;width:min(760px,calc(100vw - 60px));max-height:calc(100vh - 80px);min-height:min(520px,calc(100vh - 80px));display:flex;flex-direction:column">
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:14px">
          <h3 id="performerLinkSearchTitle"
              style="margin:0;font-family:var(--font-display, var(--mono));font-size:18px;flex:1"
              title="Search the source databases for additional performer profiles. Each pick adds an ID to this group so scenes and Prowlarr searches fan out across every member.">Link DB profile</h3>
          <button type="button" id="performerLinkSearchInfoBtn" aria-label="What does this do?"
                  title="Search the source databases for additional performer profiles. Each pick adds an ID to this group so scenes and Prowlarr searches fan out across every member."
                  style="background:transparent;border:1px solid rgba(255,255,255,0.15);color:var(--dim);width:26px;height:26px;border-radius:50%;cursor:default;display:inline-flex;align-items:center;justify-content:center">
            <i class="fa-solid fa-info" style="font-size:11px"></i>
          </button>
        </div>
        <input type="search" id="performerLinkSearchInput"
               placeholder="Search TPDB · StashDB · FansDB · JAVStash…"
               autocomplete="off"
               style="background:rgba(0,0,0,0.35);border:1px solid rgba(255,255,255,0.12);border-radius:6px;color:var(--text);padding:8px 12px;font-size:13px;font-family:var(--mono);outline:none;margin-bottom:10px">
        <div id="performerLinkSearchStatus" style="font-size:11px;color:var(--dim);margin-bottom:8px"></div>
        <div id="performerLinkSearchResults" style="flex:1 1 0;min-height:240px;overflow-y:auto;display:flex;flex-direction:column;gap:4px"></div>
        <div class="ts-link-modal-foot" style="display:flex;justify-content:flex-end;margin-top:12px">
          <button type="button" id="performerLinkSearchCloseBtn"
                  style="background:transparent;border:1px solid rgba(255,255,255,0.15);color:var(--dim);padding:8px 16px;border-radius:6px;font-size:11px;font-family:var(--mono);cursor:pointer;text-transform:uppercase;letter-spacing:0.04em">Close</button>
        </div>
      </div>`;
    document.body.appendChild(div);
    div.addEventListener('click', (e) => {
      if (e.target === div) closePerformerLinkSearchModal();
    });
    div.querySelector('#performerLinkSearchCloseBtn').addEventListener('click', closePerformerLinkSearchModal);
    const input = div.querySelector('#performerLinkSearchInput');
    let debTimer = null;
    input.addEventListener('input', () => {
      if (debTimer) clearTimeout(debTimer);
      debTimer = setTimeout(() => runPerformerLinkSearch(input.value), 250);
    });
    input.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') closePerformerLinkSearchModal();
    });
  }

  async function refreshPerfLinkSearchLinkedSet() {
    _perfLinkSearchLinkedSet = new Set();
    if (!_perfLinkSearchRowId) return;
    try {
      const r = await fetch('/api/performer/popup?row_id=' + encodeURIComponent(_perfLinkSearchRowId),
        { credentials: 'same-origin' });
      const d = await r.json().catch(() => ({}));
      const gids = (d && d.identity && d.identity.group_ids) || {};
      ['tpdb', 'stashdb', 'fansdb', 'javstash'].forEach((src) => {
        (gids[src] || []).forEach((entry) => {
          const xid = (entry && typeof entry === 'object') ? (entry.id || '') : entry;
          const s = String(xid || '').trim();
          if (s) _perfLinkSearchLinkedSet.add(src + ':' + s);
        });
      });
    } catch (_) { /* leave empty set on failure */ }
  }

  function openPerformerLinkSearchModal(rowId, name) {
    if (!rowId) return;
    ensurePerformerLinkSearchModal();
    _perfLinkSearchRowId = Number(rowId);
    _perfLinkSearchName  = String(name || '');
    const div = document.getElementById('performerLinkSearchModal');
    const title = div.querySelector('#performerLinkSearchTitle');
    if (title) {
      title.textContent = _perfLinkSearchName
        ? `Link DB profile → ${_perfLinkSearchName}`
        : 'Link DB profile';
    }
    const input = div.querySelector('#performerLinkSearchInput');
    if (input) {
      input.value = _perfLinkSearchName || '';
      setTimeout(() => input.focus(), 0);
    }
    document.getElementById('performerLinkSearchResults').innerHTML = '';
    document.getElementById('performerLinkSearchStatus').textContent = '';
    div.style.display = 'flex';
    div.classList.add('open');
    // Pull the current linked-set, THEN fire the prefilled search — so
    // the first render correctly greys out already-linked rows.
    refreshPerfLinkSearchLinkedSet().then(() => {
      if (_perfLinkSearchName) runPerformerLinkSearch(_perfLinkSearchName);
    });
  }
  window.openPerformerLinkSearchModal = openPerformerLinkSearchModal;

  function closePerformerLinkSearchModal() {
    const div = document.getElementById('performerLinkSearchModal');
    if (!div) return;
    div.classList.remove('open');
    div.style.display = '';
    _perfLinkSearchToken++;  // invalidate any in-flight fetches
  }

  async function runPerformerLinkSearch(q) {
    const query = String(q || '').trim();
    const status  = document.getElementById('performerLinkSearchStatus');
    const results = document.getElementById('performerLinkSearchResults');
    if (!status || !results) return;
    if (!query) {
      status.textContent = '';
      results.innerHTML = '';
      return;
    }
    status.textContent = 'Searching…';
    results.innerHTML = '';
    const token = ++_perfLinkSearchToken;
    try {
      // strict=0 so a partial query still surfaces hits — the picker
      // wants the user to find candidates by typing part of a name.
      const r = await fetch('/api/metadata/search?type=performer&strict=0&q=' + encodeURIComponent(query), {
        credentials: 'same-origin',
      });
      const d = await r.json().catch(() => ({}));
      if (token !== _perfLinkSearchToken) return;
      if (!r.ok) {
        status.textContent = d.error || ('HTTP ' + r.status);
        return;
      }
      const items = Array.isArray(d.results) ? d.results : [];
      if (!items.length) {
        status.textContent = 'No matches';
        return;
      }
      status.textContent = `${items.length} match${items.length === 1 ? '' : 'es'}`;
      _perfLinkSearchResults = items;
      results.innerHTML = items.map((it, i) => {
        const srcRaw = (it.source || '').toLowerCase();
        const srcKey = srcRaw === 'theporndb' ? 'tpdb' : srcRaw;
        const src   = (it.source || '').toUpperCase();
        const nm    = it.name || '';
        const id    = String(it.id || '');
        const img   = it.image || '';
        const initial = nm.trim().charAt(0).toUpperCase() || '?';
        const alreadyLinked = _perfLinkSearchLinkedSet.has(srcKey + ':' + id);
        const bg = alreadyLinked ? 'rgba(74,222,128,0.08)' : 'rgba(255,255,255,0.02)';
        const border = alreadyLinked ? '1px solid rgba(74,222,128,0.45)' : '1px solid rgba(255,255,255,0.06)';
        const cursor = alreadyLinked ? 'default' : 'pointer';
        const rightAction = alreadyLinked
          ? `<span style="display:inline-flex;align-items:center;gap:4px;font-family:var(--mono);font-size:9px;letter-spacing:0.08em;text-transform:uppercase;color:#4ade80;background:rgba(74,222,128,0.12);border:1px solid rgba(74,222,128,0.45);padding:3px 8px;border-radius:999px"><i class="fa-solid fa-check"></i> Linked</span>`
          : `<i class="fa-solid fa-link" style="color:var(--accent);font-size:13px"></i>`;
        return `<button type="button" class="performer-link-search-row${alreadyLinked ? ' is-linked' : ''}"
                        data-i="${i}"
                        ${alreadyLinked ? 'disabled' : ''}
                        style="display:flex;align-items:center;gap:12px;padding:8px 10px;border-radius:6px;background:${bg};border:${border};cursor:${cursor};text-align:left;color:var(--text);font-family:inherit">
          <div style="width:48px;height:48px;border-radius:6px;background:rgba(0,0,0,0.35);display:flex;align-items:center;justify-content:center;flex-shrink:0;overflow:hidden">
            ${img
              ? `<img src="${ESC(img)}" alt="" referrerpolicy="no-referrer" style="max-width:100%;max-height:100%;object-fit:cover;width:100%;height:100%" onerror="this.replaceWith(Object.assign(document.createElement('span'),{textContent:'${ESC(initial)}',style:'font-family:var(--mono);font-size:18px;color:var(--dim)'}))">`
              : `<span style="font-family:var(--mono);font-size:18px;color:var(--dim)">${ESC(initial)}</span>`}
          </div>
          <div style="flex:1;min-width:0">
            <div style="font-size:13px;color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${ESC(nm)}</div>
            <div style="font-size:10px;color:var(--dim);margin-top:2px">${ESC(src)} · ${ESC(id)}</div>
          </div>
          ${rightAction}
        </button>`;
      }).join('');
      results.querySelectorAll('.performer-link-search-row').forEach((btn) => {
        if (btn.classList.contains('is-linked')) return;
        btn.addEventListener('mouseover', () => { btn.style.background = 'rgba(255,255,255,0.06)'; });
        btn.addEventListener('mouseout',  () => { btn.style.background = 'rgba(255,255,255,0.02)'; });
        btn.addEventListener('click', () => linkPickedPerformer(parseInt(btn.dataset.i, 10)));
      });
    } catch (e) {
      if (token !== _perfLinkSearchToken) return;
      status.textContent = e.message || 'Search failed';
    }
  }

  async function linkPickedPerformer(idx) {
    const it = _perfLinkSearchResults[idx];
    if (!it || !_perfLinkSearchRowId) return;
    const source = (it.source || '').toLowerCase();
    // The metadata search returns TheporndB labelled "TPDB" already,
    // but accept the long form too for safety.
    const siteKey = source === 'theporndb' ? 'tpdb' : source;
    if (!['tpdb', 'stashdb', 'fansdb', 'javstash'].includes(siteKey)) {
      if (window.toast) window.toast('Unknown source: ' + source, { kind: 'error' });
      return;
    }
    try {
      const r = await fetch('/api/favourites/group-add-link', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          row_id: _perfLinkSearchRowId,
          source: siteKey,
          ext_id: String(it.id || ''),
          name:   it.name || '',
          image:  it.image || '',
        }),
      });
      if (!r.ok) {
        const d = await r.json().catch(() => ({}));
        if (window.toast) window.toast(d.error || 'Link failed', { kind: 'error' });
        return;
      }
      if (window.toast) {
        window.toast(`Linked ${it.name || it.id} to ${_perfLinkSearchName || 'this group'}`);
      }
      // Keep the modal open so the user can link several profiles in
      // one session. Refresh the linked-set + re-render results so the
      // just-picked row flips to "LINKED" without a manual re-search.
      _perfLinkSearchLinkedSet.add(siteKey + ':' + String(it.id || ''));
      const input = document.getElementById('performerLinkSearchInput');
      if (input && input.value.trim()) runPerformerLinkSearch(input.value);
      // Repaint the popup behind the modal so chips appear immediately.
      if (typeof window.refreshPerformerPopup === 'function'
          && window._performerPopupActiveId === _perfLinkSearchRowId) {
        window.refreshPerformerPopup();
      }
    } catch (e) {
      if (window.toast) window.toast(e.message || 'Link failed', { kind: 'error' });
    }
  }

  /* ── Group members modal ─────────────────────────────────────────
   * Clicked from the group badge on a is_group performer popup. Each
   * tile is one logical member (collapsed from group_ids_json by name).
   * Two actions per tile:
   *   • Open — drills into that member's standalone performer popup
   *     so the user can see their bio, scenes, etc.
   *   • Add DB link — opens the link-search picker with the member's
   *     name pre-filled, so each new pick attaches THIS member's name
   *     and clusters under the same tile on the next render.
   * The trailing "+" tile opens the picker with no pre-filled name so
   * a brand-new member can be added.
   */
  let _groupMembersRowId = null;
  let _groupMembersGroupName = '';

  function ensureGroupMembersModal() {
    if (document.getElementById('performerGroupMembersModal')) return;
    const div = document.createElement('div');
    div.id = 'performerGroupMembersModal';
    div.className = 'modal-overlay';
    div.style.setProperty('z-index', '1750', 'important');
    div.innerHTML = `
      <div class="modal-box pp-group-members-box"
           style="max-width:760px;width:min(760px,calc(100vw - 60px));max-height:calc(100vh - 80px);min-height:min(540px,calc(100vh - 80px));display:flex;flex-direction:column">
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:14px">
          <h3 id="performerGroupMembersTitle" style="margin:0;font-family:var(--font-display, var(--mono));font-size:18px;flex:1">Group members</h3>
          <button type="button" id="performerGroupMembersClose"
                  style="background:transparent;border:1px solid rgba(255,255,255,0.15);color:var(--dim);padding:6px 12px;border-radius:6px;font-size:11px;font-family:var(--mono);cursor:pointer;text-transform:uppercase;letter-spacing:0.04em">Close</button>
        </div>
        <div id="performerGroupMembersHint" style="font-size:11px;color:var(--dim);margin-bottom:14px"></div>
        <div id="performerGroupMembersGrid"
             style="flex:1 1 0;min-height:260px;overflow-y:auto;display:grid;grid-template-columns:repeat(auto-fill,minmax(190px,1fr));gap:20px;padding:6px 4px 16px;align-content:start"></div>
      </div>`;
    document.body.appendChild(div);
    div.addEventListener('click', (e) => {
      if (e.target === div) closeGroupMembersModal();
    });
    div.querySelector('#performerGroupMembersClose').addEventListener('click', closeGroupMembersModal);
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && div.classList.contains('open')) closeGroupMembersModal();
    });
  }

  function closeGroupMembersModal() {
    const div = document.getElementById('performerGroupMembersModal');
    if (!div) return;
    div.classList.remove('open');
    div.style.display = '';
  }

  function openGroupMembersModal(rowId, groupName, members) {
    ensureGroupMembersModal();
    _groupMembersRowId = Number(rowId);
    _groupMembersGroupName = String(groupName || '');
    const div = document.getElementById('performerGroupMembersModal');
    div.querySelector('#performerGroupMembersTitle').textContent =
      `Members of ${groupName || 'this group'}`;
    const hint = div.querySelector('#performerGroupMembersHint');
    const count = members.length;
    hint.innerHTML = count
      ? `Click a tile to open that member's profile. Use <strong>+ Add DB link</strong> on any tile to link more source profiles to that specific member, or the “Add new member” tile to bring in someone fresh.`
      : `This group has no linked profiles yet. Use the “Add new member” tile to pull one in from TPDB / StashDB / FansDB / JAVStash.`;
    renderGroupMembersGrid(members);
    div.style.display = 'flex';
    div.classList.add('open');

    // If any orphan (bare-id) members exist, transparently fire the
    // backend enrich endpoint so their names + images get filled in.
    // The re-render then collapses ones with the same resolved name
    // into a single tile (legacy 4-orphan groups become 2 tiles once
    // both members are recognised across TPDB + StashDB).
    const hasOrphans = members.some((m) => !m.name);
    if (hasOrphans) {
      maybeEnrichGroupMembers();
    }
  }

  /** Background-fire the enrich endpoint for the active group. While
   * in flight, show a small status line so the user knows tiles will
   * repaint. Re-renders the grid on completion (collapsed by name). */
  async function maybeEnrichGroupMembers() {
    if (!_groupMembersRowId) return;
    const div = document.getElementById('performerGroupMembersModal');
    if (!div || !div.classList.contains('open')) return;
    const hint = div.querySelector('#performerGroupMembersHint');
    const priorHint = hint ? hint.innerHTML : '';
    if (hint) {
      hint.innerHTML = '<i class="fa-solid fa-spinner fa-spin" style="margin-right:6px"></i> Resolving member names from source DBs…';
    }
    try {
      const r = await fetch('/api/favourites/group-enrich-members', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ row_id: _groupMembersRowId }),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) {
        if (hint) hint.innerHTML = priorHint;
        return;
      }
      const groupIds = (d && d.group_ids) || {};
      const refreshed = deriveGroupMembers(groupIds);
      // Modal could be closed by the time the fetch returns.
      if (!div.classList.contains('open')) return;
      if (hint) hint.innerHTML = priorHint;
      renderGroupMembersGrid(refreshed);
      // Repaint the popup behind too so its badge count reflects the
      // post-dedupe member count.
      if (typeof window.refreshPerformerPopup === 'function'
          && window._performerPopupActiveId === _groupMembersRowId) {
        window.refreshPerformerPopup();
      }
    } catch (_) {
      if (hint) hint.innerHTML = priorHint;
    }
  }

  const SOURCE_LABEL = { tpdb: 'TPDB', stashdb: 'StashDB', fansdb: 'FansDB', javstash: 'JAVStash' };

  function renderGroupMembersGrid(members) {
    const grid = document.getElementById('performerGroupMembersGrid');
    if (!grid) return;
    const tiles = members.map((m, i) => {
      // Truncate raw uuid orphans so the chip doesn't overflow its grid
      // cell when no name has resolved yet. Full id stays in the title
      // tooltip for the curious.
      const shortId = (s) => {
        const v = String(s || '');
        return v.length > 12 ? v.slice(0, 8) + '…' : v;
      };
      const displayName = m.name
        || (m._id ? `${SOURCE_LABEL[m._source] || ''} · ${shortId(m._id)}` : 'Unnamed');
      const fullTitle = m.name
        || (m._id ? `${SOURCE_LABEL[m._source] || ''} · ${m._id}` : 'Unnamed');
      const initial = (m.name || (SOURCE_LABEL[m._source] || '')).trim().charAt(0).toUpperCase() || '?';
      const srcChips = ['tpdb','stashdb','fansdb','javstash']
        .filter((s) => m.ids[s])
        .map((s) => `<img class="pp-member-src-logo" src="/static/logos/${s}.webp" alt="${SOURCE_LABEL[s]}" title="${SOURCE_LABEL[s]} · ${ESC(m.ids[s])}" onerror="this.replaceWith(document.createTextNode('${SOURCE_LABEL[s]}'))">`)
        .join('');
      return `<div class="pp-member-tile" data-member-i="${i}" title="${ESC(fullTitle)}">
        <button type="button" class="pp-member-tile-remove" data-action="remove"
                title="Remove from group" aria-label="Remove member">
          <i class="fa-solid fa-xmark"></i>
        </button>
        <button type="button" class="pp-member-tile-main" data-action="open"
                title="Open profile">
          <span class="pp-member-tile-avatar">
            ${m.image
              ? `<img src="${ESC(m.image)}" alt="" referrerpolicy="no-referrer" loading="lazy" onerror="this.replaceWith(Object.assign(document.createElement('span'),{className:'pp-member-tile-initial',textContent:'${ESC(initial)}'}))">`
              : `<span class="pp-member-tile-initial">${ESC(initial)}</span>`}
          </span>
          <span class="pp-member-tile-name">${ESC(displayName)}</span>
          <span class="pp-member-tile-srcs">${srcChips || '<span class="pp-member-tile-orphan">orphan ID</span>'}</span>
        </button>
        <div class="pp-member-tile-actions">
          <button type="button" class="pp-member-tile-action" data-action="add-link"
                  title="Link another DB profile to this member">
            <i class="fa-solid fa-link"></i> Add DB link
          </button>
        </div>
      </div>`;
    }).join('');
    const addNewTile = `<button type="button" class="pp-member-tile pp-member-tile--add" data-action="add-new"
            title="Add a new member to this group">
      <span class="pp-member-tile-avatar"><i class="fa-solid fa-plus"></i></span>
      <span class="pp-member-tile-name">Add new member</span>
      <span class="pp-member-tile-srcs"><span class="pp-member-tile-add-hint">Search TPDB · StashDB · FansDB · JAVStash</span></span>
    </button>`;
    grid.innerHTML = tiles + addNewTile;

    grid.querySelectorAll('.pp-member-tile-main').forEach((btn) => {
      btn.addEventListener('click', (ev) => {
        ev.preventDefault();
        const wrap = btn.closest('[data-member-i]');
        const idx = parseInt(wrap && wrap.dataset.memberI, 10);
        const member = members[idx];
        if (!member) return;
        openMemberPopup(member);
      });
    });
    grid.querySelectorAll('.pp-member-tile-action[data-action="add-link"]').forEach((btn) => {
      btn.addEventListener('click', (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        const wrap = btn.closest('[data-member-i]');
        const idx = parseInt(wrap && wrap.dataset.memberI, 10);
        const member = members[idx];
        if (!member) return;
        // Open the picker with this member's name pre-filled so new
        // picks inherit the same name and cluster under their tile.
        closeGroupMembersModal();
        if (typeof window.openPerformerLinkSearchModal === 'function') {
          window.openPerformerLinkSearchModal(_groupMembersRowId, member.name || _groupMembersGroupName);
        }
      });
    });
    grid.querySelectorAll('.pp-member-tile--add').forEach((btn) => {
      btn.addEventListener('click', (ev) => {
        ev.preventDefault();
        closeGroupMembersModal();
        if (typeof window.openPerformerLinkSearchModal === 'function') {
          // Empty name → user types whoever they want. The picker's
          // result will write its own DB-supplied name into group_ids.
          window.openPerformerLinkSearchModal(_groupMembersRowId, '');
        }
      });
    });
    grid.querySelectorAll('.pp-member-tile-remove').forEach((btn) => {
      btn.addEventListener('click', async (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        const wrap = btn.closest('[data-member-i]');
        const idx = parseInt(wrap && wrap.dataset.memberI, 10);
        const member = members[idx];
        if (!member || !_groupMembersRowId) return;
        const displayName = member.name
          || (member._id ? `${SOURCE_LABEL[member._source] || ''} · ${member._id}` : 'this member');
        if (!window.confirm(
          `Remove ${displayName} from this group?\n\n` +
          `Their linked DB ids are unlinked from the group folder — videos already filed under the group stay where they are, but new /queue scenes carrying their ids will no longer auto-route here.`
        )) return;
        await removeMemberFromGroup(member);
      });
    });
  }

  /** Strip every (source, id) pair belonging to a derived member from
   * the group's group_ids_json. Issues one /api/favourites/group-remove-link
   * call per (source, id) since the existing endpoint is single-target —
   * fine because a member rarely owns more than 4 ids. */
  async function removeMemberFromGroup(member) {
    if (!_groupMembersRowId) return;
    // Build the flat (source, id) list this member contributes.
    const pairs = [];
    ['tpdb', 'stashdb', 'fansdb', 'javstash'].forEach((s) => {
      if (member.ids && member.ids[s]) pairs.push({ source: s, id: member.ids[s] });
    });
    // Orphan tile: the bare id wasn't projected into ids[] until
    // deriveGroupMembers added it, but cover the case anyway via _id.
    if (!pairs.length && member._id && member._source) {
      pairs.push({ source: member._source, id: member._id });
    }
    if (!pairs.length) return;
    try {
      for (const p of pairs) {
        await fetch('/api/favourites/group-remove-link', {
          method: 'POST',
          credentials: 'same-origin',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ row_id: _groupMembersRowId, source: p.source, ext_id: p.id }),
        });
      }
      if (window.toast) window.toast('Member removed from group');
      // Refresh group_ids via the popup endpoint and re-render the grid.
      const r = await fetch('/api/performer/popup?row_id=' + encodeURIComponent(_groupMembersRowId) + '&refresh=1',
        { credentials: 'same-origin' });
      const d = await r.json().catch(() => ({}));
      const gids = (d && d.identity && d.identity.group_ids) || {};
      const refreshed = deriveGroupMembers(gids);
      renderGroupMembersGrid(refreshed);
      // Update parent popup so the badge count flips.
      if (typeof window.refreshPerformerPopup === 'function'
          && window._performerPopupActiveId === _groupMembersRowId) {
        window.refreshPerformerPopup();
      }
    } catch (e) {
      if (window.toast) window.toast(e.message || 'Remove failed', { kind: 'error' });
    }
  }

  function openMemberPopup(member) {
    // Resolve the best source+id pair for opening the member's
    // standalone popup. TPDB takes priority because it's the most
    // commonly populated; falls through to other sources. `standalone`
    // tells the backend to skip every library crosswalk so the click
    // doesn't bounce back to the group row that owns this id.
    const opts = { name: member.name || '', standalone: true };
    if (member.ids.tpdb)        opts.tpdbId = member.ids.tpdb;
    else if (member.ids.stashdb)  opts.stashId = member.ids.stashdb;
    else if (member.ids.fansdb)   opts.stashId = member.ids.fansdb;
    else if (member.ids.javstash) opts.stashId = member.ids.javstash;
    else if (member._id) {
      if (member._source === 'tpdb') opts.tpdbId = member._id;
      else opts.stashId = member._id;
    }
    closeGroupMembersModal();
    if (typeof window.openPerformerPopup === 'function') {
      window.openPerformerPopup(opts);
    }
  }
  window.openPerformerGroupMembersModal = function (rowId, groupName, groupIds) {
    const members = deriveGroupMembers(groupIds || {});
    openGroupMembersModal(rowId, groupName, members);
  };
})();
