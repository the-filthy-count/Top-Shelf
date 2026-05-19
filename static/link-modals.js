/* Universal link-search / ext-link modals for the performer popup.
 *
 * Loaded on every page that hosts performer-popup.js so the
 * change-link / link-missing chips work regardless of host page.
 * /library has its own richer modals (handles movies & studios with
 * source/year/gender filters); this module handles the common
 * performer case so the popup never falls back to a window.prompt().
 *
 * Public API
 *   window.openPerformerLinkSearch({rowId, site, name, kind, genderFilters})
 *
 *   kind = 'db'  → TPDB / StashDB / FansDB / JAVStash (calls /api/metadata/search,
 *                  picks via /api/favourites/match)
 *   kind = 'ext' → IAFD / Freeones / TMDB (calls
 *                  /api/performers/search-ext-link, saves via
 *                  /api/favourites/ext-link). Babepedia / Coomer fall
 *                  through to the manual-URL form (no scraper backend).
 */
(function () {
  if (window._tsLinkModalsLoaded) return;
  window._tsLinkModalsLoaded = true;

  const ESC = (s) => String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
  const toast = (msg) => (window.toast ? window.toast(msg) : console.warn(msg));

  let _injected = false;
  function inject() {
    if (_injected) return;
    _injected = true;
    const wrap = document.createElement('div');
    wrap.innerHTML = `
      <div class="modal-overlay" id="tsLinkSearchModal">
        <div class="modal-box ts-link-modal-box">
          <h3 id="tsLinkSearchTitle" style="margin:0 0 14px 0;font-family:var(--font-display, var(--mono));font-size:18px">Link profile</h3>
          <div class="ts-link-modal-row">
            <input id="tsLinkSearchInput" type="text" placeholder="Search…">
            <button type="button" id="tsLinkSearchBtn">Search</button>
          </div>
          <div id="tsLinkSearchResults" class="ts-link-modal-results"></div>
        </div>
      </div>
      <div class="modal-overlay" id="tsLinkExtModal">
        <div class="modal-box ts-link-modal-box">
          <h3 id="tsLinkExtTitle" style="margin:0 0 14px 0;font-family:var(--font-display, var(--mono));font-size:18px">Link profile</h3>
          <div class="ts-link-modal-row">
            <input id="tsLinkExtSearchInput" type="text" placeholder="Search…">
            <button type="button" id="tsLinkExtSearchBtn">Search</button>
          </div>
          <div id="tsLinkExtResults" class="ts-link-modal-results"></div>
          <div class="ts-link-modal-row" style="margin-top:8px;margin-bottom:0">
            <input id="tsLinkExtUrlInput" type="text" placeholder="Or paste URL directly…">
          </div>
          <div class="ts-link-modal-foot">
            <button type="button" id="tsLinkExtCancelBtn" style="background:transparent;border:1px solid rgba(255,255,255,0.15);color:var(--dim);padding:8px 16px;border-radius:6px;font-size:11px;font-family:var(--mono);cursor:pointer;text-transform:uppercase;letter-spacing:0.04em">Cancel</button>
            <button type="button" id="tsLinkExtSaveBtn" style="background:rgba(var(--brand-purple-rgb),0.4);border:1px solid rgba(var(--brand-purple-rgb),0.65);color:var(--accent);padding:8px 16px;border-radius:6px;font-size:11px;font-family:var(--mono);cursor:pointer;text-transform:uppercase;letter-spacing:0.04em">Save</button>
          </div>
        </div>
      </div>`;
    while (wrap.firstChild) document.body.appendChild(wrap.firstChild);
    bind();
  }

  let _dbRowId = null;
  let _dbSite = null;     // Label as shown to the user (e.g. "ThePornDB")
  let _dbSource = null;   // Matching x.source from /api/metadata/search (e.g. "TPDB")
  let _dbGenders = null;
  let _extRowId = null;
  let _extSite = null;

  // Pill labels are the human-friendly names ("ThePornDB"), but
  // /api/metadata/search returns x.source as the API key ("TPDB").
  // This map bridges the two so the result filter actually keeps the
  // hits the user is linking to.
  const DB_LABEL_TO_SOURCE = {
    thePornDB: 'TPDB',
    theporndb: 'TPDB',
    tpdb: 'TPDB',
    stashdb: 'StashDB',
    fansdb: 'FansDB',
    javstash: 'JAVStash',
  };

  const EXT_SEARCH_SITES = new Set(['iafd', 'freeones', 'tmdb', 'javdatabase', 'babepedia']);

  function extSiteApiKey(site) {
    return String(site || '').toLowerCase().replace(/\s+/g, '');
  }

  function bind() {
    document.getElementById('tsLinkSearchModal').addEventListener('click', (e) => {
      if (e.target === e.currentTarget) close('tsLinkSearchModal');
    });
    document.getElementById('tsLinkExtModal').addEventListener('click', (e) => {
      if (e.target === e.currentTarget) close('tsLinkExtModal');
    });
    document.getElementById('tsLinkSearchBtn').addEventListener('click', runDbSearch);
    document.getElementById('tsLinkSearchInput').addEventListener('keydown', (e) => {
      if (e.key === 'Enter') runDbSearch();
      if (e.key === 'Escape') close('tsLinkSearchModal');
    });
    document.getElementById('tsLinkExtSearchBtn').addEventListener('click', runExtSearch);
    document.getElementById('tsLinkExtSearchInput').addEventListener('keydown', (e) => {
      if (e.key === 'Enter') runExtSearch();
      if (e.key === 'Escape') close('tsLinkExtModal');
    });
    document.getElementById('tsLinkExtUrlInput').addEventListener('keydown', (e) => {
      if (e.key === 'Enter') saveExt();
      if (e.key === 'Escape') close('tsLinkExtModal');
    });
    document.getElementById('tsLinkExtCancelBtn').addEventListener('click', () => close('tsLinkExtModal'));
    document.getElementById('tsLinkExtSaveBtn').addEventListener('click', saveExt);
    document.addEventListener('keydown', (e) => {
      if (e.key !== 'Escape') return;
      if (document.getElementById('tsLinkSearchModal').classList.contains('open')) close('tsLinkSearchModal');
      else if (document.getElementById('tsLinkExtModal').classList.contains('open')) close('tsLinkExtModal');
    });
  }

  function close(id) {
    const m = document.getElementById(id);
    if (m) m.classList.remove('open');
    if (id === 'tsLinkSearchModal') { _dbRowId = null; _dbSite = null; _dbSource = null; _dbGenders = null; }
    if (id === 'tsLinkExtModal') { _extRowId = null; _extSite = null; }
  }

  /* ── DB search (TPDB / StashDB / FansDB / JAVStash) ────────── */

  async function openDb(rowId, site, name, opts) {
    inject();
    _dbRowId = rowId;
    _dbSite = site;
    _dbSource = DB_LABEL_TO_SOURCE[String(site || '').toLowerCase()] || site;
    _dbGenders = (opts && Array.isArray(opts.genderFilters) && opts.genderFilters.length) ? opts.genderFilters.slice() : null;
    document.getElementById('tsLinkSearchTitle').textContent = `Link ${site} profile`;
    const input = document.getElementById('tsLinkSearchInput');
    input.placeholder = 'Search TPDB, StashDB, FansDB, JAVStash…';
    input.value = name || '';
    document.getElementById('tsLinkSearchResults').innerHTML = '';
    document.getElementById('tsLinkSearchModal').classList.add('open');
    setTimeout(() => input.focus(), 60);
    if (name) runDbSearch();
  }

  async function runDbSearch() {
    if (!_dbRowId) return;
    const q = document.getElementById('tsLinkSearchInput').value.trim();
    const box = document.getElementById('tsLinkSearchResults');
    if (!q) {
      box.innerHTML = '<div class="ts-link-modal-empty">Enter a search term.</div>';
      return;
    }
    box.innerHTML = '<div class="ts-link-modal-empty">Searching…</div>';
    // strict=0: let TPDB / StashDB / FansDB / JAVStash run their own fuzzy match
    // and show every hit to the user. The previous strict=true added a
    // post-fetch exact-normalised-name filter (main.py search_performers)
    // that dropped legitimate matches whose API name varied even
    // slightly from the folder name — e.g. "Lova Q" stored on TPDB but
    // returned with a trailing period was filtered out and the modal
    // read "No results". /discover + /library's match modal already use
    // strict=0 for the same reason; matching that here. The source pill
    // (TPDB / StashDB / FansDB / JAVStash) the user clicked still filters the list
    // below, so they only ever see hits from the platform they're
    // linking against.
    let url = '/api/metadata/search?q=' + encodeURIComponent(q) + '&type=performer&strict=0';
    if (_dbGenders && _dbGenders.length) url += '&genders=' + encodeURIComponent(_dbGenders.join(','));
    try {
      const r = await fetch(url, { credentials: 'same-origin' });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) {
        box.innerHTML = `<div class="ts-link-modal-empty" style="color:var(--red)">${ESC(d.error || ('HTTP ' + r.status))}</div>`;
        return;
      }
      let list = d.results || [];
      // Filter to the site the user is linking to so they don't pick a
      // TPDB hit when they meant to link a StashDB profile (and the
      // pickDb match call requires source = the site we're linking).
      if (_dbSource) list = list.filter((x) => String(x.source || '').toLowerCase() === _dbSource.toLowerCase());
      if (!list.length) {
        box.innerHTML = `<div class="ts-link-modal-empty">No ${ESC(_dbSite)} results.</div>`;
        return;
      }
      box.innerHTML = list.map((x) => {
        const thumb = x.image
          ? `<img src="${ESC(x.image)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.remove()">`
          : '<i class="fa-solid fa-user"></i>';
        const aliases = Array.isArray(x.aliases) ? x.aliases : [];
        const aliasLine = aliases.length
          ? `<div class="ts-link-modal-aliases" title="${ESC(aliases.join(', '))}">aka ${ESC(aliases.slice(0, 6).join(', '))}${aliases.length > 6 ? ` +${aliases.length - 6}` : ''}</div>`
          : '';
        return `<div class="ts-link-modal-result"
            data-source="${ESC(x.source || '')}"
            data-id="${ESC(String(x.id || ''))}"
            data-name="${ESC(x.name || '')}"
            data-image="${ESC(x.image || '')}">
          <div class="ts-link-modal-thumb">${thumb}</div>
          <div class="ts-link-modal-body">
            <div class="ts-link-modal-source">${ESC(x.source || '')}</div>
            <div class="ts-link-modal-name">${ESC(x.name || '')}</div>
            ${aliasLine}
            ${x.id ? `<div class="ts-link-modal-url">id: ${ESC(x.id)}</div>` : ''}
          </div>
          <button type="button" class="ts-link-modal-pick">Select</button>
        </div>`;
      }).join('');
      box.querySelectorAll('.ts-link-modal-result').forEach((el) => {
        el.addEventListener('click', () => pickDb(el));
      });
    } catch (e) {
      box.innerHTML = `<div class="ts-link-modal-empty" style="color:var(--red)">${ESC(e.message || 'Search failed')}</div>`;
    }
  }

  async function pickDb(el) {
    if (!_dbRowId) return;
    const body = {
      row_id: _dbRowId,
      source: el.getAttribute('data-source'),
      external_id: el.getAttribute('data-id'),
      name: el.getAttribute('data-name'),
    };
    const image = el.getAttribute('data-image');
    if (image) body.image = image;
    el.querySelector('.ts-link-modal-pick').textContent = 'Saving…';
    try {
      const r = await fetch('/api/favourites/match', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) { toast(d.error || 'Could not save match'); return; }
      close('tsLinkSearchModal');
      afterChange(_dbRowId);
    } catch (e) {
      toast(e.message || 'Save failed');
    }
  }

  /* ── External-link search (IAFD / Freeones / TMDB) ─────────── */

  async function openExt(rowId, site, name) {
    inject();
    _extRowId = rowId;
    _extSite = site;
    document.getElementById('tsLinkExtTitle').textContent = `Link ${site} profile`;
    const input = document.getElementById('tsLinkExtSearchInput');
    input.placeholder = `Search ${site}…`;
    input.value = name || '';
    document.getElementById('tsLinkExtUrlInput').value = '';
    document.getElementById('tsLinkExtResults').innerHTML = '';
    document.getElementById('tsLinkExtModal').classList.add('open');
    setTimeout(() => input.focus(), 60);
    // Some ext sites have no search backend (Babepedia, Coomer); the
    // user can still paste a URL directly into the bottom input.
    const lc = extSiteApiKey(site);
    if (name && EXT_SEARCH_SITES.has(lc)) runExtSearch();
  }

  async function runExtSearch() {
    if (!_extRowId || !_extSite) return;
    const lc = extSiteApiKey(_extSite);
    const box = document.getElementById('tsLinkExtResults');
    if (!EXT_SEARCH_SITES.has(lc)) {
      box.innerHTML = `<div class="ts-link-modal-empty">No search backend for ${ESC(_extSite)} — paste a URL below.</div>`;
      return;
    }
    box.innerHTML = '<div class="ts-link-modal-empty">Searching…</div>';
    try {
      const r = await fetch('/api/performers/search-ext-link', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          row_id: _extRowId,
          site: lc,
          q: (document.getElementById('tsLinkExtSearchInput').value || '').trim(),
        }),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) {
        box.innerHTML = `<div class="ts-link-modal-empty" style="color:var(--red)">${ESC(d.error || ('HTTP ' + r.status))}</div>`;
        return;
      }
      const results = d.results || [];
      if (!results.length) {
        box.innerHTML = `<div class="ts-link-modal-empty">No ${ESC(_extSite)} results.</div>`;
        return;
      }
      box.innerHTML = results.map((res) => {
        const thumb = res.thumb_url
          ? `<img src="${ESC(res.thumb_url)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.outerHTML='<i class=\\'fa-solid fa-user\\'></i>'">`
          : '<i class="fa-solid fa-user"></i>';
        return `<div class="ts-link-modal-result" data-url="${ESC(res.url || '')}">
          <div class="ts-link-modal-thumb">${thumb}</div>
          <div class="ts-link-modal-body">
            <div class="ts-link-modal-source">${ESC(_extSite)}</div>
            <div class="ts-link-modal-name">${ESC(res.name || '')}</div>
            <div class="ts-link-modal-url">${ESC(res.url || '')}</div>
          </div>
          <button type="button" class="ts-link-modal-pick">Select</button>
        </div>`;
      }).join('');
      box.querySelectorAll('.ts-link-modal-result').forEach((el) => {
        el.addEventListener('click', () => pickExt(el));
      });
    } catch (e) {
      box.innerHTML = `<div class="ts-link-modal-empty" style="color:var(--red)">${ESC(e.message || 'Search failed')}</div>`;
    }
  }

  function pickExt(el) {
    const url = el.getAttribute('data-url') || '';
    if (!url) return;
    document.querySelectorAll('#tsLinkExtResults .ts-link-modal-result').forEach((i) => i.classList.remove('is-selected'));
    el.classList.add('is-selected');
    document.getElementById('tsLinkExtUrlInput').value = url;
  }

  async function saveExt() {
    const url = (document.getElementById('tsLinkExtUrlInput').value || '').trim();
    if (!url || !_extRowId || !_extSite) return;
    const btn = document.getElementById('tsLinkExtSaveBtn');
    btn.disabled = true;
    btn.textContent = 'Saving…';
    try {
      const r = await fetch('/api/favourites/ext-link', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ row_id: _extRowId, site: extSiteApiKey(_extSite), url }),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) { toast(d.error || 'Failed'); return; }
      const rid = _extRowId;
      close('tsLinkExtModal');
      toast('Link saved');
      afterChange(rid);
    } finally {
      btn.disabled = false;
      btn.textContent = 'Save';
    }
  }

  function afterChange(rowId) {
    if (window._performerPopupActiveId === rowId && typeof window.refreshPerformerPopup === 'function') {
      window.refreshPerformerPopup();
    } else if (typeof window.load === 'function') {
      try { window.load(); } catch (_) { /* host page may not expose load() */ }
    }
  }

  window.openPerformerLinkSearch = function (opts) {
    if (!opts || !opts.rowId || !opts.site) return;
    if (opts.kind === 'db') {
      openDb(opts.rowId, opts.site, opts.name || '', opts);
    } else {
      openExt(opts.rowId, opts.site, opts.name || '');
    }
  };
  // Exposed so app-shell.js's universal Escape handler can close these
  // via the ID_CLOSE_FN map. Without this, Escape falls through to the
  // generic `el.style.display = 'none'` fallback, leaving an inline
  // display:none stuck on the modal — next openDb/openExt does
  // classList.add('open') but the inline rule overrides .open's
  // display:flex, so the modal opens invisibly while still blocking
  // pointer events on its overlay.
  window.closeTsLinkSearchModal = function () { close('tsLinkSearchModal'); };
  window.closeTsLinkExtModal = function () { close('tsLinkExtModal'); };
})();
