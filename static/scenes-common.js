// scenes-common.js
//
// JavaScript for /scenes (scene-feed grid + movie/JAV detail overlays).

  let _lottiePromise = null;

  function ensureLottie() {
    if (window.lottie && typeof window.lottie.loadAnimation === 'function') {
      return Promise.resolve();
    }
    if (!_lottiePromise) {
      _lottiePromise = new Promise(function (resolve, reject) {
        const s = document.createElement('script');
        s.src = '/static/vendor/lottie.min.js';
        s.onload = function () { resolve(); };
        s.onerror = function () { reject(new Error('lottie failed to load')); };
        document.head.appendChild(s);
      });
    }
    return _lottiePromise;
  }


  let currentType     = 'performer';
  let selectedResult  = null;
  let selectedDest    = null;

  // Country flag helpers (`countryFlagHtml`, `COUNTRY_FLAG_LOOKUP`) live
  // in ts-utils.js so /library can use them too — they're attached to
  // `window` and called below directly.


  // Memoised library-check — same name fired multiple times per session
  // (showDetail, etc.) shouldn't re-hit the API.
  const _libCheckCache = new Map();
  function libraryCheck(name, item, opts) {
    const q = new URLSearchParams();
    const n = (name || '').trim();
    if (n) q.set('name', n);
    const kind = (opts && opts.kind) || '';
    if (kind) q.set('kind', kind);
    if (item) {
      const id = String(item.id || item.slug || '').trim();
      const src = String(item.source || '').trim();
      if (id) {
        if (src === 'StashDB') q.set('stash_id', id);
        else if (src === 'TPDB') q.set('tpdb_id', id);
        else if (src === 'FansDB') q.set('fans_id', id);
        else if (src === 'JAVStash') q.set('javstash_id', id);
      }
    }
    const key = q.toString();
    if (!key) return Promise.resolve({ found: false });
    if (_libCheckCache.has(key)) return _libCheckCache.get(key);
    const p = fetch(`/api/library/check?${key}`)
      .then(r => r.json())
      .catch(() => ({ found: false }));
    _libCheckCache.set(key, p);
    return p;
  }
  /** Scene objects for the current grid (index matches data-scene-i on .scene-card) */
  let _sceneGridItems = [];
  /** Title-case for performer/studio labels (APIs often return all-lowercase). */
  function capDisplayName(s) {
    if (s == null || s === '') return '';
    const t = String(s).trim();
    // Leave Japanese (and other CJK) names untouched — title-casing
    // mangles roman numerals / product codes mixed with kana.
    if (/[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]/.test(t)) return t;
    return t.split(/\s+/).map(function (word) {
      if (!word) return word;
      return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
    }).join(' ');
  }
  // ── Wanted-list state ─────────────────────────────────────────────
  // Shared singleton state + click handler live in ts-utils.js
  // (window.tsWantedKeys / window.tsLoadWantedKeys / document-level
  // toggle handler) so studio + performer popups can render the same
  // watch eye without re-implementing the wiring. Local references
  // below stay because the rest of this file calls _wantedKeys.has()
  // inline for the overlay button.
  const _wantedKeys = window.tsWantedKeys;
  const _wkey = window.tsWantedKey;
  const _loadWantedKeys = window.tsLoadWantedKeys;
  function _cardWantedButtonHtml(kind, sourceGuess, externalId) {
    const key = _wkey(kind, sourceGuess, externalId);
    const on = _wantedKeys.has(key);
    const title = on ? 'In your Wanted list — click to remove' : 'Add to Wanted';
    return `<button class="scene-wanted-btn${on ? ' is-wanted' : ''}" data-wanted-kind="${esc(kind)}" data-wanted-source="${esc(sourceGuess)}" data-wanted-id="${esc(externalId)}" title="${esc(title)}" aria-pressed="${on}"><i class="fa-solid fa-eye"></i></button>`;
  }

  const _FEED_MODES = ['movies', 'jav', 'performers', 'studios', 'tags', 'search'];

  function _readSavedScenesFeedMode() {
    try {
      let v = localStorage.getItem('topShelfScenesFeedMode');
      if (v == null || v === '' || v === 'library' || v === 'random' || v === 'favourites') v = 'studios';
      if (v === 'recent') v = 'performers';
      if (!_FEED_MODES.includes(v)) v = 'studios';
      return v;
    } catch (_) {
      return 'studios';
    }
  }

  function _scenesFeedTitleForMode(mode) {
    if (mode === 'movies') return 'Latest Movie Releases';
    if (mode === 'jav') return 'Latest JAV Releases';
    if (mode === 'performers') return 'Latest Star Releases';
    if (mode === 'studios') return 'Latest Studio Releases';
    if (mode === 'tags') return 'Latest Vice Releases';
    if (mode === 'search') return 'Scene Search';
    return 'Latest Studio Releases';
  }

  let _scenesFeedMode = _readSavedScenesFeedMode();

  let _monitoredTagsCache = [];
  // null = never saved (first-ever entry to Tags mode); [] = user explicitly
  // deselected everything; [...] = persisted selection. The null sentinel
  // lets us auto-select every monitored tag on first entry while still
  // honouring an intentionally empty selection afterwards.
  let _selectedTagIds = (function() {
    try {
      const raw = localStorage.getItem('topShelfScenesSelectedTags');
      if (raw === null) return null;
      const parsed = JSON.parse(raw);
      return Array.isArray(parsed) ? parsed.map(String) : [];
    } catch(_) { return []; }
  })();

  // Which database(s) back each feed mode — drives the small source
  // logos shown next to the refresh button so the user can see at a
  // glance where the rows are coming from. Search is multi-source so
  // we list all four; the user-selected sources inside the search
  // panel narrow further, but the indicator stays comprehensive.
  const _FEED_SOURCE_LOGO_SEP =
    '<i class="fa-solid fa-grip-lines-vertical source-logo-sep" aria-hidden="true"></i>';

  const _FEED_MODE_SOURCES = {
    movies:     [{src: '/static/logos/tpdb.webp',     label: 'TPDB'}],
    jav:        [{src: '/static/logos/javstash.webp', label: 'JAVStash'}],
    performers: [
      {src: '/static/logos/stashdb.webp',  label: 'StashDB'},
      {src: '/static/logos/fansdb.webp',   label: 'FansDB'},
      {src: '/static/logos/javstash.webp', label: 'JAVStash'},
    ],
    studios:    [{src: '/static/logos/stashdb.webp',  label: 'StashDB'}],
    tags:       [{src: '/static/logos/stashdb.webp',  label: 'StashDB'}],
    // Search mode: no toolbar logos. The source multi-toggle inside
    // the search panel below the toolbar already shows the same four
    // logos and lets the user pick which ones the query hits — the
    // toolbar copy would just be a duplicate hint at a non-clickable
    // version of the same thing.
  };

  function _renderFeedSourceLogos() {
    const el = document.getElementById('feedSourceLogos');
    if (!el) return;
    const sources = _FEED_MODE_SOURCES[_scenesFeedMode] || [];
    if (!sources.length) {
      el.innerHTML = '';
      el.removeAttribute('title');
      return;
    }
    const title = 'Source: ' + sources.map(s => s.label).join(' + ');
    el.setAttribute('title', title);
    el.innerHTML = sources.map(s =>
      `<img class="feed-source-logo" src="${s.src}" alt="${s.label}" loading="lazy" referrerpolicy="no-referrer">`
    ).join(_FEED_SOURCE_LOGO_SEP);
  }

  function _applyFeedModeToggleUI() {
    document.querySelectorAll('#feedModeToggle .feed-mode-btn').forEach(btn => {
      btn.classList.toggle('active', btn.getAttribute('data-mode') === _scenesFeedMode);
    });
    const titleText = document.querySelector('#scenesTitle .ts-page-title__text');
    if (titleText) titleText.textContent = _scenesFeedTitleForMode(_scenesFeedMode);
    const tagWrap = document.getElementById('tagFilterWrap');
    if (tagWrap) tagWrap.style.display = (_scenesFeedMode === 'tags') ? 'block' : 'none';
    const searchPanel = document.getElementById('scenesSearchPanel');
    if (searchPanel) searchPanel.style.display = (_scenesFeedMode === 'search') ? 'block' : 'none';
    // Mark the grid with the active feed-mode so per-mode CSS can
    // hook in (e.g. studios/vices nudges the studio logo inside the
    // TV frame bezel).
    const grid = document.getElementById('scenesGrid');
    if (grid) grid.setAttribute('data-feed-mode', _scenesFeedMode || '');
    _renderFeedSourceLogos();
    _applyScenesDocumentLang();
  }

  /** `/scenes` only: hint document language for browser translate (JAV tab → Japanese). */
  function _applyScenesDocumentLang() {
    try {
      if (!document.getElementById('scenesGrid') || !document.getElementById('feedModeToggle')) return;
      document.documentElement.setAttribute('lang', _scenesFeedMode === 'jav' ? 'ja' : 'en');
    } catch (_) {}
  }

  /** Build the set of ids the UI / feed may use for a monitored tag row. */
  function _monitoredTagKeys(t) {
    const keys = new Set();
    if (!t || typeof t !== 'object') return keys;
    for (const k of ['id', 'slug', 'stashdb_id', 'stash_id', 'tpdb_id']) {
      const v = String(t[k] || '').trim();
      if (v) keys.add(v);
    }
    return keys;
  }

  /** Drop stale TPDB-era ids from localStorage after migrating to StashDB tags. */
  function _reconcileTagSelection() {
    if (!_monitoredTagsCache.length) {
      if (_selectedTagIds === null) _selectedTagIds = [];
      return;
    }
    const valid = new Set();
    _monitoredTagsCache.forEach(t => {
      _monitoredTagKeys(t).forEach(k => valid.add(k));
    });
    const canonicalIds = _monitoredTagsCache.map(t => String(t.id)).filter(Boolean);
    if (_selectedTagIds === null) {
      _selectedTagIds = canonicalIds.slice();
    } else if (Array.isArray(_selectedTagIds)) {
      const kept = _selectedTagIds.filter(id => valid.has(String(id)));
      // Had a saved selection but every id is stale (e.g. TPDB → Stash migration).
      _selectedTagIds = kept.length ? kept : canonicalIds.slice();
    } else {
      _selectedTagIds = canonicalIds.slice();
    }
    try { localStorage.setItem('topShelfScenesSelectedTags', JSON.stringify(_selectedTagIds)); } catch (_) {}
    _updateTagFilterBadge();
  }

  async function _ensureMonitoredTagsLoaded() {
    if (_monitoredTagsCache.length) {
      _reconcileTagSelection();
      return _monitoredTagsCache;
    }
    try {
      const r = await fetch('/api/settings');
      const d = await r.json();
      const raw = (d && d.settings && d.settings.monitored_tags) || '[]';
      const parsed = JSON.parse(raw);
      _monitoredTagsCache = Array.isArray(parsed)
        ? parsed.filter(t => t && t.name && (t.id || t.slug))
        : [];
    } catch(_) { _monitoredTagsCache = []; }
    _reconcileTagSelection();
    return _monitoredTagsCache;
  }

  function _renderTagFilterList() {
    const list = document.getElementById('tagFilterList');
    if (!list) return;
    if (!_monitoredTagsCache.length) {
      list.innerHTML = '<div style="padding:14px;color:var(--dim);font-size:12px;line-height:1.5">No vices configured. Add some under Settings → Content Filters → Vices.</div>';
      return;
    }
    const header = `
      <div style="display:flex;gap:6px;padding:4px 6px 8px;border-bottom:1px solid rgba(var(--brand-purple-rgb),0.18);margin-bottom:4px">
        <button type="button" onclick="selectAllTags()" style="flex:1;padding:4px 8px;border-radius:6px;background:rgba(var(--brand-purple-rgb),0.12);border:1px solid rgba(var(--brand-purple-rgb),0.25);color:var(--text);font-size:11px;cursor:pointer">All</button>
        <button type="button" onclick="clearSelectedTags()" style="flex:1;padding:4px 8px;border-radius:6px;background:rgba(var(--brand-purple-rgb),0.12);border:1px solid rgba(var(--brand-purple-rgb),0.25);color:var(--text);font-size:11px;cursor:pointer">None</button>
      </div>`;
    const sortedTags = _monitoredTagsCache
      .slice()
      .sort((a, b) => String(a.name || '').localeCompare(String(b.name || ''), undefined, { sensitivity: 'base' }));
    const selIds = Array.isArray(_selectedTagIds) ? _selectedTagIds : [];
    const _tagFilterLabel = (t) => {
      const name = t.name || '';
      const uuidRe = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
      const hasSt = !!(t.stashdb_id || uuidRe.test(String(t.id || '')));
      const hasTp = !!(t.tpdb_id || (/^\d+$/.test(String(t.id || '')) && !uuidRe.test(String(t.id || ''))));
      if (hasTp && hasSt) return `${name} [TPDB·Stash]`;
      if (hasSt) return `${name} [StashDB]`;
      if (hasTp) return `${name} [TPDB]`;
      return name;
    };
    const rows = sortedTags.map(t => {
      const id = String(t.id);
      const checked = selIds.includes(id);
      return `
        <label style="display:flex;align-items:center;gap:8px;padding:6px 8px;border-radius:6px;cursor:pointer;font-size:13px;color:var(--text)" onmouseenter="this.style.background='rgba(var(--brand-purple-rgb),0.10)'" onmouseleave="this.style.background=''">
          <input type="checkbox" ${checked ? 'checked' : ''} data-tag-id="${esc(id)}" onchange="toggleTagSelection('${esc(id)}', this.checked)" style="accent-color:var(--accent)">
          <span>${esc(_tagFilterLabel(t))}</span>
        </label>`;
    }).join('');
    list.innerHTML = header + rows;
  }

  function _updateTagFilterBadge() {
    const badge = document.getElementById('tagFilterBadge');
    if (!badge) return;
    // `_selectedTagIds` is intentionally `null` until the user (or
    // `_ensureMonitoredTagsLoaded`) seeds it — a fresh browser with no
    // localStorage key hits this path on the initial paint, before the
    // monitored-tags fetch resolves.
    const n = Array.isArray(_selectedTagIds) ? _selectedTagIds.length : 0;
    if (n > 0) {
      badge.textContent = String(n);
      badge.style.display = 'inline-block';
    } else {
      badge.style.display = 'none';
    }
  }

  function _positionTagFilterDropdown() {
    const btn = document.getElementById('tagFilterBtn');
    const dd = document.getElementById('tagFilterDropdown');
    if (!btn || !dd || dd.style.display !== 'block') return;
    const r = btn.getBoundingClientRect();
    const ddW = dd.offsetWidth || 280;
    const vw = window.innerWidth;
    // Align the dropdown's right edge to the button's right edge;
    // clamp to viewport so nothing gets clipped off-screen.
    let left = r.right - ddW;
    if (left < 8) left = 8;
    if (left + ddW > vw - 8) left = vw - ddW - 8;
    dd.style.top = (r.bottom + 6) + 'px';
    dd.style.left = left + 'px';
  }

  function toggleTagFilterDropdown() {
    const dd = document.getElementById('tagFilterDropdown');
    if (!dd) return;
    if (dd.style.display === 'block') {
      dd.style.display = 'none';
      return;
    }
    _ensureMonitoredTagsLoaded().then(() => {
      _renderTagFilterList();
      dd.style.display = 'block';
      _positionTagFilterDropdown();
    });
  }

  window.addEventListener('resize', _positionTagFilterDropdown);
  window.addEventListener('scroll', _positionTagFilterDropdown, true);

  function toggleTagSelection(id, checked) {
    if (!Array.isArray(_selectedTagIds)) _selectedTagIds = [];
    const sid = String(id);
    const idx = _selectedTagIds.indexOf(sid);
    if (checked && idx === -1) _selectedTagIds.push(sid);
    if (!checked && idx !== -1) _selectedTagIds.splice(idx, 1);
    try { localStorage.setItem('topShelfScenesSelectedTags', JSON.stringify(_selectedTagIds)); } catch(_) {}
    _updateTagFilterBadge();
    loadFeed();
  }

  function selectAllTags() {
    _selectedTagIds = _monitoredTagsCache.map(t => String(t.id));
    try { localStorage.setItem('topShelfScenesSelectedTags', JSON.stringify(_selectedTagIds)); } catch(_) {}
    _renderTagFilterList();
    _updateTagFilterBadge();
    loadFeed();
  }

  function clearSelectedTags() {
    _selectedTagIds = [];
    try { localStorage.setItem('topShelfScenesSelectedTags', JSON.stringify(_selectedTagIds)); } catch(_) {}
    _renderTagFilterList();
    _updateTagFilterBadge();
    loadFeed();
  }

  document.addEventListener('click', function(e) {
    const dd = document.getElementById('tagFilterDropdown');
    const btn = document.getElementById('tagFilterBtn');
    if (!dd || dd.style.display !== 'block') return;
    if (btn && (btn === e.target || btn.contains(e.target))) return;
    if (dd.contains(e.target)) return;
    dd.style.display = 'none';
  });

  /** Warm VHS chrome after the feed is up — idle, one PNG per slice. */
  function _scheduleMovieDetailChromePreload() {
    if (typeof window.tsPreloadMovieDetailChrome !== 'function') return;
    window.tsPreloadMovieDetailChrome({ timeout: 15000 });
  }

  /** User is about to open a movie/JAV popup — start the queue without idle wait. */
  function _preloadMovieDetailChromeOnIntent() {
    if (typeof window.tsPreloadMovieDetailChrome !== 'function') return;
    window.tsPreloadMovieDetailChrome({ immediate: true });
  }

  function setScenesFeedMode(mode) {
    const m = _FEED_MODES.includes(mode) ? mode : 'studios';
    _scenesFeedMode = m;
    try { localStorage.setItem('topShelfScenesFeedMode', m); } catch (_) {}
    _applyFeedModeToggleUI();
    _updateTagFilterBadge();
    if (m === 'tags') _ensureMonitoredTagsLoaded().then(() => loadFeed());
    else loadFeed();
  }
  // Toolbar refresh button — dumps browser-side _feedCache and forces the
  // server to clear its feed caches plus the durable SQLite feed pools
  // (``feed_display_pools``), then re-pulls the active feed.
  async function refreshScenesFeed() {
    try { _feedCache.clear(); } catch (_) {}
    const btn = document.getElementById('feedRefreshBtn');
    const ico = btn ? btn.querySelector('i') : null;
    let loaderEl = null;
    if (btn) {
      btn.disabled = true;
      if (ico) ico.style.display = 'none';
      loaderEl = document.createElement('span');
      loaderEl.className = 'loader loader--btn';
      loaderEl.setAttribute('role', 'status');
      loaderEl.setAttribute('aria-label', 'Loading');
      btn.appendChild(loaderEl);
    }
    try {
      await loadFeed({ force: true });
    } finally {
      if (btn) btn.disabled = false;
      if (loaderEl && loaderEl.parentNode) loaderEl.remove();
      if (ico) ico.style.display = '';
    }
  }
  window.refreshScenesFeed = refreshScenesFeed;

  function clearScenesSearch() {
    ['scenesSrchTitle','scenesSrchPerformer','scenesSrchStudio','scenesSrchTag','scenesSrchDateFrom','scenesSrchDateTo'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.value = '';
    });
    const status = document.getElementById('scenesSearchStatus');
    if (status) {
      status.textContent = '';
      status.classList.remove('is-error');
    }
    const grid = document.getElementById('scenesGrid');
    if (grid) {
      grid.innerHTML = '<div class="empty">Fill in the form and search across StashDB, ThePornDB, FansDB, and JAVStash.</div>';
      delete grid.dataset.searchPopulated;
    }
    _updateScenesSrchSourceState();
  }

  // Multi-source toggle: switch the "All / None" pill label based on
  // current selection and disable the Search action when nothing is
  // selected. Hint text spells out exactly why the button is dead so
  // users don't click a greyed-out button and wonder.
  const _SCENES_SRCH_SRC_IDS = [
    'scenesSrchSrcTpdb',
    'scenesSrchSrcStashdb',
    'scenesSrchSrcFansdb',
    'scenesSrchSrcJavstash',
  ];
  function _scenesSrchSelectedSources() {
    return _SCENES_SRCH_SRC_IDS
      .map(id => document.getElementById(id))
      .filter(el => el && el.checked);
  }
  function _updateScenesSrchSourceState() {
    const checked = _scenesSrchSelectedSources();
    const total = _SCENES_SRCH_SRC_IDS.length;
    const allBtn = document.getElementById('scenesSrchSrcAll');
    if (allBtn) {
      // "All" when nothing / some are off; "None" when every box is on.
      // Click flips to whichever isn't current.
      allBtn.textContent = (checked.length === total) ? 'None' : 'All';
    }
    const goBtn = document.getElementById('scenesSrchGo');
    if (goBtn) {
      const dead = checked.length === 0;
      goBtn.disabled = dead;
      goBtn.setAttribute('aria-disabled', dead ? 'true' : 'false');
      goBtn.title = dead ? 'Pick at least one source above' : 'Search (Enter)';
    }
    const status = document.getElementById('scenesSearchStatus');
    if (status && !checked.length) {
      status.textContent = 'No sources selected — pick at least one.';
      status.classList.add('is-error');
    } else if (status && status.classList.contains('is-error')) {
      status.textContent = '';
      status.classList.remove('is-error');
    }
  }
  function toggleScenesSrchAllSources() {
    const checked = _scenesSrchSelectedSources();
    const total = _SCENES_SRCH_SRC_IDS.length;
    const targetState = checked.length < total;  // any off → flip all on; all on → flip all off
    _SCENES_SRCH_SRC_IDS.forEach(id => {
      const el = document.getElementById(id);
      if (el) el.checked = targetState;
    });
    _updateScenesSrchSourceState();
  }
  // Expose so the inline `onclick` handlers in scenes.html bind correctly.
  window.toggleScenesSrchAllSources = toggleScenesSrchAllSources;
  window._updateScenesSrchSourceState = _updateScenesSrchSourceState;

  async function runScenesSearch() {
    // Scene search keys off performer / studio / tag / date — title
    // is intentionally not a scene-search field (movies use their own
    // title-based search panel below). Backend treats absent `title`
    // as no filter, so we just don't send one.
    const status = document.getElementById('scenesSearchStatus');
    const setStatus = (txt, isErr) => {
      if (!status) return;
      status.textContent = txt;
      status.classList.toggle('is-error', !!isErr);
    };
    const payload = {
      title:     document.getElementById('scenesSrchTitle')?.value.trim() || '',
      performer: document.getElementById('scenesSrchPerformer')?.value.trim() || '',
      studio:    document.getElementById('scenesSrchStudio')?.value.trim() || '',
      tag:       document.getElementById('scenesSrchTag')?.value.trim() || '',
      date_from: document.getElementById('scenesSrchDateFrom')?.value.trim() || '',
      date_to:   document.getElementById('scenesSrchDateTo')?.value.trim() || '',
      sources: [],
      per_page: 30,
    };
    if (document.getElementById('scenesSrchSrcTpdb')?.checked) payload.sources.push('tpdb');
    if (document.getElementById('scenesSrchSrcStashdb')?.checked) payload.sources.push('stashdb');
    if (document.getElementById('scenesSrchSrcFansdb')?.checked) payload.sources.push('fansdb');
    if (document.getElementById('scenesSrchSrcJavstash')?.checked) payload.sources.push('javstash');
    if (!payload.sources.length) {
      setStatus('No sources selected — pick at least one.', true);
      return;
    }
    if (!payload.title && !payload.performer && !payload.studio && !payload.tag && !payload.date_from && !payload.date_to) {
      setStatus('Fill in at least one field above.', true);
      return;
    }
    const grid = document.getElementById('scenesGrid');
    setStatus('Searching ' + payload.sources.map(s => s.toUpperCase()).join(' · ') + '…', false);
    grid.innerHTML = sceneGridSkeleton();
    try {
      const r = await fetch('/api/scenes/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const d = await r.json();
      if (d && d.error) {
        grid.innerHTML = `<div class="empty">${esc(d.error)}</div>`;
        setStatus(d.error, true);
        return;
      }
      const scenes = d.scenes || [];
      if (scenes.length) {
        setStatus(`${scenes.length} result${scenes.length === 1 ? '' : 's'} from ${(d.sources_queried || payload.sources).join(' · ').toUpperCase()}`, false);
      } else {
        setStatus('No scenes matched. Try broader search terms.', false);
      }
      if (!scenes.length) {
        grid.innerHTML = '<div class="empty">No scenes matched. Try broader search terms.</div>';
        grid.dataset.searchPopulated = '1';
        return;
      }
      renderSceneGrid(scenes, true);
      grid.dataset.searchPopulated = '1';
    } catch (e) {
      grid.innerHTML = `<div class="empty">Search failed: ${esc(e.message || e)}</div>`;
      setStatus(`Search failed: ${e.message || e}`, true);
    }
  }

  function _shortIndexer(name) {
    const map = {
      'theporndb': 'TPDB', 'the porn db': 'TPDB', 'porndb': 'TPDB',
      'stashdb': 'StashDB', 'fansdb': 'FansDB',
    };
    const lower = (name || '').toLowerCase().trim();
    return map[lower] || name;
  }

  function setDetailBg(imageUrl) {
    const el = document.getElementById('detailBgImage');
    if (imageUrl) {
      el.innerHTML = `<img class="detail-bg-image" src="${esc(imageUrl)}" onerror="this.remove()" loading="lazy">`;
    } else {
      el.innerHTML = '';
    }
  }

  const _dbSources = ['TPDB', 'StashDB', 'FansDB', 'JAVStash'];
  function detailLoadingSkeleton() {
    // Rendered into #detailMeta (the text column under the name), not the
    // whole panel — so don't include a poster placeholder here, otherwise
    // it appears as a phantom box next to the real poster image.
    return `
      <div style="flex:1;min-width:0">
        <div class="skeleton-line" style="width:42%"></div>
        <div class="skeleton-line" style="width:68%"></div>
        <div style="display:flex;gap:6px;flex-wrap:wrap;margin:12px 0 16px">
          <div class="skeleton-chip"></div><div class="skeleton-chip"></div><div class="skeleton-chip"></div>
        </div>
        <div class="skeleton-line" style="width:100%"></div>
        <div class="skeleton-line" style="width:96%"></div>
        <div class="skeleton-line" style="width:88%"></div>
        <div class="skeleton-line" style="width:74%"></div>
      </div>`;
  }

  function sceneGridSkeleton(count = 30) {
    return Array.from({length: count}, () => `
      <div class="scene-card">
        <div class="skeleton-box" style="width:100%;aspect-ratio:16/9">
          <span class="loader loader--tile" aria-hidden="true"></span>
        </div>
        <div class="scene-info">
          <div class="skeleton-line" style="width:84%"></div>
          <div class="skeleton-line" style="width:62%"></div>
        </div>
        <div class="scene-actions">
          <div class="skeleton-box" style="height:28px;border-radius:4px;flex:1"></div>
          <div class="skeleton-box" style="height:28px;border-radius:4px;flex:1"></div>
        </div>
      </div>`).join('');
  }

  const _LOCAL_LOGOS = { 'TPDB': 'tpdb', 'ThePornDB': 'tpdb', 'StashDB': 'stashdb', 'FansDB': 'fansdb', 'JAVStash': 'javstash', 'javstash': 'javstash', 'TMDB': 'tmdb', 'Freeones': 'freeones', 'IAFD': 'iafd', 'Babepedia': 'babepedia', 'Coomer': 'coomer' };
  // Inline badge used by the /scenes + /discover search results to
  // show which source a row came from. Swaps the raw text label for
  // the source's logo when one exists; otherwise prints the label so
  // unknown sources still read.
  function sourceBadgeHtml(source) {
    if (!source) return '';
    const key = _LOCAL_LOGOS[source];
    return key
      ? `<img class="result-source-logo" src="/static/logos/${key}.webp" alt="${esc(source)}" title="${esc(source)}" style="height:14px;width:auto;vertical-align:middle">`
      : esc(source);
  }
  function renderLinks(links) {
    if (!links || !links.length) return '';
    return links.map(l => {
      const isDb = _dbSources.includes(l.label);
      const cls = isDb ? 'detail-link db-link' : 'detail-link';
      const logoKey = _LOCAL_LOGOS[l.label];
      const iconHtml = logoKey
        ? `<img src="/static/logos/${logoKey}.webp" alt="${esc(l.label)}" style="height:16px;width:auto;vertical-align:middle;opacity:0.9">`
        : (() => { try { const d = new URL(l.url).hostname.replace('www.',''); return `<img src="https://www.google.com/s2/favicons?domain=${d}&sz=32" onerror="this.remove()">`; } catch { return ''; } })();
      return `<a class="${cls}" href="${esc(l.url)}" target="_blank" title="${esc(l.label)}">${iconHtml}</a>`;
    }).join('');
  }








  // ── Poster overlay ───────────────────────────────────────────────────

  // Wired to `#detailPoster`'s inline onclick. The big detail headshot
  // on /discover doubles as the entry point into the performer popup:
  // for `currentType === 'performer'` we resolve the active result's
  // {stashId, tpdbId, fansdbId, javstashId, name} and hand it to the
  // shared popup. Studios and movies fall back to the full-size image
  // overlay (the previous behaviour for every type).

  // ── Image overlay ────────────────────────────────────────────────────

  function openImageOverlay(url) {
    if (!url) return;
    document.getElementById('imgOverlayImg').src = url;
    document.getElementById('imgOverlay').style.display = 'flex';
  }

  function closeImageOverlay() {
    document.getElementById('imgOverlay').style.display = 'none';
    document.getElementById('imgOverlayImg').src = '';
  }


  document.addEventListener('keydown', e => {
    if (e.key === 'Escape') {
      closeImageOverlay();
      if (typeof window.closeMagCarousel === 'function') window.closeMagCarousel();
      closeSceneOverlay();
    } else if (document.getElementById('magCarousel')?.style.display === 'flex') {
      if (e.key === 'ArrowLeft' && typeof window.magCarouselNav === 'function') window.magCarouselNav(-1);
      if (e.key === 'ArrowRight' && typeof window.magCarouselNav === 'function') window.magCarouselNav(1);
    }
  });

  // ── Scenes ───────────────────────────────────────────────────────────

  let _sceneSources = {};  // cached scenes by source


  // Tracks which DB source ('tpdb'|'stashdb'|'fansdb') the visible
  // scene grid was rendered from. When the user clicks Grab on a
  // search result we tag the download with this so /downloads can
  // later render the source scene's poster on the tile.
  let _currentSceneSource = 'tpdb';

  function openSceneOverlay(scene) {
    if (!scene) return;
    if (typeof window.ensurePopupBundle === 'function') {
      window.ensurePopupBundle().catch(function () {});
    }
    window._sceneOverlay = scene;
    document.getElementById('sceneOverlayTitle').textContent = scene.title || 'Scene';
    // Prefer the gender-filtered performer list (`hover_performers`)
    // populated by the backend per the user's Content Filter settings —
    // when gay content is excluded, for example, this drops the male
    // performers so the auto-built Prowlarr query doesn't accidentally
    // bias the indexer search toward the excluded gender.
    const _perfList = (Array.isArray(scene.hover_performers) && scene.hover_performers.length)
      ? scene.hover_performers
      : (scene.performer || '').split(',').map(x => x.trim()).filter(Boolean);
    const _perfStr = _perfList.join(' ').trim();
    document.getElementById('sceneOverlayQuery').value = [_perfStr, scene.title || ''].filter(Boolean).join(' ');
    const sceneTags = Array.isArray(scene.tags) ? scene.tags : [];
    // Tags now live in the right column (under the performer info) and
    // are capped to the image height — see `.scene-overlay-tags-capped`
    // in scenes.html / discover.html for the hover-to-expand behaviour.
    // The trailing `<div class="scene-overlay-tags-more-pill">` is the
    // "+N more" affordance; JS measures the chips post-render and toggles
    // `.is-shown` (with the actual count) only when overflow is present.
    const tagsHtml = sceneTags.length
      ? `<div class="scene-tag-chips scene-overlay-tags-capped">${sceneTags.map(t => `<span class="scene-card-tag-chip">${esc(t)}</span>`).join('')}<div class="scene-overlay-tags-more-pill" data-tags-more-pill></div></div>`
      : '';
    const sceneDesc = (scene.description || '').trim();
    // Plot now sits under the image in a full-width strip below the
    // grid so it can run the entire width of the popup.
    const descHtml = sceneDesc
      ? `<div class="scene-overlay-synopsis scene-overlay-plot-below">${esc(sceneDesc)}</div>`
      : '';
    // DB links (TPDB / StashDB / FansDB) sit beside the Search button.
    // Each one shows only if the scene has a usable ID for that source —
    // we resolve from explicit `tpdb_id` / `stash_id` / `fansdb_id`
    // fields first, then fall back to `scene.id` interpreted via
    // `scene.source` (since lists return the per-source ID under `id`).
    (function _wireSceneOverlayDbLinks() {
      const sceneSrc = String(scene.source || '').toLowerCase();
      const tpdbId   = String(scene.tpdb_id   || (sceneSrc.includes('tpdb')   ? (scene.id || '') : '') || '').trim();
      const stashId  = String(scene.stash_id  || (sceneSrc === 'stashdb'      ? (scene.id || '') : '') || '').trim();
      const fansdbId = String(scene.fansdb_id || (sceneSrc === 'fansdb'       ? (scene.id || '') : '') || '').trim();
      const javId    = String(scene.javstash_id || (sceneSrc === 'javstash' ? (scene.id || '') : '') || '').trim();
      const cfgs = [
        { el: document.getElementById('sceneOverlayTpdbLink'),    href: scene.link || (tpdbId   ? 'https://theporndb.net/scenes/' + tpdbId   : ''), id: tpdbId },
        { el: document.getElementById('sceneOverlayStashdbLink'), href: stashId  ? 'https://stashdb.org/scenes/' + stashId : '', id: stashId },
        { el: document.getElementById('sceneOverlayFansdbLink'),  href: fansdbId ? 'https://fansdb.cc/scenes/'  + fansdbId : '', id: fansdbId },
        { el: document.getElementById('sceneOverlayJavstashLink'), href: javId ? 'https://javstash.org/scenes/' + javId : '', id: javId },
      ];
      cfgs.forEach(({ el, href, id }) => {
        if (!el) return;
        if (href && id) {
          el.href = href;
          el.classList.add('is-visible');
          el.style.display = 'inline-flex';
        } else {
          el.removeAttribute('href');
          el.classList.remove('is-visible');
          el.style.display = 'none';
        }
      });
    })();
    // Inline the scene-card structure so the popup image picks up the
    // same theming chain as /scenes tiles (duotone tint, VHS-theme
    // overrides, studio-logo overlay). `.img-load` wraps the thumb
    // with the duo-tint sibling that the cascading rules target.
    // `.scene-card--popup` is the modifier that flips the normally
    // hover-gated overlays (headshots, wanted-eye, tags, performer
    // label) to always-visible — the popup has no hover state, the
    // user has already clicked, so we show the card chrome flat.
    const _ovStudio = (scene.studio || '').trim();
    const _ovTitle  = (scene.title || '').trim();
    const _feedCleanPopup = ['performers', 'studios', 'tags'].includes(_scenesFeedMode);
    const _ovStudioLogo = (!_feedCleanPopup && (_ovStudio || _ovTitle))
      ? `<img class="scene-studio-logo" src="/api/studio-logo?name=${encodeURIComponent(_ovStudio)}&q=${encodeURIComponent(_ovTitle)}" alt="" loading="lazy" onload="this.closest('.scene-card')?.classList.add('has-studio-logo')" onerror="this.remove()">`
      : '';
    // Wanted ("watch") button — same data-attrs the document-level
    // wanted-toggle handler in scenes-common.js (line ~125) listens
    // for, so clicking it queues the scene to Wanted exactly like the
    // grid eye does. Pre-resolved external_id derives from
    // scene.{tpdb,stash,fansdb,javstash}_id with `scene.source` as
    // fallback when only the canonical `id` is present.
    const _ovSrcRaw = String(scene.source || '').toLowerCase();
    const _ovSrcNorm = _ovSrcRaw.startsWith('search_') ? _ovSrcRaw.slice(7) : _ovSrcRaw;
    const _ovIdBySource = (
      _ovSrcNorm === 'stashdb'  ? scene.stash_id || scene.id || '' :
      _ovSrcNorm === 'fansdb'   ? scene.fansdb_id || scene.id || '' :
      _ovSrcNorm === 'javstash' ? scene.javstash_id || scene.id || '' :
      _ovSrcNorm.includes('tpdb') ? scene.tpdb_id || scene.id || '' :
      scene.id || ''
    );
    const _ovWantedSrc = _ovSrcNorm.includes('tpdb') ? 'tpdb'
                       : _ovSrcNorm === 'stashdb'  ? 'stashdb'
                       : _ovSrcNorm === 'fansdb'   ? 'fansdb'
                       : _ovSrcNorm === 'javstash' ? 'javstash'
                       : 'tpdb';
    // JAVStash releases filed from the JAV / movie-style feeds belong in the
    // movie column on /index (same as TPDB movies), not the scene column.
    const _ovWantedKind = (_ovSrcNorm === 'javstash' || _scenesFeedMode === 'jav') ? 'movie' : 'scene';
    const _ovIsWanted = !!_ovIdBySource && _wantedKeys.has(_wkey(_ovWantedKind, _ovWantedSrc, String(_ovIdBySource)));
    const _ovWantedBtn = _ovIdBySource
      ? `<button class="scene-wanted-btn${_ovIsWanted ? ' is-wanted' : ''}" data-wanted-kind="${esc(_ovWantedKind)}" data-wanted-source="${esc(_ovWantedSrc)}" data-wanted-id="${esc(String(_ovIdBySource))}" title="${_ovIsWanted ? 'In your Wanted list — click to remove' : 'Add to Wanted'}" aria-pressed="${_ovIsWanted ? 'true' : 'false'}"><i class="fa-solid fa-eye"></i></button>`
      : '';
    // Performer label at the bottom — mirrors the on-card pill so the
    // image reads as a /scenes tile even outside its grid context.
    const _ovPerfLabel = (!_feedCleanPopup && (scene.performer || '').trim())
      ? `<div class="scene-performer-label"><span class="perf-hl">${esc(scene.performer)}</span></div>`
      : '';
    // Tag chips overlay along the bottom of the image (in addition to
    // the structured tag pill cap in the right column). Capped at 10
    // chips so the overlay never crowds the image; the `+N more`
    // affordance carries the rest. Hidden by the existing
    // .scene-card-tags-hover opacity rule unless `.scene-card--popup`
    // is on the parent.
    const _ovImgTagsCap = 10;
    const _ovImgTags = sceneTags.slice(0, _ovImgTagsCap);
    const _ovImgExtra = sceneTags.length - _ovImgTags.length;
    const _ovImgTagsHtml = (!_feedCleanPopup && sceneTags.length)
      ? `<div class="scene-card-tags-hover">${_ovImgTags.map(t => `<span class="scene-card-tag-chip">${esc(t)}</span>`).join('')}${_ovImgExtra > 0 ? `<span class="scene-card-tag-chip" style="opacity:0.82">+${_ovImgExtra} more</span>` : ''}</div>`
      : '';
    (function _wireSceneOverlayStudioLogoRow() {
      const row = document.getElementById('sceneOverlayStudioLogoRow');
      const img = document.getElementById('sceneOverlayStudioLogo');
      if (!row || !img) return;
      if (_feedCleanPopup && (_ovStudio || _ovTitle)) {
        img.src = `/api/studio-logo?name=${encodeURIComponent(_ovStudio)}&q=${encodeURIComponent(_ovTitle)}`;
        img.alt = _ovStudio || '';
        img.onerror = function () { row.classList.remove('is-visible'); row.setAttribute('aria-hidden', 'true'); };
        img.onload = function () { row.classList.add('is-visible'); row.removeAttribute('aria-hidden'); };
        row.classList.add('is-visible');
        row.removeAttribute('aria-hidden');
      } else {
        row.classList.remove('is-visible');
        row.setAttribute('aria-hidden', 'true');
        img.removeAttribute('src');
        img.alt = '';
      }
    })();
    document.getElementById('sceneOverlayMain').innerHTML = `
      <div class="scene-overlay-grid">
        <div>
          <div class="scene-card scene-card--popup scene-overlay-thumb-card" data-performer="${esc(scene.performer || '')}">
            <div class="img-load">
              <span class="loader loader--tile" aria-hidden="true"></span>
              <img class="scene-thumb scene-overlay-thumb" src="${esc(scene.thumb || '/static/img/missing.webp')}" loading="lazy" onload="this.closest('.img-load')?.classList.add('ready')" onerror="this.onerror=null;this.src='/static/img/missing.webp';this.closest('.img-load')?.classList.add('ready');">
              <div class="duo-tint" aria-hidden="true"></div>
              ${_ovStudioLogo}
              ${_ovWantedBtn}
              ${_ovPerfLabel}
            </div>
            ${_ovImgTagsHtml}
          </div>
        </div>
        <div>
          <div id="sceneLibPerfs"></div>
          <div style="font-size:12px;color:var(--dim);line-height:1.8">
            ${scene.date ? `<div>Date: <span style="color:var(--text)">${esc(scene.date)}</span></div>` : ''}
            ${scene.studio ? `<div>Studio: <span style="color:var(--text)">${esc(capDisplayName(scene.studio))}</span></div>` : ''}
            ${(() => {
              // Performer line — prefer display_performers (with per-
              // performer gender) so each name renders with its
              // colour-coded badge. Falls back to the plain comma
              // string when display_performers is absent.
              const dp = Array.isArray(scene.display_performers) ? scene.display_performers : null;
              if (dp && dp.length) {
                // /api/scenes/recent embeds tpdb_id when the source is
                // TPDB; falls back to the unified `id` field on stash-
                // box-sourced lists. Pass both so the popup endpoint
                // can pick whichever matches the source it has data for.
                const sceneSrcRaw = String(scene.source || '').toLowerCase();
                const sceneSrc = sceneSrcRaw.startsWith('search_') ? sceneSrcRaw.slice(7) : sceneSrcRaw;
                const html = dp.map(o => {
                  const nm = capDisplayName(o.name || '');
                  const tpdbId = o.tpdb_id || (sceneSrc.includes('tpdb') ? (o.id || o._id || '') : '');
                  const stashId = (sceneSrc === 'stashdb' || sceneSrc === 'fansdb' || sceneSrc === 'javstash') ? (o.id || o.stash_id || '') : (o.stash_id || '');
                  const attrs = window.performerLinkAttrs(o.name || '', {
                    gender: o.gender,
                    stashId: stashId,
                    tpdbId: tpdbId,
                  });
                  // Star is in the user's library when the raw name (or
                  // one of its aliases) hits the entityByName map — this
                  // catches Japanese / Cyrillic stars the regex-based
                  // highlighter can't tokenize.
                  const libEntry = (typeof window.libraryEntryForPerformerName === 'function')
                    ? window.libraryEntryForPerformerName(o.name || '')
                    : null;
                  const cls = [
                    attrs ? 'perf-name-link' : '',
                    libEntry ? 'is-library-perf' : '',
                  ].filter(Boolean).join(' ');
                  // Wrap NAME + BADGE in one clickable span so clicks on
                  // either part fire the popup.
                  return `<span${attrs ? ' ' + attrs : ''}${cls ? ' class="' + cls + '"' : ''}>${esc(nm)}${genderBadge(o.gender)}</span>`;
                }).join(', ');
                return `<div>Star: <span style="color:var(--text)">${html}</span></div>`;
              }
              return scene.performer
                ? `<div>Star: <span style="color:var(--text)">${performerCsvHtml(scene.performer)}</span></div>`
                : '';
            })()}
          </div>
          ${tagsHtml}
        </div>
      </div>
      ${descHtml}`;
    const sceneOvEl = document.getElementById('sceneOverlay');
    if (sceneOvEl) {
      const rawSrc = String(scene.source || '').toLowerCase();
      const normSrc = rawSrc.startsWith('search_') ? rawSrc.slice(7) : rawSrc;
      const isJavScene = normSrc === 'javstash' || normSrc.includes('javstash') || String(scene.javstash_id || '').trim() !== '';
      sceneOvEl.setAttribute('lang', isJavScene ? 'ja' : 'en');
      sceneOvEl.classList.toggle('scene-overlay--feed-clean', _feedCleanPopup);
      sceneOvEl.classList.add('open');
    }
    // Lazy-fetch library performer headshots so the popup image shows
    // the same circular-avatar row the grid cards do (see
    // ensureCardHeadshots). Skipped on Performers / Studios / Vices
    // feeds — headshots live in the right column only.
    if (!_feedCleanPopup && typeof ensureCardHeadshots === 'function') {
      const popupCard = document.querySelector('#sceneOverlayMain .scene-card--popup');
      if (popupCard) ensureCardHeadshots(popupCard, scene.performer || '');
    }
    // Tag-cap overflow detection: after layout, count chips whose top
    // edge sits below the container's clip line. If any do, show the
    // "+N more" pill so users know there's hidden content to hover.
    requestAnimationFrame(() => {
      const cap = document.querySelector('.scene-overlay-tags-capped');
      const pill = cap && cap.querySelector('[data-tags-more-pill]');
      if (!cap || !pill) return;
      const chips = Array.from(cap.querySelectorAll('.scene-card-tag-chip'));
      if (!chips.length) { pill.classList.remove('is-shown'); return; }
      const capRect = cap.getBoundingClientRect();
      const cutoff = capRect.bottom - 4;
      let hidden = 0;
      for (const chip of chips) {
        const r = chip.getBoundingClientRect();
        if (r.top >= cutoff) hidden++;
      }
      if (hidden > 0) {
        pill.textContent = '+' + hidden + ' more';
        pill.classList.add('is-shown');
      } else {
        pill.classList.remove('is-shown');
      }
    });
    // Async: fetch library performer headshots
    if (scene.performer) {
      const names = scene.performer;
      fetch(`/api/performers/headshots-by-name?names=${encodeURIComponent(names)}`, { credentials: 'same-origin' })
        .then(r => r.json())
        .then(d => {
          const perfs = d.performers || [];
          const el = document.getElementById('sceneLibPerfs');
          if (!el || !perfs.length) return;
          el.innerHTML = `<div class="lib-perfs-row">${perfs.map(p => {
            const img = p.headshot_url
              ? `<img src="${esc(p.headshot_url)}" alt="" loading="lazy" onerror="this.replaceWith(Object.assign(document.createElement('div'),{className:'lib-perf-ph',innerHTML:'<i class=\\'fa-solid fa-user\\'></i>'}))">`
              : `<div class="lib-perf-ph"><i class="fa-solid fa-user"></i></div>`;
            const attrs = window.performerLinkAttrs(p.name, { gender: p.gender, libraryRowId: p.row_id || p.id });
            return `<div class="lib-perf-hs" title="${esc(p.name)}"${attrs ? ' ' + attrs : ''}>${img}<div class="lib-perf-hs-name">${esc(p.name)}</div></div>`;
          }).join('')}</div>`;
        })
        .catch(() => {});
    }
  }

  function closeSceneOverlay() {
    const so = document.getElementById('sceneOverlay');
    if (so) {
      so.classList.remove('open');
      so.classList.remove('scene-overlay--feed-clean');
      so.removeAttribute('lang');
    }
    const logoRow = document.getElementById('sceneOverlayStudioLogoRow');
    const logoImg = document.getElementById('sceneOverlayStudioLogo');
    if (logoRow) {
      logoRow.classList.remove('is-visible');
      logoRow.setAttribute('aria-hidden', 'true');
    }
    if (logoImg) {
      logoImg.removeAttribute('src');
      logoImg.alt = '';
    }
    window._sceneOverlay = null;
  }

  // ── Prowlarr (overlay) ────────────────────────────────────────────────

  // Scene-overlay search button now opens the unified Prowlarr search
  // popup (window.openProwlarrSearchPopup, defined in ts-utils.js) so
  // every search across the app shares one results UI / row layout.
  // Pulls the typed query and the scene's studio/performer context for
  // the popup's automatic title/studio fan-out.
  function openSceneOverlayProwlarrPopup() {
    const q = (document.getElementById('sceneOverlayQuery').value || '').trim();
    const _scene = window._sceneOverlay || {};
    const _perfList = (Array.isArray(_scene.hover_performers) && _scene.hover_performers.length)
      ? _scene.hover_performers
      : (_scene.performer || '').split(',').map(x => x.trim()).filter(Boolean);
    if (!q && !_scene.title) return;
    if (typeof window.openProwlarrSearchPopup !== 'function') return;
    window.openProwlarrSearchPopup({
      title:      q || _scene.title || '',
      studio:     _scene.studio || '',
      performers: _perfList.join(', '),
      thumb_url:  _scene.thumb || '',
      kind:       'scene',
    });
  }
  window.openSceneOverlayProwlarrPopup = openSceneOverlayProwlarrPopup;

  function truncateFilename(name, maxLen) {
    if (!name || name.length <= maxLen) return esc(name || '');
    return esc(name.slice(0, maxLen - 3)) + '...';
  }












  // ── Feed (default scenes on page load) ────────────────────────────────

  //: Per-mode feed cache — switching between Your Feed / Vices /
  //: Studios / Performers / Movies refetches the same data every
  //: time, which for Movies takes multiple seconds and for the
  //: scene feeds makes the grid flash blank and rebuild. Keep the
  //: last response in memory keyed by "mode|filters"; re-render
  //: from cache when it's fresh, refetch in the background to refresh.
  //: ``_feedCacheGet`` only skips ``fetch`` when younger than this TTL;
  //: older entries still paint instantly (stale-while-revalidate) until
  //: the new response lands.
  const _FEED_CACHE_TTL_MS = 15 * 60 * 1000;
  //: Scene/movie feed payloads are merged and capped on the server
  //: (SQLite ``feed_display_pools``); the client only caches the last
  //: JSON for instant tab switches and stale-while-revalidate paint.

  /** Two ``requestAnimationFrame`` ticks — lets layout/paint run before ``fetch``. */
  function _yieldTwoFrames() {
    return new Promise(function (resolve) {
      requestAnimationFrame(function () {
        requestAnimationFrame(resolve);
      });
    });
  }
  const _feedCache = new Map();  // key -> { data, ts, title }

  function _feedCacheKey() {
    if (_scenesFeedMode === 'tags') {
      const ids = Array.isArray(_selectedTagIds) ? _selectedTagIds : [];
      return `tags|${ids.slice().sort().join(',')}`;
    }
    return _scenesFeedMode;
  }

  function _feedCachePeekAny(key) {
    return _feedCache.get(key) || null;
  }

  function _feedCacheGet(key) {
    const hit = _feedCache.get(key);
    if (!hit) return null;
    if (Date.now() - hit.ts > _FEED_CACHE_TTL_MS) return null;
    return hit;
  }

  function _feedCachePut(key, data, titleText) {
    _feedCache.set(key, { data, ts: Date.now(), title: titleText });
  }

  function _feedCacheInvalidate(key) {
    if (key) _feedCache.delete(key);
    else _feedCache.clear();
  }

  function _renderFeedCacheHit(hit, grid, title, pag) {
    if (!hit || !grid) return;
    if (title && hit.title) tsSetPageTitleText(title, hit.title, 'scenes');
    if (pag) pag.style.display = 'none';
    const sp = document.getElementById('scenesSearchPanel');
    if (sp) sp.style.display = 'none';
    if (_scenesFeedMode === 'movies') {
      grid.className = 'movie-grid';
      if (hit.data && hit.data.movies) {
        renderMovieGrid(hit.data.movies, grid);
        if (hit.data.movies.length) _scheduleMovieDetailChromePreload();
      }
    } else if (_scenesFeedMode === 'jav') {
      grid.className = 'movie-grid';
      if (hit.data && Array.isArray(hit.data.scenes)) {
        if (!hit.data.scenes.length && hit.data.emptyHtml) {
          grid.innerHTML = hit.data.emptyHtml;
        } else if (hit.data.scenes.length) {
          renderJavMovieGrid(hit.data.scenes, grid);
          _scheduleMovieDetailChromePreload();
        } else {
          grid.innerHTML = '<div class="empty">No scenes found</div>';
        }
      }
    } else {
      grid.className = 'scene-grid';
      if (hit.data && Array.isArray(hit.data.scenes)) {
        if (!hit.data.scenes.length && hit.data.emptyHtml) {
          grid.innerHTML = hit.data.emptyHtml;
        } else if (hit.data.scenes.length) {
          renderSceneGrid(hit.data.scenes, true);
        } else {
          grid.innerHTML = '<div class="empty">No scenes found</div>';
        }
      }
    }
  }

  /** Empty-state HTML for prefetched scene feeds — must match ``loadFeed``. */
  const _PREFETCH_SCENE_EMPTY = {
    studios: '<div class="empty">No studio feed scenes — add your StashDB API key under Settings → Databases, and link library studios to StashDB under Favourites so recent releases from those studios can be loaded.</div>',
    performers: '<div class="empty">No performer feed scenes — add your StashDB and/or FansDB API keys under Settings → Databases, and link library performers to StashDB and/or FansDB under Favourites so their recent scene credits can be loaded.</div>',
    jav: '<div class="empty">No JAV scenes — add your JAVStash API key under Settings → Databases, or check access to javstash.org.</div>',
    tagsNoScenes: '<div class="empty">No scenes for the selected vices — add your StashDB API key under Settings → Databases, and ensure each vice uses StashDB tags (open Settings → Content Filters → Vices and re-add tags if you previously used ThePornDB).</div>',
  };

  /**
   * After the active feed loads, warm every *other* tab in `_feedCache`
   * (same client-side TTL as a normal visit) so toggling Movies / JAV / Performers /
   * Studios / Vices is instant without extra round-trips. Skips Search and
   * the currently visible mode. Fire-and-forget — failures are ignored.
   */
  function _maybePrefetchInactiveFeeds() {
    if (!document.getElementById('scenesGrid')) return;
    if (_scenesFeedMode === 'search') return;
    // Skip entirely on slow / saveData connections — these fire 4–5
    // upstream-backed feed calls each. Avoid burning the user's mobile
    // budget without consent.
    try {
      const c = navigator.connection || navigator.mozConnection || navigator.webkitConnection;
      if (c) {
        if (c.saveData) return;
        const eff = String(c.effectiveType || '');
        if (eff === 'slow-2g' || eff === '2g' || eff === '3g') return;
      }
    } catch (_) { /* connection API not available — proceed */ }
    const cur = _scenesFeedMode;
    // THUNKS: zero-arg fns returning a promise. They only fire when
    // we await them in sequence below — prevents the 4–5 upstream
    // feed calls firing simultaneously on idle.
    const thunks = [];

    if (cur !== 'movies' && !_feedCacheGet('movies')) {
      thunks.push(() =>
        fetch('/api/movies/tpdb/latest?page=1')
          .then(r => r.json())
          .then(d => {
            if (!d || typeof d !== 'object') return;
            const movies = d.results || [];
            if (movies.length) _feedCachePut('movies', { movies }, _scenesFeedTitleForMode('movies'));
          })
          .catch(() => {})
      );
    }

    function prefetchSceneMode(mode) {
      if (cur === mode || _feedCacheGet(mode)) return;
      const title = _scenesFeedTitleForMode(mode);
      thunks.push(() =>
        fetch('/api/scenes/feed?mode=' + encodeURIComponent(mode))
          .then(r => r.json())
          .then(d => {
            if (!d || typeof d !== 'object' || !Array.isArray(d.scenes)) return;
            if (!d.scenes.length) {
              const emptyHtml = mode === 'jav' ? _PREFETCH_SCENE_EMPTY.jav : _PREFETCH_SCENE_EMPTY[mode];
              _feedCachePut(mode, { scenes: [], emptyHtml }, title);
            } else {
              _feedCachePut(mode, { scenes: d.scenes }, title);
            }
          })
          .catch(() => {})
      );
    }

    prefetchSceneMode('studios');
    prefetchSceneMode('performers');
    prefetchSceneMode('jav');

    if (cur !== 'tags') {
      thunks.push(() =>
        _ensureMonitoredTagsLoaded()
          .then(() => {
            const ids = Array.isArray(_selectedTagIds) ? _selectedTagIds : [];
            const key = `tags|${ids.slice().sort().join(',')}`;
            if (_feedCacheGet(key)) return;
            let url = '/api/scenes/feed?mode=tags';
            if (ids.length) url += '&tag_ids=' + encodeURIComponent(ids.join(','));
            return fetch(url)
              .then(r => r.json())
              .then(d => {
                if (!d || typeof d !== 'object') return;
                if (d.error === 'no_tags') {
                  const emptyHtml = !_monitoredTagsCache.length
                    ? '<div class="empty">No vices configured. Add some under Settings &rarr; Content Filters &rarr; Vices.</div>'
                    : '<div class="empty">No vices selected. Click the <i class="fa-solid fa-fire"></i> button above and pick one or more (or <b>All</b> if your selection was cleared after switching to StashDB tags).</div>';
                  _feedCachePut(key, { scenes: [], emptyHtml }, _scenesFeedTitleForMode('tags'));
                  return;
                }
                if (!Array.isArray(d.scenes)) return;
                if (!d.scenes.length) {
                  _feedCachePut(key, { scenes: [], emptyHtml: _PREFETCH_SCENE_EMPTY.tagsNoScenes }, _scenesFeedTitleForMode('tags'));
                  return;
                }
                _feedCachePut(key, { scenes: d.scenes }, _scenesFeedTitleForMode('tags'));
              });
          })
          .catch(() => {})
      );
    }

    // Concurrency cap = 1. Run each thunk in sequence so we never burn
    // 4–5 upstream feed calls in parallel on idle. The auto-prefetch
    // is still valuable — it masks tab-switch latency — but a serial
    // chain spreads the upstream cost out over a few seconds instead
    // of all at once.
    thunks.reduce((p, fn) => p.then(fn), Promise.resolve()).catch(() => {});
  }

  async function loadFeed(opts) {
    const _prefetchInactiveFeedsAfterLoad = () => {
      try {
        if (!document.getElementById('scenesGrid') || _scenesFeedMode === 'search') return;
        const run = () => _maybePrefetchInactiveFeeds();
        if (typeof requestIdleCallback === 'function') {
          requestIdleCallback(run, { timeout: 5000 });
        } else {
          setTimeout(run, 2000);
        }
      } catch (_) {}
    };
    try {
    const force = !!(opts && opts.force);
    const section = document.getElementById('scenesSection');
    const grid    = document.getElementById('scenesGrid');
    const title   = document.getElementById('scenesTitle');
    const pag     = document.getElementById('movieFeedPagination');
    _applyFeedModeToggleUI();
    _sceneSources = {};
    section.style.display = 'block';
    // Library tokens feed `.qs-match` highlighting on every scene-card
    // and movie-card title. Fire-and-forget — first call primes the
    // cache; later calls are deduped inside `_refreshLibraryTokens`.
    if (typeof _refreshLibraryTokens === 'function') { _refreshLibraryTokens(); }

    //: Fresh cache hit: skip network. Stale hit: paint last payload then
    //: revalidate below without wiping the grid with a skeleton.
    const cacheKey = _feedCacheKey();
    if (force && _scenesFeedMode !== 'search') {
      _feedCacheInvalidate(cacheKey);
    }
    let staleRevalidate = false;
    if (!force && _scenesFeedMode !== 'search') {
      const hitFresh = _feedCacheGet(cacheKey);
      if (hitFresh) {
        _renderFeedCacheHit(hitFresh, grid, title, pag);
        return;
      }
      const hitAny = _feedCachePeekAny(cacheKey);
      if (hitAny && Date.now() - hitAny.ts > _FEED_CACHE_TTL_MS) {
        staleRevalidate = true;
        _renderFeedCacheHit(hitAny, grid, title, pag);
      }
    }

    // Show/hide the search-form panel based on the active feed mode.
    const searchPanel = document.getElementById('scenesSearchPanel');
    if (searchPanel) searchPanel.style.display = (_scenesFeedMode === 'search') ? 'block' : 'none';
    // Search mode: let the panel drive the grid; don't auto-fetch.
    if (_scenesFeedMode === 'search') {
      tsSetPageTitleText(title, _scenesFeedTitleForMode('search'), 'scenes');
      if (pag) pag.style.display = 'none';
      grid.className = 'scene-grid';
      if (!grid.dataset.searchPopulated) {
        grid.innerHTML = '<div class="empty">Fill in the form and search across StashDB, ThePornDB, FansDB, and JAVStash.</div>';
      }
      // Sync the "All / None" pill label + Search button enabled
      // state to the current checkbox row. Cheap; covers first-paint
      // and any state held over from a previous visit.
      try { _updateScenesSrchSourceState(); } catch (_) {}
      return;
    }

    // Movies feed mode — show TPDB movie grid
    if (_scenesFeedMode === 'movies') {
      tsSetPageTitleText(title, _scenesFeedTitleForMode('movies'), 'scenes');
      if (pag) pag.style.display = 'none';
      if (!staleRevalidate) {
        grid.innerHTML = movieGridSkeleton(30);
        grid.className = 'movie-grid';
        await _yieldTwoFrames();
      }
      try {
        // `force` (from the toolbar refresh button) maps to the
        // movies endpoint's `refresh=1` param so the user can wipe
        // the 1-hour page-1 cache when TPDB ordering changes (e.g.
        // after we flipped from `created_at desc` to `date desc`).
        const r = await fetch('/api/movies/tpdb/latest?page=1' + (force ? '&refresh=1' : ''));
        const d = await r.json();
        const movies = d.results || [];
        if (!movies.length) {
          if (staleRevalidate) {
            const prev = _feedCachePeekAny(cacheKey);
            if (prev && prev.data && Array.isArray(prev.data.movies) && prev.data.movies.length) return;
          }
          let msg = 'No movies found';
          if (d.error) {
            if (String(d.error).includes('TPDB returned 401')) {
              msg += '. TPDB authentication failed (401). Add or update your TPDB API key in Settings.';
            } else { msg += ' (' + esc(d.error) + ')'; }
          }
          grid.innerHTML = `<div class="empty movie-grid-empty">${msg}</div>`;
          return;
        }
        _movieFeedPage = 1;
        _movieFeedTotalPages = d.total_pages || 1;
        renderMovieGrid(movies, grid);
        _feedCachePut(cacheKey, { movies }, _scenesFeedTitleForMode('movies'));
        _scheduleMovieDetailChromePreload();
      } catch(e) {
        grid.innerHTML = `<div class="empty movie-grid-empty">Error: ${esc(e.message)}</div>`;
      }
      return;
    }

    // Scene feed modes (JAV uses the same portrait movie-grid as Latest Movies)
    if (pag) pag.style.display = 'none';
    if (!staleRevalidate) {
      if (_scenesFeedMode === 'jav') {
        grid.className = 'movie-grid';
        grid.innerHTML = movieGridSkeleton(30);
      } else {
        grid.className = 'scene-grid';
        grid.innerHTML = sceneGridSkeleton();
      }
      if (title) tsSetPageTitleText(title, _scenesFeedTitleForMode(_scenesFeedMode), 'scenes');
      await _yieldTwoFrames();
    }
    try {
      let url = '/api/scenes/feed?mode=' + encodeURIComponent(_scenesFeedMode);
      const _tagIdsForFeed = Array.isArray(_selectedTagIds) ? _selectedTagIds : [];
      if (_scenesFeedMode === 'tags' && _tagIdsForFeed.length) {
        url += '&tag_ids=' + encodeURIComponent(_tagIdsForFeed.join(','));
      }
      // `force` (set by the toolbar refresh button) bypasses both the
      // local feedCache freshness check and the server's 1h _feed_cache so the
      // request hits the source DBs fresh.
      if (force) url += '&refresh=1';
      const r = await fetch(url);
      const d = await r.json();
      if (_scenesFeedMode === 'tags') {
        tsSetPageTitleText(title, _scenesFeedTitleForMode('tags'), 'scenes');
        if (d && d.error === 'no_tags') {
          if (staleRevalidate) {
            const prev = _feedCachePeekAny(cacheKey);
            if (prev && prev.data && Array.isArray(prev.data.scenes) && prev.data.scenes.length) return;
          }
          // Distinguish "no vices configured" from "has vices but none
          // selected on this page".
          const emptyHtml = !_monitoredTagsCache.length
            ? '<div class="empty">No vices configured. Add some under Settings &rarr; Content Filters &rarr; Vices.</div>'
            : '<div class="empty">No vices selected. Click the <i class="fa-solid fa-fire"></i> button above and pick one or more (or <b>All</b> if your selection was cleared after switching to StashDB tags).</div>';
          grid.innerHTML = emptyHtml;
          _feedCachePut(cacheKey, { scenes: [], emptyHtml }, _scenesFeedTitleForMode('tags'));
          return;
        }
        if (!d.scenes?.length) {
          if (staleRevalidate) {
            const prev = _feedCachePeekAny(cacheKey);
            if (prev && prev.data && Array.isArray(prev.data.scenes) && prev.data.scenes.length) return;
          }
          const emptyHtml = '<div class="empty">No scenes for the selected vices — add your StashDB API key under Settings → Databases, and ensure each vice uses StashDB tags (open Settings → Content Filters → Vices and re-add tags if you previously used ThePornDB).</div>';
          grid.innerHTML = emptyHtml;
          _feedCachePut(cacheKey, { scenes: [], emptyHtml }, _scenesFeedTitleForMode('tags'));
          return;
        }
        renderSceneGrid(d.scenes, true);
        _feedCachePut(cacheKey, { scenes: d.scenes }, _scenesFeedTitleForMode('tags'));
        return;
      }
      if (_scenesFeedMode === 'studios') {
        tsSetPageTitleText(title, _scenesFeedTitleForMode('studios'), 'scenes');
        if (!d.scenes?.length) {
          if (staleRevalidate) {
            const prev = _feedCachePeekAny(cacheKey);
            if (prev && prev.data && Array.isArray(prev.data.scenes) && prev.data.scenes.length) return;
          }
          const emptyHtml = '<div class="empty">No studio feed scenes — add your StashDB API key under Settings → Databases, and link library studios to StashDB under Favourites so recent releases from those studios can be loaded.</div>';
          grid.innerHTML = emptyHtml;
          _feedCachePut(cacheKey, { scenes: [], emptyHtml }, _scenesFeedTitleForMode('studios'));
          return;
        }
        renderSceneGrid(d.scenes, true);
        _feedCachePut(cacheKey, { scenes: d.scenes }, _scenesFeedTitleForMode('studios'));
        return;
      }
      if (_scenesFeedMode === 'performers') {
        tsSetPageTitleText(title, _scenesFeedTitleForMode('performers'), 'scenes');
        if (!d.scenes?.length) {
          if (staleRevalidate) {
            const prev = _feedCachePeekAny(cacheKey);
            if (prev && prev.data && Array.isArray(prev.data.scenes) && prev.data.scenes.length) return;
          }
          const emptyHtml = '<div class="empty">No performer feed scenes — add your StashDB and/or FansDB API keys under Settings → Databases, and link library performers to StashDB and/or FansDB under Favourites so their recent scene credits can be loaded.</div>';
          grid.innerHTML = emptyHtml;
          _feedCachePut(cacheKey, { scenes: [], emptyHtml }, _scenesFeedTitleForMode('performers'));
          return;
        }
        renderSceneGrid(d.scenes, true);
        _feedCachePut(cacheKey, { scenes: d.scenes }, _scenesFeedTitleForMode('performers'));
        return;
      }
      if (_scenesFeedMode === 'jav') {
        tsSetPageTitleText(title, _scenesFeedTitleForMode('jav'), 'scenes');
        if (!d.scenes?.length) {
          if (staleRevalidate) {
            const prev = _feedCachePeekAny(cacheKey);
            if (prev && prev.data && Array.isArray(prev.data.scenes) && prev.data.scenes.length) return;
          }
          const emptyHtml = '<div class="empty">No JAV scenes — add your JAVStash API key under Settings → Databases, or check access to javstash.org.</div>';
          grid.innerHTML = emptyHtml;
          _feedCachePut(cacheKey, { scenes: [], emptyHtml }, _scenesFeedTitleForMode('jav'));
          return;
        }
        renderJavMovieGrid(d.scenes, grid);
        _feedCachePut(cacheKey, { scenes: d.scenes }, _scenesFeedTitleForMode('jav'));
        _scheduleMovieDetailChromePreload();
        return;
      }
      tsSetPageTitleText(title, _scenesFeedTitleForMode(_scenesFeedMode), 'scenes');
      if (!d.scenes?.length) {
        if (staleRevalidate) {
          const prev = _feedCachePeekAny(cacheKey);
          if (prev && prev.data && Array.isArray(prev.data.scenes) && prev.data.scenes.length) return;
        }
        const emptyHtml = '<div class="empty">No feed scenes — check your TPDB API key in Settings</div>';
        grid.innerHTML = emptyHtml;
        _feedCachePut(cacheKey, { scenes: [], emptyHtml }, _scenesFeedTitleForMode(_scenesFeedMode));
        return;
      }
      renderSceneGrid(d.scenes, true);
      _feedCachePut(cacheKey, { scenes: d.scenes }, _scenesFeedTitleForMode(_scenesFeedMode));
    } catch(e) {
      grid.innerHTML = '<div class="empty">Failed to load feed</div>';
    }
    } finally {
      _prefetchInactiveFeedsAfterLoad();
    }
  }

  // Lazy-fetch library headshots matching a card's performer names on first
  // hover. Caches via data attribute so a second hover is free. Silently
  // no-ops when: the card has no performer string, the fetch fails, or no
  // library matches come back.
  async function ensureCardHeadshots(card, performerStr) {
    if (!card || card.dataset.headshotsLoaded === '1') return;
    card.dataset.headshotsLoaded = '1';
    const s = (performerStr || '').trim();
    if (!s) return;
    try {
      const r = await fetch('/api/performers/headshots-by-name?names=' + encodeURIComponent(s));
      const d = await r.json();
      const perfs = (d && d.performers) || [];
      if (!perfs.length) return;
      const imgLoad = card.querySelector('.img-load');
      if (!imgLoad) return;
      // Movies fit more performers into their centered middle band
      // than the landscape scene still does — scene cards tend to
      // credit 2-3 faces, movies can have ten or more.
      const isMovie = card.classList.contains('movie-card');
      const cap = isMovie ? 8 : 4;
      const shown = perfs.slice(0, cap);
      const extra = perfs.length - shown.length;
      const wrap = document.createElement('div');
      wrap.className = 'scene-headshots-hover';
      const avatarsHtml = shown.map(p => {
        const attrs = window.performerLinkAttrs(p.name, { gender: p.gender, libraryRowId: p.row_id || p.id });
        return `<img class="scene-headshot-avatar" src="${esc(p.headshot_url || '')}" alt="${esc(p.name)}" title="${esc(p.name)}" onerror="this.remove()"${attrs ? ' ' + attrs : ''} loading="lazy">`;
      }).join('');
      // `+N` chip on movies when the cast overflows the visible cap —
      // gives a sense of how deep the ensemble is without blowing the
      // layout past the logo band.
      const moreChip = (isMovie && extra > 0)
        ? `<span class="scene-headshot-more" title="${extra} more performer${extra === 1 ? '' : 's'}">+${extra}</span>`
        : '';
      wrap.innerHTML = avatarsHtml + moreChip;
      imgLoad.appendChild(wrap);
      // Flag the card so the studio-logo fallback (rendered for the
      // /discover studio view) can hide itself once real headshots
      // arrive — see `.discover-info-scene-card--studio.has-headshots`
      // in discover.html.
      card.classList.add('has-headshots');
    } catch (e) {}
  }

  // ── In-library badge ─────────────────────────────────────────────
  // After a grid renders, batch-fetch /api/library/scenes-in for the
  // visible scenes and decorate matched cards with a tick-in-a-box
  // overlay in the bottom-right of the thumbnail. Called from
  // renderSceneGrid (scenes) and renderMovieGrid (movies).
  async function decorateLibraryMatches(items, opts) {
    if (!Array.isArray(items) || !items.length) return;
    const sourceMap = (opts && opts.sourceMap) || null;
    const containerSelector = (opts && opts.containerSelector) || '.scene-card';
    const idAttr = (opts && opts.idAttr) || 'data-scene-i';
    // Build the richer items[] form so the backend's title+date fallback
    // can match scenes imported under a different stash-box than the one
    // currently being browsed (e.g. file matched on StashDB but TPDB
    // serves the same scene under a different id). Without title/date
    // the fallback is a no-op and we only get id-direct + phash hits.
    const itemsOut = [];
    items.forEach((s, i) => {
      const sid = String(s.id || s._id || '');
      if (!sid) return;
      let src = (s.source || '').toLowerCase();
      if (sourceMap && sourceMap[i]) src = sourceMap[i].toLowerCase();
      if (src.startsWith('search_')) src = src.slice(7);
      if (src === 'theporndb') src = 'tpdb';
      if (!['stashdb', 'fansdb', 'tpdb', 'javstash'].includes(src)) return;
      itemsOut.push({
        source: src,
        id: sid,
        title:  s.title  || '',
        date:   s.date   || s.release_date || '',
        studio: s.studio || (s.studio_name || ''),
      });
    });
    if (!itemsOut.length) return;
    let data;
    try {
      const r = await fetch('/api/library/scenes-in', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ items: itemsOut }),
      });
      data = await r.json();
    } catch (e) { return; }
    const matches = (data && data.matches) || {};
    if (!Object.keys(matches).length) return;
    items.forEach((s, i) => {
      const sid = String(s.id || s._id || '');
      if (!sid) return;
      let src = (s.source || '').toLowerCase();
      if (sourceMap && sourceMap[i]) src = sourceMap[i].toLowerCase();
      if (src.startsWith('search_')) src = src.slice(7);
      if (src === 'theporndb') src = 'tpdb';
      const key = `${src}:${sid}`;
      if (!matches[key]) return;
      const card = document.querySelector(`${containerSelector}[${idAttr}="${i}"]`);
      if (!card) return;
      card.classList.add('is-in-library');
      // Swap the watch button into a "collected" indicator when the
      // scene is in our library — same corner as the eye, bookmark icon +
      // muted red; always visible (CSS in app-shell.css).
      const watchBtn = card.querySelector('.scene-wanted-btn');
      if (watchBtn && !watchBtn.classList.contains('is-collected')) {
        watchBtn.classList.add('is-collected');
        watchBtn.title = 'In your library';
        watchBtn.innerHTML = '<i class="fa-solid fa-bookmark" aria-hidden="true"></i>';
      }
      // Card has no wanted button. Drop a standalone bookmark badge
      // onto the thumbnail so the in-library signal stays obvious.
      // Self-contained class — does NOT inherit `.scene-wanted-btn`
      // page-local styling (hover-only on /scenes; would hide the
      // badge at rest).
      if (!watchBtn && !card.querySelector('.lib-collected-badge')) {
        const host = card.querySelector('.img-load') || card;
        const badge = document.createElement('span');
        badge.className = 'lib-collected-badge';
        badge.title = 'In your library';
        badge.innerHTML = '<i class="fa-solid fa-bookmark"></i>';
        host.appendChild(badge);
      }
    });
  }
  window.decorateLibraryMatches = decorateLibraryMatches;

  // De-dupe: same scene returned by multiple stash-boxes (e.g. StashDB
  // and TPDB both index "Brianna Brown's Audacious Bang Casting" with
  // slightly different titles). Key = studio|date|performers (sorted,
  // case-folded). Static placeholders are skipped (no real identity).
  // First occurrence wins so the bucket order in /api/scenes/recent
  // (TPDB → StashDB → FansDB) is preserved.
  function dedupeScenes(scenes) {
    if (!Array.isArray(scenes) || scenes.length < 2) return scenes || [];
    const seen = new Set();
    const out = [];
    for (const s of scenes) {
      if (!s || s.__static) { out.push(s); continue; }
      const studio = String(s.studio || s.site_name || '').trim().toLowerCase();
      const date = String(s.date || s.release_date || '').slice(0, 10);
      let perfs = '';
      if (Array.isArray(s.performers)) {
        perfs = s.performers.map(p => String(typeof p === 'string' ? p : (p && p.name) || '').trim().toLowerCase()).filter(Boolean).sort().join(',');
      } else if (typeof s.performer === 'string') {
        perfs = s.performer.split(',').map(x => x.trim().toLowerCase()).filter(Boolean).sort().join(',');
      }
      // Fallback when none of those are populated: title prefix + date.
      const titlePrefix = String(s.title || '').trim().toLowerCase().slice(0, 24);
      const key = (studio || date || perfs)
        ? `${studio}|${date}|${perfs}`
        : `t:${titlePrefix}|${date}`;
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(s);
    }
    return out;
  }
  window.dedupeScenes = dedupeScenes;

  function renderSceneGrid(scenes, isFeed) {
    const grid = document.getElementById('scenesGrid');
    const deduped = dedupeScenes(scenes);
    const visibleScenes = deduped.slice(0, 30); // 6 rows x 5 columns
    _sceneGridItems = visibleScenes;
    grid.innerHTML = visibleScenes.map((s, i) => `
      <div class="scene-card" id="sc-${esc(s.id)}" data-scene-i="${i}" data-performer="${esc(s.performer || '')}" role="button" tabindex="0" aria-label="Open scene details" onmouseenter="ensureCardHeadshots(this, this.dataset.performer)">
        <div class="img-load">
          <span class="loader loader--tile" aria-hidden="true"></span>
          ${s.thumb
            ? `<img class="scene-thumb" src="${esc(s.thumb)}" loading="lazy" onload="this.closest('.img-load')?.classList.add('ready')" onerror="const w=this.closest('.img-load'); if(this.src.indexOf('missing.jpg')<0){this.src='/static/img/missing.webp'}else{w?.classList.add('ready')}">`
            : `<img class="scene-thumb" src="/static/img/missing.webp" loading="lazy" onload="this.closest('.img-load')?.classList.add('ready')" onerror="this.closest('.img-load')?.classList.add('ready')">`}
          <div class="duo-tint" aria-hidden="true"></div>
          ${(s.studio || s.title) ? `<img class="scene-studio-logo" src="/api/studio-logo?name=${encodeURIComponent(s.studio || '')}&q=${encodeURIComponent(s.title || '')}" alt="" loading="lazy" onload="this.closest('.scene-card')?.classList.add('has-studio-logo')" onerror="this.remove()">` : ''}
          ${(() => {
            // Top-LEFT source-DB badge, only rendered in Search mode
            // where the source actually varies (TPDB / StashDB / FansDB /
            // JAVStash). Hidden on hover so the top-left headshot cluster that
            // fades in has the corner to itself. Most feed modes are TPDB-only;
            // the performers tab uses StashDB, so the badge would still be noise.
            const raw = (s.source || '').toLowerCase();
            if (!raw.startsWith('search_')) return '';
            const key = raw.slice(7);
            const logos = {
              tpdb:    { src: '/static/logos/tpdb.webp',    label: 'TPDB' },
              stashdb: { src: '/static/logos/stashdb.webp', label: 'StashDB' },
              fansdb:  { src: '/static/logos/fansdb.webp',  label: 'FansDB' },
              javstash: { src: '/static/logos/javstash.webp', label: 'JAVStash' },
            };
            const meta = logos[key];
            if (!meta) return '';
            if (meta.text) {
              return `<span class="scene-source-logo scene-source-logo--text" title="From ${esc(meta.label)}">${esc(meta.text)}</span>`;
            }
            return `<img class="scene-source-logo" src="${esc(meta.src)}" alt="${esc(meta.label)}" title="From ${esc(meta.label)}" onerror="this.remove()" loading="lazy">`;
          })()}
          ${s.id ? (() => {
            let x = String(s.source || '').toLowerCase();
            if (x.startsWith('search_')) x = x.slice(7);
            const src = (x === 'stashdb' || x === 'fansdb' || x === 'javstash') ? x : 'tpdb';
            const kind = src === 'javstash' ? 'movie' : 'scene';
            return _cardWantedButtonHtml(kind, src, String(s.id));
          })() : ''}
          <button type="button" class="scene-card-blacklist-btn" data-title="${esc(s.title || '')}" onclick="event.stopPropagation();sceneCardBlacklist(this)" title="Blacklist this title — hides it everywhere in /scenes" aria-label="Blacklist title"><i class="fa-solid fa-ban"></i></button>

          ${(() => {
            // Performer name overlay, bottom-centre. Single line, highlighter
            // background, ellipsis on overflow. Prefer the gender-filtered
            // ``hover_performers`` for *which* performers to show. No
            // gender badges here — labels overlaid on the image stay
            // text-only so the photo carries the visual focus.
            // Prefer display_performers (objects with gender) so the
            // clickability gate can apply per-name. Fall back to
            // hover_performers / split CSV when the backend didn't
            // attach the gendered list.
            const dp = Array.isArray(s.display_performers) ? s.display_performers : null;
            const filtered = dp && dp.length
              ? dp
              : (Array.isArray(s.hover_performers) && s.hover_performers.length
                  ? s.hover_performers
                  : (s.performer || '').split(',').map(x => x.trim()).filter(Boolean));
            if (!filtered.length) return '';
            const sceneSrcRaw = String(s.source || '').toLowerCase();
            const sceneSrc = sceneSrcRaw.startsWith('search_') ? sceneSrcRaw.slice(7) : sceneSrcRaw;
            const html = filtered.map(p => {
              const nm = typeof p === 'object' ? (p.name || '') : String(p || '');
              if (!nm) return '';
              const g = typeof p === 'object' ? p.gender : '';
              const o = typeof p === 'object' ? p : null;
              const tpdbId = o ? (o.tpdb_id || (sceneSrc.includes('tpdb') ? (o.id || o._id || '') : '')) : '';
              const stashId = o
                ? ((sceneSrc === 'stashdb' || sceneSrc === 'fansdb' || sceneSrc === 'javstash')
                  ? (o.id || o.stash_id || '')
                  : (o.stash_id || ''))
                : '';
              const attrs = window.performerLinkAttrs(nm, {
                gender: g,
                stashId: stashId || undefined,
                tpdbId: tpdbId || undefined,
              });
              const cls = attrs ? ' class="perf-name-link"' : '';
              return `<span${attrs ? ' ' + attrs : ''}${cls}>${esc(capDisplayName(nm))}</span>`;
            }).filter(Boolean).join(', ');
            return `<div class="scene-performer-label"><span class="perf-hl">${html}</span></div>`;
          })()}
          ${(() => {
            // Tag chips overlaid in the middle of the thumbnail on hover.
            // In Tags mode the backend annotates ``matched_tags`` (the
            // intersection of scene tags with the user's selection —
            // this is the only correct set to display, so we honour it
            // even when empty). In other modes ``matched_tags`` is
            // undefined and we fall back to every tag on the scene.
            const source = Array.isArray(s.matched_tags)
              ? s.matched_tags
              : (Array.isArray(s.tags) ? s.tags : []);
            // Cap at 10 so the overlay comfortably fits ~3-4 rows within
            // the 46%-height band; the CSS mask fades any overflow at the
            // top edge. Any extra tags are summarised as a "+N more" chip
            // so the card still hints at how rich the scene tagging is.
            const cap = 10;
            const tags = source.slice(0, cap);
            const extra = Math.max(0, source.length - tags.length);
            if (!tags.length) return '';
            const chips = tags.map(t => `<span class="scene-card-tag-chip">${esc(t)}</span>`).join('');
            const more = extra > 0
              ? `<span class="scene-card-tag-chip" style="opacity:0.82">+${extra} more</span>`
              : '';
            return `<div class="scene-card-tags-hover">${chips}${more}</div>`;
          })()}
        </div>
        <div class="scene-info">
          <div class="scene-title" title="${esc(s.title)}">${(typeof _libraryHighlight === 'function') ? _libraryHighlight(s.title || '') : esc(s.title)}</div>
          <div class="scene-date" title="${esc((s.date || '') + (s.studio ? ' · ' + s.studio : ''))}"><span class="meta-date">${s.date || ''}</span>${s.studio ? `<span class="meta-studio-fallback">${s.date ? ' · ' : ''}${esc(capDisplayName(s.studio))}</span>` : ''}</div>
        </div>
      </div>`).join('');
    // Each rendered card carries data-scene-i; selector targets only
    // those.
    decorateLibraryMatches(visibleScenes, {
      sourceMap: visibleScenes.map(s => {
        let x = String(s.source || _currentSceneSource || 'tpdb').toLowerCase();
        if (x === 'theporndb') x = 'tpdb';
        if (x === 'javstash' || x === 'javstash.org') x = 'javstash';
        return x;
      }),
      containerSelector: '.scene-card',
      idAttr: 'data-scene-i',
    });
  }

  // `scenesGrid` only exists on /scenes; the optional-chain guards
  // against pages that load this bundle without the feed grid.
  document.getElementById('scenesGrid')?.addEventListener('click', function (e) {
    // The wanted-eye button sits on top of the card; clicking it should
    // ONLY toggle wanted, never open the scene overlay underneath. The
    // button's own listener calls stopPropagation, but this grid-level
    // handler is on an ancestor that fires first during bubble, so we
    // explicitly bail out here for clicks that originated on the eye.
    if (e.target.closest('.scene-wanted-btn')) return;
    const javCard = e.target.closest('.movie-card[data-scene-i]');
    if (javCard) {
      const idx = parseInt(javCard.getAttribute('data-scene-i') || '-1', 10);
      if (!Number.isNaN(idx) && idx >= 0 && _sceneGridItems[idx]) {
        showJavSceneDetail(_sceneGridItems[idx]);
      }
      return;
    }
    const card = e.target.closest('.scene-card[data-scene-i]');
    if (!card) return;
    const i = parseInt(card.getAttribute('data-scene-i'), 10);
    if (Number.isNaN(i) || !_sceneGridItems[i]) return;
    openSceneOverlay(_sceneGridItems[i]);
  });
  document.getElementById('scenesGrid')?.addEventListener('keydown', function (e) {
    if (e.key !== 'Enter' && e.key !== ' ') return;
    const javCard = e.target.closest('.movie-card[data-scene-i]');
    if (javCard) {
      e.preventDefault();
      const i = parseInt(javCard.getAttribute('data-scene-i'), 10);
      if (!Number.isNaN(i) && _sceneGridItems[i]) showJavSceneDetail(_sceneGridItems[i]);
      return;
    }
    const card = e.target.closest('.scene-card[data-scene-i]');
    if (!card) return;
    e.preventDefault();
    const i = parseInt(card.getAttribute('data-scene-i'), 10);
    if (Number.isNaN(i) || !_sceneGridItems[i]) return;
    openSceneOverlay(_sceneGridItems[i]);
  });

  // ── Movie functions ──────────────────────────────────────────────────

  let _movieFeedPage = 1, _movieFeedTotalPages = 1;

  function _movieTitleNatural(s) {
    // Storage form puts leading articles at the end for sort:
    //   "Education of My Young Neighbor, The"
    //   "Hangover, A"
    // Returns the unescaped natural-order display string.
    const trimmed = String(s || '').trim();
    const m = trimmed.match(/^(.*),\s+(The|A|An)\s*$/i);
    return m ? `${m[2]} ${m[1]}` : trimmed;
  }
  function movieTitleDisplay(s) {
    // Plain-text-safe display string (escaped; no highlight markup).
    // Use `movieTitleDisplayHtml` when injecting via innerHTML and you
    // want library-token highlighting.
    return esc(_movieTitleNatural(s));
  }
  function movieTitleDisplayHtml(s) {
    const display = _movieTitleNatural(s);
    return (typeof _libraryHighlight === 'function') ? _libraryHighlight(display) : esc(display);
  }

  function movieGridSkeleton(count = 20) {
    return Array.from({length: count}, () => `
      <div class="movie-card loading">
        <div class="skeleton-box" style="width:100%;aspect-ratio:27/40">
          <span class="loader loader--tile" aria-hidden="true"></span>
        </div>
        <div class="movie-info">
          <div class="skeleton-line" style="width:82%"></div>
          <div class="skeleton-line" style="width:58%;margin-bottom:0"></div>
        </div>
      </div>`).join('');
  }

  function movieDetailSkeleton() {
    return `
      <div class="movie-detail-inner">
        <div class="movie-detail-poster-wrap"><div class="skeleton-box" style="position:absolute;inset:0"><span class="loader loader--tile" aria-hidden="true"></span></div></div>
        <div class="movie-detail-text">
          <div class="skeleton-line" style="width:45%;height:20px"></div>
          <div class="skeleton-line" style="width:72%"></div>
          <div class="skeleton-line" style="width:100%"></div>
          <div class="skeleton-line" style="width:96%"></div>
          <div class="skeleton-line" style="width:88%"></div>
          <div style="display:flex;gap:8px;margin-top:18px">
            <div class="skeleton-box" style="height:34px;width:170px;border-radius:4px"></div>
            <div class="skeleton-box" style="height:34px;width:110px;border-radius:4px"></div>
          </div>
        </div>
      </div>`;
  }

  function renderMovieGrid(movies, gridEl) {
    const posterFallback = '/static/img/poster.webp';
    gridEl.innerHTML = movies.map((m, i) => {
      const posterUrl = (m.poster && String(m.poster).trim()) ? m.poster : posterFallback;
      return `
      <div class="movie-card" data-movie-i="${i}" data-movie-id="${esc(m.id)}" data-performer="${esc(m.performer || '')}" onmouseenter="ensureCardHeadshots(this, this.dataset.performer)">
        <div class="img-load"><span class="loader loader--tile" aria-hidden="true"></span><img class="movie-poster" src="${esc(posterUrl)}" loading="lazy" onload="this.closest('.img-load')?.classList.add('ready');typeof tsApplyMovieCoverSplit==='function'&&tsApplyMovieCoverSplit(this,'solo')" onerror="this.onerror=null;this.src='${posterFallback}';this.closest('.img-load')?.classList.add('ready');"><div class="duo-tint" aria-hidden="true"></div>${(m.studio || m.title) ? `<img class="scene-studio-logo" src="/api/studio-logo?name=${encodeURIComponent(m.studio || '')}&q=${encodeURIComponent(m.title || '')}" alt="" loading="lazy" onload="this.closest('.movie-card')?.classList.add('has-studio-logo')" onerror="this.remove()">` : ''}${m.id ? _cardWantedButtonHtml('movie', 'tpdb', String(m.id)) : ''}<button type="button" class="scene-card-blacklist-btn" data-title="${esc(m.title || '')}" onclick="event.stopPropagation();sceneCardBlacklist(this)" title="Blacklist this title" aria-label="Blacklist title"><i class="fa-solid fa-ban"></i></button></div>
        <div class="movie-info">
          <div class="movie-title" title="${esc(m.title)}">${movieTitleDisplayHtml(m.title)}</div>
          <div class="movie-meta" title="${esc((m.date || '') + (m.studio ? ' · ' + m.studio : ''))}"><span class="meta-date">${m.date || ''}</span>${m.studio ? `<span class="meta-studio-fallback">${m.date ? ' · ' : ''}${esc(m.studio)}</span>` : ''}</div>
        </div>
      </div>`;
    }).join('');
    // Movies in /scenes are TPDB-sourced; phash crosswalk only resolves
    // when fingerprints are cached (so first-visit shows nothing,
    // subsequent visits decorate after the backfill catches up).
    decorateLibraryMatches(movies, {
      sourceMap: movies.map(() => 'tpdb'),
      containerSelector: '.movie-card',
      idAttr: 'data-movie-i',
    });
  }

  /** Load/error helpers for JAV movie tiles (inline handlers; duo waits for both slots). */
  (function () {
    if (window._javFeedGridImgOnload) return;
    window._javFeedGridImgOnload = function (img, side) {
      const wrap = img && img.closest('.img-load');
      if (!wrap) return;
      if (typeof tsApplyMovieCoverSplit === 'function') {
        tsApplyMovieCoverSplit(img, side || 'solo');
      }
      if (wrap.classList.contains('img-load--jav-duo')) {
        const n = (+wrap.getAttribute('data-jav-slot-ready') || 0) + 1;
        wrap.setAttribute('data-jav-slot-ready', String(n));
        if (n >= 2) wrap.classList.add('ready');
      } else {
        wrap.classList.add('ready');
      }
    };
    window._javFeedGridImgOnerror = function (img, soloFallback) {
      const wrap = img && img.closest('.img-load');
      if (!wrap || !img) return;
      if (!wrap.classList.contains('img-load--jav-duo')) {
        if (soloFallback) {
          img.onerror = null;
          img.src = soloFallback;
        }
        wrap.classList.add('ready');
        return;
      }
      const n = (+wrap.getAttribute('data-jav-slot-ready') || 0) + 1;
      wrap.setAttribute('data-jav-slot-ready', String(n));
      if (n >= 2) wrap.classList.add('ready');
    };
  })();

  /** JAV feed — same portrait cards as Latest Movies; wide single bitmap = right crop (`solo`); distinct back URL = left back / right front columns. */
  function renderJavMovieGrid(scenes, gridEl) {
    const posterFallback = '/static/img/poster.webp';
    const deduped = dedupeScenes(scenes);
    const visible = deduped.slice(0, 30);
    _sceneGridItems = visible;
    try { window._sceneGridItems = visible; } catch (_) {}
    gridEl.innerHTML = visible.map((s, i) => {
      const front = (s.poster && String(s.poster).trim()) || (s.thumb && String(s.thumb).trim()) || posterFallback;
      const back = (s.background && String(s.background).trim()) || '';
      const spreadDup = !!(back && String(front).trim() === String(back).trim());
      const coverForce = !!(s.cover_art_is_spread || spreadDup);
      const dForce = coverForce ? ' data-force-split="1"' : '';
      const duoDifferent = !!(back && String(front).trim() !== String(back).trim());
      const dForceDuo = s.cover_art_is_spread ? ' data-force-split="1"' : '';
      const overlays = `<div class="duo-tint" aria-hidden="true"></div>${(s.studio || s.title) ? `<img class="scene-studio-logo" src="/api/studio-logo?name=${encodeURIComponent(s.studio || '')}&q=${encodeURIComponent(s.title || '')}" alt="" loading="lazy" onload="this.closest('.movie-card')?.classList.add('has-studio-logo')" onerror="this.remove()">` : ''}${s.id ? _cardWantedButtonHtml('movie', 'javstash', String(s.id)) : ''}<button type="button" class="scene-card-blacklist-btn" data-title="${esc(s.title || '')}" onclick="event.stopPropagation();sceneCardBlacklist(this)" title="Blacklist this title" aria-label="Blacklist title"><i class="fa-solid fa-ban"></i></button>`;
      let imgBlock;
      if (duoDifferent) {
        imgBlock = `
        <div class="img-load img-load--jav-duo" data-jav-slot-ready="0">
          <span class="loader loader--tile" aria-hidden="true"></span>
          <div class="jav-duo-slot jav-duo-slot--back">
            <img class="movie-poster"${dForceDuo} src="${esc(back)}" alt="" loading="lazy" onload="window._javFeedGridImgOnload(this,'back')" onerror="window._javFeedGridImgOnerror(this)">
          </div>
          <div class="jav-duo-slot jav-duo-slot--front">
            <img class="movie-poster"${dForceDuo} src="${esc(front)}" alt="" loading="lazy" onload="window._javFeedGridImgOnload(this,'front')" onerror="window._javFeedGridImgOnerror(this)">
          </div>
          ${overlays}
        </div>`;
      } else {
        const posterUrl = front;
        imgBlock = `
        <div class="img-load">
          <span class="loader loader--tile" aria-hidden="true"></span>
          <img class="movie-poster"${dForce} src="${esc(posterUrl)}" alt="" loading="lazy" onload="window._javFeedGridImgOnload(this,'solo')" onerror="window._javFeedGridImgOnerror(this,'${posterFallback}')">
          ${overlays}
        </div>`;
      }
      return `
      <div class="movie-card" data-scene-i="${i}" data-performer="${esc(s.performer || '')}" role="button" tabindex="0" aria-label="Open scene details" onmouseenter="ensureCardHeadshots(this, this.dataset.performer)">
        ${imgBlock}
        <div class="movie-info">
          <div class="movie-title" title="${esc(s.title)}">${movieTitleDisplayHtml(s.title)}</div>
          <div class="movie-meta" title="${esc((s.date || '') + (s.studio ? ' · ' + s.studio : ''))}"><span class="meta-date">${s.date || ''}</span>${s.studio ? `<span class="meta-studio-fallback">${s.date ? ' · ' : ''}${esc(s.studio)}</span>` : ''}</div>
        </div>
      </div>`;
    }).join('');
    decorateLibraryMatches(visible, {
      sourceMap: visible.map(() => 'javstash'),
      containerSelector: '.movie-card',
      idAttr: 'data-scene-i',
    });
  }



  // ── Magazine layout registry ──────────────────────────────────────
  // Each entry is a candidate page-2 (gallery) layout. `match(ctx)` is
  // a boolean predicate (eligibility) and `weight(ctx)` is a number
  // used only to break ties when multiple layouts match. `slots`
  // describes the cells the renderer needs to emit; `key` is also the
  // CSS class modifier on `.mag-gallery-grid` (each layout's
  // grid-template-areas is defined in discover.html via
  // `.mag-gallery-grid--{key}`). To add a new layout:
  //   1. Push an entry here describing its slots.
  //   2. Add a matching `.mag-gallery-grid--{key}` rule in CSS with
  //      the grid-template-areas it needs.
  async function showMovieDetail(movieId) {
    _preloadMovieDetailChromeOnIntent();
    // Kick the popup bundle in flight while we fetch — perf-name-link
    // chips in the rendered detail invoke openPerformerPopup, which
    // depends on the lazy bundle. Without this preload, the FIRST
    // performer click after page load pays ~3 sequential script loads.
    if (typeof window.ensurePopupBundle === 'function') {
      window.ensurePopupBundle().catch(function () {});
    }
    // Legacy popup-style movie detail overlay. The /discover inline
    // panel that used to share this code path is gone.
    document.getElementById('movieDetailContent').innerHTML = movieDetailSkeleton();
    const mdo = document.getElementById('movieDetailOverlay');
    if (mdo) {
      // Don't pin the overlay to English — TPDB synopses come back
      // in the movie's source language (French / German / Japanese
      // etc.) and `lang="en"` told Chrome the whole popup was
      // already English, suppressing the translate offer. Clearing
      // the attribute lets the inner `lang=""` markers on user
      // content kick in so Chrome's per-section translate works.
      mdo.removeAttribute('lang');
      mdo.classList.add('open');
    }
    try {
      const r = await fetch(`/api/movies/tpdb/${movieId}`);
      const m = await r.json();
      if (m.error) {
        document.getElementById('movieDetailContent').innerHTML = `<div class="empty">${esc(m.error)}</div>`;
        return;
      }
      // Stash full movie metadata so the prowlarr-grab path can tag
      // each movie download with its source DB id + poster URL the
      // same way scene grabs do — without this, /downloads tiles for
      // movie grabs render with no poster.
      window._currentMovie = m;
      const bg = m.background ? `<img class="movie-detail-bg" src="${esc(m.background)}" onerror="this.remove()" loading="lazy">` : '';
      const posterFallback = '/static/img/poster.webp';
      const detailPosterUrl = (m.poster && String(m.poster).trim()) ? m.poster : posterFallback;
      const bgUrl = (m.background && String(m.background).trim()) || '';
      const spreadDup = !!(bgUrl && detailPosterUrl === bgUrl);
      const dForce = (m.cover_art_is_spread || spreadDup) ? ' data-force-split="1"' : '';
      const overlaySrc = esc((m.poster && String(m.poster).trim()) ? m.poster : posterFallback);
      const poster = `<div class="img-load"><span class="loader loader--tile" aria-hidden="true"></span><img class="movie-poster"${dForce} src="${esc(detailPosterUrl)}" style="cursor:pointer;width:100%;height:100%;object-fit:cover;display:block" onclick="openImageOverlay('${overlaySrc}')" onload="this.closest('.img-load')?.classList.add('ready');typeof tsApplyMovieCoverSplit==='function'&&tsApplyMovieCoverSplit(this,'solo')" onerror="this.onerror=null;this.src='${posterFallback}';this.closest('.img-load')?.classList.add('ready');" loading="lazy"></div>`;
      // Random hue rotation for the vhs.png frame so each open varies.
      const vhsHue = Math.floor(Math.random() * 360);
      // Studio logo (rotated 90° CCW behind the poster) — only if we
      // have a studio name to look up. Same /api/studio-logo lookup as
      // the regular movie cards.
      const studioLogoHtml = (m.studio || m.title)
        ? `<img class="movie-detail-studio-logo-rotated" src="/api/studio-logo?name=${encodeURIComponent(m.studio || '')}&q=${encodeURIComponent(m.title || '')}" alt="" loading="lazy" onerror="this.remove()">`
        : '';
      // Rotated movie title fills the rest of the cassette label area
      // to the LEFT of the studio logo.
      const titleForVhs = movieTitleDisplay(m.title || '');
      const titleHtml = titleForVhs
        ? `<div class="movie-detail-vhs-title-rotated" aria-hidden="true">${titleForVhs}</div>`
        : '';
      const posterFrame = `
        <div class="movie-detail-vhs-bg" style="--vhs-hue:${vhsHue}deg" aria-hidden="true"></div>
        ${titleHtml}
        ${studioLogoHtml}
        <div class="movie-detail-poster-card">${poster}</div>`;
      const meta = [];
      if (m.studio) meta.push(`Studio: <span>${esc(m.studio)}</span>`);
      if (m.date) meta.push(`Released: <span>${esc(m.date)}</span>`);
      if (m.duration) meta.push(`Duration: <span>${Math.round(m.duration/60)} min</span>`);
      if (m.directors?.length) {
        const dirNames = m.directors.map(d => esc(typeof d === 'object' && d !== null ? (d.name || d.full_name || '') : String(d))).filter(Boolean);
        if (dirNames.length) meta.push(`Director: <span>${dirNames.join(', ')}</span>`);
      }
      const libPerfs = m.library_performers || [];
      const libPerfsHtml = libPerfs.length
        ? `<div class="lib-perfs-row">${libPerfs.map(p => {
            const img = p.headshot_url
              ? `<img src="${esc(p.headshot_url)}" alt="" loading="lazy" onerror="this.replaceWith(Object.assign(document.createElement('div'),{className:'lib-perf-ph',innerHTML:'<i class=\\'fa-solid fa-user\\'></i>'}))">`
              : `<div class="lib-perf-ph"><i class="fa-solid fa-user"></i></div>`;
            const attrs = window.performerLinkAttrs(p.name, { gender: p.gender, libraryRowId: p.row_id || p.id });
            return `<div class="lib-perf-hs" title="${esc(p.name)}"${attrs ? ' ' + attrs : ''}>${img}<div class="lib-perf-hs-name">${esc(p.name)}</div></div>`;
          }).join('')}</div>`
        : '';
      const perfLinks = (m.performer_links || []).map(p => {
        const nameRaw = p.name || '';
        if (!nameRaw) return '';
        const name = esc(nameRaw);
        const badge = genderBadge(p.gender);
        // Click → universal popup (gender-gated). External profile is
        // surfaced via the popup's own link pills.
        const attrs = window.performerLinkAttrs(nameRaw, { gender: p.gender, stashId: p.id || p.stash_id });
        return `<span class="movie-perf-tag${attrs ? ' perf-name-link' : ''}"${attrs ? ' ' + attrs : ''}>${name}${badge}</span>`;
      }).join('');
      const perfs = perfLinks || (m.performers||[]).map(p => {
        const nm = esc(p);
        return `<span class="movie-perf-tag perf-name-link" data-performer-link data-name="${esc(p)}">${nm}</span>`;
      }).join('');
      const movieTags = Array.isArray(m.tags) ? m.tags : [];
      const movieTagsHtml = movieTags.length
        ? `<div class="movie-detail-tags" style="display:flex;flex-wrap:wrap;gap:6px;margin-top:10px">${movieTags.map(t => `<span class="scene-card-tag-chip">${esc(t)}</span>`).join('')}</div>`
        : '';
      let scenes = '';
      if (m.scenes?.length) {
        scenes = `<div class="movie-detail-scenes"><div class="movie-detail-scenes-title">Scenes (${m.scenes.length})</div><div class="movie-detail-scene-grid">${m.scenes.map(s => `<div class="movie-detail-scene-card">${s.thumb ? `<div class="img-load"><span class="loader loader--tile" aria-hidden="true"></span><img class="movie-detail-scene-thumb" src="${esc(s.thumb)}" loading="lazy" onload="this.closest('.img-load')?.classList.add('ready')" onerror="const w=this.closest('.img-load'); if(w){ this.outerHTML='<div class=\\'movie-detail-scene-thumb-ph\\'></div>'; w.classList.add('ready'); }"></div>` : '<div class="movie-detail-scene-thumb-ph"></div>'}<div class="movie-detail-scene-info">${esc(s.title)}${s.date ? ' · '+s.date : ''}</div></div>`).join('')}</div></div>`;
      }
      // `lang=""` on user-content fields tells the browser the language
      // is unknown so Chrome detects per-section and offers translate
      // when it sees foreign text. We leave the chrome (action buttons,
      // meta lines, scene-count headers) without lang so they inherit
      // the document's 'en' and stay untranslated. TPDB synopses are
      // the usual foreign-text carriers (French / German / Italian /
      // Japanese releases).
      document.getElementById('movieDetailContent').innerHTML = `${bg}
        <div class="movie-detail-inner">
          <div class="movie-detail-poster-wrap">${posterFrame}</div>
          <div class="movie-detail-text">
            <div class="movie-detail-title" lang="" title="${esc(m.title)}">${movieTitleDisplayHtml(m.title)}</div>
            ${libPerfsHtml}
            <div class="movie-detail-meta-line">${meta.join(' &middot; ')}</div>
            ${perfs ? `<div class="movie-detail-performers">${perfs}</div>` : ''}
            ${movieTagsHtml}
            ${m.synopsis ? `<div class="movie-detail-synopsis" lang="">${esc(m.synopsis)}</div>` : ''}
            <div class="movie-detail-actions">
              <button class="movie-btn-action movie-btn-prowlarr" onclick="event.stopPropagation();window.openProwlarrSearchPopup({title:'${esc((m.title || '').replace(/'/g, "\\'"))}',kind:'movie'})" title="Search Prowlarr"><span class="ts-prowlarr-btn-content"><img class="ts-prowlarr-btn-logo" src="/static/logos/prowlarr.webp" alt="Prowlarr"><i class="fa-solid fa-magnifying-glass"></i></span></button>
              <a class="movie-btn-action movie-btn-link" href="${esc(m.tmdb_url || ('https://www.themoviedb.org/search/movie?query=' + encodeURIComponent(m.title || '')))}" target="_blank" onclick="event.stopPropagation()"><img src="/static/logos/tmdb.webp" alt="TMDB" style="height:20px;width:auto;object-fit:contain;vertical-align:middle;opacity:0.9"></a>
              <a class="movie-btn-action movie-btn-link" href="${esc(m.url)}" target="_blank" onclick="event.stopPropagation()"><img src="/static/logos/tpdb.webp" alt="TPDB" style="height:20px;width:auto;object-fit:contain;vertical-align:middle;opacity:0.9"></a>
            </div>
          </div>
        </div>
        ${scenes}`;
    } catch(e) { document.getElementById('movieDetailContent').innerHTML = '<div class="empty">Error loading movie</div>'; }
  }

  /** Prowlarr from JAV movie-style overlay — context set in showJavSceneDetail (avoids brittle inline escaping). */
  function _javSceneDetailOpenProwlarr() {
    const s = window._javSceneDetailContext;
    if (!s || typeof window.openProwlarrSearchPopup !== 'function') return;
    window.openProwlarrSearchPopup({
      title:      (s.titleQuery || s.title || '').trim(),
      studio:     s.studio || '',
      performers: s.performers || '',
      thumb_url:  s.thumb_url || '',
      kind:       'movie',
    });
  }
  window._javSceneDetailOpenProwlarr = _javSceneDetailOpenProwlarr;

  /**
   * JAV feed cards use the same portrait grid as movies; open the same
   * #movieDetailOverlay chrome as TPDB movies (VHS frame, meta, tags,
   * synopsis, Prowlarr + JAVStash actions) instead of the scene overlay.
   */
  async function showJavSceneDetail(scene) {
    if (!scene) return;
    _preloadMovieDetailChromeOnIntent();
    // Preload the popup bundle so perf-name-link clicks inside the
    // detail render don't pay the cold-load cost. The idle preload at
    // popup-loader's bottom handles this for slow page-load → click
    // gaps; this catches the fast user who clicks a card immediately.
    if (typeof window.ensurePopupBundle === 'function') {
      window.ensurePopupBundle().catch(function () {});
    }
    const detailHost = document.getElementById('movieDetailContent');
    const detailOv = document.getElementById('movieDetailOverlay');
    if (!detailHost || !detailOv) {
      openSceneOverlay(scene);
      return;
    }
    window._currentMovie = null;
    const _perfList = (Array.isArray(scene.hover_performers) && scene.hover_performers.length)
      ? scene.hover_performers
      : (scene.performer || '').split(',').map(x => x.trim()).filter(Boolean);
    window._javSceneDetailContext = {
      title:       scene.title || '',
      titleQuery:  [_perfList.join(' '), scene.title || ''].filter(Boolean).join(' '),
      performers:  _perfList.join(', '),
      studio:      scene.studio || '',
      thumb_url:   scene.thumb || scene.poster || '',
    };

    detailHost.innerHTML = movieDetailSkeleton();
    detailOv.setAttribute('lang', 'ja');
    detailOv.classList.add('open');

    try {
      const sceneSrcRaw = String(scene.source || '').toLowerCase();
      const sceneSrc = sceneSrcRaw.startsWith('search_') ? sceneSrcRaw.slice(7) : sceneSrcRaw;
      const javId = String(scene.javstash_id || (sceneSrc === 'javstash' ? (scene.id || '') : '') || '').trim();
      const javHref = (scene.link && String(scene.link).trim()) || (javId ? 'https://javstash.org/scenes/' + javId : '');

      const bg = (scene.background && String(scene.background).trim())
        ? `<img class="movie-detail-bg" src="${esc(scene.background)}" onerror="this.remove()" loading="lazy">`
        : '';
      const posterFallback = '/static/img/poster.webp';
      const detailPosterUrl = (scene.poster && String(scene.poster).trim()) || (scene.thumb && String(scene.thumb).trim()) || posterFallback;
      const bgUrl = (scene.background && String(scene.background).trim()) || '';
      const spreadDup = !!(bgUrl && String(detailPosterUrl).trim() === String(bgUrl).trim());
      const dForce = (scene.cover_art_is_spread || spreadDup) ? ' data-force-split="1"' : '';
      const overlaySrc = esc(detailPosterUrl);
      const poster = `<div class="img-load"><span class="loader loader--tile" aria-hidden="true"></span><img class="movie-poster"${dForce} src="${esc(detailPosterUrl)}" style="cursor:pointer;width:100%;height:100%;object-fit:cover;display:block" onclick="openImageOverlay('${overlaySrc}')" onload="this.closest('.img-load')?.classList.add('ready');typeof tsApplyMovieCoverSplit==='function'&&tsApplyMovieCoverSplit(this,'solo')" onerror="this.onerror=null;this.src='${posterFallback}';this.closest('.img-load')?.classList.add('ready');" loading="lazy"></div>`;
      const vhsHue = Math.floor(Math.random() * 360);
      const studioLogoHtml = (scene.studio || scene.title)
        ? `<img class="movie-detail-studio-logo-rotated" src="/api/studio-logo?name=${encodeURIComponent(scene.studio || '')}&q=${encodeURIComponent(scene.title || '')}" alt="" loading="lazy" onerror="this.remove()">`
        : '';
      const titleForVhs = movieTitleDisplay(scene.title || '');
      const titleHtml = titleForVhs
        ? `<div class="movie-detail-vhs-title-rotated" aria-hidden="true">${titleForVhs}</div>`
        : '';
      const posterFrame = `
        <div class="movie-detail-vhs-bg" style="--vhs-hue:${vhsHue}deg" aria-hidden="true"></div>
        ${titleHtml}
        ${studioLogoHtml}
        <div class="movie-detail-poster-card">${poster}</div>`;

      const meta = [];
      if (scene.studio) meta.push(`Studio: <span>${esc(scene.studio)}</span>`);
      if (scene.date) meta.push(`Released: <span>${esc(scene.date)}</span>`);

      const dp = Array.isArray(scene.display_performers) ? scene.display_performers : null;
      let perfs = '';
      if (dp && dp.length) {
        perfs = dp.map(o => {
          const nm = capDisplayName(o.name || '');
          const tpdbId = o.tpdb_id || (sceneSrc.includes('tpdb') ? (o.id || o._id || '') : '');
          const stashId = (sceneSrc === 'stashdb' || sceneSrc === 'fansdb' || sceneSrc === 'javstash') ? (o.id || o.stash_id || '') : (o.stash_id || '');
          const attrs = window.performerLinkAttrs(o.name || '', {
            gender: o.gender,
            stashId: stashId,
            tpdbId: tpdbId,
          });
          return `<span class="movie-perf-tag${attrs ? ' perf-name-link' : ''}"${attrs ? ' ' + attrs : ''}>${esc(nm)}${genderBadge(o.gender)}</span>`;
        }).join('');
      } else if (scene.performer) {
        perfs = performerCsvHtml(scene.performer).replace(/class="perf-name-link"/g, 'class="movie-perf-tag perf-name-link"');
      }

      const movieTags = Array.isArray(scene.tags) ? scene.tags : [];
      const synopsis = (scene.description || '').trim();
      // JAV titles + synopses are Japanese — tag them explicitly so
      // Chrome offers the JA→user-lang translate prompt even when the
      // popup opens outside JAV feed mode (e.g., from search results
      // where the document lang is still 'en').
      const synopsisBlock = synopsis
        ? `<div class="movie-detail-synopsis" lang="ja">${esc(synopsis)}</div>`
        : '';
      const movieTagsHtml = movieTags.length
        ? `<div class="movie-detail-tags" style="display:flex;flex-wrap:wrap;gap:6px;margin-top:10px">${movieTags.map(tg => {
            const t = String(tg || '');
            return `<span class="scene-card-tag-chip">${esc(t)}</span>`;
          }).join('')}</div>`
        : '';
      const javBtn = javHref
        ? `<a class="movie-btn-action movie-btn-link" href="${esc(javHref)}" target="_blank" rel="noopener noreferrer" onclick="event.stopPropagation()" title="Open on JAVStash"><img src="/static/logos/javstash.webp" alt="JAVStash" style="height:20px;width:auto;object-fit:contain;vertical-align:middle;opacity:0.9"></a>`
        : '';

      detailHost.innerHTML = `${bg}
        <div class="movie-detail-inner">
          <div class="movie-detail-poster-wrap">${posterFrame}</div>
          <div class="movie-detail-text">
            <div class="movie-detail-title" lang="ja" title="${esc(scene.title)}">${movieTitleDisplayHtml(scene.title)}</div>
            <div id="javDetailLibPerfs"></div>
            <div class="movie-detail-meta-line">${meta.join(' &middot; ')}</div>
            ${perfs ? `<div class="movie-detail-performers">${perfs}</div>` : ''}
            ${movieTagsHtml}
            ${synopsisBlock}
            <div class="movie-detail-actions">
              <button type="button" class="movie-btn-action movie-btn-prowlarr" onclick="event.stopPropagation();window._javSceneDetailOpenProwlarr && window._javSceneDetailOpenProwlarr()" title="Search Prowlarr"><span class="ts-prowlarr-btn-content"><img class="ts-prowlarr-btn-logo" src="/static/logos/prowlarr.webp" alt="Prowlarr"><i class="fa-solid fa-magnifying-glass"></i></span></button>
              ${javBtn}
            </div>
            </div>
        </div>`;

      if (scene.performer) {
        fetch(`/api/performers/headshots-by-name?names=${encodeURIComponent(scene.performer)}`, { credentials: 'same-origin' })
          .then(r => r.json())
          .then(d => {
            const perfsOut = d.performers || [];
            const el = document.getElementById('javDetailLibPerfs');
            if (!el || !perfsOut.length) return;
            el.innerHTML = `<div class="lib-perfs-row">${perfsOut.map(p => {
              const img = p.headshot_url
                ? `<img src="${esc(p.headshot_url)}" alt="" loading="lazy" onerror="this.replaceWith(Object.assign(document.createElement('div'),{className:'lib-perf-ph',innerHTML:'<i class=\\'fa-solid fa-user\\'></i>'}))">`
                : `<div class="lib-perf-ph"><i class="fa-solid fa-user"></i></div>`;
              const attrs = window.performerLinkAttrs(p.name, { gender: p.gender, libraryRowId: p.row_id || p.id });
              return `<div class="lib-perf-hs" title="${esc(p.name)}"${attrs ? ' ' + attrs : ''}>${img}<div class="lib-perf-hs-name">${esc(p.name)}</div></div>`;
            }).join('')}</div>`;
          })
          .catch(() => {});
      }
    } catch (e) {
      detailHost.innerHTML = '<div class="empty">Error loading details</div>';
    }
  }

  function closeMovieDetail() {
    const ov = document.getElementById('movieDetailOverlay');
    if (ov) {
      ov.classList.remove('open');
      ov.removeAttribute('lang');
    }
  }

  async function searchMovieProwlarr(title) {
    document.getElementById('movieProwlarrTitle').textContent = `Prowlarr: ${title}`;
    document.getElementById('movieProwlarrResults').innerHTML = '<div class="empty">Searching indexers...</div>';
    document.getElementById('movieProwlarrOverlay').classList.add('open');
    try {
      const r = await fetch(`/api/prowlarr/search?q=${encodeURIComponent(title)}`);
      const d = await r.json();
      const results = d.results || [];
      if (!results.length) { document.getElementById('movieProwlarrResults').innerHTML = '<div class="empty">No results found</div>'; return; }
      window._movieProwlarrResults = results;
      document.getElementById('movieProwlarrResults').innerHTML = results.map((r,i) => `
        <div class="movie-prowlarr-result">
          <span class="movie-prowlarr-indexer">${esc(r.indexer||'')}</span>
          <span class="movie-prowlarr-name" title="${esc(r.title)}">${esc(r.title)}</span>
          <span class="movie-prowlarr-size">${r.size_mb ? Math.round(r.size_mb)+' MB' : ''}${r.seeders != null ? ' · S:'+r.seeders : ''}</span>
          <button type="button" class="btn-prowlarr-grab ${r.type === 'nzb' ? 'nzb' : ''}" title="Send to download client" onclick="grabMovieRelease(event, ${i})"><i class="fa-solid fa-download" aria-hidden="true"></i></button>
        </div>`).join('');
    } catch(e) { document.getElementById('movieProwlarrResults').innerHTML = `<div class="empty">Search failed: ${esc(e.message)}</div>`; }
  }

  async function grabMovieRelease(ev, idx) {
    const r = window._movieProwlarrResults[idx];
    if (!r) return;
    const btn = ev && ev.target && ev.target.closest ? ev.target.closest('button') : null;
    if (btn) {
      btn.disabled = true;
      btn.classList.remove('btn-prowlarr-grab--sent');
      btn.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Loading"></span>';
    }
    // Tag the grab with the originating movie's metadata so
    // /downloads can render its poster on the tile and we can match
    // this download back to the movie later. Movies on /scenes
    // currently come from TPDB only.
    const m = window._currentMovie || {};
    const sourceScene = m && m.id ? {
      db:         'tpdb',
      id:         String(m.id || ''),
      title:      m.title || '',
      studio:     m.studio || '',
      performers: Array.isArray(m.performers)
        ? m.performers
        : (m.performer ? [m.performer] : []),
      poster_url: m.poster || '',
      date:       m.date || m.year || '',
    } : null;
    try {
      const resp = await fetch('/api/prowlarr/grab', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({kind: 'movie', guid: r.guid, indexer_id: r.indexer_id, download_url: r.download_url || r.magnet, type: r.type, title: r.title, source_scene: sourceScene}) });
      const d = await resp.json();
      if (d.ok || d.success) {
        if (btn) { btn.classList.add('btn-prowlarr-grab--sent'); btn.innerHTML = '<i class="fa-solid fa-check" aria-hidden="true"></i>'; }
      } else {
        window.toast(d.error || 'Could not send to download client');
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-download" aria-hidden="true"></i>'; }
      }
    } catch(e) {
      window.toast(e.message || 'Could not send to download client');
      if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-download" aria-hidden="true"></i>'; }
    }
  }

  function closeMovieProwlarr() { document.getElementById('movieProwlarrOverlay').classList.remove('open'); }

  // Handle click on movie cards in the feed grid (only exists on /scenes)
  document.getElementById('scenesGrid')?.addEventListener('click', function(e) {
    const movieCard = e.target.closest('.movie-card[data-movie-id]');
    if (movieCard) {
      showMovieDetail(movieCard.getAttribute('data-movie-id') || '');
      return;
    }
  });

  // Escape key handling for movie overlays
  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') { closeMovieDetail(); closeMovieProwlarr(); }
  });

  // Load feed on page init.
  _applyFeedModeToggleUI();
  _updateTagFilterBadge();
  // Wanted eye buttons exist only on the /scenes feed grid.
  if (document.getElementById('scenesGrid')) {
    // Load before (or concurrent with) the feed so first paint matches.
    _loadWantedKeys();
  }
  // Warm VHS chrome on first hover over a movie/JAV card (does not run at parse time).
  let _chromePreloadFromHover = false;
  document.addEventListener('mouseover', function (e) {
    if (_chromePreloadFromHover) return;
    const t = e.target;
    if (!t || !t.closest) return;
    if (!t.closest('.movie-card[data-scene-i], .movie-card[data-movie-i]')) return;
    _chromePreloadFromHover = true;
    _preloadMovieDetailChromeOnIntent();
  }, { passive: true });
  // /discover has been removed — only /scenes still mounts this bundle.
  if (document.getElementById('scenesGrid')) {
    const _bootFeed = function () {
      requestAnimationFrame(function () {
        requestAnimationFrame(function () {
          if (_scenesFeedMode === 'tags') _ensureMonitoredTagsLoaded().then(function () { loadFeed(); });
          else loadFeed();
        });
      });
    };
    _bootFeed();
  }

  //: -----------------------------------------------------------------
  //: Tag-action popup. Clicking any rendered tag chip anywhere in the
  //: app opens a tiny floating menu offering "Blacklist" / "Add to
  //: Vice". Lazy-builds its DOM on first invocation; same instance is
  //: reused for every click. Dismisses on Esc, scroll, or outside-
  //: click.
  //: -----------------------------------------------------------------
  let _tagActionPopup = null;
  let _tagActionDismissBound = false;

  function _ensureTagActionPopup() {
    if (_tagActionPopup) return _tagActionPopup;
    const wrap = document.createElement('div');
    wrap.id = 'tagActionPopup';
    wrap.setAttribute('role', 'menu');
    wrap.style.cssText =
      'position:fixed;z-index:10080;display:none;flex-direction:column;gap:2px;' +
      'min-width:180px;max-width:260px;padding:6px;border-radius:10px;' +
      'background:linear-gradient(160deg, rgba(36,36,48,0.96) 0%, rgba(18,18,26,0.96) 100%);' +
      'border:1px solid rgba(var(--brand-purple-rgb), 0.35);' +
      'box-shadow:0 12px 36px rgba(0,0,0,0.7), 0 0 0 1px rgba(0,0,0,0.5);' +
      'backdrop-filter:blur(6px);-webkit-backdrop-filter:blur(6px);' +
      'font-family:var(--secs, system-ui);font-size:12px;letter-spacing:0.04em;';
    wrap.innerHTML =
      '<div data-tag-label style="padding:4px 8px 6px;color:var(--accent);font-weight:600;text-transform:uppercase;font-size:10px;letter-spacing:0.08em;border-bottom:1px solid rgba(255,255,255,0.06);margin-bottom:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis"></div>' +
      '<button type="button" data-tag-action="blacklist" style="appearance:none;background:transparent;border:none;color:var(--text);padding:8px 10px;border-radius:6px;cursor:pointer;text-align:left;display:flex;align-items:center;gap:8px;font-size:12px"><i class="fa-solid fa-ban" style="color:#ef4444;width:14px;text-align:center"></i><span>Blacklist this tag</span></button>' +
      '<button type="button" data-tag-action="vice" style="appearance:none;background:transparent;border:none;color:var(--text);padding:8px 10px;border-radius:6px;cursor:pointer;text-align:left;display:flex;align-items:center;gap:8px;font-size:12px"><i class="fa-solid fa-fire" style="color:#fb923c;width:14px;text-align:center"></i><span>Add to Vice…</span></button>' +
      '<div data-tag-vice-picker style="display:none;flex-direction:column;gap:2px;padding:6px;border-top:1px solid rgba(255,255,255,0.08);margin-top:4px;max-height:240px;overflow-y:auto"></div>';
    //: Lightweight hover affordance for action rows.
    wrap.addEventListener('mouseover', (ev) => {
      const b = ev.target.closest('button[data-tag-action], button[data-vice-pick]');
      if (b && wrap.contains(b)) b.style.background = 'rgba(var(--brand-purple-rgb), 0.18)';
    });
    wrap.addEventListener('mouseout', (ev) => {
      const b = ev.target.closest('button[data-tag-action], button[data-vice-pick]');
      if (b && wrap.contains(b)) b.style.background = 'transparent';
    });
    document.body.appendChild(wrap);
    _tagActionPopup = wrap;
    return wrap;
  }

  function _hideTagActionPopup() {
    if (_tagActionPopup) {
      _tagActionPopup.style.display = 'none';
      const picker = _tagActionPopup.querySelector('[data-tag-vice-picker]');
      if (picker) { picker.style.display = 'none'; picker.innerHTML = ''; }
      _tagActionPopup.dataset.tagName = '';
      delete _tagActionPopup.dataset.tagSource;
    }
    //: Unbind document/window listeners so they aren't firing on
    //: every scroll / keydown for the rest of the session.
    _detachTagActionDismiss();
  }

  function _positionTagActionPopup(popup, anchor) {
    const rect = anchor.getBoundingClientRect();
    const vw = window.innerWidth;
    const vh = window.innerHeight;
    //: Show, then measure — display:none has zero size.
    popup.style.left = '0px';
    popup.style.top = '0px';
    popup.style.display = 'flex';
    const pw = popup.offsetWidth || 200;
    const ph = popup.offsetHeight || 80;
    let x = rect.left;
    let y = rect.bottom + 6;
    if (x + pw + 12 > vw) x = vw - pw - 12;
    if (x < 8) x = 8;
    if (y + ph + 12 > vh) y = rect.top - ph - 6;
    if (y < 8) y = 8;
    popup.style.left = x + 'px';
    popup.style.top = y + 'px';
  }

  async function _tagActionPick(tagName, sourceDb) {
    const popup = _ensureTagActionPopup();
    popup.dataset.tagName = tagName;
    if (sourceDb) popup.dataset.tagSource = sourceDb;
    else delete popup.dataset.tagSource;
    const label = popup.querySelector('[data-tag-label]');
    if (label) label.textContent = tagName;
    const picker = popup.querySelector('[data-tag-vice-picker]');
    if (picker) { picker.style.display = 'none'; picker.innerHTML = ''; }
    return popup;
  }

  //: Resolve the source DB that served the scene whose tag chip was
  //: clicked. Looks up the chip's nearest scene card and reads its
  //: source via either:
  //:   1. ``data-scene-source`` attribute (preferred, set at render
  //:      time for chips outside the main grid)
  //:   2. ``data-scene-i`` index into ``window._sceneGridItems`` (the
  //:      main /scenes feed grid)
  //: Returns one of "tpdb" / "stashdb" / "fansdb" / "javstash" or "".
  //: The empty return is a name-only fallback — backend handles it.
  function _tagChipSourceDb(chip) {
    if (!chip) return '';
    const direct = chip.closest('[data-scene-source]');
    if (direct) {
      const v = String(direct.getAttribute('data-scene-source') || '').toLowerCase().trim();
      if (v) return v.startsWith('search_') ? v.slice(7) : v;
    }
    const card = chip.closest('[data-scene-i]');
    if (card && window._sceneGridItems) {
      const idx = parseInt(card.getAttribute('data-scene-i') || '-1', 10);
      const scene = window._sceneGridItems[idx];
      if (scene) {
        const raw = String(scene.source || scene._source || '').toLowerCase().trim();
        if (raw) return raw.startsWith('search_') ? raw.slice(7) : raw;
      }
    }
    return '';
  }

  async function _tagActionBlacklist(tagName, sourceDb) {
    try {
      const body = { name: tagName };
      if (sourceDb) body.source = sourceDb;
      const r = await fetch('/api/settings/tag-blacklist/add', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify(body),
      });
      if (!r.ok) {
        const d = await r.json().catch(() => ({}));
        window.toast && window.toast('Blacklist failed: ' + (d.error || r.statusText), { kind: 'error' });
        return;
      }
      const d = await r.json().catch(() => ({}));
      const where = d.matched_source
        ? ` (${d.matched_source.toUpperCase()})`
        : (sourceDb ? ` (${sourceDb.toUpperCase()})` : '');
      window.toast && window.toast(`Blacklisted: ${tagName}${where}`, { kind: 'success' });
    } catch (e) {
      window.toast && window.toast('Blacklist failed: ' + (e && e.message || e), { kind: 'error' });
    }
  }

  async function _tagActionOpenVicePicker(tagName) {
    const popup = _ensureTagActionPopup();
    const picker = popup.querySelector('[data-tag-vice-picker]');
    if (!picker) return;
    picker.style.display = 'flex';
    picker.innerHTML = '<div style="padding:8px;color:var(--dim);font-style:italic">Loading…</div>';
    let vices = [];
    try {
      const r = await fetch('/api/vices', { credentials: 'same-origin' });
      const d = await r.json();
      vices = Array.isArray(d.vices) ? d.vices : [];
    } catch (_) {
      picker.innerHTML = '<div style="padding:8px;color:#ef4444">Error loading vices</div>';
      return;
    }
    if (!vices.length) {
      picker.innerHTML = '<div style="padding:8px;color:var(--dim)">No vices configured</div>';
      return;
    }
    picker.innerHTML = vices.map(v => {
      const n = v && v.name ? String(v.name) : '';
      return `<button type="button" data-vice-pick="${esc(n)}" style="appearance:none;background:transparent;border:none;color:var(--text);padding:6px 10px;border-radius:6px;cursor:pointer;text-align:left;font-size:12px;display:flex;align-items:center;gap:6px"><i class="fa-solid fa-fire" style="color:#fb923c;font-size:10px"></i><span>${esc(n)}</span></button>`;
    }).join('');
    _positionTagActionPopup(popup, popup);  //: Re-clamp after height grew.
  }

  async function _tagActionAddToVice(tagName, viceName, sourceDb) {
    try {
      const body = { vice_name: viceName, name: tagName };
      if (sourceDb) body.source = sourceDb;
      const r = await fetch('/api/vices/add-tag', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify(body),
      });
      if (!r.ok) {
        const d = await r.json().catch(() => ({}));
        window.toast && window.toast('Add-to-vice failed: ' + (d.error || r.statusText), { kind: 'error' });
        return;
      }
      const d = await r.json().catch(() => ({}));
      const where = d.matched_source
        ? ` (${d.matched_source.toUpperCase()})`
        : (sourceDb ? ` (${sourceDb.toUpperCase()})` : '');
      window.toast && window.toast(`Added "${tagName}" to ${viceName}${where}`, { kind: 'success' });
    } catch (e) {
      window.toast && window.toast('Add-to-vice failed: ' + (e && e.message || e), { kind: 'error' });
    }
  }

  //: Bind on every show, unbind on hide. Avoids leaving a capture-phase
  //: scroll listener live on every page after the user's first tag
  //: click — that fired on every scroll-anywhere event for the rest of
  //: the session. The outside-click + Esc + resize listeners share the
  //: same lifecycle so they all cost nothing while the popup is closed.
  function _attachTagActionDismiss() {
    if (_tagActionDismissBound) return;
    _tagActionDismissBound = true;
    document.addEventListener('click', _tagActionDismissClick, true);
    document.addEventListener('keydown', _tagActionDismissKey);
    window.addEventListener('scroll', _hideTagActionPopup, true);
    window.addEventListener('resize', _hideTagActionPopup);
  }
  function _detachTagActionDismiss() {
    if (!_tagActionDismissBound) return;
    _tagActionDismissBound = false;
    document.removeEventListener('click', _tagActionDismissClick, true);
    document.removeEventListener('keydown', _tagActionDismissKey);
    window.removeEventListener('scroll', _hideTagActionPopup, true);
    window.removeEventListener('resize', _hideTagActionPopup);
  }
  function _tagActionDismissClick(ev) {
    if (!_tagActionPopup || _tagActionPopup.style.display === 'none') return;
    if (_tagActionPopup.contains(ev.target)) return;
    //: Don't dismiss when the click came from a tag chip — the
    //: opening handler will swap the popup to its tag.
    if (ev.target.closest('.scene-card-tag-chip,[data-tag-name]')) return;
    _hideTagActionPopup();
  }
  function _tagActionDismissKey(ev) {
    if (ev.key === 'Escape' && _tagActionPopup && _tagActionPopup.style.display !== 'none') {
      _hideTagActionPopup();
    }
  }

  //: Document-level click delegation — every tag chip in the app
  //: routes through here without the chip-render call sites needing
  //: per-chip onclick wiring. The tag NAME comes from the chip's
  //: text content (or a data-tag-name override). Selectors limited
  //: to what's actually rendered: `.scene-card-tag-chip` and any
  //: future element that opts in via `data-tag-name`.
  document.addEventListener('click', (ev) => {
    const chip = ev.target.closest('.scene-card-tag-chip,[data-tag-name]');
    if (!chip) return;
    //: Skip chips inside the popup itself (avoid recursive triggers).
    if (_tagActionPopup && _tagActionPopup.contains(chip)) return;
    ev.preventDefault();
    ev.stopPropagation();
    const tagName = (chip.getAttribute('data-tag-name') || chip.textContent || '').trim();
    if (!tagName) return;
    //: Capture which source DB served the scene this chip belongs to,
    //: so the blacklist/vice entry lands in the right per-source
    //: bucket. We REQUIRE a source — name-only entries are off the
    //: table (saved-list bookkeeping needs an ID to deduplicate
    //: across sources and the filter-set partitioning is per-source).
    //: Chips outside a scene-aware context (movie detail tags, etc.)
    //: silently bail so they stay passive labels.
    const sourceDb = _tagChipSourceDb(chip);
    if (!sourceDb) return;
    _attachTagActionDismiss();
    const popup = _ensureTagActionPopup();
    _tagActionPick(tagName, sourceDb);
    _positionTagActionPopup(popup, chip);
    //: One-shot action wiring per open (rebound each open so the
    //: closures capture the current tag name + source).
    popup.querySelector('[data-tag-action="blacklist"]').onclick = async () => {
      _hideTagActionPopup();
      await _tagActionBlacklist(tagName, sourceDb);
    };
    popup.querySelector('[data-tag-action="vice"]').onclick = async () => {
      await _tagActionOpenVicePicker(tagName);
    };
    const picker = popup.querySelector('[data-tag-vice-picker]');
    if (picker) {
      picker.onclick = async (pev) => {
        const btn = pev.target.closest('button[data-vice-pick]');
        if (!btn) return;
        const viceName = btn.getAttribute('data-vice-pick') || '';
        _hideTagActionPopup();
        await _tagActionAddToVice(tagName, viceName, sourceDb);
      };
    }
  }, true);

  //: Per-scene title blacklist — clicked from the hover ban-icon on
  //: any .scene-card in /scenes. POSTs the card's title, fades the
  //: card out on success, and lets the next feed render skip it via
  //: the backend filter (`_passes_title_filter`). All other cards in
  //: the grid carrying the same title get hidden in the same DOM pass
  //: so the user doesn't have to click ban once per duplicate.
  async function sceneCardBlacklist(btn) {
    if (!btn) return;
    const title = (btn.getAttribute('data-title') || '').trim();
    if (!title) return;
    btn.disabled = true;
    btn.classList.add('is-busy');
    try {
      const r = await fetch('/api/scenes/title-blacklist/add', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify({ title }),
      });
      if (!r.ok) {
        const d = await r.json().catch(() => ({}));
        if (typeof window.toast === 'function') {
          window.toast('Blacklist failed: ' + (d.error || r.statusText), { kind: 'error' });
        }
        btn.disabled = false;
        btn.classList.remove('is-busy');
        return;
      }
    } catch (e) {
      if (typeof window.toast === 'function') {
        window.toast('Blacklist failed: ' + (e && e.message || e), { kind: 'error' });
      }
      btn.disabled = false;
      btn.classList.remove('is-busy');
      return;
    }
    //: Hide every card on this page carrying the same title — keeps
    //: the grid honest without forcing a full refetch. Covers both
    //: ``.scene-card`` (Performers / Studios / Vices feeds) and
    //: ``.movie-card`` (Latest Movies + JAV) since the ban button is
    //: now on both card flavours.
    const norm = title.trim().toLowerCase();
    document.querySelectorAll('.scene-card .scene-card-blacklist-btn, .movie-card .scene-card-blacklist-btn').forEach(b => {
      const t = (b.getAttribute('data-title') || '').trim().toLowerCase();
      if (t && t === norm) {
        const c = b.closest('.scene-card, .movie-card');
        if (c) {
          c.style.transition = 'opacity 0.18s ease, transform 0.18s ease';
          c.style.opacity = '0';
          c.style.transform = 'scale(0.96)';
          setTimeout(() => { c.remove(); }, 200);
        }
      }
    });
    if (typeof window.toast === 'function') {
      window.toast('Hidden: ' + title, { kind: 'success' });
    }
  }
  window.sceneCardBlacklist = sceneCardBlacklist;

