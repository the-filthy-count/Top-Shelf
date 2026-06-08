/* Externalized from favourites.html. */

  const POSTER_FALLBACK = '/static/img/poster.webp';
  const STUDIO_POSTER_FALLBACK = '/static/img/missing.webp';
  let _searchRowId = null;
  let _searchKind = 'performer';
  let _searchSourceFilter = null;
  let _searchGenderFilters = null;
  let _lastSearchResults = [];
  let _lastSearchQuery = '';  // user-typed query, used by `_qsHighlight` to colour matches in result names
  /** Set when TMDB movie search returns an error string (e.g. missing API key). */
  let _movieSearchTmdbError = '';
  let _view = 'performer';
  let _rowsPerf = [];
  let _rowsStudio = [];
  let _rowsMovie = [];
  let _rowsVice = [];
  let _favStashLocalOn = false;
  let _indexPollTimer = null;
  let _entityPanelRowId = null;
  let _favLetterFilter = '';
  let _favPanelProwlarrResults = [];

  let _imagePickRowId = null;
  let _imagePickKind = 'performer';
  let _imagePickItems = [];
  let _imagePickSelectedUrl = null;

  function escAttr(s) {
    return String(s || '').replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;');
  }

  function openFavFilterSheet() {
    const sheet = document.getElementById('favFilterSheet');
    const toolbar = document.getElementById('favToolbar');
    const body = document.getElementById('favFilterSheetBody');
    if (!sheet || !toolbar || !body) return;
    if (toolbar.parentNode !== body) body.appendChild(toolbar);
    sheet.hidden = false;
    sheet.setAttribute('aria-hidden', 'false');
    sheet.classList.add('is-open');
    document.body.classList.add('fav-filter-sheet-open');
  }
  window.openFavFilterSheet = openFavFilterSheet;

  function closeFavFilterSheet() {
    const sheet = document.getElementById('favFilterSheet');
    const toolbar = document.getElementById('favToolbar');
    const host = document.getElementById('favToolbarHost');
    if (toolbar && host && toolbar.parentNode !== host) host.appendChild(toolbar);
    if (sheet) {
      sheet.classList.remove('is-open');
      sheet.hidden = true;
      sheet.setAttribute('aria-hidden', 'true');
    }
    document.body.classList.remove('fav-filter-sheet-open');
  }
  window.closeFavFilterSheet = closeFavFilterSheet;

  document.addEventListener('keydown', (ev) => {
    if (ev.key === 'Escape' && document.body.classList.contains('fav-filter-sheet-open')) {
      closeFavFilterSheet();
      ev.preventDefault();
    }
  });

  function favSceneExternalUrl(s) {
    const id = String((s && s.id) || '').trim();
    if (!id) return '#';
    const src = String((s && s.scene_source) || 'tpdb').toLowerCase();
    if (src === 'stashdb') return 'https://stashdb.org/scenes/' + encodeURIComponent(id);
    if (src === 'fansdb') return 'https://fansdb.cc/scenes/' + encodeURIComponent(id);
    if (src === 'javstash') return 'https://javstash.org/scenes/' + encodeURIComponent(id);
    return 'https://theporndb.net/scenes/' + encodeURIComponent(id);
  }

  function favSceneSourceLabel(s) {
    const src = String((s && s.scene_source) || 'tpdb').toLowerCase();
    if (src === 'stashdb') return 'StashDB';
    if (src === 'fansdb') return 'FansDB';
    if (src === 'javstash') return 'JAVStash';
    return 'TPDB';
  }

  const _SRC_LOGOS = { tpdb: 'tpdb', stashdb: 'stashdb', fansdb: 'fansdb', javstash: 'javstash', stashapp: 'stashapp', iafd: 'iafd', babepedia: 'babepedia', coomer: 'coomer', tmdb: 'tmdb', freeones: 'freeones', javdatabase: 'javdatabase' };
  function srcLogo(key, alt, cls) {
    const k = (key || '').toLowerCase().replace(/\s/g, '');
    const file = _SRC_LOGOS[k];
    if (!file) return esc(alt || key);
    const extra = cls ? ` ${cls}` : '';
    return `<img class="src-logo${extra}" src="/static/logos/${file}.webp" alt="${esc(alt || key)}">`;
  }

  function displayFolderName(name) {
    return String(name || '').trim();
  }

  window._favGetPerformerFolderName = function (rowId) {
    const row = [..._rowsPerf, ..._rowsStudio].find((r) => r.id === rowId);
    return row && row.kind === 'performer' ? displayFolderName(row.folder_name) : null;
  };

  function showMsg(t) {
    const el = document.getElementById('toolMsg');
    el.textContent = t || '';
    if (t) setTimeout(() => { el.textContent = ''; }, 4000);
  }

  function setIndexBusy(busy) {
    document.getElementById('btnSearchMissing').disabled = !!busy;
    const bScan = document.getElementById('btnLibraryScan');
    if (bScan) bScan.disabled = !!busy;
    const bImg = document.getElementById('btnRefreshImages');
    if (bImg) bImg.disabled = !!busy;
    document.getElementById('btnRefreshAll').disabled = !!busy;
  }

  function applyIndexProgress(p) {
    if (!p || !p.running) {
      setIndexBusy(false);
      if (window.TsActivity && window.TsActivity.refresh) window.TsActivity.refresh();
      return;
    }
    setIndexBusy(true);
    if (window.TsActivity && window.TsActivity.refresh) window.TsActivity.refresh();
  }

  let _indexSse = null;

  function stopIndexPolling() {
    if (_indexPollTimer) {
      clearInterval(_indexPollTimer);
      _indexPollTimer = null;
    }
    if (_indexSse) {
      try { _indexSse.close(); } catch (_) {}
      _indexSse = null;
    }
  }

  function _startIndexPollingFallback() {
    if (_indexPollTimer) return;
    const tick = async () => {
      if (document.visibilityState !== 'visible') return;
      try {
        const r = await fetch('/api/favourites/index-progress', { credentials: 'same-origin' });
        const p = await r.json();
        applyIndexProgress(p);
        if (!p.running) {
          stopIndexPolling();
          load();
        }
      } catch (_) {}
    };
    tick();
    _indexPollTimer = setInterval(tick, 1500);
  }

  function startIndexPolling() {
    // Prefer SSE — the server pushes only on actual progress changes
    // instead of the client banging on /api/favourites/index-progress
    // every 1.5 s. Falls back to the legacy polling loop on browsers
    // without EventSource or if the stream errors immediately.
    stopIndexPolling();
    if (typeof window.EventSource !== 'function') {
      _startIndexPollingFallback();
      return;
    }
    try {
      const es = new EventSource('/api/favourites/index-progress/stream', { withCredentials: true });
      _indexSse = es;
      let everOpened = false;
      es.addEventListener('open', () => { everOpened = true; });
      es.addEventListener('state', (ev) => {
        try {
          const p = JSON.parse(ev.data);
          applyIndexProgress(p);
          if (!p.running) {
            stopIndexPolling();
            load();
          }
        } catch (_) {}
      });
      es.addEventListener('error', () => {
        // Browser auto-retries SSE; if it failed before ever opening,
        // assume the endpoint is unavailable and drop back to polling.
        if (!everOpened) {
          stopIndexPolling();
          _startIndexPollingFallback();
        }
      });
    } catch (_) {
      _startIndexPollingFallback();
    }
  }

  async function checkIndexProgressOnce() {
    try {
      const r = await fetch('/api/favourites/index-progress', { credentials: 'same-origin' });
      const p = await r.json();
      applyIndexProgress(p);
      if (p.running) startIndexPolling();
    } catch (_) {}
  }

  function rowMatchesFilter(row, q) {
    if (!q) return true;
    const n = q.toLowerCase();
    const hay = [
      row.folder_name,
      row.root_label,
      row.match_tmdb_name,
      row.match_tpdb_name,
      row.match_stashdb_name,
      row.match_fansdb_name,
      (row.aliases || []).join(' '),
    ].filter(Boolean).join(' ').toLowerCase();
    return hay.includes(n);
  }

  function populateRootFilter() {
    const sel = document.getElementById('favFilterRoot');
    if (!sel) return;
    const list = _view === 'studio' ? _rowsStudio : (_view === 'movie' ? _rowsMovie : _rowsPerf);
    const labels = new Set();
    list.forEach(r => labels.add((r.root_label || '').trim()));
    const sorted = [...labels].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
    const prev = sel.value;
    const parts = ['<option value="all">All directories</option>'];
    sorted.forEach(lab => {
      if (!lab) {
        parts.push('<option value="__empty__">(no label)</option>');
      } else {
        parts.push(`<option value="${escAttr(lab)}">${esc(lab)}</option>`);
      }
    });
    sel.innerHTML = parts.join('');
    const nonEmpty = sorted.filter(Boolean);
    if (prev === 'all' || prev === '__empty__' || nonEmpty.includes(prev)) {
      sel.value = prev;
    } else {
      sel.value = 'all';
    }
  }

  function cycleFavFilterValue(h, order) {
    if (!h) return;
    const idx = order.indexOf(h.value);
    const i = idx < 0 ? 0 : idx;
    h.value = order[(i + 1) % order.length];
  }

  function updateFavFilterIconBtns() {
    normalizeUnlinkFilterValue();
    const starH = document.getElementById('favFilterStar');
    const lockH = document.getElementById('favFilterLock');
    const pathH = document.getElementById('favFilterPath');
    const starBtn = document.getElementById('favFilterStarBtn');
    const lockBtn = document.getElementById('favFilterLockBtn');
    const pathBtn = document.getElementById('favFilterPathBtn');
    if (!starH || !lockH || !pathH || !starBtn || !lockBtn || !pathBtn) return;

    const sv = starH.value;
    starBtn.dataset.filterState = sv;
    starBtn.classList.toggle('is-on', sv !== 'all');
    starBtn.setAttribute('aria-pressed', sv !== 'all' ? 'true' : 'false');
    const lipsTag = '<span class="fav-lips" aria-hidden="true"></span>';
    if (sv === 'yes') {
      starBtn.innerHTML = lipsTag;
      starBtn.title = 'Favourites: starred only. Next: non-favourites only.';
    } else if (sv === 'no') {
      starBtn.innerHTML = lipsTag;
      starBtn.title = 'Favourites: non-favourites only. Next: filter off.';
    } else {
      starBtn.innerHTML = lipsTag;
      starBtn.title = 'Favourites filter off. Click: starred only.';
    }

    const lv = lockH.value;
    lockBtn.dataset.filterState = lv;
    lockBtn.classList.toggle('is-on', lv !== 'all');
    lockBtn.setAttribute('aria-pressed', lv !== 'all' ? 'true' : 'false');
    if (lv === 'unlocked') {
      lockBtn.innerHTML = '<i class="fa-solid fa-lock-open" aria-hidden="true"></i>';
      lockBtn.title = 'Matches: unlocked only. Next: locked only.';
    } else if (lv === 'locked') {
      lockBtn.innerHTML = '<i class="fa-solid fa-lock" aria-hidden="true"></i>';
      lockBtn.title = 'Matches: locked only. Next: filter off.';
    } else {
      lockBtn.innerHTML = '<i class="fa-solid fa-lock-open" aria-hidden="true"></i>';
      lockBtn.title = 'Lock filter off. Click: unlocked only.';
    }

    const pv = pathH.value;
    pathBtn.dataset.filterState = pv;
    pathBtn.classList.toggle('is-on', pv !== 'all');
    pathBtn.setAttribute('aria-pressed', pv !== 'all' ? 'true' : 'false');
    if (pv === 'missing') {
      pathBtn.innerHTML = '<i class="fa-solid fa-triangle-exclamation" aria-hidden="true"></i>';
      pathBtn.title = 'Path: missing on disk. Next: on disk only.';
    } else if (pv === 'ok') {
      pathBtn.innerHTML = '<i class="fa-solid fa-circle-check" aria-hidden="true"></i>';
      pathBtn.title = 'Path: on disk only. Next: filter off.';
    } else {
      pathBtn.innerHTML = '<i class="fa-solid fa-triangle-exclamation" aria-hidden="true"></i>';
      pathBtn.title = 'Path filter off. Click: missing on disk.';
    }

    const unlinkH = document.getElementById('favFilterUnlinked');
    const unlinkBtn = document.getElementById('favFilterUnlinkedBtn');
    if (unlinkH && unlinkBtn) {
      unlinkBtn.dataset.filterState = unlinkH.value;
      unlinkBtn.classList.toggle('is-on', unlinkH.value !== 'all');
      unlinkBtn.setAttribute('aria-pressed', unlinkH.value !== 'all' ? 'true' : 'false');
      const u = unlinkH.value;
      if (u === 'linked') {
        unlinkBtn.innerHTML = '<i class="fa-solid fa-link" aria-hidden="true"></i>';
        unlinkBtn.title = 'Links: with database links only. Next: filter off.';
      } else if (u === 'unlinked') {
        unlinkBtn.innerHTML = '<i class="fa-solid fa-link-slash fav-unlinked-fa"></i>';
        unlinkBtn.title = 'Links: no database links. Next: linked only.';
      } else {
        unlinkBtn.innerHTML = '<i class="fa-solid fa-link-slash fav-unlinked-fa"></i>';
        unlinkBtn.title = 'Links filter off. Click: no database links.';
      }
    }
  }

  function clearAllFavFilters() {
    const star = document.getElementById('favFilterStar');
    const lock = document.getElementById('favFilterLock');
    const path = document.getElementById('favFilterPath');
    const unlink = document.getElementById('favFilterUnlinked');
    const root = document.getElementById('favFilterRoot');
    const q = document.getElementById('favFilter');
    if (star) star.value = 'all';
    if (lock) lock.value = 'all';
    if (path) path.value = 'all';
    if (unlink) unlink.value = 'all';
    if (root) root.value = 'all';
    if (q) q.value = '';
    setAlphaFilter('', false);
    updateFavFilterIconBtns();
    _favPage = 1;
    render();
  }

  function setAlphaFilter(letter, doRender = true) {
    _favLetterFilter = letter;
    document.querySelectorAll('#favAlphaBar .fav-alpha-btn').forEach(btn => {
      btn.classList.toggle('is-active', btn.dataset.letter === letter);
    });
    if (doRender) { _favPage = 1; render(); }
  }

  // Filter changes reset the pager to page 1 — otherwise a narrower
  // filter that still has > _FAV_PAGE_SIZE rows would leave you on
  // an unrelated mid-list page. Wraps `render()` so every filter
  // entry point uses the same path.
  function _renderResetPage() { _favPage = 1; render(); }

  function toggleFavFilterStar() {
    const h = document.getElementById('favFilterStar');
    cycleFavFilterValue(h, ['all', 'yes', 'no']);
    updateFavFilterIconBtns();
    _renderResetPage();
  }

  function toggleFavFilterLock() {
    const h = document.getElementById('favFilterLock');
    cycleFavFilterValue(h, ['all', 'unlocked', 'locked']);
    updateFavFilterIconBtns();
    _renderResetPage();
  }

  function toggleFavFilterPath() {
    const h = document.getElementById('favFilterPath');
    cycleFavFilterValue(h, ['all', 'missing', 'ok']);
    updateFavFilterIconBtns();
    _renderResetPage();
  }

  function rowHasAnyDatabaseLink(row) {
    const k = row.kind;
    if (k === 'movie' || k === 'jav') {
      return !!(row.match_tmdb_id || row.match_tpdb_id || row.match_javstash_id);
    }
    return !!(row.match_tpdb_id || row.match_stashdb_id || row.match_fansdb_id || row.match_javstash_id);
  }

  function toggleFavFilterUnlinked() {
    const h = document.getElementById('favFilterUnlinked');
    if (!h) return;
    if (h.value === 'only') h.value = 'unlinked';
    cycleFavFilterValue(h, ['all', 'unlinked', 'linked']);
    updateFavFilterIconBtns();
    _renderResetPage();
  }

  function normalizeUnlinkFilterValue() {
    const el = document.getElementById('favFilterUnlinked');
    if (el && el.value === 'only') el.value = 'unlinked';
  }

  function rowPassesFilters(row) {
    normalizeUnlinkFilterValue();
    const q = document.getElementById('favFilter').value.trim();
    if (!rowMatchesFilter(row, q)) return false;
    const fav = document.getElementById('favFilterStar')?.value || 'all';
    const lock = document.getElementById('favFilterLock')?.value || 'all';
    const root = document.getElementById('favFilterRoot')?.value ?? 'all';
    const pathSel = document.getElementById('favFilterPath')?.value || 'all';
    const unlinkSel = document.getElementById('favFilterUnlinked')?.value || 'all';
    const isFav = !!row.is_favourite;
    const isLocked = !!row.matches_locked;
    const pathMissing = row.path_missing === true || row.path_missing === 1;
    if (fav === 'yes' && !isFav) return false;
    if (fav === 'no' && isFav) return false;
    if (lock === 'locked' && !isLocked) return false;
    if (lock === 'unlocked' && isLocked) return false;
    if (pathSel === 'missing' && !pathMissing) return false;
    if (pathSel === 'ok' && pathMissing) return false;
    if (unlinkSel === 'unlinked' && rowHasAnyDatabaseLink(row)) return false;
    if (unlinkSel === 'linked' && !rowHasAnyDatabaseLink(row)) return false;
    if (_favLetterFilter) {
      const first = (row.folder_name || '').trimStart()[0] || '';
      if (first.toUpperCase() !== _favLetterFilter) return false;
    }
    if (root !== 'all') {
      const rl = (row.root_label || '').trim();
      if (root === '__empty__') {
        if (rl) return false;
      } else if (rl !== root) {
        return false;
      }
    }
    return true;
  }

  function filtersActive() {
    normalizeUnlinkFilterValue();
    const q = document.getElementById('favFilter').value.trim();
    const fav = document.getElementById('favFilterStar')?.value || 'all';
    const lock = document.getElementById('favFilterLock')?.value || 'all';
    const root = document.getElementById('favFilterRoot')?.value || 'all';
    const pathSel = document.getElementById('favFilterPath')?.value || 'all';
    const unlinkSel = document.getElementById('favFilterUnlinked')?.value || 'all';
    return !!(q || fav !== 'all' || lock !== 'all' || root !== 'all' || pathSel !== 'all' || unlinkSel !== 'all' || _favLetterFilter);
  }

  const _FAV_VIEW_TITLES = {
    movie: 'Movies Library',
    performer: 'Stars Library',
    studio: 'Studios Library',
    vice: 'Vice Library',
    search: 'Search Library',
  };

  function setView(kind) {
    _view = kind;
    _favPage = 1;  // each tab starts on its first page
    if (typeof tsSetPageTitleText === 'function') {
      tsSetPageTitleText(document.getElementById('favPageTitle'), _FAV_VIEW_TITLES[kind] || 'Library', 'library');
    }
    document.getElementById('segSearch')?.classList.toggle('active', kind === 'search');
    document.getElementById('segPerf').classList.toggle('active', kind === 'performer');
    document.getElementById('segStudio').classList.toggle('active', kind === 'studio');
    document.getElementById('segMovie').classList.toggle('active', kind === 'movie');
    document.getElementById('segVice')?.classList.toggle('active', kind === 'vice');
    // Search tab: hide the filter chrome + paged grid, show the search
    // panel. Everything else stays cached so flipping back is instant.
    const searchHost = document.getElementById('favSearchHost');
    const toolbarHost = document.getElementById('favToolbarHost');
    const mobileFilterBtn = document.getElementById('favMobileFilterOpen');
    const pager = document.getElementById('favPager');
    const gridEl = document.getElementById('favGrid');
    if (kind === 'search') {
      if (searchHost) searchHost.hidden = false;
      if (toolbarHost) toolbarHost.style.display = 'none';
      if (mobileFilterBtn) mobileFilterBtn.style.display = 'none';
      if (pager) pager.style.display = 'none';
      if (gridEl) gridEl.style.display = 'none';
      _mountFavSearchOnce();
      return;
    } else {
      if (searchHost) searchHost.hidden = true;
      if (toolbarHost) toolbarHost.style.display = '';
      if (mobileFilterBtn) mobileFilterBtn.style.display = '';
      if (gridEl) gridEl.style.display = '';
    }
    const grid = document.getElementById('favGrid');
    grid.classList.remove('fav-grid--perf', 'fav-grid--studio', 'fav-grid--movie', 'fav-grid--vice');
    // Vices render as studio-style tiles (no functionality wired yet, just
    // reusing the studio grid + duotone + logo pipeline).
    const studioLike = kind === 'studio' || kind === 'vice';
    grid.classList.add(studioLike ? 'fav-grid--studio' : 'fav-grid--perf');
    // Add a kind-specific modifier alongside the layout class so the
    // VHS theme can apply different effects per kind (movies → paper
    // VHS cover + halftone, vices → studio-style with VHS static).
    if (kind === 'movie') grid.classList.add('fav-grid--movie');
    if (kind === 'vice')  grid.classList.add('fav-grid--vice');
    // Lazy per-tab fetch — if we've never loaded this tab's kind, pull
    // just its rows. The skeleton below paints first so the tab-switch
    // visual lands instantly even if the fetch takes a moment.
    const kinds = _LIB_TAB_KINDS[kind] || [];
    const need = kinds.filter(k => !_loadedKinds.has(k));
    populateRootFilter();
    // Paint a skeleton on the same tick so the click visually lands
    // immediately, then defer the heavy `render()` to the next frame.
    // Stops the multi-hundred-tile rebuild from blocking the tab-active
    // class swap and any active CSS transition. The browser paints the
    // skeleton on the current frame and the real grid on the next.
    const list = _view === 'studio' ? _rowsStudio
               : _view === 'movie'  ? _rowsMovie
               : _view === 'vice'   ? _rowsVice
               :                      _rowsPerf;
    if (list && list.length) {
      const target = Math.min(list.length, 24);
      grid.innerHTML = Array.from({length: target}, () =>
        '<div class="fav-cell fav-cell--skeleton" aria-hidden="true"><div class="fav-cell-visual" style="aspect-ratio:2/3"></div></div>'
      ).join('');
    } else if (need.length) {
      // First-time visit to a tab that has no data yet — show a generic
      // 24-tile skeleton until the per-kind fetch lands.
      grid.innerHTML = Array.from({length: 24}, () =>
        '<div class="fav-cell fav-cell--skeleton" aria-hidden="true"><div class="fav-cell-visual" style="aspect-ratio:2/3"></div></div>'
      ).join('');
    }
    if (need.length) {
      // Fire the per-kind fetches in parallel; render once they all
      // land. Failures fall through to the existing toast/showMsg path.
      Promise.all(need.map(_loadKind))
        .then(() => {
          populateRootFilter();
          updateFavFilterIconBtns();
          render();
        })
        .catch((e) => showMsg(e.message || 'Error'));
    } else {
      requestAnimationFrame(render);
    }
  }

  function buildExternalUrl(row, src) {
    if (src === 'TMDB' && row.match_tmdb_id) {
      return `https://www.themoviedb.org/movie/${encodeURIComponent(String(row.match_tmdb_id))}`;
    }
    if (src === 'TPDB' && row.match_tpdb_id) {
      const id = String(row.match_tpdb_id);
      if (row.kind === 'movie' || row.kind === 'jav') {
        return `https://theporndb.net/movies/${encodeURIComponent(id)}`;
      }
      return row.kind === 'studio'
        ? `https://theporndb.net/sites/${encodeURIComponent(id)}`
        : `https://theporndb.net/performers/${encodeURIComponent(id)}`;
    }
    if (src === 'StashDB' && row.match_stashdb_id) {
      const id = String(row.match_stashdb_id);
      return row.kind === 'studio'
        ? `https://stashdb.org/studios/${encodeURIComponent(id)}`
        : `https://stashdb.org/performers/${encodeURIComponent(id)}`;
    }
    if (src === 'FansDB' && row.match_fansdb_id) {
      const id = encodeURIComponent(String(row.match_fansdb_id));
      return row.kind === 'studio'
        ? `https://fansdb.cc/studios/${id}`
        : `https://fansdb.cc/performers/${id}`;
    }
    if (src === 'JAVStash' && row.match_javstash_id) {
      const id = encodeURIComponent(String(row.match_javstash_id));
      if (row.kind === 'movie' || row.kind === 'jav') {
        return `https://javstash.org/scenes/${id}`;
      }
      return row.kind === 'studio'
        ? `https://javstash.org/studios/${id}`
        : `https://javstash.org/performers/${id}`;
    }
    if (src === 'IAFD' && row.match_iafd_url) return row.match_iafd_url;
    if (src === 'Freeones' && row.match_freeones_url) return row.match_freeones_url;
    if (src === 'Babepedia' && row.match_babepedia_url) return row.match_babepedia_url;
    if ((src === 'JAV Database' || src === 'JAVDATABASE') && row.match_javdatabase_url) return row.match_javdatabase_url;
    return null;
  }

  function renderSearchFilterChips() {
    const wrap = document.getElementById('searchFilters');
    if (_searchKind === 'movie') {
      const chips = [
        { key: null, label: 'All' },
        { key: 'TMDB',     logo: 'tmdb',     label: 'TMDB' },
        { key: 'TPDB',     logo: 'tpdb',     label: 'TPDB' },
        { key: 'JAVSTASH', logo: 'javstash', label: 'JAVStash' },
      ];
      wrap.innerHTML = chips.map(c => {
        const on = _searchSourceFilter === c.key || (c.key === null && !_searchSourceFilter);
        const inner = c.logo ? srcLogo(c.logo, c.label, 'src-logo--sm') : esc(c.label);
        return `<button type="button" class="${on ? 'active' : ''}" data-sf="${c.key === null ? '' : esc(c.key)}">${inner}</button>`;
      }).join('');
      wrap.querySelectorAll('button').forEach(btn => {
        btn.onclick = () => {
          const v = btn.getAttribute('data-sf');
          _searchSourceFilter = v === '' ? null : v;
          renderSearchFilterChips();
          renderSearchResultsList();
        };
      });
      return;
    }
    const chips = [
      { key: null, label: 'All' },
      { key: 'TPDB', label: 'TPDB' },
      { key: 'StashDB', label: 'StashDB' },
      { key: 'FansDB', label: 'FansDB' },
      { key: 'JAVStash', label: 'JAVStash' },
    ];
    wrap.innerHTML = chips.map(c => {
      const on = _searchSourceFilter === c.key || (c.key === null && !_searchSourceFilter);
      const display = c.key ? srcLogo(c.key, c.label, 'src-logo--sm') : esc(c.label);
      return `<button type="button" class="${on ? 'active' : ''}" data-sf="${c.key === null ? '' : esc(c.key)}">${display}</button>`;
    }).join('');
    wrap.querySelectorAll('button').forEach(btn => {
      btn.onclick = () => {
        const v = btn.getAttribute('data-sf');
        _searchSourceFilter = v === '' ? null : v;
        renderSearchFilterChips();
        renderSearchResultsList();
      };
    });
  }

  function favPillRow(id, label, src, ok) {
    const x = ok
      ? `<button type="button" class="fav-pill-x" data-act="unmatch" data-id="${id}" data-src="${src}" title="Remove ${esc(label)} link" aria-label="Remove ${esc(label)} link"><i class="fa-solid fa-xmark"></i></button>`
      : '';
    return `<div class="fav-pill-row">
      <button type="button" class="fav-pill ${ok ? 'fav-pill--ok' : 'fav-pill--miss'}" data-act="pill" data-id="${id}" data-src="${src}">${label}</button>
      ${x}
    </div>`;
  }

  /** External reference links for a performer: IAFD, Babepedia, Freeones (always URL-constructable). */
  function favExternalLinks(row) {
    const name = (row.folder_name || '').trim();
    if (!name) return '';
    const iafdUrl = 'https://www.iafd.com/results.asp?searchtype=comprehensive&searchstring=' + encodeURIComponent(name);
    const babepediaUrl = 'https://www.babepedia.com/babe/' + encodeURIComponent(name.replace(/ /g, '_'));
    const freeonesUrl = 'https://www.freeones.com/' + encodeURIComponent(name.replace(/ /g, '-')) + '/bio';
    return `<div class="fav-ov-ext-links">
      <a href="${escAttr(iafdUrl)}" target="_blank" rel="noopener noreferrer" class="fav-ov-ext-btn" title="IAFD">${srcLogo('iafd','IAFD')}</a>
      <a href="${escAttr(babepediaUrl)}" target="_blank" rel="noopener noreferrer" class="fav-ov-ext-btn" title="Babepedia">${srcLogo('babepedia','Babepedia')}</a>
      <a href="${escAttr(freeonesUrl)}" target="_blank" rel="noopener noreferrer" class="fav-ov-ext-btn" title="Freeones">${srcLogo('freeones','Freeones')}</a>
    </div>`;
  }

  /** Icon row: recent-scenes popup (ticket) + local Stash app (circle-play). */
  function favOverlayLinkIcons(id, kind, row) {
    const tpOk = !!(row.match_tpdb_id);
    const stOk = !!(row.match_stashdb_id);
    const fnOk = !!(row.match_fansdb_id);
    const hasBoxId = !!(row.match_tpdb_id || row.match_stashdb_id || row.match_fansdb_id || row.match_javstash_id);
    const isMovieKind = kind === 'movie' || kind === 'jav';
    const scenesOk = !isMovieKind && (tpOk || stOk || fnOk);
    const ticketBtn = `<button type="button" class="fav-ov-icon-btn" data-act="scenes-popup" data-id="${id}" data-stop="1" ${isMovieKind || !scenesOk ? 'disabled' : ''} title="Recent Scenes."><i class="fa-solid fa-ticket" aria-hidden="true"></i></button>`;
    let stashBtn;
    if (!_favStashLocalOn || isMovieKind) {
      stashBtn = `<button type="button" class="fav-ov-icon-btn" disabled title="Stash"><i class="fa-solid fa-circle-play" aria-hidden="true"></i></button>`;
    } else {
      stashBtn = `<button type="button" class="fav-ov-icon-btn" data-act="local-stash-fav" data-id="${id}" ${hasBoxId ? '' : 'disabled'} title="Stash"><i class="fa-solid fa-circle-play" aria-hidden="true"></i></button>`;
    }
    return `<div class="fav-ov-link-icons">${ticketBtn}${stashBtn}</div>`;
  }

  function mapSearchHitSourceToStashApi(src) {
    const u = String(src || '').trim();
    if (u === 'StashDB') return 'stashdb';
    if (u === 'FansDB') return 'fansdb';
    if (u === 'TPDB') return 'tpdb';
    if (u === 'JAVStash') return 'javstash';
    return null;
  }

  async function openLocalStashFromBtn(btn) {
    const kind = btn.getAttribute('data-kind') || '';
    const source = btn.getAttribute('data-src') || '';
    const ext = btn.getAttribute('data-ext') || '';
    if (!kind || !source || !ext) return;
    showMsg('Looking up Stash…');
    try {
      const q = new URLSearchParams({ kind, source, external_id: ext });
      const r = await fetch('/api/stash/local-entity-url?' + q.toString(), { credentials: 'same-origin' });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) {
        showMsg('');
        window.toast(d.error || 'Stash lookup failed');
        return;
      }
      showMsg('');
      if (d.url) window.open(d.url, '_blank', 'noopener,noreferrer');
      else window.toast('No star/studio in local Stash is linked to this stash-box id (check Stash ↔ stash-box sync).');
    } catch (e) {
      showMsg('');
      window.toast(e.message || 'Stash lookup failed');
    }
  }

  async function openLocalStashFromFavouriteRow(rowId) {
    if (!rowId) return;
    showMsg('Looking up Stash…');
    try {
      const r = await fetch('/api/stash/local-entity-from-favourite?row_id=' + encodeURIComponent(rowId), { credentials: 'same-origin' });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) {
        showMsg('');
        window.toast(d.error || 'Stash lookup failed');
        return;
      }
      showMsg('');
      if (d.url) window.open(d.url, '_blank', 'noopener,noreferrer');
      else {
        window.toast('No matching star/studio in local Stash. Ensure Stash has this entity linked to ThePornDB / StashDB / FansDB, and that Settings has your Stash URL and API key.');
      }
    } catch (e) {
      showMsg('');
      window.toast(e.message || 'Stash lookup failed');
    }
  }

  function entityPanelLocalStashBlock(row) {
    if (!_favStashLocalOn || row.kind === 'movie' || row.kind === 'jav') return '';
    const hasBoxId = !!(row.match_tpdb_id || row.match_stashdb_id || row.match_fansdb_id || row.match_javstash_id);
    return `<div class="entity-section-title">Local Stash</div><div class="entity-match-btns">
      <button type="button" class="entity-stash-local" data-act="local-stash-fav" data-id="${row.id}" ${hasBoxId ? '' : 'disabled'} title="Open in Stash">Open in local Stash…</button>
    </div>`;
  }

  function searchHitProfileUrl(x) {
    const src = String(x.source || '').trim();
    const id = String(x.id || '').trim();
    const slug = String(x.slug || x.id || '').trim();
    if (!id && !slug) return '';
    const isStudio = _searchKind === 'studio';
    const segSlug = encodeURIComponent(slug || id);
    const segId = encodeURIComponent(id);
    if (src === 'TPDB')
      return isStudio ? `https://theporndb.net/sites/${segSlug}` : `https://theporndb.net/performers/${segSlug}`;
    if (src === 'StashDB')
      return isStudio ? `https://stashdb.org/studios/${segId}` : `https://stashdb.org/performers/${segId}`;
    if (src === 'FansDB')
      return isStudio ? `https://fansdb.cc/studios/${segId}` : `https://fansdb.cc/performers/${segId}`;
    if (src === 'JAVStash')
      return isStudio ? `https://javstash.org/studios/${segId}` : `https://javstash.org/performers/${segId}`;
    return '';
  }

  function searchHitBrowseUrl(site, x) {
    const q = encodeURIComponent((x.name || '').trim() || '');
    const isStudio = _searchKind === 'studio';
    if (site === 'TPDB')
      return isStudio ? `https://theporndb.net/sites?search=${q}` : `https://theporndb.net/performers?search=${q}`;
    if (site === 'StashDB')
      return isStudio ? `https://stashdb.org/studios?q=${q}` : `https://stashdb.org/performers?q=${q}`;
    if (site === 'FansDB')
      return isStudio ? `https://fansdb.cc/studios?q=${q}` : `https://fansdb.cc/performers?q=${q}`;
    if (site === 'JAVStash')
      return isStudio ? `https://javstash.org/studios?q=${q}` : `https://javstash.org/performers?q=${q}`;
    return '#';
  }

  function searchHitExternalHref(site, x) {
    const hit = String(x.source || '').trim() === site;
    if (hit) {
      const direct = searchHitProfileUrl(x);
      if (direct) return direct;
    }
    return searchHitBrowseUrl(site, x);
  }

  function movieSearchHitExternalHref(site, x) {
    const hit = String(x.source || '').trim() === site;
    if (site === 'TMDB' && hit) {
      const id = String(x.id || '').trim();
      return id ? `https://www.themoviedb.org/movie/${encodeURIComponent(id)}` : '';
    }
    if (site === 'TPDB' && hit) {
      const id = String(x.id || '').trim();
      return id ? `https://theporndb.net/movies/${encodeURIComponent(id)}` : '';
    }
    const q = encodeURIComponent((x.name || '').trim() || '');
    if (site === 'TMDB') return `https://www.themoviedb.org/search/movie?query=${q}`;
    if (site === 'TPDB') return `https://theporndb.net/movies?search=${q}`;
    return '#';
  }

  function renderMovieSearchResultsList() {
    const box = document.getElementById('searchResults');
    let list = _lastSearchResults || [];
    if (_searchSourceFilter) {
      list = list.filter(x => (x.source || '') === _searchSourceFilter);
    }
    const warn = _movieSearchTmdbError
      ? `<div class="empty-tile" style="padding:10px 20px;font-size:13px;color:var(--dim)">${esc(_movieSearchTmdbError)}</div>`
      : '';
    if (!list.length) {
      box.innerHTML = warn + '<div class="empty-tile" style="padding:20px">No results for this filter.</div>';
      return;
    }
    const noSearchImg = POSTER_FALLBACK;
    const kindClass = 'sr-item--movie';
    const qsHighlight = (typeof _qsBuildHighlightSet === 'function') ? _qsBuildHighlightSet(_lastSearchQuery) : null;
    const _hl = (s) => (qsHighlight && typeof _qsHighlight === 'function') ? _qsHighlight(s, qsHighlight) : esc(s);
    const rows = list.map(x => {
      const thumbInner = x.image
        ? `<img src="${esc(x.image)}" alt="" loading="lazy" referrerpolicy="no-referrer">`
        : `<img src="${noSearchImg}" alt="" loading="lazy" referrerpolicy="no-referrer">`;
      const yearBit = x.year ? ` <span style="color:var(--dim)">${esc(x.year)}</span>` : '';
      const links = [{ key:'TMDB', logo:'tmdb' }, { key:'TPDB', logo:'tpdb' }].map(s => {
        const href = movieSearchHitExternalHref(s.key, x);
        const isHit = String(x.source || '').trim() === s.key;
        const hint = isHit ? 'This hit — open on ' + s.key : 'Open ' + s.key + ' search';
        return `<a href="${escAttr(href)}" target="_blank" rel="noopener noreferrer" title="${esc(hint)}" onclick="event.stopPropagation()">${srcLogo(s.logo, s.key, 'src-logo--sm')}</a>`;
      }).join('<span class="sr-ext-sep" aria-hidden="true">·</span>');
      const srcColour = x.source === 'TPDB' ? 'var(--accent)' : '#60a5fa';
      const srcBadge = `<span style="display:inline-flex;align-items:center;gap:4px;font-size:9px;font-weight:700;letter-spacing:0.06em;text-transform:uppercase;color:${srcColour};background:${srcColour}22;padding:2px 8px;border-radius:4px;margin-bottom:6px">${srcLogo(x.source, x.source, 'src-logo--sm')}</span>`;
      return `<div class="sr-item ${kindClass}" role="button" tabindex="0"
        data-source="${esc(x.source)}"
        data-id="${esc(String(x.id))}"
        data-name="${esc(x.name)}"
        data-image="${esc(x.image || '')}"
        onclick="pickSearch(this)">
        <div class="sr-item__thumb">${thumbInner}</div>
        <div class="sr-item__body">
          ${srcBadge}
          <div class="sr-name">${_hl(x.name)}${yearBit}</div>
          <div class="sr-ext">
            <span class="sr-ext-label">Open</span>
            ${links}
          </div>
        </div>
        <div style="flex-shrink:0;display:flex;align-items:center;padding-left:8px">
          <button type="button" style="background:rgba(var(--brand-purple-rgb),0.4);border:1px solid rgba(var(--brand-purple-rgb),0.65);color:var(--accent);border-radius:6px;padding:6px 12px;font-size:11px;font-family:var(--mono);cursor:pointer;white-space:nowrap" onclick="event.stopPropagation();pickSearch(this.closest('.sr-item'))">Select</button>
        </div>
      </div>`;
    }).join('');
    box.innerHTML = warn + rows;
  }

  function normPerformerSearchText(s) {
    return String(s || '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim().replace(/\s+/g, ' ');
  }

  function performerSearchMatchScore(item, query) {
    const want = normPerformerSearchText(query);
    if (!want) return 0;
    const name = normPerformerSearchText(item.name);
    const qTok = want.split(' ').filter(Boolean);
    if (name === want) return 100;
    const aliases = Array.isArray(item.aliases) ? item.aliases : [];
    for (const a of aliases) {
      if (normPerformerSearchText(a) === want) return 95;
    }
    if (want.length >= 2 && name.includes(want)) return 88;
    if (qTok.length && qTok.every((t) => name.includes(t))) {
      const nameTok = name.split(' ').filter(Boolean);
      const qKey = [...qTok].sort().join(' ');
      const nKey = [...nameTok].sort().join(' ');
      if (qKey === nKey && name !== want) return 72;
      return 65;
    }
    for (const a of aliases) {
      const an = normPerformerSearchText(a);
      if (qTok.length && qTok.every((t) => an.includes(t))) return 78;
    }
    return 10;
  }

  function sortPerformerSearchResults(list, query) {
    return [...list].sort((a, b) => {
      const d = performerSearchMatchScore(b, query) - performerSearchMatchScore(a, query);
      if (d !== 0) return d;
      return String(a.name || '').localeCompare(String(b.name || ''), undefined, { sensitivity: 'base' });
    });
  }

  function performerSearchDisplayAliases(item) {
    const primary = String(item.name || '').trim();
    const pk = normPerformerSearchText(primary);
    let list = (Array.isArray(item.aliases) ? item.aliases : [])
      .map((a) => String(a || '').trim())
      .filter((a) => a && normPerformerSearchText(a) !== pk);
    const q = normPerformerSearchText(_lastSearchQuery || '');
    if (q) {
      list.sort((a, b) => {
        const as = normPerformerSearchText(a).includes(q) ? 1 : 0;
        const bs = normPerformerSearchText(b).includes(q) ? 1 : 0;
        return bs - as;
      });
    }
    // JAV rows often use a Japanese display name; surface the Latin search
    // term when the API returned no alias list so the user sees the match.
    if (!list.length && _lastSearchQuery) {
      const qRaw = String(_lastSearchQuery || '').trim();
      const q = normPerformerSearchText(qRaw);
      const nk = normPerformerSearchText(primary);
      if (q && q !== nk && /[a-z0-9]/.test(q) && !/[a-z]/.test(nk)) {
        list = [qRaw];
      }
    }
    return list;
  }

  function searchSourceColour(source) {
    const s = String(source || '').trim();
    if (s === 'TPDB') return 'var(--accent)';
    if (s === 'FansDB') return '#22c55e';
    if (s === 'JAVStash') return '#7c3aed';
    if (s === 'StashDB') return '#60a5fa';
    return 'var(--accent)';
  }

  function renderSearchResultsList() {
    const box = document.getElementById('searchResults');
    if (_searchKind === 'movie') {
      renderMovieSearchResultsList();
      return;
    }
    let list = _lastSearchResults || [];
    if (_searchKind === 'performer' && _lastSearchQuery) {
      list = sortPerformerSearchResults(list, _lastSearchQuery);
    }
    if (_searchSourceFilter) {
      list = list.filter(x => (x.source || '') === _searchSourceFilter);
    }
    if (!list.length) {
      box.innerHTML = '<div class="empty-tile" style="padding:20px">No results for this filter.</div>';
      return;
    }
    const noSearchImg = _searchKind === 'studio' ? STUDIO_POSTER_FALLBACK : POSTER_FALLBACK;
    const kindClass = _searchKind === 'studio' ? 'sr-item--studio' : 'sr-item--performer';
    const qsHighlight = (typeof _qsBuildHighlightSet === 'function') ? _qsBuildHighlightSet(_lastSearchQuery) : null;
    const _hl = (s) => (qsHighlight && typeof _qsHighlight === 'function') ? _qsHighlight(s, qsHighlight) : esc(s);
    box.innerHTML = list.map(x => {
      const thumbInner = x.image
        ? `<img src="${esc(x.image)}" alt="" loading="lazy" referrerpolicy="no-referrer">`
        : `<img src="${noSearchImg}" alt="" loading="lazy" referrerpolicy="no-referrer">`;
      const sites = [
        { site: 'TPDB', label: 'TPDB' },
        { site: 'StashDB', label: 'StashDB' },
        { site: 'FansDB', label: 'FansDB' },
      ];
      sites.push({ site: 'JAVStash', label: 'JAVStash' });
      const links = sites.map(({ site, label }) => {
        const href = searchHitExternalHref(site, x);
        const isHit = String(x.source || '').trim() === site;
        const hint = isHit ? 'This search hit — open profile' : 'Open site search';
        return `<a href="${escAttr(href)}" target="_blank" rel="noopener noreferrer" title="${esc(hint)}" onclick="event.stopPropagation()">${srcLogo(site, label, 'src-logo--sm')}</a>`;
      }).join('<span class="sr-ext-sep" aria-hidden="true">·</span>');
      const stashApi = _favStashLocalOn ? mapSearchHitSourceToStashApi(x.source) : null;
      const sid = String(x.id || '').trim();
      const stashHit = stashApi && sid
        ? `<span class="sr-ext-sep" aria-hidden="true">·</span><button type="button" class="sr-stash-local" data-act="local-stash" data-kind="${escAttr(_searchKind)}" data-src="${escAttr(stashApi)}" data-ext="${escAttr(sid)}" title="Open in local Stash">${srcLogo('stashapp', 'Stash', 'src-logo--sm')}</button>`
        : '';
      const srcColour = x.source === 'TPDB'
        ? 'var(--accent)'
        : x.source === 'FansDB'
          ? '#22c55e'
          : x.source === 'JAVStash'
            ? '#7c3aed'
            : 'var(--accent)';
      const srcBadge = `<span style="display:inline-flex;align-items:center;gap:4px;font-size:9px;font-weight:700;letter-spacing:0.06em;text-transform:uppercase;color:${srcColour};background:${srcColour}22;padding:2px 8px;border-radius:4px;margin-bottom:6px">${srcLogo(x.source, x.source, 'src-logo--sm')}</span>`;
      return `<div class="sr-item ${kindClass}" role="button" tabindex="0"
        data-source="${esc(x.source)}"
        data-id="${esc(String(x.id))}"
        data-name="${esc(x.name)}"
        data-image="${esc(x.image || '')}"
        onclick="pickSearch(this)">
        <div class="sr-item__thumb">${thumbInner}</div>
        <div class="sr-item__body">
          ${srcBadge}
          <div class="sr-name">${_hl(x.name)}</div>
          <div class="sr-ext">
            <span class="sr-ext-label">Open</span>
            ${links}${stashHit}
          </div>
        </div>
        <div style="flex-shrink:0;display:flex;align-items:center;padding-left:8px">
          <button type="button" style="background:rgba(var(--brand-purple-rgb),0.4);border:1px solid rgba(var(--brand-purple-rgb),0.65);color:var(--accent);border-radius:6px;padding:6px 12px;font-size:11px;font-family:var(--mono);cursor:pointer;white-space:nowrap" onclick="event.stopPropagation();pickSearch(this.closest('.sr-item'))">Select</button>
        </div>
      </div>`;
    }).join('');
  }

  function renderCard(row) {
    const id = row.id;
    const kind = row.kind;
    const locked = !!row.matches_locked;
    const posterUnlockBtn = locked
      ? `<button type="button" class="fav-card-lock" title="Unlock" data-act="matches-lock" data-id="${id}" data-locked="1"><i class="fa-solid fa-lock"></i></button>`
      : `<button type="button" class="fav-card-lock" title="Lock matches" data-act="matches-lock" data-id="${id}" data-locked="0"><i class="fa-solid fa-lock-open"></i></button>`;
    const isFav = Number(row.is_favourite) === 1;
    const posterSolidHeartBtn = isFav
      ? `<button type="button" class="fav-card-heart heart-on" title="Unfavourite" data-act="star" data-id="${id}" data-on="0"><span class="fav-lips"></span></button>`
      : `<button type="button" class="fav-card-heart" title="Add to favourites" data-act="star" data-id="${id}" data-on="1"><span class="fav-lips"></span></button>`;
    const imgUrl = row.image_url;
    const studioLike = kind === 'studio' || kind === 'vice';
    const posterFallback = studioLike ? STUDIO_POSTER_FALLBACK : POSTER_FALLBACK;
    const posterSrc = imgUrl ? esc(imgUrl) : posterFallback;
    const imgErr = imgUrl ? `this.onerror=null;this.src='${posterFallback}'` : '';
    const errAttr = imgErr ? ` onerror="${imgErr}"` : '';
    const pathMissing = row.path_missing === true || row.path_missing === 1;
    const pathMissingWarn = pathMissing
      ? '<span class="fav-path-missing-icon" title="Missing on disk" aria-label="Folder path missing on disk"><i class="fa-solid fa-circle-exclamation"></i></span>'
      : '';

    // Hover icon stack: performer / studio / vice tiles render no
    // chrome — bio, posters, Prowlarr search, scene links are all
    // reachable from inside their respective panels. Movies keep the
    // local-Stash shortcut on hover (still the fastest path for a
    // file-tied movie row).
    const hasBoxId = !!(row.match_tpdb_id || row.match_stashdb_id || row.match_fansdb_id || row.match_javstash_id);
    let cardIcons = '';
    if (kind === 'movie' || kind === 'jav') {
      const stashBtn = !_favStashLocalOn
        ? `<button type="button" class="fav-card-icon-btn" disabled title="Stash (not configured)"><i class="fa-solid fa-circle-play"></i></button>`
        : `<button type="button" class="fav-card-icon-btn" data-act="local-stash-fav" data-id="${id}" ${hasBoxId ? '' : 'disabled'} title="Open in Stash"><i class="fa-solid fa-circle-play"></i></button>`;
      cardIcons = `<div class="fav-card-icons">${stashBtn}</div>`;
    }

    const flagOverlay = (kind === 'performer' && row.country && window.countryFlagHtml)
      ? `<span class="fav-perf-flag" aria-hidden="false">${window.countryFlagHtml(row.country)}</span>`
      : '';
    const genderOverlay = (kind === 'performer' && row.gender && typeof genderBadge === 'function')
      ? `<span class="fav-perf-gender" aria-hidden="true">${genderBadge(row.gender)}</span>`
      : '';
    //: Compute years from sort_birth_date (YYYY-MM-DD); omit silently
    //: when missing or unparseable. Done client-side so the cached
    //: /api/favourites snapshot doesn't drift the moment a birthday
    //: passes — the next page load just reads "today" again.
    const ageOverlay = (() => {
      if (kind !== 'performer') return '';
      const bd = (row.sort_birth_date || '').trim();
      const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(bd);
      if (!m) return '';
      const y = +m[1], mo = +m[2], d = +m[3];
      const now = new Date();
      let age = now.getFullYear() - y;
      const past = (now.getMonth() + 1 > mo) || (now.getMonth() + 1 === mo && now.getDate() >= d);
      if (!past) age -= 1;
      if (age < 0 || age > 130) return '';
      return `<span class="fav-perf-age" aria-label="Age ${age}">${age}</span>`;
    })();
    const phInner = (() => {
      // JAV duo cover — mirrors /scenes JAVStash feed cards. When the
      // payload includes a separate back-cover URL we render back-left
      // / front-right halves instead of the single solo poster. Front
      // still falls through to `tsApplyMovieCoverSplit('solo')` when
      // no back is available, so wide cover-art spreads still crop to
      // the front side as before.
      const backUrl = (kind === 'jav' && row.background_url && String(row.background_url).trim()) ? String(row.background_url).trim() : '';
      if (backUrl) {
        return `<div class="duo-inner duo-inner--jav-duo">
          <div class="jav-duo-slot jav-duo-slot--back"><img class="duo-img" src="${esc(backUrl)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.style.opacity=0"></div>
          <div class="jav-duo-slot jav-duo-slot--front"><img class="duo-img" src="${posterSrc}" alt="" loading="lazy" referrerpolicy="no-referrer"${errAttr}></div>
          <div class="duo-tint" aria-hidden="true"></div>
        </div>${pathMissingWarn}${posterUnlockBtn}${posterSolidHeartBtn}${cardIcons}${flagOverlay}${ageOverlay}${genderOverlay}`;
      }
      const movieSplitOnload = (kind === 'movie' || kind === 'jav')
        ? ' onload="typeof tsApplyMovieCoverSplit===\'function\'&&tsApplyMovieCoverSplit(this,\'solo\')"'
        : '';
      return `<div class="duo-inner"><img class="duo-img" src="${posterSrc}" alt="" loading="lazy" referrerpolicy="no-referrer"${errAttr}${movieSplitOnload}><div class="duo-tint" aria-hidden="true"></div></div>${pathMissingWarn}${posterUnlockBtn}${posterSolidHeartBtn}${cardIcons}${flagOverlay}${ageOverlay}${genderOverlay}`;
    })();
    // CRT/TV-frame overlay (`tv.png`) — only applied to studio + vice
    // tiles where the static-noise + green-vignette backdrop already
    // sells the cathode-ray look. Placed AFTER `phInner` in DOM order
    // so painting puts the scan-line frame on top of the existing
    // `::before` vignette, the logo, and the `::after` accent layer.
    // `pointer-events: none` on the overlay (in CSS) keeps the
    // action buttons in `phInner` clickable through it.
    const tvOverlay = studioLike
      ? `<div class="fav-studio-tv-overlay" aria-hidden="true"></div>`
      : '';
    const visual = studioLike
      ? `<div class="fav-studio-ph">${phInner}${tvOverlay}</div>`
      : `<div class="fav-perf-ph">${phInner}</div>`;

    const folderName = displayFolderName(row.folder_name);
    return `<div class="fav-cell" data-id="${id}" tabindex="0">
      <div class="fav-cell-visual">${visual}
        <div class="fav-cell-title">${esc(folderName)}</div>
      </div>
    </div>`;
  }


  function mountEntityPanelMenu(row) {
    const mount = document.getElementById('entityPanelActionsMount');
    if (!mount || !window.LibEntityActions) return;
    mount.innerHTML = '';
    const ctx = LibEntityActions.ctxFromRow(row);
    if (!ctx) return;
    LibEntityActions.mountMenuButton(mount, ctx, () => {
      closeEntityPanel();
      if (typeof closePerformerPopup === 'function') closePerformerPopup();
      load();
    });
  }

  function closeEntityPanel() {
    const modal = document.getElementById('entityPanelModal');
    modal.classList.remove('open');
    modal.querySelector('.modal-box').classList.remove('entity-panel-box--movie', 'entity-panel-box--vice');
    document.getElementById('entityPanelBody').className = 'entity-panel-body';
    const mount = document.getElementById('entityPanelActionsMount');
    if (mount) mount.innerHTML = '';
    _entityPanelRowId = null;
  }


  /* ── Ext Link Modal (IAFD / Freeones / Babepedia) ───── */
  let _extLinkRowId = null;
  let _extLinkSite = null;
  const _EXT_SEARCH_SITES = new Set(['iafd', 'freeones', 'tmdb', 'javdatabase']);
  function _extSiteApiKey(site) {
    return String(site || '').toLowerCase().replace(/\s+/g, '');
  }

  function openExtLinkModal(rowId, site, row) {
    _extLinkRowId = rowId;
    _extLinkSite = site;
    window.openExtLinkModal = openExtLinkModal;
    document.getElementById('extLinkTitle').textContent = `Link ${site} profile`;
    const name = typeof row === 'string' ? row : (row?.folder_name || '');
    document.getElementById('extLinkSearchQ').value = name;
    document.getElementById('extLinkInput').value = '';
    document.getElementById('extLinkResults').style.display = 'none';
    document.getElementById('extLinkResults').innerHTML = '';
    document.getElementById('extLinkModal').classList.add('open');
    setTimeout(() => {
      document.getElementById('extLinkSearchQ').focus();
      if (name) doExtLinkSearch();
    }, 60);
  }

  // Helper for the bio popup edit buttons (row_id + site + name string)
  function _openExtLinkForBio(rowId, site, name) {
    openExtLinkModal(rowId, site, name);
  }

  function closeExtLinkModal() {
    document.getElementById('extLinkModal').classList.remove('open');
    _extLinkRowId = null; _extLinkSite = null;
  }

  async function doExtLinkSearch() {
    if (!_extLinkRowId || !_extLinkSite) return;
    const btn = document.getElementById('extLinkSearchBtn');
    const resultsEl = document.getElementById('extLinkResults');
    btn.disabled = true;
    btn.textContent = '…';
    resultsEl.innerHTML = '';
    resultsEl.style.display = 'flex';
    try {
      const r = await fetch('/api/performers/search-ext-link', {
        method: 'POST', credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          row_id: _extLinkRowId,
          site: _extSiteApiKey(_extLinkSite),
          q: (document.getElementById('extLinkSearchQ').value || '').trim(),
        }),
      });
      const d = await r.json().catch(() => ({ results: [] }));
      const results = d.results || [];
      if (!results.length) {
        resultsEl.innerHTML = '<div style="font-size:11px;color:var(--dim);padding:6px 4px">No results found.</div>';
        return;
      }
      resultsEl.innerHTML = results.map((res, i) =>
        `<div class="ext-link-result-item" data-url="${escAttr(res.url)}" onclick="_pickExtLinkResult(this,'${escAttr(res.url)}')">
          ${res.thumb_url ? `<img class="ext-link-result-thumb" src="${escAttr(res.thumb_url)}" referrerpolicy="no-referrer" onerror="this.style.display='none'" alt="" loading="lazy">` : `<div class="ext-link-result-thumb" style="font-size:14px;display:flex;align-items:center;justify-content:center;color:rgba(255,255,255,0.2)"><i class="fa-solid fa-user"></i></div>`}
          <div style="flex:1;min-width:0">
            <div class="ext-link-result-name">${esc(res.name)}</div>
            <div class="ext-link-result-url">${esc(res.url)}</div>
          </div>
          <i class="fa-solid fa-check" style="font-size:10px;color:rgba(var(--brand-purple-rgb),0.65);display:none" class="pick-check"></i>
        </div>`
      ).join('');
    } catch (e) {
      resultsEl.innerHTML = `<div style="font-size:11px;color:var(--red);padding:6px 4px">${esc(e.message)}</div>`;
    } finally {
      btn.disabled = false;
      btn.textContent = 'Search';
    }
  }

  function _pickExtLinkResult(el, url) {
    document.querySelectorAll('.ext-link-result-item').forEach(i => i.classList.remove('selected'));
    el.classList.add('selected');
    document.getElementById('extLinkInput').value = url;
  }

  async function saveExtLink() {
    const url = (document.getElementById('extLinkInput').value || '').trim();
    if (!url || !_extLinkRowId || !_extLinkSite) return;
    const btn = document.getElementById('extLinkSaveBtn');
    btn.disabled = true;
    try {
      const r = await fetch('/api/favourites/ext-link', {
        method: 'POST', credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ row_id: _extLinkRowId, site: _extSiteApiKey(_extLinkSite), url }),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) { window.toast(d.error || 'Failed'); return; }
      const savedRow = _extLinkRowId;
      closeExtLinkModal();
      showMsg('Link saved');
      if (window._performerPopupActiveId === savedRow && typeof window.refreshPerformerPopup === 'function') {
        window.refreshPerformerPopup();
      } else {
        const ok = await _refreshOneRow(savedRow);
        if (!ok) load();
        if (_entityPanelRowId === savedRow) openEntityDetail(savedRow);
      }
    } finally {
      btn.disabled = false;
    }
  }

  document.getElementById('extLinkInput')?.addEventListener('keydown', e => {
    if (e.key === 'Enter') saveExtLink();
    if (e.key === 'Escape') closeExtLinkModal();
  });
  document.getElementById('extLinkSearchQ')?.addEventListener('keydown', e => {
    if (e.key === 'Enter') doExtLinkSearch();
    if (e.key === 'Escape') closeExtLinkModal();
  });
  /* ── End Ext Link Modal ─────────────────────────────── */

  /* ── Remove Ext Link ────────────────────────────────── */
  async function _removeExtLink(rowId, site) {
    const ok = await (window.tsConfirm
      ? window.tsConfirm(`Remove the ${site} link? This cannot be undone.`,
                         { title: 'Remove link', confirm: 'Remove', destructive: true })
      : Promise.resolve(window.confirm(`Remove the ${site} link?`)));
    if (!ok) return;
    try {
      const r = await fetch('/api/favourites/clear-ext-link', {
        method: 'POST', credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ row_id: rowId, site }),
      });
      if (!r.ok) { const d = await r.json().catch(() => ({})); window.toast(d.error || 'Failed'); return; }
      showMsg(`${site} link removed`);
      // Refresh whichever popup the click came from. Movies use the
      // entity-panel modal; performers use the bio popup.
      const entityOpen = document.getElementById('entityPanelModal').classList.contains('open')
        && _entityPanelRowId === rowId;
      if (window._performerPopupActiveId === rowId && typeof window.refreshPerformerPopup === 'function') window.refreshPerformerPopup();
      else if (entityOpen) openEntityDetail(rowId);
      else { const ok = await _refreshOneRow(rowId); if (!ok) load(); }
    } catch (e) {
      window.toast(e.message);
    }
  }

  /* ── Image Upload Modal ─────────────────────────────── */
  let _imgUploadRowId = null;

  function _openImageModal() {
    const rid = window._performerPopupActiveId;
    if (!rid) return;
    _imgUploadRowId = rid;
    document.getElementById('imgUploadUrl').value = '';
    document.getElementById('imgUploadFile').value = '';
    document.getElementById('imgUploadModal').classList.add('open');
  }

  function closeImgUploadModal() {
    document.getElementById('imgUploadModal').classList.remove('open');
    _imgUploadRowId = null;
  }

  async function saveImgFromUrl() {
    const url = (document.getElementById('imgUploadUrl').value || '').trim();
    if (!url || !_imgUploadRowId) return;
    const btn = document.getElementById('imgUrlSaveBtn');
    btn.disabled = true;
    btn.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Saving"></span> Saving…';
    try {
      const r = await fetch('/api/performers/set-headshot-url', {
        method: 'POST', credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ row_id: _imgUploadRowId, image_url: url }),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) { window.toast(d.error || 'Failed'); return; }
      const savedRowId = _imgUploadRowId;
      closeImgUploadModal();
      showMsg('Image saved');
      _refreshLibraryImagesForRow(savedRowId);
      if (window._performerPopupActiveId === savedRowId && typeof window.refreshPerformerPopup === 'function') window.refreshPerformerPopup();
    } catch (e) {
      window.toast(e.message);
    } finally {
      btn.disabled = false;
      btn.textContent = 'Save URL';
    }
  }

  async function uploadImgFile() {
    const fileInput = document.getElementById('imgUploadFile');
    if (!fileInput.files?.length || !_imgUploadRowId) return;
    const btn = document.getElementById('imgFileSaveBtn');
    btn.disabled = true;
    try {
      const form = new FormData();
      form.append('row_id', String(_imgUploadRowId));
      form.append('file', fileInput.files[0]);
      const r = await fetch('/api/performers/upload-image', {
        method: 'POST', credentials: 'same-origin',
        body: form,
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) { window.toast(d.error || 'Failed'); return; }
      const savedRowId = _imgUploadRowId;
      closeImgUploadModal();
      showMsg('Image uploaded');
      _refreshLibraryImagesForRow(savedRowId);
      if (window._performerPopupActiveId === savedRowId && typeof window.refreshPerformerPopup === 'function') window.refreshPerformerPopup();
    } catch (e) {
      window.toast(e.message);
    } finally {
      btn.disabled = false;
    }
  }

  document.getElementById('imgUploadUrl')?.addEventListener('keydown', e => {
    if (e.key === 'Enter') saveImgFromUrl();
    if (e.key === 'Escape') closeImgUploadModal();
  });
  /* Capture-phase Escape handler for every modal-over-modal on /library.
   * Without this, pressing Escape with an inner modal open lets the event
   * bubble up to performer-popup.js's document-level Escape handler,
   * which closes the parent performer popup but leaves the inner modal
   * `.open`. With z=1600 (modal-over-modal tier) it can sit invisibly
   * above the page and silently block every click on the favourites grid.
   * Closes one layer per Escape press, topmost first. */
  const _LIB_INNER_MODALS = [
    ['imgUploadModal',  () => closeImgUploadModal()],
    ['posterRoleModal', () => closePosterRolePicker()],
    ['imagePickModal',  () => closeImagePickModal()],
    ['searchModal',     () => closeSearch()],
    ['extLinkModal',    () => closeExtLinkModal()],
    ['cropModal',       () => closeCropModal()],
  ];
  document.addEventListener('keydown', (e) => {
    if (e.key !== 'Escape') return;
    for (const [id, closer] of _LIB_INNER_MODALS) {
      const m = document.getElementById(id);
      if (m && m.classList.contains('open')) {
        e.stopPropagation();
        try { closer(); } catch (_) {}
        return;
      }
    }
  }, true);  // capture phase — fires before performer-popup.js's document handler
  /* Backdrop-click dismiss for modals that only had an X button.
   * Click on the dimmed area outside the modal-box should close. */
  document.getElementById('posterRoleModal')?.addEventListener('click', (e) => {
    if (e.target === e.currentTarget) closePosterRolePicker();
  });
  document.getElementById('imagePickModal')?.addEventListener('click', (e) => {
    if (e.target === e.currentTarget) closeImagePickModal();
  });
  document.getElementById('searchModal')?.addEventListener('click', (e) => {
    if (e.target === e.currentTarget) closeSearch();
  });
  /* When the universal performer popup closes (Escape, X button, or
   * backdrop click — all paths fire the `performer-popup-close` event),
   * also run /library's full close functions for any sub-modal that
   * was open. Just stripping `.open` (which the popup itself does
   * defensively) hides the modal but leaves local state — _searchRowId,
   * _imagePickRowId, _posterRoleRowId, etc. — pointing at the closed
   * popup, which then breaks the next interaction. */
  document.addEventListener('performer-popup-close', () => {
    const ifOpen = (id, fn) => {
      const el = document.getElementById(id);
      if (el && el.classList.contains('open')) {
        try { fn(); } catch (_) {}
      }
    };
    if (typeof closeImgUploadModal === 'function') ifOpen('imgUploadModal', closeImgUploadModal);
    if (typeof closePosterRolePicker === 'function') ifOpen('posterRoleModal', closePosterRolePicker);
    if (typeof closeImagePickModal === 'function') ifOpen('imagePickModal', closeImagePickModal);
    if (typeof closeSearch === 'function') ifOpen('searchModal', closeSearch);
    if (typeof closeExtLinkModal === 'function') ifOpen('extLinkModal', closeExtLinkModal);
    if (typeof closeCropModal === 'function') ifOpen('cropModal', closeCropModal);
  });
  /* ── End Image Upload Modal ─────────────────────────── */

  async function openEntityDetail(rowId) {
    _entityPanelRowId = rowId;
    const modal = document.getElementById('entityPanelModal');
    const body = document.getElementById('entityPanelBody');
    // Reset any movie/studio-specific overrides from a previous open
    modal.querySelector('.modal-box').classList.remove('entity-panel-box--movie', 'entity-panel-box--vice');
    body.className = 'entity-panel-body';
    modal.classList.add('open');
    body.innerHTML = '<div class="entity-panel-loading">Loading…</div>';
    try {
      const r = await fetch('/api/favourites/entity-panel?row_id=' + encodeURIComponent(rowId), { credentials: 'same-origin' });
      const d = await r.json();
      if (!r.ok) throw new Error(d.error || 'Failed to load');
      if (d && d.row && d.row.kind === 'studio') {
        closeEntityPanel();
        window.openStudioPopup({
          libraryRowId: rowId,
          name: d.row.folder_name || d.row.display_name || '',
        });
        return;
      }
      renderEntityPanelContent(d);
    } catch (e) {
      body.innerHTML = `<div class="entity-panel-loading" style="color:var(--red)">${esc(e.message || 'Error')}</div>`;
    }
  }

  // Vices have no backend panel endpoint — the data (name + tag list)
  // is already on the client in `_rowsVice`. This renders a panel
  // styled like the studio one (logo hero, two-column body) with the
  // tag list on the right and a Search Prowlarr CTA on the left.
  function openViceDetail(rowId) {
    const row = _rowsVice.find(r => r.id === rowId);
    if (!row) return;
    _entityPanelRowId = rowId;
    const modal = document.getElementById('entityPanelModal');
    const body = document.getElementById('entityPanelBody');
    modal.querySelector('.modal-box').classList.remove('entity-panel-box--movie');
    modal.querySelector('.modal-box').classList.add('entity-panel-box--vice');
    modal.classList.add('open');

    const name = displayFolderName(row.folder_name) || (row.folder_name || 'Vice');
    document.getElementById('entityPanelTitle').textContent = name;
    const tagsArr = Array.isArray(row.tags) ? row.tags : [];
    document.getElementById('entityPanelMeta').textContent =
      (tagsArr.length) + ' tag' + (tagsArr.length === 1 ? '' : 's') + ' · Vice';
    mountEntityPanelMenu(row);

    const imgUrl = row.image_url || STUDIO_POSTER_FALLBACK;
    const tags = Array.isArray(row.tags) ? row.tags : [];
    const tagCount = tags.length;
    // JSON.stringify produces a JS string literal with raw " chars. Inline
    // onclick="..." attributes terminate at the first ", so we have to
    // HTML-escape the quotes (escAttr handles & " < ). The browser
    // decodes the entities when parsing the attribute, so the JS engine
    // sees the original JSON literal.
    const nameJson = escAttr(JSON.stringify(name));
    const posterJson = escAttr(JSON.stringify(imgUrl));

    const tagListHtml = tagCount
      ? `<div class="vice-panel-tag-grid">${tags.map(t => {
          const tn = (t && t.name) || '';
          const tid = String((t && t.id) || '').trim();
          const tpdbId = tid && /^\d+$/.test(tid) ? tid : '';
          return `<div class="vice-panel-tag-row">
            <i class="fa-solid fa-fire"></i>
            <span class="vice-panel-tag-name">${esc(tn)}</span>
            ${tpdbId ? `<a class="vice-panel-tag-id" href="https://theporndb.net/tags/${encodeURIComponent(tpdbId)}" target="_blank" rel="noopener noreferrer" title="Open on TPDB">TPDB ${esc(tpdbId)}</a>` : ''}
          </div>`;
        }).join('')}</div>`
      : `<div class="entity-panel-meta" style="margin:0">No tags assigned. Add some in Settings → Pipeline → Vices.</div>`;

    body.className = 'vice-panel-body';
    body.innerHTML = `
      <div class="vice-panel-logo-wrap">
        <img src="${esc(imgUrl)}" alt="" loading="eager" referrerpolicy="no-referrer" onerror="this.onerror=null;this.src='${STUDIO_POSTER_FALLBACK}'">
      </div>
      <div class="vice-panel-2col">
        <div class="vice-panel-left">
          <div>
            <div class="bio-section-label">About</div>
            <div style="font-size:11px;color:rgba(212,207,196,0.85);line-height:1.7">
              <strong>${esc(name)}</strong> bundles
              <strong>${tagCount}</strong> tag${tagCount === 1 ? '' : 's'}.
              Edit the assignment in Settings → Pipeline → Vices.
            </div>
          </div>
          <div class="bio-links-block" style="margin-top:auto">
            <button type="button" class="pp-bio-search-btn vice-panel-prowlarr-btn" onclick="window.openProwlarrSearchPopup({title:${nameJson},kind:'scene',thumb_url:${posterJson}})" title="Search Prowlarr for ${esc(name)}" aria-label="Search Prowlarr">
              <img src="/static/logos/prowlarr.webp" alt="" onerror="this.replaceWith(Object.assign(document.createElement('i'),{className:'fa-solid fa-magnifying-glass'}))">
              <i class="fa-solid fa-magnifying-glass"></i>
            </button>
          </div>
        </div>
        <div class="vice-panel-right">
          <div class="bio-section-label" style="margin-bottom:10px">Tags in this vice</div>
          ${tagListHtml}
        </div>
      </div>`;
  }

  function renderEntityPanelContent(d) {
    const row = d.row;
    const scenes = d.tpdb_scenes || [];
    const scenesSource = d.scenes_source || null;
    const kind = row.kind;
    if (kind === 'movie' || kind === 'jav') {
      const title = displayFolderName(row.folder_name);
      const panelPosterFallback = POSTER_FALLBACK;
      const imgUrl = row.image_url || panelPosterFallback;
      const panelPathMissing = row.path_missing === true || row.path_missing === 1;
      const kindLabel = kind === 'jav' ? 'JAV' : 'Movie';
      document.getElementById('entityPanelTitle').textContent = title;
      document.getElementById('entityPanelMeta').textContent =
        (row.root_label || '—') + ' · ' + kindLabel +
        (panelPathMissing ? ' · Path missing on disk' : '') +
        (!(row.match_tmdb_id || row.match_tpdb_id || row.match_javstash_id) ? ' · No match yet — use Refresh' : '');
      document.getElementById('entityPanelRefresh').onclick = () => refreshOneFromPanel(row.id);
      mountEntityPanelMenu(row);

      // Switch box to wider movie variant
      document.getElementById('entityPanelModal').querySelector('.modal-box').classList.add('entity-panel-box--movie');

      const plocked = !!row.matches_locked;
      const panelFav = Number(row.is_favourite) === 1;
      const lockIcon = plocked ? 'lock' : 'lock-open';
      const lockTitle = plocked ? 'Matches locked — click to unlock' : 'Lock matches';
      // Movie popup shows lock + favourite as inline icons next to the
      // title (not overlaid on the poster). Render them here so they sit
      // immediately after the H3 in the header.
      const titleEl = document.getElementById('entityPanelTitle');
      if (titleEl) {
        titleEl.innerHTML = `${esc(title)}
          <button type="button" class="entity-title-lock${plocked ? ' lock-on' : ''}"
                  title="${esc(lockTitle)}"
                  onclick="event.stopPropagation(); toggleMatchesLock(${row.id}, ${!plocked});">
            <i class="fa-solid fa-${lockIcon}"></i>
          </button>
          <button type="button" class="entity-title-heart${panelFav ? ' heart-on' : ''}"
                  id="entityPanelStar" title="Favourite">
            <i class="fa-solid fa-heart"></i>
          </button>`;
      }
      const tpdbId = (row.match_tpdb_id || '').toString().trim();
      const tmdbId = (row.match_tmdb_id || '').toString().trim();
      const javId  = (row.match_javstash_id || '').toString().trim();
      const tpdbUrl = tpdbId ? `https://theporndb.net/movies/${encodeURIComponent(tpdbId)}` : null;
      const tmdbUrl = tmdbId ? `https://www.themoviedb.org/movie/${encodeURIComponent(tmdbId)}` : null;
      const javUrl  = javId  ? `https://javstash.org/scenes/${encodeURIComponent(javId)}` : null;
      const prowlarrSearchUrl = `https://theporndb.net/movies?search=${encodeURIComponent(title)}`;

      // Profile pills mirror the performer popup convention:
      //   linked   → green check + edit/remove hover actions
      //   missing  → magnifier icon, click opens the same search modal
      // openSearchForRow(rowId, 'TPDB' | 'TMDB' | 'JAVSTASH') routes to the
      // movie search dialog (which fetches TPDB + TMDB + JAVStash hits).
      const movieSources = [
        { key: 'tpdb',     label: 'TPDB',     site: 'TPDB',     url: tpdbUrl },
        { key: 'tmdb',     label: 'TMDB',     site: 'TMDB',     url: tmdbUrl },
        { key: 'javstash', label: 'JAVStash', site: 'JAVSTASH', url: javUrl  },
      ];
      const moviePillsHtml = movieSources.map(s => {
        const logo = srcLogo(s.key, s.label, 'src-logo--sm') || '';
        const labelEl = `<span class="pp-profile-name">${esc(s.label)}</span>`;
        if (s.url) {
          // Linked: logo + name on the left, green tick at the right.
          // Hover swaps the tick for the edit / remove action stack
          // (CSS-driven via `.pp-profile-actions`).
          return `<a href="${escAttr(s.url)}" target="_blank" rel="noopener noreferrer" class="pp-profile-pill is-linked" title="Open on ${esc(s.label)}">
            ${logo}
            ${labelEl}
            <i class="fa-solid fa-check pp-profile-check"></i>
            <span class="pp-profile-actions">
              <button type="button" class="pp-profile-action-btn" onclick="event.preventDefault();event.stopPropagation();openSearchForRow(${row.id}, '${s.site}')" title="Change link"><i class="fa-solid fa-pen"></i></button>
              <button type="button" class="pp-profile-action-btn is-remove" onclick="event.preventDefault();event.stopPropagation();_removeExtLink(${row.id}, '${s.site}')" title="Remove link"><i class="fa-solid fa-xmark"></i></button>
            </span>
          </a>`;
        }
        // Missing: dim dashed pill with a magnifier on the right hinting
        // "click to search this DB and link a match".
        return `<button type="button" class="pp-profile-pill is-missing" onclick="openSearchForRow(${row.id}, '${s.site}')" title="Search ${esc(s.label)} and link a match">
          ${logo}
          ${labelEl}
          <i class="fa-solid fa-magnifying-glass pp-profile-search-ico"></i>
        </button>`;
      }).join('');

      const panelBody = document.getElementById('entityPanelBody');
      panelBody.className = 'movie-panel-body';
      panelBody.innerHTML = `
        <div class="movie-panel-bg" id="moviePanelBg" style="background-image:url('${esc(imgUrl)}')"></div>
        <div class="movie-panel-bg-overlay"></div>
        <div class="movie-panel-layout">
          <div class="movie-panel-stats-col">
            <div class="bio-stats-list" id="moviePanelStats">
              <div class="bio-stat-row"><div class="bio-stat-value" style="color:var(--dim);font-style:italic">Loading details…</div></div>
            </div>
            <div class="bio-links-block" style="margin-top:auto">
              <div class="bio-section-label">Profiles</div>
              <div class="pp-profiles-grid">
                ${moviePillsHtml}
              </div>
            </div>
          </div>
          <div class="movie-panel-info-col">
            <div class="movie-panel-section">Synopsis</div>
            <div class="movie-panel-synopsis" id="moviePanelSynopsis" style="font-style:italic">—</div>
            <div class="movie-panel-genres" id="moviePanelGenres" style="display:none"></div>

            <div class="movie-panel-section">Cast in your library</div>
            <div class="movie-panel-cast" id="moviePanelCast">
              <span class="movie-panel-cast-loading">Fetching…</span>
            </div>
          </div>
          <div class="movie-panel-poster-col" style="--vhs-hue:${Math.floor(Math.random()*360)}deg">
            <div class="movie-panel-vhs-bg" aria-hidden="true"></div>
            <div class="movie-panel-vhs-title-rotated" aria-hidden="true">${esc(row.match_tmdb_name || row.match_tpdb_name || title)}</div>
            <img class="movie-panel-studio-logo-rotated" id="moviePanelStudioLogo" src="" alt="" style="display:none" onerror="this.style.display='none'" loading="lazy">
            <div class="movie-panel-poster-card">
              ${(kind === 'jav' && row.background_url) ? `
              <div class="img-load img-load--jav-duo ready" style="display:flex;flex-direction:row;align-items:stretch;aspect-ratio:27/40">
                <div class="jav-duo-slot jav-duo-slot--back" style="flex:1 1 50%;min-width:0;position:relative;overflow:hidden;border-radius:8px 0 0 8px">
                  <img class="movie-poster" src="${esc(row.background_url)}" alt="" loading="eager" referrerpolicy="no-referrer" style="position:absolute;inset:0;width:100%;height:100%;aspect-ratio:unset;border-radius:inherit;object-fit:cover" onerror="this.style.opacity=0">
                </div>
                <div class="jav-duo-slot jav-duo-slot--front" style="flex:1 1 50%;min-width:0;position:relative;overflow:hidden;border-radius:0 8px 8px 0">
                  <img class="movie-poster" src="${esc(imgUrl)}" alt="" loading="eager" referrerpolicy="no-referrer" style="position:absolute;inset:0;width:100%;height:100%;aspect-ratio:unset;border-radius:inherit;object-fit:cover" onerror="this.onerror=null;this.src='${panelPosterFallback}'">
                </div>
              </div>` : `
              <div class="img-load ready"><img class="movie-poster" src="${esc(imgUrl)}" alt="" loading="eager" referrerpolicy="no-referrer" onload="typeof tsApplyMovieCoverSplit==='function'&&tsApplyMovieCoverSplit(this,'solo')" onerror="this.onerror=null;this.src='${panelPosterFallback}'"></div>`}
            </div>
          </div>
        </div>`;

      document.getElementById('entityPanelStar').onclick = () => toggleStar(row.id, !panelFav);

      // Helpers for the new three-column layout: stats list (left col),
      // synopsis / cast / genres / match (centre col).
      const renderMovieStats = (pairs) => {
        const el = document.getElementById('moviePanelStats');
        if (!el) return;
        const rows = pairs.filter(p => p && p.value !== '' && p.value != null);
        if (!rows.length) {
          el.innerHTML = `<div class="bio-stat-row"><div class="bio-stat-value" style="color:var(--dim);font-style:italic">No details available</div></div>`;
          return;
        }
        el.innerHTML = rows.map(p => `
          <div class="bio-stat-row">
            <div class="bio-stat-label">${esc(p.label)}</div>
            <div class="bio-stat-value${p.wrap ? ' wrap' : ''}">${esc(p.value)}</div>
          </div>`).join('');
      };
      const renderMovieGenres = (genres) => {
        const el = document.getElementById('moviePanelGenres');
        if (!el || !genres || !genres.length) return;
        el.style.display = '';
        el.innerHTML = genres.map(g => `<span class="movie-panel-genre">${esc(g)}</span>`).join('');
      };
      const renderMovieCastNames = (names) => {
        const el = document.getElementById('moviePanelCast');
        if (!el) return;
        el.innerHTML = names.map(n => `<span class="movie-panel-genre" style="color:var(--text);background:rgba(255,255,255,0.05);border-color:rgba(255,255,255,0.1);font-family:var(--mono);font-size:10px">${esc(n)}</span>`).join('');
      };
      const showMovieCastEmpty = (msg) => {
        const el = document.getElementById('moviePanelCast');
        if (el) el.innerHTML = `<span style="color:var(--dim);font-size:11px;font-style:italic">${esc(msg)}</span>`;
      };

      // Single unified fetch — backend caches the merged TPDB/TMDB/NFO
      // payload to metadata/movies/{row_id}/{row_id}.json so re-opening
      // the popup is instant. The refresh button passes refresh=1 via
      // _movieInfoForceRefresh to bust the cache.
      const force = _movieInfoForceRefresh ? '&refresh=1' : '';
      _movieInfoForceRefresh = false;
      fetch(`/api/movies/info?row_id=${row.id}${force}`, { credentials: 'same-origin' })
        .then(r => r.json())
        .then(info => {
          if (info.error && info.source !== 'tpdb' && info.source !== 'tmdb' && info.source !== 'nfo') {
            renderMovieStats([]);
            showMovieCastEmpty('No match yet — use Refresh');
            return;
          }
          renderMovieStats([
            { label: 'Studio',   value: info.studio || '' },
            { label: 'Year',     value: info.year || '' },
            { label: 'Runtime',  value: info.runtime ? `${info.runtime} min` : '' },
            { label: 'Director', value: (info.directors || []).join(', '), wrap: true },
          ]);
          // Rotated studio logo behind the poster card — populated once
          // we know the studio name from the info fetch.
          const studioLogoImg = document.getElementById('moviePanelStudioLogo');
          if (studioLogoImg && (info.studio || title)) {
            studioLogoImg.src = `/api/studio-logo?name=${encodeURIComponent(info.studio || '')}&q=${encodeURIComponent(title || '')}`;
            studioLogoImg.style.display = '';
          }
          const synEl = document.getElementById('moviePanelSynopsis');
          if (synEl) {
            synEl.textContent = info.synopsis || '—';
            if (info.synopsis) synEl.style.fontStyle = '';
          }
          renderMovieGenres(info.genres || []);
          const castEl = document.getElementById('moviePanelCast');
          if (castEl) {
            // Merge library_performers (with headshot + row_id) and the
            // full performer list. Library entries win when names match
            // (so we get headshots + clickable row id), but performers
            // not in the library still surface as tiles — clicking them
            // opens the universal popup which resolves by name.
            const libPerfs = info.library_performers || [];
            const allPerfs = info.performers || [];
            const libByName = new Map(libPerfs.map(p => [(p.name || '').toLowerCase(), p]));
            // Order: full cast first (from TPDB/TMDB), library tiles
            // automatically merge on name match.
            let merged = [];
            if (allPerfs.length) {
              merged = allPerfs.map(p => {
                const nm = typeof p === 'object' ? (p.name || '') : String(p || '');
                const lib = libByName.get(nm.toLowerCase());
                return {
                  name: nm,
                  gender: (lib && lib.gender) || (typeof p === 'object' ? p.gender : ''),
                  headshot_url: lib && lib.headshot_url,
                  row_id: lib && lib.id,
                  stash_id: typeof p === 'object' ? (p.id || p.stash_id) : null,
                };
              });
            } else {
              merged = libPerfs.map(p => ({
                name: p.name,
                gender: p.gender,
                headshot_url: p.headshot_url,
                row_id: p.id,
              }));
            }
            if (merged.length) {
              castEl.innerHTML = merged.map(p => {
                const img = p.headshot_url
                  ? `<img src="${esc(p.headshot_url)}" alt="" loading="lazy" onerror="this.outerHTML='<div class=\\'mp-perf-ph\\'><i class=\\'fa-solid fa-user\\'></i></div>'">`
                  : `<div class="mp-perf-ph"><i class="fa-solid fa-user"></i></div>`;
                const attrs = (typeof window.performerLinkAttrs === 'function')
                  ? window.performerLinkAttrs(p.name, { gender: p.gender, libraryRowId: p.row_id, stashId: p.stash_id })
                  : `data-performer-link data-name="${esc(p.name)}"`;
                return `<div class="movie-panel-perf" title="${esc(p.name)}"${attrs ? ' ' + attrs : ''}>${img}<div class="movie-panel-perf-name">${esc(p.name)}</div></div>`;
              }).join('');
            } else if (info.source === 'tmdb') {
              showMovieCastEmpty('No TPDB match — cast unavailable');
            } else if (info.source === 'none') {
              showMovieCastEmpty('No match yet — use Refresh');
            } else {
              showMovieCastEmpty('No cast data');
            }
          }
        })
        .catch(() => {
          renderMovieStats([]);
          showMovieCastEmpty('Could not load details');
        });
      return;
    }
    const panelPosterFallback = kind === 'studio' ? STUDIO_POSTER_FALLBACK : POSTER_FALLBACK;
    const imgUrl = row.image_url || panelPosterFallback;
    const title = displayFolderName(row.folder_name);
    document.getElementById('entityPanelTitle').textContent = title;
    const panelPathMissing = row.path_missing === true || row.path_missing === 1;
    const anySceneLink = !!(row.match_tpdb_id || row.match_stashdb_id || row.match_fansdb_id || row.match_javstash_id);
    document.getElementById('entityPanelMeta').textContent =
      (row.root_label || '—') + ' · ' + (kind === 'studio' ? 'Studio' : 'Star') +
      (panelPathMissing ? ' · Folder path missing on disk' : '') +
      (anySceneLink ? '' : ' · No DB link for scene list — add TPDB / StashDB / FansDB / JAVStash');

    const panelFav = Number(row.is_favourite) === 1;
    document.getElementById('entityPanelRefresh').onclick = () => refreshOneFromPanel(row.id);
    mountEntityPanelMenu(row);

    const scenesHeading =
      scenesSource === 'stashdb' ? 'Recent on StashDB'
        : scenesSource === 'fansdb' ? 'Recent on FansDB'
        : 'Recent on ThePornDB';
    // Tile-based scene grid — mirrors /scenes detail panel layout
    // (`.scene-card` + `.discover-info-scenes--studio`). Click the
    // thumbnail to open the source-DB scene page; Prowlarr search
    // hover-overlay button drops the user into the Prowlarr popup.
    const scenesBlock = scenes.length
      ? `<div class="discover-info-scenes discover-info-scenes--studio">${scenes.map(s => {
          const thumb = s.thumb || POSTER_FALLBACK;
          const link = favSceneExternalUrl(s);
          const sl = favSceneSourceLabel(s);
          const studio = s.studio || '';
          const titleJson = escAttr(JSON.stringify(s.title || ''));
          const studioJson = escAttr(JSON.stringify(studio));
          const studioLogo = (studio || s.title)
            ? `<img class="scene-studio-logo" src="/api/studio-logo?name=${encodeURIComponent(studio)}&q=${encodeURIComponent(s.title || '')}" alt="" loading="lazy" onerror="this.remove()">`
            : '';
          return `<div class="scene-card discover-info-scene-card">
            <a class="scene-card-thumb-link" href="${escAttr(link)}" target="_blank" rel="noopener noreferrer" title="Open on ${esc(sl)}">
              <div class="img-load">
                <img class="scene-thumb" src="${esc(thumb)}" loading="lazy" onerror="this.onerror=null;this.src='${POSTER_FALLBACK}'">
                <div class="duo-tint" aria-hidden="true"></div>
                ${studioLogo}
              </div>
            </a>
            <div class="scene-info" style="padding:6px 8px">
              <div class="scene-title" title="${escAttr(s.title || '')}">${esc(s.title || '—')}</div>
              <div class="scene-date">${esc(s.date || '')}${studio ? ' · ' + esc(studio) : ''}</div>
            </div>
            <div class="entity-scene-prowlarr-overlay">
              <button type="button" class="fav-icon-btn entity-scene-prowlarr" onclick="window.openProwlarrSearchPopup({title:${titleJson},studio:${studioJson},kind:'scene'})" title="Search Prowlarr"><img src="/static/logos/prowlarr.webp" alt="Prowlarr" onerror="this.replaceWith(Object.assign(document.createElement('i'),{className:'fa-solid fa-magnifying-glass'}))"><i class="fa-solid fa-magnifying-glass"></i></button>
            </div>
          </div>`;
        }).join('')}</div>`
      : `<div class="entity-panel-meta" style="margin-bottom:16px">${anySceneLink ? 'No recent scenes returned from the linked database(s).' : 'Link TPDB, StashDB, FansDB, or JAVStash to see recent scenes.'}</div>`;

    // Performer/studio fallback: profile-pill grid using the same
    // performer-popup convention (linked → check + edit/remove;
    // missing → magnifier + click-to-search).
    const fallbackSources = [
      { key: 'tpdb',    label: 'TPDB',    site: 'TPDB',    url: (row.match_tpdb_id    ? (kind === 'studio' ? `https://theporndb.net/sites/${encodeURIComponent(row.match_tpdb_id)}` : `https://theporndb.net/performers/${encodeURIComponent(row.match_tpdb_id)}`) : null) },
      { key: 'stashdb', label: 'StashDB', site: 'StashDB', url: (row.match_stashdb_id ? (kind === 'studio' ? `https://stashdb.org/studios/${encodeURIComponent(row.match_stashdb_id)}` : `https://stashdb.org/performers/${encodeURIComponent(row.match_stashdb_id)}`) : null) },
      { key: 'fansdb',  label: 'FansDB',  site: 'FansDB',  url: (row.match_fansdb_id  ? (kind === 'studio' ? `https://fansdb.cc/studios/${encodeURIComponent(row.match_fansdb_id)}` : `https://fansdb.cc/performers/${encodeURIComponent(row.match_fansdb_id)}`) : null) },
    ];
    if (kind !== 'studio') {
      fallbackSources.push({
        key: 'javstash',
        label: 'JAVStash',
        site: 'JAVStash',
        url: row.match_javstash_id
          ? (kind === 'studio'
              ? `https://javstash.org/studios/${encodeURIComponent(row.match_javstash_id)}`
              : `https://javstash.org/performers/${encodeURIComponent(row.match_javstash_id)}`)
          : null,
      });
    }
    const matchBtns = `<motion class="pp-profiles-grid">${fallbackSources.map(s => {
      const logo = srcLogo(s.key, s.label, 'src-logo--sm') || esc(s.label);
      if (s.url) {
        return `<a href="${escAttr(s.url)}" target="_blank" rel="noopener noreferrer" class="pp-profile-pill is-linked" title="${esc(s.label)} — click to open">
          <i class="fa-solid fa-check pp-profile-check"></i>
          ${logo}
          <span class="pp-profile-actions">
            <button type="button" class="pp-profile-action-btn" onclick="event.preventDefault();event.stopPropagation();openSearchForRow(${row.id}, '${s.site}')" title="Change link"><i class="fa-solid fa-pen"></i></button>
            <button type="button" class="pp-profile-action-btn is-remove" onclick="event.preventDefault();event.stopPropagation();_removeExtLink(${row.id}, '${s.site}')" title="Remove link"><i class="fa-solid fa-xmark"></i></button>
          </span>
        </a>`;
      }
      return `<button type="button" class="pp-profile-pill is-missing" onclick="openSearchForRow(${row.id}, '${s.site}')" title="${esc(s.label)} — click to search and link">
        ${logo}
        <i class="fa-solid fa-magnifying-glass" style="font-size:9px;opacity:0.7"></i>
      </button>`;
    }).join('')}</div>`;

    const plocked = !!row.matches_locked;
    const panelLockTitle = plocked
      ? 'Matches locked — scans and refresh will not change links'
      : 'Lock matches — scans and refresh will not change links';
    const panelLockBtn = `<button type="button" class="entity-panel-lock${plocked ? ' lock-on' : ''}" title="${esc(panelLockTitle)}" onclick="event.stopPropagation(); toggleMatchesLock(${row.id}, ${!plocked});"><i class="fa-solid fa-${plocked ? 'lock' : 'lock-open'}"></i></button>`;
    const panelBodyEl = document.getElementById('entityPanelBody');

    panelBodyEl.innerHTML = `
      <div class="entity-panel-grid">
        <div class="entity-panel-poster">
          ${panelLockBtn}
          <button type="button" class="entity-panel-heart${panelFav ? ' heart-on' : ''}" id="entityPanelStar" title="Favourite"><span class="fav-lips"></span></button>
          <img src="${esc(imgUrl)}" alt="" loading="eager" referrerpolicy="no-referrer" onerror="this.onerror=null;this.src='${panelPosterFallback}'">
        </div>
        <div>
          <div class="entity-section-title">${esc(scenesHeading)}</div>
          <div class="entity-tpdb-scenes">${scenesBlock}</div>
          <div class="entity-section-title">Database match</div>
          ${entityPanelLocalStashBlock(row)}
          ${matchBtns}
        </div>
      </div>`;

    const starBtn = document.getElementById('entityPanelStar');
    if (starBtn) starBtn.onclick = () => toggleStar(row.id, !panelFav);
  }

  // Set true by refreshOneFromPanel to make the next /api/movies/info
  // call bypass the disk cache. Consumed exactly once by the movie popup
  // render and reset to false immediately after.
  let _movieInfoForceRefresh = false;

  async function refreshOneFromPanel(id) {
    showMsg('Refreshing…');
    const r = await fetch('/api/favourites/refresh', {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id, scrape_aliases: true }),
    });
    const d = await r.json();
    if (!r.ok) window.toast(d.error || 'Refresh failed');
    else if (d.skipped) showMsg('Matches are locked');
    else showMsg('Updated');
    const ok = await _refreshOneRow(id);
    if (!ok) await load();
    if (_entityPanelRowId === id) {
      _movieInfoForceRefresh = true;
      await openEntityDetail(id);
    }
  }

  async function runEntityPanelProwlarrSearch() {
    const q = (document.getElementById('entityProwlarrQuery') || {}).value?.trim() || '';
    const el = document.getElementById('entityProwlarrResults');
    if (!q || !el) return;
    el.innerHTML = '<div class="entity-panel-meta">Searching…</div>';
    try {
      const r = await fetch('/api/prowlarr/search?q=' + encodeURIComponent(q), { credentials: 'same-origin' });
      const d = await r.json();
      if (d.error) {
        el.innerHTML = `<div style="color:var(--red);font-size:11px">${esc(d.error)}</div>`;
        return;
      }
      _favPanelProwlarrResults = d.results || [];
      if (!_favPanelProwlarrResults.length) {
        el.innerHTML = '<div class="entity-panel-meta">No results</div>';
        return;
      }
      el.innerHTML = _favPanelProwlarrResults.map((res, i) => {
        const isTor = res.type === 'torrent';
        const dl = isTor && res.magnet ? res.magnet : (res.download_url || '');
        const meta = `${res.age != null ? Math.round(res.age / 24) + 'd' : ''}${res.seeders != null ? ' · ' + res.seeders + ' seed' : ''} · ${Math.round(res.size_mb || 0)} MB`;
        return `<div class="entity-pr-row">
          <div style="min-width:0">
            <div class="entity-pr-title" title="${esc(res.title)}">${esc(res.title)}</div>
            <div class="entity-pr-meta">${esc(meta)} · ${esc(res.indexer || '')}</div>
          </div>
          <img class="dl-type-logo" src="/static/logos/${isTor ? 'torrent' : 'nzb'}.webp" alt="${isTor ? 'Torrent' : 'NZB'}" title="${isTor ? 'Torrent' : 'NZB'}">
          <button type="button" class="btn-prowlarr-grab ${res.type === 'nzb' ? 'nzb' : ''}" title="Send to client" onclick="grabEntityPanelProwlarr(event,${i})"><i class="fa-solid fa-download" aria-hidden="true"></i></button>
        </div>`;
      }).join('');
    } catch (e) {
      el.innerHTML = `<div style="color:var(--red);font-size:11px">${esc(e.message)}</div>`;
    }
  }

  async function grabEntityPanelProwlarr(ev, idx) {
    const result = _favPanelProwlarrResults[idx];
    if (!result) return;
    const btn = ev.target && ev.target.closest ? ev.target.closest('button') : null;
    if (btn) { btn.disabled = true; btn.classList.remove('btn-prowlarr-grab--sent'); btn.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Loading"></span>'; }
    try {
      const panelRow = _entityPanelRowId
        ? [..._rowsPerf, ..._rowsStudio, ..._rowsMovie].find(r => r.id === _entityPanelRowId)
        : null;
      const grabKind = panelRow && panelRow.kind === 'movie' ? 'movie' : 'scene';
      const isTor = result.type === 'torrent';
      const downloadUrl = isTor && result.magnet ? result.magnet : (result.download_url || '');
      const r = await fetch('/api/prowlarr/grab', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          guid: result.guid,
          indexer_id: result.indexer_id,
          type: result.type,
          download_url: downloadUrl,
          kind: grabKind,
        }),
      });
      const d = await r.json();
      if (d.ok) {
        if (btn) { btn.classList.add('btn-prowlarr-grab--sent'); btn.innerHTML = '<i class="fa-solid fa-check" aria-hidden="true"></i>'; }
      } else {
        window.toast(d.error || 'Could not send to download client');
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-download" aria-hidden="true"></i>'; }
      }
    } catch (e) {
      window.toast(e.message || 'Could not send to download client');
      if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-download" aria-hidden="true"></i>'; }
    }
  }

  function closeAllFavMenus() {
    document.querySelectorAll('.fav-cell.is-open').forEach(c => c.classList.remove('is-open'));
  }

  document.addEventListener('click', (e) => {
    if (!e.target.closest('.fav-cell')) closeAllFavMenus();
  });

  document.getElementById('favGrid').addEventListener('click', (e) => {
    const postersPop = e.target.closest('[data-act="posters-popup"]');
    if (postersPop) {
      e.preventDefault(); e.stopPropagation();
      openPosterRolePicker(parseInt(postersPop.getAttribute('data-id'), 10));
      return;
    }
    const bioPop = e.target.closest('[data-act="bio-popup"]');
    if (bioPop) {
      e.preventDefault(); e.stopPropagation();
      window.openPerformerPopup({ libraryRowId: parseInt(bioPop.getAttribute('data-id'), 10) });
      return;
    }
    const localStashFav = e.target.closest('[data-act="local-stash-fav"]');
    if (localStashFav) {
      e.preventDefault(); e.stopPropagation();
      const rid = parseInt(localStashFav.getAttribute('data-id'), 10);
      if (rid) openLocalStashFromFavouriteRow(rid);
      return;
    }
    const mlock = e.target.closest('[data-act="matches-lock"]');
    if (mlock) {
      e.preventDefault(); e.stopPropagation();
      const id = parseInt(mlock.getAttribute('data-id'), 10);
      toggleMatchesLock(id, mlock.getAttribute('data-locked') !== '1');
      return;
    }
    const star = e.target.closest('[data-act="star"]');
    if (star) {
      e.stopPropagation();
      toggleStar(parseInt(star.getAttribute('data-id'), 10), star.getAttribute('data-on') === '1');
      return;
    }
    if (e.target.closest('button')) return;
    const cell = e.target.closest('.fav-cell');
    if (!cell || !e.target.closest('.fav-cell-visual')) return;
    const id = parseInt(cell.getAttribute('data-id'), 10);
    if (!id) return;
    // Vice rows use negative synthetic IDs (vices_json is settings-driven,
    // not in the favourites table). They render their own client-side
    // panel since all the data — name, tags, logo URL — is already in
    // _rowsVice and no /api/favourites/entity-panel call would resolve.
    if (id < 0) {
      openViceDetail(id);
      return;
    }
    const rowData = [..._rowsPerf, ..._rowsStudio, ..._rowsMovie].find(r => r.id === id);
    if (rowData?.kind === 'performer') {
      window.openPerformerPopup({ libraryRowId: id });
    }
    else if (rowData?.kind === 'studio') {
      window.openStudioPopup({
        libraryRowId: id,
        name: rowData.folder_name || rowData.display_name || '',
      });
    }
    else openEntityDetail(id);
  });

  document.getElementById('entityPanelModal').addEventListener('click', (e) => {
    const lsf = e.target.closest('[data-act="local-stash-fav"]');
    if (lsf && e.currentTarget.contains(lsf)) {
      e.preventDefault();
      e.stopPropagation();
      if (lsf.disabled) return;
      const rid = parseInt(lsf.getAttribute('data-id'), 10);
      if (rid) openLocalStashFromFavouriteRow(rid);
      return;
    }
    if (e.target === e.currentTarget) closeEntityPanel();
  });

  document.getElementById('searchResults').addEventListener('click', (e) => {
    const ls = e.target.closest('[data-act="local-stash"]');
    if (!ls) return;
    e.preventDefault();
    e.stopPropagation();
    openLocalStashFromBtn(ls);
  });

  // ── In-place tile / count helpers ────────────────────────────────
  // Per-row actions (star, lock, unmatch, clear, refresh, delete) used
  // to call `load()`, which refetches /api/favourites (heavy: returns
  // all 5 kinds + does a filesystem stat per row) and rebuilds the
  // entire grid via `innerHTML =`. Even toggling one heart cost the
  // full payload + a 100-500-tile teardown.
  //
  // These helpers update one row at a time:
  //   _findRowAcrossLists  — locate the row in _rowsPerf/Studio/Movie/Vice
  //   _replaceRowInLists   — swap an updated row into whichever list it lives in
  //   _replaceTileInDom    — re-render just that .fav-cell, honouring filters
  //   _removeTileFromDom   — drop the tile (delete / filtered out)
  //   _refreshFavCount     — update the toolbar "N performers" counter
  //
  // Each per-row action also takes a short-lived in-flight lock so a
  // user double-tap can't queue duplicate requests + re-renders.
  function _findRowAcrossLists(rowId) {
    return _rowsPerf.find(r => r.id === rowId)
        || _rowsStudio.find(r => r.id === rowId)
        || _rowsMovie.find(r => r.id === rowId)
        || _rowsVice.find(r => r.id === rowId)
        || null;
  }
  function _replaceRowInLists(updated) {
    if (!updated || updated.id == null) return false;
    for (const list of [_rowsPerf, _rowsStudio, _rowsMovie, _rowsVice]) {
      const idx = list.findIndex(r => r.id === updated.id);
      if (idx !== -1) { list[idx] = updated; return true; }
    }
    return false;
  }
  function _removeRowFromLists(rowId) {
    for (const list of [_rowsPerf, _rowsStudio, _rowsMovie, _rowsVice]) {
      const idx = list.findIndex(r => r.id === rowId);
      if (idx !== -1) return list.splice(idx, 1)[0];
    }
    return null;
  }
  function _replaceTileInDom(rowId) {
    const row = _findRowAcrossLists(rowId);
    const cell = document.querySelector(`.fav-cell[data-id="${rowId}"]`);
    if (!cell) return;
    if (!row || !rowPassesFilters(row)) { cell.remove(); _refreshFavCount(); return; }
    const wrap = document.createElement('div');
    wrap.innerHTML = renderCard(row);
    const fresh = wrap.firstElementChild;
    if (fresh) cell.replaceWith(fresh);
  }
  function _removeTileFromDom(rowId) {
    const cell = document.querySelector(`.fav-cell[data-id="${rowId}"]`);
    if (cell) cell.remove();
  }
  function _refreshFavCount() {
    const list = _view === 'studio' ? _rowsStudio
               : _view === 'movie'  ? _rowsMovie
               : _view === 'vice'   ? _rowsVice
               :                      _rowsPerf;
    const filtered = list.filter(r => rowPassesFilters(r));
    const total = list.length;
    const kindLabel = _view === 'studio' ? 'studios'
                    : _view === 'movie'  ? 'movies'
                    : _view === 'vice'   ? 'vices'
                    :                      'stars';
    const fa = filtersActive();
    const el = document.getElementById('favCount');
    if (el) el.textContent = fa ? `${filtered.length} / ${total} ${kindLabel}` : `${total} ${kindLabel}`;
  }
  // Single-row refetch — used by actions whose response we can't
  // synthesise client-side (refresh, manual match through the search
  // modal). Falls back to a full `load()` if the row endpoint is
  // missing (older backend / cache miss).
  async function _refreshOneRow(rowId) {
    try {
      const r = await fetch(`/api/favourites/row?id=${encodeURIComponent(rowId)}`, { credentials: 'same-origin' });
      if (!r.ok) throw new Error('row fetch failed');
      const d = await r.json();
      if (!d.row) throw new Error('no row');
      _replaceRowInLists(d.row);
      _replaceTileInDom(rowId);
      return true;
    } catch (e) {
      return false;
    }
  }
  // Per-row + per-action lock: blocks a second click on the same row
  // for the same action while a fetch is in flight. Star/lock/unmatch
  // are the common offenders for double-tap.
  const _inflightRowActions = new Set();
  function _claimInflight(rowId, action) {
    const k = `${rowId}:${action}`;
    if (_inflightRowActions.has(k)) return false;
    _inflightRowActions.add(k);
    return true;
  }
  function _releaseInflight(rowId, action) {
    _inflightRowActions.delete(`${rowId}:${action}`);
  }

  function render() {
    const list = _view === 'studio' ? _rowsStudio
               : _view === 'movie'  ? _rowsMovie
               : _view === 'vice'   ? _rowsVice
               :                      _rowsPerf;
    const filtered = list.filter(r => rowPassesFilters(r));
    const grid = document.getElementById('favGrid');
    const total = list.length;
    const kindLabel = _view === 'studio' ? 'studios'
                    : _view === 'movie'  ? 'movies'
                    : _view === 'vice'   ? 'vices'
                    :                      'stars';
    const fa = filtersActive();
    document.getElementById('favCount').textContent = fa
      ? `${filtered.length} / ${total} ${kindLabel}`
      : `${total} ${kindLabel}`;

    if (!list.length) {
      grid.innerHTML = `<div class="empty-tile">${_view === 'studio'
        ? 'No studio folders. Set <strong>Series Library Directory</strong> in Settings, then Scan.'
        : _view === 'movie'
          ? 'No movie folders. Set <strong>Movies Library Directory</strong> (features_dir) in Settings, then Scan. TMDB matches use the API key under Settings (same as the Movies page).'
          : _view === 'vice'
            ? 'No vice folders. Set <strong>Vices Library Directory</strong> in Settings, then Scan.'
            : 'No star folders. Add <strong>Star Library Directories</strong> in Settings, then Scan.'}</div>`;
      return;
    }
    if (!filtered.length) {
      grid.innerHTML = '<div class="empty-tile">No folders match these filters.</div>';
      _renderFavPager(0);
      return;
    }
    // Pagination — slice to one page so the DOM never holds more
    // than `_FAV_PAGE_SIZE` tiles. With pagination + the lazy-render
    // observer working on the rendered slice, even a 2000-row library
    // pays the cost of ~100 tiles per page.
    const totalPages = Math.max(1, Math.ceil(filtered.length / _FAV_PAGE_SIZE));
    if (_favPage > totalPages) _favPage = totalPages;
    if (_favPage < 1) _favPage = 1;
    const start = (_favPage - 1) * _FAV_PAGE_SIZE;
    const slice = filtered.slice(start, start + _FAV_PAGE_SIZE);
    // Lazy-render: emit a lightweight placeholder for every row in
    // the current page slice (preserves grid layout + scroll height)
    // and only paint the expensive renderCard markup for tiles
    // entering the viewport. With page size 100 + ~60 visible per
    // viewport, this trims the first-paint of any page to ~20 ms.
    const LAZY_THRESHOLD = 60;
    if (slice.length <= LAZY_THRESHOLD) {
      grid.innerHTML = slice.map(renderCard).join('');
    } else {
      // Skeleton placeholder retains data-id (used by _replaceTileInDom
      // for in-place actions) and a `data-fav-lazy` marker the observer
      // matches against. Aspect-ratio matches both performer (2/3) and
      // studio (4/3 via .fav-grid--studio override) — the wrapper sizes
      // off the parent grid track either way.
      grid.innerHTML = slice.map(row =>
        `<div class="fav-cell fav-cell--skeleton fav-cell--lazy" data-id="${row.id}" data-fav-lazy="1" aria-hidden="true"><div class="fav-cell-visual" style="aspect-ratio:2/3"></div></div>`
      ).join('');
      _ensureFavLazyObserver();
      grid.querySelectorAll('[data-fav-lazy="1"]').forEach(el => _favLazyObserver.observe(el));
    }
    _renderFavPager(filtered.length);
  }

  // ── Pagination ───────────────────────────────────────────────────
  // Slices the filtered set to a single page so the DOM never carries
  // more than `_FAV_PAGE_SIZE` tiles at a time. Combined with the
  // lazy-render observer above, even 2000-row libraries render
  // first-paint in ~20 ms. Page resets to 1 whenever filters or the
  // active tab change so the user doesn't land on an out-of-range page.
  const _FAV_PAGE_SIZE = 100;
  let _favPage = 1;
  function _renderFavPager(totalRows) {
    const pager = document.getElementById('favPager');
    if (!pager) return;
    if (totalRows <= _FAV_PAGE_SIZE) { pager.style.display = 'none'; return; }
    pager.style.display = '';
    const totalPages = Math.max(1, Math.ceil(totalRows / _FAV_PAGE_SIZE));
    const stateEl = document.getElementById('favPagerState');
    const metaEl  = document.getElementById('favPagerMeta');
    const firstBtn = document.getElementById('favPagerFirst');
    const prevBtn  = document.getElementById('favPagerPrev');
    const nextBtn  = document.getElementById('favPagerNext');
    const lastBtn  = document.getElementById('favPagerLast');
    const lo = (_favPage - 1) * _FAV_PAGE_SIZE + 1;
    const hi = Math.min(totalRows, _favPage * _FAV_PAGE_SIZE);
    if (stateEl) stateEl.textContent = `Page ${_favPage} / ${totalPages}`;
    if (metaEl)  metaEl.textContent = `· ${lo}–${hi} of ${totalRows}`;
    [firstBtn, prevBtn].forEach(b => { if (b) b.disabled = (_favPage <= 1); });
    [nextBtn, lastBtn].forEach(b => { if (b) b.disabled = (_favPage >= totalPages); });
  }
  function _favGoToPage(p) {
    const next = Math.max(1, Math.floor(Number(p) || 1));
    if (next === _favPage) return;
    _favPage = next;
    render();
    // Scroll the grid back to its top so the new page lands in view.
    const scroll = document.querySelector('.fav-scroll');
    if (scroll) scroll.scrollTo({ top: 0, behavior: 'smooth' });
  }
  // Wire up once at script load — buttons exist in the static HTML.
  document.getElementById('favPagerFirst')?.addEventListener('click', () => _favGoToPage(1));
  document.getElementById('favPagerPrev')?.addEventListener('click',  () => _favGoToPage(_favPage - 1));
  document.getElementById('favPagerNext')?.addEventListener('click',  () => _favGoToPage(_favPage + 1));
  document.getElementById('favPagerLast')?.addEventListener('click',  () => {
    const list = _view === 'studio' ? _rowsStudio
               : _view === 'movie'  ? _rowsMovie
               : _view === 'vice'   ? _rowsVice
               :                      _rowsPerf;
    const filtered = (list || []).filter(r => rowPassesFilters(r));
    _favGoToPage(Math.max(1, Math.ceil(filtered.length / _FAV_PAGE_SIZE)));
  });

  // ── Lazy-render observer ────────────────────────────────────────
  // Watches placeholder tiles and swaps in the real markup the moment
  // they enter the viewport (or come within `rootMargin` of it). Once
  // a tile is painted it's no longer observed, so memory grows
  // monotonically with what the user has scrolled past — the trade
  // we want (smooth scroll-up + no re-paint flicker).
  let _favLazyObserver = null;
  function _ensureFavLazyObserver() {
    if (_favLazyObserver) return;
    _favLazyObserver = new IntersectionObserver((entries) => {
      for (const ent of entries) {
        if (!ent.isIntersecting) continue;
        const cell = ent.target;
        _favLazyObserver.unobserve(cell);
        const id = parseInt(cell.getAttribute('data-id'), 10);
        if (!id) continue;
        const row = _findRowAcrossLists(id);
        if (!row) continue;
        // Match the wrapper renderCard would have produced. Swapping
        // outerHTML preserves the grid track this tile occupies.
        const wrap = document.createElement('div');
        wrap.innerHTML = renderCard(row);
        const fresh = wrap.firstElementChild;
        if (fresh) cell.replaceWith(fresh);
      }
    }, {
      // Render ~one viewport ahead in either direction so the user
      // never sees a placeholder while scrolling at normal speeds.
      rootMargin: '600px 0px 600px 0px',
      threshold: 0.01,
    });
  }

  // Track which tabs have data loaded so we don't refetch on every
  // tab-switch. `load()` (no arg) wipes these so a full refresh after
  // a scan re-pulls everything.
  let _loadedKinds = new Set();
  // Map the tab-name to the `kind` filter the backend accepts. Movies
  // and JAV always travel together because the Movies tab merges them.
  const _LIB_TAB_KINDS = {
    performer: ['performer'],
    studio:    ['studio'],
    movie:     ['movie', 'jav'],
    vice:      ['vice'],
  };

  // ── Search tab (added alongside Movies/Stars/Studios/Vices) ────────
  // Reuses /api/metadata/search for performer + studio, /api/movies/search
  // for movies. Results render as fav-cell-style tiles; clicks open the
  // existing performer / studio popups. Items already in the library
  // route to {libraryRowId}, new items to {stashId, name}.

  let _favSearchType = 'performer';   // 'performer' | 'studio' | 'movie'
  let _favSearchMounted = false;
  let _favSearchLastQ = '';
  let _favSearchSeq = 0;

  function _mountFavSearchOnce() {
    if (_favSearchMounted) return;
    _favSearchMounted = true;
    // Toggle the clear-button visibility based on input state — clears
    // the query, hides itself, and re-focuses the input so the user can
    // keep typing without grabbing the mouse.
    const inp = document.getElementById('favSearchQ');
    const clr = document.getElementById('favSearchClearBtn');
    if (inp && clr) {
      const sync = () => { clr.hidden = !inp.value.trim(); };
      inp.addEventListener('input', sync);
      sync();
      // Bind on mousedown so we fire BEFORE the click moves focus to the
      // button (which would blur the input, drop its :focus styling and
      // cause a small visual shift that read as a "jump"). preventDefault
      // keeps focus on the input. Inline onclick used to swallow the
      // value-clear in some browsers — JS-bound listener is reliable.
      clr.addEventListener('mousedown', (e) => {
        e.preventDefault();
        e.stopPropagation();
        clearFavSearch();
      });
    }
    setTimeout(() => { if (inp) inp.focus(); }, 30);
  }

  function setFavSearchType(t) {
    _favSearchType = t;
    document.getElementById('favSearchTypeStars')?.classList.toggle('active', t === 'performer');
    document.getElementById('favSearchTypeStudios')?.classList.toggle('active', t === 'studio');
    document.getElementById('favSearchTypeMovies')?.classList.toggle('active', t === 'movie');
    const results = document.getElementById('favSearchResults');
    if (results) {
      // Movies reuse the .fav-grid--perf column layout (10/8/6/4
      // responsive) and layer .fav-grid--movie on top as a styling
      // modifier — same combo the Movies tab uses. Studios get
      // their own narrower column count.
      results.classList.remove('fav-grid--perf', 'fav-grid--studio', 'fav-grid--movie');
      if (t === 'studio') {
        results.classList.add('fav-grid--studio');
      } else if (t === 'movie') {
        results.classList.add('fav-grid--perf', 'fav-grid--movie');
      } else {
        results.classList.add('fav-grid--perf');
      }
    }
    if (_favSearchLastQ) runFavSearch();
  }
  window.setFavSearchType = setFavSearchType;

  function clearFavSearch() {
    const inp = document.getElementById('favSearchQ');
    if (inp) { inp.value = ''; inp.focus(); }
    const clr = document.getElementById('favSearchClearBtn');
    if (clr) clr.hidden = true;
    _favSearchLastQ = '';
    const results = document.getElementById('favSearchResults');
    if (results) results.innerHTML = '';
    const status = document.getElementById('favSearchStatus');
    if (status) { status.hidden = true; status.textContent = ''; }
  }
  window.clearFavSearch = clearFavSearch;

  // Find a loaded library row whose canonical or per-source-DB name
  // matches the incoming search hit. Used to mark "already in library"
  // on result tiles and to route clicks through the existing popups by
  // libraryRowId instead of stashId.
  function _favSearchMatchLibraryRow(hit, type) {
    const list = type === 'studio' ? (_rowsStudio || [])
               : type === 'movie'  ? (_rowsMovie  || [])
               :                     (_rowsPerf   || []);
    const hitName = String(hit.name || '').trim().toLowerCase();
    const hitId   = String(hit.id   || '').trim();
    if (!list.length) return null;
    for (const r of list) {
      if (!r) continue;
      // Crosswalk-ID match wins over name match.
      if (hitId) {
        const ids = [r.match_tpdb_id, r.match_stashdb_id, r.match_fansdb_id, r.match_javstash_id, r.match_tmdb_id]
          .filter(Boolean).map(x => String(x));
        if (ids.includes(hitId)) return r;
      }
    }
    if (!hitName) return null;
    for (const r of list) {
      if (!r) continue;
      const candidates = [
        r.folder_name, r.display_name,
        r.match_stashdb_name, r.match_tpdb_name, r.match_fansdb_name, r.match_javstash_name,
      ].filter(Boolean).map(s => String(s).trim().toLowerCase());
      if (candidates.includes(hitName)) return r;
    }
    return null;
  }

  async function runFavSearch() {
    const inp = document.getElementById('favSearchQ');
    const q = (inp && inp.value || '').trim();
    const results = document.getElementById('favSearchResults');
    const status = document.getElementById('favSearchStatus');
    if (!q) {
      if (results) results.innerHTML = '';
      if (status) { status.hidden = false; status.textContent = 'Type a name and hit Enter.'; }
      return;
    }
    _favSearchLastQ = q;
    const mySeq = ++_favSearchSeq;
    const type = _favSearchType;
    if (status) { status.hidden = false; status.textContent = 'Searching…'; }
    if (results) {
      // Three skeleton tiles is enough to signal "loading" without
      // flashing 12 empty cells when typical results return 1-5.
      results.innerHTML = Array.from({length: 3}, () =>
        '<div class="fav-cell fav-cell--skeleton" aria-hidden="true"><div class="fav-cell-visual" style="aspect-ratio:2/3"></div></div>'
      ).join('');
    }
    // Ensure the library cache for this search type is loaded so
    // _favSearchMatchLibraryRow can flag "already in library" hits.
    // The Search tab doesn't visit the other tabs' lazy fetches, so on
    // a cold load these arrays are empty and every result renders as
    // "new" even when the row exists.
    try {
      const tabKind = type === 'movie' ? 'movie' : type;
      const needKinds = (_LIB_TAB_KINDS[tabKind] || [tabKind]).filter(k => !_loadedKinds.has(k));
      if (needKinds.length) await Promise.all(needKinds.map(_loadKind));
    } catch (_) { /* match badge will just be missing — soft fail */ }
    if (mySeq !== _favSearchSeq) return;
    let hits = [];
    try {
      if (type === 'movie') {
        const r = await fetch('/api/movies/search?q=' + encodeURIComponent(q), { credentials: 'same-origin' });
        const d = await r.json();
        if (mySeq !== _favSearchSeq) return;
        if (d && d.error) throw new Error(d.error);
        hits = Array.isArray(d?.results) ? d.results : (Array.isArray(d?.movies) ? d.movies : []);
      } else {
        const r = await fetch('/api/metadata/search?q=' + encodeURIComponent(q) + '&type=' + encodeURIComponent(type) + '&strict=0', { credentials: 'same-origin' });
        const d = await r.json();
        if (mySeq !== _favSearchSeq) return;
        if (d && d.error) throw new Error(d.error);
        hits = Array.isArray(d?.results) ? d.results : [];
      }
    } catch (e) {
      if (mySeq !== _favSearchSeq) return;
      if (status) { status.hidden = false; status.textContent = 'Search failed: ' + (e?.message || 'error'); }
      if (results) results.innerHTML = '';
      return;
    }
    if (mySeq !== _favSearchSeq) return;
    if (!hits.length) {
      if (status) { status.hidden = false; status.textContent = 'No matches.'; }
      if (results) results.innerHTML = '';
      return;
    }
    if (status) { status.hidden = false; status.textContent = `${hits.length} result${hits.length === 1 ? '' : 's'}.`; }
    _renderFavSearchResults(hits, type);
  }
  window.runFavSearch = runFavSearch;

  function _renderFavSearchResults(hits, type) {
    const results = document.getElementById('favSearchResults');
    if (!results) return;
    const studioLike = type === 'studio';
    const movieLike  = type === 'movie';
    // Studios without an image: use the Top-Shelf wordmark
    // (logo.webp) instead of the wide lips placeholder — looks
    // less out-of-place inside the studio TV-frame tile.
    // Performers + movies are portrait → 2:3 poster fallback so
    // missing covers don't get cropped weird.
    const fallback = studioLike ? '/static/img/logo.webp' : POSTER_FALLBACK;
    const tiles = hits.map((hit, idx) => {
      const name  = String(hit.name || hit.title || '').trim() || '—';
      const image = String(hit.image || hit.image_url || hit.poster || hit.cover || hit.front_cover || '').trim();
      const src   = String(hit.source || hit._source || '').trim();
      const matched = _favSearchMatchLibraryRow(hit, type);
      const inLib = !!matched;
      const libId = matched ? Number(matched.id || 0) : 0;
      const hitId = String(hit.id || '').trim();
      const posterSrc = image || fallback;
      const imgErr = image ? `this.onerror=null;this.src='${fallback}'` : '';
      const errAttr = imgErr ? ` onerror="${imgErr}"` : '';
      const heartIcon = inLib
        ? '<span class="fav-search-tile-badge" title="Already in library" aria-label="In library"><i class="fa-solid fa-bookmark"></i></span>'
        : '';
      // Map source-DB names → logo files (same icons used everywhere
      // else: queue Scene Search, scenes feed source toggle, etc.).
      // Falls back to the plain text pill when the source string
      // doesn't match a known DB.
      const srcLogo = (() => {
        const k = src.toLowerCase();
        if (k === 'tpdb' || k === 'theporndb') return '/static/logos/tpdb.webp';
        if (k === 'stashdb') return '/static/logos/stashdb.webp';
        if (k === 'fansdb')  return '/static/logos/fansdb.webp';
        if (k === 'javstash') return '/static/logos/javstash.webp';
        return '';
      })();
      const sourceBadge = src
        ? (srcLogo
            ? `<span class="fav-search-tile-source fav-search-tile-source--logo" title="Source: ${esc(src)}"><img src="${srcLogo}" alt="${esc(src)}" loading="lazy"></span>`
            : `<span class="fav-search-tile-source" title="Source: ${esc(src)}">${esc(src)}</span>`)
        : '';
      const phClass = studioLike ? 'fav-studio-ph' : 'fav-perf-ph';
      // Studio tiles get the CRT/TV-frame overlay so they look like
      // the Studios tab (vignette + scan-line frame on top of the
      // duotone logo). Performer tiles skip it.
      const tvOverlay = studioLike
        ? '<div class="fav-studio-tv-overlay" aria-hidden="true"></div>'
        : '';
      // Movies: the Movies tab routes posters through
      // tsApplyMovieCoverSplit(img, 'solo') so wide front+back cover
      // spreads get cropped to the front side. Mirror that here so
      // search results don't look squished compared to the rest of
      // /library.
      const movieSplitOnload = movieLike
        ? ' onload="typeof tsApplyMovieCoverSplit===\'function\'&&tsApplyMovieCoverSplit(this,\'solo\')"'
        : '';
      return `<div class="fav-cell fav-cell--search" data-search-idx="${idx}" data-in-lib="${inLib ? '1' : '0'}" data-lib-id="${libId}" data-stash-id="${esc(hitId)}" data-source="${esc(src)}" data-name="${esc(name)}" tabindex="0">
        <div class="fav-cell-visual">
          <div class="${phClass}">
            <div class="duo-inner"><img class="duo-img" src="${esc(posterSrc)}" alt="" loading="lazy" referrerpolicy="no-referrer"${errAttr}${movieSplitOnload}><div class="duo-tint" aria-hidden="true"></div></div>
            ${heartIcon}${sourceBadge}
            ${tvOverlay}
          </div>
          <div class="fav-cell-title">${esc(name)}</div>
        </div>
      </div>`;
    }).join('');
    results.innerHTML = tiles;
    results.classList.remove('fav-grid--perf', 'fav-grid--studio', 'fav-grid--movie');
    if (studioLike) {
      results.classList.add('fav-grid--studio');
    } else if (movieLike) {
      results.classList.add('fav-grid--perf', 'fav-grid--movie');
    } else {
      results.classList.add('fav-grid--perf');
    }
  }

  // Delegated click on the search results grid — routes to the same
  // popups the rest of /library uses. Already-in-library tiles open
  // by libraryRowId; new tiles open by source-correct id (no name,
  // because /api/performer/popup falls back to alias lookup when a
  // name is supplied and would mis-snap a hit like "Anna Marie" onto
  // Ariana Marie via her aliases).
  document.addEventListener('click', (ev) => {
    const cell = ev.target.closest('#favSearchResults .fav-cell');
    if (!cell) return;
    ev.preventDefault();
    const inLib = cell.getAttribute('data-in-lib') === '1';
    const libId = parseInt(cell.getAttribute('data-lib-id') || '0', 10) || 0;
    const stashId = cell.getAttribute('data-stash-id') || '';
    const name = cell.getAttribute('data-name') || '';
    const src  = (cell.getAttribute('data-source') || '').toLowerCase();
    const type = _favSearchType;
    if (type === 'studio') {
      if (typeof window.openStudioPopup === 'function') {
        if (inLib && libId) {
          window.openStudioPopup({ libraryRowId: libId, name });
        } else if (stashId) {
          // External studio: pass the source so the popup can fetch
          // the source-DB's logo, description, parent and recent
          // scenes via /api/studios/external-panel.
          window.openStudioPopup({ stashId, source: src, name });
        } else {
          window.openStudioPopup({ name });
        }
      }
    } else if (type === 'movie') {
      // Movie popup is its own module — fetches /api/movies/tpdb/{id}
      // and renders the same VHS-frame card /scenes uses. Falls back
      // to Prowlarr by title when no id is available.
      if (stashId && typeof window.openMoviePopup === 'function') {
        window.openMoviePopup(stashId);
      } else if (typeof window.openProwlarrSearchPopup === 'function') {
        window.openProwlarrSearchPopup({ title: name, kind: 'movie' });
      }
    } else {
      if (typeof window.openPerformerPopup === 'function') {
        if (inLib && libId) {
          window.openPerformerPopup({ libraryRowId: libId, name });
        } else if (stashId) {
          // Route the id to the param matching its source so the
          // popup backend hits the right crosswalk column. Omit
          // `name` so an unmatched id can't alias-fallback onto a
          // similarly-named library row (the Ariana Maria → Ariana
          // Marie bug).
          if (src === 'tpdb' || src === 'theporndb') {
            window.openPerformerPopup({ tpdbId: stashId });
          } else {
            window.openPerformerPopup({ stashId });
          }
        } else {
          window.openPerformerPopup({ name });
        }
      }
    }
  });

  async function _loadKind(kind) {
    const url = '/api/favourites?kind=' + encodeURIComponent(kind);
    const r = await fetch(url, { credentials: 'same-origin' });
    const d = await r.json();
    if (!r.ok) throw new Error(d.error || 'Failed to load');
    if (kind === 'performer') _rowsPerf   = d.performers || [];
    if (kind === 'studio')    _rowsStudio = d.studios    || [];
    if (kind === 'movie')     _rowsMovie  = [...(d.movies || []),
                                             ...(_rowsMovie || []).filter(r => r.kind === 'jav')];
    if (kind === 'jav')       _rowsMovie  = [...(_rowsMovie || []).filter(r => r.kind === 'movie'),
                                             ...(d.jav || [])];
    if (kind === 'vice')      _rowsVice   = d.vices || [];
    if (d.settings) _favStashLocalOn = !!d.settings.stash_local_lookup_enabled;
    _loadedKinds.add(kind);
  }

  // Performer / studio / movie added via popup or /discover → refresh
  // the affected kind so the new tile shows up in the active tab and
  // the Search-tab "in library" badge flips without a manual reload.
  document.addEventListener('library-row-added', async (ev) => {
    const kind = (ev && ev.detail && ev.detail.kind) || '';
    const refreshKind = (kind === 'studio') ? 'studio'
                      : (kind === 'movie')  ? 'movie'
                      : (kind === 'jav')    ? 'jav'
                      : (kind === 'vice')   ? 'vice'
                      : 'performer';
    _loadedKinds.delete(refreshKind);
    if (_view === 'search') {
      // Search view bypasses the grid — pull fresh rows so the
      // "in library" lookup picks up the new id, then re-run the
      // active query to flip badges on visible tiles.
      try { await _loadKind(refreshKind); } catch (_) { /* noop */ }
      if (_favSearchLastQ) runFavSearch();
    } else {
      loadActiveView();
    }
  });

  // Lazy-load just the active tab on first paint. Switching to another
  // tab triggers a single-kind fetch the first time. Big perf win when
  // your library has thousands of performers but you only want movies.
  async function loadActiveView() {
    const kinds = _LIB_TAB_KINDS[_view] || ['performer'];
    try {
      const missing = kinds.filter(k => !_loadedKinds.has(k));
      if (missing.length) await Promise.all(missing.map(_loadKind));
      populateRootFilter();
      updateFavFilterIconBtns();
      render();
      checkIndexProgressOnce();
    } catch (e) { showMsg(e.message || 'Error'); }
  }

  // Full refresh — wipes cache and refetches every kind. Used by
  // scan-finished, refresh-all, and other actions that touch many
  // rows across kinds.
  async function load() {
    try {
      const r = await fetch('/api/favourites', { credentials: 'same-origin' });
      const d = await r.json();
      if (!r.ok) throw new Error(d.error || 'Failed to load');
      _rowsStudio = d.studios || [];
      _rowsPerf = d.performers || [];
      // JAV releases are kind='jav' in the favourites table but live
      // alongside feature films under the Movies tab — they're both
      // movie-shaped entities (a folder of one video with NFO/poster).
      // Keep the original kind so external-link routing (JAVStash vs
      // TPDB movies) and any future per-kind branches still work.
      _rowsMovie = [...(d.movies || []), ...(d.jav || [])];
      _rowsVice = d.vices || [];
      _favStashLocalOn = !!(d.settings && d.settings.stash_local_lookup_enabled);
      _loadedKinds = new Set(['performer', 'studio', 'movie', 'jav', 'vice']);
      // /downloads + /news + /scenes share a cached library-token map
      // (ts-utils.js _refreshLibraryTokens). A full /library reload is
      // the canonical "favourites set may have changed" moment — bust
      // the cache so the next cross-page nav sees fresh tokens for
      // release-name highlighting without waiting on the 10-min TTL.
      if (typeof window._invalidateLibraryTokens === 'function') window._invalidateLibraryTokens();
      populateRootFilter();
      updateFavFilterIconBtns();
      render();
      checkIndexProgressOnce();
    } catch (e) {
      showMsg(e.message || 'Error');
    }
  }

  async function toggleStar(id, on) {
    if (!id || !_claimInflight(id, 'star')) return;
    const row = _findRowAcrossLists(id);
    const prev = row ? Number(row.is_favourite) : null;
    if (row) {
      row.is_favourite = on ? 1 : 0;
      _replaceTileInDom(id);
    }
    try {
      const r = await fetch('/api/favourites/star', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id, is_favourite: !!on }),
      });
      if (!r.ok) throw new Error('Star toggle failed');
    } catch (e) {
      if (row && prev !== null) { row.is_favourite = prev; _replaceTileInDom(id); }
      if (window.toast) window.toast(e.message || 'Star toggle failed');
    } finally {
      _releaseInflight(id, 'star');
    }
  }

  async function toggleMatchesLock(id, locked) {
    if (!id || !_claimInflight(id, 'lock')) return;
    const row = _findRowAcrossLists(id);
    const prev = row ? Number(row.matches_locked) : null;
    if (row) {
      row.matches_locked = locked ? 1 : 0;
      _replaceTileInDom(id);
    }
    try {
      const r = await fetch('/api/favourites/lock', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id, matches_locked: !!locked }),
      });
      if (!r.ok) throw new Error('Lock toggle failed');
      // Entity panel mirrors the lock state in its own chrome — repaint
      // it without touching the grid.
      if (_entityPanelRowId === id) openEntityDetail(id);
    } catch (e) {
      if (row && prev !== null) { row.matches_locked = prev; _replaceTileInDom(id); }
      if (window.toast) window.toast(e.message || 'Lock toggle failed');
    } finally {
      _releaseInflight(id, 'lock');
    }
  }

  async function removeFromDatabase(id) {
    if (!id) return;
    const ok = await (window.tsConfirm
      ? window.tsConfirm(
          'Remove this folder from the favourites index?\n\nFiles on disk are not deleted; a later library scan can add it again.',
          { title: 'Remove from library', confirm: 'Remove', destructive: true })
      : Promise.resolve(window.confirm('Remove this folder from the favourites index?')));
    if (!ok) return;
    if (!_claimInflight(id, 'delete')) return;
    // Optimistic remove: drop from list + DOM immediately. If the
    // backend rejects we re-insert and full-render. The folder may
    // still exist on disk so a later Scan can re-add it either way.
    const removed = _removeRowFromLists(id);
    _removeTileFromDom(id);
    closeAllFavMenus();
    if (_entityPanelRowId === id) closeEntityPanel();
    _refreshFavCount();
    showMsg('Removing…');
    try {
      const r = await fetch('/api/favourites/delete', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id }),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(d.error || 'Remove failed');
      showMsg('Removed from index');
      // Token set just shrunk — invalidate the shared cache so other
      // pages stop highlighting the removed name.
      if (typeof window._invalidateLibraryTokens === 'function') window._invalidateLibraryTokens();
    } catch (e) {
      if (removed) {
        // Re-insert into the matching kind list and full re-render
        // (cheaper than tracking the original index for restoration).
        const target = removed.kind === 'studio' ? _rowsStudio
                     : removed.kind === 'movie' || removed.kind === 'jav' ? _rowsMovie
                     : removed.kind === 'vice' ? _rowsVice
                     : _rowsPerf;
        target.push(removed);
        render();
      }
      window.toast(e.message || 'Remove failed');
    } finally {
      _releaseInflight(id, 'delete');
    }
  }

  async function clearMatchesOne(id) {
    const ok = await (window.tsConfirm
      ? window.tsConfirm(
          'Clear all TPDB / Stash / Fans / JAVStash links for this folder?',
          { title: 'Clear DB links', confirm: 'Clear', destructive: true })
      : Promise.resolve(window.confirm('Clear all TPDB / Stash / Fans / JAVStash links for this folder?')));
    if (!ok) return;
    if (!_claimInflight(id, 'clear-matches')) return;
    showMsg('Clearing…');
    try {
      const r = await fetch('/api/favourites/clear-matches', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id }),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(d.error || 'Clear failed');
      showMsg('Cleared');
      // Pull the cleared row fresh so the pills update without a
      // full-grid refetch.
      await _refreshOneRow(id);
      if (_entityPanelRowId === id) openEntityDetail(id);
    } catch (e) {
      window.toast(e.message || 'Clear failed');
    } finally {
      _releaseInflight(id, 'clear-matches');
    }
  }

  async function unmatchOne(id, source) {
    if (!_claimInflight(id, `unmatch:${source}`)) return;
    // Optimistic clear of the single source's id/name so the pill flips
    // straight away. Refetched row overwrites these on success.
    const row = _findRowAcrossLists(id);
    const u = String(source || '').toUpperCase();
    const fieldMap = {
      TMDB:     ['match_tmdb_id',     'match_tmdb_name'],
      TPDB:     ['match_tpdb_id',     'match_tpdb_name'],
      STASHDB:  ['match_stashdb_id',  'match_stashdb_name'],
      FANSDB:   ['match_fansdb_id',   'match_fansdb_name'],
      JAVSTASH: ['match_javstash_id', 'match_javstash_name'],
      IAFD:     ['match_iafd_url'],
      FREEONES: ['match_freeones_url'],
      BABEPEDIA:['match_babepedia_url'],
      COOMER:   ['match_coomer_url'],
    };
    const cols = fieldMap[u] || [];
    const prev = {};
    if (row) {
      for (const c of cols) { prev[c] = row[c]; row[c] = null; }
      _replaceTileInDom(id);
    }
    showMsg('Removing…');
    try {
      const r = await fetch('/api/favourites/unmatch', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id, source }),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(d.error || 'Failed');
      showMsg('Updated');
      await _refreshOneRow(id);
      if (_entityPanelRowId === id) openEntityDetail(id);
    } catch (e) {
      if (row) { for (const c of cols) row[c] = prev[c]; _replaceTileInDom(id); }
      window.toast(e.message || 'Failed');
    } finally {
      _releaseInflight(id, `unmatch:${source}`);
    }
  }

  async function refreshOne(id, onlyMissing) {
    if (!_claimInflight(id, 'refresh')) return;
    const miss = !!onlyMissing;
    showMsg(miss ? 'Searching missing…' : 'Refreshing…');
    try {
      const r = await fetch('/api/favourites/refresh', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          id,
          scrape_aliases: !miss,
          only_missing: miss,
        }),
      });
      const d = await r.json();
      if (!r.ok) { window.toast(d.error || 'Refresh failed'); return; }
      if (d.skipped) { showMsg('Matches are locked'); return; }
      showMsg('Updated');
      // Refresh scrapes external APIs and rewrites match fields — re-pull
      // just this row instead of the full /api/favourites payload.
      const ok = await _refreshOneRow(id);
      if (!ok) load();
      if (_entityPanelRowId === id) openEntityDetail(id);
    } finally {
      _releaseInflight(id, 'refresh');
    }
  }

  async function startFavouritesScan(toolbarMsg) {
    setIndexBusy(true);
    showMsg(toolbarMsg || 'Scanning…');
    try {
      const r = await fetch('/api/favourites/scan?prune_missing=false&only_missing=true', {
        method: 'POST',
        credentials: 'same-origin',
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) {
        window.toast(d.error || d.detail || 'Scan failed');
        setIndexBusy(false);
        return;
      }
      startIndexPolling();
    } catch (e) {
      window.toast(e.message || 'Scan failed');
      setIndexBusy(false);
    }
  }

  async function scanLibrary() {
    await startFavouritesScan('Scanning library for new folders…');
  }

  async function searchMissing() {
    await startFavouritesScan('Search missing started');
  }

  async function refreshAll() {
    setIndexBusy(true);
    showMsg('Re-matching all…');
    await fetch('/api/favourites/refresh-all', { method: 'POST', credentials: 'same-origin' });
    startIndexPolling();
  }

  async function refreshImagesAll() {
    setIndexBusy(true);
    showMsg('Refreshing images…');
    await fetch('/api/favourites/refresh-images-all', { method: 'POST', credentials: 'same-origin' });
    startIndexPolling();
  }

  async function lockAllMatches() {
    showMsg('Locking all…');
    try {
      const r = await fetch('/api/favourites/lock-all', { method: 'POST', credentials: 'same-origin' });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) { window.toast(d.error || 'Failed'); return; }
      showMsg(typeof d.updated === 'number' ? `Locked ${d.updated} folders` : 'Locked');
      await load();
      if (_entityPanelRowId != null) await openEntityDetail(_entityPanelRowId);
    } catch (e) {
      window.toast(e.message || 'Failed');
    }
  }

  async function unlockAllMatches() {
    showMsg('Unlocking all…');
    try {
      const r = await fetch('/api/favourites/unlock-all', { method: 'POST', credentials: 'same-origin' });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) { window.toast(d.error || 'Failed'); return; }
      showMsg(typeof d.updated === 'number' ? `Unlocked ${d.updated} folders` : 'Unlocked');
      await load();
      if (_entityPanelRowId != null) await openEntityDetail(_entityPanelRowId);
    } catch (e) {
      window.toast(e.message || 'Failed');
    }
  }

  const _EXT_LINK_MODAL_SOURCES = new Set(['TMDB', 'IAFD', 'Freeones', 'Babepedia', 'Coomer', 'JAV Database']);

  function openSearchForRow(rowId, focusSource) {
    const row = [..._rowsPerf, ..._rowsStudio, ..._rowsMovie].find(r => r.id === rowId);
    if (!row) return;
    closeAllFavMenus();
    if (row.kind === 'performer' && focusSource && _EXT_LINK_MODAL_SOURCES.has(focusSource)
        && typeof openExtLinkModal === 'function') {
      openExtLinkModal(rowId, focusSource, row.folder_name || '');
      return;
    }
    _searchRowId = rowId;
    // Normalize JAV rows to 'movie' search-kind so the movie pathway
    // (TMDB + TPDB + JAVStash) runs. Backend still stores the match
    // against the row's real kind in favourite_entities.
    _searchKind = (row.kind === 'jav') ? 'movie' : row.kind;
    _movieSearchTmdbError = '';
    const yearRow = document.getElementById('searchYearRow');
    const yearIn = document.getElementById('searchYearInput');
    const qIn = document.getElementById('searchInput');
    if (row.kind === 'movie' || row.kind === 'jav') {
      const allowedFocus = (focusSource === 'TMDB' || focusSource === 'TPDB' || focusSource === 'JAVSTASH');
      _searchSourceFilter = allowedFocus ? focusSource : null;
      _searchGenderFilters = null;
      _lastSearchResults = [];
      document.getElementById('searchModalTitle').textContent = row.kind === 'jav' ? 'Link JAV release' : 'Link movie';
      qIn.placeholder = row.kind === 'jav' ? 'Release title or code…' : 'Movie title…';
      qIn.value = row.folder_name || '';
      if (yearRow) yearRow.style.display = '';
      if (yearIn) {
        const y = (row.folder_name || '').match(/\b(19\d{2}|20\d{2})\b/);
        yearIn.value = y ? y[1] : '';
      }
    } else {
      if (yearRow) yearRow.style.display = 'none';
      if (yearIn) yearIn.value = '';
      _searchSourceFilter = focusSource || null;
      _searchGenderFilters = (row.kind === 'performer' && Array.isArray(row.gender_filters) && row.gender_filters.length)
        ? row.gender_filters.slice()
        : null;
      _lastSearchResults = [];
      document.getElementById('searchModalTitle').textContent = row.kind === 'studio' ? 'Link studio' : 'Link star';
      qIn.placeholder = 'Search TPDB, StashDB, FansDB, JAVStash…';
      qIn.value = row.folder_name || '';
    }
    document.getElementById('searchResults').innerHTML = '';
    renderSearchFilterChips();
    document.getElementById('searchModal').classList.add('open');
    setTimeout(() => qIn.focus(), 80);
    runManualSearch();
  }

  function closeSearch() {
    document.getElementById('searchModal').classList.remove('open');
    _searchRowId = null;
    _lastSearchResults = [];
    _movieSearchTmdbError = '';
    const yearRow = document.getElementById('searchYearRow');
    if (yearRow) yearRow.style.display = 'none';
    const yearIn = document.getElementById('searchYearInput');
    if (yearIn) yearIn.value = '';
  }

  function openImagePickModal(rowId) {
    const row = [..._rowsPerf, ..._rowsStudio].find(r => r.id === rowId);
    if (!row) return;
    closeAllFavMenus();
    _imagePickRowId = rowId;
    _imagePickKind = row.kind || 'performer';
    _imagePickItems = [];
    _imagePickSelectedUrl = null;
    document.getElementById('imagePickTitle').textContent =
      (_imagePickKind === 'studio' ? 'Pick studio image — ' : 'Pick performer image — ') +
      displayFolderName(row.folder_name);
    document.getElementById('imagePickSaveLabel').textContent = _imagePickKind === 'studio'
      ? 'Save to this folder as logo.png (overwrites if present)'
      : 'Save to this folder as poster.jpg (overwrites if present)';
    document.getElementById('imagePickSaveLocal').checked = false;
    document.getElementById('imagePickResults').innerHTML = '';
    document.getElementById('imagePickApplyBtn').disabled = true;
    document.getElementById('imagePickModal').classList.add('open');
    loadImagePickCandidates();
  }

  function closeImagePickModal() {
    document.getElementById('imagePickModal').classList.remove('open');
    _imagePickRowId = null;
    _imagePickItems = [];
    _imagePickSelectedUrl = null;
  }

  function renderImagePickResults() {
    const box = document.getElementById('imagePickResults');
    const list = _imagePickItems || [];
    const kindClass = _imagePickKind === 'studio' ? 'fav-imgpick-tile--studio' : 'fav-imgpick-tile--performer';
    const fallback = _imagePickKind === 'studio' ? STUDIO_POSTER_FALLBACK : POSTER_FALLBACK;
    if (!list.length) {
      box.innerHTML = '<div class="empty-tile" style="padding:20px">No images from linked profiles. Add a TPDB, StashDB, FansDB, or JAVStash link first (card pills or detail panel).</div>';
      return;
    }
    box.innerHTML = `<div class="fav-imgpick-grid">${list.map((x, i) => {
      const sel = x.image === _imagePickSelectedUrl ? ' is-selected' : '';
      const img = x.image
        ? `<img src="${escAttr(x.image)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.onerror=null;this.src='${fallback}'">`
        : `<img src="${fallback}" alt="" loading="lazy">`;
      return `<button type="button" class="fav-imgpick-tile ${kindClass}${sel}" data-idx="${i}" title="${escAttr((x.name || '') + ' — ' + (x.source || ''))}">
        ${img}
        <span class="fav-imgpick-badge">${esc(x.source || '')}</span>
      </button>`;
    }).join('')}</div>`;
  }

  async function loadImagePickCandidates() {
    if (!_imagePickRowId) return;
    const box = document.getElementById('imagePickResults');
    box.innerHTML = '<div class="empty-tile" style="padding:20px">Loading linked images…</div>';
    _imagePickSelectedUrl = null;
    document.getElementById('imagePickApplyBtn').disabled = true;
    try {
      const r = await fetch(
        '/api/favourites/image-search?row_id=' + encodeURIComponent(_imagePickRowId),
        { credentials: 'same-origin' },
      );
      const d = await r.json().catch(() => ({}));
      if (!r.ok) {
        box.innerHTML = `<div class="empty-tile" style="padding:20px;color:var(--red)">${esc(typeof d.error === 'string' ? d.error : 'Load failed')}</div>`;
        _imagePickItems = [];
        return;
      }
      _imagePickItems = d.items || [];
      if (d.kind) _imagePickKind = d.kind;
      renderImagePickResults();
    } catch (e) {
      box.innerHTML = `<div class="empty-tile" style="padding:20px;color:var(--red)">${esc(e.message)}</div>`;
      _imagePickItems = [];
    }
  }


  async function applyPickedImage() {
    if (!_imagePickRowId || !_imagePickSelectedUrl) return;
    const saveLocal = document.getElementById('imagePickSaveLocal').checked;
    const btn = document.getElementById('imagePickApplyBtn');
    btn.disabled = true;
    showMsg('Applying…');
    try {
      const r = await fetch('/api/favourites/apply-image', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          row_id: _imagePickRowId,
          image_url: _imagePickSelectedUrl,
          save_local: saveLocal,
        }),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) {
        window.toast(typeof d.error === 'string' ? d.error : 'Could not apply image');
        btn.disabled = false;
        return;
      }
      const reopenId = document.getElementById('entityPanelModal').classList.contains('open') ? _entityPanelRowId : null;
      const targetRowId = _imagePickRowId;
      closeImagePickModal();
      showMsg(saveLocal ? 'Image saved and applied' : 'Image applied');
      // Targeted tile refresh instead of a full `load()` rebuild — the
      // performer-thumb URL is stable (constructed server-side from the
      // row id), so a cache-buster on the existing IMG element is all
      // we need to swap in the new artwork.
      _refreshLibraryImagesForRow(targetRowId);
      if (window._performerPopupActiveId === targetRowId && typeof window.refreshPerformerPopup === 'function') {
        window.refreshPerformerPopup();
      }
      if (reopenId) await openEntityDetail(reopenId);
    } catch (e) {
      window.toast(e.message || 'Failed');
      btn.disabled = false;
    }
  }

  async function runManualSearch() {
    const q = document.getElementById('searchInput').value.trim();
    const box = document.getElementById('searchResults');
    _lastSearchQuery = q;  // shared with the renderers below for word-match highlighting
    if (!q) {
      box.innerHTML = '<div class="empty-tile" style="padding:20px">Enter a search term.</div>';
      _lastSearchResults = [];
      _movieSearchTmdbError = '';
      return;
    }
    if (_searchKind === 'movie') {
      const yearEl = document.getElementById('searchYearInput');
      const year = yearEl ? yearEl.value.trim() : '';
      box.innerHTML = '<div class="empty-tile" style="padding:20px">Searching…</div>';
      _movieSearchTmdbError = '';
      try {
        const [resTmdb, resTpdb] = await Promise.all([
          fetch('/api/movies/tmdb-search?q=' + encodeURIComponent(q) + (year ? '&year=' + encodeURIComponent(year) : ''), { credentials: 'same-origin' }),
          fetch('/api/movies/search?q=' + encodeURIComponent(q) + (year ? '&year=' + encodeURIComponent(year) : '') + '&page=1', { credentials: 'same-origin' }),
        ]);
        const [rt, rp] = await Promise.all([resTmdb.json(), resTpdb.json()]);
        if (rt.error) _movieSearchTmdbError = String(rt.error);
        if (!resTpdb.ok) {
          const err = rp && (rp.error || rp.detail);
          box.innerHTML = `<div class="empty-tile" style="padding:20px;color:var(--red)">${esc(typeof err === 'string' ? err : 'TPDB search failed')}</div>`;
          _lastSearchResults = [];
          renderSearchFilterChips();
          return;
        }
        const merged = [];
        (rt.results || []).slice(0, 15).forEach(m => {
          merged.push({
            source: 'TMDB',
            id: String(m.id),
            name: m.title || '',
            image: m.poster_url || '',
            year: m.year || '',
          });
        });
        (rp.results || []).slice(0, 15).forEach(m => {
          merged.push({
            source: 'TPDB',
            id: String(m.id),
            name: m.title || '',
            image: m.poster || '',
            year: m.date || '',
          });
        });
        // JAVStash scenes ride along on /api/movies/search via `jav_scenes`.
        // Map to the same {source,id,name,image,year} shape the result
        // renderer expects so the JAVSTASH chip/filter and pickSearch flow
        // both work without bespoke branches.
        (rp.jav_scenes || []).slice(0, 15).forEach(m => {
          merged.push({
            source: 'JAVSTASH',
            id: String(m.id || m.javstash_id || ''),
            name: m.title || m.name || '',
            image: m.poster || m.image || '',
            year: (m.date || '').slice(0, 4),
          });
        });
        _lastSearchResults = merged;
        renderSearchFilterChips();
        renderSearchResultsList();
      } catch (e) {
        box.innerHTML = `<div class="empty-tile" style="padding:20px;color:var(--red)">${esc(e.message)}</div>`;
        _lastSearchResults = [];
      }
      return;
    }
    box.innerHTML = '<div class="empty-tile" style="padding:20px">Searching…</div>';
    const type = _searchKind === 'studio' ? 'studio' : 'performer';
    let searchUrl = '/api/metadata/search?q=' + encodeURIComponent(q) + '&type=' + encodeURIComponent(type);
    if (type === 'performer') {
      // strict=0 — same as /discover: APIs match aliases; strict=true dropped those hits.
      searchUrl += '&strict=0';
      if (_searchGenderFilters && _searchGenderFilters.length) {
        searchUrl += '&genders=' + encodeURIComponent(_searchGenderFilters.join(','));
      }
    }
    try {
      const r = await fetch(searchUrl, { credentials: 'same-origin' });
      const raw = await r.text();
      let d;
      try { d = JSON.parse(raw); } catch (_) {
        box.innerHTML = `<div class="empty-tile" style="padding:20px">Invalid response</div>`;
        return;
      }
      if (!r.ok) {
        const err = d.error || d.detail || ('HTTP ' + r.status);
        box.innerHTML = `<div class="empty-tile" style="padding:20px;color:var(--red)">${esc(typeof err === 'string' ? err : JSON.stringify(err))}</div>`;
        return;
      }
      _lastSearchResults = d.results || [];
      renderSearchFilterChips();
      renderSearchResultsList();
    } catch (e) {
      box.innerHTML = `<div class="empty-tile" style="padding:20px;color:var(--red)">${esc(e.message)}</div>`;
    }
  }

  async function pickSearch(el) {
    if (!_searchRowId) return;
    const source = el.getAttribute('data-source');
    const id = el.getAttribute('data-id');
    const name = el.getAttribute('data-name');
    const image = el.getAttribute('data-image');
    const body = { row_id: _searchRowId, source, external_id: id, name };
    if (image) body.image = image;
    const r = await fetch('/api/favourites/match', {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) {
      window.toast(d.error || 'Could not save match');
      return;
    }
    const reopenId = document.getElementById('entityPanelModal').classList.contains('open') ? _entityPanelRowId : null;
    const popupRow = _searchRowId;
    const popupOpen = window._performerPopupActiveId === popupRow;
    closeSearch();
    const ok = await _refreshOneRow(popupRow);
    if (!ok) await load();
    if (reopenId) await openEntityDetail(reopenId);
    // If the bio popup is the surface that launched this search, refresh
    // it so the user sees the updated link chip without having to close
    // and reopen the popup. /library's own panels are already covered by
    // the load() + openEntityDetail() pair above.
    if (popupOpen && typeof window.refreshPerformerPopup === 'function') {
      window.refreshPerformerPopup();
    }
  }

  document.getElementById('searchModal').addEventListener('click', e => {
    if (e.target === e.currentTarget) closeSearch();
  });
  document.getElementById('imagePickModal').addEventListener('click', (e) => {
    if (e.target === e.currentTarget) closeImagePickModal();
    const tile = e.target.closest('.fav-imgpick-tile');
    const resultsEl = document.getElementById('imagePickResults');
    if (!tile || !resultsEl || !resultsEl.contains(tile)) return;
    const idx = parseInt(tile.getAttribute('data-idx'), 10);
    const x = _imagePickItems[idx];
    if (!x || !x.image) return;
    _imagePickSelectedUrl = x.image;
    document.getElementById('imagePickApplyBtn').disabled = false;
    resultsEl.querySelectorAll('.fav-imgpick-tile.is-selected').forEach(t => t.classList.remove('is-selected'));
    tile.classList.add('is-selected');
  });
  document.getElementById('searchInput').addEventListener('keydown', e => {
    if (e.key === 'Enter') runManualSearch();
  });
  const searchYearInputEl = document.getElementById('searchYearInput');
  if (searchYearInputEl) {
    searchYearInputEl.addEventListener('keydown', e => {
      if (e.key === 'Enter') runManualSearch();
    });
  }
  document.getElementById('searchResults').addEventListener('keydown', e => {
    if (e.key !== 'Enter') return;
    const row = e.target.closest('.sr-item');
    if (!row) return;
    e.preventDefault();
    pickSearch(row);
  });

  // Initial paint pulls only the active tab's data via the per-kind
  // endpoint. Other tabs fetch lazily on first switch. The previous
  // full `load()` call returned all 5 kinds (often 2-5 MB) before
  // any pixel hit the screen.
  // ?focus=<row_id> still opens the universal popup on that row;
  // ?manage=images drops into the poster picker.
  const _initParams = new URLSearchParams(window.location.search);
  const _focusId = parseInt(_initParams.get('focus') || '0', 10);
  const _manage  = _initParams.get('manage') || '';
  // Library cards are cached in module-scoped arrays (_rowsPerf etc.).
  // When LibEntityActions performs an in-place mutation (currently
  // just rename — remove/delete already trigger reloads via their own
  // close paths), the cards on /library don't know to refresh. Listen
  // for the dispatched event and refetch the single mutated row, or
  // fall through to a full load() when the row endpoint can't resolve.
  document.addEventListener('lib-entity-changed', async (e) => {
    const detail = (e && e.detail) || {};
    const rid = detail.id ? parseInt(detail.id, 10) : 0;
    if (rid) {
      const ok = await _refreshOneRow(rid);
      if (ok) return;
    }
    try { await load(); } catch (_) {}
  });
  // ?focus= deep link: open the popup ASAP and load the grid behind
  // it. Previously fell back to a full `load()` which fetches every
  // kind (often 2-5 MB) before the popup paints — now we hit the
  // single-row endpoint, open the popup against that row, switch the
  // visible tab to match row.kind, and run loadActiveView() in the
  // background to paint the grid. Performer popup short-circuit kept
  // because it's the most common case and the popup itself doesn't
  // need the grid in memory.
  const _LIB_KIND_TO_VIEW = {
    performer: 'performer',
    studio:    'studio',
    movie:     'movie',
    jav:       'movie',
    vice:      'vice',
  };
  async function _focusBoot() {
    let row = null;
    try {
      const r = await fetch('/api/favourites/row?id=' + encodeURIComponent(_focusId), { credentials: 'same-origin' });
      if (r.ok) {
        const d = await r.json();
        row = d && d.row;
      }
    } catch (_) { /* fall through to full load */ }
    if (!row) {
      // Row endpoint couldn't resolve — fall back to the full reload
      // so the focus succeeds against the cached arrays.
      try { await load(); } catch (_) {}
      return;
    }
    // Switch the visible tab so when the user closes the popup, the
    // grid behind it shows their kind. setView() kicks the per-kind
    // load if it isn't cached yet.
    const targetView = _LIB_KIND_TO_VIEW[row.kind] || 'performer';
    if (typeof setView === 'function' && _view !== targetView) {
      try { setView(targetView); } catch (_) { loadActiveView(); }
    } else {
      loadActiveView();
    }
    // Open the popup immediately. openPerformerPopup handles
    // performer rows; studio rows route to openStudioPopup. Other
    // kinds (movie/jav/vice) currently fall through to the entity
    // panel rendered by setView → render(), which is fine.
    if (_manage === 'images' && row.kind === 'performer' && typeof openPosterRolePicker === 'function') {
      openPosterRolePicker(_focusId);
    } else if (row.kind === 'performer' && typeof window.openPerformerPopup === 'function') {
      window.openPerformerPopup({ libraryRowId: _focusId });
    } else if (row.kind === 'studio' && typeof window.openStudioPopup === 'function') {
      window.openStudioPopup({ libraryRowId: _focusId, name: row.display_name || row.folder_name });
    }
  }
  if (_focusId) {
    _focusBoot();
  } else {
    loadActiveView();
  }
