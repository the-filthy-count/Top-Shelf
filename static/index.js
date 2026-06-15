/* Externalized from index.html. */

  const TS_DL_PAGE = 'index';
const DL_SEARCH_MODE_KEY = 'ts_downloads_search_mode';
  let _dlProwlarrResults = [];

  // RSS-tile click handler. Opens the standard Prowlarr-search popup
  // used everywhere else in the app, but pre-loaded with the releases
  // the backend's RSS aggregator already matched for this tile — so
  // no re-search runs and indexer tokenisation can't drop the result.
  // Falls back to the live search when no pre-fetched releases exist.
  // `ev` is optional; when present we skip clicks that originated on
  // an interactive descendant (medallion headshot link, carousel
  // arrow, fav heart) so those keep their own behaviour.
  function openRssTilePopup(tileIdx, ev) {
    if (ev && ev.target && ev.target.closest && ev.target.closest('[data-performer-link], button, a')) {
      return;
    }
    const res = _dlProwlarrResultsFiltered[tileIdx];
    if (!res) return;
    const cleanTitle = res.clean_title || cleanRssTitle(res.title || '');
    const matched = res.matched_rows || [];
    const studioRow = matched.find(m => m && m.kind === 'studio');
    const studio = (studioRow && studioRow.folder_name) || res.studio || '';
    const perfRows = matched.filter(m => m && m.kind === 'performer');
    const performers = perfRows.map(p => p.folder_name || '').filter(Boolean).join(', ');
    // The tile already has `res.releases[]` from the backend's RSS
    // aggregator — pass it straight through so the popup renders the
    // known-good matches instead of re-querying indexers with a long
    // descriptive title that almost never tokenises cleanly.
    const preloaded = Array.isArray(res.releases) && res.releases.length
      ? res.releases
      : (res.guid || res.download_url || res.magnet ? [res] : null);
    if (typeof window.openProwlarrSearchPopup === 'function') {
      window.openProwlarrSearchPopup({
        title: cleanTitle,
        studio,
        performers,
        kind: 'scene',
        preloadedResults: preloaded,
      });
    }
  }

  function getDlSearchMode() {
    const _indexModes = ['prowlarr', 'studios', 'wanted', 'rss', 'favourites'];
    try {
      const view = new URLSearchParams(location.search).get('view');
      if (view && _indexModes.includes(view)) return view;
    } catch (_) {}
    const v = localStorage.getItem(DL_SEARCH_MODE_KEY);
    if (v && _indexModes.includes(v)) return v;
    return 'favourites';
  }

  const _INDEX_MODE_TITLES = {
    favourites: 'Latest Favourite Downloads',
    rss: 'Latest Performer Downloads',
    studios: 'Latest Studio Downloads',
    wanted: 'Wanted Scenes and Movies',
    prowlarr: 'Search Available Downloads',
  };

  function applyDlSearchModeUi() {
    const mode = getDlSearchMode();
    const titleText = document.getElementById('indexPageTitleText');
    if (titleText) titleText.textContent = _INDEX_MODE_TITLES[mode] || 'Feeds';
    const isPoster = mode === 'rss' || mode === 'studios' || mode === 'favourites';
    const search = mode === 'prowlarr';
    const studios = mode === 'studios';
    const favourites = mode === 'favourites';
    const wanted = mode === 'wanted';
    const elP = document.getElementById('dlSearchModeProwlarr');
    const elR = document.getElementById('dlSearchModeRss');
    const elW = document.getElementById('dlSearchModeWanted');
    const bP = document.getElementById('dlModeProwlarr');
    const bR = document.getElementById('dlModeRss');
    const bSt = document.getElementById('dlModeStudios');
    const bF = document.getElementById('dlModeFavourites');
    const bW = document.getElementById('dlModeWanted');
    const hdrCtrl = document.getElementById('dlRssHeaderControls');
    const bodyWrap = document.getElementById('dlProwlarrBodyWrap');
    if (elP) elP.style.display = search ? '' : 'none';
    if (elR) elR.style.display = (mode === 'rss') ? '' : 'none';
    if (elW) elW.style.display = wanted ? 'flex' : 'none';
    if (bP) bP.classList.toggle('active', search);
    if (bR) bR.classList.toggle('active', mode === 'rss');
    if (bSt) bSt.classList.toggle('active', studios);
    if (bF) bF.classList.toggle('active', favourites);
    if (bW) bW.classList.toggle('active', wanted);
    if (hdrCtrl) hdrCtrl.style.visibility = isPoster ? 'visible' : 'hidden';
    // The prowlarr body holds the "Enter a query and search." results
    // grid for the Search / Favourites / Performers / Studios tabs. On
    // Wanted (and any other non-results mode) it has nothing to show —
    // previously it was always set to `display:flex`, which left the
    // empty-state message stacked above the Wanted panel.
    if (bodyWrap) {
      const showResults = isPoster || search;
      if (showResults) {
        bodyWrap.style.display = 'flex';
        bodyWrap.style.flexDirection = 'column';
        bodyWrap.style.flex = '1';
        bodyWrap.style.minHeight = '0';
        bodyWrap.style.padding = '0';
        const inner = document.getElementById('dlProwlarrResults');
        if (inner) {
          inner.className = studios ? 'rss-poster-grid rss-poster-grid--studio' : (isPoster ? 'rss-poster-grid' : 'dl-prowlarr-results');
        }
      } else {
        bodyWrap.style.display = 'none';
      }
    }
  }

  function setDlSearchMode(mode) {
    localStorage.setItem(DL_SEARCH_MODE_KEY, mode);
    applyDlSearchModeUi();
    if (mode === 'wanted') {
      loadWantedPanel();
    } else if (mode === 'favourites' || mode === 'rss' || mode === 'studios') {
      // Paint from cache immediately so tab switches don't flash the
      // wrong tile layout while a slower fetch is still in flight.
      _dlRssPaintFromCache(mode);
      loadDlRssFeed(mode);
    } else {
      const el = document.getElementById('dlProwlarrResults');
      if (el) { el.innerHTML = '<div class="empty" style="padding:16px">Enter a query and search.</div>'; }
      _dlProwlarrResults = [];
    }
  }

  function cleanRssTitle(raw) {
    let t = raw || '';
    // Dots as word separators (not decimal points)
    t = t.replace(/\.(?=[^\d])|(?<=[^\d])\./g, ' ');
    // Bracketed/parenthesised dates e.g. [24.07.2025] (26.03.20)
    t = t.replace(/[\[\(]\d{2}[.\-]\d{2}[.\-]\d{2,4}[\]\)]/g, ' ');
    // Bare dates e.g. 25.07.24 26.04.05
    t = t.replace(/\b\d{2}[.\-]\d{2}[.\-]\d{2,4}\b/g, ' ');
    // Resolution tokens
    t = t.replace(/\b\d{3,4}[piP]\b/g, ' ');
    // Bracketed release tags e.g. [XC] [XvX]
    t = t.replace(/\[XC\]|\[XvX\]/gi, ' ');
    // Codec / container / release-group tokens
    t = t.replace(/\b(x264|x265|h264|h265|hevc|avc|av1|xvid|divx|mp4|mkv|avi|wmv|mov|web|webrip|web-?dl|bluray|blu-?ray|bdrip|dvdrip|hdtv|hdrip|p2p|wrb|nbq|ntr|cpg|rarbg|yify|fgt|ntb|playxd|eztv|ipt.?team|ktr|p0rnfuscated|pornfuscated|lulustream|fuckingsession|xleech|gapfill|nogroup|fetish|prt|hq)\b/gi, ' ');
    // Standalone XXX
    t = t.replace(/(?<![A-Za-z])XXX(?![A-Za-z])/g, ' ');
    // Dash-separated noise e.g. " - No Tapping" prefix separators
    t = t.replace(/\s[-\u2013]+\s/g, ' ');
    // Strip trailing/leading punctuation left by token removal
    // Remove isolated trailing dots left by date stripping
    t = t.replace(/(?<=\w)\.(?=\s|$)/g, ' ');
    t = t.replace(/^[\s\-\[\]().]+|[\s\-\[\]().]+$/g, '');
    return t.replace(/\s{2,}/g, ' ').trim();
  }

  const _rssCarouselState = {};
  let _dlProwlarrResultsFiltered = [];

  function rssCarouselImg(tileEl, delta, mode) {
    const tileIdx = parseInt(tileEl.dataset.tileIdx, 10);
    const res = _dlProwlarrResultsFiltered[tileIdx];
    if (!res) return;
    const isStudios = (mode || getDlSearchMode()) === 'studios';
    const allMatches = res.matched_rows || [];
    const matches = isStudios
      ? allMatches.filter(m => m.kind === 'studio')
      : allMatches.filter(m => m.kind === 'performer');
    if (matches.length < 2) return;
    const cur = _rssCarouselState[tileIdx] || 0;
    const next = (cur + delta + matches.length) % matches.length;
    _rssCarouselState[tileIdx] = next;
    const m = matches[next];

    // Primary swap: poster + match label + tile title name.
    const imgUrl = m.kind === 'studio'
      ? `/api/favourites/studio-thumb?row_id=${m.id}`
      : `/api/favourites/performer-thumb?prefer=secondary&row_id=${m.id}`;
    const poster = tileEl.querySelector('.rss-tile-poster');
    if (poster) { poster.src = imgUrl; poster.style.display = ''; }
    const noImg = tileEl.querySelector('.rss-tile-no-img');
    if (noImg) noImg.style.display = 'none';
    const nameEl = tileEl.querySelector('.rss-tile-match-name');
    if (nameEl) nameEl.textContent = m.folder_name;
    const labelEl = tileEl.querySelector('.rss-tile-name');
    if (labelEl) labelEl.textContent = m.folder_name;

    // Studio tiles have no medallion trio — done.
    if (isStudios) return;

    // Performer-mode: repoint all three medallions (center + right + left)
    // to the new carousel window so the whole trio rotates together. With
    // 4+ matches this effectively "advances" which three faces are visible.
    const n = matches.length;
    const cap = Math.min(3, n);
    const slots = [];
    for (let i = 0; i < cap; i++) slots.push(matches[(next + i) % n]);
    const [center, right, left] = slots;
    function setMedallion(selector, row) {
      const el = tileEl.querySelector(selector);
      if (!el) return;
      if (row) {
        el.src = `/api/favourites/performer-thumb?row_id=${row.id}`;
        el.alt = row.folder_name || '';
        el.style.display = '';
        // Refresh popup link attributes so the rotated face matches the
        // current performer (gender gate may differ).
        const allowed = window.performerLinkAllowed ? window.performerLinkAllowed(row.gender) : true;
        if (allowed) {
          el.setAttribute('data-performer-link', '');
          el.setAttribute('data-name', row.folder_name || '');
          el.setAttribute('data-library-row-id', String(row.id));
        } else {
          el.removeAttribute('data-performer-link');
          el.removeAttribute('data-name');
          el.removeAttribute('data-library-row-id');
        }
      } else {
        // Fewer matches than slots — hide the unused medallion element.
        el.style.display = 'none';
      }
    }
    setMedallion('.rss-tile-headshot-medallion',          center);
    setMedallion('.rss-tile-headshot-medallion--right',   right);
    setMedallion('.rss-tile-headshot-medallion--left',    left);
  }

  function renderRssTiles(results, mode) {
    mode = mode || getDlSearchMode();
    const isStudios    = mode === 'studios';
    const isFavourites = mode === 'favourites';
    const filterRaw = (document.getElementById('dlRssFilter')?.value || '').toLowerCase().trim();
    // Normalise dots to spaces so 'sasha grey' matches 'sasha.grey'
    const filter = filterRaw.replace(/\./g, ' ');
    // Filter to only items whose matches are the right kind. Favourites
    // mode uses the same layout as the Performers tab but narrows the
    // list to RSS items where at least one matched library row is
    // flagged `is_favourite`. Items matching a favourite studio with
    // zero matched performers are still included (the tile renders with
    // an empty medallion slot and the studio's own poster / logo).
    let list = results.filter(res => {
      const matches = res.matched_rows || [];
      if (isFavourites) return matches.some(m => m && m.is_favourite);
      if (isStudios)    return matches.some(m => m.kind === 'studio');
      return matches.some(m => m.kind === 'performer') || matches.length === 0;
    });
    if (filter) list = list.filter(r => (r.title || '').toLowerCase().replace(/\./g, ' ').includes(filter));
    _dlProwlarrResultsFiltered = list;
    const el = document.getElementById('dlProwlarrResults');
    if (!el) return;
    if (!list.length) {
      const emptyMsg = isFavourites
        ? (filter
            ? 'No favourited matches for this filter.'
            : 'No RSS results match any of your favourited library entries yet. Star performers / studios in /library, then come back here.')
        : 'No results match the filter.';
      el.innerHTML = `<div class="empty" style="padding:32px">${esc(emptyMsg)}</div>`;
      return;
    }
    el.innerHTML = list.map((res, tileIdx) => {
      const realIdx = _dlProwlarrResults.indexOf(res);
      const isTor = res.type === 'torrent';
      const cleanTitle = res.clean_title || cleanRssTitle(res.title || '');
      const agePart = res.age != null ? Math.round(res.age / 24) + 'd' : '';
      const sizePart = res.size_mb ? (res.size_mb > 1024 ? (res.size_mb/1024).toFixed(1) + ' GB' : Math.round(res.size_mb) + ' MB') : '';
      const meta = [agePart, sizePart].filter(Boolean).join(' \u00b7 ');
      const allMatches = res.matched_rows || [];
      // Full match pools — kept separate so studio tiles can still show
      // library-performer headshots in their top-left corner even though
      // the tile itself is a studio match.
      // Favourites mode narrows each pool to just the favourited rows —
      // the tile shouldn't advertise non-favourited co-matches when the
      // whole point of the tab is "things I starred".
      const perfMatchesAllRaw   = allMatches.filter(m => m.kind === 'performer');
      const studioMatchesAllRaw = allMatches.filter(m => m.kind === 'studio');
      const perfMatchesAll   = isFavourites ? perfMatchesAllRaw.filter(m => m.is_favourite)   : perfMatchesAllRaw;
      const studioMatchesAll = isFavourites ? studioMatchesAllRaw.filter(m => m.is_favourite) : studioMatchesAllRaw;
      const matches = isStudios ? studioMatchesAll : perfMatchesAll;
      const fallbackMatches = matches.length ? matches : allMatches;
      const carouselIdx = _rssCarouselState[tileIdx] || 0;
      const firstMatch = fallbackMatches[carouselIdx] || fallbackMatches[0] || null;
      const currentName = firstMatch ? firstMatch.folder_name : '';

      // Poster / logo
      // Favourites tab: when a tile has ONLY studio matches (no
      // favourited performer matches to pull a scene-style poster
      // from), serve the cached studio poster from the metadata
      // dir (`/api/favourites/studio-poster?row_id=…`). That endpoint
      // lazy-imports from the studio folder's `poster.jpg` /
      // `folder.jpg` on first access, saving an optimised copy into
      // `metadata/studios/{id}/{id}_poster.jpg`, then streams the
      // cached copy on every subsequent request — same pattern the
      // performer secondary posters use. On 404 the onerror chain
      // swaps to `/static/img/poster.webp` as the final fallback.
      let posterHtml = '';
      const favNoPerfMatch = isFavourites && !perfMatchesAll.length && studioMatchesAll.length;
      if (favNoPerfMatch) {
        const studioRow = studioMatchesAll[0];
        const studioPosterUrl = `/api/favourites/studio-poster?row_id=${studioRow.id}`;
        posterHtml = `<img class="rss-tile-poster" src="${studioPosterUrl}" alt="" loading="lazy" crossorigin="anonymous" onload="autoLevelPosterImage(this)" onerror="this.onerror=null;this.src='/static/img/poster.webp';this.removeAttribute('crossorigin');">`;
      } else if (firstMatch) {
        const imgUrl = firstMatch.kind === 'studio'
          ? `/api/favourites/studio-thumb?row_id=${firstMatch.id}`
          : `/api/favourites/performer-thumb?prefer=secondary&row_id=${firstMatch.id}`;
        posterHtml = `<img class="rss-tile-poster" src="${imgUrl}" alt="" loading="lazy" crossorigin="anonymous" onload="autoLevelPosterImage(this)" onerror="this.style.display='none'">`;
      } else {
        posterHtml = `<div class="rss-tile-no-img"><i class="fa-solid fa-${isStudios ? 'building' : 'person'}"></i></div>`;
      }

      // Glass layer + overlay (both sit above poster)
      const glassHtml = '<div class="rss-tile-duo-tint" aria-hidden="true"></div><div class="rss-tile-glass"></div>';

      // Carousel arrows: any tile with ≥2 matches (performer medallions
      // rotate in performer mode; studio poster + name rotate in studio
      // mode, unchanged).
      const carousel = (matches.length >= 2) ? `
        <div class="rss-tile-carousel">
          <button class="rss-tile-carousel-btn" onclick="event.stopPropagation();rssCarouselImg(this.closest('.rss-tile'),-1,'${mode}')" title="Previous"><i class="fa-solid fa-chevron-left"></i></button>
          <button class="rss-tile-carousel-btn" onclick="event.stopPropagation();rssCarouselImg(this.closest('.rss-tile'),1,'${mode}')" title="Next"><i class="fa-solid fa-chevron-right"></i></button>
        </div>` : '';

      // Name label below poster (mirrors fav-cell-title) — plain text
      // for both performer and studio tiles.
      const nameLabel = currentName
        ? `<div class="rss-tile-name">${esc(currentName)}</div>`
        : '';

      // Favourite-match heart: any matched row flagged as favourite.
      // Suppressed in the Favourites tab because every tile there is
      // by definition a favourited match — the heart would be noise.
      const hasFav = fallbackMatches.some(m => m && m.is_favourite);
      const favHeart = (hasFav && !isFavourites)
        ? '<div class="rss-tile-fav-heart" title="Favourited"><span class="fav-lips"></span></div>'
        : '';

      // Studio logo overlay (top-centre) — shown on performer-mode AND
      // favourites-mode tiles. In both cases the tile layout is
      // performer-style (poster bg + medallion stack), so the studio
      // brand mark is useful chrome on top. Skip on the dedicated
      // Studios tab where the whole tile IS the studio logo.
      //
      // Pass the library-matched studio as ``name=`` when we have
      // one — the endpoint's ``q=`` fallback scans release titles
      // for known brand names, which misses titles where the
      // studio isn't mentioned by name (generic "Scene 42" etc).
      // ``matched_rows`` already identifies library studios that
      // matched this release, so a direct lookup is cheap and
      // resolves the "sometimes doesn't load" gap.
      const studioNameForLogo = (studioMatchesAllRaw[0] && studioMatchesAllRaw[0].folder_name) || '';
      // If the tile has no centre headshot medallion (no library performer
      // matched or no thumb available), centre the studio logo instead so
      // the poster's middle isn't visually empty.
      const hasCenterHeadshot = !isStudios && perfMatchesAll.length > 0;
      const studioLogo = (!isStudios && res.title)
        ? `<img class="rss-tile-studio-logo${hasCenterHeadshot ? '' : ' rss-tile-studio-logo--centered'}" src="/api/studio-logo?${studioNameForLogo ? 'name=' + encodeURIComponent(studioNameForLogo) + '&' : ''}q=${encodeURIComponent(res.title)}" alt="" loading="lazy" onerror="this.remove()">`
        : '';

      // Rotate a match array by carouselIdx, wrapping around the end.
      // Returns up to 3 entries in render order [center, right, left].
      function rotateMatches(arr, start) {
        if (!arr.length) return [];
        const n = arr.length;
        const cap = Math.min(3, n);
        const out = [];
        for (let i = 0; i < cap; i++) out.push(arr[(start + i) % n]);
        return out;
      }

      // ── Performer-mode: triple medallion stack ──
      // Centre (primary) + right + left, all visible at rest, all fading
      // out on tile hover. Side medallions only render when there are 2+
      // / 3+ matches; with 4+ the arrows rotate which three are shown.
      let medallionHtml = '';
      if (!isStudios) {
        const slots = rotateMatches(perfMatchesAll, carouselIdx);
        const [center, right, left] = slots;
        const thumb = (m, cls) => {
          const attrs = window.performerLinkAttrs(m.folder_name || '', { gender: m.gender, libraryRowId: m.id });
          return `<img class="${cls}" src="/api/favourites/performer-thumb?row_id=${m.id}" alt="${esc(m.folder_name || '')}" loading="lazy" onerror="this.remove()" ${attrs}>`;
        };
        const parts = [];
        // Paint order matters: side medallions first so the central one
        // renders on top of any overlap zone.
        if (left)   parts.push(thumb(left,   'rss-tile-headshot-medallion--left'));
        if (right)  parts.push(thumb(right,  'rss-tile-headshot-medallion--right'));
        if (center) parts.push(thumb(center, 'rss-tile-headshot-medallion'));
        medallionHtml = parts.join('');
      }

      // ── Studio-mode: performer headshot row in the top-left ──
      // Studios don't get a centre medallion (the poster IS the studio
      // visual) but if the RSS title also matched performers from the
      // library we surface them as small circular avatars stacked in
      // the top-left corner so the viewer can see who's in the scene.
      const studioPerfsHtml = (isStudios && perfMatchesAll.length)
        ? `<div class="rss-tile-studio-perfs">${perfMatchesAll.slice(0, 4).map(m => {
            const attrs = window.performerLinkAttrs(m.folder_name || '', { gender: m.gender, libraryRowId: m.id });
            return `<img class="rss-tile-studio-perf-avatar" src="/api/favourites/performer-thumb?row_id=${m.id}" alt="${esc(m.folder_name || '')}" title="${esc(m.folder_name || '')}" loading="lazy" onerror="this.remove()" ${attrs}>`;
          }).join('')}</div>`
        : '';

      const releases = res.releases || [res];
      const grabClass = `rss-tile-grab${isTor ? '' : ' nzb'}`;
      const releaseCount = releases.length;

      // CRT/TV-frame overlay — studio tiles only, mirroring the
      // /library studio treatment. Placed last inside `.rss-tile-inner`
      // so it paints on top of the logo + accent layers; CSS sets
      // `pointer-events: none` so the grab button + carousel arrows
      // stay clickable through it.
      const tvOverlay = isStudios ? '<div class="rss-tile-tv-overlay" aria-hidden="true"></div>' : '';
      return `<div class="rss-tile${isStudios ? ' rss-tile--studio' : ''}" data-tile-idx="${tileIdx}" title="${esc(res.title)}" role="button" tabindex="0" onclick="openRssTilePopup(${tileIdx}, event)" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();openRssTilePopup(${tileIdx})}">
        <div class="rss-tile-inner">
          ${posterHtml}
          ${glassHtml}
          ${medallionHtml}
          ${studioPerfsHtml}
          ${studioLogo}
          ${favHeart}
          <div class="rss-tile-overlay">
              ${meta ? `<div class="rss-tile-meta">${esc(meta)}</div>` : ''}
              <div class="rss-tile-title">${(typeof _libraryHighlight === 'function') ? _libraryHighlight(cleanTitle) : esc(cleanTitle)}</div>
          </div>
          <button type="button" class="${grabClass}" title="${releaseCount > 1 ? releaseCount + ' releases' : 'Download'}" onclick="event.stopPropagation();openRssTilePopup(${tileIdx})"><i class="fa-solid fa-download"></i></button>${releaseCount > 1 ? `<div style="position:absolute;top:calc(50% - 42px);left:calc(50% + 18px);background:rgba(var(--brand-purple-rgb),0.7);color:#fff;font-size:9px;font-family:var(--mono);border-radius:10px;padding:1px 5px;pointer-events:none;z-index:3;opacity:0;transition:opacity 0.18s" class="rss-tile-count-badge">${releaseCount}</div>` : ''}
          ${carousel}
          ${tvOverlay}
        </div>
        ${nameLabel}
      </div>`;
    }).join('');
  }

  function filterRssResults() {
    renderRssTiles(_dlProwlarrResults, getDlSearchMode());
  }

  //: Shared cache for the RSS feed response. favourites/rss/studios
  //: all render from the same `/api/downloads/rss-feed` payload (built
  //: hourly on the server + warmed at startup). In-memory TTL covers
  //: tab switches; sessionStorage survives a full page reload.
  const _DL_RSS_CACHE_TTL_MS = 10 * 60 * 1000;
  const _DL_RSS_SS_KEY = 'ts_dl_rss_feed_v1';
  const _DL_RSS_SS_TTL_MS = 30 * 60 * 1000;
  let _dlRssCache = null;   // { results, errors, diagnostics, ts }
  let _dlRssFetchGen = 0;
  function _dlRssCacheGet() {
    if (!_dlRssCache) return null;
    if (Date.now() - _dlRssCache.ts > _DL_RSS_CACHE_TTL_MS) {
      _dlRssCache = null;
      return null;
    }
    return _dlRssCache;
  }
  function _dlRssCacheInvalidate() {
    _dlRssCache = null;
    try { sessionStorage.removeItem(_DL_RSS_SS_KEY); } catch (_) {}
  }
  function _dlRssSessionLoad() {
    try {
      const raw = sessionStorage.getItem(_DL_RSS_SS_KEY);
      if (!raw) return null;
      const o = JSON.parse(raw);
      if (!o || !o.ts || Date.now() - o.ts > _DL_RSS_SS_TTL_MS) return null;
      return o;
    } catch (_) { return null; }
  }
  function _dlRssSessionSave(cache) {
    try {
      sessionStorage.setItem(_DL_RSS_SS_KEY, JSON.stringify({
        results: cache.results,
        errors: cache.errors,
        diagnostics: cache.diagnostics,
        ts: cache.ts,
      }));
    } catch (_) {}
  }
  function _dlRssResolveCacheHit() {
    const mem = _dlRssCacheGet();
    if (mem) return mem;
    const ss = _dlRssSessionLoad();
    if (!ss) return null;
    _dlRssCache = {
      results: ss.results || [],
      errors: ss.errors || [],
      diagnostics: ss.diagnostics || {},
      ts: ss.ts,
    };
    return _dlRssCache;
  }
  function _dlRssPaintFromCache(mode) {
    const hit = _dlRssResolveCacheHit();
    if (!hit) return false;
    const el = document.getElementById('dlProwlarrResults');
    if (!el) return false;
    _dlProwlarrResults = hit.results || [];
    if (!_dlProwlarrResults.length) {
      const diag = hit.diagnostics || {};
      const diagMsg = diag.configured_urls === 0
        ? 'No RSS feed URLs configured — add one under Settings → Downloads → RSS feeds.'
        : `Feed returned 0 matching items. ${diag.configured_urls} URL(s) configured. Check the feed URL is valid and accessible from your server.`;
      el.innerHTML = `<div class="empty" style="padding:32px">${esc(diagMsg)}</div>`;
      return true;
    }
    renderRssTiles(_dlProwlarrResults, mode || getDlSearchMode());
    return true;
  }

  async function loadDlRssFeed(mode) {
    const refreshRequested = (mode === '_refresh');
    const el = document.getElementById('dlProwlarrResults');
    if (!el) return;
    const fetchGen = ++_dlRssFetchGen;
    const hadCache = !!_dlRssResolveCacheHit();

    if (!refreshRequested && hadCache) {
      _dlProwlarrResults = _dlRssCache.results || [];
      if (!_dlProwlarrResults.length) {
        const diag = _dlRssCache.diagnostics || {};
        const diagMsg = diag.configured_urls === 0
          ? 'No RSS feed URLs configured — add one under Settings → Downloads → RSS feeds.'
          : `Feed returned 0 matching items. ${diag.configured_urls} URL(s) configured. Check the feed URL is valid and accessible from your server.`;
        el.innerHTML = `<div class="empty" style="padding:32px">${esc(diagMsg)}</div>`;
      } else {
        renderRssTiles(_dlProwlarrResults, getDlSearchMode());
      }
    } else if (!_dlRssPaintFromCache(getDlSearchMode())) {
      el.innerHTML = '<div class="empty" style="padding:32px">Loading feeds…</div>';
    }

    const libPromise = _refreshLibraryStudios();
    try {
      const r = await fetch('/api/downloads/rss-feed' + (refreshRequested ? '?refresh=true' : ''), { credentials: 'same-origin' });
      const d = await r.json();
      if (fetchGen !== _dlRssFetchGen) return;
      _dlProwlarrResults = d.results || [];
      const errs = (d.errors || []).filter(x => typeof x === 'string' && x);
      _dlRssCache = { results: _dlProwlarrResults, errors: errs, diagnostics: d.diagnostics || {}, ts: Date.now() };
      _dlRssSessionSave(_dlRssCache);
      if (libPromise) {
        libPromise.then(() => {
          if (fetchGen !== _dlRssFetchGen) return;
          if (_dlProwlarrResults.length) filterRssResults();
        }).catch(() => {});
      }
      if (!_dlProwlarrResults.length) {
        const diag = d.diagnostics || {};
        const diagMsg = diag.configured_urls === 0
          ? 'No RSS feed URLs configured — add one under Settings → Downloads → RSS feeds.'
          : `Feed returned 0 matching items. ${diag.configured_urls} URL(s) configured. Check the feed URL is valid and accessible from your server.`;
        el.innerHTML = `<div class="empty" style="padding:32px">${esc(diagMsg)}</div>`;
        return;
      }
      renderRssTiles(_dlProwlarrResults, getDlSearchMode());
      if (errs.length) {
        el.insertAdjacentHTML('afterbegin', `<div class="err" style="padding:8px 12px;margin-bottom:8px">${errs.map(e => esc(e)).join('<br>')}</div>`);
      }
    } catch (e) {
      if (fetchGen !== _dlRssFetchGen) return;
      if (!el.querySelector('.rss-tile')) {
        el.innerHTML = `<div class="err" style="padding:12px 16px">${esc(e.message)}</div>`;
      }
    }
  }

    async function applyProwlarrCategoryLabels() {
    const sel = document.getElementById('dlProwlarrKind');
    if (!sel) return;
    let sceneCat = 'Top-Shelf';
    let movieCat = '';
    try {
      const r = await fetch('/api/settings', { credentials: 'same-origin' });
      const d = await r.json();
      const s = (d && d.settings) || {};
      sceneCat = (s.prowlarr_category || 'Top-Shelf').trim() || 'Top-Shelf';
      movieCat = (s.prowlarr_category_movies || '').trim();
    } catch (_) { /* keep defaults */ }
    const movieEffective = movieCat || sceneCat;
    sel.querySelector('option[value="scene"]').textContent = sceneCat;
    sel.querySelector('option[value="movie"]').textContent = movieEffective;
  }
  async function runDlProwlarrSearch() {
    const q = document.getElementById('dlProwlarrQuery').value.trim();
    const el = document.getElementById('dlProwlarrResults');
    if (!q) {
      el.innerHTML = '<div class="empty" style="padding:16px">Enter a search query.</div>';
      return;
    }
    el.innerHTML = '<div class="empty" style="padding:16px">Searching…</div>';
    try {
      // Fan out: original query plus a dot-joined variant. Indexers
      // tokenise q= on whitespace so a single phrase often returns
      // releases that share only one token (eg "MUSE DELLA CATE"
      // surfaced "Silvia Dellai Muse Of Lust" because "DELLA" is a
      // prefix of "DELLAI"). The dot variant matches release-naming
      // conventions; backend dedupes by guid.
      const _params = new URLSearchParams();
      _params.append('q', q);
      if (/\s/.test(q)) _params.append('q', q.replace(/\s+/g, '.'));
      const r = await fetch('/api/prowlarr/search?' + _params.toString(), { credentials: 'same-origin' });
      const d = await r.json();
      if (d.error) {
        el.innerHTML = `<div class="err" style="padding:12px 16px">${esc(d.error)}</div>`;
        return;
      }
      // Whole-word AND-filter against the release title — drops fuzzy
      // matches the indexer let through (prefix hits, single-token
      // hits, etc). Unicode-aware lookarounds replace \b so accented
      // characters work as expected (\b uses \w which is ASCII-only,
      // so "musé" would otherwise have a spurious boundary inside it).
      // Each token must be flanked by anything that isn't a letter or
      // digit — dots, dashes, underscores, whitespace, or string
      // edges all qualify, so "DELLA" inside "DELLAI" correctly fails.
      const _tokens = q.toLowerCase().split(/\s+/).filter(Boolean);
      const _esc = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const _titleHasAllTokens = (title) => {
        const t = (title || '').toLowerCase();
        return _tokens.every(tok => new RegExp('(?<![\\p{L}\\p{N}])' + _esc(tok) + '(?![\\p{L}\\p{N}])', 'iu').test(t));
      };
      const allResults = d.results || [];
      _dlProwlarrResults = allResults.filter(r => _titleHasAllTokens(r.title));
      if (!_dlProwlarrResults.length) {
        const noteSuffix = allResults.length
          ? ` <span style="opacity:0.6">(${allResults.length} loose match${allResults.length === 1 ? '' : 'es'} hidden)</span>`
          : '';
        el.innerHTML = `<div class="empty" style="padding:16px">No exact matches.${noteSuffix}</div>`;
        return;
      }
      el.innerHTML = _dlProwlarrResults.map((res, i) => {
        const isTor = res.type === 'torrent';
        const agePart = res.age != null ? Math.round(res.age / 24) + 'd' : '';
        const seedPart = res.seeders != null ? res.seeders + ' seed' : '';
        const meta = [agePart, seedPart, Math.round(res.size_mb || 0) + ' MB'].filter(Boolean).join(' · ');
        const typLabel = isTor ? 'Torrent' : 'NZB';
        const typLogo = `<img class="dl-type-logo" src="/static/logos/${isTor ? 'torrent' : 'nzb'}.webp" alt="${typLabel}" title="${typLabel}">`;
        const m = res.match || {};
        const studio = (m.studio || '').trim();
        const perfs = (m.performers || []).filter(Boolean);
        const studioLogo = studio
          ? `<img class="dl-pr-studio-logo" src="/api/studio-logo?q=${encodeURIComponent(studio)}" alt="${esc(studio)}" title="${esc(studio)}" onerror="this.style.display='none'" loading="lazy">`
          : '';
        const headshotSlot = perfs.length
          ? `<span class="dl-pr-headshots" data-perf-names="${esc(perfs.join('|'))}"></span>`
          : '';
        return `<div class="dl-pr-row">
          <div class="dl-pr-cell">${studioLogo}</div>
          <div class="dl-pr-cell-name">
            <div class="dl-pr-title" title="${esc(res.title)}">${(typeof _libraryHighlight === 'function') ? _libraryHighlight(spaceifyReleaseName(res.title)) : esc(spaceifyReleaseName(res.title))}</div>
            <div class="dl-pr-meta">${esc(meta)}</div>
          </div>
          <div class="dl-pr-cell dl-pr-cell-headshots">${headshotSlot}</div>
          <div class="dl-pr-cell dl-pr-cell-badges">
            <span class="dl-pr-type">${typLogo}</span>
            <span class="dl-pr-indexer" title="${esc(res.indexer || '')}">${esc(res.indexer || '')}</span>
          </div>
          <div class="dl-pr-cell">
            <button type="button" class="btn-prowlarr-grab ${isTor ? '' : 'nzb'}" title="Send to client" onclick="grabDlProwlarr(event,${i})"><i class="fa-solid fa-download" aria-hidden="true"></i></button>
          </div>
        </div>`;
      }).join('');
      _hydrateProwlarrHeadshots();
    } catch (e) {
      el.innerHTML = `<div class="err" style="padding:12px 16px">${esc(e.message)}</div>`;
    }
  }

  async function grabDlProwlarr(ev, idx) {
    const result = _dlProwlarrResults[idx];
    if (!result) return;
    const kind = document.getElementById('dlProwlarrKind').value === 'movie' ? 'movie' : 'scene';
    const btn = ev.target && ev.target.closest ? ev.target.closest('button') : null;
    if (btn) { btn.disabled = true; btn.classList.remove('btn-prowlarr-grab--sent'); btn.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Loading"></span>'; }
    const isTor = result.type === 'torrent';
    const downloadUrl = isTor && result.magnet ? result.magnet : (result.download_url || '');
    try {
      const r = await fetch('/api/prowlarr/grab', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          guid: result.guid || '',
          indexer_id: result.indexer_id != null ? result.indexer_id : null,
          type: result.type,
          download_url: downloadUrl,
          kind,
          title: result.title || '',
        }),
      });
      const d = await r.json();
      if (d.ok) {
        if (btn) { btn.classList.add('btn-prowlarr-grab--sent'); btn.innerHTML = '<i class="fa-solid fa-check" aria-hidden="true"></i>'; }
        // Grab succeeded — the checkmark is the only feedback needed;
        // the download itself is tracked on the dedicated /downloads page.
      } else {
        window.toast(d.error || 'Could not send to download client');
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-download" aria-hidden="true"></i>'; }
      }
    } catch (e) {
      window.toast(e.message || 'Could not send to download client');
      if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-download" aria-hidden="true"></i>'; }
    }
  }

  function spaceifyReleaseName(name) {
    // Release names from download clients come in two shapes:
    //   1. dotted:  "DogHouseDigital.26.04.15.Emma.Rosie.XXX.1080p.MP4-WRB"
    //   2. camel/run: "DogHouseDigital260415EmmaRosieSheLovesToSquirtXXX1080pMP4-WRB"
    // We transform both to: "Dog House Digital 26 04 15 Emma Rosie ... XXX 1080p MP4-WRB"
    // Keep "1080p"/"720p"/"4k" atomic (digit→lowercase stays together).
    return (name || '')
      // dots → spaces
      .replace(/\./g, ' ')
      // lowercase → Uppercase: "DogHouse" → "Dog House"
      .replace(/([a-z])([A-Z])/g, '$1 $2')
      // Uppercase run → Uppercase+lowercase: "XXXMp4" → "XXX Mp4"
      .replace(/([A-Z]+)([A-Z][a-z])/g, '$1 $2')
      // letter → digit: "Digital260" → "Digital 260", "XXX1080" → "XXX 1080"
      .replace(/([a-zA-Z])(\d)/g, '$1 $2')
      // digit → Uppercase letter only: "260415Emma" → "260415 Emma"
      // (lowercase after digit stays glued so "1080p" doesn't become "1080 p")
      .replace(/(\d)([A-Z])/g, '$1 $2')
      // Re-glue codec/container tags the splits above chopped in half:
      // MP4 / MP3 → they got split as "MP 4" / "MP 3"; H264/H265 → "H 264"/"H 265"
      .replace(/\b(MP) (\d)\b/g, '$1$2')
      .replace(/\b(H) (\d{3})\b/g, '$1$2')
      .replace(/\b(AAC|DDP|EAC|DTS) (\d)\b/g, '$1$2')
      // collapse stray whitespace
      .replace(/\s+/g, ' ')
      .trim();
  }

  // Shared client-side headshots cache. The four hydration paths below
  // (downloads, prowlarr, wanted, perf-piles) used to each fire a fresh
  // `/api/performers/headshots-by-name` round-trip on every render —
  // even when toggling Discover tabs repeatedly resolved the same names.
  // Cache the resolved name → url map per-session (10 min TTL, matching
  // the library-tokens pattern) and only fetch names not already known.
  const _HEADSHOT_CACHE_TTL_MS = 10 * 60 * 1000;
  const _headshotCache = new Map(); // key: name.toLowerCase() → {url, ts}
  async function _fetchHeadshotsByName(nameSet) {
    const lookup = {};
    if (!nameSet || !nameSet.size) return lookup;
    const now = Date.now();
    const missing = [];
    for (const n of nameSet) {
      const key = n.toLowerCase();
      const hit = _headshotCache.get(key);
      if (hit && (now - hit.ts) < _HEADSHOT_CACHE_TTL_MS) {
        lookup[key] = hit.url;
      } else {
        missing.push(n);
      }
    }
    if (missing.length) {
      try {
        const r = await fetch('/api/performers/headshots-by-name?names=' + encodeURIComponent(missing.join(',')));
        const d = await r.json();
        (d.performers || []).forEach(p => {
          if (p && p.name) {
            const key = p.name.toLowerCase();
            const url = p.headshot_url || null;
            _headshotCache.set(key, { url, ts: now });
            lookup[key] = url;
          }
        });
        for (const n of missing) {
          const key = n.toLowerCase();
          if (!(key in lookup)) {
            _headshotCache.set(key, { url: null, ts: now });
            lookup[key] = null;
          }
        }
      } catch (_) { /* network blip — caller decides how to render misses */ }
    }
    return lookup;
  }

  async function _hydrateProwlarrHeadshots() {
    const slots = Array.from(document.querySelectorAll('#dlProwlarrResults .dl-pr-headshots'));
    if (!slots.length) return;
    const allNames = new Set();
    slots.forEach(slot => {
      (slot.dataset.perfNames || '').split('|').forEach(n => {
        const t = (n || '').trim();
        if (t) allNames.add(t);
      });
    });
    if (!allNames.size) return;
    const lookup = await _fetchHeadshotsByName(allNames);
    slots.forEach(slot => {
      const names = (slot.dataset.perfNames || '').split('|').map(n => n.trim()).filter(Boolean);
      const imgs = [];
      for (const n of names) {
        const url = lookup[n.toLowerCase()];
        if (url) {
          imgs.push(`<img class="dl-pr-headshot" src="${url}" alt="${n.replace(/"/g, '&quot;')}" title="${n.replace(/"/g, '&quot;')}" onerror="this.style.display='none'" loading="lazy">`);
          if (imgs.length >= 3) break;
        }
      }
      slot.innerHTML = imgs.reverse().join('');
    });
  }

  // Hoisted alias — boot + feed loaders run synchronously before later `const`
  // bindings; `typeof` on a TDZ `const` throws ReferenceError.
  function _refreshLibraryStudios() {
    return (typeof _refreshLibraryTokens === 'function')
      ? _refreshLibraryTokens()
      : Promise.resolve();
  }

  (() => {
    // Synchronous category-dropdown setup off the URL `?category=` param.
    // Used to live behind an `await fetch('/api/auth/status')` — that
    // round-trip blocked every /downloads visit for nothing (auth was
    // already validated when the HTML was served; the response wasn't
    // even read), so it's gone. Just run the dropdown init inline.
    try {
      const sel = document.getElementById('catFilter');
      const def = new URLSearchParams(location.search).get('category');
      if (def !== null && sel) {
        if (def === '*' || def === '') {
          sel.value = def === '*' ? '*' : '';
        } else {
          let opt = Array.from(sel.options).find(o => o.value === def);
          if (!opt) {
            opt = document.createElement('option');
            opt.value = def;
            opt.textContent = `Category: ${def}`;
            sel.appendChild(opt);
          }
          sel.value = def;
        }
      }
    } catch (_) {}
    // Cosmetic dropdown labels (Prowlarr scene/movie category names) —
    // fire in parallel rather than blocking the tab loader. Two
    // <option> text values that aren't visible until the user opens
    // the Search tab; no reason to gate the page on them.
    applyProwlarrCategoryLabels().catch(() => {});
    applyDlSearchModeUi();
    const _initMode = getDlSearchMode();
    if (_initMode === 'wanted') {
      // Initial page load — force-fetch so we bypass any stale session
      // cache. The non-force path was getting wedged on first paint
      // (cache-hit branch swallowed the render in some cases), and
      // sessionStorage is empty on a true cold load anyway so we lose
      // nothing skipping it here.
      loadWantedPanel({ force: true });
    } else if (_initMode === 'favourites' || _initMode === 'rss' || _initMode === 'studios') {
      loadDlRssFeed(_initMode);
    }
    // Search tab (prowlarr) needs no boot-time load — its results grid
    // shows the "Enter a query and search." empty state until the user
    // runs a search.
  })();

  // ── Wanted panel (main toggle tab) ─────────────────────────────────
  // Cache the wanted payload by id so buttons don't have to re-scrape
  // DOM or hit the API for title/description — direct lookup.
  const _wantedById = new Map();

  function filterWantedPanel() {
    const term = (document.getElementById('wantedFilter')?.value || '').toLowerCase().trim();
    document.querySelectorAll('#dlSearchModeWanted .wanted-row').forEach(row => {
      if (!term) { row.style.display = ''; return; }
      row.style.display = row.textContent.toLowerCase().includes(term) ? '' : 'none';
    });
  }

  //: Wanted-list cache — same pattern as the RSS cache. Invalidated
  //: on add/remove via toggleWanted so the list stays correct when
  //: the user actively mutates it; the TTL is a safety net for
  //: changes made elsewhere (e.g. phash auto-clear on /scenes).
  const _DL_WANTED_CACHE_TTL_MS = 5 * 60 * 1000;
  const _DL_WANTED_SS_KEY = 'ts_dl_wanted_v1';
  const _DL_WANTED_SS_TTL_MS = 15 * 60 * 1000;
  let _dlWantedCache = null;   // { items, ts }
  function _dlWantedCacheInvalidate() {
    _dlWantedCache = null;
    try { sessionStorage.removeItem(_DL_WANTED_SS_KEY); } catch (_) {}
  }
  function _dlWantedSessionLoad() {
    try {
      const raw = sessionStorage.getItem(_DL_WANTED_SS_KEY);
      if (!raw) return null;
      const o = JSON.parse(raw);
      if (!o || !o.ts || Date.now() - o.ts > _DL_WANTED_SS_TTL_MS) return null;
      return o.items || null;
    } catch (_) { return null; }
  }
  function _dlWantedSessionSave(items) {
    try {
      sessionStorage.setItem(_DL_WANTED_SS_KEY, JSON.stringify({ items, ts: Date.now() }));
    } catch (_) {}
  }

  async function loadWantedPanel(opts) {
    const force = !!(opts && opts.force);
    const sceneListEl = document.getElementById('wantedListScenes');
    const movieListEl = document.getElementById('wantedListMovies');
    const countEl = document.getElementById('wantedCount');
    const sceneCountEl = document.getElementById('wantedCountScenes');
    const movieCountEl = document.getElementById('wantedCountMovies');
    if (!sceneListEl || !movieListEl) return;

    // Refresh the library token cache in parallel so wanted-row titles
    // can paint `.qs-match` for any library performer / studio / vice
    // tokens that appear in the title.
    let libPromise;
    try {
      libPromise = _refreshLibraryStudios();
    } catch (_) {
      libPromise = Promise.resolve();
    }

    //: Cache hit — reuse the last response if still fresh.
    let items = null;
    if (!force && _dlWantedCache && (Date.now() - _dlWantedCache.ts <= _DL_WANTED_CACHE_TTL_MS)) {
      items = _dlWantedCache.items;
    } else if (!force) {
      const ssItems = _dlWantedSessionLoad();
      if (ssItems) {
        items = ssItems;
        _dlWantedCache = { items, ts: Date.now() };
      }
    }
    if (items === null) {
      sceneListEl.innerHTML = '<div class="empty">Loading…</div>';
      movieListEl.innerHTML = '';
      try {
        const r = await fetch('/api/wanted' + (force ? '?reconcile=1&refresh=1' : ''));
        const d = await r.json();
        items = (d && d.items) || [];
        _dlWantedCache = { items, ts: Date.now() };
        _dlWantedSessionSave(items);
      } catch (e) {
        // Use the file-local _escH instead of the global `esc` —
        // if `esc` ever goes missing (load-order regression, etc.)
        // the catch handler itself would throw and leave the panel
        // stuck on "Loading…" with no surfaced error.
        sceneListEl.innerHTML = `<div class="err" style="padding:12px 16px">${_escH(e.message || e)}</div>`;
        return;
      }
    }
    try {
      _wantedById.clear();
      items.forEach(w => _wantedById.set(w.id, w));
      if (countEl) countEl.textContent = `(${items.length})`;
      const sceneItems = items.filter(w => w.kind !== 'movie');
      const movieItems = items.filter(w => w.kind === 'movie');
      if (sceneCountEl) sceneCountEl.textContent = `(${sceneItems.length})`;
      if (movieCountEl) movieCountEl.textContent = `(${movieItems.length})`;
      if (!items.length) {
        sceneListEl.innerHTML = '<div class="empty" style="padding:40px 20px;text-align:center;color:var(--dim);line-height:1.6">Nothing on your Wanted list yet.<br><span style="font-size:12px">Click the <i class="fa-solid fa-eye" style="color:var(--accent)"></i> icon on any scene or movie on /scenes to add it here.</span></div>';
        movieListEl.innerHTML = '';
        return;
      }
      const renderRow = (w) => {
        const kindColor = w.kind === 'movie' ? '#c084fc' : '#f472b6';
        const sep = ' <span style="color:rgba(var(--brand-purple-rgb),0.4);margin:0 4px">·</span> ';
        const durationStr = w.duration > 0 ? `${Math.round(w.duration / 60)} min` : '';
        const metaBits = [w.release_date, durationStr].filter(Boolean).map(_escH);
        const meta = metaBits.length
          ? `<div class="wanted-meta">${metaBits.join(sep)}</div>`
          : '';
        const src = (w.source || '').toLowerCase();
        let srcLogo = '';
        let srcLabel = w.source || '';
        if (src === 'tpdb' || src === 'theporndb' || src === 'porndb') { srcLogo = '/static/logos/tpdb.webp'; srcLabel = 'TPDB'; }
        else if (src === 'stashdb') { srcLogo = '/static/logos/stashdb.webp'; srcLabel = 'StashDB'; }
        else if (src === 'fansdb') { srcLogo = '/static/logos/fansdb.webp'; srcLabel = 'FansDB'; }
        else if (src === 'javstash') { srcLogo = '/static/logos/javstash.webp'; srcLabel = 'JAVStash'; }
        else if (src === 'tmdb') { srcLogo = '/static/logos/tmdb.webp'; srcLabel = 'TMDB'; }
        // DB crossref tiles — one per stash-box DB. Active (full colour
        // link) when we have a URL for that DB; disabled (greyed) when
        // the wanted item has no match there. For the DB that matches
        // the item's own source, the source URL is used; for the others
        // we fall back to whatever phash crossref the enricher saved.
        // (JAVStash crossref columns aren't stored on wanted_items yet,
        // so only items sourced from JAVStash get an enabled tile.)
        const buildCrossref = (url, logoPath, label) => {
          if (url) {
            return `<a class="wanted-action-btn wanted-action-btn--link wanted-action-btn--wide" href="${_escH(url)}" target="_blank" rel="noopener noreferrer" title="Open on ${_escH(label)}"><img src="${logoPath}" alt="${label}" loading="lazy"></a>`;
          }
          return `<span class="wanted-action-btn wanted-action-btn--link wanted-action-btn--wide is-disabled" title="No ${_escH(label)} match"><img src="${logoPath}" alt="${label}" loading="lazy"></span>`;
        };
        const stashUrl = (src === 'stashdb' ? w.source_url : '') || w.stashdb_url || '';
        const fansUrl  = (src === 'fansdb'  ? w.source_url : '') || w.fansdb_url  || '';
        const javUrl   = (src === 'javstash' ? w.source_url : '') || '';
        const tpdbUrl  = ((src === 'tpdb' || src === 'theporndb' || src === 'porndb') ? w.source_url : '') || w.tpdb_url || '';
        const stashBtn = buildCrossref(stashUrl, '/static/logos/stashdb.webp',  'StashDB');
        const fansBtn  = buildCrossref(fansUrl,  '/static/logos/fansdb.webp',   'FansDB');
        const javBtn   = buildCrossref(javUrl,   '/static/logos/javstash.webp', 'JAVStash');
        const tpdbBtn  = buildCrossref(tpdbUrl,  '/static/logos/tpdb.webp',     'TPDB');
        // Studio logo overlay disabled — the studio name in the
        // wanted-text block already identifies the site, and the
        // bottom-left logo overlap with the TV bezel was visually
        // noisy.
        const studioLogoHtml = '';
        const kindIcon = w.kind === 'movie' ? 'ts-icon-movies' : 'ts-icon-scenes';
        const isMovie = w.kind === 'movie';
        // Class-alias the wanted-thumb to the matching VHS card type
        // so every `.<card>-card .img-load*` rule in app-shell.css
        // applies automatically. Scenes → .scene-card (TV bezel +
        // phosphor + scanlines), movies → .movie-card (paper VHS
        // sleeve halftone, applied to both the front poster and the
        // back cassette artwork).
        const thumbKindCls = isMovie
          ? 'wanted-thumb--movie movie-card'
          : 'wanted-thumb--scene scene-card';
        let thumbInner = '';
        if (w.thumb_url) {
          if (isMovie) {
            // Front sleeve only — the back cassette stack used to
            // render here was just a duplicate of the same image and
            // looked cluttered, so we drop it. The .movie-card class
            // chain still brings paper-VHS halftone via app-shell.css.
            const url = _escH(w.thumb_url);
            thumbInner = `
              <div class="wt-mov-poster">
                <div class="img-load">
                  <img src="${url}" loading="lazy" decoding="async" onerror="this.style.visibility='hidden'">
                </div>
              </div>`;
          } else {
            thumbInner = `<div class="img-load"><img class="scene-thumb" src="${_escH(w.thumb_url)}" loading="lazy" decoding="async" onerror="this.style.visibility='hidden'"></div>`;
          }
        }
        const rowKindCls = isMovie ? 'wanted-row--movie' : 'wanted-row--scene';
        // Search-Prowlarr button lives in the actions group on the
        // right side of the row.
        const searchOverlayBtn = '';
        const searchActionBtn = `<button type="button" class="wanted-action-btn wanted-action-btn--link" data-action="search" data-id="${w.id}" title="Search Prowlarr"><span class="ts-prowlarr-btn-content"><img class="ts-prowlarr-btn-logo" src="/static/logos/prowlarr.webp" alt="Prowlarr"><i class="fa-solid fa-magnifying-glass"></i></span></button>`;
        // Database button — probes the library for an existing copy
        // (id/phash reconcile + predicted-path lookup) without grabbing
        // anything. Paired under it sits Search Prowlarr.
        const databaseBtn = `<button type="button" class="wanted-action-btn wanted-action-btn--db" data-action="dbcheck" data-id="${w.id}" title="Check the library for a match"><i class="fa-solid fa-database"></i></button>`;
        // Title highlighting:
        //   1. Library multi-word phrases take priority (`Mia Khalifa`
        //      paints as a single span only when both words appear).
        //   2. Per-row tokens from THIS wanted entry's performers /
        //      studio fill in single-word matches in the gaps.
        //   3. Single-word library entities are folded into the
        //      per-row token set so other library performers /
        //      studios mentioned in the title still light up.
        // Studio is filtered through `_studioIfInLibrary` so unknown
        // studios don't paint.
        const _wTitle = w.title || 'Untitled';
        const _wPerf  = Array.isArray(w.performers) ? w.performers.join(' ') : (w.performers || '');
        const _wHl = (typeof _qsBuildHighlightSet === 'function')
          ? _qsBuildHighlightSet(
              _wPerf,
              (typeof _studioIfInLibrary === 'function' ? _studioIfInLibrary(w.studio) : w.studio))
          : null;
        if (_wHl) {
          for (const t of (window._libraryTokensLc || [])) _wHl.add(t);
        }
        const _wTitleHtml = (typeof _libraryAndTokenHighlight === 'function')
          ? _libraryAndTokenHighlight(_wTitle, _wHl)
          : _escH(_wTitle);
        const textBlock = `<div class="wanted-text">
            <div class="wanted-title-row">
              <i class="${kindIcon} wanted-kind-ico" style="color:${kindColor}" title="${_escH(w.kind)}" aria-hidden="true"></i>
              <div class="wanted-title" title="${_escH(w.title || '')}">${_wTitleHtml}</div>
            </div>
            ${w.studio ? `<div class="wanted-studio-name">${_escH(w.studio)}</div>` : ''}
            ${meta}
          </div>`;
        // Both kinds render as horizontal rows: thumb on left,
        // text block in middle (title/studio/date), actions
        // on the right. Scenes get a 16:9 landscape thumb;
        // movies get a 27:40 portrait thumb. Same template.
        return `
        <div class="ts-row wanted-row ${rowKindCls}" data-wanted-id="${w.id}">
          <div class="wanted-thumb ${thumbKindCls}">
            ${thumbInner}
            ${searchOverlayBtn}
          </div>
          ${textBlock}
          <div class="wanted-actions">
            <div class="wanted-actions-row">
              ${databaseBtn}
              ${tpdbBtn}
              ${stashBtn}
              <button type="button" class="wanted-action-btn wanted-action-btn--refresh" data-action="refresh" data-id="${w.id}" title="Refresh metadata"><i class="fa-solid fa-arrows-rotate"></i></button>
            </div>
            <div class="wanted-actions-row">
              ${searchActionBtn}
              ${fansBtn}
              ${javBtn}
              <button type="button" class="wanted-action-btn wanted-action-btn--danger" data-action="remove" data-id="${w.id}" title="Remove from Wanted"><i class="fa-solid fa-xmark"></i></button>
            </div>
          </div>
        </div>`;
      };
      sceneListEl.innerHTML = sceneItems.length
        ? sceneItems.map(renderRow).join('')
        : '<div class="empty" style="padding:24px 16px;text-align:center;color:var(--dim);font-size:12px">No wanted scenes.</div>';
      movieListEl.innerHTML = movieItems.length
        ? movieItems.map(renderRow).join('')
        : '<div class="empty" style="padding:24px 16px;text-align:center;color:var(--dim);font-size:12px">No wanted movies.</div>';
      if (typeof enrichPerformerNames === 'function') {
        enrichPerformerNames(sceneListEl);
        enrichPerformerNames(movieListEl);
      }
    } catch (e) {
      console.error('[wanted] render threw', e);
      sceneListEl.innerHTML = `<div class="empty">Failed to load: ${_escH(e.message || e)}</div>`;
      movieListEl.innerHTML = '';
    }
  }

  function _escH(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  // Delegated click handler — catches the action-row buttons and the
  // centred Search overlay button on the thumb. Anchor (``<a href>``)
  // variants share `.wanted-action-btn` for styling but keep their
  // default navigation behaviour.
  document.addEventListener('click', function(e) {
    const btn = e.target && e.target.closest && e.target.closest('button.wanted-action-btn, button.wanted-thumb-search');
    if (!btn) return;
    const action = btn.getAttribute('data-action');
    if (!action) return;
    e.preventDefault();
    e.stopPropagation();
    const id = parseInt(btn.getAttribute('data-id'), 10);
    if (!id || isNaN(id)) return;
    if (action === 'search') {
      // Route through the shared popup so /downloads, /scenes, /movies
      // and /favourites all share one search UI.
      const w = _wantedById.get(id);
      if (w && w.title) {
        window.openProwlarrSearchPopup({
          title: w.title,
          studio: w.studio || '',
          performers: w.performers || '',
          kind: w.kind || 'scene',
          thumb_url: w.thumb_url || '',
        });
      }
    }
    else if (action === 'remove') removeWantedItem(id);
    else if (action === 'refresh') refreshWantedItem(id, btn);
    else if (action === 'dbcheck') dbCheckWantedItem(id, btn);
  });

  async function refreshWantedItem(wantedId, btn) {
    if (btn) {
      btn.disabled = true;
      btn.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Loading"></span>';
    }
    try {
      await fetch(`/api/wanted/${wantedId}/refresh`, { method: 'POST' });
      // force — the refresh may have updated metadata or auto-acquired
      // the row; a cached reload wouldn't reflect either.
      loadWantedPanel({ force: true });
    } catch (_) {
      if (btn) {
        btn.disabled = false;
        btn.innerHTML = '<i class="fa-solid fa-arrows-rotate"></i>';
      }
    }
  }

  // "Database" button — asks the backend whether the library already
  // holds this wanted scene (id/phash reconcile + predicted-path
  // probe). On a hit the row is marked acquired and drops out of the
  // list on reload; on a miss the button flashes a brief "No match".
  async function dbCheckWantedItem(wantedId, btn) {
    if (!btn) return;
    const origHtml = btn.innerHTML;
    btn.disabled = true;
    btn.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Checking"></span>';
    try {
      const r = await fetch(`/api/wanted/${wantedId}/library-check`, { method: 'POST' });
      const d = await r.json().catch(() => ({}));
      if (d && d.acquired) {
        loadWantedPanel({ force: true });
        return;
      }
      btn.disabled = false;
      btn.classList.add('is-nomatch');
      btn.innerHTML = '<i class="fa-solid fa-circle-xmark" title="No match"></i>';
      setTimeout(() => {
        btn.classList.remove('is-nomatch');
        btn.innerHTML = origHtml;
      }, 1900);
    } catch (_) {
      btn.disabled = false;
      btn.innerHTML = origHtml;
    }
  }

  // Open a self-contained Prowlarr-search popup for a wanted item. The
  // title is known so there's no need to jump back to the scenes search
  // panel — we just hit /api/prowlarr/search directly and render the
  // releases with download buttons. Results don't modify the wanted row;
  // the item only auto-clears when the library scans a matching phash.
  let _wantedSearchToken = 0;
  async function searchWantedItem(wantedId) {
    const w = _wantedById.get(wantedId);
    if (!w || !w.title) return;
    const overlay = document.getElementById('wantedSearchOverlay');
    const header  = document.getElementById('wantedSearchHeader');
    const status  = document.getElementById('wantedSearchStatus');
    const results = document.getElementById('wantedSearchResults');
    overlay.style.display = 'flex';
    const kindPillColor = w.kind === 'movie' ? '#c084fc' : '#f472b6';
    header.innerHTML = `
      <div style="display:flex;gap:14px;align-items:center;min-width:0;flex:1">
        ${w.thumb_url ? `<img src="${_escH(w.thumb_url)}" style="width:96px;height:54px;object-fit:cover;border-radius:6px;flex-shrink:0;background:rgba(0,0,0,0.4)" onerror="this.style.visibility='hidden'" loading="lazy">` : ''}
        <div style="min-width:0;flex:1">
          <div style="font-family:var(--secs);font-size:16px;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="${_escH(w.title)}">${_escH(w.title)}</div>
          <div style="font-size:12px;color:var(--dim);margin-top:3px">
            <span style="color:${kindPillColor};font-weight:600;text-transform:uppercase;letter-spacing:0.08em;font-size:10px">${_escH(w.kind)}</span>
            ${w.studio ? ` · ${_escH(w.studio)}` : ''}
            ${w.release_date ? ` · ${_escH(w.release_date)}` : ''}
          </div>
        </div>
      </div>`;
    status.textContent = 'Searching Prowlarr…';
    results.innerHTML = '<div style="padding:24px;text-align:center;color:var(--dim);font-size:13px">Please wait…</div>';
    const myToken = ++_wantedSearchToken;
    try {
      // Fan out: search for the title, plus a `performers + studio`
      // variant so we catch releases tagged by studio rather than by
      // scene title. Backend dedupes results by guid.
      const _params = new URLSearchParams();
      _params.append('q', w.title);
      const _perfStr = (w.performers || '').trim();
      const _studio  = (w.studio || '').trim();
      if (_perfStr && _studio) {
        const _studioQ = `${_perfStr} ${_studio}`.trim();
        if (_studioQ.toLowerCase() !== (w.title || '').toLowerCase()) {
          _params.append('q', _studioQ);
        }
      }
      const r = await fetch('/api/prowlarr/search?' + _params.toString());
      const d = await r.json();
      if (myToken !== _wantedSearchToken) return;
      if (d && d.error) {
        status.textContent = d.error;
        results.innerHTML = '';
        return;
      }
      const releases = (d && d.results) || [];
      status.innerHTML = `<strong style="color:var(--text)">${releases.length}</strong> release${releases.length === 1 ? '' : 's'} found`;
      if (!releases.length) {
        results.innerHTML = '<div style="padding:32px;text-align:center;color:var(--dim);font-size:13px">No releases matched this title on your configured indexers.</div>';
        return;
      }
      // Highlight set built from the wanted item's title + studio +
      // performers so matching tokens light up in indexer release names.
      const qsHighlight = (typeof _qsBuildHighlightSet === 'function') ? _qsBuildHighlightSet(w.title, w.studio, w.performers) : null;
      const _hl = (s) => (qsHighlight && typeof _qsHighlight === 'function') ? _qsHighlight(s, qsHighlight) : _escH(s);
      results.innerHTML = releases.slice(0, 50).map((rel, i) => {
        const size = rel.size ? `${(rel.size / 1024 / 1024 / 1024).toFixed(2)} GB` : '';
        const seeders = rel.seeders != null ? `${rel.seeders} seeders` : '';
        const indexer = rel.indexer || rel.tracker || '';
        return `
        <div class="wanted-release-row">
          <div style="flex:1;min-width:0">
            <div class="wanted-release-title" title="${_escH(rel.title || '')}">${_hl(rel.title || 'Untitled release')}</div>
            <div class="wanted-release-meta">
              ${size ? `<span>${_escH(size)}</span>` : ''}
              ${indexer ? `<span>${_escH(indexer)}</span>` : ''}
              ${seeders ? `<span style="color:${rel.seeders > 5 ? '#4ade80' : rel.seeders > 0 ? '#fbbf24' : '#f87171'}">${_escH(seeders)}</span>` : ''}
            </div>
          </div>
          <button type="button" class="wanted-release-grab" data-release-idx="${i}" title="Send to client"><i class="fa-solid fa-download"></i> Grab</button>
        </div>`;
      }).join('');
      // Stash the releases so the grab button can resolve from idx.
      overlay.dataset.releases = JSON.stringify(releases.slice(0, 50));
    } catch (e) {
      if (myToken !== _wantedSearchToken) return;
      status.textContent = 'Search error: ' + (e.message || e);
      results.innerHTML = '';
    }
  }

  function closeWantedSearch() {
    _wantedSearchToken++;
    const o = document.getElementById('wantedSearchOverlay');
    if (o) o.style.display = 'none';
  }

  // Delegated grab handler for Prowlarr-result buttons inside the popup.
  document.addEventListener('click', function(e) {
    const btn = e.target && e.target.closest && e.target.closest('.wanted-release-grab');
    if (!btn) return;
    e.preventDefault();
    e.stopPropagation();
    const overlay = document.getElementById('wantedSearchOverlay');
    if (!overlay) return;
    const idx = parseInt(btn.getAttribute('data-release-idx'), 10);
    let releases = [];
    try { releases = JSON.parse(overlay.dataset.releases || '[]'); } catch (_) {}
    const rel = releases[idx];
    if (!rel) return;
    btn.disabled = true;
    btn.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Loading"></span>';
    fetch('/api/prowlarr/grab', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(rel),
    }).then(r => r.json()).then(d => {
      if (d && d.ok) {
        btn.innerHTML = '<i class="fa-solid fa-check"></i> Sent';
        btn.style.borderColor = 'rgba(74,222,128,0.6)';
        btn.style.color = '#4ade80';
      } else {
        btn.innerHTML = '<i class="fa-solid fa-triangle-exclamation"></i> ' + (d.error || 'Failed');
        btn.style.borderColor = 'rgba(var(--brand-accent-rgb), 0.6)';
        btn.style.color = 'var(--accent)';
      }
    }).catch(() => {
      btn.innerHTML = '<i class="fa-solid fa-triangle-exclamation"></i> Failed';
      btn.disabled = false;
    });
  });

  async function removeWantedItem(wantedId) {
    // Optimistic remove: pull the row from the DOM and the in-memory
    // map immediately. Cache is invalidated so the next manual refresh
    // re-fetches; we skip the full `loadWantedPanel({force:true})`
    // round-trip that used to follow every delete.
    const row = document.querySelector(`.wanted-row[data-wanted-id="${wantedId}"]`);
    const cached = _wantedById.get(wantedId);
    _wantedById.delete(wantedId);
    if (row) row.remove();
    try {
      const r = await fetch(`/api/wanted/${wantedId}`, { method: 'DELETE' });
      if (!r.ok) throw new Error('delete failed');
      _dlWantedCacheInvalidate();
    } catch (e) {
      if (cached) _wantedById.set(wantedId, cached);
      _dlWantedCacheInvalidate();
      loadWantedPanel({ force: true });
    }
  }

  // Escape key closes the Wanted search overlay.
  document.addEventListener('keydown', e => {
    if (e.key === 'Escape') {
      closeWantedSearch();
    }
  });
