/* Externalized from queue.html. Original inline blocks 1/3.
 * Block 1: main queue + Prowlarr search overlay + IAFD picker. */

  const TS_DL_PAGE = 'queue';

  // ── Queue filmstrip popup ───────────────────────────────────────
  // Five evenly-spaced frames pulled from the source video — handy
  // for IDing files the matcher couldn't auto-resolve. Frames are
  // generated server-side and cached, so reopening the same file
  // is instant. Right-clicking a frame uses the browser's native
  // "Search image with…" menu for reverse-image-search.
  let _qfsItems = [];
  let _qfsIndex = -1;
  let _qfsKeydownBound = false;

  let _qfsToken = 0;
  async function openQueueFilmstrip(filename) {
    const overlay = document.getElementById('qFilmstripOverlay');
    const strip   = document.getElementById('qFilmstripStrip');
    const status  = document.getElementById('qFilmstripStatus');
    if (!overlay || !strip) return;
    const myToken = ++_qfsToken;
    _qfsItems = [];
    _qfsIndex = -1;
    strip.innerHTML = Array.from({length: 5}, () =>
      '<div class="qfs-thumb"><div class="qfs-thumb-empty"><i class="ts-icon-scenes" aria-hidden="true"></i></div></div>'
    ).join('');
    if (status) status.textContent = 'Generating filmstrip…';
    overlay.classList.add('open');
    if (!_qfsKeydownBound) {
      document.addEventListener('keydown', _qfsHandleKey);
      _qfsKeydownBound = true;
    }
    // Poll loop — the endpoint is non-blocking (see _loadSearchFrames
    // for the rationale). Frames trickle in as the prewarm finishes
    // each ffmpeg seek, so the strip paints partial state instead of
    // sitting on a spinner.
    const startMs = Date.now();
    const POLL_INTERVAL_MS = 1500;
    // 60s ceiling — after a minute of "generating" without all 5 frames
    // appearing, ffmpeg is genuinely stuck or struggling on this file.
    // Stop polling so the user gets a clear "stalled, click Retry" state
    // instead of staring at pulsing loaders. The backoff fix (1h park
    // after a complete pass) also short-circuits a lot of this — if any
    // pass finishes with partial frames, the next poll flips backoff:
    // true and we drop out before the 60s deadline.
    const POLL_TIMEOUT_MS = 60 * 1000;

    const renderState = (d) => {
      const thumbs = Array.isArray(d.thumbs) ? d.thumbs : [];
      _qfsItems = thumbs;
      const byIdx = new Map(thumbs.map(t => [t.i, t]));
      const slotsHtml = [];
      for (let i = 0; i < 5; i++) {
        const t = byIdx.get(i);
        if (t) {
          slotsHtml.push(`<div class="qfs-thumb" onclick="openQueueFilmstripLightbox(${thumbs.indexOf(t)})" title="Enlarge"><img src="${esc(t.url)}" alt="Frame ${t.i + 1}" draggable="true" loading="lazy"></div>`);
        } else if (d.generating) {
          slotsHtml.push('<div class="qfs-thumb"><div class="qfs-thumb-empty" title="Generating frame…"><span class="loader loader--btn loader--muted" role="status" aria-label="Generating"></span></div></div>');
        } else if (d.backoff) {
          slotsHtml.push(`<div class="qfs-thumb"><button type="button" class="qfsRetryBtn qfs-thumb-empty" data-filename="${esc(filename)}" title="ffmpeg gave up — click to retry" style="background:transparent;border:1px dashed rgba(244,114,182,0.45);color:rgba(244,114,182,0.85);cursor:pointer"><i class="fa-solid fa-rotate-right"></i></button></div>`);
        } else {
          slotsHtml.push('<div class="qfs-thumb"><div class="qfs-thumb-empty"><i class="ts-icon-scenes" aria-hidden="true"></i></div></div>');
        }
      }
      strip.innerHTML = slotsHtml.join('');
      strip.querySelectorAll('.qfsRetryBtn').forEach(btn => {
        btn.addEventListener('click', async (ev) => {
          ev.preventDefault();
          ev.stopPropagation();
          const fn = btn.getAttribute('data-filename') || filename;
          btn.disabled = true;
          btn.innerHTML = '<span class="loader loader--btn loader--muted" role="status" aria-label="Retrying"></span>';
          try {
            await fetch('/api/queue/thumbs/retry', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              credentials: 'same-origin',
              body: JSON.stringify({ filename: fn }),
            });
          } catch { /* fall through */ }
          openQueueFilmstrip(fn);
        }, { once: true });
      });
      if (status) {
        if (d.ready) status.textContent = filename;
        else if (d.backoff) status.textContent = 'FFmpeg gave up on this file — click any frame to retry';
        else if (d.generating) status.textContent = `Generating filmstrip… (${thumbs.length}/5)`;
        else if (!thumbs.length) status.textContent = 'No frames could be captured (FFmpeg failed)';
        else status.textContent = filename;
      }
    };

    try {
      while (true) {
        const r = await fetch('/api/queue/thumbs?filename=' + encodeURIComponent(filename), { credentials: 'same-origin' });
        if (myToken !== _qfsToken) return;
        const d = await r.json().catch(() => ({}));
        if (myToken !== _qfsToken) return;
        if (!r.ok) {
          if (status) status.textContent = (d && d.error) ? d.error : 'Failed to generate filmstrip';
          return;
        }
        renderState(d);
        if (d.ready || d.backoff || !d.generating) return;
        if (Date.now() - startMs > POLL_TIMEOUT_MS) {
          //: Same stalled-state paint as _loadSearchFrames — force the
          //: missing slots into retry buttons rather than leaving loaders
          //: spinning when ffmpeg has gone silent.
          renderState({ ...d, backoff: true, generating: false });
          return;
        }
        await new Promise(res => setTimeout(res, POLL_INTERVAL_MS));
        if (myToken !== _qfsToken) return;
      }
    } catch (e) {
      if (myToken !== _qfsToken) return;
      if (status) status.textContent = 'Error: ' + (e && e.message || e);
    }
  }

  function closeQueueFilmstrip() {
    const overlay = document.getElementById('qFilmstripOverlay');
    if (overlay) overlay.classList.remove('open');
    closeQueueFilmstripLightbox();
    if (_qfsKeydownBound) {
      document.removeEventListener('keydown', _qfsHandleKey);
      _qfsKeydownBound = false;
    }
    _qfsItems = [];
    _qfsIndex = -1;
  }

  function openQueueFilmstripLightbox(idx) {
    if (idx < 0 || idx >= _qfsItems.length) return;
    _qfsIndex = idx;
    const lb  = document.getElementById('qFilmstripLightbox');
    const img = document.getElementById('qFilmstripBigImg');
    const ctr = document.getElementById('qFilmstripCounter');
    if (!lb || !img) return;
    img.src = _qfsItems[idx].url;
    if (ctr) ctr.textContent = (idx + 1) + ' / ' + _qfsItems.length;
    lb.classList.add('open');
  }

  function closeQueueFilmstripLightbox() {
    const lb = document.getElementById('qFilmstripLightbox');
    if (lb) lb.classList.remove('open');
    _qfsIndex = -1;
  }

  function queueFilmstripStep(delta) {
    if (!_qfsItems.length || _qfsIndex < 0) return;
    const next = (_qfsIndex + delta + _qfsItems.length) % _qfsItems.length;
    openQueueFilmstripLightbox(next);
  }

  function _qfsHandleKey(e) {
    const overlay = document.getElementById('qFilmstripOverlay');
    if (!overlay || !overlay.classList.contains('open')) return;
    if (e.key === 'Escape') {
      const lb = document.getElementById('qFilmstripLightbox');
      if (lb && lb.classList.contains('open')) {
        closeQueueFilmstripLightbox();
      } else {
        closeQueueFilmstrip();
      }
      e.preventDefault();
      return;
    }
    if (_qfsIndex < 0) return;
    if (e.key === 'ArrowLeft')  { queueFilmstripStep(-1); e.preventDefault(); }
    if (e.key === 'ArrowRight') { queueFilmstripStep(1);  e.preventDefault(); }
  }

  // Queue-only stub. The original multi-tab Downloads page used this to
  // toggle Prowlarr / RSS / Wanted / Downloads views; on /queue only the
  // queue section exists, so this just reveals it. Kept callable because
  // the boot IIFE invokes it.
  function applyDlSearchModeUi() {
    const elQ = document.getElementById('dlSearchModeQueue');
    if (elQ) elQ.style.display = 'flex';
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
  // (downloads, prowlarr, queue, perf-piles) used to each fire a fresh
  // `/api/performers/headshots-by-name` round-trip on every render —
  // even when toggling scenes↔movies repeatedly resolved the same names.
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
        // Names the server didn't return — cache the miss so we don't
        // re-fetch them every render.
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

  // Queue state must exist before the boot IIFE — it calls setQueueMode(queueMode)
  // when ?view=queue or a saved Queue tab is active.
  let _qIsRunning = false;
  let _qSelectedFile = null;
  let _qSelectedMovieFile = null;
  let _qManualFile = null;
  let queueMode = localStorage.getItem('ts_queue_mode') || 'scenes';
  let _queuePollTimer = null;
  /** Bumped on `pagehide` so async queue work skips DOM writes after navigate away. */
  let _queueLoadGen = 0;
  /** Aborts the in-flight `/api/status` poll on teardown so pagehide doesn't leak a request. */
  let _queueStatusAbort = null;
  let _queuePage = 0;
  let _queuePageSize = 25;
  let _queueFilteredIndexes = [];
  let _queueActiveKind = 'scenes';
  let _queueStatusFilter = '';
  let _queueMatchedOnly = false;
  let _queueVicesOnly = false;
  let _queueErrorsOnly = false;
  let _sceneQueuePayload = null;
  let _movieQueuePayload = null;
  let _queueStatsPayload = null;
  let _queueStatsFetchedAt = 0;
  let _viceSlugsFetchedAt = 0;
  const _QUEUE_STATS_TTL_MS = 30000;
  const _VICE_SLUGS_TTL_MS = 60000;

  // Hoisted alias — boot + feed loaders run synchronously before later `const`
  // bindings; `typeof` on a TDZ `const` throws ReferenceError.
  function _refreshLibraryStudios() {
    return (typeof _refreshLibraryTokens === 'function')
      ? _refreshLibraryTokens()
      : Promise.resolve();
  }

  // ══════════════════════════════════════════════════════════════════════
  // Queue functions
  // ══════════════════════════════════════════════════════════════════════

  function clearLog() {}

  /**
   * Push the queue count into the header's .ts-dl-stats LCD pill so it
   * matches the Queue tile on the page. The pill polls /api/downloads/stats
   * every 8s independently, which means a 0–8 s drift after files arrive
   * — calling this from the queue payload applier closes that gap on
   * any page that renders the queue list.
   */
  function _syncHeaderQueuePill(count) {
    const el = document.querySelector('.ts-dl-stats__num[data-key="queue"]');
    if (!el) return;
    const n = Math.max(0, Number(count) || 0);
    // Pad to the same width the pill's own painter uses (3 chars).
    const next = String(n).padStart(3, '0');
    if (el.textContent !== next) el.textContent = next;
  }

  /**
   * Inline onclicks on queue rows bake an index into the markup. If a
   * background refresh (5s poll, completed run, manual reload) swaps
   * `window._queueFiles` between render and click, the index may point
   * at a different file — or no file at all. Route every action through
   * this helper so a stale index is caught with a clear toast instead
   * of either silently invoking the wrong file or throwing on
   * `undefined.filename`.
   */
  function _qCall(fnName, i, extra) {
    const arr = window._queueFiles || [];
    const f = arr[i];
    if (!f || !f.filename) {
      if (typeof window.toast === 'function') {
        window.toast('Queue entry no longer exists — refresh and retry', { kind: 'error' });
      }
      return;
    }
    const fn = window[fnName];
    if (typeof fn !== 'function') return;
    if (arguments.length > 2) fn(f.filename, extra);
    else fn(f.filename);
  }
  window._qCall = _qCall;

  // Adaptive cadence: fast (5s) while the pipeline is running; slow
  // (30s heartbeat) when idle. The transition is driven by the
  // last /api/status response — `pollQueueStatus` calls
  // `_queuePollAdapt(d.running)` after each tick.
  const QUEUE_POLL_BUSY_MS = 5000;
  const QUEUE_POLL_IDLE_MS = 30000;
  let _queuePollIntervalMs = QUEUE_POLL_BUSY_MS;

  function _queuePollAdapt(running) {
    const target = running ? QUEUE_POLL_BUSY_MS : QUEUE_POLL_IDLE_MS;
    if (target === _queuePollIntervalMs && _queuePollTimer) return;
    _queuePollIntervalMs = target;
    if (_queuePollTimer) {
      clearInterval(_queuePollTimer);
      _queuePollTimer = setInterval(pollQueueStatus, _queuePollIntervalMs);
    }
  }

  function startQueuePolling() {
    stopQueuePolling();
    pollQueueStatus();
    _queuePollTimer = setInterval(pollQueueStatus, _queuePollIntervalMs);
  }
  function stopQueuePolling() {
    if (_queuePollTimer) { clearInterval(_queuePollTimer); _queuePollTimer = null; }
  }

  // Coming back to a hidden tab — snap to fast cadence so the user
  // sees a fresh state immediately; the next tick adapts to idle if
  // nothing's running.
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible' && _queuePollTimer) {
      _queuePollAdapt(true);
      pollQueueStatus();
    }
  });

  function _tearDownQueuePage() {
    if (typeof TS_DL_PAGE === 'undefined' || TS_DL_PAGE !== 'queue') return;
    _queueLoadGen++;
    stopQueuePolling();
    if (_queueStatusAbort) {
      try { _queueStatusAbort.abort(); } catch (_) {}
      _queueStatusAbort = null;
    }
  }
  window.addEventListener('pagehide', _tearDownQueuePage);

  /* One-shot guard so the version string is stamped into #appVersion
   * exactly once — the boot-time second `/api/status` fetch (formerly
   * at the end of the file) was duplicate work; pollQueueStatus now
   * does that write on the first response. */
  let _appVersionWritten = false;
  async function pollQueueStatus() {
    // Skip while the tab is hidden.
    if (document.visibilityState !== 'visible') return;
    const mountGen = _queueLoadGen;
    // Cancel any prior in-flight poll before issuing a new one (and let
    // pagehide abort the current one without leaving an orphan fetch).
    if (_queueStatusAbort) {
      try { _queueStatusAbort.abort(); } catch (_) {}
    }
    const ctrl = (typeof AbortController === 'function') ? new AbortController() : null;
    _queueStatusAbort = ctrl;
    try {
      const r = await fetch('/api/status', ctrl ? { signal: ctrl.signal } : undefined);
      if (mountGen !== _queueLoadGen) return;
      const d = await r.json();
      if (!_appVersionWritten && d && d.version) {
        const verEl = document.getElementById('appVersion');
        if (verEl) verEl.textContent = 'v' + d.version;
        _appVersionWritten = true;
      }
      const btn = document.getElementById('btnRunAll');
      const wasRunning = _qIsRunning;
      const nowRunning = !!d.running;
      _queuePollAdapt(nowRunning);
      if (nowRunning) {
        if (btn) btn.disabled = true;
        // Only disable the per-row Run buttons inside queue items — the
        // "Use this" buttons in the search panel share the class but
        // fire `/api/file/manual` independently of the queue processor,
        // so they must stay clickable even while a run is in flight.
        // Mutation gated on transition so we don't re-query and walk the
        // node list every 2 s.
        if (!wasRunning) {
          document.querySelectorAll('.queue-item .btn-run-file').forEach(b => b.disabled = true);
          if (window.TsActivity && window.TsActivity.refresh) window.TsActivity.refresh();
        }
        _qIsRunning = true;
      } else {
        if (wasRunning) {
          if (mountGen !== _queueLoadGen) return;
          _sceneQueuePayload = null;
          _movieQueuePayload = null;
          loadQueue({ preserveView: true, force: true });
          loadQueueStats();
          if (window.TsActivity && window.TsActivity.refresh) window.TsActivity.refresh();
          document.querySelectorAll('.queue-item .btn-run-file').forEach(b => b.disabled = false);
        }
        if (btn) btn.disabled = false;
        _qIsRunning = false;
      }
    } catch (e) {
      // AbortError fires on intentional teardown — silently ignore. Other
      // errors usually mean a transient network blip; the next 5 s tick
      // will re-try, so don't toast on every poll failure.
      if (!e || e.name !== 'AbortError') {
        if (typeof console !== 'undefined') console.warn('[queue] /api/status poll failed', e);
      }
    } finally {
      if (_queueStatusAbort === ctrl) _queueStatusAbort = null;
    }
  }

  function setQueueMode(mode, preserveFilter) {
    if (!preserveFilter) _queueStatusFilter = '';
    queueMode = mode === 'movies' ? 'movies' : 'scenes';
    localStorage.setItem('ts_queue_mode', queueMode);
    const btnScenes = document.getElementById('btnQueueScenes');
    const btnMovies = document.getElementById('btnQueueMovies');
    if (btnScenes) btnScenes.classList.toggle('active', queueMode === 'scenes');
    if (btnMovies) btnMovies.classList.toggle('active', queueMode === 'movies');
    const qpt = document.getElementById('queuePanelTitle');
    if (qpt && typeof tsSetPageTitleText === 'function') {
      tsSetPageTitleText(qpt, queueMode === 'movies' ? 'Movie queue' : 'Scene queue', 'queue');
    }
    const statFiled = document.querySelector('.queue-section .stat-card.stat-filed');
    const statNd = document.getElementById('statCardNoDir');
    const statErr = document.querySelector('.queue-section .stat-card.stat-errors');
    if (queueMode === 'movies') {
      if (statFiled) { statFiled.setAttribute('href', '/history?status=filed&kind=movie'); statFiled.style.opacity = ''; }
      if (statNd) { statNd.style.opacity = '0.85'; }
      if (statErr) { statErr.setAttribute('href', '/history?status=error&kind=movie'); statErr.style.opacity = ''; }
      const _slnd = document.getElementById('statLabelNoDir');
      if (_slnd) _slnd.textContent = 'No Destination';
    } else {
      if (statFiled) { statFiled.setAttribute('href', '/history?status=filed'); statFiled.style.opacity = ''; }
      if (statNd) { statNd.style.opacity = ''; }
      if (statErr) { statErr.setAttribute('href', '/history?status=error'); statErr.style.opacity = ''; }
      const _slnd2 = document.getElementById('statLabelNoDir');
      if (_slnd2) _slnd2.textContent = 'No Directory';
    }
    const btnRun = document.getElementById('btnRunAll');
    if (btnRun) btnRun.style.display = '';
    const qf = document.getElementById('queueFilter');
    if (qf) { qf.placeholder = 'Filter queue...'; }
    const _qsp = document.getElementById('qSearchPanel');
    if (_qsp) _qsp.style.display = 'none';
    closeMovieSearchPanel();
    _qSelectedMovieFile = null;
    clearSelectedFile();
    loadQueue({ preserveView: true });
    if (_queueStatsPayload && (Date.now() - _queueStatsFetchedAt) < _QUEUE_STATS_TTL_MS) {
      _paintQueueStats(_queueStatsPayload);
    } else {
      loadQueueStats();
    }
  }

  function refreshActiveQueue() {
    _sceneQueuePayload = null;
    _movieQueuePayload = null;
    loadQueue({ preserveView: true, force: true });
    loadQueueStats();
  }
  function filterActiveQueue() { filterQueue(); }


  // ── Queue pagination state — declared before boot IIFE ─────────────

  //: /discover deep-link button on queue rows was removed — the
  //: filing/search popup now covers the same workflow and the row
  //: action strip was getting crowded.

  function _renderSceneQueueItem(f, i) {
    const statusKey = f.prev_status || 'none';
    let statusEl;
    if (f.prev_status === 'unmatched') {
      statusEl = '<i class="fa-solid fa-not-equal qi-status-icon" title="Previously unmatched"></i>';
    } else if (f.prev_status === 'no_dir') {
      // Build a "why this is no_dir" hint from the row data so the
      // tooltip explains the failure mode instead of just saying
      // "No directory" — which collides visually with a TPDB match
      // showing on the same row.
      const _ndStudio = (f.match_studio || '').trim();
      const _ndPerfs = String(f.performers || '').trim();
      const _ndBits = [];
      _ndBits.push('Identified — but no filing destination');
      if (_ndStudio) _ndBits.push('Studio "' + _ndStudio + '" has no Series folder');
      if (_ndPerfs) _ndBits.push('No performer folder for ' + _ndPerfs);
      if (!_ndStudio && !_ndPerfs) _ndBits.push('Studio + performers both unresolved');
      const _ndTip = _ndBits.join(' · ');
      statusEl = '<i class="fa-solid fa-equals qi-status-icon" title="' + esc(_ndTip) + '"></i>';
    } else if (f.prev_status === 'error') {
      statusEl = `<i class="fa-solid fa-triangle-exclamation qi-status-icon" title="${esc(f.error || 'Previous run errored')}" style="color:#ef4444"></i>`;
    } else {
      statusEl = `<span class="status-dot-queue ${statusKey}" title="${esc(statusKey)}"></span>`;
    }
    // Always request a logo, even when no studio is matched — the
    // endpoint's vice-fallback scans the q= text for any configured
    // Vice name, so release titles like "…Spanking…" surface the
    // Spanking vice logo even though no studio was matched. Pass the
    // studio as `name=` (preferred exact lookup) AND the filename as
    // `q=` (substring + vice fallback scope).
    const studio = (f.match_studio || '').trim();
    const logoQ = f.filename || f.display_name || studio;
    const studioLogo = (studio || logoQ)
      ? `<img class="qi-studio-logo" loading="lazy" decoding="async" src="/api/studio-logo?name=${encodeURIComponent(studio)}&q=${encodeURIComponent(logoQ)}" alt="${esc(studio || '')}" title="${esc(studio || logoQ)}" onerror="this.style.display='none'">`
      : '';
    const perfRaw = (f.performers || '').trim();
    const perfs = perfRaw
      ? perfRaw.replace(/\|/g, ',').replace(/ \/ /g, ',').split(',').map(s => s.trim()).filter(Boolean)
      : [];
    // Server pre-resolves performers → library row + headshot URL in
    // `_build_queue_snapshot`. Render those tiles directly so there's
    // no `/api/performers/headshots-by-name` round-trip on page open
    // and no empty-then-fill flash. Falls back to the empty slot when
    // the server didn't return any matches — `_hydrateQueueHeadshots`
    // (kept for backward compat) will still resolve them later if the
    // data lands after the initial paint.
    const headshotData = Array.isArray(f.performer_headshots) ? f.performer_headshots : [];
    let headshotSlot = '';
    if (headshotData.length) {
      const imgs = [];
      for (const h of headshotData) {
        const url = (h && h.headshot_url) || '';
        const nm = (h && h.name) || '';
        const rid = (h && h.row_id) || 0;
        const safeName = nm.replace(/"/g, '&quot;');
        if (url) {
          // `data-performer-link` is picked up by the global delegated
          // handler in performer-popup.js → opens the universal popup
          // using the library row id (preferred) or the name fallback.
          const ridAttr = rid ? ` data-library-row-id="${rid}"` : '';
          imgs.push(`<img class="qi-headshot qi-headshot-link" loading="lazy" decoding="async" data-performer-link data-name="${safeName}"${ridAttr} src="${url}" alt="${safeName}" title="${safeName} — open profile" onerror="this.style.display='none'">`);
        }
        if (imgs.length >= 3) break;
      }
      headshotSlot = `<span class="qi-headshots">${imgs.reverse().join('')}</span>`;
    } else if (perfs.length) {
      // Fallback: server didn't pre-resolve (older payload, edge case).
      // `_hydrateQueueHeadshots` picks this up via `data-perf-names`.
      headshotSlot = `<span class="qi-headshots" data-perf-names="${esc(perfs.join('|'))}"></span>`;
    }
    const histBits = [];
    if (f.match_title)  histBits.push(esc(f.match_title));
    if (f.match_source) histBits.push(esc(f.match_source));
    if (f.match_date)   histBits.push(esc(f.match_date));
    let metaText = '';
    let metaClass = 'qi-meta';
    if (histBits.length) {
      metaText = histBits.join(' · ');
    } else if (f.match_guess) {
      const guessBits = [];
      if (studio)       guessBits.push(esc(studio));
      if (perfs.length) guessBits.push(esc(perfs.join(', ')));
      metaText = guessBits.length ? guessBits.join(' · ') : 'Filename guess';
      metaClass = 'qi-meta qi-meta-guess';
    }
    // No_dir reason: the file was matched to a scene but has no
    // destination folder. The chip text + tooltip distinguishes between
    // three states so the user isn't told "no folder" when they
    // actually have one of the listed performers:
    //   - in-library performers exist → "Studio missing — re-run to
    //     route via Lucy Mochi"
    //   - no in-library performers + no studio dir → "No library
    //     folder for this match"
    //   - filename-dropped names (TPDB-listed but absent from filename)
    //     are listed separately so the user knows we ignored them on
    //     purpose, not by mistake.
    let noDirChip = '';
    if (f.prev_status === 'no_dir' && f.match_external_id) {
      const _ndStudio = (f.match_studio || '').trim();
      const inLib = Array.isArray(f.performers_in_library) ? f.performers_in_library : [];
      const missingLib = Array.isArray(f.performers_missing_in_library) ? f.performers_missing_in_library : [];
      const droppedByFilename = Array.isArray(f.performers_dropped_by_filename) ? f.performers_dropped_by_filename : [];
      const tipBits = [];
      if (_ndStudio) tipBits.push('Studio "' + _ndStudio + '" has no library folder');
      if (missingLib.length) tipBits.push('No folder for: ' + missingLib.join(', '));
      if (inLib.length) tipBits.push('Library has: ' + inLib.join(', ') + ' — re-run to file here');
      if (droppedByFilename.length) tipBits.push('Dropped (not in filename): ' + droppedByFilename.join(', '));
      if (!tipBits.length) tipBits.push('No filing destination for this match');
      const _reason = tipBits.join(' · ');
      const _label = inLib.length ? 'Re-run to file' : 'No library folder';
      noDirChip = '<span class="qi-nodir-chip" title="' + esc(_reason) + '"><i class="fa-solid fa-folder-tree"></i><span class="qi-nodir-chip-msg">' + esc(_label) + '</span></span>';
    }
    const matchLinkEl = _queueMatchLinkHtml(f);
    //: Surface the stored error next to the title so the user can see
    //: what failed without opening the log. `margin-left:auto` pushes
    //: it into the empty space between the title and the headshots;
    //: `max-width:0;flex:1 1 auto` stops a long message from forcing
    //: the title to ellipsise — truncates on the error itself instead.
    const errorText = (f.prev_status === 'error' && f.error) ? String(f.error).trim() : '';
    const errorChip = errorText
      ? `<span class="qi-error-chip" title="${esc(errorText)}"><i class="fa-solid fa-triangle-exclamation"></i><span class="qi-error-chip-msg">${esc(errorText)}</span></span>`
      : '';
    // Colour the matched performers' / studio's / configured vices'
    // words inside the filename title — same `.qs-match` highlight
    // used by the search results. Lets the user spot at a glance
    // which performer / studio / vice each row corresponds to
    // without scanning the full filename.
    const _titleText = spaceifyReleaseName(f.display_name || f.filename);
    // Per-row tokens come from THIS file's matched performers + studio
    // (filtered to library studios only). Vices and other library
    // entities arrive via `_libraryAndTokenHighlight`, which paints
    // multi-word library phrases as a unit (so a vice named
    // "Hard Anal" doesn't bleed "hard" / "anal" onto unrelated rows).
    const _hl = (typeof _qsBuildHighlightSet === 'function')
      ? _qsBuildHighlightSet(
          perfs.join(' '),
          (typeof _studioIfInLibrary === 'function' ? _studioIfInLibrary(studio) : studio))
      : null;
    const _titleHtml = (typeof _libraryAndTokenHighlight === 'function')
      ? _libraryAndTokenHighlight(_titleText, _hl)
      : esc(_titleText);
    return `<div class="queue-item" id="qi-${i}" data-filename="${esc(f.filename)}">
      <div class="qi-cell">${statusEl}</div>
      <div class="qi-cell">${studioLogo}</div>
      <div class="qi-cell-name" title="${esc(f.filename)}">
        <div class="qi-title-row">
          <span class="qi-title status-${statusKey}">${_titleHtml}</span>
          ${matchLinkEl}
          ${noDirChip}
          ${errorChip}
        </div>
        ${metaText ? `<div class="${metaClass}">${metaText}</div>` : ''}
      </div>
      <div class="qi-cell qi-cell-headshots">${headshotSlot}</div>
      <div class="qi-cell"><span class="badge-size">${f.size_mb} MB</span></div>
      <div class="qi-cell">${f.has_phash ? '<span class="badge-cached" title="phash cached"><i class="fa-solid fa-fingerprint queue-phash-fa"></i></span>' : ''}${f.in_duplicate_group ? `<span class="q-dup-badge" title="Duplicate — ${esc(String(f.duplicate_group_size || 2))} files share the same phash"><i class="fa-solid fa-clone"></i><span>${esc(String(f.duplicate_group_size || 2))}</span></span>` : ''}</div>
      <div class="qi-cell qi-actions">
        <button type="button" class="btn-icon" onclick="_qCall('openQueueFilmstrip', ${i})" title="Filmstrip"><i class="fa-solid fa-photo-film"></i></button>
        <button type="button" class="btn-icon qi-filing-btn ${_viceMatchesFilename(f.filename) ? 'is-vice-match' : ''}" onclick="_qCall('openManualSearch', ${i})" title="Search / manual (includes suggested matches + Vice)"><span style="display:inline-block;width:1.1em;height:1.1em;background-color:currentColor;-webkit-mask:url(/static/logos/edit_search.webp) center/contain no-repeat;mask:url(/static/logos/edit_search.webp) center/contain no-repeat;vertical-align:middle"></span></button>
        <button type="button" class="btn-icon qi-delete-btn" onclick="_qCall('confirmDeleteQueueFile', ${i})" title="Delete"><i class="fa-solid fa-trash"></i></button>
        <button type="button" class="btn-run-file" onclick="_qCall('runQueueFile', ${i})" title="Run"><i class="fa-solid fa-play"></i></button>
      </div>
    </div>`;
  }

  function _queueMatchLinkHtml(f) {
    const id = (f.match_external_id || '').trim();
    const src = (f.match_source || '').toLowerCase();
    if (!id) return '';
    let url = '', label = '', logo = '';
    if (src === 'stashdb') {
      url = 'https://stashdb.org/scenes/' + id;
      label = 'StashDB';
      logo = '/static/logos/stashdb.webp';
    } else if (src === 'tpdb' || src === 'theporndb') {
      url = 'https://theporndb.net/scenes/' + id;
      label = 'TPDB';
      logo = '/static/logos/tpdb.webp';
    } else if (src === 'fansdb') {
      url = 'https://fansdb.cc/scenes/' + id;
      label = 'FansDB';
      logo = '/static/logos/fansdb.webp';
    } else {
      return '';
    }
    return `<a class="qi-match-link" href="${url}" target="_blank" rel="noopener noreferrer" onclick="event.stopPropagation()" title="Open on ${label}"><img src="${logo}" alt="${label}" onerror="this.replaceWith(Object.assign(document.createElement('span'),{textContent:'${label}',className:'qi-match-link-text'}))" loading="lazy"></a>`;
  }

  function _renderMovieQueueItem(f, i) {
    const statusKey = f.prev_status || 'none';
    let statusEl;
    if (f.prev_status === 'unmatched') {
      statusEl = '<i class="fa-solid fa-not-equal qi-status-icon" title="Previously unmatched"></i>';
    } else if (f.prev_status === 'no_dir') {
      // Movie queue equivalent of the scene-row tooltip — same logic:
      // surface WHY the row has no filing destination instead of just
      // saying "No directory".
      const _ndStudio = (f.match_studio || '').trim();
      const _ndBits = ['Identified — but no Features folder'];
      if (_ndStudio) _ndBits.push('Studio "' + _ndStudio + '" has no path mapped');
      const _ndTip = _ndBits.join(' · ');
      statusEl = '<i class="fa-solid fa-equals qi-status-icon" title="' + esc(_ndTip) + '"></i>';
    } else {
      statusEl = `<span class="status-dot-queue ${statusKey}" title="${esc(statusKey)}"></span>`;
    }
    const matchBits = [];
    if (f.match_title)  matchBits.push(esc(f.match_title));
    if (f.match_source) matchBits.push(esc(f.match_source));
    const metaText = matchBits.join(' · ');
    return `<div class="queue-item" id="qi-${i}" data-filename="${esc(f.filename)}">
      <div class="qi-cell">${statusEl}</div>
      <div class="qi-cell"></div>
      <div class="qi-cell-name" title="${esc(f.filename)}">
        <div class="qi-title status-${statusKey}">${esc(spaceifyReleaseName(f.display_name || f.filename))}</div>
        ${metaText ? `<div class="qi-meta">${metaText}</div>` : ''}
      </div>
      <div class="qi-cell"></div>
      <div class="qi-cell"><span class="badge-size">${f.size_mb} MB</span></div>
      <div class="qi-cell"></div>
      <div class="qi-cell qi-actions">
        <button type="button" class="btn-icon" onclick="_qCall('openMovieManualSearch', ${i})" title="Search"><i class="fa-solid fa-magnifying-glass"></i></button>
        <button type="button" class="btn-icon qi-delete-btn" onclick="_qCall('confirmDeleteQueueFile', ${i})" title="Delete"><i class="fa-solid fa-trash"></i></button>
        <button type="button" class="btn-run-file" onclick="_qCall('runQueueFile', ${i})" title="Auto-match"><i class="fa-solid fa-play"></i></button>
      </div>
    </div>`;
  }

  function toggleQueueMatchedOnly() {
    _queueMatchedOnly = !_queueMatchedOnly;
    const btn = document.getElementById('btnQueueMatchedOnly');
    if (btn) {
      btn.classList.toggle('is-active', _queueMatchedOnly);
      btn.setAttribute('aria-pressed', _queueMatchedOnly ? 'true' : 'false');
      btn.title = _queueMatchedOnly ? 'Showing matched items — click to show all' : 'Show only matched items';
    }
    _queuePage = 0;
    _recomputeQueueFilter();
    _renderQueuePage();
  }

  function toggleQueueVicesOnly() {
    _queueVicesOnly = !_queueVicesOnly;
    const btn = document.getElementById('btnQueueVicesOnly');
    if (btn) {
      btn.classList.toggle('is-active', _queueVicesOnly);
      btn.setAttribute('aria-pressed', _queueVicesOnly ? 'true' : 'false');
      btn.title = _queueVicesOnly ? 'Showing vice matches — click to show all' : 'Show only files that match a vice';
    }
    _queuePage = 0;
    _recomputeQueueFilter();
    _renderQueuePage();
  }

  function toggleQueueErrorsOnly() {
    _queueErrorsOnly = !_queueErrorsOnly;
    //: Keep the status-filter in step so the Errors stat-card and the
    //: filter chip reflect the same state as this toggle.
    _queueStatusFilter = _queueErrorsOnly ? 'error' : '';
    const btn = document.getElementById('btnQueueErrorsOnly');
    if (btn) {
      btn.classList.toggle('is-active', _queueErrorsOnly);
      btn.setAttribute('aria-pressed', _queueErrorsOnly ? 'true' : 'false');
      btn.title = _queueErrorsOnly ? 'Showing errored files — click to show all' : 'Show only errored files';
    }
    _queuePage = 0;
    _recomputeQueueFilter();
    _renderQueuePage();
    _renderQueueStatusChip();
  }

  // Alphabetical sort on the file's display name (or raw filename
  // fallback), case-insensitive, using localeCompare so accented
  // characters order naturally. `numeric: true` keeps "scene 2" before
  // "scene 10" instead of the lexicographic "scene 10 < scene 2".
  function _cmpQueueFiles(a, b) {
    const an = (a.display_name || a.filename || '').toLowerCase();
    const bn = (b.display_name || b.filename || '').toLowerCase();
    return an.localeCompare(bn, undefined, { numeric: true, sensitivity: 'base' });
  }

  function _recomputeQueueFilter() {
    const term = (document.getElementById('queueFilter')?.value || '').toLowerCase().trim();
    const files = window._queueFiles || [];
    const statusFilter = _queueStatusFilter;
    _queueFilteredIndexes = [];
    files.forEach((f, i) => {
      if (statusFilter && (f.prev_status || '') !== statusFilter) return;
      if (_queueMatchedOnly) {
        const src = (f.match_source || '').trim();
        const ext = (f.match_external_id || '').trim();
        if (!src || !ext) return;
      }
      if (_queueVicesOnly && !_viceMatchesFilename(f.filename || '')) return;
      if (term) {
        const name = (f.filename || '').toLowerCase();
        const display = spaceifyReleaseName(f.display_name || f.filename || '').toLowerCase();
        if (!name.includes(term) && !display.includes(term)) return;
      }
      _queueFilteredIndexes.push(i);
    });
  }

  //: Filed files are moved out of source_dir after processing, so they
  //: don't appear in the queue — clicking the Filed card jumps to the
  //: history page which is where the filed rows actually live. Scope
  //: to the current queueMode (scenes/movies) so each card lands on
  //: the matching history view.
  function openFiledHistory() {
    const kind = (queueMode === 'movies') ? '&kind=movie' : '';
    window.location.href = `/history?status=filed${kind}`;
  }

  //: Clicking Unmatched Scenes / Unmatched Movies needs to flip the
  //: queue to the right mode first — otherwise the filter does
  //: nothing because the other queue is rendered. setQueueMode's
  //: preserveFilter arg stops it from clearing _queueStatusFilter
  //: before we re-set it.
  // Wipe every queue filter and re-render. Wired onto the Queue stat
  // tile so a single click resets the view: status filter, errors-only
  // toggle, free-text filter, and pagination all return to the
  // "everything" baseline.
  function clearAllQueueFilters() {
    _queueStatusFilter = '';
    _queueErrorsOnly = false;
    const errBtn = document.getElementById('btnQueueErrorsOnly');
    if (errBtn) {
      errBtn.classList.remove('is-active');
      errBtn.setAttribute('aria-pressed', 'false');
      errBtn.title = 'Show only errored files';
    }
    const qf = document.getElementById('queueFilter');
    if (qf) qf.value = '';
    _queuePage = 0;
    _recomputeQueueFilter();
    _renderQueuePage();
    _renderQueueStatusChip();
    document.querySelector('.queue-section')?.scrollIntoView({behavior:'smooth', block:'start'});
  }
  window.clearAllQueueFilters = clearAllQueueFilters;

  function filterQueueByStatusScoped(status, mode) {
    const want = mode === 'movies' ? 'movies' : 'scenes';
    if (queueMode !== want) {
      _queueStatusFilter = status || '';
      setQueueMode(want, true);
      // setQueueMode → loadQueue → _renderQueuePage picks up the
      // pre-set filter; _renderQueueStatusChip needs an extra nudge
      // because loadQueue doesn't call it.
      _renderQueueStatusChip();
    } else {
      filterQueueByStatus(status);
    }
  }

  function filterQueueByStatus(status) {
    const next = status || '';
    // Switching to a scene-side filter from movies mode: flip to scenes
    // first so the renderer uses _renderSceneQueueItem. setQueueMode
    // triggers loadQueue which eventually calls _recomputeQueueFilter,
    // so the status filter needs to be set AFTER the mode switch.
    _queueStatusFilter = next;
    //: Keep the Errors toggle button in sync whenever the status
    //: filter is set or cleared from elsewhere (stat-card click, chip
    //: close, etc).
    _queueErrorsOnly = (next === 'error');
    const errBtn = document.getElementById('btnQueueErrorsOnly');
    if (errBtn) {
      errBtn.classList.toggle('is-active', _queueErrorsOnly);
      errBtn.setAttribute('aria-pressed', _queueErrorsOnly ? 'true' : 'false');
      errBtn.title = _queueErrorsOnly ? 'Showing errored files — click to show all' : 'Show only errored files';
    }
    if (next === 'no_dir' && _queueActiveKind !== 'scenes') {
      setQueueMode('scenes', true);
    } else {
      _queuePage = 0;
      _recomputeQueueFilter();
      _renderQueuePage();
    }
    _renderQueueStatusChip();
    document.querySelector('.queue-section')?.scrollIntoView({behavior:'smooth', block:'start'});
  }

  function _renderQueueStatusChip() {
    const panel = document.getElementById('queuePanelTitle');
    if (!panel) return;
    const existing = document.getElementById('queueStatusFilterChip');
    if (existing) existing.remove();
    const status = _queueStatusFilter;
    if (!status) return;
    const label = status === 'no_dir' ? 'No Directory'
      : status === 'error'  ? 'Errors'
      : status;
    panel.insertAdjacentHTML('beforeend', `<span id="queueStatusFilterChip" style="margin-left:10px;font-size:11px;font-weight:600;letter-spacing:0.04em;text-transform:uppercase;color:#60a5fa;background:rgba(96,165,250,0.14);border:1px solid rgba(96,165,250,0.35);padding:2px 8px;border-radius:4px;cursor:pointer" onclick="filterQueueByStatus('')" title="Clear filter">${label} ✕</span>`);
  }

  //: Signature of the last successfully rendered queue slice — used
  //: to skip the expensive innerHTML rewrite (and the image fetches
  //: it retriggers) when the poll tick delivers the exact same data.
  //: Cleared whenever the mode flips or the file list materially
  //: changes in a way the signature wouldn't otherwise catch.
  let _lastRenderedSig = '';

  function _computeRenderSig(slice, files) {
    //: Only include fields that actually affect the rendered output.
    //: Skip transient flags (size_mb drifts during download but the
    //: row looks identical), scroll position, selection — those are
    //: handled out-of-band without a rebuild.
    const parts = [_queueActiveKind, _queuePage, _queuePageSize, _queueFilteredIndexes.length];
    for (const i of slice) {
      const f = files[i];
      if (!f) continue;
      parts.push(
        f.filename, f.prev_status || '', f.match_source || '',
        f.match_external_id || '', f.match_studio || '', f.performers || '',
        f.size_mb || 0, f.has_phash ? 1 : 0, f.error || '', f.match_guess ? 1 : 0,
      );
    }
    return parts.join('|');
  }

  function _renderQueuePage() {
    const list = document.getElementById('queueList');
    const pager = document.getElementById('queuePager');
    const files = window._queueFiles || [];
    //: The search panel lives inside queueList (docked under the
    //: active row) while a search is in flight. If we let the
    //: innerHTML rewrite below run while the panel is still a child
    //: of queueList, it'd be destroyed along with the old rows —
    //: and `document.getElementById('qSearchPanel')` would then
    //: return null, so subsequent clicks on the row's Search button
    //: would silently fail (and never re-open the panel). Undock
    //: first so the panel keeps its DOM identity, then re-dock after
    //: the new rows render.
    const sp = document.getElementById('qSearchPanel');

    if (!files.length) {
      if (list && list.contains(sp)) _undockSearchPanel();
      if (list) list.innerHTML = `<div class="empty">${_queueActiveKind === 'movies' ? 'No video files in the Movies input folder (Settings → Movies). That path is not the same as “Movies download complete”.' : 'No files in queue'}</div>`;
      if (pager) pager.style.display = 'none';
      _lastRenderedSig = '';
      return;
    }
    if (!_queueFilteredIndexes.length) {
      if (list && list.contains(sp)) _undockSearchPanel();
      if (list) list.innerHTML = '<div class="empty">No files match the filter</div>';
      if (pager) pager.style.display = 'none';
      _lastRenderedSig = '';
      return;
    }
    const total = _queueFilteredIndexes.length;
    const pages = Math.max(1, Math.ceil(total / _queuePageSize));
    if (_queuePage >= pages) _queuePage = pages - 1;
    if (_queuePage < 0) _queuePage = 0;
    const start = _queuePage * _queuePageSize;
    const end   = Math.min(start + _queuePageSize, total);
    const renderFn = _queueActiveKind === 'movies' ? _renderMovieQueueItem : _renderSceneQueueItem;
    const slice = _queueFilteredIndexes.slice(start, end);

    //: Diff check — if the slice's render-relevant state is
    //: bit-identical to the last paint, skip the innerHTML write.
    //: Selection / dock / pager fiddling below still runs in case
    //: the user clicked around without actual data changing.
    const sig = _computeRenderSig(slice, files);
    const skipRebuild = sig === _lastRenderedSig && list && list.children.length > 0;

    if (!skipRebuild) {
      if (list && list.contains(sp)) _undockSearchPanel();
      list.innerHTML = slice.map(i => renderFn(files[i], i)).join('');
      if (_queueActiveKind === 'scenes') _hydrateQueueHeadshots();
      _lastRenderedSig = sig;
    }

    // Modal-mode: the search panel lives in its own overlay layer
    // (`#qSearchOverlay`), not in the queue list, so we no longer
    // dock the panel under the active row. We still mark the row
    // active so the user can see which file the search is bound to
    // when they dismiss the modal.
    if (_qSelectedFile) {
      document.querySelectorAll('.queue-section .queue-item.active').forEach(el => el.classList.remove('active'));
      const idx = (window._queueFiles || []).findIndex(f => f.filename === _qSelectedFile);
      const activeEl = idx !== -1 ? document.getElementById(`qi-${idx}`) : null;
      if (activeEl) activeEl.classList.add('active');
    }

    if (pager) {
      pager.style.display = 'flex';
      const status = document.getElementById('queuePagerStatus');
      if (status) {
        status.textContent = `${start + 1}–${end} of ${total} · page ${_queuePage + 1}/${pages}`;
      }
      document.getElementById('queuePagerFirst').disabled = _queuePage === 0;
      document.getElementById('queuePagerPrev').disabled  = _queuePage === 0;
      document.getElementById('queuePagerNext').disabled  = _queuePage >= pages - 1;
      document.getElementById('queuePagerLast').disabled  = _queuePage >= pages - 1;
    }
    //: Re-apply filing-in-progress shimmer to any visible row whose
    //: filename is in the last broadcast from /api/activity/banner.
    //: Necessary because innerHTML rewrites blow away the class; the
    //: banner only re-broadcasts on change, so the row would otherwise
    //: lose its shimmer between renders while the move is still active.
    _applyFilingNowClasses();
  }

  // Filing-in-progress shimmer wiring. The activity banner script
  // fires `ts:filing-now` with `{filenames: [...]}` each time the
  // backend's _filing_in_progress set changes. We track the last
  // received set so newly-rendered rows pick up the class too.
  let _filingNowFilenames = new Set();
  function _applyFilingNowClasses() {
    const items = document.querySelectorAll('.queue-section .queue-item[data-filename]');
    items.forEach(el => {
      const fn = el.getAttribute('data-filename') || '';
      const on = fn && _filingNowFilenames.has(fn);
      el.classList.toggle('qi-filing-active', !!on);
    });
  }
  window.addEventListener('ts:filing-now', (ev) => {
    const list = (ev && ev.detail && Array.isArray(ev.detail.filenames)) ? ev.detail.filenames : [];
    _filingNowFilenames = new Set(list);
    _applyFilingNowClasses();
  });

  function queuePagerStep(delta) {
    _queuePage += delta;
    _renderQueuePage();
  }
  function queuePagerGoto(page) {
    const pages = Math.max(1, Math.ceil(_queueFilteredIndexes.length / _queuePageSize));
    _queuePage = Math.max(0, Math.min(pages - 1, page === Infinity ? pages - 1 : page));
    _renderQueuePage();
  }
  function queuePagerSetSize(size) {
    const n = parseInt(size, 10);
    if (!Number.isNaN(n) && n > 0) {
      _queuePageSize = n;
      _queuePage = 0;
      _renderQueuePage();
    }
  }

  function _applyMovieQueuePayload(d, preserveView) {
    const savedScroll = preserveView ? window.scrollY : 0;
    const savedPage = preserveView ? _queuePage : 0;
    const qc = document.getElementById('queueCount');
    if (qc) { qc.textContent = d.files?.length ? `-${d.files.length}-` : ''; qc.style.color = 'var(--text)'; }
    const statQ = document.getElementById('statQueue'); if (statQ) statQ.textContent = d.files?.length || 0;
    window._movieQueueStatusCounts = d.status_counts || null;
    if (_queueStatsPayload) _paintQueueStats(_queueStatsPayload);
    _syncHeaderQueuePill(d.files?.length || 0);
    if (d.error) {
      document.getElementById('queueList').innerHTML = `<div class="empty">${esc(d.error)}</div>`;
      const pager = document.getElementById('queuePager');
      if (pager) pager.style.display = 'none';
      window._queueFiles = [];
      _queueFilteredIndexes = [];
      return;
    }
    window._queueFiles = (d.files || []).slice().sort(_cmpQueueFiles);
    _queueActiveKind = 'movies';
    _queuePage = preserveView ? savedPage : 0;
    _recomputeQueueFilter();
    _renderQueuePage();
    if (preserveView) requestAnimationFrame(() => window.scrollTo({ top: savedScroll }));
  }

  async function loadMovieQueue(opts) {
    const preserveView = !!(opts && opts.preserveView);
    const force = !!(opts && opts.force);
    const mountGen = _queueLoadGen;
    if (!force && _movieQueuePayload) {
      if (mountGen !== _queueLoadGen) return;
      _applyMovieQueuePayload(_movieQueuePayload, preserveView);
      return;
    }
    try {
      const r = await fetch('/api/movies/queue');
      if (mountGen !== _queueLoadGen) return;
      const d = await r.json();
      if (mountGen !== _queueLoadGen) return;
      _movieQueuePayload = d;
      _applyMovieQueuePayload(d, preserveView);
    } catch {
      if (mountGen !== _queueLoadGen) return;
      document.getElementById('queueList').innerHTML = '<div class="empty">Error loading movie queue</div>';
      const pager = document.getElementById('queuePager');
      if (pager) pager.style.display = 'none';
    }
  }

  // Vice slug cache — mirrors the /queue implementation so per-row
  // Vice button flames reflect current vice config. Yellow if no vice
  // keyword is found in the filename, fire-orange when one matches.
  let _viceSlugs = [];
  function _slugify(s) {
    return String(s || '').toLowerCase()
      .replace(/[^a-z0-9]+/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
  }
  function _viceMatchesFilename(fn) {
    if (!_viceSlugs.length) return false;
    const t = _slugify(fn);
    if (!t) return false;
    const padded = ' ' + t + ' ';
    const spaceless = t.replace(/\s+/g, '');
    for (const s of _viceSlugs) {
      if (!s) continue;
      if (padded.indexOf(' ' + s + ' ') >= 0) return true;
      const ss = s.replace(/\s+/g, '');
      if (ss.length >= 8 && spaceless.indexOf(ss) >= 0) return true;
    }
    return false;
  }
  // Display names (used for `_qsBuildHighlightSet` so vice tokens
  // light up in queue row titles and other contextual lists).
  let _viceNames = [];
  // Lower-cased set of every library studio's folder_name + display
  // name. Used to filter the highlight set so a candidate's `studio`
  // field only paints with `.qs-match` when it actually corresponds
  // to a library folder. Stops studios the user doesn't have a folder
  // for (e.g. random TPDB studio on a search result) from lighting up
  // in titles where they'd just be noise.
  async function _refreshViceSlugs() {
    if (_viceSlugs.length && (Date.now() - _viceSlugsFetchedAt) < _VICE_SLUGS_TTL_MS) return;
    try {
      const r = await fetch('/api/vices', { credentials: 'same-origin' });
      const d = await r.json();
      const vices = d.vices || [];
      _viceSlugs = vices.map(v => _slugify(v.name)).filter(Boolean);
      _viceNames = vices.map(v => v.name).filter(Boolean);
      _viceSlugsFetchedAt = Date.now();
    } catch(_) { /* keep stale slugs */ }
  }

  // ── IAFD bulk matcher ────────────────────────────────────────────
  //: State for the bulk matcher: groups returned by the backend,
  //: tick state for each (group_idx, file_idx) pair, and the performer
  //: the scan was launched for (used as a display label and for
  //: re-applying the scene-pick heuristic when the user changes the
  //: scene radio in a row).
  let _iafdBulkGroups = [];
  let _iafdBulkTicked = {};   // key `${gi}:${fi}` -> bool
  //: Per-row special-feature kind. Seeded from the filename detection
  //: on render; an empty string means "no marker" (the user explicitly
  //: chose --None-- or the filename didn't match any pattern). The
  //: bulk-file POST attaches this to each item's scene as `_extra_kind`
  //: so the server uses the explicit choice instead of re-detecting.
  let _iafdBulkKinds = {};    // key `${gi}:${fi}` -> string ("" / "BTS" / …)
  //: Full filmography returned by the bulk-scan, used to seed the
  //: per-row "Film" type-to-complete (a `<datalist>` lookup). Keyed by
  //: label ("Title (Year)") → film object so the change handler can
  //: resolve typed values back to a URL + studio + year.
  let _iafdFilmography = [];          // list[{url,title,year,studio}]
  let _iafdFilmByLabel = new Map();   // label → film entry
  let _iafdBulkPerformer = '';
  let _iafdBulkFiling = false;
  //: AbortController for the in-flight bulk-scan fetch so the user
  //: can cancel a slow 100-file scan without waiting for it.
  //: Client-side only — the server keeps grinding but its result is
  //: discarded, which is the pragmatic middle ground without
  //: threading per-request cancellation through the IAFD helpers.
  let _iafdBulkAbortCtl = null;

  //: Build the performer list from window._queueFiles. Each queue row
  //: already carries a `performers` field (comma / pipe separated —
  //: either history's filed cast or the library-guess from the
  //: filename parser). We count files per performer and sort by
  //: count desc so the pickable performers most relevant to the
  //: current queue surface first. Empty field → skipped.
  function _buildIafdPerformerIndex() {
    const index = new Map();  // performer_name_lower -> { name, count, filenames:[] }
    for (const f of (window._queueFiles || [])) {
      const raw = (f.performers || '').trim();
      if (!raw) continue;
      const names = raw.replace(/\|/g, ',').replace(/ \/ /g, ',').split(',').map(s => s.trim()).filter(Boolean);
      const seenInFile = new Set();
      for (const n of names) {
        const key = n.toLowerCase();
        if (seenInFile.has(key)) continue;
        seenInFile.add(key);
        let entry = index.get(key);
        if (!entry) {
          entry = { name: n, count: 0, filenames: [] };
          index.set(key, entry);
        }
        entry.count += 1;
        entry.filenames.push(f.filename);
      }
    }
    return [...index.values()].sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));
  }

  function openIafdBulkPicker() {
    const entries = _buildIafdPerformerIndex();
    const list = document.getElementById('qIafdPerformerList');
    if (!entries.length) {
      list.innerHTML = '<div class="qs-perf-empty" style="padding:18px;text-align:center">No stars detected in the current queue — files need a matched or library-guessed star name before the bulk matcher can target them.</div>';
    } else {
      //: Render as a <div> (not <label>) to avoid the double-fire bug:
      //: a <label> wrapping an <input> synthesises a second click on
      //: the input when the label is clicked, and that click bubbles
      //: back up through the label and re-fires the onclick handler —
      //: which was starting TWO bulk scans per click. Plain <div>
      //: click has no synthetic re-dispatch, so the handler fires
      //: exactly once. Radio is still there so the visual selection
      //: still reads correctly.
      list.innerHTML = entries.map((e, i) =>
        `<div class="qs-perf-row" style="cursor:pointer" onclick="launchIafdBulkScan(${i}, event)">
          <input type="radio" name="qIafdPerfRadio" tabindex="-1" style="accent-color:rgba(var(--brand-purple-rgb),0.85);pointer-events:none">
          <span style="flex:1;min-width:0;display:flex;align-items:center;justify-content:space-between;gap:12px">
            <span>${esc(e.name)}</span>
            <span style="font-size:11px;color:var(--dim)">${e.count} file${e.count === 1 ? '' : 's'}</span>
          </span>
        </div>`
      ).join('');
    }
    //: Cache the entries on window so the click handler can look up
    //: filenames without re-walking the queue (keeps the picker
    //: stable even if the poll refreshes the queue between open and
    //: click).
    window._iafdPerfEntries = entries;
    document.getElementById('qIafdPickerModal').classList.add('open');
  }

  function closeIafdBulkPicker() {
    document.getElementById('qIafdPickerModal').classList.remove('open');
  }

  //: Progress poll token — ensures the poll loop bails when the
  //: scan finishes (or is cancelled) without leaking between runs.
  let _iafdBulkProgressToken = 0;
  //: The SVG arc's total dasharray (2πr where r=34) — used to
  //: translate a 0..1 progress fraction into stroke-dashoffset.
  const IAFD_SCAN_ARC_TOTAL = 213.6;

  function _iafdSetScanProgress(done, total, current) {
    const pct = total > 0 ? Math.max(0, Math.min(1, done / total)) : 0;
    const arc = document.getElementById('qIafdScanArc');
    if (arc) arc.setAttribute('stroke-dashoffset', String(IAFD_SCAN_ARC_TOTAL * (1 - pct)));
    const pctEl = document.getElementById('qIafdScanPct');
    if (pctEl) pctEl.textContent = `${Math.round(pct * 100)}%`;
    const counterEl = document.getElementById('qIafdScanCounter');
    if (counterEl) counterEl.textContent = total > 0 ? `${done} / ${total}` : '0 / 0';
    const tgtEl = document.getElementById('qIafdScanTarget');
    if (tgtEl) tgtEl.textContent = current ? current : '';
  }

  async function _iafdPollScanProgress(token) {
    // Capture the mount-gen so the loop bails on page navigation —
    // _tearDownQueuePage bumps _queueLoadGen on pagehide.
    const mountGen = _queueLoadGen;
    let consecutiveFailures = 0;
    let warnedStale = false;
    const STALE_WARN_AFTER = 10; // ~5 s of failed polls before warning the user.
    while (_iafdBulkProgressToken === token) {
      if (mountGen !== _queueLoadGen) return;
      try {
        const r = await fetch('/api/iafd/bulk-scan-status');
        if (!r.ok) throw new Error('HTTP ' + r.status);
        const d = await r.json();
        if (_iafdBulkProgressToken !== token || mountGen !== _queueLoadGen) return;
        consecutiveFailures = 0;
        if (warnedStale) {
          warnedStale = false;
          const tgtEl = document.getElementById('qIafdScanTarget');
          if (tgtEl && tgtEl.dataset.staleHint) {
            tgtEl.classList.remove('is-stale');
            delete tgtEl.dataset.staleHint;
          }
        }
        _iafdSetScanProgress(d.done || 0, d.total || 0, d.current || '');
        if (!d.running && (d.done || 0) >= (d.total || 0) && (d.total || 0) > 0) return;
      } catch (e) {
        consecutiveFailures++;
        if (!warnedStale && consecutiveFailures >= STALE_WARN_AFTER) {
          warnedStale = true;
          if (typeof console !== 'undefined') console.warn('[queue] IAFD progress poll stale', e);
          const tgtEl = document.getElementById('qIafdScanTarget');
          if (tgtEl) {
            tgtEl.dataset.staleHint = '1';
            tgtEl.classList.add('is-stale');
            tgtEl.textContent = 'Progress unavailable — scan still running, results will appear when done';
          }
        }
      }
      await new Promise(res => setTimeout(res, 500));
    }
  }

  //: Reentrancy guard — prevents two scans from running in parallel
  //: (and two results modals opening stacked) if the click handler
  //: somehow fires twice, or if the user double-clicks the picker.
  let _iafdBulkScanInFlight = false;

  async function launchIafdBulkScan(entryIdx, evt) {
    if (evt) evt.stopPropagation();
    if (_iafdBulkScanInFlight) return;
    const entries = window._iafdPerfEntries || [];
    const entry = entries[entryIdx];
    if (!entry) return;
    _iafdBulkScanInFlight = true;
    closeIafdBulkPicker();
    _iafdBulkPerformer = entry.name;
    //: Reset the ring + counter so a previous run's numbers don't
    //: flash before the first poll resolves.
    _iafdSetScanProgress(0, entry.filenames.length, '');
    document.getElementById('qIafdScanModal').classList.add('open');
    _iafdBulkAbortCtl = ('AbortController' in window) ? new AbortController() : null;
    //: Kick off the progress poll alongside the scan POST. The
    //: token is a generation counter so the poll self-terminates
    //: when this scan ends (or gets cancelled mid-flight).
    const token = ++_iafdBulkProgressToken;
    _iafdPollScanProgress(token);
    try {
      const r = await fetch('/api/iafd/bulk-scan', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ performer: entry.name, filenames: entry.filenames }),
        signal: _iafdBulkAbortCtl ? _iafdBulkAbortCtl.signal : undefined,
      });
      const d = await r.json();
      _iafdBulkProgressToken += 1;   // stop the poll loop
      document.getElementById('qIafdScanModal').classList.remove('open');
      _iafdBulkAbortCtl = null;
      _iafdBulkScanInFlight = false;
      if (d.error) { window.toast(`Scan failed: ${d.error}`); return; }
      _renderIafdBulkResults(d);
    } catch (e) {
      _iafdBulkProgressToken += 1;
      document.getElementById('qIafdScanModal').classList.remove('open');
      _iafdBulkAbortCtl = null;
      _iafdBulkScanInFlight = false;
      //: AbortError is the user clicking Cancel — silent, no alert.
      if (e && (e.name === 'AbortError' || (e.message || '').toLowerCase().includes('abort'))) return;
      window.toast(`Scan failed: ${e.message || e}`);
    }
  }

  function cancelIafdBulkScan() {
    _iafdBulkProgressToken += 1;
    _iafdBulkScanInFlight = false;
    if (_iafdBulkAbortCtl) {
      try { _iafdBulkAbortCtl.abort(); } catch(_) {}
      _iafdBulkAbortCtl = null;
    }
    document.getElementById('qIafdScanModal').classList.remove('open');
  }

  //: Build/refresh the shared filmography datalist. One <datalist> is
  //: injected at the modal root and referenced by every row's film
  //: picker via ``list="iafdFilmographyOptions"``. Options carry the
  //: full "Title (Year)" label as value so the user can type any
  //: substring and the browser filters in place; ``data-url`` is the
  //: ground truth the change handler looks up.
  function _renderIafdFilmographyDatalist() {
    let dl = document.getElementById('iafdFilmographyOptions');
    if (!dl) {
      dl = document.createElement('datalist');
      dl.id = 'iafdFilmographyOptions';
      document.body.appendChild(dl);
    }
    dl.innerHTML = _iafdFilmography.map(f => {
      const lab = _filmLabel(f);
      return `<option value="${(window.esc || ((s)=>s))(lab)}" data-url="${(window.esc || ((s)=>s))(f.url || '')}">${(window.esc || ((s)=>s))([f.studio || '', f.year || ''].filter(Boolean).join(' · '))}</option>`;
    }).join('');
  }
  function _filmLabel(f) {
    if (!f) return '';
    const title = (f.title || '').trim();
    const year = (f.year || '').trim();
    return year ? `${title} (${year})` : title;
  }
  //: Accepts the full /api/iafd/bulk-scan response shape, including
  //: ``unmatched`` filenames (rendered as a synthetic group at the
  //: bottom so the user can still tick + manually assign a film) and
  //: ``filmography`` (drives the per-row Film type-to-complete).
  //: Tolerates the legacy bare-list shape for safety while in-flight
  //: scans drain.
  function _renderIafdBulkResults(payload) {
    const data = Array.isArray(payload) ? { groups: payload, unmatched: [], filmography: [] } : (payload || {});
    const baseGroups = (data.groups || []).slice();
    _iafdFilmography = data.filmography || [];
    _iafdFilmByLabel = new Map();
    for (const f of _iafdFilmography) {
      const lab = _filmLabel(f);
      if (lab) _iafdFilmByLabel.set(lab.toLowerCase(), f);
    }
    _renderIafdFilmographyDatalist();
    //: Synthetic group for unmatched filenames. ``iafd_url`` empty
    //: marks the placeholder group so the renderer + file-payload
    //: builder can special-case it (orange heading + each row needs
    //: a film pick before it can file).
    const unmatched = data.unmatched || [];
    if (unmatched.length) {
      baseGroups.push({
        iafd_url:    '',
        iafd_id:     '',
        iafd_title:  '— Unmatched —',
        studio:      '',
        date:        '',
        year:        '',
        all_scenes:  [],
        movie_cast:  [],
        files:       unmatched.map(fn => ({
          filename:                  fn,
          parsed_title:              '',
          parsed_site:               '',
          parsed_date:               '',
          scene_idx:                 -1,
          scene_number_in_filename:  null,
          proposed_performers:       [],
        })),
      });
    }
    _iafdBulkGroups = baseGroups;
    _iafdBulkTicked = {};
    _iafdBulkKinds = {};
    const wrap = document.getElementById('qIafdResultsGroups');
    const empty = document.getElementById('qIafdResultsEmpty');
    document.getElementById('qIafdResultsTitle').textContent = `Results for ${_iafdBulkPerformer}`;
    document.getElementById('qIafdResultsProgress').textContent = '';
    const fileBtn = document.getElementById('qIafdFileSelectedBtn');
    if (!baseGroups.length) {
      wrap.innerHTML = '';
      empty.style.display = '';
      if (fileBtn) fileBtn.disabled = true;
    } else {
      empty.style.display = 'none';
      const isUnmatchedGroup = (g) => !g || !g.iafd_url;
      wrap.innerHTML = baseGroups.map((g, gi) => {
        const sceneOpts = (g.all_scenes || []).map((sc, si) =>
          `<option value="${si}">${esc(sc.label || ('Scene ' + (sc.number || (si + 1))))} — ${esc((sc.cast || []).join(', '))}</option>`
        ).join('');
        const movieLine = [g.studio, g.year].filter(Boolean).map(esc).join(' · ');
        const fileRows = (g.files || []).map((f, fi) => {
          //: Default-tick any row where we landed on a specific scene
          //: (scene_idx >= 0) — that's the case where either the
          //: filename's Scene N or a cast-match pinned it. Rows
          //: without a narrowed scene stay unticked so the user
          //: reviews them. Unmatched-group rows default off too —
          //: they need a film pick before they can file.
          const defaultOn = !isUnmatchedGroup(g) && f.scene_idx >= 0;
          const key = `${gi}:${fi}`;
          _iafdBulkTicked[key] = defaultOn;
          //: Seed per-row override fields so the renderer can read the
          //: current pick without branching on which dict it lives in.
          //: Empty `_iafd_url` on the unmatched group ⇒ no film picked.
          if (typeof f._iafd_url === 'undefined') {
            f._iafd_url   = isUnmatchedGroup(g) ? '' : (g.iafd_url || '');
            f._film_title = isUnmatchedGroup(g) ? '' : (g.iafd_title || '');
            f._film_year  = isUnmatchedGroup(g) ? '' : (g.year || '');
            f._film_studio= isUnmatchedGroup(g) ? '' : (g.studio || '');
            f._all_scenes = isUnmatchedGroup(g) ? [] : (g.all_scenes || []);
            f._movie_cast = isUnmatchedGroup(g) ? [] : (g.movie_cast || []);
          }
          const sceneLabel = f.scene_idx >= 0 && (f._all_scenes || []).length
            ? esc((f._all_scenes[f.scene_idx] || {}).label || ('Scene ' + (f.scene_idx + 1)))
            : (f._iafd_url
                ? '<span style="color:#f59e0b">Movie-level cast (no scene narrowed)</span>'
                : '<span style="color:#fca5a5">Pick a film →</span>');
          const perfNames = (f.proposed_performers || [])
            .map(p => (p.performer && p.performer.name) || '')
            .filter(Boolean)
            .join(', ');
          const sceneSelect = (f._all_scenes || []).length
            ? `<select class="field-input iafd-bulk-scene-select" id="iafdBulkScene-${gi}-${fi}" style="font-size:11px;padding:3px 6px;min-width:200px;max-width:260px" onchange="iafdBulkUpdateSceneIdx(${gi}, ${fi}, this.value)">
                <option value="-1"${f.scene_idx < 0 ? ' selected' : ''}>Movie-level cast</option>
                ${(f._all_scenes || []).map((sc, si) => `<option value="${si}"${f.scene_idx === si ? ' selected' : ''}>${esc(sc.label || ('Scene ' + (sc.number || (si + 1))))}</option>`).join('')}
              </select>`
            : `<span class="iafd-bulk-scene-empty" id="iafdBulkScene-${gi}-${fi}" style="font-size:11px;color:var(--dim);padding:3px 6px">No breakdown available</span>`;
          //: Film picker — type-to-complete against the full
          //: filmography (shared <datalist>). Empty for unmatched
          //: rows; pre-filled with the current auto-match otherwise.
          //: ``onchange`` fires on blur / Enter; ``oninput`` is too
          //: chatty (fires on every keystroke). The change handler
          //: resolves the typed label back to a URL via the
          //: filmography map and fetches scene breakdowns if needed.
          const filmInputValue = f._iafd_url ? _filmLabel({ title: f._film_title, year: f._film_year }) : '';
          const filmInput = `<input type="text" list="iafdFilmographyOptions" class="field-input iafd-bulk-film-input" id="iafdBulkFilm-${gi}-${fi}" value="${esc(filmInputValue)}" placeholder="Type to search filmography…" style="font-size:11px;padding:5px 8px;width:100%;min-width:240px" onchange="iafdBulkChangeFilm(${gi}, ${fi}, this.value)" title="Type to filter the star's full IAFD filmography. Picking a different film here re-fetches scene breakdowns for this row.">`;
          //: Per-row Kind picker — defaults to the filename detection
          //: (BTS / Bloopers / Outtakes / …), falling back to "(none)"
          //: when nothing matched. The selected value is sent verbatim
          //: as ``_extra_kind`` on the scene at file-time, so an empty
          //: string force-disables the marker even on filenames that
          //: would otherwise auto-detect.
          const detectedKind = (typeof _detectExtraKind === 'function') ? _detectExtraKind(f.filename) : '';
          _iafdBulkKinds[key] = detectedKind;
          const kindOpts = [
            ['',               '— None —'],
            ['BTS',            'BTS'],
            ['Bloopers',       'Bloopers'],
            ['Outtakes',       'Outtakes'],
            ['Making Of',      'Making Of'],
            ['Interview',      'Interview'],
            ['Trailer',        'Trailer'],
            ['Deleted Scenes', 'Deleted Scenes'],
            ['Photoshoot',     'Photoshoot'],
            ['Commentary',     'Commentary'],
            ['Extras',         'Extras'],
          ].map(([v, lab]) => `<option value="${esc(v)}"${detectedKind === v ? ' selected' : ''}>${esc(lab)}</option>`).join('');
          const kindSelect = `<select class="field-input iafd-bulk-kind-select" style="font-size:11px;padding:3px 6px;min-width:140px;max-width:160px" onchange="iafdBulkUpdateKind(${gi}, ${fi}, this.value)" title="Special-feature marker — appended as [Kind] to the filed name + NFO tag">${kindOpts}</select>`;
          return `<div class="iafd-bulk-file-row" style="display:grid;grid-template-columns:22px 1fr 320px;gap:10px;align-items:center;padding:12px 18px;border-radius:0;background:linear-gradient(145deg, rgba(var(--brand-purple-rgb),0.14) 0%, rgba(var(--panel-hi-rgb),0.5) 55%, rgba(var(--panel-lo-rgb),0.35) 100%);border:none;border-top:1px solid rgba(255,255,255,0.06);box-shadow:inset 0 1px 0 rgba(255,255,255,0.06)">
            <input type="checkbox" id="iafdBulkTick-${gi}-${fi}" ${defaultOn ? 'checked' : ''} onchange="iafdBulkUpdateTick(${gi}, ${fi}, this.checked)" style="accent-color:rgba(var(--brand-purple-rgb),0.85);margin:0">
            <div style="min-width:0;display:flex;flex-direction:column;gap:2px">
              <span style="font-family:var(--mono);font-size:12px;color:var(--text);word-break:break-all">${esc(f.filename)}</span>
              <span class="iafd-bulk-scene-label" id="iafdBulkSceneLabel-${gi}-${fi}" style="font-size:11px;color:var(--dim)"><strong style="color:var(--text);font-weight:500">Scene:</strong> ${sceneLabel}${f.scene_number_in_filename ? ` <span style="color:var(--accent)">· filename says Scene ${f.scene_number_in_filename}</span>` : ''}</span>
              <span class="iafd-bulk-perfs" id="iafdBulkPerfs-${gi}-${fi}" style="font-size:11px;color:var(--dim)">${perfNames ? `<strong style="color:var(--text);font-weight:500">Cast:</strong> ${esc(perfNames)}` : '<em>No cast proposed</em>'}</span>
            </div>
            <div style="display:flex;flex-direction:column;align-items:stretch;gap:6px;min-width:0">
              ${filmInput}
              <div style="display:flex;gap:6px;align-items:center;justify-content:flex-end;flex-wrap:wrap">${kindSelect}${sceneSelect}</div>
            </div>
          </div>`;
        }).join('');
        const unmatched = isUnmatchedGroup(g);
        const headingTitle = unmatched
          ? `<span style="font-weight:600;color:#f59e0b">— Unmatched — pick a film for each</span>`
          : `<a href="${esc(g.iafd_url || '')}" target="_blank" rel="noopener noreferrer" style="font-weight:600;color:var(--accent);text-decoration:none">${esc(g.iafd_title || 'Untitled')}</a>`;
        const headingSubtitle = unmatched
          ? `${(g.files || []).length} file${(g.files || []).length === 1 ? '' : 's'} · auto-match couldn't pin a film`
          : `${(g.files || []).length} file${(g.files || []).length === 1 ? '' : 's'} · ${(g.all_scenes || []).length} breakdown${(g.all_scenes || []).length === 1 ? '' : 's'}`;
        return `<div class="iafd-bulk-group${unmatched ? ' iafd-bulk-group--unmatched' : ''}" style="display:flex;flex-direction:column;gap:8px;padding:12px;border-radius:8px;background:rgba(255,255,255,0.02);border:1px solid ${unmatched ? 'rgba(245,158,11,0.34)' : 'rgba(var(--brand-purple-rgb),0.14)'}">
          <div style="display:flex;align-items:center;justify-content:space-between;gap:12px">
            <div style="min-width:0;display:flex;flex-direction:column;gap:2px">
              ${headingTitle}
              ${!unmatched && movieLine ? `<span style="font-size:11px;color:var(--dim)">${movieLine}</span>` : ''}
            </div>
            <div style="font-size:11px;color:var(--dim)">${headingSubtitle}</div>
          </div>
          <div style="display:flex;flex-direction:column;gap:4px">${fileRows}</div>
        </div>`;
      }).join('');
      if (fileBtn) fileBtn.disabled = false;
    }
    _iafdBulkRefreshFileBtn();
    document.getElementById('qIafdResultsModal').classList.add('open');
  }

  function iafdBulkUpdateTick(gi, fi, on) {
    _iafdBulkTicked[`${gi}:${fi}`] = !!on;
    _iafdBulkRefreshFileBtn();
  }

  function iafdBulkUpdateKind(gi, fi, val) {
    _iafdBulkKinds[`${gi}:${fi}`] = (val == null ? '' : String(val));
  }

  //: Tick every row whose film resolved (auto-matched OR manually
  //: picked via the per-row Film input). Rows in the synthetic
  //: Unmatched group that the user never assigned a film to stay
  //: untocked — there's nothing to file there yet.
  function iafdBulkSelectAllMatched() {
    let ticked = 0;
    let skipped = 0;
    for (let gi = 0; gi < _iafdBulkGroups.length; gi++) {
      const grp = _iafdBulkGroups[gi];
      const files = (grp && grp.files) || [];
      for (let fi = 0; fi < files.length; fi++) {
        const f = files[fi];
        const filmUrl = (f && (f._iafd_url || grp.iafd_url)) || '';
        const key = `${gi}:${fi}`;
        if (filmUrl) {
          _iafdBulkTicked[key] = true;
          const cb = document.getElementById(`iafdBulkTick-${gi}-${fi}`);
          if (cb) cb.checked = true;
          ticked++;
        } else {
          skipped++;
        }
      }
    }
    _iafdBulkRefreshFileBtn();
    if (window.toast) {
      const parts = [`Ticked ${ticked} matched row${ticked === 1 ? '' : 's'}`];
      if (skipped) parts.push(`${skipped} unmatched skipped`);
      window.toast(parts.join(' · '));
    }
  }

  //: In-flight cache of {url → {all_scenes, movie_cast}} so picking the
  //: same film for two rows only pays one /api/iafd/movie-breakdown
  //: round-trip. Cleared on closeIafdBulkResults.
  let _iafdBulkBreakdownCache = new Map();

  async function _iafdBulkFetchBreakdown(url) {
    if (!url) return null;
    if (_iafdBulkBreakdownCache.has(url)) return _iafdBulkBreakdownCache.get(url);
    try {
      const r = await fetch('/api/iafd/movie-breakdown?url=' + encodeURIComponent(url), { credentials: 'same-origin' });
      if (!r.ok) return null;
      const d = await r.json();
      if (d.error) return null;
      const out = { all_scenes: d.all_scenes || [], movie_cast: d.movie_cast || [] };
      _iafdBulkBreakdownCache.set(url, out);
      return out;
    } catch (_) {
      return null;
    }
  }

  //: User typed (or picked from the datalist) a film for this row.
  //: Resolve the label back to a film entry, pull scene breakdowns,
  //: and repaint the row's scene dropdown + cast preview in place.
  //: Empty value ⇒ user cleared the picker → reset row to "Pick a
  //: film" state. Unknown label (free-text that doesn't match any
  //: filmography option) ⇒ silently leave the row alone so the
  //: existing pick isn't clobbered by a typo.
  async function iafdBulkChangeFilm(gi, fi, value) {
    const grp = _iafdBulkGroups[gi];
    const file = grp && grp.files && grp.files[fi];
    if (!file) return;
    const labRaw = (value || '').trim();
    const filmInput = document.getElementById(`iafdBulkFilm-${gi}-${fi}`);
    if (!labRaw) {
      file._iafd_url = '';
      file._film_title = '';
      file._film_year = '';
      file._film_studio = '';
      file._all_scenes = [];
      file._movie_cast = [];
      file.scene_idx = -1;
      file.proposed_performers = [];
      _repaintIafdBulkRowAfterFilm(gi, fi);
      return;
    }
    const hit = _iafdFilmByLabel.get(labRaw.toLowerCase());
    if (!hit) {
      //: Restore the input to whatever the row still says so a typo
      //: doesn't visually leave the field "stuck" on a non-match.
      if (filmInput) filmInput.value = file._iafd_url ? _filmLabel({ title: file._film_title, year: file._film_year }) : '';
      return;
    }
    if (filmInput) filmInput.style.opacity = '0.6';
    const bd = await _iafdBulkFetchBreakdown(hit.url);
    if (filmInput) filmInput.style.opacity = '';
    if (!bd) {
      window.toast && window.toast('Could not fetch breakdowns for that film');
      return;
    }
    file._iafd_url    = hit.url;
    file._film_title  = hit.title || '';
    file._film_year   = hit.year || '';
    file._film_studio = hit.studio || '';
    file._all_scenes  = bd.all_scenes || [];
    file._movie_cast  = bd.movie_cast || [];
    //: Re-apply the same auto scene-pick the bulk-scan does: prefer
    //: filename's "Scene N" if present, else the scene whose cast
    //: contains the bulk-scan's performer. Falls back to movie-level.
    let sceneIdx = -1;
    const fnSceneNum = file.scene_number_in_filename;
    if (fnSceneNum !== null && fnSceneNum !== undefined) {
      for (let i = 0; i < file._all_scenes.length; i++) {
        if (file._all_scenes[i].number === fnSceneNum) { sceneIdx = i; break; }
      }
    }
    if (sceneIdx === -1 && _iafdBulkPerformer) {
      const pLow = _iafdBulkPerformer.toLowerCase();
      for (let i = 0; i < file._all_scenes.length; i++) {
        const cast = file._all_scenes[i].cast || [];
        for (const nm of cast) {
          const nmLow = (nm || '').toLowerCase();
          if (nmLow && (nmLow.includes(pLow) || pLow.includes(nmLow))) { sceneIdx = i; break; }
        }
        if (sceneIdx !== -1) break;
      }
    }
    file.scene_idx = sceneIdx;
    if (sceneIdx >= 0) {
      const sc = file._all_scenes[sceneIdx] || {};
      file.proposed_performers = (sc.cast || []).map(n => ({ performer: { id: '', name: n } }));
    } else {
      file.proposed_performers = (file._movie_cast || []).slice();
    }
    _repaintIafdBulkRowAfterFilm(gi, fi);
  }

  function _repaintIafdBulkRowAfterFilm(gi, fi) {
    const grp = _iafdBulkGroups[gi];
    const file = grp && grp.files && grp.files[fi];
    if (!file) return;
    //: Scene select — replace innerHTML and switch the element type
    //: between <select> and a placeholder span depending on whether
    //: any breakdowns came back. Easier to rebuild the wrapping
    //: element than swap tags in place.
    const sceneEl = document.getElementById(`iafdBulkScene-${gi}-${fi}`);
    if (sceneEl) {
      const parent = sceneEl.parentNode;
      let html;
      if ((file._all_scenes || []).length) {
        const opts = ['<option value="-1"' + (file.scene_idx < 0 ? ' selected' : '') + '>Movie-level cast</option>']
          .concat(file._all_scenes.map((sc, si) => `<option value="${si}"${file.scene_idx === si ? ' selected' : ''}>${(window.esc||(s=>s))(sc.label || ('Scene ' + (sc.number || (si + 1))))}</option>`))
          .join('');
        html = `<select class="field-input iafd-bulk-scene-select" id="iafdBulkScene-${gi}-${fi}" style="font-size:11px;padding:3px 6px;min-width:200px;max-width:260px" onchange="iafdBulkUpdateSceneIdx(${gi}, ${fi}, this.value)">${opts}</select>`;
      } else {
        html = `<span class="iafd-bulk-scene-empty" id="iafdBulkScene-${gi}-${fi}" style="font-size:11px;color:var(--dim);padding:3px 6px">${file._iafd_url ? 'No breakdown available' : 'Pick a film first'}</span>`;
      }
      const tmp = document.createElement('div');
      tmp.innerHTML = html;
      parent.replaceChild(tmp.firstChild, sceneEl);
    }
    //: Scene label line under the filename.
    const labelEl = document.getElementById(`iafdBulkSceneLabel-${gi}-${fi}`);
    if (labelEl) {
      let labHtml;
      if (file.scene_idx >= 0 && (file._all_scenes || []).length) {
        const sc = file._all_scenes[file.scene_idx] || {};
        labHtml = (window.esc||(s=>s))(sc.label || ('Scene ' + (file.scene_idx + 1)));
      } else if (file._iafd_url) {
        labHtml = '<span style="color:#f59e0b">Movie-level cast (no scene narrowed)</span>';
      } else {
        labHtml = '<span style="color:#fca5a5">Pick a film →</span>';
      }
      const fnHint = file.scene_number_in_filename
        ? ` <span style="color:var(--accent)">· filename says Scene ${file.scene_number_in_filename}</span>`
        : '';
      labelEl.innerHTML = `<strong style="color:var(--text);font-weight:500">Scene:</strong> ${labHtml}${fnHint}`;
    }
    //: Cast preview.
    const perfEl = document.getElementById(`iafdBulkPerfs-${gi}-${fi}`);
    if (perfEl) {
      const names = (file.proposed_performers || [])
        .map(p => (p.performer && p.performer.name) || '')
        .filter(Boolean)
        .join(', ');
      perfEl.innerHTML = names
        ? `<strong style="color:var(--text);font-weight:500">Cast:</strong> ${(window.esc||(s=>s))(names)}`
        : '<em>No cast proposed</em>';
    }
  }

  function iafdBulkUpdateSceneIdx(gi, fi, val) {
    const idx = parseInt(val, 10);
    const grp = _iafdBulkGroups[gi];
    const file = grp && grp.files && grp.files[fi];
    if (!file) return;
    file.scene_idx = Number.isFinite(idx) ? idx : -1;
    //: Rebuild proposed cast to match the new pick so the filing
    //: request sends the right performers. Per-row scenes/cast live
    //: on the file itself now (so rows whose film was manually
    //: changed still read the right breakdowns), with fall-through
    //: to the group for unmodified rows.
    const allScenes = (file._all_scenes && file._all_scenes.length) ? file._all_scenes : (grp.all_scenes || []);
    const movieCast = (file._movie_cast && file._movie_cast.length) ? file._movie_cast : (grp.movie_cast || []);
    if (file.scene_idx >= 0) {
      const sc = allScenes[file.scene_idx] || {};
      file.proposed_performers = (sc.cast || []).map(n => ({ performer: { id: '', name: n } }));
    } else {
      file.proposed_performers = [...movieCast];
    }
    _repaintIafdBulkRowAfterFilm(gi, fi);
  }

  function _iafdBulkRefreshFileBtn() {
    const btn = document.getElementById('qIafdFileSelectedBtn');
    if (!btn) return;
    const any = Object.values(_iafdBulkTicked).some(Boolean);
    btn.disabled = !any || _iafdBulkFiling;
  }

  function closeIafdBulkResults() {
    if (_iafdBulkFiling) return;  // don't close mid-file
    document.getElementById('qIafdResultsModal').classList.remove('open');
    _iafdBulkGroups = [];
    _iafdBulkTicked = {};
    _iafdBulkKinds = {};
    _iafdFilmography = [];
    _iafdFilmByLabel = new Map();
    _iafdBulkBreakdownCache = new Map();
    _iafdBulkPerformer = '';
  }

  async function iafdBulkFileSelected() {
    if (_iafdBulkFiling) return;
    //: Flatten the ticked pairs into a batch request. One POST to
    //: /api/iafd/bulk-file kicks off a single background task that
    //: files each item serially — avoids the race where per-item
    //: /api/file/manual calls fire before the previous call's BG
    //: task has acquired the pipeline lock.
    const items = [];
    const indexMap = {};  // filename -> [gi, fi] so we can untick on success
    const skippedNoFilm = [];
    for (const [key, on] of Object.entries(_iafdBulkTicked)) {
      if (!on) continue;
      const [gi, fi] = key.split(':').map(n => parseInt(n, 10));
      const grp = _iafdBulkGroups[gi];
      const file = grp && grp.files && grp.files[fi];
      if (!grp || !file) continue;
      //: Per-row override fields win over the group when set. An
      //: empty `_iafd_url` means the user never picked a film for
      //: this row (typical for the synthetic Unmatched group); skip
      //: it with a toast at the end so the user knows why some rows
      //: didn't file.
      const filmUrl   = file._iafd_url   || grp.iafd_url || '';
      const filmTitle = file._film_title || grp.iafd_title || '';
      const filmYear  = file._film_year  || grp.year || '';
      const filmDate  = file._film_year  ? `${file._film_year}-01-01` : (grp.date || '');
      const filmStudio = file._film_studio || grp.studio || '';
      const filmScenes = (file._all_scenes && file._all_scenes.length) ? file._all_scenes : (grp.all_scenes || []);
      if (!filmUrl) { skippedNoFilm.push(file.filename); continue; }
      const sceneLabel = file.scene_idx >= 0
        ? (filmScenes[file.scene_idx] || {}).label || ('Scene ' + (file.scene_idx + 1))
        : '';
      const baseTitle = filmTitle;
      const title = sceneLabel ? `${baseTitle} - ${sceneLabel}` : baseTitle;
      const scene = {
        id: `iafd:${filmUrl}`,
        _iafd_url: filmUrl,
        title,
        release_date: filmDate,
        studio: filmStudio ? { name: filmStudio } : null,
        performers: file.proposed_performers || [],
        source: 'IAFD',
      };
      //: User's Kind picker wins over server-side filename detection.
      //: ``""`` (— None —) is sent verbatim so the server treats it as
      //: an explicit force-off; the wrapper only auto-detects when
      //: `_extra_kind` is absent from the scene dict.
      const _kindKey = `${gi}:${fi}`;
      if (Object.prototype.hasOwnProperty.call(_iafdBulkKinds, _kindKey)) {
        scene._extra_kind = _iafdBulkKinds[_kindKey];
      }
      items.push({ filename: file.filename, scene });
      indexMap[file.filename] = [gi, fi];
    }
    if (skippedNoFilm.length && window.toast) {
      const n = skippedNoFilm.length;
      window.toast(`Skipped ${n} ticked row${n === 1 ? '' : 's'} with no film picked`);
    }
    if (!items.length) {
      _iafdBulkRefreshFileBtn();
      return;
    }
    _iafdBulkFiling = true;
    const btn = document.getElementById('qIafdFileSelectedBtn');
    btn.disabled = true;
    const progressEl = document.getElementById('qIafdResultsProgress');
    progressEl.textContent = `Starting bulk file of ${items.length} item(s)…`;
    try {
      const kickoff = await fetch('/api/iafd/bulk-file', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ items }),
      });
      const kd = await kickoff.json();
      if (kd && kd.queued && typeof window.toast === 'function') {
        const pos = Number(kd.queued_position || 0);
        const suffix = pos > 1 ? ` (position ${pos})` : '';
        window.toast(`Pipeline busy — bulk file of ${items.length} item(s) queued${suffix}`, { kind: 'success' });
        progressEl.textContent = `Queued behind pipeline · ${items.length} item(s)…`;
      }
      if (kd.error) {
        progressEl.textContent = `Failed: ${kd.error}`;
        _iafdBulkFiling = false;
        _iafdBulkRefreshFileBtn();
        //: ``all_missing`` ⇒ every ticked filename is no longer in
        //: source dir (almost always because a prior bulk-file moved
        //: them and the queue UI hasn't reloaded). Force-refresh the
        //: queue + close the stale modal so the user isn't staring
        //: at a list of ghost rows.
        if (kd.code === 'all_missing') {
          if (window.toast) window.toast('Refreshing — those files have already been filed');
          closeIafdBulkResults();
          if (typeof loadQueue === 'function') loadQueue({ preserveView: true, force: true });
          if (typeof loadQueueStats === 'function') loadQueueStats();
        } else if (Array.isArray(kd.missing) && kd.missing.length) {
          //: Some-but-not-all missing — server still files the rest;
          //: log to console for diagnosis but keep the modal open so
          //: the user sees which rows didn't make it.
          console.warn('IAFD bulk-file: skipped (missing from source)', kd.missing);
        }
        return;
      }
    } catch (e) {
      progressEl.textContent = `Failed: ${e.message || e}`;
      _iafdBulkFiling = false;
      _iafdBulkRefreshFileBtn();
      return;
    }

    //: Poll bulk-file-status until running flips false. Per-item
    //: results arrive incrementally so we can untick filed rows in
    //: near-realtime and show an accurate progress line. mountGen
    //: gates the loop so page-navigation kills the polling.
    const processedFilenames = new Set();
    const mountGen = _queueLoadGen;
    while (true) {
      if (mountGen !== _queueLoadGen) return;
      let d;
      try {
        const r = await fetch('/api/iafd/bulk-file-status');
        d = await r.json();
      } catch(_) {
        await new Promise(res => setTimeout(res, 800));
        continue;
      }
      for (const res of (d.results || [])) {
        if (processedFilenames.has(res.filename)) continue;
        processedFilenames.add(res.filename);
        if (res.status === 'filed' || res.status === 'ok') {
          const idx = indexMap[res.filename];
          if (idx) {
            _iafdBulkTicked[`${idx[0]}:${idx[1]}`] = false;
            const cb = document.querySelector(
              `#qIafdResultsGroups input[type="checkbox"][onchange*="iafdBulkUpdateTick(${idx[0]}, ${idx[1]}"]`
            );
            if (cb) cb.checked = false;
          }
        }
      }
      const cur = d.current ? ` · ${d.current}` : '';
      progressEl.textContent = `Filing ${d.done} / ${d.total}${cur}`;
      if (!d.running) {
        const failed = (d.results || []).filter(r => r.status !== 'filed' && r.status !== 'ok').length;
        const filed = (d.results || []).filter(r => r.status === 'filed' || r.status === 'ok').length;
        progressEl.textContent = `Done: ${filed} filed${failed ? `, ${failed} failed` : ''}`;
        break;
      }
      await new Promise(res => setTimeout(res, 600));
    }

    _iafdBulkFiling = false;
    _iafdBulkRefreshFileBtn();
    loadQueue({ preserveView: true });
    loadQueueStats();
  }

  //: Delete a queued video from disk. Uses the native confirm prompt
  //: so the user gets a blocking "are you sure?" that shows the full
  //: filename before we fire the DELETE — the action is irreversible
  //: so a fancy styled modal here would be overkill. Refreshes the
  //: queue in place (preserveView) so the row disappears without
  //: losing scroll position.
  async function confirmDeleteQueueFile(filename) {
    if (!filename) return;
    const ok = confirm(`Delete this file from disk?\n\n${filename}\n\nThis cannot be undone — the video and any matching .nfo / poster sidecars will be removed.`);
    if (!ok) return;
    try {
      const r = await fetch('/api/queue/delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ filename }),
      });
      const d = await r.json();
      if (d.error) { window.toast(`Delete failed: ${d.error}`); return; }
      //: If this was the file currently selected for search/confirm,
      //: drop that state so the now-orphaned panel doesn't stay docked.
      if (_qSelectedFile === filename) { clearSelectedFile(); clearSearch(); }
      loadQueue({ preserveView: true });
      loadQueueStats();
    } catch (e) {
      window.toast('Delete failed: ' + (e.message || e));
    }
  }

  function _applySceneQueuePayload(d, preserveView) {
    const savedScroll = preserveView ? window.scrollY : 0;
    const savedPage = preserveView ? _queuePage : 0;
    const qc = document.getElementById('queueCount');
    if (qc) { qc.textContent = d.files?.length ? `-${d.files.length}-` : ''; qc.style.color = 'var(--text)'; }
    const statQ = document.getElementById('statQueue'); if (statQ) statQ.textContent = d.files?.length || 0;
    // Stash the per-status totals so _paintQueueStats can read them
    // instead of mixing in /api/stats' processed_files SQL count (which
    // excludes pending rows and drifts vs the filesystem walk).
    window._sceneQueueStatusCounts = d.status_counts || null;
    if (_queueStatsPayload) _paintQueueStats(_queueStatsPayload);
    _syncHeaderQueuePill(d.files?.length || 0);
    if (d.error) {
      document.getElementById('queueList').innerHTML = `<div class="empty">${esc(d.error)}</div>`;
      const pager = document.getElementById('queuePager');
      if (pager) pager.style.display = 'none';
      window._queueFiles = [];
      _queueFilteredIndexes = [];
      return;
    }
    window._queueFiles = (d.files || []).slice().sort(_cmpQueueFiles);
    _queueActiveKind = 'scenes';
    _queuePage = preserveView ? savedPage : 0;
    _recomputeQueueFilter();
    _renderQueuePage();
    if (preserveView) requestAnimationFrame(() => window.scrollTo({ top: savedScroll }));
  }

  async function loadQueue(opts) {
    const preserveView = !!(opts && opts.preserveView);
    const force = !!(opts && opts.force);
    const mountGen = _queueLoadGen;
    if (queueMode === 'movies') { await loadMovieQueue(opts); return; }
    if (!force && _sceneQueuePayload) {
      if (mountGen !== _queueLoadGen) return;
      _applySceneQueuePayload(_sceneQueuePayload, preserveView);
      return;
    }
    try {
      void _refreshViceSlugs();
      void _refreshLibraryStudios();
      const r = await fetch('/api/queue');
      if (mountGen !== _queueLoadGen) return;
      const d = await r.json();
      if (mountGen !== _queueLoadGen) return;
      _sceneQueuePayload = d;
      _applySceneQueuePayload(d, preserveView);
    } catch {
      if (mountGen !== _queueLoadGen) return;
      document.getElementById('queueList').innerHTML = '<div class="empty">Error loading queue</div>';
      const pager = document.getElementById('queuePager');
      if (pager) pager.style.display = 'none';
    }
  }

  async function _hydrateQueueHeadshots() {
    const mountGen = _queueLoadGen;
    // Pre-resolved slots (server already filled in <img> tags via the
    // payload's `performer_headshots` field) carry no `data-perf-names`
    // attribute — skip them so this fallback path never clobbers the
    // already-rendered headshots.
    const slots = Array.from(document.querySelectorAll('#queueList .qi-headshots[data-perf-names]'));
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
    if (mountGen !== _queueLoadGen) return;
    slots.forEach(slot => {
      const names = (slot.dataset.perfNames || '').split('|').map(n => n.trim()).filter(Boolean);
      const imgs = [];
      for (const n of names) {
        const url = lookup[n.toLowerCase()];
        if (url) {
          imgs.push(`<img class="qi-headshot" loading="lazy" decoding="async" src="${url}" alt="${n.replace(/"/g, '&quot;')}" title="${n.replace(/"/g, '&quot;')}" onerror="this.style.display='none'">`);
          if (imgs.length >= 3) break;
        }
      }
      slot.innerHTML = imgs.reverse().join('');
    });
  }

  async function runAll() {
    const url = queueMode === 'movies' ? '/api/movies/run/all' : '/api/run/all';
    const r = await fetch(url, {method:'POST'});
    if (r.ok) { _qIsRunning = true; const b = document.getElementById('btnRunAll'); if (b) b.disabled = true; }
  }

  async function runQueueFile(filename) {
    const base = queueMode === 'movies' ? '/api/movies/run/file/' : '/api/run/file/';
    const r = await fetch(base + encodeURIComponent(filename), {method:'POST'});
    if (r.ok) {
      _qIsRunning = true;
      const b = document.getElementById('btnRunAll'); if (b) b.disabled = true;
      try {
        const d = await r.json();
        if (d && d.queued && typeof window.toast === 'function') {
          const pos = Number(d.queued_position || 0);
          const suffix = pos > 1 ? ` (position ${pos})` : '';
          window.toast(`Pipeline busy — ${filename} queued${suffix}`, { kind: 'success' });
        }
      } catch (_) {}
    }
  }

  function _paintQueueStats(data) {
    const scenes = data.scenes || {};
    const movies = data.movies || {};
    const wanted = data.wanted || { items: [] };
    const sugg = data.sugg || { count: 0 };
    const active = queueMode === 'movies' ? movies : scenes;
    const setText = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
    setText('statFiled',            active.filed    || 0);
    // No Dir + Errors tiles read from the queue payload's status_counts
    // when available so they stay in lockstep with the Errors / No Dir
    // filter pills below — `/api/stats` counts the underlying SQL rows
    // including ones whose source file has been removed from disk, which
    // the filter can't render (no row to click). Tile says "1 error" →
    // user clicks the filter → "no files match" was the result of that
    // mismatch. Falls back to /api/stats only until the queue payload
    // arrives so first paint isn't blank.
    const activeStatus = (queueMode === 'movies'
      ? window._movieQueueStatusCounts
      : window._sceneQueueStatusCounts) || null;
    setText('statNoDir',            activeStatus ? (activeStatus.no_dir || 0) : (active.no_dir || 0));
    setText('statErrors',           activeStatus ? (activeStatus.error  || 0) : (active.errors || 0));
    // Unmatched tiles read from the queue payload's status_counts when
    // it's available — that data set is what the queue list itself
    // renders, so "Queue 106" and "Unmatched 105" can't disagree just
    // because one fetched processed_files SQL while the other walked
    // the filesystem. Falls back to /api/stats when the queue payload
    // hasn't loaded yet (boot-time first paint).
    const sceneStatus = (window._sceneQueueStatusCounts || null);
    const movieStatus = (window._movieQueueStatusCounts || null);
    const sceneUnmatched = sceneStatus
      ? ((sceneStatus.unmatched || 0) + (sceneStatus.untracked || 0))
      : (scenes.unmatched || 0);
    const movieUnmatched = movieStatus
      ? ((movieStatus.unmatched || 0) + (movieStatus.untracked || 0))
      : (movies.unmatched || 0);
    setText('statUnmatchedScenes',  sceneUnmatched);
    setText('statUnmatchedMovies',  movieUnmatched);
    setText('statWanted',           (wanted.items || []).length);
    setText('statSuggested',        sugg.count || 0);
  }

  async function loadQueueStats() {
    // Fetch scene stats + movie stats + wanted count in parallel so
    // every tile stays live regardless of the current queue mode. The
    // Filed/No Dir/Errors values still track the active mode; Unmatched
    // is split permanently into scenes + movies tiles.
    const mountGen = _queueLoadGen;
    // Progressive paint: each tile lights up as its endpoint resolves
    // instead of waiting for the slowest of the four. Build a mutable
    // payload and re-paint after every fulfilled fetch — `_paintQueueStats`
    // is cheap (5 textContent writes) so the extra paints don't matter.
    const payload = { scenes: {}, movies: {}, wanted: { items: [] }, sugg: { count: 0 }, dup: { groups: 0, files: 0 } };
    const paintIfMounted = () => {
      if (mountGen !== _queueLoadGen) return;
      _queueStatsPayload = payload;
      _queueStatsFetchedAt = Date.now();
      _paintQueueStats(payload);
      _updateQueueDupButton(payload.dup);
    };
    const failed = [];
    const fetchInto = (url, key, fallback) =>
      fetch(url)
        .then(r => {
          if (!r.ok) throw new Error('HTTP ' + r.status);
          return r.json();
        })
        .then(d => { payload[key] = d || fallback; paintIfMounted(); })
        .catch(e => {
          failed.push({ url, key, message: (e && e.message) || 'fetch failed' });
          if (typeof console !== 'undefined') console.warn('[queue] stats fetch failed', url, e);
        });
    await Promise.all([
      fetchInto('/api/stats', 'scenes', {}),
      fetchInto('/api/movies/stats', 'movies', {}),
      fetchInto('/api/wanted', 'wanted', { items: [] }),
      fetchInto('/api/queue/suggestions/count', 'sugg', { count: 0 }),
      fetchInto('/api/queue/duplicates/count', 'dup', { groups: 0, files: 0 }),
    ]);
    if (mountGen !== _queueLoadGen) return;
    if (failed.length) {
      _markQueueStatsStale(failed);
      // Throttle the toast so a 5 s poll doesn't spam the banner — first
      // failure of a contiguous run alerts, subsequent failures stay
      // quiet until at least one stat fetch succeeds again.
      if (!_queueStatsErrorActive) {
        _queueStatsErrorActive = true;
        if (typeof window.toast === 'function') {
          window.toast(`Stats refresh failed (${failed.length}/${5}) — values may be out of date`, { kind: 'error' });
        }
      }
    } else if (_queueStatsErrorActive) {
      _queueStatsErrorActive = false;
    }
  }

  /** Tracks whether the current stats-fetch run started in an error state — keeps the toast from firing every 5 s while the endpoint is down. */
  let _queueStatsErrorActive = false;

  /** Paint a dash on tiles whose underlying fetch failed so the user sees the value isn't current. */
  function _markQueueStatsStale(failed) {
    const dashTiles = new Set();
    for (const f of failed) {
      if (f.key === 'scenes') {
        dashTiles.add('statUnmatchedScenes');
        if (queueMode === 'scenes') {
          dashTiles.add('statFiled');
          dashTiles.add('statNoDir');
          dashTiles.add('statErrors');
        }
      } else if (f.key === 'movies') {
        dashTiles.add('statUnmatchedMovies');
        if (queueMode === 'movies') {
          dashTiles.add('statFiled');
          dashTiles.add('statNoDir');
          dashTiles.add('statErrors');
        }
      } else if (f.key === 'wanted') {
        dashTiles.add('statWanted');
      } else if (f.key === 'sugg') {
        dashTiles.add('statSuggested');
      }
    }
    for (const id of dashTiles) {
      const el = document.getElementById(id);
      if (el && (el.textContent === '0' || el.textContent === '')) el.textContent = '—';
    }
  }

  function _updateQueueDupButton(dup) {
    const btn = document.getElementById('btnQueueDuplicates');
    if (!btn) return;
    const n = Number((dup && dup.groups) || 0);
    btn.classList.toggle('has-dup', n > 0);
    btn.title = n > 0
      ? `Queue duplicates — ${n} group${n === 1 ? '' : 's'}, ${Number(dup.files || 0)} file(s)`
      : 'Queue duplicates (same phash)';
  }

  function filterQueue() {
    // Pagination-aware: recompute the filtered index set from
    // `window._queueFiles`, reset to page 0, and re-render. Matches
    // against both the raw filename and the spaceified display title
    // so typing "emma rosie" finds EmmaRosie-style release names too.
    _queuePage = 0;
    _recomputeQueueFilter();
    _renderQueuePage();
  }

  // Strip a performer name out of a filename-parsed title and clean up
  // leftover separators. Handles performer at the start
  // ("Cytherea - Scene Name" → "Scene Name"), at the end
  // ("Scene Name - Cytherea" → "Scene Name"), and wraps up dangling
  // " - " either side. Case-insensitive because parsed titles preserve
  // whatever casing the filename had.
  function _stripPerformerFromTitle(title, performer) {
    return _stripTokensFromTitle(title, performer);
  }
  /* Generalised version of `_stripPerformerFromTitle` — accepts any
   * number of token strings (performer, studio, etc.) and strips each
   * from the leading/trailing position of the title with surrounding
   * dash/separator. Then cleans up dangling separators and collapses
   * runs of whitespace. Tokens are case-insensitive, regex-safe-escaped.
   *
   * Example:
   *   _stripTokensFromTitle("Brazzers - Sunny Lane - Hellcats 8", "Sunny Lane", "Brazzers")
   *     → "Hellcats 8"
   */
  function _stripTokensFromTitle(title, ...tokens) {
    let out = String(title || '').trim();
    if (!out) return out;
    const dash = '[\\-\\u2013\\u2014]';   // -, en-dash, em-dash
    for (const raw of tokens) {
      const tok = String(raw || '').trim();
      if (!tok) continue;
      const escTok = tok.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      // Leading: "<token> - " or "<token>"
      out = out.replace(new RegExp('^\\s*' + escTok + '\\s*(' + dash + '\\s*)?', 'i'), '');
      // Trailing: " - <token>" or "<token>"
      out = out.replace(new RegExp('(\\s*' + dash + ')?\\s*' + escTok + '\\s*$', 'i'), '');
      // Embedded: " - <token> - " collapses to " - "
      out = out.replace(new RegExp('\\s*' + dash + '\\s*' + escTok + '\\s*(?=' + dash + ')', 'gi'), '');
    }
    // Cleanup pass: drop dangling separators, collapse multi-space.
    out = out.replace(new RegExp('^\\s*' + dash + '\\s*'), '');
    out = out.replace(new RegExp('\\s*' + dash + '\\s*$'), '');
    out = out.replace(new RegExp('\\s*' + dash + '\\s*' + dash + '\\s*', 'g'), ' - ');
    out = out.replace(/\s{2,}/g, ' ');
    return out.trim();
  }

  // ── Scene manual search ──
  //: Monotonic tokens so async races (user clicks Search on row A,
  //: then B before A's parse/search comes back) don't let the slower
  //: fetch stomp fields/results that belong to the newer selection.
  //: Each call increments the token; awaited continuations bail if
  //: their captured token is no longer current.
  let _qOpenSearchSeq = 0;
  let _qRunSearchSeq = 0;

  //: Filename → special-feature label detection. Mirror of the
  //: `EXTRA_KIND_PATTERNS` table in `app/utils/text.py` — keep both
  //: lists in sync so the Scene Search popup pre-selects the same kind
  //: the server would auto-detect at filing time. The first regex that
  //: matches wins (more specific patterns come first).
  const _EXTRA_KIND_REGEXES = [
    [/\bbehind[\s._-]+the[\s._-]+scenes?\b/i,  'BTS'],
    [/\bb\.?t\.?s\.?\b/i,                      'BTS'],
    [/\bbloopers?\b/i,                         'Bloopers'],
    [/\bouttakes?\b/i,                         'Outtakes'],
    [/\bmaking[\s._-]+of\b/i,                  'Making Of'],
    [/\binterviews?\b/i,                       'Interview'],
    [/\btrailers?\b/i,                         'Trailer'],
    [/\bdeleted[\s._-]+scenes?\b/i,            'Deleted Scenes'],
    [/\bphotoshoots?\b/i,                      'Photoshoot'],
    [/\bcommentary\b/i,                        'Commentary'],
    [/\b(?:bonus|extras?)\b/i,                 'Extras'],
  ];
  function _detectExtraKind(text) {
    if (!text) return '';
    const s = String(text);
    for (const [rx, label] of _EXTRA_KIND_REGEXES) {
      if (rx.test(s)) return label;
    }
    return '';
  }
  //: Set the Kind dropdown to the detected value (or "Normal scene"
  //: when nothing matches) so the user sees what was auto-picked. The
  //: special ``__auto__`` sentinel is only used as the initial value
  //: before any file is loaded; once a filename is in flight we always
  //: resolve to a concrete label or the empty (normal-scene) option.
  function _setSearchExtraKindFromFilename(filename) {
    const sel = document.getElementById('srchExtraKind');
    if (!sel) return;
    const detected = _detectExtraKind(filename);
    const opts = Array.from(sel.options).map(o => o.value);
    sel.value = opts.includes(detected) ? detected : '';
  }
  //: Read the user's choice. Returns ``null`` when the dropdown is on
  //: ``Auto-detect`` (server-side detection wins); otherwise returns
  //: the chosen label or ``""`` for "Normal scene" (force-off).
  function _readSearchExtraKind() {
    const sel = document.getElementById('srchExtraKind');
    if (!sel) return null;
    const v = sel.value;
    return v === '__auto__' ? null : v;
  }

  // Cached `manual_submit_stashdb_default` from /api/settings. Loaded
  // once per page session — the value changes only when the user
  // toggles it in Settings, and a stale value just defaults the
  // submit-to-StashDB checkbox to the previous state, which is fine
  // until next page reload.
  let _qManualSubmitStashdbDefault = null;
  async function _qGetManualSubmitStashdbDefault() {
    if (_qManualSubmitStashdbDefault !== null) return _qManualSubmitStashdbDefault;
    try {
      const r = await fetch('/api/settings');
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const s = await r.json();
      _qManualSubmitStashdbDefault = (s.manual_submit_stashdb_default || 'false') === 'true';
    } catch (e) {
      if (typeof console !== 'undefined') console.warn('[queue] settings load failed', e);
      _qManualSubmitStashdbDefault = false;
    }
    return _qManualSubmitStashdbDefault;
  }

  // Applies the filename-parser blob to the manual-search form fields.
  // Library-guess fields (studio + performers) take precedence over the
  // parser when both are present, since the guess is grounded in the
  // user's actual library.
  function _qApplyParseToManualSearch(parsed, libStudio, libPerfs) {
    if (!parsed || typeof parsed !== 'object') return;
    const titleEl = document.getElementById('srchTitle');
    const dateEl  = document.getElementById('srchDate');
    const studEl  = document.getElementById('srchStudio');
    const perfEl  = document.getElementById('srchPerformer');
    // Only fill empty fields. The parse-filename fetch is async when
    // the queue snapshot hasn't already cached it, so a user who types
    // before it returns would otherwise get their typed value clobbered
    // by the late-arriving parser output — exactly the "I typed a title
    // but the file kept the auto-fill" symptom.
    if (parsed.title && titleEl && !titleEl.value.trim()) titleEl.value = parsed.title;
    if (dateEl && !dateEl.value.trim()) {
      if (parsed.date) dateEl.value = parsed.date;
      else if (parsed.file_mtime) dateEl.value = parsed.file_mtime;
    }
    if (!libStudio && parsed.site && studEl && !studEl.value.trim()) studEl.value = parsed.site;
    if (!libPerfs.length && parsed.performers && perfEl && !perfEl.value.trim()) perfEl.value = parsed.performers;
    // Strip auto-populated performer + studio names from the title so a
    // "Brazzers - Asa Akira - Whatever" filename doesn't have all three
    // tokens jammed into the search title.
    if (titleEl && titleEl.value) {
      const perfStripList = libPerfs.length
        ? libPerfs
        : ((perfEl && perfEl.value || '').split(',').map(s => s.trim()).filter(Boolean));
      const studVal = (studEl && studEl.value || '').trim();
      if ((perfStripList.length || studVal)) {
        titleEl.value = _stripTokensFromTitle(titleEl.value, ...perfStripList, studVal);
      }
    }
  }

  async function openManualSearch(filename) {
    const mySeq = ++_qOpenSearchSeq;
    _qSelectedFile = filename;
    document.getElementById('searchFileLabel').textContent = `Filing: ${filename}`;
    document.getElementById('searchFileLabel').style.display = 'inline-block';
    // Show the modal IMMEDIATELY before any awaits — the user clicked
    // a button, they should see the modal frame within a frame or two.
    // Previously the display flip lived at the bottom of this function,
    // which meant clicking didn't visibly do anything until the awaited
    // parse + match-scene round-trips returned (often delayed by 5
    // ffmpeg thumb-generates fighting for the same connection pool).
    const sp = document.getElementById('qSearchPanel');
    if (sp) sp.style.display = 'flex';
    const ov = document.getElementById('qSearchOverlay');
    if (ov) {
      ov.style.display = 'flex';
      ov.classList.add('open');
    }
    // Defer the heavy ffmpeg/ffprobe fan-out to the next animation
    // frame so the modal paints first. The five thumb-generate calls
    // and the file-probe each spawn a subprocess server-side; firing
    // them before the modal is on screen is the main cause of the
    // "click registers but action takes ages" symptom on /queue.
    requestAnimationFrame(() => {
      if (mySeq !== _qOpenSearchSeq) return;
      _loadSearchFrames(filename);
      _loadSearchFileMeta(filename, mySeq);
    });
    document.querySelectorAll('.queue-section .queue-item').forEach(el => el.classList.remove('active'));
    if (window._queueFiles) {
      const idx = window._queueFiles.findIndex(f => f.filename === filename);
      if (idx !== -1) document.getElementById(`qi-${idx}`)?.classList.add('active');
    }
    document.getElementById('srchTerm').value = '';
    document.getElementById('srchTitle').value = '';
    document.getElementById('srchPerformer').value = '';
    if (typeof _perfPillsClear === 'function') _perfPillsClear();
    document.getElementById('srchStudio').value = '';
    document.getElementById('srchDate').value = '';
    //: Auto-detect BTS / Bloopers / Outtakes / ... marker in the source
    //: filename and pre-select it in the Kind dropdown. User can override
    //: before filing. ``Auto-detect`` (the default sentinel) tells the
    //: server to re-detect from the filename at filing time.
    if (typeof _setSearchExtraKindFromFilename === 'function') {
      _setSearchExtraKindFromFilename(filename);
    }
    // Drop any stored phash match from a prior popup-open so it can't
    // bleed into this file's stored-match render.
    _qStoredMatchScene = null;
    // Manual-extras state — the merged modal also serves as the
    // manual-filing surface, so reset the thumbnail / plot / DB
    // submit toggles on every fresh open.
    _qManualFile = filename;
    const plotEl = document.getElementById('mPlot');
    const imgUrlEl = document.getElementById('mImageUrl');
    if (plotEl) plotEl.value = '';
    if (imgUrlEl) imgUrlEl.value = '';
    if (typeof _mResetThumb === 'function') _mResetThumb();
    // Inline scene-pick — reset so the previous file's picked scene
    // doesn't leak into the new openManualSearch. Doesn't touch the
    // search form fields (those get repopulated below from the file's
    // library guess).
    if (typeof clearScenePick === 'function') clearScenePick();
    const matchBanner = document.getElementById('manualMatchBanner');
    if (matchBanner) { matchBanner.style.display = 'none'; matchBanner.innerHTML = ''; }
    ['mStashdbResolveResults','mStashdbResolveStatus','mFansdbResolveResults','mFansdbResolveStatus','mJavstashResolveResults','mJavstashResolveStatus'].forEach(id => { const el = document.getElementById(id); if (el) el.innerHTML = ''; });
    const stashChk = document.getElementById('mSubmitStashdb');
    const fansChk  = document.getElementById('mSubmitFansdb');
    const javChk   = document.getElementById('mSubmitJavstash');
    if (stashChk) { stashChk.checked = false; document.getElementById('mStashdbFields').style.display = 'none'; }
    if (fansChk)  { fansChk.checked  = false; document.getElementById('mFansdbFields').style.display  = 'none'; }
    if (javChk)   { javChk.checked   = false; document.getElementById('mJavstashFields').style.display = 'none'; }
    mThumbMode = null; mThumbDataUrl = null;
    if (typeof mResolved !== 'undefined') {
      mResolved.stashdb  = { studio: null, performers: [] };
      mResolved.fansdb   = { studio: null, performers: [] };
      mResolved.javstash = { studio: null, performers: [] };
    }
    // Default to expanded: the left column has plenty of vertical
    // room in the new two-column layout, so showing the extras
    // up-front saves a click. User can still collapse via the toggle.
    _setManualExtrasOpen(true);
    // Reset the inline vice picker — collapse it, clear any
    // invalid-field flag, and re-auto-select on next open. We keep
    // the dropdown HTML around (lazy-loaded the first time the user
    // clicks the flame), only the visibility resets.
    const _viceSelInline = document.getElementById('qViceSelectInline');
    const _viceFileBtn = document.getElementById('qViceFileBtn');
    if (_viceSelInline) {
      _viceSelInline.style.display = 'none';
      _viceSelInline.classList.remove('field-invalid');
      _viceSelInline.value = '';
    }
    if (_viceFileBtn) _viceFileBtn.style.display = 'none';
    if (typeof _mPerfWireHandlers === 'function') _mPerfWireHandlers();
    // `_mPerfLoadLibrary()` used to fire here on every open. It's
    // only consumed by the performer-input autocomplete, which
    // already awaits it on first keystroke — fetching the full
    // performer list before the modal is even visible burned a
    // connection slot and event-loop time for no perceptible win.
    // Restore the manual-file button to its idle label so a prior
    // stuck "Filing…" spinner from a failed submit doesn't carry over.
    const mfBtn = document.getElementById('qSearchManualFileBtn');
    if (mfBtn) {
      mfBtn.disabled = false;
      mfBtn.innerHTML = '<i class="fa-solid fa-pen"></i><span class="btn-label" style="margin-left:6px">Submit</span>';
    }
    // Settings cache — `_qGetManualSubmitStashdbDefault` returns
    // immediately on cache-hit, so this no longer round-trips on
    // every open.
    _qGetManualSubmitStashdbDefault().then((def) => {
      if (mySeq !== _qOpenSearchSeq) return;
      const chk = document.getElementById('mSubmitStashdb');
      if (chk) {
        chk.checked = def;
        const fields = document.getElementById('mStashdbFields');
        if (fields) fields.style.display = def ? 'block' : 'none';
      }
    });

    // Start with the library-guess match we've already computed for
    // this queue row: studio + performers vetted against the user's
    // actual favourites / studio_logos table. These beat the generic
    // filename parser because they're grounded in local library data,
    // so we fall back to the parser only when the guess didn't fire.
    const queueFile = (window._queueFiles || []).find(f => f.filename === filename) || {};
    const _libStudioRaw = (queueFile.match_studio || '').trim();
    const libPerfs = (queueFile.performers || '')
      .replace(/\|/g, ',')
      .replace(/ \/ /g, ',')
      .split(',')
      .map(s => s.trim())
      .filter(Boolean);
    // Drop the auto-populated studio when it collides (case-insensitive)
    // with a matched performer name. Some studios share names with
    // performers (e.g. "Sasha Grey", "Stoya") and history can match
    // either — having the same string in both fields just doubles the
    // search noise.
    const _perfsLc = new Set(libPerfs.map(p => p.toLowerCase()));
    const libStudio = (_libStudioRaw && _perfsLc.has(_libStudioRaw.toLowerCase())) ? '' : _libStudioRaw;

    if (libStudio)     document.getElementById('srchStudio').value = libStudio;
    if (libPerfs.length) {
      const inp = document.getElementById('srchPerformer');
      if (inp) inp.value = '';
      if (typeof _perfPillsSet === 'function') _perfPillsSet(libPerfs);
    }

    // ── Filename parse: served from the queue payload when present
    // (filled by the server's queue-snapshot builder, zero round-trip)
    // and falls back to /api/parse/filename for older payloads.
    let parsed = (queueFile && queueFile.parsed && typeof queueFile.parsed === 'object')
      ? queueFile.parsed
      : null;
    if (parsed) {
      _qApplyParseToManualSearch(parsed, libStudio, libPerfs);
    } else {
      try {
        const r = await fetch(`/api/parse/filename?filename=${encodeURIComponent(filename)}`);
        if (mySeq !== _qOpenSearchSeq) return;
        parsed = await r.json();
        if (mySeq !== _qOpenSearchSeq) return;
        _qApplyParseToManualSearch(parsed, libStudio, libPerfs);
      } catch {
        if (mySeq !== _qOpenSearchSeq) return;
      }
    }
    // Fallback: parser produced nothing AND the library guess didn't
    // fire either — drop the cleaned-up filename into the broad
    // "term" field so the user has something to search on.
    {
      const d = parsed || {};
      const anyField = libStudio || libPerfs.length || d.title || d.site || d.performers;
      if (!anyField) {
        document.getElementById('srchTerm').value = filename.replace(/\.[^.]+$/, '').replace(/[._]/g, ' ');
      }
    }
    if (mySeq !== _qOpenSearchSeq) return;
    // If the queue row carries a stored DB match, fold its plot +
    // image into the merged modal's manual-extras so a "File manually"
    // fallback ships with proper metadata. Runs async, parallel to
    // the auto-search below, and skips if a newer open has already
    // taken over.
    if (queueFile.match_external_id && queueFile.match_source) {
      _mRenderMatchBanner(queueFile.match_source, queueFile.match_external_id, 'Loading scene details…');
      (async () => {
        const matchSeq = mySeq;
        try {
          const r = await fetch(`/api/queue/match-scene?filename=${encodeURIComponent(filename)}`);
          if (matchSeq !== _qOpenSearchSeq) return;
          const d = await r.json();
          if (matchSeq !== _qOpenSearchSeq) return;
          if (d.found) {
            if (d.description) document.getElementById('mPlot').value = d.description;
            if (d.image_url) {
              document.getElementById('mImageUrl').value = d.image_url;
              mThumbMode = 'url';
              mThumbPreview(d.image_url);
            }
            // Pre-fill the search form from the stored match so the user
            // sees the actual matched scene's metadata, not the filename
            // parser's best guess. Overwrites the library/parser fills
            // because the match is more authoritative when present.
            const titleEl = document.getElementById('srchTitle');
            const dateEl  = document.getElementById('srchDate');
            const studEl  = document.getElementById('srchStudio');
            if (d.title && titleEl) titleEl.value = d.title;
            if (d.date && dateEl) dateEl.value = d.date;
            if (d.studio && studEl) studEl.value = d.studio;
            if (Array.isArray(d.performers) && d.performers.length && typeof _perfPillsSet === 'function') {
              _perfPillsSet(d.performers);
            }
            // Stash the stored match so the search-results renderer can
            // pin it at the top of the list — one click to accept the
            // already-known match without having to hunt for it.
            _qStoredMatchScene = {
              source:      _qsNormalizeMatchSource(queueFile.match_source),
              id:          queueFile.match_external_id || '',
              title:       d.title || '',
              date:        d.date || '',
              studio:      d.studio || '',
              performers:  Array.isArray(d.performers) ? d.performers.join(', ') : '',
              image:       d.image_url || '',
              description: d.description || '',
              view_url:    '',
              _stored:     true,
            };
            _qsRenderStoredMatchIfIdle();
            _mRenderMatchBanner(queueFile.match_source, queueFile.match_external_id, '');
          } else {
            _mRenderMatchBanner(queueFile.match_source, queueFile.match_external_id, d.error ? `Lookup failed: ${d.error}` : 'Scene not reachable on source');
          }
        } catch (e) {
          if (matchSeq === _qOpenSearchSeq) {
            _mRenderMatchBanner(queueFile.match_source, queueFile.match_external_id, `Lookup failed: ${e.message || e}`);
          }
        }
      })();
    }
    // Modal frame was opened up-top before any awaits. Kick off the
    // search now that the form fields are filled.
    runQueueSearch();
  }

  //: Parent node the qSearchPanel was created in — restored on
  //: `clearSearch()` / when opening a different row cleans up first.
  let _qSearchPanelHome = null;
  let _qSearchPanelHomeNext = null;
  function _dockSearchPanelUnderActiveRow(panel) {
    if (!panel) return;
    const active = document.querySelector('.queue-section .queue-item.active');
    if (!active || !active.parentNode) return;
    // Record home location on first move so we can put the panel back
    // when the user clears or dismisses the search.
    if (!_qSearchPanelHome) {
      _qSearchPanelHome = panel.parentNode;
      _qSearchPanelHomeNext = panel.nextSibling;
    }
    // Already directly below the active row? Skip the DOM churn.
    if (active.nextSibling === panel) return;
    active.parentNode.insertBefore(panel, active.nextSibling);
  }
  function _undockSearchPanel() {
    const panel = document.getElementById('qSearchPanel');
    if (!panel || !_qSearchPanelHome) return;
    if (panel.parentNode === _qSearchPanelHome) return;
    _qSearchPanelHome.insertBefore(panel, _qSearchPanelHomeNext || null);
  }

  function clearSelectedFile() {
    _qSelectedFile = null;
    const lbl = document.getElementById('searchFileLabel');
    if (lbl) lbl.style.display = 'none';
    document.querySelectorAll('.queue-section .queue-item').forEach(el => el.classList.remove('active'));
    const sp = document.getElementById('qSearchPanel');
    if (sp) sp.style.display = 'none';
    // Modal-mode: fully hide the overlay. The .open class only toggles
    // min-height — without `display:none` the inline `display:flex` from
    // openManualSearch leaves a transparent dim+blur layer covering the
    // page, which traps clicks on the queue rows below.
    const ov = document.getElementById('qSearchOverlay');
    if (ov) {
      ov.style.display = 'none';
      ov.classList.remove('open');
    }
    window._qSearchResults = null;
    _clearSearchFrames();
    if (typeof clearScenePick === 'function') clearScenePick();
  }

  // Video-frame strip for the Scene Search header. Samples five stills
  // via /api/thumb/generate at evenly-spaced percents so the user gets
  // a quick visual summary of the file while they search.
  const QUEUE_THUMB_COUNT_JS = 5;
  let _searchFrameToken = 0;
  //: Source URLs for the five film-strip frames in order — populated by
  //: `_loadSearchFrames` once each thumbnail arrives from the server.
  //: Drives the overlay prev/next navigation (keyboard + side buttons).
  let _searchFrameSources = [];
  let _searchFrameIndex = 0;
  function _clearSearchFrames() {
    const strip = document.getElementById('searchFrameStrip');
    if (!strip) return;
    strip.innerHTML = '';
    strip.style.display = 'none';
    _searchFrameToken++;
    _searchFrameSources = [];
    _searchFrameIndex = 0;
  }
  // Click handler for a filmstrip frame — sets the picked frame as
  // the manual-filing thumbnail. Replaces the prior behaviour of
  // opening a zoom overlay; clicking on the strip is a more natural
  // gesture for "use this frame as the thumb" since the strip already
  // shows the frames at full filmstrip size.
  function _pickFrameAsThumb(i) {
    const src = _searchFrameSources[i];
    if (!src) return;
    const pct = (_searchFramePercents && _searchFramePercents[i]);
    mThumbDataUrl = src;
    mThumbMode = 'generated';
    const img = document.getElementById('mThumbImg');
    const empty = document.getElementById('mThumbEmpty');
    const pctLabel = document.getElementById('mThumbPercent');
    if (img) {
      img.src = src;
      img.style.display = 'block';
    }
    if (empty) empty.style.display = 'none';
    if (pctLabel && pct != null) pctLabel.textContent = `Frame at ${pct}%`;
    if (typeof _mShowRegenBtn === 'function') _mShowRegenBtn();
    // Clear any pasted URL so the "use URL" mode doesn't fight the
    // chosen filmstrip frame.
    const urlEl = document.getElementById('mImageUrl');
    if (urlEl) urlEl.value = '';
    document.querySelectorAll('#searchFrameStrip > div').forEach((el, idx) => {
      el.classList.toggle('is-picked', idx === i);
    });
  }
  window._pickFrameAsThumb = _pickFrameAsThumb;

  // Percents the server returned for the current strip — kept here
  // so `_pickFrameAsThumb` can label the picked frame correctly even
  // though the percent set is now driven by the backend.
  let _searchFramePercents = [];

  async function _loadSearchFrames(filename) {
    const strip = document.getElementById('searchFrameStrip');
    if (!strip || !filename) return;
    // Bump the token so an in-flight load for a previous filename
    // can't paint into the current strip.
    const myToken = ++_searchFrameToken;
    strip.innerHTML = '';
    strip.style.display = 'flex';
    _searchFrameSources = [];
    _searchFramePercents = [];
    _searchFrameIndex = 0;
    // Show 5 placeholder slots up-front so the strip doesn't visually
    // pop in when the cached frames arrive. They'll be replaced as
    // each frame paints.
    const placeholderCount = 5;
    const slots = [];
    for (let i = 0; i < placeholderCount; i++) {
      const s = document.createElement('div');
      s.innerHTML = '<span class="loader loader--btn loader--muted" role="status" aria-label="Loading"></span>';
      strip.appendChild(s);
      slots.push(s);
    }

    // Poll loop. The endpoint is non-blocking — it returns whatever's
    // on disk plus a `generating` flag, dispatching the async prewarm
    // (gated to 2 concurrent ffmpeg seeks) when frames are missing.
    // Previously the request blocked inside _ensure_queue_thumbs, so
    // a slow (or backoff-d) file would freeze the popup for minutes.
    const startMs = Date.now();
    const POLL_INTERVAL_MS = 1500;
    // 60s ceiling — after a minute of "generating" without all 5 frames
    // appearing, ffmpeg is genuinely stuck or struggling on this file.
    // Stop polling so the user gets a clear "stalled, click Retry" state
    // instead of staring at pulsing loaders. The backoff fix (1h park
    // after a complete pass) also short-circuits a lot of this — if any
    // pass finishes with partial frames, the next poll flips backoff:
    // true and we drop out before the 60s deadline.
    const POLL_TIMEOUT_MS = 60 * 1000;  // 5 min hard ceiling

    const paintFromResponse = (d) => {
      const thumbs = Array.isArray(d.thumbs) ? d.thumbs : [];
      // Rebuild the strip every poll so partial frames appear as soon
      // as the prewarm finishes each ffmpeg seek (the snapshot writes
      // to disk between frames, not all-or-nothing).
      strip.innerHTML = '';
      // Pad to 5 slots so the strip width doesn't jitter as frames
      // trickle in. Missing slots get a spinner (generating) or an
      // empty-image icon (final state with fewer than 5 frames).
      const total = QUEUE_THUMB_COUNT_JS;
      _searchFrameSources = new Array(total).fill('');
      _searchFramePercents = new Array(total).fill(null);
      const byIdx = new Map(thumbs.map(t => [t.i, t]));
      for (let i = 0; i < total; i++) {
        const slot = document.createElement('div');
        const t = byIdx.get(i);
        if (t) {
          const src = t.data_url || t.url || '';
          const pct = t.percent;
          _searchFrameSources[i] = src;
          _searchFramePercents[i] = pct;
          if (src) {
            slot.innerHTML = `<img src="${src}" alt="Frame ${pct != null ? pct + '%' : ''}" data-frame-index="${i}" style="width:100%;height:100%;object-fit:cover;display:block;cursor:pointer" onclick="_pickFrameAsThumb(${i})" title="Use this frame as the thumbnail" loading="lazy">`;
          } else {
            slot.innerHTML = '<i class="fa-solid fa-image" style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);color:var(--dim);font-size:12px;opacity:0.4"></i>';
          }
        } else if (d.generating) {
          slot.innerHTML = '<span class="loader loader--btn loader--muted" role="status" aria-label="Generating" title="Generating frame…"></span>';
        } else if (d.backoff) {
          slot.innerHTML = `<button type="button" class="searchFrameRetryBtn" data-filename="${esc(filename)}" title="ffmpeg gave up on this file — click to retry" style="background:transparent;border:1px dashed rgba(244,114,182,0.45);border-radius:6px;color:rgba(244,114,182,0.85);padding:6px 8px;font-size:11px;cursor:pointer;display:flex;align-items:center;justify-content:center;width:100%;height:100%"><i class="fa-solid fa-rotate-right" style="font-size:14px;opacity:0.85"></i></button>`;
        } else {
          slot.innerHTML = '<i class="fa-solid fa-image" style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);color:var(--dim);font-size:12px;opacity:0.4"></i>';
        }
        strip.appendChild(slot);
      }
      // Bind retry buttons to a single delegated click that clears the
      // backoff entry server-side and restarts polling.
      strip.querySelectorAll('.searchFrameRetryBtn').forEach(btn => {
        btn.addEventListener('click', async (ev) => {
          ev.preventDefault();
          ev.stopPropagation();
          const fn = btn.getAttribute('data-filename') || filename;
          btn.disabled = true;
          btn.innerHTML = '<span class="loader loader--btn loader--muted" role="status" aria-label="Retrying"></span>';
          try {
            await fetch('/api/queue/thumbs/retry', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              credentials: 'same-origin',
              body: JSON.stringify({ filename: fn }),
            });
          } catch { /* fall through */ }
          _loadSearchFrames(fn);
        }, { once: true });
      });
    };

    try {
      let lastResponse = null;
      while (true) {
        const r = await fetch('/api/queue/thumbs?data=1&filename=' + encodeURIComponent(filename), { credentials: 'same-origin' });
        if (myToken !== _searchFrameToken) return;
        const d = await r.json().catch(() => ({}));
        if (myToken !== _searchFrameToken) return;
        lastResponse = d;
        paintFromResponse(d);
        if (d.ready || d.backoff || !d.generating) break;
        if (Date.now() - startMs > POLL_TIMEOUT_MS) {
          //: ffmpeg is stuck or unusually slow on this file. Force the
          //: paint into the same "backoff" shape so missing slots become
          //: retry buttons instead of perpetual loaders. We don't trust
          //: the prewarm to recover on its own here — Retry clears the
          //: server backoff (if any) and kicks off a fresh attempt.
          paintFromResponse({ ...d, backoff: true, generating: false });
          break;
        }
        await new Promise(res => setTimeout(res, POLL_INTERVAL_MS));
        if (myToken !== _searchFrameToken) return;
      }
      // First filmstrip frame doubles as the default thumbnail for
      // manual filing — populates the preview area automatically so
      // the user doesn't have to click "Random frame" to get a
      // sensible default. Skipped if the user has already chosen a
      // thumb manually (URL paste, Random, or a stored DB match).
      const firstSrc = _searchFrameSources[0] || '';
      const firstPct = _searchFramePercents[0];
      const firstIsDataUrl = firstSrc.startsWith('data:');
      if (firstSrc && firstIsDataUrl && mThumbMode === null) {
        mThumbDataUrl = firstSrc;
        mThumbMode = 'generated';
        const img = document.getElementById('mThumbImg');
        const empty = document.getElementById('mThumbEmpty');
        const pctLabel = document.getElementById('mThumbPercent');
        if (img) {
          img.src = firstSrc;
          img.style.display = 'block';
        }
        if (empty) empty.style.display = 'none';
        if (pctLabel) pctLabel.textContent = `Frame at ${firstPct != null ? firstPct : ''}%`;
        if (typeof _mShowRegenBtn === 'function') _mShowRegenBtn();
        // Mark the first slot as picked so the user sees which frame
        // the preview is showing.
        const firstSlot = strip.children[0];
        if (firstSlot) firstSlot.classList.add('is-picked');
      }
    } catch(_) {
      if (myToken !== _searchFrameToken) return;
      slots.forEach((s) => {
        s.innerHTML = '<i class="fa-solid fa-triangle-exclamation" style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);color:var(--red);font-size:12px;opacity:0.6"></i>';
      });
    }
  }
  function _raiseQueueImageOverlay() {
    const ov = document.getElementById('queueImageOverlay');
    if (!ov) return;
    // Reparent to <body> and move to the end so we paint above modals
    // (suggestionsOverlay is relocated to body with z-index 9998).
    if (ov.parentElement !== document.body) {
      document.body.appendChild(ov);
    } else {
      document.body.appendChild(ov);
    }
    ov.style.zIndex = '10050';
  }
  function _openQueueOverlay(indexOrSrc) {
    const ov = document.getElementById('queueImageOverlay');
    const im = document.getElementById('queueOverlayImg');
    if (!ov || !im) return;
    // Accepts either a film-strip index or a raw image URL. Result-row
    // thumbnails still pass the raw src, so we detect a string and
    // disable the prev/next chrome in that case.
    if (typeof indexOrSrc === 'number') {
      const idx = Math.max(0, Math.min((_searchFrameSources.length || 1) - 1, indexOrSrc));
      _searchFrameIndex = idx;
      im.src = _searchFrameSources[idx] || '';
      _updateQueueOverlayChrome(true);
    } else {
      im.src = String(indexOrSrc || '');
      _updateQueueOverlayChrome(false);
    }
    _raiseQueueImageOverlay();
    ov.style.display = 'flex';
  }
  function _openSuggestionsFrameOverlay(index) {
    _searchFrameSources = _suggestionsFrameSources.slice();
    _openQueueOverlay(index);
  }
  function _updateQueueOverlayChrome(isFilmStrip) {
    const prev = document.getElementById('queueOverlayPrev');
    const next = document.getElementById('queueOverlayNext');
    const counter = document.getElementById('queueOverlayCounter');
    const show = isFilmStrip && _searchFrameSources.length > 1;
    if (prev) prev.style.display = show ? 'inline-flex' : 'none';
    if (next) next.style.display = show ? 'inline-flex' : 'none';
    if (counter) {
      if (show) {
        counter.style.display = 'block';
        counter.textContent = (_searchFrameIndex + 1) + ' / ' + _searchFrameSources.length;
      } else {
        counter.style.display = 'none';
      }
    }
  }
  function queueOverlayStep(delta) {
    const n = _searchFrameSources.length;
    if (!n) return;
    // Wrap-around so left at index 0 jumps to the last frame and vice
    // versa — matches the expectations of a simple 5-frame carousel.
    let next = (_searchFrameIndex + delta) % n;
    if (next < 0) next += n;
    // Skip slots that failed to load (`_searchFrameSources[i] === ''`).
    // Walk at most `n` steps so we don't infinite-loop when every
    // frame is empty.
    let guard = n;
    while (!_searchFrameSources[next] && guard > 0) {
      next = (next + (delta || 1) + n) % n;
      guard--;
    }
    _searchFrameIndex = next;
    const im = document.getElementById('queueOverlayImg');
    if (im) im.src = _searchFrameSources[next] || '';
    _updateQueueOverlayChrome(true);
  }

  // ── Scene-confirm modal state ───────────────────────────────────────
  // _qConfirmPerformers holds the full cast (from the search row or the
  // on-demand IAFD enrichment). _qConfirmChecked is a parallel bool[]
  // — index-aligned with _qConfirmPerformers — tracking which names the
  // user has ticked; we start every row unchecked so an inherited-cast
  // IAFD scene doesn't sneak the whole movie into the filing by
  // accident. _submitScenePick() rebuilds the stash-box-shaped
  // performers array from just the ticked indices.
  let _qConfirmScene = null;
  let _qConfirmPerformers = [];
  let _qConfirmChecked = [];
  let _qConfirmFilename = '';
  //: IAFD-only: per-scene breakdowns from the title page, and which
  //: scene is currently picked. -1 = "use movie-level cast" (the old
  //: behaviour, for titles without a breakdown). Picking a scene
  //: pre-ticks only that scene's cast and appends " - Scene N" to
  //: the title preview.
  let _qConfirmScenes = [];
  let _qConfirmSceneIdx = -1;
  //: Title state — editable in the modal. `_qConfirmBaseTitle` is
  //: the untouched scene.title; `_qConfirmTitleEdited` flips when the
  //: user hand-edits so we stop auto-rewriting it on scene picks.
  let _qConfirmBaseTitle = '';
  let _qConfirmTitleEdited = false;
  //: Default for the "Submit to StashDB after filing" checkbox —
  //: read from the user's Databases setting (`submit_scene_stashdb`)
  //: so the modal reflects the same "always submit" rule the
  //: automated post-file flow uses. null = not fetched yet.
  let _qConfirmSubmitStashdbDefault = null;

  async function _getSubmitStashdbDefault() {
    if (_qConfirmSubmitStashdbDefault !== null) return _qConfirmSubmitStashdbDefault;
    try {
      const r = await fetch('/api/settings', { credentials: 'same-origin' });
      const s = await r.json();
      _qConfirmSubmitStashdbDefault = (s.submit_scene_stashdb || 'false') === 'true';
    } catch(_) {
      _qConfirmSubmitStashdbDefault = false;
    }
    return _qConfirmSubmitStashdbDefault;
  }

  async function useThisScene(idx, sceneOverride) {
    if (!_qSelectedFile) { window.toast('No file selected'); return; }
    const scene = window._qSearchResults[idx]?._raw;
    if (!scene) return;

    // IAFD search results ship without a cast or a breakdown (the
    // search page only has title/year/studio). Fetch the movie's
    // detail page so the picker can offer a per-scene chooser *and*
    // have a fallback cast list. Other sources already carry
    // scene-level performers so we skip the extra request.
    const isIafd = typeof scene.id === 'string' && scene.id.startsWith('iafd:');
    const hasPerformers = Array.isArray(scene.performers) && scene.performers.length > 0;
    let enrichNote = '';
    let breakdowns = [];
    if (isIafd) {
      const iafdUrl = scene._iafd_url || scene.id.slice(5);
      try {
        const r = await fetch('/api/iafd/scene-performers', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ url: iafdUrl }),
        });
        const d = await r.json();
        if (!hasPerformers && Array.isArray(d.performers)) scene.performers = d.performers;
        if (Array.isArray(d.scenes)) breakdowns = d.scenes;
        //: IAFD search results don't carry a synopsis — only the
        //: detail page does. Stash it on the scene object so the
        //: plot-import fallthrough below picks it up.
        if (typeof d.synopsis === 'string' && d.synopsis.trim()) {
          scene.synopsis = d.synopsis.trim();
        }
      } catch(e) { /* best-effort — picker still shows with what we have */ }
    }
    // Multi-scene IAFD movie + no pre-chosen scene: open the picker
    // popup so the user narrows the cast to one scene before the
    // manual form populates. Picker re-enters this function with
    // sceneOverride pinned. Clicking "Use whole movie" calls back
    // with sceneOverride={} which falls through to the whole-movie
    // path with the enrich-toast.
    if (isIafd && breakdowns.length > 1 && !sceneOverride) {
      _openIafdScenePicker(idx, scene, breakdowns);
      return;
    }
    // Apply the scene override (from the picker) by narrowing the
    // performers list to that scene's cast and tweaking the title.
    if (isIafd && sceneOverride && sceneOverride.scene) {
      const sc = sceneOverride.scene;
      const castNames = Array.isArray(sc.cast) ? sc.cast.filter(Boolean) : [];
      scene.performers = castNames.map(n => ({ performer: { id: '', name: String(n).trim() } }));
      const baseTitle = String(scene.title || '').trim();
      const label = sc.label || ('Scene ' + (sc.number || (sceneOverride.idx + 1)));
      scene.title = baseTitle ? (baseTitle + ' - ' + label) : label;
    }
    if (isIafd && breakdowns.length > 1 && sceneOverride && !sceneOverride.scene) {
      enrichNote = 'IAFD lists the full movie cast — tick only the performers in this scene.';
    } else if (isIafd && Array.isArray(scene.performers) && scene.performers.length > 0 && breakdowns.length <= 1) {
      enrichNote = 'IAFD lists the full movie cast — tick only the performers in this scene.';
    }

    _qConfirmScene = scene;
    _qConfirmPerformers = (scene.performers || []).map(p => ({
      id:   (p.performer && p.performer.id)   || '',
      name: (p.performer && p.performer.name) || '',
    })).filter(p => p.name);
    _qConfirmChecked = _qConfirmPerformers.map(() => false);
    _qConfirmFilename = _qSelectedFile;
    _qConfirmScenes = breakdowns;
    _qConfirmSceneIdx = -1;
    _qConfirmBaseTitle = String(scene.title || '').trim();
    _qConfirmTitleEdited = false;

    // Populate the left filter fields with the picked scene's metadata
    // so the user can review/edit before filing. Title/Studio/Date are
    // the canonical fields; performers go straight into the unified
    // pill input so the user can untick by clicking the pill's X.
    const titleEl   = document.getElementById('srchTitle');
    const studioEl  = document.getElementById('srchStudio');
    const dateEl    = document.getElementById('srchDate');
    const performerEl = document.getElementById('srchPerformer');
    const plotEl    = document.getElementById('mPlot');
    if (titleEl)     titleEl.value     = _qConfirmBaseTitle;
    const studioName = (scene.studio && scene.studio.name) || scene.studio || '';
    if (studioEl)    studioEl.value    = studioName;
    if (dateEl)      dateEl.value      = (scene.release_date || scene.date || '');
    if (performerEl) performerEl.value = '';
    if (typeof _perfPillsSet === 'function') {
      // Make sure the library index is loaded so the pills know which
      // names route to existing folders (green) vs. free-text (purple).
      try { if (typeof _mPerfLoadLibrary === 'function') await _mPerfLoadLibrary(); } catch {}
      _perfPillsSet(_qConfirmPerformers.map(p => p.name));
    }
    //: Different upstream DBs use different field names for the
    //: scene blurb. StashDB / FansDB ship `details`; TPDB ships
    //: `description`; IAFD scrapers sometimes put it under
    //: `synopsis`. Fall through them so the plot lands wherever
    //: it actually lives.
    if (plotEl) {
      const plotSrc = (scene.details || scene.description || scene.synopsis || scene.plot || '').toString().trim();
      if (plotSrc) plotEl.value = plotSrc;
    }
    //: Hydrate the Tags pill input from whichever shape the upstream
    //: DB used. Stash-box returns ``tags: [{name}]``; TPDB returns
    //: ``tags: [{name}]`` or ``tag_objects``. Bare-string variants are
    //: also accepted. The user can still edit/remove pills after.
    if (typeof _tagPillsSet === 'function') {
      const rawTags = scene.tags || scene.tag_objects || [];
      const names = [];
      if (Array.isArray(rawTags)) {
        for (const t of rawTags) {
          if (typeof t === 'string') {
            const v = t.trim(); if (v) names.push(v);
          } else if (t && typeof t === 'object') {
            const n = String(t.name || '').trim();
            if (n) names.push(n);
          }
        }
      }
      _tagPillsSet(names);
    }
    //: Thumbnail — populate from whichever URL the scene carried. The
    //: search row's `.image` is already a normalised string; the _raw
    //: payload may also have `images[0].url`, `poster`, or `thumb`
    //: depending on source. Falling back through them gives us a hit
    //: on every database.
    const row = window._qSearchResults[idx] || {};
    let posterUrl = (row.image || '').trim();
    if (!posterUrl) {
      const imgs = scene.images || [];
      if (Array.isArray(imgs) && imgs.length) {
        const first = imgs[0];
        posterUrl = (first && (first.url || first)) || '';
      }
    }
    if (!posterUrl) posterUrl = (scene.poster || scene.image || scene.thumb || '').toString();
    if (posterUrl) {
      const imgEl = document.getElementById('mImageUrl');
      if (imgEl) imgEl.value = posterUrl;
      mThumbMode = 'url';
      mThumbDataUrl = null;
      if (typeof mThumbPreview === 'function') mThumbPreview(posterUrl);
    }

    //: The Filing-from inline panel was removed entirely. Title,
    //: studio, date, plot, thumbnail and cast (as pills) all
    //: populate directly into the manual form fields, and the
    //: right-column STASH submit toggle is the single
    //: submit-to-external-DB control. Notice anything you'd want to
    //: surface to the user (e.g. the IAFD enrich note) is toasted
    //: instead of rendered inline.
    if (enrichNote) {
      try { window.toast(enrichNote); } catch {}
    }
    //: Persistent confirmation banner — surfaces inside the manual-
    //: extras header so the user has a clear visual cue that the
    //: click landed and the form is now pre-filled. Without this the
    //: row click is silent (form fields fill but nothing else
    //: changes), which read as "couldn't select" in field reports.
    try {
      //: ``row.source`` is the API's normalised label ("IAFD",
      //: "StashDB", "TPDB", "FansDB", "JAVStash"). Fall through to
      //: scene._source / id-prefix for older payloads that may lack
      //: it.
      const srcLabel = (row && row.source) ? row.source
        : (typeof scene._source === 'string' ? scene._source
          : (isIafd ? 'IAFD'
            : ((scene.source || '') || '')));
      const sceneIdForLink = (scene && scene.id) || '';
      const titleForBanner = String(scene.title || 'scene').trim() || 'scene';
      const msg = `Selected: ${titleForBanner} — review form below + click Submit to file`;
      _mRenderMatchBanner(srcLabel, sceneIdForLink, msg);
      //: Make sure the user sees it — the banner lives inside the
      //: manual-extras toggle, which is hidden when extras is
      //: collapsed. Force-open so the confirmation is immediately
      //: visible on every "Use this" click.
      if (typeof _setManualExtrasOpen === 'function') _setManualExtrasOpen(true);
    } catch (exc) {
      if (typeof console !== 'undefined') console.warn('[queue] match banner paint failed', exc);
    }
  }

  /* IAFD scene-picker (Scene Search panel).
   *
   * Opens when the user clicks an IAFD movie row whose detail page
   * has >1 scenes in the breakdown. The picker lists each scene's
   * label + cast; clicking a row pins that scene and re-enters
   * useThisScene() so the manual form populates with the narrowed
   * cast and a "Movie - Scene N" title. "Use whole movie" footer
   * button bails out of the picker and files against the full cast
   * (legacy behaviour from before this fix). Esc / backdrop close
   * cancels — no form changes. */
  let _qIafdPickerCtx = null;
  function _openIafdScenePicker(idx, scene, breakdowns) {
    _qIafdPickerCtx = { idx, scene, breakdowns };
    const listEl = document.getElementById('qIafdScenePickerList');
    const hintEl = document.getElementById('qIafdScenePickerHint');
    if (hintEl) {
      const tt = String(scene.title || '').trim();
      hintEl.textContent = tt
        ? `“${tt}” — IAFD lists the full movie cast. Pick the scene that matches this file to narrow the performers.`
        : 'IAFD lists the full movie cast — pick the scene that matches this file to narrow the performers.';
    }
    listEl.innerHTML = breakdowns.map((sc, i) => {
      const label = sc.label || ('Scene ' + (sc.number || (i + 1)));
      const cast  = (sc.cast || []).filter(Boolean).join(', ');
      return `<button type="button" class="iafd-search-scene-pick" data-idx="${i}"
        style="display:flex;flex-direction:column;align-items:flex-start;gap:4px;padding:12px 14px;border:1px solid rgba(var(--brand-purple-rgb),0.22);border-radius:8px;background:linear-gradient(160deg, rgba(var(--brand-purple-rgb),0.06) 0%, rgba(0,0,0,0.20) 100%);color:var(--text);text-align:left;cursor:pointer;font:inherit;width:100%;transition:border-color 0.15s, background 0.15s"
        onmouseover="this.style.borderColor='rgba(var(--brand-accent-rgb), 0.55)';this.style.background='linear-gradient(160deg, rgba(var(--brand-accent-rgb),0.10) 0%, rgba(0,0,0,0.22) 100%)'"
        onmouseout="this.style.borderColor='rgba(var(--brand-purple-rgb),0.22)';this.style.background='linear-gradient(160deg, rgba(var(--brand-purple-rgb),0.06) 0%, rgba(0,0,0,0.20) 100%)'">
        <span style="font-family:var(--secs);font-size:13px;letter-spacing:0.04em;text-transform:uppercase">${esc(label)}</span>
        <span style="font-size:12px;color:var(--dim);line-height:1.4">${cast ? esc(cast) : '<em style="color:var(--muted)">No cast listed</em>'}</span>
      </button>`;
    }).join('') || '<div class="empty" style="padding:24px;text-align:center;color:var(--dim);font-size:12px">No scenes listed for this movie.</div>';
    const overlay = document.getElementById('qIafdScenePickerOverlay');
    overlay.classList.add('open');
  }
  function closeIafdScenePicker() {
    const overlay = document.getElementById('qIafdScenePickerOverlay');
    if (overlay) overlay.classList.remove('open');
    _qIafdPickerCtx = null;
  }
  window.closeIafdScenePicker = closeIafdScenePicker;
  // "Use whole movie" footer button — re-enters useThisScene with an
  // empty override so the form populates against the full movie cast.
  function useThisSceneWholeMovie() {
    if (!_qIafdPickerCtx) { closeIafdScenePicker(); return; }
    const { idx } = _qIafdPickerCtx;
    closeIafdScenePicker();
    useThisScene(idx, { scene: null });
  }
  window.useThisSceneWholeMovie = useThisSceneWholeMovie;
  // Delegated picker-row click. Reads the scene index off the button's
  // data-idx and routes back through useThisScene with a sceneOverride
  // that pins the picked scene's cast + label.
  document.addEventListener('click', function (ev) {
    const btn = ev.target && ev.target.closest && ev.target.closest('.iafd-search-scene-pick');
    if (!btn) return;
    if (!_qIafdPickerCtx) return;
    const sceneIdx = parseInt(btn.getAttribute('data-idx') || '-1', 10);
    if (sceneIdx < 0) return;
    const { idx, breakdowns } = _qIafdPickerCtx;
    const sc = breakdowns[sceneIdx];
    closeIafdScenePicker();
    if (sc) useThisScene(idx, { scene: sc, idx: sceneIdx });
  });
  // Esc closes the picker.
  document.addEventListener('keydown', function (ev) {
    if (ev.key !== 'Escape') return;
    const overlay = document.getElementById('qIafdScenePickerOverlay');
    if (overlay && overlay.classList.contains('open')) {
      ev.preventDefault();
      closeIafdScenePicker();
    }
  });

  function clearScenePick() {
    _qConfirmScene = null;
    _qConfirmPerformers = [];
    _qConfirmChecked = [];
    _qConfirmFilename = '';
    _qConfirmScenes = [];
    _qConfirmSceneIdx = -1;
    _qConfirmBaseTitle = '';
    _qConfirmTitleEdited = false;
    const performerEl = document.getElementById('srchPerformer');
    if (performerEl) delete performerEl.dataset.scenePickAuto;
    if (typeof _perfPillsClear === 'function') _perfPillsClear();
  }
  window.clearScenePick = clearScenePick;

  /* POST a manual-file endpoint with auto-retry when the backend returns
   * 409 "Pipeline already running". The pipeline lock is short-lived
   * (one file per processing tick) so a few-second wait usually
   * clears it — much friendlier than throwing an error at the user
   * mid-sort. Retries every 2.5s for up to 60s, with a button-status
   * callback so the UI can show progress.
   *
   * url defaults to /api/file/manual (scene-pick path); manual-metadata
   * submissions pass /api/file/manual/metadata so the same wait-and-retry
   * behaviour applies regardless of which submit path the user takes.
   *
   * Resolves to the parsed JSON response on success; throws if the
   * pipeline stays busy past the timeout, or on non-409 errors. */
  async function _postManualFileWithRetry(body, onWait, url) {
    const RETRY_DELAY_MS  = 2500;
    const MAX_WAIT_MS     = 60000;
    const target = url || '/api/file/manual';
    const start = Date.now();
    let attempt = 0;
    while (true) {
      attempt += 1;
      const r = await fetch(target, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(body),
      });
      // 409 "Pipeline already running" → wait + retry
      if (r.status === 409) {
        let blob = {};
        try { blob = await r.json(); } catch (_) {}
        const errMsg = String(blob.error || '');
        if (/pipeline already running/i.test(errMsg)) {
          if (Date.now() - start > MAX_WAIT_MS) {
            const seconds = Math.round((Date.now() - start) / 1000);
            const e = new Error(`Pipeline busy after ${seconds}s — try again later.`);
            e._pipelineBusy = true;
            throw e;
          }
          if (typeof onWait === 'function') {
            onWait(attempt, Math.round((Date.now() - start) / 1000));
          }
          await new Promise(res => setTimeout(res, RETRY_DELAY_MS));
          continue;
        }
        // Some other 409 (e.g. bulk filing already running) — bubble up.
        const e = new Error(errMsg || 'Conflict');
        throw e;
      }
      // Non-409 path: parse and return. Caller decides what to do
      // with `error` fields in the body.
      const d = await r.json();
      // Server-side manual-action queue now defers filing behind an
      // in-flight pipeline instead of returning 409. Surface a subtle
      // toast so the user sees confirmation that their submission is
      // queued (they no longer see the modal's retry-in-progress
      // countdown, since we return 200 immediately).
      if (d && d.queued && typeof window.toast === 'function') {
        const pos = Number(d.queued_position || 0);
        const suffix = pos > 1 ? ` (position ${pos})` : '';
        window.toast(`Pipeline busy — queued for filing${suffix}`, { kind: 'success' });
      }
      return d;
    }
  }

  /* Search-query word-match highlight helpers (`_qsBuildHighlightSet`,
   * `_qsHighlight`, `_qsPerformerCsvHtml`) live in ts-utils.js so
   * /queue can share them. CSS `.qs-match` is per-page (below). */

  /* Surface codec / resolution / duration / size of the queue file
   * being searched, alongside the filename in the modal header. Ships
   * a one-liner like:
   *    Filing: abc.mp4 · 1920×1080 · h264 · 28m 14s · 412 MB
   * Falls back gracefully when ffprobe can't read the file (`null`
   * fields are skipped). */
  function _fmtResLabel(w, h) {
    if (!w || !h) return '';
    const minDim = Math.min(w, h);
    if (minDim >= 2160) return `${w}×${h} 4K`;
    if (minDim >= 1080) return `${w}×${h} 1080p`;
    if (minDim >= 720)  return `${w}×${h} 720p`;
    if (minDim >= 480)  return `${w}×${h} SD`;
    return `${w}×${h}`;
  }
  function _fmtDuration(secs) {
    if (!secs || secs <= 0) return '';
    const s = Math.round(secs);
    const h = Math.floor(s / 3600);
    const m = Math.floor((s % 3600) / 60);
    const r = s % 60;
    if (h) return `${h}h ${String(m).padStart(2,'0')}m`;
    if (m) return `${m}m ${String(r).padStart(2,'0')}s`;
    return `${r}s`;
  }
  async function _loadSearchFileMeta(filename, openSeq) {
    const lbl = document.getElementById('searchFileLabel');
    if (!lbl) return;
    try {
      const r = await fetch('/api/queue/file-probe?filename=' + encodeURIComponent(filename), { credentials: 'same-origin' });
      // Don't paint stale results — newer search opened during fetch.
      if (openSeq !== _qOpenSearchSeq) return;
      if (!r.ok) return;
      const d = await r.json();
      const bits = [`Filing: ${filename}`];
      const res = _fmtResLabel(d.width, d.height);
      if (res) bits.push(res);
      if (d.codec) bits.push(String(d.codec).toUpperCase());
      const dur = _fmtDuration(d.duration);
      if (dur) bits.push(dur);
      if (d.size_mb) bits.push(`${d.size_mb} MB`);
      lbl.textContent = bits.join(' · ');
    } catch (_) { /* keep filename-only label */ }
  }

  /* Per-search source toggle. Selection persists in localStorage so it
   * survives reloads. Default is all-on. */
  const _QS_SOURCE_KEY = 'ts_qsearch_sources';
  function _qsLoadSources() {
    try {
      const raw = localStorage.getItem(_QS_SOURCE_KEY);
      if (!raw) return null;
      const arr = JSON.parse(raw);
      return Array.isArray(arr) ? arr : null;
    } catch (_) { return null; }
  }
  function _qsSaveSources(arr) {
    try { localStorage.setItem(_QS_SOURCE_KEY, JSON.stringify(arr)); } catch (_) {}
  }
  // Pills that existed at the time the saved-selection format was last
  // released. When a new pill is added (e.g. JAVStash), users with a
  // saved selection from before then won't have it in their list — so
  // we treat "absent and not in this known set" as "new pill → default
  // on". Existing toggles still apply (anything in this set that's
  // missing from saved was explicitly turned off).
  const _QS_KNOWN_PILLS = ['StashDB', 'TPDB', 'FansDB', 'IAFD'];
  function _qsApplySourcesToUI() {
    const saved = _qsLoadSources();
    document.querySelectorAll('.qs-source-pill').forEach(p => {
      const id = p.getAttribute('data-source');
      let on;
      if (!saved) on = true;
      else if (saved.includes(id)) on = true;
      else if (!_QS_KNOWN_PILLS.includes(id)) on = true;
      else on = false;
      p.classList.toggle('is-on', on);
    });
  }
  function _qsCurrentSources() {
    return Array.from(document.querySelectorAll('.qs-source-pill.is-on'))
      .map(p => p.getAttribute('data-source'));
  }

  // Stored phash match for the currently-open queue row. Populated by
  // the openManualSearch match-scene fetch; cleared on every fresh
  // open / clearSearch. The search-results renderer pins this at the
  // top of the list so the user can accept it with one click.
  let _qStoredMatchScene = null;
  function _qsNormalizeMatchSource(src) {
    const s = (src || '').toLowerCase();
    if (s === 'tpdb' || s === 'theporndb') return 'TPDB';
    if (s === 'stashdb') return 'StashDB';
    if (s === 'fansdb')  return 'FansDB';
    if (s === 'javstash') return 'JAVStash';
    return src || '';
  }
  // Render the stored-match row as the only entry in qSearchResults
  // when nothing has been searched yet. Skips when search results are
  // already on screen — the prepend in runQueueSearch handles that case.
  function _qsRenderStoredMatchIfIdle() {
    if (!_qStoredMatchScene) return;
    const el = document.getElementById('qSearchResults');
    if (!el) return;
    // If runQueueSearch has already painted real results, leave them
    // alone — those renders prepend the stored match themselves.
    if (Array.isArray(window._qSearchResults) && window._qSearchResults.length) return;
    window._qSearchResults = [_qsBuildStoredMatchResult()];
    el.innerHTML = `<div style="display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px">${_qsRenderStoredMatchRow(0)}</div>`;
  }
  // Build a /api/search-shaped row from the stored match so the existing
  // useThisScene(idx) handler can consume it without a special branch.
  function _qsBuildStoredMatchResult() {
    const m = _qStoredMatchScene || {};
    const perfNames = (m.performers || '').split(',').map(s => s.trim()).filter(Boolean);
    return {
      _stored: true,
      source: m.source,
      id: m.id,
      title: m.title || '',
      date: m.date || '',
      studio: m.studio || '',
      performers: m.performers || '',
      performer_count: perfNames.length,
      image: m.image || '',
      view_url: m.view_url || '',
      _raw: {
        id: m.id,
        title: m.title || '',
        release_date: m.date || '',
        studio: { name: m.studio || '' },
        performers: perfNames.map(n => ({ performer: { id: '', name: n } })),
        details: m.description || '',
        description: m.description || '',
        images: m.image ? [{ url: m.image, width: 0, height: 0 }] : [],
        _source: m.source,
      },
    };
  }
  // Render the stored-match row HTML — same shape as runQueueSearch's
  // own row template, with an extra "Stored match" badge so the user
  // can tell it apart from fresh search results.
  function _qsRenderStoredMatchRow(idx) {
    const m = _qStoredMatchScene || {};
    const thumbSrc = m.image || '/static/img/missing.webp';
    const thumb = `<img src="${thumbSrc}" onclick="event.stopPropagation();openQueueImageOverlay(this.src)" style="width:140px;height:80px;object-fit:cover;border-radius:6px;cursor:zoom-in;display:block;flex-shrink:0" onerror="this.onerror=null;this.src='/static/img/missing.webp'" loading="lazy">`;
    const srcLabel = esc(m.source || '');
    const sourceLogo = m.source === 'TPDB' ? '/static/logos/tpdb.webp'
      : m.source === 'StashDB' ? '/static/logos/stashdb.webp'
      : m.source === 'FansDB'  ? '/static/logos/fansdb.webp'
      : m.source === 'JAVStash' ? '/static/logos/javstash.webp'
      : '';
    const logoImg = sourceLogo
      ? `<img src="${sourceLogo}" alt="${srcLabel}" style="height:22px;width:auto;display:block;flex-shrink:0" onerror="this.replaceWith(Object.assign(document.createElement('span'),{textContent:'${srcLabel}',style:'font-size:11px;font-weight:600;color:var(--accent)'}))" loading="lazy">`
      : `<span style="font-size:11px;font-weight:600;letter-spacing:0.04em;text-transform:uppercase;color:var(--accent);padding:2px 7px;border-radius:4px">${srcLabel}</span>`;
    const sourceTile = `<span style="flex-shrink:0;display:inline-flex;align-items:center;padding:4px 8px">${logoImg}</span>`;
    const storedBadge = `<span style="flex-shrink:0;font-size:10px;font-weight:600;letter-spacing:0.06em;text-transform:uppercase;padding:3px 8px;border-radius:4px;background:rgba(var(--brand-accent-rgb),0.18);border:1px solid rgba(var(--brand-accent-rgb),0.45);color:var(--accent)">phash match</span>`;
    const canFile = !!m.date;
    const useBtn = _qSelectedFile
      ? (canFile
        ? `<button type="button" onclick="event.stopPropagation();useThisScene(${idx})" style="flex-shrink:0;background:linear-gradient(135deg, rgba(var(--brand-accent-rgb), 0.30) 0%, rgba(var(--brand-accent-rgb), 0.18) 100%);border:1px solid rgba(var(--brand-accent-rgb), 0.55);border-radius:8px;color:var(--text);padding:8px 14px;cursor:pointer;font-family:var(--secs);font-size:12px;font-weight:600;letter-spacing:0.04em;text-transform:uppercase;white-space:nowrap;display:inline-flex;align-items:center;gap:6px"><i class="fa-solid fa-check"></i> Use this</button>`
        : `<span style="font-size:11px;color:var(--muted);flex-shrink:0" title="No date">No date</span>`)
      : `<span style="font-size:11px;color:var(--text);flex-shrink:0">No file selected</span>`;
    const rowClickable = !!(_qSelectedFile && canFile);
    const rowOnClick = rowClickable ? ` onclick="useThisScene(${idx})"` : '';
    const rowCursor = rowClickable ? 'cursor:pointer;' : '';
    const perfNamesPipe = (m.performers || '').split(',').map(s => s.trim()).filter(Boolean).join('|');
    const perfPile = perfNamesPipe
      ? `<div class="ts-perf-pile" data-perf-names="${esc(perfNamesPipe)}" data-cap="5" style="margin-top:6px"></div>`
      : '';
    return `<div${rowOnClick} style="display:flex;gap:14px;align-items:center;padding:12px;border-radius:10px;border:1px solid rgba(var(--brand-accent-rgb),0.45);background:linear-gradient(160deg, rgba(var(--brand-accent-rgb),0.10) 0%, rgba(0,0,0,0.18) 100%);${rowCursor}min-width:0">
        ${thumb}
        <div style="flex:1;min-width:0;display:flex;flex-direction:column;justify-content:center;gap:4px">
          <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">
            ${sourceTile}
            ${storedBadge}
            <span style="font-size:12px;color:var(--dim);flex-shrink:0">${esc(m.date||'')}</span>
          </div>
          <div style="font-size:14px;color:var(--text);font-weight:500;line-height:1.35">${esc(m.title || '')}</div>
          <div style="font-size:11px;color:var(--dim)">${esc(m.studio||'')}${m.performers ? ' · ' + esc(m.performers) : ''}</div>
          ${perfPile}
        </div>
        <div style="flex-shrink:0;align-self:center">${useBtn}</div>
      </div>`;
  }
  function toggleQSearchSource(btn) {
    btn.classList.toggle('is-on');
    _qsSaveSources(_qsCurrentSources());
  }
  window.toggleQSearchSource = toggleQSearchSource;
  // Apply the saved selection on page load.
  _qsApplySourcesToUI();

  async function runQueueSearch() {
    const mySeq = ++_qRunSearchSeq;
    const title     = document.getElementById('srchTitle').value.trim();
    //: Join the locked-pill names + whatever's currently being typed
    //: into a single CSV — preserves the legacy search semantics so the
    //: backend doesn't have to learn a new shape.
    const _perfTyped = (document.getElementById('srchPerformer').value || '').trim();
    const _perfFromPills = (typeof _perfPillsValue === 'function') ? _perfPillsValue() : '';
    const performer = [_perfFromPills, _perfTyped].filter(Boolean).join(', ').trim();
    const studio    = document.getElementById('srchStudio').value.trim();
    const termRaw   = document.getElementById('srchTerm').value.trim();
    const term      = (title || performer || studio) ? '' : termRaw;
    const sources   = _qsCurrentSources();
    if (!sources.length) {
      const el = document.getElementById('qSearchResults');
      if (el) el.innerHTML = '<div class="empty">Select at least one source above to search.</div>';
      return;
    }
    // Single Date input expands to a ±7-day window for search — gives
    // the GraphQL query some slack so a slightly-off date in the
    // filename parser still hits. Manual filing uses the raw date as-is.
    const rawDate = document.getElementById('srchDate').value.trim();
    let dateFrom = '', dateTo = '';
    if (rawDate) {
      const m = rawDate.match(/^(\d{4})-(\d{2})-(\d{2})$/);
      if (m) {
        const base = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3]));
        const lo = new Date(base.getTime() - 7 * 86400 * 1000);
        const hi = new Date(base.getTime() + 7 * 86400 * 1000);
        const fmt = d => d.toISOString().slice(0, 10);
        dateFrom = fmt(lo);
        dateTo = fmt(hi);
      } else {
        // Malformed date — pass through as both bounds so the backend
        // still gets *something* and the user sees no silent drop.
        dateFrom = rawDate;
        dateTo = rawDate;
      }
    }
    const payload = { term, title, performer, studio, sources, date_from: dateFrom, date_to: dateTo };
    const resultsEl = document.getElementById('qSearchResults');
    resultsEl.innerHTML = '<div style="padding:48px;text-align:center;color:var(--dim);font-size:13px"><span class="loader loader--block" role="status" aria-label="Loading"></span>Searching…</div>';
    try {
      const r = await fetch('/api/search', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload) });
      //: Newer search fired while this one was in flight — drop the
      //: stale results so they don't overwrite whatever the newer
      //: search is rendering / will render.
      if (mySeq !== _qRunSearchSeq) return;
      const d = await r.json();
      if (mySeq !== _qRunSearchSeq) return;
      if (d.error) { resultsEl.innerHTML = `<div class="empty">${d.error}</div>`; return; }
      // Pin the stored phash match at the top of the results so the
      // user can accept the already-known scene in one click. The
      // stored match is the strongest evidence we have — phash beats
      // any fresh name search.
      let prependedStored = false;
      let combined = d.results;
      if (_qStoredMatchScene) {
        const storedRow = _qsBuildStoredMatchResult();
        const dupIdx = (d.results || []).findIndex(r =>
          (r.id || '') === storedRow.id && (r.source || '').toLowerCase() === (storedRow.source || '').toLowerCase());
        if (dupIdx >= 0) {
          combined = [storedRow, ...d.results.slice(0, dupIdx), ...d.results.slice(dupIdx + 1)];
        } else {
          combined = [storedRow, ...d.results];
        }
        prependedStored = true;
      }
      if (!combined.length) { resultsEl.innerHTML = '<div class="empty">No results found</div>'; return; }
      window._qSearchResults = combined;
      // Build the highlight set ONCE per search from the title +
      // performer query fields. Stop words are dropped, words are
      // lowercased. Reused inside the row-render closure for both
      // titles and performer-name CSV.
      const qsHighlight = _qsBuildHighlightSet(title, performer);
      //: 2-column grid for the search results — packs roughly twice as
      //: many rows above the fold. We render the row HTML the same way
      //: and just rely on grid-template-columns on the wrapper. The
      //: per-row margin-bottom from the legacy single-column layout
      //: is now redundant; the grid `gap` handles spacing instead.
      const rowsHtml = combined.map((row, idx) => {
        if (row._stored) {
          return _qsRenderStoredMatchRow(idx);
        }
        // Fall back to /static/img/missing.webp so every row has a consistent
        // 140×80 slot — avoids the jarring empty frame when a DB row
        // genuinely has no image (common on IAFD results). Shrunk from
        // 160×90 to give the 2-col layout more text room.
        const thumbSrc = row.image || '/static/img/missing.webp';
        const thumb = `<img src="${thumbSrc}" onclick="event.stopPropagation();openQueueImageOverlay(this.src)" style="width:140px;height:80px;object-fit:cover;border-radius:6px;cursor:zoom-in;display:block;flex-shrink:0" onerror="this.onerror=null;this.src='/static/img/missing.webp'" loading="lazy">`;
        // Backend emits `source: "TPDB"` (see main.py:7397) — the prior
        // `'ThePornDB'` check never matched, so TPDB rows fell through
        // to the plain-text-label branch instead of showing the logo.
        const sourceLogo = (row.source === 'TPDB' || row.source === 'ThePornDB') ? '/static/logos/tpdb.webp'
          : row.source === 'FansDB'  ? '/static/logos/fansdb.webp'
          : row.source === 'IAFD'    ? '/static/logos/iafd.webp'
          : row.source === 'StashDB' ? '/static/logos/stashdb.webp'
          : row.source === 'JAVStash' ? '/static/logos/javstash.webp'
          : '';
        // Resolve the View URL in a fixed order so IAFD rows never leak
        // into the stash-box fallback:
        //   1. Backend-provided view_url (correct by construction)
        //   2. id starts with `iafd:` — strip prefix, the rest is an
        //      absolute IAFD title URL
        //   3. Known stash-box source → build the scene URL from id
        //   4. Otherwise: no link (better than a wrong one)
        let viewHref = row.view_url || null;
        if (!viewHref && typeof row.id === 'string' && row.id.startsWith('iafd:')) {
          viewHref = row.id.slice(5);
        }
        if (!viewHref && row.id && row.source) {
          if      (row.source === 'TPDB' || row.source === 'ThePornDB') viewHref = 'https://theporndb.net/scenes/' + row.id;
          else if (row.source === 'FansDB')    viewHref = 'https://fansdb.cc/scenes/'    + row.id;
          else if (row.source === 'StashDB')   viewHref = 'https://stashdb.org/scenes/'  + row.id;
          else if (row.source === 'JAVStash')  viewHref = 'https://javstash.org/scenes/' + row.id;
        }
        const srcLabel = esc(row.source || '');
        const logoImg = sourceLogo
          ? `<img src="${sourceLogo}" alt="${srcLabel}" style="height:22px;width:auto;display:block;flex-shrink:0" onerror="this.replaceWith(Object.assign(document.createElement('span'),{textContent:'${srcLabel}',style:'font-size:11px;font-weight:600;color:var(--accent)'}))" loading="lazy">`
          : `<span style="font-size:11px;font-weight:600;letter-spacing:0.04em;text-transform:uppercase;color:var(--accent);padding:2px 7px;border-radius:4px">${srcLabel}</span>`;
        const sourceTile = viewHref
          ? `<a href="${viewHref}" target="_blank" title="Open on ${srcLabel}" onclick="event.stopPropagation()" style="flex-shrink:0;display:inline-flex;align-items:center;padding:4px 8px;border-radius:6px;border:1px solid transparent;background:rgba(255,255,255,0.04);text-decoration:none;transition:border-color 0.12s, background 0.12s" onmouseover="this.style.borderColor='rgba(var(--brand-purple-rgb),0.5)';this.style.background='rgba(var(--brand-purple-rgb),0.08)'" onmouseout="this.style.borderColor='transparent';this.style.background='rgba(255,255,255,0.04)'">${logoImg}</a>`
          : `<span style="flex-shrink:0;display:inline-flex;align-items:center;padding:4px 8px">${logoImg}</span>`;
        const metaBits = [];
        if (row.performer_count > 0) metaBits.push(`${row.performer_count} star${row.performer_count === 1 ? '' : 's'}`);
        if (row.duration > 0) {
          const mins = Math.floor(row.duration / 60);
          const secs = row.duration % 60;
          metaBits.push(mins >= 60
            ? `${Math.floor(mins / 60)}h ${mins % 60}m`
            : `${mins}m${secs ? ' ' + secs + 's' : ''}`);
        }
        if (row.code) metaBits.push(esc(row.code));
        const metaLine = metaBits.length
          ? `<span style="font-size:11px;color:var(--dim)">${metaBits.join(' · ')}</span>`
          : '';
        const canFile = !!row.date;
        const useBtn = _qSelectedFile
          ? (canFile
            ? `<button type="button" onclick="event.stopPropagation();useThisScene(${idx})" style="flex-shrink:0;background:linear-gradient(135deg, rgba(var(--brand-accent-rgb), 0.30) 0%, rgba(var(--brand-accent-rgb), 0.18) 100%);border:1px solid rgba(var(--brand-accent-rgb), 0.55);border-radius:8px;color:var(--text);padding:8px 14px;cursor:pointer;font-family:var(--secs);font-size:12px;font-weight:600;letter-spacing:0.04em;text-transform:uppercase;white-space:nowrap;display:inline-flex;align-items:center;gap:6px;transition:background 0.15s, border-color 0.15s" onmouseenter="this.style.background='linear-gradient(135deg, rgba(var(--brand-accent-rgb), 0.45) 0%, rgba(var(--brand-accent-rgb), 0.28) 100%)';this.style.borderColor='rgba(var(--brand-accent-rgb), 0.85)'" onmouseleave="this.style.background='linear-gradient(135deg, rgba(var(--brand-accent-rgb), 0.30) 0%, rgba(var(--brand-accent-rgb), 0.18) 100%)';this.style.borderColor='rgba(var(--brand-accent-rgb), 0.55)'"><i class="fa-solid fa-check"></i> Use this</button>`
            : `<span style="font-size:11px;color:var(--muted);flex-shrink:0" title="No date">No date</span>`)
          : `<span style="font-size:11px;color:var(--text);flex-shrink:0">No file selected</span>`;
        // Headshot pile slot — populated by `_hydratePerfPiles` once
        // every search-result row is rendered. Single fetch covers
        // every name across all rows.
        const perfNamesPipe = (row.performers || '').split(',').map(s => s.trim()).filter(Boolean).join('|');
        const perfPile = perfNamesPipe
          ? `<div class="ts-perf-pile" data-perf-names="${esc(perfNamesPipe)}" data-cap="5" style="margin-top:6px"></div>`
          : '';
        const rowClickable = !!(_qSelectedFile && canFile);
        const rowOnClick = rowClickable ? ` onclick="useThisScene(${idx})"` : '';
        const rowCursor = rowClickable ? 'cursor:pointer;' : '';
        return `<div${rowOnClick} style="display:flex;gap:14px;align-items:center;padding:12px;border-radius:10px;border:1px solid rgba(var(--brand-purple-rgb),0.16);background:linear-gradient(160deg, rgba(var(--brand-purple-rgb),0.06) 0%, rgba(0,0,0,0.18) 100%);transition:border-color 0.15s, transform 0.15s;${rowCursor}min-width:0" onmouseover="this.style.borderColor='rgba(var(--brand-accent-rgb), 0.45)'" onmouseout="this.style.borderColor='rgba(var(--brand-purple-rgb),0.16)'">
          ${thumb}
          <div style="flex:1;min-width:0;display:flex;flex-direction:column;justify-content:center;gap:4px">
            <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">
              ${sourceTile}
              <span style="font-size:12px;color:var(--dim);flex-shrink:0">${esc(row.date||'')}</span>
              ${metaLine}
            </div>
            <div style="font-size:14px;color:var(--text);font-weight:500;line-height:1.35">${_qsHighlight(row.title, qsHighlight)}</div>
            <div style="font-size:11px;color:var(--dim)">${esc(row.studio||'')}${row.performers ? ' · ' + _qsPerformerCsvHtml(row.performers, qsHighlight) : ''}</div>
            ${perfPile}
          </div>
          <div style="flex-shrink:0;align-self:center">${useBtn}</div>
        </div>`;
      }).join('');
      resultsEl.innerHTML = `<div style="display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px">${rowsHtml}</div>`;
      _hydratePerfPiles(resultsEl);
      if (typeof enrichPerformerNames === 'function') enrichPerformerNames(resultsEl);
    } catch(e) {
      if (mySeq !== _qRunSearchSeq) return;
      resultsEl.innerHTML = `<div class="empty">Search failed: ${e.message}</div>`;
    }
  }

  function clearSearch() {
    // Bump the open-seq so any in-flight openManualSearch continuations
    // (filename parse, match-scene seed, settings probe) bail before
    // painting into the freshly emptied form. Without this, a stale
    // late-arriving fetch can repopulate srchTitle/srchPerformer/etc.
    // moments after the user closed the modal.
    _qOpenSearchSeq++;
    _qStoredMatchScene = null;
    ['srchTerm','srchTitle','srchPerformer','srchStudio','srchDate','mPlot','mImageUrl','srchTag'].forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });
    if (typeof _perfPillsClear === 'function') _perfPillsClear();
    if (typeof _tagPillsClear === 'function') _tagPillsClear();
    //: Reset Kind to auto-detect so the next openManualSearch starts
    //: fresh and re-detects from its own filename.
    const _kindEl = document.getElementById('srchExtraKind');
    if (_kindEl) _kindEl.value = '__auto__';
    const sr = document.getElementById('qSearchResults');
    if (sr) sr.innerHTML = '<div class="empty">Enter search terms above and press Search</div>';
    _qSelectedFile = null;
    _qManualFile = null;
    const lbl = document.getElementById('searchFileLabel');
    if (lbl) lbl.style.display = 'none';
    document.querySelectorAll('.queue-section .queue-item').forEach(el => el.classList.remove('active'));
    const sp = document.getElementById('qSearchPanel');
    if (sp) sp.style.display = 'none';
    const ov = document.getElementById('qSearchOverlay');
    if (ov) {
      ov.style.display = 'none';
      ov.classList.remove('open');
    }
    // Collapse the manual-extras section and reset its state so the
    // next file opens with a clean slate.
    if (typeof _setManualExtrasOpen === 'function') _setManualExtrasOpen(false);
    if (typeof _mResetThumb === 'function') _mResetThumb();
    const matchBanner = document.getElementById('manualMatchBanner');
    if (matchBanner) { matchBanner.style.display = 'none'; matchBanner.innerHTML = ''; }
    ['mStashdbResolveResults','mStashdbResolveStatus','mFansdbResolveResults','mFansdbResolveStatus','mJavstashResolveResults','mJavstashResolveStatus'].forEach(id => { const el = document.getElementById(id); if (el) el.innerHTML = ''; });
    const stashChk = document.getElementById('mSubmitStashdb');
    const fansChk  = document.getElementById('mSubmitFansdb');
    const javChk   = document.getElementById('mSubmitJavstash');
    if (stashChk) { stashChk.checked = false; document.getElementById('mStashdbFields').style.display = 'none'; }
    if (fansChk)  { fansChk.checked  = false; document.getElementById('mFansdbFields').style.display  = 'none'; }
    if (javChk)   { javChk.checked   = false; document.getElementById('mJavstashFields').style.display = 'none'; }
    mThumbMode = null; mThumbDataUrl = null;
    if (typeof mResolved !== 'undefined') {
      mResolved.stashdb  = { studio: null, performers: [] };
      mResolved.fansdb   = { studio: null, performers: [] };
      mResolved.javstash = { studio: null, performers: [] };
    }
    _clearSearchFrames();
    // Inline scene-pick state lives alongside the search form — wipe
    // it so a fresh openManualSearch doesn't inherit a prior pick.
    if (typeof clearScenePick === 'function') clearScenePick();
    // Defensive: a queueImageOverlay opened from the inline filmstrip
    // can stick around if the user closes the search panel without
    // dismissing the lightbox first — kill it so its invisible-ish
    // dark backdrop doesn't trap clicks on the main page.
    const qov = document.getElementById('queueImageOverlay');
    if (qov && qov.style.display === 'flex' && typeof closeQueueImageOverlay === 'function') {
      closeQueueImageOverlay();
    }
  }

  // Manual-extras collapse/expand. Default state is collapsed so the
  // results body takes the full panel; clicking the toggle (or
  // pressing the "File manually" button while collapsed) reveals it.
  function _setManualExtrasOpen(open) {
    const body = document.getElementById('qManualExtrasBody');
    const chev = document.getElementById('qManualExtrasChevron');
    if (!body || !chev) return;
    body.style.display = open ? 'block' : 'none';
    // Chevron is fa-chevron-down at rest (visually correct for the
    // expanded default). Rotate -90° when collapsed so it points left.
    chev.style.transform = open ? 'rotate(0deg)' : 'rotate(-90deg)';
  }
  function toggleManualExtras() {
    const body = document.getElementById('qManualExtrasBody');
    if (!body) return;
    _setManualExtrasOpen(body.style.display !== 'block');
    // No need to re-pin results height — they live in the right
    // column and aren't affected by the left column's extras toggle.
  }

  // ── Movie search (centered modal, unified results) ──
  const _MOVIE_SRCH_SRC = {
    tmdb: { logo: 'tmdb', label: 'TMDB', attr: 'data-tmdb-id', pick: 'pickMovieTmdb' },
    tpdb: { logo: 'tpdb', label: 'TPDB', attr: 'data-tpdb-id', pick: 'pickMovieTpdb' },
    javstash: { logo: 'javstash', label: 'JAVStash', attr: 'data-javstash-id', pick: 'pickMovieJavstash' },
  };

  function _movieSrchPickBtn(key, id, sel) {
    const s = _MOVIE_SRCH_SRC[key];
    const dis = !sel;
    const tip = dis ? 'Select a queue file first' : ('File using ' + s.label);
    return '<button type="button" class="movie-srch-pick" title="' + esc(tip) + '"'
      + (dis ? ' disabled' : '')
      + ' ' + s.attr + '="' + esc(String(id)) + '"'
      + ' onclick="' + s.pick + '(this)">'
      + '<img class="movie-srch-pick__logo" src="/static/logos/' + s.logo + '.webp" alt="' + esc(s.label) + '">'
      + '</button>';
  }

  function _movieSrchRow(key, item, sel) {
    const poster = item.poster
      ? '<img src="' + esc(item.poster) + '" alt="" loading="lazy" onerror="this.classList.add(\'is-broken\')">'
      : '<span class="movie-srch-thumb-ph"><i class="ts-icon-movies" aria-hidden="true"></i></span>';
    const meta = item.meta ? '<div class="movie-srch-row__meta">' + item.meta + '</div>' : '';
    return '<div class="movie-srch-row">'
      + '<div class="movie-srch-row__thumb">' + poster + '</div>'
      + '<div class="movie-srch-row__body">'
      + '<div class="movie-srch-row__title">' + esc(item.title || '') + '</div>'
      + meta
      + '</div>'
      + _movieSrchPickBtn(key, item.id, sel)
      + '</div>';
  }

  function closeMovieSearchPanel() {
    const ov = document.getElementById('qMovieSearchOverlay');
    if (ov) {
      ov.style.display = 'none';
      ov.classList.remove('open');
    }
    _qSelectedMovieFile = null;
    const lbl = document.getElementById('movieSearchFileLabel');
    if (lbl) lbl.style.display = 'none';
    document.querySelectorAll('.queue-section .queue-item').forEach(el => el.classList.remove('active'));
  }
  function clearMovieSearchPick() { closeMovieSearchPanel(); }

  async function openMovieManualSearch(filename) {
    _qSelectedMovieFile = filename;
    const lbl = document.getElementById('movieSearchFileLabel');
    if (lbl) {
      lbl.textContent = filename;
      lbl.style.display = 'block';
    }
    document.querySelectorAll('.queue-section .queue-item').forEach(el => el.classList.remove('active'));
    if (window._queueFiles) {
      const idx = window._queueFiles.findIndex(f => f.filename === filename);
      if (idx !== -1) document.getElementById('qi-' + idx)?.classList.add('active');
    }
    const stem = filename.replace(/\.[^.]+$/, '').replace(/[._]/g, ' ');
    document.getElementById('movieSrchQ').value = stem;
    document.getElementById('movieSrchYear').value = '';
    const y = filename.match(/\b(19\d{2}|20\d{2})\b/);
    if (y) document.getElementById('movieSrchYear').value = y[1];
    const ov = document.getElementById('qMovieSearchOverlay');
    if (ov) {
      ov.style.display = 'flex';
      ov.classList.add('open');
    }
    document.getElementById('movieSrchQ')?.focus();
    runMovieSearch();
  }

  async function runMovieSearch() {
    const q = document.getElementById('movieSrchQ').value.trim();
    const year = document.getElementById('movieSrchYear').value.trim();
    const el = document.getElementById('movieSearchResults');
    if (!q) { el.innerHTML = '<div class="empty">Enter a title</div>'; return; }
    el.innerHTML = '<div class="empty">Searching…</div>';
    let tmdb = [], tpdbRes = [], javRes = [], tmdbErr = '';
    try {
      const [rt, rp] = await Promise.all([
        fetch('/api/movies/tmdb-search?q=' + encodeURIComponent(q) + (year ? '&year=' + encodeURIComponent(year) : '')).then(r => r.json()),
        fetch('/api/movies/search?q=' + encodeURIComponent(q) + (year ? '&year=' + encodeURIComponent(year) : '')).then(r => r.json()),
      ]);
      if (rt.results) tmdb = rt.results;
      if (rt.error) tmdbErr = String(rt.error);
      if (rp.results) tpdbRes = rp.results;
      if (rp.jav_scenes) javRes = rp.jav_scenes;
    } catch { el.innerHTML = '<div class="empty">Search failed</div>'; return; }
    const sel = !!_qSelectedMovieFile;
    const rows = [];
    (tmdb || []).slice(0, 12).forEach(function (m) {
      rows.push(_movieSrchRow('tmdb', {
        id: m.id,
        title: m.title,
        meta: m.year ? esc(String(m.year)) : '',
        poster: m.poster_url || '',
      }, sel));
    });
    (tpdbRes || []).slice(0, 12).forEach(function (m) {
      rows.push(_movieSrchRow('tpdb', {
        id: m.id,
        title: m.title,
        meta: m.date ? esc(String(m.date)) : '',
        poster: m.poster || '',
      }, sel));
    });
    (javRes || []).slice(0, 12).forEach(function (s) {
      const meta = [s.studio, s.date].filter(Boolean).map(function (x) { return esc(String(x)); }).join(' · ');
      const poster = (s.poster && String(s.poster).trim()) || (s.thumb && String(s.thumb).trim()) || '';
      rows.push(_movieSrchRow('javstash', { id: s.id, title: s.title || '', meta: meta, poster: poster }, sel));
    });
    const warn = tmdbErr ? '<div class="movie-srch-warn">' + esc(tmdbErr) + '</div>' : '';
    el.innerHTML = warn + '<div class="movie-srch-list">' + (rows.join('') || '<div class="movie-srch-empty">No results</div>') + '</div>';
  }

  /* Shared post-pick refresh — invalidates the cached movie queue
   * payload and triggers an immediate reload so the just-filed row
   * disappears without waiting for the 5s status poll. */
  function _refreshAfterMoviePick() {
    _movieQueuePayload = null;
    if (typeof loadQueue === 'function') loadQueue({ preserveView: true, force: true });
    if (typeof loadQueueStats === 'function') loadQueueStats();
  }

  /* If the /api/movies/file response says the item was queued behind
   * the pipeline instead of starting immediately, surface a subtle
   * toast so the user knows their pick was accepted. */
  function _toastIfQueued(d, label) {
    if (!d || !d.queued || typeof window.toast !== 'function') return;
    const pos = Number(d.queued_position || 0);
    const suffix = pos > 1 ? ` (position ${pos})` : '';
    window.toast(`Pipeline busy — ${label} queued${suffix}`, { kind: 'success' });
  }

  async function pickMovieTmdb(btn) {
    const tmdbId = btn.getAttribute('data-tmdb-id');
    if (!_qSelectedMovieFile) { window.toast('No file selected'); return; }
    const r = await fetch('/api/movies/file', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ filename: _qSelectedMovieFile, tmdb_id: tmdbId }) });
    const d = await r.json().catch(() => ({}));
    if (d.error) { window.toast(d.error); return; }
    _toastIfQueued(d, 'movie filing');
    _qIsRunning = true; const b = document.getElementById('btnRunAll'); if (b) b.disabled = true;
    closeMovieSearchPanel();
    _refreshAfterMoviePick();
  }

  async function pickMovieTpdb(btn) {
    const tpdbId = btn.getAttribute('data-tpdb-id');
    if (!_qSelectedMovieFile) { window.toast('No file selected'); return; }
    const r = await fetch('/api/movies/file', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ filename: _qSelectedMovieFile, tpdb_id: tpdbId }) });
    const d = await r.json().catch(() => ({}));
    if (d.error) { window.toast(d.error); return; }
    _toastIfQueued(d, 'movie filing');
    _qIsRunning = true; const b = document.getElementById('btnRunAll'); if (b) b.disabled = true;
    closeMovieSearchPanel();
    _refreshAfterMoviePick();
  }

  async function pickMovieJavstash(btn) {
    const jid = btn.getAttribute('data-javstash-id');
    if (!_qSelectedMovieFile) { window.toast('No file selected'); return; }
    const r = await fetch('/api/movies/file', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ filename: _qSelectedMovieFile, javstash_id: jid }) });
    const d = await r.json().catch(() => ({}));
    if (d.error) { window.toast(d.error); return; }
    _toastIfQueued(d, 'movie filing');
    _qIsRunning = true; const b = document.getElementById('btnRunAll'); if (b) b.disabled = true;
    closeMovieSearchPanel();
    _refreshAfterMoviePick();
  }

  // ── Image overlay ──
  function openQueueImageOverlay(src) {
    document.getElementById('queueOverlayImg').src = src;
    _raiseQueueImageOverlay();
    document.getElementById('queueImageOverlay').style.display = 'flex';
  }
  function closeQueueImageOverlay() {
    document.getElementById('queueImageOverlay').style.display = 'none';
    document.getElementById('queueOverlayImg').src = '';
    _updateQueueOverlayChrome(false);
  }

  // ── Manual metadata modal ──
  let mThumbMode = null, mThumbDataUrl = null;
  // Library performer index for the autocomplete dropdown. Lazy loaded
  // the first time the modal opens; refreshed only if explicitly
  // invalidated elsewhere.
  let _mPerfLibrary = null;
  let _mPerfActiveIdx = -1;
  //: Pill state for the unified Stars input. Each pill = { name,
  //: inLibrary } — `inLibrary` colours the chip differently so the
  //: user can see at a glance which entries route to an existing
  //: performer folder vs. a free-text name. Replaces both the legacy
  //: CSV input and the scene-pick "tick to include" list.
  let _perfPills = [];

  function _perfPillsClear() {
    _perfPills = [];
    _perfPillsRender();
  }

  function _perfPillsSet(names, opts) {
    //: opts.libCheck = run a library lookup so new pills get coloured
    //: as in-library when applicable. Cheap once the library has been
    //: loaded — falls back to "not in library" if the load hasn't run
    //: yet.
    const lib = _mPerfLibrary || [];
    const libIndex = new Map();
    for (const e of lib) libIndex.set(String(e.name || '').trim().toLowerCase(), true);
    _perfPills = (names || []).map(n => {
      const nm = String(n || '').trim();
      return { name: nm, inLibrary: libIndex.has(nm.toLowerCase()) };
    }).filter(p => p.name);
    _perfPillsRender();
  }

  function _perfPillsAdd(name) {
    const nm = String(name || '').trim();
    if (!nm) return;
    const dup = _perfPills.some(p => p.name.toLowerCase() === nm.toLowerCase());
    if (dup) return;
    const lib = _mPerfLibrary || [];
    const inLib = lib.some(e => String(e.name || '').trim().toLowerCase() === nm.toLowerCase());
    _perfPills.push({ name: nm, inLibrary: inLib });
    _perfPillsRender();
  }

  function _perfPillsRemoveAt(i) {
    if (i < 0 || i >= _perfPills.length) return;
    _perfPills.splice(i, 1);
    _perfPillsRender();
  }

  function _perfPillsValue() {
    return _perfPills.map(p => p.name).join(', ');
  }
  function _perfPillsNames() {
    return _perfPills.map(p => p.name);
  }
  window._perfPillsValue = _perfPillsValue;
  window._perfPillsNames = _perfPillsNames;

  // Tag pills — simpler twin of the performer pill input. No library
  // lookup (tags aren't catalogued client-side; the backend resolves
  // each pill against the upstream stash-box tag list at submit time).
  // Comma-separated input, Enter or comma locks the in-progress text
  // into a pill, click X to remove.
  let _tagPills = [];

  function _tagPillsClear() {
    _tagPills = [];
    _tagPillsRender();
  }

  function _tagPillsSet(names) {
    _tagPills = (names || [])
      .map(n => String(n || '').trim())
      .filter(Boolean);
    _tagPillsRender();
  }

  function _tagPillsAdd(name) {
    const nm = String(name || '').trim();
    if (!nm) return;
    const dup = _tagPills.some(t => t.toLowerCase() === nm.toLowerCase());
    if (dup) return;
    _tagPills.push(nm);
    _tagPillsRender();
  }

  function _tagPillsRemoveAt(i) {
    if (i < 0 || i >= _tagPills.length) return;
    _tagPills.splice(i, 1);
    _tagPillsRender();
  }

  function _tagPillsValue() {
    return _tagPills.join(', ');
  }
  function _tagPillsNames() {
    return _tagPills.slice();
  }
  window._tagPillsValue = _tagPillsValue;
  window._tagPillsNames = _tagPillsNames;

  function _tagPillsRender() {
    const host = document.getElementById('srchTagChips');
    const input = document.getElementById('srchTag');
    if (!host) return;
    host.innerHTML = _tagPills.map((nm, i) => {
      const bg = 'rgba(var(--brand-accent-rgb),0.14)';
      const border = 'rgba(var(--brand-accent-rgb),0.45)';
      const color = 'var(--accent)';
      return `<span class="qs-pill" data-idx="${i}" title="Tag" style="display:inline-flex;align-items:center;gap:6px;padding:3px 4px 3px 10px;border-radius:14px;background:${bg};border:1px solid ${border};color:${color};font-family:var(--mono);font-size:11px;letter-spacing:0.02em;line-height:1.4;white-space:nowrap">
        <span>${esc(nm)}</span>
        <button type="button" class="qs-tag-pill-x" data-idx="${i}" title="Remove" aria-label="Remove ${esc(nm)}" style="background:rgba(0,0,0,0.30);border:none;color:inherit;width:16px;height:16px;border-radius:50%;cursor:pointer;display:flex;align-items:center;justify-content:center;font-size:9px;padding:0;flex-shrink:0"><i class="fa-solid fa-xmark"></i></button>
      </span>`;
    }).join('');
    host.querySelectorAll('.qs-tag-pill-x').forEach(btn => {
      btn.addEventListener('click', (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        _tagPillsRemoveAt(parseInt(btn.dataset.idx, 10));
        if (input) input.focus();
      });
    });
    if (input) {
      input.placeholder = _tagPills.length ? '' : 'Add tag, comma to lock…';
    }
  }

  // Wire the Tags input — comma or Enter commits the in-progress
  // token to a pill; Backspace at empty input drops the last pill so
  // the user doesn't have to mouse over to the X.
  function _tagPillsBindInput() {
    const input = document.getElementById('srchTag');
    if (!input || input._tagBound) return;
    input._tagBound = true;
    input.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ',') {
        const v = (input.value || '').trim().replace(/,$/, '');
        if (v) {
          e.preventDefault();
          _tagPillsAdd(v);
          input.value = '';
        } else if (e.key === ',') {
          e.preventDefault();
        }
        return;
      }
      if (e.key === 'Backspace' && !input.value && _tagPills.length) {
        e.preventDefault();
        _tagPillsRemoveAt(_tagPills.length - 1);
      }
    });
    input.addEventListener('input', () => {
      // Bulk-paste of "a, b, c" auto-splits on the comma.
      const v = input.value || '';
      if (v.includes(',')) {
        const parts = v.split(',');
        const last = parts.pop();
        parts.forEach(p => _tagPillsAdd(p));
        input.value = last.trimStart();
      }
    });
    input.addEventListener('blur', () => {
      const v = (input.value || '').trim();
      if (v) {
        _tagPillsAdd(v);
        input.value = '';
      }
    });
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _tagPillsBindInput);
  } else {
    _tagPillsBindInput();
  }

  function _perfPillsRender() {
    const host = document.getElementById('srchPerformerChips');
    const input = document.getElementById('srchPerformer');
    if (!host) return;
    host.innerHTML = _perfPills.map((p, i) => {
      const inLib = !!p.inLibrary;
      const bg = inLib ? 'rgba(74,222,128,0.14)' : 'rgba(var(--brand-purple-rgb),0.16)';
      const border = inLib ? 'rgba(74,222,128,0.45)' : 'rgba(var(--brand-purple-rgb),0.45)';
      const color = inLib ? '#4ade80' : 'var(--accent)';
      const tip = inLib ? 'In library' : 'Free-text name';
      return `<span class="qs-pill" data-idx="${i}" title="${esc(tip)}" style="display:inline-flex;align-items:center;gap:6px;padding:3px 4px 3px 10px;border-radius:14px;background:${bg};border:1px solid ${border};color:${color};font-family:var(--mono);font-size:11px;letter-spacing:0.02em;line-height:1.4;white-space:nowrap">
        <span>${esc(p.name)}</span>
        <button type="button" class="qs-pill-x" data-idx="${i}" title="Remove" aria-label="Remove ${esc(p.name)}" style="background:rgba(0,0,0,0.30);border:none;color:inherit;width:16px;height:16px;border-radius:50%;cursor:pointer;display:flex;align-items:center;justify-content:center;font-size:9px;padding:0;flex-shrink:0"><i class="fa-solid fa-xmark"></i></button>
      </span>`;
    }).join('');
    host.querySelectorAll('.qs-pill-x').forEach(btn => {
      btn.addEventListener('click', (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        _perfPillsRemoveAt(parseInt(btn.dataset.idx, 10));
        if (input) input.focus();
      });
    });
    if (input) {
      input.placeholder = _perfPills.length ? '' : 'Type a name…';
    }
  }
  // Resolution state keyed per-target so StashDB and FansDB can be
  // tracked independently. Each entry: { studio, performers }.
  const mResolved = {
    stashdb:  { studio: null, performers: [] },
    fansdb:   { studio: null, performers: [] },
    javstash: { studio: null, performers: [] },
  };

  async function _mPerfLoadLibrary() {
    if (_mPerfLibrary) return _mPerfLibrary;
    try {
      const r = await fetch('/api/performers');
      const d = await r.json();
      // Flatten to a search-friendly shape once, reuse on every keystroke.
      _mPerfLibrary = (d.performers || []).map(p => ({
        id: p.id,
        name: p.folder_name || '',
        aliases: Array.isArray(p.aliases) ? p.aliases : [],
        image_url: p.image_url || '',
      })).filter(p => p.name);
    } catch {
      _mPerfLibrary = [];
    }
    return _mPerfLibrary;
  }

  function _mPerfCurrentToken(input) {
    //: Pill input only carries the in-progress token — the locked
    //: pills live in `_perfPills`. Return the trimmed value directly.
    const rawToken = input.value || '';
    return {
      tokenStart: 0,
      tokenEnd: rawToken.length,
      rawToken,
      trimmed: rawToken.trim(),
    };
  }

  function _mPerfRenderSuggestions(matches, tokenText) {
    const box = document.getElementById('srchPerfSuggestions');
    if (!box) return;
    if (!matches.length && !tokenText) { box.style.display = 'none'; box.innerHTML = ''; return; }
    let html = matches.slice(0, 8).map((m, i) => {
      const img = m.image_url ? `<img src="${esc(m.image_url)}" onerror="this.style.visibility='hidden'" loading="lazy">` : `<img alt="" style="visibility:hidden">`;
      const aliasHit = m._aliasMatch ? `<span class="manual-perf-suggestion-alias">alias: ${esc(m._aliasMatch)}</span>` : '';
      return `<div class="manual-perf-suggestion${i === _mPerfActiveIdx ? ' active' : ''}" data-idx="${i}" data-name="${esc(m.name)}">${img}<span class="manual-perf-suggestion-name">${esc(m.name)}</span>${aliasHit}</div>`;
    }).join('');
    // Always offer the raw-typed token as "use this name" so free-text
    // entries for performers not in the library stay supported.
    if (tokenText) {
      const exists = matches.some(m => m.name.toLowerCase() === tokenText.toLowerCase());
      if (!exists) {
        html += `<div class="manual-perf-suggestion-new" data-new="1" data-name="${esc(tokenText)}">Use &ldquo;${esc(tokenText)}&rdquo; as typed</div>`;
      }
    }
    box.innerHTML = html;
    box.style.display = html ? 'block' : 'none';
  }

  function _mPerfScoreMatch(entry, q) {
    const name = entry.name.toLowerCase();
    if (name === q) return 100;
    if (name.startsWith(q)) return 50 + (q.length / name.length) * 10;
    if (name.includes(q)) return 30 + (q.length / name.length) * 5;
    for (const a of entry.aliases) {
      const al = (a || '').toLowerCase();
      if (!al) continue;
      if (al === q) { entry._aliasMatch = a; return 70; }
      if (al.startsWith(q)) { entry._aliasMatch = a; return 40 + (q.length / al.length) * 8; }
      if (al.includes(q)) { entry._aliasMatch = a; return 20 + (q.length / al.length) * 4; }
    }
    return 0;
  }

  async function _mPerfHandleInput() {
    const input = document.getElementById('srchPerformer');
    if (!input) return;
    const tok = _mPerfCurrentToken(input);
    const q = tok.trimmed.toLowerCase();
    const lib = await _mPerfLoadLibrary();
    if (!q) { _mPerfRenderSuggestions([], ''); return; }
    const scored = lib.map(e => { delete e._aliasMatch; return { e, s: _mPerfScoreMatch(e, q) }; })
      .filter(x => x.s > 0)
      .sort((a, b) => b.s - a.s)
      .slice(0, 20)
      .map(x => x.e);
    _mPerfActiveIdx = scored.length ? 0 : -1;
    _mPerfRenderSuggestions(scored, tok.trimmed);
  }

  function _mPerfAcceptSuggestion(name) {
    const input = document.getElementById('srchPerformer');
    if (!input) return;
    _perfPillsAdd(name);
    input.value = '';
    document.getElementById('srchPerfSuggestions').style.display = 'none';
    _mPerfActiveIdx = -1;
    input.focus();
  }

  function _mPerfHandleKeydown(e) {
    const input = e.target;
    const box = document.getElementById('srchPerfSuggestions');
    const suggOpen = box && box.style.display !== 'none';
    const items = suggOpen ? box.querySelectorAll('.manual-perf-suggestion, .manual-perf-suggestion-new') : [];
    if (e.key === 'ArrowDown' && items.length) {
      e.preventDefault();
      _mPerfActiveIdx = Math.min(items.length - 1, _mPerfActiveIdx + 1);
      items.forEach((el, i) => el.classList.toggle('active', i === _mPerfActiveIdx));
      return;
    }
    if (e.key === 'ArrowUp' && items.length) {
      e.preventDefault();
      _mPerfActiveIdx = Math.max(0, _mPerfActiveIdx - 1);
      items.forEach((el, i) => el.classList.toggle('active', i === _mPerfActiveIdx));
      return;
    }
    if (e.key === 'Enter') {
      e.preventDefault();
      if (items.length && _mPerfActiveIdx >= 0 && _mPerfActiveIdx < items.length) {
        _mPerfAcceptSuggestion(items[_mPerfActiveIdx].dataset.name || '');
        return;
      }
      const typed = (input.value || '').trim();
      if (typed) {
        _perfPillsAdd(typed);
        input.value = '';
        if (box) { box.style.display = 'none'; box.innerHTML = ''; }
        _mPerfActiveIdx = -1;
      } else {
        //: No typed text + Enter → user wants to run the search with
        //: whatever pills are already locked in.
        if (typeof runQueueSearch === 'function') runQueueSearch();
      }
      return;
    }
    if (e.key === 'Escape') {
      if (box) box.style.display = 'none';
      _mPerfActiveIdx = -1;
      return;
    }
    if (e.key === 'Backspace' && !input.value && _perfPills.length) {
      e.preventDefault();
      _perfPillsRemoveAt(_perfPills.length - 1);
      return;
    }
    if (e.key === ',' || e.key === ';') {
      //: Convenience: comma / semicolon also commits the pill, matching
      //: the legacy CSV input's muscle memory.
      const typed = (input.value || '').trim();
      if (typed) {
        e.preventDefault();
        _perfPillsAdd(typed);
        input.value = '';
        if (box) { box.style.display = 'none'; box.innerHTML = ''; }
      }
    }
  }

  function _mPerfWireHandlers() {
    const input = document.getElementById('srchPerformer');
    const box = document.getElementById('srchPerfSuggestions');
    if (!input || !box || input.dataset.wired === '1') return;
    input.dataset.wired = '1';
    input.addEventListener('input', _mPerfHandleInput);
    input.addEventListener('keydown', _mPerfHandleKeydown);
    input.addEventListener('focus', _mPerfHandleInput);
    input.addEventListener('blur', () => { setTimeout(() => { box.style.display = 'none'; }, 150); });
    box.addEventListener('mousedown', (e) => {
      const row = e.target.closest('[data-name]');
      if (!row) return;
      e.preventDefault();
      _mPerfAcceptSuggestion(row.dataset.name || '');
    });
  }

  // The Random-frame and Re-roll buttons are mutually exclusive: only
  // one is ever visible. Random-frame is the default; the moment a
  // frame is generated it disappears and Re-roll takes its place.
  // Resetting the thumbnail flips them back.
  function _mShowRandBtn() {
    const rand = document.getElementById('mRandBtn');
    const regen = document.getElementById('mRegenBtn');
    if (rand)  rand.style.display  = 'inline-flex';
    if (regen) regen.style.display = 'none';
  }
  function _mShowRegenBtn() {
    const rand = document.getElementById('mRandBtn');
    const regen = document.getElementById('mRegenBtn');
    if (rand)  rand.style.display  = 'none';
    if (regen) regen.style.display = 'inline-flex';
  }

  function _mResetThumb() {
    const img = document.getElementById('mThumbImg');
    const empty = document.getElementById('mThumbEmpty');
    if (img) { img.style.display = 'none'; img.src = ''; }
    if (empty) empty.style.display = 'flex';
    document.getElementById('mThumbPercent').textContent = '';
    _mShowRandBtn();
  }

  function mThumbPreview(url) {
    const img = document.getElementById('mThumbImg');
    const empty = document.getElementById('mThumbEmpty');
    if (!url) { _mResetThumb(); return; }
    if (img) {
      img.onerror = () => { _mResetThumb(); };
      img.src = url;
      img.style.display = 'block';
    }
    if (empty) empty.style.display = 'none';
    document.getElementById('mThumbPercent').textContent = '';
    _mShowRandBtn();
  }

  // openManual is now an alias into the merged Scene-Search modal —
  // the same surface hosts both name-search and manual-entry. We
  // route through openManualSearch (which seeds the shared srch*
  // fields + manual-extras state) and then auto-expand the extras
  // section so the user sees the thumb / plot / DB-submit controls
  // straight away.
  async function openManual(filename) {
    await openManualSearch(filename);
    _setManualExtrasOpen(true);
  }

  function _mRenderMatchBanner(source, externalId, statusMsg) {
    const el = document.getElementById('manualMatchBanner');
    if (!el) return;
    const src = (source || '').toLowerCase();
    let label = source || '', logo = '', url = '';
    if (src === 'stashdb')       { label = 'StashDB'; logo = '/static/logos/stashdb.webp'; url = 'https://stashdb.org/scenes/' + externalId; }
    else if (src === 'javstash') { label = 'JAVStash'; logo = '/static/logos/javstash.webp'; url = 'https://javstash.org/scenes/' + externalId; }
    else if (src === 'tpdb' || src === 'theporndb') { label = 'TPDB'; logo = '/static/logos/tpdb.webp'; url = 'https://theporndb.net/scenes/' + externalId; }
    else if (src === 'fansdb')   { label = 'FansDB'; logo = '/static/logos/fansdb.webp'; url = 'https://fansdb.cc/scenes/' + externalId; }
    else if (src === 'iafd')     {
      label = 'IAFD'; logo = '/static/logos/iafd.webp';
      //: IAFD's externalId is the full title URL prefixed with
      //: ``iafd:`` (the scene-id convention) — strip the prefix so the
      //: link points at the IAFD page directly.
      const raw = String(externalId || '');
      url = raw.startsWith('iafd:') ? raw.slice(5) : raw;
    }
    const logoHtml = logo ? `<img src="${logo}" alt="${label}" onerror="this.replaceWith(Object.assign(document.createElement('span'),{textContent:'${label}',className:'banner-label'}))" loading="lazy">` : `<span class="banner-label">${esc(label)}</span>`;
    // stopPropagation: the banner now lives inside the manual-extras
    // toggle button, so a plain click on this anchor would also fire
    // toggleManualExtras. Keep navigation but swallow the bubble.
    const linkHtml = url ? `<a href="${url}" target="_blank" rel="noopener noreferrer" onclick="event.stopPropagation()">Open ↗</a>` : '';
    const statusHtml = statusMsg ? `<span style="color:var(--dim);font-style:italic">${esc(statusMsg)}</span>` : `<span style="color:var(--dim)">Matched scene · pre-filled</span>`;
    el.innerHTML = `${logoHtml} ${statusHtml} ${linkHtml}`;
    el.style.display = 'flex';
  }

  // closeManual is kept as a named alias so legacy callers (Esc
  // handler, overlay click, error paths) keep working — it now just
  // tears down the merged Scene-Search overlay via clearSearch.
  function closeManual() { clearSearch(); }

  //: Reentrancy guard so a double-click or stray Enter while a
  //: submitManual is in flight doesn't queue a second request (which
  //: would then stomp the first's button-state restore on completion).
  let _mSubmitInFlight = false;
  async function submitManual() {
    if (_mSubmitInFlight) return;
    if (!_qManualFile) { window.toast('No file selected for filing'); return; }
    const btn = document.getElementById('qSearchManualFileBtn');
    //: Always use the canonical idle HTML as the restore target so a
    //: stuck "Filing…" from a prior session doesn't persist through
    //: error paths. openManualSearch also resets this, but covering both
    //: ends keeps the button resilient to unexpected state.
    const origHtml = '<i class="fa-solid fa-pen"></i><span class="btn-label" style="margin-left:6px">Submit</span>';

    // Scene-pick mode: a search result is loaded into the left fields.
    // Route through /api/file/manual with the original DB scene object
    // (preserves source IDs, lets the backend pull richer metadata) and
    // only the user-ticked performers. Falls back to the manual-metadata
    // path below when no scene is picked.
    if (_qConfirmScene && _qConfirmFilename) {
      await _submitScenePick(btn, origHtml);
      return;
    }

    try {
      const imageUrl = mThumbMode === 'generated' ? '' : document.getElementById('mImageUrl').value.trim();
      const payload = {
        filename: _qManualFile,
        title: document.getElementById('srchTitle').value.trim(),
        studio: document.getElementById('srchStudio').value.trim(),
        date: document.getElementById('srchDate').value.trim(),
        performers: (typeof _perfPillsValue === 'function' ? _perfPillsValue() : document.getElementById('srchPerformer').value.trim()),
        plot: document.getElementById('mPlot').value.trim(),
        tags: (typeof _tagPillsNames === 'function' ? _tagPillsNames() : []),
        image_url: imageUrl,
      };
      if (mThumbMode === 'generated' && mThumbDataUrl) payload.thumb_data_url = mThumbDataUrl;
      //: Kind dropdown override → server uses it verbatim. ``null``
      //: leaves the field off the payload so the server falls back to
      //: its own filename detection.
      const _extraKindManual = (typeof _readSearchExtraKind === 'function') ? _readSearchExtraKind() : null;
      if (_extraKindManual !== null) payload.extra_kind = _extraKindManual;
      if (!payload.title || !payload.studio || !payload.date) {
        window.toast('Title, studio and date are required');
        return;
      }
      _mSubmitInFlight = true;
      if (btn) { btn.disabled = true; btn.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Loading"></span><span class="btn-label" style="margin-left:6px">Filing…</span>'; }
      const submitToStashdb  = document.getElementById('mSubmitStashdb').checked;
      const submitToFansdb   = document.getElementById('mSubmitFansdb').checked;
      const submitToJavstash = document.getElementById('mSubmitJavstash').checked;
      //: Use the same wait-and-retry wrapper as the scene-pick path so
      //: clicking Submit while the pipeline is mid-run queues up instead
      //: of failing with a "Pipeline already running" toast. The lock is
      //: short-lived (one file per tick), so a couple of retries usually
      //: clears it transparently to the user.
      const d = await _postManualFileWithRetry(
        payload,
        (attempt, secondsElapsed) => {
          if (btn) btn.innerHTML = `<i class="fa-solid fa-clock"></i> Waiting for pipeline… ${secondsElapsed}s`;
          if (attempt === 1) window.toast('Pipeline busy — your file will be sorted automatically');
        },
        '/api/file/manual/metadata',
      );
      if (d.error) {
        window.toast(`Error: ${d.error}`);
        if (btn) { btn.disabled = false; btn.innerHTML = origHtml; }
        _mSubmitInFlight = false;
        return;
      }
      _qIsRunning = true; const runBtn = document.getElementById('btnRunAll'); if (runBtn) runBtn.disabled = true;
      if (submitToStashdb)  _postSubmitToExternal('stashdb',  payload, imageUrl);
      if (submitToFansdb)   _postSubmitToExternal('fansdb',   payload, imageUrl);
      if (submitToJavstash) _postSubmitToExternal('javstash', payload, imageUrl);
      closeManual();
      _sceneQueuePayload = null;
      _movieQueuePayload = null;
      //: Reset flag + button so the next openManual starts clean,
      //: even if openManual's own reset somehow raced (shouldn't,
      //: but insurance).
      _mSubmitInFlight = false;
      if (btn) { btn.disabled = false; btn.innerHTML = origHtml; }
      // Give the user a visible nudge that something happened even
      // though the modal vanished — the running indicator elsewhere
      // already reflects pipeline state but a fresh queue reload
      // ensures the row's status updates promptly. preserveView keeps
      // the user's scroll position and page so filing one row doesn't
      // lose their place in the list.
      setTimeout(() => {
        if (typeof loadQueue === 'function') loadQueue({ preserveView: true, force: true });
        if (typeof loadQueueStats === 'function') loadQueueStats();
      }, 600);
    } catch (e) {
      console.error('submitManual failed:', e);
      window.toast(`File scene failed: ${e.message || e}`);
      if (btn) { btn.disabled = false; btn.innerHTML = origHtml; }
      _mSubmitInFlight = false;
    }
  }

  /* Inline scene-pick filing — POSTs the picked scene to
   * /api/file/manual with the user's ticked performers + edited title.
   * Reads from the left srchTitle field and uses qSearchManualFileBtn
   * as the busy/restore target. Called from submitManual() when a
   * scene result is loaded into the inline UI. */
  async function _submitScenePick(btn, origHtml) {
    if (!_qConfirmScene || !_qConfirmFilename) return;
    //: Pills replaced the per-row tick-list — read names off the pill
    //: state. Match each pill back to the scene's performers array (so
    //: we preserve any ID / source metadata the search result carried),
    //: falling back to a bare-name entry for pills the user added
    //: free-text.
    const pillNames = (typeof _perfPillsNames === 'function') ? _perfPillsNames() : [];
    if (!pillNames.length) {
      window.toast('Add at least one star pill before filing');
      return;
    }
    const perfByName = new Map();
    for (const p of _qConfirmPerformers) {
      perfByName.set((p.name || '').toLowerCase().trim(), p);
    }
    const chosen = pillNames.map(name => {
      const hit = perfByName.get(name.toLowerCase().trim());
      return { performer: { id: (hit && hit.id) || '', name } };
    });
    const titleEl = document.getElementById('srchTitle');
    const finalTitle = (titleEl ? titleEl.value : '').trim() || _qConfirmBaseTitle;
    // Overlay the user's edits in the Plot textarea + Tags pills onto
    // the source-DB scene so the NFO carries whatever the form shows.
    // ``_plot`` / ``_tags`` are the form-provided overrides;
    // ``_scene_extract_plot_and_tags`` prefers them over the upstream
    // values.
    const plotEl = document.getElementById('mPlot');
    const finalPlot = (plotEl ? plotEl.value : '').trim();
    const formTags = (typeof _tagPillsNames === 'function') ? _tagPillsNames() : [];
    const scene = Object.assign({}, _qConfirmScene, {
      performers: chosen,
      title: finalTitle,
      _plot: finalPlot,
      _tags: formTags,
    });
    //: Kind override travels with the scene dict so /api/file/manual
    //: routes it into the filing plan. ``null`` means "use server
    //: auto-detect" — leave ``_extra_kind`` off so the wrapper detects
    //: from the filename instead of treating the missing key as
    //: "force-off".
    const _extraKindScene = (typeof _readSearchExtraKind === 'function') ? _readSearchExtraKind() : null;
    if (_extraKindScene !== null) scene._extra_kind = _extraKindScene;
    //: The duplicated "Submit to StashDB after filing" checkbox in the
    //: removed Filing-from panel is gone — the right-column STASH
    //: toggle in Submit-to-external-databases is the single source of
    //: truth.
    const submitCb = document.getElementById('mSubmitStashdb');
    const submitStashdb = !!(submitCb && submitCb.checked);
    _mSubmitInFlight = true;
    if (btn) { btn.disabled = true; btn.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Loading"></span><span class="btn-label" style="margin-left:6px">Filing…</span>'; }
    try {
      const d = await _postManualFileWithRetry(
        { filename: _qConfirmFilename, scene, submit_stashdb: submitStashdb },
        (attempt, secondsElapsed) => {
          if (btn) btn.innerHTML = `<i class="fa-solid fa-clock"></i> Waiting for pipeline… ${secondsElapsed}s`;
          if (attempt === 1) window.toast('Pipeline busy — your file will be sorted automatically');
        },
      );
      if (d.error) {
        window.toast(`Error: ${d.error}`);
        if (btn) { btn.disabled = false; btn.innerHTML = origHtml; }
        _mSubmitInFlight = false;
        return;
      }
      _qIsRunning = true; const runBtn = document.getElementById('btnRunAll'); if (runBtn) runBtn.disabled = true;
      clearSelectedFile();
      clearSearch();
      _sceneQueuePayload = null;
      _movieQueuePayload = null;
      _mSubmitInFlight = false;
      if (btn) { btn.disabled = false; btn.innerHTML = origHtml; }
      setTimeout(() => {
        if (typeof loadQueue === 'function') loadQueue({ preserveView: true, force: true });
        if (typeof loadQueueStats === 'function') loadQueueStats();
      }, 600);
    } catch(e) {
      window.toast('Failed to file: ' + (e.message || e));
      if (btn) { btn.disabled = false; btn.innerHTML = origHtml; }
      _mSubmitInFlight = false;
    }
  }

  function _postSubmitToExternal(target, payload, imageUrl) {
    const cfg = _MSUBMIT_TARGETS[target];
    if (!cfg) return;
    const resolved = mResolved[target] || { studio: null, performers: [] };
    setTimeout(async () => {
      try {
        const sr = await fetch(cfg.submit, { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({
          filename: _qManualFile || payload.filename,
          title: payload.title, date: payload.date, plot: payload.plot,
          studio_id: resolved.studio || null,
          performer_ids: (resolved.performers || []).filter(Boolean),
          tags: Array.isArray(payload.tags) ? payload.tags : [],
          image_url: imageUrl,
        }) });
        const sd = await sr.json();
        if (!sd.success && sd.error) console.warn(`${target} submit failed:`, sd.error);
      } catch (e) { console.warn(`${target} submit error:`, e); }
    }, 500);
  }

  async function generateThumb() {
    if (!_qManualFile) return;
    _mResetThumb();
    // Swap the empty placeholder for a spinner so the user sees the
    // re-roll is actually doing something while ffmpeg seeks. Cached
    // the original markup so the failure path can restore the empty
    // state cleanly. Also disable both buttons + add a "generating"
    // ring on the thumb frame so the visible affordance matches.
    const img = document.getElementById('mThumbImg');
    const empty = document.getElementById('mThumbEmpty');
    const pctLabel = document.getElementById('mThumbPercent');
    const randBtn = document.getElementById('mRandBtn');
    const regenBtn = document.getElementById('mRegenBtn');
    const _emptyOrigHtml = empty ? empty.innerHTML : '';
    if (empty) {
      empty.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Loading"></span><span style="font-size:11px;color:var(--dim);margin-top:6px">Generating frame…</span>';
      empty.style.display = 'flex';
      empty.style.flexDirection = 'column';
    }
    if (pctLabel) pctLabel.textContent = '';
    if (randBtn)  randBtn.disabled  = true;
    if (regenBtn) regenBtn.disabled = true;
    try {
      const r = await fetch('/api/thumb/generate', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ filename: _qManualFile }) });
      const d = await r.json();
      if (d.error) { window.toast(`Thumb error: ${d.error}`); return; }
      mThumbDataUrl = d.data_url; mThumbMode = 'generated';
      if (img) { img.src = d.data_url; img.style.display = 'block'; }
      if (empty) empty.style.display = 'none';
      if (pctLabel) pctLabel.textContent = `Frame at ${d.percent}%`;
      _mShowRegenBtn();
      document.getElementById('mImageUrl').value = '';
    } catch(e) {
      window.toast(`Thumb error: ${e.message}`);
    } finally {
      if (empty) {
        empty.innerHTML = _emptyOrigHtml;
        empty.style.flexDirection = '';
      }
      if (randBtn)  randBtn.disabled  = false;
      if (regenBtn) regenBtn.disabled = false;
    }
  }

  // ── Vice filing modal ───────────────────────────────────
  // Mirrors /queue's implementation so /downloads can sort an RSS'd
  // or dropped file into a Vice without switching pages. Backend
  // endpoint is shared: POST /api/file/manual/vice.
  let _viceFile = null;
  let _viceSubmitInFlight = false;
  let vThumbMode = null;
  let vThumbDataUrl = null;

  function _vShowRandBtn() {
    const rand = document.getElementById('vRandBtn');
    const regen = document.getElementById('vRegenBtn');
    if (rand)  rand.style.display  = 'inline-flex';
    if (regen) regen.style.display = 'none';
  }
  function _vShowRegenBtn() {
    const rand = document.getElementById('vRandBtn');
    const regen = document.getElementById('vRegenBtn');
    if (rand)  rand.style.display  = 'none';
    if (regen) regen.style.display = 'inline-flex';
  }

  function _vResetThumb() {
    const img = document.getElementById('vThumbImg');
    const empty = document.getElementById('vThumbEmpty');
    if (img) { img.src = ''; img.style.display = 'none'; }
    if (empty) empty.style.display = 'flex';
    document.getElementById('vThumbPercent').textContent = '';
    _vShowRandBtn();
  }

  async function openVice(filename) {
    _viceFile = filename;
    document.getElementById('viceFilename').textContent = filename;
    const titleEl = document.getElementById('vTitle');
    const dateEl  = document.getElementById('vDate');
    const perfEl  = document.getElementById('vPerformers');
    titleEl.value = '';
    dateEl.value  = '';
    perfEl.value  = '';
    document.getElementById('vImageUrl').value = '';
    _vResetThumb();
    vThumbMode = null; vThumbDataUrl = null;
    document.getElementById('viceModal').classList.add('open');

    // Populate the vice dropdown. Once the options exist, auto-select
    // whichever vice's slug appears in the filename (if any) so the
    // user's first keystroke isn't picking from a blank select.
    const sel = document.getElementById('vViceSelect');
    sel.innerHTML = '<option value="">Loading…</option>';
    let viceList = [];
    try {
      const r = await fetch('/api/vices', { credentials: 'same-origin' });
      const d = await r.json();
      viceList = Array.isArray(d.vices) ? d.vices : [];
      if (!viceList.length) {
        sel.innerHTML = '<option value="">— No vices configured —</option>';
      } else {
        sel.innerHTML = viceList.map(v => `<option value="${esc(v.name)}">${esc(v.name)}</option>`).join('');
        const hit = _firstMatchingVice(filename, viceList);
        if (hit) sel.value = hit;
      }
    } catch(_) {
      sel.innerHTML = '<option value="">— Error loading vices —</option>';
    }

    // Step 1: pull the queue row's stored match info (history /
    // library guess) — same shape the Manual modal reads.
    const qf = (window._queueFiles || []).find(f => f.filename === filename) || {};
    if (qf.match_title)  titleEl.value = qf.match_title;
    if (qf.match_date)   dateEl.value  = qf.match_date;
    if (qf.performers)   perfEl.value  = qf.performers;

    // Step 2: filename parser fills anything the queue row didn't carry.
    try {
      const r = await fetch(`/api/parse/filename?filename=${encodeURIComponent(filename)}`);
      const d = await r.json();
      if (d.title && !titleEl.value)      titleEl.value = d.title;
      if (d.date && !dateEl.value)        dateEl.value  = d.date;
      if (d.performers && !perfEl.value)  perfEl.value  = d.performers;
    } catch(_) {}

    // Step 3: if the queue row has a stash-box match id, fetch the
    // full scene detail — these ALWAYS win because they're authoritative.
    if (qf.match_external_id && qf.match_source) {
      try {
        const r = await fetch(`/api/queue/match-scene?filename=${encodeURIComponent(filename)}`);
        const d = await r.json();
        if (d.found) {
          if (d.title)      titleEl.value = d.title;
          if (d.date)       dateEl.value  = d.date;
          if (d.performers && d.performers.length) perfEl.value = d.performers.join(', ');
          if (d.image_url) {
            document.getElementById('vImageUrl').value = d.image_url;
            vThumbMode = 'url';
            vThumbPreview(d.image_url);
          }
        }
      } catch(_) { /* non-fatal — fallbacks below fill the gaps */ }
    }

    // Step 4: last-chance fallbacks — filename-minus-extension title
    // and source file mtime date — so the form never opens blank.
    if (!titleEl.value) titleEl.value = filename.replace(/\.[^./\\]+$/, '');
    if (!dateEl.value) {
      try {
        const r = await fetch(`/api/queue/file-mtime?filename=${encodeURIComponent(filename)}`);
        const d = await r.json();
        dateEl.value = d.date || '';
      } catch(_) {
        const now = new Date();
        dateEl.value = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-${String(now.getDate()).padStart(2,'0')}`;
      }
    }
  }

  // Pick the first configured vice whose slug appears in the filename
  // — shared logic for the dropdown auto-select. Same rules as the
  // per-row flame colouring: whole-word or ≥8-char spaceless match.
  function _firstMatchingVice(fn, vices) {
    const t = _slugify(fn);
    if (!t) return null;
    const padded = ' ' + t + ' ';
    const spaceless = t.replace(/\s+/g, '');
    for (const v of (vices || [])) {
      const s = _slugify(v.name || '');
      if (!s) continue;
      if (padded.indexOf(' ' + s + ' ') >= 0) return v.name;
      const ss = s.replace(/\s+/g, '');
      if (ss.length >= 8 && spaceless.indexOf(ss) >= 0) return v.name;
    }
    return null;
  }

  function closeVice() {
    document.getElementById('viceModal').classList.remove('open');
    _viceFile = null;
  }

  function vThumbPreview(url) {
    const img = document.getElementById('vThumbImg');
    const empty = document.getElementById('vThumbEmpty');
    if (!url || !url.trim()) {
      _vResetThumb();
      return;
    }
    img.onload = () => {
      img.style.display = 'block';
      if (empty) empty.style.display = 'none';
    };
    img.onerror = () => { _vResetThumb(); };
    img.src = url;
    document.getElementById('vThumbPercent').textContent = '';
    _vShowRandBtn();
  }

  async function generateViceThumb() {
    if (!_viceFile) return;
    _vResetThumb();
    const img = document.getElementById('vThumbImg');
    const empty = document.getElementById('vThumbEmpty');
    const pctLabel = document.getElementById('vThumbPercent');
    const randBtn = document.getElementById('vRandBtn');
    const regenBtn = document.getElementById('vRegenBtn');
    const _emptyOrigHtml = empty ? empty.innerHTML : '';
    if (empty) {
      empty.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Loading"></span><span style="font-size:11px;color:var(--dim);margin-top:6px">Generating frame…</span>';
      empty.style.display = 'flex';
      empty.style.flexDirection = 'column';
    }
    if (pctLabel) pctLabel.textContent = '';
    if (randBtn)  randBtn.disabled  = true;
    if (regenBtn) regenBtn.disabled = true;
    try {
      const r = await fetch('/api/thumb/generate', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ filename: _viceFile }),
      });
      const d = await r.json();
      if (d.error) { window.toast(`Thumb error: ${d.error}`); return; }
      vThumbDataUrl = d.data_url;
      vThumbMode = 'generated';
      if (img) { img.src = d.data_url; img.style.display = 'block'; }
      if (empty) empty.style.display = 'none';
      if (pctLabel) pctLabel.textContent = `Frame at ${d.percent}%`;
      _vShowRegenBtn();
      document.getElementById('vImageUrl').value = '';
    } catch(e) {
      window.toast(`Thumb error: ${e.message}`);
    } finally {
      if (empty) {
        empty.innerHTML = _emptyOrigHtml;
        empty.style.flexDirection = '';
      }
      if (randBtn)  randBtn.disabled  = false;
      if (regenBtn) regenBtn.disabled = false;
    }
  }

  async function submitVice() {
    if (_viceSubmitInFlight) return;
    if (!_viceFile) return;
    const viceSel  = document.getElementById('vViceSelect');
    const titleEl  = document.getElementById('vTitle');
    const dateEl   = document.getElementById('vDate');
    const vice_name = (viceSel && viceSel.value || '').trim();
    const title     = (titleEl && titleEl.value || '').trim();
    const date      = (dateEl && dateEl.value || '').trim();
    const performers = document.getElementById('vPerformers').value.trim();
    const imageUrl  = vThumbMode === 'generated' ? '' : document.getElementById('vImageUrl').value.trim();
    // Mark the field that's missing so the user sees what to fix instead
    // of hunting through the modal. Clear any prior flags first.
    [viceSel, titleEl, dateEl].forEach(el => el && el.classList.remove('field-invalid'));
    if (!vice_name) {
      if (viceSel) { viceSel.classList.add('field-invalid'); viceSel.focus(); }
      window.toast('Pick a vice from the dropdown', { kind: 'error' });
      return;
    }
    if (!title) {
      if (titleEl) { titleEl.classList.add('field-invalid'); titleEl.focus(); }
      window.toast('Title is required', { kind: 'error' });
      return;
    }
    if (!date) {
      if (dateEl) { dateEl.classList.add('field-invalid'); dateEl.focus(); }
      window.toast('Date is required', { kind: 'error' });
      return;
    }
    const payload = {
      filename: _viceFile,
      vice_name, title, date, performers,
      image_url: imageUrl,
    };
    if (vThumbMode === 'generated' && vThumbDataUrl) payload.thumb_data_url = vThumbDataUrl;

    const btn = document.getElementById('vSubmitBtn');
    const origHtml = btn ? btn.innerHTML : '';
    _viceSubmitInFlight = true;
    if (btn) { btn.disabled = true; btn.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Loading"></span> <span class="btn-label">Filing…</span>'; }
    try {
      const r = await fetch('/api/file/manual/vice', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload),
      });
      let d = {};
      try { d = await r.json(); } catch (_) { d = {}; }
      if (!r.ok || d.error) {
        window.toast(`Error: ${d.error || (r.status + ' ' + r.statusText)}`, { kind: 'error' });
        if (btn) { btn.disabled = false; btn.innerHTML = origHtml; }
        _viceSubmitInFlight = false;
        return;
      }
      if (btn) { btn.disabled = false; btn.innerHTML = origHtml; }
      closeVice();
      _sceneQueuePayload = null;
      _movieQueuePayload = null;
      _qIsRunning = true; const runBtn = document.getElementById('btnRunAll'); if (runBtn) runBtn.disabled = true;
      if (typeof window.toast === 'function') {
        const pos = Number(d && d.queued_position || 0);
        const msg = d && d.queued
          ? `Pipeline busy — queued for Vice ${vice_name}${pos > 1 ? ` (position ${pos})` : ''}`
          : 'Filing to ' + vice_name + '…';
        window.toast(msg, { kind: 'success' });
      }
      setTimeout(() => {
        if (typeof loadQueue === 'function') loadQueue({ preserveView: true, force: true });
        if (typeof loadQueueStats === 'function') loadQueueStats();
      }, 600);
      _viceSubmitInFlight = false;
    } catch (e) {
      console.error('submitVice failed:', e);
      window.toast(`File to Vice failed: ${e.message || e}`, { kind: 'error' });
      if (btn) { btn.disabled = false; btn.innerHTML = origHtml; }
      _viceSubmitInFlight = false;
    }
  }

  window.openVice = openVice;
  window.closeVice = closeVice;
  window.submitVice = submitVice;
  window.generateViceThumb = generateViceThumb;

  // Per-target metadata for the submit-to-external panels. Keyed by
  // payload identifier (`target`) so callers below stay terse and a
  // future DB addition is a one-line map entry.
  const _MSUBMIT_TARGETS = {
    stashdb:  { label: 'StashDB',  fields: 'mStashdbFields',  status: 'mStashdbResolveStatus',  results: 'mStashdbResolveResults',  resolve: '/api/stashdb/resolve',  submit: '/api/stashdb/submit-manual'  },
    fansdb:   { label: 'FansDB',   fields: 'mFansdbFields',   status: 'mFansdbResolveStatus',   results: 'mFansdbResolveResults',   resolve: '/api/fansdb/resolve',   submit: '/api/fansdb/submit-manual'   },
    javstash: { label: 'JAVStash', fields: 'mJavstashFields', status: 'mJavstashResolveStatus', results: 'mJavstashResolveResults', resolve: '/api/javstash/resolve', submit: '/api/javstash/submit-manual' },
  };

  function toggleSubmitFields(target, show) {
    const cfg = _MSUBMIT_TARGETS[target];
    if (!cfg) return;
    const el = document.getElementById(cfg.fields);
    if (el) el.style.display = show ? 'block' : 'none';
    if (show) resolveOnTarget(target);
  }

  async function resolveOnTarget(target) {
    const cfg = _MSUBMIT_TARGETS[target];
    if (!cfg) return;
    const studio = document.getElementById('srchStudio').value.trim();
    const performers = (typeof _perfPillsValue === 'function' ? _perfPillsValue() : document.getElementById('srchPerformer').value.trim());
    const statusEl = document.getElementById(cfg.status);
    const resultsEl = document.getElementById(cfg.results);
    if (!studio && !performers) return;
    statusEl.textContent = `Looking up on ${cfg.label}...`;
    resultsEl.innerHTML = '';
    mResolved[target] = { studio: null, performers: [] };
    try {
      const r = await fetch(cfg.resolve, { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ studio, performers }) });
      const d = await r.json();
      if (d.error) { statusEl.textContent = d.error; return; }
      renderResolveResults(target, d);
    } catch(e) { statusEl.textContent = `Lookup failed: ${e.message}`; }
  }

  function renderResolveResults(target, d) {
    const cfg = _MSUBMIT_TARGETS[target];
    if (!cfg) return;
    const statusEl = document.getElementById(cfg.status);
    const resultsEl = document.getElementById(cfg.results);
    statusEl.textContent = '';
    let html = '';
    if (d.studio) {
      if (d.studio.confirmed) { mResolved[target].studio = d.studio.id; html += `<div style="font-size:12px;color:var(--green);margin-bottom:6px">✓ Studio: ${esc(d.studio.name)}</div>`; }
      else if (d.studio.suggestions?.length) {
        html += `<div style="font-size:12px;color:var(--amber);margin-bottom:4px">Studio not found. Did you mean:</div>`;
        d.studio.suggestions.forEach(s => { html += `<button class="btn-secondary" style="margin:2px;font-size:12px;padding:4px 8px" onclick="confirmStudio('${target}','${s.id}','${esc(s.name)}')">${esc(s.name)}</button>`; });
        html += `<button class="btn-secondary" style="margin:2px;font-size:12px;padding:4px 8px;color:var(--dim)" onclick="confirmStudio('${target}',null,'${esc(d.studio.name)}')">Skip</button><br>`;
      } else { html += `<div style="font-size:12px;color:var(--dim);margin-bottom:6px">Studio not found</div>`; }
    }
    if (d.performers?.length) {
      d.performers.forEach((p, i) => {
        if (p.confirmed) { mResolved[target].performers[i] = p.id; html += `<div style="font-size:12px;color:var(--green);margin-bottom:4px">✓ Star: ${esc(p.name)}</div>`; }
        else if (p.suggestions?.length) {
          html += `<div style="font-size:12px;color:var(--amber);margin-bottom:4px">${esc(p.name)}: Did you mean:</div>`;
          p.suggestions.forEach(s => { html += `<button class="btn-secondary" style="margin:2px;font-size:12px;padding:4px 8px" onclick="confirmPerformer('${target}',${i},'${s.id}','${esc(s.name)}')">${esc(s.name)}</button>`; });
          html += `<button class="btn-secondary" style="margin:2px;font-size:12px;padding:4px 8px;color:var(--dim)" onclick="confirmPerformer('${target}',${i},null,'${esc(p.name)}')">Skip</button><br>`;
        } else { mResolved[target].performers[i] = null; html += `<div style="font-size:12px;color:var(--dim);margin-bottom:4px">${esc(p.name)}: not found</div>`; }
      });
    }
    resultsEl.innerHTML = html;
  }

  function confirmStudio(target, id, name) {
    const cfg = _MSUBMIT_TARGETS[target];
    if (!cfg) return;
    mResolved[target].studio = id;
    const colour = id ? 'var(--green)' : 'var(--dim)';
    const resultsEl = document.getElementById(cfg.results);
    resultsEl.querySelectorAll(`[onclick^="confirmStudio('${target}'"]`).forEach(b => b.closest('div')?.remove());
    resultsEl.insertAdjacentHTML('afterbegin', `<div style="font-size:12px;color:${colour};margin-bottom:6px">${id ? '✓' : '–'} Studio: ${esc(name)}</div>`);
  }

  function confirmPerformer(target, idx, id, name) {
    const cfg = _MSUBMIT_TARGETS[target];
    if (!cfg) return;
    mResolved[target].performers[idx] = id;
    const colour = id ? 'var(--green)' : 'var(--dim)';
    const results = document.getElementById(cfg.results);
    const marker = `confirmPerformer('${target}',${idx},`;
    let replaced = false;
    results.querySelectorAll('button').forEach(b => {
      if (!replaced && b.getAttribute('onclick')?.startsWith(marker)) {
        let node = b;
        while (node && !node.tagName?.match(/^BR$/i)) { const next = node.nextSibling; if (node.nodeType === 1) node.remove(); node = next; }
        if (node) node.remove();
        replaced = true;
      }
    });
    results.insertAdjacentHTML('beforeend', `<div style="font-size:12px;color:${colour};margin-bottom:4px">${id ? '✓' : '–'} Star: ${esc(name)}</div>`);
  }

  // Page boot — after all `let`/`const`. setQueueMode → clearSelectedFile →
  // _clearSearchFrames touches _searchFrameToken; running earlier throws TDZ.
  (() => {
    applyDlSearchModeUi();
    setQueueMode(queueMode, true);
    startQueuePolling();
    document.addEventListener('visibilitychange', () => {
      if (document.visibilityState === 'visible' && typeof TS_DL_PAGE !== 'undefined' && TS_DL_PAGE === 'queue') {
        loadQueue({ preserveView: true });
        loadQueueStats();
      }
    });
  })();

  // Single merged keydown delegator for queue overlays. Previously
  // there were five separate `document.addEventListener('keydown')`
  // calls (RSS chooser, queue image overlay arrows + escape, manual
  // search escape, this one, suggestions in script 2). Five-deep was
  // unnecessary; every keystroke fanned out through all of them.
  // Now: one handler routes Escape to whichever overlays are open and
  // routes arrow keys to the topmost image lightbox.
  document.addEventListener('keydown', e => {
    if (e.key === 'ArrowLeft' || e.key === 'ArrowRight') {
      const qov = document.getElementById('queueImageOverlay');
      if (qov && qov.style.display === 'flex') {
        e.preventDefault();
        queueOverlayStep(e.key === 'ArrowLeft' ? -1 : 1);
      }
      return;
    }
    if (e.key !== 'Escape') return;
    // Close the image lightbox first (highest z); subsequent close
    // calls are idempotent so leaking an unrelated overlay close is
    // harmless.
    closeQueueImageOverlay();
    const movOv = document.getElementById('qMovieSearchOverlay');
    if (movOv && movOv.classList.contains('open')) {
      closeMovieSearchPanel();
    } else {
      const sov = document.getElementById('qSearchOverlay');
      if (sov && sov.classList.contains('open')) clearSearch();
    }
  });


/* Block 2/3: queue duplicates modal. */

  // ── Queue duplicates modal ───────────────────────────────────────
  let _queueDupGroups = [];

  function closeQueueDuplicatesModal() {
    const el = document.getElementById('queueDupOverlay');
    if (el) el.style.display = 'none';
    // Bump the global epoch so any in-flight strip polls bail out
    // instead of continuing to fetch /api/queue/thumbs in the background
    // after the user has closed the modal.
    _qdStripEpoch++;
  }

  function openQueueDuplicatesModal() {
    const el = document.getElementById('queueDupOverlay');
    if (!el) return;
    el.style.display = 'flex';
    refreshQueueDuplicatesModal();
  }

  function _fmtQueueDupSize(bytes) {
    const n = Number(bytes);
    if (!n || n < 0) return '—';
    if (n >= 1073741824) return (n / 1073741824).toFixed(2) + ' GB';
    if (n >= 1048576) return (n / 1048576).toFixed(1) + ' MB';
    return Math.round(n / 1024) + ' KB';
  }

  function _renderQueueDupModal(data) {
    const body = document.getElementById('queueDupBody');
    const sub = document.getElementById('queueDupSubtitle');
    const computeBtn = document.getElementById('queueDupComputeBtn');
    if (!body) return;
    const groups = data.groups || [];
    const noPh = Number(data.no_phash_count || 0);
    const activeN = groups.filter(g => !g.ignored).length;
    if (sub) {
      sub.textContent = activeN
        ? `${activeN} duplicate group${activeN === 1 ? '' : 's'} in the download folder`
        : 'No duplicate groups (files need a cached phash — run the pipeline or Compute phash)';
      if (noPh > 0) sub.textContent += ` · ${noPh} file${noPh === 1 ? '' : 's'} without phash`;
    }
    if (computeBtn) computeBtn.style.display = noPh > 0 ? '' : 'none';
    if (!groups.length) {
      body.innerHTML = `<div class="empty" style="padding:28px;text-align:center;color:var(--dim);font-size:13px;line-height:1.5">
        No duplicate groups. Duplicates are detected when two or more queue files share the same perceptual hash.
        ${noPh ? '<br><br>Some files have no phash yet — click <strong>Compute phash</strong> above, or run them through the pipeline.' : ''}
      </div>`;
      return;
    }
    body.innerHTML = groups.map((g, gi) => {
      const isIg = !!g.ignored;
      const conf = (g.recommendation && g.recommendation.confidence) || 'high';
      const needRev = !isIg && conf === 'low';
      const keepFn = (g.recommendation && g.recommendation.keeper_filename) || '';
      const gClass = 'queue-dup-group' + (isIg ? ' queue-dup-group--ignored' : needRev ? ' queue-dup-group--review' : '');
      const badge = isIg
        ? '<span class="queue-dup-badge queue-dup-badge--ignored">Ignored</span>'
        : needRev
          ? '<span class="queue-dup-badge queue-dup-badge--review">Needs review</span>'
          : '<span class="queue-dup-badge queue-dup-badge--rec">Recommended</span>';
      const phEnc = encodeURIComponent(g.phash || '');
      const rows = (g.files || []).map(f => {
        const fn = f.current_filename || '';
        const isKeep = !needRev && keepFn && fn === keepFn;
        const res = (f.media_width && f.media_height) ? `${f.media_width}×${f.media_height}` : '—';
        const fnAttr = fn.replace(/'/g, "\\'");
        const fnB64 = encodeURIComponent(fn);
        const placeholders = Array.from({ length: 5 }, () =>
          '<div class="qd-thumb" style="aspect-ratio:16/9;min-height:54px;background:rgba(0,0,0,0.55);border:1px solid rgba(192,132,252,0.18);border-radius:3px;display:flex;align-items:center;justify-content:center;overflow:hidden;cursor:zoom-in"><span class="loader loader--btn loader--muted" role="status" aria-label="Generating"></span></div>'
        ).join('');
        const deleteBtn = !isKeep
          ? `<button type="button" class="qd-action qd-action--del" onclick="queueDupDeleteOne('${fnAttr}', ${gi})" title="Delete from queue" style="background:linear-gradient(160deg,rgba(244,114,182,0.22) 0%,rgba(244,114,182,0.08) 100%);border:1px solid rgba(244,114,182,0.45);border-radius:8px;color:#f472b6;width:32px;height:32px;display:inline-flex;align-items:center;justify-content:center;cursor:pointer;font-size:13px;flex-shrink:0"><i class="fa-solid fa-trash"></i></button>`
          : '<span style="display:inline-block;width:32px;flex-shrink:0"></span>';
        return `<div class="queue-dup-row${isKeep ? ' is-keeper' : ''}" style="display:flex;flex-direction:row;align-items:center;gap:14px;padding:10px 14px;border-bottom:1px solid rgba(255,255,255,0.04);font-size:12px">
          <div style="min-width:0;flex:0 0 360px">
            <div class="queue-dup-row-name" style="word-break:break-word;font-family:var(--secs);font-size:13px;color:var(--text)">${esc(fn)}${isKeep ? ' <span style="color:#86efac;font-size:10px">(keeper)</span>' : ''}</div>
            <div class="queue-dup-row-meta" style="font-size:10px;color:var(--dim);margin-top:3px">${_fmtQueueDupSize(f.size_bytes)} · ${esc(res)}</div>
          </div>
          <div class="queue-dup-row-strip" data-qd-strip="${fnB64}" style="display:grid;grid-template-columns:repeat(5,1fr);gap:4px;flex:1;min-width:0">
            ${placeholders}
          </div>
          ${deleteBtn}
        </div>`;
      }).join('');
      const keepBtn = (!isIg && !needRev && keepFn)
        ? `<button type="button" class="btn-secondary" onclick="queueDupKeepRecommended(${gi})" title="Keep recommended, delete others"><i class="fa-solid fa-brain"></i> Keep best</button>`
        : '';
      const ignBtn = `<button type="button" class="btn-secondary" onclick="queueDupToggleIgnore('${phEnc}', ${isIg ? 'false' : 'true'})" title="${isIg ? 'Include again' : 'Ignore group'}"><i class="fa-solid fa-dice-two"></i></button>`;
      return `<div class="${gClass}">
        <div class="queue-dup-group-head">
          <div style="font-size:11px;color:var(--dim);word-break:break-all">Phash: <span style="color:var(--text);font-family:var(--mono)">${esc((g.phash || '').slice(0, 16))}…</span> ${badge}</div>
          <span style="display:flex;gap:6px">${keepBtn}${ignBtn}</span>
        </div>
        ${rows}
      </div>`;
    }).join('');
    body.querySelectorAll('[data-qd-strip]').forEach(el => {
      const fn = decodeURIComponent(el.getAttribute('data-qd-strip') || '');
      if (fn) _loadQueueDupStrip(el, fn);
    });
  }

  // Lazy-load each row's 5-frame filmstrip. Frames are cached server-side
  // once generated, so reopening the duplicates modal is instant. Polls
  // up to ~30s while ffmpeg generates frames; gives up cleanly if backoff
  // hits or no frames ever land. The epoch counter is bumped on modal
  // close so background polls don't keep hammering /api/queue/thumbs.
  const _qdStripTokens = new WeakMap();
  let _qdStripEpoch = 0;
  async function _loadQueueDupStrip(container, filename) {
    const startEpoch = _qdStripEpoch;
    const myToken = (_qdStripTokens.get(container) || 0) + 1;
    _qdStripTokens.set(container, myToken);
    const isStale = () => _qdStripEpoch !== startEpoch || _qdStripTokens.get(container) !== myToken;
    const renderState = (d) => {
      if (isStale()) return;
      const thumbs = Array.isArray(d.thumbs) ? d.thumbs : [];
      const byIdx = new Map(thumbs.map(t => [t.i, t]));
      const slots = [];
      const cellStyle = 'aspect-ratio:16/9;min-height:54px;background:rgba(0,0,0,0.55);border:1px solid rgba(192,132,252,0.18);border-radius:3px;overflow:hidden;cursor:zoom-in;display:flex;align-items:center;justify-content:center';
      for (let i = 0; i < 5; i++) {
        const t = byIdx.get(i);
        if (t) {
          const fnAttr = filename.replace(/'/g, "\\'");
          slots.push(`<div class="qd-thumb" style="${cellStyle}" onclick="openQueueFilmstrip('${fnAttr}')" title="Frame ${t.i + 1} — click for full filmstrip"><img src="${esc(t.url)}" alt="Frame ${t.i + 1}" loading="lazy" style="width:100%;height:100%;object-fit:cover;display:block"></div>`);
        } else if (d.backoff) {
          slots.push(`<div class="qd-thumb" style="${cellStyle};color:rgba(244,114,182,0.7);font-size:14px" title="FFmpeg gave up — open the large filmstrip to retry"><i class="fa-solid fa-triangle-exclamation"></i></div>`);
        } else if (d.generating) {
          slots.push(`<div class="qd-thumb" style="${cellStyle}"><span class="loader loader--btn loader--muted" role="status" aria-label="Generating"></span></div>`);
        } else {
          slots.push(`<div class="qd-thumb" style="${cellStyle};color:rgba(192,132,252,0.45);font-size:14px"><i class="fa-solid fa-film" aria-hidden="true"></i></div>`);
        }
      }
      container.innerHTML = slots.join('');
    };
    const startMs = Date.now();
    const POLL_TIMEOUT_MS = 30 * 1000;
    // Backoff schedule: fire the first few polls quickly (frames often
    // land within a second on a warm SSD), then slow to 2s for the long
    // tail. Cached files return `ready` on the first fetch so they never
    // even touch the schedule.
    const POLL_INTERVALS_MS = [400, 600, 900, 1400, 2000];
    let pollIdx = 0;
    try {
      while (true) {
        if (isStale()) return;
        const r = await fetch('/api/queue/thumbs?filename=' + encodeURIComponent(filename), { credentials: 'same-origin' });
        if (isStale()) return;
        const d = await r.json().catch(() => ({}));
        if (!r.ok) {
          renderState({ thumbs: [], backoff: true });
          return;
        }
        renderState(d);
        if (d.ready || d.backoff || !d.generating) return;
        if (Date.now() - startMs > POLL_TIMEOUT_MS) {
          renderState({ ...d, backoff: true, generating: false });
          return;
        }
        const delay = POLL_INTERVALS_MS[Math.min(pollIdx, POLL_INTERVALS_MS.length - 1)];
        pollIdx++;
        await new Promise(res => setTimeout(res, delay));
      }
    } catch (_) {
      renderState({ thumbs: [], backoff: true });
    }
  }

  async function refreshQueueDuplicatesModal() {
    const body = document.getElementById('queueDupBody');
    if (body) body.innerHTML = '<div class="empty" style="padding:24px">Loading…</div>';
    try {
      const r = await fetch('/api/queue/duplicates', { credentials: 'same-origin' });
      const d = await r.json();
      _queueDupGroups = d.groups || [];
      _renderQueueDupModal(d);
      _updateQueueDupButton({ groups: d.group_count, files: d.file_count });
    } catch (e) {
      if (body) body.innerHTML = `<div class="empty" style="padding:24px;color:var(--red)">Failed to load duplicates: ${esc(e.message || e)}</div>`;
    }
  }

  async function queueDupComputePhash() {
    const btn = document.getElementById('queueDupComputeBtn');
    if (btn) { btn.disabled = true; btn.innerHTML = '<span class="loader loader--btn"></span>'; }
    try {
      await fetch('/api/queue/duplicates/compute-phash', { method: 'POST', credentials: 'same-origin' });
      await refreshQueueDuplicatesModal();
      if (typeof loadQueue === 'function') loadQueue({ preserveView: true, force: true });
      if (typeof loadQueueStats === 'function') loadQueueStats();
    } catch (e) {
      _toastBelow('Phash compute failed: ' + (e.message || e));
    } finally {
      if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-fingerprint"></i> Compute phash'; }
    }
  }

  async function queueDupToggleIgnore(phEnc, ignored) {
    let ph = '';
    try { ph = decodeURIComponent(phEnc); } catch (_) { return; }
    try {
      await fetch('/api/queue/duplicates/ignore', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify({ phash: ph, ignored: ignored === true || ignored === 'true' }),
      });
      await refreshQueueDuplicatesModal();
      if (typeof loadQueueStats === 'function') loadQueueStats();
    } catch (e) {
      _toastBelow('Ignore failed: ' + (e.message || e));
    }
  }

  async function queueDupKeepRecommended(gi) {
    const g = _queueDupGroups[gi];
    if (!g || !g.recommendation) return;
    const keepFn = (g.recommendation.keeper_filename || '').trim();
    if (!keepFn) { _toastBelow('No recommended keeper for this group'); return; }
    const deleteFns = (g.files || []).map(f => f.current_filename).filter(fn => fn && fn !== keepFn);
    if (!deleteFns.length) return;
    if (!confirm(`Keep “${keepFn}” and delete ${deleteFns.length} other duplicate(s) from the queue folder?`)) return;
    try {
      const r = await fetch('/api/queue/duplicates/resolve-one', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify({ keep_filename: keepFn, delete_filenames: deleteFns }),
      });
      const d = await r.json();
      if (!r.ok) throw new Error(d.error || 'Resolve failed');
      _toastBelow(`Removed ${d.deleted || 0} duplicate(s)`);
      await refreshQueueDuplicatesModal();
      if (typeof loadQueue === 'function') loadQueue({ preserveView: true, force: true });
      if (typeof loadQueueStats === 'function') loadQueueStats();
    } catch (e) {
      _toastBelow('Resolve failed: ' + (e.message || e));
    }
  }

  async function queueDupDeleteOne(fn, gi) {
    if (!fn) return;
    if (!confirm(`Delete “${fn}” from the queue folder?`)) return;
    try {
      const r = await fetch('/api/queue/delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify({ filename: fn }),
      });
      const d = await r.json();
      if (!r.ok) throw new Error(d.error || 'Delete failed');
      await refreshQueueDuplicatesModal();
      if (typeof loadQueue === 'function') loadQueue({ preserveView: true, force: true });
      if (typeof loadQueueStats === 'function') loadQueueStats();
    } catch (e) {
      _toastBelow('Delete failed: ' + (e.message || e));
    }
  }

  // ── Suggestions modal logic ──────────────────────────────────────
  // Single-file presenter — paginates through every unmatched file
  // that has cached suggestions. The current file's 5-frame filmstrip
  // sits at the top so the user can compare on-disk frames to the
  // candidate posters/thumbnails on a single screen.
  let _suggestionsFiles = [];
  let _suggestionsIndex = 0;
  let _suggestionsFrameToken = 0;
  let _suggestionsFrameSources = [];
  function openSuggestionsModal() {
    const overlay = document.getElementById('suggestionsOverlay');
    overlay.style.display = 'flex';
    loadSuggestions();
  }
  function closeSuggestionsModal() {
    document.getElementById('suggestionsOverlay').style.display = 'none';
    _suggestionsFrameToken++;  // cancel any in-flight thumb fetches
    // Reset state so reopening from a different brain-icon entry
    // starts clean instead of briefly flashing the previous file's
    // card before the new fetch lands.
    _suggestionsFiles = [];
    _suggestionsIndex = 0;
    const body = document.getElementById('suggestionsBody');
    if (body) body.innerHTML = '';
  }
  document.addEventListener('keydown', function(e) {
    const o = document.getElementById('suggestionsOverlay');
    if (!o || o.style.display !== 'flex') return;
    const qov = document.getElementById('queueImageOverlay');
    if (qov && qov.style.display === 'flex') {
      if (e.key === 'Escape') { e.preventDefault(); closeQueueImageOverlay(); return; }
      if (e.key === 'ArrowLeft' || e.key === 'ArrowRight') {
        e.preventDefault();
        queueOverlayStep(e.key === 'ArrowLeft' ? -1 : 1);
        return;
      }
    }
    if (e.key === 'Escape')      { closeSuggestionsModal(); }
    else if (e.key === 'ArrowLeft')  { suggestionsStep(-1); }
    else if (e.key === 'ArrowRight') { suggestionsStep(1); }
  });
  async function loadSuggestions() {
    const body = document.getElementById('suggestionsBody');
    body.innerHTML = '<div style="padding:48px;text-align:center;color:var(--dim);font-size:13px"><span class="loader loader--block" role="status" aria-label="Loading"></span>Loading suggestions…</div>';
    try {
      const r = await fetch('/api/queue/suggestions');
      const d = await r.json();
      _suggestionsFiles = d.files || [];
      if (_suggestionsIndex >= _suggestionsFiles.length) {
        _suggestionsIndex = Math.max(0, _suggestionsFiles.length - 1);
      }
      _renderSuggestionsCurrent();
    } catch (e) {
      body.innerHTML = '<div style="padding:18px;color:var(--red);font-size:13px">Failed to load suggestions: ' + esc(e.message) + '</div>';
    }
  }
  function suggestionsStep(delta) {
    if (!_suggestionsFiles.length) return;
    _suggestionsIndex = Math.max(0, Math.min(_suggestionsFiles.length - 1, _suggestionsIndex + delta));
    _renderSuggestionsCurrent();
  }
  function _suggestionsUpdateCounter() {
    const total = _suggestionsFiles.length;
    const cur = total ? (_suggestionsIndex + 1) : 0;
    const lbl = document.getElementById('suggestionsCounter');
    if (lbl) lbl.textContent = total ? `${cur} / ${total}` : '— / —';
    const pBtn = document.getElementById('suggestionsPrevBtn');
    const nBtn = document.getElementById('suggestionsNextBtn');
    if (pBtn) pBtn.disabled = (cur <= 1);
    if (nBtn) nBtn.disabled = (cur >= total);
    if (pBtn) pBtn.style.opacity = pBtn.disabled ? '0.35' : '1';
    if (nBtn) nBtn.style.opacity = nBtn.disabled ? '0.35' : '1';
    const cnt = document.getElementById('suggestionsCountLabel');
    if (cnt) cnt.textContent = total
      ? `${total} file${total === 1 ? '' : 's'} awaiting review`
      : '';
  }
  function _renderSuggestionsCurrent() {
    const body = document.getElementById('suggestionsBody');
    _suggestionsUpdateCounter();
    if (!_suggestionsFiles.length) {
      body.innerHTML = '<div style="padding:48px;text-align:center;color:var(--dim);font-size:13px"><i class="fa-solid fa-check-double" style="font-size:32px;display:block;margin-bottom:12px;color:rgba(34,197,94,0.6)"></i>No pending suggestions. Try <strong style="color:var(--text)">Re-scan all</strong> to query the source DBs by filename.</div>';
      return;
    }
    const f = _suggestionsFiles[_suggestionsIndex];
    body.innerHTML = _renderSuggestionsPanel(f);
    _loadSuggestionsFilmStrip(f.filename);
    _loadSuggestionsSourceMeta(f.filename);
    _hydratePerfPiles(body);
    _enrichIafdCardsCast(body);
  }
  // IAFD candidate cards land empty-handed for performers — the
  // search-results scrape only carries title/year/studio. Walk every
  // IAFD card after render, fetch the cast (already in cache thanks to
  // the suggestion pre-warmer), and inject the same performer row +
  // headshot pile the other source DBs render natively.
  async function _enrichIafdCardsCast(rootEl) {
    const iafdCards = (rootEl || document).querySelectorAll('.suggestion-card[data-source-db="IAFD"]');
    if (!iafdCards.length) return;
    for (const card of iafdCards) {
      const sourceId = card.getAttribute('data-source-id') || '';
      if (!sourceId) continue;
      // Skip cards that already have a performer row OR pile —
      // IAFD's local-DB search path populates performers natively
      // when the performer is already in `iafd_performers`, so the
      // base card-template renders them and we don't need to enrich.
      if (card.querySelector('.iafd-cast-row')) continue;
      if (card.querySelector('.ts-perf-pile')) continue;
      try {
        const r = await fetch('/api/queue/suggestions/iafd-cast?source_id=' + encodeURIComponent(sourceId));
        const d = await r.json();
        const names = (d && d.performers) || [];
        if (!names.length) continue;
        // Body section is the only flex-column sibling of the hero;
        // its inline padding marks it.
        const cardBody = Array.from(card.children).find(el =>
          el.style && /padding:\s*14px\s*16px/.test(el.getAttribute('style') || '')
        );
        if (!cardBody) continue;
        const namesText = names.join(', ');
        const namesPipe = names.join('|');
        const html =
          `<div class="iafd-cast-row" style="font-size:12px;color:rgba(var(--brand-accent-rgb), 0.85);line-height:1.4;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden" title="${esc(namesText)}"><i class="fa-solid fa-user" style="font-size:10px;margin-right:6px;opacity:0.7"></i>${esc(namesText)}</div>` +
          `<div class="ts-perf-pile" data-perf-names="${esc(namesPipe)}" data-cap="5"></div>`;
        cardBody.insertAdjacentHTML('beforeend', html);
        _hydratePerfPiles(cardBody);
        // Also feed the cast back through the suggestion-card
        // dataset so the IAFD scene-picker fall-through (if it
        // fires) inherits the same names without a second fetch.
        card.setAttribute('data-iafd-cast', namesPipe);
      } catch (_) {
        // Pre-warmer hadn't populated yet OR network blip — leave
        // the card without a cast row; user can still accept.
      }
    }
  }
  // Format a duration in seconds as a compact label: 1h 23m / 23m 47s.
  function _fmtDuration(sec) {
    const s = Number(sec || 0);
    if (!s || s < 1) return '';
    if (s >= 3600) {
      const h = Math.floor(s / 3600);
      const m = Math.round((s % 3600) / 60);
      return m ? `${h}h ${m}m` : `${h}h`;
    }
    if (s >= 60) {
      const m = Math.floor(s / 60);
      const r = Math.round(s % 60);
      return r ? `${m}m ${r}s` : `${m}m`;
    }
    return `${Math.round(s)}s`;
  }
  // Lazy-loads the source video's resolution + duration into the file
  // row caption that replaces 'Source video · 5-frame preview'.
  async function _loadSuggestionsSourceMeta(filename) {
    const cell = document.querySelector(`.suggestion-source-meta[data-filename="${CSS.escape(filename)}"]`);
    if (!cell) return;
    try {
      const r = await fetch('/api/queue/suggestions/source-meta?filename=' + encodeURIComponent(filename));
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const d = await r.json();
      const parts = [];
      if (d.width && d.height) parts.push(`${d.width}×${d.height}`);
      const dur = _fmtDuration(d.duration_sec);
      if (dur) parts.push(dur);
      cell.textContent = parts.length ? parts.join(' · ') : 'Source video';
      cell.title = '';
    } catch (e) {
      cell.textContent = 'Source video (metadata unavailable)';
      cell.title = (e && e.message) || 'metadata fetch failed';
      if (typeof console !== 'undefined') console.warn('[queue] source-meta failed for', filename, e);
    }
  }
  function _renderSuggestionsPanel(f) {
    const fnameSafe = esc(f.filename);
    const fnAttr = (f.filename || '').replace(/'/g, "\\'");
    const cands = (f.candidates || []).map(c => _renderSuggestionCard(f.filename, c)).join('') ||
      '<div style="grid-column:1/-1;padding:32px;text-align:center;color:var(--dim);font-size:12px;background:rgba(0,0,0,0.20);border:1px dashed rgba(var(--brand-purple-rgb),0.18);border-radius:10px">No candidates yet — try the per-file rescan.</div>';
    // Persistent "last accept failed" banner — stays on the row until
    // a successful accept clears the error or the user dismisses it.
    // Toasts get auto-dismissed in 5–12s and were too easy to miss
    // when the failure repeated; the banner makes it impossible to
    // wonder "why isn't anything happening" — the reason is right there.
    const errBanner = f._acceptError
      ? `<div class="suggestion-error-banner" style="display:flex;align-items:center;gap:10px;padding:10px 14px;margin-bottom:14px;border-radius:8px;border:1px solid rgba(248,113,113,0.55);background:rgba(248,113,113,0.12);color:#fca5a5;font-size:12px;line-height:1.4">
          <i class="fa-solid fa-triangle-exclamation" style="font-size:14px"></i>
          <div style="flex:1;min-width:0;word-break:break-word">Last accept failed: <strong style="color:#fff">${esc(f._acceptError)}</strong></div>
          <button type="button" onclick="_dismissAcceptError('${fnAttr}')" title="Dismiss" style="background:transparent;border:1px solid rgba(248,113,113,0.45);color:#fca5a5;border-radius:6px;width:26px;height:26px;cursor:pointer;display:inline-flex;align-items:center;justify-content:center"><i class="fa-solid fa-xmark" style="font-size:11px"></i></button>
        </div>`
      : '';
    // Weak-match banner — when every candidate scored below ~70 % the
    // numbers are barely a signal; promote a Manual Search shortcut so
    // the user doesn't have to close this modal and hunt the file down
    // in the queue list. Counts a missing candidate list as weak too
    // (so the "rescan returned nothing" case offers the same out).
    const cArr = f.candidates || [];
    const topScore = cArr.length ? Math.round((cArr[0].score || 0) * 100) : 0;
    const isWeak = !cArr.length || topScore < 70;
    const weakBanner = isWeak ? `
      <div class="suggestion-weak-banner" style="display:flex;align-items:center;gap:12px;padding:10px 14px;margin-bottom:14px;border-radius:8px;border:1px solid rgba(251,191,36,0.45);background:rgba(251,191,36,0.10);color:#fcd34d;font-size:12px;line-height:1.4">
        <i class="fa-solid fa-circle-info" style="font-size:14px;opacity:0.85"></i>
        <div style="flex:1;min-width:0">
          ${cArr.length
            ? `Weak matches — top score is <strong style="color:#fde68a">${topScore}%</strong>. Manual search will let you pin the right scene.`
            : `No candidates surfaced for this file. Try a manual search to pin it directly.`}
        </div>
        <button type="button" onclick="(window.closeSuggestionsModal||(()=>{}))();(window.openManualSearch||(()=>{}))('${fnAttr}')" title="Open manual search" style="background:rgba(251,191,36,0.18);border:1px solid rgba(251,191,36,0.55);border-radius:6px;color:#fde68a;padding:6px 12px;cursor:pointer;font-size:11px;font-weight:600;letter-spacing:0.04em;text-transform:uppercase;display:inline-flex;align-items:center;gap:6px;white-space:nowrap" onmouseenter="this.style.background='rgba(251,191,36,0.28)';this.style.borderColor='rgba(251,191,36,0.80)'" onmouseleave="this.style.background='rgba(251,191,36,0.18)';this.style.borderColor='rgba(251,191,36,0.55)'"><i class="fa-solid fa-magnifying-glass" style="font-size:10px"></i> Manual search</button>
      </div>` : '';
    return `
      <div class="suggestion-file" data-filename="${fnameSafe}">
        ${errBanner}
        ${weakBanner}
        <!-- Filename row -->
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:14px">
          <div style="width:36px;height:36px;border-radius:8px;background:rgba(var(--brand-purple-rgb),0.14);border:1px solid rgba(var(--brand-purple-rgb),0.30);display:flex;align-items:center;justify-content:center;flex-shrink:0">
            <i class="fa-solid fa-file-video" style="color:rgb(var(--brand-purple-rgb));font-size:14px"></i>
          </div>
          <div style="flex:1;min-width:0">
            <div style="font-family:var(--secs);font-size:16px;color:var(--text);letter-spacing:0.02em;white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="${fnameSafe}">${fnameSafe}</div>
            <div class="suggestion-source-meta" data-filename="${fnameSafe}" style="font-size:10px;color:var(--dim);letter-spacing:0.06em;text-transform:uppercase;margin-top:2px">…</div>
          </div>
          <button onclick="rescanSuggestionFile('${fnAttr}')" title="Re-run search" style="background:rgba(255,255,255,0.04);border:1px solid rgba(var(--brand-purple-rgb),0.25);border-radius:8px;color:var(--text);padding:8px 14px;cursor:pointer;font-size:11px;display:flex;align-items:center;gap:6px;flex-shrink:0"><i class="fa-solid fa-arrows-rotate"></i> Rescan this file</button>
        </div>

        <!-- 5-frame film strip from the on-disk video -->
        <div id="suggestionsFilmStrip" class="film-strip film-strip--suggestions" style="display:flex;justify-content:center;gap:4px;margin-bottom:22px;overflow:hidden">
          <!-- sprocket holes painted via ::before/::after on the .film-strip--suggestions class block in CSS -->
        </div>

        <!-- Candidate cards — fixed 3-up grid so the row always matches
             the top-3 search budget. Tracks share the body evenly; if
             fewer than 3 candidates land they sit left-aligned. -->
        <div style="display:grid;grid-template-columns:repeat(3, minmax(0, 1fr));gap:16px">${cands}</div>
      </div>`;
  }
  function _scenePublicUrl(source_db, source_id) {
    const id = String(source_id || '').trim();
    if (!id) return '';
    switch ((source_db || '').toLowerCase()) {
      case 'stashdb':   return 'https://stashdb.org/scenes/' + encodeURIComponent(id);
      case 'tpdb':
      case 'theporndb': return 'https://theporndb.net/scenes/' + encodeURIComponent(id);
      case 'fansdb':    return 'https://fansdb.cc/scenes/' + encodeURIComponent(id);
      case 'javstash':  return 'https://javstash.org/scenes/' + encodeURIComponent(id);
      case 'iafd':      return id.startsWith('iafd:') ? id.slice(5) : id;
      default:          return '';
    }
  }
  function _renderSuggestionCard(filename, c) {
    // Per-row tokens (this candidate's performers + library-only
    // studio) paint single-word matches; library multi-word phrases
    // paint via `_libraryAndTokenHighlight`. That avoids the old
    // false-positive flood from token-splitting multi-word vice /
    // performer names.
    const _hl = (typeof _qsBuildHighlightSet === 'function')
      ? _qsBuildHighlightSet(
          c.performers,
          (typeof _studioIfInLibrary === 'function' ? _studioIfInLibrary(c.studio) : c.studio))
      : null;
    const _h = (s) => (typeof _libraryAndTokenHighlight === 'function')
      ? _libraryAndTokenHighlight(s, _hl)
      : esc(s);
    const logoSrc = {
      'StashDB': '/static/logos/stashdb.webp',
      'TPDB':    '/static/logos/tpdb.webp',
      'FansDB':  '/static/logos/fansdb.webp',
      'IAFD':    '/static/logos/iafd.webp',
      'JAVStash': '/static/logos/javstash.webp',
      'javstash': '/static/logos/javstash.webp',
    }[c.source_db] || '';
    const fnAttr = (filename || '').replace(/'/g, "\\'");
    const sidAttr = (c.source_id || '').replace(/'/g, "\\'");
    const dbAttr = (c.source_db || '').replace(/'/g, "\\'");
    const titleAttr = (c.title || '').replace(/"/g, '&quot;');
    const sceneUrl = _scenePublicUrl(c.source_db, c.source_id);
    const scoreNum = Math.min(100, Math.round((c.score || 0) * 100));
    // Pull the scene's runtime out of the cached source-DB payload —
    // stash-box `searchScene` returns it as an integer in seconds, so
    // the user can compare it against the source video's duration.
    const candDuration = (c.payload && c.payload.duration) ? Number(c.payload.duration) : 0;
    const candDurationLabel = candDuration ? _fmtDuration(candDuration) : '';
    const scoreColor = scoreNum >= 80 ? '#86efac' : scoreNum >= 50 ? '#fbbf24' : '#fca5a5';
    // Reason pills — backend's `_score_name_match` stashes a list of
    // {kind, label, ok} objects in payload._reasons so the user can see
    // *why* a score landed where it did (which signals matched, which
    // didn't). Render as a wrapping flex row of small chips coloured
    // green for ok, dim-red for not-ok.
    const reasons = (c.payload && Array.isArray(c.payload._reasons)) ? c.payload._reasons : [];
    const reasonIcon = (k) => ({
      title: 'fa-quote-right',
      performer: 'fa-user',
      duration: 'fa-clock',
      year: 'fa-calendar',
      cast: 'fa-users',
      scene_idx: 'fa-hashtag',
    }[k] || 'fa-circle-info');
    const reasonsHtml = reasons.length ? `
      <div class="sg-reasons" style="display:flex;flex-wrap:wrap;gap:4px;margin-top:2px">
        ${reasons.map(r => {
          const ok = !!r.ok;
          const bg = ok ? 'rgba(34,197,94,0.12)' : 'rgba(248,113,113,0.10)';
          const bd = ok ? 'rgba(34,197,94,0.40)' : 'rgba(248,113,113,0.35)';
          const fg = ok ? '#86efac' : '#fca5a5';
          return `<span class="sg-reason" title="${esc(r.label || '')}" style="display:inline-flex;align-items:center;gap:5px;padding:2px 7px;border-radius:999px;border:1px solid ${bd};background:${bg};color:${fg};font-size:10px;line-height:1.5;letter-spacing:0.02em;white-space:nowrap"><i class="fa-solid ${reasonIcon(r.kind)}" style="font-size:8px;opacity:0.85"></i>${esc(r.label || '')}</span>`;
        }).join('')}
      </div>` : '';
    return `
      <div class="suggestion-card" data-source-db="${esc(c.source_db || '')}" data-source-id="${esc(c.source_id || '')}" data-base-title="${titleAttr}" style="display:flex;flex-direction:column;border:1px solid rgba(var(--brand-purple-rgb),0.25);border-radius:12px;background:linear-gradient(160deg, rgba(var(--brand-purple-rgb),0.10) 0%, rgba(0,0,0,0.45) 100%);overflow:hidden;box-shadow:0 6px 20px rgba(0,0,0,0.35);transition:border-color 0.18s, transform 0.18s, box-shadow 0.18s" onmouseenter="this.style.borderColor='rgba(var(--brand-accent-rgb), 0.55)';this.style.transform='translateY(-2px)';this.style.boxShadow='0 12px 30px rgba(0,0,0,0.50), 0 0 24px rgba(var(--brand-accent-rgb), 0.20)'" onmouseleave="this.style.borderColor='rgba(var(--brand-purple-rgb),0.25)';this.style.transform='';this.style.boxShadow='0 6px 20px rgba(0,0,0,0.35)'">
        <!-- Hero thumbnail: 16:9, source-DB logo top-left, score top-right, scene link bottom-right -->
        <div style="position:relative;aspect-ratio:16/9;background:#0a0410;overflow:hidden">
          ${c.thumb_url
            ? `<img src="${esc(c.thumb_url)}" loading="lazy" style="width:100%;height:100%;object-fit:cover;display:block" onerror="this.onerror=null;this.src='/static/img/missing.webp'">`
            : `<img src="/static/img/missing.webp" alt="No artwork" loading="lazy" style="width:100%;height:100%;object-fit:cover;display:block">`}
          <!-- Bottom gradient for legibility -->
          <div style="position:absolute;left:0;right:0;bottom:0;height:55%;background:linear-gradient(180deg, transparent 0%, rgba(0,0,0,0.55) 100%);pointer-events:none"></div>
          <!-- Source logo badge -->
          ${logoSrc
            ? `<div style="position:absolute;top:10px;left:10px;background:rgba(0,0,0,0.65);backdrop-filter:blur(4px);border:1px solid rgba(255,255,255,0.10);border-radius:6px;padding:5px 9px;display:flex;align-items:center;gap:6px"><img src="${logoSrc}" alt="${esc(c.source_db)}" style="height:14px;width:auto;display:block" loading="lazy"></div>`
            : `<div style="position:absolute;top:10px;left:10px;background:rgba(0,0,0,0.65);backdrop-filter:blur(4px);border-radius:6px;padding:4px 9px;font-size:10px;color:var(--text);letter-spacing:0.06em;text-transform:uppercase">${esc(c.source_db || '')}</div>`}
          <!-- Score chip -->
          <div style="position:absolute;top:10px;right:10px;background:rgba(0,0,0,0.70);border:1px solid ${scoreColor};border-radius:6px;padding:3px 8px;font-family:var(--mono);font-size:11px;font-weight:600;color:${scoreColor};font-variant-numeric:tabular-nums">${scoreNum}%</div>
          <!-- Open-on-source link -->
          ${sceneUrl
            ? `<a href="${esc(sceneUrl)}" target="_blank" rel="noopener noreferrer" onclick="event.stopPropagation()" title="Open on ${esc(c.source_db)}" style="position:absolute;bottom:10px;right:10px;background:rgba(0,0,0,0.65);backdrop-filter:blur(4px);border:1px solid rgba(255,255,255,0.10);border-radius:6px;padding:6px 10px;color:var(--text);text-decoration:none;font-size:11px;display:inline-flex;align-items:center;gap:6px;transition:border-color 0.15s, background 0.15s" onmouseenter="this.style.borderColor='rgba(var(--brand-accent-rgb), 0.55)';this.style.background='rgba(var(--brand-accent-rgb), 0.18)'" onmouseleave="this.style.borderColor='rgba(255,255,255,0.10)';this.style.background='rgba(0,0,0,0.65)'"><span>Open</span><i class="fa-solid fa-arrow-up-right-from-square" style="font-size:9px;opacity:0.8"></i></a>`
            : ''}
        </div>
        <!-- Body: title, studio · date, performers -->
        <div style="padding:14px 16px;flex:1;display:flex;flex-direction:column;gap:6px">
          <div style="font-family:var(--secs);font-size:15px;color:var(--text);line-height:1.3;letter-spacing:0.01em;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden" title="${esc(c.title)}">${c.title ? _h(c.title) : 'Unknown'}</div>
          ${(c.studio || c.date || candDurationLabel) ? `<div style="font-size:11px;color:var(--dim);line-height:1.4;letter-spacing:0.04em;text-transform:uppercase;display:flex;align-items:center;gap:6px;flex-wrap:wrap">
            ${c.studio ? `<span style="display:inline-flex;align-items:center;gap:6px"><i class="ts-icon-studios" style="font-size:9px;opacity:0.6" aria-hidden="true"></i><span>${_h(c.studio)}</span></span>` : ''}
            ${c.studio && c.date ? '<span style="opacity:0.5">·</span>' : ''}
            ${c.date ? `<span style="display:inline-flex;align-items:center;gap:6px"><i class="fa-solid fa-calendar" style="font-size:9px;opacity:0.6"></i><span>${esc(c.date)}</span></span>` : ''}
            ${candDurationLabel && (c.studio || c.date) ? '<span style="opacity:0.5">·</span>' : ''}
            ${candDurationLabel ? `<span style="display:inline-flex;align-items:center;gap:6px" title="Scene runtime"><i class="fa-solid fa-clock" style="font-size:9px;opacity:0.6"></i><span>${esc(candDurationLabel)}</span></span>` : ''}
          </div>` : ''}
          ${c.performers ? `<div style="font-size:12px;color:rgba(var(--brand-accent-rgb), 0.85);line-height:1.4;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden" title="${esc(c.performers)}"><i class="fa-solid fa-user" style="font-size:10px;margin-right:6px;opacity:0.7"></i>${_h(c.performers)}</div>` : ''}
          ${c.performers ? `<div class="ts-perf-pile" data-perf-names="${esc((c.performers || '').split(',').map(s => s.trim()).filter(Boolean).join('|'))}" data-cap="5"></div>` : ''}
          ${reasonsHtml}
        </div>
        <!-- Action row -->
        <div style="display:flex;gap:8px;padding:12px 16px;border-top:1px solid rgba(var(--brand-purple-rgb),0.14);background:rgba(0,0,0,0.30)">
          <button class="sg-accept-btn" title="File this match" style="flex:1;background:linear-gradient(135deg, rgba(var(--brand-accent-rgb), 0.30) 0%, rgba(var(--brand-accent-rgb), 0.18) 100%);border:1px solid rgba(var(--brand-accent-rgb), 0.55);border-radius:8px;color:#fce7f3;padding:9px 12px;cursor:pointer;font-size:12px;font-weight:600;letter-spacing:0.04em;text-transform:uppercase;display:flex;align-items:center;justify-content:center;gap:6px;transition:background 0.15s, border-color 0.15s" onmouseenter="this.style.background='linear-gradient(135deg, rgba(var(--brand-accent-rgb), 0.45) 0%, rgba(var(--brand-accent-rgb), 0.28) 100%)';this.style.borderColor='rgba(var(--brand-accent-rgb), 0.85)'" onmouseleave="this.style.background='linear-gradient(135deg, rgba(var(--brand-accent-rgb), 0.30) 0%, rgba(var(--brand-accent-rgb), 0.18) 100%)';this.style.borderColor='rgba(var(--brand-accent-rgb), 0.55)'"><i class="fa-solid fa-check"></i> Accept</button>
          <button class="sg-reject-btn" title="Dismiss" style="flex:0 0 auto;background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.10);border-radius:8px;color:var(--dim);padding:9px 14px;cursor:pointer;font-size:12px;display:flex;align-items:center;justify-content:center;gap:6px;transition:border-color 0.15s, color 0.15s" onmouseenter="this.style.borderColor='rgba(239,68,68,0.55)';this.style.color='#fca5a5'" onmouseleave="this.style.borderColor='rgba(255,255,255,0.10)';this.style.color='var(--dim)'"><i class="fa-solid fa-xmark"></i> Reject</button>
        </div>
      </div>`;
  }
  // Shared headshot-pile hydration. Walks every `.ts-perf-pile`
  // (with a `data-perf-names="A|B|C"` attribute) inside `rootEl`,
  // batch-fetches library headshots in one round-trip via
  // `/api/performers/headshots-by-name`, and renders circular
  // avatars. Library-resolved names get headshots; misses get a
  // neutral silhouette badge so the slot stays balanced.
  async function _hydratePerfPiles(rootEl) {
    const slots = Array.from((rootEl || document).querySelectorAll('.ts-perf-pile[data-perf-names]'));
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
      const cap = parseInt(slot.dataset.cap || '5', 10);
      const shown = names.slice(0, cap);
      const extra = names.length - shown.length;
      const tiles = shown.map(n => {
        const url = lookup[n.toLowerCase()];
        const safeName = n.replace(/"/g, '&quot;');
        return url
          ? `<img class="ts-perf-pile-avatar" src="${url}" alt="${safeName}" title="${safeName}" loading="lazy" decoding="async" onerror="this.outerHTML='<span class=ts-perf-pile-avatar ts-perf-pile-avatar--missing title=\\'${safeName}\\'><i class=&quot;fa-solid fa-user&quot;></i></span>'">`
          : `<span class="ts-perf-pile-avatar ts-perf-pile-avatar--missing" title="${safeName}"><i class="fa-solid fa-user"></i></span>`;
      });
      if (extra > 0) {
        tiles.push(`<span class="ts-perf-pile-avatar ts-perf-pile-avatar--more" title="${extra} more star${extra === 1 ? '' : 's'}">+${extra}</span>`);
      }
      slot.innerHTML = tiles.join('');
    });
  }
  window._hydratePerfPiles = _hydratePerfPiles;

  // Mirror of _loadSearchFrames but targets the suggestions modal
  // strip — uses the same 5-percent capture points and the same
  // `/api/thumb/generate` endpoint so frames match what the queue
  // search panel would show.
  async function _loadSuggestionsFilmStrip(filename) {
    const strip = document.getElementById('suggestionsFilmStrip');
    if (!strip || !filename) return;
    const myToken = ++_suggestionsFrameToken;
    strip.innerHTML = '';
    const percents = [15, 32, 50, 68, 85];
    _suggestionsFrameSources = new Array(percents.length).fill('');
    const slots = percents.map(() => {
      const s = document.createElement('div');
      s.style.cssText = 'aspect-ratio:16/9;height:108px;background:#000;border:1px solid rgba(255,255,255,0.06);overflow:hidden;position:relative;flex:0 1 auto;cursor:zoom-in;display:flex;align-items:center;justify-content:center';
      s.innerHTML = '<span class="loader loader--btn loader--muted" role="status" aria-label="Loading"></span>';
      strip.appendChild(s);
      return s;
    });
    const results = await Promise.all(percents.map(async (p, i) => {
      try {
        const r = await fetch('/api/thumb/generate', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ filename, percent: p }),
        });
        if (myToken !== _suggestionsFrameToken) return 'cancelled';
        if (!r.ok) throw new Error('HTTP ' + r.status);
        const d = await r.json();
        if (d.data_url) {
          _suggestionsFrameSources[i] = d.data_url;
          slots[i].innerHTML = `<img src="${d.data_url}" alt="Frame ${p}%" data-frame-index="${i}" style="width:100%;height:100%;object-fit:cover;display:block" onclick="event.stopPropagation();_openSuggestionsFrameOverlay(${i})" loading="lazy">`;
          return 'ok';
        }
        slots[i].innerHTML = '<i class="fa-solid fa-image" style="color:var(--dim);font-size:14px;opacity:0.4"></i>';
        return 'empty';
      } catch (e) {
        if (myToken !== _suggestionsFrameToken) return 'cancelled';
        slots[i].title = (e && e.message) || 'frame extraction failed';
        slots[i].innerHTML = '<i class="fa-solid fa-triangle-exclamation" style="color:var(--red);font-size:14px;opacity:0.6"></i>';
        return 'fail';
      }
    }));
    if (myToken !== _suggestionsFrameToken) return;
    // If every slot failed, the strip just looks like a row of warning
    // icons. Stamp a one-line hint below it so the user knows it's a
    // server issue rather than a missing file.
    const failures = results.filter(r => r === 'fail').length;
    if (failures === results.length && results.length) {
      strip.title = 'Frame extraction unavailable — check FFmpeg and the source video path';
    } else {
      strip.title = '';
    }
  }
  window.suggestionsStep = suggestionsStep;
  // Resolve the parent title for an auto-pinned scene by reading the
  // owning candidate card's data-base-title attribute. Used so a
  // single-scene auto-pin produces "Parent - Scene N" instead of the
  // bare "Scene N" label, keeping destination filenames unique when
  // two sibling files (Scene 5 + Scene 8) auto-pin against the same
  // parent record.
  //
  // Implementation note: CSS.escape over-escapes for attribute-value
  // selectors (turns `:` into `\3A ` etc), which breaks lookups for
  // IAFD source_ids that look like `iafd:https://www.iafd.com/...`.
  // We walk all .suggestion-card elements inside the file's wrapper
  // and compare attributes literally instead.
  function _suggestionsBaseTitleFor(filename, source_db, source_id) {
    try {
      const wrappers = document.querySelectorAll('.suggestion-file');
      for (const w of wrappers) {
        if (w.getAttribute('data-filename') !== filename) continue;
        const cards = w.querySelectorAll('.suggestion-card');
        for (const c of cards) {
          if (c.getAttribute('data-source-db') === source_db &&
              c.getAttribute('data-source-id') === source_id) {
            return (c.getAttribute('data-base-title') || '').trim();
          }
        }
      }
    } catch { /* fall through */ }
    return '';
  }
  async function acceptSuggestion(filename, source_db, source_id, btn, scene_override) {
    if (btn) { btn.disabled = true; btn.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Loading"></span>'; }
    // IAFD intercept: title pages are movie-level. If the IAFD page
    // ships a per-scene breakdown, pause the accept and let the user
    // pick the right scene first — otherwise we'd commit the entire
    // movie cast against a single-scene file.
    if (source_db === 'IAFD' && !scene_override) {
      try {
        const r = await fetch('/api/queue/suggestions/iafd-breakdown?filename=' +
          encodeURIComponent(filename) + '&source_id=' + encodeURIComponent(source_id));
        if (!r.ok) {
          throw new Error(`IAFD breakdown HTTP ${r.status}`);
        }
        const d = await r.json();
        const scenes = (d && d.scenes) || [];
        // Single-scene movies: there's nothing to pick. Auto-pin the
        // lone scene's cast + label and fall through to the regular
        // accept path so the user gets a one-click file. Title becomes
        // "Parent - Scene N" so destinations stay unique when two
        // sibling files (e.g. Scene 5 + Scene 8) auto-pin against the
        // same parent record.
        if (scenes.length === 1) {
          const sc = scenes[0];
          const cast = (sc.cast || []).map(n => ({ performer: { id: '', name: String(n || '').trim() } }))
                                       .filter(p => p.performer.name);
          const label = sc.label || ('Scene ' + (sc.number || 1));
          const baseTitle = _suggestionsBaseTitleFor(filename, source_db, source_id);
          const finalTitle = baseTitle ? (baseTitle + ' - ' + label) : label;
          scene_override = { performers: cast, title: finalTitle };
          // Falls through to the POST below.
        } else if (scenes.length > 1) {
          if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-check"></i> Accept'; }
          _showIafdScenePicker(filename, source_db, source_id, scenes);
          return;
        }
        // No breakdown → fall through to normal accept.
      } catch (e) {
        console.error('IAFD breakdown intercept failed:', e);
        // Fall through to the normal accept path; the file still gets
        // filed using the cached scene record. Surface a toast so
        // silent failures stop being silent.
        _suggestionsToast(`IAFD scene-picker skipped: ${e && (e.message || e)}`);
      }
    }
    // TPDB intercept: when the suggestion's scene is part of a movie
    // (multi-scene feature), the searchScene result usually carries the
    // movie's full cast against a single-scene file. Surface the per-
    // scene breakdown so the user can pin the actual scene before we
    // commit. Same picker UI as IAFD; the picker re-enters this
    // function with a scene_override so we skip the intercept.
    if (source_db === 'TPDB' && !scene_override) {
      try {
        const r = await fetch('/api/queue/suggestions/tpdb-breakdown?filename=' +
          encodeURIComponent(filename) + '&source_id=' + encodeURIComponent(source_id));
        if (!r.ok) {
          throw new Error(`TPDB breakdown HTTP ${r.status}`);
        }
        const d = await r.json();
        const scenes = (d && d.scenes) || [];
        // Single-scene fall-through, same logic as IAFD above.
        if (scenes.length === 1) {
          const sc = scenes[0];
          const cast = (sc.cast || []).map(n => ({ performer: { id: '', name: String(n || '').trim() } }))
                                       .filter(p => p.performer.name);
          const label = sc.label || ('Scene ' + (sc.number || 1));
          const targetId = (sc.scene_id && String(sc.scene_id).trim()) || source_id;
          const baseTitle = _suggestionsBaseTitleFor(filename, source_db, source_id);
          const finalTitle = baseTitle ? (baseTitle + ' - ' + label) : label;
          scene_override = { performers: cast, title: finalTitle, id: targetId };
        } else if (scenes.length > 1) {
          if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-check"></i> Accept'; }
          _showTpdbScenePicker(filename, source_db, source_id, scenes);
          return;
        }
      } catch (e) {
        console.error('TPDB breakdown intercept failed:', e);
        _suggestionsToast(`TPDB scene-picker skipped: ${e && (e.message || e)}`);
      }
    }
    // Fire-and-forget: kick off the accept POST but don't await the
    // response. The backend file move + NFO + thumb + media-server
    // scan can take 5–30 s; blocking the UI on it makes the user wait
    // between every Accept click. Optimistically remove the file from
    // the in-memory list, advance to the next, and let the POST run
    // in the background. Errors surface as a non-blocking toast.
    // Capture the file + its index BEFORE the optimistic remove so
    // we can restore exact placement if the backend rejects the
    // accept. Without this, a failed POST used to call
    // `loadSuggestions()` which re-fetched the whole list and reset
    // the paginator to index 0 — visually identical to "the click
    // did nothing" because the bounce was too fast to see.
    const failedIdx = _suggestionsFiles.findIndex(f => f.filename === filename);
    const failedFile = failedIdx >= 0 ? _suggestionsFiles[failedIdx] : null;
    _suggestionsFiles = _suggestionsFiles.filter(f => f.filename !== filename);
    if (_suggestionsIndex >= _suggestionsFiles.length) {
      _suggestionsIndex = Math.max(0, _suggestionsFiles.length - 1);
    }
    _renderSuggestionsCurrent();

    function restoreOnFailure(errMessage) {
      if (!failedFile) return;
      // Splice the file back at its original index so the paginator
      // doesn't reshuffle around the user. Cap to current length in
      // case other rows came/went in the meantime.
      const restoreAt = Math.min(failedIdx, _suggestionsFiles.length);
      // Stamp the error onto the file dict so the renderer can show
      // a persistent banner. Without this the only failure feedback
      // was a 5-8s toast that often disappeared before the user
      // looked at it — leaving "click did nothing" as the impression.
      failedFile._acceptError = errMessage || 'Filing failed';
      _suggestionsFiles.splice(restoreAt, 0, failedFile);
      _suggestionsIndex = restoreAt;
      _renderSuggestionsCurrent();
    }

    // Retry handler — re-fires this same accept call. Used as the
    // toast action so a user can recover from a transient failure
    // (network blip, dest folder briefly missing, etc.) without
    // hunting for the row in the modal.
    const retry = () => acceptSuggestion(filename, source_db, source_id, null, scene_override);
    fetch('/api/queue/suggestions/accept', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ filename, source_db, source_id, scene_override: scene_override || null }),
    }).then(r => r.json()).then(d => {
      if (d && (d.ok || d.status === 'filed')) {
        loadQueueStats();
        if (typeof loadQueue === 'function') loadQueue({ preserveView: true });
      } else {
        const msg = (d && (d.error || d.status)) || 'unknown error';
        _suggestionsToast(
          `Filing failed for ${filename}: ${msg}`,
          { label: 'Retry', callback: retry, timeout: 12000 },
        );
        restoreOnFailure(msg);
      }
    }).catch(e => {
      const msg = (e && (e.message || e.toString())) || 'network error';
      _suggestionsToast(
        `Network error filing ${filename}: ${msg}`,
        { label: 'Retry', callback: retry, timeout: 12000 },
      );
      restoreOnFailure('network error: ' + msg);
    });
  }
  // Minimal non-blocking toast for background-filing failures. Pinned
  // bottom-centre over the suggestions overlay; auto-dismisses after
  // 5 s. Stacks vertically when multiple errors fire in a row.
  //
  // Optional `action = {label, callback}` renders a right-aligned
  // button alongside the message — clicking it fires `callback()` and
  // dismisses the toast. Modelled after Material 3 Snackbar's
  // single-action affordance so users can recover from a failed
  // action without digging through the modal.
  function _suggestionsToast(message, action) {
    const overlay = document.getElementById('suggestionsOverlay');
    if (!overlay) return;
    let stack = document.getElementById('suggestionsToastStack');
    if (!stack) {
      stack = document.createElement('div');
      stack.id = 'suggestionsToastStack';
      stack.style.cssText = 'position:absolute;left:50%;bottom:24px;transform:translateX(-50%);display:flex;flex-direction:column;gap:8px;z-index:3;pointer-events:none';
      overlay.appendChild(stack);
    }
    const toast = document.createElement('div');
    toast.style.cssText = 'background:rgba(220,38,38,0.92);color:#fff;border:1px solid rgba(255,255,255,0.18);border-radius:8px;padding:10px 16px;font-family:var(--mono);font-size:12px;line-height:1.45;letter-spacing:0.04em;max-width:min(540px, 90vw);word-break:break-word;overflow-wrap:anywhere;white-space:normal;box-shadow:0 8px 24px rgba(0,0,0,0.5);backdrop-filter:blur(8px);-webkit-backdrop-filter:blur(8px);pointer-events:auto;display:flex;align-items:center;gap:14px;animation:fadeInUp 0.18s ease';
    const msg = document.createElement('span');
    msg.style.cssText = 'flex:1;min-width:0';
    msg.textContent = message;
    toast.appendChild(msg);
    if (action && typeof action.callback === 'function') {
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.style.cssText = 'flex-shrink:0;background:rgba(255,255,255,0.14);border:1px solid rgba(255,255,255,0.32);color:#fff;border-radius:6px;padding:5px 12px;font-family:var(--secs);font-size:11px;font-weight:600;letter-spacing:0.06em;text-transform:uppercase;cursor:pointer;transition:background 0.15s, border-color 0.15s';
      btn.textContent = action.label || 'Retry';
      btn.onmouseover = () => { btn.style.background = 'rgba(255,255,255,0.22)'; btn.style.borderColor = 'rgba(255,255,255,0.45)'; };
      btn.onmouseout = () => { btn.style.background = 'rgba(255,255,255,0.14)'; btn.style.borderColor = 'rgba(255,255,255,0.32)'; };
      btn.onclick = (e) => { e.stopPropagation(); try { action.callback(); } finally { toast.remove(); } };
      toast.appendChild(btn);
    } else {
      toast.style.cursor = 'pointer';
      toast.onclick = () => toast.remove();
    }
    stack.appendChild(toast);
    setTimeout(() => toast.remove(), action ? 8000 : 5000);
  }
  // Walk the suggestion DOM literally to find a card. CSS.escape
  // over-escapes characters like `:` and `/` for attribute-value
  // selectors, which produces a lookup that misses IAFD source_ids
  // shaped like `iafd:https://www.iafd.com/...`.
  function _findSuggestionCard(filename, source_db, source_id) {
    const wrappers = document.querySelectorAll('.suggestion-file');
    for (const w of wrappers) {
      if (w.getAttribute('data-filename') !== filename) continue;
      const cards = w.querySelectorAll('.suggestion-card');
      for (const c of cards) {
        if (c.getAttribute('data-source-db') === source_db &&
            c.getAttribute('data-source-id') === source_id) return c;
      }
    }
    return null;
  }
  // IAFD scene picker — replaces the IAFD candidate card's body with
  // a list of the movie's scenes. Clicking a scene fires the accept
  // flow with a `scene_override` that pins that scene's performers and
  // appends the scene label to the title.
  function _showIafdScenePicker(filename, source_db, source_id, scenes) {
    const card = _findSuggestionCard(filename, source_db, source_id);
    if (!card) {
      console.error('IAFD picker: card not found', { filename, source_db, source_id });
      _suggestionsToast(`Couldn't open scene picker for ${filename}.`);
      return;
    }
    // Stamp identifiers + per-scene payload onto data attributes so a
    // delegated click handler reads them off the DOM. Inline `onclick`
    // with `pickIafdScene("filename", ...)` broke whenever the
    // filename contained an apostrophe / ampersand — same class of
    // bug as the Accept buttons, fixed the same way (delegation).
    const rowsHtml = scenes.map((sc, i) => {
      const cast = (sc.cast || []).join(', ');
      const label = sc.label || ('Scene ' + (sc.number || (i + 1)));
      const castJson = JSON.stringify(sc.cast || []).replace(/"/g, '&quot;');
      return `<button type="button" class="iafd-scene-pick" data-idx="${i}"
        data-scene-label="${esc(label)}"
        data-scene-cast="${castJson}"
        style="display:flex;flex-direction:column;align-items:flex-start;gap:2px;padding:8px 10px;border:1px solid rgba(var(--brand-purple-rgb),0.22);border-radius:6px;background:rgba(0,0,0,0.20);color:var(--text);text-align:left;cursor:pointer;font:inherit;width:100%">
        <span style="font-family:var(--secs);font-size:12px;letter-spacing:0.04em">${esc(label)}</span>
        <span style="font-size:11px;color:var(--dim);line-height:1.3">${esc(cast)}</span>
      </button>`;
    }).join('');
    card.innerHTML = `
      <div style="padding:10px 10px 6px;display:flex;align-items:center;gap:8px;border-bottom:1px solid rgba(var(--brand-purple-rgb),0.14)">
        <img src="/static/logos/iafd.webp" alt="IAFD" style="height:14px;width:auto">
        <h3 class="table-title" style="margin:0;color:var(--text);text-transform:uppercase">Pick scene</h3>
        <button type="button" onclick="loadSuggestions()" title="Cancel" style="margin-left:auto;background:transparent;border:none;color:var(--dim);cursor:pointer;font-size:14px">&times;</button>
      </div>
      <div style="padding:8px 10px;display:flex;flex-direction:column;gap:6px;max-height:340px;overflow-y:auto">
        ${rowsHtml}
      </div>`;
  }
  async function pickIafdScene(filename, source_db, source_id, label, cast, btn) {
    if (btn) { btn.disabled = true; btn.style.opacity = '0.6'; }
    // Build a stash-box-shaped performers list from the names IAFD
    // gave us. Empty ids are fine here — `file_scene_from_match` only
    // needs the names to route into performer folders.
    const performers = (cast || []).map(name => ({
      performer: { id: '', name: String(name || '').trim() }
    })).filter(p => p.performer.name);
    // Pull the candidate's existing title so we can append " - Scene N".
    const card = btn && btn.closest && btn.closest('.suggestion-card');
    let baseTitle = '';
    if (card) {
      const hint = card.getAttribute('data-base-title');
      if (hint) baseTitle = hint;
    }
    const finalTitle = baseTitle ? (baseTitle + ' - ' + label) : label;
    const scene_override = { performers, title: finalTitle };
    await acceptSuggestion(filename, source_db, source_id, null, scene_override);
  }
  window.pickIafdScene = pickIafdScene;
  // TPDB scene picker — same shape as the IAFD one, only the logo +
  // pickTpdbScene callback differ. TPDB's scene-level cast is more
  // reliable than IAFD's so we send the cast verbatim from the
  // breakdown.
  function _showTpdbScenePicker(filename, source_db, source_id, scenes) {
    const card = _findSuggestionCard(filename, source_db, source_id);
    if (!card) {
      console.error('TPDB picker: card not found', { filename, source_db, source_id });
      _suggestionsToast(`Couldn't open scene picker for ${filename}.`);
      return;
    }
    const rowsHtml = scenes.map((sc, i) => {
      const cast = (sc.cast || []).join(', ');
      const label = sc.label || ('Scene ' + (sc.number || (i + 1)));
      const castJson = JSON.stringify(sc.cast || []).replace(/"/g, '&quot;');
      return `<button type="button" class="tpdb-scene-pick" data-idx="${i}"
        data-scene-id="${esc(sc.scene_id || '')}"
        data-scene-label="${esc(label)}"
        data-scene-cast="${castJson}"
        style="display:flex;flex-direction:column;align-items:flex-start;gap:2px;padding:8px 10px;border:1px solid rgba(var(--brand-purple-rgb),0.22);border-radius:6px;background:rgba(0,0,0,0.20);color:var(--text);text-align:left;cursor:pointer;font:inherit;width:100%">
        <span style="font-family:var(--secs);font-size:12px;letter-spacing:0.04em">${esc(label)}</span>
        <span style="font-size:11px;color:var(--dim);line-height:1.3">${esc(cast)}</span>
      </button>`;
    }).join('');
    card.innerHTML = `
      <div style="padding:10px 10px 6px;display:flex;align-items:center;gap:8px;border-bottom:1px solid rgba(var(--brand-purple-rgb),0.14)">
        <img src="/static/logos/tpdb.webp" alt="TPDB" style="height:14px;width:auto">
        <h3 class="table-title" style="margin:0;color:var(--text);text-transform:uppercase">Pick scene</h3>
        <button type="button" onclick="loadSuggestions()" title="Cancel" style="margin-left:auto;background:transparent;border:none;color:var(--dim);cursor:pointer;font-size:14px">&times;</button>
      </div>
      <div style="padding:8px 10px;display:flex;flex-direction:column;gap:6px;max-height:340px;overflow-y:auto">
        ${rowsHtml}
      </div>`;
  }
  async function pickTpdbScene(filename, source_db, source_id, scene_id, label, cast, btn) {
    if (btn) { btn.disabled = true; btn.style.opacity = '0.6'; }
    const performers = (cast || []).map(name => ({
      performer: { id: '', name: String(name || '').trim() }
    })).filter(p => p.performer.name);
    const card = btn && btn.closest && btn.closest('.suggestion-card');
    let baseTitle = '';
    if (card) {
      const hint = card.getAttribute('data-base-title');
      if (hint) baseTitle = hint;
    }
    const finalTitle = baseTitle ? (baseTitle + ' - ' + label) : label;
    // For TPDB, the sibling scene id is what we actually want to
    // commit — re-route the accept to the picked scene's id rather
    // than the originally-suggested one. The cached payload's other
    // fields (studio/date/poster) still apply since they came from
    // the same parent movie.
    const targetId = (scene_id && String(scene_id).trim()) || source_id;
    const scene_override = { performers, title: finalTitle, id: targetId };
    await acceptSuggestion(filename, source_db, source_id, null, scene_override);
  }
  window.pickTpdbScene = pickTpdbScene;
  async function rejectSuggestion(filename, source_db, source_id, btn) {
    if (btn) { btn.disabled = true; btn.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Loading"></span>'; }
    // Optimistic in-memory update — drop the rejected candidate from
    // the cached `_suggestionsFiles` and re-render the SAME file so
    // the user stays put. Previously this called `loadSuggestions()`
    // which re-fetched the whole list and reset the paginator to
    // index 0 — meaning a reject would jump the user back to the top
    // of the queue, losing their place if the list re-sorted.
    const file = _suggestionsFiles.find(f => f.filename === filename);
    if (file) {
      file.candidates = (file.candidates || []).filter(
        c => !(c.source_db === source_db && c.source_id === source_id)
      );
      // If that was the file's last candidate, drop the file from the
      // list too and clamp the index so the next file slides in.
      if (!file.candidates.length) {
        const idx = _suggestionsFiles.findIndex(f => f.filename === filename);
        if (idx !== -1) {
          _suggestionsFiles.splice(idx, 1);
          if (_suggestionsIndex >= _suggestionsFiles.length) {
            _suggestionsIndex = Math.max(0, _suggestionsFiles.length - 1);
          }
        }
      }
      _renderSuggestionsCurrent();
    }
    // Fire-and-forget persistence — the user's UI has already moved
    // on. On failure, surface a toast and re-fetch the canonical list
    // so the rejected candidate doesn't silently re-appear.
    fetch('/api/queue/suggestions/reject', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ filename, source_db, source_id }),
    }).then(() => {
      loadQueueStats();
    }).catch(e => {
      console.error('Reject persist failed:', e);
      _suggestionsToast('Reject failed: ' + (e && (e.message || e)));
      loadSuggestions();
    });
  }
  async function rescanSuggestionFile(filename) {
    try {
      await fetch('/api/queue/suggestions/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ filename }),
      });
      loadSuggestions();
      loadQueueStats();
    } catch {}
  }
  async function runSuggestionScan() {
    const btn = document.getElementById('suggestionsRescanBtn');
    if (btn) { btn.disabled = true; btn.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Loading"></span> Scanning…'; }
    try {
      await fetch('/api/queue/suggestions/run', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' });
      await loadSuggestions();
      loadQueueStats();
    } finally {
      if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-arrows-rotate"></i> Re-scan all'; }
    }
  }
  // Per-row entry point — fired by the brain icon on each queue
  // item. Runs a name search for just that one file. If candidates
  // come back, opens the Suggestions modal scrolled to that file.
  // If NOTHING came back (no source DB had a hit), opens the manual-
  // search dialog so the user can keep digging by hand instead of
  // dead-ending on a toast.
  async function runRowSuggestions(filename, btn) {
    if (!filename) return;
    const original = btn ? btn.innerHTML : null;
    if (btn) {
      btn.disabled = true;
      btn.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Loading"></span>';
    }
    try {
      const r = await fetch('/api/queue/suggestions/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ filename }),
      });
      const d = await r.json().catch(() => ({}));
      const newlyAdded = Number(d.added || 0);
      // Even if no new suggestions were just added, the file may
      // already have cached candidates from a previous scan that we
      // can show. Check before deciding to open manual search.
      const hasCached = await _suggestionsFileHasCached(filename);
      if (newlyAdded === 0 && !hasCached) {
        // Truly nothing — drop straight into manual search so the
        // user can pivot without an extra click.
        if (typeof openManualSearch === 'function') {
          openManualSearch(filename);
        } else {
          _toastBelow(`No suggestions found for ${filename}. Try the magnifying-glass icon.`);
        }
        return;
      }
      openSuggestionsModal();
      // loadSuggestions() is async and triggered by openSuggestionsModal;
      // a tiny grace period lets it populate _suggestionsFiles first.
      setTimeout(() => {
        let attempts = 0;
        const seek = () => {
          const idx = _suggestionsFiles.findIndex(f => f.filename === filename);
          if (idx >= 0) {
            _suggestionsIndex = idx;
            _renderSuggestionsCurrent();
            return;
          }
          if (++attempts < 20) setTimeout(seek, 100);
        };
        seek();
      }, 50);
      loadQueueStats();
    } catch (e) {
      console.error('runRowSuggestions failed:', e);
      _toastBelow(`Brain search failed: ${e && (e.message || e)}`);
    } finally {
      if (btn) {
        btn.disabled = false;
        btn.innerHTML = original || '<i class="fa-solid fa-brain"></i>';
      }
    }
  }
  // Cheap "does this file already have cached suggestions?" check.
  // Hits the existing list endpoint and looks for the filename — the
  // list is small (only files with at least one cached candidate),
  // so the round-trip is acceptable even on slower setups.
  async function _suggestionsFileHasCached(filename) {
    try {
      const r = await fetch('/api/queue/suggestions');
      const d = await r.json();
      return (d.files || []).some(f => f.filename === filename);
    } catch {
      return false;
    }
  }

  // (Suggested matches / brain panel removed — the per-file
  // suggestion run was rarely informative and the bottom-right
  // quadrant now hosts a wider search-results pane instead.)

  //: Footer flame button — toggles an inline vice dropdown + "File"
  //: button next to it. No second modal: the user picks a vice from
  //: the dropdown, hits File, and the row gets routed through the
  //: existing `/api/file/manual/vice` endpoint using whatever they've
  //: already typed in the Scene Search form (title/date/performer)
  //: plus the picked thumbnail. Click the flame a second time to hide
  //: the dropdown without filing.
  let _qViceInlineLoaded = false;
  async function toggleViceInline() {
    const sel = document.getElementById('qViceSelectInline');
    const fileBtn = document.getElementById('qViceFileBtn');
    if (!sel || !fileBtn) return;
    const isOpen = sel.style.display !== 'none';
    if (isOpen) {
      sel.style.display = 'none';
      fileBtn.style.display = 'none';
      return;
    }
    sel.style.display = 'inline-block';
    fileBtn.style.display = 'inline-flex';
    //: Lazy-load the vice list on first reveal. Auto-select whichever
    //: vice's slug matches the filename (same heuristic the old vice
    //: popup used) so the user's typical case is one-click.
    if (!_qViceInlineLoaded) {
      try {
        const r = await fetch('/api/vices', { credentials: 'same-origin' });
        const d = await r.json();
        const list = Array.isArray(d.vices) ? d.vices : [];
        if (!list.length) {
          //: Don't latch the cache on an empty list — could be a
          //: transient empty response from a misconfigured backend.
          //: Leave `_qViceInlineLoaded` false so the next flame-toggle
          //: retries the fetch instead of permanently showing "no
          //: vices configured" until page reload.
          sel.innerHTML = '<option value="">— No vices configured —</option>';
        } else {
          sel.innerHTML = '<option value="">Pick a Vice…</option>'
            + list.map(v => `<option value="${esc(v.name)}">${esc(v.name)}</option>`).join('');
          if (_qSelectedFile && typeof _firstMatchingVice === 'function') {
            const hit = _firstMatchingVice(_qSelectedFile, list);
            if (hit) sel.value = hit;
          }
          _qViceInlineLoaded = true;
        }
      } catch (_) {
        sel.innerHTML = '<option value="">— Error loading vices —</option>';
      }
    }
  }
  window.toggleViceInline = toggleViceInline;

  async function submitViceFromSearch() {
    const fn = _qSelectedFile;
    if (!fn) return;
    const sel = document.getElementById('qViceSelectInline');
    const vice_name = (sel && sel.value || '').trim();
    if (!vice_name) {
      if (sel) { sel.classList.add('field-invalid'); sel.focus(); }
      window.toast && window.toast('Pick a Vice from the dropdown', { kind: 'error' });
      return;
    }
    if (sel) sel.classList.remove('field-invalid');
    //: Collect from the in-flight Scene Search form. Title falls back
    //: to the parsed filename stem so a quick "just file under Vice"
    //: doesn't force the user to type a title.
    const titleEl = document.getElementById('srchTitle');
    const dateEl  = document.getElementById('srchDate');
    const perfEl  = document.getElementById('srchPerformer');
    let title = (titleEl && titleEl.value || '').trim();
    const date  = (dateEl  && dateEl.value  || '').trim();
    const performers = (perfEl && perfEl.value || '').trim();
    //: Default title to the filename stem when blank — the vice
    //: backend rejects empty titles, and re-typing the stem the user
    //: can already see in the header is busywork.
    if (!title) title = fn.replace(/\.[^.]+$/, '');
    //: Date stays empty if the user didn't fill it — silently writing
    //: today's date into NFO metadata would mislead Stash / Kodi /
    //: anything downstream. Bail with a focused field instead so the
    //: user knows to either set a real date or clear the field.
    if (!date) {
      if (dateEl) { dateEl.classList.add('field-invalid'); dateEl.focus(); }
      window.toast && window.toast('Date is required for vice filing (YYYY-MM-DD)', { kind: 'error' });
      return;
    }
    //: Thumbnail: prefer the user-picked frame (base64 data URL set
    //: by generateThumb), then fall back to the pasted image URL,
    //: then the currently-shown <img src>.
    const imageUrl = (document.getElementById('mImageUrl') || {}).value || '';
    const payload = {
      filename: fn,
      vice_name, title, date, performers,
      image_url: imageUrl.trim(),
    };
    if (typeof mThumbDataUrl !== 'undefined' && mThumbDataUrl) {
      payload.thumb_data_url = mThumbDataUrl;
      payload.image_url = '';
    }
    const btn = document.getElementById('qViceFileBtn');
    const orig = btn ? btn.innerHTML : '';
    if (btn) { btn.disabled = true; btn.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Loading"></span> <span class="btn-label">Filing…</span>'; }
    try {
      const r = await fetch('/api/file/manual/vice', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      let d = {};
      try { d = await r.json(); } catch(_) {}
      if (!r.ok || d.error) {
        window.toast && window.toast(`Error: ${d.error || (r.status + ' ' + r.statusText)}`, { kind: 'error' });
        if (btn) { btn.disabled = false; btn.innerHTML = orig; }
        return;
      }
      if (typeof clearSearch === 'function') clearSearch();
      const pos = Number(d && d.queued_position || 0);
      const msg = d && d.queued
        ? `Pipeline busy — queued for Vice ${vice_name}${pos > 1 ? ` (position ${pos})` : ''}`
        : `Filing to ${vice_name}…`;
      window.toast && window.toast(msg, { kind: 'success' });
      setTimeout(() => {
        if (typeof loadQueue === 'function') loadQueue({ preserveView: true, force: true });
        if (typeof loadQueueStats === 'function') loadQueueStats();
      }, 600);
    } catch (e) {
      window.toast && window.toast(`File to Vice failed: ${e && (e.message || e)}`, { kind: 'error' });
      if (btn) { btn.disabled = false; btn.innerHTML = orig; }
    }
  }
  window.submitViceFromSearch = submitViceFromSearch;
  // Document-level toast for cases where the suggestions modal isn't
  // open (brain-icon failure paths). Same look as `_suggestionsToast`
  // but pinned to the bottom of the viewport instead of the modal.
  // Same action affordance signature: pass `{label, callback}` to add
  // a right-aligned recovery button.
  function _toastBelow(message, action) {
    let stack = document.getElementById('queueGlobalToastStack');
    if (!stack) {
      stack = document.createElement('div');
      stack.id = 'queueGlobalToastStack';
      stack.style.cssText = 'position:fixed;left:50%;bottom:24px;transform:translateX(-50%);display:flex;flex-direction:column;gap:8px;z-index:9999;pointer-events:none';
      document.body.appendChild(stack);
    }
    const toast = document.createElement('div');
    toast.style.cssText = 'background:rgba(40,30,18,0.94);color:#fff;border:1px solid rgba(var(--brand-accent-rgb),0.35);border-radius:8px;padding:10px 16px;font-family:var(--mono);font-size:12px;line-height:1.45;letter-spacing:0.04em;max-width:min(540px, 90vw);word-break:break-word;overflow-wrap:anywhere;white-space:normal;box-shadow:0 8px 24px rgba(0,0,0,0.5);backdrop-filter:blur(8px);-webkit-backdrop-filter:blur(8px);pointer-events:auto;display:flex;align-items:center;gap:14px';
    const msg = document.createElement('span');
    msg.style.cssText = 'flex:1;min-width:0';
    msg.textContent = message;
    toast.appendChild(msg);
    if (action && typeof action.callback === 'function') {
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.style.cssText = 'flex-shrink:0;background:rgba(var(--brand-accent-rgb),0.18);border:1px solid rgba(var(--brand-accent-rgb),0.55);color:var(--text);border-radius:6px;padding:5px 12px;font-family:var(--secs);font-size:11px;font-weight:600;letter-spacing:0.06em;text-transform:uppercase;cursor:pointer;transition:background 0.15s, border-color 0.15s';
      btn.textContent = action.label || 'Retry';
      btn.onmouseover = () => { btn.style.background = 'rgba(var(--brand-accent-rgb),0.30)'; btn.style.borderColor = 'rgba(var(--brand-accent-rgb),0.85)'; };
      btn.onmouseout = () => { btn.style.background = 'rgba(var(--brand-accent-rgb),0.18)'; btn.style.borderColor = 'rgba(var(--brand-accent-rgb),0.55)'; };
      btn.onclick = (e) => { e.stopPropagation(); try { action.callback(); } finally { toast.remove(); } };
      toast.appendChild(btn);
    } else {
      toast.style.cursor = 'pointer';
      toast.onclick = () => toast.remove();
    }
    stack.appendChild(toast);
    setTimeout(() => toast.remove(), action ? 8000 : 5000);
  }
  window.runRowSuggestions = runRowSuggestions;
  // Event delegation for suggestion-card Accept / Reject buttons.
  // Replaces inline `onclick="acceptSuggestion('${...}',...)"` so any
  // weird character in the filename or source_id (apostrophes,
  // ampersands, parentheses) can't break the attribute. Reads the
  // identifiers off the data attributes on `.suggestion-file` and
  // `.suggestion-card` instead.
  document.addEventListener('click', function (e) {
    const btn = e.target && e.target.closest && e.target.closest(
      'button.sg-accept-btn, button.sg-reject-btn, button.iafd-scene-pick, button.tpdb-scene-pick'
    );
    if (!btn) return;
    const card = btn.closest('.suggestion-card');
    const fileEl = btn.closest('.suggestion-file');
    if (!card || !fileEl) {
      console.error('[suggestions] click missing card/file ancestor', { card, fileEl });
      return;
    }
    const filename = fileEl.getAttribute('data-filename');
    const source_db = card.getAttribute('data-source-db');
    const source_id = card.getAttribute('data-source-id');
    if (!filename || !source_db || !source_id) {
      console.error('[suggestions] missing identifiers', { filename, source_db, source_id });
      return;
    }
    if (btn.classList.contains('sg-accept-btn')) {
      acceptSuggestion(filename, source_db, source_id, btn);
    } else if (btn.classList.contains('sg-reject-btn')) {
      rejectSuggestion(filename, source_db, source_id, btn);
    } else if (btn.classList.contains('iafd-scene-pick')) {
      // Read the scene's label + cast off the data attributes the
      // picker stamped at render time. Replaces the inline-onclick
      // path that broke on filenames with apostrophes / ampersands.
      let cast = [];
      try { cast = JSON.parse(btn.getAttribute('data-scene-cast') || '[]'); } catch (_) {}
      const label = btn.getAttribute('data-scene-label') || '';
      pickIafdScene(filename, source_db, source_id, label, cast, btn);
    } else if (btn.classList.contains('tpdb-scene-pick')) {
      let cast = [];
      try { cast = JSON.parse(btn.getAttribute('data-scene-cast') || '[]'); } catch (_) {}
      const label = btn.getAttribute('data-scene-label') || '';
      const sceneId = btn.getAttribute('data-scene-id') || '';
      pickTpdbScene(filename, source_db, source_id, sceneId, label, cast, btn);
    }
  });
  // Clear the "last accept failed" banner on a row — used by the
  // banner's dismiss button and called automatically when the user
  // navigates to a different file.
  window._dismissAcceptError = function (filename) {
    const f = (_suggestionsFiles || []).find(x => x.filename === filename);
    if (f) {
      delete f._acceptError;
      _renderSuggestionsCurrent();
    }
  };
  window.openSuggestionsModal = openSuggestionsModal;
  window.closeSuggestionsModal = closeSuggestionsModal;
  window.acceptSuggestion = acceptSuggestion;
  window.rejectSuggestion = rejectSuggestion;
  window.rescanSuggestionFile = rescanSuggestionFile;
  window.runSuggestionScan = runSuggestionScan;


/* Block 3/3: modal overlay relocation IIFE. */

// (Version-stamp duplicate fetch removed; pollQueueStatus writes
// #appVersion on its first response — see _appVersionWritten guard.)
// Relocate the manual-search overlay to <body> so it escapes any
// ancestor that creates a containing block for fixed-position
// elements. The overlay is currently nested inside
// `.prowlarr-search-panel`, which uses `backdrop-filter: blur()` —
// per CSS spec that establishes a containing block for descendants
// with `position: fixed`, so the overlay anchors to the panel
// instead of the viewport and lands halfway down the page.
(function relocateModalOverlays() {
  //: ``qIafdScenePickerOverlay`` is the per-row Scene Search IAFD
  //: picker (opens when a multi-scene IAFD movie is selected). It was
  //: missing from this list — so it stayed nested inside
  //: ``.prowlarr-search-panel``, whose ``backdrop-filter`` per spec
  //: creates a containing block for ``position:fixed`` descendants,
  //: which anchors the overlay to the panel box and traps its
  //: z-index inside that panel's stacking context. Net effect: the
  //: picker opens but is invisible, and the form-populate path is
  //: skipped because useThisScene returns at the picker-open branch.
  const overlays = ['qSearchOverlay', 'qMovieSearchOverlay', 'qIafdPickerModal',
                    'qIafdResultsModal', 'qIafdScanModal',
                    'qIafdScenePickerOverlay',
                    'viceModal', 'suggestionsOverlay', 'queueDupOverlay',
                    'queueImageOverlay'];
  overlays.forEach(id => {
    const el = document.getElementById(id);
    if (el && el.parentElement !== document.body) {
      document.body.appendChild(el);
    }
  });
})();
