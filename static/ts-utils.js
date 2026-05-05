// HTML escaping
function esc(str) {
  return String(str || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

/**
 * Build a small inline gender badge for a performer name. Returns an
 * empty string when the gender is unknown so callers can concatenate
 * unconditionally without worrying about stray spaces. The TPDB /
 * StashDB raw values come through unchanged; this helper normalises
 * them and maps to the three-colour scheme: men blue, women (incl.
 * trans-women) pink, everyone else (intersex / non-binary / trans-men)
 * green. Display only — never used for headshot tiles where the photo
 * is the visual cue.
 */
function genderBadge(gender) {
  const g = String(gender || '').trim().toLowerCase();
  if (!g) return '';
  if (g === 'male' || g === 'm' || g === 'man') {
    return '<i class="fa-solid fa-mars g-badge g-male" title="Male" aria-label="Male"></i>';
  }
  if (g === 'female' || g === 'f' || g === 'woman'
      || g === 'transgender_female' || g === 'trans_female' || g === 'trans-female') {
    return '<i class="fa-solid fa-venus g-badge g-female" title="Female" aria-label="Female"></i>';
  }
  return '<i class="fa-solid fa-transgender g-badge g-other" title="' + esc(g.replace(/_/g, ' ')) + '" aria-label="Other"></i>';
}

/**
 * Async helper: scan `rootEl` for elements that already carry a
 * `data-perf-name` attribute, fetch their genders in a single batch
 * call, and append the matching `genderBadge(...)` glyph after each
 * name. Used on pages whose rows come from local-DB string columns
 * (history, library tables, queue, favourites) where the renderer
 * had only the performer name and no gender.
 *
 * Idempotent — elements with `data-perf-enriched` set are skipped on
 * subsequent calls. Names that resolve to an empty gender (no library
 * match) get no badge but are still flagged as enriched so we don't
 * keep retrying them.
 *
 * Cached in a module-level Map so navigating between rows that share
 * performers (e.g. a star with 50 scenes in the queue) only hits the
 * backend once per unique name.
 */
window._genderCache = window._genderCache || new Map();
async function enrichPerformerNames(rootEl) {
  if (!rootEl || typeof rootEl.querySelectorAll !== 'function') return;
  const els = rootEl.querySelectorAll('[data-perf-name]:not([data-perf-enriched])');
  if (!els.length) return;
  const need = new Set();
  els.forEach(el => {
    const nm = (el.getAttribute('data-perf-name') || '').trim();
    if (!nm) return;
    if (!window._genderCache.has(nm.toLowerCase())) need.add(nm);
  });
  if (need.size) {
    try {
      const r = await fetch('/api/performers/genders-by-name?names=' + encodeURIComponent([...need].join(',')));
      const d = await r.json();
      const map = (d && d.genders) || {};
      Object.keys(map).forEach(k => {
        window._genderCache.set(String(k).toLowerCase(), map[k] || '');
      });
      // Anything we asked about but didn't get back: cache as unknown
      // so we don't refetch repeatedly.
      need.forEach(n => {
        const k = n.toLowerCase();
        if (!window._genderCache.has(k)) window._genderCache.set(k, '');
      });
    } catch (_) {}
  }
  els.forEach(el => {
    const nm = (el.getAttribute('data-perf-name') || '').trim();
    if (!nm) { el.setAttribute('data-perf-enriched', '1'); return; }
    const g = window._genderCache.get(nm.toLowerCase()) || '';
    if (g) {
      // Append the badge as raw HTML so the glyph renders. The name
      // itself is already escaped by the upstream renderer.
      el.insertAdjacentHTML('beforeend', genderBadge(g));
    }
    el.setAttribute('data-perf-enriched', '1');
  });
}

/**
 * Wrap a comma-separated performer-name string in name-spans tagged
 * for `enrichPerformerNames()`. Each name becomes
 *   `<span data-perf-name="…">…</span>`
 * so a follow-up call to `enrichPerformerNames(rootEl)` can inject
 * gender badges asynchronously. Pure presentation — no fetches here.
 */
/**
 * Per-gender clickability gate. Direct one-to-one mapping between
 * content-filter settings and gender:
 *
 *   FEMALE → cat_solo_female
 *   MALE   → cat_gay (acts as "show male performer pages")
 *   TRANS  → cat_trans
 *
 * Settings are fetched once and cached on window. While the fetch is
 * in flight (and on hard failure) the helper returns `true` so the
 * UI never breaks; once the response lands, subsequent renders apply
 * the gate. Missing/unknown gender → always clickable.
 */
window.performerLinkAllowed = (function () {
  let cached = null;
  let pending = null;
  function load() {
    if (cached) return cached;
    if (!pending) {
      pending = fetch('/api/settings', { credentials: 'same-origin' })
        .then(r => r.json())
        .then(s => {
          const on = (v, dflt) => String(v ?? (dflt ? 'true' : 'false')).toLowerCase() === 'true';
          cached = {
            female: on(s.cat_solo_female, true),
            male:   on(s.cat_gay, true),
            trans:  on(s.cat_trans, true),
          };
          return cached;
        })
        .catch(() => {
          cached = { female: true, male: true, trans: true };
          return cached;
        });
    }
    return null;
  }
  load();
  return function (gender) {
    if (!gender) return true;
    if (!cached) { load(); return true; }
    const g = String(gender).toUpperCase();
    if (g === 'FEMALE' || g === 'F' || g === 'CIS_FEMALE' || g === 'WOMAN') return cached.female;
    if (g === 'MALE' || g === 'M' || g === 'CIS_MALE' || g === 'MAN') return cached.male;
    if (g.includes('TRANS') || g === 'NON_BINARY' || g === 'NONBINARY' || g === 'INTERSEX' || g === 'OTHER') return cached.trans;
    return true;
  };
})();

/**
 * Build the data-* attributes that turn a span into a popup link.
 * Returns an empty string (no attributes, no clickability) when the
 * caller's gender is excluded by the user's content filter — see
 * `performerLinkAllowed` above for the per-gender mapping.
 *
 * Optional opts:
 *   gender        — 'FEMALE'|'MALE'|'TRANS'|… (gates clickability)
 *   stashId       — StashDB / FansDB performer UUID
 *   libraryRowId  — favourite_entities.id when known
 *   tpdbId        — ThePornDB performer id (used by the popup
 *                   endpoint to scrape the right TPDB profile when
 *                   the click came from a TPDB-sourced scene)
 */
window.performerLinkAttrs = function (name, opts = {}) {
  if (!name) return '';
  if (opts.gender && !window.performerLinkAllowed(opts.gender)) return '';
  const parts = ['data-performer-link'];
  parts.push(`data-name="${esc(name)}"`);
  if (opts.stashId)      parts.push(`data-stash-id="${esc(opts.stashId)}"`);
  if (opts.libraryRowId) parts.push(`data-library-row-id="${esc(opts.libraryRowId)}"`);
  if (opts.tpdbId)       parts.push(`data-tpdb-id="${esc(opts.tpdbId)}"`);
  return parts.join(' ');
};

function performerCsvHtml(csv) {
  if (csv == null) return '';
  const s = String(csv).trim();
  if (!s) return '';
  // CSV has no per-name gender info — gate happens at object-level
  // renderers (renderPerformerList). CSV stays universally clickable.
  return s.split(',').map(n => n.trim()).filter(Boolean).map(n =>
    `<span data-perf-name="${esc(n)}" data-performer-link data-name="${esc(n)}" class="perf-name-link">${esc(n)}</span>`
  ).join(', ');
}

/**
 * Render a performer-name list with gender badges appended after each
 * name. Accepts either an array of `{name, gender}` objects (preferred,
 * via the backend's `display_performers` field) or an array of plain
 * strings (the legacy `hover_performers`/comma-split fallback, where
 * gender is unknown so no badge is drawn).
 */
function renderPerformerList(list, sep) {
  const _sep = sep == null ? ', ' : sep;
  if (!Array.isArray(list) || !list.length) return '';
  return list.map(function (p) {
    if (p && typeof p === 'object') {
      const name = String(p.name || '').trim();
      if (!name) return '';
      const attrs = window.performerLinkAttrs(name, {
        gender:        p.gender,
        stashId:       p.id || p.stash_id,
        libraryRowId:  p.row_id || p.library_row_id,
      });
      const cls = attrs ? ' class="perf-name-link"' : '';
      return `<span${attrs ? ' ' + attrs : ''}${cls}>${esc(name)}</span>` + genderBadge(p.gender);
    }
    const name = String(p || '').trim();
    if (!name) return '';
    return `<span data-performer-link data-name="${esc(name)}" class="perf-name-link">${esc(name)}</span>`;
  }).filter(Boolean).join(_sep);
}

/**
 * Format a counter value as a 6-character, right-aligned string for the
 * Skyfont display tiles used on /health and /downloads stats cards.
 *
 * Skyfont renders every glyph (digits, punctuation, U+0020) in a fixed
 * tile, so callers get the same 6-tile grid on every card regardless of
 * the counter's magnitude. Values wider than 6 characters pass through
 * untouched — a real count is more important than the layout's neatness.
 *
 * Null / undefined / empty → six blanks. Numbers are coerced through
 * String() so 0 renders as "     0" rather than "".
 */
function padSkyCounter(value) {
  if (value === null || value === undefined) return '      ';
  // Strip thousands separators AND internal whitespace — the Skyfont
  // tile grid is 6 fixed tiles, so commas/spaces inside a value steal
  // a slot (or overflow the grid entirely). "7.43 TB" → "7.43TB" fits
  // exactly; "12,345" → " 12345" pads as expected.
  const s = String(value).replace(/[,\s]/g, '');
  if (!s) return '      ';
  if (s.length >= 6) return s;
  return ' '.repeat(6 - s.length) + s;
}

/**
 * Auto-pad every `.sky-counter` element on the page so any code that
 * still writes `.textContent = fmtCount(...)` (without knowing about
 * Skyfont) still renders a stable 6-tile grid. A MutationObserver
 * watches the subtree; when the current text differs from
 * `padSkyCounter(current)`, we rewrite it. The guard `current !==
 * padded` prevents the observer from ping-ponging on its own writes
 * — once padded, the same text reflects back as already padded.
 *
 * Runs once at DOMContentLoaded. New `.sky-counter` elements added
 * later pick up padding via the document-wide subtree observer.
 */
(function initSkyCounterAutopad() {
  if (typeof document === 'undefined') return;

  /* Cache of the last successful fit per element so repeat calls with
   * unchanged parent-width + text are O(1). Without this, every time
   * a panel re-renders (e.g. perfApplyFilter updating tbody, or the
   * perfUpdateSummary batch touching 9 tiles back-to-back) we'd re-do
   * the 18-iter binary-search on each counter and layout-thrash the
   * page into a visible hang. */
  const fitState = new WeakMap();

  /* Pending-fit set + rAF scheduler. Rapid-fire fit triggers (a burst
   * of textContent writes from perfUpdateSummary, a panel opening
   * that fires ResizeObserver on every child card, etc.) coalesce
   * into a single fit per animation frame per element. */
  const pending = new Set();
  let rafHandle = 0;
  function flushPending() {
    rafHandle = 0;
    const batch = Array.from(pending);
    pending.clear();
    for (const el of batch) fitSkyCounterNow(el);
  }
  function scheduleFit(el) {
    if (!el) return;
    pending.add(el);
    if (!rafHandle) rafHandle = requestAnimationFrame(flushPending);
  }

  /**
   * Fit the 6-char strip to the parent tile's width via a single
   * linear-scale measurement. LEDBDREV is monospaced, so scrollWidth
   * scales linearly with font-size — measure once at a reference
   * size, compute the target from the ratio. This is critical for
   * performance: the previous binary-search approach forced 18
   * synchronous layouts per fit (write fontSize → read scrollWidth,
   * repeat), which compounded into multi-second hangs when a burst of
   * updates hit 9+ counters at once (e.g. perfUpdateSummary on
   * /health). One measurement + one write = 2 layouts per fit.
   */
  const REF_FONT_SIZE = 40;   // px probe value — arbitrary, cancels out
  //: Six-glyph probe string for worst-case width measurement. LEDBDREV
  //: renders the space character narrower than a digit (no tabular-nums
  //: guarantee for whitespace), so measuring the padded placeholder
  //: "     0" yields a tiny scrollWidth and blows the computed font-size
  //: up to the 160px clamp — visibly overflowing neighbouring tiles on
  //: first load. A string of six zeros measures the maximum width the
  //: tile ever needs to accommodate and keeps the fit stable regardless
  //: of current value.
  const FIT_PROBE = '000000';
  function fitSkyCounterNow(el) {
    const parent = el && el.parentElement;
    if (!parent) return;
    const limit = parent.clientWidth - 4;   // 4px safety margin
    if (limit <= 0) return;
    /* Belt-and-braces: re-pad to 6 chars before measuring. A race between
     * `setText(...)` writing an unpadded digit and the MutationObserver
     * microtask padding it can catch the ResizeObserver measuring a
     * single-glyph scrollWidth, which scales the font-size to 6× target
     * (and clamps to the 160px ceiling — the giant digit the user sees
     * on tab switch). Padding here keeps the baseline stable regardless
     * of which observer fires first. */
    const rawContent = el.textContent || '';
    const padded = padSkyCounter(rawContent);
    if (rawContent !== padded) el.textContent = padded;
    /* Cache hit: same parent width = nothing to re-measure. We key on
     * `limit` alone now — the probe-based fit is content-independent
     * (any 6-glyph value fits the same target size), so no need to
     * re-measure on every textContent change. */
    const cached = fitState.get(el);
    if (cached && cached.limit === limit) {
      if (cached.fontSize) el.style.fontSize = cached.fontSize;
      return;
    }
    /* Measure against the 6-zero probe rather than the live content:
     * the live content may contain spaces (narrow glyph in LEDBDREV)
     * which measure narrower than their digit-replacement counterparts.
     * Swap text temporarily, measure, restore. For values that
     * already exceed 6 chars (rare — `padSkyCounter` returns longer
     * strings as-is), fit to the real string so it doesn't overflow. */
    el.style.fontSize = REF_FONT_SIZE + 'px';
    const probe = padded.length > FIT_PROBE.length ? padded : FIT_PROBE;
    const restore = el.textContent;
    if (restore !== probe) el.textContent = probe;
    const refWidth = el.scrollWidth;
    if (restore !== probe) el.textContent = restore;
    if (!refWidth) return;
    // 0.98 leaves a sliver of sub-pixel headroom so kerning/rounding
    // quirks don't push the last glyph off the edge. Ceiling of 72px
    // matches (and slightly exceeds) the CSS fallback clamp of 48px —
    // tighter than the old 160px so that a miscomputed refWidth (e.g.
    // measured with the fallback font before LEDBDREV has loaded)
    // caps at a sane "slightly too small" size instead of 3x-oversize
    // digits bleeding across neighbouring stat tiles.
    const target = Math.max(10, Math.min(72, (limit / refWidth) * REF_FONT_SIZE * 0.98));
    const finalSize = target.toFixed(1) + 'px';
    el.style.fontSize = finalSize;
    fitState.set(el, { limit, fontSize: finalSize });
  }

  // Public entry point — always goes through the rAF scheduler.
  function fitSkyCounter(el) { scheduleFit(el); }

  function padIfSky(el) {
    if (!el || !el.classList || !el.classList.contains('sky-counter')) return;
    const cur = el.textContent || '';
    const padded = padSkyCounter(cur);
    if (cur !== padded) el.textContent = padded;
    scheduleFit(el);
  }

  function init() {
    // Per-element observation — a document-wide subtree observer would
    // fire on every unrelated innerHTML change (queue rows, activity
    // log, etc.) which wastes cycles on busy pages.
    const obs = new MutationObserver(muts => {
      for (const m of muts) {
        if (m.type === 'characterData') {
          padIfSky(m.target.parentElement);
        } else if (m.type === 'childList') {
          padIfSky(m.target);
        }
      }
    });
    // Refit on tile resize — window resize / responsive layout changes.
    // IMPORTANT: only refit when the WIDTH changed. The fit itself grows
    // the sky-counter's height (bigger font-size → taller strip) which
    // propagates to the parent card's height — without a width guard we
    // loop forever (fit → taller → RO fires → fit → …), which locks the
    // page.
    const lastWidth = new WeakMap();
    const ro = (typeof ResizeObserver !== 'undefined')
      ? new ResizeObserver(entries => {
          for (const e of entries) {
            const w = Math.round(e.contentRect.width);
            if (lastWidth.get(e.target) === w) continue;
            lastWidth.set(e.target, w);
            const child = e.target.querySelector('.sky-counter');
            if (child) scheduleFit(child);
          }
        })
      : null;
    document.querySelectorAll('.sky-counter').forEach(el => {
      padIfSky(el);
      obs.observe(el, { childList: true, characterData: true, subtree: true });
      if (ro && el.parentElement) ro.observe(el.parentElement);
    });
    // LED font load can change glyph metrics after first paint — refit
    // every counter (cache-busted) when fonts finish loading so the
    // display matches the real rendered width.
    if (document.fonts && document.fonts.ready) {
      document.fonts.ready.then(() => {
        document.querySelectorAll('.sky-counter').forEach(el => {
          fitState.delete(el);     // font metrics changed → bust cache
          scheduleFit(el);
        });
      });
    }
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();

// Consistent fetch wrapper with error handling
async function apiFetch(url, options = {}) {
  const res = await fetch(url, options);
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`${res.status} ${res.statusText}: ${text}`);
  }
  return res.json();
}

// Polling helper
function poll(fn, intervalMs, shouldStop) {
  const timer = setInterval(async () => {
    try {
      const result = await fn();
      if (shouldStop(result)) clearInterval(timer);
    } catch (e) {
      console.error('poll error', e);
    }
  }, intervalMs);
  return timer;
}

/**
 * Duotone mode toggle — per-page setting.
 *
 * Each of /library, /downloads and /scenes (Your Feed only; Spotlight is
 * always its own thing) remembers its own preference independently in
 * localStorage so the three pages don't stomp on each other.
 *
 *   data-duotone-inverted="0" (default)  — duotone appears in whatever state
 *                                          the page's base CSS specifies.
 *   data-duotone-inverted="1"            — each page's CSS flips the duotone
 *                                          so the rest / hover states swap.
 *
 * Storage keys:
 *   duotone_inverted_library     — /library palette toggle
 *   duotone_inverted_downloads   — /downloads palette toggle
 *   duotone_inverted_scenes      — /scenes "Your feed" palette toggle
 */
function _duotonePageKey() {
  var p = '';
  try { p = (window.location && window.location.pathname) || ''; } catch (e) { p = ''; }
  if (p.indexOf('/library') === 0) return 'duotone_inverted_library';
  if (p.indexOf('/downloads') === 0) return 'duotone_inverted_downloads';
  if (p.indexOf('/scenes') === 0) return 'duotone_inverted_scenes';
  // Any other page: leave the attribute cleared and skip persistence.
  return null;
}
function _getDuotoneInverted() {
  var key = _duotonePageKey();
  if (!key) return false;
  try { return localStorage.getItem(key) === '1'; } catch (e) { return false; }
}
function _applyDuotoneAttr() {
  if (!_duotonePageKey()) {
    document.documentElement.removeAttribute('data-duotone-inverted');
    return;
  }
  var v = _getDuotoneInverted() ? '1' : '0';
  document.documentElement.setAttribute('data-duotone-inverted', v);
  // Sync any palette buttons on the page so their active-state pips match.
  document.querySelectorAll('[data-duotone-toggle]').forEach(function (btn) {
    btn.classList.toggle('is-active', v === '1');
    btn.setAttribute('aria-pressed', v === '1' ? 'true' : 'false');
  });
}
function initDuotoneMode() {
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _applyDuotoneAttr);
  } else {
    _applyDuotoneAttr();
  }
}
function toggleDuotoneMode() {
  var key = _duotonePageKey();
  if (!key) return;
  var next = !_getDuotoneInverted();
  try { localStorage.setItem(key, next ? '1' : '0'); } catch (e) {}
  _applyDuotoneAttr();
}
// Apply immediately so CSS `html[data-duotone-inverted]` rules match before
// first paint. Re-apply on DOMContentLoaded so any [data-duotone-toggle]
// buttons rendered later in the body get their active-state class synced.
(function () {
  try { _applyDuotoneAttr(); } catch (e) {}
  if (typeof document !== 'undefined' && document.addEventListener) {
    document.addEventListener('DOMContentLoaded', function () {
      try { _applyDuotoneAttr(); } catch (e) {}
    });
  }
})();

/**
 * Auto-levels + white-balance a poster image for duotone or clean display.
 *
 * Reads pixel data from a downsampled offscreen canvas, computes a percentile
 * histogram for black/white points, and writes CSS custom properties on the
 * element (--auto-b, --auto-c, --auto-h, --auto-b-hover, --auto-c-hover). The
 * host CSS is expected to reference these in its own `filter:` chain, e.g.:
 *
 *   .poster img {
 *     filter: brightness(var(--auto-b, 1.06))
 *             contrast(var(--auto-c, 1.12))
 *             hue-rotate(var(--auto-h, 0deg))
 *             grayscale(1) contrast(1.12);
 *   }
 *
 * Runs per-image so every tile gets correction tailored to its own source.
 *
 * CORS: canvas reads require the image to be served with an appropriate
 * Access-Control-Allow-Origin header AND the <img> to carry
 * crossorigin="anonymous". If the canvas becomes tainted, getImageData throws
 * and we silently fall back to the CSS fallback values in the var() calls.
 */
function autoLevelPosterImage(img) {
  if (!img || !img.naturalWidth) return;
  try {
    // Downsample to ~48px wide for cheap sampling (~1500 pixels).
    var targetW = 48;
    var w = Math.min(targetW, img.naturalWidth);
    var h = Math.max(1, Math.round(img.naturalHeight * (w / img.naturalWidth)));
    var canvas = document.createElement('canvas');
    canvas.width = w; canvas.height = h;
    var ctx = canvas.getContext('2d', { willReadFrequently: true });
    if (!ctx) return;
    ctx.drawImage(img, 0, 0, w, h);
    var data;
    try { data = ctx.getImageData(0, 0, w, h).data; }
    catch (e) { return; } // tainted canvas — CORS fail
    var count = 0, rSum = 0, gSum = 0, bSum = 0;
    // Luminance histogram in 32 buckets for percentile picking.
    var BUCKETS = 32;
    var hist = new Uint32Array(BUCKETS);
    for (var i = 0; i < data.length; i += 4) {
      var r = data[i], g = data[i+1], b = data[i+2];
      rSum += r; gSum += g; bSum += b; count++;
      var lum = 0.299 * r + 0.587 * g + 0.114 * b;
      var bucket = Math.min(BUCKETS - 1, (lum * BUCKETS / 256) | 0);
      hist[bucket]++;
    }
    if (count === 0) return;
    // Percentile black/white points so blown highlights / crushed shadows
    // don't skew the whole correction.
    var pLow = count * 0.02;
    var pHigh = count * 0.98;
    var running = 0, blackBucket = 0, whiteBucket = BUCKETS - 1;
    for (var b1 = 0; b1 < BUCKETS; b1++) {
      running += hist[b1];
      if (running >= pLow) { blackBucket = b1; break; }
    }
    running = 0;
    for (var b2 = BUCKETS - 1; b2 >= 0; b2--) {
      running += hist[b2];
      if (running >= count - pHigh) { whiteBucket = b2; break; }
    }
    var blackL = (blackBucket * 256) / BUCKETS;
    var whiteL = ((whiteBucket + 1) * 256) / BUCKETS;
    var range = Math.max(32, whiteL - blackL); // guard flat images

    // Approximate linear stretch with CSS contrast()+brightness().
    //   CSS output = ((px * m) - 128) * k + 128  where k=contrast, m=brightness
    //   Target: black→0, white→255
    var k = 255 / range;
    var m = (128 - (255 * blackL) / range + 128 * (k - 1)) / (128 * k);
    var contrastVal = Math.max(0.85, Math.min(1.45, k));
    var brightVal   = Math.max(0.85, Math.min(1.25, m));

    // White balance from per-channel means vs luminance mean.
    var rMean = rSum / count, gMean = gSum / count, bMean = bSum / count;
    var lumMean = 0.299 * rMean + 0.587 * gMean + 0.114 * bMean || 1;
    var warmBias = (rMean - bMean) / lumMean;   // + warm, − cool
    var hueShift = Math.max(-12, Math.min(12, -warmBias * 10));

    img.style.setProperty('--auto-b',       brightVal.toFixed(3));
    img.style.setProperty('--auto-c',       contrastVal.toFixed(3));
    img.style.setProperty('--auto-h',       hueShift.toFixed(1) + 'deg');
    img.style.setProperty('--auto-b-hover', (brightVal * 0.98).toFixed(3));
    img.style.setProperty('--auto-c-hover', (contrastVal * 0.95).toFixed(3));
  } catch (e) { /* leave CSS defaults in place */ }
}


/* Spotlight tiles render the photo as a CSS background-image on a
 * .spotlight-tile-art wrapper so an airbrush ::after layer + tritone SVG
 * filter can be applied to the same surface. The <img> element is kept
 * in the DOM for autolevel sampling but rendered invisible (opacity:0).
 * After autolevel reads pixel data from the img, the resulting custom
 * properties have to be MIRRORED up to the wrapper so the wrapper's
 * filter chain can pick them up — custom properties don't bubble up
 * the tree, only down. */
function autoLevelSpotlightTile(img) {
  if (!img) return;
  autoLevelPosterImage(img);
  var art = img.closest && img.closest('.spotlight-tile-art');
  if (!art) return;
  ['--auto-b', '--auto-c', '--auto-h', '--auto-b-hover', '--auto-c-hover']
    .forEach(function (prop) {
      var v = img.style.getPropertyValue(prop);
      if (v) art.style.setProperty(prop, v);
    });
}


// ── Auto-lock on session expiry ──────────────────────────
// The session is a sliding idle window — once it times out, every
// subsequent authenticated fetch returns 401 and the page stops
// working. Previously the user had to refresh manually to be shown
// the login form; this interceptor catches the first 401 and sends
// the browser to /login with a return-URL so the page reloads on
// its own the moment the session goes cold.
//
// Guards:
//  • Already on /login — never redirect (prevents bounce loops when
//    the login form itself returns 401 on bad password).
//  • Single-shot — `_tsAuthRedirecting` flag blocks duplicate nav
//    when multiple concurrent fetches all return 401 at once.
//  • Same-origin only — cross-origin 401s (e.g. an external CDN we
//    fetched an image through) aren't auth errors for this app.
(function () {
  if (!window.fetch || window._tsAuthWrapped) return;
  window._tsAuthWrapped = true;
  const _origFetch = window.fetch.bind(window);
  window._tsAuthRedirecting = false;

  function _sameOrigin(url) {
    try {
      if (!url) return true;
      if (typeof url === 'string') {
        if (url.startsWith('/')) return true;
        const u = new URL(url, window.location.href);
        return u.origin === window.location.origin;
      }
      if (url.url) return _sameOrigin(url.url);
    } catch (_) {}
    return false;
  }

  window.fetch = function (resource, init) {
    return _origFetch(resource, init).then(function (res) {
      if (
        res.status === 401 &&
        !window._tsAuthRedirecting &&
        !window.location.pathname.startsWith('/login') &&
        _sameOrigin(resource)
      ) {
        window._tsAuthRedirecting = true;
        // login.html reads `?next=<url>` and hops there after a
        // successful login — match that param name so the user lands
        // back on the page they were using.
        const nextUrl = encodeURIComponent(window.location.pathname + window.location.search + window.location.hash);
        // Let any in-flight .then() handlers finish before we nuke
        // the document — a 0ms timeout yields to the microtask queue.
        setTimeout(function () { window.location.replace('/login?next=' + nextUrl); }, 0);
      }
      return res;
    });
  };
})();

// ── Country → ISO alpha-2 (lowercase) lookup for the round flag chip
//    rendered next to performer names across the app (/discover detail
//    panel, /library bio popup). Covers ISO codes (passed through),
//    canonical country names, and common demonyms / aliases that
//    StashDB and TPDB sometimes return instead of the proper noun.
//    Anything unmatched falls through silently — no flag is rendered
//    rather than a broken image. Exposed on `window` so any page that
//    loads ts-utils.js can use it. ──
window.COUNTRY_FLAG_LOOKUP = (function () {
  const M = {};
  const add = (code, name, ...aliases) => {
    const c = code.toLowerCase();
    M[c] = { code: c, name };
    M[name.toLowerCase()] = { code: c, name };
    for (const a of aliases) M[a.toLowerCase()] = { code: c, name };
  };
  add('us', 'United States', 'usa', 'u.s.', 'u.s.a.', 'united states of america', 'american', 'america');
  add('gb', 'United Kingdom', 'uk', 'u.k.', 'great britain', 'england', 'britain', 'scotland', 'wales', 'northern ireland', 'british', 'english', 'scottish', 'welsh');
  add('ca', 'Canada', 'canadian');
  add('au', 'Australia', 'aus', 'australian');
  add('de', 'Germany', 'deutschland', 'german');
  add('cz', 'Czech Republic', 'czechia', 'czech', 'czech rep', 'czech rep.');
  add('fr', 'France', 'french');
  add('es', 'Spain', 'spanish', 'españa');
  add('br', 'Brazil', 'brasil', 'brazilian');
  add('jp', 'Japan', 'japanese');
  add('kr', 'South Korea', 'korea', 'korean', 'republic of korea');
  add('hu', 'Hungary', 'hungarian');
  add('ro', 'Romania', 'romanian');
  add('pl', 'Poland', 'polish');
  add('nl', 'Netherlands', 'the netherlands', 'holland', 'dutch');
  add('it', 'Italy', 'italian');
  add('ru', 'Russia', 'russian federation', 'russian');
  add('ua', 'Ukraine', 'ukrainian');
  add('ie', 'Ireland', 'irish');
  add('mx', 'Mexico', 'mexican');
  add('ar', 'Argentina', 'argentinian', 'argentine');
  add('co', 'Colombia', 'colombian');
  add('cl', 'Chile', 'chilean');
  add('ve', 'Venezuela', 'venezuelan');
  add('cu', 'Cuba', 'cuban');
  add('pe', 'Peru', 'peruvian');
  add('do', 'Dominican Republic', 'dominican');
  add('pr', 'Puerto Rico', 'puerto rican');
  add('za', 'South Africa', 'south african');
  add('ng', 'Nigeria', 'nigerian');
  add('ke', 'Kenya', 'kenyan');
  add('eg', 'Egypt', 'egyptian');
  add('ma', 'Morocco', 'moroccan');
  add('tr', 'Turkey', 'turkish', 'türkiye');
  add('gr', 'Greece', 'greek');
  add('pt', 'Portugal', 'portuguese');
  add('be', 'Belgium', 'belgian');
  add('ch', 'Switzerland', 'swiss');
  add('at', 'Austria', 'austrian');
  add('se', 'Sweden', 'swedish');
  add('no', 'Norway', 'norwegian');
  add('dk', 'Denmark', 'danish');
  add('fi', 'Finland', 'finnish');
  add('is', 'Iceland', 'icelandic');
  add('ee', 'Estonia', 'estonian');
  add('lv', 'Latvia', 'latvian');
  add('lt', 'Lithuania', 'lithuanian');
  add('sk', 'Slovakia', 'slovak', 'slovakian');
  add('si', 'Slovenia', 'slovenian', 'slovene');
  add('hr', 'Croatia', 'croatian');
  add('rs', 'Serbia', 'serbian');
  add('ba', 'Bosnia and Herzegovina', 'bosnia', 'bosnian');
  add('bg', 'Bulgaria', 'bulgarian');
  add('mk', 'North Macedonia', 'macedonia', 'macedonian');
  add('al', 'Albania', 'albanian');
  add('md', 'Moldova', 'moldovan');
  add('by', 'Belarus', 'belarusian');
  add('cn', 'China', 'chinese');
  add('tw', 'Taiwan', 'taiwanese');
  add('hk', 'Hong Kong');
  add('th', 'Thailand', 'thai');
  add('vn', 'Vietnam', 'vietnamese');
  add('ph', 'Philippines', 'filipino', 'filipina');
  add('id', 'Indonesia', 'indonesian');
  add('my', 'Malaysia', 'malaysian');
  add('sg', 'Singapore', 'singaporean');
  add('in', 'India', 'indian');
  add('pk', 'Pakistan', 'pakistani');
  add('bd', 'Bangladesh', 'bangladeshi');
  add('lk', 'Sri Lanka', 'sri lankan');
  add('np', 'Nepal', 'nepali', 'nepalese');
  add('il', 'Israel', 'israeli');
  add('lb', 'Lebanon', 'lebanese');
  add('sy', 'Syria', 'syrian');
  add('jo', 'Jordan', 'jordanian');
  add('ae', 'United Arab Emirates', 'uae', 'emirati');
  add('sa', 'Saudi Arabia', 'saudi', 'saudi arabian');
  add('ir', 'Iran', 'iranian', 'persian');
  add('iq', 'Iraq', 'iraqi');
  add('nz', 'New Zealand', 'kiwi', 'new zealander');
  add('lu', 'Luxembourg');
  add('mt', 'Malta', 'maltese');
  add('cy', 'Cyprus', 'cypriot');
  add('ge', 'Georgia', 'georgian');
  add('am', 'Armenia', 'armenian');
  add('az', 'Azerbaijan', 'azerbaijani');
  add('kz', 'Kazakhstan', 'kazakh');
  add('uz', 'Uzbekistan', 'uzbek');
  return M;
})();

window.countryFlagHtml = function (raw, extraClass) {
  if (!raw) return '';
  const key = String(raw).trim().toLowerCase();
  if (!key) return '';
  const cls = 'detail-flag' + (extraClass ? ' ' + extraClass : '');
  const m = window.COUNTRY_FLAG_LOOKUP[key];
  if (m) {
    return `<img class="${cls}" src="/static/flags/${m.code}.svg" alt="${m.name}" title="${m.name}" onerror="this.remove()">`;
  }
  if (key.length === 2 && /^[a-z]{2}$/.test(key)) {
    return `<img class="${cls}" src="/static/flags/${key}.svg" alt="${key.toUpperCase()}" title="${key.toUpperCase()}" onerror="this.remove()">`;
  }
  return '';
};

/* ─────────────────────────────────────────────────────────────────
 * Shared Prowlarr-search popup
 * ─────────────────────────────────────────────────────────────────
 * Single in-browser popup that any page can summon to run a Prowlarr
 * search for a known title (scene or movie) and grab a release. The
 * popup auto-injects its own DOM on first use so callers don't need
 * to drop overlay markup into every page template.
 *
 * Usage:
 *   window.openProwlarrSearchPopup({
 *     title: 'Tiny Tit Teens 13',
 *     kind:  'movie' | 'scene',          // optional, defaults to scene
 *     studio:    'Lethal Hardcore',      // optional, used for fallback query
 *     performers: 'Jane Doe, Mary Sue',  // optional, used for fallback query
 *     thumb_url: '/static/img/x.jpg',    // optional
 *   })
 */
(function () {
  if (window.openProwlarrSearchPopup) return; // already wired by another include

  const _esc = (s) => String(s ?? '')
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');

  let _searchToken = 0;
  let _activeReleases = [];
  let _activeKind = 'scene';

  function ensureMarkup() {
    if (document.getElementById('tsProwlarrPopup')) return;
    const div = document.createElement('div');
    div.id = 'tsProwlarrPopup';
    div.className = 'ts-prowlarr-popup-overlay';
    div.innerHTML = `
      <div class="ts-prowlarr-popup-shell" role="dialog" aria-modal="true" aria-label="Prowlarr search">
        <button type="button" class="ts-prowlarr-popup-close" aria-label="Close" title="Close">&times;</button>
        <div class="ts-prowlarr-popup-header" id="tsProwlarrPopupHeader"></div>
        <div class="ts-prowlarr-popup-status" id="tsProwlarrPopupStatus"></div>
        <div class="ts-prowlarr-popup-results" id="tsProwlarrPopupResults"></div>
      </div>`;
    document.body.appendChild(div);
    div.addEventListener('click', (ev) => { if (ev.target === div) closePopup(); });
    div.querySelector('.ts-prowlarr-popup-close').addEventListener('click', closePopup);
    document.addEventListener('keydown', (ev) => {
      if (ev.key === 'Escape' && div.classList.contains('open')) closePopup();
    });
    div.querySelector('.ts-prowlarr-popup-results').addEventListener('click', onGrabClick);
  }

  function closePopup() {
    const o = document.getElementById('tsProwlarrPopup');
    if (o) o.classList.remove('open');
    _searchToken++; // invalidate any in-flight search
  }
  window.closeProwlarrSearchPopup = closePopup;

  async function onGrabClick(ev) {
    const btn = ev.target.closest('.ts-prowlarr-grab');
    if (!btn) return;
    ev.preventDefault();
    ev.stopPropagation();
    const idx = parseInt(btn.dataset.releaseIdx, 10);
    const result = _activeReleases[idx];
    if (!result) return;
    btn.disabled = true;
    btn.classList.remove('is-sent');
    btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>';
    try {
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
          kind: _activeKind === 'movie' ? 'movie' : 'scene',
        }),
      });
      const d = await r.json();
      if (d && d.ok) {
        btn.classList.add('is-sent');
        btn.innerHTML = '<i class="fa-solid fa-check"></i>';
      } else {
        btn.disabled = false;
        btn.innerHTML = '<i class="fa-solid fa-download"></i>';
        alert((d && d.error) || 'Could not send to download client');
      }
    } catch (e) {
      btn.disabled = false;
      btn.innerHTML = '<i class="fa-solid fa-download"></i>';
      alert(e.message || 'Failed');
    }
  }

  window.openProwlarrSearchPopup = async function (opts) {
    ensureMarkup();
    const o = opts || {};
    const title = (o.title || '').trim();
    if (!title) return;
    _activeKind = (o.kind === 'movie') ? 'movie' : 'scene';
    const overlay = document.getElementById('tsProwlarrPopup');
    const headerEl = document.getElementById('tsProwlarrPopupHeader');
    const statusEl = document.getElementById('tsProwlarrPopupStatus');
    const resultsEl = document.getElementById('tsProwlarrPopupResults');
    headerEl.innerHTML = `<span class="ts-prowlarr-popup-title">PROWLARR: ${_esc(title)}</span>`;
    statusEl.textContent = 'Searching Prowlarr…';
    resultsEl.innerHTML = '<div class="ts-prowlarr-popup-empty">Please wait…</div>';
    overlay.classList.add('open');
    const myToken = ++_searchToken;
    try {
      const params = new URLSearchParams();
      params.append('q', title);
      const perfStr = (o.performers || '').trim();
      const studio = (o.studio || '').trim();
      if (perfStr && studio) {
        const studioQ = `${perfStr} ${studio}`.trim();
        if (studioQ.toLowerCase() !== title.toLowerCase()) params.append('q', studioQ);
      }
      const r = await fetch('/api/prowlarr/search?' + params.toString(), { credentials: 'same-origin' });
      const d = await r.json();
      if (myToken !== _searchToken) return;
      if (d && d.error) {
        statusEl.textContent = d.error;
        resultsEl.innerHTML = '';
        return;
      }
      const releases = (d && d.results) || [];
      _activeReleases = releases.slice(0, 50);
      statusEl.innerHTML = `<strong>${_activeReleases.length}</strong> release${_activeReleases.length === 1 ? '' : 's'} found`;
      if (!_activeReleases.length) {
        resultsEl.innerHTML = '<div class="ts-prowlarr-popup-empty">No releases matched this title on your configured indexers.</div>';
        return;
      }
      // Parse a release title for resolution / quality / image-pack
      // markers and return ordered pills (highest-fidelity first).
      const QUALITY_PATTERNS = [
        { rx: /\b(2160p|4k|uhd)\b/i,                cls: 'ts-q-4k',     label: '4K'    },
        { rx: /\b1080p\b/i,                         cls: 'ts-q-1080',   label: '1080P' },
        { rx: /\b720p\b/i,                          cls: 'ts-q-720',    label: '720P'  },
        { rx: /\b(480p|sd|dvdrip|xvid)\b/i,         cls: 'ts-q-sd',     label: 'SD'    },
        { rx: /\b(imageset|image[-_ ]?set|imgset|jpg|pics?|photos?)\b/i, cls: 'ts-q-img', label: 'IMG' },
      ];
      const buildQualityPills = (title) => {
        const t = title || '';
        const seen = new Set();
        const pills = [];
        for (const p of QUALITY_PATTERNS) {
          if (seen.has(p.cls)) continue;
          if (p.rx.test(t)) {
            seen.add(p.cls);
            pills.push(`<span class="ts-prowlarr-q ${p.cls}">${p.label}</span>`);
          }
        }
        return pills.join('');
      };
      // Render rows in the same shape as `/downloads` Search-tab results
      // (studio logo · name+meta · headshots · indexer/type · grab).
      resultsEl.innerHTML = _activeReleases.map((rel, i) => {
        const isTor = rel.type === 'torrent';
        const agePart = rel.age != null ? Math.round(rel.age / 24) + 'd' : '';
        const seedPart = isTor && rel.seeders != null ? rel.seeders + ' seed' : '';
        const sizeMb = rel.size_mb || (rel.size ? rel.size / 1024 / 1024 : 0);
        const sizeLabel = sizeMb >= 1024
          ? (sizeMb / 1024).toFixed(2) + ' GB'
          : Math.round(sizeMb) + ' MB';
        const meta = [agePart, seedPart, sizeLabel].filter(Boolean).join(' · ');
        const indexer = (rel.indexer || rel.tracker || '').toString();
        const typeLabel = isTor ? 'Torrent' : 'NZB';
        const typeLogo = `<img class="ts-prowlarr-typelogo" src="/static/logos/${isTor ? 'torrent' : 'nzb'}.png" alt="${typeLabel}" title="${typeLabel}" onerror="this.style.display='none'">`;
        const m = rel.match || {};
        const studio = (m.studio || '').trim();
        const studioLogo = studio
          ? `<img class="ts-prowlarr-studiologo" src="/api/studio-logo?name=${encodeURIComponent(studio)}&q=${encodeURIComponent(rel.title || '')}" alt="${_esc(studio)}" title="${_esc(studio)}" onerror="this.style.display='none'">`
          : '';
        const performers = (m.performers || []).filter(Boolean).slice(0, 3);
        const headshotSlot = performers.length
          ? `<span class="ts-prowlarr-headshots" data-perf-names="${_esc(performers.join('|'))}"></span>`
          : '';
        const qualityPills = buildQualityPills(rel.title || '');
        return `<div class="ts-prowlarr-row">
          <div class="ts-prowlarr-cell">${studioLogo}</div>
          <div class="ts-prowlarr-cell-name">
            <div class="ts-prowlarr-title" title="${_esc(rel.title || '')}">${_esc(rel.title || 'Untitled')}</div>
            <div class="ts-prowlarr-meta">${qualityPills}<span>${_esc(meta)}</span></div>
          </div>
          <div class="ts-prowlarr-cell ts-prowlarr-cell-headshots">${headshotSlot}</div>
          <div class="ts-prowlarr-cell ts-prowlarr-cell-badges">
            <span>${typeLogo}</span>
            <span class="ts-prowlarr-indexer" title="${_esc(indexer)}">${_esc(indexer)}</span>
          </div>
          <div class="ts-prowlarr-cell">
            <button type="button" class="ts-prowlarr-grab ${isTor ? 'is-torrent' : 'is-nzb'}" data-release-idx="${i}" title="Send to download client">
              <i class="fa-solid fa-download"></i>
            </button>
          </div>
        </div>`;
      }).join('');
      // Replace the per-perf placeholder loop with the proven batch
      // hydrator from /downloads — fetch once, paint up to 3 faces.
      const slots = Array.from(resultsEl.querySelectorAll('.ts-prowlarr-headshots[data-perf-names]'));
      if (slots.length) {
        const allNames = new Set();
        slots.forEach(slot => {
          (slot.dataset.perfNames || '').split('|').forEach(n => {
            const t = (n || '').trim();
            if (t) allNames.add(t);
          });
        });
        if (allNames.size) {
          fetch('/api/performers/headshots-by-name?names=' + encodeURIComponent([...allNames].join(',')), { credentials: 'same-origin' })
            .then(r => r.json())
            .then(d => {
              const lookup = {};
              (d.performers || []).forEach(p => {
                if (p && p.name) lookup[p.name.toLowerCase()] = p.headshot_url || null;
              });
              slots.forEach(slot => {
                const names = (slot.dataset.perfNames || '').split('|').map(n => n.trim()).filter(Boolean);
                const imgs = [];
                for (const n of names) {
                  const url = lookup[n.toLowerCase()];
                  if (url) {
                    imgs.push(`<span class="ts-prowlarr-headshot" title="${_esc(n)}"><img src="${_esc(url)}" alt="${_esc(n)}" onerror="this.parentElement.style.display='none'"></span>`);
                    if (imgs.length >= 3) break;
                  }
                }
                // Reverse so the primary face stacks on top in the
                // overlap chain (matches /downloads list).
                slot.innerHTML = imgs.reverse().join('');
              });
            })
            .catch(() => {});
        }
      }
    } catch (e) {
      if (myToken !== _searchToken) return;
      statusEl.textContent = 'Search error: ' + (e.message || e);
      resultsEl.innerHTML = '';
    }
  };
})();
