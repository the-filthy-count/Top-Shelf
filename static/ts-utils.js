// HTML escaping
function esc(str) {
  return String(str || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

// Backend stamps log lines with an ISO 8601 UTC prefix
// (``2026-05-29T01:00:49Z``) so the wire format is unambiguous about
// timezone. Frontend reformats to the browser's local clock so the
// user sees "their" time even when the server runs in UTC.
//
// Display shape is ``MM-DD HH:MM:SS`` — compact (14 chars vs 20 for
// the wire prefix) but keeps the date visible so a multi-day log
// doesn't make "Tuesday 09:00" and "Wednesday 09:00" indistinguishable.
//
// Lines that don't carry the ISO prefix (legacy, hand-rolled, or the
// scheduler's "—" separators) pass through untouched.
const _TS_LOG_PREFIX_RE = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z\s([\s\S]*)$/;
function tsLogFormatLine(line) {
  const s = String(line == null ? '' : line);
  const m = _TS_LOG_PREFIX_RE.exec(s);
  if (!m) return s;
  const d = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6]));
  if (isNaN(d.getTime())) return s;
  const pad = n => String(n).padStart(2, '0');
  const stamp = `${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  return `${stamp} ${m[7]}`;
}
window.tsLogFormatLine = tsLogFormatLine;

/** Rik spike-bar loader markup. Optional extra class(es), e.g. `loader--btn`. */
function loaderHtml(extraClass) {
  const cls = extraClass ? 'loader ' + extraClass : 'loader';
  return '<span class="' + cls + '" role="status" aria-label="Loading"></span>';
}
window.loaderHtml = loaderHtml;

/** Menu-icon modifier for each route (matches header `ts-nav-icon--*`). */
const TS_PAGE_NAV_ICON = {
  queue: 'queue',
  downloads: 'downloads',
  index: 'index',
  scenes: 'scenes',
  discover: 'discover',
  library: 'library',
  news: 'news',
  health: 'health',
  settings: 'settings',
  log: 'list',
};

/**
 * Ensure a title node has icon + text (+ optional meta) markup.
 * Safe to call repeatedly — only rebuilds when the icon modifier changes.
 */
function tsEnsurePageTitle(el, iconMod, text, metaHtml) {
  if (!el) return;
  const mod = iconMod || 'scenes';
  el.classList.add('ts-page-title');
  if (!el.querySelector('.ts-page-title__icon')) {
    el.innerHTML =
      '<i class="ts-page-title__icon ts-nav-icon ts-nav-icon--' + esc(mod) + '" aria-hidden="true"></i>' +
      '<span class="ts-page-title__text"></span>';
  } else {
    const ico = el.querySelector('.ts-page-title__icon');
    if (ico) ico.className = 'ts-page-title__icon ts-nav-icon ts-nav-icon--' + mod;
  }
  const textEl = el.querySelector('.ts-page-title__text');
  if (textEl && text != null) textEl.textContent = String(text);
  let metaEl = el.querySelector('.ts-page-title__meta');
  if (metaHtml != null) {
    if (!metaEl) {
      metaEl = document.createElement('span');
      metaEl.className = 'ts-page-title__meta';
      el.appendChild(metaEl);
    }
    metaEl.innerHTML = metaHtml;
  }
}
window.tsEnsurePageTitle = tsEnsurePageTitle;

/** Update only the title text; rebuilds structure if needed. */
function tsSetPageTitleText(el, text, iconMod) {
  if (!el) return;
  if (!el.querySelector('.ts-page-title__text')) {
    tsEnsurePageTitle(el, iconMod || 'scenes', text);
    return;
  }
  if (iconMod) {
    const ico = el.querySelector('.ts-page-title__icon');
    if (ico) ico.className = 'ts-page-title__icon ts-nav-icon ts-nav-icon--' + iconMod;
  }
  const textEl = el.querySelector('.ts-page-title__text');
  if (textEl) textEl.textContent = String(text);
}
window.tsSetPageTitleText = tsSetPageTitleText;

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
  if (g === 'trans' || g === 'transgender' || g === 'transgender_male' || g === 'trans-male') {
    return '<i class="fa-solid fa-transgender g-badge g-other" title="Trans" aria-label="Trans"></i>';
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

/* ── Search-query word-match highlighting ────────────────────────
 * Used by the Scene Search modal on /queue and /downloads to colour
 * matching words in result-row titles + performer-name CSV. Stop
 * words ("the", "and", "of"...) are dropped so common connectors
 * don't paint the whole row.
 *
 * Match: whole-word, case-insensitive, alphanumerics + apostrophes
 *        only (punctuation ignored).
 * CSS:   `.qs-match` (defined per-page; subtle accent tint + weight bump).
 */
const _QS_STOP_WORDS = new Set([
  'the','and','or','of','a','an','in','on','at','to','for','by','with',
  'from','as','is','be','this','that','it','its','vol','volume','part',
  'parts','scene','scenes','feat','featuring','presents','xxx','vs',
]);
function _qsBuildHighlightSet(...sources) {
  const out = new Set();
  for (const s of sources) {
    if (!s) continue;
    String(s).toLowerCase().split(/[^a-z0-9]+/).filter(Boolean).forEach(w => {
      if (w.length >= 2 && !_QS_STOP_WORDS.has(w)) out.add(w);
    });
  }
  return out;
}
function _qsHighlight(text, words) {
  if (text == null) return '';
  const s = String(text);
  if (!words || !words.size) return esc(s);
  // Walk alternating word / non-word chunks so punctuation is preserved
  // AND escaped, while word matches get wrapped in `.qs-match`.
  const re = /([A-Za-z0-9']+)|([^A-Za-z0-9']+)/g;
  let out = '';
  let m;
  while ((m = re.exec(s)) !== null) {
    if (m[1]) {
      const lc = m[1].toLowerCase();
      if (words.has(lc)) out += `<span class="qs-match">${esc(m[1])}</span>`;
      else out += esc(m[1]);
    } else if (m[2]) {
      out += esc(m[2]);
    }
  }
  return out;
}
/* Mirror of `performerCsvHtml` that runs each name through
 * `_qsHighlight` so matched words colour inside the link spans. Same
 * data attributes so the bio-popover delegate keeps working. */
function _qsPerformerCsvHtml(csv, words) {
  if (csv == null) return '';
  const s = String(csv).trim();
  if (!s) return '';
  return s.split(',').map(n => n.trim()).filter(Boolean).map(n =>
    `<span data-perf-name="${esc(n)}" data-performer-link data-name="${esc(n)}" class="perf-name-link">${_qsHighlight(n, words)}</span>`
  ).join(', ');
}

/* ── Library-phrase highlighting ─────────────────────────────────
 * Page surfaces that show raw filenames or release titles with no
 * per-row metadata (RSS feed tiles, Prowlarr search rows, download
 * client rows, /scenes feed cards) lean on the library's own
 * vocabulary to decide which words deserve `.qs-match`.
 *
 * Originally we tokenised every name + alias into individual words,
 * but multi-word entities ("Hard Anal", "Mia Khalifa") leaked common
 * English fragments — "hard", "her", "case", "off" — that then
 * painted random words across unrelated titles.
 *
 * Phrase matching fixes that: each library entity contributes ONE
 * phrase. A multi-word phrase only paints when ALL its words appear
 * consecutively (separated by any non-alphanumeric run, so spacing /
 * punctuation variations still match). Single-word entities still
 * paint as a whole word.
 *
 * Cache shape:
 *   window._libraryPhrasesLc   — array of {regex} — one regex per name/alias
 *   window._libraryStudioNamesLc — Set of folder names (filter helper)
 *   window._libraryTokensLc    — kept for backwards compat, populated
 *                                with single-word entity tokens only */
window._libraryStudioNamesLc = window._libraryStudioNamesLc || new Set();
window._libraryPhrasesLc     = window._libraryPhrasesLc     || [];
window._libraryTokensLc      = window._libraryTokensLc      || new Set();
// Lookup table from a normalised phrase key (lowercase, single-spaced)
// to the entity entry that produced it: {kind, name, rowId}. Used by
// _libraryHighlight to decide whether the matched span should carry
// data-performer-link, data-studio-link, or stay plain (vices).
// Aliases share the entry of their primary; the key is whatever was
// ingested, so an alias hit lands on the same canonical row.
window._libraryEntryByKey    = window._libraryEntryByKey    || new Map();
// Raw lowercased name lookup for non-Latin scripts (Japanese kanji,
// Cyrillic, etc.) that the token-based regex highlighter can't match.
// Stores both folder_name and any alias verbatim (whitespace-collapsed,
// lowercased) → entity entry. Lets callers ask "is this exact star
// name in my library?" without going through tokenization, which would
// strip every non-[a-z0-9] character to empty.
window._libraryEntryByName   = window._libraryEntryByName   || new Map();

const _LIB_RE_ESC = /[.*+?^${}()|[\]\\]/g;
function _libEscRe(s) { return String(s).replace(_LIB_RE_ESC, '\\$&'); }

let _libraryTokensInflight = null;
// Result cache. The library token set (performer / studio / vice
// names + aliases) only changes when the user manually adds/removes
// a favourite — fetching the full /api/favourites payload (which can
// be 2-5 MB and runs filesystem stats per row) on every page tab
// switch was the single biggest tax on /downloads sub-tab transitions
// and /news refresh. 10-minute TTL is a pragmatic balance: name
// highlighting catches up within minutes of a favourite being added
// elsewhere, and callers that know they mutated favourites can
// force-bust via `_refreshLibraryTokens(true)` or the exported
// `window._invalidateLibraryTokens()` helper.
let _libraryTokensFetchedAt = 0;
const _LIB_TOKENS_TTL_MS = 10 * 60 * 1000;
function _invalidateLibraryTokens() { _libraryTokensFetchedAt = 0; }
window._invalidateLibraryTokens = _invalidateLibraryTokens;
function _refreshLibraryTokens(force) {
  if (_libraryTokensInflight) return _libraryTokensInflight;
  // Cache hit — caller gets an already-resolved promise, no fetch
  // fires. Window globals (`_libraryStudioNamesLc`, `_libraryTokensLc`,
  // `_libraryPhrasesLc`, `_libraryEntryByKey`) stay populated from
  // the previous run.
  if (!force
      && _libraryTokensFetchedAt > 0
      && Date.now() - _libraryTokensFetchedAt < _LIB_TOKENS_TTL_MS
      && window._libraryEntryByKey) {
    return Promise.resolve();
  }
  _libraryTokensInflight = (async () => {
    try {
      const r = await fetch('/api/favourites', { credentials: 'same-origin' });
      const d = await r.json();
      const studioSet = new Set();
      const singleTokens = new Set();
      const phraseEntries = []; // [{tokens: [...], len: N, kind, name, rowId}]
      const phraseSeen = new Set();
      const entryByKey = new Map();
      const entryByName = new Map();

      const ingest = (raw, isAlias, entityKind, primaryName, rowId) => {
        if (!raw) return;
        const trimmed = String(raw).trim();
        if (!trimmed) return;
        // Raw whitespace-collapsed lowercased name — works for any
        // script (Japanese, Cyrillic, etc.) where the [a-z0-9]
        // tokenizer below produces nothing. Lets the scene-popup star
        // row check membership without depending on phrase regex
        // assembly. Stored unconditionally for performers so
        // `_libraryHasPerformerName` covers JP names too.
        const rawKey = trimmed.toLowerCase().replace(/\s+/g, ' ');
        if (entityKind === 'performer' && rawKey && !entryByName.has(rawKey)) {
          entryByName.set(rawKey, {
            kind: 'performer',
            name: primaryName || trimmed,
            rowId: rowId || null,
          });
        }
        const tokens = trimmed.toLowerCase()
          .split(/[^a-z0-9]+/)
          .filter(Boolean);
        if (!tokens.length) return;
        // Single-word aliases like "Violet" or "Daisy" are common
        // English / first-name fragments that paint unrelated titles
        // (a performer aliased "Violet" would match every release
        // mentioning the colour). Drop them — only the primary name
        // gets to be a single-word matcher.
        if (isAlias && tokens.length === 1) return;
        // Skip phrases that are entirely stop words ("the", "of", …) —
        // they'd paint absolutely everything.
        if (tokens.every(t => _QS_STOP_WORDS.has(t))) return;
        const key = tokens.join(' ');
        if (phraseSeen.has(key)) return;
        phraseSeen.add(key);
        if (tokens.length === 1) {
          // Drop single tokens that are stop words OR shorter than 2
          // chars — same filter as `_qsBuildHighlightSet` so the
          // fall-through token set stays high-signal.
          const t = tokens[0];
          if (t.length < 2 || _QS_STOP_WORDS.has(t)) return;
          singleTokens.add(t);
        }
        phraseEntries.push({ tokens, len: trimmed.length });
        // Aliases keep the entity's primary name + row id so the popup
        // resolves to the canonical row even when the green text was
        // an alias hit ("Mia" → main entry "Mia Khalifa").
        entryByKey.set(key, {
          kind: entityKind,
          name: primaryName || trimmed,
          rowId: rowId || null,
        });
      };

      for (const s of (d.studios || [])) {
        const nm = String(s.folder_name || '').trim();
        const dn = String(s.display_name || s.name || '').trim();
        const primary = dn || nm;
        const rid = s.id || s.row_id || null;
        if (nm) studioSet.add(nm.toLowerCase());
        if (dn) studioSet.add(dn.toLowerCase());
        ingest(nm, false, 'studio', primary, rid);
        ingest(dn, false, 'studio', primary, rid);
        for (const a of (s.aliases || [])) ingest(a, true, 'studio', primary, rid);
      }
      for (const p of (d.performers || [])) {
        const primary = String(p.name || '').trim();
        const rid = p.id || p.row_id || null;
        ingest(p.name, false, 'performer', primary, rid);
        for (const a of (p.aliases || [])) ingest(a, true, 'performer', primary, rid);
      }
      for (const v of (d.vices || [])) {
        const primary = String(v.name || '').trim();
        const rid = v.id || v.row_id || null;
        ingest(v.name, false, 'vice', primary, rid);
        for (const a of (v.aliases || [])) ingest(a, true, 'vice', primary, rid);
      }

      // Sort longest-first so the combined regex matches longer
      // phrases preferentially (regex alternation is left-to-right).
      phraseEntries.sort((a, b) => b.len - a.len);

      window._libraryStudioNamesLc = studioSet;
      window._libraryTokensLc = singleTokens;
      window._libraryPhrasesLc = phraseEntries;
      window._libraryEntryByKey = entryByKey;
      window._libraryEntryByName = entryByName;
      _libraryTokensFetchedAt = Date.now();
    } catch (_) { /* keep stale cache */ }
    finally { _libraryTokensInflight = null; }
  })();
  return _libraryTokensInflight;
}

/* Build (and cache) a single combined regex that matches any library
 * phrase. Re-built whenever `_libraryPhrasesLc` is replaced. The
 * `[^A-Za-z0-9']+` separator between tokens lets us match across
 * punctuation / hyphenation variants — e.g. phrase "Mia Khalifa"
 * still hits "mia-khalifa" or "Mia.Khalifa". */
let _libraryRegexCache = { phrases: null, regex: null };
function _libraryCombinedRegex() {
  const phrases = window._libraryPhrasesLc;
  if (_libraryRegexCache.phrases === phrases && _libraryRegexCache.regex !== null) {
    return _libraryRegexCache.regex;
  }
  if (!phrases || !phrases.length) {
    _libraryRegexCache = { phrases, regex: null };
    return null;
  }
  const parts = phrases.map(p => p.tokens.map(_libEscRe).join("[^A-Za-z0-9']+"));
  const re = new RegExp("\\b(?:" + parts.join('|') + ")\\b", 'gi');
  _libraryRegexCache = { phrases, regex: re };
  return re;
}

/* Resolve a library match back to its source entry. The combined regex
 * matches across `[^A-Za-z0-9']+` separators (so "mia.khalifa" hits
 * the "mia khalifa" phrase), so we tokenise the matched text the same
 * way the cache was tokenised and look up the canonical key. Returns
 * null when no entry is found (regex matched but cache was rebuilt
 * mid-render, etc.) — caller falls back to a plain `.qs-match` span. */
function _libraryEntryForMatch(matchedText) {
  const map = window._libraryEntryByKey;
  if (!map || !map.size) return null;
  const tokens = String(matchedText || '').toLowerCase().split(/[^a-z0-9]+/).filter(Boolean);
  if (!tokens.length) return null;
  return map.get(tokens.join(' ')) || null;
}

/**
 * "Is this exact star name in my library?" — works for Latin and
 * non-Latin scripts. Returns the matching library entry
 * ({kind, name, rowId}) or null. Falls through the tokenized lookup
 * when that yields nothing (Japanese, Cyrillic, …) by hitting the
 * raw-name map populated by ``ingest`` in `_loadLibraryTokens`.
 */
window.libraryEntryForPerformerName = function (name) {
  const s = String(name || '').trim();
  if (!s) return null;
  const tokenized = _libraryEntryForMatch(s);
  if (tokenized && tokenized.kind === 'performer') return tokenized;
  const byName = window._libraryEntryByName;
  if (byName && byName.size) {
    const key = s.toLowerCase().replace(/\s+/g, ' ');
    return byName.get(key) || null;
  }
  return null;
};

/* Build the highlight span for a single match. Performer entries get
 * `data-performer-link` so the global delegated handler in
 * performer-popup.js opens the universal popup; studio entries get
 * `data-studio-link` for studio-popup.js. Vices and unknown matches
 * stay as plain `.qs-match` so they paint green but don't trap clicks
 * on the surrounding text — vices have no popup yet. */
function _libraryMatchSpan(matchedText) {
  const entry = _libraryEntryForMatch(matchedText);
  const safe = esc(matchedText);
  if (!entry) return `<span class="qs-match">${safe}</span>`;
  const ridAttr = entry.rowId ? ` data-library-row-id="${entry.rowId}"` : '';
  if (entry.kind === 'performer') {
    return `<span data-performer-link data-name="${esc(entry.name)}"${ridAttr} class="qs-match qs-match--perf">${safe}</span>`;
  }
  if (entry.kind === 'studio') {
    return `<span data-studio-link data-name="${esc(entry.name)}"${ridAttr} class="qs-match qs-match--studio">${safe}</span>`;
  }
  return `<span class="qs-match">${safe}</span>`;
}

function _libraryHighlight(text) {
  if (text == null) return '';
  const s = String(text);
  if (!s) return '';
  const re = _libraryCombinedRegex();
  if (!re) return esc(s);
  re.lastIndex = 0;
  let out = '';
  let cursor = 0;
  let m;
  while ((m = re.exec(s)) !== null) {
    if (m.index > cursor) out += esc(s.slice(cursor, m.index));
    out += _libraryMatchSpan(m[0]);
    cursor = m.index + m[0].length;
    if (m[0].length === 0) re.lastIndex++; // safety against zero-width
  }
  if (cursor < s.length) out += esc(s.slice(cursor));
  return out;
}

function _studioIfInLibrary(name) {
  const s = String(name || '').trim();
  if (!s) return '';
  return window._libraryStudioNamesLc.has(s.toLowerCase()) ? s : '';
}

/* Combined highlight: library phrases take priority (multi-word
 * matches paint first), then `_qsHighlight` runs per-row tokens on
 * whatever gaps the phrase pass left untouched. Used by surfaces
 * that have BOTH per-row metadata to highlight (this scene's
 * performers / studio) AND want to surface other library entities
 * mentioned in the title. */
function _libraryAndTokenHighlight(text, extraTokens) {
  if (text == null) return '';
  const s = String(text);
  if (!s) return '';
  const tokenize = (chunk) => (extraTokens && extraTokens.size && typeof _qsHighlight === 'function')
    ? _qsHighlight(chunk, extraTokens)
    : esc(chunk);
  const re = _libraryCombinedRegex();
  if (!re) return tokenize(s);
  re.lastIndex = 0;
  let out = '';
  let cursor = 0;
  let m;
  while ((m = re.exec(s)) !== null) {
    if (m.index > cursor) out += tokenize(s.slice(cursor, m.index));
    out += _libraryMatchSpan(m[0]);
    cursor = m.index + m[0].length;
    if (m[0].length === 0) re.lastIndex++;
  }
  if (cursor < s.length) out += tokenize(s.slice(cursor));
  return out;
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

// Polling helper. Skips ticks while the tab is hidden so background tabs
// don't keep hitting the API; the next visible tick picks up where it
// left off.
function poll(fn, intervalMs, shouldStop) {
  const timer = setInterval(async () => {
    if (document.visibilityState !== 'visible') return;
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
  if (p.indexOf('/downloads') === 0 || p.indexOf('/index') === 0 || p.indexOf('/queue') === 0) {
    return 'duotone_inverted_downloads';
  }
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
  let _highlightedIdx = -1;
  // Normalised release titles already present in the download client queue
  // or source-folder queue; used to badge "QUEUED" rows so the user
  // doesn't double-grab. Built once per popup open.
  let _queuedTitlesNorm = new Set();

  // Persist last-used kind across invocations. Callers that pass an
  // explicit `kind` still win, but the toggle in the header writes back
  // to localStorage so the popup reopens on whichever the user picked.
  const KIND_STORAGE_KEY = 'tsProwlarrKindLast';
  function _readStoredKind() {
    try {
      const v = localStorage.getItem(KIND_STORAGE_KEY);
      return (v === 'movie' || v === 'scene') ? v : null;
    } catch (_) { return null; }
  }
  function _writeStoredKind(kind) {
    try { localStorage.setItem(KIND_STORAGE_KEY, kind === 'movie' ? 'movie' : 'scene'); } catch (_) {}
  }
  // Normalise a release title for cross-source comparison: lowercase,
  // strip extension, replace dot/underscore separators with spaces,
  // collapse repeats. Identical normalisation runs on both the Prowlarr
  // result and any active queue item.
  function _normReleaseTitle(s) {
    return String(s || '')
      .toLowerCase()
      .replace(/\.(nzb|torrent|magnet|mkv|mp4|avi|wmv|mov|m4v|webm|flv)$/i, '')
      .replace(/[._]+/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
  }

  /** Strip quality tokens so duplicate indexers collapse to one row per scene. */
  function _normReleaseTitleAggressive(s) {
    return String(s || '').toLowerCase()
      .replace(/[\[\(].*?[\]\)]/g, '')
      .replace(/\b(720p|1080p|2160p|4k|uhd|sd|480p|hdtv|webrip|web-dl|webdl|web|bdrip|brrip|bluray|x264|x265|h264|h265|hevc|aac|mp3|xvid|divx|imageset|imgset)\b/gi, '')
      .replace(/[\W_]+/g, ' ')
      .trim();
  }

  function _dedupeProwlarrReleases(releases, aggressive) {
    const normFn = aggressive ? _normReleaseTitleAggressive : _normReleaseTitle;
    const seen = new Set();
    const unique = [];
    for (const rel of releases || []) {
      const norm = normFn(rel.title);
      if (!norm || seen.has(norm)) continue;
      seen.add(norm);
      unique.push(rel);
    }
    return unique;
  }

  /** Shared with row meta pills and the keyword / quality filter bar.
   *  ``img`` (when present) is a static badge in /static/logos/ — the
   *  row meta and the filter bar both render the image instead of a
   *  text pill. Patterns without ``img`` (e.g. IMG for imagesets)
   *  fall back to the legacy text pill so non-video markers still
   *  show something. Order matters: VR/3D/8K listed before the
   *  resolution patterns so a "4K VR" title is tagged as VR + 4K
   *  (both pills emit) without one regex shadowing the other. */
  const PROWLARR_QUALITY_PATTERNS = [
    { rx: /\bvr\b/i,                            cls: 'ts-q-vr',     label: 'VR',     img: '/static/logos/badges/vr.webp'        },
    { rx: /\b3d\b/i,                            cls: 'ts-q-3d',     label: '3D',     img: '/static/logos/badges/3d.webp'        },
    { rx: /\buncen(sored)?\b/i,                 cls: 'ts-q-unc',    label: 'UNCENSORED', img: '/static/logos/badges/uncensored.webp' },
    { rx: /\b(bluray|blu[-_ ]?ray|bdrip|brrip|bd[-_ ]?rip)\b/i, cls: 'ts-q-bluray', label: 'BLU-RAY', img: '/static/logos/badges/bluray.webp' },
    { rx: /\b(dvd|dvdrip|dvd[-_ ]?rip)\b/i,     cls: 'ts-q-dvd',    label: 'DVD',    img: '/static/logos/badges/dvd.webp'       },
    { rx: /\b(4320p|8k)\b/i,                    cls: 'ts-q-8k',     label: '8K',     img: '/static/logos/badges/8k.webp'        },
    { rx: /\b(2160p|4k|uhd)\b/i,                cls: 'ts-q-4k',     label: '4K',     img: '/static/logos/badges/4k.webp'        },
    { rx: /\b1080p\b/i,                         cls: 'ts-q-1080',   label: '1080P',  img: '/static/logos/badges/1080.webp'      },
    { rx: /\b720p\b/i,                          cls: 'ts-q-720',    label: '720P',   img: '/static/logos/badges/720.webp'       },
    { rx: /\b480p\b/i,                          cls: 'ts-q-480',    label: '480P',   img: '/static/logos/badges/480.webp'       },
    { rx: /\b(sd|xvid)\b/i,                     cls: 'ts-q-sd',     label: 'SD',     img: '/static/logos/badges/sd.webp'        },
    { rx: /\b(imageset|image[-_ ]?set|imgset|jpg|jpeg|pics?|photos?)\b/i, cls: 'ts-q-img', label: 'IMG', img: '/static/logos/badges/img.webp' },
    { rx: /\b(par2?|rar|zip|7z|tar|tgz|gz|bz2|tbz)\b/i, cls: 'ts-q-archive', label: 'ARCHIVE', img: '/static/logos/badges/archive.webp' },
  ];

  /** Bar + filter matching aliases the row pattern set now that
   *  uncensored has its own badge image and renders as a normal row badge. */
  const PROWLARR_BAR_RX_PATTERNS = PROWLARR_QUALITY_PATTERNS;

  /** Shared image-badge renderer for any Prowlarr surface (the main
   *  overlay row, embedded mounts, the legacy performer-tile popup).
   *  Returns a string of ``<img>`` tags — one per matched pattern that
   *  has a badge image — with no surrounding pill. ``variant`` picks
   *  the size class: ``row`` (~16px tall) for inline meta lines,
   *  ``filter`` (~20px tall) for filter strips. */
  window._tsProwlarrQualityBadges = function (title, variant) {
    const t = title || '';
    const seen = new Set();
    const out = [];
    const sizeCls = variant === 'filter' ? 'ts-prowlarr-q-img--filter' : 'ts-prowlarr-q-img--row';
    //: Bake the height directly into each <img>'s style attribute —
    //: same 24px for both variants so the filter strip and the row
    //: meta line stay locked in lock-step regardless of CSS cache
    //: state or specificity drift.
    const heightStyle = 'height:24px;width:auto;vertical-align:middle;display:inline-block';
    for (const p of PROWLARR_QUALITY_PATTERNS) {
      if (!p.img) continue;
      if (seen.has(p.cls)) continue;
      if (p.rx.test(t)) {
        seen.add(p.cls);
        out.push(`<img src="${p.img}" alt="${_esc(p.label)}" title="${_esc(p.label)}" class="ts-prowlarr-q-img ${sizeCls} ${p.cls}" style="${heightStyle}" onerror="this.style.display='none'" loading="lazy">`);
      }
    }
    return out.join('');
  };

  function _prowlarrBarPatternByLabel(lab) {
    return PROWLARR_BAR_RX_PATTERNS.find((p) => p.label === lab) || null;
  }

  let _prowlarrAllReleases = [];
  /** ``null`` = show all; else ``{ kind:'qual', pattern, label }``. */
  let _prowlarrFilterSpec = null;
  let _prowlarrChipOpts = null;
  let _prowlarrResultToken = 0;
  let _prowlarrPaintGen = 0;

  /** Which resolution / uncensored tags appear in the result set (for the filter strip). */
  function _prowlarrBuildQualFilterChips(allReleases) {
    const qualChips = [];
    for (const p of PROWLARR_BAR_RX_PATTERNS) {
      let c = 0;
      for (const rel of allReleases) {
        if (p.rx.test(rel.title || '')) c++;
      }
      if (c) qualChips.push({ kind: 'qual', pattern: p, label: p.label, count: c });
    }
    return qualChips;
  }

  function _prowlarrRowMatchesFilter(rel, spec) {
    if (!spec) return true;
    const tit = rel.title || '';
    if (spec.kind === 'qual') return spec.pattern.rx.test(tit);
    return true;
  }

  function _prowlarrHtmlFilterBar(chips, total, spec) {
    const allOn = !spec;
    const parts = [
      `<button type="button" class="ts-prowlarr-filter-all${allOn ? ' is-active' : ''}" data-filter-all="1" title="Show all releases">All <span class="ts-prowlarr-filter-count">(${total})</span></button>`,
    ];
    for (const ch of chips) {
      if (ch.kind !== 'qual') continue;
      const on = spec && spec.kind === 'qual' && spec.label === ch.label;
      const active = on ? ' is-active' : '';
      if (ch.pattern.img) {
        //: Image-only filter button — no pill background or text. The
        //: ``is-active`` outline still indicates the current selection
        //: (CSS handles it via .ts-prowlarr-filter-pill--img.is-active).
        parts.push(
          `<button type="button" class="ts-prowlarr-filter-pill ts-prowlarr-filter-pill--img${active}" data-filter-qual="${_esc(ch.label)}" title="Show ${_esc(ch.label)} releases only" style="padding:0;border:1px solid transparent;background:transparent;height:24px;line-height:0"><img src="${ch.pattern.img}" alt="${_esc(ch.label)}" class="ts-prowlarr-q-img ts-prowlarr-q-img--filter ${ch.pattern.cls}" style="height:24px;width:auto;display:block" onerror="this.style.display='none'" loading="lazy"></button>`,
        );
      } else {
        const cls = ch.pattern.cls;
        parts.push(
          `<button type="button" class="ts-prowlarr-q ${cls} ts-prowlarr-filter-pill${active}" data-filter-qual="${_esc(ch.label)}" title="Show ${_esc(ch.label)} releases only">${_esc(ch.label)}</button>`,
        );
      }
    }
    return `<div class="ts-prowlarr-filter-bar-inner" role="toolbar" aria-label="Filter by resolution">${parts.join('')}</div>`;
  }

  function _setHighlightedIdxIn(resultsEl, idx, scroll, ctx) {
    ctx.setHighlight(idx);
    if (!resultsEl) return;
    const rows = resultsEl.querySelectorAll('.ts-prowlarr-row');
    rows.forEach((r) => r.classList.remove('is-highlighted'));
    const target = resultsEl.querySelector(`.ts-prowlarr-row[data-row-idx="${idx}"]`);
    if (target) {
      target.classList.add('is-highlighted');
      if (scroll) target.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    }
  }

  function _repaintQueuedBadgesIn(resultsEl, activeList) {
    if (!resultsEl || !activeList || !activeList.length) return;
    const isInQueue = (relTitle) => {
      const norm = _normReleaseTitle(relTitle);
      if (!norm) return false;
      if (_queuedTitlesNorm.has(norm)) return true;
      for (const q of _queuedTitlesNorm) {
        if (q && (q.includes(norm) || norm.includes(q))) return true;
      }
      return false;
    };
    resultsEl.querySelectorAll('.ts-prowlarr-row').forEach((row) => {
      const idx = parseInt(row.dataset.rowIdx, 10);
      const rel = activeList[idx];
      if (!rel) return;
      const queued = isInQueue(rel.title);
      row.classList.toggle('is-in-queue', queued);
      const grab = row.querySelector('.ts-prowlarr-grab');
      const meta = row.querySelector('.ts-prowlarr-meta');
      if (grab) {
        grab.disabled = !!queued;
        grab.title = queued ? 'Already in queue' : 'Send to download client';
        grab.innerHTML = queued ? '<i class="fa-solid fa-check"></i>' : '<i class="fa-solid fa-download"></i>';
      }
      if (meta) {
        const existing = meta.querySelector('.ts-prowlarr-queued-pill');
        if (queued && !existing) {
          meta.insertAdjacentHTML('beforeend', '<span class="ts-prowlarr-queued-pill" title="A release with this name is already in your active queue or download client"><i class="fa-solid fa-check"></i> Queued</span>');
        } else if (!queued && existing) {
          existing.remove();
        }
      }
    });
  }

  function _prowlarrPaintResultTable(statusEl, filtersEl, resultsEl, ctx, myToken) {
    const fe = filtersEl || document.getElementById('tsProwlarrPopupFilters');
    const all = ctx.all;
    let filterSpec = ctx.getFilterSpec();
    if (filterSpec && filterSpec.kind === 'kw') {
      filterSpec = null;
      ctx.setFilterSpec(null);
    }
    const chips = _prowlarrBuildQualFilterChips(all);
    const filtered = all.filter((rel) => _prowlarrRowMatchesFilter(rel, filterSpec));
    ctx.setActive(filtered);

    const n = filtered.length;
    const nAll = all.length;
    if (filterSpec) {
      statusEl.innerHTML = `<strong>${n}</strong> of <strong>${nAll}</strong> release${nAll === 1 ? '' : 's'} (filtered)`;
    } else {
      statusEl.innerHTML = `<strong>${n}</strong> release${n === 1 ? '' : 's'} found`;
    }

    if (!chips.length) {
      if (fe) {
        fe.innerHTML = '';
        fe.hidden = true;
      }
    } else if (fe) {
      fe.hidden = false;
      fe.innerHTML = _prowlarrHtmlFilterBar(chips, nAll, filterSpec);
    }

    if (!n) {
      resultsEl.innerHTML = '<div class="ts-prowlarr-popup-empty">No releases match this filter.</div>';
      ctx.setHighlight(-1);
      return;
    }

    const pg = ctx.bumpPaintGen();
    const buildQualityPills = (rTitle) => {
      const t = rTitle || '';
      const seen = new Set();
      const pills = [];
      for (const p of PROWLARR_QUALITY_PATTERNS) {
        if (seen.has(p.cls)) continue;
        if (p.rx.test(t)) {
          seen.add(p.cls);
          if (p.img) {
            //: Image-only marker — no pill background. Sized to match
            //: the row's uncensored.png badge for consistent rhythm.
            pills.push(`<img src="${p.img}" alt="${_esc(p.label)}" title="${_esc(p.label)}" class="ts-prowlarr-q-img ts-prowlarr-q-img--row ${p.cls}" style="height:24px;width:auto;vertical-align:middle" onerror="this.style.display='none'" loading="lazy">`);
          } else {
            pills.push(`<span class="ts-prowlarr-q ${p.cls}">${p.label}</span>`);
          }
        }
      }
      return pills.join('');
    };
    const isInQueue = (relTitle) => {
      const norm = _normReleaseTitle(relTitle);
      if (!norm) return false;
      if (_queuedTitlesNorm.has(norm)) return true;
      for (const q of _queuedTitlesNorm) {
        if (q && (q.includes(norm) || norm.includes(q))) return true;
      }
      return false;
    };

    const _earlyNames = new Set();
    for (const rel of filtered) {
      const performers = ((rel.match && rel.match.performers) || []).filter(Boolean).slice(0, 3);
      for (const nm of performers) {
        const t = (nm || '').trim();
        if (t) _earlyNames.add(t);
      }
    }
    const _headshotPromise = _earlyNames.size
      ? fetch('/api/performers/headshots-by-name?names=' + encodeURIComponent([..._earlyNames].join(',')), { credentials: 'same-origin' })
          .then((r) => r.json())
          .catch(() => null)
      : Promise.resolve(null);

    resultsEl.innerHTML = filtered.map((rel, i) => {
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
      const typeLogo = `<img class="ts-prowlarr-typelogo" src="/static/logos/${isTor ? 'torrent' : 'nzb'}.webp" alt="${typeLabel}" title="${typeLabel}" onerror="this.style.display='none'">`;
      const m = rel.match || {};
      const studio = (m.studio || '').trim();
      const studioLogo = studio
        ? `<img class="ts-prowlarr-studiologo" src="/api/studio-logo?name=${encodeURIComponent(studio)}&q=${encodeURIComponent(rel.title || '')}" alt="${_esc(studio)}" title="${_esc(studio)}" onerror="this.style.display='none'" loading="lazy">`
        : '';
      const performers = (m.performers || []).filter(Boolean).slice(0, 3);
      const headshotSlot = performers.length
        ? `<span class="ts-prowlarr-headshots" data-perf-names="${_esc(performers.join('|'))}"></span>`
        : '';
      const qualityPills = buildQualityPills(rel.title || '');
      const queued = isInQueue(rel.title);
      const queuedClass = queued ? ' is-in-queue' : '';
      const queuedPill = queued ? '<span class="ts-prowlarr-queued-pill" title="A release with this name is already in your active queue or download client"><i class="fa-solid fa-check"></i> Queued</span>' : '';
      const grabBtn = queued
        ? `<button type="button" class="ts-prowlarr-grab ${isTor ? 'is-torrent' : 'is-nzb'}" data-release-idx="${i}" title="Already in queue" disabled><i class="fa-solid fa-check"></i></button>`
        : `<button type="button" class="ts-prowlarr-grab ${isTor ? 'is-torrent' : 'is-nzb'}" data-release-idx="${i}" title="Send to download client"><i class="fa-solid fa-download"></i></button>`;
      return `<div class="ts-prowlarr-row${queuedClass}" data-row-idx="${i}">
          <div class="ts-prowlarr-cell">${studioLogo}</div>
          <div class="ts-prowlarr-cell-name">
            <div class="ts-prowlarr-title" title="${_esc(rel.title || '')}">${_esc(rel.title || 'Untitled')}</div>
            <div class="ts-prowlarr-meta"><span>${_esc(meta)}</span>${qualityPills}${queuedPill}</div>
          </div>
          <div class="ts-prowlarr-cell ts-prowlarr-cell-headshots">${headshotSlot}</div>
          <div class="ts-prowlarr-cell ts-prowlarr-cell-badges">
            <span class="ts-prowlarr-typerow">${typeLogo}</span>
            <span class="ts-prowlarr-indexer" title="${_esc(indexer)}">${_esc(indexer)}</span>
          </div>
          <div class="ts-prowlarr-cell">${grabBtn}</div>
        </div>`;
    }).join('');

    const firstFreeIdx = filtered.findIndex((rel) => !isInQueue(rel.title));
    if (firstFreeIdx >= 0) _setHighlightedIdxIn(resultsEl, firstFreeIdx, false, ctx);
    else ctx.setHighlight(-1);

    const slots = Array.from(resultsEl.querySelectorAll('.ts-prowlarr-headshots[data-perf-names]'));
    if (slots.length && _earlyNames.size) {
      _headshotPromise.then((d) => {
        if (myToken !== ctx.getSearchToken() || pg !== ctx.getPaintGen() || !d) return;
        const lookup = {};
        (d.performers || []).forEach((p) => {
          if (p && p.name) lookup[p.name.toLowerCase()] = p.headshot_url || null;
        });
        slots.forEach((slot) => {
          const names = (slot.dataset.perfNames || '').split('|').map((n) => n.trim()).filter(Boolean);
          const imgs = [];
          for (const n of names) {
            const url = lookup[n.toLowerCase()];
            if (url) {
              imgs.push(`<span class="ts-prowlarr-headshot" title="${_esc(n)}"><img src="${_esc(url)}" alt="${_esc(n)}" onerror="this.parentElement.style.display='none'" loading="lazy"></span>`);
              if (imgs.length >= 3) break;
            }
          }
          slot.innerHTML = imgs.reverse().join('');
        });
      });
    }
    _repaintQueuedBadgesIn(resultsEl, ctx.getActive());
  }

  function _popupCtx() {
    return {
      get all() { return _prowlarrAllReleases; },
      getFilterSpec: () => _prowlarrFilterSpec,
      setFilterSpec: (v) => { _prowlarrFilterSpec = v; },
      getActive: () => _activeReleases,
      setActive: (v) => { _activeReleases = v; },
      getHighlight: () => _highlightedIdx,
      setHighlight: (v) => { _highlightedIdx = v; },
      getSearchToken: () => _searchToken,
      getPaintGen: () => _prowlarrPaintGen,
      bumpPaintGen: () => ++_prowlarrPaintGen,
      getKind: () => _activeKind,
    };
  }

  function ensureMarkup() {
    let div = document.getElementById('tsProwlarrPopup');
    if (div) {
      if (!document.getElementById('tsProwlarrPopupFilters')) {
        const status = document.getElementById('tsProwlarrPopupStatus');
        const results = document.getElementById('tsProwlarrPopupResults');
        if (status && results && status.parentNode) {
          const fe = document.createElement('div');
          fe.id = 'tsProwlarrPopupFilters';
          fe.className = 'ts-prowlarr-popup-filters ts-prowlarr-filter-bar';
          fe.hidden = true;
          status.parentNode.insertBefore(fe, results);
        }
      }
      if (!div.dataset.tsProwlarrFilterNavV2) {
        div.dataset.tsProwlarrFilterNavV2 = '1';
        div.addEventListener('click', (ev) => {
          const chip = ev.target.closest('[data-filter-all],[data-filter-qual]');
          if (!chip || !div.contains(chip)) return;
          ev.preventDefault();
          ev.stopPropagation();
          if (!div.classList.contains('open')) return;
          if (chip.hasAttribute('data-filter-all')) {
            _prowlarrFilterSpec = null;
          } else if (chip.hasAttribute('data-filter-qual')) {
            const lab = chip.getAttribute('data-filter-qual') || '';
            const pat = _prowlarrBarPatternByLabel(lab);
            _prowlarrFilterSpec = pat ? { kind: 'qual', pattern: pat, label: lab } : null;
          }
          const statusEl = document.getElementById('tsProwlarrPopupStatus');
          const filtersEl = document.getElementById('tsProwlarrPopupFilters');
          const resultsEl = document.getElementById('tsProwlarrPopupResults');
          if (!statusEl || !filtersEl || !resultsEl || !_prowlarrChipOpts) return;
          _prowlarrPaintResultTable(statusEl, filtersEl, resultsEl, _popupCtx(), _prowlarrResultToken);
        });
      }
      return;
    }
    div = document.createElement('div');
    div.id = 'tsProwlarrPopup';
    div.className = 'ts-prowlarr-popup-overlay';
    div.innerHTML = `
      <div class="ts-prowlarr-popup-shell" role="dialog" aria-modal="true" aria-label="Prowlarr search">
        <button type="button" class="ts-prowlarr-popup-close" aria-label="Close" title="Close">&times;</button>
        <div class="ts-prowlarr-popup-header" id="tsProwlarrPopupHeader"></div>
        <div class="ts-prowlarr-popup-status" id="tsProwlarrPopupStatus"></div>
        <div class="ts-prowlarr-popup-filters ts-prowlarr-filter-bar" id="tsProwlarrPopupFilters" hidden></div>
        <div class="ts-prowlarr-popup-results" id="tsProwlarrPopupResults"></div>
      </div>`;
    document.body.appendChild(div);
    div.addEventListener('click', (ev) => { if (ev.target === div) closePopup(); });
    div.addEventListener('click', (ev) => {
      const chip = ev.target.closest('[data-filter-all],[data-filter-qual]');
      if (!chip || !div.contains(chip)) return;
      ev.preventDefault();
      ev.stopPropagation();
      if (!div.classList.contains('open')) return;
      if (chip.hasAttribute('data-filter-all')) {
        _prowlarrFilterSpec = null;
      } else if (chip.hasAttribute('data-filter-qual')) {
        const lab = chip.getAttribute('data-filter-qual') || '';
        const pat = _prowlarrBarPatternByLabel(lab);
        _prowlarrFilterSpec = pat ? { kind: 'qual', pattern: pat, label: lab } : null;
      }
      const statusEl = document.getElementById('tsProwlarrPopupStatus');
      const filtersEl = document.getElementById('tsProwlarrPopupFilters');
      const resultsEl = document.getElementById('tsProwlarrPopupResults');
      if (!statusEl || !filtersEl || !resultsEl || !_prowlarrChipOpts) return;
      _prowlarrPaintResultTable(statusEl, filtersEl, resultsEl, _popupCtx(), _prowlarrResultToken);
    });
    div.dataset.tsProwlarrFilterNavV2 = '1';
    div.querySelector('.ts-prowlarr-popup-close').addEventListener('click', closePopup);
    div.querySelector('.ts-prowlarr-popup-results').addEventListener('click', onGrabClick);
    // Hover should follow the keyboard caret so cursor & keyboard agree
    // on which row would fire on Enter.
    div.querySelector('.ts-prowlarr-popup-results').addEventListener('mouseover', (ev) => {
      const row = ev.target.closest('.ts-prowlarr-row');
      if (!row) return;
      const idx = parseInt(row.dataset.rowIdx, 10);
      if (Number.isFinite(idx)) _setHighlightedIdxIn(ev.currentTarget, idx, false, _popupCtx());
    });
  }

  // Keyboard nav: Esc closes, ↑/↓ moves the highlight, Enter triggers
  // grab on the highlighted row. Only active when the popup is open.
  // Capture on ``window`` so we run before per-page ``document`` Escape
  // handlers (e.g. /downloads search overlay) and we stop propagation
  // so one Escape does not close stacked UI behind this dialog.
  function _onPopupKeydown(ev) {
    const overlay = document.getElementById('tsProwlarrPopup');
    if (!overlay || !overlay.classList.contains('open')) return;
    if (ev.key === 'Escape') {
      closePopup();
      ev.preventDefault();
      ev.stopPropagation();
      ev.stopImmediatePropagation();
      return;
    }
    const popupResults = document.getElementById('tsProwlarrPopupResults');
    const pctx = _popupCtx();
    if (!_activeReleases.length || !popupResults) return;
    if (ev.key === 'ArrowDown') {
      ev.preventDefault();
      ev.stopPropagation();
      ev.stopImmediatePropagation();
      const hi = pctx.getHighlight();
      _setHighlightedIdxIn(popupResults, Math.min(_activeReleases.length - 1, (hi < 0 ? -1 : hi) + 1), true, pctx);
    } else if (ev.key === 'ArrowUp') {
      ev.preventDefault();
      ev.stopPropagation();
      ev.stopImmediatePropagation();
      const hi = pctx.getHighlight();
      _setHighlightedIdxIn(popupResults, Math.max(0, (hi <= 0 ? 1 : hi) - 1), true, pctx);
    } else if (ev.key === 'Enter' && pctx.getHighlight() >= 0) {
      const row = popupResults.querySelector(`.ts-prowlarr-row[data-row-idx="${pctx.getHighlight()}"]`);
      if (!row) return;
      const btn = row.querySelector('.ts-prowlarr-grab');
      if (!btn || btn.disabled) return;
      ev.preventDefault();
      ev.stopPropagation();
      ev.stopImmediatePropagation();
      btn.click();
    }
  }

  window.addEventListener('keydown', _onPopupKeydown, true);

  // Pull active titles from both the download-client queues and the
  // source-folder waiting list, normalise them, and stash them in the
  // shared lookup set. Best-effort — failures fall through silently and
  // simply mean rows render without queued badges.
  async function _refreshQueuedTitles() {
    const fetches = [
      fetch('/api/downloads', { credentials: 'same-origin' })
        .then((r) => r.ok ? r.json() : null)
        .catch(() => null),
      fetch('/api/queue', { credentials: 'same-origin' })
        .then((r) => r.ok ? r.json() : null)
        .catch(() => null),
    ];
    const [dl, q] = await Promise.all(fetches);
    const set = new Set();
    if (dl && Array.isArray(dl.items)) {
      dl.items.forEach((it) => {
        const norm = _normReleaseTitle(it && (it.display_name || it.name));
        if (norm) set.add(norm);
      });
    }
    if (q && Array.isArray(q.files)) {
      q.files.forEach((f) => {
        const norm = _normReleaseTitle(f && (f.display_name || f.filename));
        if (norm) set.add(norm);
      });
    }
    _queuedTitlesNorm = set;
    const popupResults = document.getElementById('tsProwlarrPopupResults');
    if (popupResults && _activeReleases.length) {
      _repaintQueuedBadgesIn(popupResults, _activeReleases);
    }
    if (_embedHost) {
      const embedResults = _embedHost.querySelector('.ts-prowlarr-embed-results');
      if (embedResults && _embedActiveReleases.length) {
        _repaintQueuedBadgesIn(embedResults, _embedActiveReleases);
      }
    }
  }

  function closePopup() {
    const o = document.getElementById('tsProwlarrPopup');
    if (o) o.classList.remove('open');
    _searchToken++; // invalidate any in-flight search
    _prowlarrAllReleases = [];
    _prowlarrFilterSpec = null;
    _prowlarrChipOpts = null;
    const fe = document.getElementById('tsProwlarrPopupFilters');
    if (fe) {
      fe.innerHTML = '';
      fe.hidden = true;
    }
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
    btn.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Loading"></span>';
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
          // Title is critical — the backend stamps `[ts-XXXXXXXX]`
          // into it for tracking, and the download client uses it
          // as the job/filename. Without this the client falls back
          // to the bare GUID (e.g. "2a1048ae5e4eb554c5b8c2fb2ab2814c")
          // as the name. Some preloaded RSS rows ship only the
          // `clean_title` (grouped-primary fields) so we cascade
          // through the available title-shaped fields.
          title: result.title || result.clean_title || result.name || '',
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

  // ── Embedded Prowlarr list (performer popup tab, etc.) ─────────────
  let _embedHost = null;
  let _embedToken = 0;
  let _embedAllReleases = [];
  let _embedFilterSpec = null;
  let _embedActiveReleases = [];
  let _embedKind = 'scene';
  let _embedHighlightedIdx = -1;
  let _embedPaintGen = 0;

  function _embedCtx() {
    return {
      get all() { return _embedAllReleases; },
      getFilterSpec: () => _embedFilterSpec,
      setFilterSpec: (v) => { _embedFilterSpec = v; },
      getActive: () => _embedActiveReleases,
      setActive: (v) => { _embedActiveReleases = v; },
      getHighlight: () => _embedHighlightedIdx,
      setHighlight: (v) => { _embedHighlightedIdx = v; },
      getSearchToken: () => _embedToken,
      getPaintGen: () => _embedPaintGen,
      bumpPaintGen: () => ++_embedPaintGen,
      getKind: () => _embedKind,
    };
  }

  function _wireEmbedHost(hostEl) {
    if (!hostEl || hostEl.dataset.tsProwlarrEmbedWired) return;
    hostEl.dataset.tsProwlarrEmbedWired = '1';
    hostEl.addEventListener('click', (ev) => {
      const chip = ev.target.closest('[data-filter-all],[data-filter-qual]');
      if (chip && hostEl.contains(chip)) {
        ev.preventDefault();
        ev.stopPropagation();
        if (chip.hasAttribute('data-filter-all')) {
          _embedFilterSpec = null;
        } else if (chip.hasAttribute('data-filter-qual')) {
          const lab = chip.getAttribute('data-filter-qual') || '';
          const pat = _prowlarrBarPatternByLabel(lab);
          _embedFilterSpec = pat ? { kind: 'qual', pattern: pat, label: lab } : null;
        }
        const statusEl = hostEl.querySelector('.ts-prowlarr-embed-status');
        const filtersEl = hostEl.querySelector('.ts-prowlarr-embed-filters');
        const resultsEl = hostEl.querySelector('.ts-prowlarr-embed-results');
        if (statusEl && filtersEl && resultsEl) {
          _prowlarrPaintResultTable(statusEl, filtersEl, resultsEl, _embedCtx(), _embedToken);
        }
        return;
      }
      const grab = ev.target.closest('.ts-prowlarr-grab');
      if (grab && hostEl.contains(grab)) _onEmbedGrabClick(ev);
    });
    const resultsEl = hostEl.querySelector('.ts-prowlarr-embed-results');
    if (resultsEl) {
      resultsEl.addEventListener('mouseover', (ev) => {
        const row = ev.target.closest('.ts-prowlarr-row');
        if (!row) return;
        const idx = parseInt(row.dataset.rowIdx, 10);
        if (Number.isFinite(idx)) _setHighlightedIdxIn(ev.currentTarget, idx, false, _embedCtx());
      });
    }
  }

  async function _onEmbedGrabClick(ev) {
    const btn = ev.target.closest('.ts-prowlarr-grab');
    if (!btn) return;
    ev.preventDefault();
    ev.stopPropagation();
    const idx = parseInt(btn.dataset.releaseIdx, 10);
    const result = _embedActiveReleases[idx];
    if (!result) return;
    btn.disabled = true;
    btn.classList.remove('is-sent');
    btn.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Loading"></span>';
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
          title: result.title || result.clean_title || result.name || '',
          kind: _embedKind === 'movie' ? 'movie' : 'scene',
        }),
      });
      const d = await r.json();
      if (d && d.ok) {
        btn.classList.add('is-sent');
        btn.innerHTML = '<i class="fa-solid fa-check"></i>';
      } else {
        btn.disabled = false;
        btn.innerHTML = '<i class="fa-solid fa-download"></i>';
        if (typeof window.toast === 'function') {
          window.toast((d && d.error) || 'Could not send to download client', { kind: 'error' });
        } else {
          alert((d && d.error) || 'Could not send to download client');
        }
      }
    } catch (e) {
      btn.disabled = false;
      btn.innerHTML = '<i class="fa-solid fa-download"></i>';
      if (typeof window.toast === 'function') {
        window.toast(e.message || 'Failed', { kind: 'error' });
      } else {
        alert(e.message || 'Failed');
      }
    }
  }

  window.unmountEmbeddedProwlarrSearch = function (hostEl) {
    if (hostEl && _embedHost === hostEl) _embedHost = null;
    _embedToken++;
    _embedAllReleases = [];
    _embedFilterSpec = null;
    _embedActiveReleases = [];
    _embedHighlightedIdx = -1;
  };

  window.mountEmbeddedProwlarrSearch = async function (hostEl, opts) {
    if (!hostEl) return;
    const o = opts || {};
    const title = (o.title || '').trim();
    if (!title) return;
    window.unmountEmbeddedProwlarrSearch(hostEl);
    _embedHost = hostEl;
    _embedKind = (o.kind === 'movie' || o.kind === 'scene') ? o.kind : 'scene';
    _embedFilterSpec = null;
    _embedHighlightedIdx = -1;
    _queuedTitlesNorm = new Set();
    _wireEmbedHost(hostEl);

    const statusEl = hostEl.querySelector('.ts-prowlarr-embed-status');
    const filtersEl = hostEl.querySelector('.ts-prowlarr-embed-filters');
    const resultsEl = hostEl.querySelector('.ts-prowlarr-embed-results');
    if (!statusEl || !resultsEl) return;

    if (filtersEl) {
      filtersEl.innerHTML = '';
      filtersEl.hidden = true;
    }
    statusEl.textContent = 'Searching Prowlarr…';
    resultsEl.innerHTML = '<div class="ts-prowlarr-popup-empty">Please wait…</div>';
    _refreshQueuedTitles().catch(() => {});

    const myToken = ++_embedToken;
    try {
      let d;
      if (Array.isArray(o.preloadedResults) && o.preloadedResults.length) {
        d = { results: o.preloadedResults };
      } else {
        const params = new URLSearchParams();
        params.append('q', title);
        if (o.dotVariant && /\s/.test(title)) {
          params.append('q', title.replace(/\s+/g, '.'));
        }
        const perfStr = (o.performers || '').trim();
        const studio = (o.studio || '').trim();
        if (perfStr && studio) {
          const studioQ = `${perfStr} ${studio}`.trim();
          if (studioQ.toLowerCase() !== title.toLowerCase()) params.append('q', studioQ);
        }
        const r = await fetch('/api/prowlarr/search?' + params.toString(), { credentials: 'same-origin' });
        d = await r.json();
      }
      if (myToken !== _embedToken || _embedHost !== hostEl) return;
      if (d && d.error) {
        statusEl.textContent = d.error;
        resultsEl.innerHTML = '';
        if (filtersEl) {
          filtersEl.innerHTML = '';
          filtersEl.hidden = true;
        }
        return;
      }
      let releases = ((d && d.results) || []).slice(0, 50);
      const totalRaw = releases.length;
      if (o.dedupe !== false) {
        const aggressive = o.dedupe !== 'norm';
        releases = _dedupeProwlarrReleases(releases, aggressive);
      }
      _embedAllReleases = releases;
      if (!_embedAllReleases.length) {
        _embedActiveReleases = [];
        if (filtersEl) {
          filtersEl.innerHTML = '';
          filtersEl.hidden = true;
        }
        statusEl.innerHTML = '<strong>0</strong> releases found';
        resultsEl.innerHTML = '<div class="ts-prowlarr-popup-empty">No releases matched this title on your configured indexers.</div>';
        return;
      }
      _prowlarrPaintResultTable(statusEl, filtersEl, resultsEl, _embedCtx(), myToken);
      if (totalRaw > releases.length && o.dedupe !== false) {
        statusEl.innerHTML = `<strong>${releases.length}</strong> unique release${releases.length === 1 ? '' : 's'} <span class="ts-prowlarr-embed-status-dim">(${totalRaw} before dedupe)</span>`;
      }
    } catch (e) {
      if (myToken !== _embedToken || _embedHost !== hostEl) return;
      statusEl.textContent = 'Search error: ' + (e.message || e);
      resultsEl.innerHTML = '';
      if (filtersEl) {
        filtersEl.innerHTML = '';
        filtersEl.hidden = true;
      }
    }
  };

  window.openProwlarrSearchPopup = async function (opts) {
    ensureMarkup();
    const o = opts || {};
    const title = (o.title || '').trim();
    if (!title) return;
    // Resolve the active kind: explicit opts.kind wins, else stored
    // preference, else fallback to scene. Whatever resolves becomes the
    // initial state of the in-popup toggle.
    const explicitKind = (o.kind === 'movie' || o.kind === 'scene') ? o.kind : null;
    _activeKind = explicitKind || _readStoredKind() || 'scene';
    _highlightedIdx = -1;
    _queuedTitlesNorm = new Set();
    const overlay = document.getElementById('tsProwlarrPopup');
    const headerEl = document.getElementById('tsProwlarrPopupHeader');
    const statusEl = document.getElementById('tsProwlarrPopupStatus');
    const resultsEl = document.getElementById('tsProwlarrPopupResults');
    const filtersEl = document.getElementById('tsProwlarrPopupFilters');
    if (filtersEl) {
      filtersEl.innerHTML = '';
      filtersEl.hidden = true;
    }
    headerEl.innerHTML = `
      <span class="ts-prowlarr-popup-title">PROWLARR: ${_esc(title)}</span>
      <div class="ts-prowlarr-kind-toggle" role="group" aria-label="Result kind">
        <button type="button" data-kind="scene" class="${_activeKind === 'scene' ? 'is-active' : ''}">Scene</button>
        <button type="button" data-kind="movie" class="${_activeKind === 'movie' ? 'is-active' : ''}">Movie</button>
      </div>`;
    headerEl.querySelectorAll('.ts-prowlarr-kind-toggle button').forEach((btn) => {
      btn.addEventListener('click', () => {
        _activeKind = btn.dataset.kind === 'movie' ? 'movie' : 'scene';
        _writeStoredKind(_activeKind);
        headerEl.querySelectorAll('.ts-prowlarr-kind-toggle button').forEach((b) => {
          b.classList.toggle('is-active', b.dataset.kind === _activeKind);
        });
      });
    });
    statusEl.textContent = 'Searching Prowlarr…';
    resultsEl.innerHTML = '<div class="ts-prowlarr-popup-empty">Please wait…</div>';
    overlay.classList.add('open');
    // Kick off active-queue lookup in parallel — we don't block search
    // results on it, but it usually arrives first and we re-paint badges
    // as soon as it lands. Pulls from both /api/downloads (download
    // client queues) and /api/queue (source-folder waiting room) so any
    // release name the user already has in flight gets badged.
    _refreshQueuedTitles().catch(() => {});
    const myToken = ++_searchToken;
    try {
      let d;
      // Pre-loaded path: when the caller already has matched releases
      // (e.g. an RSS-feed tile click on /downloads — the backend's
      // RSS aggregator runs the matching pass during feed fetch, so
      // by the time the tile renders we already know which releases
      // the user can grab), skip the live Prowlarr query and render
      // them directly. Indexer search tokenises `q=` on whitespace
      // and requires every token to hit, so long descriptive RSS
      // titles routinely returned "0 releases found" even though a
      // perfectly matching release was sitting on the tile already.
      if (Array.isArray(o.preloadedResults) && o.preloadedResults.length) {
        d = { results: o.preloadedResults };
      } else {
        const params = new URLSearchParams();
        params.append('q', title);
        // Person/place-name searches benefit from a dot-joined fan-out
        // because indexers tokenise q= on whitespace and otherwise return
        // releases that contain *either* name part. Release filenames use
        // dots (`Firstname.Lastname.XXX...`) so the dotted form keeps the
        // name as a single matchable token. Callers opt in via
        // `o.dotVariant: true` (post-add-performer flow, etc.). Backend
        // dedupes by guid so the extra query never adds duplicate rows.
        if (o.dotVariant && /\s/.test(title)) {
          params.append('q', title.replace(/\s+/g, '.'));
        }
        const perfStr = (o.performers || '').trim();
        const studio = (o.studio || '').trim();
        if (perfStr && studio) {
          const studioQ = `${perfStr} ${studio}`.trim();
          if (studioQ.toLowerCase() !== title.toLowerCase()) params.append('q', studioQ);
        }
        const r = await fetch('/api/prowlarr/search?' + params.toString(), { credentials: 'same-origin' });
        d = await r.json();
      }
      if (myToken !== _searchToken) return;
      if (d && d.error) {
        statusEl.textContent = d.error;
        resultsEl.innerHTML = '';
        if (filtersEl) {
          filtersEl.innerHTML = '';
          filtersEl.hidden = true;
        }
        return;
      }
      const releases = (d && d.results) || [];
      _prowlarrAllReleases = releases.slice(0, 50);
      _prowlarrFilterSpec = null;
      _prowlarrChipOpts = Object.assign({}, o, {
        title,
        studio: (o.studio || '').trim(),
        performers: (o.performers || '').trim(),
      });
      _prowlarrResultToken = myToken;
      if (!_prowlarrAllReleases.length) {
        _activeReleases = [];
        if (filtersEl) {
          filtersEl.innerHTML = '';
          filtersEl.hidden = true;
        }
        statusEl.innerHTML = '<strong>0</strong> releases found';
        resultsEl.innerHTML = '<div class="ts-prowlarr-popup-empty">No releases matched this title on your configured indexers.</div>';
        return;
      }
      _prowlarrPaintResultTable(statusEl, filtersEl, resultsEl, _popupCtx(), myToken);
    } catch (e) {
      if (myToken !== _searchToken) return;
      statusEl.textContent = 'Search error: ' + (e.message || e);
      resultsEl.innerHTML = '';
      const fe = document.getElementById('tsProwlarrPopupFilters');
      if (fe) {
        fe.innerHTML = '';
        fe.hidden = true;
      }
    }
  };
})();

// ── Global toast → header activity banner ───────────────────────
// Routes to TsActivity.pushAlert (icon dot, message on hover, click
// to dismiss). Queues until activity-banner.js loads if needed.
//
// Usage:
//   toast('Filed.');
//   toast('Filing failed', { kind: 'error' });
//   toast('Network error', { kind: 'error', action: { label: 'Retry', callback: doRetry } });
(function () {
  var _pendingToasts = [];

  function flushPendingToasts() {
    if (!window.TsActivity || typeof window.TsActivity.pushAlert !== 'function') return;
    while (_pendingToasts.length) {
      var item = _pendingToasts.shift();
      window.TsActivity.pushAlert(item.message, item.opts);
    }
  }

  function toast(message, opts) {
    if (!message) return null;
    opts = opts || {};
    if (window.TsActivity && typeof window.TsActivity.pushAlert === 'function') {
      return window.TsActivity.pushAlert(message, opts);
    }
    _pendingToasts.push({ message: message, opts: opts });
    if (_pendingToasts.length > 32) _pendingToasts.shift();
    if (typeof console !== 'undefined' && console.warn) {
      console.warn('[toast]', message);
    }
    return null;
  }

  window.toast = toast;
  window.__tsFlushPendingToasts = flushPendingToasts;
  document.addEventListener('DOMContentLoaded', flushPendingToasts);
})();

/** Wide combined front+back cover art: crop portrait slots with object-position (right = front, left = back). */
(function () {
  var SPLIT_CLASS_FRONT = 'movie-poster--split-front';
  var SPLIT_CLASS_BACK = 'movie-poster--split-back';
  var DEFAULT_MIN_ASPECT = 1.34;
  var FORCED_MIN_ASPECT = 1.05;

  function clearSplitClasses(el) {
    if (!el || !el.classList) return;
    el.classList.remove(SPLIT_CLASS_FRONT, SPLIT_CLASS_BACK);
  }

  /**
   * @param {HTMLImageElement} img
   * @param {'front'|'back'|'solo'} side — solo = single slot (prefer right crop when wide)
   * @param {{ force?: boolean }} [opts] — force=true when API marks combined spread (lower aspect threshold)
   */
  function tsApplyMovieCoverSplit(img, side, opts) {
    if (!img || !img.naturalWidth || !img.naturalHeight) return;
    opts = opts || {};
    var w = img.naturalWidth;
    var h = img.naturalHeight;
    var force = !!opts.force || img.getAttribute('data-force-split') === '1';
    var minA = force ? FORCED_MIN_ASPECT : DEFAULT_MIN_ASPECT;
    if ((w / h) < minA) {
      clearSplitClasses(img);
      return;
    }
    clearSplitClasses(img);
    if (side === 'back') {
      img.classList.add(SPLIT_CLASS_BACK);
    } else {
      img.classList.add(SPLIT_CLASS_FRONT);
    }
  }
  window.tsApplyMovieCoverSplit = tsApplyMovieCoverSplit;
})();

/** Local PNGs for the movie/JAV detail popup (cassette frame, paper overlay, sticker).
 *  Loads one file per idle slice so feed posters and API calls aren't starved.
 *  opts.immediate — start the queue now (e.g. hover / popup open), still low priority.
 *  opts.defer — when true (default), wait for requestIdleCallback before the first file. */
window.tsPreloadMovieDetailChrome = (function () {
  var urls = [
    '/static/img/vhs.webp',
    '/static/img/vhsoverlay.webp',
    '/static/img/sticker.webp',
  ];
  var queued = false;
  var pumping = false;
  var index = 0;

  function scheduleNext() {
    if (index >= urls.length) return;
    if (pumping) return;
    pumping = true;
    var run = function () {
      pumping = false;
      if (index >= urls.length) return;
      var src = urls[index++];
      var img = new Image();
      if ('fetchPriority' in HTMLImageElement.prototype) {
        try { img.fetchPriority = 'low'; } catch (_) {}
      }
      img.decoding = 'async';
      var done = function () {
        if (typeof requestIdleCallback === 'function') {
          requestIdleCallback(function () { scheduleNext(); }, { timeout: 10000 });
        } else {
          setTimeout(scheduleNext, 80);
        }
      };
      img.onload = done;
      img.onerror = done;
      img.src = src;
    };
    if (typeof requestIdleCallback === 'function') {
      requestIdleCallback(run, { timeout: 10000 });
    } else {
      setTimeout(run, 80);
    }
  }

  return function tsPreloadMovieDetailChrome(opts) {
    opts = opts || {};
    if (opts.immediate) {
      if (!queued) {
        queued = true;
        index = 0;
      }
      scheduleNext();
      return;
    }
    if (queued && !opts.force) return;
    queued = true;
    index = 0;
    var delay = typeof opts.delay === 'number' ? opts.delay : 0;
    var run = function () { scheduleNext(); };
    if (delay > 0) {
      setTimeout(run, delay);
      return;
    }
    if (typeof requestIdleCallback === 'function') {
      requestIdleCallback(run, { timeout: opts.timeout || 12000 });
    } else {
      setTimeout(run, 4000);
    }
  };
})();

/* Debounced-input dispatcher — coalesces rapid keystrokes on filter
 * boxes so the table/grid doesn't full-render on every character.
 *
 *   <input oninput="tsDebouncedInput('favFilter', render, 100)">
 *
 * Each (key, fn) pair keeps its own timer. ms defaults to 100, which
 * matches "feels instant" perception while skipping ~95% of the
 * intermediate renders during a typed query. */
window.tsDebouncedInput = (function () {
  var timers = new Map();
  return function (key, fn, ms) {
    if (typeof ms !== 'number') ms = 100;
    var prev = timers.get(key);
    if (prev) clearTimeout(prev);
    timers.set(key, setTimeout(function () { timers.delete(key); fn(); }, ms));
  };
})();

/* Glass-style confirm dialog. Drop-in replacement for window.confirm()
 * that returns a Promise<boolean>. Matches the rest of the modal
 * chrome so destructive actions don't fall back to the browser's
 * native dialog (which doesn't honour the dark theme).
 *
 *   const ok = await tsConfirm('Delete this row?');
 *   if (ok) doDelete();
 *
 * Options:
 *   {
 *     title:   'Confirm'           // header text
 *     confirm: 'Confirm'           // OK button label
 *     cancel:  'Cancel'            // cancel button label
 *     destructive: true            // styles confirm as red/danger
 *   }
 */
window.tsConfirm = function (message, opts) {
  opts = opts || {};
  return new Promise(function (resolve) {
    var ESC = function (s) {
      return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
        return ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]);
      });
    };
    var overlay = document.createElement('div');
    overlay.className = 'modal-overlay open ts-confirm-overlay';
    overlay.style.zIndex = '1900';
    var destructive = !!opts.destructive;
    var confirmBg = destructive
      ? 'rgba(239,68,68,0.32)' : 'rgba(var(--brand-purple-rgb),0.4)';
    var confirmBorder = destructive
      ? 'rgba(239,68,68,0.7)' : 'rgba(var(--brand-purple-rgb),0.65)';
    var confirmColor = destructive ? '#fca5a5' : 'var(--accent)';
    overlay.innerHTML =
      '<div class="modal-box ts-confirm-box" style="max-width:480px">' +
        '<h3 style="margin:0 0 12px 0;font-family:var(--font-display,var(--mono));font-size:17px">' +
          ESC(opts.title || 'Confirm') +
        '</h3>' +
        '<div class="ts-confirm-msg" style="font-size:13px;line-height:1.5;color:var(--text);margin-bottom:18px;white-space:pre-wrap">' +
          ESC(message) +
        '</div>' +
        '<div class="ts-link-modal-foot">' +
          '<button type="button" class="ts-confirm-cancel" ' +
            'style="background:transparent;border:1px solid rgba(255,255,255,0.15);' +
            'color:var(--dim);padding:8px 16px;border-radius:6px;font-size:11px;' +
            'font-family:var(--mono);cursor:pointer;text-transform:uppercase;' +
            'letter-spacing:0.04em">' +
            ESC(opts.cancel || 'Cancel') +
          '</button>' +
          '<button type="button" class="ts-confirm-ok" ' +
            'style="background:' + confirmBg + ';border:1px solid ' + confirmBorder +
            ';color:' + confirmColor + ';padding:8px 16px;border-radius:6px;' +
            'font-size:11px;font-family:var(--mono);cursor:pointer;' +
            'text-transform:uppercase;letter-spacing:0.04em">' +
            ESC(opts.confirm || 'Confirm') +
          '</button>' +
        '</div>' +
      '</div>';
    function done(ok) {
      try { overlay.remove(); } catch (_) {}
      document.removeEventListener('keydown', onKey);
      resolve(!!ok);
    }
    function onKey(e) {
      if (e.key === 'Escape') { e.preventDefault(); done(false); }
      else if (e.key === 'Enter') { e.preventDefault(); done(true); }
    }
    document.body.appendChild(overlay);
    overlay.addEventListener('click', function (e) {
      if (e.target === overlay) done(false);
    });
    overlay.querySelector('.ts-confirm-cancel').addEventListener('click', function () { done(false); });
    overlay.querySelector('.ts-confirm-ok').addEventListener('click', function () { done(true); });
    document.addEventListener('keydown', onKey);
    // Focus the OK button so Enter confirms by default (matches
    // native confirm() muscle memory).
    setTimeout(function () {
      var ok = overlay.querySelector('.ts-confirm-ok');
      if (ok && typeof ok.focus === 'function') ok.focus();
    }, 0);
  });
};

/* ── Shared Wanted-list helpers ──────────────────────────────────────
 * Used to be scoped to scenes-common.js (loaded only on /scenes); now
 * lives in ts-utils.js so every page that renders scene/movie tiles
 * (studio popup, performer popup, movie popup, …) can drop a hover
 * "watch" eye onto each tile without duplicating state.
 *
 * Surface:
 *   window.tsWantedKeys           — Set of "${kind}:${source}:${id}"
 *   window.tsWantedKey(k, s, id)  — normalise to that key shape
 *   window.tsLoadWantedKeys()     — populate from /api/wanted/keys (once)
 *   window.tsDeriveWantedAttrs(scene, defaultKind)
 *   window.tsBuildWantedBtnHtml(scene, defaultKind)
 *
 * One document-level click handler debounces concurrent toggles per key
 * and optimistically flips state for every matching button on the page.
 */
(function () {
  if (window._tsWantedInit) return;
  window._tsWantedInit = true;

  window.tsWantedKeys = new Set();
  window.tsWantedKey = function (kind, source, externalId) {
    return (
      (kind || 'scene').toLowerCase() + ':' +
      (source || '').toLowerCase() + ':' +
      (externalId || '')
    );
  };

  var _wantedLoaded = false;
  var _wantedLoadPromise = null;
  window.tsLoadWantedKeys = function (force) {
    if (_wantedLoaded && !force) return Promise.resolve();
    if (_wantedLoadPromise && !force) return _wantedLoadPromise;
    _wantedLoadPromise = fetch('/api/wanted/keys')
      .then(function (r) { return r.json(); })
      .then(function (d) {
        (d && d.keys || []).forEach(function (k) {
          window.tsWantedKeys.add(window.tsWantedKey(k.kind, k.source, k.external_id));
        });
        _wantedLoaded = true;
      })
      .catch(function () {});
    return _wantedLoadPromise;
  };

  /** Pull kind/source/externalId out of a scene-like row. Returns null
   * when we can't resolve an external_id — caller should skip rendering
   * the button in that case (toggling without an id is meaningless). */
  window.tsDeriveWantedAttrs = function (scene, defaultKind) {
    if (!scene) return null;
    var rawSrc = String(scene.source || '').toLowerCase();
    var norm = rawSrc.indexOf('search_') === 0 ? rawSrc.slice(7) : rawSrc;
    var source = norm.indexOf('tpdb') !== -1 ? 'tpdb'
      : norm === 'stashdb' ? 'stashdb'
      : norm === 'fansdb' ? 'fansdb'
      : norm === 'javstash' ? 'javstash'
      : 'tpdb';
    var externalId = (
      source === 'stashdb'  ? (scene.stash_id    || scene.id || '') :
      source === 'fansdb'   ? (scene.fansdb_id   || scene.id || '') :
      source === 'javstash' ? (scene.javstash_id || scene.id || '') :
                              (scene.tpdb_id     || scene.id || '')
    );
    if (!externalId) return null;
    var kind = defaultKind || 'scene';
    return { kind: kind, source: source, externalId: String(externalId) };
  };

  window.tsBuildWantedBtnHtml = function (scene, defaultKind) {
    var attrs = window.tsDeriveWantedAttrs(scene, defaultKind);
    if (!attrs) return '';
    var wkey = window.tsWantedKey(attrs.kind, attrs.source, attrs.externalId);
    var on = window.tsWantedKeys.has(wkey);
    var title = on ? 'In your Wanted list — click to remove' : 'Add to Wanted';
    return '<button class="scene-wanted-btn' + (on ? ' is-wanted' : '') +
      '" data-wanted-kind="' + esc(attrs.kind) +
      '" data-wanted-source="' + esc(attrs.source) +
      '" data-wanted-id="' + esc(attrs.externalId) +
      '" title="' + esc(title) +
      '" aria-pressed="' + (on ? 'true' : 'false') +
      '" onclick="event.stopPropagation()"><i class="fa-solid fa-eye"></i></button>';
  };

  /** One document-level click handler — runs on every page that loads
   * ts-utils.js. Scenes-common.js no longer registers its own. */
  var _inflightWanted = new Set();
  document.addEventListener('click', function (e) {
    var btn = e.target && e.target.closest && e.target.closest('.scene-wanted-btn');
    if (!btn) return;
    e.preventDefault();
    e.stopPropagation();
    var kind = btn.getAttribute('data-wanted-kind') || 'scene';
    var source = btn.getAttribute('data-wanted-source') || 'tpdb';
    var externalId = btn.getAttribute('data-wanted-id') || '';
    if (!externalId) return;
    var wkey = window.tsWantedKey(kind, source, externalId);
    if (_inflightWanted.has(wkey)) return;
    _inflightWanted.add(wkey);

    var card = btn.closest('.scene-card, .movie-card');
    var grid = window._sceneGridItems;
    var idx = card ? parseInt(card.getAttribute('data-scene-i') || '-1', 10) : -1;
    var scene = (!isNaN(idx) && idx >= 0 && grid && grid[idx]) ? grid[idx] : null;
    var titleFromCard = card
      ? (card.querySelector('.scene-title')?.textContent
         || card.querySelector('.movie-title')?.textContent
         || '')
      : '';
    var thumbFromCard = card
      ? (card.querySelector('.scene-thumb')?.src
         || card.querySelector('.movie-poster')?.src
         || '')
      : '';
    var payload = {
      kind: kind, source: source, external_id: externalId,
      title:       (scene && scene.title)       || titleFromCard,
      studio:      (scene && scene.studio)      || '',
      date:        (scene && scene.date)        || '',
      performers:  (scene && scene.performer)   || '',
      thumb:       (scene && scene.thumb)       || thumbFromCard,
      description: (scene && scene.description) || '',
      tags:        (scene && Array.isArray(scene.tags)) ? scene.tags : [],
      duration:    (scene && scene.duration)    || 0,
    };

    var wasOn = window.tsWantedKeys.has(wkey);
    var applyState = function (on) {
      if (on) window.tsWantedKeys.add(wkey); else window.tsWantedKeys.delete(wkey);
      document.querySelectorAll('.scene-wanted-btn[data-wanted-id="' + CSS.escape(externalId) + '"]').forEach(function (b) {
        b.classList.toggle('is-wanted', !!on);
        b.setAttribute('aria-pressed', on ? 'true' : 'false');
        b.setAttribute('title', on ? 'In your Wanted list — click to remove' : 'Add to Wanted');
      });
    };
    applyState(!wasOn);

    fetch('/api/wanted/toggle', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    })
      .then(function (r) { return r.json(); })
      .then(function (d) {
        if (d && typeof d.wanted === 'boolean') applyState(d.wanted);
      })
      .catch(function () { applyState(wasOn); })
      .finally(function () { _inflightWanted.delete(wkey); });
  });
})();
