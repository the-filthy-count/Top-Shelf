/** Top-Shelf settings page logic. */
  function _settingsCancelRevert() {
    Object.keys(_tagPickerTimers).forEach((k) => {
      clearTimeout(_tagPickerTimers[k]);
      delete _tagPickerTimers[k];
    });
    if (typeof _uiThemeOriginal !== 'undefined' && _uiThemeOriginal && window.setUiTheme) {
      window.setUiTheme(_uiThemeOriginal, _customSpecOriginal || undefined);
    }
    if (window.setAccentOverride) {
      window.setAccentOverride(_accentOverrideOriginal || null);
    }
  }

  function settingsLeavePage() {
    _settingsCancelRevert();
    if (window.history.length > 1) {
      window.history.back();
      return;
    }
    window.location.href = '/downloads';
  }
  window.settingsLeavePage = settingsLeavePage;

  async function settingsLogout() {
    await fetch('/api/auth/logout', { method: 'POST' });
    window.location.href = '/login';
  }
  window.settingsLogout = settingsLogout;
  window.logout = settingsLogout;

  let _settingsSaveToastTimer = null;
  let _monitoredTags = [];   // legacy flat list — kept in sync from _vices on save
  let _blacklistTags = [];
  let _vices = [];           // [{id, name, tags:[{id,slug,name,tpdb_id?,stashdb_id?}]}]
  let _vicesLoaded = false;  // guard: do not POST empty vices before /api/vices fetch completes
  let _settingsPageLoaded = false;  // block save until loadSettingsPage finishes
  let dirs = [];  // performer directory rows from GET /api/settings
  // Each vice's per-row tag search hits StashDB — no module-level
  // store — but we keep the active search's row index here so the shared
  // runner knows which result container to populate.
  const _tagPickerTimers = {};

  function _escHtml(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }
  /* Don't redeclare `esc` — ts-utils.js already exports it at script scope, and
     redeclaring throws SyntaxError, which kills every handler below this point. */

  function _parseTagListSetting(raw) {
    // New format: JSON array of {id, slug, name}. Legacy format: comma-
    // separated names (tag_blacklist pre-picker). Migrate legacy entries
    // to objects with id=slug=name=lowercase-text so the picker can render
    // chips; the backend will accept both shapes.
    const s = (raw || '').trim();
    if (!s) return [];
    if (s.charAt(0) === '[') {
      try {
        const p = JSON.parse(s);
        if (Array.isArray(p)) {
          return p
            .filter(t => t && typeof t === 'object' && (t.name || t.id))
            .map(t => ({
              id: String(t.id || t.slug || t.name || ''),
              slug: String(t.slug || ''),
              name: String(t.name || t.id || ''),
              tpdb_id:     String(t.tpdb_id || '').trim(),
              stashdb_id:  String(t.stashdb_id || t.stash_id || '').trim(),
              fansdb_id:   String(t.fansdb_id || '').trim(),
              javstash_id: String(t.javstash_id || '').trim(),
            }));
        }
      } catch(_) {}
    }
    return s.split(',')
      .map(x => x.trim())
      .filter(Boolean)
      .map(name => ({ id: name.toLowerCase(), slug: '', name,
        tpdb_id: '', stashdb_id: '', fansdb_id: '', javstash_id: '' }));
  }

  // Render a 2x2 grid of fixed-height scrollable boxes, one per source.
  // Each box lists the tags blacklisted for that source, one per row,
  // name on the left and × remove button right-aligned. `onRemove(name,
  // source)` is invoked when the user clicks ×.
  function _tagPickerRenderSourceGrid(list, containerId, onRemove, opts) {
    const wrap = document.getElementById(containerId);
    if (!wrap) return;
    //: ``opts.emptyVerb`` lets callers swap the wording shown when a
    //: source bucket has no tags. The blacklist picker uses
    //: ``"blocked"`` (the original / default) since the entries
    //: actually do block content; the vice picker passes
    //: ``"assigned"`` because vice tags drive filing, they don't
    //: block anything. Without this, the vice card was misleading.
    const emptyVerb = (opts && opts.emptyVerb) || 'blocked';
    const rowsBySource = {};
    _TAG_SOURCES.forEach(s => {
      rowsBySource[s.key] = (list || [])
        .filter(t => _tagHasSource(t, s.key))
        .sort((a, b) => String(a.name || '').localeCompare(String(b.name || ''), undefined, { sensitivity: 'base' }));
    });
    wrap.innerHTML = _TAG_SOURCES.map(s => {
      const rows = rowsBySource[s.key];
      const body = rows.length
        ? rows.map(t => `<div class="ts-tag-source-row" data-name="${_escHtml(t.name)}" data-src="${s.key}">` +
            `<span class="ts-tag-source-row__name">${_escHtml(_tagChipLabel(t))}</span>` +
            `<button type="button" class="ts-tag-source-row__remove" title="Remove from ${s.label}" aria-label="Remove ${_escHtml(t.name)} from ${s.label}">&times;</button>` +
          `</div>`).join('')
        : `<div class="ts-tag-source-empty">No ${s.label} tags ${emptyVerb}.</div>`;
      return `<div class="ts-tag-source-box" data-src="${s.key}">` +
        `<div class="ts-tag-source-box__head">` +
          `<img class="ts-tag-source-box__logo" src="${_escHtml(s.logo)}" alt="${_escHtml(s.label)}">` +
          `<span class="ts-tag-source-box__label">${_escHtml(s.label)}</span>` +
          `<span class="ts-tag-source-box__count">${rows.length}</span>` +
        `</div>` +
        `<div class="ts-tag-source-box__body">${body}</div>` +
        `</div>`;
    }).join('');
    if (!wrap._removeDelegated) {
      wrap._removeDelegated = true;
      wrap.addEventListener('click', (ev) => {
        const btn = ev.target.closest('button.ts-tag-source-row__remove');
        if (!btn || !wrap.contains(btn)) return;
        const row = btn.closest('.ts-tag-source-row');
        if (!row) return;
        const name = row.getAttribute('data-name') || '';
        const src = row.getAttribute('data-src') || '';
        if (name && src && wrap._removeHandler) wrap._removeHandler(name, src);
      });
    }
    wrap._removeHandler = onRemove;
  }

  function renderBlacklistTagChips() {
    _tagPickerRenderSourceGrid(
      _blacklistTags, 'cfgBlacklistTagChips',
      (name, src) => {
        _removeTagSource(_blacklistTags, { name }, src);
        renderBlacklistTagChips();
      }
    );
  }

  // ── Vice management ─────────────────────────────────────
  // Vices are named entities with their own folder under vices_dir.
  // Each vice groups zero-or-more tags (TPDB + StashDB ids when both exist).
  // `_vices` is the edit-time
  // state; it's persisted via POST /api/vices on save, which rewrites
  // vices_json and keeps monitored_tags in sync for feed/blacklist use.

  // Client no longer synthesises vice ids — the backend's POST /api/vices
  // handler assigns the next integer id to any vice row that arrives
  // without one. This keeps `metadata/vices/{id}/` aligned with the
  // pattern used by performers / studios / movies (integer PKs) and
  // means renaming a vice never orphans its metadata folder.

  function renderVicesList() {
    const wrap = document.getElementById('cfgVicesList');
    if (!wrap) return;
    // Keep the source list in alphabetical order so add / rename / delete
    // always see the same order the user sees on screen, and so the
    // persisted vices_json matches /library's A-Z tile order.
    _vices.sort((a, b) => (a.name || '').toLowerCase().localeCompare((b.name || '').toLowerCase()));
    if (!_vices.length) {
      wrap.innerHTML = '<div style="font-size:13px;color:var(--dim);font-style:italic;padding:10px 0">No vices yet — add one above.</div>';
      return;
    }
    wrap.innerHTML = _vices.map((v, i) => _viceCardHtml(v, i)).join('');
    // Click-to-expand on the card head. Body is hidden by default —
    // adding `.is-open` flips the chevron and reveals the grid + search.
    wrap.querySelectorAll('.vice-card-toggle').forEach(btn => {
      btn.addEventListener('click', (ev) => {
        ev.stopPropagation();
        const card = btn.closest('.vice-card');
        if (!card) return;
        card.classList.toggle('is-open');
      });
    });
    // Wire per-row search inputs. Each card has a tag search input with
    // id `cfgViceTagSearch_${i}` and a results panel `cfgViceTagResults_${i}`.
    _vices.forEach((v, i) => {
      const input = document.getElementById(`cfgViceTagSearch_${i}`);
      if (!input) return;
      let timer = null;
      const resultsEl = document.getElementById(`cfgViceTagResults_${i}`);

      function _syncViceChips() {
        const card = input.closest('.vice-card');
        if (!card) return;
        // Re-render the per-source counts in the collapsed header and
        // the 2x2 grid inside the expanded body.
        const counts = _viceSourceCounts(_vices[i].tags || []);
        _TAG_SOURCES.forEach(s => {
          const el = card.querySelector(`.vice-count[data-src="${s.key}"]`);
          if (el) el.textContent = String(counts[s.key]);
        });
        const gridId = `cfgViceTagGrid_${i}`;
        const grid = card.querySelector(`#${gridId}`);
        if (grid) {
          _tagPickerRenderSourceGrid(_vices[i].tags || [], gridId,
            (name, src) => {
              _removeTagSource(_vices[i].tags, { name }, src);
              _syncViceChips();
            },
            { emptyVerb: 'assigned' });
        }
      }

      function _runViceSearch() {
        const q = input.value.trim();
        if (!q) { resultsEl.style.display = 'none'; return; }
        _runTagSearch(q, resultsEl, _vices[i].tags,
          (tag, source) => {
            _mergeTagPick(_vices[i].tags, tag, source);
            _syncViceChips();
            _runViceSearch();
            input.focus();
          },
          (tag, source) => {
            if (source) _removeTagSource(_vices[i].tags, tag, source);
            else _removeTagFromList(_vices[i].tags, tag);
            _syncViceChips();
            _runViceSearch();
            input.focus();
          }
        );
      }

      input.addEventListener('input', () => {
        clearTimeout(timer);
        timer = setTimeout(_runViceSearch, 220);
      });
      document.addEventListener('click', (e) => {
        if (e.target === input || (resultsEl && resultsEl.contains(e.target))) return;
        if (resultsEl) resultsEl.style.display = 'none';
      });
      // Initial paint — _viceCardHtml leaves the grid container empty
      // so the renderer can wire its own delegated remove handler.
      _syncViceChips();
    });
  }

  function _viceSourceCounts(tags) {
    const out = { tpdb: 0, stashdb: 0, fansdb: 0, javstash: 0 };
    (tags || []).forEach(t => {
      _TAG_SOURCES.forEach(s => { if (_tagHasSource(t, s.key)) out[s.key] += 1; });
    });
    return out;
  }

  function _viceCardHtml(v, i) {
    const counts = _viceSourceCounts(v.tags || []);
    const countPills = _TAG_SOURCES.map(s =>
      `<span class="vice-count-pill"><span class="vice-count-pill__label">${_escHtml(s.label)}</span><span class="vice-count" data-src="${s.key}">${counts[s.key]}</span></span>`
    ).join('');
    return `<div class="vice-card">
      <div class="vice-card-head">
        <button type="button" class="vice-card-toggle" data-vice-idx="${i}" title="Expand">
          <i class="fa-solid fa-chevron-right vice-card-chevron"></i>
        </button>
        <i class="fa-solid fa-fire vice-card-icon"></i>
        <input type="text" class="field-input vice-card-name" value="${_escHtml(v.name)}" onchange="renameVice(${i}, this.value)" title="Rename">
        <div class="vice-card-counts">${countPills}</div>
        <button type="button" class="btn-secondary vice-card-delete" onclick="deleteVice(${i})" title="Delete vice"><i class="fa-solid fa-trash"></i></button>
      </div>
      <div class="vice-card-body" data-vice-idx="${i}">
        <div class="vice-card-search">
          <input class="field-input" id="cfgViceTagSearch_${i}" type="text" placeholder="Search TPDB, StashDB, FansDB &amp; JAVStash tags…" autocomplete="off">
          <div class="ts-tag-results" id="cfgViceTagResults_${i}" style="display:none"></div>
        </div>
        <div id="cfgViceTagGrid_${i}" class="ts-tag-source-grid"></div>
      </div>
    </div>`;
  }

  function addVice() {
    const input = document.getElementById('cfgNewViceName');
    const name = (input?.value || '').trim();
    if (!name) return;
    if (_vices.some(v => String(v.name || '').toLowerCase() === name.toLowerCase())) {
      input.value = '';
      return;
    }
    // No id — backend assigns the next integer on save.
    _vices.push({ name, tags: [] });
    input.value = '';
    renderVicesList();
  }

  function deleteVice(index) {
    if (index < 0 || index >= _vices.length) return;
    if (!confirm(`Delete vice "${_vices[index].name}"? The folder on disk is left alone.`)) return;
    _vices.splice(index, 1);
    renderVicesList();
  }

  function renameVice(index, newName) {
    if (index < 0 || index >= _vices.length) return;
    const trimmed = (newName || '').trim();
    if (!trimmed) return;
    _vices[index].name = trimmed;
    // Re-render so the list stays sorted after the rename lands.
    renderVicesList();
    // Note: server-side folder rename is NOT performed — if the user
    // renames a vice, a new folder is created under the new name and
    // the old one is left orphaned. Documented behaviour for now.
  }

  function removeTagFromVice(viceIndex, tagIndex) {
    if (viceIndex < 0 || viceIndex >= _vices.length) return;
    const tags = _vices[viceIndex].tags || [];
    if (tagIndex < 0 || tagIndex >= tags.length) return;
    tags.splice(tagIndex, 1);
    renderVicesList();
  }

  function _normTagName(s) {
    return String(s || '').trim().toLowerCase();
  }

  function _isStashTagUuid(id) {
    return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(String(id || '').trim());
  }

  function _tagChipLabel(t) {
    // Source breakdown is now shown by which per-source box the tag
    // lives in — no need to suffix names with [TPDB] / [StashDB] etc.
    return String(t.name || t.id || '').trim();
  }

  function _tagListHas(list, tag) {
    const norm = _normTagName(tag.name);
    return (list || []).some(t => {
      if (_normTagName(t.name) === norm) return true;
      if (tag.id && String(t.id) === String(tag.id)) return true;
      if (tag.tpdb_id && String(t.tpdb_id) === String(tag.tpdb_id)) return true;
      if (tag.stashdb_id && String(t.stashdb_id) === String(tag.stashdb_id)) return true;
      if (tag.fansdb_id && String(t.fansdb_id) === String(tag.fansdb_id)) return true;
      if (tag.javstash_id && String(t.javstash_id) === String(tag.javstash_id)) return true;
      return false;
    });
  }

  // True if the row carries an id for `source` (tpdb / stashdb / fansdb / javstash).
  function _tagHasSource(t, source) {
    if (!t) return false;
    if (source === 'tpdb')     return !!String(t.tpdb_id || '').trim();
    if (source === 'stashdb')  return !!String(t.stashdb_id || '').trim();
    if (source === 'fansdb')   return !!String(t.fansdb_id || '').trim();
    if (source === 'javstash') return !!String(t.javstash_id || '').trim();
    return false;
  }

  function _clearTagSource(t, source) {
    if (!t) return;
    if (source === 'tpdb')     t.tpdb_id = '';
    if (source === 'stashdb')  t.stashdb_id = '';
    if (source === 'fansdb')   t.fansdb_id = '';
    if (source === 'javstash') t.javstash_id = '';
  }

  // Remove a row entirely (used when the user clicks 'remove' on the
  // last source for that name) — drops the row from the underlying list.
  function _removeTagFromList(list, tag) {
    const norm = _normTagName(tag.name);
    const idx = (list || []).findIndex(t =>
      _normTagName(t.name) === norm ||
      (tag.id && String(t.id) === String(tag.id))
    );
    if (idx >= 0) list.splice(idx, 1);
  }

  // Remove just one source id for a row. If no sources remain, drop
  // the row entirely so the per-source boxes don't keep a ghost entry.
  function _removeTagSource(list, tag, source) {
    const norm = _normTagName(tag.name);
    const idx = (list || []).findIndex(t => _normTagName(t.name) === norm);
    if (idx < 0) return;
    _clearTagSource(list[idx], source);
    const anyLeft =
      _tagHasSource(list[idx], 'tpdb') ||
      _tagHasSource(list[idx], 'stashdb') ||
      _tagHasSource(list[idx], 'fansdb') ||
      _tagHasSource(list[idx], 'javstash');
    if (!anyLeft) list.splice(idx, 1);
  }

  // Merge a search hit into the list. When `sourceFilter` is set, only
  // the matching id field is recorded — so picking 'add to TPDB' on a
  // grouped result that also has a StashDB id does NOT also blacklist
  // StashDB. Pass `sourceFilter = null` or `'all'` to merge every id.
  function _mergeTagPick(list, hit, sourceFilter) {
    const name = String(hit.name || '').trim();
    if (!name) return;
    const norm = _normTagName(name);
    let row = (list || []).find(t => _normTagName(t.name) === norm);
    if (!row) {
      row = { id: '', slug: '', name, tpdb_id: '', stashdb_id: '', fansdb_id: '', javstash_id: '' };
      list.push(row);
    }
    const wantAll = !sourceFilter || sourceFilter === 'all';
    const src = String(hit.source || '').toLowerCase();
    const fromSrc = (key) =>
      String(hit[`${key}_id`] || (src === key ? hit.id : '') || '').trim();
    if (wantAll || sourceFilter === 'tpdb') {
      const v = fromSrc('tpdb');     if (v) row.tpdb_id     = v;
    }
    if (wantAll || sourceFilter === 'stashdb') {
      const v = fromSrc('stashdb');  if (v) row.stashdb_id  = v;
    }
    if (wantAll || sourceFilter === 'fansdb') {
      const v = fromSrc('fansdb');   if (v) row.fansdb_id   = v;
    }
    if (wantAll || sourceFilter === 'javstash') {
      const v = fromSrc('javstash'); if (v) row.javstash_id = v;
    }
    if (hit.slug) row.slug = hit.slug;
    if (row.stashdb_id)      row.id = row.stashdb_id;
    else if (row.fansdb_id)  row.id = row.fansdb_id;
    else if (row.javstash_id) row.id = row.javstash_id;
    else if (row.tpdb_id)    row.id = row.tpdb_id;
    else if (hit.id)         row.id = String(hit.id);
  }

  function _tagSearchResultHit(el) {
    return {
      id: el.getAttribute('data-id') || '',
      slug: el.getAttribute('data-slug') || '',
      name: el.getAttribute('data-name') || '',
      source: el.getAttribute('data-source') || '',
      tpdb_id:     el.getAttribute('data-tpdb-id') || '',
      stashdb_id:  el.getAttribute('data-stashdb-id') || '',
      fansdb_id:   el.getAttribute('data-fansdb-id') || '',
      javstash_id: el.getAttribute('data-javstash-id') || '',
    };
  }

  // Source metadata used for the grouped result pills + per-source
  // box headers. `logo` is the brand image in /static/logos/ used in
  // the same recipe as the source toggle on /scenes.
  const _TAG_SOURCES = [
    { key: 'tpdb',     label: 'TPDB',     logo: '/static/logos/tpdb.webp' },
    { key: 'stashdb',  label: 'StashDB',  logo: '/static/logos/stashdb.webp' },
    { key: 'fansdb',   label: 'FansDB',   logo: '/static/logos/fansdb.webp' },
    { key: 'javstash', label: 'JAVStash', logo: '/static/logos/javstash.webp' },
  ];

  async function _runTagSearch(query, resultsEl, currentList, onPick, onUnpick) {
    const q = (query || '').trim();
    if (q.length < 2) {
      resultsEl.style.display = 'none';
      resultsEl.innerHTML = '';
      return;
    }
    try {
      const r = await fetch('/api/settings/tag-search?q=' + encodeURIComponent(q));
      const d = await r.json();
      const tags = (d && d.tags) || [];
      const errs = (d && d.errors) || [];
      if (!tags.length) {
        const missing = _TAG_SOURCES
          .filter(s => errs.includes(`no_${s.key}_key`))
          .map(s => s.label);
        const msg = missing.length
          ? `No tags found. Add API key${missing.length > 1 ? 's' : ''} under Databases: ${missing.join(', ')}.`
          : 'No tags found';
        resultsEl.innerHTML = `<div class="ts-tag-results__${errs.length ? 'error' : 'empty'}">${_escHtml(msg)}</div>`;
        resultsEl.style.display = 'block';
        return;
      }
      // Each `t` is now a grouped result: one entry per lowercase name,
      // with `sources[]` listing the source keys that returned a hit,
      // and per-source id fields populated. Render one row per group
      // with a pill per source + an 'All' pill.
      resultsEl.innerHTML = tags.map(t => {
        const name = t.name || '';
        const sources = Array.isArray(t.sources) ? t.sources : [];
        const tpdbId     = String(t.tpdb_id || '').trim();
        const stashId    = String(t.stashdb_id || '').trim();
        const fansId     = String(t.fansdb_id || '').trim();
        const javstashId = String(t.javstash_id || '').trim();
        const hit = {
          name: t.name, id: t.id || '', slug: t.slug || '', source: '',
          tpdb_id: tpdbId, stashdb_id: stashId,
          fansdb_id: fansId, javstash_id: javstashId,
        };
        const pills = _TAG_SOURCES.filter(s => sources.includes(s.key)).map(s => {
          // Per-row already-added state for THIS source — drives the
          // `.is-added` chip style and the click handler's toggle.
          const probe = { name: t.name, id: '', tpdb_id: '', stashdb_id: '', fansdb_id: '', javstash_id: '' };
          probe[`${s.key}_id`] = String(t[`${s.key}_id`] || '').trim();
          const row = (currentList || []).find(x => _normTagName(x.name) === _normTagName(t.name));
          const inSource = row && _tagHasSource(row, s.key);
          return `<button type="button" class="ts-tag-pill${inSource ? ' is-added' : ''}" data-src="${s.key}" data-action="toggle-source" title="${inSource ? 'Remove from ' + s.label : 'Add to ' + s.label}">${_escHtml(s.label)}</button>`;
        }).join('');
        const allPill = sources.length > 1
          ? `<button type="button" class="ts-tag-pill ts-tag-pill--all" data-action="add-all" title="Add to every source where this tag exists">All</button>`
          : '';
        const dataAttrs =
          ` data-id="${_escHtml(t.id || '')}" data-slug="${_escHtml(t.slug || '')}"` +
          ` data-name="${_escHtml(t.name)}"` +
          ` data-tpdb-id="${_escHtml(tpdbId)}" data-stashdb-id="${_escHtml(stashId)}"` +
          ` data-fansdb-id="${_escHtml(fansId)}" data-javstash-id="${_escHtml(javstashId)}"`;
        return `<div class="ts-tag-result ts-tag-result--grouped"${dataAttrs} role="option">` +
          `<span class="ts-tag-result__name">${_escHtml(name)}</span>` +
          `<span class="ts-tag-result__pills">${pills}${allPill}</span>` +
          `</div>`;
      }).join('');
      resultsEl.style.display = 'block';
      // Pill clicks: delegate so we don't bind 4 listeners per row.
      resultsEl.querySelectorAll('.ts-tag-result--grouped').forEach(row => {
        row.addEventListener('click', (ev) => {
          const btn = ev.target.closest('button.ts-tag-pill');
          if (!btn || !row.contains(btn)) return;
          ev.stopPropagation();
          const hit = _tagSearchResultHit(row);
          const action = btn.getAttribute('data-action');
          if (action === 'add-all') {
            if (onPick) onPick(hit, 'all');
            return;
          }
          const src = btn.getAttribute('data-src');
          if (!src) return;
          // Toggle: if this source is already on the row, remove it.
          const existing = (currentList || []).find(x => _normTagName(x.name) === _normTagName(hit.name));
          if (existing && _tagHasSource(existing, src)) {
            if (onUnpick) onUnpick(hit, src);
          } else if (onPick) {
            onPick(hit, src);
          }
        });
      });
    } catch(_) {
      resultsEl.style.display = 'none';
      resultsEl.innerHTML = '';
    }
  }

  function _wireTagPicker(inputId, resultsId, getList, onPick, onRender) {
    const input = document.getElementById(inputId);
    const results = document.getElementById(resultsId);
    if (!input || !results) return;
    function _afterTagToggle() {
      onRender();
      _refresh();
      input.focus();
    }
    function _refresh() {
      const v = input.value;
      if (v.trim().length >= 2) {
        _runTagSearch(v, results, getList(),
          (tag, source) => {
            _mergeTagPick(getList(), tag, source);
            onPick(tag);
            _afterTagToggle();
          },
          (tag, source) => {
            if (source) _removeTagSource(getList(), tag, source);
            else _removeTagFromList(getList(), tag);
            _afterTagToggle();
          }
        );
      }
    }
    input.addEventListener('input', function() {
      clearTimeout(_tagPickerTimers[inputId]);
      _tagPickerTimers[inputId] = setTimeout(_refresh, 220);
    });
    document.addEventListener('click', function(e) {
      if (e.target === input || results.contains(e.target)) return;
      results.style.display = 'none';
    });
  }

  document.addEventListener('DOMContentLoaded', function() {
    // Vice tag pickers are wired per-row by renderVicesList() since each
    // vice card has its own search input + results panel. Only the flat
    // blacklist picker uses the global wiring here.
    _wireTagPicker(
      'cfgBlacklistTagSearch', 'cfgBlacklistTagResults',
      () => _blacklistTags,
      () => {},
      renderBlacklistTagChips
    );
  });

  // Saving uses the centred overlay spinner; success/error use the global
  // activity toast when available (falls back to the overlay).
  function showSettingsSavedToast(message, opts) {
    opts = opts || {};
    const msg = String(message || 'Settings saved');
    const spinning = !!opts.spinner;
    const el = document.getElementById('settingsSavedToast');
    clearTimeout(_settingsSaveToastTimer);
    if (!spinning && typeof window.toast === 'function') {
      if (el) {
        el.classList.remove('visible', 'is-saving');
        el.innerHTML = '';
      }
      let kind = opts.kind;
      if (!kind) {
        const lower = msg.toLowerCase();
        kind = (lower.includes('fail') || lower.includes('error') || lower.includes('invalid'))
          ? 'error'
          : (lower.includes('saving') || lower.includes('loading') || lower.includes('wait'))
            ? 'info'
            : 'success';
      }
      window.toast(msg, { kind: kind });
      return;
    }
    if (!el) return;
    el.innerHTML = (spinning ? '<span class="loader loader--toast" role="status" aria-label="Loading"></span>' : '') +
      `<span class="settings-saved-toast__msg">${_escHtml(msg)}</span>`;
    el.classList.add('visible');
    el.classList.toggle('is-saving', spinning);
    if (!spinning) {
      _settingsSaveToastTimer = setTimeout(function () {
        el.classList.remove('visible');
        el.classList.remove('is-saving');
      }, 2200);
    }
  }
  // ── Settings ──────────────────────────────────────────────────────────

  function _setInputValue(id, value) {
    const el = document.getElementById(id);
    if (el) el.value = value == null ? '' : String(value);
  }

  function _setChecked(id, checked) {
    const el = document.getElementById(id);
    if (el) el.checked = !!checked;
  }

  async function _loadVicesFromServer() {
    _vicesLoaded = false;
    try {
      const rv = await fetch('/api/vices', { credentials: 'same-origin' });
      if (rv.ok) {
        const dv = await rv.json();
        _vices = Array.isArray(dv.vices) ? dv.vices : [];
        _vicesLoaded = true;
      } else {
        _vices = [];
      }
    } catch (_) {
      _vices = [];
    }
    try {
      renderVicesList();
    } catch (e) {
      console.error('Vices list render failed:', e);
    }
  }

  async function loadSettingsPage() {
    _settingsPageLoaded = false;
    try {
    const r = await fetch('/api/settings', { credentials: 'same-origin' });
    if (!r.ok) {
      throw new Error(r.status === 401 ? 'Not signed in — open /login first' : `Settings API HTTP ${r.status}`);
    }
    const d = await r.json();
    const s = d.settings || {};
    const rawList = (Array.isArray(d.directories) ? d.directories : [])
      .filter(x => String((x && x.type) || '').trim().toLowerCase() === 'performer');
    dirs = rawList.map(x => {
      let gf = x.gender_filters;
      if (!Array.isArray(gf) || gf.length === 0) gf = ['female', 'male', 'trans', 'other'];
      return Object.assign({}, x, { type: 'performer', gender_filters: gf });
    });
    dirs.sort((a, b) => (a.rank - b.rank) || String(a.path || '').localeCompare(String(b.path || '')));

    // Migrate legacy 'nord' → 'nord-dark' so the dropdown finds a match.
    let storedTheme = s.ui_theme || 'vhs';
    if (storedTheme === 'nord') storedTheme = 'nord-dark';
    if (storedTheme === 'instagram') storedTheme = 'sunset';
    document.getElementById('cfgUiTheme').value           = storedTheme;
    _uiThemeOriginal = storedTheme;
    try { _customSpecOriginal = s.ui_theme_custom_json ? JSON.parse(s.ui_theme_custom_json) : null; } catch(e) { _customSpecOriginal = null; }
    loadCustomThemeFields(_customSpecOriginal);
    toggleCustomThemeSection(document.getElementById('cfgUiTheme').value);

    // Accent override — single hex on top of the active theme. Empty
    // string clears, any valid hex applies. Cache for the cancel revert
    // path (we restore to whatever the modal opened with).
    _accentOverrideOriginal = (s.ui_accent_override || '').trim();
    const accentInput = document.getElementById('cfgUiAccentOverride');
    if (accentInput) {
      // If the saved override is a valid hex, show it; otherwise fall
      // back to the active theme's default --accent so the picker
      // mirrors what the page is actually painting with.
      const validOverride = /^#[0-9a-fA-F]{6}$/.test(_accentOverrideOriginal);
      accentInput.value = validOverride
        ? _accentOverrideOriginal
        : (_activeThemeDefaultAccent() || '#e040c0');
      _updateHex(accentInput);
    }
    document.getElementById('cfgSourceDir').value        = s.source_dir        || '';
    document.getElementById('cfgMoviesSourceDir').value  = s.movies_source_dir  || '';
    document.getElementById('cfgSeriesDir').value        = s.series_dir        || '';
    document.getElementById('cfgFeaturesDir').value      = s.features_dir      || '';
    document.getElementById('cfgVicesDir').value         = s.vices_dir         || '';
    document.getElementById('cfgJavDir').value           = s.jav_dir           || '';
    document.getElementById('cfgLibraryOwnerUser').value  = s.library_owner_user  || '';
    document.getElementById('cfgLibraryOwnerGroup').value = s.library_owner_group || '';
    document.getElementById('cfgPatternSeries').value    = s.pattern_series    || '';
    document.getElementById('cfgPatternPerformer').value = s.pattern_performer || '';
    const watchOn = (s.folder_watch_enabled || 'true') === 'true';
    const dlWatchOn = (s.download_watch_enabled || 'false') === 'true';
    document.getElementById('cfgDownloadWatchEnabled').checked = dlWatchOn;
    toggleSection('downloadWatchBody', dlWatchOn);
    document.getElementById('cfgDownloadWatchDir').value  = s.download_watch_dir       || '';
    document.getElementById('cfgMovieDownloadWatchDir').value = s.movie_download_watch_dir || '';
    document.getElementById('cfgDownloadWatchHold').value = s.download_watch_hold_secs || '300';
    document.getElementById('cfgDownloadImportRemove').checked = (s.download_import_remove_client || 'false') === 'true';
    document.getElementById('cfgWatchEnabled').checked = watchOn;
    toggleSection('watchBody', watchOn);
    document.getElementById('cfgWatchHold').value = s.folder_watch_hold_secs || '60';
    fetch('/api/watcher/status').then(r=>r.json()).then(d=>{
      const el = document.getElementById('watcherStatus');
      el.textContent = d.watching ? `Watching${d.pending.length ? ` · ${d.pending.length} pending` : ''}` : 'Not watching';
      el.style.color = d.watching ? 'var(--green)' : 'var(--red)';
    });
    const retryOn = (s.retry_enabled || 'true') === 'true';
    document.getElementById('cfgRetryEnabled').checked = retryOn;
    toggleSection('retryBody', retryOn);
    document.getElementById('cfgFilenamePatterns').value = s.filename_patterns  || 'pipe|dot_date|dot_date_short|bracket_site|dash_date';
    document.getElementById('cfgStripWords').value         = s.filename_strip_words || '';
    document.getElementById('cfgMinFileSize').value      = s.min_file_size_mb  || '10';
    document.getElementById('cfgLogRetentionDays').value = s.log_retention_days || '14';
    try { document.getElementById('cfgSiteAbbrevs').value = JSON.stringify(JSON.parse(s.site_abbreviations || '{}'), null, 2); } catch { document.getElementById('cfgSiteAbbrevs').value = s.site_abbreviations || ''; }
    try { document.getElementById('cfgSiteRenameMap').value = JSON.stringify(JSON.parse(s.site_rename_map || '{}'), null, 2); } catch { document.getElementById('cfgSiteRenameMap').value = s.site_rename_map || ''; }
    document.getElementById('cfgRetryHour').value    = s.retry_hour         || '1';
    document.getElementById('cfgRetryFreq').value    = s.retry_frequency_h  || '24';
    // Show next run time
    fetch('/api/retry/status').then(r=>r.json()).then(d=>{
      const el = document.getElementById('retryNextRun');
      if (d.next_run) el.textContent = `Next run: ${d.next_run.slice(0,16)} — ${d.pending} file(s) queued for retry`;
      else el.textContent = d.enabled ? 'Scheduler active, no job scheduled yet' : 'Retries disabled';
    });
    document.getElementById('cfgSubmitPhashStashdb').checked   = (s.submit_phash_stashdb             || 'true')  === 'true';
    document.getElementById('cfgSubmitPhashFansdb').checked    = (s.submit_phash_fansdb              || 'true')  === 'true';
    document.getElementById('cfgSubmitSceneStashdb').checked   = (s.submit_scene_stashdb             || 'false') === 'true';
    document.getElementById('cfgSubmitSceneFansdb').checked    = (s.submit_scene_fansdb              || 'false') === 'true';
    document.getElementById('cfgManualSubmitDefault').checked  = (s.manual_submit_stashdb_default || 'false') === 'true';
    document.getElementById('cfgAliasLookup').checked          = (s.alias_lookup_enabled || 'false') === 'true';
    document.getElementById('cfgLibraryIndexQueue').checked = (String(s.library_index_queue_matching_enabled ?? s.favourites_crosswalk_queue_enabled ?? 'true') === 'true');
    document.getElementById('cfgLibraryWalkPrune').value = s.library_walk_prune_dirs || '';
    document.getElementById('cfgCatStraight').checked           = (s.cat_straight    || 'true') === 'true';
    document.getElementById('cfgCatLesbian').checked            = (s.cat_lesbian     || 'true') === 'true';
    document.getElementById('cfgCatGay').checked                = (s.cat_gay         || 'true') === 'true';
    document.getElementById('cfgCatSoloFemale').checked         = (s.cat_solo_female || 'true') === 'true';
    document.getElementById('cfgCatTrans').checked              = (s.cat_trans       || 'true') === 'true';
    _monitoredTags = _parseTagListSetting(s.monitored_tags);
    _blacklistTags = _parseTagListSetting(s.tag_blacklist);
    renderBlacklistTagChips();
    document.getElementById('cfgDlNzbClient').value    = s.dl_nzb_client     || '';
    document.getElementById('cfgDlNzbHost').value      = s.dl_nzb_host       || '';
    document.getElementById('cfgDlNzbPort').value      = s.dl_nzb_port       || '';
    document.getElementById('cfgDlNzbUser').value      = s.dl_nzb_user       || '';
    document.getElementById('cfgDlNzbPass').value      = s.dl_nzb_pass       || '';
    document.getElementById('cfgDlNzbApiKey').value    = s.dl_nzb_api_key    || '';
    document.getElementById('cfgDlTorrentClient').value = s.dl_torrent_client || '';
    document.getElementById('cfgDlTorrentHost').value  = s.dl_torrent_host   || '';
    document.getElementById('cfgDlTorrentPort').value  = s.dl_torrent_port   || '';
    document.getElementById('cfgDlTorrentUser').value  = s.dl_torrent_user   || '';
    document.getElementById('cfgDlTorrentPass').value  = s.dl_torrent_pass   || '';
    // Rebuild the priority dropdowns once the client values are set,
    // then seed the saved value (if it still maps to a valid option).
    toggleNzbFields();
    _rebuildTorrentPriorityOptions();
    {
      const nzbSel = document.getElementById('cfgDlNzbPriority');
      const torrSel = document.getElementById('cfgDlTorrentPriority');
      const nzbCli = (s.dl_nzb_client || '').toLowerCase();
      const torrCli = (s.dl_torrent_client || '').toLowerCase();
      const nzbVals  = (_NZB_PRIORITY_OPTIONS[nzbCli]   || []).map(o => o[0]);
      const torrVals = (_TORRENT_PRIORITY_OPTIONS[torrCli] || []).map(o => o[0]);
      if (nzbSel  && nzbVals.includes(s.dl_nzb_priority || ''))     nzbSel.value  = s.dl_nzb_priority;
      if (torrSel && torrVals.includes(s.dl_torrent_priority || '')) torrSel.value = s.dl_torrent_priority;
    }
    document.getElementById('cfgProwlarrUrl').value      = s.prowlarr_url       || '';
    document.getElementById('cfgProwlarrKey').value      = s.prowlarr_api_key   || '';
    document.getElementById('cfgProwlarrCategory').value = s.prowlarr_category  || 'Top-Shelf';
    document.getElementById('cfgProwlarrCategoryMovies').value = s.prowlarr_category_movies || '';
    toggleNzbFields();
    fetch('/api/prowlarr/indexers').then(r=>r.json()).then(d=>{
      const el = document.getElementById('indexerCount');
      if (el && d.count) el.textContent = `${d.count} indexers cached`;
    }).catch(() => {});
    fetch('/api/prowlarr/status').then(r=>r.json()).then(d=>{
      const el = document.getElementById('prowlarrStatus');
      if (!el) return;
      el.textContent = d.connected ? `Connected v${d.version}` : (d.error || 'Not connected');
      el.style.color = d.connected ? 'var(--green)' : 'var(--red)';
    }).catch(() => {});
    document.getElementById('cfgKeyTmdb').value          = s.api_key_tmdb      || '';
    document.getElementById('cfgMediaScanEnabled').checked = (s.media_scan_enabled || 'true') === 'true';
    document.getElementById('cfgScanDebounce').value        = s.media_scan_debounce_mins || '5';
    document.getElementById('cfgStashEnabled').checked    = (s.stash_enabled    || 'true') === 'true';
    document.getElementById('cfgJellyfinEnabled').checked = (s.jellyfin_enabled || 'true') === 'true';
    document.getElementById('cfgPlexEnabled').checked     = (s.plex_enabled     || 'true') === 'true';
    document.getElementById('cfgEmbyEnabled').checked     = (s.emby_enabled     || 'true') === 'true';
    document.getElementById('cfgStashUrl').value          = s.stash_url          || '';
    document.getElementById('cfgStashKey').value          = s.stash_api_key      || '';
    document.getElementById('cfgJellyfinUrl').value       = s.jellyfin_url       || '';
    document.getElementById('cfgJellyfinKey').value       = s.jellyfin_api_key   || '';
    document.getElementById('cfgPlexUrl').value           = s.plex_url           || '';
    document.getElementById('cfgPlexToken').value         = s.plex_token         || '';
    document.getElementById('cfgEmbyUrl').value           = s.emby_url           || '';
    document.getElementById('cfgEmbyKey').value           = s.emby_api_key       || '';
    document.getElementById('cfgKeyStashdb').value        = s.api_key_stashdb    || '';
    document.getElementById('cfgKeyTpdb').value          = s.api_key_tpdb      || '';
    document.getElementById('cfgKeyFansdb').value        = s.api_key_fansdb    || '';
    document.getElementById('cfgKeyJavstash').value      = s.api_key_javstash  || '';
    document.getElementById('cfgIafdSearchEnabled').checked = (s.iafd_search_enabled || 'true') === 'true';
    document.getElementById('cfgTpdbSyncEnabled').checked = (s.tpdb_sync_enabled || 'false') === 'true';
    document.getElementById('cfgTpdbSyncToFavs').checked  = (s.tpdb_sync_to_favs || 'false') === 'true';
    document.getElementById('cfgTpdbSyncHour').value     = s.tpdb_sync_hour         || '2';
    document.getElementById('cfgTpdbSyncFreq').value     = s.tpdb_sync_frequency_h  || '24';
    document.getElementById('cfgTpdbSyncPerformerDir').value = s.tpdb_sync_performer_dir || '';
    document.getElementById('cfgTpdbSyncStudioDir').value   = s.tpdb_sync_studio_dir   || '';
    document.getElementById('cfgHealthPhash3Recurring').checked = (s.library_phash3_rescan_enabled || 'false') === 'true';
    document.getElementById('cfgHealthPhash3IntervalDays').value = s.library_phash3_rescan_interval_days || '30';
    syncHealthPhash3IntervalDisabled();

    renderDirList();
    try { renderRssFeedListFromSettings(s); } catch (e) { console.error('RSS feeds load:', e); }
    try { renderNewsRssFeedListFromSettings(s); } catch (e) { console.error('News RSS load:', e); }
    document.getElementById('cfgRssKeywordBlacklist').value = s.rss_keyword_blacklist || '';
    // Load auth status
    try {
      const ra = await fetch('/api/auth/status');
      const da = await ra.json();
      const mins = da.session_minutes != null
        ? da.session_minutes
        : (da.session_hours ? da.session_hours * 60 : 1440);
      document.getElementById('cfgSessionMinutes').value = mins;
    } catch {}
    await _loadVicesFromServer();
    /* Do not force Security here — it races nav clicks during slow loads (panels snap back).
       Initial HTML already shows Security; hash routing below selects rss when needed. */
    if (window.location.hash === '#settingsSectionRssFeeds') {
      setTimeout(function () {
        document.getElementById('settingsSectionRssFeeds')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }, 100);
    }
    _settingsPageLoaded = true;
    } catch (e) {
      _settingsPageLoaded = false;
      throw e;
    }
  }

  /** Dismiss open tag-search dropdowns inside settings. Return true if
   * something was closed so Escape does not exit settings yet. */
  function _settingsDismissEscapeLayers() {
    const modal = document.getElementById('settingsRoot');
    if (!modal || !modal) return false;
    let dismissed = false;
    modal.querySelectorAll('.ts-tag-results').forEach((el) => {
      const open = el.style.display === 'block' ||
        (el.style.display !== 'none' && (el.innerHTML || '').trim());
      if (open) {
        el.style.display = 'none';
        el.innerHTML = '';
        dismissed = true;
      }
    });
    const ae = document.activeElement;
    if (ae && modal.contains(ae)) {
      const isTagSearch = ae.id === 'cfgBlacklistTagSearch' ||
        (typeof ae.id === 'string' && ae.id.startsWith('cfgViceTagSearch_'));
      if (isTagSearch) {
        if (ae.value) ae.value = '';
        ae.blur();
        dismissed = true;
      }
    }
    return dismissed;
  }
  window._settingsDismissEscapeLayers = _settingsDismissEscapeLayers;

  function closeSettings() {
    // Cancel any in-flight tag-picker debounce timers — otherwise they
    // fire after the modal is gone, wasting a fetch + DOM write into
    // detached results panels.
    Object.keys(_tagPickerTimers).forEach((k) => {
      clearTimeout(_tagPickerTimers[k]);
      delete _tagPickerTimers[k];
    });
    // Revert unsaved theme preview to whatever was stored on modal open.
    if (typeof _uiThemeOriginal !== 'undefined' && _uiThemeOriginal && window.setUiTheme) {
      window.setUiTheme(_uiThemeOriginal, _customSpecOriginal || undefined);
    }
    // Restore the accent override snapshot — empty string clears.
    if (window.setAccentOverride) {
      window.setAccentOverride(_accentOverrideOriginal || null);
    }
  }

  // ── Appearance — live theme preview + custom-palette builder ──
  // _uiThemeOriginal  / _customSpecOriginal  : snapshot at modal open for Cancel revert.
  // onUiThemeChange   : fired when the top theme <select> changes.
  // onCustomFieldChange: fired when any custom picker / base radio changes.
  let _uiThemeOriginal = 'dark';
  let _customSpecOriginal = null;
  let _accentOverrideOriginal = '';

  // Live preview hook for the accent override picker. Empty / invalid
  // value clears the override; a valid #rrggbb applies it on top of
  // the active theme until the user hits Save (or Cancel reverts).
  function onAccentOverrideChange(value) {
    if (window.setAccentOverride) window.setAccentOverride(value || null);
  }
  // Named distinctly from the global `window.clearAccentOverride`
  // (defined in theme-init.js, which only strips the override style +
  // localStorage). A top-level `function clearAccentOverride()` at the
  // script root would have created its own global binding and clobbered
  // theme-init's version — then this local would recurse into itself.
  // The picker variant calls the global stripper AND resets the swatch.
  function clearAccentOverridePicker() {
    if (window.clearAccentOverride) window.clearAccentOverride();
    const el = document.getElementById('cfgUiAccentOverride');
    if (el) {
      const def = _activeThemeDefaultAccent();
      const next = def || '#e040c0';
      el.value = next;
      // Some browsers (Chrome/Safari) don't always refresh the colour
      // swatch when `.value` is set programmatically. Re-assigning via
      // setAttribute makes the visual refresh unconditional. Don't fire
      // `input` here — that would re-apply the picker value as a new
      // override; Clear is meant to remove the override entirely.
      el.setAttribute('value', next);
      _updateHex(el);
    }
  }
  /** Canonical Top-Shelf brand orange. Drives the app chrome (Save
   * button, settings gear, active-state highlights) when no theme
   * accent is overriding it. Kept here so the picker, the comment in
   * settings.html, and any future references stay aligned. */
  const TS_BRAND_ACCENT_DEFAULT = '#f55600';
  function restoreDefaultAccent() {
    const el = document.getElementById('cfgUiAccentOverride');
    if (el) {
      el.value = TS_BRAND_ACCENT_DEFAULT;
      el.setAttribute('value', TS_BRAND_ACCENT_DEFAULT);
      _updateHex(el);
      // Fire input — same path a user-typed colour takes; runs the
      // standard `onAccentOverrideChange` which calls setAccentOverride.
      el.dispatchEvent(new Event('input', { bubbles: true }));
    } else if (window.setAccentOverride) {
      window.setAccentOverride(TS_BRAND_ACCENT_DEFAULT);
    }
  }

  // Read the currently-applied theme's --accent CSS var off <html>.
  // After window.setUiTheme() returns, the new theme is on the DOM and
  // its --accent has cascaded; getComputedStyle resolves to that
  // theme's hex. Any active accent override is cleared first so the
  // resolved value is the theme's own default rather than the override.
  function _activeThemeDefaultAccent() {
    const root = document.documentElement;
    const hadOverride = root.hasAttribute('data-accent-override');
    if (hadOverride && window.clearAccentOverride) {
      // Temporarily strip so getComputedStyle returns the theme default.
      window.clearAccentOverride();
    }
    const raw = getComputedStyle(root).getPropertyValue('--accent').trim();
    // Normalise to #rrggbb. The CSS var may resolve to '#df0024',
    // '#e1306c', or longer forms. Color inputs require 7-char hex.
    let hex = raw.toLowerCase();
    if (/^#[0-9a-f]{3}$/.test(hex)) {
      hex = '#' + hex.slice(1).split('').map(c => c + c).join('');
    }
    if (!/^#[0-9a-f]{6}$/.test(hex)) hex = '';
    return hex;
  }
  const CUSTOM_DEFAULTS_DARK = {
    base: 'dark',
    brand_purple: '#9b4dca', brand_accent: '#d94a6c',
    bg_panel: '#0c0c0e', panel_hi: '#242430', panel_lo: '#12121a',
  };

  function toggleCustomThemeSection(themeKey) {
    const el = document.getElementById('customThemeSection');
    if (el) el.style.display = themeKey === 'custom' ? '' : 'none';
  }

  // Mirror a <input type="color"> value into an adjacent <code class="hex-value">
  // so the user can see the exact hex without opening the native picker.
  function _updateHex(input) {
    const code = input.parentElement && input.parentElement.querySelector('.hex-value');
    if (code) code.textContent = (input.value || '').toUpperCase();
  }

  function loadCustomThemeFields(spec) {
    const s = Object.assign({}, CUSTOM_DEFAULTS_DARK, spec || {});
    const base = s.base === 'light' ? 'light' : 'dark';
    document.querySelectorAll('input[name="customBase"]').forEach(r => { r.checked = r.value === base; });
    _setInputValue('cfgCustomBrandPurple', s.brand_purple || CUSTOM_DEFAULTS_DARK.brand_purple);
    _setInputValue('cfgCustomBrandAccent', s.brand_accent || CUSTOM_DEFAULTS_DARK.brand_accent);
    _setInputValue('cfgCustomBgPanel', s.bg_panel || CUSTOM_DEFAULTS_DARK.bg_panel);
    _setInputValue('cfgCustomPanelHi', s.panel_hi || CUSTOM_DEFAULTS_DARK.panel_hi);
    _setInputValue('cfgCustomPanelLo', s.panel_lo || CUSTOM_DEFAULTS_DARK.panel_lo);
    document.querySelectorAll('#customThemeSection input[type="color"]').forEach(_updateHex);
    updateCustomContrastBadge();
  }

  function readCustomThemeFields() {
    const baseEl = document.querySelector('input[name="customBase"]:checked');
    return {
      base: baseEl ? baseEl.value : 'dark',
      brand_purple: document.getElementById('cfgCustomBrandPurple').value,
      brand_accent: document.getElementById('cfgCustomBrandAccent').value,
      bg_panel:     document.getElementById('cfgCustomBgPanel').value,
      panel_hi:     document.getElementById('cfgCustomPanelHi').value,
      panel_lo:     document.getElementById('cfgCustomPanelLo').value,
    };
  }

  function onUiThemeChange(themeKey) {
    toggleCustomThemeSection(themeKey);
    if (themeKey === 'custom') {
      // Seed pickers with the saved spec, or dark defaults on first run.
      // readCustomThemeFields() would return empty #000000 values before the
      // pickers are populated, producing a solid-black duotone, so never read
      // field state here — always start from a known-good spec.
      const spec = _customSpecOriginal || CUSTOM_DEFAULTS_DARK;
      loadCustomThemeFields(spec);
      window.setUiTheme && window.setUiTheme('custom', spec);
    } else {
      window.setUiTheme && window.setUiTheme(themeKey);
    }
    // Reset the accent picker to the new theme's default. Any active
    // override is cleared inside _activeThemeDefaultAccent so the
    // page paints with the theme's own --accent instead of carrying a
    // stale override from the previous theme.
    const accentInput = document.getElementById('cfgUiAccentOverride');
    if (accentInput) {
      const def = _activeThemeDefaultAccent();
      if (def) {
        accentInput.value = def;
        _updateHex(accentInput);
      }
    }
  }

  function onCustomFieldChange() {
    window.setUiTheme && window.setUiTheme('custom', readCustomThemeFields());
    updateCustomContrastBadge();
  }

  // WCAG relative luminance + contrast ratio. `bg` is the effective panel
  // colour the user picked; `fg` is the text colour sourced from the base
  // theme (dark → cream, light → near-black).
  function _hexToRgb(hex) {
    if (!hex) return [0,0,0];
    let h = String(hex).trim().replace('#','');
    if (h.length === 3) h = h[0]+h[0]+h[1]+h[1]+h[2]+h[2];
    return [parseInt(h.slice(0,2),16), parseInt(h.slice(2,4),16), parseInt(h.slice(4,6),16)];
  }
  function _relLum(rgb) {
    const [r,g,b] = rgb.map(v => {
      const s = v / 255;
      return s <= 0.03928 ? s / 12.92 : Math.pow((s + 0.055) / 1.055, 2.4);
    });
    return 0.2126*r + 0.7152*g + 0.0722*b;
  }
  function _contrastRatio(a, b) {
    const la = _relLum(a), lb = _relLum(b);
    const [hi, lo] = la > lb ? [la, lb] : [lb, la];
    return (hi + 0.05) / (lo + 0.05);
  }

  function updateCustomContrastBadge() {
    const badge = document.getElementById('customContrastBadge');
    if (!badge) return;
    const spec = readCustomThemeFields();
    const textHex = spec.base === 'light' ? '#2b2a25' : '#d4cfc4';
    const panelHi = spec.panel_hi || '#242430';
    const panelLo = spec.panel_lo || '#12121a';
    const rHi = _contrastRatio(_hexToRgb(textHex), _hexToRgb(panelHi));
    const rLo = _contrastRatio(_hexToRgb(textHex), _hexToRgb(panelLo));
    const worst = Math.min(rHi, rLo);
    const fmt = n => n.toFixed(2) + ':1';
    let cls = 'is-ok', label = fmt(worst);
    if (worst < 3)      { cls = 'is-bad';  label = fmt(worst) + ' ⚠'; }
    else if (worst < 4.5) { cls = 'is-warn'; label = fmt(worst); }
    badge.className = 'custom-contrast-badge ' + cls;
    badge.textContent = label;
    const tip = 'Text vs panels (worst of light/dark panel): ' + fmt(worst) + '.\n' +
                'WCAG AA needs 4.5:1 for body text, 3:1 for UI chrome.\n' +
                'Panel (light) contrast: ' + fmt(rHi) + '\n' +
                'Panel (dark) contrast: ' + fmt(rLo) + '\n' +
                (worst < 3
                  ? '⚠ Below 3:1 — text will be hard to read on at least one panel.'
                  : worst < 4.5
                    ? 'Body text (4.5:1) not met, UI chrome (3:1) OK.'
                    : 'Passes WCAG AA for body text.');
    badge.title = tip;
  }

  const ALL_PERF_GENDERS = ['female', 'male', 'trans', 'other'];
  const DIR_GENDER_ICONS = { female: 'fa-venus', male: 'fa-mars', trans: 'fa-transgender', other: 'fa-venus-mars' };
  /** Display order (by rank); must match row index used in remove/move/gender handlers — ``dirs`` itself may be unsorted. */
  function _performerDirsSortedForDisplay() {
    return [...dirs].sort((a, b) => (a.rank - b.rank) || String(a.path || '').localeCompare(String(b.path || '')));
  }
  function dirGenderRowHtml(d, i) {
    if (d.type !== 'performer') return '';
    const gf = (!d.gender_filters || !d.gender_filters.length) ? ALL_PERF_GENDERS.slice() : d.gender_filters;
    const L = { female: 'Female', male: 'Male', trans: 'Trans', other: 'Other' };
    return `<div class="dir-gender-filters">
      <span class="dir-gender-label"><i class="fa-solid fa-filter" aria-hidden="true"></i> Favourites matching</span>
      <div class="dir-gender-chips">
      ${ALL_PERF_GENDERS.map(g => {
    const on = gf.includes(g);
    return `<label class="dir-gender-chip${on ? ' dir-gender-chip--on' : ''}">
      <input type="checkbox" ${on ? 'checked' : ''} onchange="setDirGenderFilter(${i},'${g}',this.checked)">
      <span class="dir-gender-chip-inner"><i class="fa-solid ${DIR_GENDER_ICONS[g]}"></i> ${L[g]}</span>
    </label>`;
  }).join('')}
      </div>
      <p class="dir-gender-hint">Only checked genders are used when auto-linking folders to TPDB / StashDB / FansDB. All four = no restriction.</p>
    </div>`;
  }
  function setDirGenderFilter(displayIdx, g, checked) {
    const sorted = _performerDirsSortedForDisplay();
    const row = sorted[displayIdx];
    if (!row) return;
    const j = dirs.indexOf(row);
    if (j < 0) return;
    let gf = dirs[j].gender_filters;
    if (!gf || gf.length === 0) gf = ALL_PERF_GENDERS.slice();
    if (checked) {
      if (!gf.includes(g)) gf = [...gf, g];
    } else {
      gf = gf.filter(x => x !== g);
    }
    if (gf.length === 0) gf = ALL_PERF_GENDERS.slice();
    dirs[j].gender_filters = gf;
    renderDirList();
  }
  function serializeGenderFiltersForSave(gf) {
    if (!gf || !gf.length) return [];
    if (gf.length === ALL_PERF_GENDERS.length) return [];
    return gf;
  }

  function getRssFeedUrlsFromDomRaw() {
    const host = document.getElementById('rssFeedsList');
    if (!host) return [];
    return Array.from(host.querySelectorAll('.rss-feed-url-input')).map(el => el.value.trim());
  }

  function getRssFeedUrlsForSave() {
    return getRssFeedUrlsFromDomRaw().filter(Boolean);
  }

  function renderRssFeedList(urls) {
    const host = document.getElementById('rssFeedsList');
    if (!host) return;
    const list = urls || [];
    if (!list.length) {
      host.innerHTML = '<div class="empty" style="padding:12px">No RSS feeds added. Use <strong>Add feed URL</strong> below (or leave blank to configure later).</div>';
      return;
    }
    host.innerHTML = list.map((url, i) => `
    <div class="rss-feed-row" style="display:flex;flex-direction:row;align-items:center;gap:8px;margin-bottom:6px">
      <input class="field-input rss-feed-url-input" type="url" value="${esc(url)}" placeholder="https://…" style="flex:1;min-width:0">
      <button type="button" class="btn-secondary" onclick="removeRssFeedRow(${i})" title="Remove" style="flex-shrink:0"><i class="fa-solid fa-x"></i></button>
    </div>`).join('');
  }

  function renderRssFeedListFromSettings(s) {
    let urls = [];
    try {
      const arr = JSON.parse((s && s.download_rss_feeds) || '[]');
      urls = Array.isArray(arr) ? arr.filter(u => typeof u === 'string') : [];
    } catch (e) { urls = []; }
    renderRssFeedList(urls);
  }

  function addRssFeedRow() {
    const cur = getRssFeedUrlsFromDomRaw();
    if (!document.getElementById('rssFeedsList')?.querySelector('.rss-feed-url-input') && cur.length === 0) {
      renderRssFeedList(['']);
    } else {
      cur.push('');
      renderRssFeedList(cur);
    }
    const inputs = document.querySelectorAll('#rssFeedsList .rss-feed-url-input');
    if (inputs.length) inputs[inputs.length - 1].focus();
  }

  function removeRssFeedRow(i) {
    const cur = getRssFeedUrlsFromDomRaw();
    cur.splice(i, 1);
    renderRssFeedList(cur);
  }

  // ── News RSS feeds — independent editor that mirrors the download
  //    editor's render / add / remove / save shape. State lives in the
  //    DOM rows; we read it back on Save into `news_rss_feeds`.
  //
  //    Each row now carries an optional tint colour alongside the URL
  //    so /news can wash the tile body in that feed's brand colour.
  //    Storage shape per entry is `{url, tint}`; legacy plain-string
  //    entries (older saves, the seed defaults) are accepted on read
  //    and upgraded on the next save.
  function renderNewsRssFeedList(entries) {
    const host = document.getElementById('newsRssFeedsList');
    if (!host) return;
    const list = entries || [];
    if (!list.length) {
      host.innerHTML = '<div class="empty" style="padding:12px">No news feeds. Use <strong>Add feed URL</strong> below or <strong>Restore defaults</strong> to bring back the seed list.</div>';
      return;
    }
    host.innerHTML = list.map((entry, i) => {
      const url = (entry && entry.url) || '';
      // `<input type="color">` requires a 7-char `#RRGGBB`; an empty
      // tint defaults to dim grey so the swatch is visible but
      // visually obvious as "no override applied".
      const tint = (entry && entry.tint) || '#2a2030';
      const hasTint = !!(entry && entry.tint);
      return `
    <div class="news-rss-feed-row" style="display:flex;flex-direction:row;align-items:center;gap:8px;margin-bottom:6px">
      <input class="news-rss-feed-tint-input" type="color" value="${esc(tint)}" data-set="${hasTint ? '1' : '0'}" title="${hasTint ? 'Feed tint (click to change, double-click to clear)' : 'Set a tint colour (click to pick)'}" style="width:38px;height:34px;padding:0;border:1px solid rgba(192,132,252,0.25);border-radius:6px;background:transparent;cursor:pointer;flex-shrink:0;opacity:${hasTint ? '1' : '0.55'}" ondblclick="clearNewsRssFeedTint(${i})" oninput="this.dataset.set='1';this.style.opacity='1';this.title='Feed tint (click to change, double-click to clear)'">
      <input class="field-input news-rss-feed-url-input" type="url" value="${esc(url)}" placeholder="https://…" style="flex:1;min-width:0">
      <button type="button" class="btn-secondary" onclick="removeNewsRssFeedRow(${i})" title="Remove" style="flex-shrink:0"><i class="fa-solid fa-x"></i></button>
    </div>`;
    }).join('');
  }
  function getNewsRssFeedEntriesFromDomRaw() {
    const rows = document.querySelectorAll('#newsRssFeedsList .news-rss-feed-row');
    return Array.from(rows).map(row => {
      const urlEl = row.querySelector('.news-rss-feed-url-input');
      const tintEl = row.querySelector('.news-rss-feed-tint-input');
      const url = urlEl ? urlEl.value : '';
      const tintSet = tintEl && tintEl.dataset && tintEl.dataset.set === '1';
      const tint = tintSet && tintEl ? (tintEl.value || '') : '';
      return { url, tint };
    });
  }
  function getNewsRssFeedUrlsForSave() {
    // Returns the array shape persisted to `news_rss_feeds`. Drops
    // empty URLs and emits {url, tint} objects (tint omitted when not
    // set, so the JSON stays compact for feeds the user hasn't tinted).
    return getNewsRssFeedEntriesFromDomRaw()
      .map(e => ({ url: (e.url || '').trim(), tint: (e.tint || '').trim() }))
      .filter(e => !!e.url)
      .map(e => e.tint ? { url: e.url, tint: e.tint } : { url: e.url });
  }
  function renderNewsRssFeedListFromSettings(s) {
    let entries = [];
    try {
      const arr = JSON.parse((s && s.news_rss_feeds) || '[]');
      if (Array.isArray(arr)) {
        // Accept both legacy strings and the new {url, tint} object
        // shape so older saves don't lose their feed list on first
        // render after upgrade.
        entries = arr.map(x => {
          if (typeof x === 'string') return { url: x, tint: '' };
          if (x && typeof x === 'object' && typeof x.url === 'string') {
            return { url: x.url, tint: (typeof x.tint === 'string') ? x.tint : '' };
          }
          return null;
        }).filter(Boolean);
      }
    } catch (e) { entries = []; }
    renderNewsRssFeedList(entries);
  }
  function addNewsRssFeedRow() {
    const cur = getNewsRssFeedEntriesFromDomRaw();
    if (!document.getElementById('newsRssFeedsList')?.querySelector('.news-rss-feed-url-input') && cur.length === 0) {
      renderNewsRssFeedList([{ url: '', tint: '' }]);
    } else {
      cur.push({ url: '', tint: '' });
      renderNewsRssFeedList(cur);
    }
    const inputs = document.querySelectorAll('#newsRssFeedsList .news-rss-feed-url-input');
    if (inputs.length) inputs[inputs.length - 1].focus();
  }
  function removeNewsRssFeedRow(i) {
    const cur = getNewsRssFeedEntriesFromDomRaw();
    cur.splice(i, 1);
    renderNewsRssFeedList(cur);
  }
  function clearNewsRssFeedTint(i) {
    // Double-clicking the swatch removes the tint — handy when the
    // user wants to back out of an override without picking a new one.
    const cur = getNewsRssFeedEntriesFromDomRaw();
    if (i >= 0 && i < cur.length) {
      cur[i].tint = '';
      renderNewsRssFeedList(cur);
    }
  }
  async function restoreNewsRssDefaults() {
    try {
      const r = await fetch('/api/news/defaults', { credentials: 'same-origin' });
      const d = await r.json();
      const raw = (d && Array.isArray(d.defaults)) ? d.defaults : [];
      // Defaults ship as plain URL strings; lift them into the editor's
      // {url, tint} shape so they slot in alongside any tinted entries
      // the user already had.
      const entries = raw.map(x => (typeof x === 'string') ? { url: x, tint: '' }
        : (x && typeof x === 'object' && typeof x.url === 'string') ? { url: x.url, tint: (x.tint || '') }
        : null).filter(Boolean);
      renderNewsRssFeedList(entries);
    } catch (e) {
      // Silent — empty list is the existing UI; user can re-click.
    }
  }
  window.clearNewsRssFeedTint = clearNewsRssFeedTint;

  function renderDirList() {
    const sorted = _performerDirsSortedForDisplay();
    document.getElementById('dirList').innerHTML = sorted.length === 0
      ? '<div class="empty" style="padding:16px">No star directories configured</div>'
      : sorted.map((d, i) => `
        <div class="dir-row" data-id="${d.id||''}" data-idx="${i}">
          <div class="dir-row-main">
            <div class="dir-rank">${d.rank}</div>
            <div class="dir-type-tag dir-type-${d.type}">${d.label}</div>
            <div class="dir-path" title="${d.path}">${d.path}</div>
            <div class="dir-controls">
              <button type="button" class="dir-btn" onclick="moveDirUp(${i})" title="Move up" aria-label="Move up"><i class="fa-solid fa-chevron-up" aria-hidden="true"></i></button>
              <button type="button" class="dir-btn" onclick="moveDirDown(${i})" title="Move down" aria-label="Move down"><i class="fa-solid fa-chevron-down" aria-hidden="true"></i></button>
              <button type="button" class="dir-btn dir-btn--danger" onclick="removeDir(${i})" title="Remove" aria-label="Remove directory"><i class="fa-solid fa-xmark" aria-hidden="true"></i></button>
            </div>
          </div>
          ${dirGenderRowHtml(d, i)}
        </div>`).join('');
  }

  function moveDirUp(displayIdx) {
    const sorted = _performerDirsSortedForDisplay();
    if (displayIdx <= 0) return;
    const above = sorted[displayIdx - 1];
    const cur = sorted[displayIdx];
    const ia = dirs.indexOf(above);
    const ib = dirs.indexOf(cur);
    if (ia < 0 || ib < 0) return;
    [dirs[ia], dirs[ib]] = [dirs[ib], dirs[ia]];
    rerank();
    renderDirList();
  }

  function moveDirDown(displayIdx) {
    const sorted = _performerDirsSortedForDisplay();
    if (displayIdx >= sorted.length - 1) return;
    const cur = sorted[displayIdx];
    const below = sorted[displayIdx + 1];
    const ic = dirs.indexOf(cur);
    const ib = dirs.indexOf(below);
    if (ic < 0 || ib < 0) return;
    [dirs[ic], dirs[ib]] = [dirs[ib], dirs[ic]];
    rerank();
    renderDirList();
  }

  function removeDir(displayIdx) {
    const sorted = _performerDirsSortedForDisplay();
    const victim = sorted[displayIdx];
    if (!victim) return;
    const j = dirs.indexOf(victim);
    if (j >= 0) dirs.splice(j, 1);
    rerank();
    renderDirList();
  }

  function rerank() {
    dirs.forEach((d, i) => d.rank = i + 1);
  }

  function addDir() {
    const label = document.getElementById('newDirLabel').value.trim();
    const path  = document.getElementById('newDirPath').value.trim();
    if (!label || !path) return;
    dirs.push({ type: 'performer', path, label, rank: dirs.length + 1, gender_filters: ['female', 'male', 'trans', 'other'] });
    document.getElementById('newDirLabel').value = '';
    document.getElementById('newDirPath').value  = '';
    renderDirList();
  }

  async function clearImageCache() {
    const btn = document.getElementById('clearCacheBtn');
    const status = document.getElementById('clearCacheStatus');
    btn.disabled = true;
    btn.textContent = 'Clearing…';
    try {
      const r = await fetch('/api/cache/clear', {method: 'POST'});
      const d = await r.json();
      status.textContent = d.message || 'Image cache cleared.';
    } catch(e) {
      status.textContent = 'Error clearing cache.';
    } finally {
      btn.disabled = false;
      btn.textContent = 'Clear Images';
      setTimeout(() => { status.textContent = ''; }, 8000);
    }
  }

  async function saveSettings() {
    if (!_settingsPageLoaded) {
      showSettingsSavedToast('Settings still loading — wait a moment and try again', { kind: 'error' });
      return;
    }
    // The vices POST + the main settings POST below add up to a
    // noticeable beat. Pin the toast open with a spinner so the user
    // gets immediate feedback that the click registered.
    showSettingsSavedToast('Saving…', { spinner: true });
    // Read the chosen theme from the picker; fall back to whatever is
    // currently applied on <html> if the picker isn't on this page.
    // Custom theme persists its full spec via readCustomThemeFields().
    const _themeSelect = document.getElementById('cfgUiTheme');
    const _chosenTheme = (_themeSelect && _themeSelect.value)
      || document.documentElement.getAttribute('data-theme')
      || 'vhs';
    const _customSpec = (_chosenTheme === 'custom' && typeof readCustomThemeFields === 'function')
      ? readCustomThemeFields()
      : {};
    // Persist vices first — /api/vices rewrites vices_json AND
    // monitored_tags in a single transaction, so the main settings
    // POST below doesn't need to (and must not) race on those keys.
    if (_vicesLoaded) {
      try {
        await fetch('/api/vices', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          credentials: 'same-origin',
          body: JSON.stringify({ vices: _vices || [] })
        });
      } catch(_) { /* non-fatal — the main save below may still succeed */ }
    }
    const payload = {
      settings: {
        ui_theme:          _chosenTheme,
        ui_theme_custom_json: JSON.stringify(_customSpec),
        ui_accent_override: (() => {
          const el = document.getElementById('cfgUiAccentOverride');
          if (!el) return '';
          // Only persist when the override is actually engaged. The
          // "Clear" button strips data-accent-override on the html
          // root; we mirror that signal here so a cleared modal
          // saves an empty string instead of the picker's stale hex.
          const engaged = document.documentElement.hasAttribute('data-accent-override');
          return engaged ? (el.value || '').trim() : '';
        })(),
        source_dir:        document.getElementById('cfgSourceDir').value.trim(),
        movies_source_dir: document.getElementById('cfgMoviesSourceDir').value.trim(),
        series_dir:        document.getElementById('cfgSeriesDir').value.trim(),
        features_dir:      document.getElementById('cfgFeaturesDir').value.trim(),
        vices_dir:         document.getElementById('cfgVicesDir').value.trim(),
        jav_dir:           document.getElementById('cfgJavDir').value.trim(),
        library_owner_user:  document.getElementById('cfgLibraryOwnerUser').value.trim(),
        library_owner_group: document.getElementById('cfgLibraryOwnerGroup').value.trim(),
        pattern_series:    document.getElementById('cfgPatternSeries').value.trim(),
        pattern_performer: document.getElementById('cfgPatternPerformer').value.trim(),
        download_watch_enabled:  document.getElementById('cfgDownloadWatchEnabled').checked ? 'true' : 'false',
        download_watch_dir:      document.getElementById('cfgDownloadWatchDir').value.trim(),
        movie_download_watch_dir: document.getElementById('cfgMovieDownloadWatchDir').value.trim(),
        download_watch_hold_secs: document.getElementById('cfgDownloadWatchHold').value.trim(),
        download_import_remove_client: document.getElementById('cfgDownloadImportRemove').checked ? 'true' : 'false',
        folder_watch_enabled:   document.getElementById('cfgWatchEnabled').checked ? 'true' : 'false',
        folder_watch_hold_secs: document.getElementById('cfgWatchHold').value.trim(),
        retry_enabled:     document.getElementById('cfgRetryEnabled').checked ? 'true' : 'false',
        filename_patterns:     document.getElementById('cfgFilenamePatterns').value.trim(),
        filename_strip_words: document.getElementById('cfgStripWords').value.trim(),
        min_file_size_mb:  document.getElementById('cfgMinFileSize').value.trim(),
        log_retention_days: document.getElementById('cfgLogRetentionDays').value.trim() || '14',
        site_abbreviations: (() => { try { return JSON.stringify(JSON.parse(document.getElementById('cfgSiteAbbrevs').value)); } catch { return document.getElementById('cfgSiteAbbrevs').value; } })(),
        site_rename_map:    (() => { try { return JSON.stringify(JSON.parse(document.getElementById('cfgSiteRenameMap').value)); } catch { return document.getElementById('cfgSiteRenameMap').value; } })(),
        retry_hour:        document.getElementById('cfgRetryHour').value.trim(),
        retry_frequency_h: document.getElementById('cfgRetryFreq').value,
        submit_phash_stashdb:          document.getElementById('cfgSubmitPhashStashdb').checked  ? 'true' : 'false',
        submit_phash_fansdb:           document.getElementById('cfgSubmitPhashFansdb').checked   ? 'true' : 'false',
        submit_scene_stashdb:          document.getElementById('cfgSubmitSceneStashdb').checked  ? 'true' : 'false',
        submit_scene_fansdb:           document.getElementById('cfgSubmitSceneFansdb').checked   ? 'true' : 'false',
        manual_submit_stashdb_default: document.getElementById('cfgManualSubmitDefault').checked ? 'true' : 'false',
        alias_lookup_enabled:          document.getElementById('cfgAliasLookup').checked ? 'true' : 'false',
        library_index_queue_matching_enabled: document.getElementById('cfgLibraryIndexQueue').checked ? 'true' : 'false',
        library_walk_prune_dirs: document.getElementById('cfgLibraryWalkPrune').value.trim(),
        cat_straight:                  document.getElementById('cfgCatStraight').checked ? 'true' : 'false',
        cat_lesbian:                   document.getElementById('cfgCatLesbian').checked ? 'true' : 'false',
        cat_gay:                       document.getElementById('cfgCatGay').checked ? 'true' : 'false',
        cat_solo_female:               document.getElementById('cfgCatSoloFemale').checked ? 'true' : 'false',
        cat_trans:                     document.getElementById('cfgCatTrans').checked ? 'true' : 'false',
        tag_blacklist:                  JSON.stringify(_blacklistTags || []),
        // monitored_tags is now derived server-side from _vices
        // (via POST /api/vices before this settings save fires).
        api_key_tmdb:      document.getElementById('cfgKeyTmdb').value.trim(),
        dl_nzb_client:       document.getElementById('cfgDlNzbClient').value.trim(),
        dl_nzb_host:         document.getElementById('cfgDlNzbHost').value.trim(),
        dl_nzb_port:         document.getElementById('cfgDlNzbPort').value.trim(),
        dl_nzb_user:         document.getElementById('cfgDlNzbUser').value.trim(),
        dl_nzb_pass:         document.getElementById('cfgDlNzbPass').value,
        dl_nzb_api_key:      document.getElementById('cfgDlNzbApiKey').value.trim(),
        dl_torrent_client:   document.getElementById('cfgDlTorrentClient').value.trim(),
        dl_torrent_host:     document.getElementById('cfgDlTorrentHost').value.trim(),
        dl_torrent_port:     document.getElementById('cfgDlTorrentPort').value.trim(),
        dl_torrent_user:     document.getElementById('cfgDlTorrentUser').value.trim(),
        dl_torrent_pass:     document.getElementById('cfgDlTorrentPass').value,
        dl_nzb_priority:     document.getElementById('cfgDlNzbPriority').value.trim(),
        dl_torrent_priority: document.getElementById('cfgDlTorrentPriority').value.trim(),
        prowlarr_url:          document.getElementById('cfgProwlarrUrl').value.trim(),
        prowlarr_api_key:      document.getElementById('cfgProwlarrKey').value.trim(),
        prowlarr_category:     document.getElementById('cfgProwlarrCategory').value.trim(),
        prowlarr_category_movies: document.getElementById('cfgProwlarrCategoryMovies').value.trim(),
        media_scan_enabled:        document.getElementById('cfgMediaScanEnabled').checked ? 'true' : 'false',
        media_scan_debounce_mins:  document.getElementById('cfgScanDebounce').value.trim(),
        stash_enabled:     document.getElementById('cfgStashEnabled').checked     ? 'true' : 'false',
        jellyfin_enabled:  document.getElementById('cfgJellyfinEnabled').checked  ? 'true' : 'false',
        plex_enabled:      document.getElementById('cfgPlexEnabled').checked      ? 'true' : 'false',
        emby_enabled:      document.getElementById('cfgEmbyEnabled').checked      ? 'true' : 'false',
        stash_url:         document.getElementById('cfgStashUrl').value.trim(),
        stash_api_key:     document.getElementById('cfgStashKey').value.trim(),
        jellyfin_url:      document.getElementById('cfgJellyfinUrl').value.trim(),
        jellyfin_api_key:  document.getElementById('cfgJellyfinKey').value.trim(),
        plex_url:          document.getElementById('cfgPlexUrl').value.trim(),
        plex_token:        document.getElementById('cfgPlexToken').value.trim(),
        emby_url:          document.getElementById('cfgEmbyUrl').value.trim(),
        emby_api_key:      document.getElementById('cfgEmbyKey').value.trim(),
        api_key_stashdb:   document.getElementById('cfgKeyStashdb').value.trim(),
        api_key_tpdb:      document.getElementById('cfgKeyTpdb').value.trim(),
        api_key_fansdb:    document.getElementById('cfgKeyFansdb').value.trim(),
        api_key_javstash:  document.getElementById('cfgKeyJavstash').value.trim(),
        iafd_search_enabled: document.getElementById('cfgIafdSearchEnabled').checked ? 'true' : 'false',
        tpdb_sync_enabled:       document.getElementById('cfgTpdbSyncEnabled').checked ? 'true' : 'false',
        tpdb_sync_to_favs:       document.getElementById('cfgTpdbSyncToFavs').checked ? 'true' : 'false',
        tpdb_sync_hour:          document.getElementById('cfgTpdbSyncHour').value.trim(),
        tpdb_sync_frequency_h:   document.getElementById('cfgTpdbSyncFreq').value.trim(),
        tpdb_sync_performer_dir: document.getElementById('cfgTpdbSyncPerformerDir').value.trim(),
        tpdb_sync_studio_dir:    document.getElementById('cfgTpdbSyncStudioDir').value.trim(),
        library_phash3_rescan_enabled: document.getElementById('cfgHealthPhash3Recurring').checked ? 'true' : 'false',
        library_phash3_rescan_interval_days: document.getElementById('cfgHealthPhash3IntervalDays').value.trim() || '30',
        download_rss_feeds: JSON.stringify(getRssFeedUrlsForSave()),
        news_rss_feeds:     JSON.stringify(getNewsRssFeedUrlsForSave()),
        rss_keyword_blacklist: document.getElementById('cfgRssKeywordBlacklist').value.trim(),
      },
      directories: dirs.map(d => ({
        type: d.type,
        path: d.path,
        label: d.label,
        rank: d.rank,
        gender_filters: serializeGenderFiltersForSave(d.gender_filters),
      })),
    };
    const r = await fetch('/api/settings', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload),
    });
    if (r.ok) {
      const section = _activeSettingsCategoryLabel();
      showSettingsSavedToast(
        section ? section + ' settings saved' : 'Settings saved',
        { kind: 'success' }
      );
      // Commit the previewed theme so Cancel on reopen doesn't roll it back.
      _uiThemeOriginal = _chosenTheme || 'dark';
      _customSpecOriginal = _customSpec;
      _accentOverrideOriginal = payload.settings.ui_accent_override || '';
      if (window.setUiTheme) window.setUiTheme(_uiThemeOriginal, _customSpecOriginal);
      // Re-apply the saved accent override (or clear it) so the
      // committed state matches what the user just persisted.
      if (window.setAccentOverride) {
        window.setAccentOverride(_accentOverrideOriginal || null);
      }
      
    } else {
      showSettingsSavedToast('Save failed', { kind: 'error' });
      setTimeout(revealInvalidSettingsFieldCategory, 0);
    }
  }
  window.saveSettings = saveSettings;

  async function setPassword() {
    const pw = document.getElementById('cfgNewPassword').value.trim();
    const minutes = parseInt(document.getElementById('cfgSessionMinutes').value) || 1440;
    const msg = document.getElementById('pwMsg');
    if (pw) {
      const r = await fetch('/api/auth/set-password', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ password: pw }),
      });
      const d = await r.json();
      if (d.error) { msg.style.cssText = 'display:block;color:var(--red)'; msg.textContent = d.error; return; }
    }
    // Persist the idle-timeout field — historically this was read but
    // never saved, leaving the DB default (24h) locked in. Save it
    // unconditionally so the user doesn't need a password change to
    // adjust the timeout.
    try {
      const rs = await fetch('/api/auth/session-minutes', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ minutes }),
      });
      const ds = await rs.json();
      if (ds.error) {
        msg.style.cssText = 'display:block;color:var(--red)';
        msg.textContent = ds.error;
        return;
      }
    } catch (e) {
      msg.style.cssText = 'display:block;color:var(--red)';
      msg.textContent = 'Could not save session timeout';
      return;
    }
    msg.style.cssText = 'display:block;color:var(--green)';
    msg.textContent = pw ? 'Password and session timeout updated' : 'Session timeout saved';
    document.getElementById('cfgNewPassword').value = '';
    setTimeout(() => msg.style.display = 'none', 3000);
  }

  async function removePassword() {
    if (!confirm('Remove password protection? Anyone on the network will be able to access Top-Shelf.')) return;
    const r = await fetch('/api/auth/remove-password', { method: 'POST' });
    const d = await r.json();
    const msg = document.getElementById('pwMsg');
    msg.style.cssText = 'display:block;color:var(--amber)';
    msg.textContent = 'Password removed — access is now open';
    setTimeout(() => msg.style.display = 'none', 3000);
  }
  async function refreshIndexers() {
    const btn = event.target;
    btn.disabled = true;
    btn.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Refreshing"></span> Refreshing…';
    try {
      const r = await fetch('/api/prowlarr/indexers/refresh', { method: 'POST' });
      const d = await r.json();
      document.getElementById('indexerCount').textContent = `${d.count} indexers cached`;
    } catch(e) {
      document.getElementById('indexerCount').textContent = 'Failed';
    } finally {
      btn.disabled = false;
      btn.textContent = 'Refresh indexers';
    }
  }
  async function runTpdbSync() {
    const el = document.getElementById('tpdbSyncStatus');
    el.textContent = 'Syncing...';
    el.style.color = 'var(--accent)';
    try {
      const r = await fetch('/api/tpdb/sync', { method: 'POST' });
      const d = await r.json();
      if (d.skipped) { el.textContent = 'Sync disabled'; el.style.color = 'var(--dim)'; }
      else if (d.error) { el.textContent = d.error; el.style.color = 'var(--red)'; }
      else {
        let msg = `Created ${d.count} folder(s)`;
        if (d.to_favs) {
          const p = d.to_favs.performers || {};
          const s = d.to_favs.studios || {};
          const totalAdded = (p.added||0) + (s.added||0);
          const totalSkipped = (p.skipped||0) + (s.skipped||0);
          if (totalAdded || totalSkipped) {
            msg += ` · Favs: ${totalAdded} added`;
            if (totalSkipped) msg += `, ${totalSkipped} already`;
          }
        }
        el.textContent = msg;
        el.style.color = 'var(--green)';
      }
    } catch(e) {
      el.textContent = 'Failed'; el.style.color = 'var(--red)';
    }
  }

  // ── Collapsible & toggle helpers ─────────────────────────────────────

  function toggleSettingsCategory(catId) {
    const cat = document.querySelector('#settingsAccordion .settings-category[data-settings-category="' + catId + '"]');
    if (!cat) return;
    const opening = !cat.classList.contains('is-open');
    document.querySelectorAll('#settingsAccordion .settings-category').forEach(c => {
      c.classList.remove('is-open');
      const h = c.querySelector('.settings-category-header');
      if (h) h.setAttribute('aria-expanded', 'false');
    });
    if (opening) {
      cat.classList.add('is-open');
      const h = cat.querySelector('.settings-category-header');
      if (h) h.setAttribute('aria-expanded', 'true');
    }
  }

  function syncHealthPhash3IntervalDisabled() {
    const cb = document.getElementById('cfgHealthPhash3Recurring');
    const days = document.getElementById('cfgHealthPhash3IntervalDays');
    if (!cb || !days) return;
    days.disabled = !cb.checked;
  }

  async function runStudioLogosResync() {
    const btn = document.getElementById('btnStudioLogosResync');
    const msg = document.getElementById('studioLogosResyncMsg');
    if (!btn || !msg) return;
    btn.disabled = true;
    const origHtml = btn.innerHTML;
    btn.innerHTML = (typeof loaderHtml === 'function' ? loaderHtml('loader--btn') : '<span class="loader loader--btn" role="status" aria-label="Loading"></span>') + ' Working…';
    msg.textContent = '';
    msg.style.color = 'var(--dim)';
    try {
      const r = await fetch('/api/studio-logos/resync', { method: 'POST' });
      const d = await r.json();
      if (!r.ok || d.error) {
        msg.style.color = 'var(--red)';
        msg.textContent = d.error || ('HTTP ' + r.status);
        return;
      }
      const sync = d.sync || {};
      const parts = [];
      parts.push(`raw ${sync.raw_entries || 0}`);
      parts.push(`png ${sync.inspected || 0}`);
      if (sync.skipped_hidden)          parts.push(`${sync.skipped_hidden} hidden`);
      if (sync.skipped_dir)             parts.push(`${sync.skipped_dir} subdirs`);
      if (sync.skipped_not_regular)     parts.push(`${sync.skipped_not_regular} not-regular`);
      if (sync.skipped_symlink_broken)  parts.push(`${sync.skipped_symlink_broken} broken-symlinks`);
      if (sync.skipped_unreadable)      parts.push(`${sync.skipped_unreadable} unreadable`);
      if (sync.skipped_non_png)         parts.push(`${sync.skipped_non_png} non-png`);
      if (sync.skipped_no_slug)         parts.push(`${sync.skipped_no_slug} empty-slug`);
      if (sync.duplicate_slug)          parts.push(`${sync.duplicate_slug} dup-slugs`);
      if (sync.normalised)              parts.push(`${sync.normalised} converted`);
      if (sync.normalise_failed)        parts.push(`${sync.normalise_failed} bad`);
      if (sync.new)                     parts.push(`${sync.new} new`);
      if (sync.backfilled)              parts.push(`${sync.backfilled} backfilled`);
      if (sync.updated)                 parts.push(`${sync.updated} replaced`);
      if (sync.unchanged)               parts.push(`${sync.unchanged} unchanged`);
      if (d.discovered)                 parts.push(`${d.discovered} discovered`);
      if (d.fetched)                    parts.push(`${d.fetched} fetched`);
      if (d.fetch_failed)               parts.push(`${d.fetch_failed} fetch-failed`);
      if (d.fetch_passes)               parts.push(`${d.fetch_passes} passes`);
      if (d.fetch_duration_s)           parts.push(`${d.fetch_duration_s}s`);
      if (d.fetch_skipped)              parts.push(`skipped (${d.fetch_skipped})`);
      parts.push(`${d.rows_with_logo || 0}/${d.total_rows || 0} logos · ${d.pending_after || 0} pending`);
      if (d.unresolvable)               parts.push(`${d.unresolvable} unresolved`);
      msg.style.color = 'var(--green)';
      msg.textContent = parts.join(' · ');
      // Surface skipped files inline so the user can rename/remove them
      // without having to crack open the devtools console.
      renderStudioLogosSkipped(sync.sample_skipped || []);
      // Auto-open the unresolvable panel if the drain left anything parked.
      if (d.unresolvable) {
        loadStudioLogosUnresolvable();
      }
    } catch (e) {
      msg.style.color = 'var(--red)';
      msg.textContent = e.message || String(e);
    } finally {
      btn.disabled = false;
      btn.innerHTML = origHtml;
    }
  }

  function renderStudioLogosSkipped(samples) {
    const box = document.getElementById('studioLogosSkippedBox');
    const list = document.getElementById('studioLogosSkippedList');
    if (!box || !list) return;
    if (!samples || !samples.length) {
      box.style.display = 'none';
      list.innerHTML = '';
      return;
    }
    list.innerHTML = samples.map(line => {
      const text = String(line).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
      return `<li>${text}</li>`;
    }).join('');
    box.style.display = '';
  }

  async function loadStudioLogosUnresolvable() {
    const box = document.getElementById('studioLogosUnresolvedBox');
    const list = document.getElementById('studioLogosUnresolvedList');
    const count = document.getElementById('studioLogosUnresolvedCount');
    if (!box || !list || !count) return;
    try {
      const r = await fetch('/api/studio-logos/unresolvable');
      const d = await r.json();
      const rows = d.rows || [];
      count.textContent = rows.length;
      if (!rows.length) {
        list.innerHTML = '<div style="color:var(--dim)">Nothing parked — every pending studio is still in line for a fetch.</div>';
        box.style.display = '';
        return;
      }
      const esc = (v) => String(v == null ? '' : v).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
      list.innerHTML = rows.map(row => `
        <div style="display:grid;grid-template-columns:minmax(0,1fr) 60px minmax(0,1.5fr) auto auto;gap:8px;align-items:center;padding:6px 8px;border-radius:6px;background:rgba(255,255,255,0.03)">
          <div style="min-width:0">
            <div style="color:var(--text);font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="${esc(row.name)}">${esc(row.name || '(no name)')}</div>
            <div style="color:var(--dim);font-size:11px;font-family:monospace;white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="${esc(row.slug)}">${esc(row.slug || '')}</div>
          </div>
          <div style="text-align:center;color:var(--dim);font-variant-numeric:tabular-nums">${row.fetch_attempts}×</div>
          <div style="color:var(--dim);font-size:11px;font-family:monospace;white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="${esc(row.last_error)}">${esc(row.last_error || '')}</div>
          <button type="button" class="btn-secondary" style="padding:3px 8px;font-size:11px" onclick="retryStudioLogoRow(${row.id}, this)" title="Reset attempts"><i class="fa-solid fa-rotate-right"></i></button>
          <button type="button" class="btn-secondary" style="padding:3px 8px;font-size:11px;color:var(--red)" onclick="deleteStudioLogoRow(${row.id}, this, ${JSON.stringify(row.name || '').replace(/</g,'&lt;').replace(/"/g,'&quot;')})" title="Delete row"><i class="fa-solid fa-trash"></i></button>
        </div>
      `).join('');
      box.style.display = '';
    } catch (e) {
      list.innerHTML = `<div style="color:var(--red)">Failed: ${String(e.message || e)}</div>`;
      box.style.display = '';
    }
  }

  function closeStudioLogosUnresolved() {
    const box = document.getElementById('studioLogosUnresolvedBox');
    if (box) box.style.display = 'none';
  }

  async function retryStudioLogoRow(id, btn) {
    if (btn) { btn.disabled = true; btn.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Loading"></span>'; }
    try {
      const r = await fetch('/api/studio-logos/retry', {
        method: 'POST', credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ids: [id] })
      });
      const d = await r.json();
      if (!d.ok) throw new Error(d.error || 'retry failed');
      loadStudioLogosUnresolvable();
    } catch (e) {
      if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-rotate-right"></i>'; }
      window.toast(e.message || 'retry failed');
    }
  }

  async function retryAllStudioLogos() {
    if (!confirm('Reset the attempt counter on every parked studio? They will re-enter the fetch queue on the next resync.')) return;
    try {
      const r = await fetch('/api/studio-logos/retry', {
        method: 'POST', credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({})
      });
      const d = await r.json();
      if (!d.ok) throw new Error(d.error || 'retry failed');
      loadStudioLogosUnresolvable();
    } catch (e) {
      window.toast(e.message || 'retry failed');
    }
  }

  async function deleteStudioLogoRow(id, btn, name) {
    if (!confirm(`Delete "${name || '(unnamed)'}" from studio_logos? This removes only the database row; any PNG file on disk is untouched.`)) return;
    if (btn) { btn.disabled = true; btn.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Loading"></span>'; }
    try {
      const r = await fetch('/api/studio-logos/delete', {
        method: 'POST', credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ids: [id] })
      });
      const d = await r.json();
      if (!d.ok) throw new Error(d.error || 'delete failed');
      loadStudioLogosUnresolvable();
    } catch (e) {
      if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-trash"></i>'; }
      window.toast(e.message || 'delete failed');
    }
  }

  function _activeSettingsCategoryLabel() {
    const active = document.querySelector('#settingsRoot .settings-nav-item.active');
    if (!active) return '';
    const label = active.querySelector('.settings-nav-label');
    return label ? String(label.textContent || '').trim() : '';
  }

  function selectSettingsCategory(catId) {
    const modal = document.getElementById('settingsRoot');
    if (!modal) return;
    document.querySelectorAll('#settingsRoot .settings-nav-item').forEach(n => {
      n.classList.remove('has-validation-error');
    });
    document.querySelectorAll('#settingsRoot .settings-category-panel').forEach(p => {
      const match = p.getAttribute('data-settings-category') === catId;
      /* Match stylesheet `display:flex` — inline `block` warps nested flex layouts. */
      p.style.display = match ? 'flex' : 'none';
    });
    document.querySelectorAll('#settingsRoot .settings-nav-item').forEach(n => {
      const on = n.getAttribute('data-settings-category') === catId;
      n.classList.toggle('active', on);
      n.setAttribute('aria-selected', on ? 'true' : 'false');
    });
    const scrollEl = document.getElementById('settingsPanelScroll');
    if (scrollEl) scrollEl.scrollTop = 0;
  }
  window.selectSettingsCategory = selectSettingsCategory;

  // Queue modal embedded `<div class="modal">` inside #settingsRoot; standalone /settings uses
  // `.settings-shell` only — resetting category whenever #settingsRoot.class mutates breaks nav there.
  (function initSettingsModalCategoryObserver() {
    const modal = document.getElementById('settingsRoot');
    if (!modal || !modal.querySelector('.modal')) return;
    const obs = new MutationObserver(function () {
      if (!modal) return;
      selectSettingsCategory('security');
    });
    obs.observe(modal, { attributes: true, attributeFilter: ['class'] });
  })();

  function revealInvalidSettingsFieldCategory() {
    const modal = document.getElementById('settingsRoot');
    if (!modal || !modal) return;
    const inv = document.querySelector('#settingsRoot input:invalid, #settingsRoot textarea:invalid, #settingsRoot select:invalid');
    if (!inv) return;
    const panel = inv.closest('.settings-category-panel');
    if (!panel) return;
    const cat = panel.getAttribute('data-settings-category');
    if (!cat) return;
    selectSettingsCategory(cat);
    const navBtn = document.querySelector('#settingsRoot .settings-nav-item[data-settings-category="' + cat + '"]');
    if (navBtn) navBtn.classList.add('has-validation-error');
    try { inv.focus({ preventScroll: true }); } catch (e) {}
    inv.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  }

  function toggleSection(bodyId, show) {
    const el = document.getElementById(bodyId);
    if (el) el.style.display = show ? 'block' : 'none';
  }

  function toggleNzbFields() {
    const clientEl = document.getElementById('cfgDlNzbClient');
    const client = clientEl ? clientEl.value : '';
    const row = document.getElementById('nzbApiKeyRow');
    if (row) row.style.display = client === 'sabnzbd' ? '' : 'none';
    _rebuildNzbPriorityOptions();
  }

  function _setDlTestStatus(el, state, text) {
    if (!el) return;
    el.textContent = text || '';
    el.classList.remove('dl-test-status--ok', 'dl-test-status--err', 'dl-test-status--busy');
    if (state) el.classList.add('dl-test-status--' + state);
  }

  function _dlClientConfigFromForm(kind) {
    if (kind === 'nzb') {
      return {
        client: (document.getElementById('cfgDlNzbClient')?.value || '').trim(),
        host: (document.getElementById('cfgDlNzbHost')?.value || '').trim(),
        port: (document.getElementById('cfgDlNzbPort')?.value || '').trim(),
        user: (document.getElementById('cfgDlNzbUser')?.value || '').trim(),
        pass: document.getElementById('cfgDlNzbPass')?.value || '',
        api_key: document.getElementById('cfgDlNzbApiKey')?.value || '',
      };
    }
    return {
      client: (document.getElementById('cfgDlTorrentClient')?.value || '').trim(),
      host: (document.getElementById('cfgDlTorrentHost')?.value || '').trim(),
      port: (document.getElementById('cfgDlTorrentPort')?.value || '').trim(),
      user: (document.getElementById('cfgDlTorrentUser')?.value || '').trim(),
      pass: document.getElementById('cfgDlTorrentPass')?.value || '',
    };
  }

  async function _testDlClient(kind) {
    const btnId = kind === 'nzb' ? 'dlNzbTestBtn' : 'dlTorrentTestBtn';
    const statusId = kind === 'nzb' ? 'dlNzbTestStatus' : 'dlTorrentTestStatus';
    const btn = document.getElementById(btnId);
    const status = document.getElementById(statusId);
    const cfg = _dlClientConfigFromForm(kind);
    if (!cfg.client) {
      _setDlTestStatus(status, 'err', 'Select a client first');
      return;
    }
    if (!cfg.host) {
      _setDlTestStatus(status, 'err', 'Host is required');
      return;
    }
    const prevLabel = btn ? btn.innerHTML : '';
    if (btn) {
      btn.disabled = true;
      btn.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Testing"></span> Testing…';
    }
    _setDlTestStatus(status, 'busy', 'Connecting…');
    try {
      const r = await fetch('/api/settings/test-download-client', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify({ kind, config: cfg }),
      });
      const d = await r.json();
      if (d.ok) {
        _setDlTestStatus(status, 'ok', d.message || 'Connected');
      } else {
        _setDlTestStatus(status, 'err', d.error || 'Connection failed');
      }
    } catch (e) {
      _setDlTestStatus(status, 'err', 'Request failed — is Top-Shelf running?');
    } finally {
      if (btn) {
        btn.disabled = false;
        btn.innerHTML = prevLabel || 'Test connection';
      }
    }
  }

  async function testDlNzbClient() {
    return _testDlClient('nzb');
  }
  window.testDlNzbClient = testDlNzbClient;

  async function testDlTorrentClient() {
    return _testDlClient('torrent');
  }
  window.testDlTorrentClient = testDlTorrentClient;

  // Each client has its own native priority scale. The dropdown's
  // <option> list rebuilds when the client picker changes so the user
  // only sees the values that map to a real API field. Saved as a
  // single setting (`dl_nzb_priority` / `dl_torrent_priority`) and
  // interpreted server-side by whichever client branch is active.
  const _NZB_PRIORITY_OPTIONS = {
    nzbget: [
      ['-100', 'Very Low'], ['-50', 'Low'], ['0', 'Normal'],
      ['50', 'High'], ['100', 'Very High'], ['900', 'Force'],
    ],
    sabnzbd: [
      ['-2', 'Paused'], ['-1', 'Low'], ['0', 'Normal'],
      ['1', 'High'], ['2', 'Force'],
    ],
  };
  const _TORRENT_PRIORITY_OPTIONS = {
    qbittorrent:  [['top', 'Top'],    ['normal', 'Normal'], ['bottom', 'Bottom']],
    transmission: [['low', 'Low'],    ['normal', 'Normal'], ['high', 'High']],
    deluge:       [['top', 'Top'],    ['normal', 'Normal'], ['bottom', 'Bottom']],
  };
  function _fillPrioritySelect(sel, options, currentValue) {
    if (!sel) return;
    sel.innerHTML = options.map(([v, l]) =>
      `<option value="${v}">${l}</option>`
    ).join('');
    // Preserve the user's selection if the new options still include it,
    // otherwise fall back to the neutral middle option (always Normal).
    const values = options.map(o => o[0]);
    sel.value = values.includes(currentValue) ? currentValue : (
      values.includes('0') ? '0' : (values.includes('normal') ? 'normal' : values[0])
    );
  }
  function _rebuildNzbPriorityOptions() {
    const client = document.getElementById('cfgDlNzbClient').value;
    const sel = document.getElementById('cfgDlNzbPriority');
    const row = document.getElementById('nzbPriorityRow');
    const opts = _NZB_PRIORITY_OPTIONS[client];
    if (!opts) {
      if (row) row.style.display = 'none';
      return;
    }
    if (row) row.style.display = '';
    _fillPrioritySelect(sel, opts, sel ? sel.value : '');
  }
  function _rebuildTorrentPriorityOptions() {
    const client = document.getElementById('cfgDlTorrentClient').value;
    const sel = document.getElementById('cfgDlTorrentPriority');
    const row = document.getElementById('torrentPriorityRow');
    const opts = _TORRENT_PRIORITY_OPTIONS[client];
    if (!opts) {
      if (row) row.style.display = 'none';
      return;
    }
    if (row) row.style.display = '';
    _fillPrioritySelect(sel, opts, sel ? sel.value : '');
  }

  function toggleCollapsible(bodyId, arrowId) {
    const body  = document.getElementById(bodyId);
    const arrow = document.getElementById(arrowId);
    const open  = body.classList.toggle('open');
    if (arrow) arrow.classList.toggle('open', open);
  }

  document.addEventListener('DOMContentLoaded', function () {
    const sidebar = document.querySelector('#settingsRoot .settings-nav-sidebar');
    if (sidebar) {
      sidebar.addEventListener('click', function (ev) {
        const btn = ev.target.closest('.settings-nav-item');
        if (!btn || !sidebar.contains(btn)) return;
        const cat = btn.getAttribute('data-settings-category');
        if (!cat) return;
        ev.preventDefault();
        selectSettingsCategory(cat);
      });
    }

    const hash = (window.location.hash || '').replace(/^#/, '');
    loadSettingsPage()
      .then(function () {
        if (hash === 'rss' || hash === 'settingsSectionRssFeeds') {
          selectSettingsCategory('rss');
          setTimeout(function () {
            document.getElementById('settingsSectionRssFeeds')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
          }, 100);
        }
      })
      .catch(function (err) {
        console.error('Settings load failed:', err);
        const hint = (err && err.message) ? String(err.message).slice(0, 120) : '';
        showSettingsSavedToast(
          hint
            ? 'Failed to load settings: ' + hint
            : 'Failed to load settings — refresh the page before saving',
          { kind: 'error' }
        );
      });
  });

  function _settingsDismissEscapeLayers() {
    const root = document.getElementById('settingsRoot');
    if (!root) return false;
    let dismissed = false;
    root.querySelectorAll('.ts-tag-results').forEach((el) => {
      const open = el.style.display === 'block' ||
        (el.style.display !== 'none' && (el.innerHTML || '').trim());
      if (open) {
        el.style.display = 'none';
        el.innerHTML = '';
        dismissed = true;
      }
    });
    const ae = document.activeElement;
    if (ae && root.contains(ae)) {
      const isTagSearch = ae.id === 'cfgBlacklistTagSearch' ||
        (typeof ae.id === 'string' && ae.id.startsWith('cfgViceTagSearch_'));
      if (isTagSearch) {
        if (ae.value) ae.value = '';
        ae.blur();
        dismissed = true;
      }
    }
    return dismissed;
  }
window._settingsDismissEscapeLayers = _settingsDismissEscapeLayers;
window.closeSettings = settingsLeavePage;
