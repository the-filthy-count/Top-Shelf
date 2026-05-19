/* Universal studio popup — parallel to performer-popup.js.
 *
 * Loaded on every page that paints library highlighting. Listens for
 * clicks on `[data-studio-link]` elements (the green spans
 * `_libraryHighlight` emits for studio matches in ts-utils.js) and
 * opens a read-only modal with the studio's logo, description, recent
 * scenes, and quick actions (View in Library, Search Prowlarr).
 *
 * Public API:
 *   window.openStudioPopup({libraryRowId, name})
 *   window.closeStudioPopup()
 *
 * Compared with the /library entity panel, this popup is intentionally
 * minimal: no edit/lock/favourite controls. /library remains the
 * source of truth for those — clicking "View in Library" jumps there.
 */
(function () {
  if (window._studioPopupLoaded) return;
  window._studioPopupLoaded = true;

  const ESC = (s) => String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
  const ATTR = (s) => ESC(s).replace(/`/g, '&#96;');
  const toast = (msg) => (window.toast ? window.toast(msg) : console.warn(msg));

  let _activeRowId = null;
  let _activeName = null;

  /* ── Modal scaffolding ───────────────────────────────────────── */

  function ensureModal() {
    if (document.getElementById('studioPopupModal')) return;
    const div = document.createElement('div');
    div.id = 'studioPopupModal';
    div.className = 'studio-popup-overlay';
    div.innerHTML = `
      <div class="studio-popup-shell" role="dialog" aria-modal="true" aria-label="Studio details">
        <button type="button" class="studio-popup-close" aria-label="Close" title="Close">
          <i class="fa-solid fa-xmark"></i>
        </button>
        <div class="studio-popup-hero">
          <img class="studio-popup-logo" alt="" referrerpolicy="no-referrer" onerror="this.style.display='none'">
          <h2 class="studio-popup-title" id="studioPopupTitle">Loading…</h2>
          <div class="studio-popup-meta" id="studioPopupMeta"></div>
        </div>
        <div class="studio-popup-body">
          <div class="studio-popup-desc" id="studioPopupDesc"></div>
          <div class="studio-popup-section-label" id="studioPopupScenesLabel" style="display:none">
            <i class="fa-solid fa-film"></i> Recent scenes
          </div>
          <div class="studio-popup-scenes" id="studioPopupScenes"></div>
        </div>
        <div class="studio-popup-foot">
          <button type="button" class="studio-popup-btn studio-popup-btn--prowlarr" id="studioPopupProwlarr">
            <i class="fa-solid fa-magnifying-glass"></i> Search Prowlarr
          </button>
          <a class="studio-popup-btn studio-popup-btn--library" id="studioPopupLibrary" href="/library">
            <i class="fa-solid fa-ticket"></i> View in Library
          </a>
        </div>
      </div>`;
    document.body.appendChild(div);
    div.addEventListener('click', (e) => {
      if (e.target === div) closeStudioPopup();
    });
    div.querySelector('.studio-popup-close').addEventListener('click', closeStudioPopup);
    div.querySelector('#studioPopupProwlarr').addEventListener('click', () => {
      if (typeof window.openPerformerProwlarrSearchPopup === 'function' && _activeName) {
        window.openPerformerProwlarrSearchPopup({
          name: _activeName,
          posterUrl: '',
          headshotUrl: '',
          showCenterPlaceholder: false,
        });
      } else {
        toast('Prowlarr search unavailable on this page');
      }
    });
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && div.classList.contains('open')) closeStudioPopup();
    });
  }

  function closeStudioPopup() {
    const m = document.getElementById('studioPopupModal');
    if (m) {
      m.classList.remove('open');
      // Defensive — same fix the performer popup needed when Escape's
      // generic fallback set inline display:none.
      if (m.style.display) m.style.display = '';
    }
    _activeRowId = null;
    _activeName = null;
    window._studioPopupActiveId = null;
  }
  window.closeStudioPopup = closeStudioPopup;

  /* ── Open + load ─────────────────────────────────────────────── */

  async function openStudioPopup(opts) {
    ensureModal();
    opts = opts || {};
    const m = document.getElementById('studioPopupModal');
    if (m.style.display) m.style.display = '';
    m.classList.add('open');
    _activeRowId = opts.libraryRowId || null;
    _activeName = opts.name || '';
    window._studioPopupActiveId = _activeRowId;

    // Reset to skeleton state.
    document.getElementById('studioPopupTitle').textContent = _activeName || 'Loading…';
    document.getElementById('studioPopupMeta').textContent = '';
    document.getElementById('studioPopupDesc').innerHTML = '';
    document.getElementById('studioPopupScenes').innerHTML = '';
    document.getElementById('studioPopupScenesLabel').style.display = 'none';
    const logo = m.querySelector('.studio-popup-logo');
    logo.style.display = '';
    logo.src = '';
    const libBtn = document.getElementById('studioPopupLibrary');
    libBtn.href = _activeRowId ? `/library?id=${encodeURIComponent(_activeRowId)}` : '/library';

    if (!_activeRowId) {
      document.getElementById('studioPopupDesc').innerHTML =
        '<div class="studio-popup-empty">Studio is not in your library.</div>';
      logo.style.display = 'none';
      return;
    }

    try {
      // /api/favourites/entity-panel returns: row, tpdb_scenes, scenes_source.
      // The row carries name, image_url (logo), match counts, etc.
      const startedRow = _activeRowId;
      const r = await fetch('/api/favourites/entity-panel?row_id=' + encodeURIComponent(_activeRowId), {
        credentials: 'same-origin',
      });
      const d = await r.json().catch(() => ({}));
      // Bail if the user navigated to a different studio mid-fetch.
      if (_activeRowId !== startedRow) return;
      if (!r.ok) {
        document.getElementById('studioPopupDesc').innerHTML =
          `<div class="studio-popup-empty" style="color:var(--red)">${ESC(d.error || ('HTTP ' + r.status))}</div>`;
        return;
      }
      renderRow(d);
      // Description is a separate endpoint (TPDB / StashDB scrape).
      // Fire it after the panel paints so the recent scenes don't wait.
      fetch('/api/favourites/studio-detail?row_id=' + encodeURIComponent(_activeRowId), {
        credentials: 'same-origin',
      })
        .then((rr) => rr.json())
        .then((dd) => {
          if (_activeRowId !== startedRow) return;
          const descEl = document.getElementById('studioPopupDesc');
          const desc = (dd && dd.description) || '';
          if (desc) {
            descEl.innerHTML = `<div class="studio-popup-desc-body">${ESC(desc)}</div>` +
              (dd.source ? `<div class="studio-popup-desc-source">via ${ESC(dd.source)}</div>` : '');
          }
        })
        .catch(() => {});
    } catch (e) {
      document.getElementById('studioPopupDesc').innerHTML =
        `<div class="studio-popup-empty" style="color:var(--red)">${ESC(e.message || 'Failed to load')}</div>`;
    }
  }
  window.openStudioPopup = openStudioPopup;

  function renderRow(d) {
    const row = (d && d.row) || {};
    const scenes = (d && d.tpdb_scenes) || [];
    const m = document.getElementById('studioPopupModal');
    const logo = m.querySelector('.studio-popup-logo');
    const name = row.display_name || row.folder_name || _activeName || 'Studio';
    document.getElementById('studioPopupTitle').textContent = name;
    _activeName = name;
    // Meta line: scene count if known + match-source pills (TPDB/Stash/Fans).
    const metaBits = [];
    if (row.scene_count != null) metaBits.push(row.scene_count + ' scenes in library');
    if (row.match_tpdb_id) metaBits.push('TPDB');
    if (row.match_stashdb_id) metaBits.push('StashDB');
    if (row.match_fansdb_id) metaBits.push('FansDB');
    document.getElementById('studioPopupMeta').textContent = metaBits.join(' · ');
    if (row.image_url) {
      logo.src = row.image_url;
      logo.style.display = '';
    } else {
      logo.style.display = 'none';
    }
    if (scenes.length) {
      document.getElementById('studioPopupScenesLabel').style.display = '';
      document.getElementById('studioPopupScenes').innerHTML = scenes.slice(0, 8).map((s) => {
        const img = s.image || s.poster || s.thumb || '';
        const title = s.title || '(untitled)';
        const date = s.release_date || s.date || '';
        return `<div class="studio-popup-scene-tile">
          ${img ? `<img class="studio-popup-scene-img" src="${ATTR(img)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.style.display='none'">` : ''}
          <div class="studio-popup-scene-meta">
            <div class="studio-popup-scene-title" title="${ATTR(title)}">${ESC(title)}</div>
            ${date ? `<div class="studio-popup-scene-date">${ESC(date)}</div>` : ''}
          </div>
        </div>`;
      }).join('');
    }
  }

  /* ── Delegated click handler ─────────────────────────────────── */

  // Mirrors performer-popup.js's pattern. Listens on documentElement so
  // it works even when this script loads in <head>. capture:false (the
  // default) lets per-element handlers run first. Only catches clicks
  // on `[data-studio-link]` elements that aren't nested inside another
  // button / link — same defensive bail-out.
  const _delegatedClick = (e) => {
    const el = e.target.closest && e.target.closest('[data-studio-link]');
    if (!el) return;
    // Same containment check as performer-popup.js: bail only when the
    // interactive element is *inside* the linked element. That way a
    // green studio span nested in `<a class="news-tile">` still opens
    // the popup instead of letting the wrapping anchor navigate.
    const interactive = e.target.closest('button, a:not([data-studio-link])');
    if (interactive && el.contains(interactive)) return;
    e.preventDefault();
    e.stopPropagation();
    openStudioPopup({
      libraryRowId: el.dataset.libraryRowId ? parseInt(el.dataset.libraryRowId, 10) : null,
      name: el.dataset.name || el.textContent.trim() || null,
    });
  };
  document.documentElement.addEventListener('click', _delegatedClick, false);
})();
