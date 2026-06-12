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
  let _activeOpts = null;
  let _activeRow  = null;   // last row payload from /entity-panel — drives lock-button state

  /* ── Modal scaffolding ───────────────────────────────────────── */

  function ensureModal() {
    if (document.getElementById('studioPopupModal')) return;
    const div = document.createElement('div');
    div.id = 'studioPopupModal';
    div.className = 'studio-popup-overlay';
    // Mirror the performer popup's title-bar markup so existing
    // ``.performer-popup-header`` / ``.pp-name`` / ``.pp-profile-pill``
    // CSS applies here without duplication: Shatter font + uppercase
    // + pills row + flex toolbar with the lib-entity-actions hamburger
    // and a single close button.
    div.innerHTML = `
      <div class="studio-popup-shell" role="dialog" aria-modal="true" aria-label="Studio details">
        <header class="studio-popup-header performer-popup-header" id="studioPopupHeader">
          <h2 class="pp-name">
            <span class="pp-lib-entity-actions" id="studioPopupMenuSlot"></span>
            <span class="pp-name-text" id="studioPopupTitle">Loading…</span>
            <span class="pp-name-count" id="studioPopupVideoCount" hidden>
              <span class="pp-name-count-val" id="studioPopupVideoCountVal"></span>
              <i class="fa-solid fa-video pp-name-count-icon" aria-hidden="true"></i>
            </span>
          </h2>
          <div class="pp-profiles-grid pp-profiles-grid--header" id="studioPopupPills"></div>
          <div class="performer-popup-toolbar">
            <button type="button" class="performer-popup-tool" id="studioPopupFavBtn" aria-label="Favourite" title="Favourite">
              <i class="fa-solid fa-heart"></i>
            </button>
            <button type="button" class="performer-popup-tool" id="studioPopupRefreshBtn" aria-label="Refresh from linked DBs" title="Refresh from linked DBs">
              <i class="fa-solid fa-arrows-rotate"></i>
            </button>
            <button type="button" class="performer-popup-tool" id="studioPopupLockBtn" aria-label="Lock matches" title="Lock matches">
              <i class="fa-solid fa-lock-open"></i>
            </button>
            <button type="button" class="performer-popup-tool studio-popup-close" aria-label="Close" title="Close">
              <i class="fa-solid fa-xmark"></i>
            </button>
          </div>
        </header>
        <div class="studio-popup-grid">
          <section class="studio-popup-cell studio-popup-info-cell">
            <div class="studio-popup-logo-box">
              <a class="studio-popup-logo-link" id="studioPopupLogoLink" rel="noopener noreferrer" target="_blank">
                <img class="studio-popup-logo" alt="" referrerpolicy="no-referrer" onerror="this.closest('.studio-popup-logo-box')?.classList.add('is-empty');this.style.display='none'">
              </a>
              <div class="studio-popup-linked-logos" id="studioPopupLinkedLogos" hidden></div>
            </div>
            <div class="studio-popup-desc-box">
              <div class="studio-popup-desc" id="studioPopupDesc"></div>
              <div class="studio-popup-links" id="studioPopupLinks" hidden></div>
            </div>
            <div class="studio-popup-parent-box">
              <a class="studio-popup-parent-link" id="studioPopupParentLink" rel="noopener noreferrer" target="_blank">
                <img class="studio-popup-parent-logo" id="studioPopupParentLogo" alt="" loading="lazy" decoding="async" onerror="this.closest('.studio-popup-parent-box')?.classList.add('is-empty');this.style.display='none'">
              </a>
            </div>
          </section>
          <section class="studio-popup-cell studio-popup-latest-cell">
            <div id="studioPopupLatest" class="studio-popup-latest">
              <div class="studio-popup-empty">Loading latest scene…</div>
            </div>
          </section>
          <section class="studio-popup-cell studio-popup-scenes-cell">
            <div class="studio-popup-section-label" id="studioPopupScenesLabel" style="display:none">
              <i class="ts-icon-scenes" aria-hidden="true"></i> Recent scenes
            </div>
            <div class="studio-popup-scenes" id="studioPopupScenes"></div>
          </section>
          <section class="studio-popup-cell studio-popup-prowlarr-cell">
            <div class="studio-popup-section-label studio-popup-section-label--prowlarr">
              <img src="/static/logos/prowlarr.webp" alt="" onerror="this.replaceWith(Object.assign(document.createElement('i'),{className:'fa-solid fa-magnifying-glass'}))"> Prowlarr
            </div>
            <div class="ts-prowlarr-embed studio-popup-prowlarr-embed" id="studioPopupProwlarrEmbed">
              <div class="ts-prowlarr-embed-status ts-prowlarr-popup-status">Waiting for studio…</div>
              <div class="ts-prowlarr-embed-filters ts-prowlarr-popup-filters ts-prowlarr-filter-bar" hidden></div>
              <div class="ts-prowlarr-embed-results ts-prowlarr-popup-results"></div>
            </div>
          </section>
        </div>
      </div>`;
    document.body.appendChild(div);
    div.addEventListener('click', (e) => {
      if (e.target === div) closeStudioPopup();
    });
    div.querySelector('.studio-popup-close').addEventListener('click', closeStudioPopup);
    const refreshBtn = div.querySelector('#studioPopupRefreshBtn');
    if (refreshBtn) refreshBtn.addEventListener('click', refreshFromLinkedDbs);
    const lockBtn = div.querySelector('#studioPopupLockBtn');
    if (lockBtn) lockBtn.addEventListener('click', toggleMatchesLock);
    const favBtn = div.querySelector('#studioPopupFavBtn');
    if (favBtn) favBtn.addEventListener('click', toggleFavourite);
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && div.classList.contains('open')) closeStudioPopup();
    });
  }

  /* ── Logo picker (inline modal) ──────────────────────────────── */

  let _logoPickItems = [];
  let _logoPickSelectedIdx = -1;
  //: Snapshot of the studio row id at picker-open time. The studio
  //: popup's own backdrop click handler can fire while the picker is
  //: open (clicking outside the picker but inside the studio overlay),
  //: nulling `_activeRowId` and silently breaking Apply. Capturing it
  //: here keeps the apply call self-contained.
  let _logoPickRowId = null;

  function ensureLogoPickModal() {
    if (document.getElementById('studioLogoPickModal')) return;
    const div = document.createElement('div');
    div.id = 'studioLogoPickModal';
    div.className = 'modal-overlay';
    // The studio popup overlay sits at z-index 1500 and the generic
    // .modal-overlay rule in app-shell.css ships with
    // `z-index: 1200 !important`, so a plain inline style loses to
    // that !important. setProperty(... 'important') is the only way
    // to force a higher stacking from JS.
    div.style.setProperty('z-index', '1800', 'important');
    div.innerHTML = `
      <div class="modal-box ts-link-modal-box" style="max-width:1400px;width:min(1400px,calc(100vw - 60px));height:calc(100vh - 80px);max-height:none;display:flex;flex-direction:column">
        <h3 id="studioLogoPickTitle" style="margin:0 0 8px 0;font-family:var(--font-display, var(--mono));font-size:18px">Pick studio image</h3>
        <div style="font-size:11px;color:var(--dim);margin-bottom:10px">Sourced from your linked DB profiles. Click an image to select.</div>
        <label style="display:flex;align-items:center;gap:8px;font-size:11px;color:var(--text);margin-bottom:10px;cursor:pointer">
          <input type="checkbox" id="studioLogoPickSaveLocal" checked>
          <span>Save into this folder as <code>logo.png</code> (overwrites if present)</span>
        </label>
        <div id="studioLogoPickResults" style="flex:1 1 0;min-height:0;display:flex;flex-direction:column"></div>
        <div class="ts-link-modal-foot">
          <button type="button" id="studioLogoPickCancelBtn" style="background:transparent;border:1px solid rgba(255,255,255,0.15);color:var(--dim);padding:8px 16px;border-radius:6px;font-size:11px;font-family:var(--mono);cursor:pointer;text-transform:uppercase;letter-spacing:0.04em">Cancel</button>
          <button type="button" id="studioLogoPickApplyBtn" disabled style="background:rgba(var(--brand-purple-rgb),0.4);border:1px solid rgba(var(--brand-purple-rgb),0.65);color:var(--accent);padding:8px 16px;border-radius:6px;font-size:11px;font-family:var(--mono);cursor:pointer;text-transform:uppercase;letter-spacing:0.04em">Apply</button>
        </div>
      </div>`;
    document.body.appendChild(div);
    div.addEventListener('click', (e) => { if (e.target === div) closeLogoPickModal(); });
    document.getElementById('studioLogoPickCancelBtn').addEventListener('click', closeLogoPickModal);
    document.getElementById('studioLogoPickApplyBtn').addEventListener('click', applyPickedLogo);
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && div.classList.contains('open')) closeLogoPickModal();
    });
  }

  function closeLogoPickModal() {
    const m = document.getElementById('studioLogoPickModal');
    if (m) m.classList.remove('open');
    _logoPickItems = [];
    _logoPickSelectedIdx = -1;
    _logoPickRowId = null;
  }

  async function openLogoPickModal() {
    if (!_activeRowId) return;
    _logoPickRowId = _activeRowId;
    ensureLogoPickModal();
    const m = document.getElementById('studioLogoPickModal');
    const results = document.getElementById('studioLogoPickResults');
    const apply = document.getElementById('studioLogoPickApplyBtn');
    document.getElementById('studioLogoPickTitle').textContent = `Pick image — ${_activeName || 'Studio'}`;
    results.innerHTML = '<div class="ts-link-modal-empty">Loading linked images…</div>';
    apply.disabled = true;
    _logoPickSelectedIdx = -1;
    m.classList.add('open');
    try {
      const r = await fetch('/api/favourites/image-search?row_id=' + encodeURIComponent(_logoPickRowId), { credentials: 'same-origin' });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) {
        results.innerHTML = `<div class="ts-link-modal-empty" style="color:var(--red)">${ESC(d.error || ('HTTP ' + r.status))}</div>`;
        return;
      }
      _logoPickItems = Array.isArray(d.items) ? d.items : [];
      if (!_logoPickItems.length) {
        results.innerHTML = '<div class="ts-link-modal-empty">No images from linked profiles. Link a TPDB, StashDB, FansDB, or JAVStash studio first.</div>';
        return;
      }
      results.innerHTML = `<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:14px;flex:1 1 0;min-height:0;overflow:auto;padding:4px;align-content:start">
        ${_logoPickItems.map((x, i) => {
          const src = x.image || '';
          return `<button type="button" data-idx="${i}" class="studio-logo-pick-tile" style="position:relative;padding:0;background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.08);border-radius:8px;overflow:hidden;cursor:pointer;aspect-ratio:1;display:flex;align-items:center;justify-content:center" title="${ESC((x.name || '') + (x.source ? ' — ' + x.source : ''))}">
            ${src ? `<img src="${ESC(src)}" alt="" loading="lazy" referrerpolicy="no-referrer" style="max-width:100%;max-height:100%;object-fit:contain" onerror="this.remove()">` : '<i class="fa-solid fa-image" style="opacity:0.4"></i>'}
            <span style="position:absolute;left:6px;top:6px;font-size:10px;background:rgba(0,0,0,0.65);padding:3px 7px;border-radius:4px;color:#fff;letter-spacing:0.06em;text-transform:uppercase;font-family:var(--mono)">${ESC(x.source || '')}</span>
          </button>`;
        }).join('')}
      </div>`;
      results.querySelectorAll('.studio-logo-pick-tile').forEach((tile) => {
        tile.addEventListener('click', () => {
          const idx = parseInt(tile.dataset.idx, 10);
          const item = _logoPickItems[idx];
          if (!item || !item.image) return;
          _logoPickSelectedIdx = idx;
          results.querySelectorAll('.studio-logo-pick-tile').forEach((t) => {
            t.style.outline = t === tile ? '2px solid var(--accent, #c084fc)' : '';
            t.style.outlineOffset = t === tile ? '-2px' : '';
          });
          apply.disabled = false;
        });
      });
    } catch (e) {
      results.innerHTML = `<div class="ts-link-modal-empty" style="color:var(--red)">${ESC(e.message || 'Failed')}</div>`;
    }
  }

  //: Probe the row's folder for a local background/fanart/backdrop
  //: image and stamp it as a CSS custom property on the target cell so
  //: the ::before/::after backdrop rules in app-shell.css light up.
  //: Misses (404) and errored probes leave the cell in its default
  //: transparent state. We GET the endpoint with an Image preflight so
  //: we don't waste a fetch decoding bytes the CSS will fetch anyway.
  function applyCellFanart(cell, rowId) {
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

  function wireLogoPicker(m) {
    const box = m.querySelector('.studio-popup-logo-box');
    if (!box) return;
    // Idempotent: re-rendering after refresh would otherwise stack
    // handlers and pencil overlays.
    if (!box.dataset.pickerWired) {
      box.dataset.pickerWired = '1';
      box.addEventListener('click', (ev) => {
        const link = ev.target.closest('a.studio-popup-logo-link');
        if (link && link.classList.contains('is-clickable') && link.href) return; // let the website link open
        ev.preventDefault();
        ev.stopPropagation();
        openLogoPickModal();
      });
    }
    if (!box.querySelector('.studio-popup-logo-edit')) {
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'studio-popup-logo-edit';
      btn.title = 'Change studio image';
      btn.setAttribute('aria-label', 'Change studio image');
      btn.style.cssText = 'position:absolute;right:6px;top:6px;width:26px;height:26px;border-radius:50%;background:rgba(0,0,0,0.55);border:1px solid rgba(255,255,255,0.18);color:#fff;cursor:pointer;display:flex;align-items:center;justify-content:center;font-size:11px;z-index:2;opacity:0.85';
      btn.innerHTML = '<i class="fa-solid fa-pen"></i>';
      btn.addEventListener('click', (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        openLogoPickModal();
      });
      // The box needs to be positioned for the absolute pencil to land.
      if (getComputedStyle(box).position === 'static') box.style.position = 'relative';
      box.appendChild(btn);
    }
  }

  async function applyPickedLogo() {
    const apply = document.getElementById('studioLogoPickApplyBtn');
    const rid = _logoPickRowId;
    if (!rid) { toast('Studio context lost — reopen the picker'); return; }
    if (_logoPickSelectedIdx < 0) { toast('Pick an image first'); return; }
    const item = _logoPickItems[_logoPickSelectedIdx];
    if (!item) { toast('Selection went stale — reopen the picker'); return; }
    const saveLocal = document.getElementById('studioLogoPickSaveLocal').checked;
    const prev = (apply && apply.textContent) || 'Apply';
    if (apply) {
      apply.disabled = true;
      apply.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Saving"></span> Saving…';
    }
    // Local picks (an image already inside the studio folder) send
    // `local_name`; backend reads the bytes from disk instead of
    // trying to download a server-relative URL.
    const body = { row_id: rid, save_local: saveLocal };
    if (item.local_name) body.local_name = item.local_name;
    else body.image_url = item.image;
    try {
      const r = await fetch('/api/favourites/apply-image', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) {
        toast((d && d.error) || ('Could not apply image (HTTP ' + r.status + ')'));
        if (apply) { apply.disabled = false; apply.textContent = prev; }
        return;
      }
      closeLogoPickModal();
      toast(saveLocal ? 'Logo saved' : 'Logo applied');
      if (window._studioPopupActiveId === rid) window.refreshStudioPopup();
    } catch (e) {
      console.error('apply logo failed:', e);
      toast((e && e.message) || 'Apply failed');
      if (apply) { apply.disabled = false; apply.textContent = prev; }
    }
  }

  function closeStudioPopup() {
    const m = document.getElementById('studioPopupModal');
    if (m) {
      m.classList.remove('open');
      // Defensive — same fix the performer popup needed when Escape's
      // generic fallback set inline display:none.
      if (m.style.display) m.style.display = '';
      const embed = m.querySelector('#studioPopupProwlarrEmbed');
      if (embed && typeof window.unmountEmbeddedProwlarrSearch === 'function') {
        window.unmountEmbeddedProwlarrSearch(embed);
      }
    }
    if (_prowlarrIO) {
      try { _prowlarrIO.disconnect(); } catch (e) { /* noop */ }
      _prowlarrIO = null;
    }
    _prowlarrPendingName = null;
    _activeRowId = null;
    _activeName = null;
    _activeOpts = null;
    _activeRow = null;
    window._studioPopupActiveId = null;
  }
  window.closeStudioPopup = closeStudioPopup;
  window.refreshStudioPopup = function () {
    if (!_activeOpts) return;
    openStudioPopup({ ..._activeOpts, _refresh: true });
  };

  /* ── Open + load ─────────────────────────────────────────────── */

  async function openStudioPopup(opts) {
    ensureModal();
    opts = opts || {};
    const m = document.getElementById('studioPopupModal');
    if (m.style.display) m.style.display = '';
    m.classList.add('open');
    // Warm the shared wanted-keys cache so the eye on each scene tile
    // can paint its `.is-wanted` state on first render. Cheap, fires
    // once per session.
    if (typeof window.tsLoadWantedKeys === 'function') {
      window.tsLoadWantedKeys();
    }
    _activeRowId = opts.libraryRowId || null;
    _activeName = opts.name || '';
    _activeOpts = { ...opts };
    _activeRow = null;
    window._studioPopupActiveId = _activeRowId;

    // Reset to skeleton state.
    document.getElementById('studioPopupTitle').textContent = _activeName || 'Loading…';
    document.getElementById('studioPopupDesc').innerHTML = '<div class="studio-popup-empty">Loading description…</div>';
    document.getElementById('studioPopupScenes').innerHTML = '';
    document.getElementById('studioPopupLatest').innerHTML = '<div class="studio-popup-empty">Loading latest scene…</div>';
    document.getElementById('studioPopupScenesLabel').style.display = 'none';
    const pillsEl = document.getElementById('studioPopupPills');
    if (pillsEl) pillsEl.innerHTML = '';
    const linksEl = document.getElementById('studioPopupLinks');
    if (linksEl) { linksEl.innerHTML = ''; linksEl.hidden = true; }
    const menuSlot = document.getElementById('studioPopupMenuSlot');
    if (menuSlot) menuSlot.innerHTML = '';
    const logo = m.querySelector('.studio-popup-logo');
    const logoBox = m.querySelector('.studio-popup-logo-box');
    const parentBox = m.querySelector('.studio-popup-parent-box');
    const parentLogo = document.getElementById('studioPopupParentLogo');
    const logoLink = document.getElementById('studioPopupLogoLink');
    const parentLink = document.getElementById('studioPopupParentLink');
    if (logoBox) logoBox.classList.remove('is-empty');
    if (parentBox) parentBox.classList.remove('is-empty');
    if (logoLink) {
      logoLink.removeAttribute('href');
      logoLink.classList.remove('is-clickable');
    }
    if (parentLink) {
      parentLink.removeAttribute('href');
      parentLink.classList.remove('is-clickable');
    }
    logo.style.display = '';
    logo.src = '';
    if (parentLogo) {
      parentLogo.style.display = '';
      parentLogo.src = '';
    }
    // Wipe the linked-logo row and the cell's fanart background so
    // they don't bleed through from the previous studio while the
    // new one is loading. Both get repainted below once the row
    // payload arrives.
    const linkedHost = document.getElementById('studioPopupLinkedLogos');
    if (linkedHost) {
      linkedHost.innerHTML = '';
      linkedHost.hidden = true;
    }
    const infoCell = m.querySelector('.studio-popup-info-cell');
    if (infoCell) infoCell.style.removeProperty('--ts-cell-fanart');

    if (!_activeRowId) {
      // External-studio mode: when the caller passes a source-DB id
      // (e.g. /library Search tab → studio not yet in library), hit
      // /api/studios/external-panel for logo + description + recent
      // scenes from the source DB. Falls through to the prior empty
      // state when neither id nor source is available.
      const extSource = String(opts.source || '').trim();
      const extId     = String(opts.stashId || opts.tpdbId || '').trim();
      const extName   = String(opts.name || '').trim();
      if (extSource && (extId || extName)) {
        const startedName = _activeName;
        const params = new URLSearchParams();
        params.set('source', extSource);
        if (extId)   params.set('id', extId);
        if (extName) params.set('name', extName);
        try {
          const r = await fetch('/api/studios/external-panel?' + params.toString(), {
            credentials: 'same-origin',
          });
          if (_activeName !== startedName && _activeRowId == null) return;
          const d = await r.json().catch(() => ({}));
          if (!r.ok) {
            document.getElementById('studioPopupDesc').innerHTML =
              `<div class="studio-popup-empty" style="color:var(--red)">${ESC(d.error || ('HTTP ' + r.status))}</div>`;
            renderProwlarrPanel(_activeName || extName || '');
            return;
          }
          renderRow(d);
          // Description fetched inline as part of the external-panel
          // payload (row.description) — paint it directly so we skip
          // the second /studio-detail call (which is row_id-bound).
          const desc = (d && d.row && d.row.description) || '';
          const descEl = document.getElementById('studioPopupDesc');
          if (descEl) {
            if (desc) {
              descEl.innerHTML = `<div class="studio-popup-desc-body">${ESC(desc)}</div>` +
                `<div class="studio-popup-desc-source">via ${ESC(extSource)}</div>`;
            } else {
              descEl.innerHTML = '<div class="studio-popup-empty">No studio description available.</div>';
            }
          }
          renderParentLogoFromApi((d && d.row && d.row.external_parent) || null);
          // External URLs returned by the source DB — first http link
          // becomes the studio logo's site link, the rest go in the
          // chip strip (mirrors the in-library path's behaviour).
          const allLinks = ((d && d.row && d.row.external_urls) || [])
            .filter((x) => x && x.url)
            .map((x) => ({
              url:  x.url,
              site: (x.site && x.site.name) || x.site || '',
            }));
          const siteUrl = allLinks[0]?.url || '';
          applyStudioLogoLink(siteUrl);
          renderLinksRow(allLinks.slice(1));
          renderProwlarrPanel(_activeName || extName || '');
          return;
        } catch (e) {
          document.getElementById('studioPopupDesc').innerHTML =
            `<div class="studio-popup-empty" style="color:var(--red)">${ESC(e.message || 'Failed to load')}</div>`;
          renderProwlarrPanel(_activeName || extName || '');
          return;
        }
      }
      document.getElementById('studioPopupDesc').innerHTML =
        '<div class="studio-popup-empty">Studio is not in your library.</div>';
      logo.style.display = 'none';
      if (logoBox) logoBox.classList.add('is-empty');
      renderProwlarrPanel(_activeName || '');
      return;
    }

    try {
      // Two-stage load: entity-panel?skip_scenes=1 returns the row
      // (logo, name, pills, video count) immediately from local DB;
      // entity-scenes hits TPDB/StashDB/FansDB in parallel for the
      // recent-scenes grid. The grid pre-renders NO SIGNAL placeholders
      // up-front so the studio reads "structured, scenes loading"
      // rather than holding the whole popup hostage on the slow fetch.
      const startedRow = _activeRowId;
      const r = await fetch('/api/favourites/entity-panel?skip_scenes=1&row_id=' + encodeURIComponent(_activeRowId), {
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
      // Paint the row immediately with empty scenes — renderScenesGrid
      // emits NO SIGNAL placeholder tiles so the grid has structure
      // while the slow fetch runs.
      renderRow(Object.assign({}, d, { tpdb_scenes: [], scenes_source: null, _scenes_loading: true }));
      // Fire the slow scenes fetch in parallel with studio-detail
      // (description / parent / links). They independently swap their
      // own sections in when ready.
      fetch('/api/favourites/entity-scenes?row_id=' + encodeURIComponent(_activeRowId), {
        credentials: 'same-origin',
      })
        .then((rs) => rs.json().catch(() => ({})))
        .then((ds) => {
          if (_activeRowId !== startedRow) return;
          const scenes = (ds && ds.tpdb_scenes) || [];
          // Repaint just the scenes-dependent regions, not the whole row.
          renderLatestScene(scenes[0], _activeName);
          renderScenesGrid(scenes);
        })
        .catch(() => {
          if (_activeRowId !== startedRow) return;
          renderLatestScene(null, _activeName);
          renderScenesGrid([]);
        });
      // Description is a separate endpoint (TPDB / StashDB scrape).
      // Fire it after the panel paints so the recent scenes don't wait.
      // The same payload now also carries `parent` (network from the
      // upstream DB), which replaces the disk-path heuristic for the
      // Parent box logo.
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
          } else {
            descEl.innerHTML = '<div class="studio-popup-empty">No studio description available.</div>';
          }
          renderParentLogoFromApi(dd && dd.parent);
          // The first http(s) link the upstream DB returned is the
          // studio's own website. Make the studio logo itself the
          // clickable surface for it and strip it out of the chip
          // strip so we don't render the same link twice.
          const allLinks = (dd && dd.links) || [];
          const siteUrl = (allLinks.length && allLinks[0].url) || '';
          applyStudioLogoLink(siteUrl);
          renderLinksRow(allLinks.slice(1));
        })
        .catch(() => {
          const descEl = document.getElementById('studioPopupDesc');
          if (descEl) descEl.innerHTML = '<div class="studio-popup-empty">No studio description available.</div>';
          renderParentLogoFromApi(null);
          applyStudioLogoLink('');
          renderLinksRow([]);
        });
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
    // Video-count LED badge — only shown for library-tracked studios
    // (external-mode renderRow callers pass row.id === 0 with no
    // video_count). Padded to 4 leading zeros so the pill width stays
    // constant ("0007" vs "0234").
    const vcEl = document.getElementById('studioPopupVideoCount');
    const vcValEl = document.getElementById('studioPopupVideoCountVal');
    if (vcEl) {
      const hasCount = typeof row.video_count === 'number' && row.id;
      if (hasCount) {
        const vc = Number(row.video_count || 0);
        if (vcValEl) vcValEl.textContent = String(vc).padStart(4, '0');
        const tip = `${vc} video${vc === 1 ? '' : 's'} in this folder`;
        vcEl.setAttribute('title', tip);
        vcEl.setAttribute('aria-label', tip);
        vcEl.hidden = false;
      } else {
        vcEl.hidden = true;
        if (vcValEl) vcValEl.textContent = '';
      }
    }
    // Header pill row mirrors the performer popup: one chip per DB
    // (TPDB / StashDB / FansDB) — `is-linked` when we've stored an ID
    // for that DB on the row, `is-missing` otherwise. Links jump to the
    // upstream studio profile.
    renderHeaderPills(row);
    _activeRow = row;
    syncLockButton(row);
    syncFavButton(row);
    // Hamburger menu (Remove / Delete) mounts in the same slot the
    // performer popup uses. Refreshing the popup post-action isn't
    // useful for studios since both available actions destroy the row,
    // so just close on completion.
    mountStudioMenu(row);
    if (row.image_url) {
      logo.src = row.image_url;
      logo.style.display = '';
    } else {
      logo.style.display = 'none';
      m.querySelector('.studio-popup-logo-box')?.classList.add('is-empty');
    }
    // Logo box → image picker. The anchor inside the box (studio
    // website link) wins when present; otherwise the box itself opens
    // the picker. The dedicated pencil overlay below is a clearer
    // affordance for users that already have a logo + a website link.
    wireLogoPicker(m);
    applyCellFanart(m.querySelector('.studio-popup-info-cell'), row && row.id);
    renderParentLogoFromApi(null);  // placeholder until /studio-detail returns
    renderLinkedLogos(row);
    // While entity-scenes is still in flight the caller flags
    // `_scenes_loading` so the latest-scene + scenes-grid sections
    // render placeholder slots instead of "no scenes found".
    const scenesLoading = !!(d && d._scenes_loading);
    renderLatestScene(scenes[0], name, { loading: scenesLoading });
    renderScenesGrid(scenes, { loading: scenesLoading });
    renderProwlarrPanel(name);
  }

  /** Paint the small linked-studio logos that sit to the right of the
   * main logo. Each chip is one sibling studio whose ID is stored in
   * `row.group_ids[source]` (rich `{id, name, image}` entries — written
   * by the "Link other studios" search picker). Clicking a chip opens
   * the source DB profile in a new tab; the X button removes the link.
   * Hidden entirely when the row has no linked studios. */
  function renderLinkedLogos(row) {
    const host = document.getElementById('studioPopupLinkedLogos');
    if (!host) return;
    const gids = (row && row.group_ids) || {};
    const items = [];
    ['tpdb', 'stashdb', 'fansdb', 'javstash'].forEach((src) => {
      (gids[src] || []).forEach((entry) => {
        const isObj = entry && typeof entry === 'object';
        const id    = isObj ? String(entry.id || '')   : String(entry || '');
        const nm    = isObj ? String(entry.name || '') : '';
        const img   = isObj ? String(entry.image || ''): '';
        if (!id) return;
        items.push({ source: src, id, name: nm, image: img });
      });
    });
    if (!items.length) {
      host.hidden = true;
      host.innerHTML = '';
      return;
    }
    host.hidden = false;
    host.innerHTML = items.map((it) => {
      const url = studioProfileUrl(it.source, it.id);
      // Fall back to /api/studio-logo?name= when the source DB didn't
      // return an image URL on pick. The same endpoint backs the parent
      // logo box, so any local logo PNG/WebP already cached on disk
      // resolves without an extra round-trip.
      const imgSrc = it.image
        ? it.image
        : (it.name ? `/api/studio-logo?name=${encodeURIComponent(it.name)}` : '');
      const initial = (it.name || '').trim().charAt(0).toUpperCase() || '?';
      const tile = `<div class="studio-popup-linked-tile" data-source="${ATTR(it.source)}" data-id="${ATTR(it.id)}" title="${ATTR((it.name || it.id) + ' · ' + it.source.toUpperCase())}">
        ${imgSrc
          ? `<img class="studio-popup-linked-img" src="${ATTR(imgSrc)}" alt="${ATTR(it.name || it.id)}" loading="lazy" referrerpolicy="no-referrer" onerror="this.replaceWith(Object.assign(document.createElement('span'),{className:'studio-popup-linked-initial',textContent:'${ESC(initial)}'}))">`
          : `<span class="studio-popup-linked-initial">${ESC(initial)}</span>`}
        <button type="button" class="studio-popup-linked-remove" data-source="${ATTR(it.source)}" data-id="${ATTR(it.id)}" title="Unlink ${ATTR(it.name || it.id)}" aria-label="Unlink"><i class="fa-solid fa-xmark"></i></button>
      </div>`;
      return url
        ? `<a class="studio-popup-linked-link" href="${ATTR(url)}" target="_blank" rel="noopener noreferrer">${tile}</a>`
        : tile;
    }).join('');
    // Click the X to unlink without navigating the parent <a>.
    host.querySelectorAll('.studio-popup-linked-remove').forEach((btn) => {
      btn.addEventListener('click', async (e) => {
        e.preventDefault();
        e.stopPropagation();
        const src = btn.dataset.source || '';
        const id  = btn.dataset.id || '';
        if (!src || !id || !_activeRowId) return;
        try {
          const r = await fetch('/api/favourites/group-remove-link', {
            method: 'POST',
            credentials: 'same-origin',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ row_id: _activeRowId, source: src, ext_id: id }),
          });
          if (!r.ok) {
            const d = await r.json().catch(() => ({}));
            toast(d.error || 'Unlink failed');
            return;
          }
          // Re-fetch the popup so group_ids paints fresh.
          if (window._studioPopupActiveId === _activeRowId) window.refreshStudioPopup();
        } catch (err) {
          toast(err.message || 'Unlink failed');
        }
      });
    });
  }

  function studioProfileUrl(siteKey, id) {
    if (!id) return '';
    if (siteKey === 'tpdb') return 'https://theporndb.net/sites/' + encodeURIComponent(id);
    if (siteKey === 'stashdb') return 'https://stashdb.org/studios/' + encodeURIComponent(id);
    if (siteKey === 'fansdb') return 'https://fansdb.cc/studios/' + encodeURIComponent(id);
    if (siteKey === 'javstash') return 'https://javstash.org/studios/' + encodeURIComponent(id);
    return '';
  }

  function srcLogoHtml(key, label) {
    // Reuse the performer-popup pill logo class so the studio header
    // pills are the same compact size (16px tall) as on the performer
    // popup. Files in /static/logos/ ship as .webp — the old .png
    // suffix here resolved to a 404 and the pills fell back to text.
    const file = key + '.webp';
    return `<img class="pp-src-logo" src="/static/logos/${ESC(file)}" alt="${ESC(label)}" title="${ESC(label)}" onerror="this.replaceWith(document.createTextNode('${ESC(label)}'))">`;
  }

  // Pill labels mirror the performer popup so the user can hit the
  // identical search modal; backend's `/api/favourites/match` accepts
  // the uppercase source keys we emit on the buttons.
  const STUDIO_PILL_SITES = {
    tpdb:     'TPDB',
    stashdb:  'StashDB',
    fansdb:   'FansDB',
    javstash: 'JAVStash',
  };

  function renderHeaderPills(row) {
    const el = document.getElementById('studioPopupPills');
    if (!el) return;
    const rowId = Number(row && row.id || 0);
    const studioName = row.display_name || row.folder_name || _activeName || '';
    const pills = [
      { key: 'tpdb',     label: 'ThePornDB', id: row.match_tpdb_id    },
      { key: 'stashdb',  label: 'StashDB',   id: row.match_stashdb_id },
      { key: 'fansdb',   label: 'FansDB',    id: row.match_fansdb_id  },
      { key: 'javstash', label: 'JAVStash',  id: row.match_javstash_id },
    ];
    // Markup mirrors performer-popup.js's `buildProfilePillsHtml` so
    // both popups share CSS, hover-reveal behaviour, and (after this
    // popup wires its own click handlers below) keystroke handling.
    // Same data-pp-* attribute prefix so a future shared binder can
    // operate on either popup without per-popup branching.
    el.innerHTML = pills.map((p) => {
      const url = studioProfileUrl(p.key, p.id);
      const logo = srcLogoHtml(p.key, p.label);
      const site = STUDIO_PILL_SITES[p.key];
      if (url) {
        const editAction = rowId
          ? `data-pp-edit-action="db" data-pp-site="${ESC(site)}" data-pp-row="${ESC(rowId)}" data-pp-name="${ESC(studioName)}"`
          : '';
        const removeAction = rowId
          ? `data-pp-remove="1" data-pp-site="${ESC(site)}" data-pp-row="${ESC(rowId)}"`
          : '';
        return `<a class="pp-profile-pill is-linked" href="${ESC(url)}" target="_blank" rel="noopener noreferrer" title="${ESC(p.label)} — click to open">
          <i class="fa-solid fa-check pp-profile-check"></i>
          ${logo}
          ${(editAction || removeAction) ? `<span class="pp-profile-actions">
            ${editAction ? `<button type="button" class="pp-profile-action-btn" ${editAction} title="Change link"><i class="fa-solid fa-pen"></i></button>` : ''}
            ${removeAction ? `<button type="button" class="pp-profile-action-btn is-remove" ${removeAction} title="Remove link"><i class="fa-solid fa-xmark"></i></button>` : ''}
          </span>` : ''}
        </a>`;
      }
      const linkAction = rowId
        ? `data-pp-edit-action="db" data-pp-site="${ESC(site)}" data-pp-row="${ESC(rowId)}" data-pp-name="${ESC(studioName)}"`
        : '';
      return `<button type="button" class="pp-profile-pill is-missing" ${linkAction} title="${ESC(p.label)} — click to search and link">
        ${logo}
        <i class="fa-solid fa-magnifying-glass" style="font-size:9px;opacity:0.7"></i>
      </button>`;
    }).join('');
    wirePillHandlers(el);
  }

  function wirePillHandlers(scope) {
    scope.querySelectorAll('.pp-profile-action-btn').forEach((btn) => {
      btn.addEventListener('click', (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        const rowId = parseInt(btn.dataset.ppRow, 10);
        const site  = btn.dataset.ppSite;
        if (btn.dataset.ppRemove) removeMatchLink(rowId, site);
        else openLinkSearch(btn);
      });
    });
    scope.querySelectorAll('.pp-profile-pill.is-missing[data-pp-edit-action]').forEach((btn) => {
      btn.addEventListener('click', (ev) => {
        ev.preventDefault();
        openLinkSearch(btn);
      });
    });
  }

  function openLinkSearch(btn) {
    const rowId = parseInt(btn.dataset.ppRow, 10);
    const site  = btn.dataset.ppSite;
    const name  = btn.dataset.ppName || '';
    if (!rowId || !site) return;
    // Mirror the performer popup: prefer /library's richer search modal
    // when it's loaded (it scopes results to the row's kind already),
    // otherwise fall back to the universal link modal which we just
    // taught to search studios via openStudioLinkSearch.
    if (typeof window.openSearchForRow === 'function') {
      window.openSearchForRow(rowId, site);
    } else if (typeof window.openStudioLinkSearch === 'function') {
      window.openStudioLinkSearch({ rowId, site, name });
    } else {
      toast('Studio link search not loaded');
    }
  }

  async function removeMatchLink(rowId, site) {
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
      if (window._studioPopupActiveId === rowId) window.refreshStudioPopup();
    } catch (e) { toast(e.message || 'Failed'); }
  }

  function syncLockButton(row) {
    const btn = document.getElementById('studioPopupLockBtn');
    if (!btn) return;
    const locked = !!(row && row.matches_locked);
    btn.classList.toggle('lock-on', locked);
    btn.setAttribute('aria-pressed', locked ? 'true' : 'false');
    btn.title = locked
      ? 'Matches locked — scans and refresh will not change links (click to unlock)'
      : 'Lock matches — scans and refresh will not change links';
    btn.setAttribute('aria-label', btn.title);
    const ico = btn.querySelector('i');
    if (ico) ico.className = 'fa-solid ' + (locked ? 'fa-lock' : 'fa-lock-open');
    // Inline styles for the active state since .performer-popup-tool has
    // no built-in .lock-on variant in app-shell.css.
    if (locked) {
      btn.style.color = '#fbbf24';
      btn.style.borderColor = 'rgba(251,191,36,0.55)';
      btn.style.background = 'rgba(251,191,36,0.14)';
    } else {
      btn.style.color = '';
      btn.style.borderColor = '';
      btn.style.background = '';
    }
  }

  async function toggleMatchesLock() {
    if (!_activeRowId) return;
    const next = !(_activeRow && _activeRow.matches_locked);
    const btn = document.getElementById('studioPopupLockBtn');
    if (btn) btn.disabled = true;
    try {
      const r = await fetch('/api/favourites/lock', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id: _activeRowId, matches_locked: next }),
      });
      if (!r.ok) { toast('Lock toggle failed'); return; }
      if (_activeRow) _activeRow.matches_locked = next ? 1 : 0;
      syncLockButton(_activeRow);
    } catch (e) {
      toast(e.message || 'Lock toggle failed');
    } finally {
      if (btn) btn.disabled = false;
    }
  }

  function syncFavButton(row) {
    const btn = document.getElementById('studioPopupFavBtn');
    if (!btn) return;
    const on = !!(row && row.is_favourite);
    btn.classList.toggle('is-on', on);
    btn.setAttribute('aria-pressed', on ? 'true' : 'false');
    btn.title = on ? 'Unfavourite' : 'Favourite';
    btn.setAttribute('aria-label', btn.title);
    // Inline accent state — the performer popup has its own
    // .pp-name-action CSS, but this toolbar button doesn't have an
    // equivalent built-in 'is-on' rule. Use the brand-accent fill so
    // the heart reads as the same "favourited" affordance.
    if (on) {
      btn.style.color = '#f472b6';
      btn.style.borderColor = 'rgba(244,114,182,0.55)';
      btn.style.background = 'rgba(244,114,182,0.14)';
    } else {
      btn.style.color = '';
      btn.style.borderColor = '';
      btn.style.background = '';
    }
  }

  async function toggleFavourite() {
    if (!_activeRowId) return;
    const next = !(_activeRow && _activeRow.is_favourite);
    const btn = document.getElementById('studioPopupFavBtn');
    if (btn) btn.disabled = true;
    try {
      const r = await fetch('/api/favourites/star', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id: _activeRowId, is_favourite: next }),
      });
      if (!r.ok) { toast('Favourite toggle failed'); return; }
      if (_activeRow) _activeRow.is_favourite = next ? 1 : 0;
      syncFavButton(_activeRow);
    } catch (e) {
      toast(e.message || 'Favourite toggle failed');
    } finally {
      if (btn) btn.disabled = false;
    }
  }

  async function refreshFromLinkedDbs() {
    if (!_activeRowId) return;
    const btn = document.getElementById('studioPopupRefreshBtn');
    const ico = btn ? btn.querySelector('i') : null;
    if (btn) btn.disabled = true;
    if (ico) ico.classList.add('fa-spin');
    try {
      const r = await fetch('/api/favourites/refresh', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id: _activeRowId, scrape_aliases: true }),
      });
      if (!r.ok) {
        const d = await r.json().catch(() => ({}));
        toast(d.error || 'Refresh failed');
        return;
      }
      if (window._studioPopupActiveId === _activeRowId) window.refreshStudioPopup();
    } catch (e) {
      toast(e.message || 'Refresh failed');
    } finally {
      if (btn) btn.disabled = false;
      if (ico) ico.classList.remove('fa-spin');
    }
  }

  function mountStudioMenu(row) {
    const slot = document.getElementById('studioPopupMenuSlot');
    if (!slot || !row || !row.id) return;
    if (!window.LibEntityActions || typeof window.LibEntityActions.mountMenuButton !== 'function') return;
    slot.innerHTML = '';
    window.LibEntityActions.mountMenuButton(slot, {
      id: Number(row.id),
      kind: 'studio',
      name: row.display_name || row.folder_name || _activeName || 'this studio',
      viceId: '',
      canChangeDirectory: false,
    }, () => closeStudioPopup());
  }

  function renderLinksRow(links) {
    const el = document.getElementById('studioPopupLinks');
    if (!el) return;
    const arr = Array.isArray(links) ? links : [];
    if (!arr.length) {
      el.innerHTML = '';
      el.hidden = true;
      return;
    }
    el.innerHTML = arr.map((l) => {
      const u = (l && l.url) || '';
      if (!u) return '';
      let host = '';
      try { host = new URL(u).hostname.replace(/^www\./, ''); } catch (_) { host = ''; }
      const lbl = (l && l.label) || host || 'link';
      const favicon = host
        ? `<img src="https://www.google.com/s2/favicons?domain=${ESC(host)}&sz=32" alt="" onerror="this.replaceWith(Object.assign(document.createElement('i'),{className:'fa-solid fa-globe'}))">`
        : `<i class="fa-solid fa-globe"></i>`;
      return `<a class="studio-popup-link" href="${ESC(u)}" target="_blank" rel="noopener noreferrer" title="${ESC(u)}">
        ${favicon}<span>${ESC(lbl)}</span>
      </a>`;
    }).join('');
    el.hidden = false;
  }

  // The Parent box now mirrors the upstream-DB parent network rather
  // than guessing from the on-disk folder path. ``parent`` is whatever
  // `/api/favourites/studio-detail` returned for this row (TPDB site's
  // `parent`/`network`, or StashDB studio's `parent`) — null when the
  // upstream DB doesn't know one.
  function applyStudioLogoLink(url) {
    const link = document.getElementById('studioPopupLogoLink');
    if (!link) return;
    const u = (url || '').trim();
    if (!u || !/^https?:\/\//i.test(u)) {
      link.removeAttribute('href');
      link.classList.remove('is-clickable');
      link.removeAttribute('title');
      return;
    }
    link.href = u;
    link.classList.add('is-clickable');
    link.title = 'Open studio website';
  }

  function renderParentLogoFromApi(parent) {
    const parentBox = document.querySelector('#studioPopupModal .studio-popup-parent-box');
    const parentLogo = document.getElementById('studioPopupParentLogo');
    const parentLink = document.getElementById('studioPopupParentLink');
    if (!parentBox || !parentLogo) return;
    const name = parent && (parent.name || '').trim() ? parent.name.trim() : '';
    if (!name) {
      parentBox.classList.add('is-empty');
      parentLogo.style.display = 'none';
      parentLogo.removeAttribute('alt');
      if (parentLink) {
        parentLink.removeAttribute('href');
        parentLink.classList.remove('is-clickable');
      }
      return;
    }
    // Logo only — name text used to render next to it but read as a
    // duplicate caption. Keep the name in `alt` so screen readers
    // still announce it.
    parentBox.classList.remove('is-empty');
    parentLogo.style.display = '';
    parentLogo.alt = name;
    // Prefer the direct URL the upstream DB handed us (e.g. TPDB's
    // `parent.logo`). Only fall back to the in-app `/api/studio-logo`
    // name lookup when no URL was given — that endpoint requires a
    // matching slug in our local logo cache and silently 404s for
    // network names we don't have on disk (which is why "BANG!" was
    // showing as text earlier).
    const directLogo = (parent && parent.logo_url || '').trim();
    parentLogo.src = directLogo
      || ('/api/studio-logo?name=' + encodeURIComponent(name) + '&q=' + encodeURIComponent(name));
    // Click-through to the parent network's profile on whichever DB
    // answered the studio-detail lookup (TPDB / StashDB / JAVStash).
    if (parentLink) {
      const srcKey = String((parent && parent.source) || '').toLowerCase();
      const url = studioProfileUrl(srcKey, parent && parent.id);
      if (url) {
        parentLink.href = url;
        parentLink.classList.add('is-clickable');
        parentLink.title = `Open ${name} on ${parent.source || ''}`.trim();
      } else {
        parentLink.removeAttribute('href');
        parentLink.classList.remove('is-clickable');
      }
    }
  }

  function sourceKey(s) {
    return String((s && (s.scene_source || s.source)) || '').toLowerCase() || 'tpdb';
  }

  function sceneUrl(s) {
    const id = String((s && (s.id || s.scene_id || s.tpdb_id)) || '').trim();
    if (!id) return '';
    const src = sourceKey(s);
    if (src === 'stashdb') return 'https://stashdb.org/scenes/' + encodeURIComponent(id);
    if (src === 'fansdb') return 'https://fansdb.cc/scenes/' + encodeURIComponent(id);
    if (src === 'javstash') return 'https://javstash.org/scenes/' + encodeURIComponent(id);
    return 'https://theporndb.net/scenes/' + encodeURIComponent(id);
  }

  function sceneSourceLabel(s) {
    const src = sourceKey(s);
    if (src === 'stashdb') return 'StashDB';
    if (src === 'fansdb') return 'FansDB';
    if (src === 'javstash') return 'JAVStash';
    return 'TPDB';
  }

  function sceneImage(s) {
    return (s && (s.image || s.poster || s.thumb || s.background || s.screenshot)) || '';
  }

  function renderLatestScene(scene, _studioName, opts) {
    const el = document.getElementById('studioPopupLatest');
    if (!el) return;
    const loading = !!(opts && opts.loading);
    if (!scene) {
      if (loading) {
        // Mirror the latest-scene layout so the panel doesn't reflow
        // when real data arrives — title row + spinner-only art slot.
        el.innerHTML = `
          <div class="studio-popup-latest-info">
            <div class="studio-popup-latest-kicker">Latest scene</div>
            <div class="studio-popup-latest-title" style="opacity:0.45">Loading…</div>
            <div class="studio-popup-latest-meta" style="opacity:0.35">&nbsp;</div>
          </div>
          <div class="studio-popup-latest-art is-empty studio-popup-latest-art--loading">
            <span class="loader" role="status" aria-label="Loading scene"></span>
            <div class="studio-popup-latest-glow" aria-hidden="true"></div>
            <div class="studio-popup-latest-vignette" aria-hidden="true"></div>
          </div>`;
        return;
      }
      el.innerHTML = '<div class="studio-popup-empty">No recent scene art found.</div>';
      return;
    }
    const img = sceneImage(scene);
    const title = scene.title || '(untitled)';
    const date = scene.release_date || scene.date || '';
    const srcLabel = sceneSourceLabel(scene);
    const performers = Array.isArray(scene.performers) ? scene.performers : [];
    const castHtml = performers.length
      ? `<div class="studio-popup-latest-cast">${performers.map((p) => {
          const pName = (p && p.name) || '';
          const pImg = (p && p.image) || '';
          const initial = pName ? pName.trim().charAt(0).toUpperCase() : '?';
          const avatar = pImg
            ? `<img src="${ATTR(pImg)}" alt="${ATTR(pName)}" loading="lazy" decoding="async" referrerpolicy="no-referrer" onerror="this.replaceWith(Object.assign(document.createElement('div'),{className:'studio-popup-cast-initial',textContent:'${ESC(initial)}'}))">`
            : `<div class="studio-popup-cast-initial">${ESC(initial)}</div>`;
          return `<div class="studio-popup-cast-tile" title="${ATTR(pName)}">
            <div class="studio-popup-cast-avatar">${avatar}</div>
            <span class="studio-popup-cast-name">${ESC(pName)}</span>
          </div>`;
        }).join('')}</div>`
      : '';
    // Two-column layout: scene info pinned to the left, the 16:9 image
    // floats at the right. The previous absolute-positioned copy block
    // that overlay the image is gone — title / cast read against the
    // panel background instead of fighting an image vignette.
    el.innerHTML = `
      <div class="studio-popup-latest-info">
        <div class="studio-popup-latest-kicker">Latest scene</div>
        <div class="studio-popup-latest-title" title="${ATTR(title)}">${ESC(title)}</div>
        <div class="studio-popup-latest-meta">${ESC(date)}${date ? ' · ' : ''}${ESC(srcLabel)}</div>
        ${castHtml}
      </div>
      <div class="studio-popup-latest-art${img ? '' : ' is-empty'}">
        ${img ? `<img src="${ATTR(img)}" alt="" loading="eager" decoding="async" referrerpolicy="no-referrer" onload="this.closest('.studio-popup-latest-art')?.classList.add('ready')" onerror="this.closest('.studio-popup-latest-art')?.classList.add('is-empty');this.remove()">` : ''}
        <div class="studio-popup-latest-glow" aria-hidden="true"></div>
        <div class="studio-popup-latest-vignette" aria-hidden="true"></div>
      </div>`;
  }

  function renderScenesGrid(scenes, opts) {
    const label = document.getElementById('studioPopupScenesLabel');
    const el = document.getElementById('studioPopupScenes');
    if (!el || !label) return;
    // Two empty-state modes:
    //   loading=true  → paint 9 NO SIGNAL placeholder tiles so the
    //                   section has structure while entity-scenes
    //                   resolves. Label stays visible to anchor the
    //                   grid in place.
    //   loading=false → real "no results" message (entity-scenes
    //                   returned an empty array).
    const loading = !!(opts && opts.loading);
    if (!scenes.length && !loading) {
      label.style.display = 'none';
      el.innerHTML = '<div class="studio-popup-empty">No recent scenes found.</div>';
      return;
    }
    if (!scenes.length && loading) {
      label.style.display = '';
      const slots = Array.from({ length: 9 }, () => `
        <div class="scene-card scene-card--loading" aria-hidden="true">
          <div class="img-load">
            <span class="loader" role="status" aria-label="Loading scene"></span>
          </div>
          <div class="scene-meta" style="padding:6px 4px">
            <div class="scene-title" style="font-size:11px;color:rgba(255,255,255,0.35);line-height:1.3;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">Loading…</div>
            <div style="font-size:10px;color:rgba(255,255,255,0.25)">&nbsp;</div>
          </div>
        </div>`).join('');
      el.innerHTML = `<div class="pp-scenes-grid studio-popup-pp-scenes">${slots}</div>`;
      return;
    }
    label.style.display = '';
    // Match the performer popup's scene tiles: `.pp-scenes-grid` host,
    // `.scene-card` with `.img-load` (16:9-locked wrapper) + `.scene-thumb`
    // + per-tile hover actions (source DB link top-right, Prowlarr
    // center). Same CSS rules in app-shell.css now apply to both
    // surfaces — DRY-er and visually consistent.
    el.innerHTML = '<div class="pp-scenes-grid studio-popup-pp-scenes">'
      + scenes.slice(0, 12).map((s) => {
        const img = sceneImage(s);
        const title = s.title || '(untitled)';
        const date = s.release_date || s.date || '';
        const url = sceneUrl(s);
        const srcKey = sourceKey(s);
        const srcLabel = sceneSourceLabel(s);
        const srcLogo = srcKey + '.png';
        const titleJson = ESC(JSON.stringify(title));
        const studioJson = ESC(JSON.stringify(_activeName || ''));
        const sourceLink = url
          ? `<a class="pp-scene-action pp-scene-action--source pp-scene-action--${srcKey}" href="${ATTR(url)}" target="_blank" rel="noopener noreferrer" title="Open on ${ESC(srcLabel)}" aria-label="Open on ${ESC(srcLabel)}" onclick="event.stopPropagation()">
              <img src="/static/logos/${ESC(srcLogo)}" alt="" onerror="this.replaceWith(Object.assign(document.createElement('i'),{className:'fa-solid fa-arrow-up-right-from-square'}))">
            </a>`
          : '';
        const prowlarrBtn = `<button type="button" class="pp-scene-prowlarr-center" onclick="event.stopPropagation(); window.openProwlarrSearchPopup&&window.openProwlarrSearchPopup({title:${titleJson},studio:${studioJson},kind:'scene'})" title="Search Prowlarr" aria-label="Search Prowlarr">
            <i class="fa-solid fa-download" aria-hidden="true"></i>
          </button>`;
        const blacklistBtn = `<button type="button" class="scene-card-blacklist-btn" data-title="${ATTR(title || '')}" onclick="event.stopPropagation();sceneCardBlacklist(this)" title="Blacklist this title" aria-label="Blacklist title"><i class="fa-solid fa-ban"></i></button>`;
        const wantedBtn = (typeof window.tsBuildWantedBtnHtml === 'function')
          ? window.tsBuildWantedBtnHtml(s, 'scene')
          : '';
        return `<div class="scene-card" tabindex="0" title="${ATTR(title)}">
          <div class="img-load">
            ${img
              ? `<img class="scene-thumb" src="${ATTR(img)}" loading="lazy" referrerpolicy="no-referrer" onload="this.closest('.img-load')?.classList.add('ready')" onerror="this.onerror=null;this.src='/static/img/missing.webp';this.closest('.img-load')?.classList.add('ready');">`
              : `<div class="scene-static-noise" aria-hidden="true"></div><div class="scene-static-label">NO SIGNAL</div>`}
            ${sourceLink}
            ${prowlarrBtn}
            ${wantedBtn}
            ${blacklistBtn}
          </div>
          <div class="scene-meta" style="padding:6px 4px">
            <div class="scene-title" style="font-size:11px;color:var(--text);line-height:1.3;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${ESC(title)}</div>
            <div style="font-size:10px;color:var(--dim)">${ESC(date)}</div>
          </div>
        </div>`;
      }).join('')
      + '</div>';
  }

  // Lazy-mount the Prowlarr embed: defer the actual mount until the
  // cell first intersects the viewport. On desktop the cell is
  // visible immediately so IO fires synchronously after layout (no
  // perceptible difference). On mobile (stacked grid) the Prowlarr
  // cell sits below the fold and Prowlarr is only hit if the user
  // scrolls to it — removes one network round-trip from the typical
  // open-and-close popup interaction.
  let _prowlarrIO = null;
  let _prowlarrPendingName = null;
  function renderProwlarrPanel(name) {
    const embedHost = document.getElementById('studioPopupProwlarrEmbed');
    if (!embedHost) return;
    if (typeof window.unmountEmbeddedProwlarrSearch === 'function') {
      window.unmountEmbeddedProwlarrSearch(embedHost);
    }
    if (_prowlarrIO) {
      try { _prowlarrIO.disconnect(); } catch (e) { /* noop */ }
      _prowlarrIO = null;
    }
    const statusEl = embedHost.querySelector('.ts-prowlarr-embed-status');
    const resultsEl = embedHost.querySelector('.ts-prowlarr-embed-results');
    const filtersEl = embedHost.querySelector('.ts-prowlarr-embed-filters');
    if (filtersEl) {
      filtersEl.hidden = true;
      filtersEl.innerHTML = '';
    }
    if (!name || typeof window.mountEmbeddedProwlarrSearch !== 'function') {
      if (statusEl) statusEl.textContent = name ? 'Prowlarr search unavailable on this page' : 'No studio name to search';
      if (resultsEl) resultsEl.innerHTML = '';
      if (name && typeof window.mountEmbeddedProwlarrSearch !== 'function') toast('Prowlarr search unavailable on this page');
      return;
    }
    if (statusEl) statusEl.textContent = 'Prowlarr — scroll to load';
    _prowlarrPendingName = name;
    const fireMount = () => {
      if (!_prowlarrPendingName) return;
      const pending = _prowlarrPendingName;
      _prowlarrPendingName = null;
      void window.mountEmbeddedProwlarrSearch(embedHost, {
        title: pending,
        dotVariant: true,
        dedupe: true,
        kind: 'scene',
      });
    };
    if (typeof IntersectionObserver !== 'function') {
      fireMount();
      return;
    }
    _prowlarrIO = new IntersectionObserver((entries) => {
      for (const ent of entries) {
        if (ent.isIntersecting) {
          if (_prowlarrIO) {
            try { _prowlarrIO.disconnect(); } catch (e) { /* noop */ }
            _prowlarrIO = null;
          }
          fireMount();
          break;
        }
      }
    }, { rootMargin: '0px 0px 200px 0px' });
    _prowlarrIO.observe(embedHost);
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

  /* ── "Link other studios" search modal ───────────────────────────
   * Triggered from the popup hamburger. Hits /api/metadata/search
   * with type=studio (which already fans out across TPDB/StashDB/
   * FansDB/JAVStash). Each pick POSTs to /api/favourites/group-add-link
   * with the rich payload so the linked-logo header row picks up the
   * image without an extra fetch. */
  let _linkSearchToken = 0;
  let _linkSearchRowId = null;
  let _linkSearchName  = '';

  function ensureLinkSearchModal() {
    if (document.getElementById('studioLinkSearchModal')) return;
    const div = document.createElement('div');
    div.id = 'studioLinkSearchModal';
    div.className = 'modal-overlay';
    // Sit above the studio popup (z=1500) — same trick the logo picker
    // uses (importance flag wins against the !important rule baked into
    // the generic .modal-overlay style).
    div.style.setProperty('z-index', '1800', 'important');
    div.innerHTML = `
      <div class="modal-box ts-link-modal-box studio-link-search-box"
           style="max-width:760px;width:min(760px,calc(100vw - 60px));max-height:calc(100vh - 80px);display:flex;flex-direction:column">
        <h3 id="studioLinkSearchTitle" style="margin:0 0 6px 0;font-family:var(--font-display, var(--mono));font-size:18px">Link other studios</h3>
        <div id="studioLinkSearchSub" style="font-size:11px;color:var(--dim);margin-bottom:12px">
          Find sibling sites to file under this folder. Scenes whose studio matches any linked ID will auto-route here.
        </div>
        <input type="search" id="studioLinkSearchInput"
               placeholder="Search TPDB · StashDB · FansDB · JAVStash…"
               autocomplete="off"
               style="background:rgba(0,0,0,0.35);border:1px solid rgba(255,255,255,0.12);border-radius:6px;color:var(--text);padding:8px 12px;font-size:13px;font-family:var(--mono);outline:none;margin-bottom:10px">
        <div id="studioLinkSearchStatus" style="font-size:11px;color:var(--dim);margin-bottom:8px"></div>
        <div id="studioLinkSearchResults" style="flex:1 1 0;min-height:200px;overflow-y:auto;display:flex;flex-direction:column;gap:4px"></div>
        <div class="ts-link-modal-foot" style="display:flex;justify-content:flex-end;margin-top:12px">
          <button type="button" id="studioLinkSearchCloseBtn"
                  style="background:transparent;border:1px solid rgba(255,255,255,0.15);color:var(--dim);padding:8px 16px;border-radius:6px;font-size:11px;font-family:var(--mono);cursor:pointer;text-transform:uppercase;letter-spacing:0.04em">Close</button>
        </div>
      </div>`;
    document.body.appendChild(div);
    div.addEventListener('click', (e) => {
      if (e.target === div) closeLinkSearchModal();
    });
    div.querySelector('#studioLinkSearchCloseBtn').addEventListener('click', closeLinkSearchModal);
    const input = div.querySelector('#studioLinkSearchInput');
    let debTimer = null;
    input.addEventListener('input', () => {
      if (debTimer) clearTimeout(debTimer);
      debTimer = setTimeout(() => runLinkSearch(input.value), 250);
    });
    input.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') closeLinkSearchModal();
    });
  }

  function openLinkSearchModal(rowId, name) {
    if (!rowId) return;
    ensureLinkSearchModal();
    _linkSearchRowId = Number(rowId);
    _linkSearchName  = String(name || '');
    const div = document.getElementById('studioLinkSearchModal');
    const sub = document.getElementById('studioLinkSearchSub');
    if (sub && _linkSearchName) {
      sub.innerHTML = 'Find sibling sites to file under <strong>'
        + ESC(_linkSearchName) + '</strong>. Scenes whose studio matches any linked ID will auto-route here.';
    }
    const input = div.querySelector('#studioLinkSearchInput');
    if (input) {
      input.value = _linkSearchName || '';
      setTimeout(() => input.focus(), 0);
    }
    document.getElementById('studioLinkSearchResults').innerHTML = '';
    document.getElementById('studioLinkSearchStatus').textContent = '';
    div.style.display = 'flex';
    div.classList.add('open');
    if (_linkSearchName) runLinkSearch(_linkSearchName);
  }
  window.openStudioLinkSearchModal = openLinkSearchModal;

  function closeLinkSearchModal() {
    const div = document.getElementById('studioLinkSearchModal');
    if (!div) return;
    div.classList.remove('open');
    div.style.display = '';
    _linkSearchToken++;  // invalidate any in-flight fetches
  }

  async function runLinkSearch(q) {
    const query = String(q || '').trim();
    const status  = document.getElementById('studioLinkSearchStatus');
    const results = document.getElementById('studioLinkSearchResults');
    if (!status || !results) return;
    if (!query) {
      status.textContent = '';
      results.innerHTML = '';
      return;
    }
    status.textContent = 'Searching…';
    results.innerHTML = '';
    const token = ++_linkSearchToken;
    try {
      const r = await fetch('/api/metadata/search?type=studio&q=' + encodeURIComponent(query), {
        credentials: 'same-origin',
      });
      const d = await r.json().catch(() => ({}));
      if (token !== _linkSearchToken) return;
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
      results.innerHTML = items.map((it, i) => {
        const src   = (it.source || '').toUpperCase();
        const nm    = it.name || '';
        const id    = String(it.id || '');
        const img   = it.image || '';
        const initial = nm.trim().charAt(0).toUpperCase() || '?';
        return `<button type="button" class="studio-link-search-row"
                        data-i="${i}"
                        style="display:flex;align-items:center;gap:12px;padding:8px 10px;border-radius:6px;background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.06);cursor:pointer;text-align:left;color:var(--text);font-family:inherit"
                        onmouseover="this.style.background='rgba(255,255,255,0.06)'"
                        onmouseout="this.style.background='rgba(255,255,255,0.02)'">
          <div style="width:48px;height:48px;border-radius:6px;background:rgba(0,0,0,0.35);display:flex;align-items:center;justify-content:center;flex-shrink:0;overflow:hidden">
            ${img
              ? `<img src="${ATTR(img)}" alt="" referrerpolicy="no-referrer" style="max-width:100%;max-height:100%;object-fit:contain" onerror="this.replaceWith(Object.assign(document.createElement('span'),{textContent:'${ESC(initial)}',style:'font-family:var(--mono);font-size:18px;color:var(--dim)'}))">`
              : `<span style="font-family:var(--mono);font-size:18px;color:var(--dim)">${ESC(initial)}</span>`}
          </div>
          <div style="flex:1;min-width:0">
            <div style="font-size:13px;color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${ESC(nm)}</div>
            <div style="font-size:10px;color:var(--dim);margin-top:2px">${ESC(src)} · ${ESC(id)}</div>
          </div>
          <i class="fa-solid fa-link" style="color:var(--accent);font-size:13px"></i>
        </button>`;
      }).join('');
      window._studioLinkSearchResults = items;
      results.querySelectorAll('.studio-link-search-row').forEach((btn) => {
        btn.addEventListener('click', () => linkPickedStudio(parseInt(btn.dataset.i, 10)));
      });
    } catch (e) {
      if (token !== _linkSearchToken) return;
      status.textContent = e.message || 'Search failed';
    }
  }

  async function linkPickedStudio(idx) {
    const it = window._studioLinkSearchResults?.[idx];
    if (!it || !_linkSearchRowId) return;
    const source = (it.source || '').toLowerCase();
    const siteKey = source === 'theporndb' ? 'tpdb' : source;
    if (!['tpdb', 'stashdb', 'fansdb', 'javstash'].includes(siteKey)) {
      toast('Unknown source: ' + source);
      return;
    }
    try {
      const r = await fetch('/api/favourites/group-add-link', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          row_id: _linkSearchRowId,
          source: siteKey,
          ext_id: String(it.id || ''),
          name:   it.name || '',
          image:  it.image || '',
        }),
      });
      if (!r.ok) {
        const d = await r.json().catch(() => ({}));
        toast(d.error || 'Link failed');
        return;
      }
      toast(`Linked ${it.name || it.id} to ${_linkSearchName || 'this studio'}`);
      closeLinkSearchModal();
      if (window._studioPopupActiveId === _linkSearchRowId) window.refreshStudioPopup();
    } catch (e) {
      toast(e.message || 'Link failed');
    }
  }
})();
