/* Manage-posters + crop modals — shared across /library and performer popup. */
(function () {
  'use strict';
  if (window._posterRolePickerInited) return;
  window._posterRolePickerInited = true;

  const POSTER_FALLBACK = '/static/img/poster.webp';

  function esc(s) {
    return String(s ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }
  function escAttr(s) {
    return String(s || '').replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;');
  }
  function displayFolderName(name) {
    return String(name || '').trim();
  }
  function showMsg(t) {
    const el = document.getElementById('toolMsg');
    if (el) {
      el.textContent = t || '';
      if (t) setTimeout(() => { el.textContent = ''; }, 4000);
    } else if (t && typeof window.toast === 'function') {
      window.toast(t);
    }
  }

  let _posterRoleWired = false;
  let _cropWired = false;

  function ensurePosterModals() {
    if (document.getElementById('posterRoleModal')) {
      wirePosterRolePickerOnce();
      return;
    }
    const host = document.createElement('div');
    host.id = 'tsPosterRoleModalsHost';
    host.innerHTML = `<div class="modal-overlay" id="posterRoleModal">
  <div class="modal-box fav-roles-modal-box" style="position:relative">
    <button type="button" class="fav-roles-modal-close" onclick="closePosterRolePicker()" title="Close" aria-label="Close"><i class="fa-solid fa-xmark"></i></button>
    <div class="fav-roles-modal-header">
      <div class="fav-roles-header-row">
        <h3><span id="posterRoleTitle">Manage posters</span> <button type="button" class="fav-roles-hint-btn" title="Upload or paste a URL to add an image to the candidate grid, then apply to headshot (crop), primary, or secondary." aria-label="Help"><i class="fa-solid fa-circle-info"></i></button></h3>
        <div class="fav-roles-header-center">
          <button type="button" class="fav-icon-btn" onclick="posterRoleUploadImport()" title="Upload image"><i class="fa-solid fa-upload"></i></button>
          <button type="button" class="fav-icon-btn" id="posterRoleUrlToggleBtn" onclick="posterRoleToggleUrlInput()" title="From URL"><i class="fa-solid fa-link"></i></button>
        </div>
        <div aria-hidden="true"></div>
      </div>
      <div class="fav-roles-header-url-row" id="posterRoleHeaderUrlRow">
        <input type="url" id="posterRoleImportUrl" placeholder="https://…" autocomplete="off" onkeydown="if(event.key==='Enter')posterRoleLoadImportUrl();if(event.key==='Escape')posterRoleToggleUrlInput(false)">
        <button type="button" class="fav-icon-btn fav-icon-btn--primary" onclick="posterRoleLoadImportUrl()">Load</button>
      </div>
      <input type="file" id="posterRoleImportFile" accept="image/*" style="display:none" onchange="posterRoleHandleImportFile(this.files)">
    </div>
    <div class="modal-body">
      <div class="fav-roles-layout">
        <!-- Left column: stacked role tiles (headshot, primary, secondary)
             with each tile's apply button directly underneath. Tile labels
             render as overlay pills inside the image (no external heading). -->
        <div class="fav-roles-left-col">
          <div class="fav-roles-current-tile fav-roles-headshot-tile" id="posterRoleCurHeadshot" data-role="headshot">
            <div class="fav-roles-current-img-wrap is-empty" data-role="headshot">
              <img alt="Current headshot" id="posterRoleCurHeadshotImg" onerror="this.style.display='none';this.parentElement.classList.add('is-empty')">
              <div class="fav-roles-current-empty">Not set</div>
              <div class="fav-roles-current-label-overlay">Headshot</div>
            </div>
          </div>
          <div class="fav-roles-apply-row">
            <button type="button" class="fav-icon-btn fav-roles-apply-btn" id="posterRoleSetHeadshotBtn" onclick="applyPosterRole('headshot')" disabled title="Crop &amp; set headshot"><i class="fa-solid fa-angles-up"></i></button>
          </div>

          <div class="fav-roles-current-tile" id="posterRoleCurPrimary" data-role="primary">
            <div class="fav-roles-current-img-wrap is-empty" data-role="primary">
              <img alt="Current primary poster" id="posterRoleCurPrimaryImg" onerror="this.style.display='none';this.parentElement.classList.add('is-empty')">
              <div class="fav-roles-current-empty">Not set</div>
              <div class="fav-roles-current-label-overlay">Primary</div>
            </div>
          </div>
          <div class="fav-roles-apply-row">
            <button type="button" class="fav-icon-btn fav-roles-apply-btn" id="posterRoleSetPrimaryBtn" onclick="applyPosterRole('primary')" disabled title="Set as primary"><i class="fa-solid fa-angles-up"></i></button>
          </div>

          <div class="fav-roles-current-tile" id="posterRoleCurSecondary" data-role="secondary">
            <div class="fav-roles-current-img-wrap is-empty" data-role="secondary">
              <img alt="Current secondary poster" id="posterRoleCurSecondaryImg" onerror="this.style.display='none';this.parentElement.classList.add('is-empty')">
              <div class="fav-roles-current-empty">Not set</div>
              <div class="fav-roles-current-label-overlay">Secondary</div>
            </div>
          </div>
          <div class="fav-roles-apply-row">
            <button type="button" class="fav-icon-btn fav-roles-apply-btn" id="posterRoleSetSecondaryBtn" onclick="applyPosterRole('secondary')" disabled title="Set as secondary"><i class="fa-solid fa-angles-up"></i></button>
          </div>
        </div>
        <!-- Middle: candidate grid. -->
        <div id="posterRoleCandidates" class="fav-imgpick-candidates-scroll"></div>
        <!-- Right column: large preview pane that mirrors whichever candidate
             is currently selected, scaled to fit while preserving aspect
             ratio (2:3 portraits, 3:4 headshots, square thumbs). -->
        <div class="fav-roles-preview-col">
          <div class="fav-roles-preview-pane is-empty" id="posterRolePreviewPane">
            <img alt="Selected image preview" id="posterRolePreviewImg" style="display:none">
            <div class="fav-roles-preview-empty">Select a candidate, click an existing tile, or drop a local image here</div>
          </div>
          <div class="fav-roles-preview-caption" id="posterRolePreviewCaption"></div>
        </div>
      </div>
    </div>
  </div>
</div>
<!-- Headshot crop modal: opened by the Manage-posters modal when the user
     assigns a candidate / uploaded / URL-loaded image as the headshot. -->
<div class="modal-overlay" id="cropModal" onclick="if(event.target===this)closeCropModal()">
  <div class="modal-box crop-modal-box" onclick="event.stopPropagation()">
    <h3 style="display:flex;align-items:center;gap:8px;margin:0 0 4px 0"><i class="fa-solid fa-crop-simple"></i> Crop headshot</h3>
    <div class="modal-body" style="padding:10px 0 6px">
      <!-- ``touch-action:none`` + ``user-select:none`` inline so the pan
           gesture works on pages that don't load library.css (every page
           except /library). Same for the img's ``draggable=false`` +
           ``pointer-events:none`` — otherwise the browser fires its
           native image-drag (ghost-copy of the img) and the pointer
           capture on the viewport never sees the move. -->
      <div class="crop-viewport" id="cropViewport" style="touch-action:none;user-select:none">
        <img id="cropImg" alt="" referrerpolicy="no-referrer"
             draggable="false" ondragstart="event.preventDefault();return false"
             style="pointer-events:none;user-select:none;-webkit-user-drag:none">
      </div>
      <div class="crop-scale-row">
        <i class="fa-solid fa-magnifying-glass-minus"></i>
        <input type="range" id="cropScale" min="1" max="4" step="0.01" value="1">
        <i class="fa-solid fa-magnifying-glass-plus"></i>
        <span class="crop-scale-val" id="cropScaleVal">1.00×</span>
      </div>
      <div class="crop-hint">Drag the image to reposition. Scroll or use the slider to zoom.</div>
    </div>
    <div class="modal-foot">
      <button type="button" class="fav-icon-btn" onclick="closeCropModal()" style="width:auto;padding:0 14px;font-size:12px">Cancel</button>
      <button type="button" class="fav-icon-btn fav-icon-btn--primary" id="cropConfirmBtn" onclick="confirmCrop()" style="width:auto;padding:0 14px;font-size:12px"><i class="fa-solid fa-check"></i> Apply</button>
    </div>
  </div>
</div>`;
    document.body.appendChild(host);
    wirePosterRolePickerOnce();
  }

  function wirePosterRolePickerOnce() {
    if (_posterRoleWired) return;
    const modal = document.getElementById('posterRoleModal');
    if (!modal) return;
    _posterRoleWired = true;
    modal.addEventListener('click', onPosterRoleModalClick);
    modal.addEventListener('click', onPosterRoleRoleTileClick);
    wirePosterRoleDragDrop();
    wireCropInteractions();
  }

  function onPosterRoleModalClick(e) {
    const tile = e.target.closest('[data-role-idx]');
    if (!tile) return;
    const idx = parseInt(tile.getAttribute('data-role-idx'), 10);
    const item = _posterRoleItems[idx];
    if (!item || !item.image) return;
    posterRoleSelectCandidate(item);
    tile.classList.add('is-selected');
  }

  function onPosterRoleRoleTileClick(e) {
    const wrap = e.target.closest('.fav-roles-current-img-wrap[data-role]');
    if (!wrap) return;
    if (wrap.classList.contains('is-empty')) return;
    const role = wrap.getAttribute('data-role');
    const idMap = { headshot: 'posterRoleCurHeadshotImg', primary: 'posterRoleCurPrimaryImg', secondary: 'posterRoleCurSecondaryImg' };
    const img = document.getElementById(idMap[role]);
    if (!img || !img.src) return;
    const labelMap = { headshot: 'Headshot', primary: 'Primary', secondary: 'Secondary' };
    _setPosterRoleSelectionPreview(img.src, { name: 'Current ' + labelMap[role], source: '' });
  }

  async function resolvePosterPickerTitle(rowId, opts) {
    opts = opts || {};
    if (opts.title) return opts.title;
    if (typeof window._favGetPerformerFolderName === 'function') {
      const t = window._favGetPerformerFolderName(rowId);
      if (t) return t;
    }
    try {
      const r = await fetch('/api/performer/popup?row_id=' + encodeURIComponent(rowId), { credentials: 'same-origin' });
      const d = await r.json().catch(() => ({}));
      if (d && d.identity && d.identity.canonical_name) return d.identity.canonical_name;
    } catch (_) { /* ignore */ }
    return 'Star';
  }

  let _posterRoleRowId = null;
  let _posterRoleItems = [];
  let _posterRoleSelectedUrl = null;
  let _posterRoleTempUrls = [];  // blob: URLs to revoke on close

  // Force the rendered tile (and entity-panel poster, if open) to
  // re-fetch from /api/favourites/performer-thumb after the user saves a
  // new headshot / primary / secondary. Cache-buster query param sidesteps
  // the browser keeping the previous response in memory; the endpoint URL
  // itself is stable so we don't have to mutate the in-memory row state.
  function _refreshLibraryImagesForRow(rowId) {
    if (!rowId) return;
    const hasLibraryGrid = document.querySelector('.fav-stage, .fav-grid, #favGrid, .fav-cell');
    if (!hasLibraryGrid) {
      if (window._performerPopupActiveId === rowId && typeof window.refreshPerformerPopup === 'function') {
        window.refreshPerformerPopup();
      }
      return;
    }
    const url = `/api/favourites/performer-thumb?row_id=${encodeURIComponent(rowId)}&t=${Date.now()}`;
    const cell = document.querySelector(`.fav-cell[data-id="${rowId}"]`);
    if (cell) {
      const img = cell.querySelector('.duo-img');
      if (img) {
        // Drop the static-fallback onerror so an earlier 404 doesn't
        // pin the placeholder permanently — a fresh fetch may now
        // succeed because the user just uploaded an image.
        img.onerror = null;
        img.src = url;
      }
    }
    // Entity panel hero poster, when the modal is open on this row.
    if (typeof _entityPanelRowId !== 'undefined' && _entityPanelRowId === rowId) {
      const panel = document.getElementById('entityPanelBody');
      if (panel) {
        panel.querySelectorAll('.duo-img, .entity-panel-poster, .entity-poster img').forEach((el) => {
          el.onerror = null;
          el.src = url;
        });
      }
    }
  }

  /** Keep URL input inside the centered header actions (not a second row). */
  function _ensurePosterRoleHeaderLayout() {
    const header = document.querySelector('#posterRoleModal .fav-roles-modal-header');
    const urlRow = document.getElementById('posterRoleHeaderUrlRow');
    if (!header || !urlRow) return;
    let actions = header.querySelector('.fav-roles-header-actions');
    if (!actions) {
      const legacy = header.querySelector('.fav-roles-header-center');
      if (legacy) {
        legacy.classList.add('fav-roles-header-actions');
        legacy.classList.remove('fav-roles-header-center');
        actions = legacy;
      }
    }
    if (actions && urlRow.parentElement !== actions) actions.appendChild(urlRow);
  }

  async function openPosterRolePicker(rowId, opts) {
    opts = opts || {};
    ensurePosterModals();
    if (typeof window.closeAllFavMenus === 'function') window.closeAllFavMenus();
    _ensurePosterRoleHeaderLayout();
    _posterRoleRowId = rowId;
    _posterRoleItems = [];
    _posterRoleSelectedUrl = null;
    _posterRoleTempUrls = [];
    const titleLabel = await resolvePosterPickerTitle(rowId, opts);
    document.getElementById('posterRoleTitle').textContent =
      'Manage posters — ' + displayFolderName(titleLabel);
    const candBox = document.getElementById('posterRoleCandidates');
    candBox.innerHTML = '<div class="empty-tile" style="padding:20px">Loading linked images…</div>';
    _setPosterRolePreview('primary',  null);
    _setPosterRolePreview('secondary', null);
    _setPosterRolePreview('headshot', null);
    _setPosterRoleSelectionPreview(null);
    _setPosterRoleApplyEnabled(false);
    // Reset the header URL row each open.
    const urlRow = document.getElementById('posterRoleHeaderUrlRow');
    const urlInput = document.getElementById('posterRoleImportUrl');
    const urlBtn = document.getElementById('posterRoleUrlToggleBtn');
    if (urlRow) urlRow.classList.remove('is-open');
    if (urlInput) urlInput.value = '';
    if (urlBtn) urlBtn.classList.remove('is-active');
    document.getElementById('posterRoleModal').classList.add('open');
    try {
      const r = await fetch(
        '/api/performers/poster-roles?row_id=' + encodeURIComponent(rowId),
        { credentials: 'same-origin' },
      );
      const d = await r.json().catch(() => ({}));
      if (!r.ok) {
        candBox.innerHTML = `<div class="empty-tile" style="padding:20px;color:var(--red)">${esc(d.error || 'Load failed')}</div>`;
        return;
      }
      _setPosterRolePreview('primary',   d.primary_url);
      _setPosterRolePreview('secondary', d.secondary_url);
      _setPosterRolePreview('headshot',  d.headshot_url);
      _posterRoleItems = d.candidates || [];
      _renderPosterRoleCandidates();
    } catch (e) {
      candBox.innerHTML = `<div class="empty-tile" style="padding:20px;color:var(--red)">${esc(e.message)}</div>`;
    }
  }

  function closePosterRolePicker() {
    document.getElementById('posterRoleModal').classList.remove('open');
    _posterRoleTempUrls.forEach((u) => {
      try { URL.revokeObjectURL(u); } catch (_) { /* ignore */ }
    });
    _posterRoleRowId = null;
    _posterRoleItems = [];
    _posterRoleSelectedUrl = null;
    _posterRoleTempUrls = [];
    const urlRow = document.getElementById('posterRoleHeaderUrlRow');
    if (urlRow) urlRow.classList.remove('is-open');
    const urlInput = document.getElementById('posterRoleImportUrl');
    if (urlInput) urlInput.value = '';
    const urlBtn = document.getElementById('posterRoleUrlToggleBtn');
    if (urlBtn) urlBtn.classList.remove('is-active');
  }

  function _setPosterRolePreview(role, url) {
    const idSuffix = role === 'primary' ? 'Primary' : role === 'secondary' ? 'Secondary' : 'Headshot';
    const img = document.getElementById('posterRoleCur' + idSuffix + 'Img');
    const wrap = img ? img.parentElement : null;
    if (!img || !wrap) return;
    if (url) {
      // Cache-bust so re-applying the same role after a crop reloads the file.
      const sep = url.indexOf('?') >= 0 ? '&' : '?';
      img.src = url + sep + '_t=' + Date.now();
      img.style.display = '';
      wrap.classList.remove('is-empty');
    } else {
      img.removeAttribute('src');
      img.style.display = 'none';
      wrap.classList.add('is-empty');
    }
  }

  function _setPosterRoleApplyEnabled(on) {
    document.getElementById('posterRoleSetPrimaryBtn').disabled   = !on;
    document.getElementById('posterRoleSetSecondaryBtn').disabled = !on;
    document.getElementById('posterRoleSetHeadshotBtn').disabled  = !on;
  }

  function _renderPosterRoleCandidates() {
    const box = document.getElementById('posterRoleCandidates');
    if (!_posterRoleItems.length) {
      box.innerHTML = '<div class="empty-tile" style="padding:20px">No images from linked profiles. Add a TPDB, StashDB, FansDB, or JAVStash link first.</div>';
      return;
    }
    box.innerHTML = `<div class="fav-imgpick-grid">${_posterRoleItems.map((x, i) => {
      const sel = x.image === _posterRoleSelectedUrl ? ' is-selected' : '';
      const img = x.image
        ? `<img src="${escAttr(x.image)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.onerror=null;this.src='${POSTER_FALLBACK}'">`
        : `<img src="${POSTER_FALLBACK}" alt="" loading="lazy">`;
      return `<button type="button" class="fav-imgpick-tile fav-imgpick-tile--performer${sel}" data-role-idx="${i}" title="${escAttr((x.name || '') + ' — ' + (x.source || ''))}">
        ${img}
        <span class="fav-imgpick-badge">${esc(x.source || '')}</span>
      </button>`;
    }).join('')}</div>`;
  }


  function _setPosterRoleSelectionPreview(url, meta) {
    const pane = document.getElementById('posterRolePreviewPane');
    const img  = document.getElementById('posterRolePreviewImg');
    const cap  = document.getElementById('posterRolePreviewCaption');
    if (!pane || !img) return;
    if (url) {
      img.src = url;
      img.style.display = '';
      pane.classList.remove('is-empty');
    } else {
      img.removeAttribute('src');
      img.style.display = 'none';
      pane.classList.add('is-empty');
    }
    if (cap) {
      const m = meta || {};
      const name = (m.name || '').trim();
      const source = (m.source || '').trim();
      if (!url || (!name && !source)) {
        cap.innerHTML = '';
      } else {
        const nameHtml = name ? `<span class="fav-roles-caption-name">${esc(name)}</span>` : '';
        const srcHtml = source ? `<span class="fav-roles-caption-source">${esc(source)}</span>` : '';
        cap.innerHTML = nameHtml + srcHtml;
      }
    }
  }

  // ── Drag-and-drop: drop a local image onto the preview pane or any
  //    role tile to fast-track the upload-then-crop flow. The crop modal
  //    is the same path the Upload button uses, so the cropped result is
  //    saved as the headshot via /api/performers/upload-image.
  function wirePosterRoleDragDrop() {
    const targets = [
      document.getElementById('posterRolePreviewPane'),
      document.querySelector('.fav-roles-current-img-wrap[data-role="headshot"]'),
      document.querySelector('.fav-roles-current-img-wrap[data-role="primary"]'),
      document.querySelector('.fav-roles-current-img-wrap[data-role="secondary"]'),
    ].filter(Boolean);
    targets.forEach((el) => {
      el.addEventListener('dragenter', (e) => {
        if (!_posterRoleRowId) return;
        e.preventDefault();
        e.stopPropagation();
        el.classList.add('is-dragover');
      });
      el.addEventListener('dragover', (e) => {
        if (!_posterRoleRowId) return;
        e.preventDefault();
        e.stopPropagation();
        e.dataTransfer.dropEffect = 'copy';
      });
      el.addEventListener('dragleave', (e) => {
        if (e.target !== el) return;
        el.classList.remove('is-dragover');
      });
      el.addEventListener('drop', (e) => {
        if (!_posterRoleRowId) return;
        e.preventDefault();
        e.stopPropagation();
        el.classList.remove('is-dragover');
        const files = e.dataTransfer && e.dataTransfer.files;
        if (!files || !files.length) return;
        posterRoleHandleImportFile(files);
      });
    });
  }

  function posterRoleSelectCandidate(item) {
    if (!item || !item.image) return;
    _posterRoleSelectedUrl = item.image;
    _setPosterRoleApplyEnabled(true);
    document.querySelectorAll('#posterRoleCandidates .fav-imgpick-tile').forEach((t) => t.classList.remove('is-selected'));
    const idx = _posterRoleItems.indexOf(item);
    if (idx >= 0) {
      const tile = document.querySelector(`#posterRoleCandidates [data-role-idx="${idx}"]`);
      if (tile) tile.classList.add('is-selected');
    }
    _setPosterRoleSelectionPreview(item.image, {
      name: item.name || '',
      source: item.source || '',
    });
  }

  function posterRoleAddImportCandidate(imageUrl, meta) {
    if (!imageUrl) return;
    const source = ((meta && meta.source) || 'Import').trim() || 'Import';
    const name = ((meta && meta.name) || 'Imported image').trim() || 'Imported image';
    const item = { image: imageUrl, source, name, _import: true };
    _posterRoleItems.unshift(item);
    _renderPosterRoleCandidates();
    posterRoleSelectCandidate(item);
    const box = document.getElementById('posterRoleCandidates');
    if (box) box.scrollTop = 0;
  }

  async function applyPosterRole(role) {
    if (!_posterRoleRowId || !_posterRoleSelectedUrl) return;
    // Headshot goes through the crop dialog; posters POST straight through
    // as before (unchanged 2:3 crop is handled server-side).
    if (role === 'headshot') {
      openCropModal(_posterRoleSelectedUrl);
      return;
    }
    if (role !== 'primary' && role !== 'secondary') return;
    _setPosterRoleApplyEnabled(false);
    showMsg('Applying…');
    try {
      let r;
      const src = _posterRoleSelectedUrl;
      if (src.startsWith('blob:')) {
        const blob = await fetch(src, { credentials: 'same-origin' }).then((res) => {
          if (!res.ok) throw new Error('Could not read uploaded image');
          return res.blob();
        });
        const form = new FormData();
        form.append('row_id', String(_posterRoleRowId));
        form.append('role', role);
        form.append('file', blob, 'import.jpg');
        r = await fetch('/api/performers/set-poster-role-file', {
          method: 'POST',
          credentials: 'same-origin',
          body: form,
        });
      } else {
        r = await fetch('/api/performers/set-poster-role', {
          method: 'POST',
          credentials: 'same-origin',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            row_id: _posterRoleRowId,
            image_url: src,
            role: role,
          }),
        });
      }
      const d = await r.json().catch(() => ({}));
      if (!r.ok || !d.ok) {
        window.toast(d.error || 'Could not apply image');
        _setPosterRoleApplyEnabled(true);
        return;
      }
      _setPosterRolePreview(role, d.poster_url);
      // Bust the underlying /library tile (and entity-panel poster, if
      // open) so the new artwork shows up without a page refresh.
      _refreshLibraryImagesForRow(_posterRoleRowId);
      // If the bio popup is sitting open behind the manage-posters
      // modal on this same row, refresh its images too.
      if (window._performerPopupActiveId === _posterRoleRowId && typeof window.refreshPerformerPopup === 'function') {
        window.refreshPerformerPopup();
      }
      showMsg(role === 'primary' ? 'Primary poster replaced' : 'Secondary poster replaced');
      _setPosterRoleApplyEnabled(true);
    } catch (e) {
      window.toast(e.message || 'Failed');
      _setPosterRoleApplyEnabled(true);
    }
  }

  // ── Import via header upload / URL → candidate grid ─────────────────
  // Upload and URL load add a temporary tile to the middle column. The
  // user picks headshot (crop), primary, or secondary with the apply
  // buttons on the left — same flow as scraped candidates.

  function posterRoleUploadImport() {
    if (!_posterRoleRowId) return;
    document.getElementById('posterRoleImportFile').click();
  }

  function posterRoleHandleImportFile(files) {
    if (!files || !files.length) return;
    const f = files[0];
    if (!/^image\//.test(f.type)) { window.toast('Please choose an image file'); return; }
    const url = URL.createObjectURL(f);
    _posterRoleTempUrls.push(url);
    posterRoleAddImportCandidate(url, { source: 'Upload', name: f.name || 'Upload' });
    document.getElementById('posterRoleImportFile').value = '';
  }

  function posterRoleToggleUrlInput(forceState) {
    const row = document.getElementById('posterRoleHeaderUrlRow');
    const btn = document.getElementById('posterRoleUrlToggleBtn');
    if (!row) return;
    const on = typeof forceState === 'boolean' ? forceState : !row.classList.contains('is-open');
    row.classList.toggle('is-open', on);
    if (btn) btn.classList.toggle('is-active', on);
    if (on) setTimeout(() => document.getElementById('posterRoleImportUrl')?.focus(), 30);
  }

  function posterRoleLoadImportUrl() {
    const url = (document.getElementById('posterRoleImportUrl').value || '').trim();
    if (!url) return;
    if (!/^https?:\/\//i.test(url)) { window.toast('URL must start with http:// or https://'); return; }
    posterRoleAddImportCandidate(url, { source: 'URL', name: 'From URL' });
    posterRoleToggleUrlInput(false);
    document.getElementById('posterRoleImportUrl').value = '';
  }

  // ── Crop modal state ────────────────────────────────────────────
  let _cropSrc = null;          // currently-displayed source image URL
  let _cropRevoke = false;      // URL.revokeObjectURL on close?
  let _cropNatW = 0;            // natural image width (px)
  let _cropNatH = 0;            // natural image height (px)
  let _cropBaseScale = 1;       // scale at which the image covers the viewport
  let _cropScale = 1;           // user-adjustable zoom multiplier (1..4)
  let _cropTx = 0;              // horizontal translation (px, displayed coords)
  let _cropTy = 0;              // vertical translation   (px, displayed coords)
  let _cropVpW = 0;             // viewport width  (px)
  let _cropVpH = 0;             // viewport height (px)
  let _cropDragging = false;
  let _cropDragSx = 0;
  let _cropDragSy = 0;
  let _cropDragTx0 = 0;
  let _cropDragTy0 = 0;

  function openCropModal(src, opts = {}) {
    _cropSrc = src;
    _cropRevoke = !!opts.revokeOnClose;
    const modal = document.getElementById('cropModal');
    const img = document.getElementById('cropImg');
    const vp = document.getElementById('cropViewport');
    document.getElementById('cropScale').value = '1';
    document.getElementById('cropScaleVal').textContent = '1.00×';
    _cropScale = 1;
    modal.classList.add('open');
    // Give the browser a paint so the viewport has a measurable width.
    requestAnimationFrame(() => {
      const rect = vp.getBoundingClientRect();
      _cropVpW = rect.width;
      _cropVpH = rect.height;
      img.onload = () => {
        _cropNatW = img.naturalWidth || 1;
        _cropNatH = img.naturalHeight || 1;
        // Cover-fit the viewport: short-edge of image fills the short edge
        // of the viewport, matched by long-edge overflow we can pan across.
        _cropBaseScale = Math.max(_cropVpW / _cropNatW, _cropVpH / _cropNatH);
        // Center the image in the viewport.
        const dw = _cropNatW * _cropBaseScale * _cropScale;
        const dh = _cropNatH * _cropBaseScale * _cropScale;
        _cropTx = (_cropVpW - dw) / 2;
        _cropTy = (_cropVpH - dh) / 2;
        _applyCropTransform();
      };
      img.onerror = () => { window.toast('Could not load image. Try a different URL.'); closeCropModal(); };
      img.referrerPolicy = 'no-referrer';
      img.src = src;
    });
  }

  function closeCropModal() {
    document.getElementById('cropModal').classList.remove('open');
    if (_cropRevoke && _cropSrc) {
      try { URL.revokeObjectURL(_cropSrc); } catch (_) {}
    }
    _cropSrc = null;
    _cropRevoke = false;
  }

  function _applyCropTransform() {
    const img = document.getElementById('cropImg');
    const finalScale = _cropBaseScale * _cropScale;
    const dw = _cropNatW * finalScale;
    const dh = _cropNatH * finalScale;
    // Clamp translation so the image always covers the viewport.
    _cropTx = Math.min(0, Math.max(_cropVpW - dw, _cropTx));
    _cropTy = Math.min(0, Math.max(_cropVpH - dh, _cropTy));
    img.style.width  = _cropNatW + 'px';
    img.style.height = _cropNatH + 'px';
    img.style.transform = `translate(${_cropTx}px, ${_cropTy}px) scale(${finalScale})`;
  }

  function wireCropInteractions() {
    if (_cropWired) return;
    const vp0 = document.getElementById("cropViewport");
    if (!vp0) return;
    _cropWired = true;
    const vp = document.getElementById('cropViewport');
    if (!vp) return;
    // Belt-and-braces: block the browser's native image-drag so the
    // pointer capture below always gets the move events. Fires
    // regardless of whether the inline ``draggable="false"`` attribute
    // on the injected template survived any subsequent DOM mutation.
    vp.addEventListener('dragstart', (e) => { e.preventDefault(); });
    vp.addEventListener('pointerdown', (e) => {
      if (!_cropSrc) return;
      _cropDragging = true;
      vp.classList.add('is-dragging');
      vp.setPointerCapture(e.pointerId);
      _cropDragSx = e.clientX;
      _cropDragSy = e.clientY;
      _cropDragTx0 = _cropTx;
      _cropDragTy0 = _cropTy;
    });
    vp.addEventListener('pointermove', (e) => {
      if (!_cropDragging) return;
      _cropTx = _cropDragTx0 + (e.clientX - _cropDragSx);
      _cropTy = _cropDragTy0 + (e.clientY - _cropDragSy);
      _applyCropTransform();
    });
    const end = (e) => {
      if (!_cropDragging) return;
      _cropDragging = false;
      vp.classList.remove('is-dragging');
      try { vp.releasePointerCapture(e.pointerId); } catch (_) {}
    };
    vp.addEventListener('pointerup', end);
    vp.addEventListener('pointercancel', end);
    // Scroll-to-zoom — anchor the zoom around the cursor position so the
    // image doesn't lurch away from where the user is looking.
    vp.addEventListener('wheel', (e) => {
      if (!_cropSrc) return;
      e.preventDefault();
      const rect = vp.getBoundingClientRect();
      const cx = e.clientX - rect.left;
      const cy = e.clientY - rect.top;
      const oldScale = _cropScale;
      const delta = e.deltaY < 0 ? 1.08 : (1 / 1.08);
      const next = Math.min(4, Math.max(1, oldScale * delta));
      if (next === oldScale) return;
      const ratio = next / oldScale;
      _cropTx = cx - (cx - _cropTx) * ratio;
      _cropTy = cy - (cy - _cropTy) * ratio;
      _cropScale = next;
      const slider = document.getElementById('cropScale');
      const out = document.getElementById('cropScaleVal');
      if (slider) slider.value = String(next);
      if (out) out.textContent = next.toFixed(2) + '×';
      _applyCropTransform();
    }, { passive: false });
    const slider = document.getElementById('cropScale');
    if (slider) {
      slider.addEventListener('input', () => {
        const next = parseFloat(slider.value) || 1;
        const oldScale = _cropScale;
        const ratio = next / oldScale;
        // Zoom around the viewport centre when the slider is used.
        const cx = _cropVpW / 2;
        const cy = _cropVpH / 2;
        _cropTx = cx - (cx - _cropTx) * ratio;
        _cropTy = cy - (cy - _cropTy) * ratio;
        _cropScale = next;
        document.getElementById('cropScaleVal').textContent = next.toFixed(2) + '×';
        _applyCropTransform();
      });
    }
  }

  async function confirmCrop() {
    // Diagnostic — the previous silent guard hid why Apply was a no-op.
    // Log entry unconditionally so we can tell whether the click is
    // reaching this function at all.
    try { console.log('[crop] confirmCrop entered', { _cropSrc: !!_cropSrc, _posterRoleRowId }); } catch (_) {}
    if (!_cropSrc) {
      if (typeof window.toast === 'function') window.toast('Apply failed: no image loaded in the crop modal.');
      return;
    }
    if (!_posterRoleRowId) {
      if (typeof window.toast === 'function') window.toast('Apply failed: target performer was cleared — reopen Manage Posters and try again.');
      return;
    }
    const btn = document.getElementById('cropConfirmBtn');
    btn.disabled = true;
    showMsg('Cropping…');
    try {
      // Compute the visible region in the original image's natural pixel
      // coordinates — the server redoes the crop with PIL so we don't
      // touch the canvas (cross-origin candidate images taint it and
      // toBlob() throws). sx/sy can go negative during a zoom-in pan
      // edge, clamp to 0.
      const finalScale = _cropBaseScale * _cropScale;
      const sx = Math.max(0, Math.round((-_cropTx) / finalScale));
      const sy = Math.max(0, Math.round((-_cropTy) / finalScale));
      const sw = Math.max(1, Math.round(_cropVpW / finalScale));
      const sh = Math.max(1, Math.round(_cropVpH / finalScale));

      const form = new FormData();
      form.append('row_id', String(_posterRoleRowId));
      form.append('crop_x', String(sx));
      form.append('crop_y', String(sy));
      form.append('crop_w', String(sw));
      form.append('crop_h', String(sh));

      // Remote http(s) → let the backend fetch (our requests helper
      // sends a real User-Agent, no Referer, etc.). Anything else —
      // local-upload blob URLs, our own /api/... local-library URLs —
      // is same-origin so we can fetch it client-side and pipe the raw
      // bytes as a File, which skips any backend URL-resolving logic.
      if (/^https?:\/\//i.test(_cropSrc)) {
        form.append('image_url', _cropSrc);
      } else {
        const r0 = await fetch(_cropSrc, { credentials: 'same-origin' });
        if (!r0.ok) throw new Error('Could not fetch source image (' + r0.status + ')');
        const blob = await r0.blob();
        const ext = (blob.type || '').split('/')[1] || 'jpg';
        form.append('file', blob, 'source.' + ext);
      }

      try { console.log('[crop] POST /api/performers/set-headshot-cropped', { row_id: _posterRoleRowId, sx, sy, sw, sh, is_url: /^https?:\/\//i.test(_cropSrc) }); } catch (_) {}
      const r = await fetch('/api/performers/set-headshot-cropped', {
        method: 'POST',
        credentials: 'same-origin',
        body: form,
      });
      const d = await r.json().catch(() => ({}));
      try { console.log('[crop] response', { status: r.status, ok: r.ok, body: d }); } catch (_) {}
      if (!r.ok) throw new Error(d.error || 'Crop failed');

      // Close the modal as soon as the save returns OK. Previously the
      // user had to wait for a follow-up /api/performers/poster-roles
      // round-trip (just to pull the new headshot_url for the preview);
      // we can construct that URL deterministically from row_id since
      // _performers_save_headshot_bytes always writes the same shape.
      const rowId = _posterRoleRowId;
      closeCropModal();
      showMsg('Headshot updated');

      const headshotUrl = `/api/performers/iafd-image?row_id=${encodeURIComponent(rowId)}&i=0&v=${Date.now()}`;
      _setPosterRolePreview('headshot', headshotUrl);
      // Headshot drives the /library tile thumbnail (performer-thumb
      // endpoint defaults to headshot first), so bust it here too.
      _refreshLibraryImagesForRow(rowId);
      if (window._performerPopupActiveId === rowId && typeof window.refreshPerformerPopup === 'function') {
        window.refreshPerformerPopup();
      }
    } catch (e) {
      try { console.error('[crop] error in confirmCrop', e); } catch (_) {}
      if (typeof window.toast === 'function') window.toast(e.message || 'Crop failed');
      else alert('Crop failed: ' + (e.message || e));
    } finally {
      btn.disabled = false;
    }
  }
  window.openPosterRolePicker = openPosterRolePicker;
  window.closePosterRolePicker = closePosterRolePicker;
  window.applyPosterRole = applyPosterRole;
  window.posterRoleUploadImport = posterRoleUploadImport;
  window.posterRoleHandleImportFile = posterRoleHandleImportFile;
  window.posterRoleToggleUrlInput = posterRoleToggleUrlInput;
  window.posterRoleLoadImportUrl = posterRoleLoadImportUrl;
  window.openCropModal = openCropModal;
  window.closeCropModal = closeCropModal;
  window.confirmCrop = confirmCrop;

  window.ensurePosterRolePicker = function () {
    ensurePosterModals();
    return Promise.resolve();
  };
})();
