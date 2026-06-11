/* Library entity actions — remove from app, delete from disk, move performer folder.
 * Used by /library (favourites.html) and performer-popup.js. */
(function () {
  if (window.LibEntityActions) return;

  const esc = (s) => String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
  const toast = (msg, opts) => {
    if (window.toast) return window.toast(msg, opts || {});
    return alert(msg);
  };
  const refreshBanner = () => {
    if (window.TsActivity && typeof window.TsActivity.refresh === 'function') {
      window.TsActivity.refresh();
    }
  };

  let _moveModal = null;
  let _moveDismiss = null;

  function ctxFromRow(row) {
    if (!row) return null;
    const id = row.id;
    const kind = row.kind || (id < 0 ? 'vice' : 'performer');
    const name = row.folder_name || row.name || 'this entry';
    return {
      id,
      kind,
      name,
      viceId: row.vice_id != null ? String(row.vice_id) : '',
      canChangeDirectory: kind === 'performer' && id > 0,
    };
  }

  function confirmRemove(ctx) {
    const label = ctx.kind === 'vice' ? 'vice' : ctx.kind;
    return confirm(
      `Remove “${ctx.name}” from the library?\n\n` +
      `This removes the ${label} from Top-Shelf only. Files on disk are not deleted.\n\n` +
      `This cannot be undone from the app (you can scan again later to re-index the folder).`
    );
  }

  function confirmDeleteDisk(ctx) {
    const label = ctx.kind === 'vice' ? 'vice' : ctx.kind;
    return confirm(
      `Permanently delete “${ctx.name}” and ALL files inside its folder?\n\n` +
      `This removes the ${label} from the library AND deletes everything on disk ` +
      `(videos, posters, NFOs, etc.).\n\n` +
      `THIS CANNOT BE UNDONE.`
    );
  }

  async function postJson(url, body) {
    const r = await fetch(url, {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(d.error || 'Request failed');
    return d;
  }

  async function removeFromLibrary(ctx) {
    if (!ctx || !confirmRemove(ctx)) return false;
    const body = { id: ctx.id };
    if (ctx.viceId) body.vice_id = ctx.viceId;
    await postJson('/api/library/entity/remove-from-library', body);
    toast(`Removed “${ctx.name}” from the library`, { kind: 'success' });
    refreshBanner();
    return true;
  }

  async function deleteFromDisk(ctx) {
    if (!ctx || !confirmDeleteDisk(ctx)) return false;
    const body = { id: ctx.id };
    if (ctx.viceId) body.vice_id = ctx.viceId;
    await postJson('/api/library/entity/delete-from-disk', body);
    toast(`Deleted “${ctx.name}” from disk`, { kind: 'success' });
    refreshBanner();
    return true;
  }

  function ensureMoveModal() {
    if (_moveModal) return _moveModal;
    const el = document.createElement('div');
    el.id = 'libEntityMoveModal';
    el.className = 'modal-overlay lib-entity-move-overlay';
    el.innerHTML = `
      <div class="modal-box" style="max-width:480px">
        <h3>Change directory</h3>
        <p class="lib-entity-move-hint" id="libEntityMoveHint"></p>
        <label class="field-label" style="margin-top:12px">Move folder to</label>
        <select class="field-input" id="libEntityMoveSelect" style="width:100%;margin-top:6px"></select>
        <div class="modal-actions" style="margin-top:18px;display:flex;gap:10px;justify-content:flex-end">
          <button type="button" class="btn-secondary" id="libEntityMoveCancel">Cancel</button>
          <button type="button" class="btn-primary" id="libEntityMoveConfirm">Move folder</button>
        </div>`;
    document.body.appendChild(el);
    el.addEventListener('click', (e) => {
      if (e.target === el && _moveDismiss) _moveDismiss();
    });
    _moveModal = el;
    return el;
  }

  function closeMoveModal() {
    if (_moveModal) _moveModal.classList.remove('open');
    _moveDismiss = null;
  }

  function openMoveModal(ctx) {
    return new Promise((resolve) => {
      ensureMoveModal();
      const modal = _moveModal;
      const hint = modal.querySelector('#libEntityMoveHint');
      const sel = modal.querySelector('#libEntityMoveSelect');
      const confirmBtn = modal.querySelector('#libEntityMoveConfirm');
      let settled = false;
      const done = (ok) => {
        if (settled) return;
        settled = true;
        closeMoveModal();
        resolve(!!ok);
      };
      _moveDismiss = () => done(false);
      modal.querySelector('#libEntityMoveCancel').onclick = _moveDismiss;
      hint.textContent = 'Loading directories…';
      sel.innerHTML = '';
      confirmBtn.disabled = false;
      // Stay above the universal performer popup (modal-overlay defaults to 1200).
      document.body.appendChild(modal);
      modal.classList.add('open');

      fetch('/api/library/performer-move-targets?row_id=' + encodeURIComponent(ctx.id), {
        credentials: 'same-origin',
      })
        .then((r) => r.json().then((d) => ({ ok: r.ok, d })))
        .then(({ ok, d }) => {
          if (!ok) throw new Error(d.error || 'Failed to load directories');
          const targets = d.targets || [];
          if (!targets.length) {
            hint.textContent = 'No other star directories are configured in Settings → Directories.';
            sel.innerHTML = '';
            confirmBtn.disabled = true;
            return;
          }
          confirmBtn.disabled = false;
          hint.innerHTML =
            `Move <strong>${esc(d.folder_name || ctx.name)}</strong> and everything inside it ` +
            `to another performer root. This cannot be undone.`;
          sel.innerHTML = targets.map((t) =>
            `<option value="${esc(String(t.id))}">${esc(t.label)} — ${esc(t.path)}</option>`
          ).join('');
        })
        .catch((e) => {
          hint.textContent = e.message || 'Error';
          confirmBtn.disabled = true;
        });

      const onConfirm = async () => {
        const targetId = sel.value;
        if (!targetId) return;
        if (!confirm(
          `Move “${ctx.name}” and ALL files in its folder to the selected directory?\n\n` +
          'This cannot be undone.'
        )) return;
        confirmBtn.disabled = true;
        try {
          await postJson('/api/library/entity/change-directory', {
            id: ctx.id,
            target_directory_id: targetId,
          });
          toast(`Moved “${ctx.name}”`, { kind: 'success' });
          refreshBanner();
          done(true);
        } catch (e) {
          toast(e.message || 'Move failed', { kind: 'error' });
          confirmBtn.disabled = false;
        }
      };

      confirmBtn.replaceWith(confirmBtn.cloneNode(true));
      modal.querySelector('#libEntityMoveConfirm').addEventListener('click', onConfirm);
    });
  }

  async function changeDirectory(ctx) {
    if (!ctx || !ctx.canChangeDirectory) return false;
    return openMoveModal(ctx);
  }

  async function refreshPerformerImages(ctx, triggerEl) {
    if (!ctx || ctx.kind !== 'performer' || ctx.id <= 0) return false;
    if (typeof window.refreshPerformerImagesFromPopup === 'function') {
      await window.refreshPerformerImagesFromPopup(triggerEl);
      return true;
    }
    return false;
  }

  let _renameModal = null;
  let _renameDismiss = null;
  const RENAME_STAGES = [
    { key: 'dir',          label: 'Rename local directory' },
    { key: 'app',          label: 'Update Top-Shelf record' },
    { key: 'tvshow_nfo',   label: 'Rewrite series metadata' },
    { key: 'videos',       label: 'Rename video files' },
    { key: 'episode_nfo', label: 'Update episode metadata' },
  ];

  function ensureRenameModal() {
    if (_renameModal) return _renameModal;
    const el = document.createElement('div');
    el.id = 'libEntityRenameModal';
    el.className = 'modal-overlay lib-entity-rename-overlay';
    el.innerHTML = `
      <div class="modal-box lib-entity-rename-box">
        <h3>Rename performer</h3>
        <div class="lib-entity-rename-body">
          <p class="lib-entity-rename-hint" id="libEntityRenameHint"></p>
          <div class="lib-entity-rename-section">
            <label class="field-label">Pick an alias</label>
            <div class="lib-entity-rename-aliases" id="libEntityRenameAliases">
              <div class="lib-entity-rename-empty">Loading…</div>
            </div>
          </div>
          <div class="lib-entity-rename-section">
            <label class="field-label" for="libEntityRenameCustom">Or type a custom name</label>
            <input type="text" class="field-input" id="libEntityRenameCustom"
                   placeholder="New performer name" autocomplete="off" spellcheck="false">
          </div>
          <p class="lib-entity-rename-note">
            The old name is added to this performer's aliases automatically.
            Only files following the canonical
            <code>name - S####E####</code> pattern are renamed.
          </p>
          <div class="modal-actions" style="display:flex;gap:10px;justify-content:flex-end">
            <button type="button" class="btn-secondary" id="libEntityRenameCancel">Cancel</button>
            <button type="button" class="btn-primary" id="libEntityRenameConfirm">Rename</button>
          </div>
        </div>
        <div class="lib-entity-rename-progress" id="libEntityRenameProgress" hidden>
          <p class="lib-entity-rename-progress-title" id="libEntityRenameProgressTitle">Renaming…</p>
          <ol class="lib-entity-rename-stages" id="libEntityRenameStages"></ol>
          <div class="lib-entity-rename-result" id="libEntityRenameResult" hidden></div>
          <div class="modal-actions" style="display:flex;gap:10px;justify-content:flex-end">
            <button type="button" class="btn-primary" id="libEntityRenameDone" hidden>Close</button>
          </div>
        </div>
      </div>`;
    document.body.appendChild(el);
    el.addEventListener('click', (e) => {
      if (e.target === el && _renameDismiss) _renameDismiss();
    });
    _renameModal = el;
    return el;
  }

  function closeRenameModal() {
    if (_renameModal) _renameModal.classList.remove('open');
    _renameDismiss = null;
  }

  function renderRenameAliasChips(container, aliases, selectedHandler) {
    if (!aliases.length) {
      container.innerHTML = '<div class="lib-entity-rename-empty">No aliases on file. Use the field below.</div>';
      return;
    }
    container.innerHTML = aliases.map((a, i) =>
      `<button type="button" class="lib-entity-rename-chip" data-alias="${esc(a)}">${esc(a)}</button>`
    ).join('');
    container.querySelectorAll('.lib-entity-rename-chip').forEach((b) => {
      b.addEventListener('click', () => {
        container.querySelectorAll('.lib-entity-rename-chip').forEach((c) =>
          c.classList.toggle('is-selected', c === b));
        selectedHandler(b.dataset.alias || '');
      });
    });
  }

  function renderRenameStages(currentIndex, results) {
    const ol = document.getElementById('libEntityRenameStages');
    if (!ol) return;
    ol.innerHTML = RENAME_STAGES.map((stage, i) => {
      const r = results[stage.key];
      let icon = '<i class="fa-regular fa-circle"></i>';
      let cls = '';
      if (r && r.ok === true) { icon = '<i class="fa-solid fa-circle-check"></i>'; cls = 'is-done'; }
      else if (r && r.ok === false) { icon = '<i class="fa-solid fa-circle-xmark"></i>'; cls = 'is-error'; }
      else if (i === currentIndex) { icon = '<i class="fa-solid fa-spinner fa-spin"></i>'; cls = 'is-active'; }
      let detail = '';
      if (r && stage.key === 'videos' && r.ok) {
        const parts = [];
        if (typeof r.renamed === 'number') parts.push(`${r.renamed} renamed`);
        if (r.failed) parts.push(`${r.failed} failed`);
        if (parts.length) detail = ` <span class="lib-entity-rename-stage-detail">(${parts.join(', ')})</span>`;
      } else if (r && stage.key === 'episode_nfo' && r.ok) {
        detail = ` <span class="lib-entity-rename-stage-detail">(${r.updated || 0}/${r.scanned || 0} NFOs)</span>`;
      } else if (r && stage.key === 'tvshow_nfo' && r.ok && r.updated) {
        detail = ` <span class="lib-entity-rename-stage-detail">(updated)</span>`;
      } else if (r && stage.key === 'tvshow_nfo' && r.ok && !r.updated) {
        detail = ` <span class="lib-entity-rename-stage-detail">(no change)</span>`;
      } else if (r && r.ok === false && r.error) {
        detail = ` <span class="lib-entity-rename-stage-detail is-error">${esc(r.error)}</span>`;
      }
      return `<li class="lib-entity-rename-stage ${cls}">
        <span class="lib-entity-rename-stage-icon">${icon}</span>
        <span class="lib-entity-rename-stage-label">${esc(stage.label)}</span>
        ${detail}
      </li>`;
    }).join('');
  }

  async function streamRename(ctx, newName, onUpdate) {
    const r = await fetch('/api/library/performer/rename', {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ row_id: ctx.id, new_name: newName }),
    });
    if (!r.ok) {
      let msg = 'Rename failed';
      try { const d = await r.json(); msg = d.error || msg; } catch (_) {}
      throw new Error(msg);
    }
    const reader = r.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let final = null;
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let nl;
      while ((nl = buffer.indexOf('\n')) >= 0) {
        const line = buffer.slice(0, nl).trim();
        buffer = buffer.slice(nl + 1);
        if (!line) continue;
        let evt;
        try { evt = JSON.parse(line); } catch (_) { continue; }
        onUpdate(evt);
        if (evt.stage === 'done') final = evt;
      }
    }
    if (buffer.trim()) {
      try { const evt = JSON.parse(buffer.trim()); onUpdate(evt); if (evt.stage === 'done') final = evt; } catch (_) {}
    }
    return final || { ok: false, error: 'rename did not complete' };
  }

  async function openRenameModal(ctx) {
    return new Promise((resolve) => {
      ensureRenameModal();
      const modal = _renameModal;
      const hint = modal.querySelector('#libEntityRenameHint');
      const aliasWrap = modal.querySelector('#libEntityRenameAliases');
      const customInput = modal.querySelector('#libEntityRenameCustom');
      const confirmBtn = modal.querySelector('#libEntityRenameConfirm');
      const cancelBtn = modal.querySelector('#libEntityRenameCancel');
      const body = modal.querySelector('.lib-entity-rename-body');
      const progress = modal.querySelector('#libEntityRenameProgress');
      const result = modal.querySelector('#libEntityRenameResult');
      const doneBtn = modal.querySelector('#libEntityRenameDone');

      let settled = false;
      const done = (ok) => {
        if (settled) return;
        settled = true;
        closeRenameModal();
        resolve(!!ok);
      };
      _renameDismiss = () => { if (!progress.hidden) return; done(false); };
      cancelBtn.onclick = _renameDismiss;

      hint.innerHTML = `Renaming <strong>${esc(ctx.name)}</strong>. Pick an alias or type a new name.`;
      aliasWrap.innerHTML = '<div class="lib-entity-rename-empty">Loading…</div>';
      customInput.value = '';
      confirmBtn.disabled = true;
      confirmBtn.textContent = 'Rename';
      body.hidden = false;
      progress.hidden = true;
      result.hidden = true;
      result.innerHTML = '';
      doneBtn.hidden = true;
      doneBtn.onclick = () => done(true);

      let selected = '';
      const updateConfirmEnabled = () => {
        const trimmed = (customInput.value || '').trim();
        const v = trimmed || selected || '';
        confirmBtn.disabled = !v || v.toLowerCase() === (ctx.name || '').toLowerCase();
      };
      customInput.addEventListener('input', () => {
        if (customInput.value.trim()) {
          selected = '';
          aliasWrap.querySelectorAll('.lib-entity-rename-chip')
            .forEach((c) => c.classList.remove('is-selected'));
        }
        updateConfirmEnabled();
      });

      document.body.appendChild(modal);
      modal.classList.add('open');

      // Fetch aliases from the popup endpoint (same source the alias
      // editor uses) so the picker is always in sync with what the
      // popup itself shows.
      fetch('/api/performer/popup?row_id=' + encodeURIComponent(ctx.id), {
        credentials: 'same-origin',
      })
        .then((r) => r.json())
        .then((d) => {
          const id = (d && d.identity) || {};
          const out = [];
          const seen = new Set();
          const add = (n) => {
            const s = String(n || '').trim();
            if (!s) return;
            const k = s.toLowerCase();
            if (k === (ctx.name || '').toLowerCase()) return;
            if (seen.has(k)) return;
            seen.add(k);
            out.push(s);
          };
          if (id.canonical_name) add(id.canonical_name);
          (id.aliases || []).forEach(add);
          renderRenameAliasChips(aliasWrap, out, (val) => {
            selected = val;
            customInput.value = '';
            updateConfirmEnabled();
          });
        })
        .catch(() => {
          aliasWrap.innerHTML = '<div class="lib-entity-rename-empty">Could not load aliases.</div>';
        });

      confirmBtn.onclick = async () => {
        const newName = (customInput.value || '').trim() || selected;
        if (!newName) return;
        confirmBtn.disabled = true;
        cancelBtn.disabled = true;
        body.hidden = true;
        progress.hidden = false;
        const results = {};
        let activeIdx = 0;
        renderRenameStages(activeIdx, results);
        let failure = null;
        try {
          const final = await streamRename(ctx, newName, (evt) => {
            if (!evt || !evt.stage) return;
            if (evt.stage === 'done') return;
            results[evt.stage] = evt;
            const idx = RENAME_STAGES.findIndex((s) => s.key === evt.stage);
            if (evt.ok === false) failure = evt;
            activeIdx = idx + 1;
            renderRenameStages(activeIdx, results);
          });
          renderRenameStages(RENAME_STAGES.length, results);
          if (final.ok) {
            result.hidden = false;
            result.className = 'lib-entity-rename-result is-success';
            result.innerHTML = `Renamed <strong>${esc(ctx.name)}</strong> → <strong>${esc(final.new_name || newName)}</strong>.`;
            toast(`Renamed “${ctx.name}” → “${final.new_name || newName}”`, { kind: 'success' });
            refreshBanner();
            // Tell any open library / favourites page to re-fetch its
            // cards. /library caches rows in module-scoped arrays
            // (`_rowsPerf` etc.) so a name change isn't visible until
            // it reloads. The page listens for this event and calls
            // `load()` to refetch every kind.
            try {
              document.dispatchEvent(new CustomEvent('lib-entity-changed', {
                detail: {
                  action: 'rename',
                  id: ctx.id,
                  kind: ctx.kind,
                  old_name: ctx.name,
                  new_name: final.new_name || newName,
                },
              }));
            } catch (_) { /* IE-era browsers don't ship here */ }
          } else {
            result.hidden = false;
            result.className = 'lib-entity-rename-result is-error';
            result.innerHTML = `Rename failed: ${esc(final.error || (failure && failure.error) || 'unknown error')}`;
          }
        } catch (err) {
          result.hidden = false;
          result.className = 'lib-entity-rename-result is-error';
          result.innerHTML = `Rename failed: ${esc(err.message || 'unknown error')}`;
          toast(err.message || 'Rename failed', { kind: 'error' });
        } finally {
          doneBtn.hidden = false;
          cancelBtn.disabled = false;
        }
      };

      // Focus the custom input so the user can start typing immediately.
      setTimeout(() => { try { customInput.focus(); } catch (_) {} }, 50);
    });
  }

  async function renamePerformer(ctx) {
    if (!ctx || ctx.kind !== 'performer' || ctx.id <= 0) return false;
    return openRenameModal(ctx);
  }

  async function enrichPerformer(ctx) {
    if (!ctx || ctx.kind !== 'performer' || ctx.id <= 0) return false;
    await postJson('/api/performers/enrich', { row_id: ctx.id });
    toast('Enrichment started — links and images will update shortly', { kind: 'info' });
    if (typeof window.refreshPerformerPopup === 'function') {
      [4000, 10000, 20000].forEach((ms) => {
        setTimeout(() => {
          if (window._performerPopupActiveId === ctx.id) {
            window.refreshPerformerPopup();
          }
        }, ms);
      });
    }
    return true;
  }

  /* ── Performer merge / group-add / ungroup ───────────────────────
   * Three flows wired here:
   *   merge      — solo row → multi-pick others → new group folder
   *   group-add  — group row → multi-pick others → folded into group
   *   ungroup    — group row → confirm → files → queue, row + folder gone
   */

  let _mergeLibraryCache = null;
  let _mergeLibraryCacheAt = 0;
  /** Pull every library performer once per ~30s. Cached because the
   * modal opens repeatedly during a merge session and the result set
   * doesn't change in the middle of the flow. */
  async function _mergeFetchLibraryPerformers() {
    const now = Date.now();
    if (_mergeLibraryCache && (now - _mergeLibraryCacheAt) < 30000) {
      return _mergeLibraryCache;
    }
    const r = await fetch('/api/favourites', { credentials: 'same-origin' });
    const d = await r.json().catch(() => ({}));
    const perfs = (d && d.performers) || [];
    _mergeLibraryCache = perfs;
    _mergeLibraryCacheAt = now;
    return perfs;
  }

  function _ensureMergeModal() {
    if (document.getElementById('performerMergeModal')) return;
    const div = document.createElement('div');
    div.id = 'performerMergeModal';
    div.className = 'modal-overlay';
    div.style.setProperty('z-index', '1800', 'important');
    div.innerHTML = `
      <div class="modal-box ts-link-modal-box perf-merge-box"
           style="max-width:840px;width:min(840px,calc(100vw - 60px));max-height:calc(100vh - 80px);display:flex;flex-direction:column">
        <h3 id="perfMergeTitle" style="margin:0 0 6px 0;font-family:var(--font-display, var(--mono));font-size:18px">Merge performers</h3>
        <div id="perfMergeSub" style="font-size:11px;color:var(--dim);margin-bottom:12px"></div>

        <div id="perfMergeNameRow" style="display:flex;gap:10px;align-items:center;margin-bottom:10px">
          <label style="font-size:11px;color:var(--dim);font-family:var(--mono);min-width:90px">Group name</label>
          <input type="text" id="perfMergeName"
                 placeholder="e.g. Fox Twins"
                 style="flex:1;background:rgba(0,0,0,0.35);border:1px solid rgba(255,255,255,0.12);border-radius:6px;color:var(--text);padding:8px 12px;font-size:13px;font-family:var(--mono);outline:none">
        </div>
        <div id="perfMergeDestRow" style="display:flex;gap:10px;align-items:center;margin-bottom:14px">
          <label style="font-size:11px;color:var(--dim);font-family:var(--mono);min-width:90px">Destination</label>
          <select id="perfMergeDest"
                  style="flex:1;background:rgba(0,0,0,0.35);border:1px solid rgba(255,255,255,0.12);border-radius:6px;color:var(--text);padding:8px 12px;font-size:13px;font-family:var(--mono);outline:none"></select>
        </div>

        <input type="search" id="perfMergeSearch"
               placeholder="Filter library performers…"
               autocomplete="off"
               style="background:rgba(0,0,0,0.35);border:1px solid rgba(255,255,255,0.12);border-radius:6px;color:var(--text);padding:8px 12px;font-size:13px;font-family:var(--mono);outline:none;margin-bottom:8px">
        <div id="perfMergeStatus" style="font-size:11px;color:var(--dim);margin-bottom:6px"></div>
        <div id="perfMergeResults"
             style="flex:1 1 0;min-height:240px;overflow-y:auto;display:flex;flex-direction:column;gap:4px;border:1px solid rgba(255,255,255,0.06);border-radius:6px;padding:6px"></div>

        <div class="ts-link-modal-foot" style="display:flex;justify-content:space-between;align-items:center;margin-top:14px;gap:12px">
          <div id="perfMergeSelected" style="font-size:11px;color:var(--dim);font-family:var(--mono)"></div>
          <div style="display:flex;gap:8px">
            <button type="button" id="perfMergeCancelBtn"
                    style="background:transparent;border:1px solid rgba(255,255,255,0.15);color:var(--dim);padding:8px 16px;border-radius:6px;font-size:11px;font-family:var(--mono);cursor:pointer;text-transform:uppercase;letter-spacing:0.04em">Cancel</button>
            <button type="button" id="perfMergeConfirmBtn" disabled
                    style="background:rgba(var(--brand-purple-rgb),0.4);border:1px solid rgba(var(--brand-purple-rgb),0.65);color:var(--accent);padding:8px 16px;border-radius:6px;font-size:11px;font-family:var(--mono);cursor:pointer;text-transform:uppercase;letter-spacing:0.04em;opacity:0.55">Merge</button>
          </div>
        </div>
      </div>`;
    document.body.appendChild(div);
    div.addEventListener('click', (e) => {
      if (e.target === div) _closeMergeModal();
    });
    div.querySelector('#perfMergeCancelBtn').addEventListener('click', _closeMergeModal);
    const search = div.querySelector('#perfMergeSearch');
    let debTimer = null;
    search.addEventListener('input', () => {
      if (debTimer) clearTimeout(debTimer);
      debTimer = setTimeout(() => _renderMergeList(search.value), 100);
    });
    search.addEventListener('keydown', (e) => { if (e.key === 'Escape') _closeMergeModal(); });
    div.querySelector('#perfMergeName').addEventListener('input', _refreshMergeConfirmState);
    div.querySelector('#perfMergeConfirmBtn').addEventListener('click', _submitMerge);
  }

  // Modal session state — only valid while the modal is open.
  const _mergeState = {
    mode: 'merge',          // 'merge' or 'group-add'
    ctx:  null,
    selected: new Set(),
    items: [],
  };

  function _closeMergeModal() {
    const div = document.getElementById('performerMergeModal');
    if (!div) return;
    div.classList.remove('open');
    div.style.display = '';
    _mergeState.ctx = null;
    _mergeState.selected.clear();
    _mergeState.items = [];
  }

  async function openPerformerMergeModal(ctx, mode) {
    if (!ctx || ctx.kind !== 'performer' || ctx.id <= 0) return false;
    _ensureMergeModal();
    const div = document.getElementById('performerMergeModal');
    _mergeState.mode = mode;
    _mergeState.ctx  = ctx;
    _mergeState.selected.clear();
    // Title + subline change with the mode. group-add hides the name/
    // dest rows since the group already has both.
    const title  = div.querySelector('#perfMergeTitle');
    const sub    = div.querySelector('#perfMergeSub');
    const nameRow= div.querySelector('#perfMergeNameRow');
    const destRow= div.querySelector('#perfMergeDestRow');
    const nameInput = div.querySelector('#perfMergeName');
    const destSel   = div.querySelector('#perfMergeDest');
    const confirmBtn= div.querySelector('#perfMergeConfirmBtn');
    if (mode === 'group-add') {
      title.textContent = 'Add another performer';
      sub.innerHTML = 'Folds the picked performer(s) into <strong>' + esc(ctx.name)
        + '</strong> — videos move into this group folder, original rows are removed.';
      nameRow.style.display = 'none';
      destRow.style.display = 'none';
      confirmBtn.textContent = 'Add';
    } else {
      title.textContent = 'Merge into group';
      sub.innerHTML = 'Creates a new group folder containing <strong>' + esc(ctx.name)
        + '</strong> plus the picked performer(s). Videos move into the group, original folders are removed.';
      nameRow.style.display = '';
      destRow.style.display = '';
      confirmBtn.textContent = 'Merge';
      nameInput.value = '';
    }
    // Populate destination dropdown from configured performer roots.
    if (mode === 'merge') {
      destSel.innerHTML = '<option>Loading…</option>';
      try {
        const r = await fetch('/api/metadata/dirs?kind=performer', { credentials: 'same-origin' });
        const d = await r.json().catch(() => ({}));
        const dirs = (d && d.dirs) || [];
        if (!dirs.length) {
          destSel.innerHTML = '<option value="">No performer roots configured</option>';
        } else {
          destSel.innerHTML = dirs.map((d) =>
            `<option value="${esc(d.path)}">${esc(d.label || d.path)} — ${esc(d.path)}</option>`
          ).join('');
        }
      } catch (_) {
        destSel.innerHTML = '<option value="">Failed to load dirs</option>';
      }
    }

    div.querySelector('#perfMergeSearch').value = '';
    div.querySelector('#perfMergeStatus').textContent = 'Loading library…';
    div.querySelector('#perfMergeResults').innerHTML = '';
    div.style.display = 'flex';
    div.classList.add('open');
    try {
      const perfs = await _mergeFetchLibraryPerformers();
      _mergeState.items = perfs.filter((p) => Number(p.id) !== Number(ctx.id));
      _renderMergeList('');
    } catch (e) {
      div.querySelector('#perfMergeStatus').textContent = 'Failed to load library performers';
    }
    setTimeout(() => div.querySelector('#perfMergeSearch').focus(), 0);
    // Return true so the dispatcher doesn't trigger onDone() and close
    // the popup — the modal owns the user's next move from here.
    return true;
  }

  function _renderMergeList(filterText) {
    const status  = document.getElementById('perfMergeStatus');
    const results = document.getElementById('perfMergeResults');
    if (!status || !results) return;
    const q = String(filterText || '').toLowerCase().trim();
    const matches = _mergeState.items.filter((p) => {
      if (!q) return true;
      const name = (p.folder_name || p.name || '').toLowerCase();
      return name.includes(q);
    });
    if (!matches.length) {
      status.textContent = q ? 'No matches' : 'Library is empty';
      results.innerHTML = '';
      _refreshMergeConfirmState();
      return;
    }
    status.textContent = `${matches.length} performer${matches.length === 1 ? '' : 's'}`
      + (_mergeState.selected.size ? ` · ${_mergeState.selected.size} selected` : '');
    results.innerHTML = matches.map((p) => {
      const selected = _mergeState.selected.has(p.id);
      const name = p.folder_name || p.name || '(unnamed)';
      const path = p.path || '';
      const groupBadge = p.is_group
        ? '<span style="font-size:9px;background:rgba(var(--brand-purple-rgb),0.25);color:var(--accent);padding:1px 5px;border-radius:3px;margin-left:6px">GROUP</span>'
        : '';
      return `<button type="button" class="perf-merge-row" data-id="${p.id}"
              style="display:flex;align-items:center;gap:10px;padding:8px 10px;border-radius:6px;background:${selected ? 'rgba(var(--brand-purple-rgb),0.18)' : 'rgba(255,255,255,0.02)'};border:1px solid ${selected ? 'rgba(var(--brand-purple-rgb),0.6)' : 'rgba(255,255,255,0.06)'};cursor:pointer;text-align:left;color:var(--text);font-family:inherit">
        <div style="width:18px;height:18px;border:1px solid ${selected ? 'rgba(var(--brand-purple-rgb),0.85)' : 'rgba(255,255,255,0.2)'};border-radius:3px;display:flex;align-items:center;justify-content:center;flex-shrink:0;background:${selected ? 'rgba(var(--brand-purple-rgb),0.4)' : 'transparent'}">
          ${selected ? '<i class="fa-solid fa-check" style="font-size:10px;color:var(--accent)"></i>' : ''}
        </div>
        <div style="flex:1;min-width:0">
          <div style="font-size:13px;color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(name)}${groupBadge}</div>
          <div style="font-size:10px;color:var(--dim);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(path)}</div>
        </div>
      </button>`;
    }).join('');
    results.querySelectorAll('.perf-merge-row').forEach((btn) => {
      btn.addEventListener('click', () => {
        const id = Number(btn.dataset.id);
        if (_mergeState.selected.has(id)) _mergeState.selected.delete(id);
        else _mergeState.selected.add(id);
        _renderMergeList(document.getElementById('perfMergeSearch').value);
      });
    });
    _refreshMergeConfirmState();
  }

  function _refreshMergeConfirmState() {
    const btn = document.getElementById('perfMergeConfirmBtn');
    const sel = document.getElementById('perfMergeSelected');
    if (!btn || !sel) return;
    const n = _mergeState.selected.size;
    const name = (document.getElementById('perfMergeName')?.value || '').trim();
    const needName = _mergeState.mode === 'merge';
    const canSubmit = n > 0 && (!needName || name.length > 0);
    btn.disabled = !canSubmit;
    btn.style.opacity = canSubmit ? '1' : '0.55';
    btn.style.cursor  = canSubmit ? 'pointer' : 'not-allowed';
    if (n === 0) {
      sel.textContent = 'Pick at least one performer';
    } else if (needName && !name) {
      sel.textContent = `${n} selected · enter a group name to continue`;
    } else {
      sel.textContent = `${n} selected`;
    }
  }

  async function _submitMerge() {
    if (!_mergeState.ctx) return;
    const ctx = _mergeState.ctx;
    const ids = Array.from(_mergeState.selected);
    if (!ids.length) return;
    const btn = document.getElementById('perfMergeConfirmBtn');
    btn.disabled = true;
    btn.style.opacity = '0.55';
    btn.textContent = 'Working…';
    try {
      if (_mergeState.mode === 'merge') {
        const name = (document.getElementById('perfMergeName').value || '').trim();
        const dest = document.getElementById('perfMergeDest').value || '';
        const r = await fetch('/api/performers/merge', {
          method: 'POST',
          credentials: 'same-origin',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            primary_row_id:    ctx.id,
            secondary_row_ids: ids,
            group_name:        name,
            dest_dir:          dest,
          }),
        });
        const d = await r.json().catch(() => ({}));
        if (!r.ok) { toast(d.error || 'Merge failed', { kind: 'error' }); btn.disabled = false; btn.style.opacity = '1'; btn.textContent = 'Merge'; return; }
        toast(`Merged ${ids.length + 1} performers into ${name}`);
      } else {
        const r = await fetch('/api/performers/group-add', {
          method: 'POST',
          credentials: 'same-origin',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ row_id: ctx.id, secondary_row_ids: ids }),
        });
        const d = await r.json().catch(() => ({}));
        if (!r.ok) { toast(d.error || 'Add failed', { kind: 'error' }); btn.disabled = false; btn.style.opacity = '1'; btn.textContent = 'Add'; return; }
        toast(`Added ${ids.length} performer${ids.length === 1 ? '' : 's'} to ${ctx.name}`);
      }
      _mergeLibraryCache = null;  // invalidate so re-open sees the changes
      _closeMergeModal();
      if (typeof window.refreshPerformerPopup === 'function'
          && window._performerPopupActiveId === ctx.id) {
        window.refreshPerformerPopup();
      }
    } catch (e) {
      toast(e.message || 'Failed', { kind: 'error' });
      btn.disabled = false;
      btn.style.opacity = '1';
      btn.textContent = _mergeState.mode === 'merge' ? 'Merge' : 'Add';
    }
  }

  async function ungroupPerformer(ctx) {
    if (!ctx || ctx.kind !== 'performer' || ctx.id <= 0) return false;
    if (!confirm(
      `Ungroup "${ctx.name}"?\n\n`
      + `Every video file in the group folder moves back to the queue source `
      + `directory and the pipeline re-files them as individual performers. `
      + `The group folder and library row are removed.\n\n`
      + `This cannot be undone.`
    )) return false;
    try {
      const r = await fetch('/api/performers/ungroup', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ row_id: ctx.id }),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) {
        toast(d.error || 'Ungroup failed', { kind: 'error' });
        return false;
      }
      toast(`Ungrouped ${ctx.name} — ${d.moved_videos || 0} file(s) sent back to queue`);
      _mergeLibraryCache = null;
      // The row is gone — close the performer popup.
      if (typeof window.closePerformerPopup === 'function') window.closePerformerPopup();
      return true;
    } catch (e) {
      toast(e.message || 'Ungroup failed', { kind: 'error' });
      return false;
    }
  }

  /** Open the studio popup's "Link other studios" search modal. The
   * modal lives in studio-popup.js; this just dispatches to it. The
   * picker hits TPDB/StashDB/FansDB/JAVStash via /api/metadata/search
   * and POSTs each pick to /api/favourites/group-add-link with the full
   * {source, id, name, image} payload — scenes whose studio carries any
   * linked ID then auto-route to this folder via the crosswalk lookup
   * in favourite_find_studio_folder_by_crosswalk. */
  async function editStudioAliases(ctx) {
    if (!ctx || ctx.kind !== 'studio' || ctx.id <= 0) return false;
    if (typeof window.openStudioLinkSearchModal !== 'function') {
      toast('Studio link search not loaded', { kind: 'error' });
      return false;
    }
    window.openStudioLinkSearchModal(ctx.id, ctx.name || '');
    return true;
  }

  /** Open the performer popup's "Link DB profile" search modal. Mirrors
   * the studio "Link other studios" picker: searches TPDB/StashDB/
   * FansDB/JAVStash for performers and appends each picked id to the
   * row's group_ids_json. The popup carousel then fans out across the
   * new id on its next render and /queue scenes carrying any of those
   * ids file under this group folder via favourite_find_performer_dir_via_library_index. */
  async function linkPerformerDbProfile(ctx) {
    if (!ctx || ctx.kind !== 'performer' || ctx.id <= 0) return false;
    if (typeof window.openPerformerLinkSearchModal !== 'function') {
      toast('Performer link search not loaded', { kind: 'error' });
      return false;
    }
    window.openPerformerLinkSearchModal(ctx.id, ctx.name || '');
    return true;
  }

  function closeMenu(menuEl) {
    if (menuEl) menuEl.classList.remove('open');
  }

  function buildMenuItems(ctx) {
    const items = [];
    if (ctx.kind === 'performer' && ctx.id > 0) {
      items.push(
        { action: 'refresh-images', label: 'Refresh images', icon: 'fa-arrows-rotate' },
        { action: 'enrich', label: 'Run enrichment', icon: 'fa-wand-magic-sparkles' },
        { action: 'rename', label: 'Rename performer', icon: 'fa-i-cursor' },
      );
      if (ctx.isGroup) {
        // Already a group: offer to add more members or to dissolve.
        // "Link DB profile" mirrors the studio popup's "Link other
        // studios" — search the source DBs and append the picked
        // performer's id to group_ids_json, so the popup carousel
        // pulls scenes from that profile too and a /queue file step
        // hitting the same id routes here automatically.
        // Ungroup is destructive (deletes the row + folder, sends
        // every video back to the queue) so the confirmation step
        // lives in the handler.
        items.push(
          { action: 'group-add',  label: 'Add another performer…', icon: 'fa-user-plus' },
          { action: 'group-link', label: 'Link DB profile…',       icon: 'fa-link' },
          { action: 'ungroup',    label: 'Ungroup',                icon: 'fa-user-minus' },
        );
      } else {
        // Solo row: launch the merge picker to fold this performer
        // into a new group folder alongside one or more others.
        items.push(
          { action: 'merge', label: 'Merge into group…', icon: 'fa-object-group' },
        );
      }
    }
    if (ctx.kind === 'studio' && ctx.id > 0) {
      // Sibling-site lumping: a single local folder for a network that
      // publishes under several names (e.g. one folder for "Pure Mature"
      // + "Mature NL"). Aliases here feed find_studio_dir's name index
      // so the /queue filing step lands every variant on this folder.
      items.push(
        { action: 'studio-aliases', label: 'Link other studios', icon: 'fa-link' },
      );
    }
    items.push(
      { action: 'remove', label: 'Remove from library', icon: 'fa-bookmark' },
      { action: 'delete', label: 'Delete from disk', icon: 'fa-trash', danger: true },
    );
    if (ctx.canChangeDirectory) {
      const insertAt = items.findIndex((it) => it.action === 'delete');
      items.splice(insertAt >= 0 ? insertAt : items.length, 0, {
        action: 'move',
        label: 'Change directory',
        icon: 'fa-folder-tree',
      });
    }
    return items;
  }

  function mountMenuButton(container, ctx, onDone) {
    if (!container || !ctx) return null;
    const wrap = document.createElement('div');
    wrap.className = 'lib-entity-menu-wrap';
    wrap.innerHTML = `
      <button type="button" class="lib-entity-menu-btn" title="Actions" aria-label="Entity actions" aria-haspopup="true">
        <i class="fa-solid fa-bars"></i>
      </button>
      <div class="lib-entity-menu-dropdown" role="menu"></div>`;
    const btn = wrap.querySelector('.lib-entity-menu-btn');
    const menu = wrap.querySelector('.lib-entity-menu-dropdown');
    menu.innerHTML = buildMenuItems(ctx).map((it) =>
      `<button type="button" class="lib-entity-menu-item${it.danger ? ' is-danger' : ''}" ` +
      `data-action="${esc(it.action)}" role="menuitem">` +
      `<i class="fa-solid ${esc(it.icon)}"></i><span>${esc(it.label)}</span></button>`
    ).join('');

    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      const open = menu.classList.toggle('open');
      if (open) {
        document.querySelectorAll('.lib-entity-menu-dropdown.open').forEach((m) => {
          if (m !== menu) m.classList.remove('open');
        });
      }
    });

    menu.querySelectorAll('.lib-entity-menu-item').forEach((item) => {
      item.addEventListener('click', async (e) => {
        e.stopPropagation();
        closeMenu(menu);
        const action = item.getAttribute('data-action');
        try {
          let ok = false;
          if (action === 'refresh-images') ok = await refreshPerformerImages(ctx, item);
          else if (action === 'enrich') ok = await enrichPerformer(ctx);
          else if (action === 'rename') ok = await renamePerformer(ctx);
          else if (action === 'studio-aliases') ok = await editStudioAliases(ctx);
          else if (action === 'merge')      ok = await openPerformerMergeModal(ctx, 'merge');
          else if (action === 'group-add')  ok = await openPerformerMergeModal(ctx, 'group-add');
          else if (action === 'group-link') ok = await linkPerformerDbProfile(ctx);
          else if (action === 'ungroup')    ok = await ungroupPerformer(ctx);
          else if (action === 'remove') ok = await removeFromLibrary(ctx);
          else if (action === 'delete') ok = await deleteFromDisk(ctx);
          else if (action === 'move') ok = await changeDirectory(ctx);
          if (ok
              && action !== 'enrich'
              && action !== 'refresh-images'
              && action !== 'studio-aliases'
              && action !== 'merge'
              && action !== 'group-add'
              && action !== 'group-link'
              && action !== 'ungroup'
              && typeof onDone === 'function') onDone(action);
          // studio-aliases / merge / group-add / group-link trigger an
          // in-place popup refresh from their own handlers (so the user
          // stays on the performer they were editing) — onDone() would
          // close it. ungroup destroys the row entirely, so it closes
          // the popup itself.
        } catch (err) {
          toast(err.message || 'Action failed', { kind: 'error' });
        }
      });
    });

    container.appendChild(wrap);
    return wrap;
  }

  document.addEventListener('click', (e) => {
    if (!e.target.closest('.lib-entity-menu-wrap')) {
      document.querySelectorAll('.lib-entity-menu-dropdown.open').forEach((m) => m.classList.remove('open'));
    }
  });

  window.LibEntityActions = {
    ctxFromRow,
    mountMenuButton,
    enrichPerformer,
    refreshPerformerImages,
    removeFromLibrary,
    deleteFromDisk,
    changeDirectory,
    renamePerformer,
    editStudioAliases,
    linkPerformerDbProfile,
    openPerformerMergeModal,
    ungroupPerformer,
  };
})();
