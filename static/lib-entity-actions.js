/* Library entity actions — remove from app, delete from disk, move performer folder.
 * Used by /library (favourites.html) and performer-popup.js. */
(function () {
  if (window.LibEntityActions) return;

  const esc = (s) => String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
  const toast = (msg) => (window.toast ? window.toast(msg) : alert(msg));

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
    toast('Removed from library');
    return true;
  }

  async function deleteFromDisk(ctx) {
    if (!ctx || !confirmDeleteDisk(ctx)) return false;
    const body = { id: ctx.id };
    if (ctx.viceId) body.vice_id = ctx.viceId;
    await postJson('/api/library/entity/delete-from-disk', body);
    toast('Deleted from disk and library');
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
            hint.textContent = 'No other performer directories are configured in Settings → Directories.';
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
          toast('Folder moved');
          done(true);
        } catch (e) {
          toast(e.message || 'Move failed');
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

  async function enrichPerformer(ctx) {
    if (!ctx || ctx.kind !== 'performer' || ctx.id <= 0) return false;
    await postJson('/api/performers/enrich', { row_id: ctx.id });
    toast('Enrichment started — links and images will update shortly');
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

  function closeMenu(menuEl) {
    if (menuEl) menuEl.classList.remove('open');
  }

  function buildMenuItems(ctx) {
    const items = [];
    if (ctx.kind === 'performer' && ctx.id > 0) {
      items.push(
        { action: 'refresh-images', label: 'Refresh images', icon: 'fa-arrows-rotate' },
        { action: 'enrich', label: 'Run enrichment', icon: 'fa-wand-magic-sparkles' },
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
          else if (action === 'remove') ok = await removeFromLibrary(ctx);
          else if (action === 'delete') ok = await deleteFromDisk(ctx);
          else if (action === 'move') ok = await changeDirectory(ctx);
          if (ok && action !== 'enrich' && action !== 'refresh-images' && typeof onDone === 'function') onDone(action);
        } catch (err) {
          toast(err.message || 'Action failed');
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
  };
})();
