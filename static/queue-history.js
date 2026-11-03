/* Shared, accessible presentation for file history and recoverable queue removals. */
(() => {
  let dialog, body, controller, generation = 0;
  const node = (tag, text, className) => {
    const element = document.createElement(tag);
    if (text !== undefined) element.textContent = text;
    if (className) element.className = className;
    return element;
  };
  function open(title) {
    controller?.abort(); controller = new AbortController();
    const token = ++generation;
    if (!dialog) {
      dialog = node('dialog', undefined, 'queue-details');
      const header = node('div', undefined, 'queue-details-header');
      const heading = node('h2'); heading.id = 'queue-details-title';
      const close = node('button', '×', 'btn-secondary');
      close.type = 'button'; close.setAttribute('aria-label', 'Close');
      close.addEventListener('click', () => dialog.close());
      header.append(heading, close);
      body = node('div', undefined, 'queue-details-body');
      body.setAttribute('aria-live', 'polite');
      dialog.append(header, body); dialog.setAttribute('aria-labelledby', heading.id);
      dialog.addEventListener('close', () => { controller?.abort(); generation++; });
      document.body.append(dialog);
    }
    dialog.querySelector('h2').textContent = title;
    body.replaceChildren(node('p', 'Loading…', 'queue-details-note'));
    if (!dialog.open) dialog.showModal();
    return token;
  }
  async function request(url, options = {}) {
    const response = await fetch(url, {...options, credentials:'same-origin', signal:controller.signal});
    const data = await response.json();
    if (!response.ok || data.error) throw new Error(data.error || data.detail || 'Request failed');
    return data;
  }
  function error(error, token) {
    if (error.name === 'AbortError' || generation !== token) return;
    body.append(node('p', error.message || 'Request failed', 'queue-details-error'));
  }
  const size = bytes => `${(Number(bytes || 0) / 1024 / 1024).toFixed(1)} MB retained`;
  const labels = {'registered':'Added to queue', 'queue.rename':'Renamed', 'queue.status':'Status changed',
    'queue.record_removed':'Queue record removed', 'file.destination':'Filed',
    'file.recovered_removal':'Moved to recovery', 'file.restored':'Restored', 'file.recovery_purged':'Permanently deleted from recovery'};
  async function recovery() {
    const token = open('Recover removed files');
    const list = node('ul', undefined, 'queue-details-list');
    const more = node('button', 'Load more files', 'btn-secondary queue-details-more');
    more.type = 'button'; more.hidden = true;
    let cursor = '';
    body.replaceChildren(node('p', 'Removed files are retained until you restore or permanently delete them. Their disk space is still in use.', 'queue-details-note'), list, more);
    function render(file) {
      const row = node('li', undefined, 'queue-details-row');
      const info = node('div', undefined, 'queue-details-info');
      info.append(node('p', file.filename, 'queue-details-name'), node('p', `${size(file.retained_bytes)} · ${file.created_at}`, 'queue-details-meta'));
      row.append(info);
      if (file.state === 'archived') {
        const restore = node('button', 'Restore', 'btn-secondary'); restore.type = 'button';
        restore.addEventListener('click', async () => {
          const buttons = [...row.querySelectorAll('button')]; buttons.forEach(button => button.disabled = true);
          restore.textContent = 'Restoring…';
          try {
            await request(`/api/queue/recovery/${encodeURIComponent(file.id)}/restore`, {method:'POST'});
            if (token !== generation) return;
            row.remove();
            if (!list.children.length && !cursor) list.append(node('li', 'Recovery is empty.', 'queue-details-note'));
            if (typeof queueMode !== 'undefined' && queueMode === 'movies' && typeof loadMovieQueue === 'function') loadMovieQueue({preserveView:true, force:true});
            else if (typeof loadQueue === 'function') loadQueue({preserveView:true, force:true});
            if (typeof loadQueueStats === 'function') loadQueueStats();
          } catch (exc) { error(exc, token); buttons.forEach(button => button.disabled = false); restore.textContent = 'Restore'; }
        });
        row.append(restore);
      } else {
        info.append(node('p', file.error || 'Operation needs review before restoration.', 'queue-details-error'));
      }
      if (file.state === 'archived' || file.state === 'purge_failed') {
        const purge = node('button', 'Delete permanently', 'btn-secondary'); purge.type = 'button';
        purge.addEventListener('click', async () => {
          if (!confirm(`Permanently delete all retained files for this entry?\n\n${file.filename}\n\nThis cannot be undone.`)) return;
          const buttons = [...row.querySelectorAll('button')]; buttons.forEach(button => button.disabled = true);
          try {
            await request(`/api/queue/recovery/${encodeURIComponent(file.id)}/purge`, {method:'POST'});
            if (token !== generation) return;
            row.remove();
            if (!list.children.length && !cursor) list.append(node('li', 'Recovery is empty.', 'queue-details-note'));
          } catch (exc) { error(exc, token); buttons.forEach(button => button.disabled = false); }
        });
        row.append(purge);
      }
      list.append(row);
    }
    async function load() {
      more.disabled = true;
      try {
        const data = await request('/api/queue/recovery?before=' + encodeURIComponent(cursor));
        if (token !== generation) return;
        cursor = data.next_cursor || '';
        data.files.forEach(render);
        if (!list.children.length) list.append(node('li', 'No files waiting for recovery.', 'queue-details-note'));
        more.hidden = !cursor;
      } catch (exc) { error(exc, token); }
      finally { more.disabled = false; }
    }
    more.addEventListener('click', load);
    await load();
  }
  async function history(fileId) {
    if (!fileId) return;
    const token = open('File history');
    let after = 0;
    const list = node('ul', undefined, 'queue-details-list');
    const more = node('button', 'Load earlier activity', 'btn-secondary queue-details-more');
    more.type = 'button';
    async function load() {
      more.disabled = true;
      try {
        const data = await request(`/api/files/${encodeURIComponent(fileId)}/history?after=${after}&limit=100`);
        if (token !== generation) return;
        if (!after) {
          const label = data.file.original_known ? 'Original name' : 'First observed name';
          body.replaceChildren(node('p', `${label}: ${data.file.original_path}`, 'queue-details-note'));
          if (data.job) body.append(node('p', `Processing: ${data.job.phase}${data.job.error ? ' · ' + data.job.error : ''}`, 'queue-details-note'));
          body.append(list, more);
        }
        for (const event of data.events) {
          const row = node('li', undefined, 'queue-details-row');
          const info = node('div', undefined, 'queue-details-info');
          info.append(node('p', labels[event.kind] || event.kind, 'queue-details-name'));
          const detail = event.kind === 'queue.status' ? event.detail : event.kind === 'file.recovered_removal' ? event.old_path : event.new_path || event.old_path || '';
          info.append(node('p', event.created_at + (detail ? ' · ' + detail : ''), 'queue-details-meta'));
          row.append(info); list.append(row); after = event.id;
        }
        if (!list.children.length) list.append(node('li', 'No recorded activity yet.', 'queue-details-note'));
        more.hidden = data.events.length < 100;
      } catch (exc) { error(exc, token); }
      finally { more.disabled = false; }
    }
    more.textContent = 'Load more activity'; more.addEventListener('click', load);
    await load();
  }
  window.QueueDetails = {history, recovery};
})();
