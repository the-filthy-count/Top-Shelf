/* One review list for current files and versioned intended changes. */
window.QueueChanges = (() => {
  let offset = 0, timer, generation = 0, count = 0;
  const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  async function refresh() {
    const token = ++generation;
    const body = document.getElementById('queueChangesBody');
    try {
      const response = await fetch(`/api/queue/changes?offset=${offset}`, {credentials:'same-origin'});
      const data = await response.json();
      if (!response.ok) throw new Error(data.error || 'Unable to read changes');
      if (token !== generation) return;
      const rows = data.changes || []; count = rows.length;
      body.innerHTML = rows.length ? `<table style="width:100%;border-collapse:collapse;text-align:left"><thead><tr><th>File / current location</th><th>Intended state</th><th>Status</th><th>Actions</th></tr></thead><tbody>${rows.map(row => {
        const scene = row.desired.scene || {};
        const title = scene.title || row.desired.title || row.fields.title || '';
        const destination = row.desired.destination || row.result.destination || '';
        return `<tr><td style="padding:12px 8px;border-top:1px solid #555;overflow-wrap:anywhere;max-width:360px">${escape(row.current_path)}<small style="display:block">${escape(row.fields.performers?.join(', ') || '')}</small></td>
        <td style="padding:12px 8px;border-top:1px solid #555;overflow-wrap:anywhere;max-width:360px">${escape(title)}<small style="display:block">${escape(destination || row.action || 'Awaiting an accepted match')}</small></td>
        <td style="padding:12px 8px;border-top:1px solid #555">${escape(row.state)}<small style="display:block">${escape(row.error || '')}</small></td>
        <td style="padding:12px 8px;border-top:1px solid #555" data-id="${escape(row.file_id)}" data-revision="${row.revision}" data-filename="${escape(row.filename)}">
          ${row.queue_table === 'processed_files' && !['filed','removed'].includes(row.observed_state) ? '<button class="btn-secondary" data-action="edit" title="Edit file details"><i class="fa-solid fa-pen-to-square"></i></button>' : ''}
          ${['failed','blocked'].includes(row.state) ? '<button class="btn-secondary" data-action="retry" title="Retry saved change"><i class="fa-solid fa-rotate-right"></i></button>' : ''}
          ${['pending','draft','failed','blocked'].includes(row.state) && row.revision ? '<button class="btn-secondary" data-action="cancel" title="Cancel saved change"><i class="fa-solid fa-xmark"></i></button>' : ''}
          <button class="btn-secondary" data-action="history" title="File history"><i class="fa-solid fa-clock-rotate-left"></i></button>
        </td></tr>`;
      }).join('')}</tbody></table>` : '<p>No files on this page.</p>';
      body.querySelectorAll('[data-action]').forEach(button => button.addEventListener('click',async () => {
        const {id,revision,filename} = button.parentElement.dataset;
        if (button.dataset.action === 'edit') { window.openQueueSearchDetails(filename,{file_id:id});return; }
        if (button.dataset.action === 'history') { window.QueueDetails.history(id); return; }
        button.disabled = true;
        try {
          const response = await fetch(`/api/queue/changes/${encodeURIComponent(id)}/${button.dataset.action}`, {method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},body:JSON.stringify({revision:Number(revision)})});
          const result = await response.json();
          if (!response.ok) throw new Error(result.error || 'Change failed');
          await refresh();
        } catch (error) { body.prepend(Object.assign(document.createElement('p'),{textContent:error.message}));button.disabled=false; }
      }));
    } catch (error) { if (token === generation) body.textContent = error.message; }
  }
  function toggle() {
    const panel = document.getElementById('queueChanges'); panel.hidden = !panel.hidden;
    clearInterval(timer);
    if (!panel.hidden) { refresh(); timer = setInterval(refresh,5000); }
    else generation++;
  }
  function page(direction) { if (direction > 0 && count < 100) return; offset = Math.max(0,offset+direction*100);refresh(); }
  return {toggle,refresh,page};
})();
