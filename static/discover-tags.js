/* TPDB catalogue picker. Draft edits are saved together on Apply. */
(function () {
  let dialog, selected = new Map(), excluded = new Map(), timer, request = 0;
  function paint() {
    for (const [map, target, count, label] of [[selected, 'selected', 'count', 'included'], [excluded, 'excluded', 'excluded-count', 'excluded']]) {
      const list = dialog.querySelector('[data-' + target + ']'); list.replaceChildren();
      dialog.querySelector('[data-' + count + ']').textContent = map.size + ' / 20';
      for (const tag of map.values()) {
        const button = document.createElement('button'); button.type = 'button'; button.className = 'btn-secondary';
        button.textContent = tag.name + ' ×'; button.setAttribute('aria-label', 'Remove ' + label + ' tag ' + tag.name);
        button.onclick = () => { map.delete(String(tag.id)); paint(); }; list.append(button);
      }
      if (!map.size) list.textContent = 'No ' + label + ' tags.';
    }
  }

  window.openTpdbTagPicker = async function () {
    if (!dialog) {
      dialog = document.createElement('dialog');
      dialog.className = 'tpdb-tags-dialog';
      dialog.setAttribute('aria-labelledby', 'tpdbTagsTitle');
      dialog.innerHTML = `<header><h2 id="tpdbTagsTitle"><i class="fa-solid fa-tags" aria-hidden="true"></i> TPDB Tags</h2><p>Discover scenes matching all selected tags.</p></header><div class="tpdb-tags-body"><section><label for="tpdbTagsSearch">Find a TPDB tag</label><select aria-label="Add tags to" id="tpdbTagTarget"><option value="include">Include tags</option><option value="exclude">Exclude tags</option></select><input id="tpdbTagsSearch" type="search" autocomplete="off" placeholder="Start typing a tag name…"><div data-results aria-live="polite"></div></section><section class="tpdb-tags-selection"><h3 title="Choose up to 20. Click a selected tag to remove it.">Selected tags <span data-count>0 / 20</span></h3><div data-selected></div></section><section class="tpdb-tags-selection"><h3 title="Hide scenes carrying any of these tags.">Excluded tags <span data-excluded-count>0 / 20</span></h3><div data-excluded></div></section><section class="tpdb-tags-selection"><label class="tpdb-tags-option"><input type="checkbox" id="tpdbExcludeLibraryPerformers"> Exclude scenes featuring performers in my library</label><label class="tpdb-tags-option"><input type="checkbox" id="tpdbRequirePhoto"> Exclude scenes without photos</label></section><p data-status role="status"></p></div><footer><button type="button" class="btn-secondary" data-cancel>Cancel</button><button type="button" class="btn-primary" data-apply>Apply</button></footer>`;
      document.body.append(dialog);
      dialog.querySelector('[data-cancel]').onclick = () => dialog.close();
      dialog.addEventListener('close', () => { ++request; clearTimeout(timer); window.tsCancelModalLoads(dialog); });
      dialog.querySelector('input').addEventListener('input', event => {
        clearTimeout(timer); const seq = ++request, query = event.target.value.trim();
        const results = dialog.querySelector('[data-results]'); results.replaceChildren();
        if (query.length < 2) return;
        timer = setTimeout(async () => {
          results.textContent = 'Searching TPDB…';
          try {
            const r = await window.tsModalFetch(dialog, '/api/discover/tpdb-tags?q=' + encodeURIComponent(query));
            if (!r.ok) throw new Error();
            const data = await r.json();
            if (seq !== request || !dialog.open) return;
            results.replaceChildren();
            for (const tag of data.results || []) {
              const button = document.createElement('button');
              button.type = 'button'; button.className = 'btn-secondary'; button.textContent = tag.name;
              button.onclick = () => {
                const map = dialog.querySelector('#tpdbTagTarget').value === 'exclude' ? excluded : selected;
                const other = map === selected ? excluded : selected;
                if (map.size >= 20 && !map.has(String(tag.id))) {
                  dialog.querySelector('[data-status]').textContent = 'You can select up to 20 tags.'; return;
                }
                other.delete(String(tag.id));
                map.set(String(tag.id), {id: String(tag.id), name: tag.name}); paint();
              };
              results.append(button);
            }
            if (!results.children.length) results.textContent = 'No TPDB tags found. Check the name and your TPDB connection.';
          } catch (_) { if (seq === request) results.textContent = 'Could not search TPDB. Try again.'; }
        }, 300);
      });
      dialog.querySelector('[data-apply]').onclick = async event => {
        const button = event.currentTarget; button.disabled = true;
        const status = dialog.querySelector('[data-status]'); status.textContent = 'Saving…';
        dialog.close();
        try {
          const r = await fetch('/api/discover/tpdb-tags', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({tags:[...selected.values()], excluded_tags:[...excluded.values()], exclude_library_performers:dialog.querySelector("#tpdbExcludeLibraryPerformers").checked, require_photo:dialog.querySelector("#tpdbRequirePhoto").checked})});
          const data = await r.json(); if (!r.ok) throw new Error(data.error || 'Save failed');
          dialog.close();
          await window.refreshScenesFeed();
        } catch (e) {
          status.textContent = e.message;
          if (!dialog.open) dialog.showModal();
        }
        finally { button.disabled = false; }
      };
    }
    dialog.querySelector('[data-status]').textContent = 'Loading saved tags…';
    dialog.querySelector('input').value = '';
    dialog.querySelector('input').disabled = true;
    selected = new Map(); excluded = new Map(); paint();
    dialog.querySelector('[data-results]').replaceChildren();
    dialog.querySelector('[data-apply]').disabled = true;
    dialog.showModal();
    const seq = ++request;
    try {
      const r = await window.tsModalFetch(dialog, '/api/discover/tpdb-tags'); if (!r.ok) throw new Error();
      const data = await r.json(); if (seq !== request || !dialog.open) return;
      dialog.querySelector('#tpdbExcludeLibraryPerformers').checked = !!data.exclude_library_performers;
      dialog.querySelector('#tpdbRequirePhoto').checked = !!data.require_photo;
      selected = new Map((data.tags || []).map(t => [String(t.id), t]));
      excluded = new Map((data.excluded_tags || []).map(t => [String(t.id), t])); paint();
      dialog.querySelector('[data-status]').textContent = '';
      dialog.querySelector('[data-apply]').disabled = false;
      dialog.querySelector('input').disabled = false;
      dialog.querySelector('input').focus();
    } catch (_) { dialog.querySelector('[data-status]').textContent = 'Could not load saved tags. Close and try again.'; }
  };
})();
