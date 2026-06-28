/* Standalone movie detail popup.
 *
 * Public API:
 *   window.openMoviePopup(tpdbId)   — fetch + open
 *   window.closeMoviePopup()        — close
 *
 * Self-contained: injects the #movieDetailOverlay markup and the
 * matching CSS on first call, so any page can `<script src=…>` this
 * module without needing surrounding chrome. /scenes and /discover
 * still have their own copy of the popup inside scenes-common.js;
 * this module is what /library (and any future surface) uses to open
 * a movie by its TPDB id.
 */
(function () {
  if (window._moviePopupModuleLoaded) return;
  window._moviePopupModuleLoaded = true;

  const ESC = (s) =>
    typeof esc === 'function'
      ? esc(s)
      : String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({
          '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
        }[c]));

  function _ensureCss() {
    if (document.getElementById('mp-shared-style')) return;
    const style = document.createElement('style');
    style.id = 'mp-shared-style';
    style.textContent = `
      .movie-detail-overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.8); z-index: 1100; align-items: center; justify-content: center; backdrop-filter: blur(8px); -webkit-backdrop-filter: blur(8px); }
      .movie-detail-overlay.open { display: flex; }
      .movie-detail-card {
        background: linear-gradient(160deg, rgba(var(--panel-hi-rgb),0.98) 0%, rgba(var(--panel-lo-rgb),0.95) 100%);
        border-radius: 6px; max-width: 1380px; width: 92vw; max-height: 88vh; overflow-y: auto;
        position: relative; box-shadow: 0 32px 80px rgba(0,0,0,0.6);
      }
      .movie-detail-bg { position: absolute; top: 0; left: 0; width: 100%; height: 300px; object-fit: cover; opacity: 0.15;
        mask-image: linear-gradient(to bottom, rgba(0,0,0,0.8) 0%, transparent 100%);
        -webkit-mask-image: linear-gradient(to bottom, rgba(0,0,0,0.8) 0%, transparent 100%);
        pointer-events: none; }
      .movie-detail-inner { position: relative; z-index: 1; padding: 28px; display: flex; gap: 28px; align-items: flex-start; }
      .movie-detail-poster-wrap {
        position: relative;
        width: 580px; min-width: 580px; max-width: 580px;
        aspect-ratio: 580 / 533;
        flex-shrink: 0;
        align-self: flex-start;
        background: transparent; box-shadow: none; border-radius: 0;
        overflow: visible;
      }
      .movie-detail-vhs-bg {
        position: absolute;
        top: 0; bottom: 0;
        right: 0;
        aspect-ratio: 1000 / 1813;
        z-index: 0;
        background: url('/static/img/vhs.webp') center / 100% 100% no-repeat;
        filter: hue-rotate(var(--vhs-hue, 0deg));
        pointer-events: none;
      }
      .movie-detail-studio-logo-rotated {
        position: absolute;
        top: 50%; left: 82%;
        transform: translate(-50%, -50%) rotate(-90deg);
        z-index: 1;
        max-width: 20%;
        max-height: 16%;
        object-fit: contain;
        pointer-events: none;
        opacity: 0.92;
      }
      .movie-detail-vhs-title-rotated {
        position: absolute;
        top: 50%; left: 70%;
        transform: translate(-50%, -50%) rotate(-90deg);
        z-index: 1;
        width: 30%;
        white-space: normal;
        line-height: 1.1;
        word-break: break-word;
        text-align: center;
        font-family: var(--secs);
        font-size: 12px;
        font-weight: 700;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        color: #000;
        pointer-events: none;
      }
      .movie-detail-poster-card {
        position: absolute;
        top: 0; bottom: 0;
        left: 0;
        aspect-ratio: 27 / 40;
        z-index: 2;
        border-radius: 4px;
        overflow: hidden;
        background: var(--raised);
        box-shadow:
          0 10px 28px rgba(0, 0, 0, 0.65),
          0 4px 12px rgba(0, 0, 0, 0.45),
          0 1px 0 rgba(255, 255, 255, 0.04) inset;
      }
      .movie-detail-poster-card .img-load { position: absolute; inset: 0; }
      .movie-detail-poster-card .img-load img { width: 100%; height: 100%; object-fit: cover; display: block; }
      .movie-detail-text { flex: 1; min-width: 0; }
      .movie-detail-title { font-family: var(--acid); font-size: 28px; color: var(--text); margin-bottom: 8px; }
      .movie-detail-meta-line { font-size: 12px; color: var(--dim); line-height: 2; }
      .movie-detail-meta-line span { color: var(--text); }
      .movie-detail-synopsis { font-size: 12px; color: var(--dim); line-height: 1.8; margin-top: 12px; max-height: 120px; overflow-y: auto; }
      .movie-detail-performers { margin-top: 12px; display: flex; flex-wrap: wrap; gap: 6px; }
      .movie-perf-tag { font-family: var(--mono); font-size: 10px; color: var(--text); background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1); border-radius: 3px; padding: 2px 8px; text-decoration: none; display: inline-block; }
      a.movie-perf-tag:hover { border-color: var(--accent); color: var(--accent); }
      .movie-detail-actions { display: flex; gap: 8px; margin-top: 18px; flex-wrap: wrap; }
      .movie-btn-action { padding: 8px 16px; border-radius: 4px; font-family: var(--mono); font-size: 12px; cursor: pointer; border: none; transition: all 0.15s; display: inline-flex; align-items: center; gap: 6px; text-decoration: none; }
      .movie-btn-prowlarr { background: var(--accent); color: #0c0c0e; }
      .movie-btn-prowlarr:hover { filter: brightness(1.1); }
      .movie-btn-link { background: rgba(255,255,255,0.06); color: var(--text); border: 1px solid var(--border); }
      .movie-btn-link:hover { border-color: var(--muted); }
      .movie-detail-close { position: absolute; top: 14px; right: 14px; z-index: 2; width: 30px; height: 30px; display: flex; align-items: center; justify-content: center; background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1); border-radius: 6px; color: var(--dim); font-size: 13px; cursor: pointer; transition: background 0.15s, border-color 0.15s, color 0.15s; }
      .movie-detail-close:hover { background: rgba(255,255,255,0.09); border-color: rgba(255,255,255,0.2); color: var(--text); }
      .movie-detail-scenes { padding: 16px 28px 24px; border-top: 1px solid var(--border); }
      .movie-detail-scenes-title { font-family: var(--acid); font-size: 18px; color: var(--text); margin-bottom: 10px; }
      .movie-detail-scene-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 10px; }
      .movie-detail-scene-card { border-radius: 4px; overflow: hidden; background: var(--raised); min-height: 154px; min-width: 0; }
      .movie-detail-scene-card .img-load { width: 100%; aspect-ratio: 16/9; position: relative; }
      .movie-detail-scene-card .img-load img.movie-detail-scene-thumb { position: absolute; inset: 0; width: 100%; height: 100%; object-fit: cover; display: block; }
      .movie-detail-scene-thumb-ph { width: 100%; aspect-ratio: 16/9; background: var(--raised); }
      .movie-detail-scene-info { padding: 6px 8px; font-size: 10px; color: var(--dim); }
    `;
    document.head.appendChild(style);
  }

  function _ensureOverlay() {
    let overlay = document.getElementById('movieDetailOverlay');
    if (overlay) return overlay;
    overlay = document.createElement('div');
    overlay.className = 'movie-detail-overlay';
    overlay.id = 'movieDetailOverlay';
    overlay.addEventListener('click', (e) => {
      if (e.target === overlay) closeMoviePopup();
    });
    overlay.innerHTML = `
      <div class="movie-detail-card">
        <button class="movie-detail-close" type="button" aria-label="Close"><i class="fa-solid fa-x"></i></button>
        <div id="movieDetailContent"></div>
      </div>
    `;
    document.body.appendChild(overlay);
    overlay.querySelector('.movie-detail-close')
      .addEventListener('click', closeMoviePopup);
    return overlay;
  }

  function _skeleton() {
    return `
      <div class="movie-detail-inner">
        <div class="movie-detail-poster-wrap"><div class="skeleton-box" style="position:absolute;inset:0"><span class="loader loader--tile" aria-hidden="true"></span></div></div>
        <div class="movie-detail-text">
          <div class="skeleton-line" style="width:45%;height:20px"></div>
          <div class="skeleton-line" style="width:72%"></div>
          <div class="skeleton-line" style="width:100%"></div>
          <div class="skeleton-line" style="width:96%"></div>
          <div class="skeleton-line" style="width:88%"></div>
          <div style="display:flex;gap:8px;margin-top:18px">
            <div class="skeleton-box" style="height:34px;width:170px;border-radius:4px"></div>
            <div class="skeleton-box" style="height:34px;width:110px;border-radius:4px"></div>
          </div>
        </div>
      </div>`;
  }

  function closeMoviePopup() {
    const overlay = document.getElementById('movieDetailOverlay');
    if (overlay) overlay.classList.remove('open');
  }
  window.closeMoviePopup = closeMoviePopup;

  function _genderBadge(g) {
    if (typeof window.genderBadge === 'function') return window.genderBadge(g);
    return '';
  }

  async function openMoviePopup(tpdbId) {
    if (!tpdbId) return;
    _ensureCss();
    const overlay = _ensureOverlay();
    if (typeof window.ensurePopupBundle === 'function') {
      window.ensurePopupBundle().catch(() => {});
    }
    const content = document.getElementById('movieDetailContent');
    content.innerHTML = _skeleton();
    overlay.removeAttribute('lang');
    overlay.classList.add('open');
    try {
      const r = await fetch('/api/movies/tpdb/' + encodeURIComponent(tpdbId), {
        credentials: 'same-origin',
      });
      const m = await r.json();
      if (m.error) {
        content.innerHTML = `<div class="empty" style="padding:48px;text-align:center">${ESC(m.error)}</div>`;
        return;
      }
      window._currentMovie = m;
      const bg = m.background ? `<img class="movie-detail-bg" src="${ESC(m.background)}" onerror="this.remove()" loading="lazy">` : '';
      const posterFallback = '/static/img/poster.webp';
      const detailPosterUrl = (m.poster && String(m.poster).trim()) ? m.poster : posterFallback;
      const bgUrl = (m.background && String(m.background).trim()) || '';
      const spreadDup = !!(bgUrl && detailPosterUrl === bgUrl);
      const dForce = (m.cover_art_is_spread || spreadDup) ? ' data-force-split="1"' : '';
      const overlaySrc = ESC((m.poster && String(m.poster).trim()) ? m.poster : posterFallback);
      const poster = `<div class="img-load"><span class="loader loader--tile" aria-hidden="true"></span><img class="movie-poster"${dForce} src="${ESC(detailPosterUrl)}" style="cursor:pointer;width:100%;height:100%;object-fit:cover;display:block" onclick="typeof openImageOverlay==='function'&&openImageOverlay('${overlaySrc}')" onload="this.closest('.img-load')?.classList.add('ready');typeof tsApplyMovieCoverSplit==='function'&&tsApplyMovieCoverSplit(this,'solo')" onerror="this.onerror=null;this.src='${posterFallback}';this.closest('.img-load')?.classList.add('ready');" loading="lazy"></div>`;
      const vhsHue = Math.floor(Math.random() * 360);
      const studioLogoHtml = (m.studio || m.title)
        ? `<img class="movie-detail-studio-logo-rotated" src="/api/studio-logo?name=${encodeURIComponent(m.studio || '')}&q=${encodeURIComponent(m.title || '')}" alt="" loading="lazy" onerror="this.remove()">`
        : '';
      const titleForVhs = m.title || '';
      const titleHtml = titleForVhs
        ? `<div class="movie-detail-vhs-title-rotated" aria-hidden="true">${ESC(titleForVhs)}</div>`
        : '';
      const posterFrame = `
        <div class="movie-detail-vhs-bg" style="--vhs-hue:${vhsHue}deg" aria-hidden="true"></div>
        ${titleHtml}
        ${studioLogoHtml}
        <div class="movie-detail-poster-card">${poster}</div>`;
      const meta = [];
      if (m.studio) meta.push(`Studio: <span>${ESC(m.studio)}</span>`);
      if (m.date) meta.push(`Released: <span>${ESC(m.date)}</span>`);
      if (m.duration) meta.push(`Duration: <span>${Math.round(m.duration / 60)} min</span>`);
      if (m.directors?.length) {
        const dirNames = m.directors
          .map((d) => ESC(typeof d === 'object' && d !== null ? (d.name || d.full_name || '') : String(d)))
          .filter(Boolean);
        if (dirNames.length) meta.push(`Director: <span>${dirNames.join(', ')}</span>`);
      }
      const perfLinks = (m.performer_links || [])
        .map((p) => {
          const nameRaw = p.name || '';
          if (!nameRaw) return '';
          const name = ESC(nameRaw);
          const badge = _genderBadge(p.gender);
          const attrs = (typeof window.performerLinkAttrs === 'function')
            ? window.performerLinkAttrs(nameRaw, { gender: p.gender, stashId: p.id || p.stash_id })
            : '';
          return `<span class="movie-perf-tag${attrs ? ' perf-name-link' : ''}"${attrs ? ' ' + attrs : ''}>${name}${badge}</span>`;
        })
        .join('');
      const perfs = perfLinks || (m.performers || [])
        .map((p) => `<span class="movie-perf-tag perf-name-link" data-performer-link data-name="${ESC(p)}">${ESC(p)}</span>`)
        .join('');
      const movieTags = Array.isArray(m.tags) ? m.tags : [];
      const movieTagsHtml = movieTags.length
        ? `<div class="movie-detail-tags" style="display:flex;flex-wrap:wrap;gap:6px;margin-top:10px">${movieTags
            .map((t) => `<span class="scene-card-tag-chip">${ESC(t)}</span>`).join('')}</div>`
        : '';
      let scenes = '';
      if (m.scenes?.length) {
        scenes = `<div class="movie-detail-scenes"><div class="movie-detail-scenes-title">Scenes (${m.scenes.length})</div><div class="movie-detail-scene-grid">${m.scenes
          .map((s) => `<div class="movie-detail-scene-card">${
            s.thumb
              ? `<div class="img-load"><span class="loader loader--tile" aria-hidden="true"></span><img class="movie-detail-scene-thumb" src="${ESC(s.thumb)}" loading="lazy" onload="this.closest('.img-load')?.classList.add('ready')" onerror="const w=this.closest('.img-load');if(w){this.outerHTML='<div class=\\'movie-detail-scene-thumb-ph\\'></div>';w.classList.add('ready');}"></div>`
              : '<div class="movie-detail-scene-thumb-ph"></div>'
          }<div class="movie-detail-scene-info">${ESC(s.title || '')}${s.date ? ' · ' + ESC(s.date) : ''}</div></div>`).join('')}</div></div>`;
      }
      const tmdbHref = m.tmdb_url || ('https://www.themoviedb.org/search/movie?query=' + encodeURIComponent(m.title || ''));
      const tpdbHref = m.url || '';
      content.innerHTML = `${bg}
        <div class="movie-detail-inner">
          <div class="movie-detail-poster-wrap">${posterFrame}</div>
          <div class="movie-detail-text">
            <div class="movie-detail-title" lang="" title="${ESC(m.title || '')}">${ESC(m.title || '')}</div>
            <div class="movie-detail-meta-line">${meta.join(' &middot; ')}</div>
            ${perfs ? `<div class="movie-detail-performers">${perfs}</div>` : ''}
            ${movieTagsHtml}
            ${m.synopsis ? `<div class="movie-detail-synopsis" lang="">${ESC(m.synopsis)}</div>` : ''}
            <div class="movie-detail-actions">
              <button class="movie-btn-action movie-btn-prowlarr" type="button" data-mp-prowlarr title="Search Prowlarr"><i class="fa-solid fa-magnifying-glass"></i> Prowlarr</button>
              <a class="movie-btn-action movie-btn-link" href="${ESC(tmdbHref)}" target="_blank" rel="noopener noreferrer"><img src="/static/logos/tmdb.webp" alt="TMDB" style="height:18px;width:auto;object-fit:contain;vertical-align:middle;opacity:0.9"></a>
              ${tpdbHref ? `<a class="movie-btn-action movie-btn-link" href="${ESC(tpdbHref)}" target="_blank" rel="noopener noreferrer"><img src="/static/logos/tpdb.webp" alt="TPDB" style="height:18px;width:auto;object-fit:contain;vertical-align:middle;opacity:0.9"></a>` : ''}
            </div>
          </div>
        </div>
        ${scenes}`;
      const prowlarrBtn = content.querySelector('[data-mp-prowlarr]');
      if (prowlarrBtn) {
        prowlarrBtn.addEventListener('click', (e) => {
          e.stopPropagation();
          if (typeof window.openProwlarrSearchPopup === 'function') {
            window.openProwlarrSearchPopup({ title: m.title || '', kind: 'movie' });
          }
        });
      }
    } catch (e) {
      content.innerHTML = `<div class="empty" style="padding:48px;text-align:center">Error loading movie</div>`;
    }
  }
  window.openMoviePopup = openMoviePopup;

  document.addEventListener('keydown', (e) => {
    if (e.key !== 'Escape') return;
    const overlay = document.getElementById('movieDetailOverlay');
    if (overlay && overlay.classList.contains('open')) closeMoviePopup();
  });
})();
