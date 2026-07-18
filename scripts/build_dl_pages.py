#!/usr/bin/env python3
"""Split static/downloads.html into index.html, queue.html, and downloads-only."""
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "static" / "downloads.html"
PANEL_OPEN = '  <div class="panel prowlarr-search-panel">'
PANEL_CLOSE_BEFORE = "\n\n<!-- Scene Search"

INDEX_HEADER = """    <div class="panel-header" style="display:grid;grid-template-columns:1fr auto 1fr;align-items:center;gap:12px">
      <div class="panel-title" style="justify-self:start">Feeds</div>
      <div style="justify-self:center">
        <div class="ts-seg ts-seg--5" role="group" aria-label="Feed source" style="min-width:720px">
          <button type="button" id="dlModeFavourites" class="active" onclick="setDlSearchMode('favourites')" title="Favourites"><span class="fav-lips" style="margin-right:6px"></span><span class="btn-label">Favourites</span></button>
          <button type="button" id="dlModeRss" onclick="setDlSearchMode('rss')" title="Performers"><i class="fa-solid fa-user" style="margin-right:6px"></i><span class="btn-label">Performers</span></button>
          <button type="button" id="dlModeStudios" onclick="setDlSearchMode('studios')" title="Studios"><i class="fa-solid fa-video" style="margin-right:6px"></i><span class="btn-label">Studios</span></button>
          <button type="button" id="dlModeWanted" onclick="setDlSearchMode('wanted')" title="Wanted"><i class="fa-solid fa-eye" style="margin-right:6px"></i><span class="btn-label">Wanted</span></button>
          <button type="button" id="dlModeProwlarr" onclick="setDlSearchMode('prowlarr')" title="Search"><i class="fa-solid fa-magnifying-glass" style="margin-right:6px"></i><span class="btn-label">Search</span></button>
        </div>
      </div>
      <div id="dlRssHeaderControls" style="visibility:visible;display:flex;gap:8px;align-items:center;justify-self:end">
        <input type="text" id="dlRssFilter" class="field-input" placeholder="Filter…" style="width:220px;padding:8px 10px" oninput="filterRssResults()">
        <button type="button" data-duotone-toggle class="btn-primary" onclick="toggleDuotoneMode()" title="Swap duotone" style="width:40px;height:40px;padding:0;border-radius:8px;flex-shrink:0"><i class="fa-solid fa-palette"></i></button>
        <button type="button" id="dlRssRefreshBtn" class="btn-primary" onclick="loadDlRssFeed('_refresh')" title="Refresh" style="width:40px;height:40px;padding:0;border-radius:8px;flex-shrink:0"><i class="fa-solid fa-arrows-rotate"></i></button>
      </div>
    </div>
"""

QUEUE_HEADER = """    <div class="panel-header" style="display:flex;align-items:center;justify-content:space-between;gap:12px">
      <div class="panel-title">Queue</div>
    </div>
"""

DL_HEADER = """    <div class="panel-header" style="display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap">
      <div class="panel-title">Downloads</div>
      <div class="toolbar" style="display:flex;gap:8px;align-items:center;margin-left:auto">
        <label class="meta" for="catFilter">Show</label>
        <select id="catFilter" class="field-input" style="min-width:220px" onchange="loadDownloads()" title="Category filter">
          <option value="">Top-Shelf</option>
          <option value="*">All categories</option>
        </select>
        <button class="btn-secondary" onclick="processAllDownloads()" title="Process now"><i class="fa-solid fa-play"></i></button>
        <button class="btn-secondary" onclick="loadDownloads()" title="Refresh"><i class="fa-solid fa-arrows-rotate"></i></button>
      </div>
    </div>
"""

INLINE_MOVIE = re.compile(
    r'\n\s*<!-- Movie Search -->\s*'
    r'<div class="q-search-panel" id="qMovieSearchPanel"[^>]*>.*?</div>\s*',
    re.DOTALL,
)

GET_FN = r"""  function getDlSearchMode() {
    if (typeof TS_DL_PAGE !== 'undefined' && TS_DL_PAGE === 'queue') return 'queue';
    if (typeof TS_DL_PAGE !== 'undefined' && TS_DL_PAGE === 'downloads') return 'downloads';
    const _indexModes = ['prowlarr', 'studios', 'wanted', 'rss', 'favourites'];
    try {
      const view = new URLSearchParams(location.search).get('view');
      if (view && _indexModes.includes(view)) return view;
    } catch (_) {}
    const v = localStorage.getItem(DL_SEARCH_MODE_KEY);
    if (v && _indexModes.includes(v)) return v;
    return 'favourites';
  }"""

APPLY_FN = r"""  function applyDlSearchModeUi() {
    if (typeof TS_DL_PAGE !== 'undefined' && TS_DL_PAGE === 'queue') {
      const elQ = document.getElementById('dlSearchModeQueue');
      if (elQ) elQ.style.display = 'flex';
      return;
    }
    if (typeof TS_DL_PAGE !== 'undefined' && TS_DL_PAGE === 'downloads') {
      const elD = document.getElementById('dlSearchModeDownloads');
      const decor = document.querySelector('[data-dl-decor]');
      if (elD) elD.style.display = 'flex';
      if (decor) decor.style.display = 'block';
      return;
    }
    const mode = getDlSearchMode();
    const isPoster = mode === 'rss' || mode === 'studios' || mode === 'favourites';
    const search = mode === 'prowlarr';
    const studios = mode === 'studios';
    const favourites = mode === 'favourites';
    const wanted = mode === 'wanted';
    const elP = document.getElementById('dlSearchModeProwlarr');
    const elR = document.getElementById('dlSearchModeRss');
    const elW = document.getElementById('dlSearchModeWanted');
    const bP = document.getElementById('dlModeProwlarr');
    const bR = document.getElementById('dlModeRss');
    const bSt = document.getElementById('dlModeStudios');
    const bF = document.getElementById('dlModeFavourites');
    const bW = document.getElementById('dlModeWanted');
    const hdrCtrl = document.getElementById('dlRssHeaderControls');
    const bodyWrap = document.getElementById('dlProwlarrBodyWrap');
    if (elP) elP.style.display = search ? '' : 'none';
    if (elR) elR.style.display = (mode === 'rss') ? '' : 'none';
    if (elW) elW.style.display = wanted ? 'flex' : 'none';
    if (bP) bP.classList.toggle('active', search);
    if (bR) bR.classList.toggle('active', mode === 'rss');
    if (bSt) bSt.classList.toggle('active', studios);
    if (bF) bF.classList.toggle('active', favourites);
    if (bW) bW.classList.toggle('active', wanted);
    if (hdrCtrl) hdrCtrl.style.visibility = isPoster ? 'visible' : 'hidden';
    if (bodyWrap) {
      bodyWrap.style.display = 'flex';
      bodyWrap.style.flexDirection = 'column';
      bodyWrap.style.flex = '1';
      bodyWrap.style.minHeight = '0';
      bodyWrap.style.padding = '0';
      const inner = document.getElementById('dlProwlarrResults');
      if (inner) {
        inner.className = studios ? 'rss-poster-grid rss-poster-grid--studio' : (isPoster ? 'rss-poster-grid' : 'dl-prowlarr-results');
      }
    }
  }"""

# Injected before "// Queue functions" on index/downloads only. On queue.html
# use QUEUE_BOOT_FN at end of script instead (setQueueMode → _clearSearchFrames
# touches `_searchFrameToken` — early boot throws TDZ ReferenceError).
BOOT_FN = r"""  (() => {
    if (typeof TS_DL_PAGE !== 'undefined' && TS_DL_PAGE === 'queue') {
      return;
    }
    if (typeof TS_DL_PAGE !== 'undefined' && TS_DL_PAGE === 'downloads') {
      try {
        const sel = document.getElementById('catFilter');
        const def = new URLSearchParams(location.search).get('category');
        if (def !== null && sel) {
          if (def === '*' || def === '') sel.value = def === '*' ? '*' : '';
          else {
            let opt = Array.from(sel.options).find(o => o.value === def);
            if (!opt) {
              opt = document.createElement('option');
              opt.value = def;
              opt.textContent = `Category: ${def}`;
              sel.appendChild(opt);
            }
            sel.value = def;
          }
        }
      } catch (_) {}
      loadDownloads();
      startDownloadsAutoRefresh();
      document.addEventListener('visibilitychange', () => {
        if (document.visibilityState === 'visible' && TS_DL_PAGE === 'downloads') loadDownloads();
      });
      return;
    }
    try {
      const sel = document.getElementById('catFilter');
      const def = new URLSearchParams(location.search).get('category');
      if (def !== null && sel) {
        if (def === '*' || def === '') {
          sel.value = def === '*' ? '*' : '';
        } else {
          let opt = Array.from(sel.options).find(o => o.value === def);
          if (!opt) {
            opt = document.createElement('option');
            opt.value = def;
            opt.textContent = `Category: ${def}`;
            sel.appendChild(opt);
          }
          sel.value = def;
        }
      }
    } catch (_) {}
    applyProwlarrCategoryLabels().catch(() => {});
    applyDlSearchModeUi();
    const _initMode = getDlSearchMode();
    if (_initMode === 'wanted') loadWantedPanel();
    else if (_initMode === 'favourites' || _initMode === 'rss' || _initMode === 'studios') {
      if (typeof _dlRssPaintFromCache === 'function') _dlRssPaintFromCache(_initMode);
      loadDlRssFeed(_initMode);
    } else if (_initMode === 'prowlarr') {
      const el = document.getElementById('dlProwlarrResults');
      if (el) el.innerHTML = '<div class="empty" style="padding:16px">Enter a query and search.</div>';
    }
  })();"""
BOOT_FN = BOOT_FN.replace("search.</motion>", "search.</motion>")

QUEUE_BOOT_FN = r"""  // Page boot — after all `let`/`const`. setQueueMode → clearSelectedFile →
  // _clearSearchFrames touches _searchFrameToken; running earlier throws TDZ.
  (() => {
    applyDlSearchModeUi();
    setQueueMode(queueMode);
    loadQueue();
    loadQueueStats();
    startQueuePolling();
    document.addEventListener('visibilitychange', () => {
      if (document.visibilityState === 'visible' && typeof TS_DL_PAGE !== 'undefined' && TS_DL_PAGE === 'queue') {
        loadQueue({ preserveView: true });
        loadQueueStats();
      }
    });
  })();

"""


def slice_between(text: str, start: str, end: str) -> str:
    i = text.index(start)
    j = text.index(end, i)
    return text[i:j]


def strip_inline_movie(text: str) -> str:
    return INLINE_MOVIE.sub("", text)


def rebuild_panel(text: str, inner: str) -> str:
    i = text.index(PANEL_OPEN)
    j = text.index(PANEL_CLOSE_BEFORE, i)
    return text[:i] + PANEL_OPEN + "\n" + inner.rstrip() + "\n  </div>\n" + text[j:]


def extract_feed_body(text: str) -> str:
    return slice_between(text, '    <div id="dlSearchModeProwlarr">', '\n    <div id="dlSearchModeDownloads"')


def extract_wanted(text: str) -> str:
    return slice_between(text, '    <!-- Wanted section -->', '\n\n    <!-- Queue section -->') + "\n"


def extract_downloads(text: str) -> str:
    block = slice_between(
        text,
        '    <div id="dlSearchModeDownloads"',
        '\n\n    <!-- Wanted section -->',
    )
    block = re.sub(
        r'      <div class="panel-header" style="border-top[^"]*">.*?</div>\s*\n',
        "",
        block,
        count=1,
        flags=re.DOTALL,
    )
    return block.replace('style="display:none"', 'style="display:flex"', 1)


def extract_queue(text: str) -> str:
    block = slice_between(text, '    <!-- Queue section -->', PANEL_CLOSE_BEFORE)
    block = strip_inline_movie(block)
    return block.replace(
        'style="display:none;flex:1;min-height:0;flex-direction:column"',
        'style="display:flex;flex:1;min-height:0;flex-direction:column"',
        1,
    )


def inject_page_mode(text: str, mode: str) -> str:
    return text.replace(
        "<script>\n  const DOWNLOADS_AUTO_REFRESH_MS",
        f"<script>\n  const TS_DL_PAGE = '{mode}';\n  const DOWNLOADS_AUTO_REFRESH_MS",
        1,
    )


def patch_js_common(text: str) -> str:
    text = re.sub(
        r"  function getDlSearchMode\(\) \{.*?\n  \}\n\n  function applyDlSearchModeUi",
        GET_FN + "\n\n  function applyDlSearchModeUi",
        text,
        count=1,
        flags=re.DOTALL,
    )
    text = re.sub(
        r"  function applyDlSearchModeUi\(\) \{.*?\n  \}\n\n  function setDlSearchMode",
        APPLY_FN + "\n\n  function setDlSearchMode",
        text,
        count=1,
        flags=re.DOTALL,
    )
    return text


def patch_js_early_boot(text: str) -> str:
    text = re.sub(
        r"  \(\(\) => \{.*?\n  \}\)\(\);\n\n  // ═+ Queue functions",
        BOOT_FN + "\n\n  // ══════════════════════════════════════════════════════════════════════\n  // Queue functions",
        text,
        count=1,
        flags=re.DOTALL,
    )
    return text


def patch_js_queue_boot(text: str) -> str:
    anchor = "  // Escape key for queue overlays"
    if QUEUE_BOOT_FN.strip() in text:
        return text
    if anchor not in text:
        raise ValueError("queue boot anchor not found")
    return text.replace(anchor, QUEUE_BOOT_FN + anchor, 1)


def patch_js(text: str, *, page: str) -> str:
    text = patch_js_common(text)
    if page == "queue":
        text = re.sub(
            r"  // Page boot — after all `let`/`const`.*?  \}\)\(\);\n\n",
            "",
            text,
            count=1,
            flags=re.DOTALL,
        )
        text = re.sub(
            r"  \(\(\) => \{.*?\n  \}\)\(\);\n\n  // ═+ Queue functions",
            "  // ══════════════════════════════════════════════════════════════════════\n  // Queue functions",
            text,
            count=1,
            flags=re.DOTALL,
        )
        return patch_js_queue_boot(text)
    return patch_js_early_boot(text)


def build_index(base: str) -> str:
    inner = INDEX_HEADER + "\n" + extract_feed_body(base) + "\n" + extract_wanted(base)
    t = rebuild_panel(base, inner)
    t = t.replace("<title>Top-Shelf — Downloads</title>", "<title>Top-Shelf — Feeds</title>")
    t = inject_page_mode(t, "index")
    return patch_js(t, page="index")


def build_queue(base: str) -> str:
    inner = QUEUE_HEADER + "\n" + extract_queue(base)
    t = rebuild_panel(base, inner)
    t = t.replace("<title>Top-Shelf — Downloads</title>", "<title>Top-Shelf — Queue</title>")
    t = inject_page_mode(t, "queue")
    return patch_js(t, page="queue")


def build_downloads(base: str) -> str:
    inner = DL_HEADER + "\n" + extract_downloads(base)
    t = rebuild_panel(base, inner)
    t = inject_page_mode(t, "downloads")
    return patch_js(t, page="downloads")


def main():
    base = SRC.read_text(encoding="utf-8")
    (ROOT / "static" / "index.html").write_text(build_index(base), encoding="utf-8")
    (ROOT / "static" / "queue.html").write_text(build_queue(base), encoding="utf-8")
    SRC.write_text(build_downloads(base), encoding="utf-8")
    print("built index.html, queue.html, downloads.html")


if __name__ == "__main__":
    main()
