(function () {
  var SPLASH_ID = "bootSplash";
  var MIN_VISIBLE_MS = 480;
  var HEALTH_TIMEOUT_MS = 15000;
  var SESSION_KEY = "ts_boot_splash_shown";

  function ensureSplash() {
    if (document.getElementById(SPLASH_ID)) {
      return;
    }
    var el = document.createElement("div");
    el.id = SPLASH_ID;
    el.className = "boot-splash";
    el.setAttribute("aria-busy", "true");
    el.setAttribute("aria-live", "polite");
    el.setAttribute("aria-label", "Loading application");
    el.innerHTML =
      '<div class="boot-splash__inner">' +
      '<img class="boot-splash__logo" src="/static/img/logo.png" alt="Top-Shelf" width="280" height="96">' +
      '<span class="loader" role="status" aria-label="Loading"></span>' +
      "</div>";
    document.body.insertBefore(el, document.body.firstChild);
  }

  function dismissSplash() {
    var el = document.getElementById(SPLASH_ID);
    if (!el) {
      return;
    }
    el.classList.add("boot-splash--hide");
    el.setAttribute("aria-busy", "false");
    setTimeout(function () {
      if (el.parentNode) {
        el.parentNode.removeChild(el);
      }
    }, 420);
  }

  function runBootSplash() {
    // Show the splash once per browser session. Every page navigation
    // after the first is instant — no overlay, no health gate, no fade.
    // sessionStorage clears on tab close so a fresh app open still gets
    // the splash. Health check is dropped entirely on subsequent navs:
    // if the server is down the next API call will surface the error
    // anyway; we don't need to block UI for a probe.
    try {
      if (sessionStorage.getItem(SESSION_KEY)) {
        return;
      }
      sessionStorage.setItem(SESSION_KEY, "1");
    } catch (_) {
      // sessionStorage disabled (private mode in some browsers): fall
      // through and show the splash every time rather than crashing.
    }
    ensureSplash();
    var t0 = Date.now();
    function finish() {
      var wait = Math.max(0, MIN_VISIBLE_MS - (Date.now() - t0));
      setTimeout(dismissSplash, wait);
    }
    var req = fetch("/api/health", { method: "GET", cache: "no-store" });
    var timeout = new Promise(function (_, reject) {
      setTimeout(function () {
        reject(new Error("health timeout"));
      }, HEALTH_TIMEOUT_MS);
    });
    Promise.race([req, timeout]).then(finish, finish);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", runBootSplash);
  } else {
    runBootSplash();
  }
})();

(function () {
  function normalizePath(path) {
    if (!path || path === "/") return "/queue";
    if (path === "/tv") return "/queue";
    if (path === "/metadata") return "/scenes";
    return path;
  }

  function getNavTarget(path) {
    const p = normalizePath(path);
    if (p === "/scenes") return "/scenes";
    if (p === "/movies") return "/movies";
    if (p === "/queue") return "/queue";
    if (p === "/index") return "/index";
    if (p === "/downloads") return "/downloads";
    if (p === "/favourites") return "/favourites";
    if (p === "/library") return "/library";
    if (p === "/history") return "/history";
    if (p === "/log") return "/log";
    return "";
  }

  function markActiveNav() {
    const target = getNavTarget(window.location.pathname);
    const navLinks = document.querySelectorAll(".nav-links a[href]:not(.ts-dl-stats), .header-right a[href]:not(.ts-dl-stats)");
    navLinks.forEach(function (link) {
      const href = link.getAttribute("href");
      const btn = link.querySelector(".btn-secondary");
      if (!btn) return;
      btn.classList.toggle("active-nav", !!target && href === target);
    });
  }

  /** Undo the old hamburger-menu wrapper if it is still in the DOM. */
  function unwrapHeaderNavMenu() {
    const nav = getHeaderNav();
    if (!nav) return;
    const menu = nav.querySelector(".ts-header-nav-menu");
    if (!menu) return;
    const grid = menu.querySelector(".ts-header-nav-dropdown-grid");
    if (grid) {
      while (grid.firstChild) {
        nav.insertBefore(grid.firstChild, menu);
      }
    }
    menu.remove();
  }

  /** Right header: all nav icons in one gapless segmented strip. */
  function wireHeaderNavSeg() {
    const nav = getHeaderNav();
    if (!nav) return;
    unwrapHeaderNavMenu();
    let seg = nav.querySelector(".ts-header-nav-seg");
    if (!seg) {
      const kids = [];
      while (nav.firstChild) {
        kids.push(nav.firstChild);
        nav.removeChild(nav.firstChild);
      }
      if (!kids.length) return;
      seg = document.createElement("div");
      seg.className = "ts-seg ts-header-nav-seg";
      seg.setAttribute("role", "navigation");
      seg.setAttribute("aria-label", "Site navigation");
      kids.forEach(function (el) {
        seg.appendChild(el);
      });
      nav.appendChild(seg);
    }
    flattenDlNavCluster(seg);
    const logout = seg.querySelector('button.btn-secondary[title="Logout"]');
    if (logout) logout.style.marginLeft = "";
  }

  function flattenDlNavCluster(container) {
    if (!container) return;
    const cluster = container.querySelector(".ts-dl-nav-cluster");
    if (!cluster) return;
    while (cluster.firstChild) {
      container.insertBefore(cluster.firstChild, cluster);
    }
    cluster.remove();
  }

  function addPageClass() {
    const p = normalizePath(window.location.pathname).replace("/", "") || "queue";
    document.body.classList.add("page-" + p);
  }

  function getHeaderNav() {
    return document.querySelector(".nav-links") || document.querySelector(".header-right");
  }

  function dlDownloadsNavLink(nav) {
    const btn = nav.querySelector('a[href="/downloads"] button.btn-secondary');
    if (btn) return btn.closest("a");
    return nav.querySelector('a[href="/downloads"]:not(.ts-dl-stats)');
  }

  /* icon is a ts-nav-icon modifier (e.g. "queue", "index"); swap the
   * SVG by replacing /static/logos/menu/<name>.svg, no code change. */
  function dlNavBtn(href, title, icon, attr) {
    const a = document.createElement("a");
    a.href = href;
    a.style.textDecoration = "none";
    a.setAttribute(attr, "1");
    a.innerHTML =
      '<button class="btn-secondary" title="' + title + '"><i class="ts-nav-icon ts-nav-icon--' + icon + '"></i></button>';
    return a;
  }

  function ensureDlNavButtons(nav) {
    if (!nav) return;
    if (!nav.querySelector("[data-ts-nav-queue]")) {
      nav.appendChild(dlNavBtn("/queue", "Queue", "queue", "data-ts-nav-queue"));
    }
    if (!nav.querySelector("[data-ts-nav-index]")) {
      nav.appendChild(dlNavBtn("/index", "Download Indexer Feeds", "index", "data-ts-nav-index"));
    }
  }

  /* Some pages render the discover / library / index nav buttons with
   * the wrong icon class (legacy templates, server-side renderers).
   * Re-stamp them with the ts-nav-icon variants so the custom SVGs
   * land regardless of what the markup shipped. */
  function patchHeaderNavIcons() {
    const nav = getHeaderNav();
    if (!nav) return;
    const discover = nav.querySelector('a[href="/discover"] .btn-secondary i');
    if (discover) discover.className = "ts-nav-icon ts-nav-icon--discover";
    const library = nav.querySelector('a[href="/library"] .btn-secondary i');
    if (library) library.className = "ts-nav-icon ts-nav-icon--library";
    const index = nav.querySelector("[data-ts-nav-index] .btn-secondary i")
      || nav.querySelector('a[href="/index"] .btn-secondary i');
    if (index) index.className = "ts-nav-icon ts-nav-icon--index";
  }

  function patchHeaderNavTitles() {
    const nav = getHeaderNav();
    if (!nav) return;
    const dl = dlDownloadsNavLink(nav);
    if (dl) {
      const btn = dl.querySelector(".btn-secondary");
      if (btn) btn.setAttribute("title", "Download Clients");
    }
    const index = nav.querySelector("[data-ts-nav-index] .btn-secondary")
      || nav.querySelector('a[href="/index"] .btn-secondary');
    if (index) index.setAttribute("title", "Download Indexer Feeds");
  }

  /** Keep queue → downloads → index as the first segments in the strip. */
  function ensureHeaderNavOrder(nav) {
    if (!nav) return;
    ensureDlNavButtons(nav);
    const pill = nav.querySelector(".ts-dl-stats");
    if (pill) relocateDlStatsPill(pill);
    const parent = nav.querySelector(".ts-header-nav-seg") || nav;
    flattenDlNavCluster(parent);
    flattenDlNavCluster(nav);
    const queue = nav.querySelector("[data-ts-nav-queue]");
    const dl = dlDownloadsNavLink(nav);
    const index = nav.querySelector("[data-ts-nav-index]");
    [index, dl, queue].forEach(function (el) {
      if (el && el.parentNode === parent) {
        parent.insertBefore(el, parent.firstChild);
      }
    });
  }

  function injectDlNav() {
    ensureHeaderNavOrder(getHeaderNav());
  }

  /** Move an existing dl-stats pill into the left header cell. */
  function relocateDlStatsPill(pill) {
    if (!pill) return;
    const left = document.querySelector(".ts-header-left");
    if (!left) return;
    if (pill.parentNode !== left) {
      left.insertBefore(pill, left.firstChild);
    } else if (left.firstChild !== pill) {
      left.insertBefore(pill, left.firstChild);
    }
  }

  /** Logo + version overlay the fixed 60px activity band; logo is centred
   * alone (version sits to its right via `alignCenterBrand`). Hidden when
   * the banner has content (`syncCenterLogoVisibility`). */
  function ensureCenteredLogo() {
    const left = document.querySelector(".ts-header-left");
    const banner = document.getElementById("tsActivityBanner");
    if (!banner) return null;
    let logo = (left && left.querySelector(".logo")) || null;
    if (!logo && banner.parentNode) {
      logo = banner.parentNode.querySelector(".ts-header-center-logo");
    }
    if (!logo) return null;
    let center = banner.parentNode && banner.parentNode.classList.contains("ts-header-center")
      ? banner.parentNode
      : null;
    if (!center) {
      center = document.createElement("div");
      center.className = "ts-header-center";
      banner.parentNode.insertBefore(center, banner);
      center.appendChild(banner);
    }
    let brandRow = center.querySelector(".ts-header-brand-row");
    if (!brandRow) {
      brandRow = document.createElement("div");
      brandRow.className = "ts-header-brand-row";
      center.appendChild(brandRow);
    }
    logo.classList.add("ts-header-center-logo");
    if (logo.parentNode !== brandRow) brandRow.appendChild(logo);
    const ver = document.getElementById("appVersion");
    if (ver) {
      ver.classList.add("ts-header-center-version");
      if (ver.parentNode !== brandRow) brandRow.appendChild(ver);
    }
    const img = logo.querySelector("img");
    if (img && !img.__tsBrandAlignBound) {
      img.__tsBrandAlignBound = true;
      if (!img.complete) img.addEventListener("load", alignCenterBrand);
    }
    alignCenterBrand();
    return logo;
  }

  function alignCenterBrand() {
    const row = document.querySelector(".ts-header-brand-row");
    const logo = row && row.querySelector(".ts-header-center-logo");
    const ver = document.getElementById("appVersion");
    if (!row || !logo) return;
    logo.style.transform = "";
    if (ver) {
      ver.style.left = "";
      ver.style.top = "";
      ver.style.transform = "";
    }
    const rowRect = row.getBoundingClientRect();
    const logoRect = logo.getBoundingClientRect();
    if (!rowRect.width || !logoRect.width) return;
    const centerX = rowRect.left + rowRect.width / 2;
    const logoCenterX = logoRect.left + logoRect.width / 2;
    logo.style.transform = "translateX(" + (centerX - logoCenterX) + "px)";
    if (ver) {
      const lr = logo.getBoundingClientRect();
      ver.style.left = (lr.right - rowRect.left + 10) + "px";
      ver.style.top = "50%";
      ver.style.transform = "translateY(-50%)";
    }
  }

  function syncCenterLogoVisibility() {
    const banner = document.getElementById("tsActivityBanner");
    if (!banner) return;
    const hasContent = banner.children.length > 0
      || (banner.textContent && banner.textContent.trim().length > 0);
    document.body.classList.toggle("ts-header-has-activity", hasContent);
    if (!hasContent) {
      requestAnimationFrame(alignCenterBrand);
    }
  }

  function watchBannerForLogoVisibility() {
    const banner = document.getElementById("tsActivityBanner");
    if (!banner || banner.__tsLogoObserver) return;
    const obs = new MutationObserver(syncCenterLogoVisibility);
    obs.observe(banner, { childList: true, subtree: true, characterData: true });
    banner.__tsLogoObserver = obs;
  }

  let _brandAlignTimer = null;
  function scheduleAlignCenterBrand() {
    if (_brandAlignTimer) clearTimeout(_brandAlignTimer);
    _brandAlignTimer = setTimeout(function () {
      _brandAlignTimer = null;
      if (!document.body.classList.contains("ts-header-has-activity")) {
        alignCenterBrand();
      }
    }, 50);
  }

  if (!window.__tsBrandAlignResizeBound) {
    window.__tsBrandAlignResizeBound = true;
    window.addEventListener("resize", scheduleAlignCenterBrand);
  }

  window.ensureHeaderNavOrder = ensureHeaderNavOrder;
  window.relocateDlStatsPill = relocateDlStatsPill;
  window.syncCenterLogoVisibility = syncCenterLogoVisibility;
  window.ensureCenteredLogo = ensureCenteredLogo;

  window.__tsInitHeaderNav = function () {
    addPageClass();
    if (typeof window.__tsInjectDlStats === "function") {
      window.__tsInjectDlStats();
    }
    injectDlNav();
    patchHeaderNavTitles();
    patchHeaderNavIcons();
    wireHeaderNavSeg();
    ensureCenteredLogo();
    watchBannerForLogoVisibility();
    syncCenterLogoVisibility();
    markActiveNav();
  };

})();

window.addEventListener("unhandledrejection", function (event) {
  const msg = event.reason?.message || String(event.reason) || "Unknown error";
  // Only surface errors that look like network/API failures, not expected rejections
  if (msg.includes("fetch") || msg.includes("NetworkError") || /^[45]\d\d/.test(msg)) {
    console.error("Unhandled API error:", msg);
    // Show a subtle toast if the function exists on the page
    if (typeof window.toast === "function") window.toast("Request failed: " + msg, { kind: "error" });
  }
});

// ── Body scroll-lock while any modal is open ────────────────────
// Stops scroll-wheel events from bleeding through to the page
// behind an open overlay (e.g., scrolling inside the Scene Search
// popup used to scroll the queue underneath, which pulled the user
// out of context). A MutationObserver watches every known modal /
// overlay container's `style` and `class` attributes; whenever the
// visible count crosses zero, body's `overflow` is toggled.
(function () {
  const SELECTORS = [
    '.modal-overlay',
    '.queue-modal-overlay',
    '.lib-match-modal',
    '.scene-overlay',
    '.detail-overlay',
    '.prowlarr-overlay',
    '.img-overlay',
    '[id$="Overlay"]',
    '[id$="Modal"]',
  ];

  function isVisible(el) {
    if (!el || !el.isConnected) return false;
    const cs = window.getComputedStyle(el);
    if (cs.display === 'none' || cs.visibility === 'hidden') return false;
    const r = el.getBoundingClientRect();
    return r.width > 0 || r.height > 0;
  }

  function refresh() {
    let anyVisible = false;
    for (const sel of SELECTORS) {
      for (const el of document.querySelectorAll(sel)) {
        if (isVisible(el)) { anyVisible = true; break; }
      }
      if (anyVisible) break;
    }
    if (anyVisible) {
      document.body.classList.add('modal-scroll-lock');
    } else {
      document.body.classList.remove('modal-scroll-lock');
    }
  }

  // Inject the scroll-lock CSS once. Sets overflow:hidden on body
  // and reserves the scrollbar gutter via `scrollbar-gutter: stable`
  // (or padding-right fallback) so locking doesn't reflow content.
  const style = document.createElement('style');
  style.textContent =
    'body.modal-scroll-lock { overflow: hidden !important; }' +
    '@supports (scrollbar-gutter: stable) {' +
    '  body { scrollbar-gutter: stable; }' +
    '}';
  document.head.appendChild(style);

  // Watch existing + future overlays. Style/class changes trigger a
  // refresh; childList mutations on body catch dynamically-injected
  // overlays (e.g., toast stacks).
  const obs = new MutationObserver(refresh);
  function watchAll() {
    SELECTORS.forEach(sel => {
      document.querySelectorAll(sel).forEach(el => {
        obs.observe(el, { attributes: true, attributeFilter: ['style', 'class'] });
      });
    });
  }
  watchAll();
  obs.observe(document.body, { childList: true, subtree: true });

  // Initial sync after DOM is ready (in case any overlay is open on
  // load, e.g. an error modal preserved from server state).
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => { watchAll(); refresh(); });
  } else {
    refresh();
  }
})();

// ── Universal Esc-to-close for every modal / overlay ────────────
// Top-Shelf has ~30 modal containers across the page templates and
// each one historically wired its own `keydown` listener (or none at
// all). Single page-level handler here finds the topmost visible
// overlay and closes it — preferring the page-registered close
// function when one is known, falling back to `display: none` so
// even modals without a JS close path still respond.
//
// Each entry in the ID map is a known close-function name; new
// modals can opt-in either by adding a row OR by setting a
// `data-esc-close="myCloseFn"` attribute on the overlay element.
(function () {
  const ID_CLOSE_FN = {
    qSearchOverlay:        'clearSearch',
    suggestionsOverlay:    'closeSuggestionsModal',
    cropModal:             'closeCropModal',
    imgUploadModal:        'closeImgUploadModal',
    extLinkModal:          'closeExtLinkModal',
    aboutModal:            'closeAboutModal',
    perfAliasModal:        'closePerfAliasModal',
    perfSearchModal:       'closePerfSearchModal',
    treeLeafLookupOverlay: 'closeTreeLeafLookup',
    qSceneConfirmModal:    'closeSceneConfirm',
    qIafdPickerModal:      'closeIafdBulkPicker',
    qIafdResultsModal:     'closeIafdBulkResults',
    qIafdScanModal:        'closeIafdBulkScan',
    queueImageOverlay:     'closeQueueImageOverlay',
    qFilmstripOverlay:     'closeQueueFilmstrip',
    rssChooserOverlay:     'closeRssChooser',
    wantedSearchOverlay:   'closeWantedSearch',
    detailOverlay:         'closeDetail',
    prowlarrOverlay:       'closeProwlarr',
    imgOverlay:            'closeImgOverlay',
    sceneOverlay:          'closeSceneOverlay',
    addSuccessOverlay:     'closeAddSuccessOverlay',
    manualModal:           'closeManual',
    viceModal:             'closeVice',
    libMatchModal:         'closeLibMatchModal',
    dupCompareModal:       'closeDupCompareModal',
    entityPanelModal:      'closeEntityPanel',
    imagePickModal:        'closeImagePick',
    posterRoleModal:       'closePosterRole',
    searchModal:           'closeSearchModal',
    // Universal performer popup. Without this entry the Escape handler
    // falls through to the generic `el.style.display = 'none'` fallback,
    // which leaves an inline display:none on the overlay element. Next
    // openPerformerPopup() adds `.open` but the inline style overrides
    // the class, so the popup is "open" yet invisible — its overlay
    // intercepts clicks and the user can't open another performer.
    performerPopupModal:   'closePerformerPopup',
    // Universal studio popup — same Escape-fallback bug pattern as the
    // performer popup. Registered so the global handler closes via the
    // module's own cleanup rather than slapping inline display:none.
    studioPopupModal:      'closeStudioPopup',
    // Sub-modals spawned by the performer popup (link search, ext-link
    // search, add-to-library, performer-prowlarr, gallery fullscreen).
    // All use class-based visibility (.open / .is-open) so the inline
    // display fallback would otherwise leave them stuck across reopens
    // — same bug pattern as the popup itself, just one layer deeper.
    tsLinkSearchModal:        'closeTsLinkSearchModal',
    tsLinkExtModal:           'closeTsLinkExtModal',
    ppAddToLibraryModal:      'closeAddToLibraryModal',
    ppPerfProwlarrModal:      'closePerfProwlarrPopup',
    performerPopupGalleryFs:  'closePerformerGalleryFs',
    // Movie tab overlays on /scenes (Latest Movies). /movies' analogous
    // overlays are id="detailOverlay" / "prowlarrOverlay" and already
    // mapped above; these are the /scenes-only twins with movie- prefix.
    movieDetailOverlay:    'closeMovieDetail',
    movieProwlarrOverlay:  'closeMovieProwlarr',
  };
  const SELECTORS = [
    '.modal-overlay',
    '.queue-modal-overlay',
    '.lib-match-modal',
    '.scene-overlay',
    '.detail-overlay',
    '.prowlarr-overlay',
    '.img-overlay',
    '[id$="Overlay"]',
    '[id$="Modal"]',
  ];

  function visibleModals() {
    const seen = new Set();
    const out = [];
    SELECTORS.forEach(sel => {
      document.querySelectorAll(sel).forEach(el => {
        if (seen.has(el)) return;
        seen.add(el);
        const cs = window.getComputedStyle(el);
        if (cs.display === 'none' || cs.visibility === 'hidden') return;
        const r = el.getBoundingClientRect();
        if (r.width === 0 && r.height === 0) return;
        out.push(el);
      });
    });
    out.sort((a, b) => {
      const za = parseInt(window.getComputedStyle(a).zIndex, 10) || 0;
      const zb = parseInt(window.getComputedStyle(b).zIndex, 10) || 0;
      return zb - za;
    });
    return out;
  }

  function closeTop(el) {
    const explicit = el.getAttribute('data-esc-close');
    if (explicit && typeof window[explicit] === 'function') {
      try { window[explicit](); return true; } catch (_) {}
    }
    const fnName = ID_CLOSE_FN[el.id];
    if (fnName && typeof window[fnName] === 'function') {
      try { window[fnName](); return true; } catch (_) {}
    }
    // Fallback: just hide. Loses any per-modal cleanup but the user
    // gets a guaranteed dismissal.
    el.style.display = 'none';
    el.classList.remove('open');
    return true;
  }

  document.addEventListener('keydown', function (e) {
    if (e.key !== 'Escape') return;
    // Native help <dialog> — browser closes it.
    const helpDlg = document.getElementById('tsHelpDialog');
    if (helpDlg && helpDlg.open) return;

    // /settings: dismiss tag pickers only; never navigate away.
    if (window.location.pathname === '/settings') {
      if (typeof window._settingsDismissEscapeLayers === 'function') {
        try {
          if (window._settingsDismissEscapeLayers()) {
            e.preventDefault();
            e.stopPropagation();
          }
        } catch (_) {}
      }
      return;
    }

    const open = visibleModals();
    if (!open.length) return;
    e.preventDefault();
    e.stopPropagation();
    closeTop(open[0]);
  }, true);  // capture phase so this fires before per-page handlers
})();

// ── Header download-stats LCD ────────────────────────────────────
// Auto-injected on every page that has a header. Polls
// /api/downloads/stats and renders three 2-digit LED counters above a
// play / pause / stop icon row. Done here instead of per-template so
// every page (queue, downloads, scenes, discover, library, news, log,
// movies, history, favourites, health) picks it up without HTML churn.
(function () {
  // Format a non-negative integer as a 2-character LED-friendly string:
  //   <10  → leading zero ('07') so the digit aligns with 2-digit values
  //   ≥99  → cap at "99" (the strip is sized for 2 digits)
  function fmt2(n) {
    const v = Math.max(0, Math.min(99, n | 0));
    return v < 10 ? '0' + v : String(v);
  }

  function fmt3(n) {
    const v = Math.max(0, Math.min(999, n | 0));
    if (v < 10) return '00' + v;
    if (v < 100) return '0' + v;
    return String(v);
  }

  function injectDlStats() {
    // LCD counter lives in `.ts-header-left` (where the logo used to be).
    const left = document.querySelector(".ts-header-left");
    const nav = document.querySelector(".nav-links") || document.querySelector(".header-right");
    if (!left && !nav) return null;
    let el = document.querySelector(".ts-dl-stats");
    if (!el) {
      el = document.createElement('a');
      el.className = 'ts-dl-stats';
    el.href = '/downloads';
    el.setAttribute('aria-label', 'Download counts — active, pending, complete, queue');
    el.title = 'Active · Pending · Complete · Queue';
    el.innerHTML =
      '<div class="ts-dl-stats__tile" title="Active downloads">' +
        '<span class="ts-dl-stats__num" data-key="active">00</span>' +
        '<i class="fa-solid fa-play ts-dl-stats__icon"></i>' +
      '</div>' +
      '<div class="ts-dl-stats__tile" title="Pending downloads">' +
        '<span class="ts-dl-stats__num" data-key="pending">00</span>' +
        '<i class="fa-solid fa-pause ts-dl-stats__icon"></i>' +
      '</div>' +
      '<div class="ts-dl-stats__tile" title="Complete downloads">' +
        '<span class="ts-dl-stats__num" data-key="complete">00</span>' +
        '<i class="fa-solid fa-stop ts-dl-stats__icon"></i>' +
      '</div>' +
      '<div class="ts-dl-stats__tile ts-dl-stats__tile--queue" title="Source queue">' +
        '<span class="ts-dl-stats__num ts-dl-stats__num--3" data-key="queue">000</span>' +
        '<i class="fa-solid fa-list ts-dl-stats__icon"></i>' +
      '</div>';
    }
    if (left && typeof window.relocateDlStatsPill === "function") {
      window.relocateDlStatsPill(el);
    } else if (nav) {
      if (el.parentNode !== nav) nav.insertBefore(el, nav.firstElementChild);
      else if (nav.firstElementChild !== el) nav.insertBefore(el, nav.firstElementChild);
    }
    return el;
  }

  window.__tsInjectDlStats = injectDlStats;

  async function pollDlStats() {
    try {
      const r = await fetch('/api/downloads/stats', {
        credentials: 'same-origin',
        cache: 'no-store',
      });
      if (!r.ok) return;
      const d = await r.json();
      document.querySelectorAll('.ts-dl-stats__num').forEach(function (el) {
        const k = el.getAttribute('data-key');
        const v = (d && typeof d[k] === 'number') ? d[k] : 0;
        const next = (k === 'queue') ? fmt3(v) : fmt2(v);
        if (el.textContent !== next) el.textContent = next;
      });
    } catch (_) { /* ignore — keep last paint */ }
  }

  var dlStatsTimer = null;

  function stopDlStatsPolling() {
    if (dlStatsTimer) {
      clearInterval(dlStatsTimer);
      dlStatsTimer = null;
    }
  }

  window.addEventListener("pagehide", stopDlStatsPolling);

  function start() {
    if (typeof window.__tsInitHeaderNav === "function") {
      window.__tsInitHeaderNav();
    } else if (!injectDlStats()) {
      return;
    }
    if (!document.querySelector(".ts-dl-stats")) return;
    pollDlStats();
    if (dlStatsTimer) clearInterval(dlStatsTimer);
    dlStatsTimer = setInterval(function () {
      if (document.visibilityState === "hidden") return;
      pollDlStats();
    }, 8000);
    document.addEventListener("visibilitychange", function () {
      if (document.visibilityState === "visible") pollDlStats();
    });
  }

  document.addEventListener("DOMContentLoaded", start);
})();
