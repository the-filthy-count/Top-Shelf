(function () {
  var SPLASH_ID = "bootSplash";
  // Minimum on-screen time so the splash doesn't blink in/out when
  // health responds in tens of milliseconds. Bypassed entirely when
  // /api/health resolves within FAST_PATH_MS — in that case we dismiss
  // immediately because the user definitely doesn't need a splash to
  // mask a 30 ms wait.
  var MIN_VISIBLE_MS = 480;
  var FAST_PATH_MS = 100;
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
      '<img class="boot-splash__logo" src="/static/img/logo.webp" alt="Top-Shelf" width="280" height="96">' +
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
      var elapsed = Date.now() - t0;
      // Fast-path: health came back nearly instantly — the splash
      // would have been on-screen for less than a frame anyway, so
      // dismiss right now instead of holding for the cosmetic 480 ms.
      if (elapsed < FAST_PATH_MS) {
        dismissSplash();
        return;
      }
      var wait = Math.max(0, MIN_VISIBLE_MS - elapsed);
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
    if (p === "/news") return "/news";
    if (p === "/health") return "/health";
    if (p === "/history") return "/health";
    if (p === "/log") return "/log";
    if (p === "/settings") return "/settings";
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

  const NAV_COMPACT_MQ = "(max-width: 900px)";
  const NAV_SHORT_LABELS = {
    "Download Clients": "Downloads",
    "Download Indexer Feeds": "Index",
  };

  function navShortLabel(title) {
    const t = String(title || "").trim();
    return NAV_SHORT_LABELS[t] || t;
  }

  function ensureNavButtonLabel(btn) {
    if (!btn || btn.querySelector(".ts-nav-label")) return;
    const title = (btn.getAttribute("title") || "").trim();
    if (!title) return;
    const span = document.createElement("span");
    span.className = "ts-nav-label";
    span.textContent = navShortLabel(title);
    btn.classList.add("ts-nav-btn--labeled");
    btn.appendChild(span);
  }

  function wireHeaderNavLabels() {
    const nav = getHeaderNav();
    if (!nav) return;
    nav.querySelectorAll(
      ".ts-header-nav-seg a[href] .btn-secondary, .ts-header-nav-dropdown-grid a[href] .btn-secondary",
    ).forEach(ensureNavButtonLabel);
  }

  function closeHeaderNavMenu(nav) {
    if (!nav) return;
    const menu = nav.querySelector(".ts-header-nav-menu");
    const toggle = nav.querySelector(".ts-header-nav-toggle");
    if (menu) menu.classList.remove("is-open");
    if (toggle) toggle.setAttribute("aria-expanded", "false");
  }

  function ensureHeaderNavMenu(nav) {
    if (!nav) return;
    let toggle = nav.querySelector(".ts-header-nav-toggle");
    if (!toggle) {
      toggle = document.createElement("button");
      toggle.type = "button";
      toggle.className = "btn-secondary ts-header-nav-toggle";
      toggle.setAttribute("aria-label", "Site navigation");
      toggle.setAttribute("aria-expanded", "false");
      toggle.setAttribute("aria-controls", "tsHeaderNavMenu");
      toggle.innerHTML = '<i class="fa-solid fa-bars" aria-hidden="true"></i>';
      nav.insertBefore(toggle, nav.firstChild);
    }
    let menu = nav.querySelector(".ts-header-nav-menu");
    if (!menu) {
      menu = document.createElement("div");
      menu.id = "tsHeaderNavMenu";
      menu.className = "ts-header-nav-menu";
      menu.setAttribute("role", "navigation");
      menu.setAttribute("aria-label", "Site navigation");
      const grid = document.createElement("div");
      grid.className = "ts-header-nav-dropdown-grid";
      menu.appendChild(grid);
      nav.appendChild(menu);
    }
    if (!nav.dataset.tsNavMenuBound) {
      nav.dataset.tsNavMenuBound = "1";
      toggle.addEventListener("click", function (ev) {
        ev.stopPropagation();
        const open = menu.classList.toggle("is-open");
        toggle.setAttribute("aria-expanded", open ? "true" : "false");
      });
      menu.addEventListener("click", function (ev) {
        if (ev.target.closest("a[href]")) closeHeaderNavMenu(nav);
      });
    }
  }

  function moveNavIntoMenu(nav) {
    const seg = nav.querySelector(".ts-header-nav-seg");
    const grid = nav.querySelector(".ts-header-nav-dropdown-grid");
    if (!seg || !grid) return;
    while (seg.firstChild) {
      grid.appendChild(seg.firstChild);
    }
    wireHeaderNavLabels();
  }

  /** Restore segmented strip from the compact overflow menu. */
  function unwrapHeaderNavMenu() {
    const nav = getHeaderNav();
    if (!nav) return;
    const menu = nav.querySelector(".ts-header-nav-menu");
    if (!menu) return;
    const grid = menu.querySelector(".ts-header-nav-dropdown-grid");
    const seg = nav.querySelector(".ts-header-nav-seg");
    const parent = seg || nav;
    if (grid) {
      while (grid.firstChild) {
        parent.appendChild(grid.firstChild);
      }
    }
    closeHeaderNavMenu(nav);
  }

  let _navLayoutMq = null;
  function syncHeaderNavLayout() {
    const nav = getHeaderNav();
    if (!nav) return;
    const compact = window.matchMedia(NAV_COMPACT_MQ).matches;
    if (compact) {
      ensureHeaderNavMenu(nav);
      const seg = nav.querySelector(".ts-header-nav-seg");
      if (seg && seg.querySelector("a[href], button.btn-secondary")) {
        moveNavIntoMenu(nav);
      }
    } else {
      unwrapHeaderNavMenu();
      wireHeaderNavLabels();
    }
    markActiveNav();
  }

  function bindHeaderNavLayoutListener() {
    if (_navLayoutMq) return;
    _navLayoutMq = window.matchMedia(NAV_COMPACT_MQ);
    _navLayoutMq.addEventListener("change", syncHeaderNavLayout);
    document.addEventListener("click", function (ev) {
      const nav = getHeaderNav();
      if (!nav || !nav.querySelector(".ts-header-nav-menu.is-open")) return;
      if (ev.target.closest(".ts-header-nav-toggle") || ev.target.closest(".ts-header-nav-menu")) return;
      closeHeaderNavMenu(nav);
    });
    document.addEventListener("keydown", function (ev) {
      if (ev.key !== "Escape") return;
      closeHeaderNavMenu(getHeaderNav());
    });
  }

  /** Right header: all nav icons in one gapless segmented strip. */
  function wireHeaderNavSeg() {
    const nav = getHeaderNav();
    if (!nav) return;
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
    const lab = navShortLabel(title);
    a.innerHTML =
      '<button type="button" class="btn-secondary ts-nav-btn--labeled" title="' + title + '">' +
      '<i class="ts-nav-icon ts-nav-icon--' + icon + '"></i>' +
      '<span class="ts-nav-label">' + lab + "</span></button>";
    return a;
  }

  function headerNavSeg(nav) {
    return (nav && nav.querySelector(".ts-header-nav-seg")) || nav;
  }

  function ensureDlNavButtons(nav) {
    if (!nav) return;
    const parent = headerNavSeg(nav);
    if (!nav.querySelector("[data-ts-nav-queue]")) {
      parent.appendChild(dlNavBtn("/queue", "Queue", "queue", "data-ts-nav-queue"));
    }
    if (!nav.querySelector("[data-ts-nav-index]")) {
      parent.appendChild(dlNavBtn("/index", "Download Indexer Feeds", "index", "data-ts-nav-index"));
    }
  }

  /* Some pages render the library / index nav buttons with the wrong
   * icon class (legacy templates, server-side renderers). Re-stamp
   * them with the ts-nav-icon variants so the custom SVGs land
   * regardless of what the markup shipped. */
  function patchHeaderNavIcons() {
    const nav = getHeaderNav();
    if (!nav) return;
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

  function headerNavLogoutBtn(parent) {
    if (!parent) return null;
    return parent.querySelector('button.btn-secondary[title="Logout"]');
  }

  /** Keep queue → downloads → index first; LCD pill immediately before logout. */
  function ensureHeaderNavOrder(nav) {
    if (!nav) return;
    ensureDlNavButtons(nav);
    const parent = headerNavSeg(nav);
    flattenDlNavCluster(parent);
    flattenDlNavCluster(nav);
    const queue = nav.querySelector("[data-ts-nav-queue]");
    const dl = dlDownloadsNavLink(nav);
    const index = nav.querySelector("[data-ts-nav-index]");
    /* queue → downloads → index at the front of the strip */
    [index, dl, queue].forEach(function (el) {
      if (el) parent.insertBefore(el, parent.firstChild);
    });
    const pill = nav.querySelector(".ts-dl-stats");
    if (pill) relocateDlStatsPill(pill);
  }

  function injectDlNav() {
    ensureHeaderNavOrder(getHeaderNav());
  }

  /** Download-stats LCD: last item in the nav strip, left of logout. */
  function relocateDlStatsPill(pill) {
    if (!pill) return;
    const nav = getHeaderNav();
    if (!nav) return;
    const parent = nav.querySelector(".ts-header-nav-seg") || nav;
    const logout = headerNavLogoutBtn(parent);
    if (logout) {
      if (pill.parentNode !== parent || pill.nextElementSibling !== logout) {
        parent.insertBefore(pill, logout);
      }
      return;
    }
    if (pill.parentNode !== nav) nav.appendChild(pill);
  }

  /** Logo stays in `.ts-header-left`; only the activity banner is centred. */
  function restoreLogoToLeft() {
    const left = document.querySelector(".ts-header-left");
    if (!left) return null;
    let logo = left.querySelector(".logo");
    if (!logo) {
      logo = document.querySelector(".ts-header-center-logo");
    }
    if (!logo) return null;
    logo.classList.remove("ts-header-center-logo");
    logo.style.transform = "";
    if (logo.parentNode !== left) {
      left.insertBefore(logo, left.firstChild);
    }
    const ver = document.getElementById("appVersion");
    if (ver) {
      ver.classList.remove("ts-header-center-version");
      ver.style.left = "";
      ver.style.top = "";
      ver.style.transform = "";
      if (ver.parentNode !== left) left.appendChild(ver);
    }
    const brandRow = document.querySelector(".ts-header-brand-row");
    if (brandRow && !brandRow.children.length) brandRow.remove();
    return logo;
  }

  function ensureHeaderCenter() {
    const banner = document.getElementById("tsActivityBanner");
    if (!banner || !banner.parentNode) return;
    restoreLogoToLeft();
    if (banner.parentNode.classList.contains("ts-header-center")) return;
    const center = document.createElement("div");
    center.className = "ts-header-center";
    banner.parentNode.insertBefore(center, banner);
    center.appendChild(banner);
  }

  function syncCenterLogoVisibility() {
    const banner = document.getElementById("tsActivityBanner");
    if (!banner) return;
    const hasContent = banner.children.length > 0
      || (banner.textContent && banner.textContent.trim().length > 0);
    document.body.classList.toggle("ts-header-has-activity", hasContent);
  }

  function watchBannerForLogoVisibility() {
    const banner = document.getElementById("tsActivityBanner");
    if (!banner || banner.__tsLogoObserver) return;
    const obs = new MutationObserver(syncCenterLogoVisibility);
    obs.observe(banner, { childList: true, subtree: true, characterData: true });
    banner.__tsLogoObserver = obs;
  }

  window.ensureHeaderNavOrder = ensureHeaderNavOrder;
  window.relocateDlStatsPill = relocateDlStatsPill;
  window.syncCenterLogoVisibility = syncCenterLogoVisibility;
  window.ensureCenteredLogo = ensureHeaderCenter;
  window.ensureHeaderCenter = ensureHeaderCenter;
  window.restoreLogoToLeft = restoreLogoToLeft;

  window.__tsInitHeaderNav = function () {
    addPageClass();
    patchHeaderNavTitles();
    patchHeaderNavIcons();
    wireHeaderNavSeg();
    injectDlNav();
    if (typeof window.__tsInjectDlStats === "function") {
      window.__tsInjectDlStats();
    }
    bindHeaderNavLayoutListener();
    syncHeaderNavLayout();
    ensureHeaderCenter();
    watchBannerForLogoVisibility();
    syncCenterLogoVisibility();
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

//: Global status indicator bar. Injects a thin glow-strip at the top
//: of every page, and exposes ``window.__tsSetStatusBar(state)`` so
//: activity-banner.js (the existing 2s poller) can flip it between
//: ``idle`` / ``running`` / ``error`` based on the same data it
//: already collects. CSS does all the actual glow/pulse/sheen work.
(function () {
  function ensureStatusBar() {
    var existing = document.getElementById('tsStatusBar');
    if (existing) {
      _mountStatusBarUnderHeader(existing);
      return existing;
    }
    var el = document.createElement('div');
    el.id = 'tsStatusBar';
    el.className = 'ts-status-bar';
    el.setAttribute('data-state', 'idle');
    el.setAttribute('aria-hidden', 'true');
    _mountStatusBarUnderHeader(el);
    return el;
  }
  //: Park the bar at the bottom edge of the app header so it reads
  //: as part of the header chrome rather than a top-of-viewport
  //: floating strip. Falls back to ``document.body`` (top of page)
  //: only when no header exists — e.g. the boot-splash route.
  function _mountStatusBarUnderHeader(el) {
    var header = document.querySelector('header.ts-app-header');
    if (header) {
      //: Ensure positioned ancestor so the bar's `position: absolute`
      //: resolves to the header rather than the viewport.
      var pos = getComputedStyle(header).position;
      if (pos === 'static') header.style.position = 'relative';
      if (el.parentElement !== header) header.appendChild(el);
      el.classList.add('ts-status-bar--in-header');
    } else if (el.parentElement !== document.body) {
      document.body.appendChild(el);
    }
  }
  //: Transient (flash) states override the base for a short window
  //: then auto-revert. ``flashTimer`` lets a new flash interrupt an
  //: existing one cleanly. Base states (``idle`` / ``running`` /
  //: ``paused``) persist until explicitly changed.
  var TRANSIENT = {
    error:   { ms: 1300 },  // 3 × 0.42s
    warning: { ms: 1150 },  // 2 × 0.55s
    success: { ms: 1100 },  // single 1.1s glow
  };
  var flashTimer = null;
  var lastBaseState = 'idle';
  function setStatusBar(state) {
    var bar = ensureStatusBar();
    var transient = TRANSIENT[state];
    if (transient) {
      if (flashTimer) clearTimeout(flashTimer);
      bar.setAttribute('data-state', state);
      flashTimer = setTimeout(function () {
        bar.setAttribute('data-state', lastBaseState);
        flashTimer = null;
      }, transient.ms);
      return;
    }
    //: Base-state update — store as the resting state and apply only
    //: when no transient flash is currently running.
    lastBaseState = state;
    if (!flashTimer) bar.setAttribute('data-state', state);
  }
  window.__tsSetStatusBar = setStatusBar;
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', ensureStatusBar);
  } else {
    ensureStatusBar();
  }
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
    const nav = document.querySelector(".nav-links") || document.querySelector(".header-right");
    if (!nav) return null;
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
    if (typeof window.relocateDlStatsPill === "function") {
      window.relocateDlStatsPill(el);
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

/* Keyboard-shortcut overlay (press ?).
 *
 * A power-user help panel listing every globally-available shortcut.
 * Triggered by pressing `?` (Shift+/) anywhere outside a text input.
 * Closes on Escape or click outside the panel. Lives in app-shell.js
 * so every route picks it up automatically. */
(function () {
  // Guard against double-init when this script gets loaded twice
  // (the popup-bundle preload + page-level <script> tags both pull
  //  app-shell.js on certain routes).
  if (window.__tsShortcutsWired) return;
  window.__tsShortcutsWired = true;

  // What we actually document. Each row: { keys: [...], label: '…' }.
  // `keys` renders as <kbd> chips joined by " + ". Multiple alternative
  // shortcuts on the same row sit comma-separated.
  var SHORTCUTS = [
    { keys: ["?"],       label: "Show this help" },
    { keys: ["Esc"],     label: "Close the open modal / popup" },
    { keys: ["/"],       label: "Focus the page's main filter / search input" },
    { keys: ["g", "q"],  label: "Jump to /queue" },
    { keys: ["g", "s"],  label: "Jump to /scenes" },
    { keys: ["g", "l"],  label: "Jump to /library" },
    { keys: ["g", "n"],  label: "Jump to /news" },
    { keys: ["g", "h"],  label: "Jump to /health" },
    { keys: ["g", "i"],  label: "Jump to /index (downloads)" },
  ];

  // `g` is a leader key — pressing it arms a one-shot listener for the
  // next character. Times out after 1.2 s so a stray `g` doesn't lock
  // the keyboard.
  var GO_TARGETS = {
    q: "/queue",  s: "/scenes",
    l: "/library", n: "/news",   h: "/health",
    i: "/index",
  };
  var goArmed = false;
  var goTimer = null;
  function armGo() {
    goArmed = true;
    if (goTimer) clearTimeout(goTimer);
    goTimer = setTimeout(function () { goArmed = false; }, 1200);
  }

  function inTextField(target) {
    if (!target) return false;
    var tag = (target.tagName || "").toUpperCase();
    if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return true;
    if (target.isContentEditable) return true;
    return false;
  }

  function focusMainFilter() {
    // Try the most likely candidates in order — the queue filter, the
    // /library filter, the scenes search, /downloads filter, /news
    // filter — pick whichever is on this page.
    var ids = [
      "queueFilter", "favFilter", "newsFilter", "dlRssFilter",
      "settingsSearch", "filterQueue",
    ];
    for (var i = 0; i < ids.length; i++) {
      var el = document.getElementById(ids[i]);
      if (el && typeof el.focus === "function") { el.focus(); return true; }
    }
    // Fallback — first visible text input on the page.
    var inputs = document.querySelectorAll('input[type="text"], input[type="search"]');
    for (var j = 0; j < inputs.length; j++) {
      var rect = inputs[j].getBoundingClientRect();
      if (rect.width > 30 && rect.height > 0) {
        inputs[j].focus();
        return true;
      }
    }
    return false;
  }

  var overlay = null;
  function buildOverlay() {
    var div = document.createElement("div");
    div.className = "modal-overlay open ts-shortcuts-overlay";
    div.style.zIndex = "1950";
    var html = '<div class="modal-box ts-shortcuts-box" style="max-width:520px">' +
      '<h3 style="margin:0 0 14px 0;font-family:var(--font-display,var(--mono));font-size:17px">Keyboard shortcuts</h3>' +
      '<dl class="ts-shortcuts-list" style="display:grid;grid-template-columns:auto 1fr;gap:8px 16px;align-items:center;margin:0">';
    for (var i = 0; i < SHORTCUTS.length; i++) {
      var s = SHORTCUTS[i];
      var keysHtml = s.keys.map(function (k) {
        return '<kbd style="display:inline-flex;align-items:center;justify-content:center;min-width:24px;padding:3px 8px;border:1px solid rgba(255,255,255,0.18);border-radius:5px;background:rgba(0,0,0,0.45);color:var(--text);font-family:var(--mono);font-size:11px;letter-spacing:0.04em">' + k + '</kbd>';
      }).join('<span style="color:var(--dim);font-size:10px;margin:0 4px">then</span>');
      html += '<dt style="white-space:nowrap">' + keysHtml + '</dt>' +
              '<dd style="margin:0;font-size:13px;color:var(--text)">' + s.label + '</dd>';
    }
    html += '</dl>' +
      '<div class="ts-link-modal-foot" style="margin-top:18px"><button type="button" class="ts-shortcuts-close" style="background:rgba(var(--brand-purple-rgb),0.4);border:1px solid rgba(var(--brand-purple-rgb),0.65);color:var(--accent);padding:8px 16px;border-radius:6px;font-size:11px;font-family:var(--mono);cursor:pointer;text-transform:uppercase;letter-spacing:0.04em">Close</button></div>' +
      '</div>';
    div.innerHTML = html;
    div.addEventListener("click", function (e) {
      if (e.target === div) closeOverlay();
    });
    div.querySelector(".ts-shortcuts-close").addEventListener("click", closeOverlay);
    return div;
  }

  function openOverlay() {
    if (overlay) return;
    overlay = buildOverlay();
    document.body.appendChild(overlay);
  }

  function closeOverlay() {
    if (!overlay) return;
    try { overlay.remove(); } catch (_) {}
    overlay = null;
  }

  document.addEventListener("keydown", function (e) {
    // Don't hijack typing in inputs / textareas.
    if (inTextField(e.target)) return;
    // Modifier-loaded keys (Ctrl/Cmd/Alt) belong to the browser.
    if (e.ctrlKey || e.metaKey || e.altKey) return;
    // Overlay open: Esc closes it.
    if (overlay) {
      if (e.key === "Escape") { e.preventDefault(); closeOverlay(); }
      return;
    }
    // Leader-key navigation (g, X).
    if (goArmed) {
      goArmed = false;
      if (goTimer) { clearTimeout(goTimer); goTimer = null; }
      var dest = GO_TARGETS[e.key.toLowerCase()];
      if (dest) {
        e.preventDefault();
        window.location.href = dest;
      }
      return;
    }
    if (e.key === "?") {
      e.preventDefault();
      openOverlay();
      return;
    }
    if (e.key === "/") {
      if (focusMainFilter()) e.preventDefault();
      return;
    }
    if (e.key === "g" || e.key === "G") {
      // Arm only if no input is focused (already filtered above) and
      // there's no other open overlay swallowing the keystroke.
      armGo();
      return;
    }
  });
})();
