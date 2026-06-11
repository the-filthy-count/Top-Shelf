/* Lazy-load performer / studio popups + link modals on first use.
 *
 * Two-stage strategy:
 *   • At idle time, we still preload the bundle so the FIRST click on a
 *     popup link doesn't pay a 200–400 ms script fetch + parse. That
 *     preload now runs in PARALLEL (Promise.all) instead of the
 *     previous sequential .then chain — saves ~50–100 ms on first open.
 *   • On a synchronous click (before idle preload has finished), we only
 *     load the popup the user actually clicked — performer-popup vs.
 *     studio-popup — plus the always-needed link-modals.js. The other
 *     popup waits until it's needed. The shared link-modals + the
 *     poster-role-picker still load opportunistically.
 *
 * Public surface:
 *   • window.ensurePopupBundle()   — load every popup script (used by
 *     the idle preloader; harmless if called repeatedly).
 *   • window.openPerformerPopup()  — stub that loads only what's needed
 *     for the performer popup and then dispatches.
 *   • window.openStudioPopup()     — same for the studio popup.
 *   • window.ensurePosterRolePicker() — load poster-role-picker on
 *     demand (called from the performer popup's headshot click).
 */
(function () {
  'use strict';

  /** Cache-bust token kept in sync with the rest of the static JS. */
  var V = '20260612e';

  /** Per-popup script lists. link-modals.js is shared by both. */
  var SCRIPTS = {
    'performer-popup': '/static/performer-popup.js?v=' + V,
    'studio-popup':    '/static/studio-popup.js?v=' + V,
    'link-modals':     '/static/link-modals.js?v=' + V,
    'poster-roles':    '/static/poster-role-picker.js?v=' + V,
  };

  /** Cache promises by URL so a second caller awaits the first load's
   * completion instead of short-circuiting. The previous version
   * checked `document.querySelector('script[data-ts-popup=...]')` and
   * resolved if the tag existed, but a script tag in DOM doesn't mean
   * the script has parsed/executed — a second await could resolve
   * BEFORE `window.openPosterRolePicker` was assigned, sending the
   * performer popup's headshot-click fallback to /library.
   */
  var scriptPromises = {};

  function loadScript(src) {
    if (scriptPromises[src]) return scriptPromises[src];
    scriptPromises[src] = new Promise(function (resolve, reject) {
      var existing = document.querySelector('script[data-ts-popup="' + src + '"]');
      if (existing) {
        // Script tag already in DOM from a previous (now-orphaned) call
        // path. Listen for its load event; if it already fired, the
        // attribute below will be set.
        if (existing.dataset.tsPopupLoaded === '1') { resolve(); return; }
        existing.addEventListener('load', function () { resolve(); }, { once: true });
        existing.addEventListener('error', function () { reject(new Error('Failed to load ' + src)); }, { once: true });
        return;
      }
      var s = document.createElement('script');
      s.src = src;
      s.async = false;
      s.setAttribute('data-ts-popup', src);
      s.onload = function () { s.dataset.tsPopupLoaded = '1'; resolve(); };
      s.onerror = function () { delete scriptPromises[src]; reject(new Error('Failed to load ' + src)); };
      document.head.appendChild(s);
    });
    return scriptPromises[src];
  }

  /** Load each requested src in parallel; throws if any single load
   * errors. Used by both the targeted (performer-only or studio-only)
   * loaders and the full preload. */
  function loadAllParallel(srcs) {
    return Promise.all(srcs.map(loadScript));
  }

  /** Loads only what the performer popup needs. */
  function ensurePerformerPopup() {
    if (window._performerPopupInited) return Promise.resolve();
    return loadAllParallel([SCRIPTS['performer-popup'], SCRIPTS['link-modals']]);
  }

  /** Loads only what the studio popup needs. */
  function ensureStudioPopup() {
    if (typeof window.openStudioPopup === 'function' && window._studioPopupLoaded) {
      return Promise.resolve();
    }
    return loadAllParallel([SCRIPTS['studio-popup'], SCRIPTS['link-modals']]);
  }

  /** Full preload — used at idle and by call sites that don't know
   * which popup they'll need. Parallel across every script. */
  function ensurePopupBundle() {
    if (window._performerPopupInited && typeof window.openStudioPopup === 'function') {
      return Promise.resolve();
    }
    return loadAllParallel([
      SCRIPTS['performer-popup'],
      SCRIPTS['studio-popup'],
      SCRIPTS['link-modals'],
    ]).then(function () {
      // Poster-role-picker isn't required for popup boot — it's lazily
      // pulled in when the user clicks the headshot. Preload it after
      // the popups so a quick headshot click after open doesn't race
      // with the first script tag load.
      return loadScript(SCRIPTS['poster-roles']).catch(function () {});
    });
  }

  function ensurePosterRolePicker() {
    if (typeof window.openPosterRolePicker === 'function') {
      return Promise.resolve();
    }
    return loadScript(SCRIPTS['poster-roles']);
  }
  window.ensurePosterRolePicker = ensurePosterRolePicker;
  window.ensurePopupBundle = ensurePopupBundle;

  /** Stub that loads the targeted bundle then calls the real popup
   * function once the script tags have executed. We tag each stub with
   * `_stubs[name]` so the post-load identity check `fn !== stub` short-
   * circuits the recursion that would otherwise happen when the real
   * implementation hasn't replaced the stub yet (race). */
  function stubPopup(name, ensureFn) {
    return function (opts) {
      return ensureFn().then(function () {
        var fn = window[name];
        if (typeof fn === 'function' && fn !== stubPopup._stubs[name]) {
          return fn(opts);
        }
      });
    };
  }
  stubPopup._stubs = {};

  if (typeof window.openPerformerPopup !== 'function') {
    stubPopup._stubs.openPerformerPopup = stubPopup('openPerformerPopup', ensurePerformerPopup);
    window.openPerformerPopup = stubPopup._stubs.openPerformerPopup;
  }
  if (typeof window.openStudioPopup !== 'function') {
    stubPopup._stubs.openStudioPopup = stubPopup('openStudioPopup', ensureStudioPopup);
    window.openStudioPopup = stubPopup._stubs.openStudioPopup;
  }

  /** Click-time fallback: if the user clicks a `[data-performer-link]`
   * before idle preload completes, the click bubbles here and we
   * dispatch through the performer-targeted ensure. Same for studio. */
  document.addEventListener('click', function (e) {
    if (window._performerPopupInited) return;
    var link = e.target && e.target.closest && e.target.closest('[data-performer-link]');
    if (!link) return;
    e.preventDefault();
    e.stopImmediatePropagation();
    ensurePerformerPopup().then(function () {
      link.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
    });
  }, true);

  /** Idle preload — every page outside /library reaches here without
   * the popup bundle on disk, so the first popup click pays a
   * ~3-script load cost (sequential awaits over a few hundred KB).
   * Kick off the load when the browser is idle so the first click
   * lands on a warm bundle. requestIdleCallback when available; falls
   * back to a 1.5s setTimeout. Network errors are swallowed because
   * the click-time loader will retry. */
  function _idlePreload() {
    try { ensurePopupBundle().catch(function () {}); } catch (_) {}
  }
  if (typeof window.requestIdleCallback === 'function') {
    window.requestIdleCallback(_idlePreload, { timeout: 3000 });
  } else {
    setTimeout(_idlePreload, 1500);
  }
})();
