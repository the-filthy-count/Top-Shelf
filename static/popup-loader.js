/* Lazy-load performer / studio popups + link modals on first use. */
(function () {
  'use strict';
  var BUNDLE = [
    '/static/performer-popup.js',
    '/static/studio-popup.js',
    '/static/link-modals.js',
  ];
  var loadPromise = null;

  /* Cache promises by URL so a second caller awaits the first load's
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

  function ensurePopupBundle() {
    if (window._performerPopupInited && typeof window.openStudioPopup === 'function') {
      return Promise.resolve();
    }
    if (!loadPromise) {
      loadPromise = BUNDLE.reduce(function (p, src) {
        return p.then(function () { return loadScript(src); });
      }, Promise.resolve()).then(function () {
        // Chain poster-role-picker after the bundle so a quick headshot
        // click after the popup opens lands on a ready promise instead
        // of racing the fire-and-forget load.
        return loadScript('/static/poster-role-picker.js').catch(function () {});
      });
    }
    return loadPromise;
  }

  function ensurePosterRolePicker() {
    if (typeof window.openPosterRolePicker === 'function') {
      return Promise.resolve();
    }
    return loadScript('/static/poster-role-picker.js');
  }
  window.ensurePosterRolePicker = ensurePosterRolePicker;

  window.ensurePopupBundle = ensurePopupBundle;

  function stubPopup(name) {
    return function (opts) {
      return ensurePopupBundle().then(function () {
        var fn = window[name];
        if (typeof fn === 'function' && fn !== stubPopup._stubs[name]) {
          return fn(opts);
        }
      });
    };
  }
  stubPopup._stubs = {};

  ['openPerformerPopup', 'openStudioPopup'].forEach(function (name) {
    if (typeof window[name] !== 'function') {
      stubPopup._stubs[name] = stubPopup(name);
      window[name] = stubPopup._stubs[name];
    }
  });

  document.addEventListener('click', function (e) {
    if (window._performerPopupInited) return;
    var link = e.target && e.target.closest && e.target.closest('[data-performer-link]');
    if (!link) return;
    e.preventDefault();
    e.stopImmediatePropagation();
    ensurePopupBundle().then(function () {
      link.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
    });
  }, true);

  /* Idle preload — every page outside /library reaches here without
   * the popup bundle on disk, so the first performer click pays a
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
