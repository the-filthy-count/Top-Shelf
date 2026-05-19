// theme-init.js — first-paint theme apply.
//
// Runs synchronously in <head> before app-shell.css is linked. Order of
// operations on every page load:
//   1. Read the saved theme key from cookie (`ts_ui_theme`) → localStorage
//      → 'vhs' default. Validates against KNOWN_THEMES and collapses
//      aliases ('nord' → 'nord-dark', 'instagram' → 'sunset').
//   2. Stamp html[data-theme="<key>"] so every per-theme rule below it in
//      the cascade resolves to that theme.
//   3. Build one inline <svg> with one <filter> per theme, ids of the
//      form `duotone-<themekey>`. The CSS for each theme references its
//      own `--duotone-filter: url(#duotone-<themekey>)`, so all 22 are
//      defined up-front and the picker is just a data-attribute swap.
//   4. Expose live-apply hooks (window.setUiTheme etc.) for the
//      settings modal in queue.html. setUiTheme also persists the new
//      key into the cookie + localStorage so the next page load reads
//      it before paint.

(function () {
  var DEFAULT_THEME = 'glass';
  var COOKIE_KEY    = 'ts_ui_theme';
  var STORAGE_KEY   = 'ts_ui_theme';
  // Accent override is persisted alongside the theme so every page —
  // not just /queue, where the settings modal lives — picks it up on
  // first paint. Without this, leaving /queue with an override set and
  // navigating to /scenes / /downloads / etc. drops the override
  // entirely until the user re-opens settings.
  var ACCENT_STORAGE_KEY = 'ts_ui_accent_override';

  var KNOWN_THEMES = [
    'dark', 'light',
    'nord-dark', 'nord-light',
    'solarized', 'dracula', 'tokyo-night', 'gruvbox',
    'catppuccin-latte', 'catppuccin-frappe',
    'catppuccin-macchiato', 'catppuccin-mocha',
    'rose-pine', 'rose-pine-dawn',
    'everforest-dark', 'everforest-light',
    'kanagawa', 'monokai-pro',
    'high-contrast', 'sunset',
    'game-station', 'halogen', 'glass',
    'vhs', 'custom',
  ];

  var ALIASES = {
    'nord':      'nord-dark',
    'instagram': 'sunset',
  };

  // Per-theme duotone palette. shadow/highlight RGB tuples are sourced
  // from each theme's `--duotone-lighten` and `--duotone-multiply`
  // declarations in app-shell.css — despite the misleading var names,
  // multiply is the BRIGHT colour and lighten is the DARK colour
  // across every theme block (verified against inline comments). VHS
  // additionally has a tritone midtone; every other theme falls back
  // to a computed average midtone in _filterMarkup().
  var PALETTES = {
    'dark':                 { shadow: [91, 0, 148],   highlight: [255, 77, 198]  },
    'light':                { shadow: [91, 0, 148],   highlight: [255, 77, 198]  },
    'nord-dark':            { shadow: [94, 129, 172], highlight: [143, 188, 187] },
    'nord-light':           { shadow: [94, 129, 172], highlight: [191, 97, 106]  },
    'solarized':            { shadow: [108, 113, 196],highlight: [254, 93, 93]   },
    'dracula':              { shadow: [165, 13, 13],  highlight: [255, 121, 198] },
    'tokyo-night':          { shadow: [35, 102, 164], highlight: [247, 118, 142] },
    'gruvbox':              { shadow: [177, 81, 57],  highlight: [254, 128, 25]  },
    'catppuccin-latte':     { shadow: [30, 102, 245], highlight: [254, 100, 11]  },
    'catppuccin-frappe':    { shadow: [140, 170, 238],highlight: [239, 159, 118] },
    'catppuccin-macchiato': { shadow: [138, 173, 244],highlight: [245, 169, 127] },
    'catppuccin-mocha':     { shadow: [137, 180, 250],highlight: [250, 179, 135] },
    'rose-pine':            { shadow: [49, 116, 143], highlight: [235, 111, 146] },
    'rose-pine-dawn':       { shadow: [40, 105, 131], highlight: [180, 99, 122]  },
    'everforest-dark':      { shadow: [230, 126, 128],highlight: [167, 192, 128] },
    'everforest-light':     { shadow: [248, 85, 82],  highlight: [58, 148, 197]  },
    'kanagawa':             { shadow: [232, 36, 36],  highlight: [149, 127, 184] },
    'monokai-pro':          { shadow: [255, 97, 136], highlight: [120, 220, 232] },
    'high-contrast':        { shadow: [0, 229, 255],  highlight: [255, 215, 0]   },
    'sunset':               { shadow: [91, 81, 216],  highlight: [245, 96, 64]   },
    'game-station':         { shadow: [46, 109, 180], highlight: [243, 195, 0],
                              midtone: [0, 171, 159] },
    'halogen':              { shadow: [27, 38, 44],   highlight: [255, 107, 0]   },
    'glass':                { shadow: [56, 69, 97],   highlight: [135, 166, 186],
                              midtone: [121, 131, 176] },
    'vhs':                  { shadow: [99, 1, 152],   highlight: [254, 67, 217],
                              midtone: [91, 81, 216] },
    'custom':               { shadow: [91, 29, 110],  highlight: [255, 77, 0]    },
  };

  // VHS-tuned tone-mapping defaults. Override per-palette by adding
  // matching keys to a PALETTES entry.
  var DEFAULTS = {
    contrast:         1.80,
    shadowBalance:    0.00,
    highlightBalance: 0.56,
    alphaShadow:      0.90,
    alphaMid:         0.85,
    alphaHi:          0.65,
  };

  // Rec.709 luminance weights — same as CSS grayscale(1) does internally.
  var LUM = '0.2126 0.7152 0.0722 0 0 0.2126 0.7152 0.0722 0 0 0.2126 0.7152 0.0722 0 0 0 0 0 1 0';

  function _n255(v) { return (v / 255).toFixed(4); }

  function _readCookie(name) {
    try {
      var m = document.cookie.match(new RegExp('(?:^|;\\s*)' + name + '=([^;]*)'));
      return m ? decodeURIComponent(m[1]) : null;
    } catch (_) { return null; }
  }

  function _resolveAuto() {
    // 'auto' tracks the OS dark/light preference. Resolved at apply
    // time so a system theme switch on next page load picks up.
    try {
      if (window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches) {
        return 'light';
      }
    } catch (_) {}
    return 'dark';
  }

  function _readSavedTheme() {
    var raw = _readCookie(COOKIE_KEY);
    if (!raw) {
      try { raw = window.localStorage.getItem(STORAGE_KEY); } catch (_) {}
    }
    if (!raw) return DEFAULT_THEME;
    raw = String(raw).trim().toLowerCase();
    if (raw === 'auto') return _resolveAuto();
    if (ALIASES[raw]) raw = ALIASES[raw];
    return KNOWN_THEMES.indexOf(raw) >= 0 ? raw : DEFAULT_THEME;
  }

  function _persistTheme(themeKey) {
    try {
      document.cookie = COOKIE_KEY + '=' + encodeURIComponent(themeKey)
        + '; path=/; max-age=' + (60 * 60 * 24 * 365) + '; samesite=lax';
    } catch (_) {}
    try { window.localStorage.setItem(STORAGE_KEY, themeKey); } catch (_) {}
  }

  function _filterMarkup(themeKey, palette) {
    var sh = palette.shadow;
    var hi = palette.highlight;
    var mid = palette.midtone || [
      Math.round((sh[0] + hi[0]) / 2),
      Math.round((sh[1] + hi[1]) / 2),
      Math.round((sh[2] + hi[2]) / 2),
    ];
    var contrast = palette.contrast         != null ? palette.contrast         : DEFAULTS.contrast;
    var sBal     = palette.shadowBalance    != null ? palette.shadowBalance    : DEFAULTS.shadowBalance;
    var hBal     = palette.highlightBalance != null ? palette.highlightBalance : DEFAULTS.highlightBalance;
    var alphaSh  = palette.alphaShadow      != null ? palette.alphaShadow      : DEFAULTS.alphaShadow;
    var alphaMid = palette.alphaMid         != null ? palette.alphaMid         : DEFAULTS.alphaMid;
    var alphaHi  = palette.alphaHi          != null ? palette.alphaHi          : DEFAULTS.alphaHi;

    function tbl(c) {
      var s1 = Math.round(sh[c] * sBal + mid[c] * (1 - sBal));
      var h1 = Math.round(mid[c] * (1 - hBal) + hi[c] * hBal);
      return _n255(sh[c]) + ' ' + _n255(s1) + ' ' + _n255(mid[c]) + ' '
        + _n255(h1) + ' ' + _n255(hi[c]);
    }

    var alphaS1  = (alphaSh * sBal + alphaMid * (1 - sBal)).toFixed(4);
    var alphaH1  = (alphaMid * (1 - hBal) + alphaHi * hBal).toFixed(4);
    var alphaTbl = alphaSh.toFixed(4) + ' ' + alphaS1 + ' ' + alphaMid.toFixed(4)
      + ' ' + alphaH1 + ' ' + alphaHi.toFixed(4);
    var slope     = contrast;
    var intercept = (1 - contrast) / 2;

    return '<filter id="duotone-' + themeKey + '" color-interpolation-filters="sRGB">'
      + '<feColorMatrix type="matrix" values="' + LUM + '"/>'
      + '<feComponentTransfer>'
      +   '<feFuncR type="linear" slope="' + slope + '" intercept="' + intercept + '"/>'
      +   '<feFuncG type="linear" slope="' + slope + '" intercept="' + intercept + '"/>'
      +   '<feFuncB type="linear" slope="' + slope + '" intercept="' + intercept + '"/>'
      + '</feComponentTransfer>'
      + '<feColorMatrix type="matrix" values="1 0 0 0 0  0 1 0 0 0  0 0 1 0 0  1 0 0 0 0"/>'
      + '<feComponentTransfer>'
      +   '<feFuncR type="table" tableValues="' + tbl(0) + '"/>'
      +   '<feFuncG type="table" tableValues="' + tbl(1) + '"/>'
      +   '<feFuncB type="table" tableValues="' + tbl(2) + '"/>'
      +   '<feFuncA type="table" tableValues="' + alphaTbl + '"/>'
      + '</feComponentTransfer>'
      + '</filter>';
  }

  function _allFiltersMarkup() {
    var out = '';
    for (var key in PALETTES) {
      if (Object.prototype.hasOwnProperty.call(PALETTES, key)) {
        out += _filterMarkup(key, PALETTES[key]);
      }
    }
    return out;
  }

  function _ensureDuotoneSvg() {
    var existing = document.getElementById('duotone-svg-defs');
    if (existing && existing.parentNode) existing.parentNode.removeChild(existing);
    var svg = '<svg id="duotone-svg-defs" width="0" height="0" '
      + 'style="position:absolute;width:0;height:0;pointer-events:none" '
      + 'aria-hidden="true"><defs>' + _allFiltersMarkup() + '</defs></svg>';
    var parent = document.body || document.documentElement;
    var wrapper = document.createElement('div');
    wrapper.innerHTML = svg;
    parent.appendChild(wrapper.firstChild);
  }

  function _applyTheme(themeKey) {
    var t = themeKey;
    if (ALIASES[t]) t = ALIASES[t];
    if (!PALETTES[t]) t = DEFAULT_THEME;
    document.documentElement.setAttribute('data-theme', t);
    _ensureDuotoneSvg();
    return t;
  }

  // First-paint apply.
  _applyTheme(_readSavedTheme());

  // ── Live-apply API used by the settings modal ──────────────────────
  // Calls happen during interactive theme switching in queue.html
  // (lines ~3565, 3860, 3870, 4388). Each call updates the DOM AND
  // persists the new key so the next page load reads it from the
  // cookie before paint.

  window.setUiTheme = function (themeKey, customSpec) {
    var key = themeKey || DEFAULT_THEME;
    // 'auto' is a meta-key, not a real theme — persist the literal
    // 'auto' so reloads continue to track the OS pref, but apply the
    // resolved real theme to the DOM right now.
    var persistKey = key;
    if (key === 'auto') key = _resolveAuto();
    if (key === 'custom' && customSpec && typeof customSpec === 'object') {
      // The custom spec ships its own shadow/highlight RGBs. Update
      // PALETTES['custom'] before rebuilding so #duotone-custom uses
      // the live values, not the stale ones from page load.
      var sh  = customSpec.shadow_rgb    || customSpec.shadow    || customSpec.duotone_shadow;
      var hi  = customSpec.highlight_rgb || customSpec.highlight || customSpec.duotone_highlight;
      var mid = customSpec.midtone_rgb   || customSpec.midtone   || customSpec.duotone_midtone;
      if (sh && hi) {
        PALETTES['custom'] = { shadow: sh, highlight: hi };
        if (mid) PALETTES['custom'].midtone = mid;
      }
      // Custom theme can opt into a light-base — app-shell.css scopes
      // its light overrides as `[data-theme="custom"][data-base="light"]`.
      if (customSpec.base === 'light') {
        document.documentElement.setAttribute('data-base', 'light');
      } else {
        document.documentElement.removeAttribute('data-base');
      }
    } else {
      document.documentElement.removeAttribute('data-base');
    }
    var applied = _applyTheme(key);
    _persistTheme(persistKey === 'auto' ? 'auto' : applied);
    return applied;
  };

  function _hexToRgbTriple(hex) {
    var s = String(hex || '').trim();
    if (s.charAt(0) === '#') s = s.slice(1);
    if (s.length === 3) {
      s = s.charAt(0) + s.charAt(0) + s.charAt(1) + s.charAt(1) + s.charAt(2) + s.charAt(2);
    }
    if (!/^[0-9a-fA-F]{6}$/.test(s)) return null;
    return parseInt(s.slice(0, 2), 16) + ', ' +
           parseInt(s.slice(2, 4), 16) + ', ' +
           parseInt(s.slice(4, 6), 16);
  }

  function _applyAccentOverrideStyle(hex, triple) {
    document.documentElement.setAttribute('data-accent-override', hex);
    var prev = document.getElementById('accent-override-style');
    if (prev && prev.parentNode) prev.parentNode.removeChild(prev);
    var style = document.createElement('style');
    style.id = 'accent-override-style';
    style.textContent =
      ':root, html[data-accent-override] {' +
      '  --accent: ' + hex + ' !important;' +
      '  --brand-accent-rgb: ' + triple + ' !important;' +
      '  --skyfont-color: ' + hex + ' !important;' +
      '}';
    document.head.appendChild(style);
  }

  window.setAccentOverride = function (hex) {
    if (!hex) return;
    var triple = _hexToRgbTriple(hex);
    if (!triple) return;
    _applyAccentOverrideStyle(hex, triple);
    // Persist so every other page picks the override up on its own
    // first paint instead of relying on queue.html's settings load.
    try { window.localStorage.setItem(ACCENT_STORAGE_KEY, hex); } catch (_) {}
  };

  window.clearAccentOverride = function () {
    document.documentElement.removeAttribute('data-accent-override');
    var prev = document.getElementById('accent-override-style');
    if (prev && prev.parentNode) prev.parentNode.removeChild(prev);
    try { window.localStorage.removeItem(ACCENT_STORAGE_KEY); } catch (_) {}
  };

  // First-paint apply of any persisted accent override. Runs after
  // _applyTheme above so the override's !important rules win against
  // whatever the theme block declared for `--accent` /
  // `--brand-accent-rgb`. Wrapped in a try so a localStorage failure
  // (private mode etc.) just skips the override silently.
  try {
    var savedAccent = window.localStorage.getItem(ACCENT_STORAGE_KEY);
    if (savedAccent) {
      var savedTriple = _hexToRgbTriple(savedAccent);
      if (savedTriple) _applyAccentOverrideStyle(savedAccent, savedTriple);
    }
  } catch (_) {}

})();
