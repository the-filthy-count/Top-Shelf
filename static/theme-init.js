// theme-init.js — synchronous early theme application. Loaded in <head>.
//
// Adding a theme:
//   1. Add a html[data-theme="..."] block to /static/app-shell.css.
//   2. Add the key to THEMES below.
//   3. Add an <option value="..."> to the Appearance panel in queue.html.
//
// Runtime API:
//   window.setUiTheme(name, customSpec?) — switch theme live. When name is
//   'custom', customSpec is an object:
//     { base: 'dark'|'light',
//       brand_purple: '#RRGGBB', brand_accent: '#RRGGBB',
//       bg_panel: '#RRGGBB', panel_hi: '#RRGGBB', panel_lo: '#RRGGBB',
//       duotone_multiply: '#RRGGBB', duotone_lighten: '#RRGGBB',
//       // ── Advanced duotone fields (all optional) ──
//       duotone_mode_multiply: 'multiply'|'screen'|'overlay'|…,
//       duotone_mode_lighten:  'lighten' |'screen'|'overlay'|…,
//       duotone_grayscale:   0–1 (number),
//       duotone_contrast:    number (1 = no change, >1 = more contrast),
//       duotone_saturation:  number (1 = no change),
//       duotone_exposure:    number (1 = no change, >1 = brighter),
//       duotone_opacity:     0–1 (overall strength of both layers combined) }
//   The hex / number / string values land in a single <style
//   id="custom-theme-style"> tag as CSS custom properties.

(function () {
  // 'auto' is a stored preference — at apply time it resolves to 'dark' or
  // 'light' based on prefers-color-scheme. It's not a CSS theme selector.
  var THEMES = ['auto', 'dark', 'light', 'nord-dark', 'nord-light', 'solarized', 'dracula', 'tokyo-night', 'gruvbox', 'catppuccin-latte', 'catppuccin-frappe', 'catppuccin-macchiato', 'catppuccin-mocha', 'rose-pine', 'rose-pine-dawn', 'everforest-dark', 'everforest-light', 'kanagawa', 'monokai-pro', 'high-contrast', 'sunset', 'vhs', 'custom'];
  // Legacy theme keys → current canonical key. Users who saved
  // ``nord`` before the split into Nord Dark / Nord Light still get
  // their theme honoured instead of being silently reset to default.
  // ``instagram`` was renamed to ``sunset`` to avoid the brand name.
  var THEME_ALIASES = { 'nord': 'nord-dark', 'instagram': 'sunset' };
  var DEFAULT_THEME = 'vhs';
  var STYLE_TAG_ID = 'custom-theme-style';

  function systemPrefersDark() {
    try {
      return !!(window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches);
    } catch (e) { return true; }
  }
  function resolveTheme(t) {
    return t === 'auto' ? (systemPrefersDark() ? 'dark' : 'light') : t;
  }

  function hexToRgbTriple(hex, fallback) {
    if (!hex) return fallback;
    var m = String(hex).trim().replace('#', '');
    if (m.length === 3) m = m[0]+m[0]+m[1]+m[1]+m[2]+m[2];
    if (!/^[0-9a-fA-F]{6}$/.test(m)) return fallback;
    var r = parseInt(m.slice(0,2), 16);
    var g = parseInt(m.slice(2,4), 16);
    var b = parseInt(m.slice(4,6), 16);
    return r + ', ' + g + ', ' + b;
  }

  function removeCustomStyleTag() {
    var el = document.getElementById(STYLE_TAG_ID);
    if (el && el.parentNode) el.parentNode.removeChild(el);
  }

  function safeHex(hex, fallback) {
    if (!hex) return fallback;
    var m = String(hex).trim();
    if (!/^#[0-9a-fA-F]{3}([0-9a-fA-F]{3})?$/.test(m)) return fallback;
    return m;
  }

  var _BLEND_MODES = {
    'normal': 1, 'multiply': 1, 'screen': 1, 'overlay': 1,
    'darken': 1, 'lighten': 1, 'color-dodge': 1, 'color-burn': 1,
    'hard-light': 1, 'soft-light': 1, 'difference': 1, 'exclusion': 1,
    'hue': 1, 'saturation': 1, 'color': 1, 'luminosity': 1,
    'plus-lighter': 1
  };
  function safeBlend(mode, fallback) {
    if (!mode) return fallback;
    var m = String(mode).trim().toLowerCase();
    return _BLEND_MODES[m] ? m : fallback;
  }
  function safeNum(v, fallback, lo, hi) {
    var n = parseFloat(v);
    if (!isFinite(n)) return fallback;
    if (lo != null && n < lo) n = lo;
    if (hi != null && n > hi) n = hi;
    return n;
  }

  function injectCustomStyle(spec) {
    // !important is required because this stylesheet must win over app-shell.css's
    // html[data-theme="custom"] fallback block. theme-init.js runs synchronously in
    // <head> *before* app-shell.css is linked, so the injected <style> lands earlier
    // in DOM order than app-shell.css and would otherwise lose the cascade.
    var s = spec || {};
    var css = 'html[data-theme="custom"]{' +
      '--brand-purple-rgb:' + hexToRgbTriple(s.brand_purple, '155, 77, 202') + ' !important;' +
      '--brand-accent-rgb:' + hexToRgbTriple(s.brand_accent, '217, 74, 108') + ' !important;' +
      '--bg-rgb:' + hexToRgbTriple(s.bg_panel, '12, 12, 14') + ' !important;' +
      '--panel-hi-rgb:' + hexToRgbTriple(s.panel_hi, '36, 36, 48') + ' !important;' +
      '--panel-lo-rgb:' + hexToRgbTriple(s.panel_lo, '18, 18, 26') + ' !important;' +
      '--duotone-multiply:' + safeHex(s.duotone_multiply, '#ff4d00') + ' !important;' +
      '--duotone-lighten:' + safeHex(s.duotone_lighten, '#5b1d6e') + ' !important;' +
      // ── Advanced duotone tuning ──
      '--duotone-mode-multiply:' + safeBlend(s.duotone_mode_multiply, 'multiply') + ' !important;' +
      '--duotone-mode-lighten:' + safeBlend(s.duotone_mode_lighten, 'lighten') + ' !important;' +
      '--duotone-grayscale:' + safeNum(s.duotone_grayscale, 1, 0, 1) + ' !important;' +
      '--duotone-contrast:' + safeNum(s.duotone_contrast, 1.08, 0.5, 3) + ' !important;' +
      '--duotone-saturation:' + safeNum(s.duotone_saturation, 1, 0, 3) + ' !important;' +
      '--duotone-exposure:' + safeNum(s.duotone_exposure, 1, 0.3, 2.5) + ' !important;' +
      '--duotone-opacity:' + safeNum(s.duotone_opacity, 1, 0, 1) + ' !important;' +
      '}';
    var tag = document.getElementById(STYLE_TAG_ID);
    if (!tag) {
      tag = document.createElement('style');
      tag.id = STYLE_TAG_ID;
      (document.head || document.documentElement).appendChild(tag);
    }
    tag.textContent = css;
  }

  // ── SVG duotone filters ─────────────────────────────────
  // Each named theme has a pre-built filter that maps the image's
  // luminance → interpolated colour between shadow and highlight,
  // with a per-theme contrast push and midpoint gamma to dial in
  // the look. Same machinery the custom theme uses — just frozen
  // values baked in at page load.
  //
  // Each entry: [shadowRGB, highlightRGB, contrast, midpoint].
  //   contrast:  0.5..2.5 — pre-duotone slope on the grey values.
  //              1 = linear, >1 punches shadows/highlights apart.
  //   midpoint:  0.1..0.9 — gamma-shifts which luminance lands at
  //              the gradient's 50 % point. 0.5 = straight linear.
  var DUOTONE_PAIRS = {
    'dark':                 [[91,0,148],    [255,77,198],  2.00, 0.35],
    'light':                [[91,0,148],    [255,77,198],  2.00, 0.35],
    'nord':                 [[94,129,172],  [143,188,187], 1.50, 0.50],
    'solarized':            [[108,113,196], [254,93,93],   1.50, 0.50],
    'dracula':              [[165,13,13],   [255,121,198], 1.50, 0.50],
    'tokyo-night':          [[35,102,164],  [247,118,142], 2.50, 0.50],
    'gruvbox':              [[177,81,57],   [254,128,25],  1.55, 0.14],
    // Legacy alias — keeps ``setDuotoneOverride('nord', …)`` working
    // for any setting JSON written before the split. The CSS side has
    // a matching `html[data-theme="nord"]` block selecting nord-dark,
    // so the active filter URL still resolves.
    'nord':                 [[94,129,172],  [143,188,187], 1.50, 0.50],
    // Catppuccin flavour-authentic Blue + Peach pairings — classic
    // warm/cool duotone using each flavour's own shade of those tokens.
    'catppuccin-latte':     [[30,102,245],  [254,100,11],  1.50, 0.50],
    'catppuccin-frappe':    [[140,170,238], [239,159,118], 1.50, 0.50],
    'catppuccin-macchiato': [[138,173,244], [245,169,127], 1.50, 0.50],
    'catppuccin-mocha':     [[137,180,250], [250,179,135], 1.50, 0.50],
    // New themes:
    'nord-dark':            [[94,129,172],  [143,188,187], 1.50, 0.50],
    'nord-light':           [[94,129,172],  [191,97,106],  1.55, 0.50],
    'rose-pine':            [[49,116,143],  [235,111,146], 1.50, 0.50],
    'rose-pine-dawn':       [[40,105,131],  [180,99,122],  1.50, 0.50],
    'everforest-dark':      [[230,126,128], [167,192,128], 1.55, 0.45],
    'everforest-light':     [[248,85,82],   [58,148,197],  1.55, 0.45],
    'kanagawa':             [[232,36,36],   [149,127,184], 1.65, 0.45],
    'monokai-pro':          [[255,97,136],  [120,220,232], 1.60, 0.45],
    // High contrast: deliberately punchy gold→cyan with a strong slope.
    'high-contrast':        [[0,229,255],   [255,215,0],   2.20, 0.50],
    // Sunset: cool blue-purple shadows lifting to warm orange
    // highlights — gradient inspired by social-feed brand marks
    // (#5b51d8 → #f56040). Legacy ``instagram`` key aliases here.
    'sunset':               [[91,81,216],   [245,96,64],   1.70, 0.45],
    // VHS: deep purple shadows blooming to hot magenta highlights.
    // Cyan is reserved for chromatic-aberration accents on the
    // image surfaces (added in the surface-treatments phase).
    'vhs':                  [[58,12,94],    [255,54,168],  1.75, 0.45]
  };
  // Rec.709 luminance weights — same as CSS grayscale(1) does internally.
  var _LUM = '0.2126 0.7152 0.0722 0 0 0.2126 0.7152 0.0722 0 0 0.2126 0.7152 0.0722 0 0 0 0 0 1 0';

  function _n255(v) { return (v / 255).toFixed(4); }

  function _duotoneFilterMarkup(id, shadow, highlight, contrast, midpoint) {
    var c  = (contrast == null) ? 1   : contrast;
    var mp = (midpoint == null) ? 0.5 : midpoint;
    // Pre-duotone contrast push — linear slope/intercept centred on 0.5.
    var contrastStep = '';
    if (Math.abs(c - 1) > 0.001) {
      var slope = c;
      var intercept = (1 - c) / 2;
      contrastStep = '<feComponentTransfer>'
        + '<feFuncR type="linear" slope="' + slope + '" intercept="' + intercept + '"/>'
        + '<feFuncG type="linear" slope="' + slope + '" intercept="' + intercept + '"/>'
        + '<feFuncB type="linear" slope="' + slope + '" intercept="' + intercept + '"/>'
        + '</feComponentTransfer>';
    }
    // Midpoint gamma — shifts which luminance lands at the gradient's
    // 50 % point. gamma = log(0.5)/log(mp) satisfies lum=mp → out=0.5.
    var gammaStep = '';
    if (Math.abs(mp - 0.5) > 0.001) {
      var gamma = Math.log(0.5) / Math.log(mp);
      gammaStep = '<feComponentTransfer>'
        + '<feFuncR type="gamma" exponent="' + gamma + '"/>'
        + '<feFuncG type="gamma" exponent="' + gamma + '"/>'
        + '<feFuncB type="gamma" exponent="' + gamma + '"/>'
        + '</feComponentTransfer>';
    }
    return '<filter id="' + id + '" color-interpolation-filters="sRGB">'
      + '<feColorMatrix type="matrix" values="' + _LUM + '"/>'
      + contrastStep
      + gammaStep
      + '<feComponentTransfer>'
      + '<feFuncR type="table" tableValues="' + _n255(shadow[0]) + ' ' + _n255(highlight[0]) + '"/>'
      + '<feFuncG type="table" tableValues="' + _n255(shadow[1]) + ' ' + _n255(highlight[1]) + '"/>'
      + '<feFuncB type="table" tableValues="' + _n255(shadow[2]) + ' ' + _n255(highlight[2]) + '"/>'
      + '</feComponentTransfer>'
      + '</filter>';
  }

  function _hexToRgbArray(hex, fallback) {
    if (!hex) return fallback;
    var m = String(hex).trim().replace('#', '');
    if (m.length === 3) m = m[0]+m[0]+m[1]+m[1]+m[2]+m[2];
    if (!/^[0-9a-fA-F]{6}$/.test(m)) return fallback;
    return [parseInt(m.slice(0,2),16), parseInt(m.slice(2,4),16), parseInt(m.slice(4,6),16)];
  }

  function ensureDuotoneSvg() {
    if (document.getElementById('duotone-svg-defs')) return;
    var filters = '';
    for (var name in DUOTONE_PAIRS) {
      var pair = DUOTONE_PAIRS[name];
      filters += _duotoneFilterMarkup('duotone-' + name, pair[0], pair[1], pair[2], pair[3]);
    }
    // Placeholder for custom theme — updateCustomDuotoneFilter() overwrites.
    filters += _duotoneFilterMarkup('duotone-custom', [91,29,110], [255,77,0], 1, 0.5);
    var svg = '<svg id="duotone-svg-defs" width="0" height="0" style="position:absolute;width:0;height:0;pointer-events:none" aria-hidden="true"><defs>' + filters + '</defs></svg>';
    var parent = document.body || document.documentElement;
    var wrapper = document.createElement('div');
    wrapper.innerHTML = svg;
    parent.appendChild(wrapper.firstChild);
  }

  function _clamp(n, fb, lo, hi) {
    if (typeof n !== 'number' || !isFinite(n)) return fb;
    if (n < lo) return lo; if (n > hi) return hi; return n;
  }

  // Per-channel hex → '#rrggbb'. Used when exposing built-in defaults.
  function _rgbArrayToHex(rgb) {
    if (!rgb) return '#000000';
    function h(n) { var s = Math.max(0, Math.min(255, n|0)).toString(16); return s.length < 2 ? '0' + s : s; }
    return '#' + h(rgb[0]) + h(rgb[1]) + h(rgb[2]);
  }

  // Rewrite a filter element with the given spec. `nodeId` is the SVG
  // <filter>'s id — 'duotone-custom' for the custom theme, or
  // 'duotone-<builtin>' for any named theme we want to override live.
  function _rebuildDuotoneFilterNode(nodeId, spec) {
    var s = spec || {};
    var shadow = _hexToRgbArray(s.duotone_lighten, [91,29,110]);
    var highlight = _hexToRgbArray(s.duotone_multiply, [255,77,0]);
    var midEnabled = !!s.duotone_midtone_enabled;
    var midtone = midEnabled ? _hexToRgbArray(s.duotone_midtone, null) : null;
    var intensity = _clamp(s.duotone_opacity, 1, 0, 1);
    var contrast  = _clamp(s.duotone_contrast, 1, 0.5, 2.5);
    var midpoint  = _clamp(s.duotone_midpoint, 0.5, 0.1, 0.9);
    var shadowBal    = _clamp(s.duotone_shadow_balance,    0.5, 0, 1);
    var highlightBal = _clamp(s.duotone_highlight_balance, 0.5, 0, 1);

    // Intensity blends each channel's endpoints toward the identity
    // grayscale mapping (shadow→0, highlight→255, midtone→128). At I=1
    // the full palette duotone applies; at I=0 the image passes through
    // as neutral grayscale.
    function blend(val, identity) {
      return Math.round(val * intensity + identity * (1 - intensity));
    }
    var sAdj = [blend(shadow[0],0),   blend(shadow[1],0),   blend(shadow[2],0)];
    var hAdj = [blend(highlight[0],255), blend(highlight[1],255), blend(highlight[2],255)];
    var mAdj = midtone
      ? [blend(midtone[0],128), blend(midtone[1],128), blend(midtone[2],128)]
      : null;

    // tritone: 5-entry table at evenly-spaced luminance positions [0,.25,.5,.75,1].
    // Band widths are controlled by the balance sliders — higher = pure
    // endpoint colour takes more of its half, compressing the midtone
    // transitions into narrower regions around the 0.5 midpoint.
    function tbl(ch) {
      if (mAdj) {
        var s1 = Math.round(sAdj[ch] * shadowBal    + mAdj[ch] * (1 - shadowBal));
        var h1 = Math.round(mAdj[ch] * (1 - highlightBal) + hAdj[ch] * highlightBal);
        return _n255(sAdj[ch]) + ' ' + _n255(s1) + ' ' + _n255(mAdj[ch]) + ' ' + _n255(h1) + ' ' + _n255(hAdj[ch]);
      }
      return _n255(sAdj[ch]) + ' ' + _n255(hAdj[ch]);
    }

    // Gamma that shifts the duotone midpoint to a chosen luminance.
    // Only meaningful for pure duotone — the 5-entry tritone has the
    // midpoint nailed at 0.5 by construction.
    var gamma = (midEnabled || Math.abs(midpoint - 0.5) < 0.001)
      ? 1
      : (Math.log(0.5) / Math.log(midpoint));

    // Pre-map contrast: linear slope + intercept centred on 0.5.
    var cSlope = contrast;
    var cIntercept = (1 - contrast) / 2;

    var node = document.getElementById(nodeId);
    if (!node) { ensureDuotoneSvg(); node = document.getElementById(nodeId); }
    if (!node) return;
    while (node.firstChild) node.removeChild(node.firstChild);
    var NS = 'http://www.w3.org/2000/svg';
    var ch = ['R','G','B'];

    // 1. Rec.709 luminance reduction.
    var lum = document.createElementNS(NS, 'feColorMatrix');
    lum.setAttribute('type', 'matrix'); lum.setAttribute('values', _LUM);
    node.appendChild(lum);

    // 2. Contrast (pre-duotone linear push on the grey values).
    if (Math.abs(contrast - 1) > 0.001) {
      var cct = document.createElementNS(NS, 'feComponentTransfer');
      for (var i = 0; i < 3; i++) {
        var f = document.createElementNS(NS, 'feFunc' + ch[i]);
        f.setAttribute('type', 'linear');
        f.setAttribute('slope', String(cSlope));
        f.setAttribute('intercept', String(cIntercept));
        cct.appendChild(f);
      }
      node.appendChild(cct);
    }

    // 3. Midpoint gamma (duotone mode only — tritone has a fixed 0.5 midpoint).
    if (Math.abs(gamma - 1) > 0.001) {
      var gct = document.createElementNS(NS, 'feComponentTransfer');
      for (var j = 0; j < 3; j++) {
        var g = document.createElementNS(NS, 'feFunc' + ch[j]);
        g.setAttribute('type', 'gamma');
        g.setAttribute('exponent', String(gamma));
        gct.appendChild(g);
      }
      node.appendChild(gct);
    }

    // 4. Duotone / tritone table map.
    var ct = document.createElementNS(NS, 'feComponentTransfer');
    for (var k = 0; k < 3; k++) {
      var t = document.createElementNS(NS, 'feFunc' + ch[k]);
      t.setAttribute('type', 'table');
      t.setAttribute('tableValues', tbl(k));
      ct.appendChild(t);
    }
    node.appendChild(ct);
  }

  // Back-compat wrapper for the single-filter custom-theme case.
  function updateCustomDuotoneFilter(spec) {
    _rebuildDuotoneFilterNode('duotone-custom', spec);
  }

  // Built-in theme defaults → a spec object the settings UI can load
  // into its duotone fields. Every field the Duotone section reads is
  // populated so the form starts in a sane state.
  function _defaultSpecForTheme(themeKey) {
    var pair = DUOTONE_PAIRS[themeKey];
    if (!pair) return null;
    return {
      duotone_lighten:           _rgbArrayToHex(pair[0]),
      duotone_multiply:          _rgbArrayToHex(pair[1]),
      duotone_contrast:          pair[2] != null ? pair[2] : 1,
      duotone_midpoint:          pair[3] != null ? pair[3] : 0.5,
      duotone_opacity:           1,
      duotone_midtone:           '#808080',
      duotone_midtone_enabled:   false,
      duotone_shadow_balance:    0.5,
      duotone_highlight_balance: 0.5
    };
  }

  // Per-theme CSS-variable overrides. Studios and other surfaces that
  // still rely on the pre-SVG duotone tokens (--duotone-multiply /
  // --duotone-lighten, plus the tritone --duotone-midtone) read them
  // directly off html[data-theme="<key>"], so when the user tunes a
  // theme's duotone in settings we have to write the chosen colours
  // into a scoped <style> block alongside rebuilding the SVG filter.
  var DUOTONE_VAR_STYLE_ID = 'duotone-var-overrides';
  var _duotoneVarBlocks = {};  // { themeKey: "html[data-theme=...]{...}" }
  function _writeDuotoneVarOverrides() {
    var tag = document.getElementById(DUOTONE_VAR_STYLE_ID);
    if (!tag) {
      tag = document.createElement('style');
      tag.id = DUOTONE_VAR_STYLE_ID;
      (document.head || document.documentElement).appendChild(tag);
    }
    var out = '';
    for (var k in _duotoneVarBlocks) {
      if (_duotoneVarBlocks[k]) out += _duotoneVarBlocks[k];
    }
    tag.textContent = out;
  }
  function _updateDuotoneVarsForTheme(themeKey, spec) {
    var s = spec || {};
    var mul = safeHex(s.duotone_multiply, null);
    var lig = safeHex(s.duotone_lighten,  null);
    var midEnabled = !!s.duotone_midtone_enabled;
    var midCol = midEnabled ? safeHex(s.duotone_midtone, null) : null;
    if (!mul && !lig && !midCol) { delete _duotoneVarBlocks[themeKey]; _writeDuotoneVarOverrides(); return; }
    var sel = themeKey === 'custom' ? 'html[data-theme="custom"]' : ('html[data-theme="' + themeKey + '"]');
    var body = '';
    if (mul) body += '--duotone-multiply:' + mul + ' !important;';
    if (lig) body += '--duotone-lighten:'  + lig + ' !important;';
    if (midCol) {
      // Tritone midtone — exposed for CSS surfaces that want a three-stop
      // gradient. Also nudges the studio gradient by replacing the solid
      // `linear-gradient(multiply,multiply)` layer with a 3-stop so the
      // midtone shows through on studio tile backgrounds.
      body += '--duotone-midtone:' + midCol + ' !important;';
    }
    _duotoneVarBlocks[themeKey] = sel + '{' + body + '}';
    _writeDuotoneVarOverrides();
  }

  // Public surface for queue.html's settings panel:
  //   setDuotoneOverride — rewrite a named theme's filter live with a spec.
  //   resetDuotoneFilter — reset a named theme back to its built-in values.
  //   getDuotoneDefaults — read the built-in spec for a theme (for UI prefill).
  window.setDuotoneOverride = function (themeKey, spec) {
    if (!themeKey) return;
    _rebuildDuotoneFilterNode('duotone-' + themeKey, spec);
    _updateDuotoneVarsForTheme(themeKey, spec);
  };
  window.resetDuotoneFilter = function (themeKey) {
    var pair = DUOTONE_PAIRS[themeKey];
    if (!pair) return;
    var node = document.getElementById('duotone-' + themeKey);
    if (!node) { ensureDuotoneSvg(); node = document.getElementById('duotone-' + themeKey); }
    if (!node) return;
    // Rebuild from scratch with the declared pair — bypasses the spec path.
    node.outerHTML = _duotoneFilterMarkup('duotone-' + themeKey, pair[0], pair[1], pair[2], pair[3]);
    delete _duotoneVarBlocks[themeKey];
    _writeDuotoneVarOverrides();
  };
  window.getDuotoneDefaults = _defaultSpecForTheme;

  // ── Accent override ─────────────────────────────────────────────
  // Lets a user keep a chosen theme's panel/background but swap
  // ``--brand-accent-rgb`` (and ``--accent`` / ``--red`` / ``--danger``,
  // which most themes derive from it) to any colour. Cheaper than
  // building a full Custom theme for the "I just want pink instead
  // of teal" case. A blank/null override clears the rule so the
  // active theme's own accent reasserts.
  var ACCENT_STYLE_TAG_ID = 'accent-override-style';
  function _applyAccentOverride(hex) {
    var tag = document.getElementById(ACCENT_STYLE_TAG_ID);
    var safe = safeHex(hex, null);
    if (!safe) {
      if (tag && tag.parentNode) tag.parentNode.removeChild(tag);
      return;
    }
    var triple = hexToRgbTriple(safe, null);
    if (!triple) {
      if (tag && tag.parentNode) tag.parentNode.removeChild(tag);
      return;
    }
    // Wide selector so the override beats every per-theme block. We
    // only repaint the accent triple + the three derived solid tokens
    // (--accent / --red / --danger); leaving --brand-purple-rgb and
    // panel tokens alone preserves the rest of the theme. !important
    // so per-theme [data-theme="…"] declarations don't outweigh us.
    var css =
      'html[data-accent-override="1"]{' +
        '--brand-accent-rgb:' + triple + ' !important;' +
        '--accent:' + safe + ' !important;' +
        '--red:'    + safe + ' !important;' +
        '--danger:' + safe + ' !important;' +
      '}';
    if (!tag) {
      tag = document.createElement('style');
      tag.id = ACCENT_STYLE_TAG_ID;
      (document.head || document.documentElement).appendChild(tag);
    }
    tag.textContent = css;
    document.documentElement.setAttribute('data-accent-override', '1');
  }
  function _clearAccentOverride() {
    document.documentElement.removeAttribute('data-accent-override');
    var tag = document.getElementById(ACCENT_STYLE_TAG_ID);
    if (tag && tag.parentNode) tag.parentNode.removeChild(tag);
  }

  // Public surface for the Appearance settings panel.
  window.setAccentOverride = function (hex) {
    var safe = safeHex(hex, null);
    if (!safe) {
      _clearAccentOverride();
      try { localStorage.removeItem('ui_accent_override'); } catch (e) {}
      return;
    }
    _applyAccentOverride(safe);
    try { localStorage.setItem('ui_accent_override', safe); } catch (e) {}
  };
  window.clearAccentOverride = function () { window.setAccentOverride(null); };

  function apply(t, spec) {
    if (t && THEME_ALIASES[t]) t = THEME_ALIASES[t];
    if (THEMES.indexOf(t) === -1) t = DEFAULT_THEME;
    var effective = resolveTheme(t);
    if (effective === 'custom') {
      var base = (spec && spec.base === 'light') ? 'light' : 'dark';
      document.documentElement.setAttribute('data-theme', 'custom');
      document.documentElement.setAttribute('data-base', base);
      injectCustomStyle(spec || {});
      ensureDuotoneSvg();
      updateCustomDuotoneFilter(spec);
    } else {
      document.documentElement.setAttribute('data-theme', effective);
      document.documentElement.removeAttribute('data-base');
      removeCustomStyleTag();
      ensureDuotoneSvg();
    }
    // Apply cached per-theme duotone overrides from localStorage for first
    // paint — reconcile() will re-apply with server-side values shortly.
    try {
      var raw = localStorage.getItem('ui_duotone_overrides_json');
      if (raw) {
        var overrides = JSON.parse(raw);
        if (overrides && typeof overrides === 'object') {
          Object.keys(overrides).forEach(function (k) {
            if (overrides[k]) {
              _rebuildDuotoneFilterNode('duotone-' + k, overrides[k]);
              _updateDuotoneVarsForTheme(k, overrides[k]);
            }
          });
        }
      }
    } catch (e) { /* malformed cache — skip */ }
  }

  function readCachedCustom() {
    try {
      var raw = localStorage.getItem('ui_theme_custom_json');
      return raw ? JSON.parse(raw) : null;
    } catch (e) { return null; }
  }

  // 1. First paint: read cached theme + custom spec from localStorage.
  var cached = null;
  try { cached = localStorage.getItem('ui_theme'); } catch (e) {}
  apply(cached || DEFAULT_THEME, readCachedCustom());
  //: Accent override (cached) — applied after the theme so it wins
  //: the cascade. Reconcile() will re-apply with the server-side
  //: value moments later if it differs.
  try {
    var cachedAccent = localStorage.getItem('ui_accent_override');
    if (cachedAccent) _applyAccentOverride(cachedAccent);
  } catch (e) {}

  // 2. After load, reconcile with the server-side setting.
  function reconcile() {
    if (!window.fetch) return;
    fetch('/api/settings', { credentials: 'same-origin' })
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) {
        var s = (d && d.settings) || {};
        var server = s.ui_theme || DEFAULT_THEME;
        var spec = null;
        if (server === 'custom' && s.ui_theme_custom_json) {
          try { spec = JSON.parse(s.ui_theme_custom_json); } catch (e) { spec = null; }
          try { localStorage.setItem('ui_theme_custom_json', s.ui_theme_custom_json); } catch (e) {}
        }
        apply(server, spec);
        try { localStorage.setItem('ui_theme', server); } catch (e) {}
        // Per-theme duotone overrides — each entry rewrites one theme's
        // SVG filter with a user-tuned spec so the Duotone section can
        // customise any theme, not just Custom.
        // Accent override — server-side wins over cache. Empty/null
        // clears the override so the theme's own accent reasserts.
        var serverAccent = (s.ui_accent_override || '').trim();
        if (serverAccent) {
          _applyAccentOverride(serverAccent);
          try { localStorage.setItem('ui_accent_override', serverAccent); } catch (e) {}
        } else {
          _clearAccentOverride();
          try { localStorage.removeItem('ui_accent_override'); } catch (e) {}
        }
        if (s.ui_duotone_overrides_json) {
          try {
            var overrides = JSON.parse(s.ui_duotone_overrides_json);
            if (overrides && typeof overrides === 'object') {
              Object.keys(overrides).forEach(function (k) {
                if (overrides[k]) window.setDuotoneOverride(k, overrides[k]);
              });
              localStorage.setItem('ui_duotone_overrides_json', s.ui_duotone_overrides_json);
            }
          } catch (e) { /* malformed JSON — skip */ }
        }
      })
      .catch(function () { /* pre-auth pages (login) return 401; ignore */ });
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', reconcile);
  } else {
    reconcile();
  }

  // 3. Public live-apply API used by the Appearance settings panel.
  window.setUiTheme = function (t, spec) {
    apply(t, spec);
    try { localStorage.setItem('ui_theme', t); } catch (e) {}
    if (t === 'custom' && spec) {
      try { localStorage.setItem('ui_theme_custom_json', JSON.stringify(spec)); } catch (e) {}
    }
  };

  // 4. React to OS-level colour scheme flips while the app is open — only when
  //    the stored preference is 'auto'.
  try {
    if (window.matchMedia) {
      var mq = window.matchMedia('(prefers-color-scheme: dark)');
      var onSchemeChange = function () {
        var stored = null;
        try { stored = localStorage.getItem('ui_theme'); } catch (e) {}
        if (stored === 'auto') apply('auto', readCachedCustom());
      };
      if (mq.addEventListener) mq.addEventListener('change', onSchemeChange);
      else if (mq.addListener) mq.addListener(onSchemeChange); // legacy Safari
    }
  } catch (e) {}
})();
