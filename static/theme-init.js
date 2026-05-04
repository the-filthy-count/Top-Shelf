// theme-init.js — VHS-only theme. Other themes were removed.
//
// What this script does, all on first paint, synchronously, before
// app-shell.css is linked:
//   1. Stamps html[data-theme="vhs"] so every per-theme rule below
//      it in the cascade resolves to VHS.
//   2. Builds the SVG <defs> filter the project uses for duotone
//      image treatments (one filter, id="duotone-vhs").
//   3. No-ops the live-apply API (window.setUiTheme etc.) so any
//      surviving callers in the settings UI don't error — they just
//      stay on VHS.

(function () {
  var DEFAULT_THEME = 'vhs';
  // VHS duotone palette — deep purple shadows blooming to hot magenta
  // highlights with a blue-violet tritone midtone enabled (`#5B51D8`),
  // shadow_balance 0.00 / highlight_balance 0.56.
  var VHS_PAIR     = [[99, 1, 152], [254, 67, 217], 1.80, 0.50];
  var VHS_MIDTONE  = [91, 81, 216];

  // Rec.709 luminance weights — same as CSS grayscale(1) does internally.
  var _LUM = '0.2126 0.7152 0.0722 0 0 0.2126 0.7152 0.0722 0 0 0.2126 0.7152 0.0722 0 0 0 0 0 1 0';
  function _n255(v) { return (v / 255).toFixed(4); }

  function _vhsFilterMarkup() {
    var sh = VHS_PAIR[0], hi = VHS_PAIR[1], mid = VHS_MIDTONE;
    var contrast = VHS_PAIR[2];
    // VHS uses the tritone (5-stop) table — shadow → mid → highlight
    // with the project's chosen balance values baked in.
    var sBal = 0.00, hBal = 0.56;
    function tbl(c) {
      var s1 = Math.round(sh[c] * sBal + mid[c] * (1 - sBal));
      var h1 = Math.round(mid[c] * (1 - hBal) + hi[c] * hBal);
      return _n255(sh[c]) + ' ' + _n255(s1) + ' ' + _n255(mid[c]) + ' ' + _n255(h1) + ' ' + _n255(hi[c]);
    }
    // Per-tone alpha: shadow 90% / mid 75% / highlight 50%. The two
    // intermediate stops blend with the same balance constants as the
    // colour table so darker zones stay heavier than highlights.
    var alphaSh = 0.90, alphaMid = 0.75, alphaHi = 0.50;
    var alphaS1 = (alphaSh * sBal + alphaMid * (1 - sBal)).toFixed(4);
    var alphaH1 = (alphaMid * (1 - hBal) + alphaHi * hBal).toFixed(4);
    var alphaTbl = alphaSh.toFixed(4) + ' ' + alphaS1 + ' ' + alphaMid.toFixed(4) + ' ' + alphaH1 + ' ' + alphaHi.toFixed(4);
    var slope = contrast;
    var intercept = (1 - contrast) / 2;
    // After the luminance reduction R == G == B == L. To drive the
    // alpha table off luminance (rather than the original alpha,
    // which is always 1), copy R into A via an feColorMatrix BEFORE
    // the tone-mapping feComponentTransfer. The fourth row puts R
    // into the alpha output: A_out = 1*R + 0*G + 0*B + 0*A + 0.
    return '<filter id="duotone-vhs" color-interpolation-filters="sRGB">'
      + '<feColorMatrix type="matrix" values="' + _LUM + '"/>'
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

  function ensureDuotoneSvg() {
    if (document.getElementById('duotone-svg-defs')) return;
    var svg = '<svg id="duotone-svg-defs" width="0" height="0" '
      + 'style="position:absolute;width:0;height:0;pointer-events:none" '
      + 'aria-hidden="true"><defs>' + _vhsFilterMarkup() + '</defs></svg>';
    var parent = document.body || document.documentElement;
    var wrapper = document.createElement('div');
    wrapper.innerHTML = svg;
    parent.appendChild(wrapper.firstChild);
  }

  // Pin VHS — every code path reads off this attribute, so settings
  // changes that try to switch theme are silently overridden back.
  function applyVhs() {
    document.documentElement.setAttribute('data-theme', DEFAULT_THEME);
    document.documentElement.removeAttribute('data-base');
    document.documentElement.removeAttribute('data-accent-override');
    var customStyle = document.getElementById('custom-theme-style');
    if (customStyle && customStyle.parentNode) customStyle.parentNode.removeChild(customStyle);
    var accentStyle = document.getElementById('accent-override-style');
    if (accentStyle && accentStyle.parentNode) accentStyle.parentNode.removeChild(accentStyle);
    ensureDuotoneSvg();
  }
  applyVhs();

  // Public no-op shims so any settings-UI callers keep working.
  window.setUiTheme = function () { applyVhs(); };
  window.setAccentOverride = function () {};
  window.clearAccentOverride = function () {};
  window.setDuotoneOverride = function () {};
  window.resetDuotoneFilter = function () {};
  window.getDuotoneDefaults = function () { return null; };
})();
