# Canonical UI design

`static/app-shell.css` owns theme colors and the four text sizes.
`static/design-system.css` owns button and form-control appearance. Every page
loads it after page styles and `mobile.css`. Page CSS owns layout, not a separate
visual theme. The `canonical-controls` cascade layer gives its important skin
rules precedence over legacy per-page styles, including dynamically injected
popup CSS. Add shared variants here instead of adding another local override.

| Role | Token | Desktop / mobile |
| --- | --- | --- |
| Supporting labels, dates, metadata, help | `--text-small` | 10px / 12px |
| Body copy, navigation, button labels | `--text-body` | 12px / 14px |
| Section/dialog headings, form fields | `--text-section` | 14px / 16px |
| Page and entity titles | `--text-title` | 20px / 20px |

Use these tokens in CSS and generated markup. Avoid literal font sizes and
component-specific scales. Icons keep their geometric sizing. LED counters
continue to use the shared text fitter so large values remain inside their tiles.
Use spacing, weight, and semantic color for hierarchy within each text role.

## Controls

- Primary: `.btn-primary` or `data-variant="primary"` for the main action.
- Secondary: `.btn-secondary` for other actions; unclassified buttons receive
  the same themed base skin automatically.
- Icon: `.btn-icon` with an accessible name (`aria-label` and a useful tooltip).
- Destructive: `.btn-danger` or `data-variant="danger"`; confirmation dialogs
  carry this variant when destructive. Do not encode danger only in inline red.
- Selected: `aria-pressed="true"` or `aria-selected="true"`; existing active
  classes are supported while older components migrate.
- Success: `data-variant="success"`. Warning/information/positive filter states
  use the theme's amber/blue/green tokens; they must remain distinguishable.

Controls share an 8px radius, themed surface/border, hover feedback, disabled
appearance, and a visible accent focus outline. Standard action buttons have a
28px minimum height on desktop and 44px on small screens. Input text uses 14px on desktop and 16px on phones. Reduced-motion preferences disable control transitions.

Clickable posters, logo choices, counter tiles, and switches retain their content
and layout; they are not toolbar buttons. Their existing theme-aware surface
styles and shared keyboard-focus treatment still apply. Brand/provider artwork
is not recolored as UI chrome.

## Verification

`python tests/browser/design_system.py` checks rendered text on 15 main pages (including Login and Log)
and entity pages, themed controls in dark/light/glass, and a real confirmation
dialog in every supported theme. Set `TS_BROWSER=webkit` for the same checks
in WebKit. `python tests/browser/mobile.py` covers responsive workflows and all
47 counter tiles. Fixtures intercept network calls and never touch live media.

## Responsive density

Mobile navigation is a full-screen panel with its own Close button and scrollable
links. Health and Log are omitted from phone navigation; Health opened directly
shows a desktop-only notice. Phones use three portrait cards per row, with four
on wider small screens. The home search and source filters occupy separate rows;
the decorative header logo is hidden. Home row limits follow the actual CSS grid.
Desktop layouts use the smaller text scale, narrower sidebar, and denser grids at
100% browser zoom; no CSS zoom or transform scales the app.

Segmented toggles retain their original connected shape and selected treatment.
They are excluded from the generic action-button skin, alongside content tiles.

On mobile library grids, favourite/lock hover controls are hidden; record actions
remain in the detail page. Compact card names use ellipsis rather than centered
clipping. Discovered suggestions retain their small dismiss control.
