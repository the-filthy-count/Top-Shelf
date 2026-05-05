// scenes-common.js
//
// Shared JavaScript for /scenes (scene-feed grid) and /discover (search
// + spotlight + detail panel). Both pages embed the same DOM hooks but
// for different subsets of features; init code at the bottom uses
// `document.getElementById(...)` checks to gate which paths fire on
// which page.
//
// This file was extracted from the inline <script> block that previously
// lived in BOTH scenes.html and discover.html (byte-identical copies).
// Edit here only — both pages re-load it via <script src=...>.

  let currentType     = 'performer';
  let selectedResult  = null;
  let selectedDest    = null;

  // Country flag helpers (`countryFlagHtml`, `COUNTRY_FLAG_LOOKUP`) live
  // in ts-utils.js so /library can use them too — they're attached to
  // `window` and called below directly.

  // ── Magazine page filler — adjectives + lorem ipsum used by the
  //    /discover performer magazine spread to break up the gallery
  //    page with editorial-feel quotes and (deliberately blurred,
  //    illegible) text blocks. Drawn from a 'beautiful' synonym list. ──
  const _MAG_ADJECTIVES = [
    // ── Single-word descriptors ──────────────────────────────────
    "Gorgeous","Stunning","Smoking","Drop-dead","Knockout","Sensational","Sizzling",
    "Scorching","Red-hot","Sultry","Smouldering","Alluring","Tempting","Teasing",
    "Flirty","Cheeky","Naughty","Saucy","Risqué","Ravishing","Luscious","Curvy",
    "Voluptuous","Breathtaking","Eye-catching","Head-turning","Jaw-dropping",
    "Heart-stopping","Show-stopping","Magnetic","Mesmerising","Hypnotic",
    "Irresistible","Unforgettable","Electric","Wild","Playful","Foxy","Glamorous",
    "Dazzling","Radiant","Sun-kissed","After-dark","Late-night","Trouble","Dangerous",
    "Bad","Wicked","Devilish","Sinful","Indecent","Scandalous","Outrageous","Lethal",
    "Killer","Next-level","Top-tier","Elite","Mega","Unreal","Blazing","White-hot",
    "Firecracker","Bombshell","Stunner","Looker","Scene-stealing","Slick","On-point",
    "Popping","Stacked","Toned","Trim","Curvaceous","Peachy","Perky","Busty","Leggy",
    "Snatched","Sculpted","Worldie","Belter","Cracker","Phwoar","Ten-out-of-ten",
    "Top-drawer","Classy","Pin-up","Cover-star","Dreamy","Glowing","Golden-hour",
    "Midnight","Prime-time","Boss","Queen","Star","Icon","Legend","Major","Cute",
    "Adorable","Sweet","Darling","Angelic","Bubbly","Sparkly","Rosy","Doll-like",
    "Bright-eyed","Dewy","Fresh-faced","Cosy","Kissable","Smitten","Heart-melting",
    "Innocent","Wholesome","Petite","Pixie","Starry-eyed","Cutie","Cutie-pie",
    "Sweetheart","Seductive","Enticing","Velvet","Slow-burning","Candlelit","Smoky",
    "Honeyed","Molten","Lush","Decadent","Intoxicating","Addictive","Forbidden",
    "Bewitching","Enchanting","Spellbinding","Bedroom-eyed","Coy","Come-hither",
    "Raunchy","Filthy","Dirty","X-rated","Explicit","Raw","Lewd","Lascivious",
    "Salacious","Kinky","Taboo","Carnal","Lusty","Primal","Uninhibited","Shameless",
    "Brazen","Provocative","Suggestive","Steamy","Heated","Pulse-racing","Dominant",
    "Submissive","Commanding","Untamed","Reckless","Temptress","Vixen","Minx","Siren",
    "Maneater","Juicy","Spicy","Extra","Over-the-top","In-your-face","Flashy","Showy",
    "Thirsty","Horny","Down-bad","Unhinged","Game","Up-for-it","Goddess","Heartbreaker",
    "Heartthrob","Showgirl","Pageant","Centrefold","Cover-girl","Smokeshow","Hottie",
    "Babe","Cracking","Tasty","Banging","Buff","Hourglass","Bombshell-tier","A-list",
    "First-look","Front-cover","Editorial","Couture","High-fashion","Runway","Catwalk",
    "Vogue-worthy","Studio-lit","Spotlit","Backlit","Silhouette","Statuesque","Marble",
    "Porcelain","Alabaster","Bronzed","Tanned","Beach-glow","Salt-kissed","Wind-swept",
    "Tousled","Bedhead","Drenched","Wet-look","Glistening","Slippery","Slick-skinned",
    "Oiled","Polished","Glazed","Lacquered","Liquid-gold","Champagne","Caviar",
    "Velvet-rope","Whiskey-warm","Wine-dark","Rouge","Crimson","Scarlet","Cherry",
    "Plum","Berry","Sugar-spun","Maple","Caramel","Toffee","Buttercream","Vanilla",
    "Peaches-and-cream","Strawberry","Pillowy","Plush","Buttery","Powder-soft",
    "Cashmere","Satin","Lace","Silk-sheets","Mink","Pearl","Ruby","Diamond",
    "Crystalline","Glittering","Iridescent","Holographic","Chrome","Mirror-shine",
    "Reflective","Hypnotising","Trance","Dazed","Dizzy","Lovedrunk","Heart-thumping",
    "Knee-weakening","Pulse-stopping","Breath-stealing","Soul-snatching","Wig-snatching",
    "Jaw-on-the-floor","Speechless","Curtain-call","Encore","Standing-ovation",
    "Five-star","Chef's-kiss","Faultless","Flawless","Immaculate","Pristine",
    "Untouchable","Out-of-reach","Out-of-your-league","Premium-grade","Top-shelf",
    "First-class","Best-in-class","Hand-picked","One-of-one","Rare","Exotic","Imported",
    "Limited-edition","Collector's-item","Trophy","Prized","Coveted","Sought-after",
    "In-demand","Headlining","Trending","Viral","Buzzy","Talk-of-the-town",
    "Whispered-about","Notorious","Infamous","Legendary","Mythic","Fabled","Storied",
    "Iconic","Era-defining","Generation-defining","Unmatched","Unrivalled","Peerless",
    "One-in-a-million","Unicorn","Rare-breed","Different-breed","Different-gravy",
    "Built-different","On-another-level","In-a-league-of-her-own","Set-the-bar",
    "Broke-the-mould","Stratospheric","Astronomical","Astonishing","Astounding",
    "Staggering","Stupefying","Mind-bending","Mind-melting","Mind-blowing",
    "Earth-shaking","Room-stopping","Crowd-parting","Traffic-stopping","Heads-spinning",
    "Paparazzi","Red-carpet","Champagne-on-ice","Bottle-service","Penthouse","Rooftop",
    "Yacht-deck","Riviera","Monaco","Saint-Tropez","Vegas","Miami","Ibiza","Sundown",
    "Twilight","Witching-hour","Last-call","Closing-time","Afterparty","Speakeasy",
    "Members-only","Velvet-curtain","Backstage-pass","All-access","Inner-circle",
    "Ringside","Centre-stage","Headline-act","Main-event","Heat-wave","Volcanic",
    "Lava-hot","Inferno","Wildfire","Explosive","Combustible","Incendiary","Firestorm",
    "Powder-keg","Tinderbox","Hazardous","Off-limits","Restricted","Classified",
    "Adults-only","Hush-hush","On-the-down-low","Open-secret","Worst-kept-secret",
    "Fever-dream","Daydream","Wet-dream","Fantasy","Fairytale","Mirage","Vision",
    "Otherworldly","Heaven-sent","Divine","Goddess-tier","Worship-worthy","Altar-worthy",
    "Bow-down","Knees-buckling","Lovesick","Down-horrendous","Down-catastrophic",
    "Cooked","Done-for","Wrecked","Ruined","Pampered","Gilded","Golden",
    "Diamond-encrusted","Bejewelled","Bedazzled","Decked-out","Drippy","Soaked",
    "Saturated",

    // ── Long-form pull quotes ────────────────────────────────────
    "Filthy mind, prettier face.",
    "Butter wouldn't melt — but it would.",
    "Looks like trouble. Is trouble.",
    "The reason he stays late at the office.",
    "Worth the divorce.",
    "Sunday best, Friday worst.",
    "Convent-raised, devil-trained.",
    "The reason your priest drinks.",
    "Lapsed Catholic, practising sinner.",
    "Sin in stockings.",
    "Trouble in heels.",
    "Built to ruin lives.",
    "Built to break beds.",
    "Built to end marriages.",
    "Marriage-ender, mortgage-breaker.",
    "Front-page mistress.",
    "The other woman, obviously.",
    "The reason he doesn't come home.",
    "3 a.m. text material.",
    "Last orders, last decision.",
    "Closing time, bad decisions.",
    "The mistake worth repeating.",
    "The kind of mistake you frame.",
    "Stop traffic, start rumours.",
    "Break the internet, break him.",
    "Make grown men cry.",
    "Make grown men beg.",
    "Knees weak, wallet weaker.",
    "Brings men to their knees.",
    "The reason he called out the wrong name.",
    "Knows exactly what she's doing.",
    "Knows. Doesn't care. Continues.",
    "Caught you looking.",
    "Caught you twice.",
    "The photo you saved twice.",
    "Saved to a hidden folder.",
    "Hidden-folder royalty.",
    "Vault material.",
    "The reason you're still up.",
    "The reason for the cold shower.",
    "Cold shower won't fix this.",
    "Last call, lasting damage.",
    "Backstage, backseat, back of the club.",
    "Not safe for work, marriage, or sanity.",
    "Open secret, locked door.",
    "Clear your weekend.",
    "Cancel everything.",
    "Reschedule the rest of your life.",
    "Vow-breaker by appointment.",
    "New year, same weakness.",
    "Bad for your health, great for your mood.",
    "Doctor said no. She said yes.",
    "Heart attack risk, barely dressed.",
    "Pulse-stopping, pulse-restarting.",
    "Caution: prolonged exposure causes divorce.",
    "Worth the trip to hell.",
    "Hell's not that bad if she's there.",
    "Angels filing complaints.",
    "HR can't help you here.",
    "The reason HR exists.",
    "The reason for the dress code.",
    "Dress-code violation, gloriously.",
    "Decency? Never met her.",
    "Decorum has left the building.",
    "PhD in distraction.",
    "Masters in mayhem.",
    "Certified menace.",
    "The good kind of problem.",
    "Expensive mistake.",
    "Worth the second mortgage.",
    "She walked in. Room stopped.",
    "She arrived. Conversation ended.",
    "Out of your league, your postcode, your country.",
    "Out of your tax bracket.",
    "Heartbreaker by trade.",
    "Profession: distraction.",
    "Currently accepting bad decisions.",
    "Specialist in poor choices.",
    "Guilty as charged, smug about it.",
    "Print this, frame it, hide it.",
    // ── New additions ────────────────────────────────────────────
    "Reads minds. Steals weekends.",
    "Front cover, back booth.",
    "Sunday confession in the making.",
    "Designed by sin, finished in silk.",
    "The kind of yes that sounds like trouble.",
    "The reason for the second drink.",
    "Lights low, stakes high.",
    "Not your type. Will be.",
    "Hand on the bible, foot on the gas.",
    "Sweet enough to ruin you.",
    "Pretty handwriting, dirty mind.",
    "Polite in public, lethal in private.",
    "Stays for breakfast, ruins your week.",
    "First-name basis with chaos.",
    "Lipstick on the glass, name on the wall.",
    "The thing your therapist warned about.",
    "Smiles like she means it. She doesn't.",
    "Comes with a warning label.",
    "Read the fine print. Ignore it.",
    "The headline her ex avoids.",
    "Soft voice, hard consequences.",
    "Says please. Means later.",
    "Closes deals. Closes blinds.",
    "Manners of a saint, plans of a sinner.",
    "Wears the dress like a dare.",
    "Compliments her. Lose your wallet.",
    "Quiet entrance, loud exit.",
    "The pause before the bad idea.",
    "Reason enough. Twice.",
    "Tells the truth in private.",
  ];
  const _MAG_LOREM = [
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.",
    "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.",
    "Curabitur pretium tincidunt lacus. Nulla gravida orci a odio. Nullam varius, turpis et commodo pharetra, est eros bibendum elit, nec luctus magna felis sollicitudin mauris. Integer in mauris eu nibh euismod gravida.",
    "Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia curae; Sed cursus ante dapibus diam. Sed nisi. Nulla quis sem at nibh elementum imperdiet.",
    "Phasellus tempor mauris ac suscipit feugiat. Etiam fringilla nisl ut facilisis ultrices. Sed cursus tortor at nibh aliquam, vitae dictum lacus tincidunt. Maecenas ultricies leo eget magna eleifend.",
    "Aliquam erat volutpat. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Cras vulputate, augue at sagittis aliquam, leo dolor pulvinar lectus.",
  ];
  function _magPickRandom(arr) {
    return arr[Math.floor(Math.random() * arr.length)];
  }
  function _magDedupe(arr) {
    const seen = new Set();
    const out  = [];
    for (const u of arr || []) {
      const k = String(u || '').trim();
      if (k && !seen.has(k)) { seen.add(k); out.push(k); }
    }
    return out;
  }
  function _magShuffle(arr) {
    const a = (arr || []).slice();
    for (let i = a.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [a[i], a[j]] = [a[j], a[i]];
    }
    return a;
  }

  // Memoised library-check — same name fired multiple times per session
  // (showDetail, spotlightTileClick, etc.) shouldn't re-hit the API.
  const _libCheckCache = new Map();
  function libraryCheck(name) {
    const key = (name || '').trim().toLowerCase();
    if (!key) return Promise.resolve({ found: false });
    if (_libCheckCache.has(key)) return _libCheckCache.get(key);
    const p = fetch(`/api/library/check?name=${encodeURIComponent(name)}`)
      .then(r => r.json())
      .catch(() => ({ found: false }));
    _libCheckCache.set(key, p);
    return p;
  }
  /** Scene objects for the current grid (index matches data-scene-i on .scene-card) */
  let _sceneGridItems = [];
  /** Title-case for performer/studio labels (APIs often return all-lowercase). */
  function capDisplayName(s) {
    if (s == null || s === '') return '';
    return String(s).trim().split(/\s+/).map(function (word) {
      if (!word) return word;
      return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
    }).join(' ');
  }
  // ── Wanted-list state ─────────────────────────────────────────────
  // Set of "${kind}:${source}:${external_id}" keys currently in the
  // user's Wanted list. Populated once on page load, updated in place
  // when the user toggles the eye icon.
  const _wantedKeys = new Set();
  function _wkey(kind, source, externalId) {
    return `${(kind || 'scene').toLowerCase()}:${(source || '').toLowerCase()}:${externalId || ''}`;
  }
  async function _loadWantedKeys() {
    try {
      const r = await fetch('/api/wanted/keys');
      const d = await r.json();
      (d.keys || []).forEach(k => _wantedKeys.add(_wkey(k.kind, k.source, k.external_id)));
    } catch (_) {}
  }
  function _wantedGuessSource(card) {
    // Scene cards pulled from TPDB via the performers/studios/tags/feed
    // paths; movie cards use the same convention. All of these are TPDB
    // in practice. If we ever surface StashDB cards directly we can
    // thread a real ``source`` through the card data.
    return (card.dataset.wantedSource || 'tpdb').toLowerCase();
  }
  function _cardWantedButtonHtml(kind, sourceGuess, externalId) {
    const key = _wkey(kind, sourceGuess, externalId);
    const on = _wantedKeys.has(key);
    const title = on ? 'In your Wanted list — click to remove' : 'Add to Wanted';
    return `<button class="scene-wanted-btn${on ? ' is-wanted' : ''}" data-wanted-kind="${esc(kind)}" data-wanted-source="${esc(sourceGuess)}" data-wanted-id="${esc(externalId)}" title="${esc(title)}" aria-pressed="${on}"><i class="fa-solid fa-eye"></i></button>`;
  }
  document.addEventListener('click', function(e) {
    const btn = e.target && e.target.closest && e.target.closest('.scene-wanted-btn');
    if (!btn) return;
    e.preventDefault();
    e.stopPropagation();
    const kind = btn.getAttribute('data-wanted-kind') || 'scene';
    const source = btn.getAttribute('data-wanted-source') || 'tpdb';
    const externalId = btn.getAttribute('data-wanted-id') || '';
    if (!externalId) return;
    // Pull enriching fields from the card so the backend can save a
    // full record without a follow-up fetch.
    const card = btn.closest('.scene-card, .movie-card');
    const scene = (() => {
      if (!card) return {};
      const idx = parseInt(card.getAttribute('data-scene-i') || '-1', 10);
      if (!isNaN(idx) && idx >= 0 && window._sceneGridItems && window._sceneGridItems[idx]) {
        return window._sceneGridItems[idx];
      }
      return null;
    })();
    const payload = {
      kind, source, external_id: externalId,
      title:    (scene && scene.title)    || card?.querySelector('.scene-title')?.textContent
               || card?.querySelector('.movie-title')?.textContent || '',
      studio:   (scene && scene.studio)   || '',
      date:     (scene && scene.date)     || '',
      performers: (scene && scene.performer) || '',
      thumb:    (scene && scene.thumb)    || card?.querySelector('.scene-thumb')?.src
               || card?.querySelector('.movie-poster')?.src || '',
      description: (scene && scene.description) || '',
      tags:     (scene && Array.isArray(scene.tags)) ? scene.tags : [],
      duration: (scene && scene.duration)  || 0,
    };
    btn.disabled = true;
    fetch('/api/wanted/toggle', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    }).then(r => r.json()).then(d => {
      const key = _wkey(kind, source, externalId);
      if (d && d.wanted) {
        _wantedKeys.add(key);
        document.querySelectorAll(`.scene-wanted-btn[data-wanted-id="${CSS.escape(externalId)}"]`).forEach(b => {
          b.classList.add('is-wanted');
          b.setAttribute('aria-pressed', 'true');
          b.setAttribute('title', 'In your Wanted list — click to remove');
        });
      } else {
        _wantedKeys.delete(key);
        document.querySelectorAll(`.scene-wanted-btn[data-wanted-id="${CSS.escape(externalId)}"]`).forEach(b => {
          b.classList.remove('is-wanted');
          b.setAttribute('aria-pressed', 'false');
          b.setAttribute('title', 'Add to Wanted');
        });
      }
    }).catch(() => {}).finally(() => { btn.disabled = false; });
  });

  const _FEED_MODES = ['movies', 'performers', 'studios', 'tags', 'search'];
  let _scenesFeedMode = (function() {
    try {
      let v = localStorage.getItem('topShelfScenesFeedMode');
      if (v == null || v === '' || v === 'library' || v === 'random' || v === 'favourites') v = 'studios';
      if (v === 'recent') v = 'performers';
      if (!_FEED_MODES.includes(v)) v = 'studios';
      return v;
    } catch (_) { return 'studios'; }
  })();

  let _monitoredTagsCache = [];
  // null = never saved (first-ever entry to Tags mode); [] = user explicitly
  // deselected everything; [...] = persisted selection. The null sentinel
  // lets us auto-select every monitored tag on first entry while still
  // honouring an intentionally empty selection afterwards.
  let _selectedTagIds = (function() {
    try {
      const raw = localStorage.getItem('topShelfScenesSelectedTags');
      if (raw === null) return null;
      const parsed = JSON.parse(raw);
      return Array.isArray(parsed) ? parsed.map(String) : [];
    } catch(_) { return []; }
  })();

  function _applyFeedModeToggleUI() {
    document.querySelectorAll('#feedModeToggle .feed-mode-btn').forEach(btn => {
      btn.classList.toggle('active', btn.getAttribute('data-mode') === _scenesFeedMode);
    });
    const tagWrap = document.getElementById('tagFilterWrap');
    if (tagWrap) tagWrap.style.display = (_scenesFeedMode === 'tags') ? 'block' : 'none';
  }

  async function _ensureMonitoredTagsLoaded() {
    if (_monitoredTagsCache.length) return _monitoredTagsCache;
    try {
      const r = await fetch('/api/settings');
      const d = await r.json();
      const raw = (d && d.settings && d.settings.monitored_tags) || '[]';
      const parsed = JSON.parse(raw);
      _monitoredTagsCache = Array.isArray(parsed) ? parsed.filter(t => t && t.id && t.name) : [];
    } catch(_) { _monitoredTagsCache = []; }
    // First-ever entry to Tags mode: auto-select every monitored tag so
    // the feed has something to show. User's subsequent deselect/select
    // choices persist to localStorage and override this default.
    if (_selectedTagIds === null) {
      _selectedTagIds = _monitoredTagsCache.map(t => String(t.id));
      try { localStorage.setItem('topShelfScenesSelectedTags', JSON.stringify(_selectedTagIds)); } catch(_) {}
      _updateTagFilterBadge();
    }
    return _monitoredTagsCache;
  }

  function _renderTagFilterList() {
    const list = document.getElementById('tagFilterList');
    if (!list) return;
    if (!_monitoredTagsCache.length) {
      list.innerHTML = '<div style="padding:14px;color:var(--dim);font-size:12px;line-height:1.5">No vices configured. Add some under Settings → Content Filters → Vices.</div>';
      return;
    }
    const header = `
      <div style="display:flex;gap:6px;padding:4px 6px 8px;border-bottom:1px solid rgba(var(--brand-purple-rgb),0.18);margin-bottom:4px">
        <button type="button" onclick="selectAllTags()" style="flex:1;padding:4px 8px;border-radius:6px;background:rgba(var(--brand-purple-rgb),0.12);border:1px solid rgba(var(--brand-purple-rgb),0.25);color:var(--text);font-size:11px;cursor:pointer">All</button>
        <button type="button" onclick="clearSelectedTags()" style="flex:1;padding:4px 8px;border-radius:6px;background:rgba(var(--brand-purple-rgb),0.12);border:1px solid rgba(var(--brand-purple-rgb),0.25);color:var(--text);font-size:11px;cursor:pointer">None</button>
      </div>`;
    const sortedTags = _monitoredTagsCache
      .slice()
      .sort((a, b) => String(a.name || '').localeCompare(String(b.name || ''), undefined, { sensitivity: 'base' }));
    const rows = sortedTags.map(t => {
      const id = String(t.id);
      const checked = _selectedTagIds.includes(id);
      return `
        <label style="display:flex;align-items:center;gap:8px;padding:6px 8px;border-radius:6px;cursor:pointer;font-size:13px;color:var(--text)" onmouseenter="this.style.background='rgba(var(--brand-purple-rgb),0.10)'" onmouseleave="this.style.background=''">
          <input type="checkbox" ${checked ? 'checked' : ''} data-tag-id="${esc(id)}" onchange="toggleTagSelection('${esc(id)}', this.checked)" style="accent-color:#c084fc">
          <span>${esc(t.name)}</span>
        </label>`;
    }).join('');
    list.innerHTML = header + rows;
  }

  function _updateTagFilterBadge() {
    const badge = document.getElementById('tagFilterBadge');
    if (!badge) return;
    // `_selectedTagIds` is intentionally `null` until the user (or
    // `_ensureMonitoredTagsLoaded`) seeds it — a fresh browser with no
    // localStorage key hits this path on the initial paint, before the
    // monitored-tags fetch resolves.
    const n = Array.isArray(_selectedTagIds) ? _selectedTagIds.length : 0;
    if (n > 0) {
      badge.textContent = String(n);
      badge.style.display = 'inline-block';
    } else {
      badge.style.display = 'none';
    }
  }

  function _positionTagFilterDropdown() {
    const btn = document.getElementById('tagFilterBtn');
    const dd = document.getElementById('tagFilterDropdown');
    if (!btn || !dd || dd.style.display !== 'block') return;
    const r = btn.getBoundingClientRect();
    const ddW = dd.offsetWidth || 280;
    const vw = window.innerWidth;
    // Align the dropdown's right edge to the button's right edge;
    // clamp to viewport so nothing gets clipped off-screen.
    let left = r.right - ddW;
    if (left < 8) left = 8;
    if (left + ddW > vw - 8) left = vw - ddW - 8;
    dd.style.top = (r.bottom + 6) + 'px';
    dd.style.left = left + 'px';
  }

  function toggleTagFilterDropdown() {
    const dd = document.getElementById('tagFilterDropdown');
    if (!dd) return;
    if (dd.style.display === 'block') {
      dd.style.display = 'none';
      return;
    }
    _ensureMonitoredTagsLoaded().then(() => {
      _renderTagFilterList();
      dd.style.display = 'block';
      _positionTagFilterDropdown();
    });
  }

  window.addEventListener('resize', _positionTagFilterDropdown);
  window.addEventListener('scroll', _positionTagFilterDropdown, true);

  function toggleTagSelection(id, checked) {
    const sid = String(id);
    const idx = _selectedTagIds.indexOf(sid);
    if (checked && idx === -1) _selectedTagIds.push(sid);
    if (!checked && idx !== -1) _selectedTagIds.splice(idx, 1);
    try { localStorage.setItem('topShelfScenesSelectedTags', JSON.stringify(_selectedTagIds)); } catch(_) {}
    _updateTagFilterBadge();
    loadFeed();
  }

  function selectAllTags() {
    _selectedTagIds = _monitoredTagsCache.map(t => String(t.id));
    try { localStorage.setItem('topShelfScenesSelectedTags', JSON.stringify(_selectedTagIds)); } catch(_) {}
    _renderTagFilterList();
    _updateTagFilterBadge();
    loadFeed();
  }

  function clearSelectedTags() {
    _selectedTagIds = [];
    try { localStorage.setItem('topShelfScenesSelectedTags', JSON.stringify(_selectedTagIds)); } catch(_) {}
    _renderTagFilterList();
    _updateTagFilterBadge();
    loadFeed();
  }

  document.addEventListener('click', function(e) {
    const dd = document.getElementById('tagFilterDropdown');
    const btn = document.getElementById('tagFilterBtn');
    if (!dd || dd.style.display !== 'block') return;
    if (btn && (btn === e.target || btn.contains(e.target))) return;
    if (dd.contains(e.target)) return;
    dd.style.display = 'none';
  });

  function setScenesFeedMode(mode) {
    const m = _FEED_MODES.includes(mode) ? mode : 'studios';
    _scenesFeedMode = m;
    try { localStorage.setItem('topShelfScenesFeedMode', m); } catch (_) {}
    _applyFeedModeToggleUI();
    _updateTagFilterBadge();
    if (m === 'tags') _ensureMonitoredTagsLoaded().then(() => loadFeed());
    else loadFeed();
  }
  // Toolbar refresh button — dumps every layer of cache (browser-side
  // _feedCache + server-side _feed_cache / _tag_scene_pools /
  // _SCENES_RECENT_CACHE) and re-pulls the active feed.
  async function refreshScenesFeed() {
    try { _feedCache.clear(); } catch (_) {}
    const btn = document.getElementById('feedRefreshBtn');
    const ico = btn ? btn.querySelector('i') : null;
    if (btn) btn.disabled = true;
    if (ico) ico.classList.add('fa-spin');
    try {
      await loadFeed({ force: true });
    } finally {
      if (btn) btn.disabled = false;
      if (ico) ico.classList.remove('fa-spin');
    }
  }
  window.refreshScenesFeed = refreshScenesFeed;

  function clearScenesSearch() {
    ['scenesSrchPerformer','scenesSrchStudio','scenesSrchTag','scenesSrchDateFrom','scenesSrchDateTo'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.value = '';
    });
    const status = document.getElementById('scenesSearchStatus');
    if (status) status.textContent = '';
    const grid = document.getElementById('scenesGrid');
    if (grid) {
      grid.innerHTML = '<div class="empty">Fill in the form and search across StashDB, ThePornDB, and FansDB.</div>';
      delete grid.dataset.searchPopulated;
    }
  }

  async function runScenesSearch() {
    // Scene search keys off performer / studio / tag / date — title
    // is intentionally not a scene-search field (movies use their own
    // title-based search panel below). Backend treats absent `title`
    // as no filter, so we just don't send one.
    const payload = {
      performer: document.getElementById('scenesSrchPerformer')?.value.trim() || '',
      studio:    document.getElementById('scenesSrchStudio')?.value.trim() || '',
      tag:       document.getElementById('scenesSrchTag')?.value.trim() || '',
      date_from: document.getElementById('scenesSrchDateFrom')?.value.trim() || '',
      date_to:   document.getElementById('scenesSrchDateTo')?.value.trim() || '',
      sources: [],
      per_page: 30,
    };
    if (document.getElementById('scenesSrchSrcTpdb')?.checked) payload.sources.push('tpdb');
    if (document.getElementById('scenesSrchSrcStashdb')?.checked) payload.sources.push('stashdb');
    if (document.getElementById('scenesSrchSrcFansdb')?.checked) payload.sources.push('fansdb');
    if (!payload.sources.length) {
      const status = document.getElementById('scenesSearchStatus');
      if (status) status.textContent = 'Select at least one source.';
      return;
    }
    if (!payload.performer && !payload.studio && !payload.tag && !payload.date_from && !payload.date_to) {
      const status = document.getElementById('scenesSearchStatus');
      if (status) status.textContent = 'Fill in at least one field.';
      return;
    }
    const grid = document.getElementById('scenesGrid');
    const status = document.getElementById('scenesSearchStatus');
    if (status) status.textContent = 'Searching ' + payload.sources.map(s => s.toUpperCase()).join(', ') + '…';
    grid.innerHTML = sceneGridSkeleton();
    try {
      const r = await fetch('/api/scenes/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const d = await r.json();
      if (d && d.error) {
        grid.innerHTML = `<div class="empty">${esc(d.error)}</div>`;
        if (status) status.textContent = '';
        return;
      }
      const scenes = d.scenes || [];
      if (status) status.textContent = scenes.length
        ? `${scenes.length} result${scenes.length === 1 ? '' : 's'} from ${(d.sources_queried || payload.sources).join(', ').toUpperCase()}`
        : 'No scenes matched.';
      if (!scenes.length) {
        grid.innerHTML = '<div class="empty">No scenes matched. Try broader search terms.</div>';
        grid.dataset.searchPopulated = '1';
        return;
      }
      renderSceneGrid(scenes, true);
      grid.dataset.searchPopulated = '1';
    } catch (e) {
      grid.innerHTML = `<div class="empty">Search failed: ${esc(e.message || e)}</div>`;
      if (status) status.textContent = '';
    }
  }

  function _shortIndexer(name) {
    const map = {
      'theporndb': 'TPDB', 'the porn db': 'TPDB', 'porndb': 'TPDB',
      'stashdb': 'StashDB', 'fansdb': 'FansDB',
    };
    const lower = (name || '').toLowerCase().trim();
    return map[lower] || name;
  }

  function setDetailBg(imageUrl) {
    const el = document.getElementById('detailBgImage');
    if (imageUrl) {
      el.innerHTML = `<img class="detail-bg-image" src="${esc(imageUrl)}" onerror="this.remove()">`;
    } else {
      el.innerHTML = '';
    }
  }

  const _dbSources = ['TPDB', 'StashDB', 'FansDB'];
  function detailLoadingSkeleton() {
    // Rendered into #detailMeta (the text column under the name), not the
    // whole panel — so don't include a poster placeholder here, otherwise
    // it appears as a phantom box next to the real poster image.
    return `
      <div style="flex:1;min-width:0">
        <div class="skeleton-line" style="width:42%"></div>
        <div class="skeleton-line" style="width:68%"></div>
        <div style="display:flex;gap:6px;flex-wrap:wrap;margin:12px 0 16px">
          <div class="skeleton-chip"></div><div class="skeleton-chip"></div><div class="skeleton-chip"></div>
        </div>
        <div class="skeleton-line" style="width:100%"></div>
        <div class="skeleton-line" style="width:96%"></div>
        <div class="skeleton-line" style="width:88%"></div>
        <div class="skeleton-line" style="width:74%"></div>
      </div>`;
  }

  function sceneGridSkeleton(count = 30) {
    return Array.from({length: count}, () => `
      <div class="scene-card">
        <div class="skeleton-box" style="width:100%;aspect-ratio:16/9">
          <div class="img-spin" aria-hidden="true"></div>
        </div>
        <div class="scene-info">
          <div class="skeleton-line" style="width:84%"></div>
          <div class="skeleton-line" style="width:62%"></div>
        </div>
        <div class="scene-actions">
          <div class="skeleton-box" style="height:28px;border-radius:4px;flex:1"></div>
          <div class="skeleton-box" style="height:28px;border-radius:4px;flex:1"></div>
        </div>
      </div>`).join('');
  }

  const _LOCAL_LOGOS = { 'TPDB': 'tpdb', 'ThePornDB': 'tpdb', 'StashDB': 'stashdb', 'FansDB': 'fansdb', 'TMDB': 'tmdb', 'Freeones': 'freeones', 'IAFD': 'iafd', 'Babepedia': 'babepedia', 'Coomer': 'coomer' };
  function renderLinks(links) {
    if (!links || !links.length) return '';
    return links.map(l => {
      const isDb = _dbSources.includes(l.label);
      const cls = isDb ? 'detail-link db-link' : 'detail-link';
      const logoKey = _LOCAL_LOGOS[l.label];
      const iconHtml = logoKey
        ? `<img src="/static/logos/${logoKey}.png" alt="${esc(l.label)}" style="height:16px;width:auto;vertical-align:middle;opacity:0.9">`
        : (() => { try { const d = new URL(l.url).hostname.replace('www.',''); return `<img src="https://www.google.com/s2/favicons?domain=${d}&sz=32" onerror="this.remove()">`; } catch { return ''; } })();
      return `<a class="${cls}" href="${esc(l.url)}" target="_blank" title="${esc(l.label)}">${iconHtml}</a>`;
    }).join('');
  }

  function setType(type) {
    currentType = type;
    document.getElementById('btnMovie').classList.toggle('active', type === 'movie');
    document.getElementById('btnPerformer').classList.toggle('active', type === 'performer');
    document.getElementById('btnStudio').classList.toggle('active', type === 'studio');
    // Toggle search panels
    const entityWrap = document.getElementById('entitySearchWrap');
    const movieWrap = document.getElementById('movieSearchWrap');
    if (type === 'movie') {
      entityWrap.style.display = 'none';
      movieWrap.style.display = 'flex';
      document.getElementById('movieSearchResults').innerHTML = '<div class="empty">Search for movies on TPDB</div>';
    } else {
      entityWrap.style.display = 'flex';
      movieWrap.style.display = 'none';
      document.getElementById('searchResults').innerHTML = '<div class="empty">Search for a performer or studio</div>';
    }
    clearDetail();
  }

  async function runSearch() {
    const q = document.getElementById('searchInput').value.trim();
    if (!q) return;
    const el = document.getElementById('searchResults');
    el.innerHTML = Array.from({length:6}, (_,i)=>`<div class="result-item"><div class="result-thumb-placeholder skeleton-box"></div><div style="flex:1;min-width:0"><div class="skeleton-line" style="width:${70 - (i%3)*10}%"></div><div class="skeleton-line" style="width:32%;margin-bottom:0"></div></div><div style="width:110px" class="skeleton-line"></div></div>`).join('');
    try {
      const r = await fetch(`/api/metadata/search?q=${encodeURIComponent(q)}&type=${currentType}&strict=0`);
      const d = await r.json();
      if (!d.results?.length) { el.innerHTML = '<div class="empty">No results found</div>'; return; }
      window._searchResults = d.results;
      const isStudio = currentType === 'studio';
      el.innerHTML = d.results.map((item, i) => {
        if (isStudio && item.image) {
          // Studio with image: show image only, stretched full width
          return `<div class="result-item-studio" id="ri-${i}" onclick="selectResult(${i})">
            <img class="result-studio-img" src="${esc(item.image)}" onerror="this.style.display='none';this.nextElementSibling.style.display='block'">
            <div style="display:none;padding:10px"><div class="result-name">${esc(capDisplayName(item.name))}</div></div>
            <div id="lib-${i}" style="display:none"></div>
          </div>`;
        }
        if (isStudio) {
          // Studio without image: show text
          return `<div class="result-item" id="ri-${i}" onclick="selectResult(${i})">
            <div class="result-thumb-placeholder"><i class="fa-solid fa-clapperboard"></i></div>
            <div style="flex:1;min-width:0">
              <div class="result-name">${esc(capDisplayName(item.name))}</div>
              <div class="result-source">${item.source}</div>
            </div>
            <div id="lib-${i}" style="flex-shrink:0;font-size:11px;color:var(--dim)">...</div>
          </div>`;
        }
        // Performer: standard layout
        return `<div class="result-item" id="ri-${i}" onclick="selectResult(${i})">
          ${item.image
            ? `<div class="result-thumb-wrap"><img class="result-thumb" src="${esc(item.image)}" onerror="this.closest('.result-thumb-wrap').outerHTML='<div class=result-thumb-placeholder><i class=fa-solid fa-person></i></div>'"></div>`
            : `<div class="result-thumb-placeholder"><i class="fa-solid fa-person"></i></div>`}
          <div style="flex:1;min-width:0">
            <div class="result-name">${esc(capDisplayName(item.name))}${genderBadge(item.gender)}</div>
            <div class="result-source">${item.source}</div>
          </div>
          <div id="lib-${i}" style="flex-shrink:0;font-size:11px;color:var(--dim)">...</div>
        </div>`;
      }).join('');
      // Check library status for each result
      d.results.forEach((item, i) => {
        libraryCheck(item.name)
          .then(lib => {
            const el = document.getElementById(`lib-${i}`);
            if (!el) return;
            if (lib.found) {
              el.innerHTML = '<span class="lib-tag lib-tag--in">IN-LIBRARY</span>';
              window._searchResults[i]._inLibrary = true;
              window._searchResults[i]._libraryPath = lib.path;
            } else {
              el.innerHTML = '';
              window._searchResults[i]._inLibrary = false;
            }
          }).catch(() => { const el = document.getElementById(`lib-${i}`); if (el) el.innerHTML = ''; });
      });
    } catch(e) {
      el.innerHTML = `<div class="empty">Search failed: ${esc(e.message)}</div>`;
    }
  }

  function selectResult(idx) {
    document.querySelectorAll('.result-item, .result-item-studio').forEach(el => el.classList.remove('selected'));
    document.getElementById(`ri-${idx}`)?.classList.add('selected');
    selectedResult = window._searchResults[idx];
    // Hide spotlight grid so detail content is visible
    const sg = document.getElementById('spotlightGrid');
    if (sg) sg.style.display = 'none';
    // Show back-to-spotlight button if spotlight performers exist
    if (window._spotlightPerformers?.length) {
      document.getElementById('spotlightBackBtn').style.display = 'flex';
    }
    showDetail(selectedResult);
  }

  // ── Poster overlay ───────────────────────────────────────────────────

  function openPosterOverlay() {
    const img = document.querySelector('#detailPoster img');
    if (img) openImageOverlay(img.src);
  }

  // ── Image overlay ────────────────────────────────────────────────────

  function openImageOverlay(url) {
    if (!url) return;
    document.getElementById('imgOverlayImg').src = url;
    document.getElementById('imgOverlay').style.display = 'flex';
  }

  function closeImageOverlay() {
    document.getElementById('imgOverlay').style.display = 'none';
    document.getElementById('imgOverlayImg').src = '';
  }

  // ── Magazine image carousel ─────────────────────────────────────
  function openMagCarousel(images, startIdx) {
    if (!Array.isArray(images) || !images.length) return;
    window._magCarouselImages = images;
    window._magCarouselIdx = Math.max(0, Math.min(startIdx | 0, images.length - 1));
    const el = document.getElementById('magCarousel');
    if (!el) return;
    el.style.display = 'flex';
    _magCarouselUpdate();
  }
  function closeMagCarousel() {
    const el = document.getElementById('magCarousel');
    if (el) el.style.display = 'none';
  }
  function magCarouselNav(delta) {
    const arr = window._magCarouselImages || [];
    const n = arr.length;
    if (!n) return;
    window._magCarouselIdx = ((window._magCarouselIdx + (delta | 0)) % n + n) % n;
    _magCarouselUpdate();
  }
  function _magCarouselUpdate() {
    const arr = window._magCarouselImages || [];
    const i = window._magCarouselIdx | 0;
    const url = arr[i] || '';
    const img = document.getElementById('magCarouselImg');
    const cur = document.getElementById('magCarouselCur');
    const tot = document.getElementById('magCarouselTot');
    if (img) img.src = url;
    if (cur) cur.textContent = String(i + 1);
    if (tot) tot.textContent = String(arr.length);
    // Preload neighbours so prev/next clicks are instant.
    const n = arr.length;
    if (n > 1) {
      const next = arr[(i + 1) % n];
      const prev = arr[(i - 1 + n) % n];
      [next, prev].forEach(u => {
        if (u) { const im = new Image(); im.src = u; }
      });
    }
  }
  window.openMagCarousel  = openMagCarousel;
  window.closeMagCarousel = closeMagCarousel;
  window.magCarouselNav   = magCarouselNav;

  document.addEventListener('keydown', e => {
    if (e.key === 'Escape') {
      closeImageOverlay();
      closeMagCarousel();
      closeSceneOverlay();
    } else if (document.getElementById('magCarousel')?.style.display === 'flex') {
      if (e.key === 'ArrowLeft')  magCarouselNav(-1);
      if (e.key === 'ArrowRight') magCarouselNav(1);
    }
  });

  // ── Scenes ───────────────────────────────────────────────────────────

  let _sceneSources = {};  // cached scenes by source

  async function loadScenes(item) {
    const section = document.getElementById('scenesSection');
    const grid    = document.getElementById('scenesGrid');
    const title   = document.getElementById('scenesTitle');
    const tabs    = document.getElementById('sceneTabs');
    title.textContent = item.name ? ('Recent Scenes — ' + capDisplayName(item.name)) : 'Recent Scenes';
    tabs.style.display = 'none';
    _sceneSources = {};
    section.style.display = 'block';
    grid.className = 'scene-grid';
    grid.innerHTML = sceneGridSkeleton();
    try {
      const params = new URLSearchParams({
        type: currentType,
        source: item.source,
        id: item.id,
        slug: item.slug || '',
        name: item.name || '',
      });
      const r = await fetch(`/api/scenes/recent?${params}`);
      const d = await r.json();

      // Always show source tabs for performer/studio lookup.
      // Normalize older response shape that returns only d.scenes.
      _sceneSources = d.sources || { tpdb: [], stashdb: [], fansdb: [] };
      if (!d.sources && Array.isArray(d.scenes)) {
        const sourceKey = (String(item.source || '').toLowerCase() === 'stashdb')
          ? 'stashdb'
          : (String(item.source || '').toLowerCase() === 'fansdb' ? 'fansdb' : 'tpdb');
        _sceneSources[sourceKey] = d.scenes;
      }

      tabs.style.display = 'grid';
      const available = Object.entries(_sceneSources).filter(([k, v]) => v && v.length > 0);
      document.querySelectorAll('.source-btn').forEach(btn => {
        const src = btn.dataset.src;
        const scenes = _sceneSources[src] || [];
        const label = src === 'tpdb' ? 'TPDB' : src === 'stashdb' ? 'StashDB' : 'FansDB';
        btn.innerHTML = `<img src="/static/logos/${src}.png" alt="${label}" style="height:16px;width:auto;vertical-align:middle;opacity:0.9">`;
        if (scenes.length > 0) {
          btn.classList.remove('disabled');
        } else {
          btn.classList.add('disabled');
        }
      });

      if (available.length > 0) {
        switchSceneSource(available[0][0]);
      } else {
        grid.innerHTML = '<div class="empty">No recent scenes found</div>';
      }
      return;
    } catch(e) {
      grid.innerHTML = `<div class="empty">Failed to load scenes</div>`;
    }
  }

  // Tracks which DB source ('tpdb'|'stashdb'|'fansdb') the visible
  // scene grid was rendered from. When the user clicks Grab on a
  // search result we tag the download with this so /downloads can
  // later render the source scene's poster on the tile.
  let _currentSceneSource = 'tpdb';
  function switchSceneSource(src) {
    _currentSceneSource = src || 'tpdb';
    document.querySelectorAll('.source-btn').forEach(btn => {
      btn.classList.toggle('active', btn.dataset.src === src);
    });
    const scenes = _sceneSources[src] || [];
    if (scenes.length) {
      renderSceneGrid(scenes, false);
    } else {
      document.getElementById('scenesGrid').innerHTML = '<div class="empty">No scenes from this source</div>';
    }
  }

  function openSceneOverlay(scene) {
    if (!scene) return;
    window._sceneOverlay = scene;
    document.getElementById('sceneOverlayTitle').textContent = scene.title || 'Scene';
    // Prefer the gender-filtered performer list (`hover_performers`)
    // populated by the backend per the user's Content Filter settings —
    // when gay content is excluded, for example, this drops the male
    // performers so the auto-built Prowlarr query doesn't accidentally
    // bias the indexer search toward the excluded gender.
    const _perfList = (Array.isArray(scene.hover_performers) && scene.hover_performers.length)
      ? scene.hover_performers
      : (scene.performer || '').split(',').map(x => x.trim()).filter(Boolean);
    const _perfStr = _perfList.join(' ').trim();
    document.getElementById('sceneOverlayQuery').value = [_perfStr, scene.title || ''].filter(Boolean).join(' ');
    const sceneTags = Array.isArray(scene.tags) ? scene.tags : [];
    // Tags now live in the right column (under the performer info) and
    // are capped to the image height — see `.scene-overlay-tags-capped`
    // in scenes.html / discover.html for the hover-to-expand behaviour.
    const tagsHtml = sceneTags.length
      ? `<div class="scene-tag-chips scene-overlay-tags-capped">${sceneTags.map(t => `<span class="scene-card-tag-chip">${esc(t)}</span>`).join('')}</div>`
      : '';
    const sceneDesc = (scene.description || '').trim();
    // Plot now sits under the image in a full-width strip below the
    // grid so it can run the entire width of the popup.
    const descHtml = sceneDesc
      ? `<div class="scene-overlay-synopsis scene-overlay-plot-below">${esc(sceneDesc)}</div>`
      : '';
    // TPDB link moved to the static search-row's right-side stack
    // (under the search button) — see #sceneOverlayTpdbLink in the
    // scene-overlay HTML below. Its href is updated here per scene.
    const tpdbHref = scene.link || ('https://theporndb.net/scenes/' + (scene.id || ''));
    const tpdbLinkEl = document.getElementById('sceneOverlayTpdbLink');
    if (tpdbLinkEl) tpdbLinkEl.href = tpdbHref;
    // Inline the scene-card structure so the popup image picks up the
    // same theming chain as /scenes tiles (duotone tint, VHS-theme
    // overrides, studio-logo overlay). `.img-load` wraps the thumb
    // with the duo-tint sibling that the cascading rules target.
    const _ovStudio = (scene.studio || '').trim();
    const _ovTitle  = (scene.title || '').trim();
    const _ovStudioLogo = (_ovStudio || _ovTitle)
      ? `<img class="scene-studio-logo" src="/api/studio-logo?name=${encodeURIComponent(_ovStudio)}&q=${encodeURIComponent(_ovTitle)}" alt="" loading="lazy" onload="this.closest('.scene-card')?.classList.add('has-studio-logo')" onerror="this.remove()">`
      : '';
    document.getElementById('sceneOverlayMain').innerHTML = `
      <div class="scene-overlay-grid">
        <div>
          <div class="scene-card scene-overlay-thumb-card">
            <div class="img-load">
              <div class="img-spin" aria-hidden="true"></div>
              <img class="scene-thumb scene-overlay-thumb" src="${esc(scene.thumb || '/static/img/missing.jpg')}" loading="lazy" onload="this.closest('.img-load')?.classList.add('ready')" onerror="this.onerror=null;this.src='/static/img/missing.jpg';this.closest('.img-load')?.classList.add('ready');">
              <div class="duo-tint" aria-hidden="true"></div>
              ${_ovStudioLogo}
            </div>
          </div>
        </div>
        <div>
          <div id="sceneLibPerfs"></div>
          <div style="font-size:12px;color:var(--dim);line-height:1.8">
            ${scene.date ? `<div>Date: <span style="color:var(--text)">${esc(scene.date)}</span></div>` : ''}
            ${scene.studio ? `<div>Studio: <span style="color:var(--text)">${esc(capDisplayName(scene.studio))}</span></div>` : ''}
            ${(() => {
              // Performer line — prefer display_performers (with per-
              // performer gender) so each name renders with its
              // colour-coded badge. Falls back to the plain comma
              // string when display_performers is absent.
              const dp = Array.isArray(scene.display_performers) ? scene.display_performers : null;
              if (dp && dp.length) {
                // /api/scenes/recent embeds tpdb_id when the source is
                // TPDB; falls back to the unified `id` field on stash-
                // box-sourced lists. Pass both so the popup endpoint
                // can pick whichever matches the source it has data for.
                const sceneSrc = String(scene.source || '').toLowerCase();
                const html = dp.map(o => {
                  const nm = capDisplayName(o.name || '');
                  const tpdbId = o.tpdb_id || (sceneSrc.includes('tpdb') ? (o.id || o._id || '') : '');
                  const stashId = (sceneSrc === 'stashdb' || sceneSrc === 'fansdb') ? (o.id || o.stash_id) : (o.stash_id || '');
                  const attrs = window.performerLinkAttrs(o.name || '', {
                    gender: o.gender,
                    stashId: stashId,
                    tpdbId: tpdbId,
                  });
                  // Wrap NAME + BADGE in one clickable span so clicks on
                  // either part fire the popup.
                  return `<span${attrs ? ' ' + attrs : ''}${attrs ? ' class="perf-name-link"' : ''}>${esc(nm)}${genderBadge(o.gender)}</span>`;
                }).join(', ');
                return `<div>Performer: <span style="color:var(--text)">${html}</span></div>`;
              }
              return scene.performer
                ? `<div>Performer: <span style="color:var(--text)">${performerCsvHtml(scene.performer)}</span></div>`
                : '';
            })()}
          </div>
          ${tagsHtml}
        </div>
      </div>
      ${descHtml}`;
    document.getElementById('sceneOverlay').classList.add('open');
    // Async: fetch library performer headshots
    if (scene.performer) {
      const names = scene.performer;
      fetch(`/api/performers/headshots-by-name?names=${encodeURIComponent(names)}`, { credentials: 'same-origin' })
        .then(r => r.json())
        .then(d => {
          const perfs = d.performers || [];
          const el = document.getElementById('sceneLibPerfs');
          if (!el || !perfs.length) return;
          el.innerHTML = `<div class="lib-perfs-row">${perfs.map(p => {
            const img = p.headshot_url
              ? `<img src="${esc(p.headshot_url)}" alt="" onerror="this.replaceWith(Object.assign(document.createElement('div'),{className:'lib-perf-ph',innerHTML:'<i class=\\'fa-solid fa-user\\'></i>'}))">`
              : `<div class="lib-perf-ph"><i class="fa-solid fa-user"></i></div>`;
            const attrs = window.performerLinkAttrs(p.name, { gender: p.gender, libraryRowId: p.row_id || p.id });
            return `<div class="lib-perf-hs" title="${esc(p.name)}"${attrs ? ' ' + attrs : ''}>${img}<div class="lib-perf-hs-name">${esc(p.name)}</div></div>`;
          }).join('')}</div>`;
        })
        .catch(() => {});
    }
  }

  function closeSceneOverlay() {
    document.getElementById('sceneOverlay').classList.remove('open');
    window._sceneOverlay = null;
  }

  // ── Prowlarr (overlay) ────────────────────────────────────────────────

  // Scene-overlay search button now opens the unified Prowlarr search
  // popup (window.openProwlarrSearchPopup, defined in ts-utils.js) so
  // every search across the app shares one results UI / row layout.
  // Pulls the typed query and the scene's studio/performer context for
  // the popup's automatic title/studio fan-out.
  function openSceneOverlayProwlarrPopup() {
    const q = (document.getElementById('sceneOverlayQuery').value || '').trim();
    const _scene = window._sceneOverlay || {};
    const _perfList = (Array.isArray(_scene.hover_performers) && _scene.hover_performers.length)
      ? _scene.hover_performers
      : (_scene.performer || '').split(',').map(x => x.trim()).filter(Boolean);
    if (!q && !_scene.title) return;
    if (typeof window.openProwlarrSearchPopup !== 'function') return;
    window.openProwlarrSearchPopup({
      title:      q || _scene.title || '',
      studio:     _scene.studio || '',
      performers: _perfList.join(', '),
      thumb_url:  _scene.thumb || '',
      kind:       'scene',
    });
  }
  window.openSceneOverlayProwlarrPopup = openSceneOverlayProwlarrPopup;

  function truncateFilename(name, maxLen) {
    if (!name || name.length <= maxLen) return esc(name || '');
    return esc(name.slice(0, maxLen - 3)) + '...';
  }

  function closeAddSuccessOverlay() {
    const ov = document.getElementById('addSuccessOverlay');
    if (ov) ov.classList.remove('open');
    window._addSuccessProwlarrResults = null;
  }

  function openAddSuccessOverlay(name) {
    const raw = (name || '').trim();
    const label = capDisplayName(raw) || raw;
    const titleEl = document.getElementById('addSuccessTitle');
    if (titleEl) titleEl.textContent = 'Successfully Added ' + label;
    document.getElementById('addSuccessOverlay')?.classList.add('open');
    runAddSuccessProwlarrSearch(raw);
  }

  async function runAddSuccessProwlarrSearch(q) {
    const el = document.getElementById('addSuccessProwlResults');
    if (!el) return;
    const query = (q || '').trim();
    if (!query) {
      el.innerHTML = '<div class="empty">No name to search</div>';
      return;
    }
    el.innerHTML = '<div class="empty" style="padding:16px">Searching Prowlarr…</div>';
    try {
      const r = await fetch(`/api/prowlarr/search?q=${encodeURIComponent(query)}`);
      const d = await r.json();
      if (d.error) { el.innerHTML = `<div class="empty">${esc(d.error)}</div>`; return; }
      if (!d.results?.length) {
        el.innerHTML = '<div class="empty">No Prowlarr results — check indexers in Settings</div>';
        return;
      }
      window._addSuccessProwlarrResults = d.results;
      el.innerHTML = d.results.map((rrow, i) => `
        <div class="search-result" style="grid-template-columns:auto 1fr auto auto">
          <button type="button" class="btn-prowlarr-grab ${rrow.type === 'nzb' ? 'nzb' : ''}" title="Send to download client" onclick="grabAddSuccessResult(event, ${i})"><i class="fa-solid fa-download" aria-hidden="true"></i></button>
          <div style="min-width:0">
            <div class="sr-title" title="${esc(rrow.title)}">${truncateFilename(rrow.title, 60)}</div>
            <div class="sr-meta">${rrow.age ? Math.round(rrow.age/24) + 'd ago' : ''} ${rrow.seeders != null && rrow.seeders !== undefined ? '· ' + rrow.seeders + ' seeders' : ''}</div>
          </div>
          <span class="sr-indexer">${esc(_shortIndexer(rrow.indexer).replace(/ /g, '-'))}</span>
          <span class="sr-size">${rrow.size_mb > 1024 ? (rrow.size_mb/1024).toFixed(1) + ' GB' : rrow.size_mb + ' MB'}</span>
        </div>`).join('');
    } catch (e) {
      el.innerHTML = `<div class="empty">Search failed: ${esc(e.message)}</div>`;
    }
  }

  async function grabAddSuccessResult(ev, idx) {
    const result = window._addSuccessProwlarrResults && window._addSuccessProwlarrResults[idx];
    const btn = ev.target && ev.target.closest ? ev.target.closest('button') : null;
    if (!btn || !result) return;
    btn.disabled = true;
    btn.classList.remove('btn-prowlarr-grab--sent');
    btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin" aria-hidden="true"></i>';
    try {
      const r = await fetch('/api/prowlarr/grab', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          guid:         result.guid || '',
          indexer_id:   result.indexer_id != null ? result.indexer_id : null,
          type:         result.type,
          download_url: result.type === 'torrent' && result.magnet ? result.magnet : result.download_url,
        }),
      });
      const d = await r.json();
      if (d.ok) {
        btn.classList.add('btn-prowlarr-grab--sent');
        btn.innerHTML = '<i class="fa-solid fa-check" aria-hidden="true"></i>';
      } else {
        btn.disabled = false;
        btn.innerHTML = '<i class="fa-solid fa-download" aria-hidden="true"></i>';
        alert(d.error || 'Could not send to download client');
      }
    } catch (e) {
      btn.disabled = false;
      btn.innerHTML = '<i class="fa-solid fa-download" aria-hidden="true"></i>';
      alert(e.message || 'Could not send to download client');
    }
  }

  async function grabResult(ev, idx) {
    const result = window._sceneProwlarrResults[idx];
    const btn = ev.target && ev.target.closest ? ev.target.closest('button') : null;
    if (!btn || !result) return;
    btn.disabled = true;
    btn.classList.remove('btn-prowlarr-grab--sent');
    btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin" aria-hidden="true"></i>';
    // Tag the grab with the originating scene's metadata so /downloads
    // can render its poster on the tile and we can match this download
    // back to the scene in the queue later. The scene currently shown
    // in the overlay is the one being grabbed.
    const ovScene = window._sceneOverlay || {};
    const sourceScene = ovScene && ovScene.id ? {
      db:         _currentSceneSource || 'tpdb',
      id:         String(ovScene.id || ''),
      title:      ovScene.title || '',
      studio:     ovScene.studio || '',
      performers: ovScene.performer
        ? (Array.isArray(ovScene.performer) ? ovScene.performer : [ovScene.performer])
        : [],
      poster_url: ovScene.thumb || '',
      date:       ovScene.date || '',
    } : null;
    try {
      const r = await fetch('/api/prowlarr/grab', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          guid:         result.guid || '',
          indexer_id:   result.indexer_id != null ? result.indexer_id : null,
          type:         result.type,
          download_url: result.type === 'torrent' && result.magnet ? result.magnet : result.download_url,
          title:        result.title || '',
          source_scene: sourceScene,
        }),
      });
      const d = await r.json();
      if (d.ok) {
        btn.classList.add('btn-prowlarr-grab--sent');
        btn.innerHTML = '<i class="fa-solid fa-check" aria-hidden="true"></i>';
      } else {
        btn.disabled = false;
        btn.innerHTML = '<i class="fa-solid fa-download" aria-hidden="true"></i>';
        alert(d.error || 'Could not send to download client');
      }
    } catch(e) {
      btn.disabled = false;
      btn.innerHTML = '<i class="fa-solid fa-download" aria-hidden="true"></i>';
      alert(e.message || 'Could not send to download client');
    }
  }

  async function showDetail(item) {
    document.getElementById('detailEmpty').style.display = 'none';
    document.getElementById('detailContent').style.display = 'flex';
    // Performers carry a `gender` field on the search-result; studios
    // don't. Switching to innerHTML so we can append the badge inline
    // — `genderBadge` returns '' for empty/unknown so the studio path
    // is unaffected.
    document.getElementById('detailName').innerHTML = esc(capDisplayName(item.name || '')) + genderBadge(item.gender);
    // Reset the flag slot — populated by the preview fetch below once
    // the country comes back. Hidden for studios.
    const _flagSlot = document.getElementById('detailNameFlag');
    if (_flagSlot) _flagSlot.innerHTML = '';
    document.getElementById('detailMeta').innerHTML = `<span style="color:var(--dim);font-size:11px">${item.source}</span>`;
    // Feed the /discover info panel (recent scenes + secondary image).
    // currentType is 'performer' or 'studio' (set by setType()).
    loadDiscoverInfoPanel(item, currentType === 'studio' ? 'studio' : 'performer');
    setDetailBg(item.image);
    const libEl = document.getElementById('detailLibStatus');
    const destEl = document.getElementById('quickAddBar');
    libEl.innerHTML = '';
    libraryCheck(item.name)
      .then(lib => {
        if (lib.found) {
          libEl.innerHTML = `<span class="lib-tag lib-tag--in">IN-LIBRARY</span>`;
          if (destEl) destEl.style.display = 'none';
        } else {
          libEl.innerHTML = `<span class="lib-tag lib-tag--out">NOT-IN-LIBRARY</span>`;
          if (destEl) destEl.style.display = 'inline-flex';
        }
      });
    document.getElementById('detailBio').textContent = '';
    document.getElementById('resultMsg').style.display = 'none';
    document.getElementById('detailMeta').innerHTML = detailLoadingSkeleton();
    document.getElementById('detailLinks').innerHTML = '';

    // Switch layout based on type
    const layoutEl = document.getElementById('detailLayout');
    const posterEl = document.getElementById('detailPoster');
    const isStudio = currentType === 'studio';
    const icon = isStudio ? 'clapperboard' : 'person';
    const posterClass = isStudio ? 'detail-poster-studio' : 'detail-poster';

    if (isStudio) {
      // Studio: stacked — image on top, text below
      layoutEl.style.flexDirection = 'column';
      layoutEl.style.alignItems = 'stretch';
      layoutEl.style.gap = '12px';
      posterEl.style.flexShrink = '0';
      posterEl.style.display = 'block';
      posterEl.style.height = 'auto';
    } else {
      // Performer: side-by-side — image left, text right
      layoutEl.style.flexDirection = 'row';
      layoutEl.style.alignItems = 'stretch';
      layoutEl.style.gap = '20px';
      posterEl.style.flexShrink = '0';
      posterEl.style.display = 'flex';
      posterEl.style.height = '100%';
    }

    if (item.image) {
      posterEl.innerHTML = `<img class="${posterClass}" src="${esc(item.image)}" onclick="openImageOverlay('${esc(item.image)}')" onerror="this.outerHTML='<div class=detail-poster-placeholder><i class=fa-solid fa-${icon}></i></div>'">`;
    } else {
      posterEl.innerHTML = `<div class="detail-poster-placeholder"><i class="fa-solid fa-${icon}"></i></div>`;
    }

    loadDirs();
    loadScenes(item);

    // Use a lightweight preview fetch
    try {
      const r = await fetch(`/api/metadata/preview?type=${currentType}&source=${encodeURIComponent(item.source)}&id=${encodeURIComponent(item.id)}`);
      const d = await r.json();
      if (d.image) {
        posterEl.innerHTML = `<img class="${posterClass}" src="${esc(d.image)}" onclick="openImageOverlay('${esc(d.image)}')" onerror="this.outerHTML='<div class=detail-poster-placeholder><i class=fa-solid fa-${icon}></i></div>'">`;
        setDetailBg(d.image);
      }
      document.getElementById('detailBio').textContent = d.bio || '';
      if (d.meta) document.getElementById('detailMeta').innerHTML = d.meta;
      // Country flag chip next to the performer name (performers only).
      const flagSlot = document.getElementById('detailNameFlag');
      if (flagSlot) flagSlot.innerHTML = (currentType === 'performer') ? countryFlagHtml(d.country) : '';
      // Render links
      const linksEl = document.getElementById('detailLinks');
      if (d.links && d.links.length) {
        linksEl.innerHTML = renderLinks(d.links);
      } else {
        linksEl.innerHTML = '';
      }
      // Update slug on selected result for better scene lookups
      if (d.slug && selectedResult) {
        selectedResult.slug = d.slug;
        loadScenes(selectedResult);
      }
    } catch {
      document.getElementById('detailBio').textContent = '';
    }
  }

  function clearDetail() {
    selectedResult = null;
    document.getElementById('detailEmpty').style.display = 'block';
    document.getElementById('detailContent').style.display = 'none';
    document.getElementById('detailName').textContent = '';
    document.getElementById('detailLinks').innerHTML = '';
    setDetailBg(null);
    loadFeed();
  }

  async function loadDirs() {
    try {
      const r = await fetch('/api/metadata/dirs');
      const d = await r.json();
      const sel = document.getElementById('quickDestSelect');
      if (!sel) return;
      const options = ['<option value="">Choose directory…</option>']
        .concat((d.dirs || []).map(dir => `<option value="${esc(dir.path)}">${esc(dir.label)}</option>`))
        .concat(['<option value="__custom__">Custom path…</option>']);
      sel.innerHTML = options.join('');
      if (selectedDest) sel.value = selectedDest;
      handleQuickDestChange();
    } catch {}
  }

  function handleQuickDestChange() {
    const sel = document.getElementById('quickDestSelect');
    const custom = document.getElementById('quickDestCustom');
    if (!sel || !custom) return;
    if (sel.value === '__custom__') {
      selectedDest = null;
      custom.style.display = 'block';
    } else {
      selectedDest = sel.value || null;
      custom.style.display = 'none';
      if (sel.value !== '__custom__') custom.value = '';
    }
  }

  function clearDest() {
    selectedDest = null;
    const sel = document.getElementById('quickDestSelect');
    const custom = document.getElementById('quickDestCustom');
    if (sel) sel.value = '';
    if (custom) { custom.value = ''; custom.style.display = 'none'; }
  }

  async function createTvShow() {
    if (!selectedResult) { alert('Select a result first'); return; }
    const sel = document.getElementById('quickDestSelect');
    const customEl = document.getElementById('quickDestCustom');
    const dest = (sel && sel.value === '__custom__' ? customEl.value.trim() : '') || selectedDest;
    if (!dest) { alert('Choose a destination directory'); return; }

    const btn = document.getElementById('createBtn');
    btn.disabled = true;
    btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Creating...';

    try {
      const r = await fetch('/api/metadata/create', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          type:     currentType,
          source:   selectedResult.source,
          id:       selectedResult.id,
          name:     selectedResult.name || '',
          dest_dir: dest,
        }),
      });
      const d = await r.json();
      const msg = document.getElementById('resultMsg');
      if (d.success) {
        if (msg) { msg.style.display = 'none'; msg.textContent = ''; }
        if (window.TsActivity && window.TsActivity.refresh) window.TsActivity.refresh();
        openAddSuccessOverlay(d.name);
        // Auto-exclude from spotlight when added to library
        if (currentType === 'performer' && selectedResult?.source === 'stashdb' && selectedResult?.id) {
          fetch('/api/spotlight/exclude', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({id: selectedResult.id, name: selectedResult.name || ''})
          }).catch(() => {});
        }
        // Update library status and hide dest section
        document.getElementById('detailLibStatus').innerHTML =
          `<span class="lib-tag lib-tag--in">IN-LIBRARY</span>`;
        const ds = document.getElementById('quickAddBar');
        if (ds) ds.style.display = 'none'; clearDest();
        // Update search result status if visible
        if (selectedResult) {
          window._searchResults?.forEach((item, i) => {
            if (item.name === selectedResult.name) {
              const el = document.getElementById(`lib-${i}`);
              if (el) el.innerHTML = '<span class="lib-tag lib-tag--in">IN-LIBRARY</span>';
            }
          });
        }
      } else {
        msg.className = 'result-msg error';
        msg.textContent = `Error: ${d.error}`;
        msg.style.display = 'block';
      }
    } catch(e) {
      const msg = document.getElementById('resultMsg');
      msg.className = 'result-msg error';
      msg.textContent = `Error: ${e.message}`;
      msg.style.display = 'block';
    } finally {
      btn.disabled = false;
      btn.innerHTML = '<i class="fa-solid fa-folder-plus"></i> Add';
    }
  }

  // ── Feed (default scenes on page load) ────────────────────────────────

  //: Per-mode feed cache — switching between Your Feed / Vices /
  //: Studios / Performers / Movies refetches the same data every
  //: time, which for Movies takes multiple seconds and for the
  //: scene feeds makes the grid flash blank and rebuild. Keep the
  //: last response in memory keyed by "mode|filters"; re-render
  //: from cache when it's fresh, refetch in the background to
  //: refresh. TTL tuned so quick toggling feels instant but a tab
  //: left open for a while will re-fetch on next visit.
  const _FEED_CACHE_TTL_MS = 90 * 1000;
  const _feedCache = new Map();  // key -> { data, ts, title }

  function _feedCacheKey() {
    if (_scenesFeedMode === 'tags') {
      return `tags|${_selectedTagIds.slice().sort().join(',')}`;
    }
    return _scenesFeedMode;
  }

  function _feedCacheGet(key) {
    const hit = _feedCache.get(key);
    if (!hit) return null;
    if (Date.now() - hit.ts > _FEED_CACHE_TTL_MS) {
      _feedCache.delete(key);
      return null;
    }
    return hit;
  }

  function _feedCachePut(key, data, titleText) {
    _feedCache.set(key, { data, ts: Date.now(), title: titleText });
  }

  function _feedCacheInvalidate(key) {
    if (key) _feedCache.delete(key);
    else _feedCache.clear();
  }

  async function loadFeed(opts) {
    const force = !!(opts && opts.force);
    const section = document.getElementById('scenesSection');
    const grid    = document.getElementById('scenesGrid');
    const title   = document.getElementById('scenesTitle');
    const pag     = document.getElementById('movieFeedPagination');
    _applyFeedModeToggleUI();
    document.getElementById('sceneTabs').style.display = 'none';
    _sceneSources = {};
    section.style.display = 'block';

    //: Cache hit: render instantly, skip fetch. Search mode never
    //: caches (it's form-driven per request) and force-refresh
    //: bypasses regardless.
    const cacheKey = _feedCacheKey();
    if (!force && _scenesFeedMode !== 'search') {
      const hit = _feedCacheGet(cacheKey);
      if (hit) {
        if (title && hit.title) title.textContent = hit.title;
        if (pag) pag.style.display = 'none';
        const searchPanel = document.getElementById('scenesSearchPanel');
        if (searchPanel) searchPanel.style.display = 'none';
        if (_scenesFeedMode === 'movies') {
          grid.className = 'movie-grid';
          if (hit.data && hit.data.movies) {
            renderMovieGrid(hit.data.movies, grid);
          }
        } else {
          grid.className = 'scene-grid';
          if (hit.data && Array.isArray(hit.data.scenes)) {
            if (!hit.data.scenes.length && hit.data.emptyHtml) {
              grid.innerHTML = hit.data.emptyHtml;
            } else if (hit.data.scenes.length) {
              renderSceneGrid(hit.data.scenes, true);
            } else {
              grid.innerHTML = '<div class="empty">No scenes found</div>';
            }
          }
        }
        return;
      }
    }

    // Show/hide the search-form panel based on the active feed mode.
    const searchPanel = document.getElementById('scenesSearchPanel');
    if (searchPanel) searchPanel.style.display = (_scenesFeedMode === 'search') ? 'block' : 'none';
    // Search mode: let the panel drive the grid; don't auto-fetch.
    if (_scenesFeedMode === 'search') {
      title.textContent = 'Search';
      if (pag) pag.style.display = 'none';
      grid.className = 'scene-grid';
      if (!grid.dataset.searchPopulated) {
        grid.innerHTML = '<div class="empty">Fill in the form and search across StashDB, ThePornDB, and FansDB.</div>';
      }
      return;
    }

    // Movies feed mode — show TPDB movie grid
    if (_scenesFeedMode === 'movies') {
      title.textContent = 'Latest Movies';
      if (pag) pag.style.display = 'none';
      grid.innerHTML = movieGridSkeleton(30);
      grid.className = 'movie-grid';
      try {
        const r = await fetch('/api/movies/tpdb/latest?page=1');
        const d = await r.json();
        const movies = (d.results || []).slice(0, 30);
        if (!movies.length) {
          let msg = 'No movies found';
          if (d.error) {
            if (String(d.error).includes('TPDB returned 401')) {
              msg += '. TPDB authentication failed (401). Add or update your TPDB API key in Settings.';
            } else { msg += ' (' + esc(d.error) + ')'; }
          }
          grid.innerHTML = `<div class="empty movie-grid-empty">${msg}</div>`;
          return;
        }
        _movieFeedPage = 1;
        _movieFeedTotalPages = d.total_pages || 1;
        renderMovieGrid(movies, grid);
        _feedCachePut(cacheKey, { movies }, 'Latest Movies');
      } catch(e) {
        grid.innerHTML = `<div class="empty movie-grid-empty">Error: ${esc(e.message)}</div>`;
      }
      return;
    }

    // Scene feed modes
    grid.className = 'scene-grid';
    if (pag) pag.style.display = 'none';
    grid.innerHTML = sceneGridSkeleton();
    try {
      let url = '/api/scenes/feed?mode=' + encodeURIComponent(_scenesFeedMode);
      if (_scenesFeedMode === 'tags' && _selectedTagIds.length) {
        url += '&tag_ids=' + encodeURIComponent(_selectedTagIds.join(','));
      }
      // `force` (set by the toolbar refresh button) bypasses both the
      // local 90s feedCache and the server's 1h _feed_cache so the
      // request hits the source DBs fresh.
      if (force) url += '&refresh=1';
      const r = await fetch(url);
      const d = await r.json();
      if (_scenesFeedMode === 'tags') {
        title.textContent = 'Latest by Vice';
        if (d && d.error === 'no_tags') {
          // Distinguish "no vices configured" from "has vices but none
          // selected on this page".
          const emptyHtml = !_monitoredTagsCache.length
            ? '<div class="empty">No vices configured. Add some under Settings &rarr; Content Filters &rarr; Vices.</div>'
            : '<div class="empty">No vices selected. Click the <i class="fa-solid fa-fire"></i> button above and pick one or more.</div>';
          grid.innerHTML = emptyHtml;
          _feedCachePut(cacheKey, { scenes: [], emptyHtml }, 'Latest by Vice');
          return;
        }
        if (!d.scenes?.length) {
          const emptyHtml = '<div class="empty">No scenes found for the selected tags.</div>';
          grid.innerHTML = emptyHtml;
          _feedCachePut(cacheKey, { scenes: [], emptyHtml }, 'Latest by Vice');
          return;
        }
        renderSceneGrid(d.scenes, true);
        _feedCachePut(cacheKey, { scenes: d.scenes }, 'Latest by Vice');
        return;
      }
      if (_scenesFeedMode === 'studios') {
        title.textContent = 'New Releases';
        if (!d.scenes?.length) {
          const emptyHtml = '<div class="empty">No studio feed scenes — add studios under Favourites with a TPDB site match, or favourite sites on ThePornDB (same API key). Check Settings for your TPDB key.</div>';
          grid.innerHTML = emptyHtml;
          _feedCachePut(cacheKey, { scenes: [], emptyHtml }, 'New Releases');
          return;
        }
        renderSceneGrid(d.scenes, true);
        _feedCachePut(cacheKey, { scenes: d.scenes }, 'New Releases');
        return;
      }
      if (_scenesFeedMode === 'performers') {
        title.textContent = 'New Releases';
        if (!d.scenes?.length) {
          const emptyHtml = '<div class="empty">No performer feed scenes — check your TPDB API key in Settings, and favourite performers on ThePornDB so the Atom feed can list their new releases, or add performer folders under Settings for a sampled feed.</div>';
          grid.innerHTML = emptyHtml;
          _feedCachePut(cacheKey, { scenes: [], emptyHtml }, 'New Releases');
          return;
        }
        renderSceneGrid(d.scenes, true);
        _feedCachePut(cacheKey, { scenes: d.scenes }, 'New Releases');
        return;
      }
      title.textContent = 'New Releases';
      if (!d.scenes?.length) {
        const emptyHtml = '<div class="empty">No feed scenes — check your TPDB API key in Settings</div>';
        grid.innerHTML = emptyHtml;
        _feedCachePut(cacheKey, { scenes: [], emptyHtml }, 'New Releases');
        return;
      }
      renderSceneGrid(d.scenes, true);
      _feedCachePut(cacheKey, { scenes: d.scenes }, 'New Releases');
    } catch(e) {
      grid.innerHTML = '<div class="empty">Failed to load feed</div>';
    }
  }

  // Lazy-fetch library headshots matching a card's performer names on first
  // hover. Caches via data attribute so a second hover is free. Silently
  // no-ops when: the card has no performer string, the fetch fails, or no
  // library matches come back.
  async function ensureCardHeadshots(card, performerStr) {
    if (!card || card.dataset.headshotsLoaded === '1') return;
    card.dataset.headshotsLoaded = '1';
    const s = (performerStr || '').trim();
    if (!s) return;
    try {
      const r = await fetch('/api/performers/headshots-by-name?names=' + encodeURIComponent(s));
      const d = await r.json();
      const perfs = (d && d.performers) || [];
      if (!perfs.length) return;
      const imgLoad = card.querySelector('.img-load');
      if (!imgLoad) return;
      // Movies fit more performers into their centered middle band
      // than the landscape scene still does — scene cards tend to
      // credit 2-3 faces, movies can have ten or more.
      const isMovie = card.classList.contains('movie-card');
      const cap = isMovie ? 8 : 4;
      const shown = perfs.slice(0, cap);
      const extra = perfs.length - shown.length;
      const wrap = document.createElement('div');
      wrap.className = 'scene-headshots-hover';
      const avatarsHtml = shown.map(p => {
        const attrs = window.performerLinkAttrs(p.name, { gender: p.gender, libraryRowId: p.row_id || p.id });
        return `<img class="scene-headshot-avatar" src="${esc(p.headshot_url || '')}" alt="${esc(p.name)}" title="${esc(p.name)}" onerror="this.remove()"${attrs ? ' ' + attrs : ''}>`;
      }).join('');
      // `+N` chip on movies when the cast overflows the visible cap —
      // gives a sense of how deep the ensemble is without blowing the
      // layout past the logo band.
      const moreChip = (isMovie && extra > 0)
        ? `<span class="scene-headshot-more" title="${extra} more performer${extra === 1 ? '' : 's'}">+${extra}</span>`
        : '';
      wrap.innerHTML = avatarsHtml + moreChip;
      imgLoad.appendChild(wrap);
      // Flag the card so the studio-logo fallback (rendered for the
      // /discover studio view) can hide itself once real headshots
      // arrive — see `.discover-info-scene-card--studio.has-headshots`
      // in discover.html.
      card.classList.add('has-headshots');
    } catch (e) {}
  }

  // ── In-library badge ─────────────────────────────────────────────
  // After a grid renders, batch-fetch /api/library/scenes-in for the
  // visible scenes and decorate matched cards with a tick-in-a-box
  // overlay in the bottom-right of the thumbnail. Called from
  // renderSceneGrid (scenes) and renderMovieGrid (movies).
  async function decorateLibraryMatches(items, opts) {
    if (!Array.isArray(items) || !items.length) return;
    const sourceMap = (opts && opts.sourceMap) || null;
    const containerSelector = (opts && opts.containerSelector) || '.scene-card';
    const idAttr = (opts && opts.idAttr) || 'data-scene-i';
    // Build the richer items[] form so the backend's title+date fallback
    // can match scenes imported under a different stash-box than the one
    // currently being browsed (e.g. file matched on StashDB but TPDB
    // serves the same scene under a different id). Without title/date
    // the fallback is a no-op and we only get id-direct + phash hits.
    const itemsOut = [];
    items.forEach((s, i) => {
      const sid = String(s.id || s._id || '');
      if (!sid) return;
      let src = (s.source || '').toLowerCase();
      if (sourceMap && sourceMap[i]) src = sourceMap[i].toLowerCase();
      if (src.startsWith('search_')) src = src.slice(7);
      if (src === 'theporndb') src = 'tpdb';
      if (!['stashdb', 'fansdb', 'tpdb'].includes(src)) return;
      itemsOut.push({
        source: src,
        id: sid,
        title:  s.title  || '',
        date:   s.date   || s.release_date || '',
        studio: s.studio || (s.studio_name || ''),
      });
    });
    if (!itemsOut.length) return;
    let data;
    try {
      const r = await fetch('/api/library/scenes-in', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ items: itemsOut }),
      });
      data = await r.json();
    } catch (e) { return; }
    const matches = (data && data.matches) || {};
    if (!Object.keys(matches).length) return;
    items.forEach((s, i) => {
      const sid = String(s.id || s._id || '');
      if (!sid) return;
      let src = (s.source || '').toLowerCase();
      if (sourceMap && sourceMap[i]) src = sourceMap[i].toLowerCase();
      if (src.startsWith('search_')) src = src.slice(7);
      if (src === 'theporndb') src = 'tpdb';
      const key = `${src}:${sid}`;
      if (!matches[key]) return;
      const card = document.querySelector(`${containerSelector}[${idAttr}="${i}"]`);
      if (!card) return;
      card.classList.add('is-in-library');
      // Swap the hover-only watch button into a "collected" indicator
      // when the scene is in our library — same corner position, but
      // ticket icon + muted red instead of eye icon. Click-to-toggle
      // still works (so the user can re-mark wanted if they want).
      const watchBtn = card.querySelector('.scene-wanted-btn');
      if (watchBtn && !watchBtn.classList.contains('is-collected')) {
        watchBtn.classList.add('is-collected');
        watchBtn.title = 'In your library';
        watchBtn.innerHTML = '<wa-icon name="ticket"></wa-icon>';
      }
      // Card has no wanted button (e.g. the studio/performer info-panel
      // scene cards rendered by `loadDiscoverInfoPanel`). Drop a
      // standalone ticket badge onto the thumbnail so the in-library
      // signal stays obvious there too. Self-contained class — does
      // NOT inherit `.scene-wanted-btn` page-local styling (which is
      // hover-only on /scenes and would hide the badge at rest).
      if (!watchBtn && !card.querySelector('.lib-collected-badge')) {
        const host = card.querySelector('.img-load') || card;
        const badge = document.createElement('span');
        badge.className = 'lib-collected-badge';
        badge.title = 'In your library';
        badge.innerHTML = '<i class="fa-solid fa-ticket"></i>';
        host.appendChild(badge);
      }
    });
  }
  window.decorateLibraryMatches = decorateLibraryMatches;

  // De-dupe: same scene returned by multiple stash-boxes (e.g. StashDB
  // and TPDB both index "Brianna Brown's Audacious Bang Casting" with
  // slightly different titles). Key = studio|date|performers (sorted,
  // case-folded). Static placeholders are skipped (no real identity).
  // First occurrence wins so the bucket order in /api/scenes/recent
  // (TPDB → StashDB → FansDB) is preserved.
  function dedupeScenes(scenes) {
    if (!Array.isArray(scenes) || scenes.length < 2) return scenes || [];
    const seen = new Set();
    const out = [];
    for (const s of scenes) {
      if (!s || s.__static) { out.push(s); continue; }
      const studio = String(s.studio || s.site_name || '').trim().toLowerCase();
      const date = String(s.date || s.release_date || '').slice(0, 10);
      let perfs = '';
      if (Array.isArray(s.performers)) {
        perfs = s.performers.map(p => String(typeof p === 'string' ? p : (p && p.name) || '').trim().toLowerCase()).filter(Boolean).sort().join(',');
      } else if (typeof s.performer === 'string') {
        perfs = s.performer.split(',').map(x => x.trim().toLowerCase()).filter(Boolean).sort().join(',');
      }
      // Fallback when none of those are populated: title prefix + date.
      const titlePrefix = String(s.title || '').trim().toLowerCase().slice(0, 24);
      const key = (studio || date || perfs)
        ? `${studio}|${date}|${perfs}`
        : `t:${titlePrefix}|${date}`;
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(s);
    }
    return out;
  }
  window.dedupeScenes = dedupeScenes;

  function renderSceneGrid(scenes, isFeed) {
    const grid = document.getElementById('scenesGrid');
    const deduped = dedupeScenes(scenes);
    const visibleScenes = deduped.slice(0, 30); // 6 rows x 5 columns
    _sceneGridItems = visibleScenes;
    grid.innerHTML = visibleScenes.map((s, i) => `
      <div class="scene-card" id="sc-${esc(s.id)}" data-scene-i="${i}" data-performer="${esc(s.performer || '')}" role="button" tabindex="0" aria-label="Open scene details" onmouseenter="ensureCardHeadshots(this, this.dataset.performer)">
        <div class="img-load">
          <div class="img-spin" aria-hidden="true"></div>
          ${s.thumb
            ? `<img class="scene-thumb" src="${esc(s.thumb)}" loading="lazy" onload="this.closest('.img-load')?.classList.add('ready')" onerror="const w=this.closest('.img-load'); if(this.src.indexOf('missing.jpg')<0){this.src='/static/img/missing.jpg'}else{w?.classList.add('ready')}">`
            : `<img class="scene-thumb" src="/static/img/missing.jpg" loading="lazy" onload="this.closest('.img-load')?.classList.add('ready')" onerror="this.closest('.img-load')?.classList.add('ready')">`}
          <div class="duo-tint" aria-hidden="true"></div>
          ${(s.studio || s.title) ? `<img class="scene-studio-logo" src="/api/studio-logo?name=${encodeURIComponent(s.studio || '')}&q=${encodeURIComponent(s.title || '')}" alt="" loading="lazy" onload="this.closest('.scene-card')?.classList.add('has-studio-logo')" onerror="this.remove()">` : ''}
          ${(() => {
            // Top-LEFT source-DB badge, only rendered in Search mode
            // where the source actually varies (TPDB / StashDB / FansDB).
            // Hidden on hover so the top-left headshot cluster that
            // fades in has the corner to itself. Other feed modes are
            // always TPDB, so the badge would be visual noise there.
            const raw = (s.source || '').toLowerCase();
            if (!raw.startsWith('search_')) return '';
            const key = raw.slice(7);
            const logos = {
              tpdb:    { src: '/static/logos/tpdb.png',    label: 'TPDB' },
              stashdb: { src: '/static/logos/stashdb.png', label: 'StashDB' },
              fansdb:  { src: '/static/logos/fansdb.png',  label: 'FansDB' },
            };
            const meta = logos[key];
            if (!meta) return '';
            return `<img class="scene-source-logo" src="${esc(meta.src)}" alt="${esc(meta.label)}" title="From ${esc(meta.label)}" onerror="this.remove()">`;
          })()}
          ${s.id ? _cardWantedButtonHtml('scene', ((s.source || '').startsWith('search_') ? s.source.slice(7) : ((s.source || '').toLowerCase() === 'stashdb' || (s.source || '').toLowerCase() === 'fansdb' ? (s.source || '').toLowerCase() : 'tpdb')), String(s.id)) : ''}
          ${(() => {
            // Performer name overlay, bottom-centre. Single line, highlighter
            // background, ellipsis on overflow. Prefer the gender-filtered
            // ``hover_performers`` for *which* performers to show. No
            // gender badges here — labels overlaid on the image stay
            // text-only so the photo carries the visual focus.
            // Prefer display_performers (objects with gender) so the
            // clickability gate can apply per-name. Fall back to
            // hover_performers / split CSV when the backend didn't
            // attach the gendered list.
            const dp = Array.isArray(s.display_performers) ? s.display_performers : null;
            const filtered = dp && dp.length
              ? dp
              : (Array.isArray(s.hover_performers) && s.hover_performers.length
                  ? s.hover_performers
                  : (s.performer || '').split(',').map(x => x.trim()).filter(Boolean));
            if (!filtered.length) return '';
            const html = filtered.map(p => {
              const nm = typeof p === 'object' ? (p.name || '') : String(p || '');
              if (!nm) return '';
              const g = typeof p === 'object' ? p.gender : '';
              const attrs = window.performerLinkAttrs(nm, { gender: g });
              const cls = attrs ? ' class="perf-name-link"' : '';
              return `<span${attrs ? ' ' + attrs : ''}${cls}>${esc(capDisplayName(nm))}</span>`;
            }).filter(Boolean).join(', ');
            return `<div class="scene-performer-label"><span class="perf-hl">${html}</span></div>`;
          })()}
          ${(() => {
            // Tag chips overlaid in the middle of the thumbnail on hover.
            // In Tags mode the backend annotates ``matched_tags`` (the
            // intersection of scene tags with the user's selection —
            // this is the only correct set to display, so we honour it
            // even when empty). In other modes ``matched_tags`` is
            // undefined and we fall back to every tag on the scene.
            const source = Array.isArray(s.matched_tags)
              ? s.matched_tags
              : (Array.isArray(s.tags) ? s.tags : []);
            // Cap at 10 so the overlay comfortably fits ~3-4 rows within
            // the 46%-height band; the CSS mask fades any overflow at the
            // top edge. Any extra tags are summarised as a "+N more" chip
            // so the card still hints at how rich the scene tagging is.
            const cap = 10;
            const tags = source.slice(0, cap);
            const extra = Math.max(0, source.length - tags.length);
            if (!tags.length) return '';
            const chips = tags.map(t => `<span class="scene-card-tag-chip">${esc(t)}</span>`).join('');
            const more = extra > 0
              ? `<span class="scene-card-tag-chip" style="opacity:0.82">+${extra} more</span>`
              : '';
            return `<div class="scene-card-tags-hover">${chips}${more}</div>`;
          })()}
        </div>
        <div class="scene-info">
          <div class="scene-title" title="${esc(s.title)}">${esc(s.title)}</div>
          <div class="scene-date" title="${esc((s.date || '') + (s.studio ? ' · ' + s.studio : ''))}"><span class="meta-date">${s.date || ''}</span>${s.studio ? `<span class="meta-studio-fallback">${s.date ? ' · ' : ''}${esc(capDisplayName(s.studio))}</span>` : ''}</div>
        </div>
      </div>`).join('');
    // Each rendered card carries data-scene-i; selector targets only
    // those (skips the spotlight row's tiles which use data-performer-id).
    decorateLibraryMatches(visibleScenes, {
      sourceMap: visibleScenes.map(s => (s.source || _currentSceneSource || 'tpdb')),
      containerSelector: '.scene-card',
      idAttr: 'data-scene-i',
    });
  }

  // ── Spotlight performer row ────────────────────────────────────────

  async function loadSpotlightRow() {
    try {
      const r = await fetch('/api/metadata/spotlight-performers');
      const d = await r.json();
      const performers = d.performers || [];
      if (!performers.length) {
        const emptyEl = document.getElementById('detailEmpty');
        if (emptyEl && d.error === 'no_stashdb_key') {
          emptyEl.textContent = 'Add a StashDB API key under Settings → Databases to enable the performer spotlight';
        }
        return;
      }

      window._spotlightPerformers = performers;

      const gridEl = document.getElementById('spotlightGrid');
      // Each tile carries `--tile-index` so its `::after` pseudo (CSS in
      // app-shell.css) can paint its slice of spotlight.jpg. With one
      // overlay per tile (instead of a single full-row div), hovering a
      // tile drops only that tile's overlay while the rest stay lit.
      gridEl.innerHTML = performers.map((p, i) => `
        <div class="spotlight-tile${p.library_fill ? ' spotlight-tile--library' : ''}" tabindex="0" title="${esc(p.name)}" data-performer-id="${esc(p.id)}"
             style="--tile-index:${i}"
             onclick="spotlightTileClick(${i})"
             onkeydown="if(event.key==='Enter')spotlightTileClick(${i})">
          <div class="spotlight-tile-art" style="--tile-bg:url('${esc(p.image)}')">
            <img src="${esc(p.image)}" alt="${esc(p.name)}" loading="lazy"
                 crossorigin="anonymous"
                 onload="autoLevelSpotlightTile(this)"
                 onerror="this.style.display='none'">
          </div>
          <button class="spotlight-exclude-btn" title="Never show again"
                  onclick="excludeSpotlightPerformer(event,'${esc(p.id)}','${esc(p.name.replace(/'/g,"&#39;"))}')">✕</button>
          ${p.library_fill ? `<span class="spotlight-tile-lib-badge" title="In your library"><i class="fa-solid fa-database"></i></span>` : ''}
          <div class="spotlight-tile-name">${esc(p.name)}</div>
        </div>`).join('');

      // Show grid, hide empty state
      gridEl.style.display = 'flex';
      // Expose tile count for VHS-theme CSS that scales the
      // diagonal sliver width with the actual tile width
      // (slivers should be ~12% of tile width to keep the
      // hypotenuse parallel to the tile's slanted edge).
      gridEl.style.setProperty('--tile-count', performers.length);
      document.getElementById('detailEmpty').style.display = 'none';
    } catch(e) {
      console.error('Spotlight fetch failed:', e);
    }
  }

  function showSpotlightGrid() {
    document.getElementById('detailContent').style.display = 'none';
    document.getElementById('detailEmpty').style.display = 'none';
    document.getElementById('spotlightBackBtn').style.display = 'none';
    const gridEl = document.getElementById('spotlightGrid');
    if (gridEl) gridEl.style.display = 'flex';
  }

  async function excludeSpotlightPerformer(evt, id, name) {
    evt.stopPropagation();
    const tile = evt.currentTarget.closest('.spotlight-tile');
    if (tile) { tile.style.opacity = '0.3'; tile.style.pointerEvents = 'none'; }
    try {
      await fetch('/api/spotlight/exclude', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({id, name}),
      });
      // Reload from the buffer — server already filtered this performer out
      await loadSpotlightRow();
    } catch(e) {
      if (tile) { tile.style.opacity = '1'; tile.style.pointerEvents = ''; }
    }
  }

  async function spotlightTileClick(idx) {
    const performers = window._spotlightPerformers || [];
    const p = performers[idx];
    if (!p) return;

    // Feed the /discover info panel (recent scenes + secondary image).
    loadDiscoverInfoPanel(p, 'performer');

    // Hide spotlight grid, show detail content
    const gridEl = document.getElementById('spotlightGrid');
    if (gridEl) gridEl.style.display = 'none';
    document.getElementById('detailEmpty').style.display = 'none';
    document.getElementById('detailContent').style.display = 'flex';
    document.getElementById('spotlightBackBtn').style.display = 'flex';
    document.getElementById('detailName').innerHTML = esc(capDisplayName(p.name || '')) + genderBadge(p.gender);
    // Spotlight performer payload already carries `country` — render
    // the flag immediately rather than waiting for the preview fetch.
    const _spotFlagSlot = document.getElementById('detailNameFlag');
    if (_spotFlagSlot) _spotFlagSlot.innerHTML = countryFlagHtml(p.country);

    const layoutEl = document.getElementById('detailLayout');
    const posterEl = document.getElementById('detailPoster');
    layoutEl.style.flexDirection = 'row';
    layoutEl.style.alignItems = 'stretch';
    layoutEl.style.gap = '20px';
    posterEl.style.flexShrink = '0';
    posterEl.style.display = 'flex';
    posterEl.style.height = '100%';

    if (p.image) {
      posterEl.innerHTML = `<img class="detail-poster" src="${esc(p.image)}" onclick="openImageOverlay('${esc(p.image)}')" onerror="this.outerHTML='<div class=detail-poster-placeholder><i class=fa-solid fa-person></i></div>'">`;
      setDetailBg(p.image);
    } else {
      posterEl.innerHTML = `<div class="detail-poster-placeholder"><i class="fa-solid fa-person"></i></div>`;
    }

    // Branch on the performer's source: spotlight rows now merge StashDB +
    // FansDB candidates, so a tile might be either. `p.source` is stamped
    // on each candidate by _spotlight_fetch_from_stashbox() in main.py.
    const srcLabel  = (p.source === 'FansDB') ? 'FansDB' : 'StashDB';
    const srcHost   = (srcLabel === 'FansDB') ? 'https://fansdb.cc' : 'https://stashdb.org';
    const srcLogo   = (srcLabel === 'FansDB') ? '/static/logos/fansdb.png' : '/static/logos/stashdb.png';
    const profileUrl = `${srcHost}/performers/${p.id}`;

    // Build meta using the detected source label
    const metaParts = [`<span>${srcLabel}</span>`];
    if (p.birthdate)        metaParts.push(`Born: <span>${esc(p.birthdate)}</span>`);
    const activeYear = p.career_start_year || p.career_start_inferred;
    if (activeYear)         metaParts.push(`Active: <span>${esc(String(activeYear))}${p.career_start_inferred ? '*' : ''}</span>`);
    if (p.ethnicity)        metaParts.push(`Ethnicity: <span>${esc(p.ethnicity)}</span>`);
    if (p.measurements)     metaParts.push(`Stats: <span>${esc(p.measurements)}</span>`);
    document.getElementById('detailMeta').innerHTML = metaParts.join(' &middot; ');

    document.getElementById('detailBio').textContent = '';
    document.getElementById('detailLinks').innerHTML = p.id
      ? `<a class="detail-link db-link" href="${esc(profileUrl)}" target="_blank" title="View on ${esc(srcLabel)}"><img src="${esc(srcLogo)}" alt="${esc(srcLabel)}" style="height:16px;width:auto;vertical-align:middle;opacity:0.9"></a>`
      : '';

    // Fetch full detail (bio + links) via preview — pass the right source
    try {
      const prev = await fetch(`/api/metadata/preview?type=performer&source=${encodeURIComponent(srcLabel)}&id=${encodeURIComponent(p.id)}`).then(r => r.json());
      if (prev.bio)   document.getElementById('detailBio').textContent = prev.bio;
      if (prev.links?.length) document.getElementById('detailLinks').innerHTML = renderLinks(prev.links);
    } catch(e) {}

    // Library status check
    const libEl  = document.getElementById('detailLibStatus');
    const destEl = document.getElementById('quickAddBar');
    try {
      const lib = await libraryCheck(p.name);
      if (lib.found) {
        libEl.innerHTML = `<span class="lib-tag lib-tag--in">IN-LIBRARY</span>`;
        if (destEl) destEl.style.display = 'none';
      } else {
        libEl.innerHTML = `<span class="lib-tag lib-tag--out">NOT-IN-LIBRARY</span>`;
        if (destEl) destEl.style.display = 'inline-flex';
      }
    } catch(e) {}

    selectedResult = { name: p.name, id: p.id, slug: '', source: srcLabel, image: p.image };
    // Ensure type is set to performer for spotlight additions
    currentType = 'performer';
    document.getElementById('btnMovie')?.classList.remove('active');
    document.getElementById('btnPerformer')?.classList.add('active');
    document.getElementById('btnStudio')?.classList.remove('active');
    loadDirs();
  }

  // Guard the listener attachment — `scenesGrid` only exists on
  // /scenes; on /discover the bare addEventListener was throwing on
  // null and halting the rest of the script (including spotlight init).
  document.getElementById('scenesGrid')?.addEventListener('click', function (e) {
    const card = e.target.closest('.scene-card[data-scene-i]');
    if (!card) return;
    const i = parseInt(card.getAttribute('data-scene-i'), 10);
    if (Number.isNaN(i) || !_sceneGridItems[i]) return;
    openSceneOverlay(_sceneGridItems[i]);
  });
  document.getElementById('scenesGrid')?.addEventListener('keydown', function (e) {
    if (e.key !== 'Enter' && e.key !== ' ') return;
    const card = e.target.closest('.scene-card[data-scene-i]');
    if (!card) return;
    e.preventDefault();
    const i = parseInt(card.getAttribute('data-scene-i'), 10);
    if (Number.isNaN(i) || !_sceneGridItems[i]) return;
    openSceneOverlay(_sceneGridItems[i]);
  });

  // ── Movie functions ──────────────────────────────────────────────────

  let _movieFeedPage = 1, _movieFeedTotalPages = 1;

  function movieTitleDisplay(s) {
    // Storage form puts leading articles at the end for sort:
    //   "Education of My Young Neighbor, The"
    //   "Hangover, A"
    // Restore the natural form for display.
    const trimmed = String(s || '').trim();
    const m = trimmed.match(/^(.*),\s+(The|A|An)\s*$/i);
    return esc(m ? `${m[2]} ${m[1]}` : trimmed);
  }

  function movieGridSkeleton(count = 20) {
    return Array.from({length: count}, () => `
      <div class="movie-card loading">
        <div class="skeleton-box" style="width:100%;aspect-ratio:27/40">
          <div class="img-spin" aria-hidden="true"></div>
        </div>
        <div class="movie-info">
          <div class="skeleton-line" style="width:82%"></div>
          <div class="skeleton-line" style="width:58%;margin-bottom:0"></div>
        </div>
      </div>`).join('');
  }

  function movieDetailSkeleton() {
    return `
      <div class="movie-detail-inner">
        <div class="movie-detail-poster-wrap"><div class="skeleton-box" style="position:absolute;inset:0"><div class="img-spin" aria-hidden="true"></div></div></div>
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

  function renderMovieGrid(movies, gridEl) {
    const posterFallback = '/static/img/poster.jpg';
    gridEl.innerHTML = movies.map((m, i) => {
      const posterUrl = (m.poster && String(m.poster).trim()) ? m.poster : posterFallback;
      return `
      <div class="movie-card" data-movie-i="${i}" data-movie-id="${esc(m.id)}" data-performer="${esc(m.performer || '')}" onmouseenter="ensureCardHeadshots(this, this.dataset.performer)">
        <div class="img-load"><div class="img-spin" aria-hidden="true"></div><img class="movie-poster" src="${esc(posterUrl)}" loading="lazy" onload="this.closest('.img-load')?.classList.add('ready')" onerror="this.onerror=null;this.src='${posterFallback}';this.closest('.img-load')?.classList.add('ready');"><div class="duo-tint" aria-hidden="true"></div>${(m.studio || m.title) ? `<img class="scene-studio-logo" src="/api/studio-logo?name=${encodeURIComponent(m.studio || '')}&q=${encodeURIComponent(m.title || '')}" alt="" loading="lazy" onload="this.closest('.movie-card')?.classList.add('has-studio-logo')" onerror="this.remove()">` : ''}${m.id ? _cardWantedButtonHtml('movie', 'tpdb', String(m.id)) : ''}</div>
        <div class="movie-info">
          <div class="movie-title" title="${esc(m.title)}">${movieTitleDisplay(m.title)}</div>
          <div class="movie-meta" title="${esc((m.date || '') + (m.studio ? ' · ' + m.studio : ''))}"><span class="meta-date">${m.date || ''}</span>${m.studio ? `<span class="meta-studio-fallback">${m.date ? ' · ' : ''}${esc(m.studio)}</span>` : ''}</div>
        </div>
      </div>`;
    }).join('');
    // Movies in /scenes are TPDB-sourced; phash crosswalk only resolves
    // when fingerprints are cached (so first-visit shows nothing,
    // subsequent visits decorate after the backfill catches up).
    decorateLibraryMatches(movies, {
      sourceMap: movies.map(() => 'tpdb'),
      containerSelector: '.movie-card',
      idAttr: 'data-movie-i',
    });
  }

  async function searchMovies() {
    const q = document.getElementById('movieSearchInput').value.trim();
    const year = document.getElementById('movieSearchYear').value.trim();
    const el = document.getElementById('movieSearchResults');
    if (!q && !year) return;
    el.innerHTML = '<div class="empty">Searching…</div>';
    try {
      const params = new URLSearchParams({ page: 1 });
      if (q) params.set('q', q);
      if (year) params.set('year', year);
      const url = (q || year) ? `/api/movies/search?${params}` : `/api/movies/tpdb/latest?${params}`;
      const r = await fetch(url);
      const d = await r.json();
      const movies = (d.results || []).slice(0, 20);
      if (!movies.length) {
        let msg = 'No movies found';
        if (d.error) msg += ' (' + esc(d.error) + ')';
        el.innerHTML = `<div class="empty">${msg}</div>`;
        return;
      }
      const posterFallback = '/static/img/poster.jpg';
      el.innerHTML = movies.map(m => {
        const posterUrl = (m.poster && String(m.poster).trim()) ? m.poster : posterFallback;
        return `<div class="movie-search-result" onclick="showMovieDetail('${esc(m.id)}')">
          <img class="movie-search-poster" src="${esc(posterUrl)}" onerror="this.onerror=null;this.src='${posterFallback}'">
          <div style="flex:1;min-width:0">
            <div style="font-size:12px;color:var(--text);font-weight:500">${movieTitleDisplay(m.title)}</div>
            <div style="font-size:11px;color:var(--dim)">${m.studio ? esc(m.studio) : ''}${m.date ? ' · ' + m.date : ''}</div>
          </div>
        </div>`;
      }).join('');
    } catch(e) {
      el.innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`;
    }
  }

  // ── Magazine layout registry ──────────────────────────────────────
  // Each entry is a candidate page-2 (gallery) layout. `match(ctx)` is
  // a boolean predicate (eligibility) and `weight(ctx)` is a number
  // used only to break ties when multiple layouts match. `slots`
  // describes the cells the renderer needs to emit; `key` is also the
  // CSS class modifier on `.mag-gallery-grid` (each layout's
  // grid-template-areas is defined in discover.html via
  // `.mag-gallery-grid--{key}`). To add a new layout:
  //   1. Push an entry here describing its slots.
  //   2. Add a matching `.mag-gallery-grid--{key}` rule in CSS with
  //      the grid-template-areas it needs.
  // Don't add layouts speculatively — only when a real signal in the
  // ctx (image count, aspect ratio, scene count, etc.) makes one of
  // the current layouts a poor fit. Order in this array is the
  // tiebreak for equal weights — earlier wins.
  const MAGAZINE_LAYOUTS = [
    {
      key: 'pp',
      match: (ctx) => ctx.orient === 'pp',
      weight: () => 1,
      // Three-row template: two image+text rows on the diagonal with a
      // centred callout slot between them. The callout row is `1fr` in
      // CSS so it absorbs whatever vertical gap the image rows leave —
      // when both portraits render tall and fill the page, the row
      // collapses to 0 and the quote disappears (overflow:hidden).
      // When the images render short and a gap opens up in the middle
      // of the spread, the row expands and the callout fills it.
      slots: [
        { kind: 'image', cls: 'mag-cell--image-tl',      source: 'img1' },
        { kind: 'text',  cls: 'mag-cell--text-tr',       block: 'A'    },
        { kind: 'quote', cls: 'mag-cell--quote'                          },
        { kind: 'text',  cls: 'mag-cell--text-bl',       block: 'B'    },
        { kind: 'image', cls: 'mag-cell--image-br',      source: 'img2' },
      ],
    },
    {
      key: 'pl',
      match: (ctx) => ctx.orient === 'pl',
      weight: () => 1,
      slots: [
        { kind: 'image', cls: 'mag-cell--image-tl',      source: 'img1' },
        { kind: 'text',  cls: 'mag-cell--text-tr',       block: 'A'    },
        { kind: 'quote', cls: 'mag-cell--quote'                          },
        { kind: 'image', cls: 'mag-cell--image-band-br', source: 'img2' },
      ],
    },
    {
      key: 'lp',
      match: (ctx) => ctx.orient === 'lp',
      weight: () => 1,
      slots: [
        { kind: 'image', cls: 'mag-cell--image-band-tl', source: 'img1' },
        { kind: 'quote', cls: 'mag-cell--quote'                          },
        { kind: 'text',  cls: 'mag-cell--text-bl',       block: 'B'    },
        { kind: 'image', cls: 'mag-cell--image-br',      source: 'img2' },
      ],
    },
    {
      key: 'll',
      match: (ctx) => ctx.orient === 'll',
      weight: () => 1,
      slots: [
        { kind: 'image', cls: 'mag-cell--image-band-tl', source: 'img1' },
        { kind: 'quote', cls: 'mag-cell--quote'                          },
        { kind: 'image', cls: 'mag-cell--image-band-br', source: 'img2' },
      ],
    },
  ];

  function pickMagazineLayout(ctx) {
    const eligible = MAGAZINE_LAYOUTS.filter(l => l.match(ctx));
    if (!eligible.length) return MAGAZINE_LAYOUTS[0];   // fall back to PP
    eligible.sort((a, b) => b.weight(ctx) - a.weight(ctx));
    return eligible[0];
  }

  // ── /discover info panel ─────────────────────────────────────────
  // Populates #discoverInfoPanel below the spotlight when an item is
  // selected. Performer/studio → 5 most recent scenes (themed like
  // /scenes scene-cards) plus a secondary image for performer.
  // Movie → front and back cover themed like a /scenes movie-card.
  // Silently no-ops on /scenes (the panel doesn't exist there).
  async function loadDiscoverInfoPanel(item, kind) {
    const panel = document.getElementById('discoverInfoPanel');
    if (!panel) return;
    const empty = document.getElementById('discoverInfoEmpty');
    const body  = document.getElementById('discoverInfoBody');
    if (!empty || !body) return;
    empty.style.display = 'none';
    body.style.display = 'block';
    // Stamp the moment the loader becomes visible so we can hold it
    // on screen for a minimum window even when responses come back
    // from cache in milliseconds (otherwise the loader flickers).
    const _loadStart = performance.now();
    body.innerHTML = `
      <div class="discover-loading" aria-live="polite">
        <div class="dl-mouth" id="dlMouthHost" aria-hidden="true"></div>
        <div class="dl-label">Loading<span class="dl-dots"><i></i><i></i><i></i></span></div>
        <div class="dl-shimmer" aria-hidden="true"></div>
      </div>`;
    // Bind the Lottie animation directly via lottie-web. This is more
    // reliable than the lottie-player web component — innerHTML can
    // outpace the custom-element registration, leaving the animation
    // blank. If the lib hasn't loaded yet, retry briefly.
    (function bindMouth(attempt) {
      const host = document.getElementById('dlMouthHost');
      if (!host) return;
      if (window._dlMouthAnim) {
        try { window._dlMouthAnim.destroy(); } catch (e) {}
        window._dlMouthAnim = null;
      }
      if (window.lottie && typeof window.lottie.loadAnimation === 'function') {
        try {
          window._dlMouthAnim = window.lottie.loadAnimation({
            container: host,
            renderer: 'svg',
            loop: true,
            autoplay: true,
            path: '/static/Mouth.json',
          });
        } catch (e) { /* swallow — caption still renders */ }
      } else if (attempt < 30) {
        setTimeout(() => bindMouth(attempt + 1), 100);
      }
    })(0);

    if (kind === 'movie') {
      // Front cover (VHS-framed, with the /scenes movie-tile overlay
      // stack on the artwork) plus the back cover floated to the
      // right of the cassette in a perspective-tilted "art print"
      // presentation.
      const m = item || {};
      const front = (m.poster && String(m.poster).trim()) || '/static/img/poster.jpg';
      const back  = (m.background && String(m.background).trim()) || '';
      const vhsHue = Math.floor(Math.random() * 360);
      const titleForVhs = movieTitleDisplay(m.title || '');
      const studioLogoHtml = (m.studio || m.title)
        ? `<img class="discover-info-movie-vhs-studio" src="/api/studio-logo?name=${encodeURIComponent(m.studio || '')}&q=${encodeURIComponent(m.title || '')}" alt="" loading="lazy" onerror="this.remove()">`
        : '';
      const titleHtml = titleForVhs
        ? `<div class="discover-info-movie-vhs-title" aria-hidden="true">${esc(titleForVhs)}</div>`
        : '';
      const backHtml = back
        ? `<div class="discover-info-movie-back-wrap" onclick="openImageOverlay('${esc(back)}')">
             <div class="discover-info-movie-back-stack">
               <div class="discover-info-movie-back-bg" aria-hidden="true"></div>
               <div class="discover-info-movie-back-poster">
                 <div class="img-load">
                   <div class="img-spin" aria-hidden="true"></div>
                   <img class="movie-poster" src="${esc(back)}" loading="lazy" onload="this.closest('.img-load')?.classList.add('ready')" onerror="this.closest('.discover-info-movie-back-wrap')?.remove()">
                 </div>
               </div>
               <div class="discover-info-movie-back-overlay" aria-hidden="true"></div>
             </div>
           </div>`
        : '';
      // Tile-wide blurred backdrop — anchored top-right, fades down/left.
      const hazeHtml = back
        ? `<div class="discover-info-movie-haze" style="background-image:url('${esc(back)}')" aria-hidden="true"></div>`
        : '';

      // Headshots column — merge `performer_links` (TPDB image) with
      // `library_performers` (local headshot wins when present).
      const libPerfs = Array.isArray(m.library_performers) ? m.library_performers : [];
      const libByName = new Map(libPerfs.map(p => [(p.name || '').toLowerCase(), p]));
      const credits = Array.isArray(m.performer_links) && m.performer_links.length
        ? m.performer_links.map(p => {
            const lib = libByName.get((p.name || '').toLowerCase());
            return {
              name:   p.name || '',
              url:    p.url || '',
              image:  (lib && lib.headshot_url) || p.image || '',
              gender: p.gender || (lib && lib.gender) || '',
              row_id: (lib && lib.id) || null,
              stash_id: p.id || p.stash_id || '',
            };
          })
        : libPerfs.map(p => ({ name: p.name, image: p.headshot_url || '', url: '', gender: p.gender || '', row_id: p.id || null }));
      const headshotsHtml = credits.length
        ? `<div class="discover-info-movie-headshots">
             ${credits.map(c => {
               const img = c.image
                 ? `<img src="${esc(c.image)}" alt="" loading="lazy" onerror="this.replaceWith(Object.assign(document.createElement('div'),{className:'discover-info-movie-headshot-ph',innerHTML:'<i class=\\'fa-solid fa-user\\'></i>'}))">`
                 : `<div class="discover-info-movie-headshot-ph"><i class="fa-solid fa-user"></i></div>`;
               const nameHtml = `<div class="discover-info-movie-headshot-name">${esc(c.name)}</div>`;
               const attrs = window.performerLinkAttrs(c.name, { gender: c.gender, libraryRowId: c.row_id, stashId: c.stash_id });
               return `<div class="discover-info-movie-headshot" title="${esc(c.name)}"${attrs ? ' ' + attrs : ''}>${img}${nameHtml}</div>`;
             }).join('')}
           </div>`
        : '';
      body.innerHTML = `
        <div class="discover-info-movie">
          ${hazeHtml}
          <div class="discover-info-movie-vhs-wrap" style="--vhs-hue:${vhsHue}deg" onclick="openImageOverlay('${esc(front)}')">
            <div class="discover-info-movie-vhs-bg" aria-hidden="true"></div>
            ${titleHtml}
            ${studioLogoHtml}
            <div class="discover-info-movie-vhs-poster">
              <div class="img-load">
                <div class="img-spin" aria-hidden="true"></div>
                <img class="movie-poster" src="${esc(front)}" loading="lazy" onload="this.closest('.img-load')?.classList.add('ready')" onerror="this.onerror=null;this.src='/static/img/poster.jpg';this.closest('.img-load')?.classList.add('ready');">
              </div>
            </div>
          </div>
          ${backHtml}
          ${headshotsHtml}
        </div>`;
      return;
    }

    // Performer / studio → fetch /api/scenes/recent. Performers also
    // pull source images for the gallery carousel; studios skip that
    // call (the gallery isn't shown — scenes fill the whole panel).
    const itemId    = (item && (item.id || item._id)) || '';
    const itemSlug  = (item && item.slug) || '';
    const itemName  = (item && item.name) || '';
    const itemSrc   = (item && (item.source || 'TPDB')) || 'TPDB';
    const params = new URLSearchParams({
      source: itemSrc,
      id:     itemId,
      type:   kind,  // 'performer' or 'studio'
      slug:   itemSlug,
      name:   itemName,
    });
    // Cancel any prior in-flight info-panel requests so a slow earlier
    // click can't race the latest one and overwrite the visible result.
    if (window._infoPanelAbort) {
      try { window._infoPanelAbort.abort(); } catch (_) {}
    }
    const _abort = new AbortController();
    window._infoPanelAbort = _abort;

    const fetches = [fetch(`/api/scenes/recent?${params.toString()}`, { signal: _abort.signal }).then(r => r.json())];
    if (kind !== 'studio') {
      fetches.push(fetch(`/api/discover/performer-images?${params.toString()}`, { signal: _abort.signal }).then(r => r.json()));
    }
    const [scenesRes, imagesRes] = await Promise.allSettled(fetches);
    // If a newer click superseded us, bail before rendering.
    if (_abort.signal.aborted || window._infoPanelAbort !== _abort) return;
    // Hold the loading animation on screen for at least ~600ms so it
    // doesn't flicker when /api/scenes/recent and /api/discover/
    // performer-images come back hot from cache in <50ms.
    const _MIN_LOAD_MS = 600;
    const _elapsed = performance.now() - _loadStart;
    if (_elapsed < _MIN_LOAD_MS) {
      await new Promise(r => setTimeout(r, _MIN_LOAD_MS - _elapsed));
      if (_abort.signal.aborted || window._infoPanelAbort !== _abort) return;
    }

    let scenes = [];
    let scenesSource = '';   // the bucket the scenes came from — fed
                             //   to decorateLibraryMatches so it can
                             //   build the "source:id" key correctly.
    if (scenesRes.status === 'fulfilled') {
      const d = scenesRes.value;
      const sources = (d && d.sources) || {};
      const buckets = ['tpdb', 'stashdb', 'fansdb'];
      for (const b of buckets) {
        if (Array.isArray(sources[b]) && sources[b].length) {
          scenes = sources[b];
          scenesSource = b;
          break;
        }
      }
      if (!scenes.length && Array.isArray(d && d.scenes)) {
        scenes = d.scenes;
        scenesSource = (d && d.source) || '';
      }
    }
    // Stamp each real scene with its source so decorateLibraryMatches
    // (which reads `s.source`) builds the correct lookup key.
    if (scenesSource) {
      for (const s of scenes) {
        if (s && !s.__static && !s.source) s.source = scenesSource;
      }
    }
    // De-dupe before padding with static placeholders so identical
    // scenes returned under slightly different titles (e.g. "...
    // Audition" vs "... Auditie" from different stash-box mirrors)
    // collapse into one tile. Runs ahead of the slice/pad below.
    scenes = dedupeScenes(scenes);
    // Studios get more scenes since they fill the whole panel
    // (4–6 columns × 3 rows depending on viewport width). Performers
    // use a 3 × 3 grid (9 cards) — narrower cards mean the rows fit
    // the panel height vertically without clipping. If we don't have
    // enough real scenes to fill the target grid, pad with sentinel
    // "static" tiles so the layout never has gaps.
    const sceneTarget = kind === 'studio' ? 18 : 9;
    scenes = scenes.slice(0, sceneTarget);
    while (scenes.length < sceneTarget) {
      scenes.push({ __static: true });
    }

    let images = [];
    if (kind !== 'studio' && imagesRes && imagesRes.status === 'fulfilled') {
      images = Array.isArray(imagesRes.value && imagesRes.value.images) ? imagesRes.value.images : [];
    }
    // Fallback: if the source DB returned nothing, at least show the tile's
    // own image so the carousel doesn't render blank.
    if (kind !== 'studio' && !images.length && item && item.image) images = [String(item.image)];

    const sceneCard = (s, i) => {
      // Padded slot — render an untuned-channel TV-static tile so the
      // grid stays full and visually balanced even when the source DB
      // has fewer scenes than the layout asks for.
      if (s && s.__static) {
        return `
          <div class="scene-card scene-card--static" aria-hidden="true">
            <div class="img-load">
              <div class="scene-static-noise" aria-hidden="true"></div>
              <div class="scene-static-bands" aria-hidden="true"></div>
              <div class="scene-static-label">NO SIGNAL</div>
            </div>
            <div class="scene-meta" style="padding:6px 4px">
              <div class="scene-title" style="font-size:11px;color:rgba(255,255,255,0.35);line-height:1.3;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">— — —</div>
              <div style="font-size:10px;color:rgba(255,255,255,0.25)">CH-00 · STATIC</div>
            </div>
          </div>`;
      }
      const thumb = s.thumb || s.image || '/static/img/missing.jpg';
      const title = s.title || '';
      const date  = s.date  || '';
      const studio = s.studio || s.site_name || '';
      // Studios already know their own name — drop the studio suffix
      // from each card's meta line. Performers keep it.
      const metaLine = (kind === 'studio')
        ? esc(date)
        : `${esc(date)}${studio ? ' · ' + esc(studio) : ''}`;
      // Hover overlays:
      //   • Performer view → studio logo only (the user already knows
      //     which performer they're browsing; the question on each tile
      //     is "which studio shot this?")
      //   • Studio view    → performer headshots, with the studio logo
      //     as a fallback when /api/performers/headshots-by-name returns
      //     nothing for the scene's cast.
      const studioLogoHtml = (studio || title)
        ? `<img class="scene-studio-logo" src="/api/studio-logo?name=${encodeURIComponent(studio)}&q=${encodeURIComponent(title)}" alt="" loading="lazy" onload="this.closest('.scene-card')?.classList.add('has-studio-logo')" onerror="this.remove()">`
        : '';
      const performersAttr = kind === 'studio'
        ? esc((s.performer || (Array.isArray(s.performers) ? s.performers.join(', ') : '')) || '')
        : '';
      const hoverHandler = kind === 'studio' && performersAttr
        ? ` onmouseenter="ensureCardHeadshots(this, this.dataset.performer)"`
        : '';
      const performersDataAttr = kind === 'studio'
        ? ` data-performer="${performersAttr}"`
        : '';
      // `data-discover-info-i` lets `decorateLibraryMatches` (called
      // below after innerHTML is set) map the original `scenes[i]`
      // entry back to its rendered card so the in-library indicator
      // can be applied without re-querying the DOM by id.
      const idxAttr = (typeof i === 'number') ? ` data-discover-info-i="${i}"` : '';
      return `
        <div class="scene-card discover-info-scene-card discover-info-scene-card--${kind === 'studio' ? 'studio' : 'performer'}" tabindex="0" title="${esc(title)}"${idxAttr}${performersDataAttr}${hoverHandler}>
          <div class="img-load">
            <div class="img-spin" aria-hidden="true"></div>
            <img class="scene-thumb" src="${esc(thumb)}" loading="lazy" onload="this.closest('.img-load')?.classList.add('ready')" onerror="this.onerror=null;this.src='/static/img/missing.jpg';this.closest('.img-load')?.classList.add('ready');">
            <div class="duo-tint" aria-hidden="true"></div>
            ${studioLogoHtml}
          </div>
          <div class="scene-meta" style="padding:6px 4px">
            <div class="scene-title" style="font-size:11px;color:var(--text);line-height:1.3;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(title)}</div>
            <div style="font-size:10px;color:var(--dim)">${metaLine}</div>
          </div>
        </div>`;
    };

    if (kind === 'studio') {
      // Studio: skip the gallery — fill the whole panel with the
      // latest scenes.
      body.innerHTML = `
        <div class="discover-info-studio-layout">
          ${scenes.length
            ? `<div class="discover-info-scenes discover-info-scenes--studio">${scenes.map((s, i) => sceneCard(s, i)).join('')}</div>`
            : '<div class="empty" style="padding:24px;text-align:left">No recent scenes found.</div>'}
        </div>`;
      // Decorate any cards whose scene phash matches a library file.
      // Static placeholders (no `source`/`id`) are filtered server-side.
      try {
        decorateLibraryMatches(scenes, {
          containerSelector: '.discover-info-scene-card',
          idAttr: 'data-discover-info-i',
        });
      } catch (_) { /* swallow — indicator is best-effort */ }
      return;
    }

    // Performer: open magazine spread on the left, 2×3 scenes grid on the right.
    // Drop the first image (almost always the primary headshot we
    // already show in the upper detail panel) UNLESS it's the only
    // image we've got. Then shuffle the rest so each render shows
    // something different. Dedupe + shuffle helpers hoisted to module
    // scope so we don't re-define closures on every render.
    const allImages = _magDedupe(images);
    const usable = allImages.length > 1 ? allImages.slice(1) : allImages.slice(0);
    const shuffled = _magShuffle(usable);
    // Probe candidate covers — page 1 is a full-bleed portrait crop, so
    // landscape sources letterbox awkwardly there. Pick the first
    // portrait (ratio < 1.15, matching the gallery cutoff) as the cover
    // and put any landscape rejects into the gallery rotation where
    // banner layouts can use them. If nothing portrait turns up, fall
    // back to the static poster placeholder rather than a sideways crop.
    const _coverProbe = (src) => new Promise((resolve) => {
      if (!src) return resolve({ src, portrait: false });
      const probe = new Image();
      let settled = false;
      const finish = (r) => { if (!settled) { settled = true; resolve(r); } };
      probe.onload = () => {
        const w = probe.naturalWidth || 1;
        const h = probe.naturalHeight || 1;
        finish({ src, portrait: (w / h) < 1.15 });
      };
      probe.onerror = () => finish({ src, portrait: false });
      setTimeout(() => finish({ src, portrait: false }), 5000);
      probe.src = src;
    });
    const _coverProbes = await Promise.all(shuffled.map(_coverProbe));
    let _coverIdx = _coverProbes.findIndex(p => p.portrait);
    let cover;
    let coverFallback = false;
    if (_coverIdx >= 0) {
      cover = shuffled[_coverIdx];
    } else if (item && item.image) {
      // Last resort before the static poster — try the tile's own
      // image. We don't probe this one (network dependency loop), but
      // tile images are typically headshots which are portrait.
      cover = String(item.image);
    } else {
      cover = '/static/img/poster.jpg';
      coverFallback = true;
      _coverIdx = -1;
    }
    // Build the gallery from everything that wasn't picked as cover —
    // portrait or landscape, both work in the gallery layouts.
    const galleryImgs = (_coverIdx >= 0
      ? shuffled.filter((_, i) => i !== _coverIdx)
      : shuffled.slice()
    ).slice(0, 12);
    // Stash the full ordered list so the carousel can browse them all.
    // Skip the placeholder cover so users don't click into a generic
    // graphic in the carousel.
    window._magAllImages = [coverFallback ? '' : cover, ...galleryImgs].filter(Boolean);
    const subtitle  = (item && item.source) ? String(item.source) : '';

    // Right-page layout — fixed editorial spread:
    //   ┌────────────────────────────────────────┐
    //   │  ADJECTIVE   (underlined headline)     │
    //   │  Neque porro quisquam… (subtitle)      │
    //   ├──────────────────┬─────────────────────┤
    //   │  body copy A     │   image 1           │
    //   ├──────────────────┼─────────────────────┤
    //   │  image 2         │   body copy B       │
    //   └──────────────────┴─────────────────────┘
    // Strip trailing punctuation (".", "?", "!") so a long-form
    // pull-quote like "Doctor said no. She said yes." doesn't render
    // with a hanging fullstop in the magazine masthead.
    const adjective = _magPickRandom(_MAG_ADJECTIVES).replace(/[.?!]+\s*$/, '');
    // Pick a fresh long-form pull quote (different from the title)
    // and insert it as a centred display paragraph between the
    // body-copy paragraphs so the column fills the cell.
    const _pullQuote = (() => {
      // Long-form entries have spaces; pick one that isn't the title.
      for (let i = 0; i < 16; i++) {
        const q = _magPickRandom(_MAG_ADJECTIVES);
        if (q.length > 22 && q !== adjective) return q;
      }
      return "Worth the headline.";
    })();
    const _pullHtml = `<p class="mag-pull-quote">${esc(_pullQuote)}</p>`;

    const blockA = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. In vel blandit ex, at feugiat velit. Vivamus lectus augue, interdum a vulputate vel, gravida nec urna. Phasellus vehicula at nisi vel ullamcorper. Cras eros massa, dapibus ac dui in, volutpat gravida urna. Maecenas id eros posuere dui tincidunt egestas. Aenean euismod diam ut condimentum rhoncus. Aenean fermentum ipsum at mi dictum elementum.\n\nDuis sit amet rutrum orci, ac eleifend augue. Aenean bibendum lorem at elit pulvinar posuere. Praesent rhoncus ex quis mi eleifend vehicula. Curabitur finibus sed ante sed varius. Sed commodo sapien non tortor scelerisque gravida. Nullam sem erat, ultrices sed nunc aliquam, porttitor sollicitudin sapien. Aliquam mattis ante sem, sit amet imperdiet nibh imperdiet vel. Pellentesque non luctus dolor. Quisque in quam et est consequat consectetur. Cras blandit magna id lacus fermentum, bibendum luctus libero pellentesque. Nam at gravida lectus, eu facilisis sapien. Nunc lobortis nulla at consequat varius.\n\nFusce volutpat nisi vitae lectus pretium, vel ultricies arcu commodo. Quisque eget elit a justo gravida iaculis. Mauris facilisis nibh sit amet libero sodales, sit amet dignissim arcu congue. Integer porttitor felis vel arcu cursus, in faucibus risus aliquet. Nulla a dapibus sapien. Cras at libero feugiat, dignissim sapien sed, ornare turpis.\n\nVivamus quis sapien at ipsum porta sodales. Suspendisse potenti. Aliquam erat volutpat. Maecenas a ante a augue sodales viverra ac sit amet velit. Curabitur nec sapien at est tristique aliquet. Sed elementum ipsum non nibh dignissim, vitae interdum massa imperdiet.";
    const blockB = "Donec ut varius sapien. Vestibulum rutrum tempus tortor id congue. Nam sagittis est odio, non hendrerit nulla cursus vel. Vivamus turpis purus, tristique ultrices orci ac, varius consectetur elit. Proin vel quam ornare, vehicula erat a, dictum purus. Duis vitae nulla sed sapien sollicitudin tempus. Vivamus elementum orci ut nunc ullamcorper interdum. Pellentesque quis nibh fermentum, pellentesque mauris ut, finibus mi.\n\nNam aliquam ultricies rutrum. Duis pretium tellus vitae interdum fringilla. Pellentesque luctus non dui ut vestibulum. Nulla nunc nisi, pretium vel blandit sit amet, tincidunt id leo. Cras tincidunt, velit nec viverra cursus, libero lorem mollis dolor, id tempus velit orci finibus felis. Cras eleifend vel mauris auctor ultrices. Donec tincidunt porta suscipit. In ligula sem, cursus eget consequat sit amet, aliquam at tellus. Nam lacus nisl, dapibus ut felis vitae, vehicula dapibus neque. Pellentesque vitae malesuada nibh.\n\nProin lacus diam, gravida vel enim ut, pharetra suscipit quam. Curabitur gravida metus in scelerisque congue. Sed mollis ante quis est luctus faucibus. Nulla aliquam, justo ut dictum tempor, purus purus laoreet nunc, vel congue metus dui non felis. Curabitur vulputate dictum auctor. Phasellus at nibh eget dui dictum commodo. Suspendisse a felis in orci molestie mollis. Sed interdum ultricies ligula eget aliquam. Aenean imperdiet metus non maximus fringilla. Duis faucibus felis quis suscipit sodales. Aenean vitae ante vitae sapien commodo commodo. Etiam interdum ipsum ac pellentesque accumsan.\n\nUt placerat odio at risus elementum, eu maximus est congue. Vivamus sed augue at libero ultrices porttitor. Nullam interdum sapien sit amet vehicula tristique. Sed in lectus eu mauris fermentum bibendum. Donec mollis odio sit amet sodales lacinia. Aliquam erat volutpat. Maecenas et metus quis odio luctus tristique vitae nec lectus.";

    // Render paragraphs as in-flow flex children. The cell uses
    // `min-height: 0; overflow: hidden`, so anything taller than the
    // paired image clips cleanly without absolute positioning. Each
    // cell gets a tiny per-render variation in opacity + blur so the
    // two columns don't read as identical templated copies — kills
    // the templated feel without breaking legibility.
    const _magCellStyle = () => {
      const opacity = (0.78 + Math.random() * 0.14).toFixed(2);   // 0.78–0.92
      const blur    = (0.35 + Math.random() * 0.30).toFixed(2);   // 0.35–0.65
      return `--mag-text-opacity:${opacity};--mag-text-blur:${blur}px`;
    };
    const paragraphs = (s) =>
      `<div class="mag-cell-text-inner" style="${_magCellStyle()}">${
        esc(s).split('\n\n').map(p =>
          p.trim() === '##PULL##' ? _pullHtml : `<p>${p}</p>`
        ).join('')
      }</div>`;

    // Pick the two gallery slot images so they're unique whenever
    // possible — only fall back to a duplicate if we've genuinely
    // run out of distinct source images.
    //   • cover  — shuffled[0]   → window._magAllImages index 0
    //   • img1   — first gallery image (shuffled[1])
    //   • img2   — prefer the next gallery image; if there isn't one,
    //              borrow the cover (still distinct from img1) before
    //              repeating img1 itself.
    const img1Src = galleryImgs[0] || cover || '';
    const img1Idx = galleryImgs[0] ? 1 : 0;
    let img2Src = '';
    let img2Idx = 0;
    if (galleryImgs[1] && galleryImgs[1] !== img1Src) {
      img2Src = galleryImgs[1]; img2Idx = 2;
    } else if (cover && cover !== img1Src) {
      img2Src = cover; img2Idx = 0;
    } else if (galleryImgs[0]) {
      // Forced repeat — only one unique source image available.
      img2Src = galleryImgs[0]; img2Idx = 1;
    } else if (cover) {
      img2Src = cover; img2Idx = 0;
    }

    // Probe each slot image's natural orientation — the picker uses
    // this (plus per-image aspect ratio) to choose a layout from the
    // MAGAZINE_LAYOUTS registry. 1.2 : 1 is the cutoff; near-square
    // crops stay in the portrait paired layout where they read fine.
    // Probe + render synchronously inside this async function — the
    // loader is already on screen so the extra few hundred ms of
    // decode time is invisible to the user.
    const _magProbe = (src) => new Promise((resolve) => {
      if (!src) return resolve({ orient: 'portrait', ratio: 1 });
      const probe = new Image();
      let settled = false;
      const finish = (r) => { if (!settled) { settled = true; resolve(r); } };
      probe.onload = () => {
        const w = probe.naturalWidth || 1;
        const h = probe.naturalHeight || 1;
        const ratio = w / h;
        // 1.15 cutoff (down from 1.2) so near-square sources tip to
        // landscape — a 1.18:1 image looks worse jammed into a 3-col
        // portrait slot than treated as a banner.
        finish({ orient: ratio >= 1.15 ? 'landscape' : 'portrait', ratio });
      };
      probe.onerror = () => finish({ orient: 'portrait', ratio: 1 });
      // Generous 5s timeout — the loader is already on screen so the
      // wait is invisible. StashDB / TPDB images can be 1–3 MB and
      // don't always decode inside 1.5s; falling back to portrait
      // there was the cause of landscape sources rendering at 1/3
      // page width with empty bands either side.
      setTimeout(() => finish({ orient: 'portrait', ratio: 1 }), 5000);
      probe.src = src;
    });
    const [_p1, _p2] = await Promise.all([_magProbe(img1Src), _magProbe(img2Src)]);
    // Context handed to every layout's match()/weight() — extend with
    // new signals (image count, scene count, title length, etc.) as
    // new layouts need them.
    const _layoutCtx = {
      orient:    _p1.orient[0] + _p2.orient[0],   // 'pp' | 'pl' | 'lp' | 'll'
      o1:        _p1.orient,
      o2:        _p2.orient,
      ratio1:    _p1.ratio,
      ratio2:    _p2.ratio,
      imgCount:  (img1Src ? 1 : 0) + (img2Src ? 1 : 0),
    };
    const _layout = pickMagazineLayout(_layoutCtx);

    // Pick a callout phrase for any layout that uses a `quote` slot.
    // Different from the masthead adjective and from the original
    // _pullQuote so the spread doesn't repeat itself. Prefers
    // long-form entries (>22 chars) — single-word adjectives don't
    // carry visual weight at the larger callout size.
    const _calloutText = (() => {
      const used = new Set([adjective, _pullQuote]);
      for (let i = 0; i < 24; i++) {
        const q = _magPickRandom(_MAG_ADJECTIVES);
        if (q.length > 22 && !used.has(q)) return q;
      }
      // Fall back to any non-duplicate, even short.
      for (let i = 0; i < 24; i++) {
        const q = _magPickRandom(_MAG_ADJECTIVES);
        if (!used.has(q)) return q;
      }
      return adjective;
    })();

    // Slot renderers. The registry says *what* goes where; these say
    // *how* a slot becomes HTML. New slot kinds (e.g. a pull-quote
    // banner) just add another branch here.
    const _sources = {
      img1: { src: img1Src, idx: img1Idx },
      img2: { src: img2Src, idx: img2Idx },
    };
    const _blocks = { A: blockA, B: blockB };
    const _renderSlot = (slot) => {
      if (slot.kind === 'image') {
        const { src, idx } = _sources[slot.source] || {};
        const handler = src ? ` onclick="openMagCarousel(window._magAllImages, ${idx})"` : '';
        const inner = src ? `<img src="${esc(src)}" loading="lazy" onerror="this.closest('.mag-cell')?.remove()">` : '';
        return `<div class="mag-cell mag-cell--image ${slot.cls}"${handler}>${inner}</div>`;
      }
      if (slot.kind === 'text') {
        return `<div class="mag-cell mag-cell--text ${slot.cls}" aria-hidden="true">${paragraphs(_blocks[slot.block])}</div>`;
      }
      if (slot.kind === 'quote') {
        return `<div class="mag-cell ${slot.cls}" aria-hidden="true"><div class="mag-quote-text">${esc(_calloutText)}</div></div>`;
      }
      return '';
    };
    const _gridCells = _layout.slots.map(_renderSlot).join('');

    const tilesHtml = `
      <div class="mag-gallery-header">
        <div class="mag-gallery-adjective">${esc(adjective)}</div>
      </div>
      <div class="mag-gallery-grid mag-gallery-grid--${_layout.key}">${_gridCells}</div>`;

    const galleryClass = `mag-gallery mag-gallery--editorial`;
    const magazineHtml = `
      <div class="magazine" data-key="${esc(itemId || itemName)}">
        <div class="magazine-page magazine-page--feature">
          <div class="magazine-paper" aria-hidden="true"></div>
          ${cover
            ? `<img class="magazine-feature-img" src="${esc(cover)}" loading="lazy"${coverFallback ? '' : ' onclick="openMagCarousel(window._magAllImages, 0)"'} onerror="this.style.display='none'">`
            : '<div class="magazine-feature-img magazine-feature-img--empty"></div>'}
          <img class="magazine-feature-logo" src="/static/img/logo.png" alt="" aria-hidden="true" onerror="this.remove()">
          <div class="magazine-feature-gloss" aria-hidden="true"></div>
          <div class="magazine-feature-overlay">
            ${itemName ? `<div class="magazine-eyebrow">Issue · ${esc(subtitle || 'Spotlight')}</div>` : ''}
            ${itemName ? `<div class="magazine-title">${esc(itemName)}</div>` : ''}
          </div>
        </div>
        <div class="magazine-spine" aria-hidden="true"></div>
        <div class="magazine-page magazine-page--gallery">
          <div class="magazine-paper" aria-hidden="true"></div>
          <div class="${galleryClass}">${tilesHtml}</div>
        </div>
      </div>`;

    body.innerHTML = `
      <div class="discover-info-perf-layout">
        <div class="discover-info-carousel-col">
          ${magazineHtml}
        </div>
        <div class="discover-info-scenes-col">
          ${scenes.length
            ? `<div class="discover-info-scenes">${scenes.map((s, i) => sceneCard(s, i)).join('')}</div>`
            : '<div class="empty" style="padding:24px;text-align:left">No recent scenes found.</div>'}
        </div>
      </div>`;
    try {
      decorateLibraryMatches(scenes, {
        containerSelector: '.discover-info-scene-card',
        idAttr: 'data-discover-info-i',
      });
    } catch (_) { /* swallow — indicator is best-effort */ }

    // Trigger the page-turn animation on each render. Removing the
    // class then re-adding it on the next frame restarts the keyframe.
    const mag = body.querySelector('.magazine');
    if (mag) {
      mag.classList.remove('magazine--turning');
      // eslint-disable-next-line no-unused-expressions
      mag.offsetWidth;
      requestAnimationFrame(() => mag.classList.add('magazine--turning'));
    }

    // Auto-fit the page-1 masthead title — shrinks long names that
    // overflow the overlay column (e.g. "BABYRAINBOW" runs past the
    // page edge at the natural clamp size). Names that already fit
    // are left at their clamp-driven size; only the overflow case
    // gets a font-size override. Re-run on resize so the fit stays
    // correct when the panel width changes.
    const _fitMagazineTitle = () => {
      const title = body.querySelector('.magazine-title');
      if (!title) return;
      const parent = title.parentElement;
      if (!parent) return;
      title.style.fontSize = '';
      const avail = parent.clientWidth;
      const needed = title.scrollWidth;
      if (!avail || !needed || needed <= avail) return;
      const cur = parseFloat(getComputedStyle(title).fontSize) || 32;
      const ratio = avail / needed;
      title.style.fontSize = Math.max(12, cur * ratio * 0.98).toFixed(2) + 'px';
    };
    requestAnimationFrame(_fitMagazineTitle);
    if (window._magTitleResizeObserver) {
      window._magTitleResizeObserver.disconnect();
    }
    if (typeof ResizeObserver !== 'undefined') {
      const ro = new ResizeObserver(() => _fitMagazineTitle());
      const overlay = body.querySelector('.magazine-feature-overlay');
      if (overlay) ro.observe(overlay);
      window._magTitleResizeObserver = ro;
    }
  }
  // Expose globally so onclick handlers / other inline scripts can call.
  window.loadDiscoverInfoPanel = loadDiscoverInfoPanel;

  // Populates the /discover upper detail panel with movie metadata
  // (title, studio, date, performers, synopsis, links). Mirrors the
  // performer/studio detail layout: poster on the left, info column
  // on the right. Front cover is also rendered VHS-framed in the
  // lower info panel by `loadDiscoverInfoPanel`.
  function renderMovieInDetailPanel(m) {
    if (!document.getElementById('detailContent')) return;
    // Hide spotlight grid, show detail content + back button.
    const gridEl = document.getElementById('spotlightGrid');
    if (gridEl) gridEl.style.display = 'none';
    document.getElementById('detailEmpty').style.display = 'none';
    document.getElementById('detailContent').style.display = 'flex';
    const backBtn = document.getElementById('spotlightBackBtn');
    if (backBtn) backBtn.style.display = 'flex';
    // Movies don't get the library quick-add UI.
    const quickAdd = document.getElementById('quickAddBar');
    if (quickAdd) quickAdd.style.display = 'none';
    const libStatus = document.getElementById('detailLibStatus');
    if (libStatus) libStatus.innerHTML = '';

    // Title.
    document.getElementById('detailName').textContent = movieTitleDisplay(m.title || '');

    // Layout: poster left, text right.
    const layoutEl = document.getElementById('detailLayout');
    const posterEl = document.getElementById('detailPoster');
    layoutEl.style.flexDirection = 'row';
    layoutEl.style.alignItems = 'stretch';
    layoutEl.style.gap = '20px';
    posterEl.style.flexShrink = '0';
    posterEl.style.display = 'flex';
    posterEl.style.height = '100%';

    const posterFallback = '/static/img/poster.jpg';
    const posterUrl = (m.poster && String(m.poster).trim()) ? m.poster : posterFallback;
    // Plain poster — NO VHS overlay stack here. The overlays belong on
    // the VHS-framed front cover in the lower info panel; the top
    // detail-panel image is left clean so the artwork reads as-is.
    posterEl.innerHTML = `<img class="detail-poster" style="aspect-ratio:27/40;height:100%;width:auto;object-fit:cover;cursor:pointer" src="${esc(posterUrl)}" onclick="openImageOverlay('${esc(posterUrl)}')" onerror="this.onerror=null;this.src='${posterFallback}'">`;
    setDetailBg(posterUrl);

    // Meta line — studio · date · duration · director(s).
    const metaBits = [];
    if (m.studio)   metaBits.push(`Studio: <span>${esc(m.studio)}</span>`);
    if (m.date)     metaBits.push(`Released: <span>${esc(m.date)}</span>`);
    if (m.duration) metaBits.push(`Duration: <span>${Math.round(m.duration / 60)} min</span>`);
    if (Array.isArray(m.directors) && m.directors.length) {
      const dirs = m.directors
        .map(d => esc(typeof d === 'object' && d !== null ? (d.name || d.full_name || '') : String(d)))
        .filter(Boolean);
      if (dirs.length) metaBits.push(`Director: <span>${dirs.join(', ')}</span>`);
    }
    document.getElementById('detailMeta').innerHTML = metaBits.length
      ? `<div style="line-height:2;font-size:12px;color:var(--dim)">${metaBits.join(' &middot; ')}</div>`
      : '';

    // Performer tag chips → detailLinks slot.
    const perfHtml = (m.performer_links || []).map(p => {
      const nameRaw = p.name || '';
      if (!nameRaw) return '';
      const name = esc(nameRaw);
      const badge = genderBadge(p.gender);
      const attrs = window.performerLinkAttrs(nameRaw, { gender: p.gender, stashId: p.id || p.stash_id });
      // Click goes to popup (preferred) instead of an external profile —
      // popup surfaces the external link via its profile-pill row.
      return `<span class="movie-perf-tag${attrs ? ' perf-name-link' : ''}"${attrs ? ' ' + attrs : ''}>${name}${badge}</span>`;
    }).join('') || (m.performers || []).map(p => {
      const nm = esc(p);
      return `<span class="movie-perf-tag perf-name-link" data-performer-link data-name="${esc(p)}">${nm}</span>`;
    }).join('');

    // External links — TMDB + TPDB icon-buttons.
    const tmdbHref = esc(m.tmdb_url || ('https://www.themoviedb.org/search/movie?query=' + encodeURIComponent(m.title || '')));
    const tpdbHref = esc(m.url || '');
    const linkBtns = `
      <a class="detail-link db-link" href="${tmdbHref}" target="_blank" onclick="event.stopPropagation()" title="TMDB">
        <img src="/static/logos/tmdb.png" alt="TMDB" style="height:14px;width:auto;object-fit:contain;vertical-align:middle"> TMDB
      </a>
      ${tpdbHref ? `<a class="detail-link db-link" href="${tpdbHref}" target="_blank" onclick="event.stopPropagation()" title="TPDB">
        <img src="/static/logos/tpdb.png" alt="TPDB" style="height:14px;width:auto;object-fit:contain;vertical-align:middle"> TPDB
      </a>` : ''}
      <button class="detail-link db-link" style="border:1px solid rgba(var(--brand-purple-rgb),0.35);background:rgba(var(--brand-purple-rgb),0.35);cursor:pointer" onclick="event.stopPropagation();window.openProwlarrSearchPopup({title:'${esc((m.title || '').replace(/'/g, "\\'"))}',kind:'movie'})" title="Search Prowlarr">
        <span class="ts-prowlarr-btn-content"><img class="ts-prowlarr-btn-logo" src="/static/logos/prowlarr.png" alt="Prowlarr"><i class="fa-solid fa-magnifying-glass"></i></span>
      </button>`;
    const linksWrap = perfHtml
      ? `<div class="movie-detail-performers" style="flex-basis:100%;margin:0 0 8px;display:flex;flex-wrap:wrap;gap:6px">${perfHtml}</div>`
      : '';
    document.getElementById('detailLinks').innerHTML = `${linksWrap}${linkBtns}`;

    // Synopsis as bio.
    document.getElementById('detailBio').textContent = m.synopsis || '';

    // Hide any leftover result message.
    const resultMsg = document.getElementById('resultMsg');
    if (resultMsg) resultMsg.style.display = 'none';
  }
  window.renderMovieInDetailPanel = renderMovieInDetailPanel;

  async function showMovieDetail(movieId) {
    // /discover renders the movie inline in the info panel — no popup.
    // /scenes (and any page without the info panel) keeps the legacy
    // popup behaviour.
    const inlinePanel = document.getElementById('discoverInfoPanel');
    if (!inlinePanel) {
      document.getElementById('movieDetailContent').innerHTML = movieDetailSkeleton();
      document.getElementById('movieDetailOverlay').classList.add('open');
    }
    try {
      const r = await fetch(`/api/movies/tpdb/${movieId}`);
      const m = await r.json();
      if (m.error) {
        if (inlinePanel) {
          const body = document.getElementById('discoverInfoBody');
          if (body) body.innerHTML = `<div class="empty">${esc(m.error)}</div>`;
        } else {
          document.getElementById('movieDetailContent').innerHTML = `<div class="empty">${esc(m.error)}</div>`;
        }
        return;
      }
      // Stash full movie metadata so the prowlarr-grab path can tag
      // each movie download with its source DB id + poster URL the
      // same way scene grabs do — without this, /downloads tiles for
      // movie grabs render with no poster.
      window._currentMovie = m;
      if (inlinePanel) {
        // /discover — populate the upper detail panel first (so the
        // spotlight grid is hidden immediately), then the lower info
        // panel. Each in its own try so a render error in one doesn't
        // block the other.
        try { renderMovieInDetailPanel(m); }
        catch (e) { console.error('renderMovieInDetailPanel failed', e); }
        try { loadDiscoverInfoPanel(m, 'movie'); }
        catch (e) { console.error('loadDiscoverInfoPanel failed', e); }
        return;
      }
      // /scenes legacy popup path.
      loadDiscoverInfoPanel(m, 'movie');
      const bg = m.background ? `<img class="movie-detail-bg" src="${esc(m.background)}" onerror="this.remove()">` : '';
      const posterFallback = '/static/img/poster.jpg';
      const detailPosterUrl = (m.poster && String(m.poster).trim()) ? m.poster : posterFallback;
      const overlaySrc = esc((m.poster && String(m.poster).trim()) ? m.poster : posterFallback);
      const poster = `<div class="img-load"><div class="img-spin" aria-hidden="true"></div><img src="${esc(detailPosterUrl)}" style="cursor:pointer;width:100%;height:100%;object-fit:cover;display:block" onclick="openImageOverlay('${overlaySrc}')" onload="this.closest('.img-load')?.classList.add('ready')" onerror="this.onerror=null;this.src='${posterFallback}';this.closest('.img-load')?.classList.add('ready');"></div>`;
      // Random hue rotation for the vhs.png frame so each open varies.
      const vhsHue = Math.floor(Math.random() * 360);
      // Studio logo (rotated 90° CCW behind the poster) — only if we
      // have a studio name to look up. Same /api/studio-logo lookup as
      // the regular movie cards.
      const studioLogoHtml = (m.studio || m.title)
        ? `<img class="movie-detail-studio-logo-rotated" src="/api/studio-logo?name=${encodeURIComponent(m.studio || '')}&q=${encodeURIComponent(m.title || '')}" alt="" loading="lazy" onerror="this.remove()">`
        : '';
      // Rotated movie title fills the rest of the cassette label area
      // to the LEFT of the studio logo.
      const titleForVhs = movieTitleDisplay(m.title || '');
      const titleHtml = titleForVhs
        ? `<div class="movie-detail-vhs-title-rotated" aria-hidden="true">${titleForVhs}</div>`
        : '';
      const posterFrame = `
        <div class="movie-detail-vhs-bg" style="--vhs-hue:${vhsHue}deg" aria-hidden="true"></div>
        ${titleHtml}
        ${studioLogoHtml}
        <div class="movie-detail-poster-card">${poster}</div>`;
      const meta = [];
      if (m.studio) meta.push(`Studio: <span>${esc(m.studio)}</span>`);
      if (m.date) meta.push(`Released: <span>${esc(m.date)}</span>`);
      if (m.duration) meta.push(`Duration: <span>${Math.round(m.duration/60)} min</span>`);
      if (m.directors?.length) {
        const dirNames = m.directors.map(d => esc(typeof d === 'object' && d !== null ? (d.name || d.full_name || '') : String(d))).filter(Boolean);
        if (dirNames.length) meta.push(`Director: <span>${dirNames.join(', ')}</span>`);
      }
      const libPerfs = m.library_performers || [];
      const libPerfsHtml = libPerfs.length
        ? `<div class="lib-perfs-row">${libPerfs.map(p => {
            const img = p.headshot_url
              ? `<img src="${esc(p.headshot_url)}" alt="" onerror="this.replaceWith(Object.assign(document.createElement('div'),{className:'lib-perf-ph',innerHTML:'<i class=\\'fa-solid fa-user\\'></i>'}))">`
              : `<div class="lib-perf-ph"><i class="fa-solid fa-user"></i></div>`;
            const attrs = window.performerLinkAttrs(p.name, { gender: p.gender, libraryRowId: p.row_id || p.id });
            return `<div class="lib-perf-hs" title="${esc(p.name)}"${attrs ? ' ' + attrs : ''}>${img}<div class="lib-perf-hs-name">${esc(p.name)}</div></div>`;
          }).join('')}</div>`
        : '';
      const perfLinks = (m.performer_links || []).map(p => {
        const nameRaw = p.name || '';
        if (!nameRaw) return '';
        const name = esc(nameRaw);
        const badge = genderBadge(p.gender);
        // Click → universal popup (gender-gated). External profile is
        // surfaced via the popup's own link pills.
        const attrs = window.performerLinkAttrs(nameRaw, { gender: p.gender, stashId: p.id || p.stash_id });
        return `<span class="movie-perf-tag${attrs ? ' perf-name-link' : ''}"${attrs ? ' ' + attrs : ''}>${name}${badge}</span>`;
      }).join('');
      const perfs = perfLinks || (m.performers||[]).map(p => {
        const nm = esc(p);
        return `<span class="movie-perf-tag perf-name-link" data-performer-link data-name="${esc(p)}">${nm}</span>`;
      }).join('');
      const movieTags = Array.isArray(m.tags) ? m.tags : [];
      const movieTagsHtml = movieTags.length
        ? `<div class="movie-detail-tags" style="display:flex;flex-wrap:wrap;gap:6px;margin-top:10px">${movieTags.map(t => `<span class="scene-card-tag-chip">${esc(t)}</span>`).join('')}</div>`
        : '';
      let scenes = '';
      if (m.scenes?.length) {
        scenes = `<div class="movie-detail-scenes"><div class="movie-detail-scenes-title">Scenes (${m.scenes.length})</div><div class="movie-detail-scene-grid">${m.scenes.map(s => `<div class="movie-detail-scene-card">${s.thumb ? `<div class="img-load"><div class="img-spin" aria-hidden="true"></div><img class="movie-detail-scene-thumb" src="${esc(s.thumb)}" loading="lazy" onload="this.closest('.img-load')?.classList.add('ready')" onerror="const w=this.closest('.img-load'); if(w){ this.outerHTML='<div class=\\'movie-detail-scene-thumb-ph\\'></div>'; w.classList.add('ready'); }"></div>` : '<div class="movie-detail-scene-thumb-ph"></div>'}<div class="movie-detail-scene-info">${esc(s.title)}${s.date ? ' · '+s.date : ''}</div></div>`).join('')}</div></div>`;
      }
      document.getElementById('movieDetailContent').innerHTML = `${bg}
        <div class="movie-detail-inner">
          <div class="movie-detail-poster-wrap">${posterFrame}</div>
          <div class="movie-detail-text">
            <div class="movie-detail-title" title="${esc(m.title)}">${movieTitleDisplay(m.title)}</div>
            ${libPerfsHtml}
            <div class="movie-detail-meta-line">${meta.join(' &middot; ')}</div>
            ${perfs ? `<div class="movie-detail-performers">${perfs}</div>` : ''}
            ${movieTagsHtml}
            ${m.synopsis ? `<div class="movie-detail-synopsis">${esc(m.synopsis)}</div>` : ''}
            <div class="movie-detail-actions">
              <button class="movie-btn-action movie-btn-prowlarr" onclick="event.stopPropagation();window.openProwlarrSearchPopup({title:'${esc((m.title || '').replace(/'/g, "\\'"))}',kind:'movie'})" title="Search Prowlarr"><span class="ts-prowlarr-btn-content"><img class="ts-prowlarr-btn-logo" src="/static/logos/prowlarr.png" alt="Prowlarr"><i class="fa-solid fa-magnifying-glass"></i></span></button>
              <a class="movie-btn-action movie-btn-link" href="${esc(m.tmdb_url || ('https://www.themoviedb.org/search/movie?query=' + encodeURIComponent(m.title || '')))}" target="_blank" onclick="event.stopPropagation()"><img src="/static/logos/tmdb.png" alt="TMDB" style="height:20px;width:auto;object-fit:contain;vertical-align:middle;opacity:0.9"></a>
              <a class="movie-btn-action movie-btn-link" href="${esc(m.url)}" target="_blank" onclick="event.stopPropagation()"><img src="/static/logos/tpdb.png" alt="TPDB" style="height:20px;width:auto;object-fit:contain;vertical-align:middle;opacity:0.9"></a>
            </div>
          </div>
        </div>
        ${scenes}`;
    } catch(e) { document.getElementById('movieDetailContent').innerHTML = '<div class="empty">Error loading movie</div>'; }
  }

  function closeMovieDetail() { document.getElementById('movieDetailOverlay').classList.remove('open'); }

  async function searchMovieProwlarr(title) {
    document.getElementById('movieProwlarrTitle').textContent = `Prowlarr: ${title}`;
    document.getElementById('movieProwlarrResults').innerHTML = '<div class="empty">Searching indexers...</div>';
    document.getElementById('movieProwlarrOverlay').classList.add('open');
    try {
      const r = await fetch(`/api/prowlarr/search?q=${encodeURIComponent(title)}`);
      const d = await r.json();
      const results = d.results || [];
      if (!results.length) { document.getElementById('movieProwlarrResults').innerHTML = '<div class="empty">No results found</div>'; return; }
      window._movieProwlarrResults = results;
      document.getElementById('movieProwlarrResults').innerHTML = results.map((r,i) => `
        <div class="movie-prowlarr-result">
          <span class="movie-prowlarr-indexer">${esc(r.indexer||'')}</span>
          <span class="movie-prowlarr-name" title="${esc(r.title)}">${esc(r.title)}</span>
          <span class="movie-prowlarr-size">${r.size_mb ? Math.round(r.size_mb)+' MB' : ''}${r.seeders != null ? ' · S:'+r.seeders : ''}</span>
          <button type="button" class="btn-prowlarr-grab ${r.type === 'nzb' ? 'nzb' : ''}" title="Send to download client" onclick="grabMovieRelease(event, ${i})"><i class="fa-solid fa-download" aria-hidden="true"></i></button>
        </div>`).join('');
    } catch(e) { document.getElementById('movieProwlarrResults').innerHTML = `<div class="empty">Search failed: ${esc(e.message)}</div>`; }
  }

  async function grabMovieRelease(ev, idx) {
    const r = window._movieProwlarrResults[idx];
    if (!r) return;
    const btn = ev && ev.target && ev.target.closest ? ev.target.closest('button') : null;
    if (btn) {
      btn.disabled = true;
      btn.classList.remove('btn-prowlarr-grab--sent');
      btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin" aria-hidden="true"></i>';
    }
    // Tag the grab with the originating movie's metadata so
    // /downloads can render its poster on the tile and we can match
    // this download back to the movie later. Movies on /scenes
    // currently come from TPDB only.
    const m = window._currentMovie || {};
    const sourceScene = m && m.id ? {
      db:         'tpdb',
      id:         String(m.id || ''),
      title:      m.title || '',
      studio:     m.studio || '',
      performers: Array.isArray(m.performers)
        ? m.performers
        : (m.performer ? [m.performer] : []),
      poster_url: m.poster || '',
      date:       m.date || m.year || '',
    } : null;
    try {
      const resp = await fetch('/api/prowlarr/grab', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({kind: 'movie', guid: r.guid, indexer_id: r.indexer_id, download_url: r.download_url || r.magnet, type: r.type, title: r.title, source_scene: sourceScene}) });
      const d = await resp.json();
      if (d.ok || d.success) {
        if (btn) { btn.classList.add('btn-prowlarr-grab--sent'); btn.innerHTML = '<i class="fa-solid fa-check" aria-hidden="true"></i>'; }
      } else {
        alert(d.error || 'Could not send to download client');
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-download" aria-hidden="true"></i>'; }
      }
    } catch(e) {
      alert(e.message || 'Could not send to download client');
      if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-download" aria-hidden="true"></i>'; }
    }
  }

  function closeMovieProwlarr() { document.getElementById('movieProwlarrOverlay').classList.remove('open'); }

  // Handle click on movie cards in the feed grid (only exists on /scenes)
  document.getElementById('scenesGrid')?.addEventListener('click', function(e) {
    const movieCard = e.target.closest('.movie-card[data-movie-id]');
    if (movieCard) {
      showMovieDetail(movieCard.getAttribute('data-movie-id') || '');
      return;
    }
  });

  // Escape key handling for movie overlays
  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') { closeMovieDetail(); closeMovieProwlarr(); }
  });

  // Load feed and spotlight row on page init
  _applyFeedModeToggleUI();
  _updateTagFilterBadge();
  // Load wanted keys before (or concurrent with) the feed so the eye
  // buttons render in the correct state on first paint. Feed does not
  // depend on wanted keys — races are fine.
  _loadWantedKeys();
  // Gate init calls by which DOM the page actually has. Both /scenes and
  // /discover share this JS bundle; /scenes has the feed grid only,
  // /discover has the search panel + spotlight + detail panel only.
  if (document.getElementById('scenesGrid')) {
    if (_scenesFeedMode === 'tags') _ensureMonitoredTagsLoaded().then(loadFeed);
    else loadFeed();
  }
  if (document.getElementById('spotlightGrid')) {
    loadSpotlightRow();
  }
  // /discover deep-link: ?type=performer&q=NAME pre-fills the entity
  // search input and auto-runs the lookup, so the popup's "+ add"
  // button can hand a name straight off into the add flow without the
  // user retyping it.
  try {
    if (document.getElementById('searchInput')) {
      const _qp = new URLSearchParams(location.search);
      const _qType = (_qp.get('type') || '').toLowerCase();
      const _qQuery = (_qp.get('q') || '').trim();
      if (_qQuery) {
        const t = (_qType === 'studio' || _qType === 'movie') ? _qType : 'performer';
        if (typeof setType === 'function') setType(t);
        if (t === 'movie') {
          const mIn = document.getElementById('movieSearchInput');
          if (mIn) {
            mIn.value = _qQuery;
            if (typeof searchMovies === 'function') searchMovies();
          }
        } else {
          const sIn = document.getElementById('searchInput');
          if (sIn) {
            sIn.value = _qQuery;
            runSearch();
          }
        }
      }
    }
  } catch (e) { /* swallow — deep-link is best-effort */ }
