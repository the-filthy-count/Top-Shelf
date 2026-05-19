// discover-magazine.js — /discover-only magazine spread + carousel.
// Loaded on demand by scenes-common.js (not shipped to /scenes).
(function () {
  'use strict';

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


  async function renderPerformerPanel(ctx) {
    const body = ctx.body;
    const item = ctx.item;
    const itemId = ctx.itemId;
    const itemName = ctx.itemName;
    const images = ctx.images;
    const scenes = ctx.scenes;
    const kind = ctx.kind;
    const esc = ctx.esc;
    const sceneCard = ctx.sceneCard;
    const decorateLibraryMatches = ctx.decorateLibraryMatches;
    // Performer magazine spread on the left, 2×3 scenes grid on the right.
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
    // Only probe the first few gallery URLs for a portrait cover — each
    // probe decodes a full remote image; probing the entire shuffled list
    // was routinely 10–20 parallel loads before the panel could paint.
    const _coverProbeUrls = shuffled.slice(0, 10);
    const _coverProbes = await Promise.all(_coverProbeUrls.map(_coverProbe));
    let _coverIdx = _coverProbes.findIndex(p => p.portrait);
    let cover;
    let coverFallback = false;
    if (_coverIdx >= 0) {
      cover = _coverProbeUrls[_coverIdx];
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


  window.DiscoverMagazine = {
    renderPerformerPanel: renderPerformerPanel,
  };
  window.openMagCarousel = openMagCarousel;
  window.closeMagCarousel = closeMagCarousel;
  window.magCarouselNav = magCarouselNav;
})();
