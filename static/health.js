/* Externalized from library.html. */
/* Block 1/3: main /health page logic. */

  let scanning = false;
  let _healthResults = null;
  let _healthRoots = [];
  let _libraryStats = null;
  let _duplicateGroups = [];
  let _orphanList = [];
  let _scopePath = '';
  let _selectedMetric = 'total_videos';
  let _treeOpen = new Set();
  let _treePending = new Set();
  let _treeData = null;
  let _indexPoll = null;
  let _indexWasRunning = false;
  let _lastScanTime = '';
  let _dupCompareCurrent = null;
  let _dupFreedBytesTotal = 0;
  const _dupReviewQueue = {
    queue: [],
    index: 0,
    skippedList: [],
    pendingOnly: false,
    reset() {
      this.queue = [];
      this.index = 0;
      this.skippedList = [];
      this.pendingOnly = false;
    },
  };
  const METRIC_ORDER = ['total_videos', 'library_disk', 'orphaned', 'nfo_no_video', 'folder_no_tvshow_nfo', 'video_no_nfo', 'duplicates', 'empty_folder', 'performers', 'lib_movies', 'log', 'history'];
  const DISK_INCLUDE_LS = 'topShelfDiskIncludeKeys';
  let _diskIncludeKeys = null;
  const METRIC_META = {
    nfo_no_video: {
      title: 'Orphaned Metadata',
      sub: 'Episode or movie NFO files with no matching video in the same folder (excluding tvshow.nfo).',
    },
    video_no_nfo: {
      title: 'Missing Video Metadata',
      sub: 'Videos without an accepted NFO in the same folder. Scenes and episodes (Series + performer libraries): require a same-stem .nfo next to the file. Movies (each folder directly under Features or JAV): require movie.nfo or a same-stem .nfo. Use Match: phash lookup or manual search, then write NFO + poster in place — nothing is copied to scene or movie input folders.',
    },
    folder_no_tvshow_nfo: {
      title: 'Missing Series Metadata',
      sub: 'Immediate subfolders under Series and each performer root missing tvshow.nfo. Play writes full Kodi-style tvshow.nfo from your Favourites index match; poster.jpg is added only if missing.',
    },
    // ── Merged tiles — each combines two sub-metrics into one panel ──
    orphaned_all: {
      title: 'Orphaned',
      sub: 'Combined view of every "orphaned" category: filing-history rows whose destination is gone from disk, plus episode/movie NFO files with no matching video in the same folder. Each sub-list keeps its own per-row actions (remove from history, delete NFO).',
    },
    missing_metadata: {
      title: 'Missing Metadata',
      sub: 'Combined view of every "missing metadata" category: series folders missing tvshow.nfo, plus individual videos without an accepted NFO sidecar. Each sub-list keeps its own per-row actions (write tvshow.nfo from index, match video).',
    },
    empty_folder: {
      title: 'Empty Series',
      sub: 'Show-level folders with no video files anywhere underneath.',
    },
    total_videos: {
      title: 'Total Videos',
      sub: 'Indexed library_files entries (run Index Library first). The tree loads library roots first; subfolders and files load when you expand a folder. Each file shows size, codec, a resolution tier badge (SD / HD-720 / HD-1080 / 4K), and file date (from disk). Hover the badge for exact pixel dimensions. Fingerprint icon = at least one phash stored; check or cross = sidecar NFO (same rules as Missing Video metadata: stem NFO, or movie.nfo under Features / JAV movie folders).',
    },
    library_disk: {
      title: 'Library disk usage',
      sub: 'All files under your configured library roots (Series, Features, JAV library when set, performer directories). Use the checkboxes to include or exclude roots; charts and totals update to match. The bar chart lists the ten largest immediate subfolders (studios under Series, movie folders under Features, performer folders under each performer root).',
    },
    duplicates: {
      title: 'Duplicates',
      sub: 'Groups share the same perceptual hash (phash). The app recommends a keeper by resolution, file size, and filename. Use Compare for frames and filing metadata, the brain actions for a quick resolve or Auto-resolve all (low-confidence groups are skipped for manual review). The dice control ignores a group: it drops out of the duplicate count and is skipped by bulk auto-resolve until you turn ignore off.',
    },
    orphaned: {
      title: 'Orphaned filing history',
      sub: 'processed_files rows marked filed whose destination path no longer exists.',
    },
    performers: {
      title: 'Star Enrichment',
      sub: 'Enrichment status for all stars in the library. Fill Gaps fetches missing external links and headshots. Individual rows can be fetched or cleared independently.',
    },
    studios: {
      title: 'Studio Enrichment',
      sub: 'Enrichment status for all studios in the library. Shows TPDB / StashDB / FansDB match IDs, the current local logo, and folder-path health. Rows with a missing on-disk folder are flagged with the amber warning icon.',
    },
    vices: {
      title: 'Vices',
      sub: 'Every configured Vice with its folder-path health, cached logo status, and TPDB tag link (when assigned). Custom vices without a TPDB tag are marked as such.',
    },
    lib_movies: {
      title: 'Movies',
      sub: 'Every indexed movie folder — feature films under Features and JAV releases under your JAV directory — in one list. Each row shows TPDB, TMDB, and JAVStash matches. Click a source button to filter rows that Have / Are Missing that match; click again to cycle.',
    },
    log: {
      title: 'Activity Log',
      sub: 'Live pipeline activity log. Streams new entries as they are written.',
    },
    history: {
      title: 'Filing History',
      sub: 'Every file processed by the pipeline with its outcome. Filter by status, search by name, sort columns.',
    },
  };

  let _diskUsagePayload = null;
  let _pieChart = null;
  let _barChart = null;
  // Canvas doesn't resolve var(), so splice the current theme's triple at call time.
  function _cssRgbTriple(varName, fallback) {
    try {
      var v = getComputedStyle(document.documentElement).getPropertyValue(varName).trim();
      return v || fallback;
    } catch (e) { return fallback; }
  }
  function _themedRgba(varName, alpha, fallback) {
    return 'rgba(' + _cssRgbTriple(varName, fallback) + ',' + alpha + ')';
  }
  function diskPieColors() {
    return [
      _themedRgba('--brand-purple-rgb', 0.92, '155,77,202'),
      'rgba(94,234,212,0.92)',
      'rgba(96,165,250,0.92)',
      'rgba(251,191,36,0.9)',
      'rgba(248,113,113,0.88)',
      _themedRgba('--brand-purple-rgb', 0.88, '155,77,202'),
      'rgba(167,139,250,0.9)',
      'rgba(52,211,153,0.9)',
    ];
  }

  function destroyDiskCharts() {
    if (_pieChart) {
      _pieChart.destroy();
      _pieChart = null;
    }
    if (_barChart) {
      _barChart.destroy();
      _barChart = null;
    }
  }

  function kindLabelDisk(k) {
    if (k === 'studio') return 'Studio';
    if (k === 'performer') return 'Star';
    if (k === 'movie') return 'Movie';
    return k || '';
  }

  function renderDiskCharts(d) {
    destroyDiskCharts();
    if (typeof Chart === 'undefined') {
      window.toast('Chart library failed to load. Check your network and reload the page.');
      return;
    }
    const roots = (d && d.roots) || [];
    const pieLabelsBase = roots.map(function (r) { return r.label || r.key || r.path || ''; });
    const pieDataBase = roots.map(function (r) { return Math.max(0, Number(r.bytes) || 0); });
    const pieSum = pieDataBase.reduce(function (a, b) { return a + b; }, 0);
    var pieReal = pieSum > 0 && roots.length > 0;

    var pieLabels;
    var pieData;
    var pieBg;
    if (!roots.length) {
      pieLabels = ['Add library paths in Settings'];
      pieData = [1];
      pieBg = ['rgba(82,88,112,0.65)'];
      pieReal = false;
    } else if (!pieSum) {
      pieLabels = ['No size on disk (empty or inaccessible)'];
      pieData = [1];
      pieBg = ['rgba(82,88,112,0.65)'];
      pieReal = false;
    } else {
      pieLabels = pieLabelsBase;
      pieData = pieDataBase;
      const diskPie = diskPieColors();
      pieBg = pieLabels.map(function (_, i) { return diskPie[i % diskPie.length]; });
    }

    var pieCanvas = document.getElementById('libDiskPieDetail');
    if (!pieCanvas) return;
    // Re-centres the silhouette image inside the doughnut hole on every
    // render. The legend lives on the right, so Chart.js shifts the
    // chart area leftward — a CSS-only `top:50%; left:50%` would land
    // off-axis. We read `chart.chartArea` and reposition the <img>
    // relative to the canvas-host element (the silhouette's parent).
    var _silhouettePlugin = {
      id: 'silhouetteCenter',
      afterLayout: function (chart) {
        try {
          var host = chart.canvas && chart.canvas.parentElement && chart.canvas.parentElement.parentElement;
          if (!host) return;
          var sil = host.querySelector('.lib-disk-pie-silhouette');
          if (!sil) return;
          var area = chart.chartArea;
          if (!area) return;
          var canvasRect = chart.canvas.getBoundingClientRect();
          var hostRect = host.getBoundingClientRect();
          var cx = canvasRect.left - hostRect.left + (area.left + area.right) / 2;
          var cy = canvasRect.top - hostRect.top + (area.top + area.bottom) / 2;
          sil.style.left = cx + 'px';
          sil.style.top = cy + 'px';
          sil.style.transform = 'translate(-50%, -50%)';
        } catch (e) { /* ignore — silhouette stays at last position */ }
      },
    };
    _pieChart = new Chart(pieCanvas, {
      type: 'doughnut',
      plugins: [_silhouettePlugin],
      data: {
        labels: pieLabels,
        datasets: [{
          data: pieData,
          backgroundColor: pieBg,
          borderWidth: 0,
          hoverOffset: 28,
          hoverBorderWidth: 0,
          offset: 0,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: '58%',
        // The bleed wrapper extends 34px past the tile on every side; we
        // hold the doughnut radius back from those outer edges so the
        // hover-offset slice (offset:28) pops out into the bleed area
        // without touching the absolute canvas edge.
        layout: { padding: 6 },
        plugins: {
          legend: {
            position: 'right',
            align: 'center',
            labels: {
              color: '#c8c8dc',
              padding: 10,
              boxWidth: 14,
              boxHeight: 14,
              font: { family: "'DM Mono', monospace", size: 11 },
            },
          },
          tooltip: {
            callbacks: {
              label: function (ctx) {
                if (!pieReal) return ' ';
                var v = ctx.parsed;
                var raw = roots[ctx.dataIndex];
                if (!raw) return ' ';
                var pct = pieSum ? ((v / pieSum) * 100).toFixed(1) : '0';
                return ' ' + (raw.pretty || fmtSize(v)) + '  (' + pct + '%)';
              },
            },
          },
        },
      },
    });

    var top = (d && d.top_entities) || [];
    var barCanvas = document.getElementById('libDiskBarDetail');
    if (!barCanvas) return;
    if (!top.length) {
      _barChart = new Chart(barCanvas, {
        type: 'bar',
        data: {
          labels: ['No subfolders under Series, Features, or performer roots'],
          datasets: [{
            data: [0],
            backgroundColor: ['rgba(70,76,96,0.45)'],
            borderRadius: 6,
            borderSkipped: false,
          }],
        },
        options: {
          indexAxis: 'y',
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false }, tooltip: { enabled: false } },
          scales: {
            x: { display: false, grid: { display: false } },
            y: { grid: { display: false }, ticks: { color: '#9a9aae', font: { size: 11 } } },
          },
        },
      });
      return;
    }

    var barLabels = top.map(function (t) { return t.name; });
    var barData = top.map(function (t) { return Math.max(0, Number(t.bytes) || 0); });
    // Pick a unit + nice rounded step for the x axis so ticks are whole numbers.
    var barMax = barData.reduce(function (a, b) { return Math.max(a, b); }, 0);
    var barUnit = (function () {
      if (barMax >= 1099511627776) return { div: 1099511627776, label: 'TB' };
      if (barMax >= 1073741824)    return { div: 1073741824,    label: 'GB' };
      if (barMax >= 1048576)       return { div: 1048576,       label: 'MB' };
      if (barMax >= 1024)          return { div: 1024,          label: 'KB' };
      return { div: 1, label: 'B' };
    })();
    var barMaxUnits = barMax / barUnit.div;
    var niceStep = (function (raw) {
      if (raw <= 0) return 1;
      var pow = Math.pow(10, Math.floor(Math.log10(raw)));
      var n = raw / pow;
      var step = (n >= 5) ? 10 : (n >= 2) ? 5 : (n >= 1) ? 2 : 1;
      return step * pow;
    })(barMaxUnits / 5);
    var barAxisMax = Math.ceil(barMaxUnits / niceStep) * niceStep * barUnit.div;
    var barColors = top.map(function (t) {
      if (t.kind === 'studio') return 'rgba(96,165,250,0.88)';
      if (t.kind === 'performer') return _themedRgba('--brand-purple-rgb', 0.7, '155,77,202');
      if (t.kind === 'movie') return 'rgba(251,191,36,0.88)';
      return 'rgba(148,163,184,0.75)';
    });

    _barChart = new Chart(barCanvas, {
      type: 'bar',
      data: {
        labels: barLabels,
        datasets: [{
          label: 'Size',
          data: barData,
          backgroundColor: barColors,
          borderRadius: 6,
          borderSkipped: false,
        }],
      },
      options: {
        indexAxis: 'y',
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          x: {
            beginAtZero: true,
            max: barAxisMax,
            grid: { color: 'rgba(255,255,255,0.06)' },
            ticks: {
              color: '#8b8b9a',
              font: { family: "'DM Mono', monospace", size: 10 },
              stepSize: niceStep * barUnit.div,
              callback: function (val) {
                var n = Number(val) / barUnit.div;
                return Math.round(n) + ' ' + barUnit.label;
              },
            },
          },
          y: {
            grid: { display: false },
            ticks: {
              color: '#d8d8e8',
              font: { family: "'DM Mono', monospace", size: 11 },
            },
          },
        },
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              title: function (items) {
                var i = items[0].dataIndex;
                var row = top[i];
                return row ? (row.name + ' · ' + kindLabelDisk(row.kind)) : '';
              },
              label: function (item) {
                var i = item.dataIndex;
                var row = top[i];
                return row ? (row.pretty || fmtSize(item.parsed.x)) : '';
              },
            },
          },
        },
      },
    });
  }

  // Expanded unit labels for the Library Size tile. Rendered in place of
  // "Library Size" underneath the Skyfont counter so the counter itself
  // only carries the numeric magnitude.
  const _DISK_UNIT_LABELS = {
    b: 'Bytes', kb: 'Kilobytes', mb: 'Megabytes',
    gb: 'Gigabytes', tb: 'Terabytes', pb: 'Petabytes',
  };

  async function loadLibraryDiskUsage() {
    var el = document.getElementById('statDiskTotal');
    var lbl = document.getElementById('statDiskLabel');
    if (el) el.textContent = '…';
    try {
      var r = await fetch('/api/health/library-disk-usage');
      if (!r.ok) throw new Error();
      _diskUsagePayload = await r.json();
      const pretty = (_diskUsagePayload && _diskUsagePayload.total_pretty) || '';
      // Split "7.43 TB" → ["7.43", "TB"]. Counter gets the number; the
      // label slot underneath gets the expanded unit word so the grid
      // stays at 6 tiles of pure value.
      const m = pretty.match(/^([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)$/);
      if (m) {
        if (el)  el.textContent = m[1];
        if (lbl) lbl.textContent = _DISK_UNIT_LABELS[m[2].toLowerCase()] || m[2];
      } else {
        // Unexpected format — keep the raw string in the counter and
        // restore the default label so nothing is lost.
        if (el)  el.textContent = pretty || '—';
        if (lbl) lbl.textContent = 'Library Size';
      }
      scheduleFitHealthStatCounterValues();
      return _diskUsagePayload;
    } catch (e) {
      if (el)  el.textContent = '—';
      if (lbl) lbl.textContent = 'Library Size';
      _diskUsagePayload = null;
      return null;
    }
  }

  var _diskFilterDebounce = null;

  function diskSaveIncludeKeys(keys) {
    try {
      localStorage.setItem(DISK_INCLUDE_LS, JSON.stringify(keys));
    } catch (e) { /* ignore */ }
  }

  function diskBuildIncludeQuery(allRoots, selectedKeys) {
    var valid = new Set((allRoots || []).map(function (r) { return r.key; }));
    var sel = (selectedKeys || []).filter(function (k) { return valid.has(k); });
    if (!sel.length || sel.length === valid.size) return '';
    return '?include=' + encodeURIComponent(sel.join(','));
  }

  function resizeDiskChartsAfterLayout() {
    requestAnimationFrame(function () {
      requestAnimationFrame(function () {
        try {
          if (_pieChart) _pieChart.resize();
        } catch (e) { /* ignore */ }
        try {
          if (_barChart) _barChart.resize();
        } catch (e) { /* ignore */ }
      });
    });
  }

  async function renderLibraryDiskPanel() {
    destroyDiskCharts();
    var listEl = document.getElementById('detailList');
    listEl.classList.add('issue-list--disk-stretch');
    listEl.innerHTML = '<div class="empty disk-disk-panel">Measuring folders…</div>';
    try {
      var r0 = await fetch('/api/health/library-disk-usage');
      if (!r0.ok) throw new Error();
      var meta = await r0.json();
      var allRoots = meta.all_roots || [];
      var validKeys = new Set(allRoots.map(function (x) { return x.key; }));
      if (!_diskIncludeKeys || !_diskIncludeKeys.length) {
        try {
          var raw = localStorage.getItem(DISK_INCLUDE_LS);
          if (raw) {
            var parsed = JSON.parse(raw);
            if (Array.isArray(parsed) && parsed.length) _diskIncludeKeys = parsed;
          }
        } catch (e) { /* ignore */ }
      }
      if (!_diskIncludeKeys || !_diskIncludeKeys.length) {
        _diskIncludeKeys = allRoots.map(function (x) { return x.key; });
      }
      _diskIncludeKeys = _diskIncludeKeys.filter(function (k) { return validKeys.has(k); });
      if (!_diskIncludeKeys.length) _diskIncludeKeys = allRoots.map(function (x) { return x.key; });

      var q = diskBuildIncludeQuery(allRoots, _diskIncludeKeys);
      var d;
      if (q === '') {
        d = meta;
      } else {
        var r2 = await fetch('/api/health/library-disk-usage' + q);
        if (!r2.ok) throw new Error();
        d = await r2.json();
      }

      var filterHtml = '<span class="disk-filter-label">Include library roots</span>';
      allRoots.forEach(function (root) {
        var checked = _diskIncludeKeys.indexOf(root.key) >= 0 ? ' checked' : '';
        filterHtml += '<label><input type="checkbox" data-disk-root-key="' + esc(root.key) + '"' + checked + '> ' + esc(root.label || root.key) + '</label>';
      });

      listEl.innerHTML =
        '<div class="disk-disk-panel disk-disk-panel--fill">' +
        '<div class="disk-filter-row" id="diskFilterRow">' + filterHtml + '</div>' +
        '<p class="disk-total-line">Total (selected roots): <strong id="diskDetailTotal">' + esc(d.total_pretty || '—') + '</strong></p>' +
        '<div class="lib-disk-charts">' +
        '<div class="lib-disk-chart-wrap lib-disk-chart-wrap--pie">' +
        '<h3 class="lib-disk-chart-title">By root</h3>' +
        '<div class="lib-disk-canvas-h lib-disk-canvas-h--pie"><div class="lib-disk-canvas-bleed"><canvas id="libDiskPieDetail" aria-label="Disk by library root"></canvas></div><img class="lib-disk-pie-silhouette" src="/static/img/silhouette2.webp" alt="" aria-hidden="true" loading="lazy"></div>' +
        '</div>' +
        '<div class="lib-disk-chart-wrap">' +
        '<h3 class="lib-disk-chart-title">Largest folders<span class="lib-disk-hint">Studios, stars, and movie folders (top 10 among included roots)</span></h3>' +
        '<div class="lib-disk-canvas-h lib-disk-canvas-h--bar"><canvas id="libDiskBarDetail" aria-label="Top folders by size"></canvas></div>' +
        '</div></div></div>';

      listEl.querySelectorAll('#diskFilterRow input[data-disk-root-key]').forEach(function (inp) {
        inp.addEventListener('change', function () {
          var row = document.getElementById('diskFilterRow');
          if (!row) return;
          var keys = [];
          row.querySelectorAll('input[data-disk-root-key]:checked').forEach(function (c) {
            keys.push(c.getAttribute('data-disk-root-key') || '');
          });
          if (!keys.length) {
            inp.checked = true;
            return;
          }
          _diskIncludeKeys = keys;
          diskSaveIncludeKeys(_diskIncludeKeys);
          if (_diskFilterDebounce) clearTimeout(_diskFilterDebounce);
          _diskFilterDebounce = setTimeout(function () { void refreshLibraryDiskPanelData(allRoots); }, 280);
        });
      });

      renderDiskCharts(d);
      resizeDiskChartsAfterLayout();
    } catch (e) {
      listEl.classList.remove('issue-list--disk-stretch');
      listEl.innerHTML = '<div class="empty">Could not load disk usage.</div>';
    }
  }

  async function refreshLibraryDiskPanelData(allRoots) {
    destroyDiskCharts();
    var listEl = document.getElementById('detailList');
    var panel = listEl && listEl.querySelector('.disk-disk-panel');
    if (!panel) return;
    var totalEl = document.getElementById('diskDetailTotal');
    if (totalEl) totalEl.textContent = '…';
    try {
      var q = diskBuildIncludeQuery(allRoots, _diskIncludeKeys);
      var d;
      if (q === '') {
        var r0 = await fetch('/api/health/library-disk-usage');
        if (!r0.ok) throw new Error();
        d = await r0.json();
      } else {
        var r = await fetch('/api/health/library-disk-usage' + q);
        if (!r.ok) throw new Error();
        d = await r.json();
      }
      if (totalEl) totalEl.textContent = d.total_pretty || '—';
      renderDiskCharts(d);
      resizeDiskChartsAfterLayout();
    } catch (e) {
      if (totalEl) totalEl.textContent = '—';
    }
  }

  async function refreshLibraryDiskPanel() {
    await loadLibraryDiskUsage();
    if (_selectedMetric === 'library_disk') await renderLibraryDiskPanel();
  }

  function pathInScope(p) {
    if (!_scopePath) return true;
    const pre = _scopePath;
    return String(p || '').startsWith(pre);
  }

  function filterListPaths(arr) {
    return (arr || []).filter(pathInScope);
  }

  const SCOPE_LS_KEY = 'topShelfLibraryHealthScope';

  function fmtCount(n) {
    const x = Number(n);
    if (n === '-' || n === '' || n == null) return String(n);
    if (!Number.isFinite(x)) return String(n);
    return x.toLocaleString('en-US');
  }

  async function refreshHealthProgress() {
    try {
      const r = await fetch('/api/health/index-status');
      const d = await r.json();
      if (window.TsActivity && window.TsActivity.refresh) window.TsActivity.refresh();
      return d;
    } catch (e) {
      if (window.TsActivity && window.TsActivity.refresh) window.TsActivity.refresh();
      return {};
    }
  }

  function loadScopePreference() {
    try {
      const v = localStorage.getItem(SCOPE_LS_KEY);
      _scopePath = v && typeof v === 'string' ? v : '';
    } catch (e) {
      _scopePath = '';
    }
  }

  function saveScopePreference() {
    try {
      if (_scopePath) localStorage.setItem(SCOPE_LS_KEY, _scopePath);
      else localStorage.removeItem(SCOPE_LS_KEY);
    } catch (e) { /* ignore */ }
  }

  function validateScopePath() {
    if (!_scopePath) return;
    const ok = !_healthRoots.length || _healthRoots.some(r => (r.path || '') === _scopePath);
    if (!ok) _scopePath = '';
  }

  function updateHealthActionTitles() {
    const last = _lastScanTime
      ? ('Last library scan: ' + _lastScanTime)
      : 'Last library scan: none yet (run Scan Library)';
    document.getElementById('btnScan').title =
      'Scan Library — Scan Series, Features (movies), JAV, and performer roots for NFO gaps, missing tvshow.nfo, videos without a valid NFO (same-stem for scenes; movie.nfo or same-stem under Features / JAV), and empty show folders. ' + last;
    document.getElementById('btnIndexLib').title =
      'Index Library — Walk all configured library directories, build library_files, and compute phash_2 for corpus / duplicate detection. Runs on the server: you can leave this page and it will continue until finished. ' + last;
    document.getElementById('btnPhash3').title =
      'Rescan phash_3 — Compute or refresh the phash_3 verification fingerprint for eligible rows (see Settings for recurring rescan). ' + last;
    document.getElementById('btnCleanLib').title =
      'Clean Library — Delete is_removed rows from the library index; optional purge of linked processed_files history. ' + last;
  }

  function pickDefaultMetric(results) {
    const filteredDup = dupGroupsScopedForCounter().length;
    const filteredOrph = orphanScoped().length;
    for (const k of ['nfo_no_video', 'video_no_nfo', 'folder_no_tvshow_nfo', 'empty_folder']) {
      if ((results[k] || []).filter(pathInScope).length) return k;
    }
    if (filteredDup) return 'duplicates';
    if (filteredOrph) return 'orphaned';
    return 'total_videos';
  }

  function scopedVideoCount() {
    if (!_libraryStats || !_libraryStats.roots) return 0;
    if (!_scopePath) return _libraryStats.total_count || 0;
    const hit = _libraryStats.roots.find(r => r.path === _scopePath);
    return hit ? hit.count : _libraryStats.total_count;
  }

  function dupGroupsScoped() {
    return (_duplicateGroups || []).filter(g => (g.files || []).some(f => pathInScope(f.destination)));
  }

  function dupGroupIgnored(g) {
    if (!g) return false;
    if (g.ignored) return true;
    return (g.files || []).some(f => Number(f.duplicate_ignored) === 1);
  }

  function dupGroupsScopedForCounter() {
    return dupGroupsScoped().filter(g => !dupGroupIgnored(g));
  }

  function orphanScoped() {
    return (_orphanList || []).filter(o => pathInScope(o.destination));
  }

  function renderScopePills() {
    const row = document.getElementById('scopeRow');
    const el = document.getElementById('scopePills');
    if (!_healthRoots.length) {
      row.style.visibility = 'hidden';
      return;
    }
    row.style.visibility = '';
    const n = 1 + _healthRoots.length;
    const btns = [`<button type="button" class="${_scopePath ? '' : 'active'}" data-scope="">All</button>`];
    // Sort everything after "All" alphabetically by display label
    // (case-insensitive, with localeCompare for natural ordering) so
    // the toggle row stays predictable when the user adds new roots.
    // Shallow-copy first — we don't want to reorder the canonical
    // _healthRoots list, which other code may consume in config order.
    const sortedRoots = _healthRoots.slice().sort((a, b) => {
      const la = (a.label || a.path || '').toLowerCase();
      const lb = (b.label || b.path || '').toLowerCase();
      return la.localeCompare(lb);
    });
    for (const r of sortedRoots) {
      const p = r.path || '';
      btns.push(`<button type="button" class="${_scopePath === p ? 'active' : ''}" data-scope="${esc(p)}">${esc(r.label || p)}</button>`);
    }
    el.innerHTML = `<div class="ts-seg health-scope-seg" role="group" aria-label="Library root scope" style="grid-template-columns: repeat(${n}, minmax(0, 1fr));">${btns.join('')}</div>`;
    const seg = el.querySelector('.health-scope-seg');
    seg.querySelectorAll('button').forEach(btn => {
      btn.onclick = async () => {
        _scopePath = btn.getAttribute('data-scope') || '';
        saveScopePreference();
        seg.querySelectorAll('button').forEach(b => b.classList.toggle('active', (b.getAttribute('data-scope') || '') === _scopePath));
        refreshStatNumbers();
        if (_selectedMetric === 'total_videos') await loadTree();
        else await renderDetail();
      };
    });
  }

  let _fitStatCountersRaf = null;
  const FIT_STAT_COUNTER_MIN_PX = 12;

  function fitHealthStatCounterValues() {
    document.querySelectorAll('#statsBar .stat-value-shell').forEach((shell) => {
      const el = shell.querySelector('.stat-value');
      if (!el) return;
      // Skyfont tiles scale via CSS container queries (15cqi) — setting an
      // inline font-size here would clobber that. The monospaced 6-tile
      // layout fits by design, no binary-search needed.
      if (el.classList.contains('sky-counter')) {
        el.style.removeProperty('font-size');
        return;
      }
      el.style.removeProperty('font-size');
      const maxPx = parseFloat(getComputedStyle(el).fontSize);
      if (!Number.isFinite(maxPx) || maxPx <= 0) return;
      const limit = shell.clientWidth;
      if (limit <= 0) return;
      el.style.fontSize = maxPx + 'px';
      if (el.scrollWidth <= limit) return;
      let low = FIT_STAT_COUNTER_MIN_PX;
      let high = maxPx;
      let best = FIT_STAT_COUNTER_MIN_PX;
      for (let i = 0; i < 28; i++) {
        const mid = (low + high) / 2;
        el.style.fontSize = mid + 'px';
        if (el.scrollWidth <= limit) {
          best = mid;
          low = mid;
        } else {
          high = mid;
        }
      }
      el.style.fontSize = Math.max(FIT_STAT_COUNTER_MIN_PX, best) + 'px';
    });
  }

  function scheduleFitHealthStatCounterValues() {
    if (_fitStatCountersRaf != null) cancelAnimationFrame(_fitStatCountersRaf);
    _fitStatCountersRaf = requestAnimationFrame(() => {
      _fitStatCountersRaf = null;
      requestAnimationFrame(() => fitHealthStatCounterValues());
    });
  }

  function refreshStatNumbers() {
    const r = _healthResults || {};
    const nfoCount        = filterListPaths(r.nfo_no_video).length;
    const videoCount      = filterListPaths(r.video_no_nfo).length;
    const showNfoCount    = filterListPaths(r.folder_no_tvshow_nfo).length;
    const emptyCount      = filterListPaths(r.empty_folder).length;
    const orphanCount     = orphanScoped().length;
    // Merged tile counts — sum the two underlying metrics.
    const orphanedAllCount    = orphanCount + nfoCount;
    const missingMetadataCount = showNfoCount + videoCount;

    const setText = (id, val) => {
      const el = document.getElementById(id);
      if (!el) return;
      const formatted = fmtCount(val);
      // Sky-counter tiles are always right-aligned 6-char display
      // (space-padded to keep the tile grid visually stable). Regular
      // tiles keep the raw formatted number.
      el.textContent = el.classList.contains('sky-counter')
        ? padSkyCounter(formatted)
        : formatted;
    };
    setText('statTotal',        scopedVideoCount());
    setText('statDup',          dupGroupsScopedForCounter().length);
    setText('statOrphanAll',    orphanedAllCount);
    setText('statMissingMeta',  missingMetadataCount);
    // Legacy stat IDs — kept as no-ops if the element is gone, so any
    // external code that still writes to them doesn't throw.
    setText('statNfo',     nfoCount);
    setText('statVideo',   videoCount);
    setText('statShowNfo', showNfoCount);
    setText('statOrphan',  orphanCount);
    scheduleFitHealthStatCounterValues();
  }

  async function setSelectedMetric(key) {
    if (!METRIC_META[key]) return;
    if (_selectedMetric === 'library_disk' && key !== 'library_disk') {
      destroyDiskCharts();
    }
    if (_selectedMetric === 'log' && key !== 'log') {
      if (_logLibEventSource) { _logLibEventSource.close(); _logLibEventSource = null; }
    }
    _selectedMetric = key;
    document.querySelectorAll('#statsBar [data-metric]').forEach(el => {
      const on = el.getAttribute('data-metric') === key;
      el.classList.toggle('is-selected', on);
      el.setAttribute('aria-selected', on ? 'true' : 'false');
    });
    const isCustom = key === 'performers' || key === 'studios' || key === 'vices' || key === 'lib_movies' || key === 'log' || key === 'history';
    document.getElementById('detailPanel').style.display = isCustom ? 'none' : '';
    document.getElementById('perfEnrichPanel').style.display = key === 'performers' ? 'flex' : 'none';
    // Studios: show whichever sub-panel `_stuView` is set to. The old
    // segmented toggle was removed — users drill into the logo library
    // by clicking the Logos summary card, and return via the back
    // button in the logo panel header.
    const onStudios = key === 'studios';
    document.getElementById('stuEnrichPanel').style.display = (onStudios && _stuView === 'library') ? 'flex' : 'none';
    document.getElementById('stuLogoPanel').style.display = (onStudios && _stuView === 'logos') ? 'flex' : 'none';
    document.getElementById('vicesLibPanel').style.display = key === 'vices' ? 'flex' : 'none';
    document.getElementById('moviesLibPanel').style.display = key === 'lib_movies' ? 'flex' : 'none';
    document.getElementById('logLibPanel').style.display = key === 'log' ? 'flex' : 'none';
    document.getElementById('historyLibPanel').style.display = key === 'history' ? 'flex' : 'none';
    if (key === 'performers') {
      if (!_perfLoaded) await perfLoadStatus();
    } else if (key === 'studios') {
      if (_stuView === 'logos') {
        if (!_stuLogosLoaded) await stuLogoLoad();
      } else {
        if (!_stuLoaded) await stuLoadStatus();
      }
    } else if (key === 'vices') {
      if (!_vicesLoaded) await vicesLoadStatus();
    } else if (key === 'lib_movies') {
      if (!_movLoaded) await movLoadMovies();
    } else if (key === 'log') {
      logFilterRestore();
      await logLibReload();
    } else if (key === 'history') {
      await histLoadPage();
    } else if (key === 'total_videos') {
      await loadTree();
    } else {
      await renderDetail();
    }
  }

  function findTreeNodeByPath(nodes, targetPath) {
    if (!nodes || !targetPath) return null;
    for (const n of nodes) {
      if (n.path === targetPath) return n;
      const inChild = findTreeNodeByPath(n.children || [], targetPath);
      if (inChild) return inChild;
    }
    return null;
  }

  function mergeTreeNodePayload(data, requestedPath) {
    const node =
      findTreeNodeByPath(_treeData.roots, requestedPath)
      || findTreeNodeByPath(_treeData.roots, data.path);
    if (!node) return;
    if (data.path && data.path !== requestedPath && _treeOpen.delete(requestedPath)) {
      _treeOpen.add(data.path);
    }
    if (data.path && data.path !== node.path) {
      node.path = data.path;
    }
    node.children = data.children || [];
    node.leaves = data.leaves || [];
    node.lazy = false;
  }

  async function loadTreeNode(path) {
    if (!path || _treePending.has(path)) return;
    _treePending.add(path);
    try {
      const q = new URLSearchParams();
      q.set('path', path);
      if (_scopePath) q.set('scope', _scopePath);
      const r = await fetch('/api/health/tree-node?' + q.toString());
      if (!r.ok) return;
      const data = await r.json();
      if (data.error) return;
      mergeTreeNodePayload(data, path);
    } finally {
      _treePending.delete(path);
    }
  }

  async function loadTree() {
    if (_selectedMetric === 'total_videos') {
      document.getElementById('detailList').innerHTML = '<div class="empty">Loading tree…</div>';
    }
    const q = _scopePath ? ('?scope=' + encodeURIComponent(_scopePath)) : '';
    const r = await fetch('/api/health/tree' + q);
    _treeData = await r.json();
    _treeOpen = new Set();
    await renderDetail();
  }

  function fmtSize(n) {
    if (n == null || n === '') return '—';
    const x = Number(n);
    if (!Number.isFinite(x)) return '—';
    if (x < 1024) return x + ' B';
    if (x < 1048576) return (x / 1024).toFixed(1) + ' KB';
    if (x < 1073741824) return (x / 1048576).toFixed(1) + ' MB';
    return (x / 1073741824).toFixed(2) + ' GB';
  }

  function treeMediaBit(text, cls, empty, withSep) {
    const t = text || '—';
    const e = empty ? ' tree-leaf-m-bit--empty' : '';
    const sep = withSep ? '<span class="tree-leaf-sep-inline" aria-hidden="true">·</span>' : '';
    return `<span class="tree-leaf-m-bit ${cls}${e}">${sep}<span class="tree-leaf-m-bit-text">${esc(t)}</span></span>`;
  }

  /** Map pixel dimensions to SD / HD-720 / HD-1080 / 4K (uses max and min edge to handle portrait). */
  function treeClassifyResolution(w, h) {
    const ww = Number(w);
    const hh = Number(h);
    if (!Number.isFinite(ww) || !Number.isFinite(hh) || ww <= 0 || hh <= 0) return null;
    const long = Math.max(ww, hh);
    const short = Math.min(ww, hh);
    const title = ww + '×' + hh;
    if (long >= 3840 || short >= 2160) return { tier: '4k', label: '4K', title };
    if (long >= 1920 || short >= 1080) return { tier: 'hd1080', label: 'HD-1080', title };
    if (long >= 1280 || short >= 720) return { tier: 'hd720', label: 'HD-720', title };
    return { tier: 'sd', label: 'SD', title };
  }

  function treeResolutionBadgeCell(w, h, withSep) {
    const sep = withSep ? '<span class="tree-leaf-sep-inline" aria-hidden="true">·</span>' : '';
    const inf = treeClassifyResolution(w, h);
    if (!inf) {
      return `<span class="tree-leaf-m-bit tree-leaf-m-res tree-leaf-m-bit--empty">${sep}<span class="tree-leaf-m-bit-text">—</span></span>`;
    }
    return `<span class="tree-leaf-m-bit tree-leaf-m-res">${sep}<span class="tree-leaf-res-badge tree-leaf-res-badge--${inf.tier}" title="${esc(inf.title)}">${esc(inf.label)}</span></span>`;
  }

  function treeWaWithFallback(waName, faClass, stemClass, titleOpt) {
    const t = titleOpt ? ` title="${esc(titleOpt)}"` : '';
    return `<i class="${stemClass}-fb fa-solid ${faClass}"${t}></i>`;
  }

  /** features_dir → key `features` (Movies Library Directory): clapperboard. series_dir, performers, etc. → ticket. */
  function treeLeafLibraryIcon(libraryRootKey) {
    const k = String(libraryRootKey || '');
    if (k === 'features') {
      return `<span class="tree-leaf-lib-cell"><i class="tree-leaf-lib-ico-fb ts-icon-movies" title="Movies library" aria-hidden="true"></i></span>`;
    }
    return `<span class="tree-leaf-lib-cell">${treeWaWithFallback('ticket', 'fa-ticket', 'tree-leaf-lib-ico', 'Other library root')}</span>`;
  }

  /** Split basename into stem + short extension for tree label (video-style last segment). */
  function treeLeafStemExt(basename) {
    const s = String(basename || '');
    const last = s.lastIndexOf('.');
    if (last <= 0) return { stem: s, ext: '' };
    const rawExt = s.slice(last + 1);
    if (!/^[a-z0-9]{2,8}$/i.test(rawExt)) return { stem: s, ext: '' };
    return { stem: s.slice(0, last), ext: rawExt.toLowerCase() };
  }

  function treeLeafRow(leaf, libraryRootKey) {
    const sizeRaw = leaf.size;
    const sizeNum = Number(sizeRaw);
    const sizeEmpty = sizeRaw == null || sizeRaw === '' || !Number.isFinite(sizeNum);
    const sizeStr = sizeEmpty ? '—' : fmtSize(sizeRaw);

    const codecRaw = (leaf.codec || '').trim();
    const codecEmpty = !codecRaw;
    const codecStr = codecEmpty ? '—' : codecRaw;

    const w = leaf.width != null ? Number(leaf.width) : NaN;
    const h = leaf.height != null ? Number(leaf.height) : NaN;

    const fc = (leaf.file_created || '').trim();
    const dateEmpty = !fc;
    const dateStr = dateEmpty ? '—' : fc;

    const unmatched = !leaf.has_nfo;
    let fp;
    if (unmatched) {
      const t = leaf.phash_any
        ? 'Perceptual hash stored — no sidecar NFO (unmatched)'
        : 'No phash cached — no sidecar NFO (unmatched)';
      fp = `<i class="tree-leaf-fp tree-leaf-fp-unmatched fa-solid fa-fingerprint" title="${esc(t)}" aria-hidden="true"></i>`;
    } else if (leaf.phash_any) {
      fp = '<i class="tree-leaf-fp fa-solid fa-fingerprint" title="phash stored" aria-hidden="true"></i>';
    } else {
      fp = '<i class="tree-leaf-fp tree-leaf-fp-muted fa-solid fa-fingerprint" title="No phash" aria-hidden="true"></i>';
    }
    const metaOk = leaf.has_nfo
      ? '<i class="tree-leaf-meta-ico tree-leaf-meta-ico--has fa-solid fa-file-lines" title="NFO present" aria-hidden="true"></i>'
      : '<i class="tree-leaf-meta-ico tree-leaf-meta-ico--miss fa-solid fa-file-circle-xmark" title="No NFO" aria-hidden="true"></i>';
    const ne = treeLeafStemExt(leaf.name);
    const nameInner = ne.ext
      ? `<span class="tree-leaf-name-inner"><span class="tree-leaf-name-text">${esc(ne.stem)}</span><span class="tree-leaf-ext-badge" title="Container">${esc(ne.ext)}</span></span>`
      : `<span class="tree-leaf-name-text">${esc(leaf.name)}</span>`;
    const fullPath = leaf.destination || leaf.path || '';
    const SRC_LOGO = {
      stashdb: { src: '/static/logos/stashdb.webp', label: 'StashDB' },
      tpdb:    { src: '/static/logos/tpdb.webp',    label: 'TPDB' },
      fansdb:  { src: '/static/logos/fansdb.webp',  label: 'FansDB' },
      javstash: { src: '/static/logos/javstash.webp', label: 'JAVStash' },
    };
    const order = ['stashdb', 'tpdb', 'fansdb', 'javstash'];
    const seenSrc = new Set();
    const matchLinks = [];
    for (const m of (leaf.matches || [])) {
      const meta = SRC_LOGO[m.source];
      if (!meta || !m.url || seenSrc.has(m.source)) continue;
      seenSrc.add(m.source);
      matchLinks.push({ source: m.source, url: m.url, label: meta.label, src: meta.src });
    }
    matchLinks.sort((a, b) => order.indexOf(a.source) - order.indexOf(b.source));
    const matchHtml = matchLinks.map(m =>
      `<a class="tree-leaf-match-link" href="${esc(m.url)}" target="_blank" rel="noopener noreferrer" title="Open on ${esc(m.label)}" onclick="event.stopPropagation()"><img src="${esc(m.src)}" alt="${esc(m.label)}" loading="lazy"></a>`
    ).join('');
    return `<div class="tree-leaf">
      <div class="tree-leaf-primary">
        ${treeLeafLibraryIcon(libraryRootKey)}
        <span class="tree-leaf-name">${nameInner}</span>
      </div>
      <span class="tree-leaf-fp-cell">${fp}</span>
      ${treeMediaBit(sizeStr, 'tree-leaf-m-size', sizeEmpty, false)}
      ${treeMediaBit(codecStr, 'tree-leaf-m-codec', codecEmpty, true)}
      ${treeResolutionBadgeCell(w, h, true)}
      ${treeMediaBit(dateStr, 'tree-leaf-m-date', dateEmpty, true)}
      <span class="tree-leaf-nfo-cell">${metaOk}</span>
      <span class="tree-leaf-match-cell">${matchHtml}</span>
      <span class="tree-leaf-lookup-cell">
        <button class="tree-leaf-lookup-btn" title="phash lookup" data-lookup-path="${esc(fullPath)}" data-lookup-name="${esc(leaf.name || '')}"><i class="fa-solid fa-magnifying-glass" aria-hidden="true"></i></button>
      </span>
    </div>`;
  }

  function renderTreeNode(node, libraryRootKey) {
    const rootKey = node.depth === 0 ? String(node.key || '') : String(libraryRootKey || '');
    const hasKids = (node.children && node.children.length)
      || (node.leaves && node.leaves.length)
      || (node.lazy && (node.file_count > 0));
    const open = _treeOpen.has(node.path);
    let html = '';
    const pe = encodeURIComponent(node.path || '');
    if (node.depth === 0 || node.name) {
      const rootCls = node.depth === 0 ? ' tree-dir--root' : '';
      const chev = `<i class="fa-solid ${hasKids ? (open ? 'fa-chevron-down' : 'fa-chevron-right') : 'fa-minus'}" aria-hidden="true"></i>`;
      const folderPair = hasKids && open
        ? treeWaWithFallback('folder-open', 'fa-folder-open', 'tree-wa-folder')
        : treeWaWithFallback('folder', 'fa-folder', 'tree-wa-folder');
      const leading = `<span class="tree-toggle-leading">${chev}${folderPair}</span>`;
      html += `<div class="tree-dir${rootCls}">
        <div class="tree-toggle" data-tree-path="${pe}" role="button" tabindex="0">
          ${leading}
          <span class="tree-toggle-label">${esc(node.label || node.name)}</span>
          <span class="tree-meta">${node.file_count != null ? fmtCount(node.file_count) + ' videos' : ''}</span>
        </div>`;
      if (open && hasKids) {
        html += '<div class="tree-dir-inner">';
        for (const leaf of node.leaves || []) {
          html += treeLeafRow(leaf, rootKey);
        }
        for (const ch of node.children || []) {
          html += renderTreeNode(ch, rootKey);
        }
        html += '</div>';
      }
      html += '</div>';
    }
    return html;
  }

  document.addEventListener('click', async (e) => {
    const t = e.target.closest('.tree-toggle[data-tree-path]');
    if (!t) return;
    let path = '';
    try { path = decodeURIComponent(t.getAttribute('data-tree-path') || ''); } catch (err) { return; }
    if (!path) return;
    if (_treeOpen.has(path)) {
      _treeOpen.delete(path);
    } else {
      _treeOpen.add(path);
      if (!_treeData || !(_treeData.roots || []).length) {
        await renderDetail();
        return;
      }
      const node = findTreeNodeByPath(_treeData.roots, path);
      if (node && node.lazy) {
        t.classList.add('tree-toggle--loading');
        try {
          await loadTreeNode(path);
        } finally {
          t.classList.remove('tree-toggle--loading');
        }
      }
    }
    await renderDetail();
  });

  let _dupAutoPoll = null;

  /** Web Awesome icon + Font Awesome fallback (see queue phash badge). */
  function dupWaPair(waName, faClass) {
    return '<span class="dup-wa-icon"><i class="fa-solid ' + faClass + ' dup-wa-fb"></i></span>';
  }

  function dupGroupNeedsReview(g) {
    if (!g) return false;
    if (dupGroupIgnored(g)) return false;
    const rec = g.recommendation || {};
    if (rec.confidence === 'low') return true;
    return (g.files || []).some(f => Number(f.duplicate_review_pending) === 1);
  }

  function dupRecommendedPath(g) {
    const r = g && g.recommendation;
    if (!r) return '';
    return (r.keeper_destination || '').trim();
  }

  function dupAutoResolveEstimate() {
    const groups = dupGroupsScoped();
    let bytes = 0;
    let resolveGroups = 0;
    let low = 0;
    for (const g of groups) {
      if (dupGroupIgnored(g)) continue;
      const rec = g.recommendation || {};
      if (rec.confidence === 'low') { low++; continue; }
      const k = (rec.keeper_destination || '').trim();
      if (!k) continue;
      resolveGroups++;
      for (const f of (g.files || [])) {
        if ((f.destination || '') !== k) bytes += Number(f.size_bytes) || 0;
      }
    }
    return { resolveGroups, bytes, low };
  }

  function getDupAutoConfirmHtml() {
    const est = dupAutoResolveEstimate();
    const gb = est.bytes > 0 ? (est.bytes / (1024 * 1024 * 1024)).toFixed(1) : '0';
    return `<div id="dupAutoConfirm" class="dup-auto-confirm" style="display:none">
      <p style="margin:0 0 8px">Auto-resolve will keep the recommended file in each group and delete all others.</p>
      <p style="margin:0 0 12px;color:var(--dim)">
        ${est.resolveGroups} groups · ~${gb} GB freed<br>
        ${est.low} low-confidence group(s) will be skipped
      </p>
      <div style="display:flex;gap:8px;justify-content:flex-end">
        <button type="button" class="btn-dup-auto-cancel" id="dupAutoCancel" title="Cancel" aria-label="Cancel"><i class="fa-solid fa-xmark" aria-hidden="true"></i></button>
        <button type="button" class="btn-primary" id="dupAutoGo" style="font-size:12px;padding:8px 14px" title="Auto-resolve all" aria-label="Auto-resolve all">${dupWaPair('brain', 'fa-brain')}</button>
      </div>
      <div id="dupAutoProgress" style="display:none;margin-top:10px;font-size:11px;color:var(--dim)"></div>
    </div>`;
  }

  function wireDupAutoConfirm() {
    const box = document.getElementById('dupAutoConfirm');
    if (!box) return;
    const c = document.getElementById('dupAutoCancel');
    const g = document.getElementById('dupAutoGo');
    if (c) c.onclick = () => { box.style.display = 'none'; };
    if (g) g.onclick = () => { void runDupAutoResolve(); };
  }

  function showDupAutoResolveConfirm() {
    const host = document.getElementById('dupAutoConfirmHost');
    if (!host) return;
    host.innerHTML = getDupAutoConfirmHtml();
    wireDupAutoConfirm();
    const box = document.getElementById('dupAutoConfirm');
    if (box) box.style.display = 'block';
  }

  async function runDupAutoResolve() {
    const prog = document.getElementById('dupAutoProgress');
    const go = document.getElementById('dupAutoGo');
    if (go) go.disabled = true;
    if (prog) {
      prog.style.display = 'block';
      prog.textContent = 'Starting…';
    }
    try {
      const r = await fetch('/api/health/duplicates/resolve-all', { method: 'POST' });
      if (!r.ok) {
        let err = 'Failed to start';
        try {
          const d = await r.json();
          err = d.error || err;
        } catch (e) { /* ignore */ }
        throw new Error(err);
      }
      if (_dupAutoPoll) clearInterval(_dupAutoPoll);
      _dupAutoPoll = setInterval(async () => {
        if (document.visibilityState !== 'visible') return;
        const s = await fetch('/api/health/duplicates/resolve-status').then(x => x.json()).catch(() => ({}));
        if (prog && s.running) {
          prog.textContent = 'Resolved ' + (s.resolved || 0) + ' · skipped ' + (s.skipped || 0) + '…';
        }
        if (s.done) {
          if (_dupAutoPoll) clearInterval(_dupAutoPoll);
          _dupAutoPoll = null;
          const res = s.result || {};
          if (res.error) {
            if (prog) prog.textContent = String(res.error);
            if (go) go.disabled = false;
            return;
          }
          const freed = Number(res.freed_bytes) || 0;
          _dupFreedBytesTotal += freed;
          if (prog) {
            prog.innerHTML = '✓ Resolved ' + (res.resolved || 0) + ' groups · freed ' + fmtSize(freed) +
              '<br>⚠ ' + (res.skipped || 0) + ' groups need manual review';
          }
          if (go) go.disabled = false;
          await loadDuplicates();
          refreshStatNumbers();
          if (_selectedMetric === 'duplicates') await renderDetail();
        }
      }, 600);
    } catch (e) {
      if (prog) prog.textContent = String(e.message || e);
      if (go) go.disabled = false;
    }
  }

  function startDupReviewAll() {
    _dupReviewQueue.reset();
    _dupReviewQueue.queue = dupGroupsScoped().filter(g => !dupGroupIgnored(g)).slice();
    _dupReviewQueue.index = 0;
    if (!_dupReviewQueue.queue.length) return;
    openDupCompareModal(_dupReviewQueue.queue[0], true);
  }

  function startDupReviewSkipped() {
    _dupReviewQueue.reset();
    _dupReviewQueue.pendingOnly = true;
    _dupReviewQueue.queue = dupGroupsScoped().filter(g => dupGroupNeedsReview(g) && !dupGroupIgnored(g));
    _dupReviewQueue.index = 0;
    if (!_dupReviewQueue.queue.length) return;
    openDupCompareModal(_dupReviewQueue.queue[0], true);
  }

  async function dupToggleIgnore(phash, nextIgnored, btn) {
    if (!phash) return;
    if (btn) btn.disabled = true;
    try {
      const r = await fetch('/api/health/duplicates/ignore', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ phash, ignored: !!nextIgnored }),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok || !d.ok) {
        window.toast(d.error || 'Could not update ignore state');
        if (btn) btn.disabled = false;
        return;
      }
      await loadDuplicates();
      refreshStatNumbers();
      if (_selectedMetric === 'duplicates') await renderDetail();
    } catch (e) {
      window.toast(String(e.message || e));
      if (btn) btn.disabled = false;
    }
  }

  async function dupKeepRecommendedForGroup(g, btn) {
    if (!g) return;
    if (dupGroupIgnored(g)) return;
    const recPath = dupRecommendedPath(g);
    if (!recPath) return;
    const del = (g.files || []).map(f => f.destination).filter(p => p && p !== recPath);
    btn.disabled = true;
    try {
      const r = await fetch('/api/health/duplicates/resolve-one', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ keep_path: recPath, delete_paths: del }),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok || d.error) {
        window.toast(d.error || 'Resolve failed');
        btn.disabled = false;
        return;
      }
      const fb = Number(d.freed_bytes) || 0;
      _dupFreedBytesTotal += fb;
      const row = btn.closest('.dup-group');
      if (row) row.classList.add('faded');
      await loadDuplicates();
      refreshStatNumbers();
      if (_selectedMetric === 'duplicates') await renderDetail();
    } catch (e) {
      window.toast(String(e.message || e));
      btn.disabled = false;
    }
  }

  async function renderDetail() {
    const key = _selectedMetric;
    const meta = METRIC_META[key] || { title: '—', sub: '' };
    const headAct = document.getElementById('detailHeadActions');
    headAct.innerHTML = '';
    headAct.style.display = 'none';
    document.getElementById('detailTitle').textContent = meta.title;
    const helpBtn = document.getElementById('detailHelpBtn');
    const sub = String(meta.sub || '').trim();
    if (sub) {
      helpBtn.style.display = '';
      helpBtn.onclick = function () {
        window.TsHelp.openText(sub);
      };
    } else {
      helpBtn.style.display = 'none';
      helpBtn.onclick = null;
    }
    const listEl = document.getElementById('detailList');
    listEl.classList.remove('issue-list--disk-stretch');

    if (key === 'library_disk') {
      headAct.style.display = '';
      headAct.innerHTML = '<button type="button" class="btn-tiny" id="diskHeadRefresh" title="Rescan"><i class="fa-solid fa-arrows-rotate" aria-hidden="true"></i></button>';
      const dh = document.getElementById('diskHeadRefresh');
      if (dh) dh.onclick = () => { void refreshLibraryDiskPanel(); };
      await renderLibraryDiskPanel();
      return;
    }

    if (key === 'total_videos') {
      if (!_treeData || !(_treeData.roots || []).length) {
        listEl.innerHTML = '<div class="empty">No tree data. Run <strong>Index Library</strong> to build library_files, then open this tile again.</div>';
        return;
      }
      listEl.innerHTML = '<div class="tree-view">' + ((_treeData.roots || []).map((n) => renderTreeNode(n, String(n.key || ''))).join('') || '<div class="empty">No files in scope.</div>') + '</div>';
      return;
    }

    if (key === 'duplicates') {
      const groups = dupGroupsScoped();
      if (!groups.length) {
        listEl.innerHTML = '<div class="empty">No duplicate groups in this scope. Run <strong>Index Library</strong> so files get phash fingerprints; duplicates are matched by shared phash only.</div>';
        return;
      }
      headAct.style.display = '';
      const hasSkip = groups.some(g => dupGroupNeedsReview(g));
      const freedLine = _dupFreedBytesTotal > 0
        ? `<span class="dup-freed-line" style="font-size:11px;color:var(--green);margin-right:10px">Freed ${fmtSize(_dupFreedBytesTotal)}</span>`
        : '';
      headAct.innerHTML = freedLine +
        '<button type="button" class="btn-dup-keep-rec" id="btnDupAutoResolve" title="Auto-resolve all" aria-label="Auto-resolve all">' + dupWaPair('brain', 'fa-brain') + '</button>' +
        '<button type="button" class="btn-dup-compare" id="btnDupReviewAll" style="margin-left:6px" title="Review all" aria-label="Review all"><i class="fa-solid fa-code-compare" aria-hidden="true"></i></button>' +
        (hasSkip ? '<button type="button" class="btn-dup-review-skipped" id="btnDupReviewSkipped" title="Review skipped groups" aria-label="Review skipped">' + dupWaPair('code-branch', 'fa-code-branch') + '</button>' : '');
      document.getElementById('btnDupReviewAll').onclick = () => startDupReviewAll();
      document.getElementById('btnDupAutoResolve').onclick = () => showDupAutoResolveConfirm();
      const rs = document.getElementById('btnDupReviewSkipped');
      if (rs) rs.onclick = () => startDupReviewSkipped();
      listEl.innerHTML = '<div id="dupAutoConfirmHost"></div>' + groups.map((g, gi) => {
        const isIg = dupGroupIgnored(g);
        const needRev = dupGroupNeedsReview(g);
        const recPath = dupRecommendedPath(g);
        const gClass = 'dup-group' + (isIg ? ' dup-group--ignored' : needRev ? ' dup-group--review' : ' dup-group--rec');
        const badge = isIg
          ? '<span class="dup-badge dup-badge--ignored">Ignored</span>'
          : needRev
            ? '<span class="dup-badge dup-badge--review">Needs review</span>'
            : '<span class="dup-badge dup-badge--rec">Recommended</span>';
        const headLabel = 'Phash: ' + esc(g.phash || '') + badge;
        const rows = (g.files || []).filter(f => pathInScope(f.destination)).map(f => {
          const enc = encodeURIComponent(f.destination || '');
          const isRec = !needRev && recPath && (f.destination || '') === recPath;
          const recMark = isRec
            ? '<span class="dup-row-recmark dup-row-recmark--keep" title="Recommended keeper">' + dupWaPair('circle-plus', 'fa-circle-plus') + '</span>'
            : '<span class="dup-row-recmark dup-row-recmark--drop" title="Not recommended">' + dupWaPair('circle-minus', 'fa-circle-minus') + '</span>';
          const diskHint = f.on_disk
            ? '<span class="dup-row-diskmark" title="On disk">' + dupWaPair('floppy-disk', 'fa-floppy-disk') + '</span>'
            : ' <span style="font-size:10px;color:var(--amber)" title="Missing on disk">(missing)</span>';
          return `
          <div class="issue-row">
            <span class="issue-row-path" style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;min-width:0">${recMark}<span style="min-width:0;word-break:break-all">${esc(f.destination)}</span>${diskHint}</span>
            <span class="issue-row-actions">
              <button type="button" class="btn-icon-danger" data-dup-del="${enc}" title="Delete video" aria-label="Delete video"><i class="fa-solid fa-xmark" aria-hidden="true"></i></button>
            </span>
          </div>`;
        }).join('');
        const keepBtn = !isIg && !needRev && recPath
          ? `<button type="button" class="btn-dup-keep-rec" data-dup-keep-rec="${gi}" title="Keep recommended" aria-label="Keep recommended">${dupWaPair('brain', 'fa-brain')}</button>`
          : '';
        const filmBtn = `<button type="button" class="btn-dup-filmstrip" data-dup-filmstrip="${gi}" title="Filmstrip — compare frames" aria-label="Filmstrip"><i class="fa-solid fa-photo-film" aria-hidden="true"></i></button>`;
        const phEnc = encodeURIComponent(g.phash || '');
        const ignClass = 'btn-dup-ignore' + (isIg ? ' is-ignored' : '');
        const ignBtn = `<button type="button" class="${ignClass}" data-dup-ignore-phash="${phEnc}" title="${isIg ? 'Include in duplicate count again' : 'Ignore — exclude from count and auto-resolve'}" aria-label="${isIg ? 'Stop ignoring duplicate group' : 'Ignore duplicate group'}"><i class="fa-solid fa-dice-two" aria-hidden="true"></i></button>`;
        return `<div class="${gClass}">
          <div class="dup-group-head">
            <div class="dup-phash-line">${headLabel}</div>
            <span style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">
              ${keepBtn}
              ${filmBtn}
              ${ignBtn}
              <button type="button" class="btn-dup-compare" data-dup-compare="${gi}" title="Compare" aria-label="Compare"><i class="fa-solid fa-code-compare" aria-hidden="true"></i></button>
            </span>
          </div>
          ${rows}
        </div>`;
      }).join('');
      const host = document.getElementById('dupAutoConfirmHost');
      if (host) host.innerHTML = getDupAutoConfirmHtml();
      listEl.querySelectorAll('[data-dup-del]').forEach(btn => {
        btn.onclick = () => {
          let p = '';
          try { p = decodeURIComponent(btn.getAttribute('data-dup-del') || ''); } catch (e) { return; }
          void handleDupRowDelete(p, btn);
        };
      });
      listEl.querySelectorAll('[data-dup-compare]').forEach(btn => {
        btn.onclick = () => {
          const gi = parseInt(btn.getAttribute('data-dup-compare') || '-1', 10);
          const g = dupGroupsScoped()[gi];
          if (g) {
            _dupReviewQueue.reset();
            openDupCompareModal(g);
          }
        };
      });
      listEl.querySelectorAll('[data-dup-keep-rec]').forEach(btn => {
        btn.onclick = () => {
          const gi = parseInt(btn.getAttribute('data-dup-keep-rec') || '-1', 10);
          void dupKeepRecommendedForGroup(dupGroupsScoped()[gi], btn);
        };
      });
      listEl.querySelectorAll('[data-dup-filmstrip]').forEach(btn => {
        btn.onclick = () => {
          const gi = parseInt(btn.getAttribute('data-dup-filmstrip') || '-1', 10);
          const g = dupGroupsScoped()[gi];
          if (g && typeof window.openDupFilmstrip === 'function') window.openDupFilmstrip(g);
        };
      });
      listEl.querySelectorAll('[data-dup-ignore-phash]').forEach(btn => {
        btn.onclick = () => {
          let ph = '';
          try { ph = decodeURIComponent(btn.getAttribute('data-dup-ignore-phash') || ''); } catch (e) { return; }
          const curIg = btn.classList.contains('is-ignored');
          void dupToggleIgnore(ph, !curIg, btn);
        };
      });
      wireDupAutoConfirm();
      return;
    }

    // ── Merged: Orphaned ─────────────────────────────────────────
    // Combines filing-history orphans + orphaned NFO files into one
    // panel with two section headers. Each sub-list keeps its own
    // per-row action (remove from history vs. delete NFO).
    if (key === 'orphaned_all') {
      const orphans = orphanScoped();
      const rData = _healthResults || {};
      const nfos = filterListPaths(rData.nfo_no_video || []);
      if (!orphans.length && !nfos.length) {
        listEl.innerHTML = '<div class="empty">No orphaned records or NFO files in this scope.</div>';
        return;
      }
      const headBits = [];
      if (orphans.length) {
        headBits.push('<button type="button" class="btn-tiny" id="btnResolveAllOrphans" title="Resolve any files that exist under a different extension" style="margin-right:6px">Resolve all</button>');
        headBits.push('<button type="button" class="btn-icon-danger btn-head-bulk-delete" id="btnRemoveAllOrphans" title="Remove all from history" aria-label="Remove all from history" style="margin-right:6px"><i class="fa-solid fa-xmark" aria-hidden="true"></i></button>');
      }
      if (nfos.length) {
        headBits.push('<button type="button" class="btn-icon-danger btn-head-bulk-delete" id="btnDeleteAllNfoMerged" title="Delete all orphaned NFO files" aria-label="Delete all orphaned NFO files"><i class="fa-solid fa-file-circle-xmark" aria-hidden="true"></i></button>');
      }
      if (headBits.length) {
        headAct.style.display = '';
        headAct.innerHTML = headBits.join('');
      }
      const sections = [];
      if (orphans.length) {
        sections.push(
          '<div class="issue-subhead">Filing-history orphans <span class="issue-subhead-count">' + fmtCount(orphans.length) + '</span></div>'
          + orphans.map(o => `
            <div class="issue-row issue-row--orphan" data-orphan-id="${o.id}">
              <div class="issue-row-orphan-main">
                <div class="issue-row-path">${esc(o.filename || '')}</div>
                <div style="font-size:10px;color:var(--muted)">${esc(o.destination)}</div>
              </div>
              <span class="issue-row-actions issue-row-actions--end">
                <button type="button" class="btn-icon-danger" data-orphan-remove="${o.id}" title="Remove" aria-label="Remove from history"><i class="fa-solid fa-xmark" aria-hidden="true"></i></button>
              </span>
            </div>`).join('')
        );
      }
      if (nfos.length) {
        sections.push(
          '<div class="issue-subhead">Orphaned NFO files <span class="issue-subhead-count">' + fmtCount(nfos.length) + '</span></div>'
          + nfos.map(p => `
            <div class="issue-row" data-path="${esc(p)}">
              <span class="issue-row-path">${esc(p)}</span>
              <span class="issue-row-actions">
                <button type="button" class="btn-icon-danger" data-del-nfo="${esc(p)}" title="Delete NFO" aria-label="Delete NFO"><i class="fa-solid fa-xmark" aria-hidden="true"></i></button>
              </span>
            </div>`).join('')
        );
      }
      listEl.innerHTML = sections.join('');
      listEl.querySelectorAll('[data-orphan-remove]').forEach(btn => {
        btn.onclick = () => removeOrphan(btn.getAttribute('data-orphan-remove'), btn);
      });
      listEl.querySelectorAll('[data-del-nfo]').forEach(btn => {
        btn.onclick = () => deleteNfo(btn.getAttribute('data-del-nfo'), btn);
      });
      const resolveBtn = document.getElementById('btnResolveAllOrphans');
      if (resolveBtn) resolveBtn.onclick = () => { void resolveAllOrphansScoped(); };
      const removeBtn = document.getElementById('btnRemoveAllOrphans');
      if (removeBtn) removeBtn.onclick = () => { void removeAllOrphansScoped(orphans.slice()); };
      const delAllBtn = document.getElementById('btnDeleteAllNfoMerged');
      if (delAllBtn) delAllBtn.onclick = () => deleteAllNfo(nfos.slice(), delAllBtn);
      return;
    }

    // ── Merged: Missing Metadata ─────────────────────────────────
    // Combines series folders missing tvshow.nfo + individual videos
    // missing NFO sidecars. Each sub-list keeps its own per-row action.
    if (key === 'missing_metadata') {
      const rData = _healthResults || {};
      const folders = filterListPaths(rData.folder_no_tvshow_nfo || []);
      const videos  = filterListPaths(rData.video_no_nfo || []);
      if (!folders.length && !videos.length) {
        listEl.innerHTML = '<div class="empty">No missing metadata in this scope.</div>';
        return;
      }
      const sections = [];
      if (folders.length) {
        sections.push(
          '<div class="issue-subhead">Missing Series Metadata (folders without tvshow.nfo) <span class="issue-subhead-count">' + fmtCount(folders.length) + '</span></div>'
          + folders.map(p => `
            <div class="issue-row" data-path="${esc(p)}">
              <span class="issue-row-path">${esc(p)}</span>
              <span class="issue-row-actions">
                <button type="button" class="btn-tiny btn-play-tvshow" data-tvshow-nfo="${esc(p)}" title="Write NFO"><i class="fa-solid fa-play"></i></button>
              </span>
            </div>`).join('')
        );
      }
      if (videos.length) {
        sections.push(
          '<div class="issue-subhead">'
          + 'Missing Video Metadata (videos without NFO sidecar) '
          + '<span class="issue-subhead-count">' + fmtCount(videos.length) + '</span> '
          + '<button type="button" class="btn-tiny" id="btnAutoAllLibVideo" title="Auto-match every row: phash → DB lookup, fall back to a minimal NFO parsed from the filename" style="margin-left:8px">Auto all</button>'
          + '</div>'
          + videos.map(p => `
            <div class="issue-row" data-path="${esc(p)}">
              <span class="issue-row-path">${esc(p)}</span>
              <span class="issue-row-actions">
                <button type="button" class="btn-tiny btn-auto-lib-video" data-lib-auto="${esc(p)}" title="Auto: phash → minimal NFO from filename" aria-label="Auto match">Auto</button>
                <button type="button" class="btn-icon-lib-search" data-lib-match="${esc(p)}" title="Match metadata" aria-label="Match metadata"><i class="fa-solid fa-magnifying-glass" aria-hidden="true"></i></button>
              </span>
            </div>`).join('')
        );
      }
      listEl.innerHTML = sections.join('');
      listEl.querySelectorAll('[data-tvshow-nfo]').forEach(btn => {
        btn.onclick = () => applyTvshowNfoFromIndex(btn.getAttribute('data-tvshow-nfo'), btn);
      });
      listEl.querySelectorAll('[data-lib-match]').forEach(btn => {
        btn.onclick = () => openLibVideoMatch(btn.getAttribute('data-lib-match'));
      });
      listEl.querySelectorAll('[data-lib-auto]').forEach(btn => {
        btn.onclick = () => autoMatchLibVideo(btn.getAttribute('data-lib-auto'), btn);
      });
      const autoAllBtn = document.getElementById('btnAutoAllLibVideo');
      if (autoAllBtn) {
        autoAllBtn.onclick = () => { void autoMatchAllLibVideos(videos.slice(), autoAllBtn); };
      }
      return;
    }

    if (key === 'orphaned') {
      const list = orphanScoped();
      if (!list.length) {
        listEl.innerHTML = '<div class="empty">No orphaned records in this scope.</div>';
        return;
      }
      headAct.style.display = '';
      headAct.innerHTML = ''
        + '<button type="button" class="btn-tiny" id="btnResolveAllOrphans" title="Resolve any files that exist under a different extension" style="margin-right:6px">Resolve all</button>'
        + '<button type="button" class="btn-icon-danger btn-head-bulk-delete" id="btnRemoveAllOrphans" title="Remove all" aria-label="Remove all from history"><i class="fa-solid fa-xmark" aria-hidden="true"></i></button>';
      document.getElementById('btnResolveAllOrphans').onclick = () => { void resolveAllOrphansScoped(); };
      document.getElementById('btnRemoveAllOrphans').onclick = () => { void removeAllOrphansScoped(list.slice()); };
      listEl.innerHTML = list.map(o => `
        <div class="issue-row issue-row--orphan" data-orphan-id="${o.id}">
          <div class="issue-row-orphan-main">
            <div class="issue-row-path">${esc(o.filename || '')}</div>
            <div style="font-size:10px;color:var(--muted)">${esc(o.destination)}</div>
          </div>
          <span class="issue-row-actions issue-row-actions--end">
            <button type="button" class="btn-icon-danger" data-orphan-remove="${o.id}" title="Remove" aria-label="Remove from history"><i class="fa-solid fa-xmark" aria-hidden="true"></i></button>
          </span>
        </div>`).join('');
      listEl.querySelectorAll('[data-orphan-remove]').forEach(btn => {
        btn.onclick = () => removeOrphan(btn.getAttribute('data-orphan-remove'), btn);
      });
      return;
    }

    const results = _healthResults || {};
    const items = filterListPaths(results[key] || []);
    if (!items.length) {
      listEl.innerHTML = '<div class="empty">No issues in this category for the current scope.</div>';
      return;
    }

    if (key === 'nfo_no_video') {
      if (items.length) {
        headAct.style.display = '';
        headAct.innerHTML = '<button type="button" class="btn-icon-danger btn-head-bulk-delete" id="btnDeleteAllNfo" title="Delete orphans" aria-label="Delete all orphaned NFO files"><i class="fa-solid fa-xmark" aria-hidden="true"></i></button>';
        const btnAll = document.getElementById('btnDeleteAllNfo');
        const pathsSnapshot = items.slice();
        btnAll.onclick = () => deleteAllNfo(pathsSnapshot, btnAll);
      }
      listEl.innerHTML = items.map(p => `
        <div class="issue-row" data-path="${esc(p)}">
          <span class="issue-row-path">${esc(p)}</span>
          <span class="issue-row-actions">
            <button type="button" class="btn-icon-danger" data-del-nfo="${esc(p)}" title="Delete NFO" aria-label="Delete NFO"><i class="fa-solid fa-xmark" aria-hidden="true"></i></button>
          </span>
        </div>`).join('');
      listEl.querySelectorAll('[data-del-nfo]').forEach(btn => {
        btn.onclick = () => deleteNfo(btn.getAttribute('data-del-nfo'), btn);
      });
      return;
    }

    if (key === 'video_no_nfo') {
      if (items.length) {
        headAct.style.display = '';
        headAct.innerHTML = '<button type="button" class="btn-tiny" id="btnAutoAllLibVideoFlat" title="Auto-match every row: phash → DB lookup, fall back to a minimal NFO parsed from the filename">Auto all</button>';
        const autoAll = document.getElementById('btnAutoAllLibVideoFlat');
        const snapshot = items.slice();
        autoAll.onclick = () => { void autoMatchAllLibVideos(snapshot, autoAll); };
      }
      listEl.innerHTML = items.map(p => `
        <div class="issue-row" data-path="${esc(p)}">
          <span class="issue-row-path">${esc(p)}</span>
          <span class="issue-row-actions">
            <button type="button" class="btn-tiny btn-auto-lib-video" data-lib-auto="${esc(p)}" title="Auto: phash → minimal NFO from filename" aria-label="Auto match">Auto</button>
            <button type="button" class="btn-icon-lib-search" data-lib-match="${esc(p)}" title="Match metadata" aria-label="Match metadata"><i class="fa-solid fa-magnifying-glass" aria-hidden="true"></i></button>
          </span>
        </div>`).join('');
      listEl.querySelectorAll('[data-lib-match]').forEach(btn => {
        btn.onclick = () => openLibVideoMatch(btn.getAttribute('data-lib-match'));
      });
      listEl.querySelectorAll('[data-lib-auto]').forEach(btn => {
        btn.onclick = () => autoMatchLibVideo(btn.getAttribute('data-lib-auto'), btn);
      });
      return;
    }

    if (key === 'folder_no_tvshow_nfo') {
      if (items.length) {
        headAct.style.display = '';
        headAct.innerHTML = '<button type="button" class="btn-tiny btn-play-tvshow" id="btnFixAllTvshow" title="Write all NFOs"><i class="fa-solid fa-play"></i></button>';
        const btnAll = document.getElementById('btnFixAllTvshow');
        const pathsSnapshot = items.slice();
        btnAll.onclick = () => fixAllTvshowNfo(pathsSnapshot, btnAll);
      }
      listEl.innerHTML = items.map(p => `
        <div class="issue-row" data-path="${esc(p)}">
          <span class="issue-row-path">${esc(p)}</span>
          <span class="issue-row-actions">
            <button type="button" class="btn-tiny btn-play-tvshow" data-tvshow-nfo="${esc(p)}" title="Write NFO"><i class="fa-solid fa-play"></i></button>
          </span>
        </div>`).join('');
      listEl.querySelectorAll('[data-tvshow-nfo]').forEach(btn => {
        btn.onclick = () => applyTvshowNfoFromIndex(btn.getAttribute('data-tvshow-nfo'), btn);
      });
      return;
    }

    if (key === 'empty_folder') {
      if (items.length) {
        headAct.style.display = '';
        headAct.innerHTML = '<button type="button" class="btn-icon-danger btn-head-bulk-delete" id="btnDeleteAllEmpty" title="Delete empties" aria-label="Delete all empty folders"><i class="fa-solid fa-xmark" aria-hidden="true"></i></button>';
        const btnAllEmpty = document.getElementById('btnDeleteAllEmpty');
        const pathsEmptySnapshot = items.slice();
        btnAllEmpty.onclick = () => deleteAllEmptyFolders(pathsEmptySnapshot, btnAllEmpty);
      }
      listEl.innerHTML = items.map(p => `
        <div class="issue-row" data-path="${esc(p)}">
          <span class="issue-row-path">${esc(p)}</span>
          <span class="issue-row-actions">
            <button type="button" class="btn-icon-danger" data-rmdir="${esc(p)}" title="Delete" aria-label="Delete empty folder"><i class="fa-solid fa-xmark" aria-hidden="true"></i></button>
          </span>
        </div>`).join('');
      listEl.querySelectorAll('[data-rmdir]').forEach(btn => {
        btn.onclick = () => deleteFolder(btn.getAttribute('data-rmdir'), btn);
      });
    }
  }

  async function deleteNfo(p, btn) {
    const r = await fetch('/api/health/delete-nfo', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ path: p }) });
    if (!r.ok) return;
    btn.closest('.issue-row').classList.add('faded');
    stripFromResults('nfo_no_video', p);
    refreshStatNumbers();
  }

  const _xMarkIcon = '<i class="fa-solid fa-xmark" aria-hidden="true"></i>';

  async function deleteAllNfo(paths, btn) {
    if (btn.dataset.step === '1') {
      btn.dataset.step = '';
      btn.innerHTML = _xMarkIcon;
      btn.disabled = true;
      const r = await fetch('/api/health/delete-nfo', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ paths }),
      });
      let data = {};
      try { data = await r.json(); } catch (e) { /* ignore */ }
      btn.disabled = false;
      for (const p of (data.deleted || [])) {
        stripFromResults('nfo_no_video', p);
      }
      if ((data.errors || []).length) {
        window.toast('Some deletes failed:\n' + data.errors.map(e => (e.path || '') + ': ' + (e.error || '')).join('\n'));
      }
      refreshStatNumbers();
      renderDetail();
    } else {
      btn.dataset.step = '1';
      btn.innerHTML = '<span class="btn-confirm-mark">?</span>';
    }
  }

  async function deleteAllEmptyFolders(paths, btn) {
    if (btn.dataset.step === '1') {
      btn.dataset.step = '';
      btn.innerHTML = _xMarkIcon;
      btn.disabled = true;
      const errors = [];
      for (const folder of paths) {
        const r = await fetch('/api/health/delete-folder', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ path: folder }),
        });
        let data = {};
        try { data = await r.json(); } catch (e) { /* ignore */ }
        if (!r.ok) {
          errors.push((folder || '') + ': ' + (data.error || r.statusText || 'failed'));
        } else {
          stripFromResults('empty_folder', folder);
        }
      }
      btn.disabled = false;
      if (errors.length) {
        window.toast('Some deletes failed:\n' + errors.join('\n'));
      }
      refreshStatNumbers();
      renderDetail();
    } else {
      btn.dataset.step = '1';
      btn.innerHTML = '<span class="btn-confirm-mark">?</span>';
    }
  }

  let _libMatchPath = '';
  let _libThumbDataUrl = null;

  function closeLibVideoMatch() {
    document.getElementById('libMatchModal').classList.remove('open');
    document.getElementById('libMatchModal').setAttribute('aria-hidden', 'true');
    _libMatchPath = '';
    _libThumbDataUrl = null;
    document.getElementById('libMatchResults').innerHTML = '<div class="empty" style="padding:14px">Run phash match or search, then choose a row.</div>';
    document.getElementById('libPhashStatus').textContent = '';
    document.getElementById('libMThumbWrap').style.display = 'none';
  }

  async function openLibVideoMatch(absPath) {
    _libMatchPath = absPath || '';
    _libThumbDataUrl = null;
    document.getElementById('libMatchPathLabel').textContent = _libMatchPath;
    document.getElementById('libMatchModal').classList.add('open');
    document.getElementById('libMatchModal').setAttribute('aria-hidden', 'false');
    ['libSrchTerm','libSrchTitle','libSrchPerformer','libSrchStudio','libSrchDateFrom','libSrchDateTo'].forEach(id => {
      document.getElementById(id).value = '';
    });
    document.getElementById('libMTitle').value = '';
    document.getElementById('libMStudio').value = '';
    document.getElementById('libMDate').value = '';
    document.getElementById('libMPerformers').value = '';
    document.getElementById('libMImageUrl').value = '';
    document.getElementById('libMThumbWrap').style.display = 'none';
    document.getElementById('libMatchResults').innerHTML = '<div class="empty" style="padding:14px">Run phash match or search, then choose a row.</div>';
    document.getElementById('libPhashStatus').textContent = '';
    const base = (_libMatchPath.split('/').pop() || '').trim();
    if (!base) return;
    try {
      const r = await fetch('/api/parse/filename?filename=' + encodeURIComponent(base));
      const d = await r.json();
      if (d.title) document.getElementById('libSrchTitle').value = d.title;
      if (d.site) document.getElementById('libSrchStudio').value = d.site;
      if (d.performers) document.getElementById('libSrchPerformer').value = d.performers;
      if (d.date) document.getElementById('libSrchDateFrom').value = d.date;
      if (!d.title && !d.site && !d.performers) {
        const stem = base.replace(/\.[^.]+$/, '').replace(/[._]/g, ' ');
        document.getElementById('libSrchTerm').value = stem;
      }
      if (d.title) document.getElementById('libMTitle').value = d.title;
      if (d.site) document.getElementById('libMStudio').value = d.site;
      if (d.date) document.getElementById('libMDate').value = d.date;
      if (d.performers) document.getElementById('libMPerformers').value = d.performers;
    } catch (e) { /* ignore */ }
  }

  function libSceneViewUrl(row) {
    if (!row || !row.id || !row.source) return '';
    const s = row.source;
    if (s === 'ThePornDB' || s === 'TPDB') return 'https://theporndb.net/scenes/' + row.id;
    if (s === 'FansDB') return 'https://fansdb.cc/scenes/' + row.id;
    return 'https://stashdb.org/scenes/' + row.id;
  }

  function renderLibMatchTable(rows, pickKind) {
    const el = document.getElementById('libMatchResults');
    if (!rows.length) {
      el.innerHTML = '<div class="empty" style="padding:14px">No results.</div>';
      return;
    }
    window._libPickKind = pickKind;
    window._libMatchRows = rows;
    el.innerHTML = `
      <table>
        <thead><tr>
          <th></th><th>Title</th><th>Source</th><th>Studio</th><th>Stars</th><th>Date</th><th>Link</th><th></th>
        </tr></thead>
        <tbody>
          ${rows.map((row, idx) => {
            // Highlight matched performer + studio words inside the
            // scene title. Same `.qs-match` accent treatment used by
            // search results, so the user can spot at a glance which
            // names actually appear in the indexed title.
            const _hl = (typeof _qsBuildHighlightSet === 'function')
              ? _qsBuildHighlightSet(row.performers, row.studio)
              : null;
            const _titleHtml = (_hl && _hl.size && typeof _qsHighlight === 'function')
              ? _qsHighlight(row.title, _hl) : esc(row.title);
            const _studioHtml = (_hl && _hl.size && typeof _qsHighlight === 'function')
              ? _qsHighlight(row.studio, _hl) : esc(row.studio);
            return `
            <tr>
              <td style="width:72px">${row.image
                ? `<img src="${esc(row.image)}" alt="" loading="lazy" decoding="async" style="width:64px;height:64px;object-fit:cover;border-radius:4px" onerror="this.style.visibility='hidden'">`
                : ''}</td>
              <td title="${esc(row.title)}">${_titleHtml}</td>
              <td><span style="color:var(--muted)">${esc(row.source)}</span></td>
              <td title="${esc(row.studio)}">${_studioHtml}</td>
              <td title="${esc(row.performers || '')}">${row.performers ? performerCsvHtml(row.performers) : '–'}</td>
              <td>${esc(row.date || '–')}</td>
              <td>${row.id && row.source ? `<a href="${esc(libSceneViewUrl(row))}" target="_blank" rel="noopener" style="color:var(--accent)">View ↗</a>` : '–'}</td>
              <td><button type="button" class="btn-tiny" style="border-color:var(--green);color:var(--green)" onclick="libPickMatchRow(${idx})">Use this</button></td>
            </tr>`;
          }).join('')}
        </tbody>
      </table>`;
    if (typeof enrichPerformerNames === 'function') {
      const tbl = document.querySelector('.lib-match-table tbody') || document.body;
      enrichPerformerNames(tbl);
    }
  }

  async function libPickMatchRow(idx) {
    const rows = window._libMatchRows || [];
    const row = rows[idx];
    if (!row || !row._raw) return;
    const src = row.source || 'Manual';
    await libApplyScene(row._raw, src);
  }

  async function libApplyScene(rawScene, source) {
    if (!_libMatchPath) return;
    const r = await fetch('/api/health/library-video/apply', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path: _libMatchPath, scene: rawScene, source: source || 'Manual' }),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok || d.error) {
      window.toast(d.error || 'Apply failed');
      return;
    }
    const donePath = _libMatchPath;
    closeLibVideoMatch();
    stripFromResults('video_no_nfo', donePath);
    refreshStatNumbers();
    renderDetail();
  }

  async function autoMatchLibVideo(absPath, btn) {
    if (!absPath) return null;
    if (btn) {
      btn.disabled = true;
      btn.textContent = '…';
    }
    let r, d = {};
    try {
      r = await fetch('/api/health/library-video/auto-match', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path: absPath }),
      });
      d = await r.json().catch(() => ({}));
    } catch (e) {
      if (btn) { btn.disabled = false; btn.textContent = 'Auto'; }
      window.toast('Auto match failed');
      return null;
    }
    if (!r.ok || d.error) {
      if (btn) { btn.disabled = false; btn.textContent = 'Auto'; }
      window.toast(d.error || 'Auto match failed');
      return null;
    }
    if (btn) {
      const row = btn.closest('.issue-row');
      if (row) row.classList.add('faded');
    }
    stripFromResults('video_no_nfo', absPath);
    refreshStatNumbers();
    return d;
  }

  async function autoMatchAllLibVideos(paths, btnAll) {
    const n = paths.length;
    if (!n) return;
    if (!confirm('Auto-match ' + n + ' file' + (n === 1 ? '' : 's') + '? Phash lookups first, then minimal NFOs from filename for anything not matched.')) return;
    if (btnAll) { btnAll.disabled = true; btnAll.textContent = 'Running…'; }
    let viaPhash = 0;
    let viaName = 0;
    let failed = 0;
    for (const p of paths) {
      const rowBtn = document.querySelector('[data-lib-auto="' + (window.CSS && CSS.escape ? CSS.escape(p) : p.replace(/"/g, '\\"')) + '"]');
      const d = await autoMatchLibVideo(p, rowBtn);
      if (!d) { failed++; continue; }
      if (d.via === 'phash') viaPhash++;
      else if (d.via === 'filename') viaName++;
    }
    if (btnAll) { btnAll.disabled = false; btnAll.textContent = 'Auto all'; }
    const parts = [];
    if (viaPhash) parts.push(viaPhash + ' matched via phash');
    if (viaName)  parts.push(viaName + ' filename-only NFO' + (viaName === 1 ? '' : 's'));
    if (failed)   parts.push(failed + ' failed');
    window.toast(parts.length ? parts.join(' • ') : 'Nothing to do');
    renderDetail();
  }

  async function runLibPhashMatch() {
    if (!_libMatchPath) return;
    const st = document.getElementById('libPhashStatus');
    const btn = document.getElementById('libBtnPhash');
    st.textContent = 'Computing / querying…';
    btn.disabled = true;
    try {
      const r = await fetch('/api/health/library-video/phash-match', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path: _libMatchPath }),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok || d.error) {
        st.textContent = d.error || 'Failed';
        btn.disabled = false;
        return;
      }
      st.textContent = d.results && d.results.length
        ? ('Phash ' + (d.phash || '') + ' · ' + d.results.length + ' match(es)')
        : ('Phash ' + (d.phash || '') + ' · no match');
      renderLibMatchTable(d.results || [], 'phash');
    } catch (e) {
      st.textContent = String(e.message || e);
    }
    btn.disabled = false;
  }

  async function runLibSearch() {
    const title = document.getElementById('libSrchTitle').value.trim();
    const performer = document.getElementById('libSrchPerformer').value.trim();
    const studio = document.getElementById('libSrchStudio').value.trim();
    const termRaw = document.getElementById('libSrchTerm').value.trim();
    const term = (title || performer || studio) ? '' : termRaw;
    const payload = {
      term,
      title,
      performer,
      studio,
      date_from: document.getElementById('libSrchDateFrom').value.trim(),
      date_to: document.getElementById('libSrchDateTo').value.trim(),
    };
    document.getElementById('libMatchResults').innerHTML = '<div class="empty" style="padding:14px">Searching…</div>';
    try {
      const r = await fetch('/api/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const d = await r.json().catch(() => ({}));
      if (d.error) {
        document.getElementById('libMatchResults').innerHTML = '<div class="empty" style="padding:14px">' + esc(d.error) + '</div>';
        return;
      }
      renderLibMatchTable(d.results || [], 'search');
    } catch (e) {
      document.getElementById('libMatchResults').innerHTML = '<div class="empty" style="padding:14px">Search failed.</div>';
    }
  }

  async function libGenerateThumb() {
    if (!_libMatchPath) return;
    document.getElementById('libMImageUrl').value = '';
    const r = await fetch('/api/thumb/generate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path: _libMatchPath }),
    });
    const d = await r.json().catch(() => ({}));
    if (d.error) { window.toast(d.error); return; }
    _libThumbDataUrl = d.data_url;
    document.getElementById('libMThumbImg').src = d.data_url;
    document.getElementById('libMThumbWrap').style.display = 'block';
    document.getElementById('libMThumbNote').textContent = d.percent != null ? ('Frame at ' + d.percent + '%') : '';
  }

  async function libApplyManual() {
    if (!_libMatchPath) return;
    const title = document.getElementById('libMTitle').value.trim();
    const studio = document.getElementById('libMStudio').value.trim();
    const date = document.getElementById('libMDate').value.trim();
    const performers = document.getElementById('libMPerformers').value.trim();
    const image_url = document.getElementById('libMImageUrl').value.trim();
    const payload = {
      path: _libMatchPath,
      title,
      studio,
      date,
      performers,
      image_url,
    };
    if (_libThumbDataUrl) payload.thumb_data_url = _libThumbDataUrl;
    if (!title || !studio || !date) {
      window.toast('Title, studio, and date are required.');
      return;
    }
    const r = await fetch('/api/health/library-video/apply', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok || d.error) {
      window.toast(d.error || 'Apply failed');
      return;
    }
    const donePath = _libMatchPath;
    closeLibVideoMatch();
    stripFromResults('video_no_nfo', donePath);
    refreshStatNumbers();
    renderDetail();
  }

  document.getElementById('libMatchModal').addEventListener('click', function (ev) {
    if (ev.target === this) closeLibVideoMatch();
  });

  let _dupCompareReviewMode = false;

  function closeDupCompareModal() {
    _dupReviewQueue.reset();
    _dupCompareCurrent = null;
    _dupCompareReviewMode = false;
    const m = document.getElementById('dupCompareModal');
    m.classList.remove('open');
    m.setAttribute('aria-hidden', 'true');
    document.getElementById('dupCompareList').innerHTML = '';
    const ka = document.getElementById('dupKeepAll');
    if (ka) ka.checked = false;
    const prog = document.getElementById('dupCompareProgress');
    if (prog) prog.textContent = '';
  }

  function dupReviewAfterAction() {
    if (!_dupCompareReviewMode) {
      closeDupCompareModal();
      return;
    }
    _dupReviewQueue.index++;
    if (_dupReviewQueue.index < _dupReviewQueue.queue.length) {
      openDupCompareModal(_dupReviewQueue.queue[_dupReviewQueue.index], true);
      return;
    }
    if (_dupReviewQueue.skippedList.length) {
      const prog = document.getElementById('dupCompareProgress');
      if (prog) prog.textContent = _dupReviewQueue.skippedList.length + ' skipped group(s) remaining';
      _dupReviewQueue.queue = _dupReviewQueue.skippedList.slice();
      _dupReviewQueue.skippedList = [];
      _dupReviewQueue.index = 0;
      openDupCompareModal(_dupReviewQueue.queue[0], true);
      return;
    }
    closeDupCompareModal();
  }

  function onDupCompareSkip() {
    if (_dupCompareCurrent) _dupReviewQueue.skippedList.push(_dupCompareCurrent);
    dupReviewAfterAction();
  }

  function dupModalStatsLine(f) {
    const parts = [];
    const sz = f.size_bytes;
    if (sz != null && Number.isFinite(Number(sz))) parts.push(fmtSize(Number(sz)));
    else parts.push('—');
    const w = f.media_width, h = f.media_height;
    if (w != null && h != null && Number.isFinite(Number(w)) && Number.isFinite(Number(h))) {
      parts.push(Number(w) + '×' + Number(h));
    } else parts.push('—');
    parts.push((f.media_codec || '').trim() || '—');
    return parts.join(' · ');
  }

  async function loadDupModalSideData(fi, dest) {
    const enc = encodeURIComponent(dest);
    const frameIds = [0, 1, 2];
    try {
      const [fr, db] = await Promise.all([
        fetch('/api/health/duplicate-frames?path=' + enc).then(r => r.json()),
        fetch('/api/health/duplicate-db-record?path=' + enc).then(r => r.json()),
      ]);
      const frames = (fr && fr.frames) ? fr.frames : [];
      frameIds.forEach(function (j) {
        const slot = document.getElementById('dupFrame-' + fi + '-' + j);
        if (!slot) return;
        const url = frames[j];
        if (url) {
          slot.innerHTML = '<img src="' + String(url).replace(/"/g, '&quot;') + '" alt="" loading="lazy">';
        } else {
          slot.innerHTML = '<span style="font-size:10px;color:var(--muted)">—</span>';
        }
      });
      const dbEl = document.getElementById('dupDb-' + fi);
      if (dbEl) {
        if (!db || db.source == null) {
          dbEl.innerHTML = 'DB: <span style="color:var(--muted)">No linked filing record</span>';
        } else {
          const perfs = Array.isArray(db.performers) ? db.performers.join(', ') : '';
          const src = String(db.source || '').toUpperCase();
          dbEl.innerHTML =
            'DB: <strong>' + esc(src) + '</strong> match<br>' +
            '<span style="color:var(--text)">"' + esc(db.title || '') + '"</span><br>' +
            esc((db.studio || '') + ' · ' + (db.date || '')) + '<br>' +
            esc(perfs || '—');
        }
      }
    } catch (e) {
      frameIds.forEach(function (j) {
        const slot = document.getElementById('dupFrame-' + fi + '-' + j);
        if (slot) slot.innerHTML = '<span>—</span>';
      });
      const dbEl = document.getElementById('dupDb-' + fi);
      if (dbEl) dbEl.textContent = 'Could not load side data.';
    }
  }

  async function dupModalRecreateMetadata(path, sourceRecordId, btn) {
    btn.disabled = true;
    try {
      const r = await fetch('/api/health/duplicate-apply-metadata', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path: path, source_record_id: sourceRecordId }),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok || !d.ok) {
        window.toast(d.error || 'Failed');
        btn.disabled = false;
        return;
      }
      btn.textContent = '✓ Done';
      if (_dupCompareCurrent && d.new_path) {
        const cur = _dupCompareCurrent;
        (cur.files || []).forEach(function (f) {
          if (f.destination === path) {
            f.destination = d.new_path;
            f.current_filename = d.new_filename || f.current_filename;
          }
        });
      }
      await loadDuplicates();
      if (_dupCompareCurrent) {
        const ng = (_duplicateGroups || []).find(function (g) {
          return g.phash === _dupCompareCurrent.phash;
        });
        if (ng) _dupCompareCurrent = ng;
      }
      if (_selectedMetric === 'duplicates') await renderDetail();
    } catch (e) {
      window.toast(String(e.message || e));
      btn.disabled = false;
    }
  }

  async function onDupKeepRecommendedClose() {
    const g = _dupCompareCurrent;
    if (!g) return;
    const recPath = dupRecommendedPath(g);
    if (!recPath) {
      window.toast('No recommendation for this group.');
      return;
    }
    const del = (g.files || []).map(f => f.destination).filter(p => p && p !== recPath);
    try {
      const r = await fetch('/api/health/duplicates/resolve-one', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ keep_path: recPath, delete_paths: del }),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok || d.error) {
        window.toast(d.error || 'Resolve failed');
        return;
      }
      const fb = Number(d.freed_bytes) || 0;
      _dupFreedBytesTotal += fb;
      await loadDuplicates();
      refreshStatNumbers();
      if (_dupCompareReviewMode) dupReviewAfterAction();
      else closeDupCompareModal();
      if (_selectedMetric === 'duplicates') await renderDetail();
    } catch (e) {
      window.toast(String(e.message || e));
    }
  }

  async function dupModalDeleteOne(btn) {
    const xMark = '<i class="fa-solid fa-xmark" aria-hidden="true"></i>';
    if (btn.dataset.step === '1') {
      btn.dataset.step = '';
      btn.innerHTML = xMark;
      let p = '';
      try { p = decodeURIComponent(btn.getAttribute('data-dup-modal-del') || ''); } catch (e) { return; }
      if (!p) return;
      const r = await fetch('/api/health/delete-video', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path: p }),
      });
      if (!r.ok) {
        let err = 'Delete failed';
        try {
          const d = await r.json();
          err = d.error || err;
        } catch (e) { /* ignore */ }
        window.toast(err);
        return;
      }
      await loadDuplicates();
      refreshStatNumbers();
      await renderDetail();
      const cur = _dupCompareCurrent;
      if (!cur) return;
      cur.files = (cur.files || []).filter(function (f) { return String((f.destination || '').trim()) !== p; });
      if (cur.files.length < 2) {
        if (_dupCompareReviewMode) dupReviewAfterAction();
        else closeDupCompareModal();
      } else {
        openDupCompareModal(cur, _dupCompareReviewMode);
      }
    } else {
      btn.dataset.step = '1';
      btn.innerHTML = '<span class="btn-confirm-mark">?</span>';
    }
  }

  function openDupCompareModal(group, reviewMode) {
    _dupCompareCurrent = group;
    _dupCompareReviewMode = !!reviewMode;
    const m = document.getElementById('dupCompareModal');
    const meta = document.getElementById('dupCompareMeta');
    const progEl = document.getElementById('dupCompareProgress');
    if (meta) meta.textContent = 'phash: ' + ((group && group.phash) ? group.phash : '—');
    if (progEl && _dupCompareReviewMode && (_dupReviewQueue.queue || []).length) {
      progEl.textContent = 'Group ' + (_dupReviewQueue.index + 1) + ' of ' + _dupReviewQueue.queue.length;
    } else if (progEl) progEl.textContent = '';

    const files = (group.files || []).filter(f => pathInScope(f.destination));
    const recPath = dupRecommendedPath(group);
    const needRevModal = dupGroupNeedsReview(group);
    const listEl = document.getElementById('dupCompareList');
    listEl.innerHTML = '<div class="dup-modal-grid">' + files.map(function (f, fi) {
      const p = (f.destination || '').trim();
      const enc = encodeURIComponent(p);
      const fn = (f.current_filename && String(f.current_filename).trim()) ? String(f.current_filename).trim() : (p.split(/[/\\]/).pop() || p);
      const isRec = !needRevModal && !!(recPath && p === recPath);
      const sid = f.source_record_id != null ? Number(f.source_record_id) : null;
      const cardClass = 'dup-modal-file' + (isRec ? ' dup-modal-file--rec' : '');
      const recMark = isRec
        ? '<span class="dup-modal-recmark" title="Recommended keeper">' + dupWaPair('circle-plus', 'fa-circle-plus') + '</span>'
        : '<span class="dup-modal-recmark dup-modal-recmark--drop" title="Not recommended">' + dupWaPair('circle-minus', 'fa-circle-minus') + '</span>';
      const diskLine = f.on_disk
        ? '<span class="dup-row-diskmark" title="On disk">' + dupWaPair('floppy-disk', 'fa-floppy-disk') + '</span>'
        : '<span style="color:var(--amber);font-weight:400;font-size:12px" title="Missing on disk">(missing)</span>';
      const canMeta = sid != null && sid > 0;
      return `<div class="${cardClass}" data-dup-panel-idx="${fi}">
        <div class="dup-frame-row" id="dupFrames-${fi}">` +
        [0, 1, 2].map(function (j) {
          return `<div class="dup-frame-slot" id="dupFrame-${fi}-${j}"><span style="opacity:0.6">…</span></div>`;
        }).join('') +
        `</div>
        <div style="font-weight:600;font-size:13px;margin-bottom:6px;display:flex;align-items:center;gap:8px;flex-wrap:wrap">
          ${recMark}<span>${esc(fn)}</span>${diskLine}
        </div>
        <div style="font-size:11px;color:var(--dim)">${esc(dupModalStatsLine(f))}</div>
        <div style="font-size:10px;color:var(--muted);margin-top:4px;word-break:break-all">${esc(p)}</div>
        <div class="dup-db-block" id="dupDb-${fi}">Loading record…</div>
        <div style="margin-top:10px;display:flex;flex-wrap:wrap;gap:6px;align-items:center">
          <button type="button" class="btn-tiny" data-dup-rec-meta="${fi}" ${canMeta ? '' : 'disabled'}>Recreate metadata</button>
          <button type="button" class="btn-dup-row-delete" data-dup-modal-del="${enc}" title="Delete"><i class="fa-solid fa-xmark"></i></button>
        </div>
      </div>`;
    }).join('') + '</div>';

    listEl.querySelectorAll('[data-dup-modal-del]').forEach(function (btn) {
      btn.onclick = function () { void dupModalDeleteOne(btn); };
    });
    listEl.querySelectorAll('[data-dup-rec-meta]').forEach(function (btn) {
      const fi = parseInt(btn.getAttribute('data-dup-rec-meta') || '-1', 10);
      const f = files[fi];
      if (!f || f.source_record_id == null) return;
      btn.onclick = function () { void dupModalRecreateMetadata(f.destination, Number(f.source_record_id), btn); };
    });

    files.forEach(function (f, fi) {
      void loadDupModalSideData(fi, f.destination);
    });

    const keepAll = document.getElementById('dupKeepAll');
    if (keepAll) {
      keepAll.checked = false;
      keepAll.onchange = function () {
        if (keepAll.checked && _dupCompareReviewMode) {
          keepAll.checked = false;
          dupReviewAfterAction();
        }
      };
    }

    const keepRecBtn = document.getElementById('dupCompareKeepRecBtn');
    if (keepRecBtn) {
      const igModal = dupGroupIgnored(group);
      keepRecBtn.style.display = igModal ? 'none' : '';
      keepRecBtn.disabled = !!igModal;
    }

    m.classList.add('open');
    m.setAttribute('aria-hidden', 'false');
  }

  function onDupCompareBackdropCancel() {
    closeDupCompareModal();
  }

  document.getElementById('dupCompareModal').addEventListener('click', function (ev) {
    if (ev.target === this) onDupCompareBackdropCancel();
  });
  document.getElementById('dupCompareCloseBtn').onclick = onDupCompareBackdropCancel;
  document.getElementById('dupCompareSkipBtn').onclick = onDupCompareSkip;
  document.getElementById('dupCompareKeepRecBtn').onclick = () => { void onDupKeepRecommendedClose(); };

  /** Escape closes the topmost health overlay (highest z-index first). */
  function healthDismissTopEscapeLayer() {
    const tree = document.getElementById('treeLeafLookupOverlay');
    if (tree && tree.style.display === 'flex') {
      closeTreeLeafLookup();
      return true;
    }
    const perfAlias = document.getElementById('perfAliasModal');
    if (perfAlias && perfAlias.classList.contains('open')) {
      closePerfAliasModal();
      return true;
    }
    const perfSearch = document.getElementById('perfSearchModal');
    if (perfSearch && perfSearch.classList.contains('open')) {
      closePerfSearchModal();
      return true;
    }
    const movie = document.getElementById('movieMatchModal');
    if (movie && movie.classList.contains('open')) {
      movCloseMatchModal();
      return true;
    }
    const dup = document.getElementById('dupCompareModal');
    if (dup && dup.classList.contains('open')) {
      onDupCompareBackdropCancel();
      return true;
    }
    const lib = document.getElementById('libMatchModal');
    if (lib && lib.classList.contains('open')) {
      closeLibVideoMatch();
      return true;
    }
    const about = document.getElementById('aboutModal');
    if (about && about.classList.contains('open')) {
      closeAboutModal();
      return true;
    }
    const clean = document.getElementById('cleanPanel');
    if (clean && clean.classList.contains('open')) {
      clean.classList.remove('open');
      return true;
    }
    return false;
  }

  document.addEventListener('keydown', function healthEscapeStack(ev) {
    if (ev.key !== 'Escape') return;
    if (healthDismissTopEscapeLayer()) {
      ev.preventDefault();
      ev.stopPropagation();
    }
  });

  async function applyTvshowNfoFromIndex(folder, btn) {
    btn.disabled = true;
    btn.classList.add('is-running');
    let r, d = {};
    try {
      r = await fetch('/api/health/create-tvshow-nfo', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path: folder }),
      });
      try { d = await r.json(); } catch (e) { /* ignore */ }
    } catch (netErr) {
      btn.disabled = false;
      btn.classList.remove('is-running');
      window.toast('Network error: ' + (netErr.message || netErr), { kind: 'error' });
      return;
    }
    btn.disabled = false;
    btn.classList.remove('is-running');
    if (!r.ok) {
      // Show the backend's actual reason as a red toast so the user
      // can see why the write failed (common: folder isn't matched
      // under Favourites yet, no TPDB/StashDB/FansDB IDs stored).
      window.toast(d.error || ('Failed to write tvshow.nfo (HTTP ' + r.status + ')'), { kind: 'error' });
      return;
    }
    const row = btn.closest('.issue-row');
    if (row) row.classList.add('faded');
    stripFromResults('folder_no_tvshow_nfo', folder);
    refreshStatNumbers();
    window.toast('tvshow.nfo written for ' + folder, { kind: 'success' });
  }

  async function fixAllTvshowNfo(paths, btn) {
    const playHtml = '<i class="fa-solid fa-play"></i>';
    if (btn.dataset.step === '1') {
      btn.dataset.step = '';
      btn.innerHTML = playHtml;
      btn.disabled = true;
      btn.classList.add('is-running');
      const r = await fetch('/api/health/create-tvshow-nfo', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ paths }),
      });
      let data = {};
      try { data = await r.json(); } catch (e) { /* ignore */ }
      btn.disabled = false;
      btn.classList.remove('is-running');
      for (const p of (data.written || [])) {
        stripFromResults('folder_no_tvshow_nfo', p);
      }
      if ((data.errors || []).length) {
        window.toast('Some folders failed:\n' + data.errors.map(e => (e.path || '') + ': ' + (e.error || '')).join('\n'));
      }
      refreshStatNumbers();
      renderDetail();
    } else {
      btn.dataset.step = '1';
      btn.innerHTML = '<span class="btn-confirm-mark">?</span>';
    }
  }

  async function deleteFolder(folder, btn) {
    const xMark = '<i class="fa-solid fa-xmark" aria-hidden="true"></i>';
    if (btn.dataset.step === '1') {
      btn.dataset.step = '';
      btn.innerHTML = xMark;
      const r = await fetch('/api/health/delete-folder', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ path: folder }) });
      if (!r.ok) return;
      btn.closest('.issue-row').classList.add('faded');
      stripFromResults('empty_folder', folder);
      refreshStatNumbers();
    } else {
      btn.dataset.step = '1';
      btn.innerHTML = '<span class="btn-confirm-mark">?</span>';
    }
  }

  function dupGroupShowDeleteRank(group, targetPath) {
    group.querySelectorAll('[data-dup-del]').forEach(b => {
      let p = '';
      try { p = decodeURIComponent(b.getAttribute('data-dup-del') || ''); } catch (e) { return; }
      if (p === targetPath) {
        b.className = 'btn-dup-rank-remove';
        b.innerHTML = '<i class="fa-solid fa-thumbs-down" aria-hidden="true"></i>';
        b.title = 'Removing this file';
        b.setAttribute('aria-label', 'Remove');
      } else {
        b.className = 'btn-dup-rank-keep';
        b.innerHTML = '<i class="fa-solid fa-thumbs-up" aria-hidden="true"></i>';
        b.title = 'Keeping';
        b.setAttribute('aria-label', 'Keep');
      }
    });
    group.classList.add('dup-delete-pending');
  }

  async function handleDupRowDelete(dest, btn) {
    const group = btn.closest('.dup-group');
    if (!group || group.dataset.dupDeleting === '1') return;
    group.dataset.dupDeleting = '1';
    dupGroupShowDeleteRank(group, dest);
    await new Promise(function (resolve) { setTimeout(resolve, 320); });
    function dupRestoreDeleteButtons() {
      group.classList.remove('dup-delete-pending');
      group.querySelectorAll('[data-dup-del]').forEach(b => {
        b.className = 'btn-icon-danger';
        b.innerHTML = '<i class="fa-solid fa-xmark" aria-hidden="true"></i>';
        b.title = 'Delete video';
        b.setAttribute('aria-label', 'Delete video');
      });
    }
    try {
      const r = await fetch('/api/health/delete-video', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path: dest }),
      });
      if (!r.ok) {
        dupRestoreDeleteButtons();
        return;
      }
      await loadDuplicates();
      refreshStatNumbers();
      await renderDetail();
    } catch (e) {
      dupRestoreDeleteButtons();
    } finally {
      delete group.dataset.dupDeleting;
    }
  }

  async function removeOrphan(id, btn) {
    const r = await fetch('/api/health/orphaned/' + encodeURIComponent(id), { method: 'DELETE' });
    if (!r.ok) return;
    btn.closest('.issue-row').classList.add('faded');
    _orphanList = (_orphanList || []).filter(o => String(o.id) !== String(id));
    refreshStatNumbers();
  }

  async function resolveAllOrphansScoped() {
    let r, d = {};
    try {
      r = await fetch('/api/health/orphaned/resolve-all', { method: 'POST' });
      d = await r.json();
    } catch (e) {
      window.toast('Resolve all failed');
      return;
    }
    if (!r.ok) {
      window.toast(d.error || 'Resolve all failed');
      return;
    }
    const n = Number(d.resolved || 0);
    const left = Number(d.remaining || 0);
    if (n) {
      window.toast('Resolved ' + n + ' orphan' + (n === 1 ? '' : 's') + (left ? ' — ' + left + ' still missing' : ''));
    } else {
      window.toast('No orphans had a same-stem file on disk');
    }
    await loadOrphans();
    refreshStatNumbers();
    renderDetail();
  }

  async function removeAllOrphansScoped(snapshot) {
    const n = snapshot.length;
    if (!n) return;
    if (!confirm('Remove ' + n + ' filing history record(s) for these missing paths? This cannot be undone.')) return;
    const ids = snapshot.map(o => o.id);
    const r = await fetch('/api/health/orphaned/clear', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ids }),
    });
    let d = {};
    try { d = await r.json(); } catch (e) { /* ignore */ }
    if (!r.ok) {
      window.toast(d.error || 'Failed to remove records');
      return;
    }
    await loadOrphans();
    refreshStatNumbers();
    renderDetail();
  }

  function stripFromResults(metric, pathStr) {
    if (!_healthResults || !_healthResults[metric]) return;
    _healthResults[metric] = _healthResults[metric].filter(p => p !== pathStr);
  }

  async function loadDuplicates() {
    const r = await fetch('/api/health/duplicates');
    const d = await r.json();
    _duplicateGroups = d.groups || [];
  }

  async function loadOrphans() {
    const r = await fetch('/api/health/orphaned');
    const d = await r.json();
    _orphanList = d.orphans || [];
  }

  async function loadLibraryStats() {
    const r = await fetch('/api/health/library-stats');
    _libraryStats = await r.json();
  }

  function pollIndexStatus() {
    if (_indexPoll) clearInterval(_indexPoll);
    _indexPoll = setInterval(() => {
      if (document.visibilityState !== 'visible') return;
      void (async () => {
        let d = { index: {}, phash3: {} };
        try {
          const r = await fetch('/api/health/index-status');
          d = await r.json();
        } catch (e) { /* ignore */ }
        const ix = d.index || {};
        const p3 = d.phash3 || {};
        if (window.TsActivity && window.TsActivity.refresh) window.TsActivity.refresh();
        if (_indexWasRunning && !ix.running && !p3.running && _indexPoll) {
          clearInterval(_indexPoll);
          _indexPoll = null;
          _indexWasRunning = false;
          await loadLibraryStats();
          await loadDuplicates();
          refreshStatNumbers();
          await refreshHealthProgress();
        }
      })();
    }, 900);
  }

  async function startIndexLibrary() {
    _indexWasRunning = true;
    await fetch('/api/health/index-library', { method: 'POST' });
    await refreshHealthProgress();
    pollIndexStatus();
  }

  async function startPhash3Rescan() {
    _indexWasRunning = true;
    await fetch('/api/health/phash3-rescan', { method: 'POST' });
    await refreshHealthProgress();
    pollIndexStatus();
  }

  async function startScanFolders() {
    const btn = document.getElementById('btnScanFolders');
    btn.disabled = true;
    try {
      const r = await fetch('/api/favourites/scan?only_missing=true', { method: 'POST' });
      const d = await r.json();
      if (d.started && window.TsActivity && TsActivity.refresh) TsActivity.refresh();
    } catch(e) {
      window.toast('Error: ' + e.message);
    } finally {
      setTimeout(() => { btn.disabled = false; }, 3000);
    }
  }

  async function startScan() {
    if (scanning) return;
    scanning = true;
    if (window.TsActivity && window.TsActivity.setHealthScanning) window.TsActivity.setHealthScanning(true);
    document.getElementById('btnScan').disabled = true;
    await refreshHealthProgress();
    await fetch('/api/library/scan');
    pollResults();
  }

  function pollResults() {
    const t = setInterval(async () => {
      if (document.visibilityState !== 'visible') return;
      await refreshHealthProgress();
      const r = await fetch('/api/library/results');
      const d = await r.json();
      if (d.results) {
        clearInterval(t);
        scanning = false;
        if (window.TsActivity && window.TsActivity.setHealthScanning) window.TsActivity.setHealthScanning(false);
        document.getElementById('btnScan').disabled = false;
        if (d.roots) _healthRoots = d.roots;
        validateScopePath();
        renderScopePills();
        await renderResults(d.results, d.scan_time);
        await refreshHealthProgress();
      }
    }, 2000);
  }

  async function renderResults(results, scanTime) {
    if (scanTime) _lastScanTime = scanTime;
    updateHealthActionTitles();
    _healthResults = results;
    if (!METRIC_ORDER.includes(_selectedMetric)) _selectedMetric = METRIC_ORDER[0];
    refreshStatNumbers();
    const r = _healthResults;
    let sel = _selectedMetric;
    if (['nfo_no_video','video_no_nfo','folder_no_tvshow_nfo','empty_folder'].includes(sel) && !filterListPaths(r[sel] || []).length) {
      sel = pickDefaultMetric(r);
    }
    _selectedMetric = sel;
    await setSelectedMetric(sel);
  }

  document.getElementById('statsBar').addEventListener('click', (e) => {
    const card = e.target.closest('[data-metric]');
    if (!card) return;
    void setSelectedMetric(card.getAttribute('data-metric'));
  });

  const HEALTH_STATS_HINT_LS = 'topShelfHealthStatsHintSeen';

  function dismissHealthStatsHint() {
    const el = document.getElementById('healthStatsHint');
    if (el) el.hidden = true;
    try { localStorage.setItem(HEALTH_STATS_HINT_LS, '1'); } catch (e) { /* ignore */ }
  }

  function initHealthStatsHint() {
    const el = document.getElementById('healthStatsHint');
    if (!el) return;
    try {
      if (localStorage.getItem(HEALTH_STATS_HINT_LS) === '1') return;
    } catch (e) { /* ignore */ }
    el.hidden = false;
    const btn = document.getElementById('healthStatsHintDismiss');
    if (btn && !btn.dataset.wired) {
      btn.dataset.wired = '1';
      btn.addEventListener('click', dismissHealthStatsHint);
    }
  }

  (async function init() {
    initHealthStatsHint();
    updateHealthActionTitles();
    // Boot fetches: kick all 6 endpoints in parallel and await the
    // bundle so render only blocks on the slowest. Previously the two
    // sequential awaits (/api/library/results + /api/health/index-status)
    // each added a full round-trip after the Promise.all completed.
    const [_, __, ___, ____, resultsRes, indexStatus] = await Promise.all([
      loadLibraryStats(),
      loadLibraryDiskUsage(),
      loadDuplicates(),
      loadOrphans(),
      fetch('/api/library/results').then(r => r.json()).catch(() => ({})),
      refreshHealthProgress(),
    ]);
    const d = resultsRes || {};
    if (d.roots) _healthRoots = d.roots;
    else if (_libraryStats && _libraryStats.roots) _healthRoots = _libraryStats.roots;
    loadScopePreference();
    validateScopePath();
    renderScopePills();
    if (d.results) await renderResults(d.results, d.scan_time);
    else {
      if (d.scan_time) _lastScanTime = d.scan_time;
      updateHealthActionTitles();
      _healthResults = null;
      refreshStatNumbers();
      _selectedMetric = 'total_videos';
      await setSelectedMetric('total_videos');
    }
    // refreshHealthProgress already ran in the boot Promise.all above —
    // reuse its result instead of issuing a duplicate fetch.
    if ((indexStatus.index || {}).running || (indexStatus.phash3 || {}).running) {
      pollIndexStatus();
    }
    let _healthStatResizeTimer = null;
    window.addEventListener('resize', () => {
      clearTimeout(_healthStatResizeTimer);
      _healthStatResizeTimer = setTimeout(() => scheduleFitHealthStatCounterValues(), 80);
    });
    if (document.fonts && document.fonts.ready) {
      document.fonts.ready.then(() => scheduleFitHealthStatCounterValues());
    }
  })();

  async function toggleCleanPanelOpen() {
    const p = document.getElementById('cleanPanel');
    const open = !p.classList.contains('open');
    p.classList.toggle('open', open);
    if (open) {
      const r = await fetch('/api/health/clean-preview');
      const d = r.ok ? await r.json() : { removed_count: '?' };
      document.getElementById('cleanPanelText').innerHTML =
        'This will delete <strong>' + esc(String(d.removed_count)) + '</strong> removed-index (is_removed) library_files row(s).';
    }
  }

  document.getElementById('btnCleanLib').onclick = toggleCleanPanelOpen;

  async function confirmCleanLibrary() {
    const purge = document.getElementById('cleanPurgeHistory').checked;
    const r = await fetch('/api/health/clean-library', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ purge_history: purge }) });
    const d = await r.json();
    document.getElementById('cleanPanel').classList.remove('open');
    window.toast('Cleaned: library_files ' + (d.library_files_deleted || 0) + ', processed_files ' + (d.processed_files_deleted || 0));
    await loadLibraryStats();
    await loadLibraryDiskUsage();
    refreshStatNumbers();
  }
  window.confirmCleanLibrary = confirmCleanLibrary;

  // ================================================================
  // PERFORMER ENRICHMENT PANEL
  // ================================================================
  let _perfAllPerformers = [];
  let _perfFilterMode = 'all';
  let _perfProgressPoll = null;
  let _perfLoaded = false;

  const _EMB_LOGOS = { tpdb: 'tpdb', stashdb: 'stashdb', fansdb: 'fansdb', iafd: 'iafd', babepedia: 'babepedia', coomer: 'coomer', tmdb: 'tmdb', freeones: 'freeones' };
  function embSrcLogo(key, alt) {
    const k = (key || '').toLowerCase();
    return _EMB_LOGOS[k]
      ? `<img class="emb-src-logo" src="/static/logos/${_EMB_LOGOS[k]}.webp" alt="${alt || key}">`
      : (alt || key);
  }

  async function perfLoadStatus() {
    _perfLoaded = true;
    const tbody = document.getElementById('perfTableBody');
    tbody.innerHTML = '<tr><td colspan="10" class="emb-empty">Loading…</td></tr>';
    try {
      const r = await fetch('/api/performers/enrichment-status');
      const data = await r.json();
      _perfAllPerformers = data.performers || [];
      perfUpdateSummary(_perfAllPerformers);
      perfApplyFilter();
      // Update the stat tile count
      document.getElementById('statPerformers').textContent = fmtCount(_perfAllPerformers.length);
    } catch(e) {
      tbody.innerHTML = '<tr><td colspan="10" class="emb-empty" style="color:var(--red)">Failed to load.</td></tr>';
    }
  }

  function perfUpdateSummary(rows) {
    const n = rows.length;
    const hs = rows.filter(r => r.has_headshot).length;
    const tmdb = rows.filter(r => r.tmdb_id).length;
    const iafd = rows.filter(r => r.iafd_url).length;
    const fo = rows.filter(r => r.freeones_url).length;
    const co = rows.filter(r => r.coomer_url).length;
    const tpdb = rows.filter(r => r.tpdb_id).length;
    const sdb = rows.filter(r => r.stashdb_id).length;
    const fdb = rows.filter(r => r.fansdb_id).length;
    const pct = (v) => n ? Math.round(v / n * 100) + '%' : '—';
    document.getElementById('perfSumTotal').textContent = fmtCount(n);
    document.getElementById('perfSumHeadshot').textContent = fmtCount(hs);
    document.getElementById('perfSumHeadshotPct').textContent = pct(hs);
    document.getElementById('perfSumTmdb').textContent = fmtCount(tmdb);
    document.getElementById('perfSumTmdbPct').textContent = pct(tmdb);
    document.getElementById('perfSumIafd').textContent = fmtCount(iafd);
    document.getElementById('perfSumIafdPct').textContent = pct(iafd);
    document.getElementById('perfSumFreeones').textContent = fmtCount(fo);
    document.getElementById('perfSumFreeonessPct').textContent = pct(fo);
    document.getElementById('perfSumCoomer').textContent = fmtCount(co);
    document.getElementById('perfSumCoomerPct').textContent = pct(co);
    document.getElementById('perfSumTpdb').textContent = fmtCount(tpdb);
    document.getElementById('perfSumStashdb').textContent = fmtCount(sdb);
    document.getElementById('perfSumFansdb').textContent = fmtCount(fdb);
    const hsEl = document.getElementById('perfSumHeadshot');
    // `sky-counter` preserved here (and on the studio replacements below)
    // so the MutationObserver + fitSkyCounter keeps applying — a
    // className replace without it would strip the counter styling.
    hsEl.className = 'emb-summary-value sky-counter ' + (n && hs === n ? 'green' : hs < n * 0.5 ? 'amber' : '');
  }

  function perfSetFilter(mode) {
    _perfFilterMode = mode;
    ['all','missing','nohead','noids'].forEach(m => {
      const id = 'perfFt' + m.charAt(0).toUpperCase() + m.slice(1);
      document.getElementById(id)?.classList.toggle('active', m === mode);
    });
    perfApplyFilter();
  }

  // Incrementing guard so superseded renders (user retypes in the
  // filter, switches panels, etc.) abandon their remaining chunks
  // instead of racing the new render.
  let _perfRenderToken = 0;

  function perfApplyFilter() {
    const q = (document.getElementById('perfFilterQ').value || '').toLowerCase();
    const tbody = document.getElementById('perfTableBody');
    if (!_perfAllPerformers.length) return;
    const rows = _perfAllPerformers.filter(p => {
      if (q && !p.name.toLowerCase().includes(q)) return false;
      if (_perfFilterMode === 'missing') return !p.tmdb_id || !p.iafd_url || !p.freeones_url || !p.coomer_url;
      if (_perfFilterMode === 'nohead') return !p.has_headshot;
      if (_perfFilterMode === 'noids') return !p.tpdb_id && !p.stashdb_id && !p.fansdb_id;
      return true;
    });
    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="10" class="emb-empty">No stars match this filter.</td></tr>';
      return;
    }
    // Chunked render — 1000+ performer rows parsed as a single
    // innerHTML string blocks the main thread for seconds (the HTML
    // parser + layout + style recalc all land in one frame). Breaking
    // the work into 80-row chunks yielded across animation frames
    // keeps the UI responsive; the first chunk appears immediately
    // so the table isn't empty during the bulk insert.
    _perfRenderToken++;
    const token = _perfRenderToken;
    tbody.innerHTML = '';
    let i = 0;
    const CHUNK = 80;
    function step() {
      if (token !== _perfRenderToken) return;  // superseded
      const end = Math.min(i + CHUNK, rows.length);
      let html = '';
      for (let k = i; k < end; k++) html += perfRenderRow(rows[k]);
      tbody.insertAdjacentHTML('beforeend', html);
      i = end;
      if (i < rows.length) requestAnimationFrame(step);
    }
    step();
  }

  function perfRenderRow(p) {
    const idPill = (id, label, profileUrl, logoKey) => id
      ? `<span class="emb-link-pill has">
           <a href="${esc(profileUrl)}" target="_blank" rel="noopener">${embSrcLogo(logoKey, label)}</a>
           <button class="emb-pill-remove" onclick="perfClearLink(${p.id},'${label.toUpperCase()}',this)" title="Remove">✕</button>
         </span>`
      : `<span class="emb-link-pill missing">—</span>`;

    const extPill = (url, label, site) => url
      ? `<span class="emb-link-pill has">
           <a href="${esc(url)}" target="_blank" rel="noopener">${embSrcLogo(site, label)}</a>
           <button class="emb-pill-remove" onclick="perfClearLink(${p.id},'${site.toUpperCase()}',this)" title="Remove">✕</button>
         </span>`
      : `<span class="emb-link-pill missing" onclick="perfManualLink(${p.id},'${site}','${esc(p.name)}')">${embSrcLogo(site, label) || label}</span>`;

    const groupBaseUrl = { tpdb: 'https://theporndb.net/performers/', stashdb: 'https://stashdb.org/performers/', fansdb: 'https://fansdb.cc/performers/' };
    const groupExtraPill = (site, extId) => `<span class="emb-link-pill has">
        <a href="${esc(groupBaseUrl[site] + extId)}" target="_blank" rel="noopener">${embSrcLogo(site, site)}</a>
        <button class="emb-pill-remove" onclick="perfGroupRemoveLink(${p.id},'${site}','${esc(extId)}',this)" title="Remove">✕</button>
      </span>`;
    const groupAddBtn = (site) => `<button class="emb-link-pill emb-link-pill-add" onclick="perfGroupAddLink(${p.id},'${site}','${esc(p.name)}')" title="Add additional ${site.toUpperCase()} link"><i class="fa-solid fa-plus"></i></button>`;
    const sourceCell = (primaryHtml, site) => {
      if (!p.is_group) return primaryHtml;
      const extras = ((p.group_ids || {})[site] || []).map(id => groupExtraPill(site, id)).join('');
      return `<div class="emb-group-cell">${primaryHtml}${extras}${groupAddBtn(site)}</div>`;
    };

    const tpdbUrl = p.tpdb_id ? `https://theporndb.net/performers/${p.tpdb_id}` : null;
    const stashUrl = p.stashdb_id ? `https://stashdb.org/performers/${p.stashdb_id}` : null;
    const fansUrl = p.fansdb_id ? `https://fansdb.cc/performers/${p.fansdb_id}` : null;

    const hsContent = p.has_headshot
      ? `<img class="emb-hs-thumb" src="/api/performers/iafd-image?row_id=${p.id}&i=0" alt="" loading="lazy" decoding="async" onerror="this.outerHTML='<div class=\\'emb-hs-missing\\'><i class=\\'fa-solid fa-user\\'></i></div>'">`
      : `<div class="emb-hs-missing"><i class="fa-solid fa-user"></i></div>`;

    const hsCell = p.has_headshot
      ? `<span class="emb-hs-wrap">${hsContent}<button class="emb-hs-remove" onclick="perfClearHeadshot(${p.id},this)" title="Remove headshot">✕</button></span>`
      : hsContent;

    const hasAnyId = p.tpdb_id || p.stashdb_id || p.fansdb_id;
    const missingExt = !p.tmdb_id || !p.iafd_url || !p.freeones_url || !p.coomer_url;
    const canFetch = hasAnyId && missingExt;

    const groupBtnCls = p.is_group ? 'emb-group-toggle active' : 'emb-group-toggle';
    const groupBtnTitle = p.is_group ? 'Group folder (multiple performers) — click to disable' : 'Mark as group folder (multiple performers)';

    // Path-missing warning — mirrors the /library card overlay. Shown
    // when the row's stored folder path no longer exists on disk
    // (library scanner sets favourite_entities.path_missing = 1).
    const pathMissingBadge = p.path_missing
      ? '<span class="emb-path-missing" title="Missing on disk" aria-label="Folder path missing on disk"><i class="fa-solid fa-circle-exclamation"></i></span>'
      : '';
    // Library folder path — shown on a second line under the name so
    // you can see at a glance where each performer is stored. Amber
    // when path_missing, dim when present.
    const pathLine = p.path
      ? `<span class="emb-name-path${p.path_missing ? ' is-missing' : ''}" title="${esc(p.path)}">${esc(p.path)}</span>`
      : '';
    // Router weight badge. Effective weight = manual override (1-10) if
    // set, else 2 when favourited, else 1. We paint the effective number
    // inside the star; the `.is-override` class marks a manual value so
    // defaults read as softer chrome.
    const manualWeight = (typeof p.weight === 'number' && p.weight >= 1 && p.weight <= 10) ? p.weight : null;
    const defaultWeight = p.is_favourite ? 2 : 1;
    const effWeight = manualWeight != null ? manualWeight : defaultWeight;
    const weightCls = manualWeight != null ? 'emb-weight-star is-override' : 'emb-weight-star';
    const weightTitle = manualWeight != null
      ? `Router weight ${manualWeight} (override). Click to change or clear.`
      : `Router weight ${effWeight} (default${p.is_favourite ? ' — favourited' : ''}). Click to override 1-10.`;
    const weightCell = `<span class="${weightCls}" onclick="perfSetWeight(${p.id},this)" title="${esc(weightTitle)}">
      <i class="fa-solid fa-star"></i><span class="emb-weight-num">${effWeight}</span>
    </span>`;
    return `<tr data-id="${p.id}"${p.path_missing ? ' class="is-path-missing"' : ''}>
      <td>
        <div class="emb-name-inner">
          ${hsCell}
          <div class="emb-name-text">
            <span class="emb-name-title">${esc(p.name)}${pathMissingBadge}</span>
            <div class="emb-name-meta">
              ${pathLine}
              <span class="emb-name-id">#${p.id}</span>
            </div>
          </div>
          <button class="${groupBtnCls}" onclick="perfToggleGroup(${p.id},${p.is_group ? 'false' : 'true'},this)" title="${groupBtnTitle}" aria-pressed="${p.is_group}"><i class="fa-solid fa-user-group"></i></button>
        </div>
      </td>
      <td style="text-align:center">${weightCell}</td>
      <td>${extPill(p.tmdb_url, 'TMDB', 'tmdb')}</td>
      <td>${sourceCell(idPill(p.tpdb_id, 'TPDB', tpdbUrl, 'tpdb'), 'tpdb')}</td>
      <td>${sourceCell(idPill(p.stashdb_id, 'StashDB', stashUrl, 'stashdb'), 'stashdb')}</td>
      <td>${sourceCell(idPill(p.fansdb_id, 'FansDB', fansUrl, 'fansdb'), 'fansdb')}</td>
      <td>${extPill(p.iafd_url, 'IAFD', 'iafd')}</td>
      <td>${extPill(p.freeones_url, 'Freeones', 'freeones')}</td>
      <td>${extPill(p.coomer_url, 'Coomer', 'coomer')}</td>
      <td style="white-space:nowrap">
        <button class="emb-action-btn" onclick="perfSearchMatch(${p.id},'${esc(p.name)}')" title="Search"><i class="fa-solid fa-magnifying-glass"></i></button>
        ${canFetch ? `<button class="emb-action-btn" id="perfFetchBtn-${p.id}" onclick="perfFetchExtLinks(${p.id},this)" title="Fetch ext links"><i class="fa-solid fa-link"></i></button>` : ''}
        ${!p.has_headshot ? `<button class="emb-action-btn" id="perfHsBtn-${p.id}" onclick="perfFetchHeadshot(${p.id},this)" title="Fetch headshot"><i class="fa-solid fa-image"></i></button>` : ''}
        <button class="emb-action-btn${(p.aliases || []).length ? ' has-aliases' : ''}" onclick="openPerfAliases(${p.id})" title="Manage aliases${(p.aliases || []).length ? ' (' + p.aliases.length + ')' : ''}"><i class="fa-solid fa-tags"></i></button>
        <button class="emb-action-btn emb-action-btn--danger" onclick="healthRemoveRow(${p.id},'performer','${esc(p.name)}',this)" title="Remove"><i class="fa-solid fa-trash"></i></button>
      </td>
    </tr>`;
  }

  async function healthRemoveRow(rowId, kind, name, btn) {
    if (!rowId) return;
    const label = kind === 'movie' ? 'movie' : kind === 'jav' ? 'JAV release' : kind === 'studio' ? 'studio' : 'performer';
    if (!confirm(`Remove ${label} "${name}" from the database?\n\nFiles on disk are not deleted; a later library scan can add it back.`)) return;
    if (btn) btn.disabled = true;
    try {
      const r = await fetch('/api/favourites/delete', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id: rowId }),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) { window.toast(d.error || 'Remove failed'); if (btn) btn.disabled = false; return; }
      if (kind === 'movie' || kind === 'jav') {
        _movAllMovies = _movAllMovies.filter(m => m.id !== rowId);
        refreshMoviesJavStatCounter();
        movApplyFilter();
      } else if (kind === 'studio') {
        _stuAllStudios = _stuAllStudios.filter(s => s.id !== rowId);
        document.getElementById('statStudios').textContent = fmtCount(_stuAllStudios.length);
        stuUpdateSummary(_stuAllStudios);
        stuApplyFilter();
      } else {
        _perfAllPerformers = _perfAllPerformers.filter(p => p.id !== rowId);
        perfApplyFilter();
        perfUpdateSummary(_perfAllPerformers);
      }
    } catch(e) {
      window.toast('Error: ' + (e.message || 'Remove failed'));
      if (btn) btn.disabled = false;
    }
  }

  async function perfToggleGroup(rowId, toGroup, btn) {
    btn.disabled = true;
    try {
      const r = await fetch('/api/favourites/set-group', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ row_id: rowId, is_group: toGroup }),
      });
      if (!r.ok) throw new Error('set-group failed');
      const p = _perfAllPerformers.find(x => x.id === rowId);
      if (p) { p.is_group = toGroup; if (!toGroup) p.group_ids = {tpdb:[],stashdb:[],fansdb:[]}; }
      perfApplyFilter();
    } catch(e) {
      window.toast('Error: ' + e.message);
    } finally {
      btn.disabled = false;
    }
  }

  function perfGroupAddLink(rowId, site, name) {
    perfSearchMatch(rowId, name, { groupSite: site });
  }

  async function perfGroupRemoveLink(rowId, site, extId, btn) {
    btn.disabled = true;
    try {
      const r = await fetch('/api/favourites/group-remove-link', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ row_id: rowId, source: site, ext_id: extId }),
      });
      const d = await r.json();
      if (!r.ok) throw new Error(d.error || 'remove failed');
      const p = _perfAllPerformers.find(x => x.id === rowId);
      if (p) p.group_ids = d.group_ids;
      perfApplyFilter();
    } catch(e) {
      window.toast('Error: ' + e.message);
      btn.disabled = false;
    }
  }

  // Apply the `stored` payload from /api/performers/fetch-ext-links
  // (and the equivalent fields from a manual-link write) to the
  // in-memory row so `perfApplyFilter()` re-renders just this row's
  // chrome — no full performer-list refetch.
  function _perfApplyStored(rowId, stored) {
    if (!stored) return;
    const p = _perfAllPerformers.find(x => x.id === rowId);
    if (!p) return;
    if ('tmdb' in stored) { p.tmdb_id = stored.tmdb; }
    if ('iafd' in stored)      p.iafd_url      = stored.iafd;
    if ('freeones' in stored)  p.freeones_url  = stored.freeones;
    if ('babepedia' in stored) p.babepedia_url = stored.babepedia;
    if ('coomer' in stored)    p.coomer_url    = stored.coomer;
    if ('javdatabase' in stored) p.javdatabase_url = stored.javdatabase;
    perfApplyFilter();
    perfUpdateSummary(_perfAllPerformers);
  }

  async function perfFetchExtLinks(rowId, btn) {
    btn.disabled = true; btn.classList.add('running'); btn.innerHTML = '…';
    try {
      const r = await fetch('/api/performers/fetch-ext-links', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({row_id:rowId})});
      const d = await r.json();
      btn.classList.remove('running');
      btn.innerHTML = (d.found && Object.keys(d.found).length) ? 'Done' : 'None';
      // The endpoint returns which links were stored; apply them to the
      // in-memory row so the row's pill chrome updates without the
      // full /api/performers/enrichment-status refetch the old
      // `setTimeout(perfLoadStatus, 800)` triggered.
      _perfApplyStored(rowId, d && d.stored);
    } catch(e) { btn.innerHTML = 'Err'; btn.classList.remove('running'); btn.disabled = false; }
  }

  async function perfFetchHeadshot(rowId, btn) {
    btn.disabled = true; btn.classList.add('running'); btn.innerHTML = '…';
    try {
      await fetch('/api/performers/enrich-headshot', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({row_id:rowId})});
      btn.innerHTML = 'Queued'; btn.classList.remove('running');
      // Headshot enrichment runs in a background thread on the server —
      // the row's `has_headshot` flag flips when that thread finishes,
      // not when this fetch resolves. The previous `setTimeout(perfLoadStatus, 8000)`
      // refetched the whole performer list once. Instead, flip the
      // local flag optimistically so the row drops its "Fetch headshot"
      // button immediately; the actual image URL will appear on the
      // next perfLoadStatus (manual refresh or tab re-enter).
      const p = _perfAllPerformers.find(x => x.id === rowId);
      if (p) { p.has_headshot = true; perfApplyFilter(); perfUpdateSummary(_perfAllPerformers); }
    } catch(e) { btn.innerHTML = 'Err'; btn.classList.remove('running'); btn.disabled = false; }
  }

  async function perfClearLink(rowId, site, btn) {
    btn.disabled = true;
    try {
      const r = await fetch('/api/favourites/clear-ext-link', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({row_id:rowId,site})});
      if (r.ok) {
        const p = _perfAllPerformers.find(x => x.id === rowId);
        if (p) {
          const key = site.toLowerCase();
          if (key === 'tmdb') { p.tmdb_id = null; p.tmdb_url = null; }
          else if (key === 'tpdb') p.tpdb_id = null;
          else if (key === 'stashdb') p.stashdb_id = null;
          else if (key === 'fansdb') p.fansdb_id = null;
          else p[key + '_url'] = null;
        }
        perfApplyFilter(); perfUpdateSummary(_perfAllPerformers);
      }
    } catch(e) { btn.disabled = false; }
  }

  async function perfClearHeadshot(rowId, btn) {
    btn.disabled = true;
    try {
      const r = await fetch('/api/performers/clear-headshot', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({row_id:rowId})});
      if (r.ok) {
        const p = _perfAllPerformers.find(x => x.id === rowId);
        if (p) p.has_headshot = false;
        perfApplyFilter(); perfUpdateSummary(_perfAllPerformers);
      }
    } catch(e) { btn.disabled = false; }
  }

  function perfManualLink(rowId, site, name) {
    const url = prompt(`Enter ${site} URL for "${name}":`, '');
    if (!url || !url.trim()) return;
    const cleanUrl = url.trim();
    fetch('/api/favourites/ext-link', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({row_id:rowId,site,url:cleanUrl})})
      .then(r => r.json()).then(d => {
        if (d && d.error) { window.toast && window.toast(d.error); return; }
        // Mutate the in-memory row's `{site}_url` field and let
        // perfApplyFilter() re-render just this row's chrome instead
        // of refetching the full performer list.
        const p = _perfAllPerformers.find(x => x.id === rowId);
        if (p) {
          const key = String(site).toLowerCase();
          if (key === 'tmdb') p.tmdb_id = cleanUrl;
          else p[key + '_url'] = cleanUrl;
          perfApplyFilter(); perfUpdateSummary(_perfAllPerformers);
        }
      }).catch(console.error);
  }

  /**
   * Router-weight override for the scene-filing picker. Null/empty/0
   * clears the override so the default (2 if favourite, 1 otherwise)
   * is used. Accepts 1..10; anything else is rejected. Posts to
   * /api/favourites/set-weight, then patches the in-memory row + rerenders.
   */
  async function perfSetWeight(rowId, star) {
    const p = _perfAllPerformers.find(x => x.id === rowId);
    if (!p) return;
    const cur = (typeof p.weight === 'number') ? String(p.weight) : '';
    const raw = prompt(`Router weight for "${p.name}" (1-10, blank to clear — default is ${p.is_favourite ? 2 : 1}):`, cur);
    if (raw === null) return; // cancelled
    const trimmed = raw.trim();
    let payload;
    if (trimmed === '') {
      payload = { id: rowId, weight: null };
    } else {
      const n = parseInt(trimmed, 10);
      if (!Number.isFinite(n) || n < 1 || n > 10) {
        window.toast('Weight must be an integer between 1 and 10, or blank to clear.');
        return;
      }
      payload = { id: rowId, weight: n };
    }
    if (star) star.style.opacity = '0.5';
    try {
      const r = await fetch('/api/favourites/set-weight', {
        method: 'POST',
        credentials: 'same-origin',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) { window.toast(d.error || 'Failed to save weight'); return; }
      p.weight = (typeof d.weight === 'number') ? d.weight : null;
      perfApplyFilter();
    } catch(e) {
      window.toast(e.message || 'Failed');
    } finally {
      if (star) star.style.opacity = '';
    }
  }

  /* ── Alias editor ───────────────────────────────────────────────────
   * Manages the aliases_json list on a performer folder. Used for
   * group folders (e.g. "Dellai Twins") so individual performer names
   * feeding in from scenes / RSS / movie credits ("Silvia Dellai")
   * resolve to the group's library row — wiring the headshot matcher
   * and RSS filter to cover the individual members without needing a
   * per-person folder.
   */
  let _perfAliasRowId = null;

  function openPerfAliases(rowId) {
    const p = _perfAllPerformers.find(x => x.id === rowId);
    if (!p) return;
    _perfAliasRowId = rowId;
    document.getElementById('perfAliasTitle').textContent = p.name || '';
    document.getElementById('perfAliasInput').value = '';
    _perfAliasRender(p.aliases || []);
    document.getElementById('perfAliasModal').classList.add('open');
    setTimeout(() => document.getElementById('perfAliasInput').focus(), 40);
  }

  function closePerfAliasModal() {
    document.getElementById('perfAliasModal').classList.remove('open');
    _perfAliasRowId = null;
  }

  function _perfAliasRender(aliases) {
    const list = document.getElementById('perfAliasList');
    if (!aliases || !aliases.length) {
      list.innerHTML = '<div class="emb-empty" style="width:100%">No aliases yet.</div>';
      return;
    }
    list.innerHTML = aliases.map((a, i) => `
      <span class="perf-alias-chip">
        ${esc(a)}
        <button class="perf-alias-chip-remove" onclick="perfAliasRemove(${i})" title="Remove"><i class="fa-solid fa-xmark"></i></button>
      </span>
    `).join('');
  }

  async function _perfAliasSave(aliases) {
    if (!_perfAliasRowId) return aliases;
    try {
      const r = await fetch('/api/performers/aliases', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ row_id: _perfAliasRowId, aliases }),
      });
      const d = await r.json();
      if (!r.ok) throw new Error(d.error || 'Save failed');
      const final = d.aliases || [];
      // Keep the in-memory performer record in sync so perfApplyFilter
      // reflects the change without a full re-fetch.
      const p = _perfAllPerformers.find(x => x.id === _perfAliasRowId);
      if (p) p.aliases = final;
      _perfAliasRender(final);
      // Re-render just this row's action cell so the button's
      // "has-aliases" highlight updates immediately.
      perfApplyFilter();
      return final;
    } catch (e) {
      window.toast('Could not save aliases: ' + (e.message || e));
      return aliases;
    }
  }

  function perfAliasAdd() {
    const input = document.getElementById('perfAliasInput');
    const v = (input.value || '').trim();
    if (!v) return;
    const p = _perfAllPerformers.find(x => x.id === _perfAliasRowId);
    const current = (p && p.aliases) ? [...p.aliases] : [];
    // Case-insensitive dedup; keep the casing the user typed.
    if (current.some(x => x.toLowerCase() === v.toLowerCase())) {
      input.value = '';
      return;
    }
    current.push(v);
    input.value = '';
    _perfAliasSave(current);
  }

  function perfAliasRemove(idx) {
    const p = _perfAllPerformers.find(x => x.id === _perfAliasRowId);
    if (!p || !p.aliases) return;
    const current = [...p.aliases];
    if (idx < 0 || idx >= current.length) return;
    current.splice(idx, 1);
    _perfAliasSave(current);
  }

  function _perfDisableJobBtns() {
    document.getElementById('perfBtnFillGaps').disabled = true;
    document.getElementById('perfBtnHeadshotsOnly').disabled = true;
  }
  function _perfEnableJobBtns() {
    document.getElementById('perfBtnFillGaps').disabled = false;
    document.getElementById('perfBtnHeadshotsOnly').disabled = false;
  }

  async function perfStartFillGaps() {
    _perfDisableJobBtns();
    try {
      const r = await fetch('/api/performers/enrich-ext-links-bulk', {method:'POST'});
      const d = await r.json();
      if (!d.started && d.reason !== 'already_running') {
        window.toast('Could not start: ' + (d.reason || 'unknown'));
        _perfEnableJobBtns(); return;
      }
      perfStartProgressPoll();
    } catch(e) { console.error(e); _perfEnableJobBtns(); }
  }

  async function perfStartHeadshotsOnly() {
    _perfDisableJobBtns();
    try {
      const r = await fetch('/api/performers/enrich-headshots-bulk', {method:'POST'});
      const d = await r.json();
      if (!d.started && d.reason !== 'already_running') {
        window.toast('Could not start: ' + (d.reason || 'unknown'));
        _perfEnableJobBtns(); return;
      }
      perfStartProgressPoll();
    } catch(e) { console.error(e); _perfEnableJobBtns(); }
  }

  function perfStartProgressPoll() {
    if (_perfProgressPoll) clearInterval(_perfProgressPoll);
    document.getElementById('perfProgressWrap').classList.add('visible');
    _perfProgressPoll = setInterval(perfPollProgress, 1500);
    perfPollProgress();
  }

  async function perfPollProgress() {
    if (document.visibilityState !== 'visible') return;
    try {
      const r = await fetch('/api/performers/enrichment-progress');
      const d = await r.json();
      const total = d.total || 0, done = d.done || 0;
      const pct = total ? Math.round(done / total * 100) : 0;
      document.getElementById('perfProgressFill').style.width = pct + '%';
      document.getElementById('perfProgressLabel').textContent = `${done} / ${total}`;
      document.getElementById('perfProgressPhase').textContent = d.phase === 'headshots' ? 'Headshots' : d.phase === 'links' ? 'Links' : '';
      document.getElementById('perfProgressCurrent').textContent = d.current ? d.current.replace(/^\[(links|headshot)\] /, '') : '';
      if (!d.running && done >= total && total > 0) {
        clearInterval(_perfProgressPoll); _perfProgressPoll = null;
        document.getElementById('perfProgressLabel').textContent = 'Done!';
        document.getElementById('perfProgressCurrent').textContent = '';
        _perfEnableJobBtns();
        setTimeout(() => { document.getElementById('perfProgressWrap').classList.remove('visible'); perfLoadStatus(); }, 2000);
      }
    } catch(e) { console.error(e); }
  }

  // Check if enrichment job already running on page load
  (async function perfCheckRunning() {
    try {
      const r = await fetch('/api/performers/enrichment-progress');
      const d = await r.json();
      if (d.running) { _perfDisableJobBtns(); perfStartProgressPoll(); }
    } catch(e) {}
  })();

  // ================================================================
  // STUDIO ENRICHMENT PANEL
  // ================================================================
  let _stuAllStudios = [];
  let _stuFilterMode = 'all';
  let _stuLoaded = false;

  async function stuLoadStatus() {
    _stuLoaded = true;
    const tbody = document.getElementById('stuTableBody');
    tbody.innerHTML = '<tr><td colspan="5" class="emb-empty">Loading…</td></tr>';
    try {
      const r = await fetch('/api/studios/enrichment-status');
      const data = await r.json();
      _stuAllStudios = data.studios || [];
      stuUpdateSummary(_stuAllStudios);
      stuApplyFilter();
      document.getElementById('statStudios').textContent = fmtCount(_stuAllStudios.length);
    } catch(e) {
      tbody.innerHTML = '<tr><td colspan="5" class="emb-empty" style="color:var(--red)">Failed to load.</td></tr>';
    }
  }

  let _stuFillGapsPoll = null;
  async function stuStartFillGaps() {
    const btn = document.getElementById('stuBtnFillGaps');
    btn.disabled = true;
    try {
      const r = await fetch('/api/studios/enrich-bulk', { method: 'POST' });
      const d = await r.json();
      if (!d.started) {
        if (d.reason === 'already_running') {
          // Fall through to polling so we pick up the in-progress job.
        } else {
          btn.disabled = false;
          return;
        }
      }
      // Poll progress until the backend reports not running, then reload
      // the status table so freshly-populated names show up.
      if (_stuFillGapsPoll) clearInterval(_stuFillGapsPoll);
      _stuFillGapsPoll = setInterval(async () => {
        if (document.visibilityState !== 'visible') return;
        try {
          const rp = await fetch('/api/studios/enrichment-progress');
          const dp = await rp.json();
          if (!dp.running && dp.total >= 0) {
            clearInterval(_stuFillGapsPoll);
            _stuFillGapsPoll = null;
            btn.disabled = false;
            await stuLoadStatus();
          }
        } catch(e) {}
      }, 1500);
    } catch(e) {
      btn.disabled = false;
    }
  }

  function stuUpdateSummary(rows) {
    const n = rows.length;
    const logos   = rows.filter(r => r.has_logo).length;
    const missing = rows.filter(r => r.path_missing).length;
    const tpdb    = rows.filter(r => r.tpdb_id).length;
    const sdb     = rows.filter(r => r.stashdb_id).length;
    const fdb     = rows.filter(r => r.fansdb_id).length;
    const pct = (v) => n ? Math.round(v / n * 100) + '%' : '—';
    document.getElementById('stuSumTotal').textContent       = fmtCount(n);
    document.getElementById('stuSumLogos').textContent       = fmtCount(logos);
    document.getElementById('stuSumLogosPct').textContent    = pct(logos);
    document.getElementById('stuSumPathMissing').textContent = fmtCount(missing);
    document.getElementById('stuSumTpdb').textContent        = fmtCount(tpdb);
    document.getElementById('stuSumTpdbPct').textContent     = pct(tpdb);
    document.getElementById('stuSumStashdb').textContent     = fmtCount(sdb);
    document.getElementById('stuSumStashdbPct').textContent  = pct(sdb);
    document.getElementById('stuSumFansdb').textContent      = fmtCount(fdb);
    document.getElementById('stuSumFansdbPct').textContent   = pct(fdb);
    const pmEl = document.getElementById('stuSumPathMissing');
    pmEl.className = 'emb-summary-value sky-counter ' + (missing ? 'amber' : 'green');
    const logoEl = document.getElementById('stuSumLogos');
    logoEl.className = 'emb-summary-value sky-counter ' + (n && logos === n ? 'green' : logos < n * 0.5 ? 'amber' : '');
  }

  function stuSetFilter(mode) {
    _stuFilterMode = mode;
    ['all','nologo','noids','missing'].forEach(m => {
      const id = 'stuFt' + m.charAt(0).toUpperCase() + m.slice(1);
      document.getElementById(id)?.classList.toggle('active', m === mode);
    });
    stuApplyFilter();
  }

  /* ── Logo library sub-view (under Studios → Logo library) ────────
   * Manages every row in the studio_logos table that has a file on
   * disk. Thumbnail uses /api/studio-logo?q=NAME so we don't need a
   * separate serving endpoint. Actions: refetch (from StashDB),
   * replace (upload a custom file), delete (removes row + file).
   * Uploads run through _normalise_studio_logo_to_png server-side so
   * everything on disk is 300px-wide transparent PNG regardless of
   * input format.
   */
  let _stuView = 'library';        // 'library' | 'logos'
  let _stuLogos = [];
  let _stuLogosLoaded = false;
  let _stuLogoReplaceId = null;
  let _stuLogoLetter = '';         // '' = all, 'A'..'Z' = that letter, '#' = non-alpha
  let _stuAlphaBuilt = false;      // populate A–Z once
  let _stuLogoPage = 0;            // 0-based
  let _stuLogoPageSize = 50;       // matches the <select> default

  function _stuLogoFirstLetter(row) {
    const s = (row.name || row.slug || '').trim();
    if (!s) return '#';
    const c = s[0].toUpperCase();
    return /[A-Z]/.test(c) ? c : '#';
  }

  function _stuLogoBuildAlphaBar() {
    if (_stuAlphaBuilt) return;
    const bar = document.getElementById('stuLogoAlpha');
    if (!bar) return;
    // Alphabet A–Z — inserted after the existing "All" + "#" buttons.
    const letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');
    const frag = document.createDocumentFragment();
    for (const L of letters) {
      const b = document.createElement('button');
      b.type = 'button';
      b.className = 'stu-alpha-btn';
      b.dataset.letter = L;
      b.textContent = L;
      b.onclick = () => stuLogoSetLetter(L);
      frag.appendChild(b);
    }
    bar.appendChild(frag);
    _stuAlphaBuilt = true;
  }

  function _stuLogoUpdateAlphaAvailability() {
    // Grey out letters with no logos under them so the filter bar
    // doubles as a density indicator. '#' stays lit if any row maps
    // there; 'All' is always clickable.
    const counts = { '#': 0 };
    for (let i = 0; i < 26; i++) counts[String.fromCharCode(65 + i)] = 0;
    for (const r of _stuLogos) {
      counts[_stuLogoFirstLetter(r)]++;
    }
    document.querySelectorAll('#stuLogoAlpha .stu-alpha-btn').forEach(btn => {
      const L = btn.dataset.letter || '';
      if (!L) return;  // 'All' button — skip
      const c = counts[L] || 0;
      btn.classList.toggle('is-empty', c === 0);
      btn.title = c > 0 ? `${c} logo${c === 1 ? '' : 's'}` : 'No logos';
    });
  }

  function stuLogoSetLetter(letter) {
    _stuLogoLetter = letter || '';
    _stuLogoPage = 0;  // filter change resets pagination
    document.querySelectorAll('#stuLogoAlpha .stu-alpha-btn').forEach(btn => {
      btn.classList.toggle('is-active', (btn.dataset.letter || '') === _stuLogoLetter);
    });
    stuLogoApplyFilter();
  }

  function stuLogoPagerStep(delta) {
    _stuLogoPage += delta;
    stuLogoApplyFilter();
  }
  function stuLogoPagerGoto(page) {
    _stuLogoPage = page;  // clamped inside stuLogoApplyFilter
    stuLogoApplyFilter();
  }
  function stuLogoPagerSetSize(size) {
    const n = parseInt(size, 10);
    if (!Number.isNaN(n) && n > 0) {
      _stuLogoPageSize = n;
      _stuLogoPage = 0;
      stuLogoApplyFilter();
    }
  }

  function setStuView(v) {
    _stuView = (v === 'logos') ? 'logos' : 'library';
    document.getElementById('stuEnrichPanel').style.display = _stuView === 'library' ? 'flex' : 'none';
    document.getElementById('stuLogoPanel').style.display = _stuView === 'logos' ? 'flex' : 'none';
    if (_stuView === 'logos' && !_stuLogosLoaded) stuLogoLoad();
    else if (_stuView === 'library' && !_stuLoaded) stuLoadStatus();
  }

  async function stuLogoLoad() {
    _stuLogoBuildAlphaBar();
    const grid = document.getElementById('stuLogoGrid');
    grid.innerHTML = '<div class="emb-empty">Loading…</div>';
    try {
      const r = await fetch('/api/studio-logos/saved');
      const d = await r.json();
      _stuLogos = d.rows || [];
      _stuLogosLoaded = true;
      _stuLogoUpdateAlphaAvailability();
      stuLogoApplyFilter();
    } catch (e) {
      grid.innerHTML = `<div class="emb-empty" style="color:var(--red)">Failed to load: ${esc(e.message || e)}</div>`;
    }
  }

  function stuLogoApplyFilter() {
    const qRaw = (document.getElementById('stuLogoFilterQ').value || '').toLowerCase().trim();
    const grid = document.getElementById('stuLogoGrid');
    const countEl = document.getElementById('stuLogoCount');
    const pager = document.getElementById('stuLogoPager');
    // Stage 1: text search
    let rows = qRaw
      ? _stuLogos.filter(r =>
          (r.name || '').toLowerCase().includes(qRaw) ||
          (r.slug || '').toLowerCase().includes(qRaw) ||
          (r.source || '').toLowerCase().includes(qRaw))
      : _stuLogos.slice();
    // Stage 2: letter filter (applied on top of text search)
    if (_stuLogoLetter) {
      rows = rows.filter(r => _stuLogoFirstLetter(r) === _stuLogoLetter);
    }
    const totalFiltered = rows.length;
    // Text/letter change always coalesces to page 0 via the callers
    // (setLetter / pagerSetSize / filter-input via _stuLogoFilterInput).
    // Clamp `_stuLogoPage` now so a stale value (e.g. user was on page 7
    // of 10 then filtered to 3 pages) doesn't render an empty slice.
    const pages = Math.max(1, Math.ceil(totalFiltered / _stuLogoPageSize));
    if (_stuLogoPage >= pages) _stuLogoPage = pages - 1;
    if (_stuLogoPage < 0) _stuLogoPage = 0;
    const start = _stuLogoPage * _stuLogoPageSize;
    const end = Math.min(start + _stuLogoPageSize, totalFiltered);
    const pageRows = rows.slice(start, end);

    if (countEl) {
      const filtered = (qRaw || _stuLogoLetter);
      if (!totalFiltered) {
        countEl.textContent = filtered ? `0 of ${fmtCount(_stuLogos.length)}` : `${fmtCount(_stuLogos.length)} logos`;
      } else {
        countEl.textContent = filtered
          ? `${fmtCount(start + 1)}–${fmtCount(end)} of ${fmtCount(totalFiltered)} (filtered from ${fmtCount(_stuLogos.length)})`
          : `${fmtCount(start + 1)}–${fmtCount(end)} of ${fmtCount(totalFiltered)}`;
      }
    }

    if (!totalFiltered) {
      grid.innerHTML = `<div class="emb-empty" style="grid-column:1/-1">${(qRaw || _stuLogoLetter) ? 'No logos match this filter.' : 'No logos saved yet — upload one or run Resync from Settings.'}</div>`;
      if (pager) pager.style.display = 'none';
      return;
    }

    // Render:
    //   - A specific letter is picked → flat grid (no headers needed,
    //     you're already in a single bucket).
    //   - "All" selected → insert section headers whenever the first
    //     letter changes within the current page's slice.
    if (_stuLogoLetter) {
      grid.innerHTML = pageRows.map(r => _stuLogoRenderCard(r)).join('');
    } else {
      const html = [];
      let currentLetter = null;
      for (const r of pageRows) {
        const L = _stuLogoFirstLetter(r);
        if (L !== currentLetter) {
          currentLetter = L;
          html.push(`<div class="stu-logo-section-head">${esc(L)}</div>`);
        }
        html.push(_stuLogoRenderCard(r));
      }
      grid.innerHTML = html.join('');
    }

    // Pager visibility + state. Hide when a single page covers
    // everything — no point showing 1-of-1 chrome.
    if (pager) {
      if (pages <= 1) {
        pager.style.display = 'none';
      } else {
        pager.style.display = 'flex';
        const statusEl = document.getElementById('stuLogoPagerStatus');
        if (statusEl) statusEl.textContent = `Page ${_stuLogoPage + 1} of ${pages}`;
        document.getElementById('stuLogoPagerFirst').disabled = _stuLogoPage === 0;
        document.getElementById('stuLogoPagerPrev').disabled = _stuLogoPage === 0;
        document.getElementById('stuLogoPagerNext').disabled = _stuLogoPage >= pages - 1;
        document.getElementById('stuLogoPagerLast').disabled = _stuLogoPage >= pages - 1;
      }
      // Pager's `goto(Infinity)` needs to resolve to last page.
      if (!Number.isFinite(_stuLogoPage)) _stuLogoPage = pages - 1;
    }
  }

  // Separate input handler so typing in the text filter resets the
  // page to 0 (otherwise a narrower filter can leave you on page 7
  // of the broader list).
  function _stuLogoFilterInput() {
    _stuLogoPage = 0;
    stuLogoApplyFilter();
  }

  function _stuLogoRenderCard(r) {
    // Cache-bust on the logo's last-fetched timestamp so a refetch or
    // replacement shows the new image without a hard page reload.
    const v = encodeURIComponent((r.last_fetched_at || r.created_at || '') + ':' + r.id);
    const src = `/api/studio-logo?q=${encodeURIComponent(r.name || r.slug)}&_v=${v}`;
    const srcLabel = r.source === 'upload' ? 'uploaded'
                   : r.source === 'stashdb' ? 'stashdb'
                   : r.source === 'manual'  ? 'manual drop'
                   : r.source === 'local-seed' ? 'seeded'
                   : (r.source || 'unknown');
    return `<div class="stu-logo-card" data-id="${r.id}">
      <div class="stu-logo-thumb"><img src="${src}" alt="${esc(r.name || r.slug)}" loading="lazy" onerror="this.style.display='none'"></div>
      <div class="stu-logo-meta">
        <div class="stu-logo-name" title="${esc(r.name || r.slug)}">${esc(r.name || r.slug)}</div>
        <div class="stu-logo-sub" title="slug: ${esc(r.slug)}">${esc(srcLabel)} · ${esc(r.slug)}</div>
      </div>
      <div class="stu-logo-actions">
        <button class="emb-action-btn" onclick="stuLogoRefetch(${r.id}, this)" title="Refetch"><i class="fa-solid fa-rotate"></i></button>
        <button class="emb-action-btn" onclick="stuLogoReplaceUpload(${r.id})" title="Replace"><i class="fa-solid fa-upload"></i></button>
        <button class="emb-action-btn emb-action-btn--danger" onclick="stuLogoDelete(${r.id}, ${JSON.stringify(r.name || r.slug || '')})" title="Delete logo"><i class="fa-solid fa-trash"></i></button>
      </div>
    </div>`;
  }

  async function stuLogoRefetch(rid, btn) {
    if (btn) { btn.disabled = true; btn.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Loading"></span>'; }
    try {
      const r = await fetch('/api/studio-logos/refetch', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ row_id: rid }),
      });
      const d = await r.json();
      if (!d.ok) throw new Error(d.error || 'refetch failed');
      // Cache-bust just this card's <img> via the same `_v=` token the
      // renderer uses, instead of refetching every logo row + nuking
      // the grid (was 500 ms–2 s on large libraries). Mirrors the
      // `vicesRegenLogo` pattern.
      const row = _stuLogos.find(x => x.id === rid);
      if (row) row.last_fetched_at = new Date().toISOString();
      const card = document.querySelector(`.stu-logo-card[data-id="${rid}"] .stu-logo-thumb img`);
      if (card && row) {
        const v = encodeURIComponent((row.last_fetched_at || '') + ':' + row.id);
        const baseQ = encodeURIComponent(row.name || row.slug);
        card.src = `/api/studio-logo?q=${baseQ}&_v=${v}`;
        card.style.display = '';
      }
    } catch (e) {
      window.toast('Refetch failed: ' + (e.message || e));
    } finally {
      if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-rotate"></i>'; }
    }
  }

  async function stuLogoDelete(rid, name) {
    if (!confirm(`Delete logo for "${name}"?\n\nRemoves the row AND the PNG from metadata/studio_logos/.`)) return;
    try {
      const r = await fetch('/api/studio-logos/delete', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ ids: [rid] }),
      });
      const d = await r.json();
      if (!d.ok) throw new Error(d.error || 'delete failed');
      _stuLogos = _stuLogos.filter(x => x.id !== rid);
      _stuLogoUpdateAlphaAvailability();
      stuLogoApplyFilter();
    } catch (e) {
      window.toast('Delete failed: ' + (e.message || e));
    }
  }

  function stuLogoPickUpload() {
    _stuLogoReplaceId = null;
    document.getElementById('stuLogoUploadInput').click();
  }

  function stuLogoReplaceUpload(rid) {
    _stuLogoReplaceId = rid;
    document.getElementById('stuLogoUploadInput').click();
  }

  async function stuLogoUploadSelected(event) {
    const f = event.target.files && event.target.files[0];
    event.target.value = '';  // reset so the same file can be picked twice
    if (!f) return;
    const form = new FormData();
    form.append('file', f);
    if (_stuLogoReplaceId) {
      form.append('row_id', String(_stuLogoReplaceId));
    } else {
      const defaultName = f.name.replace(/\.[^.]+$/, '').replace(/[_\-]+/g, ' ').trim();
      const nm = prompt('Studio name for this logo:', defaultName);
      if (!nm || !nm.trim()) return;
      form.append('name', nm.trim());
    }
    try {
      const r = await fetch('/api/studio-logos/upload', { method: 'POST', body: form });
      const d = await r.json();
      if (!d.ok) throw new Error(d.error || 'upload failed');
      await stuLogoLoad();
    } catch (e) {
      window.toast('Upload failed: ' + (e.message || e));
    } finally {
      _stuLogoReplaceId = null;
    }
  }

  let _stuRenderToken = 0;

  function stuApplyFilter() {
    const q = (document.getElementById('stuFilterQ').value || '').toLowerCase();
    const tbody = document.getElementById('stuTableBody');
    if (!_stuAllStudios.length) {
      tbody.innerHTML = '<tr><td colspan="5" class="emb-empty">No studios in library yet — scan to populate.</td></tr>';
      return;
    }
    const rows = _stuAllStudios.filter(s => {
      if (q && !(s.name || '').toLowerCase().includes(q)) return false;
      if (_stuFilterMode === 'nologo')  return !s.has_logo;
      if (_stuFilterMode === 'noids')   return !s.tpdb_id && !s.stashdb_id && !s.fansdb_id;
      if (_stuFilterMode === 'missing') return !!s.path_missing;
      return true;
    });
    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="5" class="emb-empty">No studios match this filter.</td></tr>';
      return;
    }
    // Chunked render (see perfApplyFilter for the reasoning — studios
    // libraries routinely exceed 5000 rows, which single-shot innerHTML
    // stalls the page for several seconds.)
    _stuRenderToken++;
    const token = _stuRenderToken;
    tbody.innerHTML = '';
    let i = 0;
    const CHUNK = 80;
    function step() {
      if (token !== _stuRenderToken) return;
      const end = Math.min(i + CHUNK, rows.length);
      let html = '';
      for (let k = i; k < end; k++) html += stuRenderRow(rows[k]);
      tbody.insertAdjacentHTML('beforeend', html);
      i = end;
      if (i < rows.length) requestAnimationFrame(step);
    }
    step();
  }

  function stuRenderRow(s) {
    const tpdbUrl  = s.tpdb_id    ? `https://theporndb.net/studios/${s.tpdb_id}` : null;
    const stashUrl = s.stashdb_id ? `https://stashdb.org/studios/${s.stashdb_id}` : null;
    const fansUrl  = s.fansdb_id  ? `https://fansdb.cc/studios/${s.fansdb_id}` : null;

    const idPill = (id, label, profileUrl, logoKey) => id
      ? `<span class="emb-link-pill has">
           <a href="${esc(profileUrl)}" target="_blank" rel="noopener">${embSrcLogo(logoKey, label)}</a>
         </span>`
      : `<span class="emb-link-pill missing">—</span>`;

    // Logo thumbnail — falls back to a neutral placeholder when the
    // slug-keyed lookup in studio_logos has no matching file.
    const logoContent = s.has_logo
      ? `<img class="emb-hs-thumb emb-hs-thumb--studio" src="/api/studio-logo?name=${encodeURIComponent(s.name)}" alt="" loading="lazy" decoding="async" onerror="this.outerHTML='<div class=\\'emb-hs-missing emb-hs-thumb--studio\\'><i class=\\'fa-solid fa-building\\'></i></div>'">`
      : `<div class="emb-hs-missing emb-hs-thumb--studio"><i class="fa-solid fa-building"></i></div>`;

    const pathMissingBadge = s.path_missing
      ? '<span class="emb-path-missing" title="Missing on disk" aria-label="Folder path missing on disk"><i class="fa-solid fa-circle-exclamation"></i></span>'
      : '';
    // Library folder path — same treatment as the performer table.
    const pathLine = s.path
      ? `<span class="emb-name-path${s.path_missing ? ' is-missing' : ''}" title="${esc(s.path)}">${esc(s.path)}</span>`
      : '';

    return `<tr data-id="${s.id}"${s.path_missing ? ' class="is-path-missing"' : ''}>
      <td>
        <div class="emb-name-inner">
          ${logoContent}
          <div class="emb-name-text">
            <span class="emb-name-title">${esc(s.name)}${pathMissingBadge}</span>
            <div class="emb-name-meta">
              ${pathLine}
              <span class="emb-name-id">#${s.id}</span>
            </div>
          </div>
        </div>
      </td>
      <td>${idPill(s.tpdb_id,    'TPDB',    tpdbUrl,  'tpdb')}</td>
      <td>${idPill(s.stashdb_id, 'StashDB', stashUrl, 'stashdb')}</td>
      <td>${idPill(s.fansdb_id,  'FansDB',  fansUrl,  'fansdb')}</td>
      <td style="white-space:nowrap">
        <button class="emb-action-btn emb-action-btn--danger" onclick="healthRemoveRow(${s.id},'studio','${esc(s.name)}',this)" title="Remove"><i class="fa-solid fa-trash"></i></button>
      </td>
    </tr>`;
  }

  // ================================================================
  // MOVIES LIBRARY PANEL — unified list of feature films (kind=movie)
  // and JAV releases (kind=jav). Three independent tri-state source
  // filters (TPDB / TMDB / JAVStash): null → 'has' → 'missing' → null,
  // ANDed together when more than one is active.
  // ================================================================
  let _movAllMovies = [];
  let _movSrcFilters = { tpdb: null, tmdb: null, javstash: null };

  function refreshMoviesJavStatCounter() {
    const el = document.getElementById('statLibMovies');
    if (!el) return;
    el.textContent = fmtCount((_movAllMovies || []).length);
  }
  // ── Vices panel ─────────────────────────────────────────
  let _vicesLoaded = false;
  let _vicesAll = [];

  async function vicesLoadStatus() {
    _vicesLoaded = true;
    const tbody = document.getElementById('vicesTableBody');
    tbody.innerHTML = '<tr><td colspan="4" class="emb-empty">Loading…</td></tr>';
    try {
      const r = await fetch('/api/favourites');
      const data = await r.json();
      _vicesAll = data.vices || [];
      const total   = _vicesAll.length;
      const tagsSum = _vicesAll.reduce((n, v) => n + (Number(v.tag_count) || 0), 0);
      const logos   = _vicesAll.filter(v => v.image_url).length;
      const missing = _vicesAll.filter(v => v.path_missing).length;
      const custom  = _vicesAll.filter(v => !v.match_tpdb_id).length;
      document.getElementById('vicesSumTotal').textContent       = fmtCount(total);
      document.getElementById('vicesSumTags').textContent        = fmtCount(tagsSum);
      document.getElementById('vicesSumLogos').textContent       = fmtCount(logos);
      document.getElementById('vicesSumLogosPct').textContent    = total ? Math.round(logos * 100 / total) + '%' : '';
      document.getElementById('vicesSumPathMissing').textContent = fmtCount(missing);
      document.getElementById('vicesSumCustom').textContent      = fmtCount(custom);
      document.getElementById('statVices').textContent           = fmtCount(total);
      vicesApplyFilter();
    } catch(e) {
      tbody.innerHTML = '<tr><td colspan="4" class="emb-empty" style="color:var(--red)">Failed to load.</td></tr>';
    }
  }

  function vicesApplyFilter() {
    const q = (document.getElementById('vicesFilterQ').value || '').toLowerCase();
    const tbody = document.getElementById('vicesTableBody');
    if (!_vicesAll.length) {
      tbody.innerHTML = '<tr><td colspan="5" class="emb-empty">No vices configured. Add some in Settings → Content Filters.</td></tr>';
      return;
    }
    const rows = _vicesAll.filter(v => !q || (v.folder_name || '').toLowerCase().includes(q));
    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="5" class="emb-empty">No vices match this filter.</td></tr>';
      return;
    }
    tbody.innerHTML = rows.map(vicesRenderRow).join('');
  }

  function vicesRenderRow(v) {
    const name = esc(v.folder_name || '—');
    const nameAttr = (v.folder_name || '').replace(/"/g, '&quot;');
    // Stable integer id from vices_json — matches the #id badge shown
    // next to performers / studios / movies so the on-disk
    // `metadata/vices/{id}/` folder is easy to locate.
    const viceIdBadge = (v.vice_id || v.vice_id === 0)
      ? `<span class="emb-name-id" style="margin-left:6px" title="Metadata folder: metadata/vices/${esc(String(v.vice_id))}/">#${esc(String(v.vice_id))}</span>`
      : '';
    const folderPath = v.folder_path
      ? `<code style="font-size:11px;color:${v.path_missing ? 'var(--red)' : 'var(--dim)'}">${esc(v.folder_path)}${v.path_missing ? ' <i class="fa-solid fa-triangle-exclamation" title="Missing on disk"></i>' : ''}</code>`
      : '<span class="emb-link-pill missing">Set Vices dir</span>';
    const logo = v.image_url
      ? `<img src="${esc(v.image_url)}" alt="" style="height:28px;max-width:80px;object-fit:contain;filter:drop-shadow(0 1px 2px rgba(0,0,0,0.5))" loading="lazy">`
      : '<span class="emb-link-pill missing">—</span>';
    const tpdbPill = v.match_tpdb_id
      ? `<span class="emb-link-pill has"><a href="https://theporndb.net/tags/${esc(v.match_tpdb_id)}" target="_blank" rel="noopener"><img class="emb-src-logo" src="/static/logos/tpdb.webp" alt="TPDB"> ${esc(v.match_tpdb_id)}</a></span>`
      : '<span class="emb-link-pill missing">Custom</span>';
    const locked = !!v.locked;
    const lockBtn = locked
      ? `<button type="button" class="emb-btn-accent" onclick="vicesToggleLock('${nameAttr}', false, this)" title="Locked — unlock?" style="color:var(--amber)"><i class="fa-solid fa-lock"></i></button>`
      : `<button type="button" class="emb-btn-accent" onclick="vicesToggleLock('${nameAttr}', true, this)" title="Unlocked — lock?"><i class="fa-solid fa-lock-open"></i></button>`;
    const regenBtn = locked
      ? `<button type="button" class="emb-btn-accent" disabled title="Locked" style="opacity:0.45;cursor:not-allowed"><i class="fa-solid fa-rotate"></i></button>`
      : `<button type="button" class="emb-btn-accent" onclick="vicesRegenLogo('${nameAttr}', this)" title="Regenerate logo"><i class="fa-solid fa-rotate"></i></button>`;
    const actions = `<div style="display:inline-flex;gap:6px;align-items:center;justify-content:center">${lockBtn}${regenBtn}</div>`;
    return `<tr><td>${name}${viceIdBadge}</td><td>${folderPath}</td><td style="text-align:center">${logo}</td><td style="text-align:center">${tpdbPill}</td><td style="text-align:center">${actions}</td></tr>`;
  }

  async function vicesToggleLock(name, locked, btn) {
    if (!name) return;
    const original = btn ? btn.innerHTML : '';
    if (btn) { btn.disabled = true; btn.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Loading"></span>'; }
    try {
      const r = await fetch('/api/vices/lock', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ name, locked }),
      });
      const d = await r.json();
      if (d.error) { window.toast(`Lock toggle failed: ${d.error}`); return; }
      // Mirror the new state locally and re-render just the table.
      const lower = name.toLowerCase();
      _vicesAll.forEach(v => {
        if ((v.folder_name || '').toLowerCase() === lower) v.locked = locked;
      });
      vicesApplyFilter();
    } catch(e) {
      window.toast(`Lock toggle failed: ${e.message || e}`);
    } finally {
      if (btn) { btn.disabled = false; btn.innerHTML = original; }
    }
  }

  async function vicesRegenLogo(name, btn) {
    if (!name) return;
    const original = btn ? btn.innerHTML : '';
    if (btn) { btn.disabled = true; btn.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Loading"></span>'; }
    try {
      const r = await fetch('/api/vice-logo/regenerate', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ name }),
      });
      const d = await r.json();
      if (d.error) { window.toast(`Regenerate failed: ${d.error}`); return; }
      // Append a cache-bust token to this vice's image_url in the
      // already-loaded rows — /api/vice-logo sets a 24h Cache-Control
      // so the same URL would return the stale PNG otherwise.
      const tok = '_t=' + Date.now();
      const lower = name.toLowerCase();
      _vicesAll.forEach(v => {
        if ((v.folder_name || '').toLowerCase() === lower && v.image_url) {
          v.image_url = v.image_url.split('?')[0] + '?name=' + encodeURIComponent(name) + '&' + tok;
        }
      });
      vicesApplyFilter();
    } catch(e) {
      window.toast(`Regenerate failed: ${e.message || e}`);
    } finally {
      if (btn) { btn.disabled = false; btn.innerHTML = original; }
    }
  }

  let _movLoaded = false;

  // Sort key mirrors backend `_movie_library_sort_key`: lower-case,
  // strip leading "the " so "The Foo" sorts under F. Keeps the merged
  // Movies + JAV list in a stable A→Z flow.
  function _movSortKey(s) {
    const v = String(s || '').trim().toLowerCase();
    return v.startsWith('the ') ? v.slice(4).trim() : v;
  }

  async function movLoadMovies() {
    _movLoaded = true;
    const tbody = document.getElementById('movTableBody');
    tbody.innerHTML = '<tr><td colspan="5" class="emb-empty">Loading…</td></tr>';
    try {
      const r = await fetch('/api/favourites');
      const data = await r.json();
      const mv  = (data.movies || []).map(m => ({ ...m, kind: m.kind || 'movie' }));
      const jv  = (data.jav    || []).map(j => ({ ...j, kind: 'jav' }));
      _movAllMovies = mv.concat(jv).sort((a, b) => {
        const ka = _movSortKey(a.folder_name);
        const kb = _movSortKey(b.folder_name);
        if (ka < kb) return -1;
        if (ka > kb) return 1;
        return 0;
      });
      refreshMoviesJavStatCounter();
      movApplyFilter();
    } catch(e) {
      tbody.innerHTML = '<tr><td colspan="5" class="emb-empty" style="color:var(--red)">Failed to load.</td></tr>';
    }
  }

  // Tri-state cycle: null → 'has' → 'missing' → null. Each source is
  // independent; active filters compose with AND in movApplyFilter().
  function movCycleSrcFilter(src) {
    const cur = _movSrcFilters[src];
    _movSrcFilters[src] = cur === null ? 'has' : cur === 'has' ? 'missing' : null;
    movUpdateFilterButtons();
    movApplyFilter();
  }

  function movClearSrcFilters() {
    _movSrcFilters = { tpdb: null, tmdb: null, javstash: null };
    movUpdateFilterButtons();
    movApplyFilter();
  }

  function movUpdateFilterButtons() {
    const map = { tpdb: 'movFtTpdb', tmdb: 'movFtTmdb', javstash: 'movFtJav' };
    let anyActive = false;
    Object.entries(map).forEach(([src, id]) => {
      const btn = document.getElementById(id);
      if (!btn) return;
      const st = _movSrcFilters[src];
      btn.classList.toggle('filter-has',     st === 'has');
      btn.classList.toggle('filter-missing', st === 'missing');
      const label = btn.querySelector('.emb-src-filter-state');
      if (label) label.textContent = st === 'has' ? 'Has' : st === 'missing' ? 'Missing' : 'Any';
      if (st !== null) anyActive = true;
    });
    const clr = document.getElementById('movFtClear');
    if (clr) clr.style.display = anyActive ? '' : 'none';
  }

  function _movHasId(m, key) {
    return String(m[key] || '').trim().length > 0;
  }

  function _movPassesSrcFilters(m) {
    const checks = [
      ['tpdb',     _movHasId(m, 'match_tpdb_id')],
      ['tmdb',     _movHasId(m, 'match_tmdb_id')],
      ['javstash', _movHasId(m, 'match_javstash_id')],
    ];
    for (const [src, has] of checks) {
      const f = _movSrcFilters[src];
      if (f === 'has'     && !has) return false;
      if (f === 'missing' &&  has) return false;
    }
    return true;
  }

  function movApplyFilter() {
    const q = (document.getElementById('movFilterQ').value || '').toLowerCase();
    const tbody = document.getElementById('movTableBody');
    if (!_movAllMovies.length) {
      tbody.innerHTML = '<tr><td colspan="5" class="emb-empty">No movies indexed. Configure a Features directory (and optionally a JAV directory) under Settings, then run Scan Library.</td></tr>';
      return;
    }
    const rows = _movAllMovies.filter(m => {
      const name = (m.folder_name || '').toLowerCase();
      const alt  = (m.match_javstash_name || '').toLowerCase();
      if (q && !name.includes(q) && !alt.includes(q)) return false;
      return _movPassesSrcFilters(m);
    });
    if (!rows.length) { tbody.innerHTML = '<tr><td colspan="5" class="emb-empty">No movies match this filter.</td></tr>'; return; }
    tbody.innerHTML = rows.map(m => movRenderRow(m)).join('');
  }

  function movRenderRow(m) {
    const isJav  = m.kind === 'jav';
    const folder = esc(m.folder_name || '—');
    const alt = (m.match_javstash_name && m.match_javstash_name !== m.folder_name)
      ? `<div style="font-size:11px;color:var(--dim);margin-top:2px">${esc(m.match_javstash_name)}</div>` : '';
    const kindIco = isJav
      ? '<span class="emb-row-kind-ico" title="JAV release"><span class="ts-flag-jp ts-flag-jp--sm" role="img" aria-label="JAV"></span></span>'
      : '<span class="emb-row-kind-ico" title="Feature film"><i class="ts-icon-movies" aria-hidden="true"></i></span>';
    const tpdbId = String(m.match_tpdb_id     || '').trim();
    const tmdbId = String(m.match_tmdb_id     || '').trim();
    const jsId   = String(m.match_javstash_id || '').trim();
    const tpdbPill = tpdbId
      ? `<span class="emb-link-pill has"><a href="https://theporndb.net/movies/${esc(tpdbId)}" target="_blank" rel="noopener"><img class="emb-src-logo" src="/static/logos/tpdb.webp" alt="TPDB"> ${esc(tpdbId)}</a></span>`
      : `<span class="emb-link-pill missing">—</span>`;
    const tmdbPill = tmdbId
      ? `<span class="emb-link-pill has"><a href="https://www.themoviedb.org/movie/${esc(tmdbId)}" target="_blank" rel="noopener"><img class="emb-src-logo" src="/static/logos/tmdb.webp" alt="TMDB"> ${esc(tmdbId)}</a></span>`
      : `<span class="emb-link-pill missing">—</span>`;
    const jsPill = jsId
      ? `<span class="emb-link-pill has"><a href="https://javstash.org/scenes/${esc(jsId)}" target="_blank" rel="noopener"><img class="emb-src-logo" src="/static/logos/javstash.webp" alt="JAVStash"> ${esc(jsId)}</a></span>`
      : `<span class="emb-link-pill missing">—</span>`;
    return `<tr>
      <td style="font-size:13px;color:var(--text);font-weight:500"><div style="display:flex;align-items:center;min-width:0">${kindIco}<span style="min-width:0;flex:1;overflow:hidden;text-overflow:ellipsis">${folder}${alt}</span></div></td>
      <td>${tpdbPill}</td>
      <td>${tmdbPill}</td>
      <td>${jsPill}</td>
      <td style="white-space:nowrap">
        <button class="emb-action-btn" onclick="movOpenMatchModal(${m.id})" title="Manage links — match this row to TPDB, TMDB, or JAVStash"><i class="fa-solid fa-link"></i></button>
        <button class="emb-action-btn emb-action-btn--danger" onclick="healthRemoveRow(${m.id},'${esc(m.kind)}','${esc(m.folder_name || '')}',this)" title="Remove"><i class="fa-solid fa-trash"></i></button>
      </td>
    </tr>`;
  }

  // ── Manage-links modal ─────────────────────────────────────
  // Per-row dialog: search TPDB / TMDB / JAVStash independently and
  // wire any hit to the row via /api/favourites/match. Clear an
  // existing match via /api/favourites/unmatch. Works for both
  // kind=movie and kind=jav rows — the backend stores the match
  // against whichever source you pick regardless of folder.
  let _movMatchRowId = null;

  const _MOV_MATCH_SOURCES = {
    tpdb: {
      label: 'TPDB',
      logo: '/static/logos/tpdb.webp',
      idKey: 'match_tpdb_id',
      nameKey: 'match_tpdb_name',
      apiSource: 'TPDB',
      url: (id) => `https://theporndb.net/movies/${encodeURIComponent(id)}`,
    },
    tmdb: {
      label: 'TMDB',
      logo: '/static/logos/tmdb.webp',
      idKey: 'match_tmdb_id',
      nameKey: 'match_tmdb_name',
      apiSource: 'TMDB',
      url: (id) => `https://www.themoviedb.org/movie/${encodeURIComponent(id)}`,
    },
    javstash: {
      label: 'JAVStash',
      logo: '/static/logos/javstash.webp',
      idKey: 'match_javstash_id',
      nameKey: 'match_javstash_name',
      apiSource: 'JAVSTASH',
      url: (id) => `https://javstash.org/scenes/${encodeURIComponent(id)}`,
    },
  };

  function movOpenMatchModal(rowId) {
    const m = (_movAllMovies || []).find(x => x.id === rowId);
    if (!m) return;
    _movMatchRowId = rowId;
    const kindLabel = m.kind === 'jav' ? 'JAV release' : 'Feature film';
    const folder = m.folder_name || '—';
    document.getElementById('movieMatchTitle').textContent = `Manage links — ${folder}`;
    document.getElementById('movieMatchPath').textContent = kindLabel + (m.path ? ` · ${m.path}` : '');
    const body = document.getElementById('movieMatchBody');
    body.innerHTML = ['tpdb','tmdb','javstash'].map(src => _movRenderMatchSection(m, src)).join('');
    // Prefill each search box with a cleaned folder name (drops trailing
    // " (YYYY)" so TPDB/TMDB don't reject the title for year noise).
    const q = String(folder).replace(/\s*\(\d{4}\)\s*$/, '').trim();
    ['tpdb','tmdb','javstash'].forEach(src => {
      const el = document.getElementById(`movMatchQ_${src}`);
      if (el) el.value = q;
    });
    const modal = document.getElementById('movieMatchModal');
    modal.classList.add('open');
    modal.setAttribute('aria-hidden', 'false');
    setTimeout(() => {
      const first = document.getElementById('movMatchQ_tpdb');
      if (first) first.focus();
    }, 80);
  }

  function movCloseMatchModal() {
    const modal = document.getElementById('movieMatchModal');
    modal.classList.remove('open');
    modal.setAttribute('aria-hidden', 'true');
    _movMatchRowId = null;
  }

  function _movRenderMatchSection(m, src) {
    const meta = _MOV_MATCH_SOURCES[src];
    if (!meta) return '';
    const id = String(m[meta.idKey] || '').trim();
    const nm = String(m[meta.nameKey] || '').trim();
    const current = id
      ? `<div class="mov-match-current">
          <span class="emb-link-pill has"><a href="${esc(meta.url(id))}" target="_blank" rel="noopener"><img class="emb-src-logo" src="${meta.logo}" alt="" loading="lazy"> ${esc(id)}</a></span>
          ${nm ? `<span class="mov-match-current-name">${esc(nm)}</span>` : ''}
          <button type="button" class="mov-match-clear" onclick="movClearMatch('${src}')" title="Clear this match"><i class="fa-solid fa-xmark"></i> Clear</button>
        </div>`
      : `<div class="mov-match-current"><span class="emb-link-pill missing">No match</span></div>`;
    return `<section class="mov-match-section" data-src="${src}">
      <header class="mov-match-section-head"><img src="${meta.logo}" alt="" loading="lazy"><span>${meta.label}</span></header>
      ${current}
      <div class="mov-match-search">
        <input type="text" id="movMatchQ_${src}" placeholder="Search ${meta.label}…" onkeydown="if(event.key==='Enter')movRunSearch('${src}')">
        <button type="button" onclick="movRunSearch('${src}')">Search</button>
      </div>
      <div class="mov-match-results" id="movMatchResults_${src}"></div>
    </section>`;
  }

  async function movRunSearch(src) {
    if (!_MOV_MATCH_SOURCES[src]) return;
    const inp = document.getElementById(`movMatchQ_${src}`);
    const out = document.getElementById(`movMatchResults_${src}`);
    if (!inp || !out) return;
    const q = (inp.value || '').trim();
    if (!q) { out.innerHTML = '<div class="mov-match-empty">Enter a search term.</div>'; return; }
    out.innerHTML = '<div class="mov-match-empty">Searching…</div>';
    try {
      let results = [];
      if (src === 'tpdb') {
        const r = await fetch(`/api/movies/search?q=${encodeURIComponent(q)}&page=1`, { credentials: 'same-origin' });
        const d = await r.json();
        if (!r.ok) throw new Error(d.error || d.detail || `HTTP ${r.status}`);
        results = (d.results || []).slice(0, 18).map(x => ({
          id: String(x.id || ''),
          name: x.title || x.name || '',
          year: (x.date || x.year || '').toString().slice(0, 4),
          poster: x.poster || x.image || '',
        }));
      } else if (src === 'tmdb') {
        const r = await fetch(`/api/movies/tmdb-search?q=${encodeURIComponent(q)}`, { credentials: 'same-origin' });
        const d = await r.json();
        if (!r.ok) throw new Error(d.error || `HTTP ${r.status}`);
        if (d.error) throw new Error(d.error);
        results = (d.results || []).slice(0, 18).map(x => ({
          id: String(x.id || ''),
          name: x.title || x.original_title || '',
          year: (x.year || x.release_date || '').toString().slice(0, 4),
          poster: x.poster_url || (x.poster_path ? `https://image.tmdb.org/t/p/w154${x.poster_path}` : ''),
        }));
      } else if (src === 'javstash') {
        // /api/movies/search returns JAVStash hits on the same call,
        // alongside TPDB results — saves a round-trip.
        const r = await fetch(`/api/movies/search?q=${encodeURIComponent(q)}&page=1`, { credentials: 'same-origin' });
        const d = await r.json();
        if (!r.ok) throw new Error(d.error || `HTTP ${r.status}`);
        results = (d.jav_scenes || []).slice(0, 18).map(x => ({
          id: String(x.id || x.javstash_id || ''),
          name: x.title || x.name || '',
          year: (x.date || '').toString().slice(0, 4),
          poster: x.poster || x.image || '',
        }));
      }
      results = results.filter(r => r.id);
      if (!results.length) { out.innerHTML = '<div class="mov-match-empty">No matches.</div>'; return; }
      out.innerHTML = results.map(r => _movRenderMatchResult(src, r)).join('');
    } catch (e) {
      out.innerHTML = `<div class="mov-match-error">Search failed: ${esc((e && e.message) || String(e))}</div>`;
    }
  }

  function _movRenderMatchResult(src, r) {
    const yr = r.year ? ` <span style="color:var(--dim)">(${esc(r.year)})</span>` : '';
    const poster = r.poster
      ? `<img class="mov-match-result-poster" src="${esc(r.poster)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.outerHTML='<div class=\\'mov-match-result-poster mov-match-result-poster--missing\\'><i class=\\'ts-icon-movies\\' aria-hidden=\\'true\\'></i></div>'">`
      : `<div class="mov-match-result-poster mov-match-result-poster--missing"><i class="ts-icon-movies" aria-hidden="true"></i></div>`;
    const nameAttr = (r.name || '').replace(/"/g, '&quot;');
    const idAttr   = String(r.id || '').replace(/"/g, '&quot;');
    return `<button type="button" class="mov-match-result" data-src="${src}" data-id="${idAttr}" data-name="${nameAttr}" onclick="movApplyMatch(this)">
      ${poster}
      <span class="mov-match-result-meta">
        <span class="mov-match-result-title">${esc(r.name)}${yr}</span>
        <span class="mov-match-result-id">#${esc(String(r.id))}</span>
      </span>
    </button>`;
  }

  async function movApplyMatch(btn) {
    if (!_movMatchRowId) return;
    const src = btn.getAttribute('data-src');
    const extId = btn.getAttribute('data-id');
    const name = btn.getAttribute('data-name');
    const meta = _MOV_MATCH_SOURCES[src];
    if (!meta) return;
    btn.disabled = true;
    try {
      const r = await fetch('/api/favourites/match', {
        method: 'POST', credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ row_id: _movMatchRowId, source: meta.apiSource, external_id: extId, name }),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok || d.error) { window.toast && window.toast(d.error || 'Match failed'); btn.disabled = false; return; }
      _movUpdateRowMatch(_movMatchRowId, src, extId, name);
      movOpenMatchModal(_movMatchRowId);  // re-render the modal with new state
      movApplyFilter();
      window.toast && window.toast(`${meta.label} match saved.`);
    } catch (e) {
      window.toast && window.toast('Error: ' + ((e && e.message) || e));
      btn.disabled = false;
    }
  }

  async function movClearMatch(src) {
    if (!_movMatchRowId) return;
    const meta = _MOV_MATCH_SOURCES[src];
    if (!meta) return;
    if (!confirm(`Clear ${meta.label} match for this row?\n\nFiles on disk are not changed.`)) return;
    try {
      const r = await fetch('/api/favourites/unmatch', {
        method: 'POST', credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id: _movMatchRowId, source: meta.apiSource }),
      });
      const d = await r.json().catch(() => ({}));
      if (!r.ok || d.error) { window.toast && window.toast(d.error || 'Clear failed'); return; }
      _movUpdateRowMatch(_movMatchRowId, src, '', '');
      movOpenMatchModal(_movMatchRowId);
      movApplyFilter();
    } catch (e) {
      window.toast && window.toast('Error: ' + ((e && e.message) || e));
    }
  }

  function _movUpdateRowMatch(rowId, src, id, name) {
    const m = (_movAllMovies || []).find(x => x.id === rowId);
    if (!m) return;
    const meta = _MOV_MATCH_SOURCES[src];
    if (!meta) return;
    m[meta.idKey] = id;
    m[meta.nameKey] = name;
  }

  // ── Activity Log Panel ─────────────────────────────────────
  let _logLibEventSource = null;
  function logLibClassifyLine(t) {
    const l = (t || '').toLowerCase();
    if (l.includes('error') || l.includes('fail') || l.includes('exception')) return 'log-line-err';
    if (l.includes('warn') || l.includes('skip')) return 'log-line-warn';
    if (l.includes('done') || l.includes('success') || l.includes('filed') || l.includes('matched')) return 'log-line-ok';
    if (l.startsWith('[debug]') || l.startsWith('debug')) return 'log-line-dim';
    return '';
  }
  function logLibAppendLine(text) {
    const body = document.getElementById('logLibBody');
    if (!body) return;
    // Backend prefixes lines with an ISO UTC stamp; reformat to the
    // browser's local clock so the Library Health log matches the
    // user's wall time regardless of where the server is running.
    const line = (typeof tsLogFormatLine === 'function') ? tsLogFormatLine(text) : text;
    const span = document.createElement('span');
    span.className = 'log-line ' + logLibClassifyLine(line);
    span.textContent = line;
    body.appendChild(span);
    body.appendChild(document.createTextNode('\n'));
    body.scrollTop = body.scrollHeight;
  }
  function logLibStartSSE() {
    if (_logLibEventSource) { _logLibEventSource.close(); _logLibEventSource = null; }
    _logLibEventSource = new EventSource('/api/log/stream');
    _logLibEventSource.onmessage = function(e) {
      try {
        const d = JSON.parse(e.data);
        if (d.ping) return;
        logLibAppendLine(d.line || d.text || String(d));
        const meta = document.getElementById('logLibMeta');
        if (meta) meta.textContent = 'Live';
      } catch {}
    };
  }
  const _LOG_PER_PAGE = 200;
  const _LOG_FILTER_KEY = 'ts_log_filter';
  let _logPage = 1;
  let _logTotal = 0;
  let _logTotalUnfiltered = 0;
  let _logFilterTimer = null;

  function _currentLogFilter() {
    return (document.getElementById('logFilterInput')?.value || '').trim();
  }

  async function logLibReload(page) {
    if (page != null) _logPage = page;
    const body = document.getElementById('logLibBody');
    const meta = document.getElementById('logLibMeta');
    if (!body) return;
    body.innerHTML = '<div style="color:var(--dim)">Loading…</div>';
    const offset = (_logPage - 1) * _LOG_PER_PAGE;
    const q = _currentLogFilter();
    const params = new URLSearchParams({
      limit: String(_LOG_PER_PAGE),
      offset: String(offset),
    });
    if (q) params.set('q', q);
    try {
      const r = await fetch('/api/log/history?' + params.toString());
      const d = await r.json();
      const lines = d.lines || [];
      _logTotal = d.total || lines.length;
      _logTotalUnfiltered = d.total_unfiltered != null ? d.total_unfiltered : _logTotal;
      body.innerHTML = '';
      lines.forEach(row => logLibAppendLine(typeof row === 'string' ? row : (row.line || row.text || JSON.stringify(row))));
      if (!lines.length) {
        body.innerHTML = q
          ? `<div style="color:var(--dim)">No lines match <strong>${q.replace(/[<&>]/g, c => ({'<':'&lt;','>':'&gt;','&':'&amp;'}[c]))}</strong>.</div>`
          : '<div style="color:var(--dim)">No log lines yet. Run the pipeline or library index.</div>';
      }
      if (meta) {
        meta.textContent = q
          ? `${fmtCount(_logTotal)} of ${fmtCount(_logTotalUnfiltered)} lines`
          : `${fmtCount(_logTotal)} lines`;
      }
      document.getElementById('statLogLines').textContent = fmtCount(_logTotalUnfiltered);
      _renderLogPagination();
      // Start SSE for live appends only on the last page AND only when
      // unfiltered. With a filter active, every appended line would
      // need to be re-tested against the query string client-side and
      // mid-stream insertions break the server-paginated count, so we
      // just skip live mode while filtered — the user can hit reload.
      const totalPages = Math.max(1, Math.ceil(_logTotal / _LOG_PER_PAGE));
      if (_logPage >= totalPages && !q) logLibStartSSE();
    } catch(e) {
      body.innerHTML = '<div style="color:var(--red)">Failed to load log</div>';
    }
  }

  // Debounced rerun whenever the filter input changes. Resets to page
  // 1 because the previous page index is meaningless under a different
  // filter, and persists the value to localStorage so it survives page
  // reloads. 200ms keeps the keystroke flow smooth without hammering
  // the server on every character.
  function logFilterChanged() {
    const q = _currentLogFilter();
    try {
      if (q) localStorage.setItem(_LOG_FILTER_KEY, q);
      else   localStorage.removeItem(_LOG_FILTER_KEY);
    } catch (_) {}
    if (_logFilterTimer) clearTimeout(_logFilterTimer);
    _logFilterTimer = setTimeout(() => {
      _logPage = 1;
      logLibReload();
    }, 200);
  }

  // Restore persisted filter on first paint so opening the page with
  // a previously-typed filter still respects it. Called once during
  // panel init alongside the first logLibReload().
  function logFilterRestore() {
    try {
      const saved = localStorage.getItem(_LOG_FILTER_KEY) || '';
      const inp = document.getElementById('logFilterInput');
      if (inp && saved) inp.value = saved;
    } catch (_) {}
  }

  function _renderLogPagination() {
    const el = document.getElementById('logPagination');
    if (!el) return;
    const pages = Math.max(1, Math.ceil(_logTotal / _LOG_PER_PAGE));
    if (pages <= 1) { el.innerHTML = ''; return; }
    let html = `<button class="h-page-btn" onclick="logLibReload(${_logPage - 1})" ${_logPage <= 1 ? 'disabled' : ''}>←</button>`;
    const ws = 7;
    let start = Math.max(1, _logPage - Math.floor(ws / 2));
    let end = Math.min(pages, start + ws - 1);
    if (end - start < ws - 1) start = Math.max(1, end - ws + 1);
    if (start > 1) html += `<button class="h-page-btn" onclick="logLibReload(1)">1</button>${start > 2 ? '<span class="h-page-info">…</span>' : ''}`;
    for (let i = start; i <= end; i++) html += `<button class="h-page-btn ${i === _logPage ? 'active' : ''}" onclick="logLibReload(${i})">${i}</button>`;
    if (end < pages) html += `${end < pages - 1 ? '<span class="h-page-info">…</span>' : ''}<button class="h-page-btn" onclick="logLibReload(${pages})">${pages}</button>`;
    html += `<button class="h-page-btn" onclick="logLibReload(${_logPage + 1})" ${_logPage >= pages ? 'disabled' : ''}>→</button>`;
    html += `<span class="h-page-info">Page ${_logPage} of ${pages}</span>`;
    el.innerHTML = html;
  }
  // The old client-side `logFilterApply` was removed. It hid
  // non-matching lines via display:none, which made server-paginated
  // pages render empty when matches lived on later pages. Filtering is
  // now a server-side LIKE on the activity_log table; pagination
  // operates on the filtered count (see logLibReload + logFilterChanged).

  async function logLibDownload() {
    //: Pass the filter input's current value through to the streaming
    //: endpoint so "download" matches what the user is currently
    //: looking at. Browser handles the actual file save via a
    //: synthetic <a download> click.
    const btn = document.getElementById('logLibDownloadBtn');
    const _orig = btn ? btn.innerHTML : '';
    if (btn) { btn.disabled = true; btn.innerHTML = '<span class="loader loader--btn" role="status" aria-label="Preparing"></span>'; }
    try {
      const qEl = document.getElementById('logFilterInput');
      const q = (qEl && qEl.value || '').trim();
      const url = '/api/log/download' + (q ? ('?q=' + encodeURIComponent(q)) : '');
      const a = document.createElement('a');
      a.href = url;
      a.rel = 'noopener';
      //: Defensive fallback — server already sets Content-Disposition
      //: attachment, but `download` ensures the browser saves rather
      //: than navigating away if that header ever drops.
      a.download = 'top-shelf-log.txt';
      document.body.appendChild(a);
      a.click();
      a.remove();
    } catch (e) {
      window.toast && window.toast('Download failed: ' + (e && e.message || e));
    } finally {
      setTimeout(() => { if (btn) { btn.disabled = false; btn.innerHTML = _orig; } }, 800);
    }
  }
  async function logLibPrune() {
    try {
      const r = await fetch('/api/log/prune', { method: 'POST' });
      const d = await r.json();
      const meta = document.getElementById('logLibMeta');
      if (meta) meta.textContent = d.removed > 0 ? `Pruned ${fmtCount(d.removed)} lines` : `Nothing to prune`;
      await logLibReload();
    } catch(e) { window.toast(e.message); }
  }
  async function logLibClear() {
    if (!confirm('Clear all stored log lines?')) return;
    try {
      await fetch('/api/log/clear', { method: 'POST' });
      const body = document.getElementById('logLibBody');
      if (body) body.innerHTML = '<div style="color:var(--dim)">Cleared.</div>';
      const meta = document.getElementById('logLibMeta');
      if (meta) meta.textContent = '';
      document.getElementById('statLogLines').textContent = '0';
    } catch {}
  }

  // ── History tile ──────────────────────────────────────────────────────

  let _histFilter = '';
  let _histPage = 1;
  let _histSort = 'processed_at';
  let _histDir = 'DESC';
  let _histText = '';
  let _histFilterTimer = null;
  const _HIST_PER_PAGE = 20;
  let _histLoaded = false;
  let _histRemovedCount = 0;

  async function histLoadStats() {
    try {
      const r = await fetch('/api/stats');
      const d = await r.json();
      const total = (d.filed||0) + (d.unmatched||0) + (d.no_dir||0) + (d.errors||0) + (d.removed||0);
      document.getElementById('statHistoryCount').textContent = fmtCount(total);
      _histRemovedCount = d.removed || 0;
      const pb = document.getElementById('histPurgeBtn');
      if (pb) pb.disabled = !(_histRemovedCount > 0);
    } catch {}
  }

  async function histLoadPage() {
    _histLoaded = true;
    const params = new URLSearchParams({ page: _histPage, per_page: _HIST_PER_PAGE, sort_by: _histSort, sort_dir: _histDir });
    if (_histFilter) params.set('status', _histFilter);
    if (_histText) params.set('filter_text', _histText);
    try {
      const r = await fetch(`/api/history?${params}`);
      const d = await r.json();
      const start = (d.page - 1) * d.per_page + 1;
      const end = Math.min(d.page * d.per_page, d.total);
      document.getElementById('historyLibMeta').textContent = d.total ? `${start}–${end} of ${d.total}` : '0 records';
      const tbody = document.getElementById('historyTableBody');
      if (!d.rows.length) {
        tbody.innerHTML = '<tr><td colspan="8" class="empty">No records found</td></tr>';
      } else {
        tbody.innerHTML = d.rows.map(row => {
          // Highlight matched performer + studio words inside the
          // filename and the matched scene title. Same `.qs-match`
          // accent treatment used by search results.
          const _hl = (typeof _qsBuildHighlightSet === 'function')
            ? _qsBuildHighlightSet(row.performers, row.match_studio)
            : null;
          const _h = (s) => (_hl && _hl.size && typeof _qsHighlight === 'function')
            ? _qsHighlight(s, _hl) : esc(s);
          return `<tr>
          <td class="h-td-dim" title="${esc(row.filename)}" style="color:var(--text)">${_h(row.display_name || row.filename)}</td>
          <td><span class="h-status-tag h-status-${row.status}">${row.status.replace('no_dir','no-dir')}</span></td>
          <td class="h-td-dim" title="${esc(row.match_title||'')}">${row.match_title ? _h(row.match_title) : '-'}</td>
          <td class="h-td-dim" title="${esc(row.match_studio||'')}">${row.match_studio ? _h(row.match_studio) : '-'}</td>
          <td class="h-td-dim" title="${esc(row.performers||'')}">${row.performers ? performerCsvHtml(row.performers) : '-'}</td>
          <td class="h-td-dim">${row.match_date||'-'}</td>
          <td>${row.match_source ? `<span class="h-source-tag">${esc(row.match_source)}</span>` : '-'}</td>
          <td class="h-td-dim">${row.processed_at ? row.processed_at.slice(0,16) : '-'}</td>
        </tr>`;
        }).join('');
        if (typeof enrichPerformerNames === 'function') enrichPerformerNames(tbody);
      }
      histRenderPagination(d.page, d.pages);
    } catch(e) {
      document.getElementById('historyTableBody').innerHTML = `<tr><td colspan="8" class="empty">Error: ${e.message}</td></tr>`;
    }
  }

  function histRenderPagination(page, pages) {
    const el = document.getElementById('historyPagination');
    if (pages <= 1) { el.innerHTML = ''; return; }
    let html = `<button class="h-page-btn" onclick="histGoPage(${page-1})" ${page===1?'disabled':''}>←</button>`;
    const ws = 7;
    let start = Math.max(1, page - Math.floor(ws/2));
    let end = Math.min(pages, start + ws - 1);
    if (end - start < ws - 1) start = Math.max(1, end - ws + 1);
    if (start > 1) html += `<button class="h-page-btn" onclick="histGoPage(1)">1</button>${start > 2 ? '<span class="h-page-info">…</span>' : ''}`;
    for (let i = start; i <= end; i++) html += `<button class="h-page-btn ${i===page?'active':''}" onclick="histGoPage(${i})">${i}</button>`;
    if (end < pages) html += `${end < pages-1 ? '<span class="h-page-info">…</span>' : ''}<button class="h-page-btn" onclick="histGoPage(${pages})">${pages}</button>`;
    html += `<button class="h-page-btn" onclick="histGoPage(${page+1})" ${page===pages?'disabled':''}>→</button>`;
    html += `<span class="h-page-info">Page ${page} of ${pages}</span>`;
    el.innerHTML = html;
  }

  function histGoPage(p) { _histPage = p; histLoadPage(); }

  function histSetFilter(status) {
    _histFilter = status;
    _histPage = 1;
    document.querySelectorAll('#historyFilterTabs .history-lib-filter-btn').forEach(b => {
      b.classList.toggle('active', b.dataset.status === status);
    });
    histLoadPage();
  }

  function histScheduleFilter() {
    clearTimeout(_histFilterTimer);
    _histFilterTimer = setTimeout(() => {
      _histText = document.getElementById('historyFilterText').value.trim();
      _histPage = 1;
      histLoadPage();
    }, 300);
  }

  function histSortBy(col) {
    _histDir = _histSort === col && _histDir === 'DESC' ? 'ASC' : 'DESC';
    _histSort = col;
    _histPage = 1;
    document.querySelectorAll('#historyLibPanel .h-sort-ind').forEach(el => {
      el.textContent = el.dataset.col === _histSort ? (_histDir === 'DESC' ? '↓' : '↑') : '';
    });
    document.querySelectorAll('#historyLibPanel th').forEach(th => th.classList.remove('h-sort-active'));
    document.querySelectorAll('#historyLibPanel .h-sort-ind').forEach(el => {
      if (el.dataset.col === _histSort) el.closest('th')?.classList.add('h-sort-active');
    });
    histLoadPage();
  }

  async function histPurgeRemoved() {
    if (_histRemovedCount < 1) return;
    if (!confirm(`Delete all ${_histRemovedCount} removed record${_histRemovedCount === 1 ? '' : 's'}? This cannot be undone.`)) return;
    const btn = document.getElementById('histPurgeBtn');
    if (btn) btn.disabled = true;
    try {
      const r = await fetch('/api/history/purge-removed', { method: 'POST' });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(d.detail || d.message || 'Failed');
      await histLoadStats();
      await histLoadPage();
    } catch(e) {
      window.toast('Error: ' + e.message);
      await histLoadStats();
    }
  }

  // Load performer/movie counts for stat tiles on page load
  (async function loadEmbCounts() {
    try {
      const r = await fetch('/api/favourites');
      const d = await r.json();
      document.getElementById('statLibMovies').textContent = fmtCount((d.movies || []).length + (d.jav || []).length);
      document.getElementById('statVices').textContent = fmtCount((d.vices || []).length);
    } catch(e) {}
    try {
      const r2 = await fetch('/api/performers/enrichment-status');
      const d2 = await r2.json();
      document.getElementById('statPerformers').textContent = fmtCount((d2.performers || []).length);
    } catch(e) {}
    try {
      const r3 = await fetch('/api/studios/enrichment-status');
      const d3 = await r3.json();
      document.getElementById('statStudios').textContent = fmtCount((d3.studios || []).length);
    } catch(e) {}
    try {
      const rLog = await fetch('/api/log/history?limit=1&offset=0');
      const dLog = await rLog.json();
      document.getElementById('statLogLines').textContent = fmtCount(dLog.total || 0);
    } catch(e) {}
    try {
      const rv = await fetch('/api/version');
      const dv = await rv.json();
      const v = dv.version || '?';
      document.getElementById('statVersion').textContent = v;
      // Stash for the About modal so it renders without another round-trip.
      window._aboutVersion = v;
    } catch(e) {}
    await histLoadStats();
    scheduleFitHealthStatCounterValues();
  })();

  // ── About / Version modal ─────────────────────────────────────────
  function openAboutModal() {
    document.getElementById('aboutModalVersion').textContent = window._aboutVersion || '?';
    document.getElementById('aboutModal').classList.add('open');
  }
  function closeAboutModal() {
    document.getElementById('aboutModal').classList.remove('open');
  }

  // ── Performer search & match modal ──────────────────────────────────
  let _perfSearchRowId = null;
  let _perfSearchGroupSite = null;

  function perfSearchMatch(rowId, name, opts) {
    _perfSearchRowId = rowId;
    _perfSearchGroupSite = (opts && opts.groupSite) || null;
    const titleEl = document.getElementById('perfSearchTitle');
    const srcSel = document.getElementById('perfSearchSource');
    if (_perfSearchGroupSite) {
      titleEl.textContent = 'Add group link: ' + name;
      const srcVal = _perfSearchGroupSite === 'tpdb' ? 'TPDB' : _perfSearchGroupSite === 'stashdb' ? 'StashDB' : 'FansDB';
      srcSel.value = srcVal;
      srcSel.disabled = true;
    } else {
      titleEl.textContent = 'Match: ' + name;
      srcSel.disabled = false;
    }
    document.getElementById('perfSearchInput').value = name;
    document.getElementById('perfSearchResults').innerHTML = '<div class="emb-empty">Enter a name and search</div>';
    document.getElementById('perfSearchModal').classList.add('open');
    setTimeout(() => document.getElementById('perfSearchInput').focus(), 100);
  }

  function closePerfSearchModal() {
    document.getElementById('perfSearchModal').classList.remove('open');
    _perfSearchRowId = null;
    _perfSearchGroupSite = null;
    const srcSel = document.getElementById('perfSearchSource');
    if (srcSel) srcSel.disabled = false;
  }

  async function runPerfSearch() {
    const q = document.getElementById('perfSearchInput').value.trim();
    if (!q) return;
    const src = document.getElementById('perfSearchSource').value;
    const el = document.getElementById('perfSearchResults');
    el.innerHTML = '<div class="emb-empty">Searching…</div>';
    try {
      const r = await fetch(`/api/metadata/search?q=${encodeURIComponent(q)}&type=performer&strict=0`);
      const d = await r.json();
      const results = (d.results || []).filter(r => !src || r.source === src);
      if (!results.length) { el.innerHTML = '<div class="emb-empty">No results found</div>'; return; }
      // Highlight set from the typed query so matching words paint
      // with `.qs-match` in result names.
      const qsHighlight = (typeof _qsBuildHighlightSet === 'function') ? _qsBuildHighlightSet(q) : null;
      const _hl = (s) => (qsHighlight && typeof _qsHighlight === 'function') ? _qsHighlight(s, qsHighlight) : esc(s);
      el.innerHTML = results.map((item, i) => `
        <div style="display:flex;gap:10px;padding:8px;border-radius:6px;cursor:pointer;transition:background 0.15s;align-items:center" onmouseover="this.style.background='rgba(255,255,255,0.04)'" onmouseout="this.style.background=''" onclick="perfPickMatch(${i})">
          ${item.image ? `<img src="${esc(item.image)}" loading="lazy" decoding="async" style="width:40px;height:54px;object-fit:cover;border-radius:4px;flex-shrink:0" onerror="this.style.display='none'">` : `<div style="width:40px;height:54px;background:var(--raised);border-radius:4px;flex-shrink:0;display:flex;align-items:center;justify-content:center;color:var(--muted)"><i class="fa-solid fa-user"></i></div>`}
          <div style="flex:1;min-width:0">
            <div style="font-size:12px;color:var(--text);font-weight:500">${_hl(item.name || '')}</div>
            <div style="font-size:10px;color:var(--dim)">${esc(item.source || '')}${item.disambiguation ? ' · ' + esc(item.disambiguation) : ''}</div>
          </div>
        </div>`).join('');
      window._perfSearchResults = results;
    } catch(e) {
      el.innerHTML = `<div class="emb-empty" style="color:var(--red)">Error: ${e.message}</div>`;
    }
  }

  async function perfPickMatch(idx) {
    const item = window._perfSearchResults?.[idx];
    if (!item || !_perfSearchRowId) return;
    const source = (item.source || '').toLowerCase();
    const siteKey = source === 'theporndb' ? 'tpdb' : source === 'stashdb' ? 'stashdb' : source === 'fansdb' ? 'fansdb' : source;
    try {
      if (_perfSearchGroupSite) {
        if (siteKey !== _perfSearchGroupSite) {
          window.toast('Pick a ' + _perfSearchGroupSite.toUpperCase() + ' result for this group link');
          return;
        }
        await fetch('/api/favourites/group-add-link', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ row_id: _perfSearchRowId, source: siteKey, ext_id: item.id }),
        });
      } else {
        await fetch('/api/favourites/set-match', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ row_id: _perfSearchRowId, source: siteKey, id: item.id, name: item.name }),
        });
      }
      closePerfSearchModal();
      await perfLoadStatus();
    } catch(e) {
      window.toast('Error: ' + e.message);
    }
  }

  document.getElementById('perfSearchModal')?.addEventListener('click', e => {
    if (e.target === e.currentTarget) closePerfSearchModal();
  });


/* Block 2/3: tree leaf lookup helpers. */

  let _treeLeafLookupToken = 0;
  function openTreeLeafLookup(ev, fullPath, name) {
    if (ev) { ev.preventDefault(); ev.stopPropagation(); }
    if (!fullPath) return;
    const overlay = document.getElementById('treeLeafLookupOverlay');
    const status  = document.getElementById('treeLeafLookupStatus');
    const results = document.getElementById('treeLeafLookupResults');
    const fileEl  = document.getElementById('treeLeafLookupFile');
    overlay.style.display = 'flex';
    fileEl.textContent = name || fullPath;
    status.textContent = 'Computing phash and querying StashDB, ThePornDB, FansDB…';
    results.innerHTML = '<div style="padding:24px;text-align:center;color:var(--dim);font-size:13px">Please wait…</div>';
    const myToken = ++_treeLeafLookupToken;
    fetch('/api/health/library-video/phash-match', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path: fullPath }),
    }).then(r => r.json().then(d => ({ ok: r.ok, d }))).then(({ ok, d }) => {
      if (myToken !== _treeLeafLookupToken) return;
      if (!ok || d.error) {
        status.textContent = d.error || 'Lookup failed';
        results.innerHTML = '<div style="padding:24px;text-align:center;color:var(--dim)">Nothing to show.</div>';
        return;
      }
      const phash = d.phash || '';
      const src = d.source || '';
      const matches = d.results || [];
      status.innerHTML = `Phash <code style="background:rgba(var(--brand-purple-rgb),0.10);padding:2px 6px;border-radius:4px;color:rgb(var(--brand-purple-rgb))">${esc(phash)}</code> — ${matches.length ? `${matches.length} match${matches.length === 1 ? '' : 'es'} from <strong style="color:var(--text)">${esc(src)}</strong>` : 'no matches on any source'}`;
      if (!matches.length) {
        results.innerHTML = '<div style="padding:32px;text-align:center;color:var(--dim);font-size:13px">No StashDB / TPDB / FansDB record matched this phash.</div>';
        return;
      }
      // Highlight set — built from the local filename stem so matching
      // words in DB result titles light up. Phash lookup has no user-
      // typed query, so the filename IS the implicit query.
      const _stem = (name || fullPath || '').replace(/\.[a-z0-9]{2,5}$/i, '');
      const qsHighlight = (typeof _qsBuildHighlightSet === 'function') ? _qsBuildHighlightSet(_stem) : null;
      const _hl = (s) => (qsHighlight && typeof _qsHighlight === 'function') ? _qsHighlight(s, qsHighlight) : esc(s);
      results.innerHTML = matches.map(m => {
        const source = (m.source || src || '').toLowerCase();
        let dbUrl = '';
        let dbLogo = '';
        let dbLabel = (m.source || src || '').toString();
        if (source === 'stashdb') {
          dbUrl = m.id ? `https://stashdb.org/scenes/${encodeURIComponent(m.id)}` : '';
          dbLogo = '/static/logos/stashdb.webp';
          dbLabel = 'StashDB';
        } else if (source === 'tpdb' || source === 'theporndb' || source === 'porndb') {
          dbUrl = m.id ? `https://theporndb.net/scenes/${encodeURIComponent(m.id)}` : '';
          dbLogo = '/static/logos/tpdb.webp';
          dbLabel = 'TPDB';
        } else if (source === 'fansdb') {
          dbUrl = m.id ? `https://fansdb.cc/scenes/${encodeURIComponent(m.id)}` : '';
          dbLogo = '/static/logos/fansdb.webp';
          dbLabel = 'FansDB';
        } else if (source === 'javstash') {
          dbUrl = m.id ? `https://javstash.org/scenes/${encodeURIComponent(m.id)}` : '';
          dbLogo = '/static/logos/javstash.webp';
          dbLabel = 'JAVStash';
        }
        const linkHtml = dbUrl
          ? `<a href="${esc(dbUrl)}" target="_blank" rel="noopener noreferrer" style="display:inline-flex;align-items:center;gap:7px;padding:7px 14px;border-radius:8px;background:linear-gradient(135deg, rgba(var(--brand-purple-rgb),0.18) 0%, rgba(var(--brand-accent-rgb), 0.14) 100%);border:1px solid rgba(var(--brand-purple-rgb),0.45);color:var(--text);font-size:12px;text-decoration:none;font-weight:500;transition:transform 0.15s, border-color 0.15s, box-shadow 0.15s" onmouseenter="this.style.borderColor='rgba(var(--brand-purple-rgb),0.85)';this.style.transform='translateY(-1px)';this.style.boxShadow='0 4px 12px rgba(var(--brand-purple-rgb),0.25)'" onmouseleave="this.style.borderColor='rgba(var(--brand-purple-rgb),0.45)';this.style.transform='';this.style.boxShadow=''">${dbLogo ? `<img src="${esc(dbLogo)}" alt="${esc(dbLabel)}" style="height:15px;width:auto;vertical-align:middle" loading="lazy">` : `<span>${esc(dbLabel)}</span>`}<i class="fa-solid fa-arrow-up-right-from-square" style="font-size:10px;opacity:0.7"></i></a>`
          : '';
        const description = (m.description || '').trim();
        return `
        <div style="display:flex;gap:18px;padding:16px;border:1px solid rgba(var(--brand-purple-rgb),0.22);border-radius:12px;margin-bottom:14px;background:linear-gradient(145deg, rgba(var(--brand-purple-rgb),0.06) 0%, rgba(var(--brand-purple-rgb),0.02) 100%);box-shadow:0 4px 16px rgba(0,0,0,0.25);transition:border-color 0.2s, transform 0.2s" onmouseenter="this.style.borderColor='rgba(var(--brand-purple-rgb),0.45)'" onmouseleave="this.style.borderColor='rgba(var(--brand-purple-rgb),0.22)'">
          ${m.image
            ? `<img src="${esc(m.image)}" style="width:300px;height:170px;object-fit:cover;border-radius:8px;flex-shrink:0;background:rgba(0,0,0,0.4);box-shadow:0 6px 18px rgba(0,0,0,0.45)" onerror="this.onerror=null;this.src='/static/img/missing.webp'" loading="lazy">`
            : `<img src="/static/img/missing.webp" alt="No artwork" style="width:300px;height:170px;object-fit:cover;border-radius:8px;flex-shrink:0;background:rgba(0,0,0,0.4);box-shadow:0 6px 18px rgba(0,0,0,0.45)" loading="lazy">`}
          <div style="flex:1;min-width:0;display:flex;flex-direction:column;gap:8px">
            <div>
              <div style="font-family:var(--secs);font-size:17px;color:var(--text);font-weight:500;line-height:1.25;margin-bottom:6px;letter-spacing:0.01em;overflow:hidden;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical">${_hl(m.title || 'Unknown')}</div>
              <div style="display:flex;flex-wrap:wrap;gap:4px 14px;font-size:12px;color:var(--dim);line-height:1.5">
                ${m.studio ? `<div><i class="ts-icon-studios" style="color:rgba(var(--brand-purple-rgb),0.55);font-size:10px;margin-right:5px" aria-hidden="true"></i><span style="color:var(--text)">${_hl(m.studio)}</span></div>` : ''}
                ${m.date ? `<div><i class="fa-solid fa-calendar" style="color:rgba(var(--brand-purple-rgb),0.55);font-size:10px;margin-right:5px"></i><span style="color:var(--text)">${esc(m.date)}</span></div>` : ''}
                ${m.performers ? `<div style="flex-basis:100%;margin-top:2px"><i class="fa-solid fa-user" style="color:rgba(var(--brand-accent-rgb), 0.65);font-size:10px;margin-right:5px"></i><span style="color:var(--text)">${(typeof _qsPerformerCsvHtml === 'function' ? _qsPerformerCsvHtml(m.performers, qsHighlight) : performerCsvHtml(m.performers))}</span></div>` : ''}
              </div>
            </div>
            ${description ? `<div style="font-size:12px;color:var(--text);opacity:0.82;line-height:1.55;overflow:hidden;display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical">${esc(description)}</div>` : ''}
            <div style="margin-top:auto;padding-top:6px;display:flex;align-items:center;gap:10px;flex-wrap:wrap">
              <span style="display:inline-flex;align-items:center;padding:4px 10px;border-radius:999px;background:rgba(var(--brand-accent-rgb), 0.16);border:1px solid rgba(var(--brand-accent-rgb), 0.40);color:var(--accent);font-weight:600">${dbLogo ? `<img src="${esc(dbLogo)}" alt="${esc(dbLabel)}" style="height:13px;width:auto;vertical-align:middle" loading="lazy">` : `<span style="font-size:10px;letter-spacing:0.10em;text-transform:uppercase">${esc(m.source || src)}</span>`}</span>
              ${linkHtml}
            </div>
          </div>
        </div>`;
      }).join('');
      if (typeof enrichPerformerNames === 'function') enrichPerformerNames(results);
    }).catch(e => {
      if (myToken !== _treeLeafLookupToken) return;
      status.textContent = 'Lookup error: ' + (e && e.message || e);
      results.innerHTML = '';
    });
  }
  function closeTreeLeafLookup() {
    _treeLeafLookupToken++;
    document.getElementById('treeLeafLookupOverlay').style.display = 'none';
  }
  // Delegated click handler — survives re-renders of the tree without
  // re-binding, and sidesteps onclick-attribute quoting problems for
  // filenames containing apostrophes or other special chars.
  document.addEventListener('click', function(e) {
    const btn = e.target && e.target.closest && e.target.closest('.tree-leaf-lookup-btn');
    if (!btn) return;
    e.preventDefault();
    e.stopPropagation();
    const path = btn.getAttribute('data-lookup-path') || '';
    const name = btn.getAttribute('data-lookup-name') || '';
    openTreeLeafLookup(null, path, name);
  });


/* Block 3/3: misc IIFE. */

  (function () {
    // Per-file filmstrip state. Each entry: {path, thumbs:[{i,url}], pollToken}
    let _dfsFiles = [];
    let _dfsLightboxItems = [];  // flat list of {url} across all files for arrow nav
    let _dfsLbIndex = -1;
    let _dfsKeydownBound = false;
    let _dfsToken = 0;

    function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }

    function _renderStage(filesState) {
      const stage = document.getElementById('dupFilmstripStage');
      if (!stage) return;
      const flat = [];
      const rowsHtml = filesState.map((fs, fi) => {
        const slots = [];
        for (let i = 0; i < 5; i++) {
          const t = (fs.thumbs || []).find(x => x.i === i);
          if (t) {
            const flatIdx = flat.length;
            flat.push({ url: t.url, file: fs.path });
            slots.push(`<div class="dfs-thumb" data-dfs-lb-idx="${flatIdx}" title="Enlarge"><img src="${esc(t.url)}" alt="Frame ${i + 1}" draggable="true" loading="lazy"></div>`);
          } else if (fs.generating) {
            slots.push('<div class="dfs-thumb"><div class="dfs-thumb-empty" title="Generating frame…"><span class="loader loader--btn loader--muted" role="status" aria-label="Generating"></span></div></div>');
          } else if (fs.backoff) {
            slots.push(`<div class="dfs-thumb"><button type="button" class="dfs-retry-btn" data-dfs-retry="${fi}" title="ffmpeg gave up — click to retry"><i class="fa-solid fa-rotate-right"></i></button></div>`);
          } else {
            slots.push('<div class="dfs-thumb"><div class="dfs-thumb-empty"><i class="fa-solid fa-film" aria-hidden="true"></i></div></div>');
          }
        }
        let statusLine;
        if (fs.error) statusLine = '<span style="color:rgba(244,114,182,0.85)">' + esc(fs.error) + '</span>';
        else if (fs.backoff) statusLine = 'FFmpeg gave up on this file — click any frame to retry';
        else if (fs.generating) statusLine = 'Generating filmstrip… (' + (fs.thumbs || []).length + '/5)';
        else if (!fs.thumbs || !fs.thumbs.length) statusLine = 'No frames captured';
        else statusLine = '';
        return `<div class="dfs-row">
          <div class="dfs-row-label">${esc(fs.path)}${statusLine ? '<br><span style="color:rgba(255,255,255,0.55);font-size:10px">' + statusLine + '</span>' : ''}</div>
          <div class="dfs-strip">${slots.join('')}</div>
        </div>`;
      });
      stage.innerHTML = rowsHtml.join('');
      _dfsLightboxItems = flat;
      stage.querySelectorAll('[data-dfs-lb-idx]').forEach(el => {
        el.addEventListener('click', () => {
          const idx = parseInt(el.getAttribute('data-dfs-lb-idx') || '-1', 10);
          if (idx >= 0) openDupFilmstripLightbox(idx);
        });
      });
      stage.querySelectorAll('[data-dfs-retry]').forEach(btn => {
        btn.addEventListener('click', async (ev) => {
          ev.preventDefault();
          ev.stopPropagation();
          const fi = parseInt(btn.getAttribute('data-dfs-retry') || '-1', 10);
          if (fi < 0 || !filesState[fi]) return;
          const p = filesState[fi].path;
          btn.disabled = true;
          btn.innerHTML = '<span class="loader loader--btn loader--muted" role="status" aria-label="Retrying"></span>';
          try {
            await fetch('/api/health/duplicate-thumbs/retry', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              credentials: 'same-origin',
              body: JSON.stringify({ path: p }),
            });
          } catch { /* fall through */ }
          filesState[fi].backoff = false;
          filesState[fi].generating = true;
          _pollFile(filesState, fi);
          _renderStage(filesState);
        }, { once: true });
      });
    }

    async function _pollFile(filesState, fi) {
      const myToken = _dfsToken;
      const fs = filesState[fi];
      if (!fs) return;
      const POLL_INTERVAL_MS = 1500;
      const POLL_TIMEOUT_MS = 60 * 1000;
      const startMs = Date.now();
      while (true) {
        if (myToken !== _dfsToken) return;
        let d = {};
        try {
          const r = await fetch('/api/health/duplicate-thumbs?path=' + encodeURIComponent(fs.path), { credentials: 'same-origin' });
          d = await r.json().catch(() => ({}));
          if (!r.ok) {
            fs.error = (d && d.error) || 'Failed to generate filmstrip';
            _renderStage(filesState);
            return;
          }
        } catch (e) {
          fs.error = 'Network error: ' + (e && e.message || e);
          _renderStage(filesState);
          return;
        }
        if (myToken !== _dfsToken) return;
        fs.thumbs = Array.isArray(d.thumbs) ? d.thumbs : [];
        fs.generating = !!d.generating;
        fs.backoff = !!d.backoff;
        _renderStage(filesState);
        if (d.ready || d.backoff || !d.generating) return;
        if (Date.now() - startMs > POLL_TIMEOUT_MS) {
          fs.backoff = true;
          fs.generating = false;
          _renderStage(filesState);
          return;
        }
        await new Promise(res => setTimeout(res, POLL_INTERVAL_MS));
      }
    }

    function _handleKey(e) {
      const overlay = document.getElementById('dupFilmstripOverlay');
      if (!overlay || !overlay.classList.contains('open')) return;
      if (e.key === 'Escape') {
        const lb = document.getElementById('dupFilmstripLightbox');
        if (lb && lb.classList.contains('open')) closeDupFilmstripLightbox();
        else closeDupFilmstrip();
        e.preventDefault();
        return;
      }
      if (_dfsLbIndex < 0) return;
      if (e.key === 'ArrowLeft')  { dupFilmstripStep(-1); e.preventDefault(); }
      if (e.key === 'ArrowRight') { dupFilmstripStep(1);  e.preventDefault(); }
    }

    window.openDupFilmstrip = function (group) {
      const overlay = document.getElementById('dupFilmstripOverlay');
      const stage   = document.getElementById('dupFilmstripStage');
      if (!overlay || !stage || !group) return;
      const files = (group.files || []).filter(f => f && f.destination);
      if (!files.length) return;
      _dfsToken++;
      _dfsFiles = files.map(f => ({
        path: f.destination,
        thumbs: [],
        generating: true,
        backoff: false,
        error: null,
      }));
      _renderStage(_dfsFiles);
      overlay.classList.add('open');
      if (!_dfsKeydownBound) {
        document.addEventListener('keydown', _handleKey);
        _dfsKeydownBound = true;
      }
      _dfsFiles.forEach((_fs, i) => _pollFile(_dfsFiles, i));
    };

    window.closeDupFilmstrip = function () {
      const overlay = document.getElementById('dupFilmstripOverlay');
      if (overlay) overlay.classList.remove('open');
      closeDupFilmstripLightbox();
      if (_dfsKeydownBound) {
        document.removeEventListener('keydown', _handleKey);
        _dfsKeydownBound = false;
      }
      _dfsToken++;  // invalidate in-flight polls
      _dfsFiles = [];
    };

    window.openDupFilmstripLightbox = function (idx) {
      if (idx < 0 || idx >= _dfsLightboxItems.length) return;
      _dfsLbIndex = idx;
      const lb  = document.getElementById('dupFilmstripLightbox');
      const img = document.getElementById('dupFilmstripBigImg');
      const ctr = document.getElementById('dupFilmstripCounter');
      if (!lb || !img) return;
      img.src = _dfsLightboxItems[idx].url;
      if (ctr) ctr.textContent = (idx + 1) + ' / ' + _dfsLightboxItems.length;
      lb.classList.add('open');
    };

    window.closeDupFilmstripLightbox = function () {
      const lb = document.getElementById('dupFilmstripLightbox');
      if (lb) lb.classList.remove('open');
      _dfsLbIndex = -1;
    };

    window.dupFilmstripStep = function (delta) {
      if (!_dfsLightboxItems.length || _dfsLbIndex < 0) return;
      const next = (_dfsLbIndex + delta + _dfsLightboxItems.length) % _dfsLightboxItems.length;
      openDupFilmstripLightbox(next);
    };
  })();
