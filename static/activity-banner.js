/**
 * Center header: Library Health–style progress bars + notifications (poll /api/activity/banner).
 * Optional: window.__tsHealthScanning — set by /health while NFO scan runs (client flag).
 */
(function () {
  var AUTO_DISMISS_MS = 20000;
  var CLIENT_TOAST_DEFAULT_MS = 8000;
  /** Max dismissible notification boxes; progress rows are uncapped and listed first. */
  var MAX_VISIBLE_ALERTS = 2;
  var _dismissTimers = {};
  var _clientDismissTimers = {};
  /** Client-side alerts (from window.toast); survive banner poll re-renders. */
  var _clientAlerts = [];
  var _clientAlertSeq = 0;
  var _lastBannerData = null;

  function clearDismissTimer(id) {
    var k = String(id);
    if (_dismissTimers[k]) {
      clearTimeout(_dismissTimers[k]);
      delete _dismissTimers[k];
    }
  }

  function clearAllDismissTimers() {
    Object.keys(_dismissTimers).forEach(function (k) {
      clearTimeout(_dismissTimers[k]);
      delete _dismissTimers[k];
    });
  }

  function scheduleAutoDismiss(id) {
    var nid = parseInt(id, 10);
    if (!nid) return;
    clearDismissTimer(nid);
    _dismissTimers[String(nid)] = setTimeout(function () {
      clearDismissTimer(nid);
      fetch("/api/activity/notifications/dismiss", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ id: nid }),
      }).then(function () {
        tick();
      });
    }, AUTO_DISMISS_MS);
  }

  function esc(s) {
    return String(s || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function fmtCount(n) {
    var x = Number(n);
    if (n === "-" || n === "" || n == null) return String(n);
    if (!isFinite(x)) return String(n);
    return x.toLocaleString("en-US");
  }

  /** Compact “+N more” chip — matches banner dot size, stacked-circle motif. */
  function bannerMoreHtml(count, title, opts) {
    var n = Math.max(0, parseInt(count, 10) || 0);
    if (n < 1) return "";
    opts = opts || {};
    var tip = String(title || n + " more");
    var live = opts.live ? ' aria-live="polite"' : "";
    var stack = n >= 3 ? " ts-banner-dot-more--stack" : "";
    return (
      '<span class="ts-banner-dot-more' + stack + '"' + live +
      ' title="' + esc(tip) + '"' +
      ' aria-label="' + esc(tip) + '">' +
      '<span class="ts-banner-dot-more__count" aria-hidden="true">+' + esc(String(n)) + "</span>" +
      "</span>"
    );
  }

  function healthBasename(pathStr) {
    var s = String(pathStr || "")
      .trim()
      .replace(/\\/g, "/");
    if (!s) return "";
    var i = s.lastIndexOf("/");
    return i >= 0 ? s.slice(i + 1) : s;
  }

  function kindClass(kind) {
    var k = String(kind || "info").toLowerCase();
    if (k === "error") return "ts-banner-error";
    if (k === "success") return "ts-banner-success";
    if (k === "library") return "ts-banner-library";
    if (k === "pipeline") return "ts-banner-pipeline";
    return "";
  }

  /** FontAwesome icon class for an alert kind — drives the dot button. */
  function kindIconHtml(kind) {
    var k = String(kind || "info").toLowerCase();
    if (k === "error")    return '<i class="fa-solid fa-triangle-exclamation"></i>';
    if (k === "success")  return '<i class="fa-solid fa-circle-check"></i>';
    if (k === "library")  return '<i class="fa-solid fa-ticket"></i>';
    if (k === "pipeline") return '<i class="fa-solid fa-list"></i>';
    return '<i class="fa-solid fa-circle-info"></i>';
  }

  function mapToastKind(kind) {
    var k = String(kind || "info").toLowerCase();
    if (k === "error") return "error";
    if (k === "success") return "success";
    return "info";
  }

  function pruneClientAlerts() {
    var now = Date.now();
    _clientAlerts = _clientAlerts.filter(function (a) {
      return a.expiresAt > now;
    });
  }

  function clearClientDismissTimer(id) {
    var k = String(id);
    if (_clientDismissTimers[k]) {
      clearTimeout(_clientDismissTimers[k]);
      delete _clientDismissTimers[k];
    }
  }

  function scheduleClientDismiss(alert) {
    if (!alert || !alert.id) return;
    clearClientDismissTimer(alert.id);
    var ms = Math.max(0, alert.expiresAt - Date.now());
    _clientDismissTimers[String(alert.id)] = setTimeout(function () {
      clearClientDismissTimer(alert.id);
      dismissClientAlert(alert.id);
    }, ms);
  }

  function dismissClientAlert(id) {
    clearClientDismissTimer(id);
    _clientAlerts = _clientAlerts.filter(function (a) {
      return a.id !== id;
    });
    render(_lastBannerData || {});
  }

  function pushAlert(message, opts) {
    opts = opts || {};
    pruneClientAlerts();
    var id = "c" + ++_clientAlertSeq;
    var timeout =
      typeof opts.timeout === "number"
        ? opts.timeout
        : opts.action
          ? 8000
          : CLIENT_TOAST_DEFAULT_MS;
    var alert = {
      id: id,
      message: String(message || ""),
      kind: mapToastKind(opts.kind),
      expiresAt: Date.now() + timeout,
      onAction: opts.action && typeof opts.action.callback === "function" ? opts.action.callback : null,
    };
    _clientAlerts.push(alert);
    if (_clientAlerts.length > 24) _clientAlerts.shift();
    scheduleClientDismiss(alert);
    render(_lastBannerData || {});
    return alert;
  }

  /**
   * Compact circular progress indicator (one per running task).
   * opts: { slot, iconHtml, pct, indeterminate, tooltip, complete }
   * Uses pathLength="100" so stroke-dasharray/offset are percent-based,
   * and stroke-dashoffset=25 shifts the arc start to 12 o'clock reliably.
   */
  var CIRCLE_R = 18;
  function circleHtml(opts) {
    var pct = Math.max(0, Math.min(100, Number(opts.pct) || 0));
    var ind = !!opts.indeterminate;
    var complete = !!opts.complete || pct >= 99.5;
    var pctVal = ind ? 28 : pct;
    var dash = pctVal + " " + Math.max(0, 100 - pctVal);
    // Explicit `centerHtml` override wins over the default tick / dots /
    // percent logic — used by the Downloads circle to render a matched
    // performer's headshot instead of a number.
    var centerHtml;
    if (opts.centerHtml) {
      centerHtml = opts.centerHtml;
    } else if (complete) {
      centerHtml = '<span class="ts-circle-tick">&#10003;</span>';
    } else if (ind) {
      centerHtml = '<span class="ts-circle-dots">&middot;&middot;&middot;</span>';
    } else {
      centerHtml =
        '<span class="ts-circle-pct">' + Math.round(pct) + "</span>";
    }
    var fillClass = "ts-circle-fill" + (ind ? " is-indeterminate" : "");
    var iconHtml = opts.iconHtml || "";
    return (
      '<div class="ts-circle" data-slot="' +
      esc(opts.slot || "") +
      '" title="' +
      esc(opts.tooltip || "") +
      '" role="img" aria-label="' +
      esc(opts.tooltip || opts.slot || "") +
      '">' +
      (iconHtml ? '<span class="ts-circle-icon">' + iconHtml + "</span>" : "") +
      '<span class="ts-circle-ring">' +
      '<svg class="ts-circle-svg" viewBox="0 0 44 44" aria-hidden="true">' +
      '<circle class="ts-circle-track" cx="22" cy="22" r="' +
      CIRCLE_R +
      '" pathLength="100"/>' +
      '<circle class="' +
      fillClass +
      '" cx="22" cy="22" r="' +
      CIRCLE_R +
      '" pathLength="100" stroke-dasharray="' +
      dash +
      '" stroke-dashoffset="25"/>' +
      "</svg>" +
      '<span class="ts-circle-center">' +
      centerHtml +
      "</span>" +
      "</span>" +
      "</div>"
    );
  }

  function circlePipeline(data) {
    if (!data || !data.running) return "";
    var total = Math.max(0, parseInt(data.pipeline_total, 10) || 0);
    var done = Math.max(0, parseInt(data.pipeline_done, 10) || 0);
    var cur = healthBasename(data.current_file || "");
    var ind = total <= 0;
    var pct = total > 0 ? (done / total) * 100 : 0;
    var count = total > 0 ? fmtCount(done) + " / " + fmtCount(total) : fmtCount(done);
    var tip = "Sorting · " + count + (cur ? " · " + cur : "");
    return circleHtml({
      slot: "pipeline",
      iconHtml: '<i class="fa-solid fa-list"></i>',
      pct: pct,
      indeterminate: ind,
      tooltip: tip,
    });
  }

  function circleLibraryIndex(ix) {
    if (!ix || !ix.running) return "";
    var ind = !!ix.indeterminate;
    var t = Math.max(0, ix.total || 0);
    var done = Math.max(0, ix.done || 0);
    var pct = !ind && t > 0 ? (done / t) * 100 : 0;
    var phase = ix.phase || "apply";
    var cur = healthBasename(ix.current_path || "");
    var count = t > 0 ? fmtCount(done) + " / " + fmtCount(t) : fmtCount(done);
    var tip =
      (phase === "stageWalk" ? "Scanning" : "Indexing") +
      " · " +
      count +
      (cur ? " · " + cur : "");
    return circleHtml({
      slot: "library_index",
      iconHtml: '<i class="fa-solid fa-ticket"></i>',
      pct: pct,
      indeterminate: ind,
      tooltip: tip,
    });
  }

  function circlePhash3(p3) {
    if (!p3 || !p3.running) return "";
    var t = Math.max(0, p3.total || 0);
    var done = Math.max(0, p3.done || 0);
    var ind = t <= 0;
    var pct = t > 0 ? (done / t) * 100 : 0;
    var cur = healthBasename(p3.current_path || "");
    var count = t > 0 ? fmtCount(done) + " / " + fmtCount(t) : fmtCount(done);
    var tip = "Phash · " + count + (cur ? " · " + cur : "");
    return circleHtml({
      slot: "phash3",
      iconHtml: '<i class="fa-solid fa-fingerprint"></i>',
      pct: pct,
      indeterminate: ind,
      tooltip: tip,
    });
  }

  function favouritesCenterLabel(p) {
    if (!p || !p.running) return "";
    var nm = String(p.current_name || "").trim();
    if (nm === "prune") return "Removing stale entries…";
    if (nm === "paths") return "Checking folder paths…";
    if (nm) return "Indexing " + nm;
    if (p.phase === "refresh_all") return "Re-matching all…";
    if (p.phase === "refresh_images_all") return "Refreshing images…";
    if (p.phase === "scan_missing") return "Searching missing…";
    if (p.phase === "scan_full") return "Full index scan…";
    return "Indexing…";
  }

  function circleFavourites(p) {
    if (!p || !p.running) return "";
    var t = Math.max(0, parseInt(p.total, 10) || 0);
    var d = Math.max(0, parseInt(p.done, 10) || 0);
    var ind = t <= 0;
    var pct = t > 0 ? (d / t) * 100 : 0;
    var count = t > 0 ? fmtCount(d) + " / " + fmtCount(t) : fmtCount(d);
    var tip = "Favourites · " + favouritesCenterLabel(p) + " · " + count;
    return circleHtml({
      slot: "favourites",
      iconHtml: '<span class="fav-lips"></span>',
      pct: pct,
      indeterminate: ind,
      tooltip: tip,
    });
  }

  function circleHealthScan() {
    if (typeof window === "undefined" || !window.__tsHealthScanning) return "";
    return circleHtml({
      slot: "health_scan",
      iconHtml: '<i class="fa-solid fa-heart-pulse"></i>',
      pct: 0,
      indeterminate: true,
      tooltip: "Scanning library",
    });
  }

  /**
   * Downloads circles — lowest priority. Renders one compact circle per
   * active download (capped server-side; overflow reported as
   * `d.extra`). The centre shows the matched performer's headshot when
   * available; otherwise the item's own progress percentage sits in
   * the middle so the caller can still read it at a glance. Ring fill
   * = that single item's progress_pct, so circles don't all tick
   * together.
   */
  function circleDownloads(d) {
    if (!d || !d.count) return "";
    var items = Array.isArray(d.items) ? d.items : [];
    if (!items.length) return "";
    var html = "";
    for (var i = 0; i < items.length; i++) {
      var it = items[i] || {};
      var pct = Math.max(0, Math.min(100, Number(it.pct) || 0));
      var ind = pct <= 0;
      var hsUrl = it.headshot_url || "";
      var hsLabel = (it.label || "").replace(/"/g, "&quot;");
      // With a matched library performer we render their headshot in
      // the centre. Without a match the ring itself is the only cue,
      // so drop the percent number into the middle instead of a
      // generic download icon — that way the user still sees *how
      // much* of the item has downloaded.
      var centerHtml;
      if (hsUrl) {
        centerHtml =
          '<img class="ts-circle-center-img" src="' + esc(hsUrl) + '" alt="' + hsLabel +
          '" onerror="this.outerHTML=\'<span class=&quot;ts-circle-pct&quot;>' +
          (ind ? "&middot;&middot;&middot;" : Math.round(pct)) +
          '</span>\'">';
      } else {
        centerHtml = ind
          ? '<span class="ts-circle-dots">&middot;&middot;&middot;</span>'
          : '<span class="ts-circle-pct">' + Math.round(pct) + '</span>';
      }
      var lines = [];
      if (it.label) lines.push(String(it.label));
      if (it.download_name) lines.push(String(it.download_name));
      lines.push("Download" + (pct > 0 ? " · " + Math.round(pct) + "%" : ""));
      var tip = lines.join("\n");
      html += circleHtml({
        slot: "downloads",
        iconHtml: '<i class="fa-solid fa-download"></i>',
        pct: pct,
        indeterminate: ind,
        tooltip: tip,
        centerHtml: centerHtml,
      });
    }
    var extra = Math.max(0, Number(d.extra) || 0);
    html += bannerMoreHtml(extra, extra + " more active downloads");
    return html;
  }

  function render(data) {
    var el = document.getElementById("tsActivityBanner");
    if (!el) return;

    var ix = (data && data.library_index) || {};
    var p3 = (data && data.phash3) || {};
    var fav = (data && data.favourites_index) || {};
    var dl = (data && data.downloads) || {};

    // Lowest priority = rendered last so it wraps to a new row first on
    // narrow headers. Downloads circle only appears when there are
    // active items; pipeline / library-index / phash / favourites all
    // beat it when several tasks run at once.
    var circlesHtml = "";
    circlesHtml += circlePipeline(data || {});
    circlesHtml += circleLibraryIndex(ix);
    circlesHtml += circlePhash3(p3);
    circlesHtml += circleFavourites(fav);
    circlesHtml += circleHealthScan();
    circlesHtml += circleDownloads(dl);

    // Notifications now render as small round icon buttons in the same
    // row as the progress circles. Hover (native tooltip) shows the
    // full message; click dismisses. Because they're compact they no
    // longer need to suppress or be suppressed by the progress circles
    // — both fit inside the fixed 60 px activity band.
    pruneClientAlerts();
    var serverNotes = (data && data.notifications) || [];
    var clientNotes = _clientAlerts.map(function (a) {
      return {
        id: a.id,
        message: a.message,
        kind: a.kind,
        _client: true,
        onAction: a.onAction,
      };
    });
    var allNotes = serverNotes.concat(clientNotes);
    var visibleNotes = allNotes.slice(0, MAX_VISIBLE_ALERTS);
    var queuedCount = Math.max(0, allNotes.length - visibleNotes.length);

    var notesHtml = "";
    for (var i = 0; i < visibleNotes.length; i++) {
      var n = visibleNotes[i];
      var id = n.id;
      var kc = kindClass(n.kind);
      var msgAttr = esc(n.message || "");
      var tip = msgAttr + (n.onAction ? " (click to act)" : " (click to dismiss)");
      if (n._client) {
        notesHtml +=
          '<button type="button" class="ts-banner-dot ' + kc + '"' +
          ' data-client-alert-id="' + esc(String(id)) + '"' +
          ' title="' + tip + '"' +
          ' aria-label="' + msgAttr + '">' +
          kindIconHtml(n.kind) +
          "</button>";
      } else {
        notesHtml +=
          '<button type="button" class="ts-banner-dot ' + kc + '"' +
          ' data-notification-id="' + esc(String(id)) + '"' +
          ' data-dismiss-id="' + esc(String(id)) + '"' +
          ' title="' + tip + '"' +
          ' aria-label="' + msgAttr + '">' +
          kindIconHtml(n.kind) +
          "</button>";
      }
    }
    notesHtml += bannerMoreHtml(queuedCount, queuedCount + " more queued", { live: true });

    var rowHtml = circlesHtml + notesHtml;
    el.innerHTML = rowHtml
      ? '<div class="ts-progress-circles">' + rowHtml + "</div>"
      : "";

    clearAllDismissTimers();
    for (var j = 0; j < visibleNotes.length; j++) {
      var vn = visibleNotes[j];
      if (!vn._client && vn.id != null) scheduleAutoDismiss(vn.id);
    }

    el.querySelectorAll("[data-dismiss-id]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var did = btn.getAttribute("data-dismiss-id");
        var nid = parseInt(did, 10);
        clearDismissTimer(nid);
        fetch("/api/activity/notifications/dismiss", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ id: nid }),
        }).then(function () {
          tick();
        });
      });
    });

    el.querySelectorAll("[data-client-alert-id]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var cid = btn.getAttribute("data-client-alert-id");
        var alert = _clientAlerts.find(function (a) {
          return a.id === cid;
        });
        if (alert && alert.onAction) {
          try {
            alert.onAction();
          } catch (e) {}
        }
        dismissClientAlert(cid);
      });
    });

    if (typeof window.syncCenterLogoVisibility === "function") {
      window.syncCenterLogoVisibility();
    }
  }

  //: Track the highest-seen notification id between ticks so each
  //: notification flashes the global status bar exactly once. Also
  //: remembers whether the pipeline was running on the previous tick
  //: so the "just-finished" success pulse can fire on the falling
  //: edge of ``running`` rather than once per poll.
  var _lastSeenNotifId = 0;
  var _prevWasRunning = false;
  var _lastFinishedAt = 0;
  var _initialPollDone = false;

  function _updateGlobalStatusBar(data) {
    if (typeof window.__tsSetStatusBar !== "function") return;
    //: Anything actively in flight (filing pipeline, library index,
    //: phash3 rescan, favourites index, active download) is enough
    //: to flip the bar to ``running``.
    var running =
      !!(data && data.running) ||
      !!(data && data.library_index && data.library_index.running) ||
      !!(data && data.phash3 && data.phash3.running) ||
      !!(data && data.favourites_index && data.favourites_index.running);
    if (!running && data && Array.isArray(data.downloads)) {
      for (var i = 0; i < data.downloads.length; i++) {
        var d = data.downloads[i] || {};
        if (d.running || (d.percent > 0 && d.percent < 100)) { running = true; break; }
      }
    }

    //: Base state: ``running`` (active work) > ``paused`` (recent
    //: work just completed) > ``idle`` (cold).
    //: ``paused`` is a transient resting state — sits in muted purple
    //: for ~30 seconds after work stops, then drops to idle blue.
    var baseState;
    if (running) {
      baseState = "running";
      _lastFinishedAt = 0;
    } else {
      if (_prevWasRunning) _lastFinishedAt = Date.now();
      var sinceFinish = _lastFinishedAt ? (Date.now() - _lastFinishedAt) : Infinity;
      baseState = sinceFinish < 30000 ? "paused" : "idle";
    }
    window.__tsSetStatusBar(baseState);

    //: Falling edge of ``running`` — fire a success pulse so the user
    //: gets a clean "✓ done" cue even if no success notification was
    //: emitted. Skipped on the initial poll when we don't know the
    //: prior state.
    if (_prevWasRunning && !running && _initialPollDone) {
      window.__tsSetStatusBar("success");
    }
    _prevWasRunning = running;
    _initialPollDone = true;

    //: Notification-driven flashes (error / warning / success). One
    //: flash per new notification, prioritising error > warning >
    //: success so a tick that surfaces multiple kinds picks the
    //: most attention-worthy.
    var notes = (data && data.notifications) || [];
    var maxId = _lastSeenNotifId;
    var sawNewError = false;
    var sawNewWarning = false;
    var sawNewSuccess = false;
    for (var j = 0; j < notes.length; j++) {
      var n = notes[j] || {};
      var nid = +n.id || 0;
      if (nid > maxId) maxId = nid;
      if (nid <= _lastSeenNotifId) continue;
      var kind = (n.kind || n.severity || n.level || "").toLowerCase();
      if (kind === "error" || kind === "danger") sawNewError = true;
      else if (kind === "warning" || kind === "warn") sawNewWarning = true;
      else if (kind === "success" || kind === "ok") sawNewSuccess = true;
    }
    _lastSeenNotifId = maxId;
    if (sawNewError) window.__tsSetStatusBar("error");
    else if (sawNewWarning) window.__tsSetStatusBar("warning");
    else if (sawNewSuccess) window.__tsSetStatusBar("success");
  }

  /** True when the last banner payload contains any in-flight progress
   * source (pipeline, library index, phash, favourites enrich,
   * downloads, etc.). Drives the adaptive poll cadence — when nothing
   * is running we drop to a 15 s heartbeat so idle queues stop firing
   * a request every 2 s. */
  function _bannerBusy(d) {
    if (!d) return false;
    if (d.pipeline && d.pipeline.running) return true;
    if (d.library_index && d.library_index.running) return true;
    if (d.phash3 && d.phash3.running) return true;
    if (d.favourites && d.favourites.running) return true;
    if (d.downloads && Array.isArray(d.downloads.items) && d.downloads.items.length) return true;
    if (Array.isArray(d.filing_now) && d.filing_now.length) return true;
    return false;
  }

  // Notify subscribers (currently queue.html) which filenames are
  // actively being copied to their destination — so the queue row can
  // shimmer for the duration. Comparison against the previous set
  // avoids redundant DOM writes on every tick.
  var _lastFilingNowKey = "";
  function _broadcastFilingNow(filingNow) {
    var list = Array.isArray(filingNow) ? filingNow : [];
    var key = list.slice().sort().join("\n");
    if (key === _lastFilingNowKey) return;
    _lastFilingNowKey = key;
    try {
      window.dispatchEvent(new CustomEvent("ts:filing-now", {
        detail: { filenames: list }
      }));
    } catch (_) {}
  }

  function tick() {
    fetch("/api/activity/banner", { cache: "no-store" })
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        _lastBannerData = data || {};
        render(_lastBannerData);
        _updateGlobalStatusBar(_lastBannerData);
        _broadcastFilingNow(_lastBannerData.filing_now);
        _maybeAdaptInterval(_bannerBusy(_lastBannerData));
      })
      .catch(function () {});
  }

  window.TsActivity = {
    refresh: tick,
    /** Optional: /health page sets while NFO scan runs */
    setHealthScanning: function (v) {
      if (typeof window !== "undefined") window.__tsHealthScanning = !!v;
      tick();
    },
    /** Client alert (header dot). Used by window.toast. */
    pushAlert: pushAlert,
    /** @deprecated use pushAlert */
    pushLocal: function (message, kind) {
      pushAlert(message, { kind: kind });
    },
  };

  /** Two cadences:
   *  • POLL_MS_BUSY = 2 s — pipeline / index / phash / favourites
   *    enrich / downloads queue actively running.
   *  • POLL_MS_IDLE = 15 s — nothing in-flight; heartbeat that
   *    keeps the banner ready for new alerts without burning a
   *    request every 2 s.
   * Switching is done in _maybeAdaptInterval() after each tick. */
  var POLL_MS_BUSY = 2000;
  var POLL_MS_IDLE = 15000;
  var pollTimer = null;
  var pollIntervalMs = POLL_MS_BUSY;  // start optimistic — first tick adapts.

  function _setPollInterval(ms) {
    if (ms === pollIntervalMs && pollTimer) return;
    pollIntervalMs = ms;
    if (pollTimer) {
      clearInterval(pollTimer);
      pollTimer = setInterval(function () {
        if (document.visibilityState === "hidden") return;
        tick();
      }, pollIntervalMs);
    }
  }

  function _maybeAdaptInterval(busy) {
    var target = busy ? POLL_MS_BUSY : POLL_MS_IDLE;
    if (target !== pollIntervalMs) _setPollInterval(target);
  }

  function startPolling() {
    if (pollTimer) return;
    if (typeof window.__tsFlushPendingToasts === "function") {
      window.__tsFlushPendingToasts();
    }
    tick();
    pollTimer = setInterval(function () {
      if (document.visibilityState === "hidden") return;
      tick();
    }, pollIntervalMs);
  }

  function onVisibility() {
    if (document.visibilityState === "visible") {
      // Coming back to the tab — always fast on resume so the user
      // sees fresh state immediately; the next tick adapts to idle if
      // nothing's running.
      _setPollInterval(POLL_MS_BUSY);
      tick();
    }
  }

  function stopPolling() {
    if (pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  }

  window.addEventListener("pagehide", stopPolling);

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function () {
      startPolling();
      document.addEventListener("visibilitychange", onVisibility);
    });
  } else {
    startPolling();
    document.addEventListener("visibilitychange", onVisibility);
  }
})();
