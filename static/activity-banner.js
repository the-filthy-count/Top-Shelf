/**
 * Center header: Library Health–style progress bars + notifications (poll /api/activity/banner).
 * Optional: window.__tsHealthScanning — set by /health while NFO scan runs (client flag).
 */
(function () {
  var AUTO_DISMISS_MS = 20000;
  /** Max dismissible notification boxes; progress rows are uncapped and listed first. */
  var MAX_VISIBLE_ALERTS = 2;
  var _dismissTimers = {};

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
    if (k === "library") return "ts-banner-library";
    if (k === "pipeline") return "ts-banner-pipeline";
    return "";
  }

  /** FontAwesome icon class for an alert kind — drives the dot button. */
  function kindIconHtml(kind) {
    var k = String(kind || "info").toLowerCase();
    if (k === "error")    return '<i class="fa-solid fa-triangle-exclamation"></i>';
    if (k === "library")  return '<i class="fa-solid fa-ticket"></i>';
    if (k === "pipeline") return '<i class="fa-solid fa-list"></i>';
    return '<i class="fa-solid fa-check"></i>';  // default: tick
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
    if (extra > 0) {
      html +=
        '<span class="ts-banner-dot-more" title="' +
        esc(String(extra) + " more active downloads") +
        '">+' + esc(String(extra)) + '</span>';
    }
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
    var notes = (data && data.notifications) || [];
    var visibleNotes = notes.slice(0, MAX_VISIBLE_ALERTS);
    var queuedCount = Math.max(0, notes.length - visibleNotes.length);

    var notesHtml = "";
    for (var i = 0; i < visibleNotes.length; i++) {
      var n = visibleNotes[i];
      var id = n.id;
      var kc = kindClass(n.kind);
      var msgAttr = esc(n.message || "");
      notesHtml +=
        '<button type="button" class="ts-banner-dot ' + kc + '"' +
        ' data-notification-id="' + esc(String(id)) + '"' +
        ' data-dismiss-id="' + esc(String(id)) + '"' +
        ' title="' + msgAttr + ' (click to dismiss)"' +
        ' aria-label="' + msgAttr + '">' +
        kindIconHtml(n.kind) +
        "</button>";
    }
    if (queuedCount > 0) {
      notesHtml +=
        '<span class="ts-banner-dot-more" aria-live="polite" title="' +
        esc(String(queuedCount) + " more queued") +
        '">+' + esc(String(queuedCount)) + "</span>";
    }

    var rowHtml = circlesHtml + notesHtml;
    el.innerHTML = rowHtml
      ? '<div class="ts-progress-circles">' + rowHtml + "</div>"
      : "";

    clearAllDismissTimers();
    for (var j = 0; j < visibleNotes.length; j++) {
      if (visibleNotes[j].id != null) scheduleAutoDismiss(visibleNotes[j].id);
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
  }

  function tick() {
    fetch("/api/activity/banner", { cache: "no-store" })
      .then(function (r) {
        return r.json();
      })
      .then(render)
      .catch(function () {});
  }

  window.TsActivity = {
    refresh: tick,
    /** Optional: /health page sets while NFO scan runs */
    setHealthScanning: function (v) {
      if (typeof window !== "undefined") window.__tsHealthScanning = !!v;
      tick();
    },
    /** Short-lived client hint; next poll replaces with server state.
     *  Renders as a small info-dot in the banner matching the new
     *  round-button style — message shows on hover via `title`. */
    pushLocal: function (message, kind) {
      var el = document.getElementById("tsActivityBanner");
      if (!el || !message) return;
      var wrap = el.querySelector(".ts-progress-circles");
      if (!wrap) {
        var n = document.createElement("div");
        n.className = "ts-progress-circles";
        el.appendChild(n);
        wrap = n;
      }
      var btn = document.createElement("button");
      btn.type = "button";
      btn.className = "ts-banner-dot " + kindClass(kind);
      btn.title = String(message);
      btn.setAttribute("aria-label", String(message));
      btn.innerHTML = kindIconHtml(kind);
      // Click to dismiss early — otherwise auto-cleans after the timer.
      btn.addEventListener("click", function () {
        try { if (btn.parentNode) btn.parentNode.removeChild(btn); } catch (e) {}
      });
      wrap.appendChild(btn);
      var dotsOnly = wrap.querySelectorAll(".ts-banner-dot");
      while (dotsOnly.length > MAX_VISIBLE_ALERTS) {
        var first = dotsOnly[0];
        if (first && first.parentNode) first.parentNode.removeChild(first);
        dotsOnly = wrap.querySelectorAll(".ts-banner-dot");
      }
      setTimeout(function () {
        try {
          if (btn.parentNode) btn.parentNode.removeChild(btn);
        } catch (e) {}
      }, AUTO_DISMISS_MS);
    },
  };

  var POLL_MS = 2000;
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function () {
      tick();
      setInterval(tick, POLL_MS);
    });
  } else {
    tick();
    setInterval(tick, POLL_MS);
  }
})();
