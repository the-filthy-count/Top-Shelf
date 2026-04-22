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
    var centerHtml;
    if (complete) {
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

  function render(data) {
    var el = document.getElementById("tsActivityBanner");
    if (!el) return;

    var ix = (data && data.library_index) || {};
    var p3 = (data && data.phash3) || {};
    var fav = (data && data.favourites_index) || {};

    var circlesHtml = "";
    circlesHtml += circlePipeline(data || {});
    circlesHtml += circleLibraryIndex(ix);
    circlesHtml += circlePhash3(p3);
    circlesHtml += circleFavourites(fav);
    circlesHtml += circleHealthScan();

    var notes = (data && data.notifications) || [];
    var visibleNotes = notes.slice(0, MAX_VISIBLE_ALERTS);
    var queuedCount = Math.max(0, notes.length - visibleNotes.length);

    var notesHtml = "";
    for (var i = 0; i < visibleNotes.length; i++) {
      var n = visibleNotes[i];
      var id = n.id;
      var msg = esc(n.message || "");
      var kc = kindClass(n.kind);
      notesHtml +=
        '<span class="ts-banner-note ' +
        kc +
        '" data-notification-id="' +
        esc(String(id)) +
        '">' +
        msg +
        '<button type="button" class="ts-banner-dismiss" title="Dismiss" aria-label="Dismiss" data-dismiss-id="' +
        esc(String(id)) +
        '">&times;</button></span>';
    }
    if (queuedCount > 0) {
      notesHtml +=
        '<span class="ts-banner-queue-hint" aria-live="polite">' +
        esc(String(queuedCount) + " more queued") +
        "</span>";
    }

    var inner = "";
    if (circlesHtml) {
      inner += '<div class="ts-progress-circles">' + circlesHtml + "</div>";
    }
    if (notesHtml) {
      inner += '<div class="ts-banner-notifications">' + notesHtml + "</div>";
    }

    el.innerHTML = inner;

    clearAllDismissTimers();
    for (var j = 0; j < visibleNotes.length; j++) {
      if (visibleNotes[j].id != null) scheduleAutoDismiss(visibleNotes[j].id);
    }

    el.querySelectorAll("button[data-dismiss-id]").forEach(function (btn) {
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
    /** Short-lived client hint; next poll replaces with server state */
    pushLocal: function (message) {
      var el = document.getElementById("tsActivityBanner");
      if (!el || !message) return;
      var wrap = el.querySelector(".ts-banner-notifications");
      if (!wrap) {
        var n = document.createElement("div");
        n.className = "ts-banner-notifications";
        el.appendChild(n);
        wrap = n;
      }
      var span = document.createElement("span");
      span.className = "ts-banner-note";
      span.textContent = message;
      wrap.appendChild(span);
      var notesOnly = wrap.querySelectorAll(".ts-banner-note");
      while (notesOnly.length > MAX_VISIBLE_ALERTS) {
        var first = notesOnly[0];
        if (first && first.parentNode) first.parentNode.removeChild(first);
        notesOnly = wrap.querySelectorAll(".ts-banner-note");
      }
      setTimeout(function () {
        try {
          if (span.parentNode) span.parentNode.removeChild(span);
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
