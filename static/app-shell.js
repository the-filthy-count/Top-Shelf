(function () {
  var SPLASH_ID = "bootSplash";
  var MIN_VISIBLE_MS = 480;
  var HEALTH_TIMEOUT_MS = 15000;

  function ensureSplash() {
    if (document.getElementById(SPLASH_ID)) {
      return;
    }
    var el = document.createElement("div");
    el.id = SPLASH_ID;
    el.className = "boot-splash";
    el.setAttribute("aria-busy", "true");
    el.setAttribute("aria-live", "polite");
    el.setAttribute("aria-label", "Loading application");
    el.innerHTML =
      '<div class="boot-splash__inner">' +
      '<img class="boot-splash__logo" src="/static/logo.png" alt="Top-Shelf" width="140" height="48">' +
      '<div class="boot-splash__spinner" aria-hidden="true"></div>' +
      "</div>";
    document.body.insertBefore(el, document.body.firstChild);
  }

  function dismissSplash() {
    var el = document.getElementById(SPLASH_ID);
    if (!el) {
      return;
    }
    el.classList.add("boot-splash--hide");
    el.setAttribute("aria-busy", "false");
    setTimeout(function () {
      if (el.parentNode) {
        el.parentNode.removeChild(el);
      }
    }, 420);
  }

  function runBootSplash() {
    ensureSplash();
    var t0 = Date.now();
    function finish() {
      var wait = Math.max(0, MIN_VISIBLE_MS - (Date.now() - t0));
      setTimeout(dismissSplash, wait);
    }
    var req = fetch("/api/health", { method: "GET", cache: "no-store" });
    var timeout = new Promise(function (_, reject) {
      setTimeout(function () {
        reject(new Error("health timeout"));
      }, HEALTH_TIMEOUT_MS);
    });
    Promise.race([req, timeout]).then(finish, finish);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", runBootSplash);
  } else {
    runBootSplash();
  }
})();

(function () {
  function normalizePath(path) {
    if (!path || path === "/") return "/queue";
    if (path === "/tv") return "/queue";
    if (path === "/metadata") return "/scenes";
    return path;
  }

  function getNavTarget(path) {
    const p = normalizePath(path);
    if (p === "/scenes") return "/scenes";
    if (p === "/movies") return "/movies";
    if (p === "/queue") return "/queue";
    if (p === "/downloads") return "/downloads";
    if (p === "/favourites") return "/favourites";
    if (p === "/library") return "/library";
    if (p === "/history") return "/history";
    if (p === "/log") return "/log";
    return "";
  }

  function markActiveNav() {
    const target = getNavTarget(window.location.pathname);
    if (!target) return;
    const navLinks = document.querySelectorAll(".nav-links a[href]");
    navLinks.forEach(function (link) {
      const href = link.getAttribute("href");
      const btn = link.querySelector(".btn-secondary");
      if (!btn) return;
      btn.classList.toggle("active-nav", href === target);
    });
  }

  function addPageClass() {
    const p = normalizePath(window.location.pathname).replace("/", "") || "queue";
    document.body.classList.add("page-" + p);
  }

  window.addEventListener("DOMContentLoaded", function () {
    addPageClass();
    markActiveNav();
  });
})();

window.addEventListener("unhandledrejection", function (event) {
  const msg = event.reason?.message || String(event.reason) || "Unknown error";
  // Only surface errors that look like network/API failures, not expected rejections
  if (msg.includes("fetch") || msg.includes("NetworkError") || /^[45]\d\d/.test(msg)) {
    console.error("Unhandled API error:", msg);
    // Show a subtle toast if the function exists on the page
    if (typeof showToast === "function") showToast("Request failed: " + msg, "error");
  }
});
