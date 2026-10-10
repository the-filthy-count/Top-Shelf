/**
 * Click [data-ts-help-template="<id>"] → open native <dialog> with template content.
 * window.TsHelp.openText(string) for plain text (e.g. Library Health metrics).
 */
(function () {
  function ensureDialog() {
    var d = document.getElementById("tsHelpDialog");
    if (d) return d;
    d = document.createElement("dialog");
    d.id = "tsHelpDialog";
    d.className = "ts-help-dialog";
    d.setAttribute("aria-labelledby", "tsHelpDialogTitle");
    d.innerHTML =
      '<div class="ts-help-dialog-box">' +
      '<h2 class="ts-help-dialog-heading" id="tsHelpDialogTitle">Information</h2>' +
      '<div id="tsHelpBody" class="ts-help-body"></div>' +
      '<div class="ts-help-dialog-actions">' +
      '<button type="button" class="btn-primary" id="tsHelpCloseBtn">Close</button>' +
      "</div></div>";
    document.body.appendChild(d);
    d.querySelector("#tsHelpCloseBtn").addEventListener("click", function () {
      d.close();
    });
    d.addEventListener("click", function (ev) {
      if (ev.target === d) d.close();
    });
    return d;
  }

  function openFromTemplate(templateId) {
    var t = document.getElementById(templateId);
    if (!t || !t.content) return;
    var d = ensureDialog();
    var body = document.getElementById("tsHelpBody");
    body.replaceChildren();
    body.appendChild(t.content.cloneNode(true));
    d.showModal();
  }

  document.addEventListener("click", function (e) {
    var btn = e.target.closest("[data-ts-help-template]");
    if (!btn) return;
    e.preventDefault();
    var id = btn.getAttribute("data-ts-help-template");
    if (id) openFromTemplate(id);
  });

  window.TsHelp = {
    openTemplate: openFromTemplate,
    openText: function (text) {
      if (text == null || String(text).trim() === "") return;
      var d = ensureDialog();
      var body = document.getElementById("tsHelpBody");
      body.replaceChildren();
      var wrap = document.createElement("div");
      wrap.className = "ts-help-prose";
      var p = document.createElement("p");
      p.style.whiteSpace = "pre-wrap";
      p.style.margin = "0";
      p.textContent = String(text);
      wrap.appendChild(p);
      body.appendChild(wrap);
      d.showModal();
    },
  };
})();
