/**
 * Live availability check for employee 6-digit login_code (debounced).
 * Requires GET endpoint returning { ok, checked, available }.
 */
(function (global) {
  function wireLoginCodeAvailability(opts) {
    var checkUrl = opts.checkUrl;
    var form = opts.form;
    var input = opts.input;
    var statusEl = opts.statusEl;
    var getExceptId = opts.getExceptId || function () {
      return null;
    };
    var beforeSubmit = opts.beforeSubmit;

    if (!checkUrl || !form || !input || !statusEl) return {};

    var submitting = false;
    var timer = null;
    var seq = 0;
    var lastAvailable = null;

    function buildUrl(code) {
      var q = "?code=" + encodeURIComponent(code);
      var ex = getExceptId();
      if (ex) q += "&except_id=" + encodeURIComponent(ex);
      return checkUrl + q;
    }

    function setStatus(text, cls) {
      statusEl.textContent = text || "";
      statusEl.className = "hint login-code-check-msg " + (cls || "");
    }

    function applyResult(available) {
      lastAvailable = available;
      input.classList.remove("login-code-input--taken");
      if (available === true) {
        setStatus("This login code is available.", "is-available");
        input.setCustomValidity("");
      } else if (available === false) {
        input.classList.add("login-code-input--taken");
        setStatus("This login code is already in use. Choose another.", "is-taken");
        input.setCustomValidity("This login code is already in use.");
      } else {
        setStatus("", "");
        input.setCustomValidity("");
      }
    }

    function runCheck() {
      var code = (input.value || "").trim();
      if (!/^\d{6}$/.test(code)) {
        seq++;
        applyResult(null);
        return Promise.resolve(null);
      }
      var mySeq = ++seq;
      setStatus("Checking…", "is-wait");
      input.setCustomValidity("");
      input.classList.remove("login-code-input--taken");
      return fetch(buildUrl(code), { credentials: "same-origin" })
        .then(function (r) {
          return r.json();
        })
        .then(function (data) {
          if (mySeq !== seq) return null;
          if (!data || !data.ok || !data.checked) {
            setStatus("", "");
            input.setCustomValidity("");
            input.classList.remove("login-code-input--taken");
            return null;
          }
          applyResult(data.available);
          if (data.available === true) return true;
          if (data.available === false) return false;
          return null;
        })
        .catch(function () {
          if (mySeq !== seq) return null;
          setStatus("", "");
          input.setCustomValidity("");
          input.classList.remove("login-code-input--taken");
          return null;
        });
    }

    input.addEventListener("input", function () {
      input.setCustomValidity("");
      input.classList.remove("login-code-input--taken");
      clearTimeout(timer);
      var code = (input.value || "").trim();
      if (!/^\d{6}$/.test(code)) {
        seq++;
        applyResult(null);
        lastAvailable = null;
        return;
      }
      timer = setTimeout(runCheck, 380);
    });

    form.addEventListener("submit", function (e) {
      if (submitting) return;

      if (typeof beforeSubmit === "function") {
        var ok = beforeSubmit(e);
        if (ok === false) return;
      }

      var code = (input.value || "").trim();
      if (!/^\d{6}$/.test(code)) return;

      if (lastAvailable === false) {
        e.preventDefault();
        input.reportValidity();
        return;
      }

      if (lastAvailable !== true) {
        e.preventDefault();
        runCheck().then(function (av) {
          if (av === true || av === null) {
            submitting = true;
            HTMLFormElement.prototype.submit.call(form);
          } else if (av === false) {
            input.reportValidity();
          }
        });
      }
    });

    return { recheck: runCheck };
  }

  global.wireLoginCodeAvailability = wireLoginCodeAvailability;
})(typeof window !== "undefined" ? window : this);
