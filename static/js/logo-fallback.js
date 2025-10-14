// static/js/logo-fallback.js
(function () {
  function onErr(e) {
    const img = e.currentTarget || e; // permite chamada direta
    const base = img.dataset.base || "";
    const prefix = img.dataset.prefix || "";
    const fallback = img.dataset.fallback || "sem_logo.png";
    const step = img.dataset.step || "jpg";

    if (step === "jpg") {
      img.dataset.step = "png";
      img.src = base + prefix + ".png";     // tenta .png
      return;
    }
    if (step === "png") {
      img.dataset.step = "fallback";
      img.removeEventListener("error", onErr); // evita loop
      img.src = base + fallback;             // cai no sem_logo
      return;
    }
    img.removeEventListener("error", onErr);
  }

  function arm(img) {
    if (!img.dataset.step) img.dataset.step = "jpg";
    img.addEventListener("error", onErr);
    // se já falhou antes do listener, força o fallback agora
    if (img.complete && img.naturalWidth === 0) onErr({ currentTarget: img });
  }

  function init() {
    document.querySelectorAll("img.js-asset-logo").forEach(arm);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
