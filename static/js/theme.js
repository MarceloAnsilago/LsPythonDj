// static/js/theme.js
(function () {
  const STORAGE_KEY = "ls_theme";

  function applyTheme(theme) {
    const html = document.documentElement;
    html.setAttribute("data-bs-theme", theme);

    try { localStorage.setItem(STORAGE_KEY, theme); } catch(e) {}

    // badge opcional no sidebar
    const badge = document.getElementById("themeBadge");
    if (badge) {
      badge.textContent = theme;
      badge.className = "badge " + (theme === "dark" ? "text-bg-dark" : "text-bg-secondary");
    }

    // ícone do botão
    const btn = document.getElementById("themeToggle");
    if (btn) {
      const icon = btn.querySelector("i");
      if (icon) icon.className = theme === "dark" ? "bi bi-sun" : "bi bi-moon-stars";
    }
  }

  function getInitialTheme() {
    const saved = localStorage.getItem(STORAGE_KEY);
    if (saved) return saved;
    // fallback: preferência do sistema
    return window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches
      ? "dark" : "light";
  }

  // aplica imediatamente (antes de DOMContentLoaded) para evitar flash
  applyTheme(getInitialTheme());

  // liga o botão quando o DOM estiver pronto
  window.addEventListener("DOMContentLoaded", () => {
    const btn = document.getElementById("themeToggle");
    if (!btn) return;
    btn.addEventListener("click", () => {
      const current = document.documentElement.getAttribute("data-bs-theme") || "light";
      const next = current === "dark" ? "light" : "dark";
      applyTheme(next);
    });
  });
})();
