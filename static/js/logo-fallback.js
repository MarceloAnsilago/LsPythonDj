document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('img.logo-img').forEach(img => {
    const handler = () => {
      if (!img.dataset.triedPng) {
        img.dataset.triedPng = "1";
        img.src = img.dataset.png;          // tenta .png
      } else {
        img.onerror = null;
        img.src = img.dataset.fallback;     // cai no sem_logo
      }
    };
    img.addEventListener('error', handler);

    // Se a imagem já falhou antes do listener (naturalWidth 0), força o handler
    if (img.complete && img.naturalWidth === 0) handler();
  });
});
