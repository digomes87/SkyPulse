// Theme toggle and chart tooltips. No dependencies.
(() => {
  const root = document.documentElement;
  const read = () => { try { return localStorage.getItem("skypulse-theme"); } catch { return null; } };
  const write = (value) => { try { localStorage.setItem("skypulse-theme", value); } catch { /* storage unavailable */ } };
  const stored = read();
  if (stored === "light" || stored === "dark") root.dataset.theme = stored;

  document.addEventListener("DOMContentLoaded", () => {
    const button = document.querySelector(".theme");
    button?.addEventListener("click", () => {
      const dark = root.dataset.theme
        ? root.dataset.theme === "dark"
        : window.matchMedia("(prefers-color-scheme: dark)").matches;
      root.dataset.theme = dark ? "light" : "dark";
      write(root.dataset.theme);
    });

    const tooltip = document.querySelector(".tooltip");
    if (!tooltip) return;
    const show = (text, x, y) => {
      tooltip.textContent = text;
      tooltip.hidden = false;
      const width = tooltip.offsetWidth;
      tooltip.style.left = `${Math.min(x + 14, window.innerWidth - width - 12)}px`;
      tooltip.style.top = `${y + 16}px`;
    };
    const hide = () => { tooltip.hidden = true; };
    document.querySelectorAll("[data-tip]").forEach((mark) => {
      mark.addEventListener("pointermove", (event) => show(mark.dataset.tip, event.clientX, event.clientY));
      mark.addEventListener("pointerleave", hide);
      mark.addEventListener("focus", () => {
        const box = mark.getBoundingClientRect();
        show(mark.dataset.tip, box.left + box.width / 3, box.top + box.height / 2);
      });
      mark.addEventListener("blur", hide);
    });
  });
})();
