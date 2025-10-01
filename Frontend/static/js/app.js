document.addEventListener("DOMContentLoaded", () => {
  console.log("Frontend listo");
  const flashes = document.querySelectorAll(".flash");
  flashes.forEach((flash) => {
    setTimeout(() => {
      flash.classList.add("is-hidden");
    }, 5000);
  });
});
