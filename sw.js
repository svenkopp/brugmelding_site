const CACHE = "brugmelding-v1";

self.addEventListener("install", (evt) => {
  evt.waitUntil(
    caches.open(CACHE).then(cache => {
      return cache.addAll([
        "/",
        "/index.html",
        "/manifest.webmanifest",
        "/brug_open.png",
        "/brug_dicht.png",
        "/favicon.ico"
      ]);
    })
  );
});

self.addEventListener("activate", (evt) => {
  evt.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    )
  );
});

self.addEventListener("fetch", (evt) => {
  evt.respondWith(
    caches.match(evt.request).then(
      (cached) => cached || fetch(evt.request)
    )
  );
});
