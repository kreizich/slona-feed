const ORIGIN = "https://kreizich.github.io/slona-feed/data/latest.json";

const NO_CACHE_HEADERS = {
  "Cache-Control": "no-store, no-cache, must-revalidate",
  "Pragma": "no-cache",
  "Surrogate-Control": "no-store",
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "*",
};

export default {
  async fetch(request) {
    // CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: NO_CACHE_HEADERS });
    }

    if (request.method !== "GET") {
      return new Response("Method Not Allowed", { status: 405 });
    }

    try {
      // Bust every cache layer:
      // 1. ?_cf= query param defeats GitHub Pages CDN keying
      // 2. cf: { cacheEverything: false } tells Cloudflare not to cache
      // 3. Cache-Control: no-cache on the subrequest tells origin not to serve stale
      const upstream = await fetch(`${ORIGIN}?_cf=${Date.now()}`, {
        cf: {
          cacheEverything: false,
          cacheTtl: 0,
          cacheKey: `slona-feed-${Date.now()}`, // unique per request
        },
        headers: {
          "Cache-Control": "no-cache, no-store",
          "Pragma": "no-cache",
          "User-Agent": "slona-feed-worker/1.0",
        },
      });

      if (!upstream.ok) {
        return new Response(
          JSON.stringify({
            error: `Origin returned HTTP ${upstream.status}`,
            origin: ORIGIN,
            ts: new Date().toISOString(),
          }),
          {
            status: upstream.status,
            headers: { "Content-Type": "application/json", ...NO_CACHE_HEADERS },
          }
        );
      }

      const body = await upstream.arrayBuffer();

      return new Response(body, {
        status: 200,
        headers: {
          "Content-Type": "application/json; charset=utf-8",
          "X-Proxy": "slona-feed-worker",
          "X-Fetched-At": new Date().toISOString(),
          ...NO_CACHE_HEADERS,
        },
      });
    } catch (err) {
      return new Response(
        JSON.stringify({ error: err.message, ts: new Date().toISOString() }),
        {
          status: 502,
          headers: { "Content-Type": "application/json", ...NO_CACHE_HEADERS },
        }
      );
    }
  },
};
