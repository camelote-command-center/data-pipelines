/**
 * TinJob — Cloudflare Worker
 * Routes content pages, sitemap, robots.txt, and llms.txt to Supabase Edge Functions.
 * All other paths pass through to Lovable origin.
 */

const SUPABASE_FUNCTIONS_URL = "https://znrvddgmczdqoucmykij.supabase.co/functions/v1";
const LOVABLE_ORIGIN = "https://job-swipe-pro-49.lovable.app";

// Content paths that should be pre-rendered
const CONTENT_PATH_PATTERNS = [
  /^\/blog$/,
  /^\/blog\/.+/,
  /^\/faq$/,
  /^\/faq\/.+/,
  /^\/glossaire$/,
  /^\/glossaire\/.+/,
  /^\/vs\/.+/,
];

function isContentPath(pathname) {
  return CONTENT_PATH_PATTERNS.some((pattern) => pattern.test(pathname));
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const pathname = url.pathname;

    // --- sitemap.xml ---
    if (pathname === "/sitemap.xml") {
      const response = await fetch(`${SUPABASE_FUNCTIONS_URL}/generate-sitemap`);
      return new Response(await response.text(), {
        status: response.status,
        headers: {
          "Content-Type": "application/xml; charset=utf-8",
          "Cache-Control": "public, max-age=3600, s-maxage=3600",
          "X-Robots-Tag": "noindex",
        },
      });
    }

    // --- robots.txt ---
    if (pathname === "/robots.txt") {
      const response = await fetch(`${SUPABASE_FUNCTIONS_URL}/serve-robots-txt?type=robots`);
      return new Response(await response.text(), {
        status: response.status,
        headers: {
          "Content-Type": "text/plain; charset=utf-8",
          "Cache-Control": "public, max-age=86400, s-maxage=86400",
        },
      });
    }

    // --- llms.txt ---
    if (pathname === "/llms.txt") {
      const response = await fetch(`${SUPABASE_FUNCTIONS_URL}/serve-robots-txt?type=llms`);
      return new Response(await response.text(), {
        status: response.status,
        headers: {
          "Content-Type": "text/plain; charset=utf-8",
          "Cache-Control": "public, max-age=3600, s-maxage=3600",
        },
      });
    }

    // --- Content pages: serve pre-rendered HTML to EVERYONE ---
    if (isContentPath(pathname)) {
      const renderUrl = `${SUPABASE_FUNCTIONS_URL}/render-page?path=${encodeURIComponent(pathname)}`;
      const response = await fetch(renderUrl);

      if (response.ok) {
        return new Response(await response.text(), {
          status: 200,
          headers: {
            "Content-Type": "text/html; charset=utf-8",
            "Cache-Control": "public, max-age=300, s-maxage=600",
          },
        });
      }

      // If render-page returns 404 (e.g. unpublished slug), fall through to SPA
    }

    // --- All other paths: proxy to Lovable origin ---
    const originUrl = new URL(pathname + url.search, LOVABLE_ORIGIN);
    const originRequest = new Request(originUrl, {
      method: request.method,
      headers: request.headers,
      body: request.body,
      redirect: "follow",
    });
    return fetch(originRequest);
  },
};
