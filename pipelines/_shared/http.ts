/**
 * Shared HTTP client with proxy fallback and exponential backoff.
 *
 * Strategy: try DIRECT first. If blocked (403/503), retry via PROXY.
 * This avoids proxy issues (407, bad auth) when direct access works,
 * and only uses the proxy as a fallback when the site blocks us.
 *
 * Usage:
 *   import { httpFetch } from '../_shared/http.js';
 *   const res = await httpFetch('https://example.com', { headers: {...} });
 */

import { HttpsProxyAgent } from 'https-proxy-agent';

const PROXY_HOST = 'p.webshare.io';
const PROXY_PORT = 80;

let agent: HttpsProxyAgent<string> | undefined;
let proxyAvailable: boolean | null = null; // null = untested

function getAgent(): HttpsProxyAgent<string> | undefined {
  if (agent) return agent;

  const user = process.env.WEBSHARE_PROXY_USER;
  const pass = process.env.WEBSHARE_PROXY_PASS;
  if (!user || !pass) return undefined;

  agent = new HttpsProxyAgent(`http://${user}:${pass}@${PROXY_HOST}:${PROXY_PORT}`);
  return agent;
}

async function fetchViaProxy(url: string, init?: any): Promise<Response | null> {
  const proxyAgent = getAgent();
  if (!proxyAgent || proxyAvailable === false) return null;

  try {
    const nodeFetch = (await import('node-fetch' as any)).default;
    const res = (await nodeFetch(url, { ...init, agent: proxyAgent })) as unknown as Response;

    if (res.status === 407) {
      console.log('  Proxy auth failed (407) — disabling proxy for this run');
      proxyAvailable = false;
      return null;
    }

    if (proxyAvailable === null) {
      console.log('  Proxy active: Webshare rotating proxy');
      proxyAvailable = true;
    }
    return res;
  } catch (err: any) {
    console.log(`  Proxy error: ${err.message} — falling back to direct`);
    proxyAvailable = false;
    return null;
  }
}

/**
 * Fetch with exponential backoff retry.
 * On blocking responses (403/503), automatically falls back to proxy.
 */
export async function httpFetch(
  url: string,
  init?: RequestInit & { retry?: number; maxRetries?: number },
): Promise<Response> {
  const retry = init?.retry ?? 0;
  const maxRetries = init?.maxRetries ?? 8;

  try {
    // 1. Try direct
    const res = await fetch(url, init);

    // 2. If blocked, try proxy before retrying
    if ([403, 503].includes(res.status) && retry === 0) {
      const proxyRes = await fetchViaProxy(url, init);
      if (proxyRes && proxyRes.ok) return proxyRes;
      // Proxy also failed or unavailable — continue with retry logic
    }

    // 3. Retry on transient errors
    if ([429, 500, 502, 503].includes(res.status)) {
      if (retry >= maxRetries) return res;

      const backoff = Math.min(120_000, 3_000 * Math.pow(2, retry));
      console.log(`  HTTP ${res.status} on ${url.split('?')[0]}, retry ${retry + 1}/${maxRetries} in ${(backoff / 1000).toFixed(0)}s...`);
      await new Promise((r) => setTimeout(r, backoff));

      // On retry, try proxy if available
      const proxyRes = await fetchViaProxy(url, init);
      if (proxyRes && proxyRes.ok) return proxyRes;

      return httpFetch(url, { ...init, retry: retry + 1, maxRetries } as any);
    }

    return res;
  } catch (err: any) {
    // Network error — try proxy
    const proxyRes = await fetchViaProxy(url, init);
    if (proxyRes) return proxyRes;

    if (retry >= maxRetries) throw err;

    const backoff = Math.min(120_000, 3_000 * Math.pow(2, retry));
    console.log(`  Network error: ${err.message}, retry ${retry + 1}/${maxRetries} in ${(backoff / 1000).toFixed(0)}s...`);
    await new Promise((r) => setTimeout(r, backoff));
    return httpFetch(url, { ...init, retry: retry + 1, maxRetries } as any);
  }
}
