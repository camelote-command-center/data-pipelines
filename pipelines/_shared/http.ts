/**
 * Shared HTTP client with proxy fallback and exponential backoff.
 *
 * Strategy: try DIRECT first. If blocked (403/503), retry via PROXY.
 * This avoids proxy issues (407, bad auth) when direct access works,
 * and only uses the proxy as a fallback when the site blocks us.
 *
 * Two proxy flavours:
 *   - Datacenter (cheap, fast, often blocked) — Webshare "Proxy Server" plan
 *     env: WEBSHARE_PROXY_USER / WEBSHARE_PROXY_PASS
 *   - Residential (expensive, slower, defeats geo/datacenter blocks) —
 *     Webshare "Rotating Residential" plan
 *     env: WEBSHARE_RESIDENTIAL_USER / WEBSHARE_RESIDENTIAL_PASS / WEBSHARE_RESIDENTIAL_HOST
 *
 * Usage:
 *   import { httpFetch } from '../_shared/http.js';
 *   await httpFetch(url, { headers: {...} });                 // direct → datacenter fallback
 *   await httpFetch(url, { useResidential: true, ... });      // straight through residential
 */

import { HttpsProxyAgent } from 'https-proxy-agent';

const DC_PROXY_HOST = 'p.webshare.io';
const DC_PROXY_PORT = 80;

let dcAgent: HttpsProxyAgent<string> | undefined;
let dcAvailable: boolean | null = null;

let resAgent: HttpsProxyAgent<string> | undefined;
let resLogged = false;

function getDatacenterAgent(): HttpsProxyAgent<string> | undefined {
  if (dcAgent) return dcAgent;
  const user = process.env.WEBSHARE_PROXY_USER;
  const pass = process.env.WEBSHARE_PROXY_PASS;
  if (!user || !pass) return undefined;
  dcAgent = new HttpsProxyAgent(`http://${user}:${pass}@${DC_PROXY_HOST}:${DC_PROXY_PORT}`);
  return dcAgent;
}

export function getResidentialAgent(): HttpsProxyAgent<string> | undefined {
  if (resAgent) return resAgent;
  const user = process.env.WEBSHARE_RESIDENTIAL_USER;
  const pass = process.env.WEBSHARE_RESIDENTIAL_PASS;
  const host = process.env.WEBSHARE_RESIDENTIAL_HOST ?? `${DC_PROXY_HOST}:${DC_PROXY_PORT}`;
  if (!user || !pass) return undefined;
  resAgent = new HttpsProxyAgent(`http://${user}:${pass}@${host}`);
  if (!resLogged) {
    console.log(`  Residential proxy configured: ${host} (user ${user})`);
    resLogged = true;
  }
  return resAgent;
}

// Back-compat alias for callers that imported the old name.
function getAgent(): HttpsProxyAgent<string> | undefined {
  return getDatacenterAgent();
}

async function fetchViaProxy(url: string, init?: any): Promise<Response | null> {
  const proxyAgent = getDatacenterAgent();
  if (!proxyAgent || dcAvailable === false) return null;

  try {
    const nodeFetch = (await import('node-fetch' as any)).default;
    const res = (await nodeFetch(url, { ...init, agent: proxyAgent })) as unknown as Response;

    if (res.status === 407) {
      console.log('  Datacenter proxy auth failed (407) — disabling for this run');
      dcAvailable = false;
      return null;
    }

    if (dcAvailable === null) {
      console.log('  Proxy active: Webshare datacenter');
      dcAvailable = true;
    }
    return res;
  } catch (err: any) {
    console.log(`  Datacenter proxy error: ${err.message} — falling back to direct`);
    dcAvailable = false;
    return null;
  }
}

async function fetchViaResidential(url: string, init?: any): Promise<Response> {
  const ag = getResidentialAgent();
  if (!ag) {
    throw new Error(
      'Residential proxy required but WEBSHARE_RESIDENTIAL_USER/PASS not configured',
    );
  }
  const nodeFetch = (await import('node-fetch' as any)).default;
  return (await nodeFetch(url, { ...init, agent: ag })) as unknown as Response;
}

/**
 * Fetch with exponential backoff retry.
 * On blocking responses (403/503), automatically falls back to proxy.
 */
export async function httpFetch(
  url: string,
  init?: RequestInit & { retry?: number; maxRetries?: number; useResidential?: boolean },
): Promise<Response> {
  const retry = init?.retry ?? 0;
  const maxRetries = init?.maxRetries ?? 8;

  // Residential mode: skip the direct/datacenter ladder entirely.
  if (init?.useResidential) {
    const { useResidential, retry: _r, maxRetries: _m, ...rest } = init;
    return fetchViaResidential(url, rest);
  }

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
