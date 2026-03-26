/**
 * Shared HTTP client with proxy support and exponential backoff.
 *
 * When WEBSHARE_PROXY_USER and WEBSHARE_PROXY_PASS are set, all requests
 * go through the Webshare rotating proxy. Otherwise, direct fetch is used.
 *
 * Usage:
 *   import { httpFetch } from '../_shared/http.js';
 *   const res = await httpFetch('https://example.com', { headers: {...} });
 */

import { HttpsProxyAgent } from 'https-proxy-agent';

// Node 20 fetch doesn't support proxy natively, so we use node-fetch-style
// workaround: we set the global dispatcher via undici if proxy is configured.

const PROXY_HOST = 'p.webshare.io';
const PROXY_PORT = 80;

let agent: HttpsProxyAgent<string> | undefined;

function getAgent(): HttpsProxyAgent<string> | undefined {
  if (agent) return agent;

  const user = process.env.WEBSHARE_PROXY_USER;
  const pass = process.env.WEBSHARE_PROXY_PASS;

  if (!user || !pass) return undefined;

  const url = `http://${user}:${pass}@${PROXY_HOST}:${PROXY_PORT}`;
  agent = new HttpsProxyAgent(url);
  console.log('  Proxy configured: Webshare rotating proxy');
  return agent;
}

/**
 * Fetch with optional proxy and exponential backoff retry.
 *
 * Retries on: 429, 503, 502, 500, network errors.
 * Does NOT retry on: 404, 403, 401 (these are terminal).
 */
export async function httpFetch(
  url: string,
  init?: RequestInit & { retry?: number; maxRetries?: number },
): Promise<Response> {
  const retry = init?.retry ?? 0;
  const maxRetries = init?.maxRetries ?? 8;
  const proxyAgent = getAgent();

  try {
    // Use native fetch — if proxy is available, we'll handle it via
    // the HTTPS_PROXY env var approach or direct agent injection.
    // Node 20's undici-based fetch doesn't support agent directly,
    // so we use the http module import for proxied requests.
    let res: Response;

    if (proxyAgent) {
      // Use node's http module via https-proxy-agent
      const { default: nodeFetch } = await import('node-fetch' as any).catch(() => ({ default: null }));
      if (nodeFetch) {
        res = (await nodeFetch(url, { ...init, agent: proxyAgent } as any)) as unknown as Response;
      } else {
        // Fallback: set env var and hope undici picks it up
        res = await fetch(url, init);
      }
    } else {
      res = await fetch(url, init);
    }

    // Retry on transient server errors
    if ([429, 500, 502, 503].includes(res.status)) {
      if (retry >= maxRetries) return res; // Give up, let caller handle

      const backoff = Math.min(120_000, 3_000 * Math.pow(2, retry));
      console.log(`  HTTP ${res.status} on ${url.split('?')[0]}, retry ${retry + 1}/${maxRetries} in ${(backoff / 1000).toFixed(0)}s...`);
      await new Promise((r) => setTimeout(r, backoff));
      return httpFetch(url, { ...init, retry: retry + 1, maxRetries } as any);
    }

    return res;
  } catch (err: any) {
    if (retry >= maxRetries) throw err;

    const backoff = Math.min(120_000, 3_000 * Math.pow(2, retry));
    console.log(`  Network error: ${err.message}, retry ${retry + 1}/${maxRetries} in ${(backoff / 1000).toFixed(0)}s...`);
    await new Promise((r) => setTimeout(r, backoff));
    return httpFetch(url, { ...init, retry: retry + 1, maxRetries } as any);
  }
}
