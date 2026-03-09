/**
 * WebShare rotating proxy helper.
 *
 * Env vars:
 *   WEBSHARE_PROXY_USER  - proxy username
 *   WEBSHARE_PROXY_PASS  - proxy password
 */

import { HttpsProxyAgent } from 'https-proxy-agent';

const PROXY_HOST = 'p.webshare.io';
const PROXY_PORT = 80;

export function proxyUrl(): string {
  const user = process.env.WEBSHARE_PROXY_USER;
  const pass = process.env.WEBSHARE_PROXY_PASS;

  if (!user || !pass) {
    throw new Error('WEBSHARE_PROXY_USER and WEBSHARE_PROXY_PASS are required');
  }

  return `http://${user}:${pass}@${PROXY_HOST}:${PROXY_PORT}`;
}

export function proxyAgent(): HttpsProxyAgent<string> {
  return new HttpsProxyAgent(proxyUrl());
}
