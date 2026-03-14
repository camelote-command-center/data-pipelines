/**
 * Shared FAO (fao.ge.ch) session helper.
 *
 * Handles CAPTCHA solving via Playwright + 2Captcha for rubrique 137 (transactions)
 * and rubrique 168 (LDTR). Returns session cookies for use with plain HTTP requests.
 *
 * Env vars:
 *   WEBSHARE_PROXY_USER  - proxy username
 *   WEBSHARE_PROXY_PASS  - proxy password
 *   TWO_CAPTCHA_API_KEY  - 2Captcha API key
 */

import { chromium, type Browser } from 'playwright';
import { solveCaptcha } from './captcha.js';
import { proxyUrl } from './proxy.js';

const MAX_CAPTCHA_RETRIES = 5;
const GOTO_TIMEOUT_MS = 120_000;
const BACKOFF_DELAYS_MS = [5_000, 15_000, 30_000, 60_000, 60_000];

export interface FaoSessionResult {
  cookies: string;
}

/**
 * Create an authenticated FAO session by solving the CAPTCHA.
 * Returns cookie string for use in subsequent HTTP requests.
 */
export async function createFaoSession(
  rubrique: number,
  dateFrom = '',
  dateTo = '',
): Promise<FaoSessionResult> {
  const url = `https://fao.ge.ch/recherche?resultsPerPage=50&rubrique=${rubrique}&dateFrom=${dateFrom}&dateTo=${dateTo}&type=exact&mot-cle=&exclude=&page=1`;

  let retries = 0;

  while (retries < MAX_CAPTCHA_RETRIES) {
    let browser: Browser | null = null;

    try {
      // Proxy is optional — if env vars not set, launch without proxy
      const useProxy = process.env.WEBSHARE_PROXY_USER && process.env.WEBSHARE_PROXY_PASS;

      if (useProxy) {
        const pUrl = proxyUrl();
        const proxyParts = new URL(pUrl);
        const proxyServer = proxyParts.port
          ? `${proxyParts.protocol}//${proxyParts.hostname}:${proxyParts.port}`
          : proxyParts.origin;
        console.log(`  Proxy server: ${proxyServer}`);

        browser = await chromium.launch({
          headless: true,
          args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-blink-features=AutomationControlled'],
          proxy: {
            server: proxyServer,
            username: proxyParts.username,
            password: proxyParts.password,
          },
        });
      } else {
        console.log('  (no proxy configured, launching direct)');
        browser = await chromium.launch({
          headless: true,
          args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-blink-features=AutomationControlled'],
        });
      }

      const context = await browser.newContext();
      await context.clearCookies();
      const page = await context.newPage();

      // Intercept CAPTCHA image response
      let captchaText: string | null = null;

      page.on('response', async (response) => {
        const respUrl = response.url();
        if (respUrl.includes('captcha-handler?get=image')) {
          try {
            const buffer = await response.body();
            const base64 = buffer.toString('base64');
            captchaText = await solveCaptcha(base64);
          } catch (err) {
            console.error(`  CAPTCHA image intercept error: ${err}`);
          }
        }
      });

      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: GOTO_TIMEOUT_MS });

      // Check if we got redirected to CAPTCHA
      const currentUrl = page.url();
      if (!currentUrl.includes('captcha')) {
        // No CAPTCHA needed — extract cookies
        const rawCookies = await context.cookies();
        const cookieStr = rawCookies.map((c) => `${c.name}=${c.value}`).join('; ');
        await browser.close();
        return { cookies: cookieStr };
      }

      // Wait for CAPTCHA image selector
      try {
        await page.waitForSelector('#FAOCaptcha_CaptchaImage', {
          state: 'visible',
          timeout: 15_000,
        });
      } catch {
        console.log('  CAPTCHA image not found, retrying...');
        retries++;
        await browser.close();
        continue;
      }

      // Wait for 2Captcha to solve it
      const deadline = Date.now() + 60_000;
      while (!captchaText && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 1_000));
      }

      if (!captchaText) {
        console.log('  CAPTCHA not solved in time, retrying...');
        retries++;
        await browser.close();
        continue;
      }

      // Type CAPTCHA solution character by character (as the dev does)
      const inputSelector = 'input[name="fao_captcha[captchaCode]"]';
      await page.fill(inputSelector, '');
      await page.click(inputSelector);
      await page.waitForTimeout(2_000);

      for (const char of captchaText) {
        await page.keyboard.press(char);
        await page.waitForTimeout(200);
      }

      await page.click('#fao_captcha_submit');
      await page.waitForTimeout(2_000);

      // Check if CAPTCHA was accepted
      const afterUrl = page.url();
      if (!afterUrl.includes('captcha')) {
        const rawCookies = await context.cookies();
        const cookieStr = rawCookies.map((c) => `${c.name}=${c.value}`).join('; ');
        console.log('  FAO session established');
        await browser.close();
        return { cookies: cookieStr };
      }

      console.log(`  CAPTCHA rejected (attempt ${retries + 1}/${MAX_CAPTCHA_RETRIES})`);
      retries++;
      await browser.close();
    } catch (err) {
      console.error(`  FAO session error: ${err}`);
      retries++;
      if (browser) await browser.close();

      if (retries < MAX_CAPTCHA_RETRIES) {
        const delay = BACKOFF_DELAYS_MS[retries - 1] ?? 60_000;
        console.log(`  Waiting ${delay / 1_000}s before retry ${retries + 1}/${MAX_CAPTCHA_RETRIES}...`);
        await new Promise((r) => setTimeout(r, delay));
      }
    }
  }

  // Fallback: try once without proxy in case the proxy is the problem
  console.log('  All proxy attempts failed — trying direct connection as fallback...');
  let browser: Browser | null = null;
  try {
    browser = await chromium.launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-blink-features=AutomationControlled'],
    });

    const context = await browser.newContext();
    await context.clearCookies();
    const page = await context.newPage();

    let captchaText: string | null = null;
    page.on('response', async (response) => {
      const respUrl = response.url();
      if (respUrl.includes('captcha-handler?get=image')) {
        try {
          const buffer = await response.body();
          const base64 = buffer.toString('base64');
          captchaText = await solveCaptcha(base64);
        } catch (err) {
          console.error(`  CAPTCHA image intercept error: ${err}`);
        }
      }
    });

    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: GOTO_TIMEOUT_MS });

    const currentUrl = page.url();
    if (!currentUrl.includes('captcha')) {
      const rawCookies = await context.cookies();
      const cookieStr = rawCookies.map((c) => `${c.name}=${c.value}`).join('; ');
      await browser.close();
      console.log('  FAO session established (direct, no proxy)');
      return { cookies: cookieStr };
    }

    try {
      await page.waitForSelector('#FAOCaptcha_CaptchaImage', { state: 'visible', timeout: 15_000 });
    } catch {
      await browser.close();
      throw new Error(`Failed to create FAO session after ${MAX_CAPTCHA_RETRIES} proxy attempts + 1 direct attempt`);
    }

    const deadline = Date.now() + 60_000;
    while (!captchaText && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 1_000));
    }

    if (!captchaText) {
      await browser.close();
      throw new Error(`Failed to create FAO session after ${MAX_CAPTCHA_RETRIES} proxy attempts + 1 direct attempt`);
    }

    const inputSelector = 'input[name="fao_captcha[captchaCode]"]';
    await page.fill(inputSelector, '');
    await page.click(inputSelector);
    await page.waitForTimeout(2_000);

    for (const char of captchaText) {
      await page.keyboard.press(char);
      await page.waitForTimeout(200);
    }

    await page.click('#fao_captcha_submit');
    await page.waitForTimeout(2_000);

    const afterUrl = page.url();
    if (!afterUrl.includes('captcha')) {
      const rawCookies = await context.cookies();
      const cookieStr = rawCookies.map((c) => `${c.name}=${c.value}`).join('; ');
      console.log('  FAO session established (direct, no proxy)');
      await browser.close();
      return { cookies: cookieStr };
    }

    await browser.close();
  } catch (err) {
    if (browser) await browser.close();
    console.error(`  Direct fallback also failed: ${err}`);
  }

  throw new Error(`Failed to create FAO session after ${MAX_CAPTCHA_RETRIES} proxy attempts + 1 direct attempt`);
}

/**
 * Fetch a page from fao.ge.ch with session cookies, via proxy.
 * If redirected to CAPTCHA, re-solves it automatically.
 */
export async function faoFetch(
  url: string,
  cookies: string,
  proxyAgentInstance?: import('https-proxy-agent').HttpsProxyAgent<string>,
): Promise<string> {
  const response = await fetch(url, {
    headers: {
      Cookie: cookies,
      'User-Agent':
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    },
    // @ts-ignore — Node 18+ supports dispatcher for proxy
    ...(proxyAgentInstance ? { dispatcher: proxyAgentInstance } : {}),
  });

  const text = await response.text();

  // Check if we were redirected to CAPTCHA page
  if (response.url.includes('captcha') || text.includes('FAOCaptcha_CaptchaImage')) {
    throw new Error('CAPTCHA_REDIRECT');
  }

  return text;
}
