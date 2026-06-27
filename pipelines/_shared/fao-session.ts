/**
 * Shared FAO (fao.ge.ch) session helper.
 *
 * fao.ge.ch protects search with Friendly Captcha (frc-captcha). This solves it
 * via Playwright + 2Captcha and returns session cookies for use with plain HTTP
 * requests.
 *
 * Flow:
 *   1. Load the search URL -> server redirects to /captcha?baseURL=... which
 *      hosts a Friendly Captcha widget (<div id="my-widget-mount" data-site-key>).
 *   2. Block the widget script so it can't overwrite our injected token.
 *   3. Solve the captcha with 2Captcha (FriendlyCaptchaTaskProxyless).
 *   4. Inject the token as <input name="frc-captcha-solution"> into #captcha-form
 *      and submit it -> server validates and redirects back to baseURL with a
 *      session cookie.
 *
 * Env vars:
 *   TWO_CAPTCHA_API_KEY  - 2Captcha API key
 */

import { chromium, type Browser, type BrowserContext } from 'playwright';
import { solveFriendlyCaptcha } from './captcha.js';

const MAX_CAPTCHA_RETRIES = 5;
const GOTO_TIMEOUT_MS = 120_000;
const BACKOFF_DELAYS_MS = [5_000, 15_000, 30_000, 60_000, 60_000];

// Requests matching this are aborted so the Friendly Captcha widget never runs
// (per 2Captcha guidance — a live widget overwrites the injected solution token).
const WIDGET_BLOCK_RE = /friendl|frc[-_.]|\/build\/captcha/i;

export interface FaoSessionResult {
  cookies: string;
}

async function cookieString(context: BrowserContext): Promise<string> {
  const raw = await context.cookies();
  return raw.map((c) => `${c.name}=${c.value}`).join('; ');
}

/**
 * Create an authenticated FAO session by solving the Friendly Captcha.
 * Returns a cookie string for use in subsequent HTTP requests.
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
      console.log('  Launching browser (direct connection)...');
      browser = await chromium.launch({
        headless: true,
        args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-blink-features=AutomationControlled'],
      });

      const context = await browser.newContext();
      await context.clearCookies();
      const page = await context.newPage();

      // Block the Friendly Captcha widget JS / API so it cannot overwrite the
      // token we inject. The document navigation (the POST) is never blocked.
      await page.route('**/*', (route) => {
        const req = route.request();
        const type = req.resourceType();
        if ((type === 'script' || type === 'xhr' || type === 'fetch') && WIDGET_BLOCK_RE.test(req.url())) {
          return route.abort();
        }
        return route.continue();
      });

      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: GOTO_TIMEOUT_MS });

      // No CAPTCHA challenge — we already have a usable session.
      if (!page.url().includes('captcha')) {
        const cookies = await cookieString(context);
        await browser.close();
        return { cookies };
      }

      // Read the Friendly Captcha site key from the widget mount.
      const mount = await page
        .waitForSelector('#my-widget-mount[data-site-key]', { state: 'attached', timeout: 15_000 })
        .catch(() => null);
      if (!mount) {
        console.log('  Captcha widget mount not found, retrying...');
        retries++;
        await browser.close();
        continue;
      }

      const siteKey = await mount.getAttribute('data-site-key');
      const pageUrl = page.url();
      if (!siteKey) {
        console.log('  No data-site-key on widget mount, retrying...');
        retries++;
        await browser.close();
        continue;
      }

      console.log(`  Solving Friendly Captcha (sitekey ${siteKey})...`);
      const token = await solveFriendlyCaptcha(pageUrl, siteKey);

      // Inject the solution as the hidden field the widget would normally create.
      // fao.ge.ch configures the FC v2 widget with formFieldName "frc-captcha-response"
      // (NOT the v1 default "frc-captcha-solution"), so the server reads that name.
      const injected = await page.evaluate((tok) => {
        const form = document.querySelector('#captcha-form');
        if (!form) return false;
        let input = form.querySelector('input[name="frc-captcha-response"]') as HTMLInputElement | null;
        if (!input) {
          input = document.createElement('input');
          input.type = 'hidden';
          input.name = 'frc-captcha-response';
          form.appendChild(input);
        }
        input.value = tok;
        return true;
      }, token);

      if (!injected) {
        console.log('  #captcha-form not found, retrying...');
        retries++;
        await browser.close();
        continue;
      }

      // Submit the form; the server validates the token and redirects to baseURL.
      await Promise.all([
        page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: GOTO_TIMEOUT_MS }).catch(() => {}),
        page.evaluate(() => (document.querySelector('#captcha-form') as HTMLFormElement | null)?.submit()),
      ]);
      await page.waitForTimeout(2_000);

      if (!page.url().includes('captcha')) {
        const cookies = await cookieString(context);
        console.log('  FAO session established (Friendly Captcha)');
        await browser.close();
        return { cookies };
      }

      console.log(`  Captcha rejected (attempt ${retries + 1}/${MAX_CAPTCHA_RETRIES})`);
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

  throw new Error(`Failed to create FAO session after ${MAX_CAPTCHA_RETRIES} attempts`);
}

/**
 * Fetch a page from fao.ge.ch with session cookies.
 * If redirected to CAPTCHA, throws CAPTCHA_REDIRECT error.
 */
export async function faoFetch(url: string, cookies: string): Promise<string> {
  const response = await fetch(url, {
    headers: {
      Cookie: cookies,
      'User-Agent':
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    },
  });

  const text = await response.text();

  // Check if we were redirected to the CAPTCHA page (new or old markup).
  if (response.url.includes('captcha') || text.includes('my-widget-mount') || text.includes('FAOCaptcha_CaptchaImage')) {
    throw new Error('CAPTCHA_REDIRECT');
  }

  return text;
}
