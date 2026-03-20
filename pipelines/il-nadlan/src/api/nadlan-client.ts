import crypto from 'crypto';
import puppeteer from 'puppeteer';

// --- Configuration from nadlan.gov.il ---
const API_BASE = 'https://x4006fhmy5.execute-api.il-central-1.amazonaws.com/api';
const RECAPTCHA_TOKEN_URL = 'https://z20z0xd164.execute-api.il-central-1.amazonaws.com/api/GetToken';
const RECAPTCHA_SITE_KEY = '6LeFXPIrAAAAADG099BKMLMI85eElTM5qon0SdRH';
const JWT_SECRET = '90c3e620192348f1bd46fcd9138c3c68';
const JWT_TTL_SECONDS = 600;
const DOMAIN = 'www.nadlan.gov.il';

const RATE_LIMIT_MS = 1000; // 1 request per second
const MAX_RETRIES = 3;
const RETRY_DELAYS = [5_000, 15_000, 30_000];
const FETCH_TIMEOUT_MS = 60_000;

let lastRequestTime = 0;

// --- JWT helpers ---
function base64url(input: string | Buffer): string {
  const buf = typeof input === 'string' ? Buffer.from(input) : input;
  return buf.toString('base64').replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');
}

function signJWT(payload: Record<string, any>): string {
  const secret = new TextEncoder().encode(JWT_SECRET);
  const header = base64url(JSON.stringify({ alg: 'HS256' }));
  const payloadB64 = base64url(JSON.stringify(payload));
  const sigInput = `${header}.${payloadB64}`;
  const sig = crypto.createHmac('sha256', Buffer.from(secret)).update(sigInput).digest();
  return `${sigInput}.${base64url(sig)}`;
}

function wrapPayload(data: Record<string, any>): string {
  const payload = {
    ...data,
    exp: Math.floor(Date.now() / 1000) + JWT_TTL_SECONDS,
    domain: DOMAIN,
  };
  const jwt = signJWT(payload);
  const reversed = jwt.split('').reverse().join('');
  return JSON.stringify({ '##': reversed });
}

// --- Rate limiting ---
async function rateLimit(): Promise<void> {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < RATE_LIMIT_MS) {
    await new Promise((r) => setTimeout(r, RATE_LIMIT_MS - elapsed));
  }
  lastRequestTime = Date.now();
}

// --- reCAPTCHA token via Puppeteer ---
let cachedServerToken: string | null = null;
let tokenExpiresAt = 0;

async function getRecaptchaToken(): Promise<string> {
  // Reuse if still valid (tokens last ~2 min, we refresh at 90s)
  if (cachedServerToken && Date.now() < tokenExpiresAt) {
    return cachedServerToken;
  }

  console.log('[Auth] Launching headless browser for reCAPTCHA Enterprise token...');
  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
  });

  try {
    const page = await browser.newPage();
    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    );

    // Navigate to nadlan.gov.il to load the recaptcha script in proper context
    await page.goto('https://www.nadlan.gov.il/', { waitUntil: 'networkidle2', timeout: 30000 });

    // Wait for reCAPTCHA Enterprise to load
    await page.waitForFunction(() => typeof (window as any).grecaptcha?.enterprise?.execute === 'function', {
      timeout: 15000,
    });

    // Execute reCAPTCHA Enterprise to get a client token
    const clientToken = await page.evaluate(
      (siteKey: string) => {
        return new Promise<string>((resolve, reject) => {
          (window as any).grecaptcha.enterprise.ready(() => {
            (window as any).grecaptcha.enterprise
              .execute(siteKey, { action: 'PAGE_LOAD' })
              .then(resolve)
              .catch(reject);
          });
        });
      },
      RECAPTCHA_SITE_KEY,
    );

    console.log('[Auth] Got reCAPTCHA client token, exchanging for server token...');

    // Exchange client token for server token
    const res = await fetch(RECAPTCHA_TOKEN_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token: clientToken }),
    });

    const data = await res.json();
    const serverToken = data?.token || (data?.body ? JSON.parse(data.body).token : null);

    if (!serverToken) {
      throw new Error(`Failed to get server token: ${JSON.stringify(data)}`);
    }

    cachedServerToken = serverToken.replace(/"/g, '') as string;
    tokenExpiresAt = Date.now() + 90_000; // Refresh after 90s
    console.log('[Auth] Server token obtained successfully');
    return cachedServerToken!;
  } finally {
    await browser.close();
  }
}

// --- Retry wrapper ---
async function fetchWithRetry(url: string, options: RequestInit): Promise<Response> {
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      await rateLimit();
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
      const res = await fetch(url, { ...options, signal: controller.signal });
      clearTimeout(timeout);
      return res;
    } catch (err: any) {
      console.error(`[API] Attempt ${attempt + 1}/${MAX_RETRIES} failed: ${err.message}`);
      if (attempt < MAX_RETRIES - 1) {
        const delay = RETRY_DELAYS[attempt];
        console.log(`[API] Retrying in ${delay / 1000}s...`);
        await new Promise((r) => setTimeout(r, delay));
      } else {
        throw err;
      }
    }
  }
  throw new Error('Unreachable');
}

// --- Public API ---

export interface NadlanDeal {
  [key: string]: any;
}

export interface NadlanResponse {
  statusCode: number;
  data: {
    total_rows: number;
    total_fetch: number;
    total_page: number;
    items: NadlanDeal[];
  };
}

/**
 * Fetch deals from nadlan.gov.il API.
 * Paginates through all results using fetch_number (10 results per page).
 */
export async function fetchDeals(options: {
  settlementId?: string;
  neighborhoodId?: string;
  fetchNumber?: number;
  dealDate?: string; // e.g. "60" for 5 years
  dealNature?: string;
  roomNum?: string;
  typeOrder?: string;
}): Promise<NadlanResponse> {
  const token = await getRecaptchaToken();

  const payload: Record<string, any> = {
    base_id: options.settlementId || options.neighborhoodId || '',
    base_name: options.settlementId ? 'settlmentID' : options.neighborhoodId ? 'neighborhoodId' : 'settlmentID',
    fetch_number: options.fetchNumber ?? 1,
    type_order: options.typeOrder ?? 'dealDate_down',
    token,
  };

  if (options.dealDate) payload.deal_date = options.dealDate;
  if (options.dealNature) payload.deal_nature = options.dealNature;
  if (options.roomNum) payload.room_num = options.roomNum;

  const body = wrapPayload(payload);
  const res = await fetchWithRetry(`${API_BASE}/deal`, {
    method: 'POST',
    headers: { 'Content-Type': 'text/plain' },
    body,
  });

  const json = await res.json();

  // Handle 405 (token expired) — refresh and retry once
  if (json.statusCode === 405) {
    console.log('[API] Token expired (405), refreshing...');
    cachedServerToken = null;
    tokenExpiresAt = 0;
    const newToken = await getRecaptchaToken();
    payload.token = newToken;
    const retryBody = wrapPayload(payload);
    const retryRes = await fetchWithRetry(`${API_BASE}/deal`, {
      method: 'POST',
      headers: { 'Content-Type': 'text/plain' },
      body: retryBody,
    });
    return retryRes.json();
  }

  return json;
}

/**
 * Fetch ALL deals for a settlement by paginating through fetch_number.
 * The API returns 10 items per page, with total_rows telling us the count.
 */
export async function fetchAllDeals(
  settlementId: string,
  onBatch: (items: NadlanDeal[], page: number, total: number) => Promise<void>,
): Promise<{ totalRows: number; totalFetched: number }> {
  let fetchNumber = 1;
  let totalRows = 0;
  let totalFetched = 0;

  while (true) {
    const res = await fetchDeals({ settlementId, fetchNumber });

    if (res.statusCode !== 200 || !res.data) {
      console.error(`[API] Unexpected response: ${JSON.stringify(res).substring(0, 200)}`);
      break;
    }

    totalRows = res.data.total_rows;
    const items = res.data.items || [];

    if (items.length === 0) break;

    await onBatch(items, fetchNumber, totalRows);
    totalFetched += items.length;

    if (totalFetched >= totalRows) break;
    fetchNumber++;
  }

  return { totalRows, totalFetched };
}

/**
 * Get list of all Israeli settlements from the nadlan index.
 */
export async function fetchSettlementIndex(): Promise<Array<{ id: string; name: string }>> {
  const res = await fetch('https://d30nq1hiio0r3z.cloudfront.net/api/index/PolyNeighSett.json');
  const data = await res.json();
  // The index contains settlement codes and names
  if (Array.isArray(data)) {
    return data.map((item: any) => ({
      id: String(item.setl_code || item.id || item.ObjectID),
      name: item.setl_name || item.name || item.title || '',
    }));
  }
  return [];
}

export function invalidateToken(): void {
  cachedServerToken = null;
  tokenExpiresAt = 0;
}
