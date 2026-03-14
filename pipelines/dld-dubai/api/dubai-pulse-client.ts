const BASE_URL = 'https://api.dubaipulse.gov.ae';
const TOKEN_URL = `${BASE_URL}/oauth/client_credential/accesstoken?grant_type=client_credentials`;
const DATA_URL = `${BASE_URL}/shared/dld`;

const PAGE_SIZE = 1000;
const REQUEST_TIMEOUT_MS = 60_000;
const RETRY_DELAYS = [5_000, 15_000, 30_000];
const TOKEN_REFRESH_BUFFER_MS = 2 * 60 * 1000; // 2 minutes before expiry

export class DubaiPulseClient {
  private accessToken: string | null = null;
  private tokenExpiresAt: number = 0;
  private apiKey: string;
  private apiSecret: string;

  constructor() {
    const apiKey = process.env.DUBAI_PULSE_API_KEY;
    const apiSecret = process.env.DUBAI_PULSE_API_SECRET;
    if (!apiKey || !apiSecret) {
      throw new Error('Missing DUBAI_PULSE_API_KEY or DUBAI_PULSE_API_SECRET environment variables');
    }
    this.apiKey = apiKey;
    this.apiSecret = apiSecret;
  }

  async ensureToken(): Promise<string> {
    if (this.accessToken && Date.now() < this.tokenExpiresAt - TOKEN_REFRESH_BUFFER_MS) {
      return this.accessToken;
    }

    const response = await fetch(TOKEN_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: `client_id=${encodeURIComponent(this.apiKey)}&client_secret=${encodeURIComponent(this.apiSecret)}`,
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });

    if (!response.ok) {
      throw new Error(`Token request failed: ${response.status} ${response.statusText}`);
    }

    const data = await response.json();
    this.accessToken = data.access_token;
    // expires_in is typically in seconds
    const expiresInMs = (data.expires_in || 1800) * 1000;
    this.tokenExpiresAt = Date.now() + expiresInMs;

    console.log(`[DubaiPulse] Token acquired, expires in ${data.expires_in}s`);
    return this.accessToken!;
  }

  async fetchPage(
    datasetSlug: string,
    limit: number,
    offset: number,
    filter?: string,
    columns?: string[],
    orderBy?: string,
  ): Promise<{ data: Record<string, any>[]; hasMore: boolean }> {
    const token = await this.ensureToken();

    const params = new URLSearchParams();
    params.set('limit', String(limit));
    params.set('offset', String(offset));
    if (filter) params.set('filter', filter);
    if (columns?.length) params.set('column', columns.join(','));
    if (orderBy) params.set('order_by', orderBy);

    const url = `${DATA_URL}/${datasetSlug}?${params.toString()}`;

    for (let attempt = 0; attempt <= RETRY_DELAYS.length; attempt++) {
      try {
        const response = await fetch(url, {
          headers: { Authorization: `Bearer ${token}` },
          signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
        });

        if (response.status >= 400 && response.status < 500) {
          const body = await response.text();
          throw new Error(`Client error ${response.status}: ${body} (no retry)`);
        }

        if (!response.ok) {
          throw new Error(`Server error ${response.status}: ${response.statusText}`);
        }

        const data = await response.json();
        const rows = Array.isArray(data) ? data : data.data ?? data.results ?? [];

        return {
          data: rows,
          hasMore: rows.length >= limit,
        };
      } catch (error: any) {
        // Don't retry client errors
        if (error.message?.includes('no retry')) throw error;

        if (attempt < RETRY_DELAYS.length) {
          const delay = RETRY_DELAYS[attempt];
          console.warn(`[DubaiPulse] Retry ${attempt + 1}/${RETRY_DELAYS.length} after ${delay}ms: ${error.message}`);
          await sleep(delay);
        } else {
          throw error;
        }
      }
    }

    throw new Error('Unreachable');
  }

  async fetchAll(
    datasetSlug: string,
    filter?: string,
    columns?: string[],
    orderBy?: string,
  ): Promise<Record<string, any>[]> {
    const allRows: Record<string, any>[] = [];
    let offset = 0;
    let pageNum = 1;

    while (true) {
      console.log(`[DubaiPulse] Fetching ${datasetSlug} page ${pageNum} (offset ${offset})...`);

      const { data, hasMore } = await this.fetchPage(datasetSlug, PAGE_SIZE, offset, filter, columns, orderBy);
      allRows.push(...data);

      console.log(`[DubaiPulse] Page ${pageNum}: ${data.length} rows (total: ${allRows.length})`);

      if (!hasMore) break;
      offset += PAGE_SIZE;
      pageNum++;
    }

    console.log(`[DubaiPulse] Fetched ${allRows.length} total rows from ${datasetSlug}`);
    return allRows;
  }

  async discoverEndpoints(): Promise<void> {
    const token = await this.ensureToken();
    console.log('[DubaiPulse] Token acquired successfully. Discovering available DLD endpoints...\n');

    const knownSlugs = [
      'dld_transactions-open-api',
      'dld_residential_sale_index-open',
      'dld_lkp_transaction_procedures-open',
      'dld_lkp_transaction_groups-open',
    ];

    for (const slug of knownSlugs) {
      try {
        const { data } = await this.fetchPage(slug, 1, 0);
        console.log(`  [OK] ${slug} — sample fields: ${Object.keys(data[0] || {}).join(', ')}`);
      } catch (e: any) {
        console.log(`  [FAIL] ${slug} — ${e.message}`);
      }
    }

    console.log('\nUse the Dubai Pulse API catalog to find additional DLD dataset slugs.');
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
