/**
 * FAO Transactions — Fetcher
 *
 * Fetches property transactions from fao.ge.ch rubrique 137 (last 6 days),
 * parses them using Anthropic Claude Sonnet, and upserts into bronze.transactions.
 *
 * Workflow:
 *   1. Solve CAPTCHA via Playwright + 2Captcha to get session cookies
 *   2. Fetch search result pages via HTTP with cookies
 *   3. Parse HTML to extract raw transaction text blocks
 *   4. Send each block to Claude Sonnet for structured JSON extraction
 *   5. Upsert on "affaire_number" column
 *
 * IMPORTANT: Do NOT supply type_clean_list — the BEFORE INSERT trigger
 * trg_populate_type_clean_list auto-populates it via silver.normalize_transaction_type()
 *
 * Usage:
 *   npx tsx fetch-transactions.ts
 *
 * Environment variables:
 *   SUPABASE_URL              - Supabase project URL  (required)
 *   SUPABASE_SERVICE_ROLE_KEY - service_role key      (required)
 *   WEBSHARE_PROXY_USER       - proxy username        (required)
 *   WEBSHARE_PROXY_PASS       - proxy password        (required)
 *   TWO_CAPTCHA_API_KEY       - 2Captcha API key     (required)
 *   ANTHROPIC_API_KEY         - Anthropic API key    (required)
 */

import Anthropic from '@anthropic-ai/sdk';
import * as cheerio from 'cheerio';
import { upsertBronze, sleep } from '../_shared/supabase.js';
import { createFaoSession } from '../_shared/fao-session.js';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const RUBRIQUE = 137;
const RESULTS_PER_PAGE = 50;
const BATCH_SIZE = 50;
const RATE_LIMIT_MS = 1_000;
const DAYS_BACK = 6;

// ---------------------------------------------------------------------------
// Anthropic client
// ---------------------------------------------------------------------------

const anthropicApiKey = process.env.ANTHROPIC_API_KEY;
if (!anthropicApiKey) {
  console.error('ERROR: ANTHROPIC_API_KEY is required');
  process.exit(1);
}

const anthropic = new Anthropic({ apiKey: anthropicApiKey });

// ---------------------------------------------------------------------------
// Date helpers
// ---------------------------------------------------------------------------

function formatDate(d: Date): string {
  return d.toISOString().split('T')[0];
}

function getDateRange(): { dateFrom: string; dateTo: string } {
  const now = new Date();
  const from = new Date(now);
  from.setDate(from.getDate() - DAYS_BACK);
  return {
    dateFrom: formatDate(from),
    dateTo: formatDate(now),
  };
}

// ---------------------------------------------------------------------------
// HTTP fetch with cookies
// ---------------------------------------------------------------------------

async function fetchPage(
  pageNum: number,
  cookies: string,
  dateFrom: string,
  dateTo: string,
): Promise<string> {
  const url = `https://fao.ge.ch/recherche?resultsPerPage=${RESULTS_PER_PAGE}&rubrique=${RUBRIQUE}&dateFrom=${dateFrom}&dateTo=${dateTo}&type=exact&mot-cle=&exclude=&page=${pageNum}`;

  const res = await fetch(url, {
    headers: {
      Cookie: cookies,
      'User-Agent':
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    },
  });

  if (!res.ok) throw new Error(`HTTP ${res.status} for page ${pageNum}`);
  const text = await res.text();

  if (text.includes('FAOCaptcha_CaptchaImage')) {
    throw new Error('CAPTCHA_REDIRECT');
  }

  return text;
}

// ---------------------------------------------------------------------------
// Parse search results to extract raw transaction data
// ---------------------------------------------------------------------------

interface RawTransaction {
  affaireNumber: string;
  date: string;
  section: string;
  subsection: string;
  title: string;
  details: string;
  combined: string;
}

function extractTransactions(html: string): RawTransaction[] {
  const $ = cheerio.load(html);
  const results: RawTransaction[] = [];

  const rows = $(
    '#block-2 > div.panel-pane.pane-edg-2016-fao-search-pane > div > div.resultats > ul > li',
  );

  rows.each((_, row) => {
    const article = $(row).find('article');

    // Date
    const rawDate = article.find('div.fao_date').children('div').toArray();
    const date = rawDate.map((d) => $(d).text().trim()).join(' ');

    // Section / subsection
    const rawSect = article.find('span.fao-rubrique').children().toArray();
    const section = rawSect.map((s) => $(s).text().trim()).filter(Boolean)[0] || '';

    const rawSub = article.find('.fao-sousrubrique').children().toArray();
    const subsection = rawSub.map((s) => $(s).text().trim()).filter(Boolean)[0] || '';

    // Title
    const title = article.find('h3.fao_titre').text().trim() || 'Unknown title';

    // Details
    const rawDet = article.find('div.fao_body_plus').children().toArray();
    const details = rawDet.map((d) => $(d).text().trim()).filter(Boolean)[0] || '';

    // Extract affaire number
    const match = details.match(/Affaire (\d{4}\/\d+\/\d+)/);
    if (!match) return;

    const affaireNumber = match[1];
    const combined = `Publication date: ${date}, rubrique: ${section} / ${subsection}, title: ${title}. transaction details: ${details}`;

    results.push({ affaireNumber, date, section, subsection, title, details, combined });
  });

  return results;
}

// ---------------------------------------------------------------------------
// Claude Sonnet parsing
// ---------------------------------------------------------------------------

const EXTRACTION_PROMPT = `
Analyze this transaction text and return the data strictly in the following JSON format:

{
  "Date of transaction": "Date of transaction",
  "FAO publication date": "publication date",
  "Commune": "Commune",
  "Commune number": "Commune number",
  "Affaire number": "Affaire number",
  "Price": price value if stated,
  "Type of transaction": "Type of transaction",
  "Old owner(s)": [
    {
      "Name": "Old owner's name",
      "City": "City",
      "Date": "Date"
    }
  ],
  "New owner(s)": [
    {
      "Name": "New owner's name",
      "City": "City",
      "Date": null
    }
  ],
  "Buildings": [
    {
      "Building types": ["Building type 1", "Building type 2"],
      "Building commune": "Building commune",
      "Building ID": "Building ID",
      "Feuillet number": "Feuillet number",
      "Pourmille number": "Pourmille number",
      "Parts COP": "Parts COP",
      "Premises": [
        {
          "Type": "Premises type",
          "Details": "Premises details"
        }
      ]
    }
  ],
  "PPE indicator": "Yes",
  "cumulative_pourmille_ppe": "Cumulative pourmille PPE"
}

Rules:
- Return only the JSON and nothing else
- If data could not be determined, leave the field as empty string ""
- The price field should contain only numbers (e.g. 100000), no characters. Do NOT include any price if there is no value
- Transaction type should be from text, like: cession - transfert, achat, héritage, donation, etc.
- Building ID usually has format "number/number" like "32/1953". Record it in format "number/number" without additional characters
- Parts COP are usually small numbers like 1/2 or 1/3
- Pourmille number must be extracted only from expressions following "sur", like: sur 22/1000 or sur 6,692/1000
- Premises example: "Type": "appartement", "Details": "2.01"
`;

function keyToSnakeCase(key: string): string {
  return key
    .replace(/[^\w\s]/g, '_')
    .replace(/\s+/g, '_')
    .toLowerCase()
    .replace(/^_+|_+$/g, '');
}

function formatKeys(obj: any): any {
  if (Array.isArray(obj)) return obj.map(formatKeys);
  if (obj !== null && typeof obj === 'object') {
    return Object.fromEntries(
      Object.entries(obj).map(([key, val]) => [keyToSnakeCase(key), formatKeys(val)]),
    );
  }
  if (typeof obj === 'string') {
    return obj.replace(/\n/g, ', ').trim().split(' ,').join(',') || null;
  }
  if (typeof obj === 'number') return obj.toString();
  return obj;
}

async function parseTransactionWithClaude(
  transactionText: string,
  retries = 0,
): Promise<any> {
  try {
    const response = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 4096,
      messages: [
        {
          role: 'user',
          content: `Here is the transaction text:\n"${transactionText}"\n\n${EXTRACTION_PROMPT}`,
        },
      ],
    });

    const resultText =
      response.content[0].type === 'text' ? response.content[0].text : '';

    // Strip markdown code block if present
    const cleaned = resultText.replace(/^```json?\n?/m, '').replace(/\n?```$/m, '');
    return JSON.parse(cleaned);
  } catch (err) {
    console.error(`  Claude parse error (attempt ${retries + 1}): ${err}`);
    if (retries >= 3) throw err;
    await sleep(2_000);
    return parseTransactionWithClaude(transactionText, retries + 1);
  }
}

// Building ID validation
const isValidBuildingId = (id: string): boolean => /^\d+\/\d+(-\d+)?$/.test(id);

// ---------------------------------------------------------------------------
// Date parsing helpers (from dev code)
// ---------------------------------------------------------------------------

function parseDateDDMMYYYY(dateStr: string): string | null {
  if (!dateStr) return null;
  const match = dateStr.match(/(\d{2})\.(\d{2})\.(\d{4})/);
  if (match) return `${match[3]}-${match[2]}-${match[1]}`;
  return null;
}

const FRENCH_MONTHS: Record<string, string> = {
  'janv': '01', 'jan': '01', 'janvier': '01',
  'févr': '02', 'fév': '02', 'février': '02',
  'mars': '03', 'mar': '03',
  'avr': '04', 'avril': '04',
  'mai': '05',
  'juin': '06', 'jun': '06',
  'juil': '07', 'jul': '07', 'juillet': '07',
  'août': '08', 'aou': '08',
  'sept': '09', 'sep': '09', 'septembre': '09',
  'oct': '10', 'octobre': '10',
  'nov': '11', 'novembre': '11',
  'déc': '12', 'dec': '12', 'décembre': '12',
};

function parseFrenchDate(dateStr: string): string | null {
  if (!dateStr) return null;
  // Try patterns: "DD MMM YYYY" or "DD MMM. YYYY"
  const match = dateStr.match(/(\d{1,2})\s+(\w+)\.?\s+(\d{4})/);
  if (!match) return null;
  const dd = match[1].padStart(2, '0');
  const mm = FRENCH_MONTHS[match[2].toLowerCase().replace('.', '')];
  if (!mm) return null;
  return `${match[3]}-${mm}-${dd}`;
}

// ---------------------------------------------------------------------------
// Main pipeline
// ---------------------------------------------------------------------------

async function main() {
  console.log('='.repeat(60));
  console.log('  FAO Transactions — Pipeline');
  console.log('  Source: fao.ge.ch (rubrique 137)');
  console.log('  Target: bronze.transactions');
  console.log('='.repeat(60));

  const startTime = Date.now();
  const { dateFrom, dateTo } = getDateRange();
  console.log(`  Date range: ${dateFrom} to ${dateTo}`);

  // 1. Get session cookies
  console.log('\n  Solving CAPTCHA...');
  let { cookies } = await createFaoSession(RUBRIQUE, dateFrom, dateTo);

  // 2. Fetch first page to get total count
  console.log('  Fetching first page...');
  const firstPageHtml = await fetchPage(1, cookies, dateFrom, dateTo);

  const $ = cheerio.load(firstPageHtml);
  const countSelector =
    '#block-2 > div.panel-pane.pane-edg-2016-fao-search-pane > div > div.nombre-resultats > p > em';
  let countText = $(countSelector).text().trim();
  const countMatch = countText.match(/^\d+/);
  if (!countMatch) {
    console.log('  No results found. Exiting.');
    return;
  }
  const total = parseInt(countMatch[0], 10);
  const pages = Math.ceil(total / RESULTS_PER_PAGE);
  console.log(`  Total results: ${total}, pages: ${pages}`);

  // 3. Extract raw transactions from all pages
  const allRaw: RawTransaction[] = [];

  const firstRaw = extractTransactions(firstPageHtml);
  allRaw.push(...firstRaw);
  console.log(`  Page 1/${pages}: ${firstRaw.length} transactions`);

  for (let p = 2; p <= pages; p++) {
    try {
      const html = await fetchPage(p, cookies, dateFrom, dateTo);
      const raw = extractTransactions(html);
      allRaw.push(...raw);
      console.log(`  Page ${p}/${pages}: ${raw.length} transactions`);
      await sleep(RATE_LIMIT_MS);
    } catch (err) {
      if (String(err).includes('CAPTCHA_REDIRECT')) {
        console.log('  CAPTCHA redirect, re-solving...');
        const newSession = await createFaoSession(RUBRIQUE, dateFrom, dateTo);
        cookies = newSession.cookies;
        const html = await fetchPage(p, cookies, dateFrom, dateTo);
        const raw = extractTransactions(html);
        allRaw.push(...raw);
        console.log(`  Page ${p}/${pages}: ${raw.length} transactions (after re-auth)`);
      } else {
        console.error(`  Error on page ${p}: ${err}`);
      }
    }
  }

  console.log(`\n  Total raw transactions: ${allRaw.length}`);

  if (allRaw.length === 0) {
    console.log('  No transactions to process. Exiting.');
    return;
  }

  // 4. Parse each transaction with Claude
  console.log('\n  Parsing transactions with Claude Sonnet...');
  const allRecords: Record<string, unknown>[] = [];
  let parseErrors = 0;

  for (let i = 0; i < allRaw.length; i++) {
    const raw = allRaw[i];
    if (i % 10 === 0) {
      console.log(`  Processing ${i + 1}/${allRaw.length}...`);
    }

    try {
      let parsedData = await parseTransactionWithClaude(raw.combined);

      if (!Array.isArray(parsedData?.Buildings)) {
        console.error(`  No buildings for affaire ${raw.affaireNumber}, skipping`);
        parseErrors++;
        continue;
      }

      // Validate building IDs with retries
      let allValid = true;
      let retries = 0;
      while (retries < 3) {
        allValid = true;
        for (const building of parsedData.Buildings) {
          if (!isValidBuildingId(building['Building ID'] || '')) {
            allValid = false;
            break;
          }
        }
        if (allValid) break;
        retries++;
        parsedData = await parseTransactionWithClaude(raw.combined);
        if (!Array.isArray(parsedData?.Buildings)) break;
      }

      if (!allValid) {
        console.error(`  Invalid building IDs for ${raw.affaireNumber} after retries`);
        parseErrors++;
        continue;
      }

      // Format keys to snake_case
      const formatted = formatKeys(parsedData);

      // Parse dates
      const dateOfTransaction =
        parseDateDDMMYYYY(formatted.date_of_transaction) || formatted.date_of_transaction || null;
      const faoPublicationDate =
        parseFrenchDate(formatted.fao_publication_date) || formatted.fao_publication_date || null;

      // Build record — DO NOT supply type_clean_list (trigger handles it)
      const record: Record<string, unknown> = {
        affaire_number: formatted.affaire_number || raw.affaireNumber,
        date_of_transaction: dateOfTransaction,
        fao_publication_date: faoPublicationDate,
        commune: formatted.commune || null,
        commune_number: formatted.commune_number || null,
        // DB column price is varchar — store as string
        price: formatted.price ? String(formatted.price).replace(/[^\d]/g, '') || null : null,
        type_of_transaction: formatted.type_of_transaction || null,
        old_owner_s: formatted.old_owner_s_ || formatted['old_owner(s)'] || null,
        new_owner_s: formatted.new_owner_s_ || formatted['new_owner(s)'] || null,
        // DB column buildings is varchar — stringify if object
        buildings: formatted.buildings
          ? (typeof formatted.buildings === 'string' ? formatted.buildings : JSON.stringify(formatted.buildings))
          : null,
        ppe_indicator: formatted.ppe_indicator || null,
        cumulative_pourmille_ppe: formatted.cumulative_pourmille_ppe || null,
        transaction: raw.details,
      };

      allRecords.push(record);
    } catch (err) {
      console.error(`  Error parsing ${raw.affaireNumber}: ${err}`);
      parseErrors++;
    }

    // Rate limit for Anthropic API
    await sleep(500);
  }

  console.log(`\n  Parsed: ${allRecords.length}, errors: ${parseErrors}`);

  if (allRecords.length === 0) {
    console.log('  No records to upsert. Exiting.');
    return;
  }

  // 5. Upsert — DO NOT supply type_clean_list
  console.log(`\n  Upserting ${allRecords.length} records (batch size: ${BATCH_SIZE})...`);
  const totalUpserted = await upsertBronze('transactions', allRecords, 'affaire_number', BATCH_SIZE);

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\n${'='.repeat(60)}`);
  console.log('  IMPORT COMPLETE');
  console.log(`  Transactions found: ${allRaw.length}`);
  console.log(`  Records parsed:     ${allRecords.length}`);
  console.log(`  Parse errors:       ${parseErrors}`);
  console.log(`  Records upserted:   ${totalUpserted}`);
  console.log(`  Duration:           ${elapsed}s`);
  console.log('='.repeat(60));

  if (totalUpserted === 0 && allRecords.length > 0) {
    console.error('  FAILED: Zero rows upserted despite having records!');
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
