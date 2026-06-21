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
 *   TWO_CAPTCHA_API_KEY       - 2Captcha API key     (required)
 *   ANTHROPIC_API_KEY         - Anthropic API key    (required)
 */

import Anthropic from '@anthropic-ai/sdk';
import * as cheerio from 'cheerio';
import { supabase, upsertBronze, sleep, verifyBronzeAccess, BRONZE_SCHEMA, resolveTable } from '../_shared/supabase.js';
import { createFaoSession } from '../_shared/fao-session.js';
import { cleanSwissPrice, validateParsedPrice } from './price.js';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const RUBRIQUE = 137;
const RESULTS_PER_PAGE = 50;
const BATCH_SIZE = 50;
const RATE_LIMIT_MS = 1_000;
const MIN_LOOKBACK_DAYS = 30;

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

async function getDateRange(): Promise<{ dateFrom: string; dateTo: string }> {
  // Allow env-var overrides for one-off backfills
  if (process.env.START_DATE && process.env.END_DATE) {
    return { dateFrom: process.env.START_DATE, dateTo: process.env.END_DATE };
  }

  const now = new Date();

  // Safety-net floor: always at least MIN_LOOKBACK_DAYS
  const floor = new Date(now);
  floor.setDate(floor.getDate() - MIN_LOOKBACK_DAYS);

  // Query DB for the latest publication date we already have
  const { data } = await supabase
    .schema(BRONZE_SCHEMA)
    .from(resolveTable('transactions'))
    .select('fao_publication_date')
    .order('fao_publication_date', { ascending: false })
    .limit(1);

  let dbMax: Date | null = null;
  if (data?.[0]?.fao_publication_date) {
    dbMax = new Date(data[0].fao_publication_date);
  }

  // Use the earlier of (30 days ago) or (latest DB date) so we never skip a gap
  const dateFrom = dbMax && dbMax < floor ? formatDate(dbMax) : formatDate(floor);

  return { dateFrom, dateTo: formatDate(now) };
}

// ---------------------------------------------------------------------------
// HTTP fetch with cookies
// ---------------------------------------------------------------------------

const FETCH_TIMEOUT_MS = 120_000;
const FETCH_RETRIES = 3;
const FETCH_BACKOFF_MS = [5_000, 15_000, 30_000];

async function fetchPage(
  pageNum: number,
  cookies: string,
  dateFrom: string,
  dateTo: string,
): Promise<string> {
  const url = `https://fao.ge.ch/recherche?resultsPerPage=${RESULTS_PER_PAGE}&rubrique=${RUBRIQUE}&dateFrom=${dateFrom}&dateTo=${dateTo}&type=exact&mot-cle=&exclude=&page=${pageNum}`;

  for (let attempt = 0; attempt <= FETCH_RETRIES; attempt++) {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

      const res = await fetch(url, {
        headers: {
          Cookie: cookies,
          'User-Agent':
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        },
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!res.ok) throw new Error(`HTTP ${res.status} for page ${pageNum}`);
      const text = await res.text();

      if (text.includes('FAOCaptcha_CaptchaImage')) {
        throw new Error('CAPTCHA_REDIRECT');
      }

      return text;
    } catch (err) {
      // Don't retry CAPTCHA redirects — caller handles re-auth
      if (String(err).includes('CAPTCHA_REDIRECT')) throw err;

      if (attempt < FETCH_RETRIES) {
        const delay = FETCH_BACKOFF_MS[attempt] ?? 30_000;
        console.log(`  Fetch page ${pageNum} failed (attempt ${attempt + 1}/${FETCH_RETRIES + 1}): ${err}`);
        console.log(`  Retrying in ${delay / 1_000}s...`);
        await sleep(delay);
      } else {
        throw err;
      }
    }
  }

  throw new Error(`fetchPage ${pageNum} failed after ${FETCH_RETRIES + 1} attempts`);
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
      // Sonnet was shown to hallucinate a digit-group on a CHF 1.875M sale
      // (Pittard, 2026-02-19), duplicating "'000" into "1'875'000'000". The
      // downstream regex extraction in price.ts now overrides any LLM-supplied
      // price with the raw-text regex match, so this hallucination is caught
      // and corrected before insert. Optional follow-up: try Opus for better
      // digit fidelity once we confirm the right model string for this project.
      model: 'claude-sonnet-4-6',
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

  // 0. Verify DB connectivity before spending hours scraping
  await verifyBronzeAccess('transactions');

  const { dateFrom, dateTo } = await getDateRange();
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
  // FAO renders the count in two forms:
  //   "N résultats correspondent à votre recherche"  (single-page window)
  //   "1 - 50 sur N résultats"                       (multi-page window)
  // And N uses the Swiss thousand separator: "1'594".
  // Original regex `/^\d+/` captured the leading "1" in the multi-page form
  // and silently truncated every fetch to one page (50 rows).
  // Fix: strip Swiss separators ('  ʼ ,  ), then prefer the number after
  // " sur ", else fall back to the largest number on the page.
  const cleanedText = countText.replace(/['’ʼ\s,](?=\d{3}\b)/g, '');
  const surMatch = cleanedText.match(/\bsur\s+(\d+)/i);
  const allNums = (cleanedText.match(/\d+/g) || []).map((n) => parseInt(n, 10));
  if (!surMatch && allNums.length === 0) {
    console.log('  No results found. Exiting.');
    return;
  }
  const total = surMatch ? parseInt(surMatch[1], 10) : Math.max(...allNums);
  const pages = Math.ceil(total / RESULTS_PER_PAGE);
  console.log(`  Total results: ${total}, pages: ${pages} (raw count text: "${countText}")`);

  // 3. Extract raw transactions from all pages
  const allRaw: RawTransaction[] = [];

  const firstRaw = extractTransactions(firstPageHtml);
  allRaw.push(...firstRaw);
  console.log(`  Page 1/${pages}: ${firstRaw.length} transactions`);

  let pageFailures = 0;
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
        try {
          const newSession = await createFaoSession(RUBRIQUE, dateFrom, dateTo);
          cookies = newSession.cookies;
          const html = await fetchPage(p, cookies, dateFrom, dateTo);
          const raw = extractTransactions(html);
          allRaw.push(...raw);
          console.log(`  Page ${p}/${pages}: ${raw.length} transactions (after re-auth)`);
        } catch (reAuthErr) {
          console.error(`  Failed page ${p} even after re-auth: ${reAuthErr}`);
          pageFailures++;
        }
      } else {
        console.error(`  Error on page ${p} (skipping): ${err}`);
        pageFailures++;
      }
    }
  }

  if (pageFailures > 0) {
    console.log(`  WARNING: ${pageFailures}/${pages - 1} pages failed to fetch`);
  }

  console.log(`\n  Total raw transactions: ${allRaw.length}`);

  if (allRaw.length === 0) {
    console.log('  No transactions to process. Exiting.');
    return;
  }

  // 4. Parse each transaction with Claude
  console.log('\n  Parsing transactions with Claude Sonnet...');
  const allRecords: Record<string, unknown>[] = [];
  const quarantined: Record<string, unknown>[] = [];
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

      // Bug fix: Split building_id PPE feuillet suffix (e.g. "27/157-108" → id="27/157", feuillet="108")
      for (const building of parsedData.Buildings) {
        const bid = building['Building ID'] || '';
        const feuilletMatch = bid.match(/^(\d+\/\d+)-(\d+)$/);
        if (feuilletMatch) {
          building['Building ID'] = feuilletMatch[1];
          building['Feuillet number'] = feuilletMatch[2];
        }
      }

      // Format keys to snake_case
      const formatted = formatKeys(parsedData);

      // Parse dates
      const dateOfTransaction =
        parseDateDDMMYYYY(formatted.date_of_transaction) || formatted.date_of_transaction || null;
      const faoPublicationDate =
        parseFrenchDate(formatted.fao_publication_date) || formatted.fao_publication_date || null;

      // Regex fallback for affaire number (belt-and-suspenders)
      const affaireRegex = raw.details.match(/Affaire\s+(\d{4}\/\d+\/\d+)/);
      const affaireNumber = formatted.affaire_number || raw.affaireNumber || (affaireRegex ? affaireRegex[1] : null);

      if (!affaireNumber) {
        console.error(`  No affaire number for transaction, skipping`);
        parseErrors++;
        continue;
      }

      // ──────────────────────────────────────────────────────────────
      // Price extraction (regex over raw gazette text wins; LLM is fallback)
      // See pipelines/transactions-fao/price.ts for the two-bug history.
      // ──────────────────────────────────────────────────────────────
      const llmCleanedPrice = formatted.price
        ? cleanSwissPrice(String(formatted.price))
        : null;
      const validation = validateParsedPrice(raw.details, llmCleanedPrice);

      if (!validation.ok) {
        // Quarantine the row instead of inserting bad data.
        console.error(`  Quarantining ${affaireNumber}: ${validation.reason} (price=${validation.parsedPrice})`);
        quarantined.push({
          affaire_number: affaireNumber,
          reason: validation.reason,
          parsed_price: validation.parsedPrice,
          raw_regex_price: null,
          llm_payload: parsedData,
          raw_text: raw.details,
          warnings: null,
        });
        parseErrors++;
        continue;
      }

      if (validation.warning) {
        console.warn(`  ⚠ ${affaireNumber}: ${validation.warning}`);
      }

      // Build record — DO NOT supply type_clean_list (trigger handles it)
      const record: Record<string, unknown> = {
        affaire_number: affaireNumber,
        date_of_transaction: dateOfTransaction,
        fao_publication_date: faoPublicationDate,
        commune: formatted.commune || null,
        commune_number: formatted.commune_number || null,
        // DB column price is varchar — store as cleaned numeric-as-string.
        // The value here comes from price.ts validateParsedPrice() which
        // always prefers the regex extraction over the LLM's value.
        price: validation.price,
        type_of_transaction: formatted.type_of_transaction || null,
        // DB columns old_owner_s, new_owner_s are JSONB — pass arrays directly
        old_owner_s: formatted.old_owner_s || null,
        new_owner_s: formatted.new_owner_s || null,
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

  console.log(`\n  Parsed: ${allRecords.length}, errors: ${parseErrors}, quarantined: ${quarantined.length}`);

  // Flush quarantine first (so even if main upsert fails, the rejected rows are logged)
  if (quarantined.length > 0) {
    console.log(`\n  Quarantining ${quarantined.length} row(s) to bronze_ch.fao_transactions_parse_errors...`);
    const { error: qError } = await supabase
      .schema(BRONZE_SCHEMA)
      .from('fao_transactions_parse_errors')
      .insert(quarantined);
    if (qError) {
      console.error(`  Quarantine insert FAILED: ${qError.message}`);
    } else {
      console.log(`  ✓ ${quarantined.length} row(s) quarantined`);
    }
  }

  if (allRecords.length === 0) {
    console.log('  No records to upsert. Exiting.');
    return;
  }

  // 5. Upsert — DO NOT supply type_clean_list
  // Dedupe within batch by affaire_number to avoid:
  //   "ON CONFLICT DO UPDATE command cannot affect row a second time"
  // which aborts a whole batch and silently drops every row in it. On
  // 2026-05-16's scheduled run this dropped batch 1/6 = 50 records,
  // including the ~95 fresh transactions dated 2026-05-15 that FAO had
  // just published. Last occurrence wins — most-recently-parsed copy
  // of each affaire.
  const dedupMap = new Map<string, typeof allRecords[number]>();
  let withinBatchDupes = 0;
  for (const rec of allRecords) {
    const key = rec.affaire_number;
    if (typeof key !== 'string' || !key) {
      // Preserve null-affaire rows (already counted by the null-affaire
      // validation gate below) — index them by a unique synthetic key.
      dedupMap.set(`__null_${dedupMap.size}`, rec);
      continue;
    }
    if (dedupMap.has(key)) withinBatchDupes++;
    dedupMap.set(key, rec);
  }
  const dedupedRecords = Array.from(dedupMap.values());
  if (withinBatchDupes > 0) {
    console.log(`  Deduped ${withinBatchDupes} within-batch duplicate(s) by affaire_number`);
  }

  console.log(`\n  Upserting ${dedupedRecords.length} records (batch size: ${BATCH_SIZE})...`);
  const totalUpserted = await upsertBronze('transactions', dedupedRecords, 'affaire_number', BATCH_SIZE);

  // 6. End-of-run validation
  const { data: latestRow } = await supabase
    .schema(BRONZE_SCHEMA)
    .from(resolveTable('transactions'))
    .select('fao_publication_date, affaire_number')
    .order('fao_publication_date', { ascending: false })
    .limit(1);

  const latestDate = latestRow?.[0]?.fao_publication_date ?? 'unknown';

  // Count rows with null affaire_number among newly upserted records
  const nullAffaireCount = allRecords.filter(
    (r) => !r.affaire_number && String(r.transaction ?? '').includes('Affaire'),
  ).length;

  // Check for recent data (within last 7 days)
  const sevenDaysAgo = new Date();
  sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
  const { count: recentCount } = await supabase
    .schema(BRONZE_SCHEMA)
    .from(resolveTable('transactions'))
    .select('*', { count: 'exact', head: true })
    .gte('fao_publication_date', formatDate(sevenDaysAgo));

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  // Summary
  console.log(`\n${'='.repeat(60)}`);
  console.log('  === RUN SUMMARY ===');
  console.log(`  Date range scraped:              ${dateFrom} to ${dateTo}`);
  console.log(`  Gazette pages processed:         ${pages}`);
  console.log(`  Transactions extracted:          ${allRaw.length}`);
  console.log(`  Records parsed:                  ${allRecords.length}`);
  console.log(`  Parse errors:                    ${parseErrors}`);
  console.log(`  Rows quarantined:                ${quarantined.length}`);
  console.log(`  Rows upserted to DB:             ${totalUpserted}`);
  console.log(`  Rows with null affaire:          ${nullAffaireCount}`);
  console.log(`  Latest fao_publication_date in DB: ${latestDate}`);
  console.log(`  Rows with pub date in last 7d:   ${recentCount ?? 'unknown'}`);
  console.log(`  Duration:                        ${elapsed}s`);
  console.log('='.repeat(60));

  // Validation gates
  let failed = false;

  if (totalUpserted === 0 && allRecords.length > 0) {
    console.error('  VALIDATION FAILED: Zero rows upserted despite having records!');
    failed = true;
  }

  if ((recentCount ?? 0) === 0) {
    console.error('  VALIDATION FAILED: No rows with fao_publication_date in the last 7 days!');
    failed = true;
  }

  if (nullAffaireCount > 0) {
    console.error(`  VALIDATION FAILED: ${nullAffaireCount} newly inserted row(s) have null affaire despite text containing "Affaire"!`);
    failed = true;
  }

  if (failed) {
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
