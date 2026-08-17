/**
 * FAO LDTR — Fetcher
 *
 * Fetches LDTR (Loi sur les Démolitions, Transformations et Rénovations)
 * records from fao.ge.ch rubrique 168 and upserts into bronze."FAO_LDTR".
 *
 * Workflow:
 *   1. Solve CAPTCHA via Playwright + 2Captcha to get session cookies
 *   2. Fetch all search result pages via HTTP with cookies
 *   3. Parse HTML to extract key:value pairs from <br>-separated content
 *   4. Upsert on "affaire" column
 *
 * Usage:
 *   npx tsx fetch-ldtr.ts
 *
 * Environment variables:
 *   SUPABASE_URL              - Supabase project URL  (required)
 *   SUPABASE_SERVICE_ROLE_KEY - service_role key      (required)
 *   TWO_CAPTCHA_API_KEY       - 2Captcha API key     (required)
 */

import * as cheerio from 'cheerio';
import { upsertBronze, sleep, verifyBronzeAccess } from '../_shared/supabase.js';
import { createFaoSession } from '../_shared/fao-session.js';
import { classifyNonPrice, extractLotPrice } from './price-classifier.js';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const RUBRIQUE = 168;
const RESULTS_PER_PAGE = 50;
const BATCH_SIZE = 100;
const RATE_LIMIT_MS = 1_000;
const DAYS_BACK = 6;

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

// Geneva communes for validation (from dev's constants)
const COMMUNES = new Set([
  'Collonge-Bellerive', 'Anières', 'Hermance', 'Versoix', 'Collex-Bossy',
  'Meinier', 'Veyrier', 'Genthod', 'Bellevue', 'Avusy', 'Perly-Certoux',
  'Laconnex', 'Gy', 'Vandoeuvres', 'Choulex', 'Plan-les-Ouates',
  'Chêne-Bourg', 'Carouge', 'Genève-Eaux-Vives', 'Chêne-Bougeries', 'Lancy',
  'Grand-Saconnex', 'Confignon', 'Onex', 'Satigny', 'Russin', 'Cologny',
  'Céligny', 'Soral', 'Bernex', 'Vernier', 'Genève-Petit-Saconnex', 'Avully',
  'Genève-Cité', 'Bardonnex', 'Presinge', 'Troinex', 'Chancy', 'Jussy',
  'Meyrin', 'Aire-la-Ville', 'Cartigny', 'Corsier', 'Genève-Plainpalais',
  'Pregny-Chambésy', 'Dardagny', 'Puplinge', 'Thônex',
]);

// French month names for date parsing
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

function parseFrenchDate(day: string, month: string, year: string): string | null {
  const m = month.toLowerCase().replace('.', '');
  const mm = FRENCH_MONTHS[m];
  if (!mm) return null;
  const dd = day.padStart(2, '0');
  return `${year}-${mm}-${dd}`;
}

// ---------------------------------------------------------------------------
// HTTP fetch with cookies
// ---------------------------------------------------------------------------

const FETCH_TIMEOUT_MS = 120_000;
const FETCH_RETRIES = 3;
const FETCH_BACKOFF_MS = [5_000, 15_000, 30_000];

async function fetchPage(pageNum: number, cookies: string, dateFrom: string, dateTo: string): Promise<string> {
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

      if (text.includes('my-widget-mount') || text.includes('FAOCaptcha_CaptchaImage')) {
        throw new Error('CAPTCHA_REDIRECT');
      }

      return text;
    } catch (err) {
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
// Parse a single search results page
// ---------------------------------------------------------------------------

function parsePage(html: string): Record<string, unknown>[] {
  const $ = cheerio.load(html);
  const records: Record<string, unknown>[] = [];

  const rows = $(
    '#block-2 > div.panel-pane.pane-edg-2016-fao-search-pane > div > div.resultats > ul > li',
  );

  rows.each((_, row) => {
    // Extract date
    const dateDiv = $(row).find('article > div:nth-child(1) > div:nth-child(1) > div');
    const day = dateDiv.find('div:nth-child(1)').text().trim();
    const month = dateDiv.find('div:nth-child(2)').text().trim();
    const year = dateDiv.find('div:nth-child(3)').text().trim();
    const formattedDate = parseFrenchDate(day, month, year);

    // Extract raw fields
    const rawFields = $(row)
      .find('article > div:nth-child(2) > div > div.fao_plus > div > p:nth-child(1)')
      .html();
    if (!rawFields) return;

    const fields = rawFields.split('<br>').map((el) =>
      el
        .replace(/<strong>/gm, '')
        .replace(/<\/strong>/gm, '')
        .replace('&nbsp', '')
        .replace(';', ''),
    );

    const res: Record<string, unknown> = {
      type: 'Vente LDTR',
      date_de_parution_au_rf: formattedDate,
      transaction_date: formattedDate,
      transaction: fields
        .filter((a) => a)
        .join(' ')
        .split(':')
        .join(': '),
      // Defaults — both columns are NOT NULL in bronze; supabase-js sends
      // explicit null (not omit) for missing fields, which would override
      // the column default. Initialize to [] so missing keys leave [] in DB.
      acheteur_list: [],
      vendeur_list: [],
    };

    for (const line of fields) {
      const colonIdx = line.indexOf(':');
      if (colonIdx === -1) continue;

      const key = line.substring(0, colonIdx).trim();
      let value = line
        .substring(colonIdx + 1)
        .replace(/&amp;/gm, '&')
        .replace(/&nbsp;/gm, ' ')
        .trim();

      if (!value) continue;

      if (key === 'Requête n°') {
        res.affaire = value;
      }
      if (key === "Requérant et propriétaire de l'appartement") {
        value = value.replace(/M\. /gm, '').replace(/Mme/gm, '');
        const persons = value
          .split(/,\s*|et|&/)
          .map((p) => p.trim())
          .filter(Boolean);

        res.vendeur = value;
        res.vendeur_list = persons;
      }
      if (key === 'Commune et lieu') {
        let [commune, address] = value.split(' - ');
        commune = commune?.includes(', section ')
          ? commune.replace(', section ', '-')
          : commune;
        res.commune = COMMUNES.has(commune) ? commune : undefined;
        res.address = address || null;
      }
      if (key === 'Objet') {
        res.lot_key = value;
      }
      if (key === "Acquéreur de l'appartement") {
        value = value.replace(/M\. /gm, '').replace(/Mme/gm, '');
        const persons = value
          .split(/,\s*|et|&/)
          .map((p) => p.trim())
          .filter(Boolean);

        res.acheteur = value;
        res.acheteur_list = persons;
      }
      if (key === 'Prix de vente') {
        const prixValue = value
          .split('.--')[0]
          .replace(/Frs /gm, '')
          .replace(/'/gm, '')
          .trim();
        const reason = classifyNonPrice(prixValue);
        if (reason) {
          // Legal transfer, no price. prix stays NULL so the downstream
          // safe_cast.to_numeric never sees it; the reason is recorded in
          // bronze_ch.fao_ldtr_price_absent.
          res.prix = null;
          res._no_price = { raw_value: prixValue, reason };
        } else {
          // A lot-labelled price ("lot n° 2.01 : 1100000") carries one lot and
          // one figure; keep the figure. Anything else passes through untouched
          // so unknown shapes still reach the quarantine.
          const lotPrice = extractLotPrice(prixValue);
          // DB column prix is varchar, store as string
          res.prix = lotPrice ?? (prixValue || '0');
        }
      }
    }

    // Only add if we have an affaire number
    if (res.affaire) {
      records.push(res);
    }
  });

  return records;
}

// ---------------------------------------------------------------------------
// Main pipeline
// ---------------------------------------------------------------------------

async function main() {
  console.log('='.repeat(60));
  console.log('  FAO LDTR — Pipeline');
  console.log('  Source: fao.ge.ch (rubrique 168)');
  console.log('  Target: bronze."FAO_LDTR"');
  console.log('='.repeat(60));

  const startTime = Date.now();

  // 0. Verify DB connectivity before spending hours scraping
  await verifyBronzeAccess('FAO_LDTR');

  const { dateFrom, dateTo } = getDateRange();
  console.log(`  Date range: ${dateFrom} to ${dateTo}`);

  // 1. Get session cookies
  console.log('\n  Solving CAPTCHA...');
  const { cookies } = await createFaoSession(RUBRIQUE, dateFrom, dateTo);

  // 2. Fetch first page to get total count
  console.log('  Fetching first page...');
  const firstPageHtml = await fetchPage(1, cookies, dateFrom, dateTo);

  const $ = cheerio.load(firstPageHtml);
  const rawResults = $(
    '#block-2 > div.panel-pane.pane-edg-2016-fao-search-pane > div > div.nombre-resultats',
  )
    .text()
    .trim();
  const totalStr = rawResults.split(' résultats')[0].trim();
  const total = parseInt(totalStr, 10) || 0;
  const pages = Math.ceil(total / RESULTS_PER_PAGE);

  console.log(`  Total results: ${total}, pages: ${pages}`);

  // 3. Parse all pages
  const allRecords: Record<string, unknown>[] = [];

  // Parse first page
  const firstRecords = parsePage(firstPageHtml);
  allRecords.push(...firstRecords);
  console.log(`  Page 1/${pages}: ${firstRecords.length} records`);

  // Parse remaining pages
  let pageFailures = 0;
  for (let p = 2; p <= pages; p++) {
    try {
      const html = await fetchPage(p, cookies, dateFrom, dateTo);
      const records = parsePage(html);
      allRecords.push(...records);
      console.log(`  Page ${p}/${pages}: ${records.length} records`);
      await sleep(RATE_LIMIT_MS);
    } catch (err) {
      if (String(err).includes('CAPTCHA_REDIRECT')) {
        console.log('  CAPTCHA redirect detected, re-solving...');
        try {
          const newSession = await createFaoSession(RUBRIQUE, dateFrom, dateTo);
          const html = await fetchPage(p, newSession.cookies, dateFrom, dateTo);
          const records = parsePage(html);
          allRecords.push(...records);
          console.log(`  Page ${p}/${pages}: ${records.length} records (after re-auth)`);
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

  console.log(`\n  Total records: ${allRecords.length}`);

  if (allRecords.length === 0) {
    console.log('  No records to upsert. Exiting.');
    console.log('='.repeat(60));
    return;
  }

  // 4. Upsert
  // Split out the non-price classifications first. The marker is internal and
  // must not reach the FAO_LDTR upsert, which would reject an unknown column.
  const noPriceRecords = allRecords
    .filter((r) => r._no_price)
    .map((r) => {
      const { raw_value, reason } = r._no_price as { raw_value: string; reason: string };
      return { affaire: r.affaire as string, raw_value, reason };
    });
  for (const r of allRecords) delete r._no_price;

  console.log(`\n  Upserting ${allRecords.length} records (batch size: ${BATCH_SIZE})...`);
  const totalUpserted = await upsertBronze('FAO_LDTR', allRecords, 'affaire', BATCH_SIZE);

  // Lowercase logical name on purpose: resolveTable() upper-cases it to look up
  // SUPABASE_TABLE_FAO_LDTR_PRICE_ABSENT, which is not set, so it falls through
  // to this literal — which is already the physical table name. No workflow
  // env change needed.
  let noPriceUpserted = 0;
  if (noPriceRecords.length > 0) {
    console.log(
      `\n  ${noPriceRecords.length} notice(s) carry a transfer type instead of a price ` +
        `(prix left NULL, reason recorded)...`,
    );
    noPriceUpserted = await upsertBronze('fao_ldtr_price_absent', noPriceRecords, 'affaire', BATCH_SIZE);
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\n${'='.repeat(60)}`);
  console.log('  IMPORT COMPLETE');
  console.log(`  Records parsed:    ${allRecords.length}`);
  console.log(`  Records upserted:  ${totalUpserted}`);
  console.log(`  Non-price notices: ${noPriceUpserted}`);
  console.log(`  Duration:          ${elapsed}s`);
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
