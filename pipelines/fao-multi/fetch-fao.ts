/**
 * FAO Multi — generic fetcher for fao.ge.ch rubriques.
 *
 * One parser, one bronze table (bronze_ch.ge_fao_publications), JSONB fields.
 * Used for the 10 rubriques that don't have a dedicated typed parser:
 *   72, 91, 97, 135, 136, 213, 217, 322, 84, 90
 *
 * The well-typed rubriques (137 transactions, 168 LDTR) keep their own parsers.
 *
 * Workflow:
 *   1. Solve CAPTCHA via Playwright + 2Captcha to get session cookies
 *   2. Fetch all search result pages via HTTP with cookies
 *   3. Extract <li> rows: date, section, subsection, title, raw text, parsed key:value pairs
 *   4. Upsert on (rubrique, dedup_key) where dedup_key = COALESCE(affaire, md5(raw_text))
 *
 * Usage:
 *   RUBRIQUE=72 npx tsx fetch-fao.ts
 *
 * Env vars:
 *   RUBRIQUE                          - rubrique number to fetch (required)
 *   RE_LLM_SUPABASE_URL               - re-llm Supabase project URL (required)
 *   RE_LLM_SUPABASE_SERVICE_ROLE_KEY  - re-llm service_role key (required)
 *   TWO_CAPTCHA_API_KEY               - 2Captcha API key (required)
 *   START_DATE / END_DATE             - YYYY-MM-DD overrides (optional)
 *   DAYS_BACK                         - lookback window in days (default: 7)
 */

import * as cheerio from 'cheerio';
import { upsert, verifyAccess, sleep } from '../_shared/re-llm.js';
import { createFaoSession } from '../_shared/fao-session.js';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const SCHEMA = 'bronze_ch';
const TABLE = 'ge_fao_publications';
const ON_CONFLICT = 'rubrique,dedup_key';
const RESULTS_PER_PAGE = 50;
const BATCH_SIZE = 100;
const RATE_LIMIT_MS = 1_000;
const DEFAULT_DAYS_BACK = 7;

const RUBRIQUE = parseInt(process.env.RUBRIQUE ?? '', 10);
if (!Number.isFinite(RUBRIQUE)) {
  console.error('ERROR: RUBRIQUE env var must be set to a number');
  process.exit(1);
}

const FETCH_TIMEOUT_MS = 120_000;
const FETCH_RETRIES = 3;
const FETCH_BACKOFF_MS = [5_000, 15_000, 30_000];

// ---------------------------------------------------------------------------
// Date helpers
// ---------------------------------------------------------------------------

function formatDate(d: Date): string {
  return d.toISOString().split('T')[0];
}

function getDateRange(): { dateFrom: string; dateTo: string } {
  if (process.env.START_DATE && process.env.END_DATE) {
    return { dateFrom: process.env.START_DATE, dateTo: process.env.END_DATE };
  }
  const now = new Date();
  const daysBack = parseInt(process.env.DAYS_BACK ?? '', 10) || DEFAULT_DAYS_BACK;
  const from = new Date(now);
  from.setDate(from.getDate() - daysBack);
  return { dateFrom: formatDate(from), dateTo: formatDate(now) };
}

const FRENCH_MONTHS: Record<string, string> = {
  janv: '01', jan: '01', janvier: '01',
  févr: '02', fév: '02', février: '02',
  mars: '03', mar: '03',
  avr: '04', avril: '04',
  mai: '05',
  juin: '06', jun: '06',
  juil: '07', jul: '07', juillet: '07',
  août: '08', aou: '08',
  sept: '09', sep: '09', septembre: '09',
  oct: '10', octobre: '10',
  nov: '11', novembre: '11',
  déc: '12', dec: '12', décembre: '12',
};

function parseFrenchDateParts(day: string, month: string, year: string): string | null {
  const m = month.toLowerCase().replace('.', '').trim();
  const mm = FRENCH_MONTHS[m];
  if (!mm) return null;
  const dd = day.padStart(2, '0');
  if (!/^\d{4}$/.test(year)) return null;
  return `${year}-${mm}-${dd}`;
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
      if (String(err).includes('CAPTCHA_REDIRECT')) throw err;
      if (attempt < FETCH_RETRIES) {
        const delay = FETCH_BACKOFF_MS[attempt] ?? 30_000;
        console.log(`  Fetch page ${pageNum} failed (attempt ${attempt + 1}): ${err}`);
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
// Affaire / dossier extraction
// ---------------------------------------------------------------------------

const AFFAIRE_PATTERNS: RegExp[] = [
  /Affaire\s+([A-Z0-9][A-Z0-9\/\-\.]+)/i,
  /Requête\s*n[°o]\s*[:\-]?\s*([A-Z0-9][A-Z0-9\/\-\.]+)/i,
  /Dossier\s*n[°o]?\s*[:\-]?\s*([A-Z0-9][A-Z0-9\/\-\.]+)/i,
  /Référence\s*[:\-]?\s*([A-Z0-9][A-Z0-9\/\-\.]+)/i,
  /R[ée]f\.\s*[:\-]?\s*([A-Z0-9][A-Z0-9\/\-\.]+)/i,
];

function extractAffaire(text: string, fields: Record<string, string>): string | null {
  // Prefer parsed fields (they tend to be cleaner than free-text)
  for (const key of Object.keys(fields)) {
    const k = key.toLowerCase();
    if (
      k.startsWith('affaire') ||
      k.startsWith('requête n') ||
      k.startsWith('dossier') ||
      k === 'référence' ||
      k.startsWith('réf')
    ) {
      const v = fields[key]?.trim();
      if (v) return v;
    }
  }
  for (const pat of AFFAIRE_PATTERNS) {
    const m = text.match(pat);
    if (m?.[1]) return m[1].trim();
  }
  return null;
}

// ---------------------------------------------------------------------------
// Page parser — generic <li> extractor
// ---------------------------------------------------------------------------

interface ParsedRow {
  rubrique: number;
  affaire: string | null;
  publication_date: string | null;
  title: string;
  section: string;
  subsection: string;
  raw_text: string;
  fields: Record<string, string>;
}

function parsePage(html: string): ParsedRow[] {
  const $ = cheerio.load(html);
  const rows: ParsedRow[] = [];

  $(
    '#block-2 > div.panel-pane.pane-edg-2016-fao-search-pane > div > div.resultats > ul > li',
  ).each((_, row) => {
    const article = $(row).find('article');

    // Date — three child divs (day / month / year)
    const dateDivs = article.find('div.fao_date').children('div');
    const day = $(dateDivs[0]).text().trim();
    const month = $(dateDivs[1]).text().trim();
    const year = $(dateDivs[2]).text().trim();
    const publication_date = parseFrenchDateParts(day, month, year);

    // Section / subsection
    const section =
      article
        .find('span.fao-rubrique')
        .children()
        .toArray()
        .map((s) => $(s).text().trim())
        .filter(Boolean)[0] || '';
    const subsection =
      article
        .find('.fao-sousrubrique')
        .children()
        .toArray()
        .map((s) => $(s).text().trim())
        .filter(Boolean)[0] || '';

    const title = article.find('h3.fao_titre').text().trim() || '';

    // Raw body — reuse fao_body_plus if present, fall back to fao_plus first <p>
    let bodyHtml: string | null =
      article.find('div.fao_body_plus').html() ??
      article.find('div.fao_plus > div > p').first().html() ??
      null;
    if (!bodyHtml) return;

    // Split on <br> tokens, strip tags + html entities, build raw text + key:value pairs
    const lines = bodyHtml
      .split(/<br\s*\/?>/i)
      .map((seg) =>
        seg
          .replace(/<[^>]+>/g, '')
          .replace(/&nbsp;?/g, ' ')
          .replace(/&amp;/g, '&')
          .replace(/&#x27;|&apos;/g, "'")
          .replace(/&quot;/g, '"')
          .replace(/&lt;/g, '<')
          .replace(/&gt;/g, '>')
          .trim(),
      )
      .filter(Boolean);

    const fields: Record<string, string> = {};
    for (const line of lines) {
      const colonIdx = line.indexOf(':');
      if (colonIdx === -1) continue;
      const key = line.substring(0, colonIdx).trim();
      const value = line.substring(colonIdx + 1).trim();
      if (key && value) fields[key] = value;
    }

    const raw_text = lines.join(' | ');
    const affaire = extractAffaire(raw_text + ' ' + title, fields);

    rows.push({
      rubrique: RUBRIQUE,
      affaire,
      publication_date,
      title,
      section,
      subsection,
      raw_text,
      fields,
    });
  });

  return rows;
}

// ---------------------------------------------------------------------------
// Total-results selector — try both formats used across rubriques
// ---------------------------------------------------------------------------

function readTotal(html: string): number {
  const $ = cheerio.load(html);
  const txt =
    $('#block-2 > div.panel-pane.pane-edg-2016-fao-search-pane > div > div.nombre-resultats > p > em')
      .text()
      .trim() ||
    $('#block-2 > div.panel-pane.pane-edg-2016-fao-search-pane > div > div.nombre-resultats')
      .text()
      .trim();
  const m = txt.match(/^\s*(\d+)/);
  return m ? parseInt(m[1], 10) : 0;
}

// ---------------------------------------------------------------------------
// One window — solve CAPTCHA, fetch all pages, upsert. Returns counts.
// ---------------------------------------------------------------------------

interface WindowResult {
  extracted: number;
  upserted: number;
  pages: number;
  pageFailures: number;
  durationSec: string;
}

async function runWindow(dateFrom: string, dateTo: string): Promise<WindowResult> {
  const t0 = Date.now();
  console.log(`  Date range: ${dateFrom} → ${dateTo}`);
  console.log('  Solving CAPTCHA...');
  let { cookies } = await createFaoSession(RUBRIQUE, dateFrom, dateTo);

  console.log('  Fetching first page...');
  const firstHtml = await fetchPage(1, cookies, dateFrom, dateTo);
  const total = readTotal(firstHtml);
  const pages = Math.max(1, Math.ceil(total / RESULTS_PER_PAGE));
  console.log(`  Total results: ${total}, pages: ${pages}`);

  const allRows: ParsedRow[] = [];
  allRows.push(...parsePage(firstHtml));
  if (total > 0) console.log(`  Page 1/${pages}: ${allRows.length} rows`);

  let pageFailures = 0;
  for (let p = 2; p <= pages; p++) {
    try {
      const html = await fetchPage(p, cookies, dateFrom, dateTo);
      const rows = parsePage(html);
      allRows.push(...rows);
      console.log(`  Page ${p}/${pages}: ${rows.length} rows`);
      await sleep(RATE_LIMIT_MS);
    } catch (err) {
      if (String(err).includes('CAPTCHA_REDIRECT')) {
        console.log('  CAPTCHA redirect, re-solving...');
        try {
          const newSession = await createFaoSession(RUBRIQUE, dateFrom, dateTo);
          cookies = newSession.cookies;
          const html = await fetchPage(p, cookies, dateFrom, dateTo);
          const rows = parsePage(html);
          allRows.push(...rows);
          console.log(`  Page ${p}/${pages}: ${rows.length} rows (after re-auth)`);
        } catch (reAuthErr) {
          console.error(`  Failed page ${p} after re-auth: ${reAuthErr}`);
          pageFailures++;
        }
      } else {
        console.error(`  Error on page ${p} (skipping): ${err}`);
        pageFailures++;
      }
    }
  }

  if (allRows.length === 0) {
    const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
    console.log(`  No rows in window. (${elapsed}s)`);
    return { extracted: 0, upserted: 0, pages, pageFailures, durationSec: elapsed };
  }

  // Drop in-batch duplicates so ON CONFLICT doesn't error on same key twice.
  const seen = new Set<string>();
  const deduped = allRows.filter((r) => {
    const key = `${r.rubrique}|${r.affaire ?? `md5:${r.raw_text}`}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
  if (deduped.length !== allRows.length) {
    console.log(`  Deduplicated: ${allRows.length} → ${deduped.length}`);
  }

  const records = deduped.map((r) => ({
    rubrique: r.rubrique,
    affaire: r.affaire,
    publication_date: r.publication_date,
    title: r.title,
    section: r.section,
    subsection: r.subsection,
    raw_text: r.raw_text,
    fields: r.fields,
  }));

  console.log(`  Upserting ${records.length} records...`);
  const totalUpserted = await upsert(SCHEMA, TABLE, records, ON_CONFLICT, BATCH_SIZE);
  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(`  ✓ Window done: ${totalUpserted} upserted in ${elapsed}s`);
  return {
    extracted: allRows.length,
    upserted: totalUpserted,
    pages,
    pageFailures,
    durationSec: elapsed,
  };
}

// ---------------------------------------------------------------------------
// Main — single window OR multi-year backfill
// ---------------------------------------------------------------------------

async function main() {
  console.log('='.repeat(60));
  console.log(`  FAO Multi — Rubrique ${RUBRIQUE}`);
  console.log(`  Target: ${SCHEMA}.${TABLE} on re-llm`);
  console.log('='.repeat(60));

  const t0 = Date.now();
  await verifyAccess(SCHEMA, TABLE);

  const startYear = parseInt(process.env.BACKFILL_START_YEAR ?? '', 10);
  const endYear = parseInt(process.env.BACKFILL_END_YEAR ?? '', 10);
  const isBackfill = Number.isFinite(startYear) && Number.isFinite(endYear);

  if (isBackfill) {
    console.log(`  BACKFILL: ${startYear} → ${endYear} (year-by-year)`);
    let totalExtracted = 0;
    let totalUpserted = 0;
    const yearFailures: number[] = [];
    for (let y = startYear; y <= endYear; y++) {
      console.log(`\n--- Year ${y} ---`);
      try {
        const r = await runWindow(`${y}-01-01`, `${y}-12-31`);
        totalExtracted += r.extracted;
        totalUpserted += r.upserted;
      } catch (err) {
        console.error(`  Year ${y} FAILED: ${err}`);
        yearFailures.push(y);
      }
    }
    const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
    console.log('\n' + '='.repeat(60));
    console.log('  BACKFILL COMPLETE');
    console.log(`  Rubrique:        ${RUBRIQUE}`);
    console.log(`  Years:           ${startYear}–${endYear}`);
    console.log(`  Rows extracted:  ${totalExtracted}`);
    console.log(`  Rows upserted:   ${totalUpserted}`);
    console.log(`  Years failed:    ${yearFailures.length === 0 ? 'none' : yearFailures.join(', ')}`);
    console.log(`  Duration:        ${elapsed}s`);
    console.log('='.repeat(60));
    return;
  }

  // Single-window mode
  const { dateFrom, dateTo } = getDateRange();
  const r = await runWindow(dateFrom, dateTo);
  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log('\n' + '='.repeat(60));
  console.log('  IMPORT COMPLETE');
  console.log(`  Rubrique:        ${RUBRIQUE}`);
  console.log(`  Date range:      ${dateFrom} → ${dateTo}`);
  console.log(`  Rows extracted:  ${r.extracted}`);
  console.log(`  Rows upserted:   ${r.upserted}`);
  console.log(`  Duration:        ${elapsed}s`);
  console.log('='.repeat(60));

  if (r.upserted === 0 && r.extracted > 0) {
    console.error('  FAILED: zero rows upserted despite having records');
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
