/**
 * FR Feuille Officielle — Transaction Fetcher (Category 15)
 *
 * Fetches property transaction articles from the Feuille officielle du canton
 * de Fribourg and upserts them into bronze.transactions_national.
 *
 * Workflow:
 *   1. For each year, fetch the archive page to discover issues
 *   2. For each issue, fetch category 15 and each district page (111-117)
 *   3. For each district page, find article node links
 *   4. Fetch each article page, extract commune headers + transaction blocks
 *   5. Parse structured data using parser.ts
 *   6. Upsert into bronze.transactions_national in batches
 *
 * Usage:
 *   npx tsx fetch-transactions.ts           # current year only
 *   npx tsx fetch-transactions.ts 2024      # specific year
 *   npx tsx fetch-transactions.ts 2020 2021 # multiple years
 *
 * Environment variables:
 *   SUPABASE_URL              - Supabase project URL (required)
 *   SUPABASE_SERVICE_ROLE_KEY - service_role key     (required)
 */

import { createClient } from '@supabase/supabase-js';
import * as cheerio from 'cheerio';
import {
  parseTransactionBlock,
  extractCommune,
  countBuyers,
  countSellers,
} from './parser.js';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const BASE_URL = 'https://fo.fr.ch';
const CANTON = 'FR';
const SOURCE_FILE = 'fo_fr_ch';
const BATCH_SIZE = 100;
const RATE_LIMIT_MS = 500;

// District IDs for category 15
const DISTRICTS: { id: number; name: string }[] = [
  { id: 111, name: 'Registre foncier de la Sarine' },
  { id: 112, name: 'Registre foncier de la Singine' },
  { id: 113, name: 'Registre foncier de la Gruyère' },
  { id: 114, name: 'Registre foncier du Lac' },
  { id: 115, name: 'Registre foncier de la Glâne' },
  { id: 116, name: 'Registre foncier de la Broye' },
  { id: 117, name: 'Registre foncier de la Veveyse' },
];

// ---------------------------------------------------------------------------
// Supabase client
// ---------------------------------------------------------------------------

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.error(
    'ERROR: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required',
  );
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchPage(url: string): Promise<string | null> {
  try {
    const response = await fetch(url);
    if (!response.ok) {
      if (response.status === 404) return null;
      console.error(`  HTTP ${response.status} for ${url}`);
      return null;
    }
    return await response.text();
  } catch (err) {
    console.error(`  Fetch error for ${url}: ${err}`);
    return null;
  }
}

/**
 * Extract issue numbers from the archive year page.
 */
function extractIssueNumbers(html: string, year: number): number[] {
  const $ = cheerio.load(html);
  const issues: number[] = [];
  const seen = new Set<number>();

  $('a[href]').each((_, el) => {
    const href = $(el).attr('href');
    if (!href) return;

    const match = href.match(new RegExp(`/archive/${year}/(\\d+)(?:\\D|$)`));
    if (match) {
      const num = parseInt(match[1], 10);
      if (!seen.has(num)) {
        seen.add(num);
        issues.push(num);
      }
    }
  });

  return issues.sort((a, b) => a - b);
}

/**
 * Extract node URLs from a district page.
 */
function extractNodeUrls(html: string): string[] {
  const $ = cheerio.load(html);
  const urls: string[] = [];
  const seen = new Set<string>();

  $('a[href]').each((_, el) => {
    const href = $(el).attr('href');
    if (!href) return;

    const match = href.match(/\/node\/(\d+)/);
    if (match) {
      const url = `${BASE_URL}/node/${match[1]}`;
      if (!seen.has(url)) {
        seen.add(url);
        urls.push(url);
      }
    }
  });

  return urls;
}

/**
 * Extract the node ID from a URL like "https://fo.fr.ch/node/118852".
 */
function extractNodeId(url: string): string {
  const match = url.match(/\/node\/(\d+)/);
  return match ? match[1] : '';
}

/**
 * Extract plain text from the article page HTML.
 * Strips HTML tags and normalizes whitespace.
 */
function extractArticleText(html: string): string {
  const $ = cheerio.load(html);

  // Try common content selectors
  let content = $('.field--name-body').text();
  if (!content) content = $('article').text();
  if (!content) content = $('.node__content').text();
  if (!content) content = $('main').text();

  return content.replace(/\s+/g, ' ').trim();
}

/**
 * Extract commune sections from article text.
 * Returns array of { commune, text } where text is the content
 * for that commune section.
 */
function extractCommuneSections(
  fullText: string,
): { commune: string; text: string }[] {
  // Split by "Commune de X" or "Gemeinde X" headers
  const pattern = /(?:Commune\s+de|Gemeinde)\s+([^\n\r,;]+)/gi;
  const sections: { commune: string; text: string }[] = [];
  const matches = [...fullText.matchAll(pattern)];

  if (matches.length === 0) {
    // No commune headers found — use the whole text with unknown commune
    const commune = extractCommune(fullText);
    return [{ commune: commune || 'Inconnu', text: fullText }];
  }

  for (let i = 0; i < matches.length; i++) {
    const commune = matches[i][1].trim();
    const start = matches[i].index! + matches[i][0].length;
    const end = i + 1 < matches.length ? matches[i + 1].index! : fullText.length;
    const text = fullText.substring(start, end).trim();
    sections.push({ commune, text });
  }

  return sections;
}

// ---------------------------------------------------------------------------
// Transaction record type
// ---------------------------------------------------------------------------

interface TransactionRecord {
  source_id: string;
  source_url: string;
  transaction_date: string | null;
  address: string;
  reason: string;
  property_type: string | null;
  price: number | null;
  surface_m2: number | null;
  price_per_m2: number | null;
  nb_buyers: number;
  buyers: string;
  nb_sellers: number;
  sellers: string;
  previous_transaction_date: string | null;
  canton: string;
  source_file: string;
  raw_data: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// Main pipeline
// ---------------------------------------------------------------------------

async function processYear(year: number): Promise<TransactionRecord[]> {
  const records: TransactionRecord[] = [];

  console.log(`\n--- Year ${year} ---`);

  // 1. Fetch archive page to get issue numbers
  const archiveUrl = `${BASE_URL}/archive/${year}`;
  console.log(`  Fetching archive: ${archiveUrl}`);
  const archiveHtml = await fetchPage(archiveUrl);
  await sleep(RATE_LIMIT_MS);

  if (!archiveHtml) {
    console.log(`  No archive page for ${year}, skipping.`);
    return records;
  }

  let issues = extractIssueNumbers(archiveHtml, year);
  if (issues.length === 0) {
    // Archive page doesn't list issues (common for years < 2025).
    // Brute-force: try all issues 1-52.
    console.log(`  No issue links found on archive page — trying 1-52 directly`);
    issues = Array.from({ length: 52 }, (_, i) => i + 1);
  }
  console.log(`  Checking ${issues.length} issues`);

  // 2. For each issue, check category 15
  for (const issue of issues) {
    const catUrl = `${BASE_URL}/archive/${year}/${issue}/15`;
    console.log(`\n  Issue ${issue}: checking category 15`);
    const catHtml = await fetchPage(catUrl);
    await sleep(RATE_LIMIT_MS);

    if (!catHtml) {
      console.log(`    Category 15 not found for issue ${issue}`);
      continue;
    }

    // 3. For each district, fetch the district page
    for (const district of DISTRICTS) {
      const districtUrl = `${BASE_URL}/archive/${year}/${issue}/15/${district.id}`;
      const districtHtml = await fetchPage(districtUrl);
      await sleep(RATE_LIMIT_MS);

      if (!districtHtml) continue;

      const nodeUrls = extractNodeUrls(districtHtml);
      if (nodeUrls.length === 0) continue;

      console.log(
        `    District ${district.id} (${district.name}): ${nodeUrls.length} articles`,
      );

      // 4. Fetch each article page
      for (const nodeUrl of nodeUrls) {
        const nodeId = extractNodeId(nodeUrl);
        const articleHtml = await fetchPage(nodeUrl);
        await sleep(RATE_LIMIT_MS);

        if (!articleHtml) continue;

        const articleText = extractArticleText(articleHtml);
        if (!articleText) {
          console.log(`      Node ${nodeId}: empty content, skipping`);
          continue;
        }

        // 5. Extract commune sections and parse transactions
        const sections = extractCommuneSections(articleText);

        let seqInArticle = 0;
        for (const section of sections) {
          const parsed = parseTransactionBlock(section.text, section.commune);

          for (const tx of parsed) {
            seqInArticle++;
            const sourceId = `FR-${year}-${issue}-${nodeId}-${seqInArticle}`;

            const addressParts = [tx.parcel_numbers, tx.address]
              .filter(Boolean)
              .join(', ');

            const record: TransactionRecord = {
              source_id: sourceId,
              source_url: nodeUrl,
              transaction_date: null, // Not available in gazette text
              address: addressParts || '',
              reason: 'acquisition',
              property_type: tx.property_type,
              price: null, // Never disclosed in gazette
              surface_m2: tx.surface_m2,
              price_per_m2: null,
              nb_buyers: countBuyers(tx),
              buyers: tx.buyers,
              nb_sellers: countSellers(tx),
              sellers: tx.sellers,
              previous_transaction_date: tx.previous_date,
              canton: CANTON,
              source_file: SOURCE_FILE,
              raw_data: {
                year,
                issue,
                district_id: district.id,
                district_name: district.name,
                commune: section.commune,
                raw_text: tx.raw_text,
              },
            };

            records.push(record);
          }
        }
      }
    }
  }

  return records;
}

async function upsertBatch(records: TransactionRecord[]): Promise<number> {
  const { error, count } = await supabase
    .schema('bronze')
    .from('transactions_national')
    .upsert(records, { onConflict: 'source_id,canton', count: 'exact' });

  if (error) {
    console.error(`  Upsert error: ${error.message}`);
    return 0;
  }

  return count ?? records.length;
}

async function main() {
  console.log('='.repeat(60));
  console.log('  FR Feuille Officielle — Transaction Pipeline');
  console.log('  Source: fo.fr.ch (category 15)');
  console.log('  Target: bronze.transactions_national');
  console.log('='.repeat(60));

  const startTime = Date.now();

  // Parse year arguments
  const args = process.argv.slice(2);
  let years: number[];

  if (args.length > 0) {
    years = args.map((a) => parseInt(a, 10)).filter((n) => !isNaN(n));
  } else {
    // Default: current year only
    years = [new Date().getFullYear()];
  }

  console.log(`  Years: ${years.join(', ')}`);

  // Process each year
  const allRecords: TransactionRecord[] = [];
  let totalPages = 0;

  for (const year of years) {
    const records = await processYear(year);
    allRecords.push(...records);
  }

  console.log(`\n  Total transactions parsed: ${allRecords.length}`);

  if (allRecords.length === 0) {
    console.log('  No transactions to upsert. Exiting.');
    console.log('='.repeat(60));
    return;
  }

  // Upsert in batches
  let totalUpserted = 0;
  console.log(
    `\n  Upserting ${allRecords.length} records (batch size: ${BATCH_SIZE})...`,
  );

  for (let i = 0; i < allRecords.length; i += BATCH_SIZE) {
    const batch = allRecords.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(allRecords.length / BATCH_SIZE);

    const upserted = await upsertBatch(batch);
    totalUpserted += upserted;

    console.log(
      `  Batch ${batchNum}/${totalBatches}: ${upserted} rows upserted`,
    );
  }

  // Summary
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  console.log(`\n${'='.repeat(60)}`);
  console.log('  IMPORT COMPLETE');
  console.log(`  Transactions parsed:  ${allRecords.length}`);
  console.log(`  Records upserted:     ${totalUpserted}`);
  console.log(`  Duration:             ${elapsed}s`);
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
