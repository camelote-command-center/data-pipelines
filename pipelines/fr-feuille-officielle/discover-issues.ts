/**
 * FR Feuille Officielle — Issue Discovery
 *
 * Crawls the fo.fr.ch archive to discover all issues and their article node
 * URLs for categories 15 (transactions) and 21 (building permits).
 *
 * Output: JSON array to stdout with structure:
 *   { year, issue, category, district_id?, node_urls: string[] }
 *
 * Usage:
 *   npx tsx discover-issues.ts              # all years 2020-2026
 *   npx tsx discover-issues.ts 2025         # single year
 *   npx tsx discover-issues.ts 2024 2025    # specific years
 */

import * as cheerio from 'cheerio';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const BASE_URL = 'https://fo.fr.ch';
const DEFAULT_START_YEAR = 2020;
const DEFAULT_END_YEAR = 2026;
const RATE_LIMIT_MS = 500;

const CATEGORIES = [15, 21] as const;

// District IDs for category 15 (transactions)
const TRANSACTION_DISTRICTS = [111, 112, 113, 114, 115, 116, 117];

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface DiscoveredEntry {
  year: number;
  issue: number;
  category: number;
  district_id: number | null;
  node_urls: string[];
}

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
      if (response.status === 404) {
        return null; // Expected for missing issues/categories
      }
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
 * Extract node IDs from article links on a page.
 * Links look like "/node/118852" or "https://fo.fr.ch/node/118852".
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
      const nodeUrl = `${BASE_URL}/node/${match[1]}`;
      if (!seen.has(nodeUrl)) {
        seen.add(nodeUrl);
        urls.push(nodeUrl);
      }
    }
  });

  return urls;
}

/**
 * Extract issue numbers from the archive year page.
 * Links look like "/archive/2025/1", "/archive/2025/9", etc.
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
      const issueNum = parseInt(match[1], 10);
      if (!seen.has(issueNum)) {
        seen.add(issueNum);
        issues.push(issueNum);
      }
    }
  });

  return issues.sort((a, b) => a - b);
}

/**
 * Extract district links from a category 15 page.
 * Links look like "/archive/2025/1/15/111".
 */
function extractDistrictIds(html: string, year: number, issue: number): number[] {
  const $ = cheerio.load(html);
  const districts: number[] = [];
  const seen = new Set<number>();

  $('a[href]').each((_, el) => {
    const href = $(el).attr('href');
    if (!href) return;

    const match = href.match(
      new RegExp(`/archive/${year}/${issue}/15/(\\d+)`),
    );
    if (match) {
      const districtId = parseInt(match[1], 10);
      if (!seen.has(districtId)) {
        seen.add(districtId);
        districts.push(districtId);
      }
    }
  });

  return districts.sort((a, b) => a - b);
}

// ---------------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------------

async function discoverYear(year: number): Promise<DiscoveredEntry[]> {
  const entries: DiscoveredEntry[] = [];

  console.error(`\n--- Year ${year} ---`);

  // 1. Fetch the archive year page to find issue numbers
  const archiveUrl = `${BASE_URL}/archive/${year}`;
  console.error(`  Fetching archive: ${archiveUrl}`);
  const archiveHtml = await fetchPage(archiveUrl);
  await sleep(RATE_LIMIT_MS);

  if (!archiveHtml) {
    console.error(`  No archive page for year ${year}`);
    return entries;
  }

  let issues = extractIssueNumbers(archiveHtml, year);
  if (issues.length === 0) {
    console.error(`  No issue links found — trying 1-52 directly`);
    issues = Array.from({ length: 52 }, (_, i) => i + 1);
  }
  console.error(`  Checking ${issues.length} issues`);

  // 2. For each issue, check each category
  for (const issue of issues) {
    for (const category of CATEGORIES) {
      if (category === 15) {
        // Category 15: fetch the category page, then each district
        const catUrl = `${BASE_URL}/archive/${year}/${issue}/${category}`;
        console.error(`  Checking ${catUrl}`);
        const catHtml = await fetchPage(catUrl);
        await sleep(RATE_LIMIT_MS);

        if (!catHtml) continue;

        // Find which districts are linked on this page
        const foundDistricts = extractDistrictIds(catHtml, year, issue);
        const districtsToCheck =
          foundDistricts.length > 0 ? foundDistricts : TRANSACTION_DISTRICTS;

        for (const districtId of districtsToCheck) {
          const districtUrl = `${BASE_URL}/archive/${year}/${issue}/${category}/${districtId}`;
          console.error(`  Checking district ${districtId}: ${districtUrl}`);
          const districtHtml = await fetchPage(districtUrl);
          await sleep(RATE_LIMIT_MS);

          if (!districtHtml) continue;

          const nodeUrls = extractNodeUrls(districtHtml);
          if (nodeUrls.length > 0) {
            entries.push({
              year,
              issue,
              category,
              district_id: districtId,
              node_urls: nodeUrls,
            });
            console.error(
              `    Found ${nodeUrls.length} articles in district ${districtId}`,
            );
          }
        }
      } else {
        // Category 21: fetch the category page directly
        const catUrl = `${BASE_URL}/archive/${year}/${issue}/${category}`;
        console.error(`  Checking ${catUrl}`);
        const catHtml = await fetchPage(catUrl);
        await sleep(RATE_LIMIT_MS);

        if (!catHtml) continue;

        const nodeUrls = extractNodeUrls(catHtml);
        if (nodeUrls.length > 0) {
          entries.push({
            year,
            issue,
            category,
            district_id: null,
            node_urls: nodeUrls,
          });
          console.error(
            `    Found ${nodeUrls.length} articles for category ${category}`,
          );
        }
      }
    }
  }

  return entries;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  // Parse year arguments
  const args = process.argv.slice(2);
  let years: number[];

  if (args.length > 0) {
    years = args.map((a) => parseInt(a, 10)).filter((n) => !isNaN(n));
  } else {
    years = [];
    for (let y = DEFAULT_START_YEAR; y <= DEFAULT_END_YEAR; y++) {
      years.push(y);
    }
  }

  console.error(`Discovering issues for years: ${years.join(', ')}`);

  const allEntries: DiscoveredEntry[] = [];

  for (const year of years) {
    const entries = await discoverYear(year);
    allEntries.push(...entries);
  }

  // Summary to stderr
  const totalNodes = allEntries.reduce((sum, e) => sum + e.node_urls.length, 0);
  console.error(`\n=== Discovery complete ===`);
  console.error(`  Entries: ${allEntries.length}`);
  console.error(`  Total article URLs: ${totalNodes}`);

  // Output JSON to stdout
  console.log(JSON.stringify(allEntries, null, 2));
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
