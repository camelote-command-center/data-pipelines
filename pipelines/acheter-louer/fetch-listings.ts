/**
 * AcheterLouer — Listing Fetcher
 *
 * Fetches property listings from acheter-louer.ch using Playwright (JS rendering)
 * and upserts them into bronze."acheterLouer".
 *
 * Workflow:
 *   1. Launch Playwright with proxy
 *   2. For each canton × transaction type (buy/rent):
 *      - Navigate to search results, extract total count
 *      - Paginate through results (50/page)
 *      - For each listing, navigate to detail page and parse
 *   3. Map to bronze schema and upsert on ad_url
 *
 * Usage:
 *   npx tsx fetch-listings.ts
 *
 * Environment variables:
 *   SUPABASE_URL              - Supabase project URL  (required)
 *   SUPABASE_SERVICE_ROLE_KEY - service_role key      (required)
 *   WEBSHARE_PROXY_USER       - proxy username        (required)
 *   WEBSHARE_PROXY_PASS       - proxy password        (required)
 */

import { chromium, type Browser, type Page } from 'playwright';
import * as cheerio from 'cheerio';
import { upsertBronze, sleep } from '../_shared/supabase.js';
import { proxyUrl } from '../_shared/proxy.js';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const BATCH_SIZE = 100;
const PAGE_SIZE = 50;
const PAGE_LOAD_WAIT = 2_000;

const CANTONS: { name: string; region: number }[] = [
  { name: 'Geneva', region: 3 },
  { name: 'Vaud', region: 8 },
  { name: 'Fribourg', region: 2 },
  { name: 'Jura', region: 4 },
  { name: 'Berne', region: 1 },
  { name: 'Neuchatel', region: 6 },
  { name: 'Valais', region: 7 },
];

// t=1 for buy, t=2 for rent
const TX_TYPES = [
  { label: 'Buy', t: 1 },
  { label: 'Rent', t: 2 },
] as const;

// ---------------------------------------------------------------------------
// Browser management
// ---------------------------------------------------------------------------

let browser: Browser;

async function launchBrowser(): Promise<void> {
  // Proxy is optional — if env vars not set, launch without proxy
  const useProxy = process.env.WEBSHARE_PROXY_USER && process.env.WEBSHARE_PROXY_PASS;

  if (useProxy) {
    const pUrl = proxyUrl();
    const parsed = new URL(pUrl);
    // Use .origin to avoid trailing colon when port is default (80 for http)
    const proxyServer = parsed.port
      ? `${parsed.protocol}//${parsed.hostname}:${parsed.port}`
      : parsed.origin;

    browser = await chromium.launch({
      headless: true,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-blink-features=AutomationControlled',
      ],
      proxy: {
        server: proxyServer,
        username: parsed.username,
        password: parsed.password,
      },
    });
  } else {
    console.log('    (no proxy configured, launching direct)');
    browser = await chromium.launch({
      headless: true,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-blink-features=AutomationControlled',
      ],
    });
  }
}

async function newPage(): Promise<Page> {
  const context = await browser.newContext({
    userAgent:
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
  });
  return context.newPage();
}

// ---------------------------------------------------------------------------
// Detail table parser
// ---------------------------------------------------------------------------

function getOtherData($: cheerio.CheerioAPI): Record<string, unknown> {
  const res: Record<string, unknown> = {};
  const table = $(
    '#content > div:nth-child(9) > div.row > div > div > div > table > tbody > tr',
  );

  table.each((_, row) => {
    const key = $(row).find('td:eq(0)').text().trim();
    const value = $(row).find('td:eq(1)').text().trim();
    if (!value) return;

    const mapping: Record<string, string> = {
      'Surface utilisable (m2)': 'usable_surface_m2',
      'Surface du terrain (m2)': 'land_surface_m2',
      'Surface (m2)': 'surface_m2',
      'Année de rénovation': 'year_of_renovation',
      'Surface habitable (m2)': 'living_surface_m2',
      'Année de construction': 'year_of_construction',
      Parking: 'parking',
      Vue: 'view',
      Etat: 'condition',
      'Date de disponibilité': 'availability_date',
      Etage: 'floor',
      'Volume (m3)': 'volume_m3',
      'Garage(s)': 'garages',
      'Parc(s) extérieur(s)': 'outdoor_parking_spaces',
      'Parc(s) intérieur(s)': 'indoor_parking_spaces',
      'Surface utile': 'usable_surface',
    };

    const field = mapping[key];
    if (field) res[field] = value;
  });

  return res;
}

// ---------------------------------------------------------------------------
// Parse a single detail page
// ---------------------------------------------------------------------------

async function parseDetailPage(page: Page, link: string): Promise<Record<string, unknown> | null> {
  if (link.includes('javascript')) return null;

  const linkSplit = link.split('-');
  const rawIdObject = linkSplit[linkSplit.length - 1];
  const idObject = rawIdObject.replace('.html', '').trim();

  try {
    await page.goto(link, { waitUntil: 'domcontentloaded', timeout: 60_000 });
    await page.waitForSelector(
      '#content > div:nth-child(5) > div.header-details > div > div > div.col-sm-8.col-md-9.hr-details > h1',
      { timeout: 15_000 },
    );
  } catch {
    return null;
  }

  const content = await page.content();
  const $ = cheerio.load(content);

  const result: Record<string, unknown> = { idObject };

  // Price
  const rawPrice = $(
    '#content > div:nth-child(5) > div.header-details > div > div > div.col-sm-8.col-md-9.hr-details > div.tablediv > div.fs-28.fw-500.js-price',
  )
    .text()
    .trim();
  result.priceFormatted = rawPrice || null;
  const textPrice = rawPrice.replace(/\D/g, '');
  result.price = textPrice ? parseInt(textPrice, 10) : null;

  // Title + address
  const title = $(
    '#content > div:nth-child(5) > div.header-details > div > div > div.col-sm-8.col-md-9.hr-details > h1',
  )
    .text()
    .trim();
  result.ad_title = title || null;

  const match = title.match(/(\d{4})\s+([^-\n]+)/);
  result.address = {
    no_postal: match?.[1] || null,
    city: match?.[2]?.trim() || null,
    street: null,
  };

  // Object type + offer type
  result.object = $('#d > a > span').text().trim() || null;
  result.offerType = $('#a > a > span').text().trim() || null;

  // Description
  result.descriptionTitle =
    $(
      '#content > div.container.content-details > div.row.m-top-15.m-bot-20 > div > h2 > i',
    )
      .text()
      .trim() || null;

  const rawShareText = $(
    '#requestBtn > div.hidden-xs > p.text-center.p-top-5.sm-p-bot-30 > a:nth-child(4)',
  ).attr('href');
  result.shareText = rawShareText
    ? decodeURIComponent(
        rawShareText.replace('mailto://?subject=', '').replace('&body=', ', '),
      )
    : null;

  const rawDescription = $(
    '#content > div.container.content-details > div:nth-child(2) > div.col-sm-9 > div > p',
  )
    .html();
  result.description = rawDescription
    ? rawDescription
        .split('<br>')
        .map((s) => s.trim())
        .filter(Boolean)
    : null;

  // Other data from table
  const otherData = getOtherData($);

  // Agency
  const agencySelector = $(
    '#content > div.container.hidden-print.js-file-request-anchor > div.crosslisting > div.row.p-top-30.p-bot-30.m-top-20.m-bot-50.bg-white.fw-400.contact.text-center > div:nth-child(1)',
  );
  const agencyImage = agencySelector.find('div:eq(0) > img').attr('src') || null;
  const agencyInfo = agencySelector
    .find('div:eq(1)')
    .contents()
    .filter(function (this: any) { return this.nodeType === 3; })
    .text()
    .trim();
  const [agencyName, , agencyAddress] = (agencyInfo || '').split('\n').map((el) => el.trim());
  const agencyPhone = agencySelector
    .find('div:eq(1) > div')
    .contents()
    .filter(function (this: any) { return this.nodeType === 3; })
    .text()
    .trim();
  result.agency = {
    name: agencyName || null,
    image: agencyImage ? { url: agencyImage } : null,
    logo: null,
    city: agencyAddress || null,
    emails: [],
    phone: agencyPhone || null,
  };

  // Images — store original URLs
  const imageRows = $('#owl-container > div > div.owl-stage-outer > div > div');
  const images: { url: string }[] = [];
  imageRows.each((_, row) => {
    let src = $(row).find('div > a > img').attr('src');
    if (src) {
      if (src.startsWith('//')) src = `https:${src}`;
      images.push({ url: src });
    }
  });
  result.images = images.length ? images : null;

  return { ...result, ...otherData };
}

// ---------------------------------------------------------------------------
// Main pipeline
// ---------------------------------------------------------------------------

async function main() {
  console.log('='.repeat(60));
  console.log('  AcheterLouer — Listing Pipeline');
  console.log('  Source: acheter-louer.ch');
  console.log('  Target: bronze."acheterLouer"');
  console.log('='.repeat(60));

  const startTime = Date.now();
  const allRecords: Record<string, unknown>[] = [];

  for (const { name: canton, region } of CANTONS) {
    for (const { label, t } of TX_TYPES) {
      console.log(`\n  [${canton.toUpperCase()}] ${label}:`);

      // Launch a fresh browser per canton/type to avoid memory leaks
      await launchBrowser();

      try {
        const firstUrl = `https://www.acheter-louer.ch/?t=${t}&page=result&tri=&triSens=&dist=0&commune=&region=${region}&bounds=&area=&npa=&p=&communeName=&prixMin=&prixMax=&surfaceMin=&surfaceMax=&pieceMin=&pieceMax=&ns=`;

        const searchPage = await newPage();

        await searchPage.goto(firstUrl, { waitUntil: 'domcontentloaded', timeout: 60_000 });

        // Accept cookies if present
        try {
          const cookieBtn = await searchPage.waitForSelector('#onetrust-accept-btn-handler', { timeout: 5_000 });
          if (cookieBtn) await cookieBtn.click();
        } catch { /* no cookie banner */ }

        // Wait for results
        try {
          await searchPage.waitForSelector('#listing-results > div > div:nth-child(1)', { timeout: 15_000 });
        } catch {
          console.log('    No results found');
          await browser.close();
          continue;
        }

        // Get total count
        const content = await searchPage.content();
        const $ = cheerio.load(content);
        const resultsText = $(
          '#results-filters > div > div > div > table > tbody > tr > td:nth-child(1) > span.nb-results',
        )
          .text()
          .trim();
        const totalResults = parseInt(resultsText, 10) || 0;
        const pages = Math.ceil(totalResults / PAGE_SIZE);
        console.log(`    ${totalResults} results, ${pages} pages`);

        await searchPage.close();

        // Process each page
        for (let p = 0; p < pages; p++) {
          const offset = p * PAGE_SIZE;
          console.log(`    Page ${p + 1}/${pages}`);

          const listPage = await newPage();

          // Navigate to first URL first for cookies, then to offset page
          if (p > 0) {
            await listPage.goto(firstUrl, { waitUntil: 'domcontentloaded', timeout: 60_000 });
            await sleep(1_000);
          }

          await listPage.goto(
            `https://www.acheter-louer.ch/?page=result&pos=${offset}&action=back`,
            { waitUntil: 'domcontentloaded', timeout: 60_000 },
          );

          try {
            await listPage.waitForSelector('#listing-results > div > div', { timeout: 15_000 });
          } catch {
            console.log('      No results on this page');
            await listPage.close();
            continue;
          }

          const pageContent = await listPage.content();
          const $page = cheerio.load(pageContent);
          const rows = $page('#listing-results > div > div');

          // Collect detail links
          const links: string[] = [];
          rows.each((_, row) => {
            const rawLink = $page(row).find('div > a').attr('href');
            if (rawLink && !rawLink.includes('javascript')) {
              links.push(`https://www.acheter-louer.ch${rawLink.replace('#contact', '')}`);
            }
          });

          await listPage.close();

          // Parse each detail page
          for (const link of links) {
            const detailPage = await newPage();
            try {
              const data = await parseDetailPage(detailPage, link);
              if (data) {
                allRecords.push({
                  ...data,
                  source: 'acheter-louer',
                  canton,
                  ad_url: link,
                  publishing_status: 'online',
                  time_online: 1,
                });
              }
            } catch (err) {
              console.error(`      Error parsing ${link}: ${err}`);
            } finally {
              await detailPage.close();
            }

            await sleep(PAGE_LOAD_WAIT);
          }
        }

        await browser.close();
      } catch (err) {
        console.error(`    Error for ${canton} ${label}: ${err}`);
        try { await browser.close(); } catch { /* ignore */ }
      }
    }
  }

  console.log(`\n  Total records: ${allRecords.length}`);

  if (allRecords.length === 0) {
    console.log('  No listings to upsert. Exiting.');
    console.log('='.repeat(60));
    return;
  }

  // Upsert
  console.log(`\n  Upserting ${allRecords.length} records (batch size: ${BATCH_SIZE})...`);
  const totalUpserted = await upsertBronze('acheterLouer', allRecords, 'ad_url', BATCH_SIZE);

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\n${'='.repeat(60)}`);
  console.log('  IMPORT COMPLETE');
  console.log(`  Listings fetched:  ${allRecords.length}`);
  console.log(`  Records upserted:  ${totalUpserted}`);
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
