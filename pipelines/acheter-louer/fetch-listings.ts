/**
 * AcheterLouer — Listing Fetcher (search-card mode)
 *
 * Fetches property listings from acheter-louer.ch by parsing search-result
 * cards directly. Per-listing detail navigation is skipped to reduce
 * residential-proxy bandwidth ~10-20× (similar to the Homegate SSR insight).
 *
 * Trade-off: detail-page-only fields are lost (year_of_construction,
 * surface_m2, floor, full description, full image gallery, agency phone).
 * Cards still provide id, URL, title, price, rooms, postal/city, object
 * type, short teaser, single thumbnail, and agency logo.
 *
 * Workflow:
 *   1. Launch Playwright with residential proxy, block images/css/fonts.
 *   2. For each canton × buy/rent: navigate to first page, get total count,
 *      then paginate via ?pos=offset&action=back (cookies persist in context).
 *   3. Parse cards from search-result HTML, map to bronze schema, upsert.
 *
 * Why Playwright at all? acheter-louer.ch is behind Cloudflare with JA3
 * fingerprinting — plain fetch returns 403. A headless browser is required
 * to defeat the TLS challenge, but loading detail pages per-listing is not.
 */

import { chromium, type Browser, type BrowserContext, type Page } from 'playwright';
import * as cheerio from 'cheerio';
import { upsert, sleep, verifyAccess, markStaleListings } from '../_shared/re-llm.js';
import { proxyUrl } from '../_shared/proxy.js';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const BATCH_SIZE = 100;
const PAGE_SIZE = 50;
const PAGE_LOAD_WAIT = 1_500;

const CANTONS: { name: string; region: number }[] = [
  { name: 'Geneva', region: 3 },
  { name: 'Vaud', region: 8 },
  { name: 'Fribourg', region: 2 },
  { name: 'Jura', region: 4 },
  { name: 'Berne', region: 1 },
  { name: 'Neuchatel', region: 6 },
  { name: 'Valais', region: 7 },
];

// t=1 buy, t=2 rent
const TX_TYPES = [
  { label: 'Buy', t: 1, offer: 'achat' },
  { label: 'Rent', t: 2, offer: 'location' },
] as const;

// Block heavy resources to cut bandwidth on the search-result pages.
const BLOCKED_RESOURCES = new Set(['image', 'media', 'font', 'stylesheet']);

// ---------------------------------------------------------------------------
// Browser
// ---------------------------------------------------------------------------

let browser: Browser;

async function launchBrowser(): Promise<void> {
  const resUser = process.env.WEBSHARE_RESIDENTIAL_USER;
  const resPass = process.env.WEBSHARE_RESIDENTIAL_PASS;
  const resHost = process.env.WEBSHARE_RESIDENTIAL_HOST;
  const dcUser = process.env.WEBSHARE_PROXY_USER;
  const dcPass = process.env.WEBSHARE_PROXY_PASS;

  let pUrl: string | null = null;
  if (resUser && resPass && resHost) {
    pUrl = `http://${resUser}:${resPass}@${resHost}`;
    console.log(`    (using residential proxy: ${resHost})`);
  } else if (dcUser && dcPass) {
    pUrl = proxyUrl();
    console.log('    (using datacenter proxy)');
  }

  const launchArgs = [
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--disable-blink-features=AutomationControlled',
  ];

  if (pUrl) {
    const parsed = new URL(pUrl);
    const proxyServer = parsed.port
      ? `${parsed.protocol}//${parsed.hostname}:${parsed.port}`
      : parsed.origin;
    browser = await chromium.launch({
      headless: true,
      args: launchArgs,
      proxy: {
        server: proxyServer,
        username: parsed.username,
        password: parsed.password,
      },
    });
  } else {
    console.log('    (no proxy configured, launching direct)');
    browser = await chromium.launch({ headless: true, args: launchArgs });
  }
}

async function newContext(): Promise<BrowserContext> {
  const context = await browser.newContext({
    userAgent:
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  });
  // Drop heavy resource types — we only need the HTML for card parsing.
  await context.route('**/*', (route) => {
    if (BLOCKED_RESOURCES.has(route.request().resourceType())) {
      route.abort().catch(() => {});
    } else {
      route.continue().catch(() => {});
    }
  });
  return context;
}

// ---------------------------------------------------------------------------
// Card parser
// ---------------------------------------------------------------------------

const ID_FROM_URL = /-(\d+)\.html$/;
const POSTAL_CITY = /^(\d{4})\s+(.+)$/;
const ID_DIGITS = /^\d+$/;

function parseCard($: cheerio.CheerioAPI, el: cheerio.Element): Record<string, unknown> | null {
  const card = $(el);
  const idAttr = card.find('[data-idobj]').first().attr('data-idobj')?.trim();
  const idObject = idAttr && ID_DIGITS.test(idAttr) ? idAttr : null;

  // Find the first detail-page link (skip mailto/contact anchors).
  let href: string | undefined;
  card.find('a[href^="/fr/"]').each((_, a) => {
    if (href) return;
    const h = $(a).attr('href');
    if (h && ID_FROM_URL.test(h)) href = h.replace('#contact', '');
  });
  if (!idObject || !href) return null;

  const adUrl = `https://www.acheter-louer.ch${href}`;

  // URL pattern: /fr/{achat|location}-immobilier/{type}/{city}/{slug}-{id}.html
  const segments = href.split('/').filter(Boolean);
  const offerSegment = segments[1] || '';   // achat-immobilier | location-immobilier
  const offerType = offerSegment.startsWith('location') ? 'location' : 'achat';
  // URL can be /fr/{offer}/{type}/{city}/... OR /fr/{offer}/{agency}/{type}/{city}/...
  // Pick the first known object-type slug after the offer segment.
  const KNOWN_TYPES = new Set(['appartement', 'maison', 'terrain', 'commercial', 'parking', 'immeuble']);
  const object = segments.slice(2, -1).find((s) => KNOWN_TYPES.has(s)) || null;

  // Price
  const priceText = card.find('.price span').first().text().trim();
  const priceFormatted = priceText || null;
  const priceDigits = priceText.replace(/\D/g, '');
  const price = priceDigits ? parseInt(priceDigits, 10) : null;

  // Title block: "<h2>Maison à vendre<br>1241 Puplinge..."
  const titleHtml = card.find('h2.vign-title').first().html() || '';
  const titleParts = titleHtml
    .split(/<br\s*\/?>/i)
    .map((s) => cheerio.load(`<x>${s}</x>`)('x').text().trim())
    .filter(Boolean);
  const ad_title = titleParts[0] || null;
  const postalCityLine = titleParts[1] || '';
  const postalMatch = postalCityLine.match(POSTAL_CITY);
  const address = {
    no_postal: postalMatch?.[1] || null,
    city: postalMatch?.[2]?.trim() || null,
    street: null,
  };

  // Teaser description (em headline + body text)
  const headline = card.find('.vign-desc em').first().text().trim() || null;
  const body = card.find('.vign-desc').first().clone();
  body.find('em').remove();
  const teaser = body.text().replace(/\s+/g, ' ').trim() || null;
  const description = [headline, teaser].filter(Boolean);

  // Single thumbnail image (only one available at card level)
  let img = card.find('.imgObj img').first().attr('src') || null;
  if (img && img.startsWith('//')) img = `https:${img}`;
  const images = img ? [{ url: img }] : null;

  // Agency logo only — no name/phone/address available on the card
  const logoStyle = card.find('.agency_logo').first().attr('style') || '';
  const logoMatch = logoStyle.match(/url\(['"]?([^'")]+)['"]?\)/);
  const agencyLogo = logoMatch?.[1] || null;
  const agency = agencyLogo
    ? { name: null, image: null, logo: { url: agencyLogo }, city: null, emails: [], phone: null }
    : null;

  return {
    idObject,
    priceFormatted,
    price,
    ad_title,
    address,
    object,
    offerType,
    descriptionTitle: headline,
    shareText: null,
    description: description.length ? description : null,
    agency,
    images,
    ad_url: adUrl,
  };
}

// ---------------------------------------------------------------------------
// Page navigation
// ---------------------------------------------------------------------------

async function readSearchPage(page: Page, url: string): Promise<string | null> {
  try {
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60_000 });
  } catch (err: any) {
    console.log(`      goto failed: ${err.message}`);
    return null;
  }
  // Wait for either real cards OR an explicit "no results" marker. The DOM
  // can take a moment to populate after domcontentloaded; if neither appears,
  // we still return the HTML so the caller can decide.
  try {
    await page.waitForSelector('[data-idobj]', { timeout: 15_000 });
  } catch {
    /* no cards — return whatever we have */
  }
  return await page.content();
}

/**
 * Pull the largest `data-useposresult` value from the pagination block. That
 * is the offset of the LAST page. Plus PAGE_SIZE → total result count.
 * Falls back to single-page (offset=0) if no pagination is rendered.
 */
function maxOffset(html: string): number {
  const matches = html.matchAll(/data-useposresult="(\d+)"/g);
  let max = 0;
  for (const m of matches) {
    const n = parseInt(m[1], 10);
    if (n > max) max = n;
  }
  return max;
}

function countCards(html: string): number {
  return (html.match(/data-idobj="\d+"/g) || []).length;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  console.log('='.repeat(60));
  console.log('  AcheterLouer — Listing Pipeline (card mode)');
  console.log('  Source: acheter-louer.ch');
  console.log('  Target: bronze_ch.acheter_louer');
  console.log('='.repeat(60));

  const startTime = Date.now();

  await verifyAccess('bronze_ch', 'acheter_louer');

  const allRecords: Record<string, unknown>[] = [];

  for (const { name: canton, region } of CANTONS) {
    for (const { label, t } of TX_TYPES) {
      console.log(`\n  [${canton.toUpperCase()}] ${label}:`);

      await launchBrowser();

      try {
        const firstUrl = `https://www.acheter-louer.ch/?t=${t}&page=result&tri=&triSens=&dist=0&commune=&region=${region}&bounds=&area=&npa=&p=&communeName=&prixMin=&prixMax=&surfaceMin=&surfaceMax=&pieceMin=&pieceMax=&ns=`;

        const ctx = await newContext();
        const page = await ctx.newPage();

        // Page 1: load with full search criteria; this seeds the session.
        const firstHtml = await readSearchPage(page, firstUrl);
        if (!firstHtml) {
          console.log('    Page 1 fetch failed');
          await ctx.close();
          await browser.close();
          continue;
        }

        // Accept cookies if present
        try {
          const cookieBtn = await page.$('#onetrust-accept-btn-handler');
          if (cookieBtn) await cookieBtn.click({ timeout: 3_000 });
        } catch { /* none */ }

        const cardsP1 = countCards(firstHtml);
        if (cardsP1 === 0) {
          console.log('    No listings on page 1 (likely empty region)');
          await ctx.close();
          await browser.close();
          continue;
        }

        const lastOffset = maxOffset(firstHtml);
        const pages = Math.floor(lastOffset / PAGE_SIZE) + 1;
        console.log(`    ${cardsP1} cards on page 1, ~${pages} pages total`);

        // Card-extract a single HTML blob and push to allRecords
        const collect = (html: string) => {
          const $ = cheerio.load(html);
          let added = 0;
          $('#listing-results > div > div').each((_, el) => {
            const rec = parseCard($, el);
            if (!rec) return;
            allRecords.push({
              ...rec,
              source: 'acheter-louer',
              canton,
              publishing_status: 'online',
              time_online: 1,
              last_seen_at: new Date().toISOString(),
            });
            added++;
          });
          return added;
        };

        let n = collect(firstHtml);
        console.log(`    Page 1/${pages}: +${n}`);

        for (let p = 1; p < pages; p++) {
          const offset = p * PAGE_SIZE;
          const url = `https://www.acheter-louer.ch/?page=result&pos=${offset}&action=back`;
          const html = await readSearchPage(page, url);
          if (!html) {
            console.log(`    Page ${p + 1}/${pages}: failed, skipping`);
            continue;
          }
          n = collect(html);
          console.log(`    Page ${p + 1}/${pages}: +${n}`);
          if (n === 0) {
            console.log('    Empty page — stopping pagination early');
            break;
          }
          await sleep(PAGE_LOAD_WAIT);
        }

        await ctx.close();
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

  // Dedupe by ad_url — same listing can appear in multiple cantons in border cases.
  const seen = new Set<string>();
  const deduped = allRecords.filter((r) => {
    const url = r.ad_url as string;
    if (seen.has(url)) return false;
    seen.add(url);
    return true;
  });
  if (deduped.length !== allRecords.length) {
    console.log(`  Deduped: ${allRecords.length} → ${deduped.length}`);
  }

  console.log(`\n  Upserting ${deduped.length} records (batch size: ${BATCH_SIZE})...`);
  const totalUpserted = await upsert('bronze_ch', 'acheter_louer', deduped, 'ad_url', BATCH_SIZE);

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\n${'='.repeat(60)}`);
  console.log('  IMPORT COMPLETE');
  console.log(`  Listings fetched:  ${deduped.length}`);
  console.log(`  Records upserted:  ${totalUpserted}`);
  console.log(`  Duration:          ${elapsed}s`);
  console.log('='.repeat(60));

  if (totalUpserted === 0 && deduped.length > 0) {
    console.error('  FAILED: Zero rows upserted despite having records!');
    process.exit(1);
  }

  await markStaleListings('bronze_ch', 'acheter_louer', 'ad_url', 50, deduped.length);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
