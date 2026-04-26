/**
 * Homegate — Listing Fetcher
 *
 * Fetches property listings from www.homegate.ch by parsing the SSR
 * `window.__INITIAL_STATE__` JSON embedded in each search-results page,
 * then upserts them into bronze_ch.homegate.
 *
 * Why SSR scraping (vs. an API)?
 *   Homegate's web pages already include the full listing payload
 *   (address, characteristics, localization, attachments, agent) in the
 *   server-rendered initial state — no separate detail call required.
 *   This is dramatically simpler and cheaper than Playwright.
 *
 * Workflow:
 *   For each canton × transaction type (buy/rent):
 *     1. GET page 1 to learn pageCount + listings on page 1
 *     2. GET pages 2..pageCount, extract listings each time
 *     3. Map each listing to the bronze_ch.homegate schema and upsert by ad_url
 *
 * Usage:
 *   npx tsx fetch-listings.ts
 *
 * Environment variables:
 *   RE_LLM_SUPABASE_URL              - re-LLM Supabase URL (required)
 *   RE_LLM_SUPABASE_SERVICE_ROLE_KEY - service_role key     (required)
 *   WEBSHARE_RESIDENTIAL_*           - residential proxy (optional but recommended)
 */

import { upsert, sleep, verifyAccess, markStaleListings } from '../_shared/re-llm.js';
import { httpFetch } from '../_shared/http.js';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const BASE_URL = 'https://www.homegate.ch';
const BATCH_SIZE = 100;
const RATE_LIMIT_MS = 600; // be polite — we hit one URL per page
const UPSERT_EVERY = 200;

const HEADERS: Record<string, string> = {
  accept:
    'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
  'accept-language': 'fr-CH,fr;q=0.9,en;q=0.8,de;q=0.7',
  'cache-control': 'max-age=0',
  'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"macOS"',
  'sec-fetch-dest': 'document',
  'sec-fetch-mode': 'navigate',
  'sec-fetch-site': 'same-origin',
  'upgrade-insecure-requests': '1',
  'user-agent':
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
};

// Homegate uses URL slugs, not Google placeIds.
const CANTONS = [
  'geneva', 'vaud', 'fribourg', 'jura', 'bern', 'neuchatel', 'valais',
  'zurich', 'aargau', 'basel-landschaft', 'basel-stadt', 'graubunden',
  'lucerne', 'st-gallen', 'ticino', 'thurgau', 'solothurn', 'schwyz',
  'schaffhausen', 'zug', 'appenzell-ausserrhoden', 'appenzell-innerrhoden',
  'glarus', 'nidwalden', 'obwalden', 'uri',
];

// "buy" -> sale, "rent" -> rental
const TRANSACTION_TYPES = ['buy', 'rent'] as const;
type TxType = typeof TRANSACTION_TYPES[number];

// ---------------------------------------------------------------------------
// Page fetcher: parse SSR JSON
// ---------------------------------------------------------------------------

interface PageResult {
  listings: any[];
  pageCount: number;
  resultCount: number;
  page: number;
}

function searchUrl(canton: string, txType: TxType, page: number): string {
  // Homegate canton URLs are e.g.
  //   https://www.homegate.ch/buy/real-estate/canton-geneva/matching-list?ep=2
  // ep is 1-indexed.
  const cantonSegment = `canton-${canton}`;
  const ep = page > 1 ? `?ep=${page}` : '';
  return `${BASE_URL}/${txType}/real-estate/${cantonSegment}/matching-list${ep}`;
}

function sliceJson(s: string, start: number): string | null {
  let depth = 0, inStr = false, esc = false;
  for (let i = start; i < s.length; i++) {
    const c = s[i];
    if (esc) { esc = false; continue; }
    if (c === '\\' && inStr) { esc = true; continue; }
    if (c === '"') inStr = !inStr;
    else if (!inStr) {
      if (c === '{') depth++;
      else if (c === '}') {
        depth--;
        if (depth === 0) return s.slice(start, i + 1);
      }
    }
  }
  return null;
}

function extractInitialState(html: string): any | null {
  const marker = 'window.__INITIAL_STATE__';
  const idx = html.indexOf(marker);
  if (idx < 0) return null;
  const eq = html.indexOf('=', idx);
  if (eq < 0) return null;
  const braceStart = html.indexOf('{', eq);
  if (braceStart < 0) return null;
  const blob = sliceJson(html, braceStart);
  if (!blob) return null;
  try {
    return JSON.parse(blob);
  } catch {
    return null;
  }
}

async function fetchPage(canton: string, txType: TxType, page: number): Promise<PageResult | null> {
  const url = searchUrl(canton, txType, page);
  const res = await httpFetch(url, {
    headers: HEADERS,
    useResidential: true,
    redirect: 'manual',
  } as any);

  if (res.status === 404) return null;
  if (!res.ok) throw new Error(`HTTP ${res.status} on ${url}`);

  const html = await res.text();
  const state = extractInitialState(html);
  const result = state?.resultList?.search?.fullSearch?.result;
  if (!result) {
    // No initial state — likely Cloudflare interstitial. Surface as transient.
    throw new Error(`No __INITIAL_STATE__ on ${url} (page ${page}, ${html.length} bytes)`);
  }

  return {
    listings: result.listings || [],
    pageCount: result.pageCount ?? 0,
    resultCount: result.resultCount ?? 0,
    page: result.page ?? page,
  };
}

// ---------------------------------------------------------------------------
// Field mapping
// ---------------------------------------------------------------------------

function pickLocText(loc: any, lang: string): string | null {
  return loc?.[lang]?.text?.title || loc?.[lang]?.text?.description
    ? (loc[lang].text.title ?? null)
    : null;
}

function pickDescription(loc: any): string[] | null {
  const lang =
    loc?.fr?.text?.description ? 'fr' :
    loc?.en?.text?.description ? 'en' :
    loc?.de?.text?.description ? 'de' :
    loc?.it?.text?.description ? 'it' : null;
  if (!lang) return null;
  const raw = loc[lang].text.description as string;
  // strip HTML and collapse to lines
  const cleaned = raw
    .split(/<[^>]*>/)
    .map((s) => s.trim())
    .filter(Boolean);
  return cleaned.length ? cleaned : null;
}

function pickTitle(loc: any): string | null {
  return (
    loc?.fr?.text?.title ||
    loc?.en?.text?.title ||
    loc?.de?.text?.title ||
    loc?.it?.text?.title ||
    null
  );
}

function pickImages(loc: any): { url: string }[] | null {
  const langs = ['fr', 'en', 'de', 'it'];
  for (const l of langs) {
    const att = loc?.[l]?.attachments;
    if (Array.isArray(att) && att.length) {
      const imgs = att
        .filter((a: any) => a.type === 'IMAGE' && a.url)
        .map((a: any) => ({ url: a.url as string }));
      if (imgs.length) return imgs;
    }
  }
  return null;
}

function categoryLabel(cats: string[] | undefined): string | null {
  if (!cats?.length) return null;
  // Lowercase and replace underscores for readability
  return cats[0].toLowerCase().replace(/_/g, ' ');
}

function extractFeatures(ch: any): string[] | null {
  if (!ch) return null;
  const flags: string[] = [];
  for (const [k, v] of Object.entries(ch)) {
    if (typeof v === 'boolean' && v && k.startsWith('has')) {
      flags.push(k.replace(/^has/, '').replace(/([A-Z])/g, ' $1').trim().toLowerCase());
    }
  }
  return flags.length ? flags : null;
}

function formatListing(canton: string, txType: TxType, raw: any): Record<string, unknown> | null {
  const id = raw.id || raw?.listing?.id;
  if (!id) return null;
  const lst = raw.listing || {};
  const ch = lst.characteristics || {};
  const addr = lst.address || {};
  const loc = lst.localization || {};

  const adUrl = `${BASE_URL}/${txType === 'buy' ? 'buy' : 'rent'}/${id}`;

  const price =
    raw.listingCard?.price?.value ??
    lst?.prices?.buy?.price?.value ??
    lst?.prices?.rent?.gross?.price?.value ??
    lst?.prices?.rent?.net?.price?.value ??
    null;

  const ad: Record<string, unknown> = {
    canton,
    idObject: String(id),
    ad_url: adUrl,
    publishing_status: 'online',
    source: 'homegate',
    time_online: 1,
    last_seen_at: new Date().toISOString(),
    offerType: txType === 'buy' ? 'Sell' : 'Rent',
    price: price != null ? Number(price) : null,
    object: categoryLabel(lst.categories),
    descriptionTitle: pickTitle(loc),
    floor: ch.floor != null ? String(ch.floor) : null,
    construction_year: ch.yearBuilt ?? null,
    renovation_year: ch.yearRenovated ?? null,
    size_m2_usable_areas: ch.livingSpace ?? null,
    surface_parcelle_m2: ch.lotSize ?? null,
    publication_date: null,
    availability_date: null,
  };

  ad.ad_title = pickTitle(loc);
  ad.description = pickDescription(loc);

  if (addr.locality || addr.postalCode || addr.street) {
    ad.address = {
      street: addr.street || null,
      city: addr.locality || null,
      no_postal: addr.postalCode || null,
    };
  } else {
    ad.address = null;
  }

  if (addr.geoCoordinates) {
    ad.geo_coordinates = {
      lat: addr.geoCoordinates.latitude ?? null,
      lon: addr.geoCoordinates.longitude ?? null,
      accuracy: addr.geoCoordinates.accuracy ?? null,
    };
  }

  ad.features = extractFeatures(ch);
  ad.images = pickImages(loc);

  // Properties string
  const parts: string[] = [];
  if (ad.object) parts.push(String(ad.object));
  if (ch.numberOfRooms) parts.push(`${ch.numberOfRooms} rooms`);
  if (ch.livingSpace) parts.push(`${ch.livingSpace} m²`);
  if (ch.numberOfFloors) parts.push(`${ch.numberOfFloors} floors`);
  ad.properties = parts.filter(Boolean).join(' • ') || null;

  // Agent
  const agent = raw.agencyAgent;
  if (agent) {
    ad.contact_users = JSON.stringify([
      {
        name: [agent.firstName, agent.lastName].filter(Boolean).join(' ') || null,
        email: agent.email || null,
        phone: agent.phoneNumber || agent.mobileNumber || null,
        address: null,
        no_postal: null,
        locality: null,
      },
    ]);
  } else {
    ad.contact_users = null;
  }

  // Null-out empty values (matches properstar pattern)
  return Object.fromEntries(
    Object.entries(ad).map(([k, v]) => [k, v != null && v !== '' ? v : null]),
  );
}

// ---------------------------------------------------------------------------
// Per-canton pipeline
// ---------------------------------------------------------------------------

async function fetchCanton(
  canton: string,
  txType: TxType,
  seen: Set<string>,
  pendingRecords: Record<string, unknown>[],
): Promise<{ fetched: number }> {
  // Page 1 to discover pageCount
  let firstPage: PageResult | null;
  try {
    firstPage = await fetchPage(canton, txType, 1);
  } catch (err: any) {
    console.error(`    Page 1 failed for [${canton}/${txType}]: ${err.message} — skipping`);
    return { fetched: 0 };
  }
  if (!firstPage) {
    console.log(`    No results for [${canton}/${txType}]`);
    return { fetched: 0 };
  }

  console.log(`    ${firstPage.resultCount} results across ${firstPage.pageCount} pages`);
  let fetched = 0;
  for (const raw of firstPage.listings) {
    const rec = formatListing(canton, txType, raw);
    if (!rec) continue;
    const adUrl = rec.ad_url as string;
    if (seen.has(adUrl)) continue;
    seen.add(adUrl);
    pendingRecords.push(rec);
    fetched++;
  }

  for (let p = 2; p <= firstPage.pageCount; p++) {
    await sleep(RATE_LIMIT_MS);
    let pg: PageResult | null;
    try {
      pg = await fetchPage(canton, txType, p);
    } catch (err: any) {
      console.error(`    Page ${p} failed for [${canton}/${txType}]: ${err.message} — skipping page`);
      continue;
    }
    if (!pg) break;
    for (const raw of pg.listings) {
      const rec = formatListing(canton, txType, raw);
      if (!rec) continue;
      const adUrl = rec.ad_url as string;
      if (seen.has(adUrl)) continue;
      seen.add(adUrl);
      pendingRecords.push(rec);
      fetched++;
    }
    if (p % 10 === 0) console.log(`    Fetched page ${p}/${firstPage.pageCount} (${fetched} listings so far)`);
  }
  return { fetched };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  console.log('='.repeat(60));
  console.log('  Homegate — Listing Pipeline');
  console.log('  Source: homegate.ch (SSR __INITIAL_STATE__)');
  console.log('  Target: bronze_ch.homegate');
  console.log('='.repeat(60));

  const startTime = Date.now();

  await verifyAccess('bronze_ch', 'homegate');

  let pendingRecords: Record<string, unknown>[] = [];
  const seen = new Set<string>();
  let totalFetched = 0;
  let totalUpserted = 0;

  for (const canton of CANTONS) {
    for (const txType of TRANSACTION_TYPES) {
      console.log(`\n  [${canton.toUpperCase()}] ${txType}:`);
      try {
        const { fetched } = await fetchCanton(canton, txType, seen, pendingRecords);
        totalFetched += fetched;
      } catch (err: any) {
        console.error(`  Canton failed [${canton}/${txType}]: ${err.message}`);
      }

      if (pendingRecords.length >= UPSERT_EVERY) {
        console.log(`    Upserting ${pendingRecords.length} records...`);
        const n = await upsert('bronze_ch', 'homegate', pendingRecords, 'ad_url', BATCH_SIZE);
        totalUpserted += n;
        pendingRecords = [];
      }

      await sleep(RATE_LIMIT_MS);
    }
  }

  if (pendingRecords.length > 0) {
    console.log(`\n  Upserting final ${pendingRecords.length} records...`);
    const n = await upsert('bronze_ch', 'homegate', pendingRecords, 'ad_url', BATCH_SIZE);
    totalUpserted += n;
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\n${'='.repeat(60)}`);
  console.log('  IMPORT COMPLETE');
  console.log(`  Listings fetched:  ${totalFetched}`);
  console.log(`  Records upserted:  ${totalUpserted}`);
  console.log(`  Duration:          ${elapsed}s`);
  console.log('='.repeat(60));

  if (totalUpserted === 0 && totalFetched > 0) {
    console.error('  FAILED: Zero rows upserted despite having records!');
    process.exit(1);
  }

  if (totalFetched > 0) {
    await markStaleListings('bronze_ch', 'homegate', 'ad_url', 100, totalFetched);
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
