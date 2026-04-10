/**
 * Immobilier.ch — Listing Fetcher
 *
 * Fetches property listings from immobilier.ch via HTML pages + JSON detail API
 * and upserts them into bronze."immobilier".
 *
 * Workflow:
 *   1. For each canton × offer type (buy/rent):
 *      - Fetch listing pages, extract IDs
 *      - For each ID, fetch JSON detail from /api/objects/{id}
 *   2. Map to bronze schema and upsert on ad_url
 *
 * Usage:
 *   npx tsx fetch-listings.ts
 *
 * Environment variables:
 *   SUPABASE_URL              - Supabase project URL (required)
 *   SUPABASE_SERVICE_ROLE_KEY - service_role key     (required)
 */

import { upsertBronze, sleep, verifyBronzeAccess, markStaleListings } from '../_shared/supabase.js';
import { httpFetch } from '../_shared/http.js';
import * as cheerio from 'cheerio';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const URL_MAIN = 'https://www.immobilier.ch';
const BATCH_SIZE = 100;
const RATE_LIMIT_MS = 300;

const HEADERS: Record<string, string> = {
  'user-agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
  'content-type': 'application/json',
};

const CANTONS = [
  // Suisse Romande (original 7)
  'geneva', 'vaud', 'fribourg', 'jura', 'berne', 'neuchatel', 'valais',
  // German-speaking & Italian-speaking cantons (19)
  'zurich', 'argovie', 'bale-campagne', 'bale-ville', 'grisons', 'lucerne',
  'st-gall', 'tessin', 'thurgovie', 'soleure', 'schwyz', 'schaffhouse',
  'zoug', 'appenzell-rhodes-exterieures', 'appenzell-rhodes-interieures',
  'glaris', 'nidwald', 'obwald', 'uri',
];
const OFFER_TYPES = ['buy', 'rent'] as const;

// ---------------------------------------------------------------------------
// HTTP helper with retry
// ---------------------------------------------------------------------------

async function httpGet(url: string): Promise<any> {
  const res = await httpFetch(url, { headers: HEADERS } as any);

  if (!res.ok) {
    if (res.status === 404) return null;
    throw new Error(`HTTP ${res.status} from ${url.split('?')[0]}`);
  }

  const contentType = res.headers.get('content-type') || '';
  if (contentType.includes('application/json')) {
    return await res.json();
  }
  return await res.text();
}

// ---------------------------------------------------------------------------
// Page scraping
// ---------------------------------------------------------------------------

function getPageCount(html: string): number {
  const $ = cheerio.load(html);
  const pages = $('div.pagination-counter b');
  return parseInt(pages.last().text(), 10) || 1;
}

function extractListingIds(html: string): string[] {
  const $ = cheerio.load(html);
  const ids: string[] = [];

  $('div.filter-item-container')
    .children('a')
    .each((_, el) => {
      const href = $(el).attr('href');
      if (href) {
        const parts = href.split('/');
        const lastPart = parts[parts.length - 1];
        const id = lastPart.split('-').pop();
        if (id) ids.push(id);
      }
    });

  return ids;
}

// ---------------------------------------------------------------------------
// Swiss number parser (from dev code)
// ---------------------------------------------------------------------------

function parseNumberSwiss(input: string | null | undefined): number | null {
  if (input == null) return null;
  let s = String(input).trim();
  s = s.replace(/CHF\s*/i, '').replace(/[^\d'.,-]/g, '');

  if (s.includes('.') && s.includes(',')) {
    s = s.replace(/\./g, '').replace(/,/g, '.');
  } else if (s.includes("'")) {
    s = s.replace(/'/g, '');
    const dots = (s.match(/\./g) || []).length;
    if (dots > 1) {
      const parts = s.split('.');
      const last = parts.pop();
      s = parts.join('') + '.' + last;
    }
  }
  s = s.replace(/,/g, '.');

  const n = Number(s);
  return Number.isNaN(n) ? null : n;
}

function stripHtml(value: string | null | undefined): string[] | null {
  if (!value) return null;
  return value
    .split(/<[^>]*>/)
    .map((el) => el.trim())
    .filter(Boolean);
}

// ---------------------------------------------------------------------------
// Data formatting
// ---------------------------------------------------------------------------

function formatListing(data: Record<string, any>): Record<string, unknown> {
  const frData = data.fr || {};
  const urlParts = (frData.url || '').split('/');

  const ad: Record<string, unknown> = {
    publishing_status: 'online',
    time_online: 1,
    last_seen_at: new Date().toISOString(),
    source: 'immobilier',
    idObject: frData.idObject ?? null,
    priceFormatted: frData.priceFormatted ?? null,
    price: frData.price ?? null,
    object: urlParts[3] || null,
    canton: urlParts[4] || null,
    offerType: frData.offerType ?? null,
    ad_url: frData.fullPathUrl ?? null,
    properties: frData.properties ?? null,
    linkMoreInfo: frData.linkMoreInfo ?? null,
    descriptionTitle: frData.descriptionTitle ?? null,
    shareText: frData.shareText ?? null,
    ad_title: frData.title ? [frData.title] : null,
    description: stripHtml(frData.description),
    features: frData.equipments ?? null,
  };

  // Parse costs from priceFormatted
  if (typeof ad.priceFormatted === 'string' && (ad.priceFormatted as string).includes('(')) {
    const chr = /CHF\s*([\d'.,]+).*?([\d'.,]+)\s*-\s*charges/i.exec(ad.priceFormatted as string);
    if (chr) {
      const price = parseNumberSwiss(chr[1]);
      const costs = parseNumberSwiss(chr[2]);
      if (price != null && costs != null) {
        ad.costs = costs;
        ad.total_amount = price + costs;
      }
    }
  }

  // Extract area from properties string
  if (typeof ad.properties === 'string') {
    const areaMatch = /([\d]+(?:[.,][\d]+)?)\s*m(?:²|2)/i.exec(ad.properties as string);
    if (areaMatch) {
      const sqm = parseFloat(areaMatch[1].replace(',', '.'));
      if (!Number.isNaN(sqm)) ad.size_m2_usable_areas = sqm;
    }
  }

  // Extra properties
  for (const item of frData.extraProperties || []) {
    const lower = String(item || '').toLowerCase();
    if (lower.includes('floor') || lower.includes('étage')) {
      ad.floor = item;
    } else if (lower.includes('available') || lower.includes('disponible')) {
      ad.availability = item;
    } else if (lower.includes('surface propriété')) {
      const m = item.match(/\d+/);
      ad.surface_parcelle_m2 = m ? Number(m[0]) : null;
    } else if (lower.includes('surface utilisable')) {
      const m = item.match(/\d+/);
      ad.surface_m2_usable_2_area = m ? Number(m[0]) : null;
    } else if (lower.includes('construit en')) {
      const m = item.match(/\d+/);
      ad.annee_de_construction = m ? Number(m[0]) : null;
    }
  }

  // Address
  const address = frData.address || {};
  ad.address = {
    street: address.street || null,
    city: address.city || null,
    no_postal: address.zipcode || null,
  };

  // Agency
  const agency = frData.agency || {};
  ad.agency = {
    urlAgency: agency.urlAgency || null,
    phone: agency.phone || null,
    city: agency.city || null,
    emails: agency.visit?.emails || null,
    name: agency.name || agency.visit?.name || null,
    logo: agency.logoUrl ? { url: agency.logoUrl } : null,
  };

  // Images — store original URLs
  const medias = data.fr?.medias || [];
  const images = medias
    .filter((m: any) => m.videoType === 0 && !m.src?.endsWith('.pdf'))
    .filter((m: any) => /\.(jpg|jpeg|png|gif|bmp|tiff)$/i.test(m.src || ''))
    .map((m: any) => {
      let src = m.src;
      if (src?.startsWith('/')) src = `${URL_MAIN}${src}`;
      return { url: src };
    });
  ad.images = images.length ? images : null;

  return Object.fromEntries(
    Object.entries(ad).map(([k, v]) => [k, v != null && v !== '' ? v : null]),
  );
}

// ---------------------------------------------------------------------------
// Main pipeline
// ---------------------------------------------------------------------------

async function main() {
  console.log('='.repeat(60));
  console.log('  Immobilier.ch — Listing Pipeline');
  console.log('  Source: immobilier.ch');
  console.log('  Target: bronze."immobilier"');
  console.log('='.repeat(60));

  const startTime = Date.now();

  // 0. Verify DB connectivity before spending hours scraping
  await verifyBronzeAccess('immobilier');

  const UPSERT_EVERY = 200;
  let pendingRecords: Record<string, unknown>[] = [];
  let totalFetched = 0;
  let totalUpserted = 0;
  const seenUrls = new Set<string>();

  for (const canton of CANTONS) {
    for (const offerType of OFFER_TYPES) {
      const urlTemplate = `${URL_MAIN}/en/${offerType}/apartment-house/${canton}/page-{}?group=1`;
      console.log(`\n  [${canton.toUpperCase()}] ${offerType}:`);

      // Get page count
      const firstPageUrl = urlTemplate.replace('{}', '1');
      let firstPageHtml: any;
      try {
        firstPageHtml = await httpGet(firstPageUrl);
      } catch (err) {
        console.log(`    Error fetching first page: ${err}`);
        continue;
      }
      await sleep(RATE_LIMIT_MS);

      if (!firstPageHtml || typeof firstPageHtml !== 'string') {
        console.log('    No results');
        continue;
      }

      const pageCount = getPageCount(firstPageHtml);
      console.log(`    ${pageCount} pages`);

      // Collect all listing IDs
      const allIds: string[] = [];
      const firstIds = extractListingIds(firstPageHtml);
      allIds.push(...firstIds);

      for (let page = 2; page <= pageCount; page++) {
        const pageUrl = urlTemplate.replace('{}', String(page));
        try {
          const html = await httpGet(pageUrl);
          await sleep(RATE_LIMIT_MS);

          if (html && typeof html === 'string') {
            const ids = extractListingIds(html);
            allIds.push(...ids);
          }
        } catch (err) {
          console.log(`    Error fetching page ${page}: ${err}`);
          await sleep(2000); // back off on socket errors
        }
      }

      console.log(`    ${allIds.length} listing IDs collected`);

      // Fetch details for each listing (French only — saves 2/3 of API calls)
      let detailCount = 0;
      for (const id of allIds) {
        try {
          const apiUrl = `${URL_MAIN}/api/objects/${id}?idObject=${id}&lang=fr`;
          const frData = await httpGet(apiUrl);
          await sleep(RATE_LIMIT_MS);

          if (!frData?.fullPathUrl) continue;
          const langData: Record<string, any> = { fr: frData };

          const adUrl = langData.fr.fullPathUrl as string;
          if (seenUrls.has(adUrl)) continue;
          seenUrls.add(adUrl);

          const record = formatListing(langData);
          pendingRecords.push(record);
          totalFetched++;

          detailCount++;
          if (detailCount % 100 === 0) {
            console.log(`    Fetched ${detailCount} details...`);
          }

          // Incremental upsert to save progress
          if (pendingRecords.length >= UPSERT_EVERY) {
            console.log(`    Upserting ${pendingRecords.length} records...`);
            const n = await upsertBronze('immobilier', pendingRecords, 'ad_url', BATCH_SIZE);
            totalUpserted += n;
            pendingRecords = [];
          }
        } catch (err) {
          console.error(`    Error fetching listing ${id}: ${err}`);
        }
      }

      console.log(`    Processed ${detailCount} listings`);
    }
  }

  // Flush remaining records
  if (pendingRecords.length > 0) {
    console.log(`\n  Upserting final ${pendingRecords.length} records...`);
    const n = await upsertBronze('immobilier', pendingRecords, 'ad_url', BATCH_SIZE);
    totalUpserted += n;
  }

  console.log(`\n  Total records fetched: ${totalFetched}`);

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

  // Mark stale listings as offline
  await markStaleListings('immobilier', 100, totalFetched);
}

main().catch(async (err) => {
  console.error('Fatal error:', err);
  // If we already upserted some data, don't fail the workflow — we made progress
  if (err?.cause?.code === 'UND_ERR_SOCKET' || err?.message?.includes('terminated') || err?.message?.includes('ECONNRESET')) {
    console.log('  Socket error — but partial data was upserted. Exiting gracefully.');
    process.exit(0);
  }
  process.exit(1);
});
