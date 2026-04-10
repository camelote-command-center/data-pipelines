/**
 * Flatfox — Listing Fetcher
 *
 * Fetches all active property listings from flatfox.ch public API
 * and upserts them into bronze_ch.flatfox on re-LLM.
 *
 * Workflow:
 *   1. Paginate through GET /api/v1/public-listing/?expand=images,documents
 *   2. Map each listing to the bronze schema
 *   3. Upsert on url (unique key)
 *
 * Usage:
 *   npx tsx fetch-listings.ts
 *
 * Environment variables:
 *   RE_LLM_SUPABASE_URL              - re-LLM Supabase URL (required)
 *   RE_LLM_SUPABASE_SERVICE_ROLE_KEY - re-LLM service_role key (required)
 *   FLATFOX_LIMIT                    - max listings to fetch (optional, for testing)
 */

import { verifyAccess, upsert, sleep, markStaleListings } from '../_shared/re-llm.js';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const API_BASE = 'https://flatfox.ch/api/v1/public-listing/';
const PAGE_SIZE = 100;
const UPSERT_EVERY = 200;
const BATCH_SIZE = 100;
const RATE_LIMIT_MS = 300;
const SCHEMA = 'bronze_ch';
const TABLE = 'flatfox';

const HEADERS: Record<string, string> = {
  Accept: 'application/json',
  'User-Agent':
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
};

// ---------------------------------------------------------------------------
// API fetch with retry
// ---------------------------------------------------------------------------

async function fetchPage(url: string, retry = 0): Promise<any> {
  try {
    const res = await fetch(url, { headers: HEADERS });

    if (res.status === 429) {
      const wait = Math.min(60_000, 5_000 * Math.pow(2, retry));
      console.log(`  Rate limited, waiting ${(wait / 1000).toFixed(0)}s...`);
      await sleep(wait);
      return fetchPage(url, retry + 1);
    }

    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } catch (err) {
    if (retry >= 5) throw err;
    console.error(`  Fetch error (attempt ${retry + 1}): ${err}`);
    await sleep(5_000);
    return fetchPage(url, retry + 1);
  }
}

// ---------------------------------------------------------------------------
// Map Flatfox listing → bronze_ch.flatfox row
// ---------------------------------------------------------------------------

function mapListing(raw: any): Record<string, unknown> {
  return {
    id: raw.pk,
    slug: raw.slug || null,
    url: raw.url || `/en/flat/${raw.slug}/${raw.pk}/`,
    status: raw.status || null,
    offer_type: raw.offer_type || null,
    object_category: raw.object_category || null,
    object_type: raw.object_type || null,
    reference: raw.reference || null,
    price_display: raw.price_display ?? null,
    price_unit: raw.price_unit || null,
    rent_net: raw.rent_net ?? null,
    rent_charges: raw.rent_charges ?? null,
    rent_gross: raw.rent_gross ?? null,
    title: raw.public_title || raw.short_title || raw.description_title || null,
    description: raw.description || null,
    surface_living: raw.surface_living ?? null,
    surface_property: raw.surface_property ?? null,
    surface_usable: raw.surface_usable ?? null,
    number_of_rooms: raw.number_of_rooms || null,
    floor: raw.floor ?? null,
    attributes: raw.attributes?.length ? raw.attributes : null,
    is_furnished: raw.is_furnished ?? null,
    is_temporary: raw.is_temporary ?? null,
    street: raw.street || null,
    zipcode: raw.zipcode ? String(raw.zipcode) : null,
    city: raw.city || null,
    public_address: raw.public_address || null,
    latitude: raw.latitude ?? null,
    longitude: raw.longitude ?? null,
    year_built: raw.year_built ?? null,
    year_renovated: raw.year_renovated ?? null,
    moving_date: raw.moving_date || null,
    moving_date_type: raw.moving_date_type || null,
    images: raw.images?.length
      ? raw.images.map((img: any) => ({
          pk: img.pk,
          url: img.url,
          caption: img.caption || null,
          width: img.width,
          height: img.height,
        }))
      : null,
    documents: raw.documents?.length
      ? raw.documents.map((doc: any) => ({
          pk: doc.pk,
          url: doc.url,
          title: doc.title || null,
        }))
      : null,
    agency: raw.agency
      ? {
          name: raw.agency.name || null,
          name_2: raw.agency.name_2 || null,
          street: raw.agency.street || null,
          zipcode: raw.agency.zipcode || null,
          city: raw.agency.city || null,
        }
      : null,
    published_at: raw.published || null,
    updated_at: new Date().toISOString(),
    publishing_status: 'online',
    last_seen_at: new Date().toISOString(),
  };
}

// ---------------------------------------------------------------------------
// Main pipeline
// ---------------------------------------------------------------------------

async function main() {
  console.log('='.repeat(60));
  console.log('  Flatfox — Listing Pipeline');
  console.log('  Source: flatfox.ch (public API)');
  console.log(`  Target: ${SCHEMA}.${TABLE} (re-LLM)`);
  console.log('='.repeat(60));

  const startTime = Date.now();
  const maxListings = process.env.FLATFOX_LIMIT ? parseInt(process.env.FLATFOX_LIMIT) : Infinity;

  // 0. Verify DB connectivity
  await verifyAccess(SCHEMA, TABLE);

  // 1. Paginate through all listings
  let pendingRecords: Record<string, unknown>[] = [];
  let totalFetched = 0;
  let totalUpserted = 0;
  let offset = 0;
  let hasMore = true;

  while (hasMore) {
    const url = `${API_BASE}?limit=${PAGE_SIZE}&offset=${offset}&expand=images,documents`;
    const data = await fetchPage(url);

    const results = data?.results || [];
    if (results.length === 0) {
      hasMore = false;
      break;
    }

    for (const raw of results) {
      if (!raw.pk || !raw.url) continue;

      pendingRecords.push(mapListing(raw));
      totalFetched++;

      if (totalFetched >= maxListings) {
        hasMore = false;
        break;
      }
    }

    offset += results.length;

    // Progress
    const total = data.count || '?';
    const pct = data.count ? ((totalFetched / data.count) * 100).toFixed(1) : '?';
    console.log(`  Fetched ${totalFetched}/${total} (${pct}%)`);

    // Incremental upsert
    if (pendingRecords.length >= UPSERT_EVERY) {
      console.log(`  Upserting ${pendingRecords.length} records...`);
      const n = await upsert(SCHEMA, TABLE, pendingRecords, 'url', BATCH_SIZE);
      totalUpserted += n;
      pendingRecords = [];
    }

    if (!data.next) {
      hasMore = false;
    }

    await sleep(RATE_LIMIT_MS);
  }

  // Flush remaining
  if (pendingRecords.length > 0) {
    console.log(`  Upserting final ${pendingRecords.length} records...`);
    const n = await upsert(SCHEMA, TABLE, pendingRecords, 'url', BATCH_SIZE);
    totalUpserted += n;
  }

  // Summary
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
  await markStaleListings('bronze_ch', 'flatfox', 'url', 500, totalFetched);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
