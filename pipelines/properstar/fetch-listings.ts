/**
 * Properstar — Listing Fetcher
 *
 * Fetches property listings from properstar.ch via ListGlobally API
 * and upserts them into bronze."properstar".
 *
 * Workflow:
 *   1. Get Bearer token from properstar.ch Set-Cookie header
 *   2. For each canton × transaction type (Rent/Sell):
 *      - Search listings via ListGlobally search API
 *      - If total > retrievable limit, use facet drilling by sub-region
 *      - Fetch detail for each listing
 *   3. Map to bronze schema and upsert on ad_url
 *
 * Usage:
 *   npx tsx fetch-listings.ts
 *
 * Environment variables:
 *   SUPABASE_URL              - Supabase project URL (required)
 *   SUPABASE_SERVICE_ROLE_KEY - service_role key     (required)
 */

import { upsertBronze, sleep } from '../_shared/supabase.js';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const BASE_URL = 'https://www.properstar.ch';
const URL_PROPERSTAR = 'https://www.properstar.ch/listing/';
const SEARCH_API = 'https://search-api.listglobally.com/api/v2/searches?count=2000';
const MAP_API = 'https://search-api.listglobally.com/api/v2/searches/map?facet=admAreaLevel2,count:0';
const DETAILS_API = 'https://listings-api.listglobally.com/api/v2/listings';
const BATCH_SIZE = 100;
const RATE_LIMIT_MS = 500;

const HEADERS_BASE: Record<string, string> = {
  accept:
    'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
  'accept-language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
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

const HEADERS_API: Record<string, string> = {
  accept: 'application/json, text/plain, */*',
  'accept-language': 'fr',
  'content-type': 'application/json',
  origin: 'https://www.properstar.ch',
  referer: 'https://www.properstar.ch/',
  'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"macOS"',
  'sec-fetch-dest': 'empty',
  'sec-fetch-mode': 'cors',
  'sec-fetch-site': 'cross-site',
  'user-agent':
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
};

// Canton placeIds from dev's code
const CANTONS = [
  { canton: 'geneve', placeId: 'ChIJ6-LQkwZljEcRObwLezWVtqA' },
  { canton: 'vaud', placeId: 'ChIJtZALbaItjEcRUNgYQIj_AAE' },
  { canton: 'fribourg', placeId: 'ChIJL31ajc1ujkcRLsM3ufbR5bs' },
  { canton: 'jura', placeId: 'ChIJtV6GwMXkkUcRkNgYQIj_AAE' },
  { canton: 'berne', placeId: 'ChIJdxs61MA5jkcRmmVXBP5fVcs' },
  { canton: 'neuchatel', placeId: 'ChIJm81S1RkKjkcRBGS2VWjDXCM' },
  { canton: 'valais', placeId: 'ChIJF5p_6psij0cRYNgYQIj_AAE' },
];

const TRANSACTION_TYPES = ['Rent', 'Sell'] as const;

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

let authorization = '';

async function getAuthorization(maxAttempts = 5): Promise<void> {
  let attempts = 0;

  while (attempts < maxAttempts) {
    try {
      const res = await fetch(BASE_URL, { headers: HEADERS_BASE, redirect: 'manual' });
      const setCookie = res.headers.getSetCookie?.() || [];

      const tokenCookie = setCookie.find((c) => c.startsWith('token'));
      if (tokenCookie) {
        const tokenValue = tokenCookie.split(';')[0].replace('token=', '');
        authorization = `Bearer ${tokenValue}`;
        console.log('  Authorization token obtained');
        return;
      }

      // Fallback: check raw set-cookie header
      const rawSetCookie = res.headers.get('set-cookie') || '';
      const match = rawSetCookie.match(/token=([^;]+)/);
      if (match) {
        authorization = `Bearer ${match[1]}`;
        console.log('  Authorization token obtained (fallback)');
        return;
      }

      attempts++;
      console.log(`  No token in Set-Cookie (attempt ${attempts}/${maxAttempts})`);
      await sleep(2_000);
    } catch (err) {
      attempts++;
      console.error(`  Auth error (attempt ${attempts}): ${err}`);
      await sleep(2_000);
    }
  }

  throw new Error(`Failed to get authorization after ${maxAttempts} attempts`);
}

// ---------------------------------------------------------------------------
// API helpers with retry + re-auth
// ---------------------------------------------------------------------------

async function apiGet(url: string, retry = 0): Promise<any> {
  try {
    const res = await fetch(url, {
      headers: { ...HEADERS_API, authorization },
    });

    if (res.status === 404) return null;
    if (!res.ok) throw new Error(`HTTP ${res.status}`);

    return await res.json();
  } catch (err) {
    if (retry >= 10) throw err;

    await sleep(5_000);
    await getAuthorization();
    return apiGet(url, retry + 1);
  }
}

async function apiPost(url: string, body: string, retry = 0): Promise<any> {
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: { ...HEADERS_API, authorization },
      body,
    });

    if (!res.ok) throw new Error(`HTTP ${res.status}`);

    return await res.json();
  } catch (err) {
    if (retry >= 10) throw err;

    await sleep(5_000);
    await getAuthorization();
    return apiPost(url, body, retry + 1);
  }
}

// ---------------------------------------------------------------------------
// Search body builder
// ---------------------------------------------------------------------------

function buildSearchBody(placeId: string, transactionType: string): string {
  return JSON.stringify({
    search: {
      filters: {
        listings: {
          viewLandscapes: null,
          preferredViewLandscapes: null,
          viewTypes: null,
          preferredViewTypes: null,
          classes: ['PropertyListing', 'UnitListing'],
          luxury: false,
          price: { min: null, max: null },
          area: { living: { min: null, max: null, unit: null } },
          numberOf: {
            rooms: { min: null, max: null },
            bedrooms: { min: null, max: null },
            bathrooms: { min: null, max: null },
          },
          availableFrom: { min: null, max: null },
          excludedTypes: null,
          types: ['Apartment', 'House'],
          studios: false,
          location: {
            placeId,
            polygons: [],
            excludedPolygons: [],
            exact: true,
          },
          transactionType,
          accountId: null,
          orientations: null,
          subTypes: null,
          preferredAmenities: null,
          preferredProximities: null,
          amenities: null,
          proximities: null,
          floors: null,
        },
      },
      currencyId: 'CHF',
      portalId: null,
    },
  });
}

// ---------------------------------------------------------------------------
// Listing fetcher (with facet drilling when > 2000 results)
// ---------------------------------------------------------------------------

async function fetchListings(
  placeId: string,
  transactionType: string,
): Promise<any[]> {
  const body = buildSearchBody(placeId, transactionType);

  const data = await apiPost(SEARCH_API, body);
  if (!data) return [];

  const total = data.total || 0;
  const totalRetrievable = data.totalRetrievable || 0;

  if (total <= totalRetrievable) {
    return data.listings || [];
  }

  // Facet drilling: get sub-regions and query each
  console.log(`    Total ${total} > retrievable ${totalRetrievable}, using facet drilling`);

  const mapData = await apiPost(MAP_API, body);
  const facets = mapData?.inside?.facets?.admAreaLevel2 || [];

  const allListings: any[] = [];
  for (const facet of facets) {
    const subPlaceId = facet.location?.placeId;
    if (!subPlaceId) continue;

    const subBody = buildSearchBody(subPlaceId, transactionType);
    const subData = await apiPost(SEARCH_API, subBody);
    const subListings = subData?.listings || [];
    allListings.push(...subListings);

    await sleep(RATE_LIMIT_MS);
  }

  return allListings;
}

// ---------------------------------------------------------------------------
// Data formatting
// ---------------------------------------------------------------------------

function dateFormat(dateRaw: string | null | undefined): string | null {
  if (!dateRaw) return null;
  const d = new Date(dateRaw);
  if (isNaN(d.getTime())) return null;
  return d.toISOString().split('T')[0];
}

function stripHtml(value: string | null | undefined): string[] | null {
  if (!value) return null;
  return value
    .split(/<[^>]*>/)
    .map((el) => el.trim())
    .filter(Boolean);
}

function formatListing(canton: string, resData: any): Record<string, unknown> {
  const data = resData.listing;

  const ad: Record<string, unknown> = {
    canton,
    idObject: data.id,
    ad_url: URL_PROPERSTAR + data.id,
    publishing_status: 'online',
    source: 'properstar',
    time_online: 1,
    offerType: data.transactionType?.name || data.transactionType?.id || null,
    price: data.price?.values?.[0]?.value ?? null,
    object: data.type?.name || data.type?.id || null,
    descriptionTitle: data.automaticTitle || null,
    floor: data.floor?.name || null,
    availability_date: dateFormat(data.availabilityDate),
    publication_date: dateFormat(data.publicationDate),
    construction_year: data.constructionYear ?? null,
    renovation_year: data.renovationYear ?? null,
  };

  // Title (prefer French)
  ad.ad_title =
    data.title?.find((t: any) => t.language === 'fr')?.text ||
    data.title?.[0]?.text ||
    null;

  // Description (prefer French, strip HTML)
  ad.description = stripHtml(
    data.descriptionFull?.find((d: any) => d.language === 'fr')?.text ||
      data.descriptionFull?.[0]?.text ||
      null,
  );

  // Address
  const { city, postcode, address1 } = data.location || {};
  if (city || postcode || address1) {
    ad.address = {
      street: address1 || null,
      city: city || null,
      no_postal: postcode || null,
    };
  }

  // Features
  const features: string[] = [];
  const amenities1 = data.extractedData?.amenities
    ?.map((f: any) => f.amenity?.name || f.amenity?.id)
    ?.filter(Boolean);
  if (amenities1?.length) features.push(...amenities1);
  const amenities2 = data.amenities
    ?.map((f: any) => f.name || f.id)
    ?.filter(Boolean);
  if (amenities2?.length) features.push(...amenities2);
  ad.features = features.length ? features : null;

  // Areas
  ad.size_m2_usable_areas =
    data.area?.living ||
    data.area?.values?.find((a: any) => a.original === true)?.living ||
    null;

  ad.surface_parcelle_m2 =
    data.area?.land ||
    data.area?.values?.find((a: any) => a.original === true)?.land ||
    null;

  // Properties string
  const propertyElements = Object.keys(data.numberOf || {}).map(
    (key) => `${data.numberOf[key]} ${key}`,
  );
  ad.properties = [
    ad.object,
    propertyElements.join(', '),
    ad.size_m2_usable_areas ? `${ad.size_m2_usable_areas} m²` : '',
    typeof ad.floor === 'string' && isNaN(Number(ad.floor)) ? ad.floor : ad.floor ? `${ad.floor} floor` : '',
    data.reference ? `ref ${data.reference}` : '',
  ]
    .map((p) => String(p || '').trim())
    .filter(Boolean)
    .join(' • ');

  // Contact users (column is varchar, not jsonb — must stringify)
  const contactUsers = data.contactUsers;
  if (contactUsers?.length) {
    ad.contact_users = JSON.stringify(
      contactUsers.map((cu: any) => ({
        name: cu.firstName || null,
        email: cu.email || null,
        phone: cu.phone || null,
        address: cu.address || null,
        no_postal: cu.postCode || null,
        locality: cu.locality || null,
      })),
    );
  }

  // Images — store original URLs (not re-uploading to GCS)
  const pictures = data.resources?.pictures?.items || [];
  if (pictures.length) {
    ad.images = pictures
      .map((p: any) => ({ url: p.url }))
      .filter((p: any) => p.url && !p.url.startsWith('/'));
  }

  // Null out falsy values
  return Object.fromEntries(
    Object.entries(ad).map(([k, v]) => [k, v != null && v !== '' ? v : null]),
  );
}

// ---------------------------------------------------------------------------
// Main pipeline
// ---------------------------------------------------------------------------

async function main() {
  console.log('='.repeat(60));
  console.log('  Properstar — Listing Pipeline');
  console.log('  Source: properstar.ch (ListGlobally API)');
  console.log('  Target: bronze."properstar"');
  console.log('='.repeat(60));

  const startTime = Date.now();

  // 1. Get auth token
  await getAuthorization();

  // 2. Fetch + format all listings (with incremental upserts every 200 records)
  const UPSERT_EVERY = 200;
  let pendingRecords: Record<string, unknown>[] = [];
  let totalFetched = 0;
  let totalUpserted = 0;
  const seenIds = new Set<number>();

  for (const { canton, placeId } of CANTONS) {
    for (const txType of TRANSACTION_TYPES) {
      console.log(`\n  [${canton.toUpperCase()}] ${txType}:`);

      const listings = await fetchListings(placeId, txType);
      console.log(`    Found ${listings.length} listings`);

      let detailCount = 0;
      for (const listing of listings) {
        const idObject = listing.id;
        if (!idObject || seenIds.has(idObject)) continue;
        seenIds.add(idObject);

        const detailUrl = `${DETAILS_API}/${idObject}?mode=ItemDetails&currencyId=CHF`;
        const detail = await apiGet(detailUrl);
        if (!detail) continue;

        const record = formatListing(canton, detail);
        pendingRecords.push(record);
        totalFetched++;

        detailCount++;
        if (detailCount % 100 === 0) {
          console.log(`    Fetched ${detailCount} details...`);
        }

        // Incremental upsert to save progress
        if (pendingRecords.length >= UPSERT_EVERY) {
          console.log(`    Upserting ${pendingRecords.length} records...`);
          const n = await upsertBronze('properstar', pendingRecords, 'ad_url', BATCH_SIZE);
          totalUpserted += n;
          pendingRecords = [];
        }

        await sleep(RATE_LIMIT_MS);
      }

      console.log(`    Processed ${detailCount} listing details`);
    }
  }

  // Flush remaining records
  if (pendingRecords.length > 0) {
    console.log(`\n  Upserting final ${pendingRecords.length} records...`);
    const n = await upsertBronze('properstar', pendingRecords, 'ad_url', BATCH_SIZE);
    totalUpserted += n;
  }

  console.log(`\n  Total records fetched: ${totalFetched}`);

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
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
