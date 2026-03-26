/**
 * RealAdvisor — Listing Fetcher
 *
 * Fetches property listings from RealAdvisor's public GraphQL API.
 * RealAdvisor aggregates from: Homegate, ImmoScout24, Newhome, Flatfox,
 * and direct RealAdvisor listings — deduplicated into ~112K unique listings.
 *
 * For each listing, we detect the original source portal from the originalId
 * pattern and store it in original_portal.
 *
 * Usage:
 *   npx tsx fetch-listings.ts
 *
 * Environment variables:
 *   RE_LLM_SUPABASE_URL              - re-LLM Supabase URL (required)
 *   RE_LLM_SUPABASE_SERVICE_ROLE_KEY - service_role key     (required)
 *   REALADVISOR_LIMIT                - max listings total (optional, for testing)
 */

import { verifyAccess, upsert, sleep } from '../_shared/re-llm.js';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const GRAPHQL_URL = 'https://realadvisor.ch/graphql';
const PAGE_SIZE = 500;
const UPSERT_EVERY = 500;
const BATCH_SIZE = 100;
const RATE_LIMIT_MS = 200;
const SCHEMA = 'bronze_ch';
const TABLE = 'realadvisor_listings';

const OFFER_TYPES = ['buy', 'rent'] as const;

const LISTING_FIELDS = `
  id
  title
  description
  slug
  originalId
  createdAt
  offerType
  propertyType
  salePrice
  currency
  address
  postcode
  countryCode
  streetNumber
  lat
  lng
  livingSurface
  landSurface
  usableSurface
  numberOfRooms
  numberOfBedrooms
  numberOfBathrooms
  constructionYear
  renovationYear
  hasBalcony
  hasGarage
  hasParking
  hasGarden
  hasElevator
  isFurnished
  availableFrom
  agencyName
  images { url }
`;

const QUERY = `
  query FetchListings($first: Int!, $after: String, $filters: AggregatesListingFiltersInput!) {
    aggregatesListings(first: $first, after: $after, filters: $filters) {
      totalCount
      pageInfo { hasNextPage endCursor }
      edges { node { ${LISTING_FIELDS} } }
    }
  }
`;

// ---------------------------------------------------------------------------
// Detect original portal from originalId pattern
// ---------------------------------------------------------------------------

function detectPortal(originalId: string | null): string {
  if (!originalId) return 'unknown';

  // Known patterns from observation:
  // Homegate/ImmoScout24: numeric IDs like "1706259.xxxxx" or "ZÜ_445.xxxxx"
  // Newhome: "XXXX-XXXX-NNN" format
  // Flatfox: short alphanumeric
  // Migrated: "mig-NNNN-NNNNNNN"

  if (originalId.startsWith('mig-')) return 'migrated';
  if (/^[A-Z0-9]{4}-[A-Z0-9]{4}-\d{3}$/.test(originalId)) return 'newhome';

  // Default: the API doesn't expose the source portal on the listing itself,
  // so we store what we can detect and fall back to 'realadvisor'
  return 'realadvisor';
}

// ---------------------------------------------------------------------------
// GraphQL fetch with retry
// ---------------------------------------------------------------------------

async function graphqlFetch(
  variables: Record<string, unknown>,
  retry = 0,
): Promise<any> {
  try {
    const res = await fetch(GRAPHQL_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
      },
      body: JSON.stringify({ query: QUERY, variables }),
    });

    if (res.status === 429) {
      const wait = Math.min(60_000, 5_000 * Math.pow(2, retry));
      console.log(`  Rate limited, waiting ${(wait / 1000).toFixed(0)}s...`);
      await sleep(wait);
      return graphqlFetch(variables, retry + 1);
    }

    if (!res.ok) throw new Error(`HTTP ${res.status}`);

    const json = await res.json();
    if (json.errors) {
      throw new Error(`GraphQL error: ${json.errors[0]?.message}`);
    }

    return json.data.aggregatesListings;
  } catch (err: any) {
    if (retry >= 5) throw err;
    const wait = Math.min(60_000, 3_000 * Math.pow(2, retry));
    console.error(`  Fetch error: ${err.message}, retry ${retry + 1}/5 in ${(wait / 1000).toFixed(0)}s`);
    await sleep(wait);
    return graphqlFetch(variables, retry + 1);
  }
}

// ---------------------------------------------------------------------------
// Map GraphQL node → DB row
// ---------------------------------------------------------------------------

function mapListing(node: any): Record<string, unknown> {
  return {
    id: node.id,
    original_id: node.originalId || null,
    original_portal: detectPortal(node.originalId),
    offer_type: node.offerType || null,
    property_type: node.propertyType || null,
    title: node.title || null,
    description: node.description || null,
    sale_price: node.salePrice ?? null,
    currency: node.currency || 'CHF',
    address: node.address || null,
    postcode: node.postcode || null,
    street_number: node.streetNumber || null,
    country_code: node.countryCode || 'CH',
    latitude: node.lat ?? null,
    longitude: node.lng ?? null,
    living_surface: node.livingSurface ?? null,
    land_surface: node.landSurface ?? null,
    usable_surface: node.usableSurface ?? null,
    number_of_rooms: node.numberOfRooms ?? null,
    number_of_bedrooms: node.numberOfBedrooms ?? null,
    number_of_bathrooms: node.numberOfBathrooms ?? null,
    construction_year: node.constructionYear ?? null,
    renovation_year: node.renovationYear ?? null,
    has_balcony: node.hasBalcony ?? null,
    has_garage: node.hasGarage ?? null,
    has_parking: node.hasParking ?? null,
    has_garden: node.hasGarden ?? null,
    has_elevator: node.hasElevator ?? null,
    is_furnished: node.isFurnished ?? null,
    available_from: node.availableFrom || null,
    agency_name: node.agencyName || null,
    images: node.images?.length ? node.images : null,
    published_at: node.createdAt || null,
    updated_at: new Date().toISOString(),
  };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  console.log('='.repeat(60));
  console.log('  RealAdvisor — Listing Pipeline');
  console.log('  Source: realadvisor.ch GraphQL API');
  console.log(`  Target: ${SCHEMA}.${TABLE} (re-LLM)`);
  console.log('  Covers: Homegate, ImmoScout24, Newhome, Flatfox, RealAdvisor');
  console.log('='.repeat(60));

  const startTime = Date.now();
  const maxListings = process.env.REALADVISOR_LIMIT
    ? parseInt(process.env.REALADVISOR_LIMIT)
    : Infinity;

  // 0. Verify DB
  await verifyAccess(SCHEMA, TABLE);

  let totalFetched = 0;
  let totalUpserted = 0;

  for (const offerType of OFFER_TYPES) {
    console.log(`\n  [${offerType.toUpperCase()}]:`);

    let pendingRecords: Record<string, unknown>[] = [];
    let after: string | null = null;
    let hasMore = true;
    let offerFetched = 0;

    while (hasMore) {
      const variables: Record<string, unknown> = {
        first: PAGE_SIZE,
        after,
        filters: { offerType_eq: offerType },
      };

      const data = await graphqlFetch(variables);
      const edges = data?.edges || [];
      if (edges.length === 0) break;

      for (const edge of edges) {
        pendingRecords.push(mapListing(edge.node));
        offerFetched++;
        totalFetched++;

        if (totalFetched >= maxListings) {
          hasMore = false;
          break;
        }
      }

      const total = Math.min(data.totalCount || 0, maxListings);
      const pct = total ? ((offerFetched / data.totalCount) * 100).toFixed(1) : '?';
      console.log(`    ${offerFetched}/${data.totalCount} (${pct}%)`);

      // Incremental upsert
      if (pendingRecords.length >= UPSERT_EVERY) {
        console.log(`    Upserting ${pendingRecords.length} records...`);
        const n = await upsert(SCHEMA, TABLE, pendingRecords, 'id', BATCH_SIZE);
        totalUpserted += n;
        pendingRecords = [];
      }

      hasMore = hasMore && data.pageInfo?.hasNextPage;
      after = data.pageInfo?.endCursor || null;

      await sleep(RATE_LIMIT_MS);
    }

    // Flush remaining
    if (pendingRecords.length > 0) {
      console.log(`    Upserting final ${pendingRecords.length} records...`);
      const n = await upsert(SCHEMA, TABLE, pendingRecords, 'id', BATCH_SIZE);
      totalUpserted += n;
    }

    console.log(`    ${offerType}: ${offerFetched} listings`);
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
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
