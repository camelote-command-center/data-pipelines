/**
 * Shared Supabase client + batch upsert helper.
 *
 * Every pipeline imports this to avoid duplicating connection logic.
 */

import { createClient, SupabaseClient } from '@supabase/supabase-js';

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.error('ERROR: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required');
  process.exit(1);
}

export const supabase: SupabaseClient = createClient(supabaseUrl, supabaseKey);

// ---------------------------------------------------------------------------
// Startup connectivity check — fail fast if bronze schema is unreachable
// ---------------------------------------------------------------------------

export async function verifyBronzeAccess(table: string): Promise<void> {
  const { error } = await supabase
    .schema('bronze')
    .from(table)
    .select('*')
    .limit(1);

  if (error) {
    console.error(`FATAL: Cannot access bronze.${table}: ${error.message}`);
    console.error(`Check that SUPABASE_URL points to the correct project and bronze schema is exposed in PostgREST.`);
    console.error(`SUPABASE_URL: ${supabaseUrl}`);
    process.exit(1);
  }
  console.log(`  ✓ Verified access to bronze.${table}`);
}

// ---------------------------------------------------------------------------
// Batch upsert into bronze schema
// ---------------------------------------------------------------------------

export async function upsertBronze(
  table: string,
  records: Record<string, unknown>[],
  onConflict: string,
  batchSize = 100,
): Promise<number> {
  if (records.length === 0) return 0;

  let totalUpserted = 0;
  const totalBatches = Math.ceil(records.length / batchSize);

  for (let i = 0; i < records.length; i += batchSize) {
    const batch = records.slice(i, i + batchSize);
    const batchNum = Math.floor(i / batchSize) + 1;

    const { error, count } = await supabase
      .schema('bronze')
      .from(table)
      .upsert(batch, { onConflict, count: 'exact' });

    if (error) {
      console.error(`  Batch ${batchNum}/${totalBatches} upsert error: ${error.message}`);
      // Log first record for debugging
      if (batch[0]) {
        console.error(`  First record keys: ${Object.keys(batch[0]).join(', ')}`);
      }
      continue;
    }

    const upserted = count ?? batch.length;
    totalUpserted += upserted;
    console.log(`  Batch ${batchNum}/${totalBatches}: ${upserted} rows upserted`);
  }

  return totalUpserted;
}

// ---------------------------------------------------------------------------
// Mark stale listings as offline
// ---------------------------------------------------------------------------

/**
 * Mark listings as offline if not seen in `staleDays` days.
 *
 * Safety: only runs if the parser fetched at least `minFetchedCount` records
 * in this run (prevents mass-deactivation on partial/failed runs).
 */
export async function markStaleListings(
  table: string,
  minFetchedCount: number,
  actualFetchedCount: number,
  staleDays = 3,
): Promise<number> {
  if (actualFetchedCount < minFetchedCount) {
    console.log(
      `  Skipping stale check: only ${actualFetchedCount} fetched (min: ${minFetchedCount})`,
    );
    return 0;
  }

  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - staleDays);
  const cutoffISO = cutoff.toISOString();

  const { data, error } = await supabase
    .schema('bronze')
    .from(table)
    .update({ publishing_status: 'offline', time_online: 0 })
    .eq('publishing_status', 'online')
    .lt('last_seen_at', cutoffISO)
    .select('ad_url');

  if (error) {
    console.error(`  Mark stale error: ${error.message}`);
    return 0;
  }

  const marked = data?.length ?? 0;
  if (marked > 0) {
    console.log(
      `  Marked ${marked} listings as offline (not seen since ${cutoffISO.split('T')[0]})`,
    );
  } else {
    console.log(`  No stale listings found (cutoff: ${cutoffISO.split('T')[0]})`);
  }
  return marked;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
