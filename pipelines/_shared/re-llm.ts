/**
 * re-LLM Supabase client + batch upsert helper.
 *
 * For pipelines that write to the re-LLM data warehouse (znrvddgmczdqoucmykij).
 * Uses RE_LLM_SUPABASE_URL / RE_LLM_SUPABASE_SERVICE_ROLE_KEY env vars.
 * Schema: bronze_ch (Switzerland), bronze_fr (France), bronze_gb (UK), etc.
 */

import { createClient, SupabaseClient } from '@supabase/supabase-js';

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

const supabaseUrl = process.env.RE_LLM_SUPABASE_URL;
const supabaseKey = process.env.RE_LLM_SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.error('ERROR: RE_LLM_SUPABASE_URL and RE_LLM_SUPABASE_SERVICE_ROLE_KEY are required');
  process.exit(1);
}

export const supabase: SupabaseClient = createClient(supabaseUrl, supabaseKey);

// ---------------------------------------------------------------------------
// Startup connectivity check
// ---------------------------------------------------------------------------

export async function verifyAccess(schema: string, table: string): Promise<void> {
  const { error } = await supabase
    .schema(schema)
    .from(table)
    .select('*')
    .limit(1);

  if (error) {
    console.error(`FATAL: Cannot access ${schema}.${table}: ${error.message}`);
    console.error(`Check RE_LLM_SUPABASE_URL and that ${schema} is exposed in PostgREST.`);
    process.exit(1);
  }
  console.log(`  ✓ Verified access to ${schema}.${table}`);
}

// ---------------------------------------------------------------------------
// Batch upsert
// ---------------------------------------------------------------------------

export async function upsert(
  schema: string,
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
      .schema(schema)
      .from(table)
      .upsert(batch, { onConflict, count: 'exact' });

    if (error) {
      console.error(`  Batch ${batchNum}/${totalBatches} upsert error: ${error.message}`);
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
  schema: string,
  table: string,
  idColumn: string,
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
    .schema(schema)
    .from(table)
    .update({ publishing_status: 'offline' })
    .eq('publishing_status', 'online')
    .lt('last_seen_at', cutoffISO)
    .select(idColumn);

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
