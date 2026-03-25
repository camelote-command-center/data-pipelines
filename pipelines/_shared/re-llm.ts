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
// Helpers
// ---------------------------------------------------------------------------

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
