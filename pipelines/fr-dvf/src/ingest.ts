import { createClient } from '@supabase/supabase-js';
import type { DVFRecord } from './parse-transform.js';

const BATCH_SIZE = 5000;

function getClient() {
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) throw new Error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  return createClient(url, key, {
    auth: { persistSession: false, autoRefreshToken: false },
    db: { schema: 'bronze_fr' },
  });
}

export interface IngestStats {
  inserted: number;
  skipped: number;
  errors: number;
}

export async function ingest(records: DVFRecord[]): Promise<IngestStats> {
  const supabase = getClient();
  const stats: IngestStats = { inserted: 0, skipped: 0, errors: 0 };

  // Deduplicate within the batch by id_mutation + id_parcelle
  // (DVF has multiple rows per mutation for different parcelles)
  // We insert all rows — dedup is only against existing DB rows

  for (let i = 0; i < records.length; i += BATCH_SIZE) {
    const batch = records.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(records.length / BATCH_SIZE);

    try {
      // Use insert with onConflict to skip existing id_mutation rows
      // DVF rows are unique by the combination of multiple fields,
      // so we do a simple insert and let the DB's id (uuid) handle uniqueness.
      // To avoid re-importing, we first check which id_mutations already exist.
      const mutationIds = [...new Set(batch.map((r) => r.id_mutation))];
      const { data: existing } = await supabase
        .from('dvf')
        .select('id_mutation')
        .in('id_mutation', mutationIds);

      const existingSet = new Set((existing ?? []).map((e: any) => e.id_mutation));
      const newRows = batch.filter((r) => !existingSet.has(r.id_mutation));

      if (newRows.length === 0) {
        stats.skipped += batch.length;
        console.log(`[Ingest] Batch ${batchNum}/${totalBatches}: all ${batch.length} skipped (already exist)`);
        continue;
      }

      const { error } = await supabase.from('dvf').insert(newRows);

      if (error) throw error;
      stats.inserted += newRows.length;
      stats.skipped += batch.length - newRows.length;
      console.log(
        `[Ingest] Batch ${batchNum}/${totalBatches}: ${newRows.length} inserted, ${batch.length - newRows.length} skipped`,
      );
    } catch (err: any) {
      console.error(`[Ingest] Batch ${batchNum}/${totalBatches} failed: ${err.message}`);
      stats.errors += batch.length;
    }
  }

  return stats;
}
