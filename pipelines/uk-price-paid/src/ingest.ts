import { createClient } from '@supabase/supabase-js';
import type { PricePaidRecord } from './parse-transform.js';

const BATCH_SIZE = 2000;
const DELETE_CHUNK_SIZE = 200;
const RETRY_DELAY_MS = 3000;

function getClient() {
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) throw new Error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  return createClient(url, key, {
    auth: { persistSession: false, autoRefreshToken: false },
    db: { schema: 'bronze_gb' },
  });
}

export interface IngestStats {
  inserted: number;
  updated: number;
  deleted: number;
  skipped: number;
  errors: number;
}

export async function ingest(records: PricePaidRecord[]): Promise<IngestStats> {
  const supabase = getClient();
  const stats: IngestStats = { inserted: 0, updated: 0, deleted: 0, skipped: 0, errors: 0 };

  // Split by record_status
  const additions = records.filter((r) => r.record_status === 'A');
  const changes = records.filter((r) => r.record_status === 'C');
  const deletions = records.filter((r) => r.record_status === 'D');

  console.log(`[Ingest] Records by type: A=${additions.length} C=${changes.length} D=${deletions.length}`);

  // --- Additions: check existing, then insert new only ---
  for (let i = 0; i < additions.length; i += BATCH_SIZE) {
    const batch = additions.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(additions.length / BATCH_SIZE);

    try {
      // Check which transaction_ids already exist
      const txIds = [...new Set(batch.map((r) => r.transaction_id))];
      const { data: existing } = await supabase
        .from('price_paid')
        .select('transaction_id')
        .in('transaction_id', txIds);

      const existingSet = new Set((existing ?? []).map((e: any) => e.transaction_id));
      const newRows = batch
        .filter((r) => !existingSet.has(r.transaction_id))
        .map((r) => ({ ...r, updated_at: new Date().toISOString() }));

      if (newRows.length === 0) {
        stats.skipped += batch.length;
        console.log(`[Ingest:Add] Batch ${batchNum}/${totalBatches}: all ${batch.length} skipped (exist)`);
        continue;
      }

      const { error } = await supabase.from('price_paid').insert(newRows);
      if (error) {
        // Retry once on timeout errors (common on cold starts)
        if (error.message?.includes('timeout') || error.code === '57014') {
          console.warn(`[Ingest:Add] Batch ${batchNum}/${totalBatches} timed out, retrying after ${RETRY_DELAY_MS}ms...`);
          await new Promise((r) => setTimeout(r, RETRY_DELAY_MS));
          const { error: retryError } = await supabase.from('price_paid').insert(newRows);
          if (retryError) throw retryError;
        } else {
          throw error;
        }
      }

      stats.inserted += newRows.length;
      stats.skipped += batch.length - newRows.length;
      console.log(`[Ingest:Add] Batch ${batchNum}/${totalBatches}: ${newRows.length} inserted, ${batch.length - newRows.length} skipped`);
    } catch (err: any) {
      console.error(`[Ingest:Add] Batch ${batchNum}/${totalBatches} failed: ${err.message}`);
      stats.errors += batch.length;
    }
  }

  // --- Changes: update existing rows by transaction_id ---
  for (let i = 0; i < changes.length; i += BATCH_SIZE) {
    const batch = changes.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(changes.length / BATCH_SIZE);

    let batchUpdated = 0;
    try {
      // Update one at a time since we can't batch-update different values
      for (const record of batch) {
        const { record_status, ...updateData } = record;
        const { error } = await supabase
          .from('price_paid')
          .update({ ...updateData, updated_at: new Date().toISOString() })
          .eq('transaction_id', record.transaction_id);

        if (error) {
          console.warn(`[Ingest:Change] Failed to update ${record.transaction_id}: ${error.message}`);
          stats.errors++;
        } else {
          batchUpdated++;
        }
      }
      stats.updated += batchUpdated;
      console.log(`[Ingest:Change] Batch ${batchNum}/${totalBatches}: ${batchUpdated} updated`);
    } catch (err: any) {
      console.error(`[Ingest:Change] Batch ${batchNum}/${totalBatches} failed: ${err.message}`);
      stats.errors += batch.length - batchUpdated;
    }
  }

  // --- Deletions: by transaction_id ---
  for (let i = 0; i < deletions.length; i += BATCH_SIZE) {
    const batch = deletions.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(deletions.length / BATCH_SIZE);

    try {
      const ids = batch.map((r) => r.transaction_id);
      // Chunk deletes into small batches to avoid PostgREST URL length limits
      for (let j = 0; j < ids.length; j += DELETE_CHUNK_SIZE) {
        const chunk = ids.slice(j, j + DELETE_CHUNK_SIZE);
        const { error } = await supabase.from('price_paid').delete().in('transaction_id', chunk);
        if (error) throw error;
      }
      stats.deleted += batch.length;
      console.log(`[Ingest:Delete] Batch ${batchNum}/${totalBatches}: ${batch.length} deleted`);
    } catch (err: any) {
      console.error(`[Ingest:Delete] Batch ${batchNum}/${totalBatches} failed: ${err.message}`);
      stats.errors += batch.length;
    }
  }

  return stats;
}
