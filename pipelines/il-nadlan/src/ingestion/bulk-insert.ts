import { supabase } from '../config/supabase.js';
import type { TransactionRecord } from '../parsers/transactions.js';

const BATCH_SIZE = 1000;

export interface InsertResult {
  inserted: number;
  skipped: number;
  failed: number;
  errors: string[];
}

/**
 * Batch insert transaction records into bronze_il.transactions.
 * Checks for existing source_id to avoid duplicates.
 */
export async function bulkInsertTransactions(
  records: TransactionRecord[],
  ingestionLogId: string,
): Promise<InsertResult> {
  const stats: InsertResult = { inserted: 0, skipped: 0, failed: 0, errors: [] };

  for (let i = 0; i < records.length; i += BATCH_SIZE) {
    const batch = records.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(records.length / BATCH_SIZE);

    try {
      // Check for existing source_ids
      const sourceIds = [...new Set(batch.map((r) => r.source_id))];
      const { data: existing } = await supabase.from('transactions').select('source_id').in('source_id', sourceIds);

      const existingSet = new Set((existing ?? []).map((e: any) => e.source_id));
      const newRows = batch.filter((r) => !existingSet.has(r.source_id));

      if (newRows.length === 0) {
        stats.skipped += batch.length;
        console.log(`[Insert] Batch ${batchNum}/${totalBatches}: all ${batch.length} skipped (exist)`);
        continue;
      }

      const rowsWithMeta = newRows.map((r) => ({
        ...r,
        ingestion_log_id: ingestionLogId,
        updated_at: new Date().toISOString(),
      }));

      const { error } = await supabase.from('transactions').insert(rowsWithMeta);

      if (error) throw error;

      stats.inserted += newRows.length;
      stats.skipped += batch.length - newRows.length;
      console.log(
        `[Insert] Batch ${batchNum}/${totalBatches}: ${newRows.length} inserted, ${batch.length - newRows.length} skipped`,
      );
    } catch (err: any) {
      console.error(`[Insert] Batch ${batchNum}/${totalBatches} failed: ${err.message}`);
      stats.errors.push(`Batch ${batchNum}: ${err.message}`);
      stats.failed += batch.length;
    }
  }

  return stats;
}
