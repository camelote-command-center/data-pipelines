/**
 * Load layer.
 *
 * (1) upsertBronze  — parsed rows → re-LLM bronze_ch.transactions_national
 *                     (natural key UNIQUE(source_id, canton); additive, never NULL-overwrite).
 * (2) syncToLamap   — bronze rows → lamap_db ref.transactions_national via client-side
 *                     streamed UPSERT (NEVER dblink — that would put the re-LLM password
 *                     into lamap_db's session/logs). Same convention as the FR/VD loaders.
 *
 * Shared clients are imported lazily so the pure ingest/parse paths (and tests) never
 * require DB env vars. Cantons LU/SZ are disjoint from SG (paused) and BE
 * (succession_events) — no row collision on the shared table.
 */

import type { BronzeTxnRow } from './types.js';

const TABLE = 'transactions_national';
const ON_CONFLICT = 'source_id,canton';

/** Upsert parsed rows into re-LLM bronze_ch.transactions_national. Returns count upserted. */
export async function upsertBronze(rows: BronzeTxnRow[]): Promise<number> {
  if (!rows.length) return 0;
  const { upsert } = await import('../../_shared/re-llm.js');
  return upsert('bronze_ch', TABLE, rows as unknown as Record<string, unknown>[], ON_CONFLICT);
}

/**
 * Stream re-LLM bronze rows for the given cantons into lamap_db ref.transactions_national.
 * Client-side streamed UPSERT (read from re-LLM, write to lamap_db) — additive, one canton
 * scope at a time. Requires the lamap_db unique index on (source_id, canton).
 */
export async function syncToLamap(cantons: string[]): Promise<number> {
  if (!cantons.length) return 0;
  const reLlm = await import('../../_shared/re-llm.js');
  const lamap = await import('../../_shared/supabase.js');

  let total = 0;
  const PAGE = 1000;
  for (const canton of cantons) {
    for (let offset = 0; ; offset += PAGE) {
      const { data, error } = await reLlm.supabase
        .schema('bronze_ch')
        .from(TABLE)
        .select('*')
        .eq('canton', canton)
        .order('id', { ascending: true })
        .range(offset, offset + PAGE - 1);
      if (error) throw new Error(`re-LLM read failed (${canton}): ${error.message}`);
      if (!data || data.length === 0) break;

      // Drop DB-managed columns before upserting into the target.
      const payload = data.map(({ id, created_at, updated_at, ...rest }) => rest);
      const { error: upErr, count } = await lamap.supabase
        .schema('ref')
        .from(TABLE)
        .upsert(payload, { onConflict: ON_CONFLICT, count: 'exact' });
      if (upErr) throw new Error(`lamap_db upsert failed (${canton}): ${upErr.message}`);
      total += count ?? payload.length;
      if (data.length < PAGE) break;
    }
  }
  return total;
}
