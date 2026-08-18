/**
 * Load layer.
 *
 * (1) upsertBronze — parsed rows → re-LLM bronze_ch.transactions_national via supabase-js
 *     (bronze_ch is PostgREST-exposed on re-LLM; natural key UNIQUE(source_id, canton)).
 * (2) syncToLamap — re-LLM bronze → lamap_db via a DIRECT pg connection (session pooler):
 *       • ref.transactions_national  — CANONICAL (raw_data + generated is_ownerless_event
 *         /egrid). `ref.*` is NOT PostgREST-exposed, so this MUST go over pg, not REST —
 *         the same "client-side streamed UPSERT, never dblink" path as the FR/VD loads.
 *       • public.transactions_national_data — SERVING twin ask_lamap reads (lean projection).
 *     Both keyed UNIQUE(source_id, canton); UPSERT COALESCEs so NULL never overwrites.
 *
 * Cantons LU/SZ are disjoint from SG (paused) and BE (succession_events).
 * Shared clients imported lazily so pure ingest/parse paths (and tests) need no DB env.
 */

import type { BronzeTxnRow } from './types.js';

const ON_CONFLICT = 'source_id,canton';
const BRONZE_TABLE = 'transactions_national'; // schema 'bronze_ch' (re-LLM, REST-exposed)

/** Upsert parsed rows into re-LLM bronze_ch.transactions_national. Returns count upserted. */
export async function upsertBronze(rows: BronzeTxnRow[]): Promise<number> {
  if (!rows.length) return 0;
  const { upsert } = await import('../../_shared/re-llm.js');
  return upsert('bronze_ch', BRONZE_TABLE, rows as unknown as Record<string, unknown>[], ON_CONFLICT);
}

/** Lean 18-col projection ref → public.transactions_national_data (no raw_data). */
function toServingRow(r: Record<string, unknown>): Record<string, unknown> {
  return {
    source_id: r.source_id,
    canton: r.canton,
    source_system: r.source_file, // serving twin names the organ 'source_system'
    transaction_date: r.transaction_date,
    address: r.address,
    type_transaction: r.reason, // 'handaenderung' | 'aneignung'
    property_type: r.property_type,
    price: r.price ?? null,
    surface_m2: r.surface_m2,
    price_per_m2: r.price_per_m2 ?? null,
    nb_buyers: r.nb_buyers,
    buyers: r.buyers,
    nb_sellers: r.nb_sellers,
    sellers: r.sellers,
    previous_transaction_date: r.previous_transaction_date ?? null,
    source_url: r.source_url,
    years_since_previous: null,
  };
}

/**
 * Stream re-LLM bronze rows for the given cantons into lamap_db canonical + serving.
 * Idempotent UPSERT. Returns rows written to the canonical (ref) table.
 */
export async function syncToLamap(cantons: string[]): Promise<number> {
  if (!cantons.length) return 0;
  const reLlm = await import('../../_shared/re-llm.js');
  const { upsertRef, upsertServing } = await import('./lamap-pg.js');

  let total = 0;
  const PAGE = 1000;
  for (const canton of cantons) {
    for (let offset = 0; ; offset += PAGE) {
      const { data, error } = await reLlm.supabase
        .schema('bronze_ch')
        .from(BRONZE_TABLE)
        .select('*')
        .eq('canton', canton)
        .order('id', { ascending: true })
        .range(offset, offset + PAGE - 1);
      if (error) throw new Error(`re-LLM read failed (${canton}): ${error.message}`);
      if (!data || data.length === 0) break;

      const refRows = data.map(({ id, created_at, updated_at, ...rest }) => rest);
      await upsertRef(refRows);
      await upsertServing(refRows.map(toServingRow));
      total += refRows.length;
      if (data.length < PAGE) break;
    }
  }
  return total;
}

/**
 * Verification/cleanup helper — remove all rows for the given cantons from the three
 * tables (re-LLM bronze via REST + lamap ref/serving via pg). Isolated per-canton
 * deletes, RESTRICT (no cascade). Live load-path verification only.
 */
export async function purgeCantons(cantons: string[]): Promise<void> {
  if (!cantons.length) return;
  const reLlm = await import('../../_shared/re-llm.js');
  const { deleteCanton } = await import('./lamap-pg.js');
  for (const canton of cantons) {
    await reLlm.supabase.schema('bronze_ch').from(BRONZE_TABLE).delete().eq('canton', canton);
    await deleteCanton('ref.transactions_national', canton);
    await deleteCanton('public.transactions_national_data', canton);
  }
}
