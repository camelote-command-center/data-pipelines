/**
 * Direct Postgres access to lamap_db via the session pooler.
 *
 * Required because `ref.*` (canonical) is NOT exposed to PostgREST — supabase-js REST
 * can only reach public/lamap_app/ask_lamap. This is the same "client-side streamed
 * UPSERT, never dblink" path the FR/VD national loads used. Connection string in
 * LAMAP_DB_URI (the registry `session_pooler_uri`; NEVER hardcode — read from env).
 */

import { Pool } from 'pg';

let _pool: Pool | null = null;
function pool(): Pool {
  if (!_pool) {
    const uri = process.env.LAMAP_DB_URI;
    if (!uri) {
      console.error('ERROR: LAMAP_DB_URI (lamap_db session_pooler_uri) is required for lamap writes');
      process.exit(1);
    }
    _pool = new Pool({ connectionString: uri, max: 4 });
  }
  return _pool;
}

/**
 * Multi-row idempotent UPSERT keyed on (source_id, canton).
 * DO UPDATE uses COALESCE(EXCLUDED.col, target.col) so an incoming NULL never
 * overwrites a populated value (additive). Generated columns must NOT be listed.
 */
async function upsert(
  qualifiedTable: string,
  cols: string[],
  rows: Array<Record<string, unknown>>,
): Promise<number> {
  if (!rows.length) return 0;
  const jsonCols = new Set(['raw_data']);
  const values: unknown[] = [];
  const tuples = rows.map((row, r) => {
    const ph = cols.map((c, i) => {
      const idx = r * cols.length + i + 1;
      const v = row[c];
      values.push(jsonCols.has(c) && v != null ? JSON.stringify(v) : (v ?? null));
      return jsonCols.has(c) ? `$${idx}::jsonb` : `$${idx}`;
    });
    return `(${ph.join(',')})`;
  });
  const setClause = cols
    .filter((c) => c !== 'source_id' && c !== 'canton')
    .map((c) => `${c} = COALESCE(EXCLUDED.${c}, ${qualifiedTable.split('.').pop()}.${c})`)
    .join(', ');
  const sql =
    `INSERT INTO ${qualifiedTable} (${cols.join(',')}) VALUES ${tuples.join(',')} ` +
    `ON CONFLICT (source_id, canton) DO UPDATE SET ${setClause}`;
  const res = await pool().query(sql, values);
  return res.rowCount ?? 0;
}

const REF_COLS = [
  'source_id', 'source_url', 'transaction_date', 'address', 'reason', 'property_type',
  'price', 'surface_m2', 'price_per_m2', 'nb_buyers', 'buyers', 'nb_sellers', 'sellers',
  'previous_transaction_date', 'canton', 'source_file', 'raw_data',
]; // NB: is_ownerless_event + egrid are GENERATED — excluded by design.

const SERVING_COLS = [
  'source_id', 'canton', 'source_system', 'transaction_date', 'address', 'type_transaction',
  'property_type', 'price', 'surface_m2', 'price_per_m2', 'nb_buyers', 'buyers', 'nb_sellers',
  'sellers', 'previous_transaction_date', 'source_url', 'years_since_previous',
];

export const upsertRef = (rows: Array<Record<string, unknown>>) =>
  upsert('ref.transactions_national', REF_COLS, rows);

export const upsertServing = (rows: Array<Record<string, unknown>>) =>
  upsert('public.transactions_national_data', SERVING_COLS, rows);

/** Isolated per-canton delete (RESTRICT — no cascade). Verification cleanup only. */
export async function deleteCanton(qualifiedTable: string, canton: string): Promise<void> {
  await pool().query(`DELETE FROM ${qualifiedTable} WHERE canton = $1`, [canton]);
}

/** (canton, commune≈grundbuchkreis, parcel_number) → egrid, unambiguous single match only. */
export async function lookupEgrid(
  canton: string,
  commune: string,
  parcel: string,
): Promise<string | null> {
  const res = await pool().query(
    `SELECT egrid FROM ref.plots
      WHERE canton_code = $1 AND parcel_number = $2 AND commune_name ILIKE $3
      LIMIT 2`,
    [canton, parcel, `%${commune}%`],
  );
  return res.rows.length === 1 && res.rows[0].egrid ? String(res.rows[0].egrid) : null;
}

export async function endPool(): Promise<void> {
  if (_pool) {
    await _pool.end();
    _pool = null;
  }
}
