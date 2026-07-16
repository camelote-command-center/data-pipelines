/**
 * egrid reconciliation (best-effort).
 *
 * Enriches bronze rows with lamap_db `ref.plots.egrid` by matching
 * (canton_code, commune_name ≈ grundbuchkreis, parcel_number). The federal plot bases
 * ARE present on lamap_db (LU 105k / SZ 52k, egrid+parcel_number+commune_name 100%
 * populated at recon), so this resolves for well-formed records.
 *
 * ⚠️ Join on egrid downstream — NEVER no_commune_no_parcelle (GE-only, `c655f036`).
 * If a parcel doesn't resolve, the row still lands (egrid stays null) — the raw feed is
 * valuable standalone; reconciliation is additive.
 *
 * Uses the lamap_db client from _shared/supabase.ts, imported lazily so the pure
 * ingest/parse paths (and their tests) never require DB env vars.
 */

import type { BronzeTxnRow } from './types.js';

export async function reconcileEgrid(rows: BronzeTxnRow[]): Promise<{ resolved: number }> {
  if (!rows.length) return { resolved: 0 };
  const { supabase } = await import('../../_shared/supabase.js');

  let resolved = 0;
  for (const row of rows) {
    const parcel = (row.raw_data.parcel_number as string | null) ?? null;
    const commune = (row.raw_data.grundbuchkreis as string | null) ?? row.address ?? null;
    if (!parcel || !commune) continue;

    const { data } = await supabase
      .schema('ref')
      .from('plots')
      .select('egrid, commune_name')
      .eq('canton_code', row.canton)
      .eq('parcel_number', parcel)
      .ilike('commune_name', `%${commune}%`)
      .limit(2);

    // Only accept an unambiguous single match.
    if (data && data.length === 1 && data[0].egrid) {
      row.raw_data.egrid = data[0].egrid;
      row.raw_data.commune_bfs_source = 'ref.plots';
      resolved++;
    }
  }
  return { resolved };
}
