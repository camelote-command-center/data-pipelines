/**
 * egrid reconciliation (best-effort).
 *
 * Enriches bronze rows with lamap_db `ref.plots.egrid` by matching
 * (canton_code, commune_name ≈ grundbuchkreis, parcel_number). The federal plot bases
 * ARE present on lamap_db (LU 105k / SZ 52k, egrid+parcel_number+commune_name 100%
 * populated at recon), so this resolves for well-formed records. The resolved egrid
 * lands in raw_data.egrid → surfaced by the generated `ref.transactions_national.egrid`
 * column (queryable). Downstream joins use egrid — NEVER no_commune_no_parcelle
 * (GE-only, `c655f036`). Unresolved rows still load (egrid stays null); reconcile is additive.
 *
 * ref.plots is NOT PostgREST-exposed → uses the direct pg helper.
 */

import type { BronzeTxnRow } from './types.js';

export async function reconcileEgrid(rows: BronzeTxnRow[]): Promise<{ resolved: number }> {
  if (!rows.length) return { resolved: 0 };
  const { lookupEgrid } = await import('./lamap-pg.js');

  let resolved = 0;
  for (const row of rows) {
    const parcel = (row.raw_data.parcel_number as string | null) ?? null;
    const commune = (row.raw_data.grundbuchkreis as string | null) ?? row.address ?? null;
    if (!parcel || !commune) continue;
    const egrid = await lookupEgrid(row.canton, commune, parcel);
    if (egrid) {
      row.raw_data.egrid = egrid;
      resolved++;
    }
  }
  return { resolved };
}
