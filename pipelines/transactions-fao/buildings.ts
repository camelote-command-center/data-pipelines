/**
 * Building identifiers in the FAO gazette.
 *
 * Three shapes appear in rubrique 137:
 *
 *   23/3009        a whole parcelle
 *   23/3009-6      PPE feuillet 6 of parcelle 23/3009
 *   23/3009-1-1    COP lot 1 of PPE feuillet 1 of parcelle 23/3009 — a co-ownership
 *                  share of one PPE lot, e.g. one of 29 shares of a collective garage
 *
 * Before 2026-10-12 both the validator and the splitter allowed a single suffix only
 * (`/^\d+\/\d+(-\d+)?$/`). A COP id therefore never split, failed validation, and the
 * ENTIRE affaire was dropped — not just that lot. Affaire 2026/8599/0 (CHF 11.4M,
 * 13 COP shares + 20 PPE lots over parcelle 23/3009) was lost that way and had to be
 * entered by hand. Any COP transaction hit this, whatever its size.
 *
 * Normalisation keeps the storage convention already in bronze_ch.fao_transactions
 * (see affaires 2026/8523/0 and 2026/8600/0): `building_id` is the bare parcelle,
 * `feuillet_number` the PPE feuillet, `parts_cop` the share. The COP lot number — which
 * says *which* of the shares changed hands — used to be thrown away with the suffix;
 * it is now kept in `cop_lot_number` so 13 shares of one feuillet are 13 distinct lots
 * rather than 13 identical rows.
 */

/** A parcelle, optionally a PPE feuillet, optionally a COP lot within it. */
export const BUILDING_ID_RE = /^(\d+\/\d+)(?:-(\d+))?(?:-(\d+))?$/;

export const isValidBuildingId = (id: string): boolean => BUILDING_ID_RE.test(id);

export interface RawBuilding {
  'Building ID'?: string;
  'Feuillet number'?: string;
  'COP lot number'?: string;
  [key: string]: unknown;
}

/**
 * Split the suffixes off every Building ID, in place.
 *
 * The id string is the source of truth: when it carries a feuillet the LLM's own
 * `Feuillet number` is overwritten, which is the pre-existing behaviour and stops a
 * model that put the whole id in one field and guessed the other from disagreeing
 * with itself. Ids that do not match are left untouched for the validator to reject.
 */
export function normalizeBuildingIds(buildings: RawBuilding[]): RawBuilding[] {
  for (const building of buildings ?? []) {
    const match = (building['Building ID'] || '').match(BUILDING_ID_RE);
    if (!match) continue;
    const [, parcelle, feuillet, copLot] = match;
    building['Building ID'] = parcelle;
    if (feuillet) building['Feuillet number'] = feuillet;
    if (copLot) building['COP lot number'] = copLot;
  }
  return buildings;
}
