/**
 * Déréliction / sans-maître flag.
 *
 * is_ownerless_event = true where a transfer looks like an appropriation of an
 * ownerless plot: the acquisition mode is Aneignung/Okkupation, a party is a public
 * body (Kanton/Gemeinde/Bürgergemeinde/Korporation), or the text names herrenlos/
 * Dereliktion/Eigentumsaufgabe. These are the rows that matter for the ownerless hunt.
 *
 * Legal note (see README): LU and SZ do NOT reserve herrenlose Grundstücke to the
 * canton/commune, so private appropriation is real and gets published. (VS is out —
 * LACC Art. 162 vests such land in the commune.)
 */

import type { ParsedRecord, Party } from './types.js';

const TEXT_MARKERS =
  /\b(herrenlos|derelikt|dereliktion|eigentumsaufgabe|aneignung|okkupation|occupation|sans[- ]ma[iî]tre|déréliction)\b/i;

const PUBLIC_BODY =
  /\b(kanton|kantons|politische gemeinde|ortsgemeinde|b[üu]rgergemeinde|korporation|genossame|municipalit[ée]|commune|[ée]tat du valais|staat)\b/i;

function partyIsPublicBody(p: Party): boolean {
  return p.names.some((n) => PUBLIC_BODY.test(n)) || (p.domicile != null && PUBLIC_BODY.test(p.domicile) && false);
  // domicile deliberately not used — a party living in a commune is not a public body.
}

/** Compute the flag from the parsed record + its raw text. */
export function flagOwnerless(rec: ParsedRecord): boolean {
  if (TEXT_MARKERS.test(rec.raw_text)) return true;
  if (rec.sellers.some(partyIsPublicBody)) return true;
  if (rec.buyers.some(partyIsPublicBody)) return true;
  return false;
}

/** Return a copy of the record with is_ownerless_event set. */
export function withOwnerlessFlag(rec: ParsedRecord): ParsedRecord {
  return { ...rec, is_ownerless_event: flagOwnerless(rec) };
}
