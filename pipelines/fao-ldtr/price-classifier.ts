// ---------------------------------------------------------------------------
// Non-price transfer types
//
// A FAO notice can put a legal transfer type in "Prix de vente" instead of a
// figure — the flat was given, inherited or bequeathed, so there is no price.
// That is a different transaction type, not a failed parse, so we classify it
// here and store prix = null. It must never reach safe_cast.to_numeric, which
// would quarantine it forever and teach the quarantine to lie.
//
// The vocabulary below is derived from every distinct value observed in
// bronze_ch.fao_ldtr and safe_cast.quarantine (2026-06-07 .. 2026-08-17):
//   donation | Donation d'actions | Succession | Succession sans soulte
//   Succession sans soultes | liquidation d'une succession | Délivrance de legs
// Matching is on normalised stems so morphological variants (plural "soultes",
// the apostrophe-stripped "dactions"/"dune" this parser itself produces) are
// covered without listing every spelling.
//
// DELIBERATELY NOT GENERALISED to "contains no digits": an unrecognised
// non-numeric shape SHOULD still quarantine, so genuinely new formats stay
// visible. Only known vocabulary is classified.
// ---------------------------------------------------------------------------

const NON_PRICE_STEMS: { stem: RegExp; reason: string }[] = [
  { stem: /\bdonation\b/, reason: 'donation' },
  { stem: /\bsuccession\b/, reason: 'succession' },
  { stem: /\blegs\b/, reason: 'delivrance_de_legs' },
  { stem: /\bpartage\b/, reason: 'partage' },
];

/** lowercase, strip accents and apostrophes, collapse whitespace */
function normaliseTerm(v: string): string {
  return v
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/['\u2019]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Returns the transfer-type reason when `raw` is a known non-price term,
 * or null when it should be treated as a price and cast normally.
 */
export function classifyNonPrice(raw: string): string | null {
  const n = normaliseTerm(raw);
  if (!n) return null;
  if (/\d/.test(n)) return null; // anything carrying digits is a price shape
  for (const { stem, reason } of NON_PRICE_STEMS) {
    if (stem.test(n)) return reason;
  }
  return null;
}

// ---------------------------------------------------------------------------
// Lot-labelled prices
//
// When a requête covers several lots, the notice prices ONE of them and labels
// the figure with its lot number:
//     "lot n° 2.01 : 1100000"
// This is NOT a multi-lot string — each bronze row carries exactly one lot and
// one price (verified: 0 rows in bronze_ch.fao_ldtr match a multi-lot pattern),
// so there is no sum/max decision. The correct value is simply the figure after
// the colon. The lot number itself is already carried by lot_key and by the
// full notice text in `transaction`, so nothing is lost by dropping the label.
//
// The regex is ANCHORED on purpose. It must match this shape and nothing else:
// an unrecognised non-numeric value has to keep reaching safe_cast.quarantine,
// or the next new format arrives invisibly. In particular this must never
// swallow the non-price transfer types handled by classifyNonPrice() — the
// test suite asserts both directions.
// ---------------------------------------------------------------------------

const LOT_PRICE_RE = /^lot\s*n[°º]?\s*\d+(?:\.\d+)*\s*:\s*(\d+(?:[.,]\d+)?)$/i;

/**
 * Returns the price as a plain string when `raw` is a lot-labelled price,
 * or null when it is anything else (including a bare number, which needs no
 * extraction, and a non-price transfer type, which classifyNonPrice handles).
 */
export function extractLotPrice(raw: string): string | null {
  const n = raw.replace(/\s+/g, ' ').trim();
  const m = LOT_PRICE_RE.exec(n);
  return m ? m[1].replace(',', '.') : null;
}
