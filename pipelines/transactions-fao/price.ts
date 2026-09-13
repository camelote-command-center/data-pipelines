/**
 * Swiss price-extraction helpers for the FAO transactions parser.
 *
 * Bugs in production drove the design:
 *   1. The previous cleaner stripped ALL non-digits including '.', so
 *      "40'032'944.47" became "4003294447" (×100 of the real value).
 *   2. Claude Sonnet sometimes duplicates a digit-group while transcribing
 *      gazette text, e.g. "1'875'000" → "1'875'000'000" (×1000 hallucination).
 *   3. (2026-09-13) The regex only knew "Prix total de l'affaire". The gazette also
 *      publishes multi-lot entries as "Prix: X.-; … Prix: Y.-", which it never
 *      matched — when the LLM also missed them the price was written NULL with
 *      exit 0. Eight such rows since 2026-05-13 (incl. a CHF 28.9M sale), plus 84
 *      "Prix total" rows parsed before this module existed and never reparsed.
 *
 * Defence in depth:
 *   - extractPrice(): deterministic extraction from untouched gazette text.
 *     ALWAYS preferred over any LLM-supplied price.
 *   - isValidSwissAmount(): strict grouping. A malformed amount in the SOURCE
 *     ("100'00.-") is never guessed at — neither by us nor by the LLM.
 *   - hasPriceMarker(): deliberately BROADER than the extractor. If the text
 *     visibly carries a price and extraction produced nothing, that is a loud
 *     failure, not a NULL. Unknown shapes fail loud instead of being guessed.
 *   - priceGate(): the end-of-run decision the parser uses to go red. Exported so
 *     the CI self-test exercises the exact same function, not a copy.
 */

const PRICE_MISMATCH_TOLERANCE = 0.5;     // CHF — rounding tolerance only
const IMPLAUSIBLE_PRICE_CHF = 2_000_000_000; // 2 billion CHF ceiling

/**
 * Clean a Swiss-formatted price string.
 *
 *   "40'032'944.47" → "40032944.47"
 *   "1'875'000.-"   → "1875000"
 *   "CHF 1'200'000.--" → "1200000"
 *   "Fr. 302'000'000.--" → "302000000"
 *   "710'000"       → "710000"
 *   "—" / null      → null
 *
 * Returns the cleaned numeric string (preserving decimals), or null if input
 * is unparseable. Stored as varchar in bronze_ch.fao_transactions, so we
 * keep the string form (no float coercion → no precision loss).
 */
export function cleanSwissPrice(raw: string | number | null | undefined): string | null {
  if (raw === null || raw === undefined) return null;
  let s = String(raw).trim();
  if (!s) return null;

  // Strip currency markers
  s = s.replace(/^(CHF|Frs?\.?)\s*/i, '');
  // Strip the placeholder ".-" / ".--" / ".---" trailing format (no cents)
  s = s.replace(/\.-+\s*$/, '');
  // Strip Swiss apostrophes (ASCII ' and Unicode ' U+2019) used as thousand sep
  s = s.replace(/['’]/g, '');
  s = s.trim();

  // Must be plain integer or integer.decimal — reject anything else
  if (!/^\d+(\.\d+)?$/.test(s)) return null;
  return s;
}

// ─────────────────────────────────────────────────────────────────────────────
// Strict amount validation
// ─────────────────────────────────────────────────────────────────────────────

// Groups of exactly three after the first group ("1'080'840"), mixed ' / ’ allowed.
const SWISS_GROUPED_RE = /^\d{1,3}(?:['’]\d{3})+(?:\.\d{1,2})?$/;
// An unseparated amount. At most TWO decimals: "1.250" is ambiguous between
// CHF 1.25 and a dot-thousands 1'250, so it must not validate.
const PLAIN_AMOUNT_RE = /^\d+(?:\.\d{1,2})?$/;

/** Strip the gazette's trailing ".-", ".--" or bare sentence-final ".". */
function stripAmountTail(raw: string): string {
  return raw.trim().replace(/\.-*\s*$/, '').trim();
}

/**
 * True only for an amount whose digit grouping is unambiguous.
 * Verified against the corpus (2026-09-13): of 54,055 "Prix total" amounts only
 * 2 fail, and both are malformed in the source ("1'500''000.00", "820'00").
 */
export function isValidSwissAmount(raw: string | null | undefined): boolean {
  if (raw === null || raw === undefined) return false;
  const s = stripAmountTail(String(raw));
  return SWISS_GROUPED_RE.test(s) || PLAIN_AMOUNT_RE.test(s);
}

/** Exact decimal sum in integer cents — no float drift ("106130.15" + …). */
function sumAmounts(cleaned: string[]): string {
  let cents = 0n;
  for (const c of cleaned) {
    const [whole, frac = ''] = c.split('.');
    cents += BigInt(whole) * 100n + BigInt((frac + '00').slice(0, 2));
  }
  const whole = cents / 100n;
  const rem = cents % 100n;
  return rem === 0n ? whole.toString() : `${whole}.${rem.toString().padStart(2, '0')}`;
}

// ─────────────────────────────────────────────────────────────────────────────
// Extraction
// ─────────────────────────────────────────────────────────────────────────────

/** "Prix total de l'affaire: 1'080'840.-" — an explicit total always wins. */
const TOTAL_RE = new RegExp(
  "Prix\\s+total\\s+de\\s+l['’]affaire" +
  "\\s*:?\\s*" +
  "(?:CHF|Fr\\.?)?\\s*" +
  "(?<price>[\\d'’]+(?:\\.\\d+)?)",
  'i',
);

/** "Prix: 950'000.-" — one per lot in a multi-lot entry. */
const SHORT_PART_RE = /\bprix\s*:\s*(?:chf|frs?\.?)?\s*([0-9][0-9'’.]*)/gi;

/**
 * Broad on purpose: any wording that visibly states a price with digits.
 * If this fires and extraction produced nothing, the run must go red.
 */
const PRICE_MARKER_RE =
  /\bprix(?:\s+(?:total|global|de\s+vente))?(?:\s+de\s+l['’]affaire)?\s*:?\s*(?:chf|frs?\.?)?\s*\d/i;

export function hasPriceMarker(rawText: string | null | undefined): boolean {
  return !!rawText && PRICE_MARKER_RE.test(rawText);
}

export type PriceExtraction =
  | { kind: 'total'; price: string }
  | { kind: 'parts'; price: string; parts: string[] }
  | { kind: 'malformed'; fragment: string }
  | { kind: 'none' };

/**
 * Deterministic price from the raw gazette text.
 *
 *   1. "Prix total de l'affaire: X"  → X.
 *   2. otherwise every "Prix: X" part → their SUM. This is the established
 *      semantics, not a new choice: of 435 already-priced multi-lot rows, 381
 *      (88%) store the sum (swaps: 7 of 10).
 *   3. any amount with ambiguous grouping → 'malformed'. Never guessed.
 */
export function extractPrice(rawText: string | null | undefined): PriceExtraction {
  if (!rawText) return { kind: 'none' };

  const total = rawText.match(TOTAL_RE);
  if (total?.groups?.price) {
    const raw = total.groups.price;
    if (!isValidSwissAmount(raw)) return { kind: 'malformed', fragment: raw };
    const cleaned = cleanSwissPrice(raw);
    return cleaned === null ? { kind: 'malformed', fragment: raw } : { kind: 'total', price: cleaned };
  }

  const raws = [...rawText.matchAll(SHORT_PART_RE)].map((m) => m[1]);
  if (raws.length === 0) return { kind: 'none' };

  const cleaned: string[] = [];
  for (const r of raws) {
    const c = isValidSwissAmount(r) ? cleanSwissPrice(stripAmountTail(r)) : null;
    if (c === null) return { kind: 'malformed', fragment: r };
    cleaned.push(c);
  }
  return {
    kind: 'parts',
    price: cleaned.length === 1 ? cleaned[0] : sumAmounts(cleaned),
    parts: cleaned,
  };
}

/** Backward-compatible: the deterministic price, or null. */
export function extractPriceFromRaw(rawText: string | null | undefined): string | null {
  const e = extractPrice(rawText);
  return e.kind === 'total' || e.kind === 'parts' ? e.price : null;
}

// ─────────────────────────────────────────────────────────────────────────────
// Validation
// ─────────────────────────────────────────────────────────────────────────────

export type UnextractedReason = 'price_in_text_not_extracted' | 'price_malformed_in_source';

export type ValidationFailure =
  | { ok: false; reason: 'implausible_price'; parsedPrice: string };

export type ValidationOk = {
  ok: true;
  price: string | null;
  source: 'regex' | 'llm_fallback' | 'none';
  warning?: string;  // non-fatal — e.g. LLM disagreed wildly with regex
  /**
   * Set when the text visibly carries a price we could not trustworthily
   * extract. The row is still written (price NULL), quarantined, and the run
   * must go red — see priceGate().
   */
  unextracted?: { reason: UnextractedReason; fragment: string };
};

/**
 * Final guard before insert. Deterministic extraction over the raw gazette text
 * is always authoritative — the LLM is a fallback only when the text carries no
 * recognisable price shape at all.
 */
export function validateParsedPrice(
  rawText: string | null | undefined,
  llmPrice: string | null,
): ValidationOk | ValidationFailure {
  const ext = extractPrice(rawText);

  // A malformed amount in the source is never resolved by the LLM's guess.
  if (ext.kind === 'malformed') {
    return {
      ok: true, price: null, source: 'none',
      unextracted: { reason: 'price_malformed_in_source', fragment: ext.fragment },
    };
  }

  let source: 'regex' | 'llm_fallback' | 'none';
  let finalPrice: string | null;
  let warning: string | undefined;

  if (ext.kind === 'total' || ext.kind === 'parts') {
    finalPrice = ext.price;
    source = 'regex';
    if (llmPrice && llmPrice !== finalPrice) {
      const diff = Math.abs(parseFloat(finalPrice) - parseFloat(llmPrice));
      if (diff > PRICE_MISMATCH_TOLERANCE) {
        warning = `LLM disagreed with regex (LLM=${llmPrice}, regex=${finalPrice}). Took regex.`;
      }
    }
  } else if (llmPrice !== null) {
    finalPrice = llmPrice;
    source = 'llm_fallback';
    warning = 'Regex missed; using LLM price as fallback. Audit periodically.';
  } else {
    // No price extracted. Silent only if the text does not visibly carry one.
    if (hasPriceMarker(rawText)) {
      const frag = rawText!.match(PRICE_MARKER_RE)?.[0] ?? '';
      return {
        ok: true, price: null, source: 'none',
        unextracted: { reason: 'price_in_text_not_extracted', fragment: frag },
      };
    }
    return { ok: true, price: null, source: 'none' };
  }

  // Implausibility floor — refuse insertion if value is absurd.
  const numeric = parseFloat(finalPrice);
  if (Number.isFinite(numeric) && numeric > IMPLAUSIBLE_PRICE_CHF) {
    return { ok: false, reason: 'implausible_price', parsedPrice: finalPrice };
  }

  return warning
    ? { ok: true, price: finalPrice, source, warning }
    : { ok: true, price: finalPrice, source };
}

// ─────────────────────────────────────────────────────────────────────────────
// Run gate
// ─────────────────────────────────────────────────────────────────────────────

export type UnextractedPrice = { affaire: string; reason: UnextractedReason; fragment: string };

/**
 * The end-of-run decision. A price visible in the source but absent from the
 * column must turn the workflow RED — the same contract as unparseable dates.
 * fetch-transactions.ts and price-gate.selftest.ts both call this.
 */
export function priceGate(unextracted: UnextractedPrice[]): { failed: boolean; lines: string[] } {
  if (unextracted.length === 0) return { failed: false, lines: [] };
  const lines = [
    `  VALIDATION FAILED: ${unextracted.length} row(s) carry a price in the FAO text that was not ` +
      `extracted. Written with price NULL and quarantined to fao_transactions_parse_errors:`,
    ...unextracted.slice(0, 25).map((u) => `    affaire ${u.affaire}: ${u.reason} — "${u.fragment}"`),
  ];
  if (unextracted.length > 25) lines.push(`    ... and ${unextracted.length - 25} more`);
  return { failed: true, lines };
}
