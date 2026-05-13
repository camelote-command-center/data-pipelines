/**
 * Swiss price-extraction helpers for the FAO transactions parser.
 *
 * Two bugs in production drove the redesign:
 *   1. The previous cleaner stripped ALL non-digits including '.', so
 *      "40'032'944.47" became "4003294447" (×100 of the real value).
 *   2. Claude Sonnet sometimes duplicates a digit-group while transcribing
 *      gazette text, e.g. "1'875'000" → "1'875'000'000" (×1000 hallucination).
 *
 * Defence in depth:
 *   - cleanSwissPrice(): correct Swiss-format → numeric string conversion.
 *   - extractPriceFromRaw(): regex-based extraction from untouched gazette text;
 *     ALWAYS prefer this over any LLM-supplied price.
 *   - validateParsedPrice(): post-parse guard that rejects rows where the
 *     LLM disagrees with the raw text or returns an implausible figure.
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
  s = s.replace(/^(CHF|Fr\.?)\s*/i, '');
  // Strip the placeholder ".-" / ".--" / ".---" trailing format (no cents)
  s = s.replace(/\.-+\s*$/, '');
  // Strip Swiss apostrophes (ASCII ' and Unicode ' U+2019) used as thousand sep
  s = s.replace(/['’]/g, '');
  s = s.trim();

  // Must be plain integer or integer.decimal — reject anything else
  if (!/^\d+(\.\d+)?$/.test(s)) return null;
  return s;
}

/**
 * Extract the "Prix total de l'affaire" price from the raw gazette text via regex.
 * This bypasses the LLM entirely — it's the authoritative source of the price.
 */
const PRICE_RE = new RegExp(
  // "Prix total de l'affaire"  (ASCII apostrophe or curly U+2019)
  "Prix\\s+total\\s+de\\s+l['’]affaire" +
  "\\s*:?\\s*" +
  // Optional currency prefix
  "(?:CHF|Fr\\.?)?\\s*" +
  // The number — digits with apostrophes (ASCII or U+2019) + optional .NN
  "(?<price>[\\d'’]+(?:\\.\\d+)?)" +
  // Optional trailing .- / .--
  "\\s*\\.?-{0,3}",
  'i',
);

export function extractPriceFromRaw(rawText: string | null | undefined): string | null {
  if (!rawText) return null;
  const m = rawText.match(PRICE_RE);
  if (!m || !m.groups?.price) return null;
  return cleanSwissPrice(m.groups.price);
}

export type ValidationFailure =
  | { ok: false; reason: 'implausible_price'; parsedPrice: string };

export type ValidationOk = {
  ok: true;
  price: string | null;
  source: 'regex' | 'llm_fallback' | 'none';
  warning?: string;  // non-fatal — e.g. LLM disagreed wildly with regex
};

/**
 * Final guard before insert. Regex over the raw gazette text is always
 * authoritative — the LLM is a fallback only when the regex misses.
 *
 * Strategy:
 *   1. If the regex finds a price in raw text → USE IT, ignore the LLM entirely.
 *      (This is the whole point of Bug 1's fix: the LLM reliably mangles prices.)
 *   2. If regex finds nothing → fall back to the LLM-cleaned price (log a warning).
 *   3. Refuse rows with > 2B CHF (implausible — almost certainly a parser/LLM bug).
 *   4. If regex disagrees with LLM by > 0.5 CHF → record a non-fatal warning so we
 *      can audit how often the LLM is wrong. Row still inserts with the regex value.
 */
export function validateParsedPrice(
  rawText: string | null | undefined,
  llmPrice: string | null,
): ValidationOk | ValidationFailure {
  const regexPrice = extractPriceFromRaw(rawText);

  let source: 'regex' | 'llm_fallback' | 'none';
  let finalPrice: string | null;
  let warning: string | undefined;

  if (regexPrice !== null) {
    finalPrice = regexPrice;
    source = 'regex';
    if (llmPrice && llmPrice !== regexPrice) {
      const diff = Math.abs(parseFloat(regexPrice) - parseFloat(llmPrice));
      if (diff > PRICE_MISMATCH_TOLERANCE) {
        warning = `LLM disagreed with regex (LLM=${llmPrice}, regex=${regexPrice}). Took regex.`;
      }
    }
  } else if (llmPrice !== null) {
    finalPrice = llmPrice;
    source = 'llm_fallback';
    warning = 'Regex missed; using LLM price as fallback. Audit periodically.';
  } else {
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
