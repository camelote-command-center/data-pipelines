/**
 * Regression tests for price.ts — covers Bug 1 (×100 decimal-strip)
 * and Bug 2 (×1000 LLM hallucination).
 *
 * Run with: npx tsx pipelines/transactions-fao/price.test.ts
 */

import {
  cleanSwissPrice,
  extractPriceFromRaw,
  validateParsedPrice,
  isValidSwissAmount,
  hasPriceMarker,
  priceGate,
} from './price.js';

let pass = 0;
let fail = 0;

function eq(actual: unknown, expected: unknown, label: string) {
  if (actual === expected || JSON.stringify(actual) === JSON.stringify(expected)) {
    pass++;
    console.log(`  ✓ ${label}`);
  } else {
    fail++;
    console.error(`  ✗ ${label}\n      expected: ${JSON.stringify(expected)}\n      actual:   ${JSON.stringify(actual)}`);
  }
}

console.log('=== cleanSwissPrice ===');
eq(cleanSwissPrice("40'032'944.47"),     '40032944.47',  'preserves decimals (Bug 1 fix)');
eq(cleanSwissPrice("112'469'616.98"),    '112469616.98', 'preserves decimals (Bug 1 fix #2)');
eq(cleanSwissPrice("36'315'735.30"),     '36315735.30',  'preserves decimals (Bug 1 fix #3 — newly found)');
eq(cleanSwissPrice("1'875'000.-"),       '1875000',      "strips trailing .-");
eq(cleanSwissPrice("CHF 1'200'000.--"),  '1200000',      "strips CHF + trailing .--");
eq(cleanSwissPrice("Fr. 302'000'000.--"),'302000000',    "strips Fr. + trailing .--");
eq(cleanSwissPrice("710'000"),           '710000',       'integer with apostrophes');
eq(cleanSwissPrice("710’000"),           '710000',       'Unicode apostrophe U+2019');
eq(cleanSwissPrice(null),                null,           'null in → null out');
eq(cleanSwissPrice(undefined),           null,           'undefined in → null out');
eq(cleanSwissPrice("—"),                 null,           'em-dash → null');
eq(cleanSwissPrice("n/a"),               null,           'non-numeric → null');
eq(cleanSwissPrice(""),                  null,           'empty string → null');
eq(cleanSwissPrice("  "),                null,           'whitespace → null');
eq(cleanSwissPrice("1'875"),             '1875',         'small integer');
eq(cleanSwissPrice(1875000),             '1875000',      'numeric input');

console.log('\n=== extractPriceFromRaw — Bug 2 regression (Pittard case) ===');
const PITTARD_RAW =
  "19.02.2026 - Genève-Eaux-Vives, 22 - Affaire 2026/1657/0 - " +
  "Prix total de l'affaire: 1'875'000.-. Achat. Ancien(s): " +
  "GERARD-ZAMMIT Catherine, Troistorrents, inscrit dès le 15.07.1999. " +
  "Nouveau(x): MEYER Olivier, Genève, PPE Genève-Eaux-Vives, " +
  "22/2384-3 sur 22/1000, 3.03 appartement, loggia - local annexe: " +
  "2.23 cave, Avenue Eugène-PITTARD 5, 1206 Genève.";
eq(extractPriceFromRaw(PITTARD_RAW), '1875000', 'Pittard case — raw extraction (must be 1875000, not 1875000000)');

console.log('\n=== extractPriceFromRaw — other formats ===');
eq(extractPriceFromRaw("Prix total de l'affaire: 40'032'944.47 (frais annexes)"), '40032944.47', 'with decimal + (frais annexes) suffix');
eq(extractPriceFromRaw("Prix total de l'affaire 535'000'000.00."),                '535000000.00', 'without colon');
eq(extractPriceFromRaw("Prix total de l'affaire: Fr. 302'000'000.--."),           '302000000',  'with Fr. prefix');
eq(extractPriceFromRaw("Prix total de l’affaire: 710'000.-"),                    '710000',     'with Unicode apostrophe in "l’affaire"');
eq(extractPriceFromRaw("nothing matches here"),                                   null,          'no match → null');
eq(extractPriceFromRaw(null),                                                     null,          'null → null');

console.log('\n=== validateParsedPrice — guards ===');
// Bug 1: cleaner produced 4003294447, regex says 40032944.47. Regex wins.
const v1 = validateParsedPrice("Prix total de l'affaire: 40'032'944.47 (frais annexes)", '4003294447');
eq(v1.ok, true, 'Bug 1: regex wins, row passes through');
if (v1.ok) {
  eq(v1.price, '40032944.47', '  price = 40032944.47');
  eq(v1.source, 'regex', '  source = regex');
  eq(typeof v1.warning === 'string', true, '  warning emitted (LLM disagreed)');
}

// Bug 2: regex says 1875000, LLM hallucination says 1875000000. Regex wins.
const v2 = validateParsedPrice(PITTARD_RAW, '1875000000');
eq(v2.ok, true, 'Bug 2: regex wins, row passes through');
if (v2.ok) {
  eq(v2.price, '1875000', '  price = 1875000 (LLM hallucination overridden)');
  eq(v2.source, 'regex', '  source = regex');
  eq(typeof v2.warning === 'string', true, '  warning emitted');
}

// Implausibility floor when regex misses
const v3 = validateParsedPrice("garbage", '99999999999');
eq(v3.ok, false, 'Implausible (>2B) → rejected for quarantine');
if (!v3.ok) eq(v3.reason, 'implausible_price', '  reason = implausible_price');

// Both null — pass through
const v4 = validateParsedPrice("no price here", null);
eq(v4.ok, true, 'Both null → valid (price=null)');
if (v4.ok) {
  eq(v4.price, null, '  price = null');
  eq(v4.source, 'none', '  source = none');
}

// LLM-only fallback when regex misses
const v5 = validateParsedPrice("weird phrasing without standard prefix", '1500000');
eq(v5.ok, true, 'LLM fallback when regex misses');
if (v5.ok) {
  eq(v5.price, '1500000', '  price = 1500000');
  eq(v5.source, 'llm_fallback', '  source = llm_fallback');
}

// Regex and LLM agree: no warning
const v6 = validateParsedPrice("Prix total de l'affaire: 1'875'000.-", '1875000');
eq(v6.ok, true, 'Regex == LLM (happy path)');
if (v6.ok) {
  eq(v6.price, '1875000', '  price = 1875000');
  eq(v6.warning, undefined, '  no warning');
}

// ─────────────────────────────────────────────────────────────────────────────
// 2026-09-13 — prices visible in the text but written NULL (fail-loud contract)
// Fixtures are the real shapes found in bronze_ch.fao_transactions.
// ─────────────────────────────────────────────────────────────────────────────

console.log('\n=== extractPrice — the 2026/1794/0 case (brief) ===');
const AYOM_RAW =
  "24.02.2026 - Thônex, 43 - Affaire 2026/1794/0 - Prix total de l'affaire: 1'080'840.- " +
  "(frais annexes). Achat. Ancien(s): AYOM SA, CHENE-BOUGERIES, inscrit le 30.05.2024. " +
  "Nouveau(x): COHEN Sarah, Genève, COHEN Ilan, Genève, cop. 1/2 chacun, PPE Thônex.";
eq(extractPriceFromRaw(AYOM_RAW), '1080840', '2026/1794/0 → 1080840 (was NULL in bronze)');
const vAyomNoLlm = validateParsedPrice(AYOM_RAW, null);
eq(vAyomNoLlm.ok && vAyomNoLlm.price, '1080840', '  deterministic even when the LLM returns nothing');

console.log('\n=== extractPrice — short-form multi-lot "Prix:" parts (live since 2026-05-13) ===');
const MULTI_LOT_RAW =
  "17.07.2026 - Meyrin - Affaire 2026/7033/0 - Achat. Prix: 14'800'000.-; B-F Meyrin, 33/11990, " +
  "264 m2. Prix: 14'125'000.-; B-F Meyrin, 33/11991. Ancien(s): X SA, Genève.";
eq(extractPriceFromRaw(MULTI_LOT_RAW), '28925000', '2026/7033/0 → sum of lots 28925000 (was NULL)');
const vMulti = validateParsedPrice(MULTI_LOT_RAW, null);
eq(vMulti.ok && vMulti.price, '28925000', '  written without the LLM');
eq(vMulti.ok && vMulti.unextracted, undefined, '  not flagged as unextracted');

eq(extractPriceFromRaw("Prix: 106'130.15; a. Prix: 224'677.85; b. Prix: 22'609.-; c. Prix: 259'269.-; d. Prix: 180'026.35."),
   '792712.35', 'decimal parts summed exactly (no float drift) — 2026/2908/0');
eq(extractPriceFromRaw("Prix: 4'578’955.-; a. Prix: 37'011'545.-; b. Prix: 384'500.-."),
   '41975000', 'mixed ASCII/U+2019 apostrophes in one entry — 2026/1026/0');
eq(extractPriceFromRaw("Prix: 950'000.-; B-F Plan-les-Ouates."), '950000', 'single short-form part');
eq(extractPriceFromRaw("Prix total de l'affaire: 3'000'000.-. Prix: 1'000'000.-; Prix: 2'000'000.-."),
   '3000000', 'explicit total wins over its parts');

console.log('\n=== isValidSwissAmount — ambiguity is refused, not guessed ===');
eq(isValidSwissAmount("1'080'840.-"),   true,  "1'080'840.-");
eq(isValidSwissAmount("106'130.15"),    true,  "106'130.15");
eq(isValidSwissAmount("710000"),        true,  'unseparated integer');
eq(isValidSwissAmount("100'00.-"),      false, "100'00 — 10'000 or 100'000? (2026/9282/0)");
eq(isValidSwissAmount("820'00"),        false, "820'00 — malformed total in corpus");
eq(isValidSwissAmount("1'500''000.00"), false, 'double apostrophe — malformed total in corpus');
eq(isValidSwissAmount("1.250.000"),     false, 'dot thousands');
eq(isValidSwissAmount("1.250"),         false, '1.250 — CHF 1.25 or 1\'250?');

console.log('\n=== validateParsedPrice — loud failure paths ===');
const vMal = validateParsedPrice("Nouveau(x): X. Prix: 100'00.-; Ancien(s): Y.", '10000');
eq(vMal.ok, true, 'malformed source amount: row still written');
if (vMal.ok) {
  eq(vMal.price, null, '  price NULL — the LLM\'s guess 10000 is NOT accepted');
  eq(vMal.unextracted?.reason, 'price_malformed_in_source', '  flagged price_malformed_in_source');
}
const vMalTotal = validateParsedPrice("Prix total de l'affaire: 820'00.-. Achat.", '82000');
eq(vMalTotal.ok && vMalTotal.unextracted?.reason, 'price_malformed_in_source', 'malformed TOTAL flagged too');

const vUnknown = validateParsedPrice("Prix de vente: Frs 1.250.000.-. Achat.", null);
eq(vUnknown.ok && vUnknown.price, null, 'unrecognised shape + no LLM value: price NULL');
eq(vUnknown.ok && vUnknown.unextracted?.reason, 'price_in_text_not_extracted', '  flagged price_in_text_not_extracted');

const vNoMarker = validateParsedPrice("Donation. Ancien(s): X. Nouveau(x): Y.", null);
eq(vNoMarker.ok && vNoMarker.unextracted, undefined, 'no price in the text: NOT flagged (no false alarm)');
eq(hasPriceMarker("Prix: voir acte"), false, '"Prix" without digits is not a price marker');

console.log('\n=== priceGate — the run-level decision the parser uses ===');
eq(priceGate([]).failed, false, 'nothing unextracted → run stays green');
const g = priceGate([{ affaire: '2026/9282/0', reason: 'price_malformed_in_source', fragment: "100'00." }]);
eq(g.failed, true, 'one unextracted price → run goes RED');
eq(g.lines.some((l) => l.includes('2026/9282/0')), true, '  red output names the affaire');

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail > 0 ? 1 : 0);
