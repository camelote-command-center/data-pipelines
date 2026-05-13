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

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail > 0 ? 1 : 0);
