/**
 * FAO LDTR — non-price vocabulary classifier.
 *
 * Every case below is a real value observed in bronze_ch.fao_ldtr or
 * safe_cast.quarantine between 2026-06-07 and 2026-08-17.
 */
import { classifyNonPrice, extractLotPrice } from './price-classifier.js';

let failures = 0;
function check(raw: string, expected: string | null) {
  const got = classifyNonPrice(raw);
  const ok = got === expected;
  if (!ok) failures++;
  console.log(`  ${ok ? '✓' : '✗'} ${JSON.stringify(raw).padEnd(34)} → ${String(got).padEnd(20)} (expected ${expected})`);
}

console.log('\n  Non-price transfer types (must classify, must NOT quarantine):');
check('donation', 'donation');
check('Donation dactions', 'donation');
check("Donation d'actions", 'donation');
check('Succession', 'succession');
check('Succession sans soulte', 'succession');
check('Succession sans soultes', 'succession');
check('liquidation dune succession', 'succession');
check("liquidation d'une succession", 'succession');
check('Délivrance de legs', 'delivrance_de_legs');
check('Delivrance de legs', 'delivrance_de_legs');

console.log('\n  Prices (must NOT classify — they cast normally):');
check('0', null);
check('330908', null);
check('2090000', null);
check('575000.- CHF', null);
check('1’180000.-', null);

console.log('\n  Lot labels must NOT be classified as non-price (classifyNonPrice stays null):');
check('lot n° 2.01 : 1100000', null);
check('lot n° 2.02 : 629000', null);
check('lot n° 5.01 : 1090000', null);

console.log('\n  Unknown non-numeric shapes must still quarantine:');
check('valeur inconnue', null);
check('à déterminer', null);

// ---------------------------------------------------------------------------
// extractLotPrice — the other half. Kept separate on purpose: widening one rule
// must not silently eat the other case, so every value is asserted against BOTH
// functions.
// ---------------------------------------------------------------------------
function checkLot(raw: string, expected: string | null) {
  const got = extractLotPrice(raw);
  const ok = got === expected;
  if (!ok) failures++;
  console.log(`  ${ok ? '✓' : '✗'} ${JSON.stringify(raw).padEnd(34)} → ${String(got).padEnd(20)} (expected ${expected})`);
}

console.log('\n  Lot-labelled prices (real observed values — must extract the figure):');
checkLot('lot n° 2.01 : 1100000', '1100000');
checkLot('lot n° 2.02 : 629000', '629000');
checkLot('lot n° 5.01 : 1090000', '1090000');

console.log('\n  Lot shape tolerances:');
checkLot('lot n 5.01 : 1090000', '1090000');      // ° dropped by an upstream strip
checkLot('LOT N° 2.01 : 1100000', '1100000');     // case
checkLot('lot  n°  2.01  :  1100000', '1100000'); // collapsed whitespace
checkLot('lot n° 12 : 450000', '450000');         // lot number without a decimal

console.log('\n  extractLotPrice must NOT fire on anything else:');
checkLot('1100000', null);            // bare price needs no extraction
checkLot('0', null);
checkLot('donation', null);           // non-price term belongs to classifyNonPrice
checkLot('Succession sans soulte', null);
checkLot('575000.- CHF', null);       // safe_cast.to_numeric already handles this
checkLot('lot n° 2.01 : donation', null);     // no figure -> must still quarantine
checkLot('lot n° 2.01 : 1100000 | lot n° 2.02 : 629000', null); // genuine multi-lot -> quarantine, do not guess
checkLot('lots 2.01 et 2.02 : 1729000', null);  // unknown variant -> must stay visible

console.log('\n  The two rules must never overlap:');
for (const v of ['donation', 'Succession', 'Délivrance de legs']) {
  checkLot(v, null);
}
for (const v of ['lot n° 2.01 : 1100000', 'lot n° 5.01 : 1090000']) {
  check(v, null);
}

console.log(failures === 0 ? '\n  ALL PASS\n' : `\n  ${failures} FAILURE(S)\n`);
process.exit(failures === 0 ? 0 : 1);
