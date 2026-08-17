/**
 * FAO LDTR — non-price vocabulary classifier.
 *
 * Every case below is a real value observed in bronze_ch.fao_ldtr or
 * safe_cast.quarantine between 2026-06-07 and 2026-08-17.
 */
import { classifyNonPrice } from './price-classifier.js';

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

console.log('\n  Deliberately left to quarantine (domain parsing, separate work):');
check('lot n° 2.01 : 1100000', null);
check('lot n° 2.02 : 629000', null);
check('lot n° 5.01 : 1090000', null);

console.log('\n  Unknown non-numeric shapes must still quarantine:');
check('valeur inconnue', null);
check('à déterminer', null);

console.log(failures === 0 ? '\n  ALL PASS\n' : `\n  ${failures} FAILURE(S)\n`);
process.exit(failures === 0 ? 0 : 1);
