/**
 * Negative control for the FAO price fail-loud contract.
 *
 * Feeds deliberately malformed / unextractable prices through the SAME
 * validateParsedPrice() + priceGate() that fetch-transactions.ts uses, and
 * exits exactly as the parser would. With the default fixture it MUST exit 1.
 *
 * CI runs it inverted: the step fails if this exits 0, i.e. if a malformed
 * price ever stops turning the run red.
 *
 *   npx tsx price-gate.selftest.ts            → exit 1 (red, expected)
 *   npx tsx price-gate.selftest.ts --clean    → exit 0 (control: valid prices stay green)
 */
import { validateParsedPrice, priceGate, type UnextractedPrice } from './price.js';

const MALFORMED: Array<[string, string, string | null]> = [
  // [affaire, raw gazette text, what the LLM returned]
  ['SELFTEST/1/0', "Achat. Prix: 100'00.-; Ancien(s): X SA, Genève.", '10000'],        // ambiguous grouping
  ['SELFTEST/2/0', "Prix total de l'affaire: 820'00.-. Achat.", '82000'],              // malformed total
  ['SELFTEST/3/0', "Prix de vente: Frs 1.250.000.-. Achat.", null],                     // unrecognised shape
];
const CLEAN: Array<[string, string, string | null]> = [
  ['SELFTEST/4/0', "Prix total de l'affaire: 1'080'840.- (frais annexes). Achat.", null],
  ['SELFTEST/5/0', "Prix: 14'800'000.-; lot a. Prix: 14'125'000.-; lot b.", null],
  ['SELFTEST/6/0', "Donation. Ancien(s): X. Nouveau(x): Y.", null],
];

const fixtures = process.argv.includes('--clean') ? CLEAN : MALFORMED;
const unextracted: UnextractedPrice[] = [];

for (const [affaire, text, llm] of fixtures) {
  const v = validateParsedPrice(text, llm);
  if (v.ok && v.unextracted) unextracted.push({ affaire, ...v.unextracted });
  console.log(`  ${affaire}: price=${v.ok ? v.price : 'REJECTED'}${v.ok && v.unextracted ? ` UNEXTRACTED(${v.unextracted.reason})` : ''}`);
}

const gate = priceGate(unextracted);
for (const line of gate.lines) console.error(line);
console.log(gate.failed ? '  → run would go RED' : '  → run would stay green');
process.exit(gate.failed ? 1 : 0);
