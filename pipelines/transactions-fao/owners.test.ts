/**
 * Regression tests for owners.ts — fixtures are real shapes from bronze_ch.fao_transactions.
 * Pure: no DB, no network.   Run with: npx tsx owners.test.ts
 */
import { normalizeOwners, makeLocalityMatcher, normalizePlace } from './owners.js';

let pass = 0, fail = 0;
function eq(actual: unknown, expected: unknown, label: string) {
  if (JSON.stringify(actual) === JSON.stringify(expected)) { pass++; console.log(`  ✓ ${label}`); }
  else { fail++; console.error(`  ✗ ${label}\n      expected: ${JSON.stringify(expected)}\n      actual:   ${JSON.stringify(actual)}`); }
}

// A slice of silver_ch.ref_communes normalized names (canonical + aliases).
const isLocality = makeLocalityMatcher([
  'chene-bougeries', 'geneve', 'geneva', 'vernier', 'bulle', 'meyrin', 'hauterive', 'corminboeuf', 'lausanne',
  'rue', 'port',   // real communes (FR, BE) that are also street words
]);

console.log('=== normalizePlace ===');
eq(normalizePlace('Chêne-Bougeries'), 'chene-bougeries', 'accents stripped');
eq(normalizePlace('à Bulle'), 'bulle', 'leading preposition removed');
eq(normalizePlace('Hauterive (FR)'), 'hauterive', 'trailing canton parenthesis removed');
eq(normalizePlace('Corminbœuf'), 'corminboeuf', 'œ ligature');

console.log('\n=== SPLIT: domicile glued into the name ===');
const r1 = normalizeOwners([{ name: 'AYOM SA, CHENE-BOUGERIES', city: null, date: '30-05-2024' }], 'old_owner_s', isLocality);
eq(r1.owners, [{ name: 'AYOM SA', city: 'CHENE-BOUGERIES', date: '30-05-2024', name_raw: 'AYOM SA, CHENE-BOUGERIES' }],
   '2025/7888/0 — "AYOM SA, CHENE-BOUGERIES" → AYOM SA + city');
eq(r1.flags, [], '  not flagged');
eq(r1.splits, 1, '  counted as one split');

const r2 = normalizeOwners([{ name: 'Blac Invest SA, à Bulle', city: '' }], 'new_owner_s', isLocality);
eq(r2.owners?.[0], { name: 'Blac Invest SA', city: 'Bulle', name_raw: 'Blac Invest SA, à Bulle' }, 'preposition dropped from the domicile');

const r3 = normalizeOwners([{ name: 'NIC SA, GENEVE', city: '5, RUE DU MONT-BLANC 1201 GENEVE' }], 'old_owner_s', isLocality);
eq(r3.owners?.[0], { name: 'NIC SA', city: '5, RUE DU MONT-BLANC 1201 GENEVE', name_raw: 'NIC SA, GENEVE' },
   'existing city (an address) is never overwritten, but the name is still cleaned');

const r4 = normalizeOwners([{ name: 'XXL Box Sàrl, à Hauterive (FR)', city: null }], 'new_owner_s', isLocality);
eq(r4.owners?.[0]?.name, 'XXL Box Sàrl', 'canton parenthesis in the domicile');

console.log('\n=== NO SPLIT: commas that are part of the name ===');
eq(normalizeOwners([{ name: 'IMMO X SA, en liquidation', city: null }], 'old_owner_s', isLocality).owners?.[0]?.name,
   'IMMO X SA, en liquidation', 'state tail "en liquidation" kept');
eq(normalizeOwners([{ name: 'Rhône Invest, société anonyme', city: null }], 'old_owner_s', isLocality).owners?.[0]?.name,
   'Rhône Invest, société anonyme', 'legal-form tail "société anonyme" kept');
eq(normalizeOwners([{ name: 'DUPONT Jean', city: 'Genève' }], 'new_owner_s', isLocality).splits, 0, 'clean owner untouched');

console.log('\n=== NO SPLIT: street word that is also a commune (corpus false positive) ===');
const s1 = normalizeOwners([{ name: 'PAYCHERE Cédric,rue', city: null }], 'new_owner_s', isLocality);
eq(s1.owners?.[0], { name: 'PAYCHERE Cédric,rue', city: null },
   'address cut across fields ("…,rue") — city must NOT become "rue"');
eq(s1.flags, [], '  and not flagged as a bare locality');
eq(normalizeOwners([{ name: 'X Holding SA, Port', city: null }], 'new_owner_s', isLocality).splits, 0,
   '"Port" (BE commune / street word) is never treated as a domicile');

console.log('\n=== FLAG, never drop or guess: bare locality as a party ===');
const f1 = normalizeOwners(
  [{ name: 'AYOM SA', city: null }, { name: 'CHENE-BOUGERIES', city: null }], 'old_owner_s', isLocality);
eq(f1.owners?.length, 2, 'phantom NOT dropped');
eq(f1.flags, [{ side: 'old_owner_s', index: 1, reason: 'party_bare_locality', name: 'CHENE-BOUGERIES', city: null }],
   '2025/7887/0 — phantom "CHENE-BOUGERIES" flagged at index 1');

const f2 = normalizeOwners([{ name: 'Vernier', city: 'ROSSI Charles' }], 'new_owner_s', isLocality);
eq(f2.flags.length, 1, 'name/city swap (name=Vernier, city=ROSSI Charles) flagged');
eq(f2.owners?.[0], { name: 'Vernier', city: 'ROSSI Charles' }, '  and left untouched for a human');

eq(normalizeOwners([{ name: 'Genève', city: null }], 'new_owner_s', isLocality).flags.length, 1,
   'public body truncated to its locality ("Genève") flagged, not guessed');
eq(normalizeOwners([{ name: 'Ville de Genève', city: null }], 'new_owner_s', isLocality).flags.length, 0,
   'the full public-body name is NOT a bare locality');

console.log('\n=== robustness ===');
eq(normalizeOwners(null, 'old_owner_s', isLocality), { owners: null, flags: [], splits: 0 }, 'null owners');
eq(normalizeOwners([{ name: null }], 'old_owner_s', isLocality).flags, [], 'owner without a name');

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail > 0 ? 1 : 0);
