/**
 * Regression tests for buildings.ts — fixtures are the real ids of affaire 2026/8599/0,
 * the CHF 11.4M Petit-Saconnex PPE/COP sale the pipeline dropped on 2026-10-12.
 * Pure: no DB, no network.   Run with: npx tsx buildings.test.ts
 */
import { readFileSync } from 'node:fs';
import { isValidBuildingId, normalizeBuildingIds } from './buildings.js';

let pass = 0, fail = 0;
function eq(actual: unknown, expected: unknown, label: string) {
  if (JSON.stringify(actual) === JSON.stringify(expected)) { pass++; console.log(`  ✓ ${label}`); }
  else { fail++; console.error(`  ✗ ${label}\n      expected: ${JSON.stringify(expected)}\n      actual:   ${JSON.stringify(actual)}`); }
}

console.log('=== isValidBuildingId ===');
eq(isValidBuildingId('32/1953'), true, 'bare parcelle');
eq(isValidBuildingId('27/157-108'), true, 'PPE feuillet');
eq(isValidBuildingId('23/3009-1-1'), true, 'COP lot — rejected before 2026-10-12, which dropped the whole affaire');
eq(isValidBuildingId('23/3009-1-25'), true, 'COP lot, two-digit');
eq(isValidBuildingId(''), false, 'empty');
eq(isValidBuildingId('23'), false, 'no parcelle part');
eq(isValidBuildingId('23/3009-'), false, 'trailing dash');
eq(isValidBuildingId('23/3009-1-1-1'), false, 'three suffixes are not a shape the gazette uses');
eq(isValidBuildingId('23/3009 -1'), false, 'embedded space');
eq(isValidBuildingId('abc/def'), false, 'non-numeric');

console.log('\n=== normalizeBuildingIds ===');
eq(normalizeBuildingIds([{ 'Building ID': '27/157-108' }]),
   [{ 'Building ID': '27/157', 'Feuillet number': '108' }], 'PPE: feuillet split off');
eq(normalizeBuildingIds([{ 'Building ID': '23/3009-1-1' }]),
   [{ 'Building ID': '23/3009', 'Feuillet number': '1', 'COP lot number': '1' }],
   'COP: parcelle + PPE feuillet + COP lot, all three kept');
eq(normalizeBuildingIds([{ 'Building ID': '32/1953' }]),
   [{ 'Building ID': '32/1953' }], 'bare parcelle untouched');
eq(normalizeBuildingIds([{ 'Building ID': '23/3009-1-1', 'Feuillet number': '9' }]),
   [{ 'Building ID': '23/3009', 'Feuillet number': '1', 'COP lot number': '1' }],
   'the id wins over a feuillet the model guessed separately');
eq(normalizeBuildingIds([{ 'Building ID': 'garbage' }]),
   [{ 'Building ID': 'garbage' }], 'unparseable id left for the validator to reject');
eq(normalizeBuildingIds([]), [], 'no buildings');
eq(normalizeBuildingIds(undefined as never), undefined as never, 'undefined tolerated');

console.log('\n=== affaire 2026/8599/0: 13 COP shares + 20 PPE lots on parcelle 23/3009 ===');
const copIds = ['1', '4', '6', '8', '10', '11', '12', '14', '15', '16', '17', '20', '25']
  .map((n) => `23/3009-1-${n}`);
const ppeIds = ['6', '7', '12', '14', '15', '16', '18', '21', '23', '24', '25', '26', '29', '30',
  '32', '35', '37', '39', '40', '41'].map((n) => `23/3009-${n}`);
const all = [...copIds, ...ppeIds];
eq(all.length, 33, '33 lots in the affaire');
eq(all.filter((id) => !isValidBuildingId(id)), [], 'every id passes validation');

const cop = normalizeBuildingIds(copIds.map((id) => ({ 'Building ID': id })));
eq([...new Set(cop.map((b) => b['Building ID']))], ['23/3009'], 'all COP shares on one parcelle');
eq([...new Set(cop.map((b) => b['Feuillet number']))], ['1'], 'all COP shares of feuillet 1');
eq(cop.map((b) => b['COP lot number']).join(','), '1,4,6,8,10,11,12,14,15,16,17,20,25',
   'which share changed hands is preserved — 13 distinct lots, not 13 identical rows');

const ppe = normalizeBuildingIds(ppeIds.map((id) => ({ 'Building ID': id })));
eq(ppe.every((b) => b['Building ID'] === '23/3009' && !b['COP lot number']), true,
   'PPE lots keep their own feuillet and carry no COP lot');

console.log('\n=== output budget (static guard on fetch-transactions.ts) ===');
// 4096 truncated this affaire's JSON mid-string on every attempt, on both providers.
const src = readFileSync(new URL('./fetch-transactions.ts', import.meta.url), 'utf8');
const budgets = [...src.matchAll(/max_tokens:\s*(\d+)/g)].map((m) => Number(m[1]));
eq(budgets.length, 2, 'both providers declare a budget');
eq(budgets.every((n) => n >= 8192), true, `no provider is back under 8192 (found ${budgets.join(', ')})`);

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail > 0 ? 1 : 0);
