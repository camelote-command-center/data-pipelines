/**
 * Regression tests for ppe.ts — fixtures are the property part of real FAO notices (no party names).
 * Pure: no DB, no network.   Run with: npx tsx ppe.test.ts
 */
import { decidePpe, hasPpeEvidence } from './ppe.js';

let pass = 0, fail = 0;
function eq(actual: unknown, expected: unknown, label: string) {
  if (JSON.stringify(actual) === JSON.stringify(expected)) { pass++; console.log(`  ✓ ${label}`); }
  else { fail++; console.error(`  ✗ ${label}\n      expected: ${JSON.stringify(expected)}\n      actual:   ${JSON.stringify(actual)}`); }
}

console.log('=== genuine PPE ===');
eq(hasPpeEvidence('PPE Meyrin, 33/15242-118 sur 49/1000, 4.09 partie appartement, balcon'), true, 'PPE word + thousandths (2026/9340/0)');
eq(hasPpeEvidence('B-F Chêne-Bourg, 13/4717-118 sur 37.1/1000, 4.06 appartement, balcon'), true, 'thousandths without the word PPE (2026/9797/0)');
eq(hasPpeEvidence('PPE Chêne-Bougeries, 12/1992-19 sur 2,1/1000, 1.43 box'), true, 'parking box in PPE (2026/10178/0)');
eq(hasPpeEvidence("COP Hermance, 28/681-3-cop sur 9/10 de PPE Hermance, 28/681-3 sur 497/1000, 3.01 studio"), true, 'COP share of a PPE lot (2026/10523/0)');
eq(hasPpeEvidence("lot sur 125/1'000"), true, "thousandths written 1'000");

console.log('=== not PPE ===');
eq(hasPpeEvidence('B-F Cologny, 17/986, 1368 m2, Garage privé, Habitation à un seul logement, Chemin BOISSIER 27C'), false, 'single-family house (2026/10757/0)');
eq(hasPpeEvidence('COP Hermance, 28/1881-2 sur 1/2 du B-F Hermance, 28/1881, 154 m2, Habitation à un seul logement'), false, 'ordinary co-ownership with a lot suffix');
eq(hasPpeEvidence('COP Genève-Eaux-Vives, 22/2563-20 sur 62,215/100000 du B-F Genève-Eaux-Vives'), false, 'share in 100000ths is not thousandths');
eq(hasPpeEvidence('DDP Bellevue, 6/3590, 1769 m2, Hangar (sur plusieurs immeubles).'), false, 'building right');
eq(hasPpeEvidence('B-F Vernier, 12/345, 4964 m2, Bureaux; pippe'), false, 'PPE only as a whole word');
eq(hasPpeEvidence(null), false, 'no text');

console.log('=== decidePpe ===');
eq(decidePpe('B-F Cologny, 17/986, Habitation à un seul logement', 'Yes', '1'), { ppe_indicator: 'No', cumulative_pourmille_ppe: null, overridden: true }, 'model said Yes without evidence → No, per-mille dropped');
eq(decidePpe('PPE Lancy, 31/5319-111 sur 42/1000, 5.03 appartement', 'Yes', '42'), { ppe_indicator: 'Yes', cumulative_pourmille_ppe: '42', overridden: false }, 'genuine PPE kept');
eq(decidePpe('PPE Onex, 34/350-101 sur 211/1000', '', ''), { ppe_indicator: 'Yes', cumulative_pourmille_ppe: null, overridden: true }, 'model missed it → Yes');
eq(decidePpe('B-F Thônex, 43/6895, partie habitation un logement', 'No', ''), { ppe_indicator: 'No', cumulative_pourmille_ppe: null, overridden: false }, 'agrees on No');

console.log(`\n${pass} passed, ${fail} failed`);
if (fail) process.exit(1);
