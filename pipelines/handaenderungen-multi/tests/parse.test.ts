/**
 * Deterministic regression tests for the Handänderungen parser core.
 * No network, no LLM — exercises regex parse / ownerless flag / normalize / ingest
 * against the committed fixtures. Run: npx tsx tests/parse.test.ts
 */
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { parseBlockRegex, parseSwissNumber, parseParties } from '../src/parse.js';
import { flagOwnerless, withOwnerlessFlag } from '../src/ownerless.js';
import { toBronzeRow, makeSourceId } from '../src/normalize.js';
import { ingest } from '../src/ingest.js';
import { schwyzAdapter } from '../src/adapters/schwyz.js';
import { lucerneAdapter } from '../src/adapters/lucerne.js';
import type { PublicationRef } from '../src/types.js';

const FIXTURES = join(dirname(fileURLToPath(import.meta.url)), '..', 'fixtures');
let pass = 0;
let fail = 0;
function eq(actual: unknown, expected: unknown, note: string) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a === e) {
    pass++;
  } else {
    fail++;
    console.error(`  ✗ ${note}\n      expected ${e}\n      actual   ${a}`);
  }
}
function ok(cond: boolean, note: string) {
  if (cond) pass++;
  else {
    fail++;
    console.error(`  ✗ ${note}`);
  }
}

// --- Swiss number ---
eq(parseSwissNumber("1'243"), 1243, "parseSwissNumber apostrophe");
eq(parseSwissNumber('46 053'), 46053, 'parseSwissNumber space');
eq(parseSwissNumber('n/a'), null, 'parseSwissNumber non-numeric');

// --- party parsing (multi-owner with ownership marker) ---
const p = parseParties("Fogarty Astrid, Schattdorf, und Reuther Bernd, GB-Cheltenham, ME zu je 1/2");
eq(p.length, 2, 'two seller parties');
eq(p[0].domicile, 'Schattdorf', 'seller1 domicile');
eq(p[1].domicile, 'GB-Cheltenham', 'seller2 domicile');

// --- full regex parse: SZ block 1 ---
const b1 = parseBlockRegex(
  "Fogarty Astrid, Schattdorf, und Reuther Bernd, GB-Cheltenham, ME zu je 1/2, an Garcia Rafael, Wilen bei Wollerau, Nr. 515, Am Freudenberg 8, Einfamilienhaus, Technikgebäude, Tiefgarage, 1'243 m2 Gesamtfläche",
);
eq(b1.parcel_number, '515', 'b1 parcel');
eq(b1.sellers.length, 2, 'b1 sellers');
eq(b1.buyers.length, 1, 'b1 buyers');
eq(b1.buyers[0].domicile, 'Wilen bei Wollerau', 'b1 buyer domicile');
eq(b1.surface_m2, 1243, 'b1 surface');
eq(b1.ownership_form, 'ME', 'b1 ownership form');
eq(b1.quote, '1/2', 'b1 quote');
eq(b1.address, 'Am Freudenberg 8', 'b1 address');
eq(b1.parse_confidence, 'high', 'b1 confidence');
ok(!flagOwnerless(b1), 'b1 not ownerless');

// --- StWE parse ---
const b2 = parseBlockRegex('Bianchi Marco, Freienbach, an Bianchi Elena, Freienbach, Nr. 2087, Seestrasse 44, StWE-WQ 85/1\'000 (Wohnung im 2. OG)');
eq(b2.parcel_number, '2087', 'b2 parcel');
eq(b2.ownership_form, 'StWE', 'b2 ownership StWE');
eq(b2.stwe_wq, "85/1'000", 'b2 wertquote');
eq(b2.address, 'Seestrasse 44', 'b2 address');

// --- ownerless flag (text markers + public-body party) ---
const b3 = parseBlockRegex('Herrenloses Grundstück (Dereliktion), an Genossame Wollerau, Nr. 998, Chatzenstrick, unproduktiv, 1\'020 m2');
ok(flagOwnerless(b3), 'b3 ownerless=true (herrenlos/Dereliktion + Genossame)');
eq(withOwnerlessFlag(b3).is_ownerless_event, true, 'b3 withOwnerlessFlag');

// --- normalize → bronze row ---
const pub: PublicationRef = {
  canton: 'SZ',
  source_organ: 'amtsblatt_sz',
  source_url: 'x',
  publication_date: '2026-03-21',
  issue: '12',
  grundbuchkreis: 'Höfe',
};
const row = toBronzeRow(withOwnerlessFlag(b1), pub);
eq(row.price, null, 'bronze price null by design');
eq(row.price_per_m2, null, 'bronze price_per_m2 null');
eq(row.canton, 'SZ', 'bronze canton stamp');
eq(row.source_file, 'amtsblatt_sz', 'bronze source_file = organ');
eq(row.surface_m2, 1243, 'bronze surface carried');
eq(row.nb_buyers, 1, 'bronze nb_buyers');
eq(row.nb_sellers, 2, 'bronze nb_sellers');
ok(typeof row.buyers === 'string' && row.buyers.includes('Wilen'), 'bronze buyers text');
eq((row.raw_data as Record<string, unknown>).ownership_form, 'ME', 'raw_data ownership_form');
eq((row.raw_data as Record<string, unknown>).is_ownerless_event, false, 'raw_data ownerless');
// source_id is stable + carries canton/date/parcel
const sid1 = makeSourceId(withOwnerlessFlag(b1), pub);
const sid2 = makeSourceId(withOwnerlessFlag(b1), pub);
eq(sid1, sid2, 'source_id deterministic');
ok(sid1.startsWith('SZ-2026-03-21-515-'), 'source_id shape');

// --- end-to-end ingest over fixtures (no LLM client) ---
const runAll = async () => {
  const sz = await ingest(schwyzAdapter, { fixtureDir: FIXTURES, client: null });
  ok(sz.stats.discovered === 4, `SZ discovered 4 (got ${sz.stats.discovered})`);
  ok(sz.rows.length >= 3, `SZ produced ≥3 rows (got ${sz.rows.length})`);
  ok(sz.stats.ownerless === 1, `SZ ownerless=1 (got ${sz.stats.ownerless})`);
  ok(sz.rows.every((r) => r.canton === 'SZ' && r.price === null), 'SZ rows canton+null price');

  const lu = await ingest(lucerneAdapter, { fixtureDir: FIXTURES, client: null });
  ok(lu.stats.discovered === 3, `LU discovered 3 (got ${lu.stats.discovered})`);
  ok(lu.rows.every((r) => r.canton === 'LU'), 'LU rows canton stamp');
  const geRow = lu.rows.find((r) => (r.raw_data as Record<string, unknown>).ownership_form === 'GE');
  ok(!!geRow, 'LU GE ownership parsed');

  console.log(`\n${pass} passed, ${fail} failed`);
  if (fail > 0) process.exit(1);
};
runAll();
