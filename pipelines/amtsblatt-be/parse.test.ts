/**
 * Fixture regression tests for the deterministic succession-notice parser.
 * No network / no DB — parses saved XML fixtures and asserts extracted fields.
 *
 * Run: npx tsx pipelines/amtsblatt-be/parse.test.ts
 */
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { parseNoticeXml, mapEventType, buildDedupeKey, norm, repudiationScope } from './lib/parse.js';

const HERE = dirname(fileURLToPath(import.meta.url));
const FX = join(HERE, 'fixtures');

let pass = 0;
let fail = 0;
function eq(actual: unknown, expected: unknown, note: string) {
  if (actual === expected) { pass++; return; }
  fail++;
  console.error(`  ✗ ${note}\n      expected: ${JSON.stringify(expected)}\n      actual:   ${JSON.stringify(actual)}`);
}
function truthy(actual: unknown, note: string) {
  if (actual) { pass++; return; }
  fail++;
  console.error(`  ✗ ${note} — expected truthy, got ${JSON.stringify(actual)}`);
}

function parse(file: string, tenant: string, canton: string) {
  return parseNoticeXml({ publicationId: file, tenant, canton, xml: readFileSync(join(FX, `${file}.xml`), 'utf-8') });
}

// --- TE-BE20 Erbenruf (FR) ---------------------------------------------------
{
  const n = parse('te-be20_erbenruf_fr', 'kabbe', 'BE');
  eq(n.eventType, 'erbenruf', 'te-be20 fr → erbenruf');
  eq(n.deceasedSurname, 'Dürst', 'te-be20 fr surname');
  eq(n.deceasedPrename, 'Henri Bernard', 'te-be20 fr prename');
  eq(n.deceasedDob, '1938-01-28', 'te-be20 fr dob');
  eq(n.deceasedHeimatort, 'Glarus Nord /GL', 'te-be20 fr heimatort');
  eq(n.deadlineDate, '2027-03-25', 'te-be20 fr deadline');
  eq(n.authority, 'Commune de La Neuveville', 'te-be20 fr authority');
  eq(n.language, 'fr', 'te-be20 fr language');
  truthy(n.deceasedLastDomicile?.includes('Courtelary'), 'te-be20 fr domicile has Courtelary');
  truthy(n.bodyText?.includes('héritiers'), 'te-be20 fr body text stripped');
  truthy(!n.bodyText?.includes('<p>'), 'te-be20 fr body html removed');
}

// --- TE-BE20 Erbenruf (DE), one of the 3× publications -----------------------
{
  const n = parse('te-be20_erbenruf_de', 'kabbe', 'BE');
  eq(n.eventType, 'erbenruf', 'te-be20 de → erbenruf');
  eq(n.deceasedSurname, 'Murri', 'te-be20 de surname');
  truthy(n.deceasedName?.includes('Helene'), 'te-be20 de full name');
}

// --- TE-BE10 Testamentseröffnung → testament ---------------------------------
{
  const n = parse('te-be10_testament_de', 'kabbe', 'BE');
  eq(n.eventType, 'testament', 'te-be10 → testament');
  eq(n.deceasedSurname, 'Schmutz', 'te-be10 surname');
  eq(n.deceasedDod, '2026-06-10', 'te-be10 date of death');
  truthy(n.bodyText?.includes('Art. 558'), 'te-be10 probate body');
}

// --- TE-BE70 öffentliches Inventar → inventar (uses dateOfDetection) ----------
{
  const n = parse('te-be70_inventar_de', 'kabbe', 'BE');
  eq(n.eventType, 'inventar', 'te-be70 → inventar');
  eq(n.deceasedSurname, 'Rüegger', 'te-be70 surname');
  eq(n.deceasedDod, '2025-09-17', 'te-be70 dod from dateOfDetection');
  eq(n.deadlineDate, '2026-07-03', 'te-be70 deadline');
}

// --- TE-BE90 Erbschaft an Gemeinwesen → escheat ------------------------------
{
  const n = parse('te-be90_gemeinwesen_de', 'kabbe', 'BE');
  eq(n.eventType, 'escheat', 'te-be90 → escheat');
  eq(n.deceasedSurname, 'Christen', 'te-be90 surname');
  truthy(n.deceasedLastDomicile?.includes('Ringgenberg'), 'te-be90 domicile');
}

// --- KK02 ausgeschlagene Erbschaft (DE) → ausschlagung -----------------------
{
  const n = parse('kk02_ausgeschlagen_de', 'shab', 'BE');
  eq(n.eventType, 'ausschlagung', 'kk02 → ausschlagung');
  eq(n.addition, 'refusedLegacy', 'kk02 addition flag');
  eq(n.deceasedSurname, 'Jakob', 'kk02 surname (debtor.person)');
  eq(n.deceasedDob, '1953-01-02', 'kk02 dob');
  eq(n.deceasedHeimatort, 'Trub BE', 'kk02 heimatort');
  truthy(n.authority?.includes('Konkursamt'), 'kk02 authority = Konkursamt');
  eq(n.deadlineDate, '2026-08-15', 'kk02 deadline');
}

// --- KK repudiated (FR) → ausschlagung ---------------------------------------
{
  const n = parse('kk_repudiee_fr', 'shab', 'BE');
  eq(n.eventType, 'ausschlagung', 'kk fr → ausschlagung');
  eq(n.addition, 'refusedLegacy', 'kk fr addition flag');
  eq(n.deceasedSurname, 'von Allmen', 'kk fr surname');
  truthy(n.authority?.toLowerCase().includes('faillite'), 'kk fr authority = Office des faillites');
}

// --- pure unit: event-type mapping & dedupe key ------------------------------
{
  eq(mapEventType('TE-ZH20', null), 'erbenruf', 'mapEventType generalizes to ZH (canton-parameterized)');
  eq(mapEventType('KK01', 'refusedLegacy'), 'ausschlagung', 'KK+refusedLegacy → ausschlagung');
  eq(mapEventType('KK01', null), 'liquidation', 'KK w/o refusedLegacy → liquidation');
  eq(norm('Dürst'), 'durst', 'norm strips umlaut diacritic');
  // estate-identity key: name + dob + last_domicile; stable across casing/diacritics
  const k1 = buildDedupeKey({ canton: 'BE', eventType: 'erbenruf', deceasedName: 'Henri Bernard Dürst', deceasedDob: '1938-01-28', deceasedLastDomicile: 'Home Hebron, Rue du Tilleul 3, 2608 Courtelary' });
  const k2 = buildDedupeKey({ canton: 'be', eventType: 'erbenruf', deceasedName: 'henri bernard durst', deceasedDob: '1938-01-28', deceasedLastDomicile: 'HOME HEBRON, RUE DU TILLEUL 3, 2608 COURTELARY' });
  eq(k1, k2, 'estate key is normalization-stable across the 3× republication casing');
  // different deceased → different key (no cross-estate merge)
  const k3 = buildDedupeKey({ canton: 'BE', eventType: 'erbenruf', deceasedName: 'Helene Murri', deceasedDob: '1940-01-01', deceasedLastDomicile: 'Langnau' });
  eq(k1 === k3, false, 'distinct deceased do not collapse');
  // repudiation scope
  eq(repudiationScope('ausschlagung', true), 'all_heirs_liquidation', 'KK refusedLegacy → all_heirs_liquidation');
  eq(repudiationScope('ausschlagung', false), 'unknown', 'ausschlagung w/o flag → unknown');
  eq(repudiationScope('erbenruf', false), 'not_applicable', 'erbenruf → not_applicable');
}

console.log(`\n${fail === 0 ? '✓ ALL PASS' : '✗ FAILURES'} — ${pass} passed, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
