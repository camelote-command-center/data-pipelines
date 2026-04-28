/**
 * OpenPLZ — Swiss postal codes / address taxonomy fetcher.
 *
 * Walks /ch/Cantons → for each canton walks /ch/Cantons/{key}/{Districts,Communes,Localities}.
 * Lands rows in bronze_ch.openplz_records.
 *
 * Env:
 *   RE_LLM_SUPABASE_URL, RE_LLM_SUPABASE_SERVICE_ROLE_KEY
 *   ONLY_CANTON (optional) — restrict to one canton key for testing
 */

import { upsert, verifyAccess, sleep } from '../_shared/re-llm.js';

const SCHEMA = 'bronze_ch';
const TABLE = 'openplz_records';
const ON_CONFLICT = 'level,natural_key';
const BASE = 'https://openplzapi.org/ch';
const PAGE_SIZE = 50;
const POLITENESS_MS = 200;

interface Canton { key: string; historicalCode: string; name: string; shortName: string; }
interface District { key: string; historicalCode: string; name: string; shortName: string; canton: Canton; }
interface Commune { key: string; historicalCode: string; name: string; shortName: string; district: District; canton: Canton; }
interface Locality { postalCode: string; name: string; commune: Commune; district: District; canton: Canton; }

async function getJson<T>(url: string): Promise<{ body: T; totalCount: number }> {
  const res = await fetch(url, { headers: { Accept: 'application/json', 'User-Agent': 'camelote-data-pipelines/openplz' } });
  if (!res.ok) throw new Error(`HTTP ${res.status} on ${url}`);
  const totalCount = parseInt(res.headers.get('x-total-count') ?? '0', 10) || 0;
  const body = (await res.json()) as T;
  return { body, totalCount };
}

async function fetchPaged<T>(url: string): Promise<T[]> {
  const all: T[] = [];
  let page = 1;
  while (true) {
    const sep = url.includes('?') ? '&' : '?';
    const { body, totalCount } = await getJson<T[]>(`${url}${sep}page=${page}&pageSize=${PAGE_SIZE}`);
    all.push(...body);
    if (all.length >= totalCount || body.length < PAGE_SIZE) break;
    page++;
    await sleep(POLITENESS_MS);
  }
  return all;
}

async function main() {
  console.log('='.repeat(64));
  console.log('  OpenPLZ — Swiss postal/address taxonomy');
  console.log(`  Source: ${BASE}`);
  console.log(`  Target: ${SCHEMA}.${TABLE} on re-llm`);
  console.log('='.repeat(64));

  const t0 = Date.now();
  await verifyAccess(SCHEMA, TABLE);

  const onlyCanton = process.env.ONLY_CANTON;

  // 1. Cantons
  console.log('\n  [1/4] Cantons...');
  const { body: cantons } = await getJson<Canton[]>(`${BASE}/Cantons`);
  console.log(`    ${cantons.length} cantons`);
  const cantonRecords = cantons.map((c) => ({
    level: 'canton',
    natural_key: c.key,
    name: c.name,
    short_name: c.shortName,
    postal_code: null,
    canton_short: c.shortName,
    district_key: null,
    commune_key: null,
    attributes: c,
    admin_level_1: c.shortName,
    admin_level_2: null,
  }));
  await upsert(SCHEMA, TABLE, cantonRecords, ON_CONFLICT, 100);

  const cantonsToWalk = onlyCanton ? cantons.filter((c) => c.key === onlyCanton) : cantons;

  // 2. Districts + 3. Communes + 4. Localities — per canton
  let totalDistricts = 0, totalCommunes = 0, totalLocalities = 0;

  for (const c of cantonsToWalk) {
    console.log(`\n  Canton ${c.shortName} (${c.key}) — ${c.name}`);

    const districts = await fetchPaged<District>(`${BASE}/Cantons/${c.key}/Districts`);
    if (districts.length > 0) {
      const rows = districts.map((d) => ({
        level: 'district',
        natural_key: d.key,
        name: d.name,
        short_name: d.shortName,
        postal_code: null,
        canton_short: c.shortName,
        district_key: d.key,
        commune_key: null,
        attributes: d,
        admin_level_1: c.shortName,
        admin_level_2: null,
      }));
      await upsert(SCHEMA, TABLE, rows, ON_CONFLICT, 200);
      totalDistricts += rows.length;
      console.log(`    districts: ${rows.length}`);
    }

    const communes = await fetchPaged<Commune>(`${BASE}/Cantons/${c.key}/Communes`);
    if (communes.length > 0) {
      const rows = communes.map((m) => ({
        level: 'commune',
        natural_key: m.key,
        name: m.name,
        short_name: m.shortName,
        postal_code: null,
        canton_short: c.shortName,
        district_key: m.district?.key ?? null,
        commune_key: m.key,
        attributes: m,
        admin_level_1: c.shortName,
        admin_level_2: m.name,
      }));
      await upsert(SCHEMA, TABLE, rows, ON_CONFLICT, 200);
      totalCommunes += rows.length;
      console.log(`    communes: ${rows.length}`);
    }

    const localities = await fetchPaged<Locality>(`${BASE}/Cantons/${c.key}/Localities`);
    if (localities.length > 0) {
      // Localities have no `key` — composite natural_key.
      const rows = localities.map((l) => ({
        level: 'locality',
        natural_key: `${l.postalCode}|${l.name}|${l.commune?.key ?? ''}`,
        name: l.name,
        short_name: null,
        postal_code: l.postalCode,
        canton_short: c.shortName,
        district_key: l.district?.key ?? null,
        commune_key: l.commune?.key ?? null,
        attributes: l,
        admin_level_1: c.shortName,
        admin_level_2: l.commune?.name ?? null,
      }));
      // De-dupe within the batch by natural_key (a few PLZ/name/commune triples
      // can repeat for la-Poste-style boxes).
      const seen = new Set<string>();
      const unique = rows.filter((r) => {
        if (seen.has(r.natural_key)) return false;
        seen.add(r.natural_key);
        return true;
      });
      await upsert(SCHEMA, TABLE, unique, ON_CONFLICT, 200);
      totalLocalities += unique.length;
      console.log(`    localities: ${unique.length}${unique.length !== rows.length ? ` (${rows.length - unique.length} dedup'd)` : ''}`);
    }

    await sleep(POLITENESS_MS);
  }

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log('\n' + '='.repeat(64));
  console.log('  IMPORT COMPLETE');
  console.log(`  Cantons:     ${cantons.length}`);
  console.log(`  Districts:   ${totalDistricts}`);
  console.log(`  Communes:    ${totalCommunes}`);
  console.log(`  Localities:  ${totalLocalities}`);
  console.log(`  Duration:    ${elapsed}s`);
  console.log('='.repeat(64));
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
