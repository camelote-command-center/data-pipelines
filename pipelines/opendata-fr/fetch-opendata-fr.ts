/**
 * opendata.fr.ch — Fribourg cantonal open data (OpenDataSoft v2).
 *
 * 1. Enumerate the catalog (~110 datasets) → bronze_ch.opendata_fr_datasets.
 * 2. For each dataset with has_records=true, page through records and land
 *    one row per ODS record in bronze_ch.opendata_fr_records (JSONB body).
 *
 * Env:
 *   RE_LLM_SUPABASE_URL, RE_LLM_SUPABASE_SERVICE_ROLE_KEY
 *   ONLY_DATASET (optional) — fetch a single dataset_id for testing
 *   MAX_RECORDS_PER_DATASET (default unlimited) — cap for safety
 */

import { upsert, verifyAccess, sleep } from '../_shared/re-llm.js';
import { createHash } from 'node:crypto';

const SCHEMA = 'bronze_ch';
const BASE = 'https://opendata.fr.ch/api/explore/v2.1';
const PAGE = 100;
const POLITENESS_MS = 150;
const MAX_RECORDS = parseInt(process.env.MAX_RECORDS_PER_DATASET ?? '0', 10) || Infinity;

interface Catalog {
  total_count: number;
  results: Array<Record<string, unknown> & { dataset_id: string }>;
}

interface Records {
  total_count: number;
  results: Array<Record<string, unknown>>;
}

async function getJson<T>(url: string): Promise<T> {
  const res = await fetch(url, { headers: { Accept: 'application/json', 'User-Agent': 'camelote-data-pipelines/opendata-fr' } });
  if (!res.ok) throw new Error(`HTTP ${res.status} on ${url}`);
  return (await res.json()) as T;
}

function md5(s: string): string {
  return createHash('md5').update(s).digest('hex');
}

function pickCommune(rec: Record<string, unknown>): string | null {
  // Heuristic: ODS records often expose a "commune" field directly.
  for (const k of ['commune', 'commune_name', 'name_commune']) {
    const v = rec[k];
    if (typeof v === 'string' && v.length > 0) return v;
  }
  return null;
}

async function fetchCatalog(): Promise<Catalog['results']> {
  const all: Catalog['results'] = [];
  let offset = 0;
  while (true) {
    const url = `${BASE}/catalog/datasets?limit=${PAGE}&offset=${offset}`;
    const page = await getJson<Catalog>(url);
    all.push(...page.results);
    if (page.results.length < PAGE) break;
    offset += PAGE;
    await sleep(POLITENESS_MS);
  }
  return all;
}

async function fetchRecords(datasetId: string): Promise<Records['results']> {
  const all: Records['results'] = [];
  let offset = 0;
  while (offset < MAX_RECORDS) {
    const url = `${BASE}/catalog/datasets/${encodeURIComponent(datasetId)}/records?limit=${PAGE}&offset=${offset}`;
    let page: Records;
    try {
      page = await getJson<Records>(url);
    } catch (err) {
      console.error(`    ${datasetId} page offset=${offset}: ${err}`);
      break;
    }
    all.push(...page.results);
    if (page.results.length < PAGE) break;
    if (offset + PAGE >= page.total_count) break;
    offset += PAGE;
    if (all.length >= MAX_RECORDS) break;
    await sleep(POLITENESS_MS);
  }
  return all;
}

async function main() {
  console.log('='.repeat(64));
  console.log('  opendata.fr.ch — Fribourg cantonal open data');
  console.log(`  Target: ${SCHEMA}.opendata_fr_datasets + opendata_fr_records on re-llm`);
  console.log('='.repeat(64));

  const t0 = Date.now();
  await verifyAccess(SCHEMA, 'opendata_fr_datasets');
  await verifyAccess(SCHEMA, 'opendata_fr_records');

  const onlyDataset = process.env.ONLY_DATASET;

  // 1. Catalog
  console.log('\n  [1/2] Fetching catalog...');
  const catalog = await fetchCatalog();
  console.log(`    ${catalog.length} datasets`);
  const catalogRows = catalog.map((d) => ({
    dataset_id: d.dataset_id,
    dataset_uid: (d as Record<string, unknown>).dataset_uid as string | null,
    has_records: (d as Record<string, unknown>).has_records as boolean | null,
    metas: (d as Record<string, unknown>).metas ?? null,
    fields: (d as Record<string, unknown>).fields ?? null,
    features: ((d as Record<string, unknown>).features as string[]) ?? null,
    attributes: d,
  }));
  await upsert(SCHEMA, 'opendata_fr_datasets', catalogRows, 'dataset_id', 50);

  // 2. Records per dataset
  const targets = onlyDataset
    ? catalog.filter((d) => d.dataset_id === onlyDataset)
    : catalog.filter((d) => (d as Record<string, unknown>).has_records);

  console.log(`\n  [2/2] Records for ${targets.length} datasets...`);
  let totalRecords = 0;
  let datasetsDone = 0;
  for (const d of targets) {
    const id = d.dataset_id;
    try {
      const recs = await fetchRecords(id);
      if (recs.length === 0) {
        console.log(`    ${id}: 0 records`);
        continue;
      }
      // Build rows; dedup within batch by natural_key.
      const seen = new Set<string>();
      const rows = [];
      for (const r of recs) {
        const recordId = (r['recordid'] as string | undefined) ?? null;
        const naturalKey = recordId ?? md5(JSON.stringify(r));
        if (seen.has(naturalKey)) continue;
        seen.add(naturalKey);
        rows.push({
          dataset_id: id,
          record_id: recordId,
          natural_key: naturalKey,
          attributes: r,
          admin_level_2: pickCommune(r),
        });
      }
      const n = await upsert(SCHEMA, 'opendata_fr_records', rows, 'dataset_id,natural_key', 200);
      totalRecords += n;
      datasetsDone++;
      console.log(`    ${id}: ${recs.length} fetched, ${rows.length} unique, ${n} upserted`);
    } catch (err) {
      console.error(`    ${id}: ${err}`);
    }
    await sleep(POLITENESS_MS);
  }

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log('\n' + '='.repeat(64));
  console.log('  IMPORT COMPLETE');
  console.log(`  Catalog datasets:   ${catalog.length}`);
  console.log(`  Datasets w/ records:${datasetsDone}/${targets.length}`);
  console.log(`  Records upserted:   ${totalRecords}`);
  console.log(`  Duration:           ${elapsed}s`);
  console.log('='.repeat(64));
}

main().catch((err) => { console.error('Fatal error:', err); process.exit(1); });
