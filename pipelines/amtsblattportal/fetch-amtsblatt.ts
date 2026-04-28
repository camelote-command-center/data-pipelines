/**
 * amtsblattportal.ch — Swiss federal SOGC + cantonal Amtsblätter publications.
 *
 * Two modes (env-controlled):
 *   - INCREMENTAL (default cron): page from `since` to today. `since` defaults to
 *     (max(publication_date) in DB) - 14d for late-publication overlap. First run
 *     starts at 30 days ago.
 *   - BACKFILL (workflow_dispatch): bounded by START_DATE / END_DATE.
 *
 * Pagination is page-based (pageRequest.page=N&pageRequest.size=200).
 * The API caps total results at 10,000 per query — when a window has more,
 * we recursively split it in half.
 *
 * Env:
 *   RE_LLM_SUPABASE_URL, RE_LLM_SUPABASE_SERVICE_ROLE_KEY
 *   START_DATE, END_DATE  (optional, YYYY-MM-DD)
 *   TENANT (optional, e.g. 'kabge' for GE only)
 *   PAGE_SIZE (default 200)
 */

import { upsert, verifyAccess, sleep } from '../_shared/re-llm.js';
import { createClient } from '@supabase/supabase-js';

const SCHEMA = 'bronze_ch';
const TABLE = 'amtsblatt_publications';
const ON_CONFLICT = 'id';
const BASE = 'https://amtsblattportal.ch/api/v1/publications.json';
const PAGE_SIZE = parseInt(process.env.PAGE_SIZE ?? '200', 10) || 200;
const POLITENESS_MS = 250;
const PAGE_HARD_CAP = 10_000; // API limit per query

// Direct supabase client for the "max(publication_date)" probe.
const supabase = createClient(
  process.env.RE_LLM_SUPABASE_URL!,
  process.env.RE_LLM_SUPABASE_SERVICE_ROLE_KEY!,
);

interface MetaT {
  id: string;
  rubric?: string;
  subRubric?: string;
  primaryTenantCode?: string;
  publicationNumber?: string;
  publicationState?: string;
  publicationDate?: string;
  expirationDate?: string;
  language?: string;
  cantons?: string[];
  registrationOffice?: { displayName?: string };
  title?: Record<string, string>;
}

interface Page {
  content: Array<{ meta: MetaT; [k: string]: unknown }>;
  total: number;
  pageRequest: { page: number; size: number };
}

function fmtDate(d: Date): string {
  return d.toISOString().slice(0, 10);
}

async function getMaxDateInDb(): Promise<string | null> {
  const { data, error } = await supabase
    .schema(SCHEMA)
    .from(TABLE)
    .select('publication_date')
    .order('publication_date', { ascending: false })
    .limit(1);
  if (error) {
    console.log(`  Could not read max(publication_date): ${error.message}`);
    return null;
  }
  return data?.[0]?.publication_date ?? null;
}

async function fetchPage(start: string, end: string, page: number, tenant?: string): Promise<Page> {
  const params = new URLSearchParams({
    publicationStates: 'PUBLISHED',
    'publicationDate.start': start,
    'publicationDate.end': end,
    'pageRequest.page': String(page),
    'pageRequest.size': String(PAGE_SIZE),
  });
  if (tenant) params.set('tenant', tenant);
  const url = `${BASE}?${params.toString()}`;

  for (let attempt = 0; attempt < 4; attempt++) {
    try {
      const res = await fetch(url, { headers: { Accept: 'application/json', 'User-Agent': 'camelote-data-pipelines/amtsblatt' } });
      if (res.status === 429 || res.status >= 500) {
        const wait = (attempt + 1) * 5_000;
        console.log(`    HTTP ${res.status}, retrying in ${wait / 1000}s...`);
        await sleep(wait);
        continue;
      }
      if (!res.ok) throw new Error(`HTTP ${res.status} on ${url}`);
      return (await res.json()) as Page;
    } catch (err) {
      if (attempt === 3) throw err;
      await sleep((attempt + 1) * 5_000);
    }
  }
  throw new Error('unreachable');
}

function metaToRow(m: MetaT, raw: unknown) {
  const t = m.title ?? {};
  return {
    id: m.id,
    publication_number: m.publicationNumber ?? null,
    publication_state: m.publicationState ?? null,
    publication_date: m.publicationDate?.slice(0, 10) ?? null,
    expiration_date: m.expirationDate?.slice(0, 10) ?? null,
    rubric: m.rubric ?? null,
    sub_rubric: m.subRubric ?? null,
    primary_tenant_code: m.primaryTenantCode ?? null,
    cantons: m.cantons ?? null,
    language: m.language ?? null,
    title_de: t.de ?? null,
    title_fr: t.fr ?? null,
    title_it: t.it ?? null,
    title_en: t.en ?? null,
    registration_office: m.registrationOffice?.displayName ?? null,
    attributes: raw,
  };
}

async function ingestWindow(start: string, end: string, tenant?: string): Promise<{ fetched: number; upserted: number }> {
  // First page tells us total. If total > PAGE_HARD_CAP, split the window in half.
  console.log(`\n  Window: ${start} → ${end}${tenant ? ` (tenant=${tenant})` : ''}`);
  const first = await fetchPage(start, end, 0, tenant);
  console.log(`    total=${first.total}, page_size=${PAGE_SIZE}`);

  if (first.total > PAGE_HARD_CAP && start !== end) {
    const startD = new Date(start), endD = new Date(end);
    const midMs = startD.getTime() + (endD.getTime() - startD.getTime()) / 2;
    const mid = new Date(midMs);
    const midStr = fmtDate(mid);
    const dayBeforeMid = fmtDate(new Date(midMs - 86_400_000));
    console.log(`    > ${PAGE_HARD_CAP}; splitting → [${start}..${dayBeforeMid}] + [${midStr}..${end}]`);
    const a = await ingestWindow(start, dayBeforeMid, tenant);
    const b = await ingestWindow(midStr, end, tenant);
    return { fetched: a.fetched + b.fetched, upserted: a.upserted + b.upserted };
  }

  // Single window — page through.
  const totalPages = Math.min(Math.ceil(first.total / PAGE_SIZE), Math.ceil(PAGE_HARD_CAP / PAGE_SIZE));
  let fetched = 0;
  let upserted = 0;

  // First page already in hand.
  const processBatch = async (entries: Page['content']) => {
    if (entries.length === 0) return 0;
    // Dedup within batch by id.
    const seen = new Set<string>();
    const rows = [];
    for (const e of entries) {
      if (!e.meta?.id || seen.has(e.meta.id)) continue;
      seen.add(e.meta.id);
      rows.push(metaToRow(e.meta, e));
    }
    return await upsert(SCHEMA, TABLE, rows, ON_CONFLICT, 200);
  };

  fetched += first.content.length;
  upserted += await processBatch(first.content);
  console.log(`    page 0/${totalPages - 1}: ${first.content.length} fetched, ${upserted} upserted`);

  for (let p = 1; p < totalPages; p++) {
    await sleep(POLITENESS_MS);
    const page = await fetchPage(start, end, p, tenant);
    fetched += page.content.length;
    const n = await processBatch(page.content);
    upserted += n;
    console.log(`    page ${p}/${totalPages - 1}: ${page.content.length} fetched, ${n} upserted (cum ${upserted}/${fetched})`);
    if (page.content.length < PAGE_SIZE) break;
  }

  return { fetched, upserted };
}

async function main() {
  console.log('='.repeat(64));
  console.log('  amtsblattportal.ch — Swiss official gazettes');
  console.log(`  Target: ${SCHEMA}.${TABLE} on re-llm`);
  console.log('='.repeat(64));

  const t0 = Date.now();
  await verifyAccess(SCHEMA, TABLE);

  let start = process.env.START_DATE;
  let end = process.env.END_DATE;
  const tenant = process.env.TENANT;

  if (!start || !end) {
    // Incremental mode.
    end = fmtDate(new Date());
    const dbMax = await getMaxDateInDb();
    if (dbMax) {
      const overlap = new Date(dbMax);
      overlap.setUTCDate(overlap.getUTCDate() - 14);
      start = fmtDate(overlap);
    } else {
      const def = new Date();
      def.setUTCDate(def.getUTCDate() - 30);
      start = fmtDate(def);
    }
    console.log(`  Mode: INCREMENTAL — last DB pub_date: ${dbMax ?? 'none'} → window ${start}..${end}`);
  } else {
    console.log(`  Mode: BACKFILL — ${start}..${end}${tenant ? ` (tenant=${tenant})` : ''}`);
  }

  const { fetched, upserted } = await ingestWindow(start!, end!, tenant);

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log('\n' + '='.repeat(64));
  console.log('  IMPORT COMPLETE');
  console.log(`  Window:        ${start}..${end}`);
  console.log(`  Fetched:       ${fetched}`);
  console.log(`  Upserted:      ${upserted}`);
  console.log(`  Duration:      ${elapsed}s`);
  console.log('='.repeat(64));
}

main().catch((err) => { console.error('Fatal error:', err); process.exit(1); });
