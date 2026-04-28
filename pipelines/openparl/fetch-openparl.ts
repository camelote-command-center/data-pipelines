/**
 * OpenParlData — Swiss parliamentary data (federal/cantonal/communal).
 *
 * Phase 1: ingest all bodies (national context) + Suisse-romande indexed bodies' affairs.
 *
 * Env:
 *   RE_LLM_SUPABASE_URL, RE_LLM_SUPABASE_SERVICE_ROLE_KEY
 *   ROMANDE_ONLY (default 'true') — set 'false' to fetch affairs for all indexed bodies
 */

import { upsert, verifyAccess, sleep } from '../_shared/re-llm.js';

const SCHEMA = 'bronze_ch';
const TABLE = 'openparl_records';
const ON_CONFLICT = 'record_type,api_id';
const BASE = 'https://api.openparldata.ch/v1';
const PAGE = 100;
const POLITENESS_MS = 200;

const ROMANDE_CANTONS = new Set(['GE', 'VD', 'NE', 'JU', 'FR', 'VS']);
const ROMANDE_ONLY = (process.env.ROMANDE_ONLY ?? 'true').toLowerCase() !== 'false';

interface Page<T> {
  meta: { offset: number; limit: number; total_records: number; has_more: boolean; next_page: string | null };
  data: T[];
}

async function getJson<T>(url: string): Promise<T> {
  const res = await fetch(url, { headers: { Accept: 'application/json', 'User-Agent': 'camelote-data-pipelines/openparl' } });
  if (!res.ok) throw new Error(`HTTP ${res.status} on ${url}`);
  return (await res.json()) as T;
}

async function fetchAll<T>(path: string, qs: string = ''): Promise<T[]> {
  const all: T[] = [];
  let url = `${BASE}${path}?limit=${PAGE}${qs ? '&' + qs : ''}`;
  while (url) {
    const page = await getJson<Page<T>>(url);
    all.push(...page.data);
    if (!page.meta.has_more || !page.meta.next_page) break;
    url = page.meta.next_page;
    await sleep(POLITENESS_MS);
  }
  return all;
}

interface Body {
  id: number;
  body_key: string;
  name: Record<string, string>;
  type: string;
  canton_key: string;
  indexed: boolean;
  lang: string;
  consultations_url: string | null;
  population: number | null;
}

interface Affair {
  id: number;
  body_id?: number;
  title?: Record<string, string> | string;
  body?: { id: number; canton_key: string; type: string; body_key: string };
  [k: string]: unknown;
}

function pickTitle(name: unknown, prefer = ['fr', 'en', 'de', 'it']): string | null {
  if (typeof name === 'string') return name;
  if (name && typeof name === 'object') {
    for (const k of prefer) {
      const v = (name as Record<string, string>)[k];
      if (v) return v;
    }
    const vals = Object.values(name as Record<string, string>);
    if (vals.length) return vals[0];
  }
  return null;
}

async function main() {
  console.log('='.repeat(64));
  console.log('  OpenParlData — Swiss parliamentary data');
  console.log(`  Romande-only mode: ${ROMANDE_ONLY}`);
  console.log(`  Target: ${SCHEMA}.${TABLE} on re-llm`);
  console.log('='.repeat(64));

  const t0 = Date.now();
  await verifyAccess(SCHEMA, TABLE);

  // 1. All bodies
  console.log('\n  [1/2] Bodies (all of CH)...');
  const bodies = await fetchAll<Body>('/bodies/');
  console.log(`    ${bodies.length} bodies fetched`);

  const bodyRows = bodies.map((b) => ({
    record_type: 'body',
    api_id: b.id,
    body_id: b.id,
    canton_key: b.canton_key ?? null,
    body_key: b.body_key ?? null,
    body_type: b.type ?? null,
    title: pickTitle(b.name),
    attributes: b,
    language: b.lang?.toLowerCase() ?? null,
    admin_level_1: b.canton_key ?? null,
    admin_level_2: b.type === 'municipality' ? pickTitle(b.name) : null,
  }));
  await upsert(SCHEMA, TABLE, bodyRows, ON_CONFLICT, 200);

  // 2. Affairs for indexed bodies (Suisse romande by default)
  const targetBodies = bodies.filter((b) => {
    if (!b.indexed) return false;
    if (ROMANDE_ONLY && !ROMANDE_CANTONS.has(b.canton_key)) return false;
    return true;
  });
  console.log(`\n  [2/2] Affairs for ${targetBodies.length} ${ROMANDE_ONLY ? 'romande indexed' : 'indexed'} bodies...`);

  let totalAffairs = 0;
  for (const b of targetBodies) {
    try {
      const affairs = await fetchAll<Affair>(`/bodies/${b.id}/affairs`);
      if (affairs.length === 0) {
        console.log(`    body ${b.id} (${b.canton_key} ${pickTitle(b.name)}): 0 affairs`);
        continue;
      }
      const rows = affairs.map((a) => ({
        record_type: 'affair',
        api_id: a.id,
        body_id: b.id,
        canton_key: b.canton_key ?? null,
        body_key: b.body_key ?? null,
        body_type: b.type ?? null,
        title: pickTitle(a.title),
        attributes: a,
        language: b.lang?.toLowerCase() ?? null,
        admin_level_1: b.canton_key ?? null,
        admin_level_2: b.type === 'municipality' ? pickTitle(b.name) : null,
      }));
      const n = await upsert(SCHEMA, TABLE, rows, ON_CONFLICT, 200);
      totalAffairs += n;
      console.log(`    body ${b.id} (${b.canton_key} ${pickTitle(b.name)}): ${rows.length} affairs (${n} upserted)`);
    } catch (err) {
      console.error(`    body ${b.id}: ${err}`);
    }
    await sleep(POLITENESS_MS);
  }

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log('\n' + '='.repeat(64));
  console.log('  IMPORT COMPLETE');
  console.log(`  Bodies upserted:  ${bodies.length}`);
  console.log(`  Bodies w/ affairs: ${targetBodies.length}`);
  console.log(`  Affairs upserted: ${totalAffairs}`);
  console.log(`  Duration:         ${elapsed}s`);
  console.log('='.repeat(64));
}

main().catch((err) => { console.error('Fatal error:', err); process.exit(1); });
