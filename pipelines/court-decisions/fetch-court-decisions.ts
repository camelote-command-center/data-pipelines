/**
 * court-decisions — Swiss judicial decisions filtered to RE-relevant chambers.
 *
 * Source: entscheidsuche.ch Elasticsearch endpoint /_searchV2.php
 * Each hit has: date, hierarchy, title (de/fr/it), abstract, attachment.content
 * (full plaintext), attachment.content_url (PDF). Land each decision as one row
 * in knowledge_ch.documents (long-form, classifier-trigger fires async).
 * Dedup map kept in bronze_ch.court_decisions_index (decision_id unique).
 *
 * Modes:
 *   - INCREMENTAL (default cron): pull from max(decision_date) in DB - 14d overlap.
 *     First run on a fresh table starts 30 days back.
 *   - BACKFILL (workflow_dispatch): bounded by START_DATE / END_DATE.
 *
 * Env:
 *   RE_LLM_SUPABASE_URL, RE_LLM_SUPABASE_SERVICE_ROLE_KEY (required)
 *   START_DATE, END_DATE  (optional, YYYY-MM-DD — backfill mode)
 *   ONLY_KAMMER (optional) — restrict to one kammer key (e.g. GE_CJ_004) for tests
 *   PAGE_SIZE   (default 100)
 *   MAX_PER_KAMMER (default unlimited; useful for smoke tests)
 */

import { createClient } from '@supabase/supabase-js';
import { sleep } from '../_shared/re-llm.js';
import { KAMMERN, ENTSCHEIDSUCHE_ENDPOINT, type Kammer } from './kammern.js';

const PAGE_SIZE = parseInt(process.env.PAGE_SIZE ?? '', 10) || 100;
const MAX_PER_KAMMER = parseInt(process.env.MAX_PER_KAMMER ?? '', 10) || Infinity;
const POLITENESS_MS = 400;
const ES_HARD_CAP = 9_500;  // entscheidsuche caps at 10K total hits per query — split if exceeded

const supabase = createClient(
  process.env.RE_LLM_SUPABASE_URL!,
  process.env.RE_LLM_SUPABASE_SERVICE_ROLE_KEY!,
);

interface ESHit {
  _id: string;
  _source: {
    date?: string;
    hierarchy?: string[];
    title?: Record<string, string>;
    abstract?: Record<string, string>;
    reference?: string[];
    attachment?: {
      content_url?: string;
      content?: string;
      content_type?: string;
      language?: string;
      author?: string;
    };
    [k: string]: unknown;
  };
}

interface ESResponse {
  hits: { total: { value: number; relation: string }; hits: ESHit[] };
}

// ---------------------------------------------------------------------------
// ES query helpers
// ---------------------------------------------------------------------------

async function esSearch(query: object): Promise<ESResponse> {
  for (let attempt = 0; attempt < 4; attempt++) {
    try {
      const res = await fetch(ENTSCHEIDSUCHE_ENDPOINT, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'User-Agent': 'camelote-data-pipelines/court-decisions',
          Accept: 'application/json',
        },
        body: JSON.stringify(query),
      });
      if (res.status === 429 || res.status >= 500) {
        const wait = (attempt + 1) * 5000;
        console.log(`    HTTP ${res.status}, retry in ${wait / 1000}s...`);
        await sleep(wait);
        continue;
      }
      if (!res.ok) throw new Error(`ES HTTP ${res.status}: ${(await res.text()).slice(0, 300)}`);
      return (await res.json()) as ESResponse;
    } catch (err) {
      if (attempt === 3) throw err;
      await sleep((attempt + 1) * 5000);
    }
  }
  throw new Error('unreachable');
}

function buildQuery(kammer: Kammer, dateFrom: string, dateTo: string, from: number, size: number): object {
  return {
    from,
    size,
    query: {
      bool: {
        // entscheidsuche indexes `hierarchy` as a plain text array (no .keyword
        // sub-field). `term` on the array means "array contains this exact value".
        must: [{ term: { hierarchy: kammer.key } }],
        filter: [{ range: { date: { gte: dateFrom, lte: dateTo } } }],
      },
    },
    sort: [{ date: { order: 'asc' } }],
    _source: ['date', 'hierarchy', 'title', 'abstract', 'reference', 'attachment.content_url', 'attachment.content', 'attachment.language'],
  };
}

// ---------------------------------------------------------------------------
// Date helpers
// ---------------------------------------------------------------------------

function fmt(d: Date): string { return d.toISOString().slice(0, 10); }

async function getMaxDateInDb(): Promise<string | null> {
  const { data, error } = await supabase
    .schema('bronze_ch')
    .from('court_decisions_index')
    .select('decision_date')
    .order('decision_date', { ascending: false })
    .limit(1);
  if (error) {
    console.log(`  Could not read max(decision_date): ${error.message}`);
    return null;
  }
  return data?.[0]?.decision_date ?? null;
}

// ---------------------------------------------------------------------------
// Field shapes
// ---------------------------------------------------------------------------

function pickLang<T>(obj: Record<string, T> | undefined, prefer: string[]): T | null {
  if (!obj) return null;
  for (const k of prefer) if (obj[k]) return obj[k];
  const vals = Object.values(obj);
  return vals.length ? vals[0] : null;
}

function detectLanguage(hit: ESHit, kammer: Kammer): string {
  const lang = hit._source.attachment?.language;
  if (lang) return lang.slice(0, 2).toLowerCase();
  if (kammer.canton === 'TI') return 'it';
  if (['ZH', 'BE', 'AG', 'BL', 'BS', 'GR', 'LU', 'SG', 'SH', 'SO', 'SZ', 'TG', 'UR', 'ZG', 'OW', 'NW', 'GL', 'AR', 'AI'].includes(kammer.canton)) return 'de';
  return 'fr';
}

// ---------------------------------------------------------------------------
// Per-kammer ingest
// ---------------------------------------------------------------------------

async function listExistingIds(decisionIds: string[]): Promise<Set<string>> {
  if (decisionIds.length === 0) return new Set();
  const { data, error } = await supabase
    .schema('bronze_ch')
    .from('court_decisions_index')
    .select('decision_id')
    .in('decision_id', decisionIds);
  if (error) {
    console.error(`    listExistingIds: ${error.message}`);
    return new Set();
  }
  return new Set((data ?? []).map((r: { decision_id: string }) => r.decision_id));
}

async function processKammer(kammer: Kammer, dateFrom: string, dateTo: string): Promise<{ fetched: number; inserted: number }> {
  console.log(`\n  ── ${kammer.key} | ${kammer.name_fr ?? kammer.name_de} ──`);
  console.log(`    window: ${dateFrom} → ${dateTo}`);

  let fetched = 0, inserted = 0;
  let from = 0;
  while (from < ES_HARD_CAP && fetched < MAX_PER_KAMMER) {
    const query = buildQuery(kammer, dateFrom, dateTo, from, PAGE_SIZE);
    let res: ESResponse;
    try {
      res = await esSearch(query);
    } catch (err) {
      console.error(`    page from=${from} failed: ${err}`);
      break;
    }
    const totalHits = res.hits.total.value;
    if (from === 0) console.log(`    total in window: ${totalHits}${totalHits >= 10000 ? ' (window may be capped — narrow the range)' : ''}`);
    const hits = res.hits.hits;
    if (hits.length === 0) break;

    fetched += hits.length;

    // Dedup against existing index.
    const ids = hits.map((h) => h._id);
    const existing = await listExistingIds(ids);
    const fresh = hits.filter((h) => !existing.has(h._id));
    if (fresh.length === 0) {
      console.log(`    page from=${from}: ${hits.length} fetched, all already indexed`);
      from += PAGE_SIZE;
      await sleep(POLITENESS_MS);
      continue;
    }

    // Build documents
    const docs = fresh.map((h) => {
      const s = h._source;
      const lang = detectLanguage(h, kammer);
      const title = pickLang(s.title, [lang, 'fr', 'de', 'it']) ?? '(untitled)';
      const abstract = pickLang(s.abstract, [lang, 'fr', 'de', 'it']);
      const content = s.attachment?.content;
      return {
        title: String(title).slice(0, 500),
        description: abstract ? String(abstract).slice(0, 2000) : null,
        source: kammer.key,
        publisher: kammer.name_fr ?? kammer.name_de ?? kammer.key,
        document_type: 'court_decision',
        original_url: s.attachment?.content_url ?? `https://entscheidsuche.ch/?lang=fr#${h._id}`,
        publication_date: s.date ?? null,
        language: lang,
        country: 'ch',
        canton_code: kammer.canton === 'CH' ? null : kammer.canton,
        tags: [...kammer.tags],
        raw_metadata: {
          decision_id: h._id,
          hierarchy: s.hierarchy,
          reference: s.reference,
          content_length: content?.length ?? 0,
          content_url: s.attachment?.content_url,
          ingested_at: new Date().toISOString(),
        },
      };
    });

    const { data: inserted_docs, error: insErr } = await supabase
      .schema('knowledge_ch')
      .from('documents')
      .insert(docs)
      .select('id, original_url');
    if (insErr) {
      console.error(`    insert error: ${insErr.message}`);
      break;
    }

    // Build a url → doc_id map so we can populate the index correctly.
    const urlToDoc = new Map<string, string>();
    for (const r of inserted_docs ?? []) urlToDoc.set(r.original_url ?? '', r.id);

    // Upsert dedup index rows.
    const indexRows = fresh.map((h) => {
      const docUrl = h._source.attachment?.content_url ?? `https://entscheidsuche.ch/?lang=fr#${h._id}`;
      return {
        decision_id: h._id,
        canton: kammer.canton,
        court: kammer.court,
        kammer: kammer.key,
        decision_date: h._source.date ?? null,
        document_id: urlToDoc.get(docUrl) ?? null,
        fetch_status: 'success',
      };
    });
    const { error: idxErr } = await supabase
      .schema('bronze_ch')
      .from('court_decisions_index')
      .upsert(indexRows, { onConflict: 'decision_id' });
    if (idxErr) console.error(`    index upsert: ${idxErr.message}`);

    inserted += inserted_docs?.length ?? 0;
    console.log(`    page from=${from}: ${hits.length} fetched, ${fresh.length} new, ${inserted_docs?.length ?? 0} inserted`);

    if (hits.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
    await sleep(POLITENESS_MS);
  }

  return { fetched, inserted };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  console.log('='.repeat(64));
  console.log('  court-decisions — Swiss judicial decisions');
  console.log(`  Endpoint: ${ENTSCHEIDSUCHE_ENDPOINT}`);
  console.log(`  Target:   knowledge_ch.documents + bronze_ch.court_decisions_index`);
  console.log('='.repeat(64));

  const t0 = Date.now();

  let dateFrom = process.env.START_DATE;
  let dateTo = process.env.END_DATE;
  const onlyKammer = process.env.ONLY_KAMMER;

  if (!dateFrom || !dateTo) {
    dateTo = fmt(new Date());
    const dbMax = await getMaxDateInDb();
    if (dbMax) {
      const overlap = new Date(dbMax);
      overlap.setUTCDate(overlap.getUTCDate() - 14);
      dateFrom = fmt(overlap);
    } else {
      const def = new Date();
      def.setUTCDate(def.getUTCDate() - 30);
      dateFrom = fmt(def);
    }
    console.log(`  Mode: INCREMENTAL — last DB date: ${dbMax ?? 'none'} → window ${dateFrom}..${dateTo}`);
  } else {
    console.log(`  Mode: BACKFILL — ${dateFrom}..${dateTo}`);
  }

  const kammern = onlyKammer ? KAMMERN.filter((k) => k.key === onlyKammer) : KAMMERN;
  console.log(`  Kammern: ${kammern.length}${onlyKammer ? ` (filtered to ${onlyKammer})` : ''}`);

  let totalFetched = 0, totalInserted = 0;
  for (const k of kammern) {
    try {
      const r = await processKammer(k, dateFrom!, dateTo!);
      totalFetched += r.fetched;
      totalInserted += r.inserted;
    } catch (err) {
      console.error(`  ${k.key} failed: ${err}`);
    }
  }

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log('\n' + '='.repeat(64));
  console.log(`  IMPORT COMPLETE`);
  console.log(`  Fetched:  ${totalFetched}`);
  console.log(`  Inserted: ${totalInserted}`);
  console.log(`  Duration: ${elapsed}s`);
  console.log('='.repeat(64));
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
