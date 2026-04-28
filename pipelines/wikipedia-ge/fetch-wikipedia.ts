/**
 * Wikipedia Geneva ingest — phase 1: real-estate context.
 *
 * Pipeline:
 *   1. Resolve seed Qids
 *      a. SPARQL queries (Wikidata Query Service) → enumerated Qid sets
 *      b. FR Wikipedia titles → MediaWiki REST → Wikidata Qid via page summary
 *   2. For each unique Qid:
 *      a. Fetch full Wikidata entity (claims, sitelinks, multilingual labels)
 *      b. UPSERT bronze_ch.wikidata_entities
 *      c. For each language (FR, EN):
 *         - Fetch REST /page/summary (revision_id, extract, description)
 *         - Skip if revision_id matches existing row (no change since last run)
 *         - Otherwise fetch REST /page/html (full HTML)
 *         - Fetch /page/categories
 *         - UPSERT bronze_ch.wikipedia_articles
 *
 * Source attribution baked into every row:
 *   - source_url, license, fetched_at, revision_id, attribution string
 *
 * Failure policy:
 *   - Per-entity failures are logged and counted, never crash the run.
 *   - The run is a "success" if ≥80% of resolved Qids ingest at least one
 *     language. Below that, exit 1 so the workflow surfaces it.
 *
 * Usage:
 *   RE_LLM_SUPABASE_URL=... RE_LLM_SUPABASE_SERVICE_ROLE_KEY=... npx tsx fetch-wikipedia.ts
 */

import { upsert, verifyAccess, sleep } from '../_shared/re-llm.js';
import { SEED_SPARQL, SEED_TITLES_FR, FETCH_LANGUAGES, type SparqlSeed } from './seeds.js';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const USER_AGENT =
  'CamelotePipelines/1.0 (https://github.com/camelote-command-center/data-pipelines; ops@camelote.io)';

const WIKIDATA_SPARQL = 'https://query.wikidata.org/sparql';
const WIKIDATA_ENTITY = 'https://www.wikidata.org/wiki/Special:EntityData';
const REST_BASE = (lang: string) => `https://${lang}.wikipedia.org/api/rest_v1`;
const ACTION_BASE = (lang: string) => `https://${lang}.wikipedia.org/w/api.php`;

const REQUEST_PAUSE_MS = 250;   // gentle on Wikimedia infra (~4 req/s)

// ---------------------------------------------------------------------------
// Tiny HTTP helper (no proxy — Wikimedia is open)
// ---------------------------------------------------------------------------

async function getJson<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, {
    ...init,
    headers: { 'User-Agent': USER_AGENT, Accept: 'application/json', ...(init?.headers || {}) },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} on ${url}: ${(await res.text()).slice(0, 200)}`);
  return (await res.json()) as T;
}

async function getText(url: string, init?: RequestInit): Promise<string> {
  const res = await fetch(url, {
    ...init,
    headers: { 'User-Agent': USER_AGENT, ...(init?.headers || {}) },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} on ${url}: ${(await res.text()).slice(0, 200)}`);
  return await res.text();
}

// ---------------------------------------------------------------------------
// Wikidata SPARQL
// ---------------------------------------------------------------------------

async function sparqlQids(seed: SparqlSeed): Promise<string[]> {
  const url = `${WIKIDATA_SPARQL}?query=${encodeURIComponent(seed.query)}&format=json`;
  const data = await getJson<{ results: { bindings: { item: { value: string } }[] } }>(url);
  return data.results.bindings
    .map((b) => b.item.value.split('/').pop()!)
    .filter((q) => /^Q\d+$/.test(q));
}

// ---------------------------------------------------------------------------
// Wikidata entity fetch
// ---------------------------------------------------------------------------

interface WikidataEntity {
  id: string;
  labels?: Record<string, { language: string; value: string }>;
  descriptions?: Record<string, { language: string; value: string }>;
  claims?: Record<string, unknown[]>;
  sitelinks?: Record<string, { site: string; title: string; url: string }>;
}

async function fetchWikidataEntity(qid: string): Promise<WikidataEntity | null> {
  try {
    const data = await getJson<{ entities: Record<string, WikidataEntity> }>(
      `${WIKIDATA_ENTITY}/${qid}.json`,
    );
    return data.entities[qid] ?? null;
  } catch (err: any) {
    console.error(`  [wikidata] ${qid} fetch failed: ${err.message}`);
    return null;
  }
}

function flattenLabels(labels?: Record<string, { language: string; value: string }>): Record<string, string> {
  if (!labels) return {};
  const out: Record<string, string> = {};
  for (const [lang, obj] of Object.entries(labels)) out[lang] = obj.value;
  return out;
}

function flattenSitelinks(
  sitelinks?: Record<string, { site: string; title: string; url: string }>,
): Record<string, { title: string; url: string }> {
  if (!sitelinks) return {};
  const out: Record<string, { title: string; url: string }> = {};
  for (const [site, obj] of Object.entries(sitelinks)) {
    out[site] = { title: obj.title, url: obj.url };
  }
  return out;
}

// ---------------------------------------------------------------------------
// FR Wikipedia title → Qid (resolve seed titles)
// ---------------------------------------------------------------------------

async function resolveTitleToQid(title: string, lang = 'fr'): Promise<string | null> {
  // REST /page/summary returns wikibase_item directly.
  try {
    const data = await getJson<{ wikibase_item?: string; type?: string }>(
      `${REST_BASE(lang)}/page/summary/${encodeURIComponent(title)}`,
    );
    if (data.type === 'disambiguation') {
      console.warn(`  [resolve] "${title}" is a disambiguation page — skip`);
      return null;
    }
    return data.wikibase_item ?? null;
  } catch (err: any) {
    console.warn(`  [resolve] "${title}" not found on ${lang}.wikipedia: ${err.message}`);
    return null;
  }
}

// ---------------------------------------------------------------------------
// Wikipedia article fetch
// ---------------------------------------------------------------------------

interface PageSummary {
  title: string;
  description?: string;
  extract?: string;
  content_urls?: { desktop: { page: string } };
  pageid?: number;
}

async function fetchPageSummary(lang: string, title: string): Promise<PageSummary | null> {
  try {
    return await getJson<PageSummary>(
      `${REST_BASE(lang)}/page/summary/${encodeURIComponent(title)}`,
    );
  } catch (err: any) {
    console.warn(`  [page] summary ${lang}:${title} failed: ${err.message}`);
    return null;
  }
}

interface RevisionInfo {
  revid: number;
  timestamp: string;
}

async function fetchLatestRevision(lang: string, pageid: number): Promise<RevisionInfo | null> {
  // Action API: cheaper than fetching HTML twice.
  const url = `${ACTION_BASE(lang)}?action=query&format=json&prop=revisions&pageids=${pageid}&rvprop=ids|timestamp&rvlimit=1&origin=*`;
  try {
    const data = await getJson<{
      query: { pages: Record<string, { revisions?: { revid: number; timestamp: string }[] }> };
    }>(url);
    const page = Object.values(data.query.pages)[0];
    return page?.revisions?.[0] ?? null;
  } catch (err: any) {
    console.warn(`  [revision] ${lang}:${pageid} failed: ${err.message}`);
    return null;
  }
}

async function fetchPageHtml(lang: string, title: string): Promise<string | null> {
  try {
    return await getText(`${REST_BASE(lang)}/page/html/${encodeURIComponent(title)}`);
  } catch (err: any) {
    console.warn(`  [page] html ${lang}:${title} failed: ${err.message}`);
    return null;
  }
}

async function fetchCategories(lang: string, pageid: number): Promise<string[]> {
  const url = `${ACTION_BASE(lang)}?action=query&format=json&prop=categories&pageids=${pageid}&cllimit=max&clshow=!hidden&origin=*`;
  try {
    const data = await getJson<{
      query: { pages: Record<string, { categories?: { title: string }[] }> };
    }>(url);
    const page = Object.values(data.query.pages)[0];
    return (page?.categories ?? []).map((c) => c.title.replace(/^Catégorie:|^Category:/, ''));
  } catch {
    return [];
  }
}

// ---------------------------------------------------------------------------
// Existing-revision lookup (skip work when nothing changed)
// ---------------------------------------------------------------------------

import { supabase } from '../_shared/re-llm.js';

async function getExistingRevisionId(qid: string, language: string): Promise<number | null> {
  const { data, error } = await supabase
    .schema('bronze_ch')
    .from('wikipedia_articles')
    .select('revision_id')
    .eq('qid', qid)
    .eq('language', language)
    .maybeSingle();
  if (error || !data) return null;
  return Number(data.revision_id);
}

// ---------------------------------------------------------------------------
// Main pipeline
// ---------------------------------------------------------------------------

interface Stats {
  qids_resolved: number;
  entities_upserted: number;
  articles_upserted: number;
  articles_skipped_unchanged: number;
  articles_failed: number;
  unresolvable_titles: string[];
}

async function main() {
  console.log('='.repeat(60));
  console.log('  Wikipedia Geneva — phase 1 ingest (RE context)');
  console.log('  Source: Wikidata + FR/EN Wikipedia');
  console.log('  Target: re-llm bronze_ch.{wikidata_entities, wikipedia_articles}');
  console.log('='.repeat(60));

  const startTime = Date.now();
  const stats: Stats = {
    qids_resolved: 0,
    entities_upserted: 0,
    articles_upserted: 0,
    articles_skipped_unchanged: 0,
    articles_failed: 0,
    unresolvable_titles: [],
  };

  // 0. Connectivity check
  await verifyAccess('bronze_ch', 'wikidata_entities');
  await verifyAccess('bronze_ch', 'wikipedia_articles');

  // 1. Resolve all seeds → unique Qid set with category metadata
  const qidToCategory = new Map<string, string>();

  console.log('\n  [1/3] Resolving SPARQL seeds...');
  for (const seed of SEED_SPARQL) {
    const qids = await sparqlQids(seed);
    console.log(`    ${seed.name}: ${qids.length} entities`);
    for (const q of qids) {
      // SPARQL category wins unless already set (specificity bias)
      if (!qidToCategory.has(q)) qidToCategory.set(q, seed.category);
    }
    await sleep(REQUEST_PAUSE_MS);
  }

  console.log('\n  [2/3] Resolving FR Wikipedia titles...');
  for (const t of SEED_TITLES_FR) {
    const qid = await resolveTitleToQid(t.title, 'fr');
    if (!qid) {
      stats.unresolvable_titles.push(t.title);
      continue;
    }
    if (!qidToCategory.has(qid)) qidToCategory.set(qid, t.category);
    await sleep(REQUEST_PAUSE_MS);
  }

  stats.qids_resolved = qidToCategory.size;
  console.log(`    → ${stats.qids_resolved} unique Qids to fetch`);
  if (stats.unresolvable_titles.length) {
    console.log(`    [!] ${stats.unresolvable_titles.length} title(s) could not be resolved:`);
    for (const t of stats.unresolvable_titles) console.log(`        - ${t}`);
  }

  // 2. Fetch each entity + its FR/EN articles
  console.log(`\n  [3/3] Fetching ${stats.qids_resolved} entities...`);
  let i = 0;
  for (const [qid, category] of qidToCategory) {
    i++;
    if (i % 25 === 0) console.log(`    ${i}/${stats.qids_resolved}...`);

    const entity = await fetchWikidataEntity(qid);
    await sleep(REQUEST_PAUSE_MS);
    if (!entity) {
      stats.articles_failed++;
      continue;
    }

    const sitelinks = flattenSitelinks(entity.sitelinks);
    const labels = flattenLabels(entity.labels);
    const descriptions = flattenLabels(entity.descriptions);

    // Wikidata entity row
    await upsert(
      'bronze_ch',
      'wikidata_entities',
      [
        {
          qid,
          labels,
          descriptions,
          claims: entity.claims ?? {},
          sitelinks,
          domain: 'real_estate',
          category,
          source_url: `https://www.wikidata.org/wiki/${qid}`,
          license: 'CC0',
          fetched_at: new Date().toISOString(),
        },
      ],
      'qid',
      1,
    );
    stats.entities_upserted++;

    // Wikipedia article rows (one per language)
    for (const lang of FETCH_LANGUAGES) {
      const sitekey = `${lang}wiki`;
      const link = sitelinks[sitekey];
      if (!link) continue;

      const summary = await fetchPageSummary(lang, link.title);
      await sleep(REQUEST_PAUSE_MS);
      if (!summary || !summary.pageid) continue;

      const rev = await fetchLatestRevision(lang, summary.pageid);
      await sleep(REQUEST_PAUSE_MS);
      if (!rev) continue;

      const existing = await getExistingRevisionId(qid, lang);
      if (existing !== null && existing === rev.revid) {
        stats.articles_skipped_unchanged++;
        continue;
      }

      const html = await fetchPageHtml(lang, link.title);
      await sleep(REQUEST_PAUSE_MS);
      if (!html) {
        stats.articles_failed++;
        continue;
      }

      const categories = await fetchCategories(lang, summary.pageid);
      await sleep(REQUEST_PAUSE_MS);

      const sourceUrl = link.url;
      const attribution =
        `Wikipedia (${lang.toUpperCase()}) — “${link.title}”, ` +
        `revision ${rev.revid} (${rev.timestamp}). ` +
        `Licensed CC BY-SA 4.0. Source: ${sourceUrl}`;

      try {
        await upsert(
          'bronze_ch',
          'wikipedia_articles',
          [
            {
              qid,
              language: lang,
              title: link.title,
              revision_id: rev.revid,
              summary: summary.extract ?? null,
              description: summary.description ?? null,
              html,
              categories,
              source_url: sourceUrl,
              license: 'CC BY-SA 4.0',
              attribution,
              fetched_at: new Date().toISOString(),
              domain: 'real_estate',
              category,
            },
          ],
          'qid,language',
          1,
        );
        stats.articles_upserted++;
      } catch (err: any) {
        console.error(`    [upsert] ${qid}/${lang}: ${err.message}`);
        stats.articles_failed++;
      }
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\n${'='.repeat(60)}`);
  console.log('  WIKIPEDIA GENEVA INGEST COMPLETE');
  console.log(`  Qids resolved:                   ${stats.qids_resolved}`);
  console.log(`  Wikidata entities upserted:      ${stats.entities_upserted}`);
  console.log(`  Wikipedia articles upserted:     ${stats.articles_upserted}`);
  console.log(`  Articles skipped (unchanged):    ${stats.articles_skipped_unchanged}`);
  console.log(`  Articles failed:                 ${stats.articles_failed}`);
  console.log(`  Unresolvable seed titles:        ${stats.unresolvable_titles.length}`);
  console.log(`  Duration:                        ${elapsed}s`);
  console.log('='.repeat(60));

  // Coverage gate: ≥80% of resolved Qids must have ingested at least one row.
  const ratio = stats.qids_resolved > 0 ? stats.entities_upserted / stats.qids_resolved : 0;
  if (ratio < 0.8) {
    console.error(`  FAILED: only ${(ratio * 100).toFixed(1)}% entity coverage (<80%).`);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
