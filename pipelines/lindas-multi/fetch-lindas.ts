/**
 * LINDAS Multi — generic SPARQL-driven fetcher for Linked Data Switzerland datasets.
 *
 * Lands rows into re-llm bronze_ch.lindas_observations. One row per RDF subject;
 * predicates → object values land in `properties` JSONB (multi-valued predicates
 * become arrays).
 *
 * Two-phase fetch per dataset:
 *   1. List subjects (paged with LIMIT/OFFSET).
 *   2. For each batch of N subjects, fetch their full triple set in one SPARQL
 *      request via VALUES, then group by ?s in code.
 *
 * Usage:
 *   DATASET=curia npx tsx fetch-lindas.ts
 *   DATASET=bfe_ogd115_gest_bilanz npx tsx fetch-lindas.ts
 *
 * Env vars:
 *   DATASET                           - slug from datasets.ts (required)
 *   RE_LLM_SUPABASE_URL               - re-llm Supabase project URL (required)
 *   RE_LLM_SUPABASE_SERVICE_ROLE_KEY  - re-llm service_role key (required)
 *   SUBJECTS_PAGE                     - subjects per listing page (default 5000)
 *   PROPS_BATCH                       - subjects per properties fetch (default 200)
 *   UPSERT_BATCH                      - rows per DB upsert (default 200)
 */

import { upsert, verifyAccess, sleep } from '../_shared/re-llm.js';
import { DATASETS, SPARQL_ENDPOINT, lookupDataset, type LindasDataset } from './datasets.js';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const SCHEMA = 'bronze_ch';
const TABLE = 'lindas_observations';
const ON_CONFLICT = 'dataset_slug,subject_iri';

const DATASET_SLUG = process.env.DATASET ?? '';
if (!DATASET_SLUG) {
  console.error(`ERROR: DATASET env var required. Known: ${DATASETS.map((d) => d.slug).join(', ')}`);
  process.exit(1);
}

const SUBJECTS_PAGE = parseInt(process.env.SUBJECTS_PAGE ?? '', 10) || 5000;
const PROPS_BATCH = parseInt(process.env.PROPS_BATCH ?? '', 10) || 200;
const UPSERT_BATCH = parseInt(process.env.UPSERT_BATCH ?? '', 10) || 200;

const SPARQL_TIMEOUT_MS = 180_000;
const SPARQL_RETRIES = 3;
const SPARQL_BACKOFF_MS = [5_000, 15_000, 30_000];

// ---------------------------------------------------------------------------
// SPARQL CSV parser
// ---------------------------------------------------------------------------

/**
 * SPARQL results in `text/csv` follow RFC 4180. The endpoint emits IRIs unquoted
 * and string literals quoted with embedded quotes doubled. Newlines inside
 * literals are quoted-string-escaped.
 *
 * This implementation handles: header row, quoted fields, embedded commas,
 * escaped double quotes (""). Good enough for SPARQL CSV — not a generic CSV
 * library.
 */
function parseSparqlCsv(text: string): Array<Record<string, string>> {
  const rows: string[][] = [];
  let cur: string[] = [];
  let field = '';
  let inQuotes = false;
  let i = 0;
  while (i < text.length) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') {
          field += '"';
          i += 2;
          continue;
        }
        inQuotes = false;
        i++;
        continue;
      }
      field += c;
      i++;
      continue;
    }
    if (c === '"') {
      inQuotes = true;
      i++;
      continue;
    }
    if (c === ',') {
      cur.push(field);
      field = '';
      i++;
      continue;
    }
    if (c === '\n' || c === '\r') {
      cur.push(field);
      field = '';
      if (cur.length > 1 || cur[0] !== '') rows.push(cur);
      cur = [];
      // Swallow \r\n
      if (c === '\r' && text[i + 1] === '\n') i += 2;
      else i++;
      continue;
    }
    field += c;
    i++;
  }
  if (field.length > 0 || cur.length > 0) {
    cur.push(field);
    rows.push(cur);
  }

  if (rows.length === 0) return [];
  const header = rows[0];
  return rows.slice(1).map((row) => {
    const obj: Record<string, string> = {};
    for (let j = 0; j < header.length; j++) obj[header[j]] = row[j] ?? '';
    return obj;
  });
}

// ---------------------------------------------------------------------------
// SPARQL HTTP client
// ---------------------------------------------------------------------------

async function sparql(query: string): Promise<Array<Record<string, string>>> {
  for (let attempt = 0; attempt <= SPARQL_RETRIES; attempt++) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), SPARQL_TIMEOUT_MS);
    try {
      const body = new URLSearchParams({ query }).toString();
      const res = await fetch(SPARQL_ENDPOINT, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          Accept: 'text/csv',
          'User-Agent': 'camelote-data-pipelines/lindas-multi',
        },
        body,
        signal: controller.signal,
      });
      clearTimeout(timeoutId);
      if (!res.ok) {
        const text = (await res.text()).slice(0, 500);
        throw new Error(`SPARQL HTTP ${res.status}: ${text}`);
      }
      const text = await res.text();
      return parseSparqlCsv(text);
    } catch (err) {
      clearTimeout(timeoutId);
      if (attempt < SPARQL_RETRIES) {
        const delay = SPARQL_BACKOFF_MS[attempt] ?? 30_000;
        console.log(`  SPARQL attempt ${attempt + 1} failed (${err}); retrying in ${delay / 1000}s...`);
        await sleep(delay);
      } else {
        throw err;
      }
    }
  }
  throw new Error('unreachable');
}

// ---------------------------------------------------------------------------
// Query builders
// ---------------------------------------------------------------------------

function buildSubjectsQuery(ds: LindasDataset, offset: number): string {
  if (ds.kind === 'cube') {
    // Cube observations are linked from <cube_iri>/observation/ via cube:observation.
    // We also include the cube IRI itself so the cube definition row lands in bronze.
    return `
PREFIX cube: <https://cube.link/>
SELECT DISTINCT ?s
FROM <${ds.graph_iri}>
WHERE {
  {
    <${ds.cube_iri!}/observation/> cube:observation ?s .
  } UNION {
    BIND(<${ds.cube_iri!}> AS ?s)
    ?s ?p ?o .
  }
}
ORDER BY ?s
LIMIT ${SUBJECTS_PAGE} OFFSET ${offset}`;
  }
  // Registry-style: every distinct subject in the graph.
  return `
SELECT DISTINCT ?s
FROM <${ds.graph_iri}>
WHERE { ?s ?p ?o }
ORDER BY ?s
LIMIT ${SUBJECTS_PAGE} OFFSET ${offset}`;
}

function buildPropertiesQuery(ds: LindasDataset, subjectIris: string[]): string {
  const values = subjectIris.map((iri) => `<${iri}>`).join(' ');
  return `
SELECT ?s ?p ?o
FROM <${ds.graph_iri}>
WHERE {
  VALUES ?s { ${values} }
  ?s ?p ?o .
}`;
}

// ---------------------------------------------------------------------------
// Per-subject grouping
// ---------------------------------------------------------------------------

interface GroupedSubject {
  subject_iri: string;
  properties: Record<string, string | string[]>;
  language: string | null;
}

function groupTriples(triples: Array<Record<string, string>>): GroupedSubject[] {
  const map = new Map<string, GroupedSubject>();
  for (const t of triples) {
    const s = t.s;
    const p = t.p;
    const o = t.o;
    if (!s || !p) continue;
    let entry = map.get(s);
    if (!entry) {
      entry = { subject_iri: s, properties: {}, language: null };
      map.set(s, entry);
    }
    const existing = entry.properties[p];
    if (existing === undefined) {
      entry.properties[p] = o;
    } else if (Array.isArray(existing)) {
      existing.push(o);
    } else {
      entry.properties[p] = [existing, o];
    }
    // Heuristic: if a property is dcterms:language or schema:inLanguage, capture it.
    if (
      (p === 'http://purl.org/dc/terms/language' || p === 'http://schema.org/inLanguage') &&
      !entry.language &&
      typeof o === 'string'
    ) {
      entry.language = o.slice(0, 16);
    }
  }
  return Array.from(map.values());
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const ds = lookupDataset(DATASET_SLUG);

  console.log('='.repeat(64));
  console.log(`  LINDAS Multi — ${ds.slug}`);
  console.log(`  Descriptor:  ${ds.descriptor_url}`);
  console.log(`  Graph:       ${ds.graph_iri}`);
  console.log(`  Kind:        ${ds.kind}${ds.kind === 'cube' ? ` (cube=${ds.cube_iri})` : ''}`);
  console.log(`  Endpoint:    ${SPARQL_ENDPOINT}`);
  console.log(`  Target:      ${SCHEMA}.${TABLE} on re-llm`);
  console.log('='.repeat(64));

  const t0 = Date.now();
  await verifyAccess(SCHEMA, TABLE);

  // Phase 1: list subjects
  const subjects: string[] = [];
  let offset = 0;
  while (true) {
    console.log(`\n  Subjects: paging offset=${offset} (size=${SUBJECTS_PAGE})`);
    const rows = await sparql(buildSubjectsQuery(ds, offset));
    const batch = rows.map((r) => r.s).filter(Boolean);
    subjects.push(...batch);
    console.log(`  Got ${batch.length} subjects (cumulative ${subjects.length})`);
    if (batch.length < SUBJECTS_PAGE) break;
    offset += SUBJECTS_PAGE;
  }

  if (subjects.length === 0) {
    console.log('\n  No subjects in graph. Exiting.');
    return;
  }

  // Phase 2: fetch properties in batches
  let totalUpserted = 0;
  for (let i = 0; i < subjects.length; i += PROPS_BATCH) {
    const slice = subjects.slice(i, i + PROPS_BATCH);
    const triples = await sparql(buildPropertiesQuery(ds, slice));
    const grouped = groupTriples(triples);

    const records = grouped.map((g) => ({
      dataset_slug: ds.slug,
      dataset_iri: ds.descriptor_url,
      graph_iri: ds.graph_iri,
      sparql_endpoint: SPARQL_ENDPOINT,
      subject_iri: g.subject_iri,
      properties: g.properties,
      tags: ds.tags,
      language: g.language,
    }));

    const upserted = await upsert(SCHEMA, TABLE, records, ON_CONFLICT, UPSERT_BATCH);
    totalUpserted += upserted;
    console.log(
      `  Batch ${i / PROPS_BATCH + 1}/${Math.ceil(subjects.length / PROPS_BATCH)}: ` +
        `${slice.length} subjects → ${triples.length} triples → ${grouped.length} rows → ${upserted} upserted ` +
        `(total ${totalUpserted}/${subjects.length})`,
    );
  }

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log('\n' + '='.repeat(64));
  console.log('  IMPORT COMPLETE');
  console.log(`  Dataset:           ${ds.slug}`);
  console.log(`  Subjects fetched:  ${subjects.length}`);
  console.log(`  Rows upserted:     ${totalUpserted}`);
  console.log(`  Duration:          ${elapsed}s`);
  console.log('='.repeat(64));

  if (totalUpserted === 0 && subjects.length > 0) {
    console.error('  FAILED: zero rows upserted despite having subjects');
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
