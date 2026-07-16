/**
 * Stage 1 — fetch per-publication content XML for succession candidates and
 * distil into bronze_ch.succession_notice_raw.
 *
 * Candidates come from the DB view bronze_ch.succession_notice_candidates
 * (already-ingested metadata; no re-ingest). The list API omits the notice body,
 * so we fetch /api/v1/publications/{id}/xml per candidate. Idempotent: publications
 * already in succession_notice_raw are skipped.
 *
 * Env:
 *   RE_LLM_SUPABASE_URL, RE_LLM_SUPABASE_SERVICE_ROLE_KEY  (required)
 *   CANTON    default 'BE'
 *   SINCE     optional YYYY-MM-DD — only candidates on/after this pub date
 *   MAX       optional cap on notices fetched this run (0 = no cap)
 *   REFETCH   '1' to re-fetch even if already in raw
 */
import { supabase, verifyAccess, upsert, sleep } from '../_shared/re-llm.js';
import { parseNoticeXml, type Notice } from './lib/parse.js';

const SCHEMA = 'bronze_ch';
const RAW = 'succession_notice_raw';
const VIEW = 'succession_notice_candidates';
const API = 'https://amtsblattportal.ch/api/v1/publications';
const UA = 'camelote-data-pipelines/amtsblatt-be (real-estate intelligence; contact via camelote-command-center)';
const POLITENESS_MS = 300;
const PAGE = 1000;

const CANTON = (process.env.CANTON ?? 'BE').toUpperCase();
const SINCE = process.env.SINCE || null;
const MAX = parseInt(process.env.MAX ?? '0', 10) || 0;
const REFETCH = process.env.REFETCH === '1';

interface Candidate {
  id: string; tenant: string; canton: string;
  rubric: string | null; sub_rubric: string | null;
  publication_date: string; language: string | null; event_type: string;
}

function sourceUrl(id: string): string {
  return `https://amtsblattportal.ch/api/v1/publications/${id}/xml`;
}

async function loadCandidates(): Promise<Candidate[]> {
  const out: Candidate[] = [];
  for (let from = 0; ; from += PAGE) {
    let q = supabase.schema(SCHEMA).from(VIEW).select('*')
      .eq('canton', CANTON)
      .order('publication_date', { ascending: false })
      .range(from, from + PAGE - 1);
    if (SINCE) q = q.gte('publication_date', SINCE);
    const { data, error } = await q;
    if (error) { console.error(`  candidate load error: ${error.message}`); process.exit(1); }
    if (!data || data.length === 0) break;
    out.push(...(data as Candidate[]));
    if (data.length < PAGE) break;
  }
  return out;
}

async function loadFetchedIds(): Promise<Set<string>> {
  const ids = new Set<string>();
  if (REFETCH) return ids;
  for (let from = 0; ; from += PAGE) {
    const { data, error } = await supabase.schema(SCHEMA).from(RAW)
      .select('publication_id').eq('canton', CANTON).range(from, from + PAGE - 1);
    if (error) { console.error(`  raw id load error: ${error.message}`); break; }
    if (!data || data.length === 0) break;
    for (const r of data) ids.add((r as { publication_id: string }).publication_id);
    if (data.length < PAGE) break;
  }
  return ids;
}

async function fetchXml(id: string): Promise<string> {
  const url = sourceUrl(id);
  for (let attempt = 0; attempt < 4; attempt++) {
    try {
      const res = await fetch(url, { headers: { Accept: 'application/xml', 'User-Agent': UA } });
      if (res.status === 429 || res.status >= 500) {
        await sleep((attempt + 1) * 4000);
        continue;
      }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.text();
    } catch (err) {
      if (attempt === 3) throw err;
      await sleep((attempt + 1) * 4000);
    }
  }
  throw new Error('unreachable');
}

function toRow(c: Candidate, n: Notice, xml: string) {
  return {
    publication_id: c.id,
    tenant: c.tenant,
    canton: c.canton,
    rubric: n.rubric ?? c.rubric,
    sub_rubric: n.subRubric ?? c.sub_rubric,
    event_type: n.eventType,
    publication_date: n.publicationDate ?? c.publication_date,
    language: n.language ?? c.language,
    title: n.title,
    source_url: sourceUrl(c.id),
    addition: n.addition,
    content_json: {
      deceased_prename: n.deceasedPrename,
      deceased_surname: n.deceasedSurname,
      deceased_name: n.deceasedName,
      deceased_dob: n.deceasedDob,
      deceased_dod: n.deceasedDod,
      deceased_heimatort: n.deceasedHeimatort,
      deceased_last_domicile: n.deceasedLastDomicile,
      authority: n.authority,
      authority_municipality_id: n.authorityMunicipalityId,
      deadline_date: n.deadlineDate,
      body_text: n.bodyText,
    },
    raw_xml: xml,
    parse_status: n.deceasedSurname ? 'ok' : 'no_person',
  };
}

async function main() {
  console.log('='.repeat(64));
  console.log(`  amtsblatt-be — succession content fetch (canton=${CANTON})`);
  console.log(`  since=${SINCE ?? 'ALL'} max=${MAX || '∞'} refetch=${REFETCH}`);
  console.log('='.repeat(64));
  await verifyAccess(SCHEMA, RAW);

  // Refresh the candidate matview so notices ingested since last run are visible.
  // Non-fatal: on failure we proceed against the existing (possibly stale) matview.
  const { error: refreshErr } = await supabase.rpc('refresh_succession_candidates');
  console.log(refreshErr ? `  ⚠ candidate refresh skipped: ${refreshErr.message}` : '  ✓ candidate matview refreshed');

  const [candidates, fetched] = await Promise.all([loadCandidates(), loadFetchedIds()]);
  const todo = candidates.filter((c) => !fetched.has(c.id));
  console.log(`  candidates=${candidates.length}  already-fetched=${fetched.size}  to-fetch=${todo.length}`);

  const batch: Record<string, unknown>[] = [];
  let done = 0, ok = 0, noPerson = 0, errors = 0;
  const t0 = Date.now();

  for (const c of todo) {
    if (MAX && done >= MAX) { console.log(`  reached MAX=${MAX}, stopping`); break; }
    try {
      const xml = await fetchXml(c.id);
      const n = parseNoticeXml({ publicationId: c.id, tenant: c.tenant, canton: c.canton, xml });
      const row = toRow(c, n, xml);
      if (row.parse_status === 'ok') ok++; else noPerson++;
      batch.push(row);
    } catch (err) {
      errors++;
      console.error(`  ✗ ${c.id} (${c.sub_rubric}): ${(err as Error).message}`);
    }
    done++;
    if (batch.length >= 100) {
      await upsert(SCHEMA, RAW, batch.splice(0), 'publication_id', 100);
    }
    if (done % 250 === 0) {
      const rate = (done / ((Date.now() - t0) / 1000)).toFixed(1);
      console.log(`  … ${done}/${todo.length} (ok=${ok} noPerson=${noPerson} err=${errors}) ${rate}/s`);
    }
    await sleep(POLITENESS_MS);
  }
  if (batch.length) await upsert(SCHEMA, RAW, batch.splice(0), 'publication_id', 100);

  const elapsed = ((Date.now() - t0) / 1000).toFixed(0);
  console.log('\n' + '='.repeat(64));
  console.log(`  FETCH DONE — processed=${done} ok=${ok} noPerson=${noPerson} err=${errors} in ${elapsed}s`);
  console.log('='.repeat(64));
}

main().catch((err) => { console.error('Fatal:', err); process.exit(1); });
