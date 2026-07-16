/**
 * Stage 2 — aggregate bronze_ch.succession_notice_raw into estate-grain
 * bronze_ch.succession_events. Deduped per brief §5 on
 * (canton, event_type, deceased_name, deceased_dob, authority).
 *
 * Rebuilds from raw each run (raw is the source of truth) → idempotent, additive,
 * never lossy: an estate's 3× Erbenruf and its KK01→02→04→06 lifecycle collapse
 * to one row whose arrays accumulate every publication seen.
 *
 * Env: RE_LLM_SUPABASE_URL, RE_LLM_SUPABASE_SERVICE_ROLE_KEY, CANTON (default BE)
 */
import { supabase, verifyAccess, upsert } from '../_shared/re-llm.js';
import { buildDedupeKey } from './lib/parse.js';

const SCHEMA = 'bronze_ch';
const RAW = 'succession_notice_raw';
const EVENTS = 'succession_events';
const PAGE = 1000;
const CANTON = (process.env.CANTON ?? 'BE').toUpperCase();

interface RawRow {
  publication_id: string; tenant: string; canton: string;
  rubric: string | null; sub_rubric: string | null; event_type: string;
  publication_date: string; language: string | null; title: string | null;
  source_url: string; addition: string | null;
  content_json: Record<string, string | null> | null;
}

async function loadRaw(): Promise<RawRow[]> {
  const out: RawRow[] = [];
  for (let from = 0; ; from += PAGE) {
    const { data, error } = await supabase.schema(SCHEMA).from(RAW)
      .select('*').eq('canton', CANTON)
      .order('publication_date', { ascending: true }).range(from, from + PAGE - 1);
    if (error) { console.error(`  raw load error: ${error.message}`); process.exit(1); }
    if (!data || data.length === 0) break;
    out.push(...(data as RawRow[]));
    if (data.length < PAGE) break;
  }
  return out;
}

const uniq = <T,>(xs: T[]) => [...new Set(xs.filter((x) => x !== null && x !== undefined))] as T[];
const latest = (xs: (string | null)[]) => xs.filter(Boolean).at(-1) ?? null; // rows are pub-date ascending

function main() {
  return (async () => {
    console.log(`\namtsblatt-be — build events (canton=${CANTON})`);
    await verifyAccess(SCHEMA, EVENTS);
    const raw = await loadRaw();
    console.log(`  raw rows: ${raw.length}`);

    const groups = new Map<string, RawRow[]>();
    for (const r of raw) {
      const c = r.content_json ?? {};
      const key = buildDedupeKey({
        canton: r.canton,
        eventType: r.event_type as never,
        deceasedName: c.deceased_name ?? null,
        deceasedDob: c.deceased_dob ?? null,
        authority: c.authority ?? null,
      });
      (groups.get(key) ?? groups.set(key, []).get(key)!).push(r);
    }
    console.log(`  distinct estate-events: ${groups.size}`);

    const rows = [...groups.entries()].map(([dedupe_key, g]) => {
      // g is publication-date ascending; pick latest-non-null for scalar fields
      const cj = (r: RawRow) => r.content_json ?? {};
      const pubDates = uniq(g.map((r) => r.publication_date)).sort();
      return {
        canton: g[0].canton,
        event_type: g[0].event_type,
        deceased_name: latest(g.map((r) => cj(r).deceased_name ?? null)),
        deceased_prename: latest(g.map((r) => cj(r).deceased_prename ?? null)),
        deceased_surname: latest(g.map((r) => cj(r).deceased_surname ?? null)),
        deceased_dob: latest(g.map((r) => cj(r).deceased_dob ?? null)),
        deceased_dod: latest(g.map((r) => cj(r).deceased_dod ?? null)),
        deceased_last_domicile: latest(g.map((r) => cj(r).deceased_last_domicile ?? null)),
        deceased_heimatort: latest(g.map((r) => cj(r).deceased_heimatort ?? null)),
        authority: latest(g.map((r) => cj(r).authority ?? null)),
        authority_municipality_id: latest(g.map((r) => cj(r).authority_municipality_id ?? null)),
        deadline_date: g.map((r) => cj(r).deadline_date ?? null).filter(Boolean).sort().at(-1) ?? null,
        first_publication_date: pubDates[0],
        latest_publication_date: pubDates.at(-1),
        publication_dates: pubDates,
        languages: uniq(g.map((r) => r.language)),
        rubric: g[0].rubric,
        sub_rubrics: uniq(g.map((r) => r.sub_rubric)),
        refused_legacy: g.some((r) => (r.addition ?? '').toLowerCase() === 'refusedlegacy'),
        publication_ids: g.map((r) => r.publication_id),
        source_urls: g.map((r) => r.source_url),
        primary_source_url: g.at(-1)!.source_url,
        notice_count: g.length,
        body_text: latest(g.map((r) => cj(r).body_text ?? null)),
        // linked_egrid intentionally omitted — never fabricated; set by a separate matcher
        dedupe_key,
      };
    });

    const n = await upsert(SCHEMA, EVENTS, rows, 'dedupe_key', 200);
    console.log(`  upserted ${n} estate-events`);
  })();
}

main().catch((err) => { console.error('Fatal:', err); process.exit(1); });
