/**
 * wiki-recent — Wikipedia edit-velocity log for watched Geneva-RE entities.
 *
 * For each (qid, language) row in bronze_ch.wikipedia_articles, fetch revisions
 * in the lookback window and upsert into bronze_ch.wikipedia_edit_log.
 *
 * Daily cron with a 7-day overlap window covers all but the longest gaps.
 * One row per revision; small table by design (~tens to hundreds of rows/day).
 *
 * Env:
 *   RE_LLM_SUPABASE_URL, RE_LLM_SUPABASE_SERVICE_ROLE_KEY (required)
 *   LOOKBACK_DAYS (default 7) — how far back to fetch revisions
 *   ONLY_LANGUAGE (optional) — restrict to 'fr' or 'de'
 */

import { createClient } from '@supabase/supabase-js';
import { sleep } from '../_shared/re-llm.js';

const LOOKBACK_DAYS = parseInt(process.env.LOOKBACK_DAYS ?? '', 10) || 7;
const ONLY_LANGUAGE = process.env.ONLY_LANGUAGE;
const POLITENESS_MS = 250;
const RVLIMIT = 50;

const supabase = createClient(
  process.env.RE_LLM_SUPABASE_URL!,
  process.env.RE_LLM_SUPABASE_SERVICE_ROLE_KEY!,
);

interface WatchedPage {
  qid: string;
  language: string;
  title: string;
}

interface ApiRevision {
  revid: number;
  parentid: number;
  user?: string;
  anon?: '';
  temp?: '';
  timestamp: string;
  size: number;
  comment?: string;
  tags?: string[];
}

interface ApiResponse {
  query?: {
    pages: Record<
      string,
      { pageid?: number; title: string; missing?: ''; revisions?: ApiRevision[] }
    >;
  };
}

async function fetchRevisions(language: string, title: string, sinceIso: string): Promise<{ pageId: number | null; revisions: ApiRevision[] }> {
  const params = new URLSearchParams({
    action: 'query',
    prop: 'revisions',
    titles: title,
    rvprop: 'ids|user|comment|timestamp|size|tags|flags',
    rvlimit: String(RVLIMIT),
    rvend: sinceIso,
    rvdir: 'older', // newest first
    format: 'json',
    formatversion: '1',
  });
  const url = `https://${language}.wikipedia.org/w/api.php?${params.toString()}`;
  const res = await fetch(url, {
    headers: {
      'User-Agent': 'camelote-data-pipelines/wiki-recent (https://github.com/camelote-command-center)',
      Accept: 'application/json',
    },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} on ${language}.wikipedia.org for "${title}"`);
  const data = (await res.json()) as ApiResponse;
  const pages = Object.values(data.query?.pages ?? {});
  if (pages.length === 0 || pages[0].missing !== undefined) {
    return { pageId: null, revisions: [] };
  }
  return { pageId: pages[0].pageid ?? null, revisions: pages[0].revisions ?? [] };
}

async function loadWatched(): Promise<WatchedPage[]> {
  const { data, error } = await supabase
    .schema('bronze_ch')
    .from('wikipedia_articles')
    .select('qid, language, title')
    .order('qid');
  if (error) throw new Error(`load watched: ${error.message}`);
  return (data ?? []).filter((r): r is WatchedPage => Boolean(r.qid && r.language && r.title));
}

async function main() {
  console.log('='.repeat(64));
  console.log('  wiki-recent — Wikipedia edit-velocity log');
  console.log(`  Lookback: ${LOOKBACK_DAYS} days`);
  console.log(`  ONLY_LANGUAGE: ${ONLY_LANGUAGE ?? '(all)'}`);
  console.log(`  Target: bronze_ch.wikipedia_edit_log on re-llm`);
  console.log('='.repeat(64));

  const t0 = Date.now();
  const sinceIso = new Date(Date.now() - LOOKBACK_DAYS * 86_400_000).toISOString();
  console.log(`  Window since: ${sinceIso}`);

  const watched = await loadWatched();
  const filtered = ONLY_LANGUAGE ? watched.filter((w) => w.language === ONLY_LANGUAGE) : watched;
  console.log(`  Watched pages: ${filtered.length}`);

  let totalRevs = 0;
  let totalUpserted = 0;
  let pagesWithEdits = 0;
  let failed = 0;

  for (const page of filtered) {
    try {
      const { pageId, revisions } = await fetchRevisions(page.language, page.title, sinceIso);
      if (revisions.length === 0) {
        await sleep(POLITENESS_MS);
        continue;
      }
      pagesWithEdits++;
      totalRevs += revisions.length;

      const rows = revisions.map((r) => ({
        qid: page.qid,
        language: page.language,
        page_title: page.title,
        page_id: pageId,
        rev_id: r.revid,
        parent_rev_id: r.parentid || null,
        user_name: r.user ?? null,
        is_anonymous: r.anon !== undefined || r.temp !== undefined,
        comment: r.comment?.slice(0, 1000) ?? null,
        edit_timestamp: r.timestamp,
        size_bytes: r.size ?? null,
        tags: r.tags && r.tags.length > 0 ? r.tags : null,
      }));

      const { data, error } = await supabase
        .schema('bronze_ch')
        .from('wikipedia_edit_log')
        .upsert(rows, { onConflict: 'rev_id,language', count: 'exact' })
        .select('id');
      if (error) {
        console.error(`  ${page.qid}/${page.language} "${page.title}": upsert failed: ${error.message}`);
        failed++;
      } else {
        const n = data?.length ?? rows.length;
        totalUpserted += n;
        console.log(`  ${page.qid}/${page.language} "${page.title}": ${revisions.length} revs, ${n} upserted`);
      }
    } catch (err) {
      console.error(`  ${page.qid}/${page.language} "${page.title}": ${err}`);
      failed++;
    }
    await sleep(POLITENESS_MS);
  }

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log('\n' + '='.repeat(64));
  console.log('  IMPORT COMPLETE');
  console.log(`  Watched pages:       ${filtered.length}`);
  console.log(`  Pages with edits:    ${pagesWithEdits}`);
  console.log(`  Total revisions:     ${totalRevs}`);
  console.log(`  Rows upserted:       ${totalUpserted}`);
  console.log(`  Failed:              ${failed}`);
  console.log(`  Duration:            ${elapsed}s`);
  console.log('='.repeat(64));
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
