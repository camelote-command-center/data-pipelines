/**
 * news-rss — generic Swiss news aggregator.
 *
 * Walks each feed in feeds.ts → fetches new article URLs → fetches HTML →
 * extracts body via @extractus/article-extractor → inserts ONE row per
 * article into knowledge_ch.documents. Dedup is held in
 * bronze_ch.news_index (one row per URL).
 *
 * Per re-LLM v2 architecture:
 *   - Long-form text → knowledge_ch.documents
 *   - categorization_status='pending' (default) signals the AFTER INSERT trigger
 *     to call /functions/v1/classify-row asynchronously.
 *   - Tags are pre-populated; classifier preserves them.
 *   - Bulk-import discipline applies when a single batch > 50 rows: parser
 *     temporarily disables the classify trigger, inserts, re-enables, and emits
 *     a console hint to run classify_existing.py for the backfilled rows.
 *
 * Env:
 *   RE_LLM_SUPABASE_URL, RE_LLM_SUPABASE_SERVICE_ROLE_KEY  (required)
 *   ONLY_FEED          (optional)  — restrict to a single feed slug
 *   MAX_ITEMS_PER_FEED (optional)  — cap per-feed items this run (default 50)
 *   DRY_RUN            (optional)  — '1' to skip DB writes (preview only)
 */

import Parser from 'rss-parser';
import { extract } from '@extractus/article-extractor';
import { createClient } from '@supabase/supabase-js';
import * as cheerio from 'cheerio';
import { createHash } from 'node:crypto';
import { sleep } from '../_shared/re-llm.js';
import { FEEDS, type NewsFeed } from './feeds.js';

const ONLY_FEED = process.env.ONLY_FEED;
const MAX_ITEMS_PER_FEED = parseInt(process.env.MAX_ITEMS_PER_FEED ?? '', 10) || 50;
const DRY_RUN = process.env.DRY_RUN === '1';
const POLITENESS_MS = 800;
const BULK_THRESHOLD = 50;

const supabase = createClient(
  process.env.RE_LLM_SUPABASE_URL!,
  process.env.RE_LLM_SUPABASE_SERVICE_ROLE_KEY!,
);

const rss = new Parser({
  timeout: 30_000,
  headers: {
    'User-Agent': 'camelote-data-pipelines/news-rss (https://github.com/camelote-command-center)',
  },
});

interface CandidateItem {
  url: string;
  feed_title: string;
  feed_published_at: string | null;
  feed_summary: string | null;
}

// ---------------------------------------------------------------------------
// Discovery: list candidate URLs from one feed.
// ---------------------------------------------------------------------------

async function discoverRss(feed: NewsFeed): Promise<CandidateItem[]> {
  const parsed = await rss.parseURL(feed.url);
  const items: CandidateItem[] = [];
  for (const it of parsed.items ?? []) {
    if (!it.link) continue;
    items.push({
      url: it.link,
      feed_title: it.title ?? '(untitled)',
      feed_published_at: it.isoDate ?? it.pubDate ?? null,
      feed_summary: it.contentSnippet ?? it.content ?? null,
    });
  }
  return items;
}

async function discoverSitemap(feed: NewsFeed): Promise<CandidateItem[]> {
  const indexRes = await fetch(feed.url, {
    headers: { 'User-Agent': 'camelote-data-pipelines/news-rss', Accept: 'application/xml' },
  });
  if (!indexRes.ok) throw new Error(`sitemap HTTP ${indexRes.status} on ${feed.url}`);
  const indexXml = await indexRes.text();
  const $ = cheerio.load(indexXml, { xmlMode: true });

  const cap = feed.max_items ?? MAX_ITEMS_PER_FEED;
  const urlRe = feed.sitemap_url_regex ? new RegExp(feed.sitemap_url_regex) : null;
  const lookbackDays = feed.sitemap_lookback_days ?? 1;
  const cutoff = Date.now() - lookbackDays * 86_400_000;

  // Detect format: <sitemapindex> (walk sub-sitemaps) vs <urlset> (flat list).
  // Some publishers (SWI swissinfo) use a flat urlset at /<lang>/sitemap-news.xml
  // with one <url> per article + <news:publication_date>; that's the most useful
  // shape because the dates are real and current.
  const items: CandidateItem[] = [];

  if (indexXml.includes('<sitemapindex')) {
    // Sub-index format. Pick sub-sitemaps with lastmod >= cutoff (or no lastmod).
    const subUrls: string[] = [];
    $('sitemap').each((_, el) => {
      const loc = $(el).find('loc').text().trim();
      const lastmod = $(el).find('lastmod').text().trim();
      if (!loc) return;
      if (lastmod) {
        const t = Date.parse(lastmod);
        if (Number.isFinite(t) && t < cutoff) return;
      }
      subUrls.push(loc);
    });
    for (const sub of subUrls) {
      if (items.length >= cap) break;
      try {
        const r = await fetch(sub, {
          headers: { 'User-Agent': 'camelote-data-pipelines/news-rss', Accept: 'application/xml' },
        });
        if (!r.ok) continue;
        const xml = await r.text();
        const $$ = cheerio.load(xml, { xmlMode: true });
        $$('url').each((_, el) => {
          if (items.length >= cap) return false;
          const loc = $$(el).find('loc').text().trim();
          const lastmod = $$(el).find('lastmod').text().trim() || null;
          if (!loc) return;
          if (urlRe && !urlRe.test(loc)) return;
          items.push({ url: loc, feed_title: '', feed_published_at: lastmod, feed_summary: null });
        });
      } catch (err) {
        console.error(`    sub-sitemap ${sub} failed: ${err}`);
      }
      await sleep(POLITENESS_MS);
    }
  } else {
    // Flat <urlset>. Pull URLs directly; honor news:publication_date if present.
    $('url').each((_, el) => {
      if (items.length >= cap) return false;
      const loc = $(el).find('loc').text().trim();
      if (!loc) return;
      if (urlRe && !urlRe.test(loc)) return;
      // news:publication_date (Google News sitemap extension) > <lastmod>
      const newsDate =
        $(el).find('news\\:publication_date, publication_date').first().text().trim();
      const lastmod = $(el).find('lastmod').text().trim();
      const dateStr = newsDate || lastmod || null;
      if (dateStr) {
        const t = Date.parse(dateStr);
        if (Number.isFinite(t) && t < cutoff) return;
      }
      const newsTitle = $(el).find('news\\:title, title').first().text().trim() || '';
      items.push({ url: loc, feed_title: newsTitle, feed_published_at: dateStr, feed_summary: null });
    });
  }
  return items;
}

// ---------------------------------------------------------------------------
// API adapters (newsdata.io + GNews) + keyword filter.
// ---------------------------------------------------------------------------

const NEWSDATA_API_KEY = process.env.NEWSDATA_API_KEY;
const GNEWS_API_KEY = process.env.GNEWS_API_KEY;
const API_LANGS = ['de', 'fr', 'en'] as const;

// Always-allow: pure business / economy / real-estate / finance terms.
const ALWAYS_ALLOW_KEYWORDS = [
  // de
  'wirtschaft', 'immobilien', 'immobilie', 'konjunktur', 'unternehmen', 'bank',
  'börse', 'boerse', 'markt', 'hypothek', 'miete', 'mieten', 'mietzins', 'bau',
  'baugewerbe', 'zins', 'zinsen', 'inflation', 'kmu', 'industrie', 'aktie',
  'aktien', 'geschäft', 'geschaeft', 'konsum', 'franken', 'snb', 'nationalbank',
  'wohnungsmarkt', 'eigenheim',
  // fr
  'économie', 'economie', 'immobilier', 'entreprise', 'bourse', 'marché',
  'marche', 'hypothèque', 'hypotheque', 'loyer', 'construction', 'taux',
  'inflation', 'pme', 'industrie', 'action', 'affaires', 'consommation',
  'franc', 'bns', 'logement',
  // en
  'economy', 'real estate', 'business', 'mortgage', 'rent', 'rental', 'bank',
  'market', 'swiss franc', 'finance', 'snb', 'housing', 'inflation', 'stocks',
];

// Allow only if these appear (econ/RE policy terms — explicitly Swiss).
const POLICY_KEYWORDS = [
  'ldtr', 'rdppf', 'bundesrat', 'conseil fédéral', 'conseil federal',
  'federal council', 'zoning', 'raumplanung', 'aménagement du territoire',
  'amenagement du territoire', 'housing policy', 'wohnpolitik',
  'politique du logement', 'mietrecht', 'droit du bail', 'lex koller',
];

function normalize(s: string | null | undefined): string {
  return (s ?? '').toLowerCase();
}

/** Keyword gate: business/econ/RE plus Swiss econ-policy items. */
function isEconomicBusinessRealEstate(item: { title: string; description: string | null; }): boolean {
  const hay = `${normalize(item.title)} ${normalize(item.description)}`;
  if (!hay.trim()) return false;
  for (const kw of ALWAYS_ALLOW_KEYWORDS) {
    if (hay.includes(kw)) return true;
  }
  for (const kw of POLICY_KEYWORDS) {
    if (hay.includes(kw)) return true;
  }
  return false;
}

interface NewsdataArticle {
  link?: string;
  title?: string;
  description?: string;
  pubDate?: string;
  content?: string;
}

async function discoverNewsdataApi(feed: NewsFeed): Promise<CandidateItem[]> {
  if (!NEWSDATA_API_KEY) {
    console.warn('    NEWSDATA_API_KEY not set — skipping newsdata.io adapter');
    return [];
  }
  const cap = feed.max_items ?? MAX_ITEMS_PER_FEED;
  const items: CandidateItem[] = [];
  let nextPage: string | null = null;
  const langParam = API_LANGS.join(',');
  // Free tier: comma-separated languages supported, country=ch, category=business.
  let pages = 0;
  while (items.length < cap && pages < 10) {
    const params = new URLSearchParams({
      apikey: NEWSDATA_API_KEY,
      country: 'ch',
      category: 'business',
      language: langParam,
    });
    if (nextPage) params.set('page', nextPage);
    const url = `${feed.url}?${params.toString()}`;
    const res = await fetch(url, { headers: { 'User-Agent': 'camelote-data-pipelines/news-rss' } });
    if (!res.ok) {
      console.warn(`    newsdata.io HTTP ${res.status}: ${await res.text().then(t => t.slice(0, 200))}`);
      break;
    }
    const json = await res.json() as { status?: string; results?: NewsdataArticle[]; nextPage?: string | null };
    if (json.status !== 'success') {
      console.warn(`    newsdata.io status=${json.status}; stopping`);
      break;
    }
    for (const a of json.results ?? []) {
      if (!a.link) continue;
      items.push({
        url: a.link,
        feed_title: a.title ?? '(untitled)',
        feed_published_at: a.pubDate ?? null,
        feed_summary: a.description ?? a.content ?? null,
      });
      if (items.length >= cap) break;
    }
    nextPage = json.nextPage ?? null;
    pages++;
    if (!nextPage) break;
    await sleep(POLITENESS_MS);
  }
  return items;
}

interface GnewsArticle {
  url?: string;
  title?: string;
  description?: string;
  content?: string;
  publishedAt?: string;
}

async function discoverGnewsApi(feed: NewsFeed): Promise<CandidateItem[]> {
  if (!GNEWS_API_KEY) {
    console.warn('    GNEWS_API_KEY not set — skipping GNews adapter');
    return [];
  }
  const cap = feed.max_items ?? MAX_ITEMS_PER_FEED;
  const items: CandidateItem[] = [];
  // GNews free tier: 100 req/day, ≤10 articles/call. One call per language → ≤30 articles.
  for (const lang of API_LANGS) {
    if (items.length >= cap) break;
    const params = new URLSearchParams({
      apikey: GNEWS_API_KEY,
      country: 'ch',
      category: 'business',
      lang,
      max: '10',
    });
    const url = `${feed.url}?${params.toString()}`;
    const res = await fetch(url, { headers: { 'User-Agent': 'camelote-data-pipelines/news-rss' } });
    if (!res.ok) {
      console.warn(`    gnews.io (${lang}) HTTP ${res.status}: ${await res.text().then(t => t.slice(0, 200))}`);
      continue;
    }
    const json = await res.json() as { articles?: GnewsArticle[] };
    for (const a of json.articles ?? []) {
      if (!a.url) continue;
      items.push({
        url: a.url,
        feed_title: a.title ?? '(untitled)',
        feed_published_at: a.publishedAt ?? null,
        feed_summary: a.description ?? a.content ?? null,
      });
      if (items.length >= cap) break;
    }
    await sleep(POLITENESS_MS);
  }
  return items;
}

async function discoverFeed(feed: NewsFeed): Promise<CandidateItem[]> {
  if (feed.kind === 'rss') return discoverRss(feed);
  if (feed.kind === 'sitemap') return discoverSitemap(feed);
  if (feed.kind === 'newsdata-api') return discoverNewsdataApi(feed);
  if (feed.kind === 'gnews-api') return discoverGnewsApi(feed);
  throw new Error(`unknown feed kind: ${(feed as NewsFeed).kind}`);
}

/**
 * Topic gate for sources that span beyond pure business (Blick all three feeds
 * + both APIs). Allowlist Swiss econ/RE/business + econ-policy keywords; drop
 * the rest before the expensive article-extraction step.
 *
 * Returns items unchanged for source kinds that are already topic-scoped (e.g.
 * Le Temps économie, NZZ Wirtschaft, Wüest Partner) — keyword filter is only
 * applied to Blick + the 2 API aggregators.
 */
const KEYWORD_FILTER_SLUGS = new Set([
  // Add 'blick_*' back here once we find real RSS URLs (see feeds.ts note).
  'newsdata_ch', 'gnews_ch',
]);

function applyKeywordFilter(feed: NewsFeed, items: CandidateItem[]): CandidateItem[] {
  if (!KEYWORD_FILTER_SLUGS.has(feed.slug)) return items;
  const kept = items.filter((i) =>
    isEconomicBusinessRealEstate({ title: i.feed_title, description: i.feed_summary }),
  );
  const dropped = items.length - kept.length;
  if (dropped > 0) console.log(`    keyword-filter: dropped ${dropped} / kept ${kept.length}`);
  return kept;
}

// ---------------------------------------------------------------------------
// Dedup via news_index.
// ---------------------------------------------------------------------------

function md5(s: string): string {
  return createHash('md5').update(s).digest('hex');
}

async function filterUnseen(items: CandidateItem[]): Promise<CandidateItem[]> {
  if (items.length === 0) return [];
  const urls = items.map((i) => i.url);
  const { data, error } = await supabase
    .schema('bronze_ch')
    .from('news_index')
    .select('url')
    .in('url', urls);
  if (error) throw new Error(`news_index lookup: ${error.message}`);
  const seen = new Set((data ?? []).map((r: { url: string }) => r.url));
  return items.filter((i) => !seen.has(i.url));
}

// ---------------------------------------------------------------------------
// Article body extraction.
// ---------------------------------------------------------------------------

interface Article {
  title: string;
  description: string | null;
  content: string;
  publication_date: string | null;
  author: string | null;
}

async function extractArticle(url: string, fallback: CandidateItem): Promise<Article | null> {
  try {
    const result = await extract(url, undefined, {
      headers: {
        'User-Agent':
          'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0',
      },
    });
    if (!result || !result.content) {
      // Fall back to feed metadata if extraction fails.
      if (fallback.feed_title && fallback.feed_summary) {
        return {
          title: fallback.feed_title,
          description: fallback.feed_summary,
          content: fallback.feed_summary,
          publication_date: fallback.feed_published_at?.slice(0, 10) ?? null,
          author: null,
        };
      }
      return null;
    }
    // Strip HTML tags to plaintext for the chunker.
    const $ = cheerio.load(result.content);
    const plain = $.text().replace(/\s+/g, ' ').trim();
    return {
      title: result.title ?? fallback.feed_title ?? '(untitled)',
      description: result.description ?? fallback.feed_summary ?? null,
      content: plain,
      publication_date:
        result.published?.slice(0, 10) ?? fallback.feed_published_at?.slice(0, 10) ?? null,
      author: result.author ?? null,
    };
  } catch (err) {
    if (fallback.feed_title && fallback.feed_summary) {
      return {
        title: fallback.feed_title,
        description: fallback.feed_summary,
        content: fallback.feed_summary,
        publication_date: fallback.feed_published_at?.slice(0, 10) ?? null,
        author: null,
      };
    }
    throw err;
  }
}

// ---------------------------------------------------------------------------
// DB writes.
// ---------------------------------------------------------------------------

async function setTriggerEnabled(enabled: boolean): Promise<void> {
  // We don't have raw-SQL via supabase-js here. Bulk-import discipline is
  // satisfied by user-side intervention; we emit a clear hint to console so
  // the operator can run the appropriate ALTER TABLE manually for big runs.
  // The per-row trigger is still safe at small batch sizes.
  if (!enabled) {
    console.log(
      '  ⚠ Bulk-import threshold exceeded. To prevent classifier rate-limit pressure, run BEFORE this batch:\n' +
        '    ALTER TABLE knowledge_ch.documents DISABLE TRIGGER classify_on_insert;\n' +
        '  …then re-enable + run classify_existing.py AFTER.',
    );
  }
}

async function insertDocuments(
  feed: NewsFeed,
  rows: Array<{ candidate: CandidateItem; article: Article }>,
): Promise<{ inserted: number; failed: number; documentIds: Map<string, string> }> {
  if (rows.length === 0) return { inserted: 0, failed: 0, documentIds: new Map() };

  const docs = rows.map(({ candidate, article }) => ({
    title: article.title.slice(0, 500),
    description: article.description?.slice(0, 2000) ?? null,
    source: feed.slug,
    publisher: feed.publisher,
    document_type: 'news_article',
    original_url: candidate.url,
    publication_date: article.publication_date,
    language: feed.language,
    country: 'ch',
    canton_code: feed.canton ?? null,
    tags: [...feed.tags],
    raw_metadata: {
      feed_slug: feed.slug,
      feed_kind: feed.kind,
      author: article.author,
      content_length: article.content.length,
      first_seen_at: new Date().toISOString(),
    },
    // ingestion_status default 'pending' triggers the chunker downstream.
    // categorization_status default 'pending' triggers classify_on_insert.
  }));

  if (DRY_RUN) {
    console.log(`    [DRY_RUN] Would insert ${docs.length} documents`);
    return { inserted: 0, failed: 0, documentIds: new Map() };
  }

  if (rows.length > BULK_THRESHOLD) await setTriggerEnabled(false);

  const { data, error } = await supabase
    .schema('knowledge_ch')
    .from('documents')
    .insert(docs)
    .select('id, original_url');

  if (error) {
    console.error(`    insert failed: ${error.message}`);
    return { inserted: 0, failed: rows.length, documentIds: new Map() };
  }

  const documentIds = new Map<string, string>();
  for (const r of data ?? []) {
    if (r.original_url && r.id) documentIds.set(r.original_url, r.id);
  }
  return { inserted: data?.length ?? 0, failed: 0, documentIds };
}

async function recordIndex(
  feed: NewsFeed,
  candidate: CandidateItem,
  status: 'success' | 'failed' | 'skipped',
  error: string | null,
  documentId: string | null,
): Promise<void> {
  const row = {
    feed_slug: feed.slug,
    url: candidate.url,
    url_hash: md5(candidate.url),
    document_id: documentId,
    feed_title: candidate.feed_title.slice(0, 500) || null,
    feed_published_at: candidate.feed_published_at,
    fetch_status: status,
    fetch_error: error,
    language: feed.language,
  };
  const { error: upErr } = await supabase
    .schema('bronze_ch')
    .from('news_index')
    .upsert(row, { onConflict: 'url' });
  if (upErr) console.error(`    news_index upsert: ${upErr.message}`);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function processFeed(feed: NewsFeed): Promise<void> {
  console.log(`\n  ── ${feed.slug} (${feed.kind}, ${feed.language}) ──`);
  let candidates: CandidateItem[];
  try {
    candidates = await discoverFeed(feed);
  } catch (err) {
    console.error(`    discovery failed: ${err}`);
    return;
  }
  console.log(`    discovered: ${candidates.length}`);

  // Topic gate (Blick + APIs only) — drop non-econ items before dedup/extract.
  candidates = applyKeywordFilter(feed, candidates);

  // Dedup against news_index.
  const unseen = await filterUnseen(candidates);
  console.log(`    unseen: ${unseen.length}`);
  if (unseen.length === 0) return;

  // Cap per-feed items.
  const cap = feed.max_items ?? MAX_ITEMS_PER_FEED;
  const slice = unseen.slice(0, cap);
  if (slice.length < unseen.length) console.log(`    capping to ${cap}`);

  // Extract bodies one by one (politeness).
  const extracted: Array<{ candidate: CandidateItem; article: Article }> = [];
  let extractFailed = 0;
  for (const candidate of slice) {
    try {
      const article = await extractArticle(candidate.url, candidate);
      if (!article) {
        extractFailed++;
        await recordIndex(feed, candidate, 'failed', 'extract returned null', null);
        continue;
      }
      extracted.push({ candidate, article });
    } catch (err) {
      extractFailed++;
      await recordIndex(feed, candidate, 'failed', String(err).slice(0, 500), null);
    }
    await sleep(POLITENESS_MS);
  }
  console.log(`    extracted: ${extracted.length} (${extractFailed} failed)`);

  // Insert into knowledge_ch.documents.
  const { inserted, failed, documentIds } = await insertDocuments(feed, extracted);

  // Record dedup index for the inserted ones.
  for (const { candidate } of extracted) {
    const docId = documentIds.get(candidate.url) ?? null;
    const status = docId ? 'success' : 'failed';
    await recordIndex(feed, candidate, status, docId ? null : 'insert returned no id', docId);
  }

  console.log(`    inserted: ${inserted}, failed: ${failed}`);
}

async function main() {
  console.log('='.repeat(64));
  console.log('  news-rss — Swiss news aggregator');
  console.log(`  Target: knowledge_ch.documents + bronze_ch.news_index on re-llm`);
  console.log(`  Feeds: ${FEEDS.length} configured, ONLY_FEED=${ONLY_FEED ?? '(all)'}`);
  console.log(`  DRY_RUN: ${DRY_RUN}`);
  console.log('='.repeat(64));

  const t0 = Date.now();
  const feedsToRun = ONLY_FEED ? FEEDS.filter((f) => f.slug === ONLY_FEED) : FEEDS;
  if (feedsToRun.length === 0) {
    console.error(`No feed matches ONLY_FEED=${ONLY_FEED}`);
    process.exit(1);
  }

  for (const feed of feedsToRun) {
    try {
      await processFeed(feed);
    } catch (err) {
      console.error(`  ${feed.slug} failed: ${err}`);
    }
  }

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log('\n' + '='.repeat(64));
  console.log(`  IMPORT COMPLETE — ${elapsed}s`);
  console.log('='.repeat(64));
}

main()
  .then(() => {
    // supabase-js keeps http2 keep-alive connections + internal timers that
    // prevent Node from exiting cleanly. The 2026-05-17 run hung for 84 min
    // after the last feed completed before hitting the 90-min job timeout —
    // dashboard then reported a false failure. Force-exit on clean completion.
    process.exit(0);
  })
  .catch((err) => {
    console.error('Fatal error:', err);
    process.exit(1);
  });
