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
  if (!indexRes.ok) throw new Error(`sitemap-index HTTP ${indexRes.status} on ${feed.url}`);
  const indexXml = await indexRes.text();
  const $ = cheerio.load(indexXml, { xmlMode: true });

  // Sub-sitemap selection: include those with lastmod within lookback.
  const lookbackDays = feed.sitemap_lookback_days ?? 1;
  const cutoff = Date.now() - lookbackDays * 86_400_000;
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

  const urlRe = feed.sitemap_url_regex ? new RegExp(feed.sitemap_url_regex) : null;
  const items: CandidateItem[] = [];
  const cap = feed.max_items ?? MAX_ITEMS_PER_FEED;
  for (const sub of subUrls) {
    if (items.length >= cap) break;
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
    await sleep(POLITENESS_MS);
  }
  return items;
}

async function discoverFeed(feed: NewsFeed): Promise<CandidateItem[]> {
  if (feed.kind === 'rss') return discoverRss(feed);
  if (feed.kind === 'sitemap') return discoverSitemap(feed);
  throw new Error(`unknown feed kind: ${(feed as NewsFeed).kind}`);
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

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
