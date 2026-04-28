// Registry for the news-rss aggregator.
// Each entry describes one source. Add new entries here and re-run.
//
// Two `kind`s supported:
//   'rss'      → standard RSS/Atom feed, parsed with rss-parser
//   'sitemap'  → sitemap-index XML (one sub-sitemap per day); pulled if
//                lastmod >= today - sitemap_lookback_days
//
// LANDSCAPE NOTE (2026-04-28): most Swiss news publishers (Tamedia: tdg, 24h,
// bilan; immoday; svit; bwo) have killed RSS. The list below is conservative —
// only sources that returned a valid feed under probe. To add a candidate,
// curl `<URL>` and confirm <item> count > 0.

export interface NewsFeed {
  /** URL-safe identifier; also lands as `tags[]` and `source` in knowledge_ch.documents. */
  slug: string;
  /** Human-readable publisher name. */
  publisher: string;
  /** Source type. */
  kind: 'rss' | 'sitemap';
  /** Feed URL (RSS) or sitemap-index URL. */
  url: string;
  /** ISO 639-1 language code of the articles. */
  language: 'fr' | 'de' | 'it' | 'en';
  /** ISO canton code if the feed is canton-specific. */
  canton?: string;
  /** Tags pre-populated on every document this feed produces (lowercase snake_case). */
  tags: string[];
  /** sitemap-only: how many days of sub-sitemaps to walk per run. */
  sitemap_lookback_days?: number;
  /** sitemap-only: regex to filter article URLs (skip category/tag pages, etc.). */
  sitemap_url_regex?: string;
  /** Cap items per run; defaults to 100. Useful for sitemap-based feeds. */
  max_items?: number;
}

export const FEEDS: NewsFeed[] = [
  {
    slug: 'letemps',
    publisher: 'Le Temps',
    kind: 'rss',
    url: 'https://www.letemps.ch/articles.rss',
    language: 'fr',
    canton: 'GE',
    tags: ['news', 'rss', 'letemps', 'fr', 'romandie'],
  },
  {
    slug: 'wuestpartner',
    publisher: 'Wüest Partner',
    kind: 'rss',
    url: 'https://www.wuestpartner.com/feed',
    language: 'de',
    tags: ['news', 'rss', 'wuestpartner', 'real_estate', 'industry_research'],
  },
  {
    slug: 'nzz-wirtschaft',
    publisher: 'Neue Zürcher Zeitung',
    kind: 'rss',
    url: 'https://www.nzz.ch/wirtschaft.rss',
    language: 'de',
    tags: ['news', 'rss', 'nzz', 'wirtschaft', 'business'],
  },
  {
    slug: 'nzz-schweiz',
    publisher: 'Neue Zürcher Zeitung',
    kind: 'rss',
    url: 'https://www.nzz.ch/schweiz.rss',
    language: 'de',
    tags: ['news', 'rss', 'nzz', 'schweiz', 'switzerland'],
  },
  {
    slug: 'swissinfo-fr',
    publisher: 'SWI swissinfo.ch',
    kind: 'sitemap',
    url: 'https://www.swissinfo.ch/sitemap.xml',
    language: 'fr',
    tags: ['news', 'sitemap', 'swissinfo', 'fr', 'multilingual'],
    sitemap_lookback_days: 2,
    // SWI's daily sub-sitemaps surface every URL touched that day; filter to
    // /fre/ language paths. Other languages can be added as separate feeds.
    sitemap_url_regex: '^https://www\\.swissinfo\\.ch/fre/.+',
    max_items: 100,
  },
];
