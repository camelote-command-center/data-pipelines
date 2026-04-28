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
  // ── Le Temps (FR, Geneva flagship). Topical sub-feeds preferred over the
  //    general one because they're cleaner per-topic and the classifier still
  //    re-routes by domain anyway.
  {
    slug: 'letemps-suisse',
    publisher: 'Le Temps',
    kind: 'rss',
    url: 'https://www.letemps.ch/suisse.rss',
    language: 'fr',
    canton: 'GE',
    tags: ['news', 'rss', 'letemps', 'fr', 'suisse', 'romandie'],
  },
  {
    slug: 'letemps-economie',
    publisher: 'Le Temps',
    kind: 'rss',
    url: 'https://www.letemps.ch/economie.rss',
    language: 'fr',
    canton: 'GE',
    tags: ['news', 'rss', 'letemps', 'fr', 'economie', 'business'],
  },
  {
    slug: 'letemps-societe',
    publisher: 'Le Temps',
    kind: 'rss',
    url: 'https://www.letemps.ch/societe.rss',
    language: 'fr',
    canton: 'GE',
    tags: ['news', 'rss', 'letemps', 'fr', 'societe'],
  },
  {
    slug: 'letemps-opinions',
    publisher: 'Le Temps',
    kind: 'rss',
    url: 'https://www.letemps.ch/opinions.rss',
    language: 'fr',
    canton: 'GE',
    tags: ['news', 'rss', 'letemps', 'fr', 'opinions'],
  },

  // ── Geneva regional (FR)
  {
    slug: 'lecourrier',
    publisher: 'Le Courrier',
    kind: 'rss',
    url: 'https://lecourrier.ch/feed/',
    language: 'fr',
    canton: 'GE',
    tags: ['news', 'rss', 'lecourrier', 'fr', 'geneve'],
  },

  // ── Vaud weekly (FR)
  {
    slug: 'gauchebdo',
    publisher: 'Gauchebdo',
    kind: 'rss',
    url: 'https://gauchebdo.ch/feed',
    language: 'fr',
    canton: 'VD',
    tags: ['news', 'rss', 'gauchebdo', 'fr', 'vaud'],
  },

  // ── Architecture / urbanism (FR/DE — RE-adjacent)
  {
    slug: 'espazium',
    publisher: 'Espazium',
    kind: 'rss',
    url: 'https://www.espazium.ch/fr/rss.xml',
    language: 'fr',
    tags: ['news', 'rss', 'espazium', 'architecture', 'urbanism', 'real_estate'],
  },

  // ── RE industry research (DE/EN)
  {
    slug: 'wuestpartner',
    publisher: 'Wüest Partner',
    kind: 'rss',
    url: 'https://www.wuestpartner.com/feed',
    language: 'de',
    tags: ['news', 'rss', 'wuestpartner', 'real_estate', 'industry_research'],
  },

  // ── NZZ (DE — biggest cross-cantonal coverage in DE Switzerland)
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
    slug: 'nzz-finanzen',
    publisher: 'Neue Zürcher Zeitung',
    kind: 'rss',
    url: 'https://www.nzz.ch/finanzen.rss',
    language: 'de',
    tags: ['news', 'rss', 'nzz', 'finanzen', 'finance'],
  },

  // ── SWI swissinfo (FR via sitemap, federal multilingual)
  {
    slug: 'swissinfo-fr',
    publisher: 'SWI swissinfo.ch',
    kind: 'sitemap',
    url: 'https://www.swissinfo.ch/sitemap.xml',
    language: 'fr',
    tags: ['news', 'sitemap', 'swissinfo', 'fr', 'multilingual'],
    sitemap_lookback_days: 2,
    // Filter to /fre/ paths. Other languages can be added as separate feeds.
    sitemap_url_regex: '^https://www\\.swissinfo\\.ch/fre/.+',
    max_items: 100,
  },
];

// ── DOWN BUT DON'T REMOVE — Swiss publishers that killed RSS in 2026 ──────────
// We keep this list so future agents don't re-discover the dead ends.
//   - tdg.ch, 24heures.ch, bilan.ch — Tamedia / TX Group; /services/rss
//     302-redirects to NZZ. Bilan was consolidated into NZZ Media.
//   - immoday.ch — paywall, no public feed.
//   - 20min.ch, watson.ch, nau.ch — no functional RSS path found; consider
//     sitemap-based ingest if needed (each has /sitemap.xml).
//   - admin.ch, news.admin.ch — RSS endpoints exist but Cloudflare-blocked
//     (403 even with browser UA). Try IP rotation if these become important.
//   - svit.ch, espacesuisse.ch — 406 (server rejects RSS Accept header).
//   - heidi.news, sept.info, largeur.com — no RSS path.
//   - moneypark.ch, comparis.ch — Cloudflare-blocked.
