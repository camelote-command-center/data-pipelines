/**
 * Seeds for the Geneva real-estate Wikipedia ingest.
 *
 * Two flavors:
 *  1. SPARQL queries (deterministic, auto-updating) — fed to Wikidata Query Service
 *     to enumerate Qids that match a structural pattern (e.g. all communes in
 *     canton Geneva). New entries Wikidata gains over time get picked up
 *     automatically on the next monthly run.
 *  2. Wikipedia titles (FR) — for entities where SPARQL coverage is unreliable
 *     (laws, project names, individual companies). Resolved to Qid at fetch
 *     time via the MediaWiki REST API; if the title doesn't exist on FR
 *     Wikipedia the seed is logged & skipped (never crashes the run).
 *
 * To extend: add to SEED_SPARQL or SEED_TITLES_FR. No code changes needed.
 */

export type SparqlSeed = { name: string; category: string; query: string };
export type TitleSeed = { title: string; category: string };

// ---------------------------------------------------------------------------
// SPARQL — deterministic enumeration
// ---------------------------------------------------------------------------

export const SEED_SPARQL: SparqlSeed[] = [
  {
    name: 'ge_communes',
    category: 'commune',
    // All "commune of Switzerland" (Q70208) located in canton of Geneva (Q11911).
    // Includes the 45 GE communes.
    query: `
      SELECT DISTINCT ?item WHERE {
        ?item wdt:P31 wd:Q70208 ;
              wdt:P131 wd:Q11911 .
      }
    `,
  },
  {
    name: 'ge_quartiers_geneve',
    category: 'quartier',
    // Subdivisions of the city of Geneva (Q71). P131 (located in admin entity)
    // chained transitively catches both quartiers (Q11881845) and sous-quartiers.
    query: `
      SELECT DISTINCT ?item WHERE {
        ?item wdt:P131+ wd:Q71 .
        ?item wdt:P31/wdt:P279* ?type .
        VALUES ?type { wd:Q11881845 wd:Q123705 wd:Q3957 wd:Q5341295 }
      }
    `,
  },
];

// ---------------------------------------------------------------------------
// Wikipedia FR titles — hand-curated, resolved to Qid at fetch time
// ---------------------------------------------------------------------------
// Only titles I have reasonable confidence exist on FR Wikipedia. The fetcher
// logs and skips any miss — first run will surface unresolvable titles in the
// workflow log so they can be replaced/removed without touching code.

export const SEED_TITLES_FR: TitleSeed[] = [
  // ── Geneva-specific real-estate laws ────────────────────────────────────
  { title: 'Loi sur les démolitions, transformations et rénovations de maisons d\'habitation', category: 'law' },
  { title: 'Lex Koller', category: 'law' },
  { title: 'Loi fédérale sur l\'aménagement du territoire', category: 'law' },
  { title: 'Code civil suisse', category: 'law' },
  { title: 'Droit du bail en Suisse', category: 'law' },

  // ── Major Geneva real-estate developments ────────────────────────────────
  { title: 'Praille-Acacias-Vernets', category: 'development' },
  { title: 'CEVA (ligne ferroviaire)', category: 'development' },           // adjacent infra: shapes RE values along the line
  { title: 'Quartier des Vergers', category: 'development' },
  { title: 'Belle-Idée', category: 'development' },

  // ── Institutions ─────────────────────────────────────────────────────────
  { title: 'ASLOCA', category: 'institution' },
  { title: 'Hospice général', category: 'institution' },                   // GE social housing
  { title: 'Office fédéral du logement', category: 'institution' },
  { title: 'Banque cantonale de Genève', category: 'institution' },        // major mortgage lender

  // ── Notable RE-adjacent figures / companies ──────────────────────────────
  // Conservative — only those I'm confident have a FR WP article.
  // The fetcher will skip any title that 404s.

  // ── Geographic context entities the chunks will reference ───────────────
  { title: 'Canton de Genève', category: 'geography' },
  { title: 'Genève', category: 'geography' },
  { title: 'Histoire de Genève', category: 'geography' },
  { title: 'Économie de Genève', category: 'geography' },
  { title: 'Géographie de Genève', category: 'geography' },
  { title: 'Architecture à Genève', category: 'geography' },
  { title: 'Vieille-Ville (Genève)', category: 'geography' },
  { title: 'Carouge', category: 'commune' },                               // already in SPARQL but dual-tag is fine (UPSERT)
];

// Languages to fetch for every resolved Qid. FR primary (canton language),
// EN as fallback / cross-check. DE/IT can be added later if needed.
export const FETCH_LANGUAGES = ['fr', 'en'] as const;
