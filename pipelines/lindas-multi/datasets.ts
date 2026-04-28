// Dataset registry for LINDAS Multi parser.
// Add a new entry here, push, dispatch the workflow with that slug.

export interface LindasDataset {
  /** Short slug used as dedup key prefix and matrix axis. */
  slug: string;
  /** User-facing descriptor URL (the one published in opendata.swiss / lindas docs). */
  descriptor_url: string;
  /** Named graph in lindas.admin.ch/query that holds the data. */
  graph_iri: string;
  /**
   * 'graph' = take all subjects in the graph (registry-style: termdat, curia).
   * 'cube' = scope to a specific cube IRI's observations + the cube IRI itself.
   */
  kind: 'graph' | 'cube';
  /** Required when kind='cube'. The cube IRI from the descriptor URL. */
  cube_iri?: string;
  /** Coarse tags pre-populated (per re-LLM v2 briefing — classifier preserves them). */
  tags: string[];
}

export const SPARQL_ENDPOINT = 'https://lindas.admin.ch/query';

export const DATASETS: LindasDataset[] = [
  // Registries
  {
    slug: 'opendataswiss-meta',
    descriptor_url: 'https://register.ld.admin.ch/.well-known/dataset/opendataswiss-meta',
    graph_iri: 'https://lindas.admin.ch/sfa/opendataswiss',
    kind: 'graph',
    tags: ['lindas', 'opendata_swiss', 'catalog', 'discovery'],
  },
  {
    slug: 'curia',
    descriptor_url: 'https://politics.ld.admin.ch/.well-known/void/dataset/curia',
    graph_iri: 'https://lindas.admin.ch/fch/curia',
    kind: 'graph',
    tags: ['lindas', 'curia', 'parliament', 'government'],
  },
  {
    slug: 'termdat',
    descriptor_url: 'https://register.ld.admin.ch/.well-known/dataset/termdat',
    graph_iri: 'https://lindas.admin.ch/fch/termdat',
    kind: 'graph',
    tags: ['lindas', 'termdat', 'terminology', 'reference'],
  },

  // Cubes — energy SFOE
  {
    slug: 'bfe_ogd115_gest_bilanz',
    descriptor_url: 'https://energy.ld.admin.ch/sfoe/bfe_ogd115_gest_bilanz/6',
    graph_iri: 'https://lindas.admin.ch/sfoe/cube',
    kind: 'cube',
    cube_iri: 'https://energy.ld.admin.ch/sfoe/bfe_ogd115_gest_bilanz/6',
    tags: ['lindas', 'sfoe', 'energy', 'electricity_balance'],
  },
  {
    slug: 'bfe_ogd18_anzahl_gesuche',
    descriptor_url: 'https://energy.ld.admin.ch/sfoe/bfe_ogd18_gebaeudeprogramm_anzahl_gesuche/20',
    graph_iri: 'https://lindas.admin.ch/sfoe/cube',
    kind: 'cube',
    cube_iri: 'https://energy.ld.admin.ch/sfoe/bfe_ogd18_gebaeudeprogramm_anzahl_gesuche/20',
    tags: ['lindas', 'sfoe', 'energy', 'gebaeudeprogramm', 'applications'],
  },
  {
    slug: 'bfe_ogd18_auszahlungen',
    descriptor_url: 'https://energy.ld.admin.ch/sfoe/bfe_ogd18_gebaeudeprogramm_auszahlungen/11',
    graph_iri: 'https://lindas.admin.ch/sfoe/cube',
    kind: 'cube',
    cube_iri: 'https://energy.ld.admin.ch/sfoe/bfe_ogd18_gebaeudeprogramm_auszahlungen/11',
    tags: ['lindas', 'sfoe', 'energy', 'gebaeudeprogramm', 'payouts'],
  },
  {
    slug: 'bfe_ogd18_energiewirkung',
    descriptor_url: 'https://energy.ld.admin.ch/sfoe/bfe_ogd18_gebaeudeprogramm_energiewirkung/9',
    graph_iri: 'https://lindas.admin.ch/sfoe/cube',
    kind: 'cube',
    cube_iri: 'https://energy.ld.admin.ch/sfoe/bfe_ogd18_gebaeudeprogramm_energiewirkung/9',
    tags: ['lindas', 'sfoe', 'energy', 'gebaeudeprogramm', 'energy_effect'],
  },
  {
    slug: 'bfe_ogd18_co2wirkung',
    descriptor_url: 'https://energy.ld.admin.ch/sfoe/bfe_ogd18_gebaeudeprogramm_co2wirkung/7',
    graph_iri: 'https://lindas.admin.ch/sfoe/cube',
    kind: 'cube',
    cube_iri: 'https://energy.ld.admin.ch/sfoe/bfe_ogd18_gebaeudeprogramm_co2wirkung/7',
    tags: ['lindas', 'sfoe', 'energy', 'gebaeudeprogramm', 'co2_effect'],
  },

  // Cube — environment / FOEN COFOG (gov spending classification)
  {
    slug: 'foen_bfs_cofog_national',
    descriptor_url: 'https://environment.ld.admin.ch/foen/BFS_cofog_national/3',
    graph_iri: 'https://lindas.admin.ch/foen/cube',
    kind: 'cube',
    cube_iri: 'https://environment.ld.admin.ch/foen/BFS_cofog_national/3',
    tags: ['lindas', 'foen', 'environment', 'cofog', 'government_finance'],
  },

  // Cube — culture / state accounts
  {
    slug: 'sfa_state_accounts_cat9',
    descriptor_url: 'https://culture.ld.admin.ch/sfa/StateAccounts_Category/9',
    graph_iri: 'https://lindas.admin.ch/sfa/cube',
    kind: 'cube',
    cube_iri: 'https://culture.ld.admin.ch/sfa/StateAccounts_Category/9',
    tags: ['lindas', 'sfa', 'state_accounts', 'finance_economy', 'culture'],
  },
];

export function lookupDataset(slug: string): LindasDataset {
  const ds = DATASETS.find((d) => d.slug === slug);
  if (!ds) {
    throw new Error(`Unknown dataset slug: ${slug}. Known: ${DATASETS.map((d) => d.slug).join(', ')}`);
  }
  return ds;
}
