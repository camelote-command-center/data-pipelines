-- ============================================================================
-- Geneva forest layers — ref targets on lamap_db
-- ============================================================================
-- Consumer-side tables. Contract matched to the existing ref.cadastral_*
-- neighbours, verified live: ref.cadastral_zones and ref.cadastral_rdppf_pnp
-- are both SRID 4326 MULTIPOLYGON with raw_data jsonb + updated_at.
--
-- Geometry subtypes were verified on the gold views BEFORE writing this DDL,
-- as platform.standards / silver_promotion_pattern_ge_overlays step 7 requires
-- ("will fail with Geometry type does not match column type"):
--     cadastre    MultiPolygon      distance_s  MultiPolygon
--     distance_l  MultiLineString   lisieres    MultiLineString
--     fonction    MultiPolygon      parcelles   no geometry
--
-- geom_2056 travels alongside the 4326 geometry so metric work on lamap_db does
-- not have to reproject. Distances and areas must never be computed in 4326.
--
-- Each live table has a matching ref._staging_* table: gold_ch.sync_full_refresh
-- Branch A stages through it and then UPSERTs into the live table, so the live
-- table is never truncated.
--
-- PURE-REF: these are read by public.* RPCs only. Nothing here reaches into
-- bronze, silver or gold.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS ref;

-- ---------------------------------------------------------------------------
-- 1. Cadastre forestier — factual surveyed forest extent
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ref.cadastral_forest_cadastre (
  feature_key  text PRIMARY KEY,
  geom_hash    text,
  objectid     integer,
  remarque     text,
  area_m2      double precision,
  geom_2056    geometry(MultiPolygon, 2056),
  geometry     geometry(MultiPolygon, 4326),
  canton_code  text,
  raw_data     jsonb,
  updated_at   timestamptz
);

CREATE TABLE IF NOT EXISTS ref._staging_cadastral_forest_cadastre
  (LIKE ref.cadastral_forest_cadastre INCLUDING DEFAULTS);

-- ---------------------------------------------------------------------------
-- 2. RDPPF distances forêt — surface (the legally registered restriction)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ref.cadastral_forest_distance_s (
  feature_key           text PRIMARY KEY,
  erebid                integer,
  geom_hash             text,
  objectid              integer,
  commune_bfs           integer,
  commune_name          text,
  statut_juridique      text,
  entree_en_force_date  date,
  lien_document         text,
  lien_plan             text,
  date_maj              date,
  area_m2               double precision,
  geom_2056             geometry(MultiPolygon, 2056),
  geometry              geometry(MultiPolygon, 4326),
  canton_code           text,
  raw_data              jsonb,
  updated_at            timestamptz
);

CREATE TABLE IF NOT EXISTS ref._staging_cadastral_forest_distance_s
  (LIKE ref.cadastral_forest_distance_s INCLUDING DEFAULTS);

-- ---------------------------------------------------------------------------
-- 3. RDPPF distances forêt — ligne (map rendering only)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ref.cadastral_forest_distance_l (
  feature_key           text PRIMARY KEY,
  erebid                integer,
  geom_hash             text,
  objectid              integer,
  commune_bfs           integer,
  commune_name          text,
  statut_juridique      text,
  entree_en_force_date  date,
  lien_document         text,
  lien_plan             text,
  date_maj              date,
  length_m              double precision,
  geom_2056             geometry(MultiLineString, 2056),
  geometry              geometry(MultiLineString, 4326),
  canton_code           text,
  raw_data              jsonb,
  updated_at            timestamptz
);

CREATE TABLE IF NOT EXISTS ref._staging_cadastral_forest_distance_l
  (LIKE ref.cadastral_forest_distance_l INCLUDING DEFAULTS);

-- ---------------------------------------------------------------------------
-- 4. Lisières forestières — boundary survey procedures
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ref.cadastral_forest_lisieres (
  feature_key           text PRIMARY KEY,
  id_dossier_key        text,
  geom_hash             text,
  id_dossier            text,
  objectid              integer,
  type_procedure        text,
  commune_raw           text,
  commune_bfs           integer,
  commune_resolution    text,
  parcelles_raw         text,
  num_autor             text,
  mz_plq                text,
  relev_etat            text,
  relev_date            date,
  etape_procedure       text,
  etat_dossier          text,
  statut_juridique      text,
  dec_natfor            text,
  dec_natfor_bool       boolean,
  fao_requete_date      date,
  fao_decision_date     date,
  entree_en_force_date  date,
  in_force              boolean,
  recours               text,
  rdppf_statut          text,
  lien_document         text,
  length_m              double precision,
  geom_2056             geometry(MultiLineString, 2056),
  geometry              geometry(MultiLineString, 4326),
  canton_code           text,
  raw_data              jsonb,
  updated_at            timestamptz
);

CREATE TABLE IF NOT EXISTS ref._staging_cadastral_forest_lisieres
  (LIKE ref.cadastral_forest_lisieres INCLUDING DEFAULTS);

-- ---------------------------------------------------------------------------
-- 5. Lisières — parcelles (normalised child, no geometry)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ref.cadastral_forest_lisieres_parcelles (
  feature_key         text PRIMARY KEY,
  id_dossier_key      text,
  geom_hash           text,
  id_dossier          text,
  token_ordinal       bigint,
  parcelles_raw       text,
  annotation          text,
  token               text,
  no_parcelle         integer,
  parcelle_suffix     text,
  is_domaine_public   boolean,
  no_commune          integer,
  commune_resolution  text,
  parse_status        text,
  canton_code         text,
  updated_at          timestamptz
);

CREATE TABLE IF NOT EXISTS ref._staging_cadastral_forest_lisieres_parcelles
  (LIKE ref.cadastral_forest_lisieres_parcelles INCLUDING DEFAULTS);

-- ---------------------------------------------------------------------------
-- 6. Fonction plan directeur forestier
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ref.cadastral_forest_fonction (
  feature_key    text PRIMARY KEY,
  geom_hash      text,
  objectid       integer,
  fonction_type  text,
  area_m2        double precision,
  geom_2056      geometry(MultiPolygon, 2056),
  geometry       geometry(MultiPolygon, 4326),
  canton_code    text,
  raw_data       jsonb,
  updated_at     timestamptz
);

CREATE TABLE IF NOT EXISTS ref._staging_cadastral_forest_fonction
  (LIKE ref.cadastral_forest_fonction INCLUDING DEFAULTS);

-- ---------------------------------------------------------------------------
-- Indexes — GIST on every geometry column, in both SRIDs
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS cadastral_forest_cadastre_gix
  ON ref.cadastral_forest_cadastre USING GIST (geometry);
CREATE INDEX IF NOT EXISTS cadastral_forest_cadastre_gix_2056
  ON ref.cadastral_forest_cadastre USING GIST (geom_2056);

CREATE INDEX IF NOT EXISTS cadastral_forest_distance_s_gix
  ON ref.cadastral_forest_distance_s USING GIST (geometry);
CREATE INDEX IF NOT EXISTS cadastral_forest_distance_s_gix_2056
  ON ref.cadastral_forest_distance_s USING GIST (geom_2056);
CREATE INDEX IF NOT EXISTS cadastral_forest_distance_s_erebid_idx
  ON ref.cadastral_forest_distance_s (erebid);

CREATE INDEX IF NOT EXISTS cadastral_forest_distance_l_gix
  ON ref.cadastral_forest_distance_l USING GIST (geometry);
CREATE INDEX IF NOT EXISTS cadastral_forest_distance_l_gix_2056
  ON ref.cadastral_forest_distance_l USING GIST (geom_2056);
CREATE INDEX IF NOT EXISTS cadastral_forest_distance_l_erebid_idx
  ON ref.cadastral_forest_distance_l (erebid);

CREATE INDEX IF NOT EXISTS cadastral_forest_lisieres_gix
  ON ref.cadastral_forest_lisieres USING GIST (geometry);
CREATE INDEX IF NOT EXISTS cadastral_forest_lisieres_gix_2056
  ON ref.cadastral_forest_lisieres USING GIST (geom_2056);
CREATE INDEX IF NOT EXISTS cadastral_forest_lisieres_dossier_idx
  ON ref.cadastral_forest_lisieres (id_dossier);
CREATE INDEX IF NOT EXISTS cadastral_forest_lisieres_num_autor_idx
  ON ref.cadastral_forest_lisieres (num_autor) WHERE num_autor IS NOT NULL;

CREATE INDEX IF NOT EXISTS cadastral_forest_fonction_gix
  ON ref.cadastral_forest_fonction USING GIST (geometry);
CREATE INDEX IF NOT EXISTS cadastral_forest_fonction_gix_2056
  ON ref.cadastral_forest_fonction USING GIST (geom_2056);

CREATE INDEX IF NOT EXISTS cadastral_forest_lisieres_parcelles_lookup_idx
  ON ref.cadastral_forest_lisieres_parcelles (no_commune, no_parcelle)
  WHERE no_parcelle IS NOT NULL;
CREATE INDEX IF NOT EXISTS cadastral_forest_lisieres_parcelles_dossier_idx
  ON ref.cadastral_forest_lisieres_parcelles (id_dossier);

NOTIFY pgrst, 'reload schema';
