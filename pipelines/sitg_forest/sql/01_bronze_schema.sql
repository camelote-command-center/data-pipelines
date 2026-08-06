-- ============================================================================
-- Geneva forest layers (SITG) — bronze schema on re-LLM (znrvddgmczdqoucmykij)
-- ============================================================================
-- Five layers, all fetched from the SITG ArcGIS REST FeatureServer in their
-- native EPSG:2056 (LV95). Bronze holds source attributes verbatim plus the
-- untouched feature properties in raw_data.
--
-- SRID: 2056, per platform.standards/silver_promotion_pattern_ge_overlays step 1
-- ("Source: bronze_ch.ge_<x> (SRID 2056 LV95)"). The eleven existing
-- ge_sitg_*_geo tables are 4326 because the shared fetcher defaults to
-- outSR=4326; that is drift from the standard, not the standard itself.
-- Forest distance and area work is metric, so reprojection on ingest is not
-- acceptable here.
--
-- KEYS. SITG states OBJECTID must not be used as a permanent unique
-- identifier, so every table is keyed on a deterministic geometry surrogate:
--     geom_hash = md5(ST_AsBinary(ST_ReducePrecision(geometry, 0.01)))
-- Precision is reduced to 1 cm before hashing so that a coordinate rounding
-- change at source does not read as a delete plus an insert.
--
-- EREBID is NOT unique, contrary to the SITG documentation. Measured
-- 2026-08-06 against the live services:
--     RDPPF_DISTANCES_FORET_S  856 rows / 855 distinct EREBID
--     RDPPF_DISTANCES_FORET_L  845 rows / 844 distinct EREBID
-- EREBID 66158076 carries two genuinely different polygons (1'006 m2 and
-- 5'127 m2) under one restriction document. Keying on EREBID alone would
-- silently drop the larger of the two. Hence the composite (erebid, geom_hash),
-- which keeps both parts while leaving EREBID queryable to group a multipart
-- restriction back together.
--
-- ID_DOSSIER on FFP_LISIERES_FORESTIERES is likewise not unique: 1'315 rows,
-- 1'314 non-null, 1'277 distinct (17 duplicated values, one NULL). The NULL is
-- why the key column is id_dossier_key = coalesce(id_dossier, '') rather than
-- id_dossier itself, since a NULL cannot participate in a primary key.
--
-- Soft delete, never hard: last_seen_at is stamped on every run; rows absent
-- from a run are marked deleted_at rather than removed. run_id makes a full
-- reload detectable and reversible.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS bronze_ch;

-- ---------------------------------------------------------------------------
-- 1. FFP_CADASTRE_FORET — factual surveyed forest extent (839 polygons)
-- ---------------------------------------------------------------------------
-- The geometric truth Phase 2 computes from. Note that SITG stamps REMARQUE =
-- 'Perimetre indicatif' on all 839 polygons; the layer is authoritative for
-- extent but self-described as indicative at the edge.
CREATE TABLE IF NOT EXISTS bronze_ch.ge_ffp_cadastre_foret (
  geom_hash      text        NOT NULL,
  objectid       integer,
  remarque       text,
  shape_area     double precision,
  shape_length   double precision,
  geometry       geometry(Geometry, 2056),
  raw_data       jsonb       NOT NULL,
  run_id         uuid        NOT NULL,
  ingested_at    timestamptz NOT NULL DEFAULT now(),
  first_seen_at  timestamptz NOT NULL DEFAULT now(),
  last_seen_at   timestamptz NOT NULL DEFAULT now(),
  deleted_at     timestamptz,
  CONSTRAINT ge_ffp_cadastre_foret_pkey PRIMARY KEY (geom_hash)
);

-- ---------------------------------------------------------------------------
-- 2. RDPPF_DISTANCES_FORET_S — legally registered 20 m surface (856 polygons)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze_ch.ge_rdppf_distances_foret_s (
  erebid                integer     NOT NULL,
  geom_hash             text        NOT NULL,
  objectid              integer,
  commune               smallint,
  statut_juridique      text,
  entree_en_force_date  date,
  lien_document         text,
  lien_plan             text,
  date_maj              date,
  shape_area            double precision,
  shape_length          double precision,
  geometry              geometry(Geometry, 2056),
  raw_data              jsonb       NOT NULL,
  run_id                uuid        NOT NULL,
  ingested_at           timestamptz NOT NULL DEFAULT now(),
  first_seen_at         timestamptz NOT NULL DEFAULT now(),
  last_seen_at          timestamptz NOT NULL DEFAULT now(),
  deleted_at            timestamptz,
  CONSTRAINT ge_rdppf_distances_foret_s_pkey PRIMARY KEY (erebid, geom_hash)
);

-- ---------------------------------------------------------------------------
-- 3. RDPPF_DISTANCES_FORET_L — same restriction, line geometry (845 lines)
-- ---------------------------------------------------------------------------
-- Map rendering only. No shape_area on this layer at source.
CREATE TABLE IF NOT EXISTS bronze_ch.ge_rdppf_distances_foret_l (
  erebid                integer     NOT NULL,
  geom_hash             text        NOT NULL,
  objectid              integer,
  commune               smallint,
  statut_juridique      text,
  entree_en_force_date  date,
  lien_document         text,
  lien_plan             text,
  date_maj              date,
  shape_length          double precision,
  geometry              geometry(Geometry, 2056),
  raw_data              jsonb       NOT NULL,
  run_id                uuid        NOT NULL,
  ingested_at           timestamptz NOT NULL DEFAULT now(),
  first_seen_at         timestamptz NOT NULL DEFAULT now(),
  last_seen_at          timestamptz NOT NULL DEFAULT now(),
  deleted_at            timestamptz,
  CONSTRAINT ge_rdppf_distances_foret_l_pkey PRIMARY KEY (erebid, geom_hash)
);

-- ---------------------------------------------------------------------------
-- 4. FFP_LISIERES_FORESTIERES — boundary survey procedures (1'315 lines)
-- ---------------------------------------------------------------------------
-- Procedural layer: FAO publication dates, appeals, linked DA numbers.
-- COMMUNE here is a String(50) holding NAMES, sometimes several at once
-- ('Chene-Bougeries, Chene-Bourg, Thonex, Vandoeuvres') — unlike the two
-- RDPPF distance layers above where COMMUNE is a SmallInteger code. Bronze
-- keeps it verbatim as text; silver resolves what it can.
-- NUM_AUTOR is the DA number joining into the SAD pipeline, but it is
-- populated on only 44 of 1'315 rows (3.3%).
CREATE TABLE IF NOT EXISTS bronze_ch.ge_ffp_lisieres_forestieres (
  id_dossier_key        text        NOT NULL,
  geom_hash             text        NOT NULL,
  id_dossier            text,
  objectid              integer,
  type_procedure        text,
  commune               text,
  parcelles             text,
  num_autor             text,
  mz_plq                text,
  relev_etat            text,
  relev_date            date,
  etape_procedure       text,
  etat_dossier          text,
  statut_juridique      text,
  dec_natfor            text,
  fao_requete_date      date,
  fao_decision_date     date,
  entree_en_force_date  date,
  recours               text,
  rdppf_statut          text,
  lien_document         text,
  shape_length          double precision,
  geometry              geometry(Geometry, 2056),
  raw_data              jsonb       NOT NULL,
  run_id                uuid        NOT NULL,
  ingested_at           timestamptz NOT NULL DEFAULT now(),
  first_seen_at         timestamptz NOT NULL DEFAULT now(),
  last_seen_at          timestamptz NOT NULL DEFAULT now(),
  deleted_at            timestamptz,
  CONSTRAINT ge_ffp_lisieres_forestieres_pkey PRIMARY KEY (id_dossier_key, geom_hash)
);

-- ---------------------------------------------------------------------------
-- 5. FFP_FONCTION_PDF — plan directeur forestier function type (942 polygons)
-- ---------------------------------------------------------------------------
-- Optional, low priority. Single useful attribute (TYPE).
CREATE TABLE IF NOT EXISTS bronze_ch.ge_ffp_fonction_pdf (
  geom_hash      text        NOT NULL,
  objectid       integer,
  type           text,
  shape_area     double precision,
  shape_length   double precision,
  geometry       geometry(Geometry, 2056),
  raw_data       jsonb       NOT NULL,
  run_id         uuid        NOT NULL,
  ingested_at    timestamptz NOT NULL DEFAULT now(),
  first_seen_at  timestamptz NOT NULL DEFAULT now(),
  last_seen_at   timestamptz NOT NULL DEFAULT now(),
  deleted_at     timestamptz,
  CONSTRAINT ge_ffp_fonction_pdf_pkey PRIMARY KEY (geom_hash)
);

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS ge_ffp_cadastre_foret_geom_gix
  ON bronze_ch.ge_ffp_cadastre_foret USING GIST (geometry);
CREATE INDEX IF NOT EXISTS ge_rdppf_distances_foret_s_geom_gix
  ON bronze_ch.ge_rdppf_distances_foret_s USING GIST (geometry);
CREATE INDEX IF NOT EXISTS ge_rdppf_distances_foret_l_geom_gix
  ON bronze_ch.ge_rdppf_distances_foret_l USING GIST (geometry);
CREATE INDEX IF NOT EXISTS ge_ffp_lisieres_forestieres_geom_gix
  ON bronze_ch.ge_ffp_lisieres_forestieres USING GIST (geometry);
CREATE INDEX IF NOT EXISTS ge_ffp_fonction_pdf_geom_gix
  ON bronze_ch.ge_ffp_fonction_pdf USING GIST (geometry);

-- run_id lookups for reload forensics
CREATE INDEX IF NOT EXISTS ge_ffp_cadastre_foret_run_idx
  ON bronze_ch.ge_ffp_cadastre_foret (run_id);
CREATE INDEX IF NOT EXISTS ge_rdppf_distances_foret_s_run_idx
  ON bronze_ch.ge_rdppf_distances_foret_s (run_id);
CREATE INDEX IF NOT EXISTS ge_rdppf_distances_foret_l_run_idx
  ON bronze_ch.ge_rdppf_distances_foret_l (run_id);
CREATE INDEX IF NOT EXISTS ge_ffp_lisieres_forestieres_run_idx
  ON bronze_ch.ge_ffp_lisieres_forestieres (run_id);
CREATE INDEX IF NOT EXISTS ge_ffp_fonction_pdf_run_idx
  ON bronze_ch.ge_ffp_fonction_pdf (run_id);

-- EREBID stays queryable so a multipart restriction can be grouped back together
CREATE INDEX IF NOT EXISTS ge_rdppf_distances_foret_s_erebid_idx
  ON bronze_ch.ge_rdppf_distances_foret_s (erebid);
CREATE INDEX IF NOT EXISTS ge_rdppf_distances_foret_l_erebid_idx
  ON bronze_ch.ge_rdppf_distances_foret_l (erebid);
CREATE INDEX IF NOT EXISTS ge_ffp_lisieres_forestieres_dossier_idx
  ON bronze_ch.ge_ffp_lisieres_forestieres (id_dossier);
CREATE INDEX IF NOT EXISTS ge_ffp_lisieres_forestieres_num_autor_idx
  ON bronze_ch.ge_ffp_lisieres_forestieres (num_autor) WHERE num_autor IS NOT NULL;
