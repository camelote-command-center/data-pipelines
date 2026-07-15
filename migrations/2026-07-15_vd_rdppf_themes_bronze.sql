-- ============================================================================
-- 2026-07-15 — VD RDPPF theme expansion (re-LLM) — BRONZE
-- ============================================================================
-- Three new RDPPF themes for canton VD, none of which existed before:
--   1. Zones réservées                    -> bronze_ch.vd_zone_reservee     (agsgc /35)
--   2. Protection des eaux souterraines   -> bronze_ch.vd_protection_eaux   (agsgc /118,/119,/120)
--   3. Cadastre des sites pollués (KbS)   -> bronze_ch.vd_site_pollue       (agsgc /116)
--
-- SOURCE ROUTE (decided 2026-07-15, deviates from the brief — see session log):
--   geodienste.ch is `Freigabe erforderlich` for VD on all three MGDM topics
--   (VD is 1 of only 3 gated cantons of 27, with NW+OW). Proven: INTERLIS
--   download -> HTTP 401 for VD vs 200 for ZH; WFS GetFeature over Lausanne ->
--   numberReturned=0 with NO error while the identical query over Zurich
--   returns features. The silent-empty is why this route is unsafe.
--   => Acquired from canton-VD's own ArcGIS REST (agsgc.map.vd.ch), which is
--   the same route all 17 existing vd_* bronze tables already use, needs no
--   Freigabe, and is only reachable from the VPS hosts.
--
-- SRID (decided 2026-07-15, deviates from the brief):
--   Stored NATIVE 2056, matching 17/17 existing VD bronze tables and the
--   source's own wkid:2056. Storing 4326 here would force a 2056->4326->2056
--   round-trip for the spatial join — the exact precision pathology that
--   CREATED an invalid geometry in bug 4d930c20. Joins transform the PLOT side.
--
-- Tracking columns follow the established vd_enrichment convention:
--   first_seen_at / last_seen_at / deleted_at (soft-delete).
--   `last_seen_at` IS the fetched_at — it is set to run_started_at on every
--   UPSERT touch, so a separate fetched_at column would be a pure duplicate.
--
-- ROLLBACK at the bottom of this file.
-- ============================================================================

BEGIN;
SET LOCAL lock_timeout = '60s';

-- ---------------------------------------------------------------------------
-- 1. vd_zone_reservee  (agsgc /35, polygon, 1,765 features, 23 attrs)
--    LAT/RPGA "zones réservées" — planning freeze perimeters.
--    COMMUNE is a genuine federal BFS: verified 1,765/1,765 in range 5000-5999.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze_ch.vd_zone_reservee (
  raw_data             jsonb,
  code                 text,          -- CODE               (code communal)
  designation          text,          -- DESIGNATION        (désignation communale)
  abreviation_type     text,          -- ABREVIATION_TYPE
  disposition_niveau   text,          -- DISPOSITION_NIVEAU
  statut_juridique     text,          -- STATUT_JURIDIQUE
  date_entree_vigueur  timestamptz,   -- DATE_EV
  date_fin             timestamptz,   -- DATE_FIN
  date_enquete         timestamptz,   -- DATE_ENQ
  date_approbation     timestamptz,   -- DATE_APPRO
  no_officiel          numeric,       -- NO_OFFICIEL
  type_doc             text,          -- TYPE_DOC
  titre                text,          -- TITRE
  commune_bfs          integer,       -- COMMUNE  (N° OFS commune — verified BFS)
  perimetre_m          numeric,       -- PERIMETRE
  surface_m2           numeric,       -- SURFACE
  arcgis_objectid      bigint NOT NULL,
  geometry             geometry(MultiPolygon, 2056),
  source_layer         text NOT NULL DEFAULT 'vd.zone_reservee'
                       CHECK (source_layer = 'vd.zone_reservee'),
  canton_code          text NOT NULL DEFAULT 'VD' CHECK (canton_code = 'VD'),
  first_seen_at        timestamptz NOT NULL DEFAULT now(),
  last_seen_at         timestamptz NOT NULL DEFAULT now(),
  deleted_at           timestamptz,
  PRIMARY KEY (source_layer, arcgis_objectid)
);
COMMENT ON TABLE bronze_ch.vd_zone_reservee IS
  'Canton-VD zones réservées (LAT art.27 planning freeze). Source: agsgc.map.vd.ch /35 vd.zone_reservee, '
  'fetched in native EPSG:2056. Feeds silver_ch.zones_reservees_vd -> link_plot_zones_reservees_vd. '
  'commune_bfs = source COMMUNE (N° OFS, verified 1765/1765 in 5000-5999). last_seen_at IS the fetched_at.';
COMMENT ON COLUMN bronze_ch.vd_zone_reservee.commune_bfs IS
  'Federal BFS/OFS commune number, from source field COMMUNE. Verified genuine BFS (all values 5000-5999).';
CREATE INDEX IF NOT EXISTS vd_zone_reservee_geom_idx
  ON bronze_ch.vd_zone_reservee USING GIST (geometry) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS vd_zone_reservee_commune_idx
  ON bronze_ch.vd_zone_reservee (commune_bfs) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS vd_zone_reservee_lastseen_idx
  ON bronze_ch.vd_zone_reservee (last_seen_at);

-- ---------------------------------------------------------------------------
-- 2. vd_protection_eaux  (agsgc /118 + /119 + /120, polygon, 3,761 features)
--    The three MGDM "Planerischer Gewässerschutz" sub-themes in one table,
--    discriminated by source_layer (mirrors the vd_classement / vd_isos
--    multi-source-layer precedent):
--      /118 vd.zone_protection_eau     2,947  zones S1/S2/S3        (MGDM 131.1)
--      /119 vd.secteur_protection_eau    799  secteurs Au/Ao/üB     (MGDM 130.1)
--      /120 vd.aire_alimentation          15  aires d'alimentation  (MGDM 132.1)
--    Source carries NO commune attribute -> commune_bfs stays NULL by design;
--    commune is resolved plot-side in the link matview.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze_ch.vd_protection_eaux (
  raw_data             jsonb,
  indice_protection    text,          -- INDICE_PROTECTION  (S1/S2/S3 | Au/Ao/üB | Zu)
  protection_kind      text NOT NULL  -- derived from source_layer
                       CHECK (protection_kind IN ('zone','secteur','aire')),
  date_acceptation     timestamptz,   -- DATE_ACCEPTATION
  commune_bfs          integer,       -- always NULL (source has no commune attr)
  arcgis_objectid      bigint NOT NULL,
  geometry             geometry(MultiPolygon, 2056),
  source_layer         text NOT NULL
                       CHECK (source_layer IN ('vd.zone_protection_eau',
                                               'vd.secteur_protection_eau',
                                               'vd.aire_alimentation')),
  canton_code          text NOT NULL DEFAULT 'VD' CHECK (canton_code = 'VD'),
  first_seen_at        timestamptz NOT NULL DEFAULT now(),
  last_seen_at         timestamptz NOT NULL DEFAULT now(),
  deleted_at           timestamptz,
  PRIMARY KEY (source_layer, arcgis_objectid)
);
COMMENT ON TABLE bronze_ch.vd_protection_eaux IS
  'Canton-VD protection des eaux souterraines — the 3 MGDM Planerischer Gewässerschutz sub-themes '
  '(zones S1/S2/S3 /118, secteurs /119, aires d''alimentation /120) unioned and discriminated by '
  'source_layer. Source: agsgc.map.vd.ch, native EPSG:2056. geodienste MGDM feed is Freigabe-gated '
  'for VD (401) — see session log. commune_bfs is NULL by design: the source carries no commune attribute.';
COMMENT ON COLUMN bronze_ch.vd_protection_eaux.commune_bfs IS
  'Always NULL — canton-VD source layers /118,/119,/120 carry no commune attribute. Commune is '
  'resolved plot-side via silver_ch.cadastral_plots.commune_bfs in the link matview. NOT a parser gap.';
CREATE INDEX IF NOT EXISTS vd_protection_eaux_geom_idx
  ON bronze_ch.vd_protection_eaux USING GIST (geometry) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS vd_protection_eaux_indice_idx
  ON bronze_ch.vd_protection_eaux (indice_protection) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS vd_protection_eaux_kind_idx
  ON bronze_ch.vd_protection_eaux (protection_kind) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS vd_protection_eaux_lastseen_idx
  ON bronze_ch.vd_protection_eaux (last_seen_at);

-- ---------------------------------------------------------------------------
-- 3. vd_site_pollue  (agsgc /116, polygon, 2,477 features, 19 attrs)
--    Cadastre des sites pollués (KbS).
--    ⚠ NO_COMMUNE is NOT a BFS number — verified: 0/2,477 fall in the VD BFS
--    range 5000-5999 and 2,473 are < 1000. It is the canton's own commune
--    numbering. It is preserved as no_commune_vd; commune_bfs stays NULL rather
--    than carry a wrong value.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze_ch.vd_site_pollue (
  raw_data                 jsonb,
  type_site                text,      -- TYPE_SITE (Accident / Ablagerung / Betrieb ...)
  nom_site                 text,      -- NOM_SITE
  activite                 text,      -- ACTIVITE
  nom_phase                text,      -- NOM_PHASE  (KbS statut: pollué / nécessite investigation / ...)
  no_dossier               text,      -- NO_DOSSIER
  no_eva                   text,      -- NO_EVA
  parcelles_polluees       text,      -- PARCELLES_POLLUEES (free-text parcel list)
  urgence_investig         integer,   -- URGENCE_INVESTIG
  investigations_realisees text,      -- INVESTIGATIONS_REALISEES
  volume_decharge          integer,   -- VOLUME_DECHARGE
  debut_activite           text,      -- DEBUT_ACTIVITE
  fin_activite             text,      -- FIN_ACTIVITE
  no_commune_vd            integer,   -- NO_COMMUNE — canton-VD numbering, NOT BFS
  commune_bfs              integer,   -- always NULL (no BFS in source)
  arcgis_objectid          bigint NOT NULL,
  geometry                 geometry(MultiPolygon, 2056),
  source_layer             text NOT NULL DEFAULT 'vd.site_pollue'
                           CHECK (source_layer = 'vd.site_pollue'),
  canton_code              text NOT NULL DEFAULT 'VD' CHECK (canton_code = 'VD'),
  first_seen_at            timestamptz NOT NULL DEFAULT now(),
  last_seen_at             timestamptz NOT NULL DEFAULT now(),
  deleted_at               timestamptz,
  PRIMARY KEY (source_layer, arcgis_objectid)
);
COMMENT ON TABLE bronze_ch.vd_site_pollue IS
  'Canton-VD cadastre des sites pollués (KbS). Source: agsgc.map.vd.ch /116 vd.site_pollue, native '
  'EPSG:2056. geodienste KbS MGDM feed is Freigabe-gated for VD (401) — see session log. '
  'Feeds silver_ch.sites_pollues_vd -> link_plot_sites_pollues_vd.';
COMMENT ON COLUMN bronze_ch.vd_site_pollue.no_commune_vd IS
  '⚠ Canton-VD internal commune number from source NO_COMMUNE. This is NOT the federal BFS: verified '
  '0/2477 values fall in the VD BFS range 5000-5999 and 2473 are < 1000. Do NOT join this to '
  'ref.communes.ofs_number or to cadastral_plots.commune_bfs.';
COMMENT ON COLUMN bronze_ch.vd_site_pollue.commune_bfs IS
  'Always NULL — the source provides no BFS number (see no_commune_vd). Commune is resolved plot-side '
  'in the link matview. NOT a parser gap.';
CREATE INDEX IF NOT EXISTS vd_site_pollue_geom_idx
  ON bronze_ch.vd_site_pollue USING GIST (geometry) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS vd_site_pollue_type_idx
  ON bronze_ch.vd_site_pollue (type_site) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS vd_site_pollue_lastseen_idx
  ON bronze_ch.vd_site_pollue (last_seen_at);

COMMIT;

-- ============================================================================
-- ROLLBACK
-- ============================================================================
-- BEGIN;
--   DROP TABLE IF EXISTS bronze_ch.vd_zone_reservee   RESTRICT;
--   DROP TABLE IF EXISTS bronze_ch.vd_protection_eaux RESTRICT;
--   DROP TABLE IF EXISTS bronze_ch.vd_site_pollue     RESTRICT;
--   DELETE FROM supabase_migrations.schema_migrations WHERE version = '20260715000001';
-- COMMIT;
-- ============================================================================
