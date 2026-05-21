-- ============================================================================
-- VD/Lausanne enrichment — BRONZE migration
-- ============================================================================
-- Target DB: re-LLM   (project ref: znrvddgmczdqoucmykij)
-- Scope    : 19 bronze tables for VD canton-wide + Lausanne-only + federal BAV
-- Pattern  : Mirror existing bronze_ch.ge_* shape. Soft-delete via first_seen_at /
--            last_seen_at / deleted_at. UPSERT only. No CASCADE. No truncate.
-- Apply via: supabase apply_migration (NEVER raw psycopg2 for DDL)
-- Rollback : DROP TABLE IF EXISTS bronze_ch.<each_table>  (no dependencies; matviews
--            in the silver migration are NOT yet created when this runs)
-- ============================================================================

BEGIN;

-- ----------------------------------------------------------------------------
-- Helper: every bronze table has these tracking columns. Encoded as default
-- expressions so parsers don't need to set them on INSERT (only on UPDATE).
-- ----------------------------------------------------------------------------

-- 1. vd_batiment_rcb  (canton-VD, ArcGIS layer 39, point geometry, ~360K rows)
CREATE TABLE IF NOT EXISTS bronze_ch.vd_batiment_rcb (
  egid              text,
  no_cadastr        text,
  categorie_txt     text,
  classe_txt        text,
  statut_txt        text,
  cons_annee        integer,
  cons_perio_txt    text,
  surface_m2        numeric,
  nb_niv_tot        integer,
  chauf1_sys_txt    text, chauf1_nrg_txt text,
  chauf2_sys_txt    text, chauf2_nrg_txt text,
  eau1_sys_txt      text, eau1_nrg_txt   text,
  eau2_sys_txt      text, eau2_nrg_txt   text,
  sre_m2            numeric,
  abri_pci          text,
  no_camac          text,
  arcgis_objectid   bigint NOT NULL,
  geometry          geometry(Point, 2056),
  source_layer      text   NOT NULL DEFAULT 'vd.batiment_rcb'
                    CHECK (source_layer = 'vd.batiment_rcb'),
  canton_code       text   NOT NULL DEFAULT 'VD' CHECK (canton_code = 'VD'),
  first_seen_at     timestamptz NOT NULL DEFAULT now(),
  last_seen_at      timestamptz NOT NULL DEFAULT now(),
  deleted_at        timestamptz,
  PRIMARY KEY (source_layer, arcgis_objectid)
);
COMMENT ON TABLE bronze_ch.vd_batiment_rcb IS
  'Canton-VD building cadastre (RCB). Source: agsgc.map.vd.ch/agsgc/rest/services/OGC/wmsVD/MapServer/39. '
  'Per-building EGID-keyed point geometry + RCB attributes (heating, surface, year). '
  'Richer than federal RegBL (heating + SRE). Owner: parsers/vd_enrichment, host VPS1.';
CREATE INDEX IF NOT EXISTS vd_batiment_rcb_egid_idx
  ON bronze_ch.vd_batiment_rcb (egid) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS vd_batiment_rcb_geom_idx
  ON bronze_ch.vd_batiment_rcb USING GIST (geometry) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS vd_batiment_rcb_lastseen_idx
  ON bronze_ch.vd_batiment_rcb (last_seen_at);


-- 2. vd_zone_affectation  (canton-VD #36, polygon, ~80K rows, 26 attrs incl. IUS/COS/SPB/H_MAX)
CREATE TABLE IF NOT EXISTS bronze_ch.vd_zone_affectation (
  code_vd_n2           text,
  designation_vd_n2    text,
  designation_vd_n1    text,
  code_ch              smallint,
  designation_ch       text,
  code_com             text,
  designation_com      text,
  abreviation          text,
  statut_juridique     text,
  date_entree_vigueur  timestamptz,
  date_fin             text,
  force_obligatoire    text,
  ius_type             text,
  ius_value            numeric,
  ius                  numeric,
  cos                  numeric,
  spb                  numeric,
  cm                   numeric,
  igt                  numeric,
  h_max                text,
  normat               text,
  symbole              text,
  sous_theme           text,
  perimetre_m          numeric,
  surface_m2           numeric,
  arcgis_objectid      bigint NOT NULL,
  geometry             geometry(MultiPolygon, 2056),
  source_layer         text   NOT NULL DEFAULT 'vd.zone_affectation'
                       CHECK (source_layer = 'vd.zone_affectation'),
  canton_code          text   NOT NULL DEFAULT 'VD' CHECK (canton_code = 'VD'),
  first_seen_at        timestamptz NOT NULL DEFAULT now(),
  last_seen_at         timestamptz NOT NULL DEFAULT now(),
  deleted_at           timestamptz,
  PRIMARY KEY (source_layer, arcgis_objectid)
);
COMMENT ON TABLE bronze_ch.vd_zone_affectation IS
  'Canton-VD PGA zones (zone d''affectation) with full density indices (IUS, COS, SPB, CM, IGT, H_MAX). '
  'Source: agsgc.map.vd.ch /36. Polygon geometry. Feeds silver_ch.cadastral_zones_vd. Owner: VPS2.';
CREATE INDEX IF NOT EXISTS vd_zone_affectation_geom_idx
  ON bronze_ch.vd_zone_affectation USING GIST (geometry) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS vd_zone_affectation_code_idx
  ON bronze_ch.vd_zone_affectation (code_vd_n2) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS vd_zone_affectation_lastseen_idx
  ON bronze_ch.vd_zone_affectation (last_seen_at);


-- 3. vd_limite_foret  (canton-VD #490, MultiPolygon, ~5K rows)
CREATE TABLE IF NOT EXISTS bronze_ch.vd_limite_foret (
  -- 13-field minimal extract — full attribute set kept in raw_data jsonb
  raw_data         jsonb,
  arcgis_objectid  bigint NOT NULL,
  geometry         geometry(MultiPolygon, 2056),
  source_layer     text   NOT NULL DEFAULT 'vd.limite_foret'
                   CHECK (source_layer = 'vd.limite_foret'),
  canton_code      text   NOT NULL DEFAULT 'VD' CHECK (canton_code = 'VD'),
  first_seen_at    timestamptz NOT NULL DEFAULT now(),
  last_seen_at     timestamptz NOT NULL DEFAULT now(),
  deleted_at       timestamptz,
  PRIMARY KEY (source_layer, arcgis_objectid)
);
COMMENT ON TABLE bronze_ch.vd_limite_foret IS
  'Canton-VD forest limit polygons. Source: agsgc.map.vd.ch /490. Stored as raw_data jsonb '
  '(13 fields). Feeds silver_ch.cadastral_zones_vd with zone_type=''foret''. Owner: VPS2.';
CREATE INDEX IF NOT EXISTS vd_limite_foret_geom_idx
  ON bronze_ch.vd_limite_foret USING GIST (geometry) WHERE deleted_at IS NULL;


-- 4. vd_batiment_projete  (canton-VD #274, polygon, ~8K rows)
CREATE TABLE IF NOT EXISTS bronze_ch.vd_batiment_projete (
  raw_data         jsonb,
  arcgis_objectid  bigint NOT NULL,
  geometry         geometry(MultiPolygon, 2056),
  source_layer     text   NOT NULL DEFAULT 'vd.batiment_projete'
                   CHECK (source_layer = 'vd.batiment_projete'),
  canton_code      text   NOT NULL DEFAULT 'VD' CHECK (canton_code = 'VD'),
  first_seen_at    timestamptz NOT NULL DEFAULT now(),
  last_seen_at     timestamptz NOT NULL DEFAULT now(),
  deleted_at       timestamptz,
  PRIMARY KEY (source_layer, arcgis_objectid)
);
COMMENT ON TABLE bronze_ch.vd_batiment_projete IS
  'Canton-VD building projects (cadastral). Source: agsgc.map.vd.ch /274. Owner: VPS1.';
CREATE INDEX IF NOT EXISTS vd_batiment_projete_geom_idx
  ON bronze_ch.vd_batiment_projete USING GIST (geometry) WHERE deleted_at IS NULL;


-- 5. vd_degre_sensibilite_bruit  (canton-VD #461, polygon, OPB noise sensitivity)
CREATE TABLE IF NOT EXISTS bronze_ch.vd_degre_sensibilite_bruit (
  ds_degre         text,          -- DS I / DS II / DS III / DS IV
  source_plan      text,
  description      text,
  raw_data         jsonb,
  arcgis_objectid  bigint NOT NULL,
  geometry         geometry(MultiPolygon, 2056),
  source_layer     text   NOT NULL DEFAULT 'vd.degre_sensibilite_bruit'
                   CHECK (source_layer = 'vd.degre_sensibilite_bruit'),
  canton_code      text   NOT NULL DEFAULT 'VD' CHECK (canton_code = 'VD'),
  first_seen_at    timestamptz NOT NULL DEFAULT now(),
  last_seen_at     timestamptz NOT NULL DEFAULT now(),
  deleted_at       timestamptz,
  PRIMARY KEY (source_layer, arcgis_objectid)
);
COMMENT ON TABLE bronze_ch.vd_degre_sensibilite_bruit IS
  'Canton-VD OPB noise sensitivity degrees (DS I–IV). Source: agsgc.map.vd.ch /461. '
  'Feeds NATIONAL silver_ch.cadastral_noise_sensitivity. Owner: VPS2.';
CREATE INDEX IF NOT EXISTS vd_degre_sensibilite_bruit_geom_idx
  ON bronze_ch.vd_degre_sensibilite_bruit USING GIST (geometry) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS vd_degre_sensibilite_bruit_ds_idx
  ON bronze_ch.vd_degre_sensibilite_bruit (ds_degre) WHERE deleted_at IS NULL;


-- 6. vd_classement  (canton-VD #398 + #161, consolidated, polygon)
CREATE TABLE IF NOT EXISTS bronze_ch.vd_classement (
  id_objet_recense  integer,
  commune           text,
  description       text,
  fiche             text,                  -- /398 only
  url_recens        text,                  -- /398 only
  numero_arrete     text,                  -- /161 only
  date_arrete       timestamptz,           -- /161 only
  type_protection   text,                  -- /161 only
  raw_data          jsonb,
  arcgis_objectid   bigint NOT NULL,
  geometry          geometry(MultiPolygon, 2056),
  source_layer      text NOT NULL
                    CHECK (source_layer IN ('vd.plan_classement',
                                            'vd.arrete_decision_classement_perimetre')),
  canton_code       text NOT NULL DEFAULT 'VD' CHECK (canton_code = 'VD'),
  first_seen_at     timestamptz NOT NULL DEFAULT now(),
  last_seen_at      timestamptz NOT NULL DEFAULT now(),
  deleted_at        timestamptz,
  PRIMARY KEY (source_layer, arcgis_objectid)
);
COMMENT ON TABLE bronze_ch.vd_classement IS
  'Canton-VD heritage classement (consolidated). Source: /398 (vd.plan_classement) '
  '+ /161 (vd.arrete_decision_classement_perimetre). Discriminator: source_layer. '
  'Feeds silver_ch.cadastral_patrimoine_classe_vd. Owner: VPS2.';
CREATE INDEX IF NOT EXISTS vd_classement_geom_idx
  ON bronze_ch.vd_classement USING GIST (geometry) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS vd_classement_layer_idx
  ON bronze_ch.vd_classement (source_layer) WHERE deleted_at IS NULL;


-- 7. vd_jardin_historique  (canton-VD #403, polygon, 47 fields → raw_data)
CREATE TABLE IF NOT EXISTS bronze_ch.vd_jardin_historique (
  designation       text,                  -- promoted from raw_data for indexability
  valeur            text,
  date_classement   timestamptz,
  raw_data          jsonb,
  arcgis_objectid   bigint NOT NULL,
  geometry          geometry(MultiPolygon, 2056),
  source_layer      text   NOT NULL DEFAULT 'vd.jardin_historique'
                    CHECK (source_layer = 'vd.jardin_historique'),
  canton_code       text   NOT NULL DEFAULT 'VD' CHECK (canton_code = 'VD'),
  first_seen_at     timestamptz NOT NULL DEFAULT now(),
  last_seen_at      timestamptz NOT NULL DEFAULT now(),
  deleted_at        timestamptz,
  PRIMARY KEY (source_layer, arcgis_objectid)
);
COMMENT ON TABLE bronze_ch.vd_jardin_historique IS
  'Canton-VD historic gardens. Source: agsgc.map.vd.ch /403 (47 fields preserved in raw_data). '
  'Feeds silver_ch.cadastral_patrimoine_inventaire_vd with valeur=''jardin_historique''. Owner: VPS2.';
CREATE INDEX IF NOT EXISTS vd_jardin_historique_geom_idx
  ON bronze_ch.vd_jardin_historique USING GIST (geometry) WHERE deleted_at IS NULL;


-- 8. vd_isos  (canton-VD #481 + #404, consolidated, mixed point/polygon)
CREATE TABLE IF NOT EXISTS bronze_ch.vd_isos (
  isos_categorie   text,           -- A / AB / B / etc.
  designation      text,
  raw_data         jsonb,
  arcgis_objectid  bigint NOT NULL,
  geometry         geometry(Geometry, 2056),   -- mixed
  source_layer     text NOT NULL
                   CHECK (source_layer IN ('vd.site_fonde_sur_isos',
                                           'vd.perimetre_fonde_sur_isos')),
  canton_code      text NOT NULL DEFAULT 'VD' CHECK (canton_code = 'VD'),
  first_seen_at    timestamptz NOT NULL DEFAULT now(),
  last_seen_at     timestamptz NOT NULL DEFAULT now(),
  deleted_at       timestamptz,
  PRIMARY KEY (source_layer, arcgis_objectid)
);
COMMENT ON TABLE bronze_ch.vd_isos IS
  'Canton-VD ISOS (federally-derived). Source: /481 sites (Point) + /404 perimetres (Polygon). '
  'Feeds silver_ch.cadastral_patrimoine_inventaire_vd with valeur in (''isos_site'',''isos_perimetre''). Owner: VPS2.';
CREATE INDEX IF NOT EXISTS vd_isos_geom_idx
  ON bronze_ch.vd_isos USING GIST (geometry) WHERE deleted_at IS NULL;


-- 9. vd_region_archeologique  (canton-VD #320, polygon)
CREATE TABLE IF NOT EXISTS bronze_ch.vd_region_archeologique (
  nom_region       text,
  raw_data         jsonb,
  arcgis_objectid  bigint NOT NULL,
  geometry         geometry(MultiPolygon, 2056),
  source_layer     text   NOT NULL DEFAULT 'vd.region_archeologique'
                   CHECK (source_layer = 'vd.region_archeologique'),
  canton_code      text   NOT NULL DEFAULT 'VD' CHECK (canton_code = 'VD'),
  first_seen_at    timestamptz NOT NULL DEFAULT now(),
  last_seen_at     timestamptz NOT NULL DEFAULT now(),
  deleted_at       timestamptz,
  PRIMARY KEY (source_layer, arcgis_objectid)
);
COMMENT ON TABLE bronze_ch.vd_region_archeologique IS
  'Canton-VD archaeological regions (polygon). Source: agsgc.map.vd.ch /320. Feeds '
  'NATIONAL silver_ch.cadastral_archaeology with record_type=''region''. Owner: VPS2.';
CREATE INDEX IF NOT EXISTS vd_region_archeologique_geom_idx
  ON bronze_ch.vd_region_archeologique USING GIST (geometry) WHERE deleted_at IS NULL;


-- 10. vd_ddp_pts  (canton-VD #430, multipoint, DDP centroids)
CREATE TABLE IF NOT EXISTS bronze_ch.vd_ddp_pts (
  numero           text,
  egris_egrid      text,
  genre_txt        text,
  arcgis_objectid  bigint NOT NULL,
  geometry         geometry(MultiPoint, 2056),
  source_layer     text   NOT NULL DEFAULT 'vd.ddp_source_mens_officielle'
                   CHECK (source_layer = 'vd.ddp_source_mens_officielle'),
  canton_code      text   NOT NULL DEFAULT 'VD' CHECK (canton_code = 'VD'),
  first_seen_at    timestamptz NOT NULL DEFAULT now(),
  last_seen_at     timestamptz NOT NULL DEFAULT now(),
  deleted_at       timestamptz,
  PRIMARY KEY (source_layer, arcgis_objectid)
);
COMMENT ON TABLE bronze_ch.vd_ddp_pts IS
  'Canton-VD DDP centroids (mensuration officielle). MultiPoint geometry. Source: '
  'agsgc.map.vd.ch /430. Joined with vd_lausanne_ddp polygons on egrid in silver. Owner: VPS1.';
CREATE INDEX IF NOT EXISTS vd_ddp_pts_egrid_idx
  ON bronze_ch.vd_ddp_pts (egris_egrid) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS vd_ddp_pts_geom_idx
  ON bronze_ch.vd_ddp_pts USING GIST (geometry) WHERE deleted_at IS NULL;


-- 11. vd_lausanne_ddp  (Lausanne WFS, polygon DDP)
CREATE TABLE IF NOT EXISTS bronze_ch.vd_lausanne_ddp (
  no_parc        text,
  type_txt       text,
  no_commune     integer,
  commune_name   text,
  proprietaire   text,
  surface_m2     numeric,
  href_geomatik  text,
  source_pk      text NOT NULL,
  geometry       geometry(MultiPolygon, 2056),
  source_layer   text NOT NULL DEFAULT 'bdcad_bf_parc_pol_ddp'
                 CHECK (source_layer = 'bdcad_bf_parc_pol_ddp'),
  canton_code    text NOT NULL DEFAULT 'VD' CHECK (canton_code = 'VD'),
  first_seen_at  timestamptz NOT NULL DEFAULT now(),
  last_seen_at   timestamptz NOT NULL DEFAULT now(),
  deleted_at     timestamptz,
  PRIMARY KEY (source_layer, source_pk)
);
COMMENT ON TABLE bronze_ch.vd_lausanne_ddp IS
  'Lausanne DDP polygons (canton-VD has multipoint only). Source: '
  'map.lausanne.ch WFS bdcad_bf_parc_pol_ddp. Joins to vd_ddp_pts on no_parc or geometry. Owner: VPS1.';
CREATE INDEX IF NOT EXISTS vd_lausanne_ddp_no_parc_idx
  ON bronze_ch.vd_lausanne_ddp (no_parc) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS vd_lausanne_ddp_geom_idx
  ON bronze_ch.vd_lausanne_ddp USING GIST (geometry) WHERE deleted_at IS NULL;


-- 12. vd_lausanne_dp  (Lausanne WFS, polygon Domaine Public)
CREATE TABLE IF NOT EXISTS bronze_ch.vd_lausanne_dp (
  no_parc        text,
  type_txt       text,
  no_commune     integer,
  commune_name   text,
  proprietaire   text,
  surface_m2     numeric,
  href_geomatik  text,
  source_pk      text NOT NULL,
  geometry       geometry(MultiPolygon, 2056),
  source_layer   text NOT NULL DEFAULT 'bdcad_bf_parc_pol_dp'
                 CHECK (source_layer = 'bdcad_bf_parc_pol_dp'),
  canton_code    text NOT NULL DEFAULT 'VD' CHECK (canton_code = 'VD'),
  first_seen_at  timestamptz NOT NULL DEFAULT now(),
  last_seen_at   timestamptz NOT NULL DEFAULT now(),
  deleted_at     timestamptz,
  PRIMARY KEY (source_layer, source_pk)
);
COMMENT ON TABLE bronze_ch.vd_lausanne_dp IS
  'Lausanne Domaine Public parcels (federal_cadastral_parcels excludes DP). '
  'Source: map.lausanne.ch WFS bdcad_bf_parc_pol_dp. Owner: VPS1.';
CREATE INDEX IF NOT EXISTS vd_lausanne_dp_geom_idx
  ON bronze_ch.vd_lausanne_dp USING GIST (geometry) WHERE deleted_at IS NULL;


-- 13. vd_lausanne_servitudes  (Lausanne WFS, 8 source layers, consolidated)
CREATE TABLE IF NOT EXISTS bronze_ch.vd_lausanne_servitudes (
  type_txt        text,
  nom             text,
  id_rf           text,            -- Register Number — silver join key
  lien_idgo       text,
  source_pk       text NOT NULL,
  geometry        geometry(Geometry, 2056),  -- line, point, or surf depending on layer
  source_layer    text NOT NULL
                  CHECK (source_layer IN (
                    'bdcad_servitudes_passages_pub_line',
                    'bdcad_servitudes_passages_pub_point',
                    'bdcad_servitudes_passages_pub_surf',
                    'bdcad_servitudes_passages_canalisations_pub_line',
                    'bdcad_servitudes_passages_canalisations_pub_point',
                    'bdcad_servitudes_passages_canalisations_pub_surf',
                    'bdcad_servitudes_usage_pub_line',
                    'bdcad_servitudes_usage_pub_surf'
                  )),
  geom_kind       text NOT NULL CHECK (geom_kind IN ('line','point','surf')),
  canton_code     text NOT NULL DEFAULT 'VD' CHECK (canton_code = 'VD'),
  first_seen_at   timestamptz NOT NULL DEFAULT now(),
  last_seen_at    timestamptz NOT NULL DEFAULT now(),
  deleted_at      timestamptz,
  PRIMARY KEY (source_layer, source_pk)
);
COMMENT ON TABLE bronze_ch.vd_lausanne_servitudes IS
  'Lausanne public servitudes (canton-VD publishes none). 8 source layers consolidated by '
  'source_layer + geom_kind discriminators. Genre derived in silver from source_layer prefix. '
  'Owner: VPS1. Naming intent: bronze=vd_lausanne_* (data is Lausanne-only today); '
  'silver=cadastral_servitudes_vd (room for other VD communes to UNION in later).';
CREATE INDEX IF NOT EXISTS vd_lausanne_servitudes_layer_idx
  ON bronze_ch.vd_lausanne_servitudes (source_layer) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS vd_lausanne_servitudes_idrf_idx
  ON bronze_ch.vd_lausanne_servitudes (id_rf) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS vd_lausanne_servitudes_geom_idx
  ON bronze_ch.vd_lausanne_servitudes USING GIST (geometry) WHERE deleted_at IS NULL;


-- 14. vd_lausanne_pa_etude  (Lausanne WFS, polygon, plans à l'étude)
CREATE TABLE IF NOT EXISTS bronze_ch.vd_lausanne_pa_etude (
  nom_projet      text,
  source_pk       text NOT NULL,
  geometry        geometry(MultiPolygon, 2056),
  source_layer    text NOT NULL DEFAULT 'amenagement_pga_pa_etude'
                  CHECK (source_layer = 'amenagement_pga_pa_etude'),
  canton_code     text NOT NULL DEFAULT 'VD' CHECK (canton_code = 'VD'),
  first_seen_at   timestamptz NOT NULL DEFAULT now(),
  last_seen_at    timestamptz NOT NULL DEFAULT now(),
  deleted_at      timestamptz,
  PRIMARY KEY (source_layer, source_pk)
);
COMMENT ON TABLE bronze_ch.vd_lausanne_pa_etude IS
  'Lausanne plans d''affectation à l''étude (densification candidates). '
  'Source: map.lausanne.ch WFS amenagement_pga_pa_etude. Owner: VPS2.';
CREATE INDEX IF NOT EXISTS vd_lausanne_pa_etude_geom_idx
  ON bronze_ch.vd_lausanne_pa_etude USING GIST (geometry) WHERE deleted_at IS NULL;


-- 15. vd_lausanne_parking_sectors  (Lausanne WFS, polygon)
CREATE TABLE IF NOT EXISTS bronze_ch.vd_lausanne_parking_sectors (
  secteur         text,
  source_pk       text NOT NULL,
  geometry        geometry(MultiPolygon, 2056),
  source_layer    text NOT NULL DEFAULT 'amenagement_pga_sect_stationnement'
                  CHECK (source_layer = 'amenagement_pga_sect_stationnement'),
  canton_code     text NOT NULL DEFAULT 'VD' CHECK (canton_code = 'VD'),
  first_seen_at   timestamptz NOT NULL DEFAULT now(),
  last_seen_at    timestamptz NOT NULL DEFAULT now(),
  deleted_at      timestamptz,
  PRIMARY KEY (source_layer, source_pk)
);
COMMENT ON TABLE bronze_ch.vd_lausanne_parking_sectors IS
  'Lausanne parking policy sectors (commune-specific by definition). '
  'Feeds silver_ch.cadastral_zones_vd with zone_type=''stationnement_sector''. Owner: VPS2.';
CREATE INDEX IF NOT EXISTS vd_lausanne_parking_sectors_geom_idx
  ON bronze_ch.vd_lausanne_parking_sectors USING GIST (geometry) WHERE deleted_at IS NULL;


-- 16. vd_lausanne_archeologie  (Lausanne WFS, 8 source layers, consolidated)
CREATE TABLE IF NOT EXISTS bronze_ch.vd_lausanne_archeologie (
  description       text,
  mesure            text,
  note_detail       text,
  note_carto        text,
  url_fiche         text,
  url_carte         text,
  source_pk         text NOT NULL,
  geometry          geometry(Geometry, 2056),
  source_layer      text NOT NULL
                    CHECK (source_layer IN (
                      'amenagement_rec_arch_notes_point',
                      'amenagement_rec_arch_notes_site',
                      'amenagement_rec_arch_notes_surf',
                      'amenagement_rec_arch_mesures_pbc',
                      'amenagement_rec_arch_mesures_plan_class',
                      'amenagement_rec_arch_mesures_point',
                      'amenagement_rec_arch_mesures_site',
                      'amenagement_rec_arch_mesures_surf'
                    )),
  record_type       text NOT NULL CHECK (record_type IN ('site','mesure')),
  record_subtype    text NOT NULL CHECK (record_subtype IN ('notes','mesures','pbc','plan_class')),
  geom_kind         text NOT NULL CHECK (geom_kind IN ('point','site','surf')),
  canton_code       text NOT NULL DEFAULT 'VD' CHECK (canton_code = 'VD'),
  first_seen_at     timestamptz NOT NULL DEFAULT now(),
  last_seen_at      timestamptz NOT NULL DEFAULT now(),
  deleted_at        timestamptz,
  PRIMARY KEY (source_layer, source_pk)
);
COMMENT ON TABLE bronze_ch.vd_lausanne_archeologie IS
  'Lausanne archaeological site/measure records (8 source layers consolidated). '
  'Feeds NATIONAL silver_ch.cadastral_archaeology. Owner: VPS2.';
CREATE INDEX IF NOT EXISTS vd_lausanne_archeologie_layer_idx
  ON bronze_ch.vd_lausanne_archeologie (source_layer) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS vd_lausanne_archeologie_geom_idx
  ON bronze_ch.vd_lausanne_archeologie USING GIST (geometry) WHERE deleted_at IS NULL;


-- 17. vd_lausanne_energie_cad_bati  (Lausanne WFS, point, per-building energy)
CREATE TABLE IF NOT EXISTS bronze_ch.vd_lausanne_energie_cad_bati (
  egid                    text,
  rue                     text,
  no_rue                  text,
  no_parcelle             text,
  solution_heat           text,
  horizon_heat            text,
  besoins_kwh             numeric,
  vecteur_actuel          text,
  detail_solution_heat    text,
  source_pk               text NOT NULL,
  geometry                geometry(Point, 2056),
  source_layer            text NOT NULL DEFAULT 'energie_cad_bati'
                          CHECK (source_layer = 'energie_cad_bati'),
  canton_code             text NOT NULL DEFAULT 'VD' CHECK (canton_code = 'VD'),
  first_seen_at           timestamptz NOT NULL DEFAULT now(),
  last_seen_at            timestamptz NOT NULL DEFAULT now(),
  deleted_at              timestamptz,
  PRIMARY KEY (source_layer, source_pk)
);
COMMENT ON TABLE bronze_ch.vd_lausanne_energie_cad_bati IS
  'Lausanne per-building energy cadastre (heating solution, demand kWh). EGID-keyed. '
  'Canton-VD has only per-hectare aggregates, hence Lausanne fallback. Owner: VPS3.';
CREATE INDEX IF NOT EXISTS vd_lausanne_energie_cad_bati_egid_idx
  ON bronze_ch.vd_lausanne_energie_cad_bati (egid) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS vd_lausanne_energie_cad_bati_geom_idx
  ON bronze_ch.vd_lausanne_energie_cad_bati USING GIST (geometry) WHERE deleted_at IS NULL;


-- 18. federal_bav_transit  (federal api3.geo.admin.ch, national scope)
CREATE TABLE IF NOT EXISTS bronze_ch.federal_bav_transit (
  didok            text,                 -- BAV unique stop ID (DiDok)
  name             text,
  mode             text,                 -- rail | bus | tram | funicular | ship
  abbr             text,
  validity_from    date,
  validity_to      date,
  accessibility    text,
  gtfs_stop_id     text,
  raw_attributes   jsonb,
  geometry         geometry(Point, 2056),
  canton_code      text,                 -- populated by spatial join in silver / on insert from federal_communes lookup
  source_layer     text NOT NULL DEFAULT 'ch.bav.haltestellen-oev'
                   CHECK (source_layer = 'ch.bav.haltestellen-oev'),
  first_seen_at    timestamptz NOT NULL DEFAULT now(),
  last_seen_at     timestamptz NOT NULL DEFAULT now(),
  deleted_at       timestamptz,
  PRIMARY KEY (source_layer, didok)
);
COMMENT ON TABLE bronze_ch.federal_bav_transit IS
  'Federal BAV public transit stops (canonical national source, ~31K stops). '
  'Source: api3.geo.admin.ch ch.bav.haltestellen-oev. canton_code derived from spatial join '
  'to bronze_ch.federal_communes during parser ingest. Quarterly cadence. Owner: VPS3.';
CREATE INDEX IF NOT EXISTS federal_bav_transit_canton_idx
  ON bronze_ch.federal_bav_transit (canton_code) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS federal_bav_transit_didok_idx
  ON bronze_ch.federal_bav_transit (didok) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS federal_bav_transit_geom_idx
  ON bronze_ch.federal_bav_transit USING GIST (geometry) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS federal_bav_transit_mode_idx
  ON bronze_ch.federal_bav_transit (mode) WHERE deleted_at IS NULL;


-- ----------------------------------------------------------------------------
-- Refresh-tracking metadata table (one row per parser run, for monitoring)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze_ch.vd_enrichment_runs (
  id              bigserial PRIMARY KEY,
  dataset_code    text NOT NULL,
  host            text NOT NULL,
  started_at      timestamptz NOT NULL DEFAULT now(),
  finished_at     timestamptz,
  rows_inserted   integer,
  rows_updated    integer,
  rows_softdeleted integer,
  status          text CHECK (status IN ('running','success','failed')) NOT NULL DEFAULT 'running',
  error_message   text
);
CREATE INDEX IF NOT EXISTS vd_enrichment_runs_dataset_idx
  ON bronze_ch.vd_enrichment_runs (dataset_code, started_at DESC);

COMMIT;

-- ============================================================================
-- After apply: NOTIFY pgrst, 'reload schema'  to refresh PostgREST cache.
-- ============================================================================
NOTIFY pgrst, 'reload schema';
