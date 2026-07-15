-- ============================================================================
-- 2026-07-15 — VD RDPPF: 3 → 7 themes  +  geometry exports for map layers
-- ============================================================================
-- Deliverable A5 (already-built themes, exported) + Deliverable B (geometry).
--
-- ── A5 WAS ALREADY BUILT — this is a SURFACING job, not an ingest ───────────
-- Four VD RDPPF link matviews were already populated and had simply never been
-- exported. Nothing was ingested for them here:
--   link_plot_patrimoine_inventaire_vd  119,446 rows / 80,023 plots (28.18%)
--   link_plot_archeology_vd              54,973 rows / 32,659 plots (11.50%)
--   link_plot_classement_vd               4,156 rows /  4,084 plots (1.44%)
--   link_plot_densification_vd            2,124 rows /  2,038 plots (0.72%)
-- (Third time on this project that an "add data" brief was really "surface what
-- already exists" — check the link layer before scoping an ingest.)
--
-- ── is_restrictive, extended to 7 themes ───────────────────────────────────
-- Rule: true = a legally binding constraint on the parcel TODAY.
-- false = classification blanket, or informational / not-yet-in-force.
--   zones_reservees      true   LAT art.27 building freeze
--   protection_eaux      S1/S2/S3 only — Au/üB/S/Zu/Périmètre are the blanket
--   sites_pollues        true   KbS legal encumbrance
--   classement           true   classified/protected object
--   archeology           true   binding survey/excavation obligations
--   patrimoine_inventaire true  ISOS perimeters/sites + jardins historiques
--   densification        FALSE  — `plan_affectation_etude` is explicitly À L'ÉTUDE
--                               (not in force) and is a densification OPPORTUNITY
--                               signal, not a restriction. Lausanne-only.
-- ⚠️ FLAGGED FOR REVIEW: patrimoine_inventaire is an INVENTORY (LPN art.5), not a
--    hard OEREB restriction, and it is 93,719 isos_perimetre rows — marking it
--    true moves the "any restriction" headline a lot. Called true because ISOS has
--    binding planning effect, but this is the single most reviewable call here.
--    (Same open-question family as protection_eaux `zone/Périmètre`.)
--
-- ── B: geometry exports ────────────────────────────────────────────────────
-- Additive. The attribute-only per-plot exports are left EXACTLY as they are —
-- the plot detail panel uses them; these are for the map.
-- Geometry stored EPSG:4326 (consumer/map convention) + GIST. The ST_MakeValid
-- guard is applied AFTER ST_Transform, never before: ST_Transform itself CREATES
-- invalid geometry (bug 4d930c20 — 1 of the 9 VD plots is valid in 4326 and
-- invalid only once reprojected), so a pre-transform guard cannot survive.
--
-- ⚠️ `cadastral_zones_vd` carries 2,607 `foret` rows with NULL geometry — the
--    41a643b8 damage propagated into silver. They are invisible in
--    link_plot_zones_vd (ST_Intersects on NULL yields nothing) but WOULD surface
--    in a geometry export, so they are filtered out explicitly.
--
-- ROLLBACK at the bottom of this file.
-- ============================================================================

BEGIN;
SET LOCAL statement_timeout = '2700s';
SET LOCAL lock_timeout = '60s';

-- ===========================================================================
-- A5 — rebuild export_rdppf_national with 7 themes (was 3)
--      Zero dependents on re-LLM (verified) ⇒ DROP … RESTRICT is safe.
--      lamap_db's loaded copy is a separate table and is NOT touched.
-- ===========================================================================
DROP MATERIALIZED VIEW IF EXISTS gold_ch.export_rdppf_national RESTRICT;
CREATE MATERIALIZED VIEW gold_ch.export_rdppf_national AS
WITH unioned AS (
  -- 1. zones réservées (LAT art.27 planning freeze)
  SELECT l.egrid, 'zones_reservees'::text AS theme, nullif(l.zone_kind,'') AS sous_type,
         true AS is_restrictive, l.zone_name AS libelle, l.overlap_m2,
         l.statut_juridique, l.date_entree_vigueur::date AS date_entree_vigueur
  FROM silver_ch.link_plot_zones_reservees_vd l

  UNION ALL
  -- 2. protection des eaux souterraines — restrictive ONLY for zones S1/S2/S3
  SELECT l.egrid, 'protection_eaux', l.indice_protection,
         (l.protection_kind='zone' AND l.indice_protection IN ('S1','S2','S3')),
         CASE l.protection_kind
           WHEN 'zone'    THEN 'Zone de protection des eaux '   || l.indice_protection
           WHEN 'secteur' THEN 'Secteur de protection des eaux ' || l.indice_protection
           WHEN 'aire'    THEN 'Aire d''alimentation '           || l.indice_protection
         END,
         l.overlap_m2, NULL::text, l.date_acceptation::date
  FROM silver_ch.link_plot_protection_eaux_vd l

  UNION ALL
  -- 3. sites pollués (KbS)
  SELECT l.egrid, 'sites_pollues', l.type_site, true,
         coalesce(nullif(l.nom_site,''), nullif(s.activite,''), l.type_site),
         l.overlap_m2, l.nom_phase, NULL::date
  FROM silver_ch.link_plot_sites_pollues_vd l
  JOIN silver_ch.sites_pollues_vd s ON s.id = l.site_pollue_id

  UNION ALL
  -- 4. patrimoine inventaire (ISOS perimetres/sites + jardins historiques)  [A5, pre-built]
  SELECT l.egrid, 'patrimoine_inventaire', nullif(l.valeur,''), true,
         l.designation, l.overlap_m2, NULL::text, NULL::date
  FROM silver_ch.link_plot_patrimoine_inventaire_vd l

  UNION ALL
  -- 5. archéologie (régions / sites / mesures)                              [A5, pre-built]
  SELECT l.egrid, 'archeology', nullif(l.record_type,''), true,
         coalesce(nullif(l.designation,''), nullif(l.record_subtype,''), l.record_type),
         l.overlap_m2, NULL::text, NULL::date
  FROM silver_ch.link_plot_archeology_vd l

  UNION ALL
  -- 6. classement (monuments classés)                                       [A5, pre-built]
  --    ⚠️ link.type_protection is 100% EMPTY (0/4,156) — a dead column, same family
  --    as h_max / sous_theme. sous_type is therefore NULL, not faked.
  --    NOTE cadastral_patrimoine_classe_vd.id is INTEGER while the link's
  --    classement_id is TEXT — cast to join (verified: 4,156/4,156 match).
  SELECT l.egrid, 'classement', NULL::text, true,
         l.designation, l.overlap_m2, NULL::text, c.date_classement::date
  FROM silver_ch.link_plot_classement_vd l
  LEFT JOIN silver_ch.cadastral_patrimoine_classe_vd c ON c.id::text = l.classement_id

  UNION ALL
  -- 7. densification / plans d'affectation à l'étude — NOT restrictive       [A5, pre-built]
  SELECT l.egrid, 'densification', nullif(l.densification_type,''), false,
         l.sector_name, l.overlap_m2, NULL::text, NULL::date
  FROM silver_ch.link_plot_densification_vd l
)
SELECT 'VD'::text AS canton_code,
       u.egrid,
       cp.commune_bfs,
       cp.parcel_number,
       u.theme,
       u.sous_type,
       bool_or(u.is_restrictive)                                                AS is_restrictive,
       (array_agg(u.libelle ORDER BY u.overlap_m2 DESC NULLS LAST))[1]          AS libelle,
       sum(u.overlap_m2)::numeric                                               AS overlap_m2,
       (array_agg(u.statut_juridique ORDER BY u.overlap_m2 DESC NULLS LAST))[1] AS statut_juridique,
       max(u.date_entree_vigueur)                                               AS date_entree_vigueur,
       now()                                                                    AS updated_at
FROM unioned u
JOIN silver_ch.cadastral_plots cp ON cp.egrid = u.egrid AND cp.canton_code = 'VD'
GROUP BY u.egrid, cp.commune_bfs, cp.parcel_number, u.theme, u.sous_type;

CREATE UNIQUE INDEX export_rdppf_national_grain_idx
  ON gold_ch.export_rdppf_national (egrid, theme, sous_type) NULLS NOT DISTINCT;
CREATE INDEX export_rdppf_national_canton_idx      ON gold_ch.export_rdppf_national (canton_code);
CREATE INDEX export_rdppf_national_egrid_idx       ON gold_ch.export_rdppf_national (egrid);
CREATE INDEX export_rdppf_national_theme_idx       ON gold_ch.export_rdppf_national (theme);
CREATE INDEX export_rdppf_national_restrictive_idx ON gold_ch.export_rdppf_national (is_restrictive) WHERE is_restrictive;
CREATE INDEX export_rdppf_national_commune_idx     ON gold_ch.export_rdppf_national (commune_bfs);
COMMENT ON MATERIALIZED VIEW gold_ch.export_rdppf_national IS
  'Consumer export → lamap_db ref.rdppf_national. One row per plot × theme × sous_type. Keyed on EGRID. '
  '7 themes (was 3 — the other 4 were already built and merely unexported). '
  '⚠️ ALWAYS filter is_restrictive=true. densification=false (plans à l''étude, not in force). '
  'protection_eaux false except S1/S2/S3 (Au/üB/S/Zu/Périmètre = classification blanket). '
  'Noise + servitudes still absent — gated on f50f2e08.';

-- ===========================================================================
-- B1 — gold_ch.export_zones_national_geom   (grain: ONE ROW PER ZONE POLYGON)
--      Renders the zoning layer; filter zone_primaire='agricole' for that layer.
-- ===========================================================================
DROP MATERIALIZED VIEW IF EXISTS gold_ch.export_zones_national_geom RESTRICT;
CREATE MATERIALIZED VIEW gold_ch.export_zones_national_geom AS
WITH zone_commune AS (
  -- The zone source carries NO commune identifier: `code_com`/`designation_com` are
  -- the COMMUNAL ZONE code/designation ("Zone villa"), NOT a commune (values
  -- 110101-490807). So commune is derived from the plots the zone actually covers.
  -- ⚠️ DOMINANT attribution: 70,818 zone polygons sit in 1 commune but 12,062 span
  --    2+, so this is the modal commune, not an exhaustive mapping.
  SELECT zone_id, commune_bfs FROM (
    SELECT l.zone_id, cp.commune_bfs,
           row_number() OVER (PARTITION BY l.zone_id
                              ORDER BY count(*) DESC, cp.commune_bfs) AS rn
    FROM silver_ch.link_plot_zones_vd l
    JOIN silver_ch.cadastral_plots cp ON cp.egrid = l.egrid AND cp.canton_code='VD'
    WHERE l.zone_type='affectation' AND cp.commune_bfs IS NOT NULL
    GROUP BY l.zone_id, cp.commune_bfs
  ) t WHERE rn = 1
),
g AS (
  SELECT z.id AS zone_id,
         za.designation_vd_n2 AS zone_affectation,
         CASE za.code_ch / 10
           WHEN 1 THEN 'zone à bâtir' WHEN 2 THEN 'agricole'
           WHEN 3 THEN 'à protéger'   WHEN 4 THEN 'autres' END AS zone_primaire,
         za.designation_ch    AS zone_synthetique,
         za.statut_juridique,
         za.date_entree_vigueur::date AS date_entree_vigueur,
         ST_Transform(z.geometry, 4326) AS g4326        -- guard AFTER the transform
  FROM silver_ch.cadastral_zones_vd z
  JOIN bronze_ch.vd_zone_affectation za
    ON 'za_' || za.arcgis_objectid::text = z.id AND za.deleted_at IS NULL
  WHERE z.zone_type = 'affectation'
    AND z.geometry IS NOT NULL          -- excludes the 2,607 NULL-geom foret rows (41a643b8)
    AND NOT ST_IsEmpty(z.geometry)
)
SELECT 'VD'::text AS canton,
       g.zone_id,
       g.zone_affectation,
       g.zone_primaire,
       g.zone_synthetique,
       zc.commune_bfs,
       ST_Multi(CASE WHEN ST_IsValid(g.g4326) THEN g.g4326
                     ELSE ST_CollectionExtract(ST_MakeValid(g.g4326), 3) END
       )::geometry(MultiPolygon, 4326) AS geometry,
       g.statut_juridique,
       g.date_entree_vigueur,
       now() AS updated_at
FROM g LEFT JOIN zone_commune zc ON zc.zone_id = g.zone_id;

CREATE UNIQUE INDEX export_zones_national_geom_id_idx   ON gold_ch.export_zones_national_geom (zone_id);
CREATE INDEX export_zones_national_geom_gix             ON gold_ch.export_zones_national_geom USING GIST (geometry);
CREATE INDEX export_zones_national_geom_canton_idx      ON gold_ch.export_zones_national_geom (canton);
CREATE INDEX export_zones_national_geom_primaire_idx    ON gold_ch.export_zones_national_geom (zone_primaire);
CREATE INDEX export_zones_national_geom_commune_idx     ON gold_ch.export_zones_national_geom (commune_bfs);
COMMENT ON MATERIALIZED VIEW gold_ch.export_zones_national_geom IS
  'MAP LAYER export → lamap_db. Grain: ONE ROW PER ZONE POLYGON (not per plot). EPSG:4326 + GIST. '
  'Renders the VD zoning layer; filter zone_primaire=''agricole'' for the agricole layer. '
  '⚠️ commune_bfs is a DOMINANT attribution derived from covered plots (the zone source has no commune '
  'column — code_com/designation_com are the COMMUNAL ZONE code/name, not a commune): 12,062 of 82,880 '
  'polygons span 2+ communes. Excludes the 2,607 NULL-geometry foret rows (bug 41a643b8).';

-- ===========================================================================
-- B2 — gold_ch.export_rdppf_national_geom  (grain: ONE ROW PER RESTRICTION POLYGON)
-- ===========================================================================
DROP MATERIALIZED VIEW IF EXISTS gold_ch.export_rdppf_national_geom RESTRICT;
CREATE MATERIALIZED VIEW gold_ch.export_rdppf_national_geom AS
WITH theme_commune AS (   -- modal commune per theme polygon, from its own link
  SELECT 'zones_reservees'::text AS theme, zone_reservee_id AS oid, commune_bfs FROM (
    SELECT l.zone_reservee_id, cp.commune_bfs,
           row_number() OVER (PARTITION BY l.zone_reservee_id ORDER BY count(*) DESC, cp.commune_bfs) rn
    FROM silver_ch.link_plot_zones_reservees_vd l
    JOIN silver_ch.cadastral_plots cp ON cp.egrid=l.egrid AND cp.canton_code='VD'
    WHERE cp.commune_bfs IS NOT NULL GROUP BY 1,2) t WHERE rn=1
  UNION ALL
  SELECT 'protection_eaux', protection_id, commune_bfs FROM (
    SELECT l.protection_id, cp.commune_bfs,
           row_number() OVER (PARTITION BY l.protection_id ORDER BY count(*) DESC, cp.commune_bfs) rn
    FROM silver_ch.link_plot_protection_eaux_vd l
    JOIN silver_ch.cadastral_plots cp ON cp.egrid=l.egrid AND cp.canton_code='VD'
    WHERE cp.commune_bfs IS NOT NULL GROUP BY 1,2) t WHERE rn=1
  UNION ALL
  SELECT 'sites_pollues', site_pollue_id, commune_bfs FROM (
    SELECT l.site_pollue_id, cp.commune_bfs,
           row_number() OVER (PARTITION BY l.site_pollue_id ORDER BY count(*) DESC, cp.commune_bfs) rn
    FROM silver_ch.link_plot_sites_pollues_vd l
    JOIN silver_ch.cadastral_plots cp ON cp.egrid=l.egrid AND cp.canton_code='VD'
    WHERE cp.commune_bfs IS NOT NULL GROUP BY 1,2) t WHERE rn=1
  UNION ALL
  SELECT 'patrimoine_inventaire', inventaire_id, commune_bfs FROM (
    SELECT l.inventaire_id, cp.commune_bfs,
           row_number() OVER (PARTITION BY l.inventaire_id ORDER BY count(*) DESC, cp.commune_bfs) rn
    FROM silver_ch.link_plot_patrimoine_inventaire_vd l
    JOIN silver_ch.cadastral_plots cp ON cp.egrid=l.egrid AND cp.canton_code='VD'
    WHERE cp.commune_bfs IS NOT NULL GROUP BY 1,2) t WHERE rn=1
  UNION ALL
  SELECT 'archeology', archeology_id, commune_bfs FROM (
    SELECT l.archeology_id, cp.commune_bfs,
           row_number() OVER (PARTITION BY l.archeology_id ORDER BY count(*) DESC, cp.commune_bfs) rn
    FROM silver_ch.link_plot_archeology_vd l
    JOIN silver_ch.cadastral_plots cp ON cp.egrid=l.egrid AND cp.canton_code='VD'
    WHERE cp.commune_bfs IS NOT NULL GROUP BY 1,2) t WHERE rn=1
  UNION ALL
  SELECT 'classement', classement_id, commune_bfs FROM (
    SELECT l.classement_id, cp.commune_bfs,
           row_number() OVER (PARTITION BY l.classement_id ORDER BY count(*) DESC, cp.commune_bfs) rn
    FROM silver_ch.link_plot_classement_vd l
    JOIN silver_ch.cadastral_plots cp ON cp.egrid=l.egrid AND cp.canton_code='VD'
    WHERE cp.commune_bfs IS NOT NULL GROUP BY 1,2) t WHERE rn=1
  UNION ALL
  SELECT 'densification', densification_id, commune_bfs FROM (
    SELECT l.densification_id, cp.commune_bfs,
           row_number() OVER (PARTITION BY l.densification_id ORDER BY count(*) DESC, cp.commune_bfs) rn
    FROM silver_ch.link_plot_densification_vd l
    JOIN silver_ch.cadastral_plots cp ON cp.egrid=l.egrid AND cp.canton_code='VD'
    WHERE cp.commune_bfs IS NOT NULL GROUP BY 1,2) t WHERE rn=1
),
polys AS (
  SELECT 'zones_reservees'::text AS theme, z.id AS oid, nullif(z.zone_kind,'') AS sous_type,
         true AS is_restrictive, z.zone_name AS libelle, z.statut_juridique,
         ST_Transform(z.geometry,4326) AS g4326
  FROM silver_ch.zones_reservees_vd z WHERE z.canton_code='VD' AND z.geometry IS NOT NULL AND NOT ST_IsEmpty(z.geometry)
  UNION ALL
  SELECT 'protection_eaux', e.id, e.indice_protection,
         (e.protection_kind='zone' AND e.indice_protection IN ('S1','S2','S3')),
         CASE e.protection_kind
           WHEN 'zone'    THEN 'Zone de protection des eaux '   || e.indice_protection
           WHEN 'secteur' THEN 'Secteur de protection des eaux ' || e.indice_protection
           WHEN 'aire'    THEN 'Aire d''alimentation '           || e.indice_protection END,
         NULL::text, ST_Transform(e.geometry,4326)
  FROM silver_ch.protection_eaux_vd e WHERE e.canton_code='VD' AND e.geometry IS NOT NULL AND NOT ST_IsEmpty(e.geometry)
  UNION ALL
  SELECT 'sites_pollues', s.id, s.type_site, true,
         coalesce(nullif(s.nom_site,''), nullif(s.activite,''), s.type_site), s.nom_phase,
         ST_Transform(s.geometry,4326)
  FROM silver_ch.sites_pollues_vd s WHERE s.canton_code='VD' AND s.geometry IS NOT NULL AND NOT ST_IsEmpty(s.geometry)
  UNION ALL
  SELECT 'patrimoine_inventaire', p.id, nullif(p.valeur,''), true, p.designation, NULL::text,
         ST_Transform(p.geometry,4326)
  FROM silver_ch.cadastral_patrimoine_inventaire_vd p WHERE p.canton_code='VD' AND p.geometry IS NOT NULL AND NOT ST_IsEmpty(p.geometry)
  UNION ALL
  SELECT 'archeology', a.id, nullif(a.record_type,''), true,
         coalesce(nullif(a.designation,''), nullif(a.record_subtype,''), a.record_type), NULL::text,
         ST_Transform(a.geometry,4326)
  FROM silver_ch.cadastral_archaeology a WHERE a.canton_code='VD' AND a.geometry IS NOT NULL AND NOT ST_IsEmpty(a.geometry)
  UNION ALL
  SELECT 'classement', c.id::text, NULL::text, true, c.designation, NULL::text,
         ST_Transform(c.geometry,4326)
  FROM silver_ch.cadastral_patrimoine_classe_vd c WHERE c.canton_code='VD' AND c.geometry IS NOT NULL AND NOT ST_IsEmpty(c.geometry)
  UNION ALL
  SELECT 'densification', d.id, nullif(d.densification_type,''), false, d.sector_name, NULL::text,
         ST_Transform(d.geometry,4326)
  FROM silver_ch.cadastral_densification_vd d WHERE d.canton_code='VD' AND d.geometry IS NOT NULL AND NOT ST_IsEmpty(d.geometry)
)
-- ⚠️ MIXED GEOMETRY TYPES — the column CANNOT be geometry(MultiPolygon,4326) as the
--    brief specified. Two sources are genuinely mixed polygon+point:
--      cadastral_patrimoine_inventaire_vd  5,797 MultiPolygon + 141 Point (isos_site)
--      cadastral_archaeology              11,062 MultiPolygon + 678 Point
--    Typing MultiPolygon would silently DROP 819 real restriction objects, and the
--    ST_CollectionExtract(...,3) guard extracts polygons ONLY — it would return EMPTY
--    for every point. So: generic geometry(Geometry,4326) + an explicit geom_type
--    column so the consumer branches (polygon → fill layer, point → marker layer).
, cleaned AS (
  SELECT p.*,
         CASE
           -- points are always valid; MakeValid/CollectionExtract would destroy them
           WHEN GeometryType(p.g4326) IN ('POINT','MULTIPOINT') THEN ST_Multi(p.g4326)
           WHEN ST_IsValid(p.g4326)                             THEN ST_Multi(p.g4326)
           ELSE ST_Multi(ST_CollectionExtract(ST_MakeValid(p.g4326), 3))
         END AS geom
  FROM polys p
)
SELECT 'VD'::text AS canton,
       c.theme,
       c.oid                                   AS object_id,
       c.sous_type,
       c.is_restrictive,
       c.libelle,
       tc.commune_bfs,
       c.geom::geometry(Geometry, 4326)        AS geometry,
       GeometryType(c.geom)                    AS geom_type,
       c.statut_juridique,
       now()                                   AS updated_at
FROM cleaned c
LEFT JOIN theme_commune tc ON tc.theme = c.theme AND tc.oid = c.oid
WHERE c.geom IS NOT NULL AND NOT ST_IsEmpty(c.geom);

CREATE UNIQUE INDEX export_rdppf_national_geom_id_idx  ON gold_ch.export_rdppf_national_geom (theme, object_id);
CREATE INDEX export_rdppf_national_geom_gix            ON gold_ch.export_rdppf_national_geom USING GIST (geometry);
CREATE INDEX export_rdppf_national_geom_canton_idx     ON gold_ch.export_rdppf_national_geom (canton);
CREATE INDEX export_rdppf_national_geom_theme_idx      ON gold_ch.export_rdppf_national_geom (theme, is_restrictive);
CREATE INDEX export_rdppf_national_geom_commune_idx    ON gold_ch.export_rdppf_national_geom (commune_bfs);
CREATE INDEX export_rdppf_national_geom_gtype_idx      ON gold_ch.export_rdppf_national_geom (geom_type);
COMMENT ON MATERIALIZED VIEW gold_ch.export_rdppf_national_geom IS
  'MAP LAYER export → lamap_db. Grain: ONE ROW PER RESTRICTION OBJECT (not per plot). EPSG:4326 + GIST. '
  '7 themes. ⚠️ Geometry is MIXED — column is geometry(Geometry,4326), NOT MultiPolygon: '
  'patrimoine_inventaire and archeology are genuinely polygon+point sources (819 point objects). Branch on '
  'geom_type (MULTIPOLYGON → fill layer, MULTIPOINT → marker layer). Typing MultiPolygon would have '
  'silently dropped those 819 real objects. '
  '⚠️ filter is_restrictive=true to draw only real restrictions — protection_eaux Au/üB blankets the canton '
  'and densification is à-l''étude. commune_bfs is a DOMINANT attribution derived from covered plots (most '
  'theme sources carry no commune column; NO_COMMUNE on sites_pollues is NOT a BFS — bug 90c20178).';
COMMENT ON COLUMN gold_ch.export_rdppf_national_geom.geom_type IS
  'MULTIPOLYGON or MULTIPOINT. Exists because patrimoine_inventaire (141 isos_site) and archeology (678) '
  'carry genuine point geometry alongside polygons. Consumer must branch on this to render both.';

GRANT SELECT ON gold_ch.export_rdppf_national,
                gold_ch.export_zones_national_geom,
                gold_ch.export_rdppf_national_geom
  TO anon, authenticated, service_role;

COMMIT;

-- ============================================================================
-- ROLLBACK
-- ============================================================================
-- BEGIN;
--   DROP MATERIALIZED VIEW IF EXISTS gold_ch.export_zones_national_geom RESTRICT;
--   DROP MATERIALIZED VIEW IF EXISTS gold_ch.export_rdppf_national_geom RESTRICT;
--   -- and re-create the 3-theme export_rdppf_national from migration 20260715000003
--   DELETE FROM supabase_migrations.schema_migrations WHERE version = '20260715000004';
-- COMMIT;
-- ============================================================================
