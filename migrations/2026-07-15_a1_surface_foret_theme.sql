-- ============================================================================
-- 2026-07-15 — A1 SURFACED: theme='foret' (+ A2 sites_pollues_federal carried forward)
-- ⚠️ FORÊT SOURCES SUBSTITUTED: /490 vd.limite_foret is UNUSABLE — the source returns NO
--    geometry for it (upstream defect, bug 41a643b8). NOT a polyline problem: /489 is the
--    same esriGeometryPolyline and returns geometry fine. Using instead:
--      /489 vd.distance_foret     6,664 MultiLineString → LFo art.17 setback  → geom_type='line'
--      /166 vd.reserve_forestiere   138 MultiPolygon    → forest reserves     → geom_type='polygon'
--    Forest AREA stays distinct (vd_zone_affectation code_ch=44, 16,932 zones).
-- ⚠️ foret is_restrictive=FALSE by default — the exported object is the LINE, not the setback
--    ZONE. The LFo art.17 setback itself IS binding; promoting it is Ilan's call. Deliberately
--    NOT re-inflating the restrictive headline (which is 46,673 plots / 16.43%).
--    distance_foret reaches 15,558 plots (5.48%) — promoting it would move the headline a lot.
-- ============================================================================
-- Adds the federal KbS sub-registers as their OWN theme. Base `sites_pollues` is
-- deliberately UNTOUCHED: it keys sous_type=type_site, so folding the federal
-- registers in under sous_type='base' would re-grain it (collapsing multiple
-- type_site rows per plot into one) and silently lower the base count.
--
--   theme      = 'sites_pollues_federal'
--   sous_type  ∈ militaire | aeroports | transports_publics   (the REGISTER)
--   libelle    = standorttyp (site type/description)
--   is_restrictive = true (binding, like base)
--   geom_type  = point | polygon (mixed as ingested — 154 point objects nationally)
--
-- The federal sources stay NOT canton-stamped at bronze/silver (they span FR/GE/VS/NE);
-- the link scopes to VD (557 fetched → 318 touch a VD plot) and the export stamps 'VD'.
-- ⚠️ overlap_m2 is NULL for point sites — a point has no area.
-- Carries forward the is_restrictive semantics of migration 20260715000005.
-- ============================================================================
-- Supersedes the flag semantics set in migration 20260715000004.
--
-- ── THE RULE IS NOW NARROWER AND SHARPER ───────────────────────────────────
--   is_restrictive = TRUE  ⇔  a BINDING BUILDING CONSTRAINT.
--   (Previously: "a legally binding constraint today" — which swept in
--    obligations that constrain PROCESS but never forbid building.)
--
-- Changes:
--   patrimoine_inventaire  true → FALSE   ISOS is an inventory (LPN art.5), not a
--                                          hard OEREB restriction. Removes the
--                                          28.18%-of-VD distortion from the headline.
--   archeology             true → FALSE   a SURVEY obligation, not a prohibition.
--   protection_eaux
--     sous_type='Périmètre' false → TRUE  Grundwasserschutzareal (2,807 plots) — a
--                                          genuine building restriction reserving land
--                                          for future water capture. GE treats its
--                                          id132_perim_protec_eaux_sout equivalent as
--                                          restrictive. It is NOT part of the Au/üB blanket.
--   unchanged: classement TRUE · sites_pollues TRUE · zones_reservees TRUE ·
--              protection_eaux S1/S2/S3 TRUE · densification FALSE
--
-- ⇒ protection_eaux restrictive set is now  ('S1','S2','S3','Périmètre')  — all of
--   which are protection_kind='zone'. The blanket (secteur Au/S/üB, aire Zu) stays FALSE.
--
-- ── geom_type ──────────────────────────────────────────────────────────────
-- Emits 'polygon' | 'point' (was PostGIS's raw 'MULTIPOLYGON'/'MULTIPOINT') so the
-- export is 1:1 with lamap_db ref.rdppf_national_geom.geom_type and the consumer
-- branches on a stable, human token rather than a PostGIS type name.
--
-- Both matviews have ZERO re-LLM dependents ⇒ DROP … RESTRICT is safe.
-- lamap_db's copies are separate tables and are NOT touched here.
--
-- ROLLBACK at the bottom of this file.
-- ============================================================================

BEGIN;
SET LOCAL statement_timeout = '2700s';
SET LOCAL lock_timeout = '60s';

-- ===========================================================================
-- 1. export_rdppf_national — corrected flags
-- ===========================================================================
DROP MATERIALIZED VIEW IF EXISTS gold_ch.export_rdppf_national RESTRICT;
CREATE MATERIALIZED VIEW gold_ch.export_rdppf_national AS
WITH unioned AS (
  -- zones réservées — LAT art.27 building freeze ⇒ binding
  SELECT l.egrid, 'zones_reservees'::text AS theme, nullif(l.zone_kind,'') AS sous_type,
         true AS is_restrictive, l.zone_name AS libelle, l.overlap_m2,
         l.statut_juridique, l.date_entree_vigueur::date AS date_entree_vigueur
  FROM silver_ch.link_plot_zones_reservees_vd l

  UNION ALL
  -- protection des eaux — S1/S2/S3 AND Périmètre are binding; Au/S/üB/Zu are the blanket
  SELECT l.egrid, 'protection_eaux', l.indice_protection,
         (l.protection_kind='zone' AND l.indice_protection IN ('S1','S2','S3','Périmètre')),
         CASE l.protection_kind
           WHEN 'zone'    THEN 'Zone de protection des eaux '   || l.indice_protection
           WHEN 'secteur' THEN 'Secteur de protection des eaux ' || l.indice_protection
           WHEN 'aire'    THEN 'Aire d''alimentation '           || l.indice_protection
         END,
         l.overlap_m2, NULL::text, l.date_acceptation::date
  FROM silver_ch.link_plot_protection_eaux_vd l

  UNION ALL
  -- sites pollués — KbS legal encumbrance ⇒ binding
  SELECT l.egrid, 'sites_pollues', l.type_site, true,
         coalesce(nullif(l.nom_site,''), nullif(s.activite,''), l.type_site),
         l.overlap_m2, l.nom_phase, NULL::date
  FROM silver_ch.link_plot_sites_pollues_vd l
  JOIN silver_ch.sites_pollues_vd s ON s.id = l.site_pollue_id

  UNION ALL
  -- patrimoine inventaire (ISOS) — INVENTORY (LPN art.5), NOT a building restriction ⇒ false
  SELECT l.egrid, 'patrimoine_inventaire', nullif(l.valeur,''), false,
         l.designation, l.overlap_m2, NULL::text, NULL::date
  FROM silver_ch.link_plot_patrimoine_inventaire_vd l

  UNION ALL
  -- archéologie — SURVEY obligation, not a prohibition ⇒ false
  SELECT l.egrid, 'archeology', nullif(l.record_type,''), false,
         coalesce(nullif(l.designation,''), nullif(l.record_subtype,''), l.record_type),
         l.overlap_m2, NULL::text, NULL::date
  FROM silver_ch.link_plot_archeology_vd l

  UNION ALL
  -- classement — classified/protected object ⇒ binding
  --   NOTE cadastral_patrimoine_classe_vd.id is INTEGER, link's classement_id is TEXT (4,156/4,156 match).
  --   link.type_protection is 100% empty (0/4,156, bug 8f1b13ba) ⇒ sous_type NULL, not faked.
  SELECT l.egrid, 'classement', NULL::text, true,
         l.designation, l.overlap_m2, NULL::text, c.date_classement::date
  FROM silver_ch.link_plot_classement_vd l
  LEFT JOIN silver_ch.cadastral_patrimoine_classe_vd c ON c.id::text = l.classement_id

  UNION ALL
  -- densification — `plan_affectation_etude` is À L'ÉTUDE (not in force) ⇒ false
  SELECT l.egrid, 'densification', nullif(l.densification_type,''), false,
         l.sector_name, l.overlap_m2, NULL::text, NULL::date
  FROM silver_ch.link_plot_densification_vd l

  UNION ALL
  -- forêt — informational LINE (+ reserve polygons). is_restrictive=false, see header.
  SELECT l.egrid, 'foret', l.foret_type, false,
         coalesce(nullif(l.designation,''), l.foret_type), l.overlap_m2, l.statut_juridique, NULL::date
  FROM silver_ch.link_plot_foret_vd l

  UNION ALL
  -- FEDERAL sites pollués sub-registers (VBS/BAZL/BAV) — own theme, base untouched.
  -- Mirrors GE id117/118/119. overlap_m2 NULL for point sites (no area).
  SELECT l.egrid, 'sites_pollues_federal', l.registre, true,
         coalesce(nullif(l.standorttyp,''), nullif(l.katasternummer,''), l.registre),
         l.overlap_m2, l.statut, NULL::date
  FROM silver_ch.link_plot_sites_pollues_federal_vd l
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
  '7 themes. is_restrictive = BINDING BUILDING CONSTRAINT only (corrected 2026-07-15): true for '
  'zones_reservees, sites_pollues, classement, and protection_eaux S1/S2/S3+Périmètre. FALSE for '
  'patrimoine_inventaire (ISOS = inventory, LPN art.5), archeology (survey obligation, not prohibition), '
  'densification (à l''étude), and the protection_eaux secteur/aire blanket (Au/S/üB/Zu). '
  'Noise + servitudes still absent — gated on f50f2e08.';

-- ===========================================================================
-- 2. export_rdppf_national_geom — corrected flags + geom_type 'polygon'|'point'
-- ===========================================================================
DROP MATERIALIZED VIEW IF EXISTS gold_ch.export_rdppf_national_geom RESTRICT;
CREATE MATERIALIZED VIEW gold_ch.export_rdppf_national_geom AS
WITH theme_commune AS (
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
  UNION ALL
  SELECT 'foret', foret_id, commune_bfs FROM (
    SELECT l.foret_id, cp.commune_bfs,
           row_number() OVER (PARTITION BY l.foret_id ORDER BY count(*) DESC, cp.commune_bfs) rn
    FROM silver_ch.link_plot_foret_vd l
    JOIN silver_ch.cadastral_plots cp ON cp.egrid=l.egrid AND cp.canton_code='VD'
    WHERE cp.commune_bfs IS NOT NULL GROUP BY 1,2) t WHERE rn=1
  UNION ALL
  SELECT 'sites_pollues_federal', kbs_id, commune_bfs FROM (
    SELECT l.kbs_id, cp.commune_bfs,
           row_number() OVER (PARTITION BY l.kbs_id ORDER BY count(*) DESC, cp.commune_bfs) rn
    FROM silver_ch.link_plot_sites_pollues_federal_vd l
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
         (e.protection_kind='zone' AND e.indice_protection IN ('S1','S2','S3','Périmètre')),
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
  SELECT 'patrimoine_inventaire', p.id, nullif(p.valeur,''), false, p.designation, NULL::text,
         ST_Transform(p.geometry,4326)
  FROM silver_ch.cadastral_patrimoine_inventaire_vd p WHERE p.canton_code='VD' AND p.geometry IS NOT NULL AND NOT ST_IsEmpty(p.geometry)
  UNION ALL
  SELECT 'archeology', a.id, nullif(a.record_type,''), false,
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
  UNION ALL
  -- forêt — MIXED line+polygon. MakeValid only; CollectionExtract(,3) would EMPTY every line.
  SELECT 'foret', f.id, f.foret_type, false,
         coalesce(nullif(f.designation,''), f.foret_type), f.statut_juridique,
         ST_Transform(f.geometry,4326)
  FROM silver_ch.foret_vd f
  WHERE f.geometry IS NOT NULL AND NOT ST_IsEmpty(f.geometry)
  UNION ALL
  -- FEDERAL KbS — only the objects that actually touch a VD plot reach the export
  -- (the sources are national; the link is the VD filter). Mixed point+polygon.
  SELECT 'sites_pollues_federal', f.id, f.registre, true,
         coalesce(nullif(f.standorttyp,''), nullif(f.katasternummer,''), f.registre), f.statut,
         ST_Transform(f.geometry,4326)
  FROM silver_ch.federal_kbs_sites f
  WHERE f.geometry IS NOT NULL AND NOT ST_IsEmpty(f.geometry)
    AND EXISTS (SELECT 1 FROM silver_ch.link_plot_sites_pollues_federal_vd l WHERE l.kbs_id = f.id)
),
cleaned AS (
  -- points are always valid; MakeValid/CollectionExtract(…,3) would DESTROY them
  -- (the extract keeps polygons only ⇒ EMPTY for every point).
  SELECT p.*,
         CASE
           -- points AND LINES must never meet CollectionExtract(,3) — it keeps polygons only
           WHEN GeometryType(p.g4326) IN ('POINT','MULTIPOINT','LINESTRING','MULTILINESTRING')
                                                                THEN ST_Multi(p.g4326)
           WHEN ST_IsValid(p.g4326)                             THEN ST_Multi(p.g4326)
           ELSE ST_Multi(ST_CollectionExtract(ST_MakeValid(p.g4326), 3))
         END AS geom
  FROM polys p
)
SELECT 'VD'::text AS canton,
       c.theme,
       c.oid                            AS object_id,
       c.sous_type,
       c.is_restrictive,
       c.libelle,
       tc.commune_bfs,
       c.geom::geometry(Geometry, 4326) AS geometry,
       CASE WHEN GeometryType(c.geom) IN ('POINT','MULTIPOINT') THEN 'point'
            WHEN GeometryType(c.geom) IN ('LINESTRING','MULTILINESTRING') THEN 'line'
            ELSE 'polygon' END AS geom_type,
       c.statut_juridique,
       now()                            AS updated_at
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
  'MAP LAYER export → lamap_db ref.rdppf_national_geom. One row per restriction OBJECT. EPSG:4326 + GIST. '
  '⚠️ geometry is MIXED — geometry(Geometry,4326), NOT MultiPolygon: patrimoine_inventaire and archeology '
  'are genuinely polygon+point (819 point objects). Branch on geom_type (''polygon'' → fill layer, '
  '''point'' → marker layer); typing MultiPolygon would silently drop them. '
  'is_restrictive = BINDING BUILDING CONSTRAINT only (corrected 2026-07-15) — see export_rdppf_national.';
COMMENT ON COLUMN gold_ch.export_rdppf_national_geom.geom_type IS
  '''polygon'' | ''point'' | ''line''. Exists because patrimoine_inventaire (141 isos_site) and archeology (678) carry '
  'genuine point geometry alongside polygons. Consumer MUST branch on this to render both.';

GRANT SELECT ON gold_ch.export_rdppf_national, gold_ch.export_rdppf_national_geom
  TO anon, authenticated, service_role;

COMMIT;

-- ============================================================================
-- ROLLBACK — re-run migration 20260715000005 to drop the sites_pollues_federal theme
-- ============================================================================
-- BEGIN;
--   DELETE FROM supabase_migrations.schema_migrations WHERE version = '20260715000010';
-- COMMIT;
-- ============================================================================
