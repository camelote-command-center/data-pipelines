-- ============================================================================
-- 2026-07-15 — VD RDPPF theme expansion (re-LLM) — SILVER + LINK
-- ============================================================================
-- Builds, per theme: silver_ch.<theme>_vd  ->  silver_ch.link_plot_<theme>_vd
--
-- Mirrors the GE RDPPF lineage shape (bronze_ch.* -> silver_ch.cadastral_* ->
-- link) and reuses the PROVEN 2026-07-14 zoning repair pattern verbatim:
--
--   1. vd_plots CTE is MATERIALIZED — the original broken body recomputed
--      ST_Transform 4x per row.
--   2. The plot side is transformed to 2056; the THEME side is joined natively
--      so its 2056 GIST index stays usable. Putting ST_Transform on the indexed
--      side is bug `aa54b8ab` (it made the index unusable -> timeout on 300 plots).
--   3. ST_MakeValid guard is applied AFTER the transform, not at bronze — bug
--      `4d930c20`: ST_Transform itself CREATES invalid geometry (1 of the 9 VD
--      plots is valid in 4326 and invalid only once reprojected to 2056), so a
--      bronze-level guard cannot survive the downstream reprojection.
--      Valid rows skip MakeValid (cheap path).
--
-- These 3 link matviews have ZERO dependents and are NOT part of the
-- `f50f2e08` chain: they do not feed core_plots_ext_vd / v_plots_full, so no
-- GE object is touched. DROP ... RESTRICT is the runtime guard — it aborts
-- rather than cascading.
--
-- NOT IN SCOPE (left at 0 rows, per brief): link_plot_noise_sensitivity_vd,
-- link_plot_servitudes_vd — genuinely gated on the f50f2e08 chain rebuild.
--
-- ROLLBACK at the bottom of this file.
-- ============================================================================

BEGIN;
SET LOCAL statement_timeout = '1800s';
SET LOCAL lock_timeout = '60s';

-- ===========================================================================
-- SILVER — theme matviews (native 2056 + GIST, mirroring cadastral_zones_vd)
-- ===========================================================================

DROP MATERIALIZED VIEW IF EXISTS silver_ch.link_plot_zones_reservees_vd RESTRICT;
DROP MATERIALIZED VIEW IF EXISTS silver_ch.zones_reservees_vd RESTRICT;
CREATE MATERIALIZED VIEW silver_ch.zones_reservees_vd AS
SELECT 'zr_' || zr.arcgis_objectid::text          AS id,
       zr.code                                     AS zone_code,
       zr.designation                              AS zone_name,
       zr.abreviation_type                         AS zone_kind,
       zr.statut_juridique,
       zr.date_entree_vigueur,
       zr.date_fin,
       zr.commune_bfs,
       ST_Multi(CASE WHEN ST_IsValid(zr.geometry) THEN zr.geometry
                     ELSE ST_CollectionExtract(ST_MakeValid(zr.geometry), 3) END) AS geometry,
       'VD'::text                                  AS canton_code,
       jsonb_build_object(
         'disposition_niveau', zr.disposition_niveau, 'type_doc', zr.type_doc,
         'titre', zr.titre, 'no_officiel', zr.no_officiel,
         'date_enquete', zr.date_enquete, 'date_approbation', zr.date_approbation,
         'perimetre_m', zr.perimetre_m, 'surface_m2', zr.surface_m2)             AS raw_data,
       now()                                       AS updated_at
FROM bronze_ch.vd_zone_reservee zr
WHERE zr.deleted_at IS NULL
  AND zr.geometry IS NOT NULL
  AND NOT ST_IsEmpty(zr.geometry);
CREATE UNIQUE INDEX zones_reservees_vd_id_idx   ON silver_ch.zones_reservees_vd (id);
CREATE INDEX        zones_reservees_vd_geom_idx ON silver_ch.zones_reservees_vd USING GIST (geometry);
CREATE INDEX        zones_reservees_vd_com_idx  ON silver_ch.zones_reservees_vd (commune_bfs);
COMMENT ON MATERIALIZED VIEW silver_ch.zones_reservees_vd IS
  'VD zones réservées (LAT art.27). From bronze_ch.vd_zone_reservee (agsgc /35). Geometry native '
  'EPSG:2056 + GIST — spatial joins must transform the PLOT side, never this one (bug aa54b8ab).';

DROP MATERIALIZED VIEW IF EXISTS silver_ch.link_plot_protection_eaux_vd RESTRICT;
DROP MATERIALIZED VIEW IF EXISTS silver_ch._protection_eaux_vd_sub RESTRICT;
DROP MATERIALIZED VIEW IF EXISTS silver_ch.protection_eaux_vd RESTRICT;
CREATE MATERIALIZED VIEW silver_ch.protection_eaux_vd AS
SELECT 'pe_' || pe.source_layer || '_' || pe.arcgis_objectid::text AS id,
       pe.indice_protection,
       pe.protection_kind,
       pe.date_acceptation,
       ST_Multi(CASE WHEN ST_IsValid(pe.geometry) THEN pe.geometry
                     ELSE ST_CollectionExtract(ST_MakeValid(pe.geometry), 3) END) AS geometry,
       'VD'::text                                  AS canton_code,
       jsonb_build_object('source_layer', pe.source_layer)                        AS raw_data,
       now()                                       AS updated_at
FROM bronze_ch.vd_protection_eaux pe
WHERE pe.deleted_at IS NULL
  AND pe.geometry IS NOT NULL
  AND NOT ST_IsEmpty(pe.geometry);
CREATE UNIQUE INDEX protection_eaux_vd_id_idx     ON silver_ch.protection_eaux_vd (id);
CREATE INDEX        protection_eaux_vd_geom_idx   ON silver_ch.protection_eaux_vd USING GIST (geometry);
CREATE INDEX        protection_eaux_vd_indice_idx ON silver_ch.protection_eaux_vd (indice_protection);
CREATE INDEX        protection_eaux_vd_kind_idx   ON silver_ch.protection_eaux_vd (protection_kind);
COMMENT ON MATERIALIZED VIEW silver_ch.protection_eaux_vd IS
  'VD protection des eaux souterraines — MGDM Planerischer Gewässerschutz sub-themes: '
  'protection_kind=zone (S1/S2/S3, /118), secteur (Au/Ao/üB, /119), aire (Zu, /120). '
  'Geometry native EPSG:2056 + GIST.';

DROP MATERIALIZED VIEW IF EXISTS silver_ch.link_plot_sites_pollues_vd RESTRICT;
DROP MATERIALIZED VIEW IF EXISTS silver_ch.sites_pollues_vd RESTRICT;
CREATE MATERIALIZED VIEW silver_ch.sites_pollues_vd AS
SELECT 'sp_' || sp.arcgis_objectid::text          AS id,
       sp.type_site,
       sp.nom_site,
       sp.nom_phase,
       sp.activite,
       sp.no_dossier,
       sp.parcelles_polluees,
       sp.urgence_investig,
       ST_Multi(CASE WHEN ST_IsValid(sp.geometry) THEN sp.geometry
                     ELSE ST_CollectionExtract(ST_MakeValid(sp.geometry), 3) END) AS geometry,
       'VD'::text                                  AS canton_code,
       jsonb_build_object(
         'no_eva', sp.no_eva, 'no_commune_vd', sp.no_commune_vd,
         'debut_activite', sp.debut_activite, 'fin_activite', sp.fin_activite,
         'investigations_realisees', sp.investigations_realisees,
         'volume_decharge', sp.volume_decharge)                                   AS raw_data,
       now()                                       AS updated_at
FROM bronze_ch.vd_site_pollue sp
WHERE sp.deleted_at IS NULL
  AND sp.geometry IS NOT NULL
  AND NOT ST_IsEmpty(sp.geometry);
CREATE UNIQUE INDEX sites_pollues_vd_id_idx   ON silver_ch.sites_pollues_vd (id);
CREATE INDEX        sites_pollues_vd_geom_idx ON silver_ch.sites_pollues_vd USING GIST (geometry);
CREATE INDEX        sites_pollues_vd_type_idx ON silver_ch.sites_pollues_vd (type_site);
COMMENT ON MATERIALIZED VIEW silver_ch.sites_pollues_vd IS
  'VD cadastre des sites pollués (KbS). From bronze_ch.vd_site_pollue (agsgc /116). Geometry native '
  'EPSG:2056 + GIST. NOTE raw_data.no_commune_vd is canton numbering, NOT federal BFS.';

-- ===========================================================================
-- LINK — plot x theme (proven 2026-07-14 zoning pattern)
-- ===========================================================================

CREATE MATERIALIZED VIEW silver_ch.link_plot_zones_reservees_vd AS
WITH vd_plots AS MATERIALIZED (
  SELECT egrid,
         CASE WHEN ST_IsValid(g) THEN g
              ELSE ST_CollectionExtract(ST_MakeValid(g), 3) END AS geom_2056
  FROM (SELECT egrid, ST_Transform(geometry, 2056) AS g
          FROM silver_ch.cadastral_plots WHERE canton_code = 'VD') t
)
SELECT p.egrid,
       z.id                                       AS zone_reservee_id,
       z.zone_code,
       z.zone_name,
       z.zone_kind,
       z.statut_juridique,
       z.date_entree_vigueur,
       ST_Area(ST_Intersection(p.geom_2056, z.geometry))::numeric AS overlap_m2,
       ST_Area(ST_Intersection(p.geom_2056, z.geometry))
         >= (0.5 * ST_Area(p.geom_2056))          AS is_dominant,
       now()                                      AS updated_at
FROM vd_plots p
JOIN silver_ch.zones_reservees_vd z
  ON z.canton_code = 'VD' AND ST_Intersects(p.geom_2056, z.geometry);
CREATE UNIQUE INDEX link_plot_zones_reservees_vd_pk_idx
  ON silver_ch.link_plot_zones_reservees_vd (egrid, zone_reservee_id);
CREATE INDEX link_plot_zones_reservees_vd_egrid_idx
  ON silver_ch.link_plot_zones_reservees_vd (egrid);
CREATE INDEX link_plot_zones_reservees_vd_dom_idx
  ON silver_ch.link_plot_zones_reservees_vd (is_dominant) WHERE is_dominant;
COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_zones_reservees_vd IS
  'VD plot x zone réservée. is_dominant = the zone covers >=50% of the plot. Zero dependents; '
  'NOT part of the f50f2e08 chain.';

-- ---------------------------------------------------------------------------
-- link_plot_protection_eaux_vd — built via ST_Subdivide TILING.
--
-- The naive body (join plots straight to protection_eaux_vd) was measured and
-- REJECTED: it ran the full 1800s statement_timeout and aborted, while the
-- other two links + all 3 silver matviews together took ~25s. Cause: the
-- groundwater `secteur` (Au/Ao/üB) polygons are canton-scale. A GIST entry for
-- a canton-sized polygon has a canton-sized bbox, so the index prefilter prunes
-- almost nothing and nearly every VD plot pays a full ST_Intersection against a
-- huge many-vertex geometry.
--
-- Fix: split each theme polygon into <=256-vertex tiles, GIST-index the tiles,
-- join against those, then re-aggregate back to the source polygon id.
--
-- CORRECTNESS: one plot can hit SEVERAL tiles of the SAME polygon, so we SUM
-- per-tile intersection areas and GROUP BY (egrid, id) — otherwise UNIQUE
-- (egrid, protection_id) would fail and overlap_m2 would be a fragment.
-- ST_Subdivide partitions the polygon (tiles are disjoint, union == original),
-- so SUM(tile intersections) == intersection with the whole polygon. is_dominant
-- is therefore computed from the SUMmed area, not per tile.
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS silver_ch._protection_eaux_vd_sub RESTRICT;
CREATE MATERIALIZED VIEW silver_ch._protection_eaux_vd_sub AS
SELECT id, ST_Subdivide(geometry, 256) AS geom
FROM silver_ch.protection_eaux_vd;
CREATE INDEX _protection_eaux_vd_sub_geom_idx
  ON silver_ch._protection_eaux_vd_sub USING GIST (geom);
CREATE INDEX _protection_eaux_vd_sub_id_idx
  ON silver_ch._protection_eaux_vd_sub (id);
COMMENT ON MATERIALIZED VIEW silver_ch._protection_eaux_vd_sub IS
  'INTERNAL tiling of protection_eaux_vd (ST_Subdivide 256) — purely to make the plot join tractable; '
  'the secteur polygons are canton-scale and defeat the GIST prefilter. NOT a consumer surface. '
  'Must be rebuilt whenever protection_eaux_vd changes, before link_plot_protection_eaux_vd.';

CREATE MATERIALIZED VIEW silver_ch.link_plot_protection_eaux_vd AS
WITH vd_plots AS MATERIALIZED (
  SELECT egrid,
         CASE WHEN ST_IsValid(g) THEN g
              ELSE ST_CollectionExtract(ST_MakeValid(g), 3) END AS geom_2056
  FROM (SELECT egrid, ST_Transform(geometry, 2056) AS g
          FROM silver_ch.cadastral_plots WHERE canton_code = 'VD') t
),
hits AS (
  SELECT p.egrid,
         s.id                                               AS protection_id,
         SUM(ST_Area(ST_Intersection(p.geom_2056, s.geom))) AS overlap_m2,
         MAX(ST_Area(p.geom_2056))                          AS plot_m2
  FROM vd_plots p
  JOIN silver_ch._protection_eaux_vd_sub s
    ON ST_Intersects(p.geom_2056, s.geom)
  GROUP BY p.egrid, s.id
)
SELECT h.egrid,
       h.protection_id,
       e.indice_protection,
       e.protection_kind,
       e.date_acceptation,
       h.overlap_m2::numeric                     AS overlap_m2,
       (h.overlap_m2 >= 0.5 * h.plot_m2)         AS is_dominant,
       now()                                     AS updated_at
FROM hits h
JOIN silver_ch.protection_eaux_vd e ON e.id = h.protection_id
WHERE e.canton_code = 'VD';
CREATE UNIQUE INDEX link_plot_protection_eaux_vd_pk_idx
  ON silver_ch.link_plot_protection_eaux_vd (egrid, protection_id);
CREATE INDEX link_plot_protection_eaux_vd_egrid_idx
  ON silver_ch.link_plot_protection_eaux_vd (egrid);
CREATE INDEX link_plot_protection_eaux_vd_indice_idx
  ON silver_ch.link_plot_protection_eaux_vd (indice_protection);
COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_protection_eaux_vd IS
  'VD plot x groundwater protection (zone S1/S2/S3, secteur, aire). A plot legitimately carries '
  'MULTIPLE rows across the 3 protection_kind sub-themes — filter by protection_kind. '
  'is_dominant = that polygon covers >=50% of the plot. Zero dependents.';

CREATE MATERIALIZED VIEW silver_ch.link_plot_sites_pollues_vd AS
WITH vd_plots AS MATERIALIZED (
  SELECT egrid,
         CASE WHEN ST_IsValid(g) THEN g
              ELSE ST_CollectionExtract(ST_MakeValid(g), 3) END AS geom_2056
  FROM (SELECT egrid, ST_Transform(geometry, 2056) AS g
          FROM silver_ch.cadastral_plots WHERE canton_code = 'VD') t
)
SELECT p.egrid,
       s.id                                       AS site_pollue_id,
       s.type_site,
       s.nom_site,
       s.nom_phase,
       ST_Area(ST_Intersection(p.geom_2056, s.geometry))::numeric AS overlap_m2,
       ST_Area(ST_Intersection(p.geom_2056, s.geometry))
         >= (0.5 * ST_Area(p.geom_2056))          AS is_dominant,
       now()                                      AS updated_at
FROM vd_plots p
JOIN silver_ch.sites_pollues_vd s
  ON s.canton_code = 'VD' AND ST_Intersects(p.geom_2056, s.geometry);
CREATE UNIQUE INDEX link_plot_sites_pollues_vd_pk_idx
  ON silver_ch.link_plot_sites_pollues_vd (egrid, site_pollue_id);
CREATE INDEX link_plot_sites_pollues_vd_egrid_idx
  ON silver_ch.link_plot_sites_pollues_vd (egrid);
CREATE INDEX link_plot_sites_pollues_vd_type_idx
  ON silver_ch.link_plot_sites_pollues_vd (type_site);
COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_sites_pollues_vd IS
  'VD plot x site pollué (KbS). is_dominant is rarely true by nature — KbS sites are usually small '
  'relative to the plot; use overlap_m2. Zero dependents.';

-- Grants mirror the existing silver_ch link matviews.
GRANT SELECT ON silver_ch.zones_reservees_vd, silver_ch.protection_eaux_vd,
                silver_ch.sites_pollues_vd,
                silver_ch._protection_eaux_vd_sub,
                silver_ch.link_plot_zones_reservees_vd,
                silver_ch.link_plot_protection_eaux_vd,
                silver_ch.link_plot_sites_pollues_vd
  TO anon, authenticated, service_role;

COMMIT;

-- ============================================================================
-- ROLLBACK
-- ============================================================================
-- BEGIN;
--   DROP MATERIALIZED VIEW IF EXISTS silver_ch.link_plot_zones_reservees_vd RESTRICT;
--   DROP MATERIALIZED VIEW IF EXISTS silver_ch.link_plot_protection_eaux_vd RESTRICT;
--   DROP MATERIALIZED VIEW IF EXISTS silver_ch.link_plot_sites_pollues_vd   RESTRICT;
--   DROP MATERIALIZED VIEW IF EXISTS silver_ch._protection_eaux_vd_sub RESTRICT;
--   DROP MATERIALIZED VIEW IF EXISTS silver_ch.zones_reservees_vd RESTRICT;
--   DROP MATERIALIZED VIEW IF EXISTS silver_ch.protection_eaux_vd RESTRICT;
--   DROP MATERIALIZED VIEW IF EXISTS silver_ch.sites_pollues_vd   RESTRICT;
--   DELETE FROM supabase_migrations.schema_migrations WHERE version = '20260715000002';
-- COMMIT;
-- ============================================================================
