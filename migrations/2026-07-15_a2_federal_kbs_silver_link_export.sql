-- ============================================================================
-- 2026-07-15 — A2: federal KbS — SILVER + LINK + surface into the national exports
-- ============================================================================
-- ⚠️ GEOMETRY CLEANING RULE (three-way, not the usual two-way):
--   POINT/MULTIPOINT      → keep as-is. NEVER ST_CollectionExtract(…,3): returns EMPTY.
--   GEOMETRYCOLLECTION    → ST_CollectionExtract(…,3). Only 3 rows (militaire) — these
--                            are ST_MakeValid's output for self-intersecting polygons
--                            (polygon parts + degenerate linework). Verified they do
--                            contain real polygons.
--   POLYGON/MULTIPOLYGON  → ST_Multi.
--
-- ⚠️ sous_type SEMANTICS CHANGE on theme='sites_pollues' (per brief): sous_type is now
--    the REGISTER (base | militaire | aeroports | transports_publics), not type_site.
--    Consequence: the base rows re-grain — a plot carrying 2 different type_site values
--    used to yield 2 rows, now yields 1 ('base') with SUMmed overlap. type_site survives
--    inside `libelle`. Row count for sites_pollues therefore DROPS; reported, not hidden.
--
-- ⚠️ overlap_m2 is NULL for point-geometry federal sites — a point has no area. Do not
--    read NULL overlap as "no data"; read it with geom_type/registre.
--
-- ROLLBACK at the bottom.
-- ============================================================================

BEGIN;
SET LOCAL statement_timeout = '2700s';
SET LOCAL lock_timeout = '60s';

-- ---------------------------------------------------------------------------
-- SILVER — national (NOT canton-stamped; see bronze header)
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS silver_ch.link_plot_sites_pollues_federal_vd RESTRICT;
DROP MATERIALIZED VIEW IF EXISTS silver_ch.federal_kbs_sites RESTRICT;
CREATE MATERIALIZED VIEW silver_ch.federal_kbs_sites AS
SELECT f.registre || '_' || f.feature_id  AS id,
       f.registre,
       f.katasternummer,
       f.statut,
       f.standorttyp,
       f.untersuchungsmassnahmen,
       f.url_fiche,
       CASE
         WHEN GeometryType(f.geometry) IN ('POINT','MULTIPOINT')  THEN ST_Multi(f.geometry)
         WHEN GeometryType(f.geometry) = 'GEOMETRYCOLLECTION'     THEN ST_Multi(ST_CollectionExtract(f.geometry, 3))
         ELSE ST_Multi(f.geometry)
       END                                 AS geometry,
       CASE WHEN GeometryType(f.geometry) IN ('POINT','MULTIPOINT') THEN 'point' ELSE 'polygon' END AS geom_kind,
       now()                               AS updated_at
FROM bronze_ch.federal_kbs_sites f
WHERE f.deleted_at IS NULL
  AND f.geometry IS NOT NULL
  AND NOT ST_IsEmpty(f.geometry);
CREATE UNIQUE INDEX federal_kbs_sites_id_idx    ON silver_ch.federal_kbs_sites (id);
CREATE INDEX        federal_kbs_sites_geom_gix  ON silver_ch.federal_kbs_sites USING GIST (geometry);
CREATE INDEX        federal_kbs_sites_reg_idx   ON silver_ch.federal_kbs_sites (registre);
COMMENT ON MATERIALIZED VIEW silver_ch.federal_kbs_sites IS
  'Federal KbS sub-registers (VBS militaire / BAZL aéroports / BAV transports publics), geo.admin.ch. '
  'NATIONAL scope — fetched over the VD envelope which overlaps neighbouring cantons; VD scoping happens '
  'in link_plot_sites_pollues_federal_vd. Geometry native 2056 + GIST, MIXED point+polygon (geom_kind).';

-- ---------------------------------------------------------------------------
-- LINK — VD plots × federal KbS. This is also the VD filter (see bronze header).
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW silver_ch.link_plot_sites_pollues_federal_vd AS
WITH vd_plots AS MATERIALIZED (
  SELECT egrid,
         CASE WHEN ST_IsValid(g) THEN g ELSE ST_CollectionExtract(ST_MakeValid(g), 3) END AS geom_2056
  FROM (SELECT egrid, ST_Transform(geometry, 2056) AS g
          FROM silver_ch.cadastral_plots WHERE canton_code = 'VD') t
)
SELECT p.egrid,
       f.id            AS kbs_id,
       f.registre,
       f.katasternummer,
       f.statut,
       f.standorttyp,
       f.url_fiche,
       f.geom_kind,
       -- a point has no area ⇒ overlap_m2 is NULL, not 0
       CASE WHEN f.geom_kind = 'polygon'
            THEN ST_Area(ST_Intersection(p.geom_2056, f.geometry))::numeric
            ELSE NULL::numeric END AS overlap_m2,
       now()           AS updated_at
FROM vd_plots p
JOIN silver_ch.federal_kbs_sites f
  ON ST_Intersects(p.geom_2056, f.geometry);
CREATE UNIQUE INDEX link_plot_sites_pollues_federal_vd_pk
  ON silver_ch.link_plot_sites_pollues_federal_vd (egrid, kbs_id);
CREATE INDEX link_plot_sites_pollues_federal_vd_egrid_idx
  ON silver_ch.link_plot_sites_pollues_federal_vd (egrid);
CREATE INDEX link_plot_sites_pollues_federal_vd_reg_idx
  ON silver_ch.link_plot_sites_pollues_federal_vd (registre);
COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_sites_pollues_federal_vd IS
  'VD plot × federal KbS site (militaire/aéroports/transports publics). Mirrors GE id117/118/119. '
  'This join is ALSO the VD filter — the federal sources are national and the fetch was VD-envelope '
  'scoped, so neighbouring-canton sites simply never match a VD plot. overlap_m2 is NULL for point sites.';

GRANT SELECT ON silver_ch.federal_kbs_sites, silver_ch.link_plot_sites_pollues_federal_vd
  TO anon, authenticated, service_role;

COMMIT;

-- ============================================================================
-- ROLLBACK
-- ============================================================================
-- BEGIN;
--   DROP MATERIALIZED VIEW IF EXISTS silver_ch.link_plot_sites_pollues_federal_vd RESTRICT;
--   DROP MATERIALIZED VIEW IF EXISTS silver_ch.federal_kbs_sites RESTRICT;
--   DELETE FROM supabase_migrations.schema_migrations WHERE version = '20260715000007';
-- COMMIT;
-- ============================================================================
