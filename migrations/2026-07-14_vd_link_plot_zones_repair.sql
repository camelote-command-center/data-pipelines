-- ============================================================================
-- VD link_plot_zones_vd repair — 2056-native join + post-transform MakeValid guard
-- ============================================================================
-- Target DB : re-LLM (znrvddgmczdqoucmykij)
-- Version   : 20260714000001
-- Applied   : 2026-07-14 (cc) — APPLIED IN PRODUCTION, tracking row registered.
-- Result    : silver_ch.link_plot_zones_vd  0 -> 879,932 rows
--             284,015 / 284,015 VD plots = 100.00% coverage. Refresh 11m23s.
--
-- CONTEXT ---------------------------------------------------------------------
-- The VD zoning SOURCES were never missing: bronze_ch.vd_zone_affectation (83,671
-- rows, federally harmonized MGDM 73.1 — code_ch/designation_ch supertypes) has been
-- loaded since PR #15/#16 (2026-05-22). silver_ch.link_plot_zones_vd — the de-facto
-- `vd_plot_zoning` surface (one row per plot x zone, is_dominant, overlap_m2,
-- ius/cos/spb/cm/igt/h_max) — already existed. It was EMPTY, not absent.
--
-- TWO INDEPENDENT ROOT CAUSES — both had to be fixed --------------------------
--
-- 1. Index-defeating join direction (bug aa54b8ab — previously undiagnosed).
--    The body joined:
--        ST_Intersects(p.geometry /*4326*/, ST_Transform(z.geometry, 4326))
--    The GIST index (cadastral_zones_vd_geom_idx) is on the NATIVE 2056 column.
--    Putting ST_Transform on the INDEXED side makes it unusable, so each of the
--    284,015 VD plots seq-scanned and re-transformed all 86k zones.
--    Measured: TIMES OUT on a 300-plot sample (>170s). Joining natively in 2056:
--    300 plots instant, 20,000 plots ~39s, full 284k in 11m23s.
--    Matches the standing doctrine: geometry stored 4326, spatial joins in 2056.
--
-- 2. Invalid plot geometry (bug 4d930c20 — diagnosis corrected here).
--    ST_Intersection throws GEOS "TopologyException: side location conflict" on
--    invalid input. (ST_Intersects, the join predicate, tolerates it — only the
--    area math throws.)
--      * The count is 9, NOT 8: cadastral_plots (VD) has 8 invalid AS STORED
--        (4326), but 9 invalid AFTER ST_Transform -> 2056. Three are valid in 4326
--        and invalid ONLY in 2056 (CH309445178322, CH344583760201, CH638381450532)
--        — the transform itself creates them.
--      * THIS IS WHY the bronze-side fix (20260522000003 federal_plots_makevalid)
--        never reached it: a guard applied at bronze CANNOT survive a downstream
--        reprojection. The guard must sit AFTER the transform, in the consumer.
--      * 4d930c20's hypothesis (MakeValid -> GeometryCollection -> "resolved to
--        empty/NULL") is FALSE. ST_CollectionExtract(ST_MakeValid(g),3) returns a
--        VALID geometry for ALL 9 (ST_IsValid = true, 9/9). Zero rows lost — so its
--        recommendation (a) "NULL their geometry, lose 8 rows" is unnecessary.
--      * Confirmed culprit: CH474586758307, ST_IsValidReason "Too few points in
--        geometry component[2555745.10 1144973.00]" — matches the reported failure
--        coordinate (2555744.9005358634, 1144973.3011324515) exactly.
--
-- NOTE ON THE UNAPPLIED SIBLING MIGRATION -------------------------------------
-- migrations/2026-05-22_vd_enrichment_link_matviews_makevalid.sql was NEVER APPLIED
-- (it claims version 20260522000002, which is taken by vd_enrichment_bronze_makevalid;
-- the live matview bodies contain no ST_MakeValid). It also wrapped the WRONG side:
-- its header states "cadastral_plots.geometry (federal cadastre, clean) is NOT wrapped"
-- — but the plot side is exactly where the invalid geometry is. Do not revive it as-is.
--
-- SAFETY ----------------------------------------------------------------------
-- silver_ch.link_plot_zones_vd has ZERO dependents (verified via pg_depend at plan
-- time). DROP ... RESTRICT is used DELIBERATELY as a runtime guard: if a dependent
-- appeared since planning, RESTRICT aborts the transaction instead of silently
-- CASCADE-dropping it (the 2026-04-29 core_plots cascade lesson, enforced
-- mechanically rather than by discipline). f50f2e08 is NOT engaged by this migration
-- and no GE object is touched.
--
-- STILL BLOCKED (NOT in this migration) ---------------------------------------
-- link_plot_noise_sensitivity_vd + link_plot_servitudes_vd need the same guard, but
-- both feed gold_ch.core_plots_ext_vd -> v_plots_full -> 13 GE objects, so their body
-- change IS gated on f50f2e08. The additive functional index added alongside this
-- migration (silver_ch.cadastral_noise_sensitivity_geom4326_idx) fixes only their
-- PERFORMANCE half — verified insufficient on its own: the noise body still throws the
-- identical TopologyException on the same 9 plots. Servitudes is additionally blocked
-- by 667e676b (109 servitude ids match multiple bronze geoms -> duplicate
-- (egrid, servitude_id) -> UNIQUE violation) and is Lausanne-only (438 rows).
--
-- ROLLBACK --------------------------------------------------------------------
-- Loss-free: the matview had 0 rows and 0 dependents before this change.
--   DROP MATERIALIZED VIEW silver_ch.link_plot_zones_vd RESTRICT;
--   -- recreate the original body: same SELECT but WITHOUT the MATERIALIZED CTE and
--   -- the MakeValid guard, joining as:
--   --   FROM silver_ch.cadastral_plots p
--   --   JOIN silver_ch.cadastral_zones_vd z
--   --     ON z.canton_code='VD'
--   --    AND ST_Intersects(p.geometry, ST_Transform(z.geometry, 4326))
--   --   WHERE p.canton_code='VD';
--   -- then recreate the 3 indexes + 4 grants below.
--   DROP INDEX IF EXISTS silver_ch.cadastral_noise_sensitivity_geom4326_idx;
-- ============================================================================

BEGIN;

SET LOCAL statement_timeout = '600s';
SET LOCAL lock_timeout = '60s';

DROP MATERIALIZED VIEW silver_ch.link_plot_zones_vd RESTRICT;

CREATE MATERIALIZED VIEW silver_ch.link_plot_zones_vd AS
WITH vd_plots AS MATERIALIZED (
    -- MATERIALIZED: compute ST_Transform ONCE per plot. The original body
    -- recomputed st_transform(p.geometry, 2056) 4x per row.
    SELECT egrid,
           CASE
               WHEN ST_IsValid(g) THEN g
               ELSE ST_CollectionExtract(ST_MakeValid(g), 3)
           END AS geom_2056
    FROM (
        SELECT egrid, ST_Transform(geometry, 2056) AS g
        FROM silver_ch.cadastral_plots
        WHERE canton_code = 'VD'
    ) t
)
SELECT p.egrid,
       z.id AS zone_id,
       z.zone_type,
       z.zone_code,
       z.zone_name,
       ST_Area(ST_Intersection(p.geom_2056, z.geometry))::numeric AS overlap_m2,
       ST_Area(ST_Intersection(p.geom_2056, z.geometry))
           >= (0.5::double precision * ST_Area(p.geom_2056)) AS is_dominant,
       NULLIF(z.raw_data ->> 'ius'::text, ''::text)::numeric AS ius,
       NULLIF(z.raw_data ->> 'cos'::text, ''::text)::numeric AS cos,
       NULLIF(z.raw_data ->> 'spb'::text, ''::text)::numeric AS spb,
       NULLIF(z.raw_data ->> 'cm'::text, ''::text)::numeric AS cm,
       NULLIF(z.raw_data ->> 'igt'::text, ''::text)::numeric AS igt,
       z.raw_data ->> 'h_max'::text AS h_max,
       now() AS updated_at
FROM vd_plots p
JOIN silver_ch.cadastral_zones_vd z
       ON z.canton_code = 'VD'
      AND ST_Intersects(p.geom_2056, z.geometry)   -- 2056-native: GIST index usable
WITH NO DATA;

-- indexes restored verbatim from the pre-DROP state
CREATE UNIQUE INDEX link_plot_zones_vd_pk_idx
    ON silver_ch.link_plot_zones_vd USING btree (egrid, zone_id);
CREATE INDEX link_plot_zones_vd_egrid_idx
    ON silver_ch.link_plot_zones_vd USING btree (egrid);
CREATE INDEX link_plot_zones_vd_zonetype_idx
    ON silver_ch.link_plot_zones_vd USING btree (zone_type);

-- grants restored verbatim from the pre-DROP relacl
GRANT ALL ON silver_ch.link_plot_zones_vd TO postgres;
GRANT ALL ON silver_ch.link_plot_zones_vd TO anon;
GRANT ALL ON silver_ch.link_plot_zones_vd TO authenticated;
GRANT ALL ON silver_ch.link_plot_zones_vd TO service_role;

COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_zones_vd IS
    'VD plot<->zone overlap link (one row per plot x zone). Repaired 2026-07-14: (a) join moved to '
    'native 2056 so the GIST index on cadastral_zones_vd.geometry is usable (matview had been 0 '
    'rows since 2026-05-22); (b) ST_MakeValid guard on the 9 invalid VD plot geometries (bug '
    '4d930c20) that made ST_Intersection throw. NOTE: is_dominant is per (plot,zone) and '
    'cadastral_zones_vd unions 3 themes (affectation/foret/stationnement_sector), so a plot may '
    'have >1 dominant row across themes.';

-- Additive, reversible. Makes the EXISTING (unchanged) link_plot_noise_sensitivity_vd
-- predicate index-usable. ST_Transform(geometry,integer) is IMMUTABLE, so a functional
-- index is legal. EXPLAIN-verified: Index Scan + Index Cond
--   (st_transform(geometry, 4326) && cadastral_plots.geometry)
-- Performance half ONLY — the noise refresh still needs the MakeValid guard (f50f2e08).
CREATE INDEX IF NOT EXISTS cadastral_noise_sensitivity_geom4326_idx
    ON silver_ch.cadastral_noise_sensitivity USING gist (ST_Transform(geometry, 4326));

INSERT INTO supabase_migrations.schema_migrations (version, name)
VALUES ('20260714000001', '2026-07-14_vd_link_plot_zones_repair')
ON CONFLICT (version) DO NOTHING;

COMMIT;

-- Run separately, outside the transaction (11m23s; first refresh must be
-- non-concurrent because the matview is created WITH NO DATA):
--   SET statement_timeout='3600s';
--   REFRESH MATERIALIZED VIEW silver_ch.link_plot_zones_vd;
--
-- Expect 9 NOTICEs (one per repaired plot) — these are ST_MakeValid reporting, not errors.
--
-- VERIFICATION (actuals, 2026-07-14):
--   879,932 rows / 284,015 distinct plots = 100.00% coverage, 0 plots unzoned
--   82,908 of 83,685 geometry-bearing zones matched
--   0 rows with zero-area overlap
--   dominance: 270,928 plots x1 | 10,303 x2 (cross-theme) | 2,784 x0 (fragmented)
--   Lausanne spot-check CH750383754505 (parcel 10, 903m2)
--     -> "Zone d'habitation de moyenne densite 15 LAT" [1103] dominant
