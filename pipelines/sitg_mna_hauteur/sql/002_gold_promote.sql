-- APPLY ON re-LLM. Promotes the SITG 20 cm height model (bronze) to the gold tables the app reads.
--
-- 1. gold_ch.plot_canopy_stats  <- bronze_ch.ge_mna_hauteur_parcel_stats
--    The drawer's canopy numbers switch from the 0.5 m swisstopo CHM (DSM 2019-2023 minus DTM 2019-2021,
--    mixed vintages) to the SITG 2025 height model: one flight, 20 cm, leaf-off.
--    Measured before the switch on 61,203 parcels: canopy cover |diff| 1.22 points, max-height corr 0.985,
--    median +0.42 m. Same 3 m vegetation threshold, so get_plot_canopy_stats() keeps its contract.
--    ge_canopy_height_model/load_stats.py no longer overwrites a parcel this height model covers.
--    Parcels whose height-model max exceeds 50 m (defect, 18 of 72,949) are NOT promoted and keep the CHM row.
--
-- 2. gold_ch.building_roof_heights  <- bronze_ch.ge_mna_hauteur_building_heights (+ bati3d for the fallback)
--    One row per EGID. height_m = height-model p95 of the roof (2025, newest). Where it disagrees with the
--    bati3d LOD2 model by more than 10 m (4.6 % of 80,708 buildings, nearly all MNA HIGHER: small sheds and
--    garages under tree crowns, where the height model sees the canopy, not the roof), bati3d is used and
--    height_source says so. p95, not max: max is off by >10 m on 6.9 % (antennas, cranes, overhanging trees).
--    Height model < 2 m (1,994 EGIDs: underground car-park roofs, demolished or roofless structures): bati3d if it
--    has the building (1,803), otherwise height_m is NULL and height_source = 'none' (191) — never a 0.1 m building.
--    computed_at moves only when a value changes, so the daily FDW push (gated on computed_at) carries exactly that.

CREATE OR REPLACE FUNCTION gold_ch.promote_mna_canopy_stats() RETURNS integer
LANGUAGE plpgsql SECURITY DEFINER SET search_path = gold_ch, bronze_ch, public SET statement_timeout = '900s' AS $$
DECLARE n integer;
BEGIN
  INSERT INTO gold_ch.plot_canopy_stats AS t (egrid, no_commune, no_parcelle, canopy_cover_pct, height_p95_m, height_max_m,
         height_mean_m, vegetated_area_m2, parcel_area_m2, polygon_area_m2, dsm_year, dtm_year, vintage_mixed, computed_at)
  SELECT m.egrid, m.no_commune, m.no_parcelle, m.canopy_cover_pct, m.veg_height_p95_m, m.veg_height_max_m, m.veg_height_mean_m,
         m.vegetated_area_m2, m.parcel_area_m2, m.parcel_area_m2, left(m.source_vintage, 4)::smallint, left(m.source_vintage, 4)::smallint,
         false, m.computed_at
  FROM bronze_ch.ge_mna_hauteur_parcel_stats m
  -- > 50 m is not a Geneva tree (gold caps height_max_m at 50): the source's altitude-leak defect also shows
  -- BELOW the 200 m cap on a few parcels (e.g. 88 m 'trees' over 100 % of a parcel). Those keep their CHM row.
  WHERE m.veg_height_max_m IS NULL OR m.veg_height_max_m <= 50
  ON CONFLICT (egrid) DO UPDATE SET
    no_commune = EXCLUDED.no_commune, no_parcelle = EXCLUDED.no_parcelle, canopy_cover_pct = EXCLUDED.canopy_cover_pct,
    height_p95_m = EXCLUDED.height_p95_m, height_max_m = EXCLUDED.height_max_m, height_mean_m = EXCLUDED.height_mean_m,
    vegetated_area_m2 = EXCLUDED.vegetated_area_m2, dsm_year = EXCLUDED.dsm_year, dtm_year = EXCLUDED.dtm_year,
    vintage_mixed = false, computed_at = EXCLUDED.computed_at
  WHERE t.computed_at IS DISTINCT FROM EXCLUDED.computed_at;
  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN n;
END $$;

CREATE TABLE IF NOT EXISTS gold_ch.building_roof_heights (
  egid             bigint PRIMARY KEY,
  no_commune       integer,
  height_m         numeric(6,2),          -- what the app shows: roof height above ground
  height_source    text NOT NULL CHECK (height_source IN ('mna_2025', 'bati3d', 'none')),
  mna_p95_m        numeric(6,2),
  mna_max_m        numeric(6,2),
  bati3d_height_m  numeric(6,2),          -- bati3d LOD2: highest roof point minus lowest base point
  roof_area_m2     numeric(12,2),
  footprints       integer NOT NULL,
  source_vintage   text NOT NULL,
  computed_at      timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION gold_ch.promote_mna_building_heights() RETURNS integer
LANGUAGE plpgsql SECURITY DEFINER SET search_path = gold_ch, bronze_ch, public SET statement_timeout = '900s' AS $$
DECLARE n integer;
BEGIN
  CREATE TEMP TABLE _bh ON COMMIT DROP AS
  WITH m AS (
    SELECT egid::bigint egid, min(no_commune) no_commune, count(*) footprints, sum(roof_area_m2) roof_area_m2,
           max(height_max_m) mna_max_m,
           (array_agg(height_p95_m ORDER BY roof_area_m2 DESC))[1] mna_p95_m,     -- p95 of the largest footprint
           max(source_vintage) source_vintage, max(computed_at) computed_at
    FROM bronze_ch.ge_mna_hauteur_building_heights WHERE egid IS NOT NULL GROUP BY 1),
  t AS (SELECT egid::bigint egid, max(ST_ZMax(geom)) zt FROM bronze_ch.ge_cad_bati3d_toit WHERE egid IS NOT NULL GROUP BY 1),
  b AS (SELECT egid::bigint egid, min(ST_ZMin(geom)) zb FROM bronze_ch.ge_cad_bati3d_base WHERE egid IS NOT NULL GROUP BY 1)
  SELECT m.*, round((t.zt - b.zb)::numeric, 2) bati3d_height_m FROM m LEFT JOIN t USING (egid) LEFT JOIN b USING (egid);

  INSERT INTO gold_ch.building_roof_heights AS g (egid, no_commune, height_m, height_source, mna_p95_m, mna_max_m, bati3d_height_m,
         roof_area_m2, footprints, source_vintage, computed_at)
  SELECT egid, no_commune,
         CASE WHEN fallback THEN bati3d_height_m WHEN mna_p95_m >= 2 THEN mna_p95_m END,
         CASE WHEN fallback THEN 'bati3d' WHEN mna_p95_m >= 2 THEN 'mna_2025' ELSE 'none' END,
         mna_p95_m, mna_max_m, bati3d_height_m, roof_area_m2, footprints, source_vintage, now()
  FROM (SELECT *, bati3d_height_m IS NOT NULL AND (mna_p95_m IS NULL OR mna_p95_m < 2 OR abs(mna_p95_m - bati3d_height_m) > 10) fallback
        FROM _bh) x
  ON CONFLICT (egid) DO UPDATE SET
    no_commune = EXCLUDED.no_commune, height_m = EXCLUDED.height_m, height_source = EXCLUDED.height_source,
    mna_p95_m = EXCLUDED.mna_p95_m, mna_max_m = EXCLUDED.mna_max_m, bati3d_height_m = EXCLUDED.bati3d_height_m,
    roof_area_m2 = EXCLUDED.roof_area_m2, footprints = EXCLUDED.footprints, source_vintage = EXCLUDED.source_vintage,
    computed_at = EXCLUDED.computed_at
  WHERE (g.no_commune, g.height_m, g.height_source, g.mna_p95_m, g.mna_max_m, g.bati3d_height_m, g.roof_area_m2, g.footprints, g.source_vintage)
        IS DISTINCT FROM (EXCLUDED.no_commune, EXCLUDED.height_m, EXCLUDED.height_source, EXCLUDED.mna_p95_m, EXCLUDED.mna_max_m,
                          EXCLUDED.bati3d_height_m, EXCLUDED.roof_area_m2, EXCLUDED.footprints, EXCLUDED.source_vintage);
  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN n;
END $$;

-- FDW push to lamap_db (ref.building_roof_heights must exist there first: 003_ref_lamap_db.sql)
CREATE FOREIGN TABLE IF NOT EXISTS lamap_db_foreign.building_roof_heights (
  egid bigint, no_commune integer, height_m numeric(6,2), height_source text, mna_p95_m numeric(6,2), mna_max_m numeric(6,2),
  bati3d_height_m numeric(6,2), roof_area_m2 numeric(12,2), footprints integer, source_vintage text, computed_at timestamptz
) SERVER lamap_db_server OPTIONS (schema_name 'ref', table_name 'building_roof_heights');

-- Both FDW pushes take an optional commune. The daily crons call them without one (small increments). A new
-- flight changes every row, and one FDW statement cannot push 73-83k rows inside 900 s (measured on the CHM
-- first load), so merge_load.py pushes commune by commune, then once without a commune for the remainder.
DROP PROCEDURE IF EXISTS gold_ch.sync_plot_canopy_stats();
CREATE OR REPLACE PROCEDURE gold_ch.sync_plot_canopy_stats(p_commune integer DEFAULT NULL)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'gold_ch', 'public'
 SET statement_timeout TO '900s'
AS $procedure$
BEGIN
  UPDATE lamap_db_foreign.plot_canopy_stats t SET
    no_commune=s.no_commune, no_parcelle=s.no_parcelle,
    canopy_cover_pct=s.canopy_cover_pct, height_p95_m=s.height_p95_m,
    height_max_m=s.height_max_m, height_mean_m=s.height_mean_m,
    vegetated_area_m2=s.vegetated_area_m2, parcel_area_m2=s.parcel_area_m2,
    polygon_area_m2=s.polygon_area_m2, dsm_year=s.dsm_year, dtm_year=s.dtm_year,
    vintage_mixed=s.vintage_mixed, computed_at=s.computed_at
  FROM gold_ch.plot_canopy_stats s
  WHERE t.egrid = s.egrid AND t.computed_at IS DISTINCT FROM s.computed_at
     AND (p_commune IS NULL OR s.no_commune = p_commune);

  INSERT INTO lamap_db_foreign.plot_canopy_stats
    (egrid,no_commune,no_parcelle,canopy_cover_pct,height_p95_m,height_max_m,height_mean_m,
     vegetated_area_m2,parcel_area_m2,polygon_area_m2,dsm_year,dtm_year,vintage_mixed,computed_at)
  SELECT s.egrid,s.no_commune,s.no_parcelle,s.canopy_cover_pct,s.height_p95_m,s.height_max_m,
     s.height_mean_m,s.vegetated_area_m2,s.parcel_area_m2,s.polygon_area_m2,s.dsm_year,
     s.dtm_year,s.vintage_mixed,s.computed_at
  FROM gold_ch.plot_canopy_stats s
  WHERE (p_commune IS NULL OR s.no_commune = p_commune)
     AND NOT EXISTS (SELECT 1 FROM lamap_db_foreign.plot_canopy_stats t WHERE t.egrid=s.egrid);
END;$procedure$;

DROP PROCEDURE IF EXISTS gold_ch.sync_building_roof_heights();
CREATE OR REPLACE PROCEDURE gold_ch.sync_building_roof_heights(p_commune integer DEFAULT NULL)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = gold_ch, public SET statement_timeout = '900s' AS $$
BEGIN
  UPDATE lamap_db_foreign.building_roof_heights t SET
    no_commune = s.no_commune, height_m = s.height_m, height_source = s.height_source, mna_p95_m = s.mna_p95_m,
    mna_max_m = s.mna_max_m, bati3d_height_m = s.bati3d_height_m, roof_area_m2 = s.roof_area_m2,
    footprints = s.footprints, source_vintage = s.source_vintage, computed_at = s.computed_at
  FROM gold_ch.building_roof_heights s
  WHERE t.egid = s.egid AND t.computed_at IS DISTINCT FROM s.computed_at AND (p_commune IS NULL OR s.no_commune = p_commune);
  INSERT INTO lamap_db_foreign.building_roof_heights
  SELECT s.egid, s.no_commune, s.height_m, s.height_source, s.mna_p95_m, s.mna_max_m, s.bati3d_height_m, s.roof_area_m2,
         s.footprints, s.source_vintage, s.computed_at
  FROM gold_ch.building_roof_heights s
  WHERE (p_commune IS NULL OR s.no_commune = p_commune)
    AND NOT EXISTS (SELECT 1 FROM lamap_db_foreign.building_roof_heights t WHERE t.egid = s.egid);
END $$;

-- daily push, next to cron 101 (plot_canopy_stats at 06:35)
-- idempotent: cron.schedule with a job name replaces that job
SELECT cron.schedule('sync_building_roof_heights', '40 6 * * *', 'CALL gold_ch.sync_building_roof_heights()');
