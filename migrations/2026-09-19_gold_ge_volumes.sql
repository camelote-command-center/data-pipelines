-- 2026-09-19 — RE-LLM gold: Geneva building and plot volumes (bug bdaa9123)
--
-- Why: gold_ch.core_buildings.volume_m3 and gold_ch.core_plots.volume_total_m3 are NULL for all of Geneva
-- (0 / 87,521 buildings, 0 / 73,000 plots): core_buildings takes GWR GVOL, which is empty for GE, and the
-- previous GE source was retired on 2026-07-03 (bug b10bb742) without repointing gold. The monthly
-- distribution therefore ships no GE volume to lamap_db / lamap-crm / lamap-lbi (lamap_db only shows volumes
-- because its registry computes them itself since 2026-09-18/19).
--
-- Rule — identical to lamap_db public.refresh_plots_registry_native STAGE 5A + 8B (2026-09-19):
--   building volume (above ground) = COALESCE(core_buildings.volume_m3,
--                                            gold_ch.ge_building_volumes_3d.volume_3d_hors_sol_m3  (SITG BATI3D),
--                                            gold_ch.ge_building_volumes_legacy (older SITG VOLUME attribute, frozen; see below))
--   plot volume_hors_sol_m3  = Σ building volumes on the plot (core_buildings.egrid)
--   plot volume_sous_sol_m3  = Σ underground structures (bronze_ch.ge_cad_batiments_souterrains + geometry
--                                bronze_ch.ge_buildings_underground_geo), area ON THE PLOT (intersection with
--                                core_plots.geometry; multi-plot car parks split) × (levels × 3.0 m + 0.5 m),
--                                utility structures excluded, no-geometry structures on the centroid plot
--                              + Σ above-ground buildings with niveaux_ssol ≥ 1 (bronze_ch.ge_cad_batiments):
--                                footprint × (levels × 3.0 m + 0.5 m)
--   plot volume_total_m3     = hors sol + COALESCE(sous sol, 0)
--
-- Effect: new table gold_ch.ge_plot_volumes + function gold_ch.refresh_ge_plot_volumes() (monthly, cron before
-- distribution); gold_ch.v_buildings_full.volume_m3 and gold_ch.v_plots_full.volume_total_m3 read them.
-- Both views keep exactly the same columns and types (CREATE OR REPLACE VIEW); core_buildings / core_plots
-- matviews untouched. Non-GE rows unchanged: the SITG fallbacks apply only when canton_code = 'GE'
-- (413 ZH buildings share EGID numbers with GE SITG records).
--
-- Rollback:
--   SELECT definition FROM backup.fn_defs_20260913 WHERE fn IN ('gold_ch.v_plots_full@pre-gold-volumes',
--     'gold_ch.v_buildings_full@pre-gold-volumes');   -- then CREATE OR REPLACE VIEW <name> AS <definition>
--   SELECT cron.unschedule('ge-plot-volumes-monthly');
--   (gold_ch.ge_plot_volumes and the function can stay; nothing else reads them)

BEGIN;
SET LOCAL statement_timeout = '1200s';
SET LOCAL lock_timeout = '5s';

INSERT INTO backup.fn_defs_20260913 (fn, md5, definition)
SELECT 'gold_ch.'||c.relname||'@pre-gold-volumes', md5(pg_get_viewdef(c.oid)), pg_get_viewdef(c.oid)
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname='gold_ch' AND c.relname IN ('v_plots_full','v_buildings_full')
  AND NOT EXISTS (SELECT 1 FROM backup.fn_defs_20260913 b WHERE b.fn='gold_ch.'||c.relname||'@pre-gold-volumes');

CREATE TABLE IF NOT EXISTS gold_ch.ge_plot_volumes (
  egrid                  text PRIMARY KEY,
  volume_hors_sol_m3     numeric,
  volume_sous_sol_m3     numeric,
  volume_sous_sol_source text,
  volume_total_m3        numeric,
  computed_at            timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE gold_ch.ge_plot_volumes IS
  'GE plot volume = above ground (SITG 3D) + estimated below ground (underground structures and basement levels × (levels × 3.0 m + 0.5 m)). Not a SIA 116 cubage. Built by gold_ch.refresh_ge_plot_volumes(); same rule as lamap_db refresh_plots_registry_native (2026-09-19).';

-- Older SITG per-building VOLUME attribute: the bronze source (CAD_BATI3D_BASIC_FACADE) was dropped on
-- 2026-07-03; the last full copy survives in lamap_db ref.buildings_raw (367,868 volumes, 85,925 GE).
-- Bring it back into the warehouse as a frozen GE fallback (raw first, then the 2025-10-17 bronze attribute).
DO $$
BEGIN
  IF to_regclass('lamap_db_foreign.buildings_raw') IS NULL THEN
    EXECUTE 'IMPORT FOREIGN SCHEMA ref LIMIT TO (buildings_raw) FROM SERVER lamap_db_server INTO lamap_db_foreign';
  END IF;
END $$;
CREATE TABLE IF NOT EXISTS gold_ch.ge_building_volumes_legacy AS
WITH ge AS (SELECT DISTINCT egid::text AS egid FROM gold_ch.core_buildings WHERE canton_code = 'GE'),
raw AS (
  SELECT DISTINCT ON (r.egid::text) r.egid::text AS egid, r.volume_m3::numeric AS volume_m3, 'lamap_ref_buildings_raw'::text AS source
  FROM lamap_db_foreign.buildings_raw r JOIN ge ON ge.egid = r.egid::text
  WHERE r.volume_m3 > 0 ORDER BY r.egid::text),
att AS (
  SELECT a.egid::text AS egid, a.volume::numeric AS volume_m3, 'bronze_bati3d_facade_legacyattr_20251017'::text AS source
  FROM bronze_ch.ge_cad_bati3d_facade_legacyattr_20251017 a JOIN ge ON ge.egid = a.egid::text
  WHERE a.volume ~ '^[0-9]+(\.[0-9]+)?$' AND a.volume::numeric > 0)
SELECT * FROM raw
UNION ALL
SELECT * FROM att WHERE egid NOT IN (SELECT egid FROM raw);
CREATE UNIQUE INDEX IF NOT EXISTS ge_building_volumes_legacy_egid ON gold_ch.ge_building_volumes_legacy (egid);
COMMENT ON TABLE gold_ch.ge_building_volumes_legacy IS
  'Frozen older SITG BATI3D VOLUME per GE building (source dropped 2026-07-03; restored from lamap_db ref.buildings_raw + bronze legacyattr 2025-10-17). Fallback when no current 3D volume.';

CREATE OR REPLACE FUNCTION gold_ch.refresh_ge_plot_volumes()
 RETURNS TABLE(plots integer, with_hors_sol integer, with_sous_sol integer)
 LANGUAGE plpgsql
 SET search_path TO 'gold_ch', 'bronze_ch', 'public'
 SET statement_timeout TO '1800s'
AS $function$
BEGIN
  DROP TABLE IF EXISTS pg_temp.gp;
  CREATE TEMP TABLE gp AS
    SELECT egrid, st_transform(geometry, 2056) AS g FROM gold_ch.core_plots
    WHERE canton_code = 'GE' AND geometry IS NOT NULL;
  CREATE INDEX ON pg_temp.gp USING gist (g); ANALYZE pg_temp.gp;

  DROP TABLE IF EXISTS pg_temp.hs;
  CREATE TEMP TABLE hs AS
    SELECT cb.egrid, sum(COALESCE(cb.volume_m3, v3.volume_3d_hors_sol_m3, lg.volume_m3)) AS v
    FROM gold_ch.core_buildings cb
    LEFT JOIN gold_ch.ge_building_volumes_3d v3 ON cb.egid ~ '^\d+$' AND v3.egid = cb.egid::bigint
    LEFT JOIN gold_ch.ge_building_volumes_legacy lg ON lg.egid = cb.egid::text
    WHERE cb.canton_code = 'GE' AND cb.egrid IS NOT NULL AND cb.egrid <> ''
    GROUP BY cb.egrid;

  DROP TABLE IF EXISTS pg_temp.ss;
  CREATE TEMP TABLE ss AS
  WITH ua AS (
    SELECT a.egid, a.egrid_centroide,
           greatest(coalesce(nullif(a.niveaux_ssol,'')::int, 1), 1) * 3.0 + 0.5 AS depth,
           coalesce(nullif(a.shape_area::text,'')::numeric, nullif(a.surface::text,'')::numeric) AS attr_area,
           st_transform(st_setsrid(ug.geometry, coalesce(nullif(st_srid(ug.geometry),0), 2056)), 2056) AS g
    FROM bronze_ch.ge_cad_batiments_souterrains a
    LEFT JOIN bronze_ch.ge_buildings_underground_geo ug ON ug.egid::text = a.egid::text
    WHERE a.destination !~* '^(Instal|Bâtiment eau|Bât\. électricité|Bâtiment électricité|Citerne|Cabine T\+T|Central de télécom|Réservoir|Station d.épuration|Chauffage à distance)'),
  us AS (
    SELECT p.egrid, st_area(st_intersection(u.g, p.g)) AS a, u.depth
    FROM ua u JOIN pg_temp.gp p ON u.g IS NOT NULL AND st_intersects(u.g, p.g)
    UNION ALL
    SELECT u.egrid_centroide, u.attr_area, u.depth FROM ua u WHERE u.g IS NULL AND u.egrid_centroide IS NOT NULL),
  bu AS (
    SELECT egrid_centroide AS egrid,
           coalesce(nullif(shape_area::text,'')::numeric, nullif(surface::text,'')::numeric) * (niveaux_ssol::int * 3.0 + 0.5) AS v
    FROM bronze_ch.ge_cad_batiments WHERE egrid_centroide IS NOT NULL AND niveaux_ssol ~ '^[1-9]')
  SELECT egrid, sum(v)::numeric AS v, string_agg(DISTINCT src, '+' ORDER BY src) AS src FROM (
    SELECT egrid, (a * depth)::numeric AS v, 'souterrain_sitg' AS src FROM us WHERE a >= 1
    UNION ALL SELECT egrid, v, 'niveaux_ssol' FROM bu WHERE v > 0) z
  GROUP BY egrid;

  -- same transaction as the caller: readers see the old rows until commit
  DELETE FROM gold_ch.ge_plot_volumes;
  INSERT INTO gold_ch.ge_plot_volumes (egrid, volume_hors_sol_m3, volume_sous_sol_m3, volume_sous_sol_source, volume_total_m3, computed_at)
  SELECT k.egrid, round(hs.v, 2), round(ss.v, 2), ss.src,
         round(coalesce(hs.v, 0) + coalesce(ss.v, 0), 2), now()
  FROM (SELECT egrid FROM pg_temp.hs UNION SELECT egrid FROM pg_temp.ss) k
  LEFT JOIN pg_temp.hs ON hs.egrid = k.egrid
  LEFT JOIN pg_temp.ss ON ss.egrid = k.egrid
  WHERE coalesce(hs.v, 0) + coalesce(ss.v, 0) > 0;

  RETURN QUERY SELECT count(*)::int, count(volume_hors_sol_m3)::int, count(volume_sous_sol_m3)::int FROM gold_ch.ge_plot_volumes;
END $function$;

SELECT * FROM gold_ch.refresh_ge_plot_volumes();

DO $$
DECLARE d text; d2 text;
BEGIN
  d := pg_get_viewdef('gold_ch.v_buildings_full'::regclass);
  d2 := replace(d, '    cb.volume_m3,', '    COALESCE(cb.volume_m3, CASE WHEN cb.canton_code = ''GE'' THEN COALESCE(gbv.volume_3d_hors_sol_m3, gbl.volume_m3) END) AS volume_m3,');
  IF d2 = d THEN RAISE EXCEPTION 'v_buildings_full volume anchor not found'; END IF;
  d := d2;
  d2 := replace(d, 'FROM (((gold_ch.core_buildings cb',
    'FROM (((((gold_ch.core_buildings cb
     LEFT JOIN gold_ch.ge_building_volumes_3d gbv ON (((cb.egid)::text ~ ''^\d+$''::text) AND (gbv.egid = ((cb.egid)::text)::bigint)))
     LEFT JOIN gold_ch.ge_building_volumes_legacy gbl ON ((gbl.egid = (cb.egid)::text)))');
  IF d2 = d THEN RAISE EXCEPTION 'v_buildings_full FROM anchor not found'; END IF;
  EXECUTE 'CREATE OR REPLACE VIEW gold_ch.v_buildings_full AS ' || d2;

  d := pg_get_viewdef('gold_ch.v_plots_full'::regclass);
  d2 := replace(d, '    cp.volume_total_m3,', '    COALESCE(gpv.volume_total_m3, cp.volume_total_m3) AS volume_total_m3,');
  IF d2 = d THEN RAISE EXCEPTION 'v_plots_full volume anchor not found'; END IF;
  d := d2;
  d2 := replace(d, 'FROM (((((((((gold_ch.core_plots cp',
    'FROM ((((((((((gold_ch.core_plots cp
     LEFT JOIN gold_ch.ge_plot_volumes gpv ON ((gpv.egrid = cp.egrid)))');
  IF d2 = d THEN RAISE EXCEPTION 'v_plots_full FROM anchor not found'; END IF;
  EXECUTE 'CREATE OR REPLACE VIEW gold_ch.v_plots_full AS ' || d2;
END $$;

-- monthly, 30 min before distribution-monthly (cron 6, 02:00 on the 1st)
SELECT cron.schedule('ge-plot-volumes-monthly', '30 1 1 * *', $c$SELECT * FROM gold_ch.refresh_ge_plot_volumes();$c$)
WHERE NOT EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'ge-plot-volumes-monthly');

COMMIT;
