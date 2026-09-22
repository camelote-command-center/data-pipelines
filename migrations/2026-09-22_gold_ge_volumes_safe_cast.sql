-- 2026-09-22 — RE-LLM: gold_ch.refresh_ge_plot_volumes casts bronze columns through safe_cast
--
-- Why: re-llm-cast-guard (bug 922cbb28) flagged 2 sites in gold_ch.refresh_ge_plot_volumes
-- (migrations/2026-09-19_gold_ge_volumes.sql). The function cast text columns of
-- bronze_ch.ge_cad_batiments / ge_cad_batiments_souterrains (niveaux_ssol, shape_area, surface)
-- and gold_ch.core_buildings.egid (varchar) with plain ::int / ::numeric / ::bigint. A regex/NULLIF
-- guard is not the house rule: one unexpected source value aborts the whole refresh, the failure mode
-- that froze the FAO gazette in June (incident 2d0aa7fc). safe_cast.* returns NULL and writes the bad
-- value to safe_cast.quarantine instead.
--
-- Behaviour is otherwise identical (same rule, same rows): verified by re-running the refresh and
-- comparing against the values loaded on 2026-09-19.
--
-- Rollback: CREATE OR REPLACE FUNCTION from backup.fn_defs_20260913
--           fn 'gold_ch.refresh_ge_plot_volumes@pre-safe-cast'.

BEGIN;
SET LOCAL statement_timeout = '600s';
SET LOCAL lock_timeout = '5s';

INSERT INTO backup.fn_defs_20260913 (fn, md5, definition)
SELECT 'gold_ch.refresh_ge_plot_volumes@pre-safe-cast', md5(pg_get_functiondef(p.oid)), pg_get_functiondef(p.oid)
FROM pg_proc p WHERE p.pronamespace = 'gold_ch'::regnamespace AND p.proname = 'refresh_ge_plot_volumes'
  AND NOT EXISTS (SELECT 1 FROM backup.fn_defs_20260913 b WHERE b.fn = 'gold_ch.refresh_ge_plot_volumes@pre-safe-cast');

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

  -- Convert each building id ONCE (safe_cast is plpgsql and cannot be inlined: inside a join condition the
  -- planner may evaluate it per candidate pair, which turned a minutes-long refresh into 37+ minutes).
  DROP TABLE IF EXISTS pg_temp.cbx;
  CREATE TEMP TABLE cbx AS
    SELECT cb.egrid, cb.volume_m3, cb.egid::text AS egid_txt,
           safe_cast.to_bigint(cb.egid::text, 'gold_ch.core_buildings', cb.egid::text, 'egid') AS egid_num
    FROM gold_ch.core_buildings cb
    WHERE cb.canton_code = 'GE' AND cb.egrid IS NOT NULL AND cb.egrid <> '';
  CREATE INDEX ON pg_temp.cbx (egid_num); ANALYZE pg_temp.cbx;

  DROP TABLE IF EXISTS pg_temp.hs;
  CREATE TEMP TABLE hs AS
    SELECT x.egrid, sum(COALESCE(x.volume_m3, v3.volume_3d_hors_sol_m3, lg.volume_m3)) AS v
    FROM pg_temp.cbx x
    LEFT JOIN gold_ch.ge_building_volumes_3d v3 ON v3.egid = x.egid_num
    LEFT JOIN gold_ch.ge_building_volumes_legacy lg ON lg.egid = x.egid_txt
    GROUP BY x.egrid;

  DROP TABLE IF EXISTS pg_temp.ss;
  CREATE TEMP TABLE ss AS
  WITH ua AS (
    SELECT a.egid, a.egrid_centroide,
           greatest(coalesce(safe_cast.to_int(a.niveaux_ssol::text, 'bronze_ch.ge_cad_batiments_souterrains', a.egid::text, 'niveaux_ssol'), 1), 1) * 3.0 + 0.5 AS depth,
           coalesce(safe_cast.to_numeric(a.shape_area::text, 'bronze_ch.ge_cad_batiments_souterrains', a.egid::text, 'shape_area'),
                    safe_cast.to_numeric(a.surface::text, 'bronze_ch.ge_cad_batiments_souterrains', a.egid::text, 'surface')) AS attr_area,
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
           coalesce(safe_cast.to_numeric(shape_area::text, 'bronze_ch.ge_cad_batiments', egid::text, 'shape_area'),
                    safe_cast.to_numeric(surface::text, 'bronze_ch.ge_cad_batiments', egid::text, 'surface'))
           * (safe_cast.to_int(niveaux_ssol::text, 'bronze_ch.ge_cad_batiments', egid::text, 'niveaux_ssol') * 3.0 + 0.5) AS v
    FROM bronze_ch.ge_cad_batiments
    WHERE egrid_centroide IS NOT NULL
      AND coalesce(safe_cast.to_int(niveaux_ssol::text, 'bronze_ch.ge_cad_batiments', egid::text, 'niveaux_ssol'), 0) >= 1)
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

COMMIT;
