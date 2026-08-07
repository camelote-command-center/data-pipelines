-- Backfill the identity that the patched refresh_plot_forest_constraints now
-- resolves, without re-running the full spatial refresh.
--
-- This is safe to do as a targeted UPDATE rather than a CALL because the patch
-- changed only how identity is resolved in _p. The row set is unchanged (still
-- one row per GE plot, the LATERAL ... LIMIT 1 guarantees no fan-out) and the
-- geometry column is untouched, so every spatial measure the procedure computes
-- is bit-identical. Only two things move: no_commune_no_parcelle itself, and
-- the two lisieres-derived columns, which key off the no_commune/no_parcelle
-- components and so can newly match for a plot that just gained an identity.
--
-- Snapshot taken first: backup.plot_forest_constraints_20260807_nocommune.

\set ON_ERROR_STOP on
BEGIN;

CREATE TEMP TABLE _res ON COMMIT DROP AS
WITH roster_1to1 AS (
  SELECT no_com_federal::int AS bfs, min(no_comm)::text AS no_comm
  FROM bronze_ch.ge_cad_communes
  WHERE no_com_federal IS NOT NULL AND no_comm IS NOT NULL
  GROUP BY 1
  HAVING count(DISTINCT no_comm) = 1
)
SELECT s.egrid,
       COALESCE(nullif(trim(c.no_comm), '') || '/' || nullif(trim(c.no_parcelle), ''),
                r.no_comm || '/' || nullif(trim(f.parcel_number), ''))        AS new_id,
       CASE WHEN COALESCE(nullif(trim(c.no_comm), ''), r.no_comm) ~ '^[0-9]+$'
            THEN COALESCE(nullif(trim(c.no_comm), ''), r.no_comm) END          AS no_commune,
       CASE WHEN COALESCE(nullif(trim(c.no_parcelle), ''), nullif(trim(f.parcel_number), '')) ~ '^[0-9]+$'
            THEN COALESCE(nullif(trim(c.no_parcelle), ''), nullif(trim(f.parcel_number), '')) END AS no_parcelle
FROM silver_ch.cadastral_plots s
LEFT JOIN LATERAL (
  SELECT p2.no_comm, p2.no_parcelle FROM bronze_ch.ge_cad_parcelles p2
  WHERE p2.egrid::text = s.egrid ORDER BY p2.no_comm, p2.no_parcelle LIMIT 1) c ON true
LEFT JOIN LATERAL (
  SELECT f2.commune_bfs, f2.parcel_number FROM bronze_ch.federal_cadastral_parcels f2
  WHERE f2.egrid = s.egrid ORDER BY f2.commune_bfs, f2.parcel_number LIMIT 1) f ON true
LEFT JOIN roster_1to1 r ON r.bfs = f.commune_bfs
WHERE s.canton_code = 'GE' AND s.no_commune_no_parcelle IS NULL;

-- Lisieres recomputed for exactly the rows that just gained an identity,
-- using the same predicates as the procedure (including parse_status).
CREATE TEMP TABLE _lisfix ON COMMIT DROP AS
SELECT p.egrid,
       bool_or(NOT COALESCE(l.in_force, false))               AS procedure_open,
       max(GREATEST(l.fao_requete_date, l.fao_decision_date)) AS last_fao_date
FROM silver_ch.cadastral_forest_lisieres_parcelles pa
JOIN silver_ch.cadastral_forest_lisieres l
  ON l.id_dossier_key = pa.id_dossier_key AND l.geom_hash = pa.geom_hash
JOIN bronze_ch.ge_cad_communes c
  ON c.no_com_federal::int = pa.no_commune
JOIN _res p
  ON p.no_commune::int = c.no_comm::int AND p.no_parcelle::int = pa.no_parcelle
WHERE pa.parse_status = 'parsed' AND pa.no_parcelle IS NOT NULL
GROUP BY p.egrid;

\echo '--- rows resolvable / of which gain lisieres ---'
SELECT (SELECT count(*) FROM _res WHERE new_id IS NOT NULL) AS resolvable,
       (SELECT count(*) FROM _res WHERE new_id IS NULL)     AS still_unresolvable,
       (SELECT count(*) FROM _lisfix)                       AS gain_lisieres;

-- Guard: never overwrite an identity that is already present.
UPDATE gold_ch.plot_forest_constraints t
SET no_commune_no_parcelle  = r.new_id,
    lisiere_procedure_open  = COALESCE(lf.procedure_open, t.lisiere_procedure_open),
    lisiere_last_fao_date   = COALESCE(lf.last_fao_date,  t.lisiere_last_fao_date)
FROM _res r
LEFT JOIN _lisfix lf ON lf.egrid = r.egrid
WHERE t.egrid = r.egrid
  AND r.new_id IS NOT NULL
  AND t.no_commune_no_parcelle IS NULL;

\echo '--- post-state on gold ---'
SELECT count(*) AS total,
       count(*) FILTER (WHERE no_commune_no_parcelle IS NULL) AS still_null
FROM gold_ch.plot_forest_constraints;

COMMIT;
