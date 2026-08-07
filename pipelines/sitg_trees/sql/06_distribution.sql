-- ═══════════════════════════════════════════════════════════════════════════
-- SITG tree cadastre — distribution wiring on re-LLM
--
-- APPLY ON re-LLM, after 04_ref_lamap_db.sql has been applied on lamap_db.
--
-- The view emits EVERY column of ref.trees_cadastre including updated_at.
-- That is not cosmetic: postgres_fdw sends all foreign-table columns on an
-- INSERT and fills any the query omits with the LOCAL default, which is NULL --
-- the remote DEFAULT never applies. Omitting updated_at here would write NULLs
-- into a NOT NULL column on lamap_db. Learned the hard way on 2026-08-06.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW gold_ch.v_trees_cadastre_full AS
SELECT
  t.tree_id,
  t.source,
  t.geom                                        AS geom_2056,
  ST_Transform(t.geom, 4326)::geometry(Point,4326) AS geometry,
  t.species,
  t.circumference_cm,
  t.trunk_diameter_cm,
  t.crown_diameter_m,
  t.crown_radius_m,
  t.height_total_m,
  t.requires_felling_authorisation,
  t.felling_flag_basis,
  t.is_remarquable,
  t.remarquable_status,
  t.remarquable_reasons,
  t.position_status,
  t.position_precision_m,
  t.is_stump,
  t.vitality,
  t.development_stage,
  t.tree_class,
  t.duplicate_of_tree_id,
  t.egrid,
  t.commune_bfs,
  'GE'::text                                    AS canton_code,
  t.observed_at,
  now()                                         AS updated_at
FROM gold_ch.trees_cadastre t;

-- ---------------------------------------------------------------------------
-- Foreign tables (live + staging). Dropped first: IMPORT FOREIGN SCHEMA has no
-- IF NOT EXISTS and errors on the first table that already exists.
-- ---------------------------------------------------------------------------
DROP FOREIGN TABLE IF EXISTS
  lamap_db_foreign.trees_cadastre,
  lamap_db_foreign._staging_trees_cadastre;

IMPORT FOREIGN SCHEMA "ref" LIMIT TO (trees_cadastre, _staging_trees_cadastre)
  FROM SERVER lamap_db_server INTO lamap_db_foreign;

-- ---------------------------------------------------------------------------
-- Registry row. enabled = false ON PURPOSE.
--
-- gold_ch.run_sync_proc iterates a registry row across all three consumer
-- databases (lamap-crm, lamap-lbi, lamap_db); only lamap_db has this table, so
-- an enabled row would fail twice per run. More importantly the brief requires
-- the sync to run AFTER the parser has refreshed gold and the counts have been
-- verified -- an independently scheduled cron cannot honour that ordering.
-- The row exists to supply pk_column and column_list, which Branch A reads from
-- the registry rather than from the foreign table's pg_attribute.
--
-- frequency = 'quarterly' is documentation here, not a schedule.
-- ---------------------------------------------------------------------------
DELETE FROM gold_ch.sync_registry WHERE target_table = 'trees_cadastre';

INSERT INTO gold_ch.sync_registry
  (source_schema, source_table, target_table, pk_column, sync_mode, frequency, enabled, column_list)
SELECT
  'gold_ch', 'v_trees_cadastre_full', 'trees_cadastre', 'tree_id',
  'full_refresh', 'quarterly', false,
  ARRAY(
    SELECT a.attname
    FROM pg_attribute a
    WHERE a.attrelid = 'gold_ch.v_trees_cadastre_full'::regclass
      AND a.attnum > 0 AND NOT a.attisdropped
    ORDER BY a.attnum
  );

-- ---------------------------------------------------------------------------
-- On-demand sync.
--
-- NOT SECURITY DEFINER and NO SET clauses, deliberately. gold_ch.sync_full_refresh
-- COMMITs internally, and PostgreSQL forbids a procedure from committing if it
-- is SECURITY DEFINER or carries any SET clause (proconfig). Adding either here
-- would fail at runtime with "invalid transaction termination", which is how
-- three wrapper procedures broke on 2026-08-06.
--
-- Not croned: the quarterly workflow CALLs this after the parser has refreshed
-- gold and the counts have been verified.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE gold_ch.sync_trees_cadastre()
LANGUAGE plpgsql
AS $procedure$
DECLARE
  v_rows int;
BEGIN
  CALL gold_ch.sync_full_refresh(
    'gold_ch', 'v_trees_cadastre_full', 'trees_cadastre',
    'lamap_db_server', 'lamap_db_foreign', 'lamap_db', NULL, v_rows);
  RAISE NOTICE 'trees_cadastre synced to lamap_db: % rows', v_rows;
END;
$procedure$;
