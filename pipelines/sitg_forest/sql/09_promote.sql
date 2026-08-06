-- ============================================================================
-- Geneva forest layers — promote bronze to silver, gold and lamap_db ref
-- ============================================================================
-- Run AFTER pipelines/sitg_forest/import.py has landed bronze and its own count
-- assertions have passed. This file refreshes silver, asserts that nothing was
-- lost between layers, and only then distributes to lamap_db.
--
-- Ordering is load-bearing:
--   cadastral_forest_lisieres_parcelles reads cadastral_forest_lisieres, so the
--   parent must refresh first. Refreshing the child against a stale parent
--   would silently parse the previous quarter's PARCELLES strings.
--
-- REFRESH is deliberately NOT CONCURRENTLY. Concurrent refresh cannot run
-- inside a transaction block, and these matviews are small (the largest is
-- 2'738 rows), so the exclusive lock is measured in milliseconds. Do not
-- "optimise" this to CONCURRENTLY without re-reading how psql wraps -f files.
-- ============================================================================

\set ON_ERROR_STOP on

-- ---------------------------------------------------------------------------
-- 1. Silver
-- ---------------------------------------------------------------------------
REFRESH MATERIALIZED VIEW silver_ch.cadastral_forest_cadastre;
REFRESH MATERIALIZED VIEW silver_ch.cadastral_forest_distance_s;
REFRESH MATERIALIZED VIEW silver_ch.cadastral_forest_distance_l;
REFRESH MATERIALIZED VIEW silver_ch.cadastral_forest_lisieres;
REFRESH MATERIALIZED VIEW silver_ch.cadastral_forest_lisieres_parcelles;  -- after its parent
REFRESH MATERIALIZED VIEW silver_ch.cadastral_forest_fonction;

-- ---------------------------------------------------------------------------
-- 2. Assert bronze -> silver conservation
-- ---------------------------------------------------------------------------
-- Silver drops exactly the rows with no usable geometry, and nothing else.
DO $$
DECLARE
  r RECORD;
  v_bronze int;
  v_silver int;
BEGIN
  FOR r IN
    SELECT * FROM (VALUES
      ('ge_ffp_cadastre_foret',       'cadastral_forest_cadastre'),
      ('ge_rdppf_distances_foret_s',  'cadastral_forest_distance_s'),
      ('ge_rdppf_distances_foret_l',  'cadastral_forest_distance_l'),
      ('ge_ffp_lisieres_forestieres', 'cadastral_forest_lisieres'),
      ('ge_ffp_fonction_pdf',         'cadastral_forest_fonction')
    ) AS t(bronze_table, silver_table)
  LOOP
    EXECUTE format(
      'SELECT count(*) FROM bronze_ch.%I WHERE deleted_at IS NULL AND geometry IS NOT NULL',
      r.bronze_table) INTO v_bronze;
    EXECUTE format('SELECT count(*) FROM silver_ch.%I', r.silver_table) INTO v_silver;

    IF v_bronze <> v_silver THEN
      RAISE EXCEPTION
        'bronze/silver mismatch: bronze_ch.% has % usable rows but silver_ch.% has %',
        r.bronze_table, v_bronze, r.silver_table, v_silver;
    END IF;
    RAISE NOTICE 'OK  bronze_ch.%  ->  silver_ch.%  (% rows)',
      r.bronze_table, r.silver_table, v_silver;
  END LOOP;
END $$;

-- Silver must be geometrically clean after repair.
DO $$
DECLARE r RECORD;
BEGIN
  FOR r IN SELECT * FROM silver_ch.v_ge_forest_geometry_audit LOOP
    IF r.invalid_after > 0 THEN
      RAISE EXCEPTION '% still has % invalid geometries after ST_MakeValid',
        r.layer, r.invalid_after;
    END IF;
    RAISE NOTICE 'OK  %  invalid before=% after=%  rows=%',
      r.layer, r.invalid_before, r.invalid_after, r.rows_silver;
  END LOOP;
END $$;

-- ---------------------------------------------------------------------------
-- 3. Distribute to lamap_db (Branch A: staging + upsert, never TRUNCATE)
-- ---------------------------------------------------------------------------
DO $$
DECLARE
  r RECORD;
  v int := 0;
BEGIN
  FOR r IN
    SELECT * FROM (VALUES
      ('v_forest_cadastre_full',           'cadastral_forest_cadastre'),
      ('v_forest_distance_s_full',         'cadastral_forest_distance_s'),
      ('v_forest_distance_l_full',         'cadastral_forest_distance_l'),
      ('v_forest_lisieres_full',           'cadastral_forest_lisieres'),
      ('v_forest_lisieres_parcelles_full', 'cadastral_forest_lisieres_parcelles'),
      ('v_forest_fonction_full',           'cadastral_forest_fonction')
    ) AS t(source_table, target_table)
  LOOP
    v := 0;
    CALL gold_ch.sync_full_refresh('gold_ch', r.source_table, r.target_table,
                                   'lamap_db_server', 'lamap_db_foreign', 'lamap_db',
                                   NULL, v);
    RAISE NOTICE 'synced %  ->  ref.%  (% rows)', r.source_table, r.target_table, v;
  END LOOP;
END $$;

-- ---------------------------------------------------------------------------
-- 4. Assert gold -> ref conservation, read back across the FDW
-- ---------------------------------------------------------------------------
DO $$
DECLARE
  r RECORD;
  v_gold int;
  v_ref  int;
BEGIN
  FOR r IN
    SELECT * FROM (VALUES
      ('v_forest_cadastre_full',           'cadastral_forest_cadastre'),
      ('v_forest_distance_s_full',         'cadastral_forest_distance_s'),
      ('v_forest_distance_l_full',         'cadastral_forest_distance_l'),
      ('v_forest_lisieres_full',           'cadastral_forest_lisieres'),
      ('v_forest_lisieres_parcelles_full', 'cadastral_forest_lisieres_parcelles'),
      ('v_forest_fonction_full',           'cadastral_forest_fonction')
    ) AS t(source_table, target_table)
  LOOP
    EXECUTE format('SELECT count(*) FROM gold_ch.%I', r.source_table) INTO v_gold;
    EXECUTE format('SELECT count(*) FROM lamap_db_foreign.%I', r.target_table) INTO v_ref;
    IF v_gold <> v_ref THEN
      RAISE EXCEPTION 'gold/ref mismatch: gold_ch.% has % but ref.% has %',
        r.source_table, v_gold, r.target_table, v_ref;
    END IF;
    RAISE NOTICE 'OK  gold_ch.%  ->  ref.%  (% rows)', r.source_table, r.target_table, v_ref;
  END LOOP;
END $$;

-- ---------------------------------------------------------------------------
-- 5. Phase 2 — per-plot enrichment
-- ---------------------------------------------------------------------------
-- Chained here deliberately, NOT scheduled independently: the recompute must
-- never run against a half-loaded forest layer, and every assertion above has
-- to pass first.
CALL gold_ch.refresh_plot_forest_constraints();

DO $$
DECLARE v int := 0; v_plots int; v_rows int;
BEGIN
  SELECT count(*) INTO v_plots FROM silver_ch.cadastral_plots WHERE canton_code = 'GE';
  SELECT count(*) INTO v_rows  FROM gold_ch.plot_forest_constraints;
  IF v_rows <> v_plots THEN
    RAISE EXCEPTION 'plot_forest_constraints has % rows for % GE plots', v_rows, v_plots;
  END IF;

  -- forest_constraint_source must never collapse to fewer than the categories
  -- the data actually contains. A sudden drop to one value means the source
  -- layers loaded empty.
  IF (SELECT count(DISTINCT forest_constraint_source) FROM gold_ch.plot_forest_constraints) < 3 THEN
    RAISE EXCEPTION 'forest_constraint_source collapsed to % distinct values, expected 4',
      (SELECT count(DISTINCT forest_constraint_source) FROM gold_ch.plot_forest_constraints);
  END IF;

  CALL gold_ch.sync_full_refresh('gold_ch', 'v_plot_forest_constraints_full',
                                 'plot_forest_constraints', 'lamap_db_server',
                                 'lamap_db_foreign', 'lamap_db', NULL, v);
  RAISE NOTICE 'synced plot_forest_constraints -> ref (% rows, % plots)', v, v_plots;
END $$;
