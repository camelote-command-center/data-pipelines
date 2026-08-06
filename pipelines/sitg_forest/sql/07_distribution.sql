-- ============================================================================
-- Geneva forest layers — distribution wiring on re-LLM
-- ============================================================================
-- Steps 8 and 5 of platform.standards / silver_promotion_pattern_ge_overlays:
-- import the consumer tables as foreign tables, then register the syncs.
--
-- WHY enabled = false ON EVERY ROW
--   gold_ch.run_sync_proc iterates a registry row across ALL THREE consumer
--   databases (lamap-crm, lamap-lbi, lamap_db). Only lamap_db has these tables,
--   so leaving the rows enabled would fail twice per layer per run.
--   More importantly the brief requires the distribution to be chained to the
--   quarterly parser run and ordered "parse, verify counts, then recompute" —
--   the enrichment must never run against a half-loaded forest layer. An
--   independently scheduled cron cannot honour that ordering. The workflow
--   therefore CALLs gold_ch.sync_full_refresh per layer after verification, and
--   these rows exist to supply pk_column and column_list, which Branch A reads
--   from the registry rather than from the foreign table's pg_attribute.
--
-- frequency = 'quarterly' is documentation here, not a schedule: no cron passes
-- 'quarterly' to run_sync_proc.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Foreign tables (live + staging)
-- ---------------------------------------------------------------------------
-- Dropped first so this file is re-runnable: IMPORT FOREIGN SCHEMA has no
-- IF NOT EXISTS and errors on the first table that already exists.
DROP FOREIGN TABLE IF EXISTS
  lamap_db_foreign.cadastral_forest_cadastre,
  lamap_db_foreign.cadastral_forest_distance_s,
  lamap_db_foreign.cadastral_forest_distance_l,
  lamap_db_foreign.cadastral_forest_lisieres,
  lamap_db_foreign.cadastral_forest_lisieres_parcelles,
  lamap_db_foreign.cadastral_forest_fonction,
  lamap_db_foreign._staging_cadastral_forest_cadastre,
  lamap_db_foreign._staging_cadastral_forest_distance_s,
  lamap_db_foreign._staging_cadastral_forest_distance_l,
  lamap_db_foreign._staging_cadastral_forest_lisieres,
  lamap_db_foreign._staging_cadastral_forest_lisieres_parcelles,
  lamap_db_foreign._staging_cadastral_forest_fonction;

IMPORT FOREIGN SCHEMA "ref" LIMIT TO (
  cadastral_forest_cadastre,
  cadastral_forest_distance_s,
  cadastral_forest_distance_l,
  cadastral_forest_lisieres,
  cadastral_forest_lisieres_parcelles,
  cadastral_forest_fonction,
  _staging_cadastral_forest_cadastre,
  _staging_cadastral_forest_distance_s,
  _staging_cadastral_forest_distance_l,
  _staging_cadastral_forest_lisieres,
  _staging_cadastral_forest_lisieres_parcelles,
  _staging_cadastral_forest_fonction
) FROM SERVER lamap_db_server INTO lamap_db_foreign;

-- ---------------------------------------------------------------------------
-- sync_registry.frequency predates quarterly pipelines and allowed only
-- daily / weekly / monthly. Widening a CHECK cannot invalidate an existing row,
-- and recording 'monthly' for a layer that is refreshed four times a year would
-- be false documentation. run_sync_proc is unaffected: it selects on
-- frequency = <argument> AND enabled, and no cron passes 'quarterly'.
-- Rollback: ALTER TABLE gold_ch.sync_registry DROP CONSTRAINT
--   sync_registry_frequency_check;
--   ALTER TABLE gold_ch.sync_registry ADD CONSTRAINT sync_registry_frequency_check
--   CHECK (frequency = ANY (ARRAY['daily','weekly','monthly']));
-- ---------------------------------------------------------------------------
ALTER TABLE gold_ch.sync_registry DROP CONSTRAINT IF EXISTS sync_registry_frequency_check;
ALTER TABLE gold_ch.sync_registry ADD CONSTRAINT sync_registry_frequency_check
  CHECK (frequency = ANY (ARRAY['daily'::text, 'weekly'::text, 'monthly'::text, 'quarterly'::text]));

-- ---------------------------------------------------------------------------
-- Registry rows. column_list is generated from the gold view definitions so it
-- cannot drift from what the views actually project.
-- ---------------------------------------------------------------------------
DELETE FROM gold_ch.sync_registry
WHERE target_table IN (
  'cadastral_forest_cadastre','cadastral_forest_distance_s',
  'cadastral_forest_distance_l','cadastral_forest_lisieres',
  'cadastral_forest_lisieres_parcelles','cadastral_forest_fonction'
);

INSERT INTO gold_ch.sync_registry
  (source_schema, source_table, target_table, pk_column, sync_mode, frequency, enabled, column_list)
SELECT
  'gold_ch',
  v.source_table,
  v.target_table,
  'feature_key',
  'full_refresh',
  'quarterly',
  false,
  ARRAY(
    SELECT a.attname
    FROM pg_attribute a
    WHERE a.attrelid = ('gold_ch.' || v.source_table)::regclass
      AND a.attnum > 0
      AND NOT a.attisdropped
    ORDER BY a.attnum
  )
FROM (VALUES
  ('v_forest_cadastre_full',           'cadastral_forest_cadastre'),
  ('v_forest_distance_s_full',         'cadastral_forest_distance_s'),
  ('v_forest_distance_l_full',         'cadastral_forest_distance_l'),
  ('v_forest_lisieres_full',           'cadastral_forest_lisieres'),
  ('v_forest_lisieres_parcelles_full', 'cadastral_forest_lisieres_parcelles'),
  ('v_forest_fonction_full',           'cadastral_forest_fonction')
) AS v(source_table, target_table);
