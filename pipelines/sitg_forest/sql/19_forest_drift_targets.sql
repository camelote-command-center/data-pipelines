-- ============================================================================
-- Watch the seven forest tables from day one (Step 5.3)
-- ============================================================================
-- These tables join the distribution mechanism that bug 177de4c5 was found in.
-- A new table riding a mechanism we have just repaired is exactly the one that
-- should be watched from the first run, not after the first incident.
--
-- All seven go through gold_ch.sync_full_refresh Branch A (staging + upsert),
-- not through a bespoke sync_ge_* procedure, so they were never exposed to the
-- timestamp-gating defect. They are registered anyway: the drift check is
-- mechanism-agnostic and Branch A has its own failure modes.
--
-- Key columns: the six cadastral_forest_* tables are keyed on feature_key, the
-- single deterministic text key gold projects for exactly this reason.
-- plot_forest_constraints is keyed on egrid.
--
-- computed_at is excluded from the comparison on plot_forest_constraints: it is
-- stamped now() on every recompute, so including it would report the whole
-- table as drifted after every quarterly run. Same reasoning as updated_at
-- everywhere else.
-- ============================================================================

INSERT INTO gold_ch.sync_drift_targets (source_rel, foreign_rel, key_column, procedure) VALUES
  ('gold_ch.v_forest_cadastre_full',           'lamap_db_foreign.cadastral_forest_cadastre',           'feature_key', 'sync_full_refresh'),
  ('gold_ch.v_forest_distance_s_full',         'lamap_db_foreign.cadastral_forest_distance_s',         'feature_key', 'sync_full_refresh'),
  ('gold_ch.v_forest_distance_l_full',         'lamap_db_foreign.cadastral_forest_distance_l',         'feature_key', 'sync_full_refresh'),
  ('gold_ch.v_forest_lisieres_full',           'lamap_db_foreign.cadastral_forest_lisieres',           'feature_key', 'sync_full_refresh'),
  ('gold_ch.v_forest_lisieres_parcelles_full', 'lamap_db_foreign.cadastral_forest_lisieres_parcelles', 'feature_key', 'sync_full_refresh'),
  ('gold_ch.v_forest_fonction_full',           'lamap_db_foreign.cadastral_forest_fonction',           'feature_key', 'sync_full_refresh'),
  ('gold_ch.v_plot_forest_constraints_full',   'lamap_db_foreign.plot_forest_constraints',             'egrid',       'sync_full_refresh')
ON CONFLICT (source_rel, foreign_rel) DO NOTHING;
