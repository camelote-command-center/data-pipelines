\set ON_ERROR_STOP on
-- trees_cadastre joins the daily drift detector. Measured cost 70s for 239746
-- rows / 25 columns, which is affordable alongside the existing 17 targets.
--
-- updated_at is EXCLUDED: v_trees_cadastre_full emits now(), so every row would
-- read as drifted on every run. That is the exact false-positive that made the
-- ge_rdppf_synthese hash report 74'251 drifted rows before it was rebuilt over
-- content columns only.
--
-- plot_trees is deliberately NOT registered. Measured: the row-hash comparison
-- dies at ~4.5 minutes over the FDW even with geom excluded (1.59M rows), and a
-- daily target that cannot finish would jeopardise the detector for all the
-- others. It is also static -- one chm-v1 batch dated 2026-07-24, no procedure
-- and no cron writes it -- so there is no generation for ref to fall behind.
-- The real exposure is the NEXT CHM run, and the fix belongs there: whatever
-- ships CHM v2 must CALL gold_ch.sync_plot_trees() in the same job.
DELETE FROM gold_ch.sync_drift_targets WHERE foreign_rel = 'lamap_db_foreign.trees_cadastre';
INSERT INTO gold_ch.sync_drift_targets
  (source_rel, foreign_rel, key_column, procedure, enabled, accepted_extra, exclude_columns)
VALUES ('gold_ch.v_trees_cadastre_full','lamap_db_foreign.trees_cadastre','tree_id',
        'sync_trees_cadastre', true, 0, ARRAY['updated_at']);
SELECT source_rel, foreign_rel, key_column, enabled, exclude_columns
FROM gold_ch.sync_drift_targets WHERE foreign_rel='lamap_db_foreign.trees_cadastre';
