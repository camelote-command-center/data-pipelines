-- ═══════════════════════════════════════════════════════════════════════════
-- gold_ch.plot_trees — add a content key so a vintage can be reloaded safely
--
-- APPLY ON re-LLM. The ref-side twin is in 02_plot_trees_ref_lamap_db.sql.
--
-- WHY (bug 2acfe744)
--   plot_trees.id is a bigint PRIMARY KEY with no default: it was assigned
--   positionally by the one-off 2026-07-24 load. A second detection run
--   renumbers every row, so re-running a vintage would duplicate the table and
--   a regeneration would collide against ref.plot_trees, whose PK is also id.
--   That is the same surrogate-key trap that doubled ge_ffp_arbres_remarquables
--   (bug f69a9dcb) -- fixed here before it can happen rather than after.
--
-- WHAT IDENTITY MEANS FOR A DETECTION, WHICH IS NOT WHAT IT MEANS FOR A REGISTER
--   trees_cadastre could be keyed on content across publications because SITG
--   republishes the same surveyed values. A LiDAR detection cannot: between CHM
--   vintages a real tree shifts by centimetres, grows, and may appear or vanish
--   entirely. There is NO stable cross-vintage identity for a detected tree,
--   and pretending otherwise would silently merge two different measurements of
--   two different years.
--
--   So the model is GENERATION REPLACEMENT, not row-level history:
--     * content_key gives idempotency WITHIN a vintage -- reloading chm-v2
--       updates its own rows instead of duplicating them.
--     * a NEW vintage lands under a new `source`, and the superseded generation
--       is snapshotted and then removed as a whole.
--   Hence UNIQUE (source, content_key), not UNIQUE (content_key).
-- ═══════════════════════════════════════════════════════════════════════════

\set ON_ERROR_STOP on

ALTER TABLE gold_ch.plot_trees
  ADD COLUMN IF NOT EXISTS content_key text;

-- Position at 1 cm plus the two measurements. 1 cm is far below the 0.5 m CHM
-- raster resolution, so it cannot merge two distinct detected peaks; including
-- height and crown radius means a genuine re-detection at the same spot with
-- different measurements is treated as a different row, which is correct.
UPDATE gold_ch.plot_trees
   SET content_key = md5(
         round(ST_X(geom)::numeric, 2)::text || ',' ||
         round(ST_Y(geom)::numeric, 2)::text || '|' ||
         height_m::text || '|' || crown_radius_m::text)
 WHERE content_key IS NULL;

-- Verify before constraining: a duplicate here would mean two detections that
-- this key cannot tell apart, and the constraint must not be created blind.
DO $$
DECLARE v_dupes int; v_rows int;
BEGIN
  SELECT count(*), coalesce(sum(n) - count(*), 0) INTO v_rows, v_dupes
  FROM (SELECT source, content_key, count(*) AS n
        FROM gold_ch.plot_trees GROUP BY 1,2) g;
  IF v_dupes > 0 THEN
    RAISE EXCEPTION 'plot_trees: % rows collide on (source, content_key); '
                    'inspect before constraining', v_dupes;
  END IF;
  RAISE NOTICE 'plot_trees content_key: % distinct (source, content_key), 0 collisions', v_rows;
END $$;

ALTER TABLE gold_ch.plot_trees
  ALTER COLUMN content_key SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS plot_trees_source_content_key_ux
  ON gold_ch.plot_trees (source, content_key);

CREATE INDEX IF NOT EXISTS plot_trees_source_ix ON gold_ch.plot_trees (source);

COMMENT ON COLUMN gold_ch.plot_trees.content_key IS
  'md5(x,y at 1 cm | height_m | crown_radius_m). Gives idempotency WITHIN one '
  'CHM vintage so a reload updates rather than duplicates. It is NOT a '
  'cross-vintage identity: between vintages a real tree shifts, grows, or '
  'disappears, so matching on it across sources would merge measurements from '
  'different years. Vintages are replaced wholesale, keyed by `source`.';

COMMENT ON COLUMN gold_ch.plot_trees.source IS
  'CHM vintage that produced this detection (chm-v1, chm-v2, ...). Exactly one '
  'vintage is live at a time: the loader snapshots and removes the superseded '
  'generation after the new one is verified. id remains the PK for the FDW but '
  'is positional and carries no meaning across runs.';
