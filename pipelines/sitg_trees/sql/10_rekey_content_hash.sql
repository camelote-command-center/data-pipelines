-- ═══════════════════════════════════════════════════════════════════════════
-- SITG tree cadastre — re-key both layers onto a deterministic content hash
--
-- WHY (bug f69a9dcb, 2026-08-07)
--   globalid is not an identity. FFP_ARBRES_REMARQUABLES regenerates it on
--   every republish: measured overlap between two consecutive publications was
--   ZERO of 594 globalids, against 594 of 594 objectids, with identical
--   geometry and species. Same trees, all-new GUIDs. The first re-run therefore
--   doubled the table (594 live + 594 soft-deleted).
--
--   SIPV_ICA_ARBRE_ISOLE happened to keep its globalids this time, but the SITG
--   metadata is explicit that OBJECTID must not be relied on as a permanent
--   identifier, and the same wholesale-republish mechanism applies. Re-keyed
--   pre-emptively rather than waiting for 239k tree_ids to churn in production.
--
--   So neither globalid nor objectid is a key. A deterministic content hash is
--   the only stable option, and it is the pattern pipelines/sitg_forest already
--   uses for SITG layers with no durable identifier.
--
-- THE KEY
--   isole:        md5(x,y at 1 cm | nom_complet | circonference_1m)
--   remarquables: md5(x,y at 1 cm | espece      | diametre_tronc)
--
--   Coordinates are rounded rather than hashed through ST_AsBinary: for points
--   this is exactly equivalent, and it stays human-inspectable when a key has
--   to be debugged. 1 cm is far below the positional accuracy of either layer
--   (1 m at best, 25 m for the 1976 survey rows), so rounding cannot merge two
--   genuinely distinct trees that the source could tell apart.
--
-- COLLISIONS ARE REAL AND COLLAPSING THEM IS CORRECT
--   The hash is not unique over the raw source: 10 pairs on isole, 5 on
--   remarquables. Every one was checked and every one is a true duplicate --
--   identical coordinates, species, all measurements, situation, vitality,
--   observation date, differing ONLY in id_arbre/objectid. Seven of the ten
--   isole pairs differ by exactly +113 in id_arbre, the signature of a batch
--   published twice upstream. Collapsing them is de-duplication, not data loss.
--   The count is surfaced by the upsert RPC as `collapsed` on every run, so a
--   NEW collision would show up rather than hide.
--
--   Net effect: isole 239167 -> 239157, remarquables 594 -> 589.
--
-- SOFT DELETE MOVES TO run_id
--   The old soft-delete shipped every globalid seen in the run as one JSON
--   array -- 239167 elements in a single request. That was fragile and is now
--   unnecessary: each run stamps last_run_id, and anything not stamped by the
--   current run is soft-deleted. Same pattern as sitg_forest.
-- ═══════════════════════════════════════════════════════════════════════════

\set ON_ERROR_STOP on

-- ── 1. Key helpers. IMMUTABLE so they can back an index. ─────────────────
CREATE OR REPLACE FUNCTION public.ge_trees_content_key(
  p_geom public.geometry, p_species text, p_measure numeric)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
  SELECT md5(
    CASE WHEN p_geom IS NULL THEN 'nogeom'
         ELSE round(public.ST_X(p_geom)::numeric, 2)::text || ',' ||
              round(public.ST_Y(p_geom)::numeric, 2)::text END
    || '|' || coalesce(btrim(p_species), '')
    || '|' || coalesce(p_measure::text, '')
  );
$$;

COMMENT ON FUNCTION public.ge_trees_content_key(public.geometry, text, numeric) IS
  'Deterministic identity for a SITG tree: position at 1 cm, species, size. '
  'Used because neither globalid nor objectid is stable across SITG '
  'republications (bug f69a9dcb). Two rows sharing this key are the same tree '
  'published twice, which is why collapsing them is correct.';

-- ── 2. Columns ───────────────────────────────────────────────────────────
ALTER TABLE bronze_ch.ge_sipv_arbre_isole
  ADD COLUMN IF NOT EXISTS content_key text,
  ADD COLUMN IF NOT EXISTS last_run_id uuid;
ALTER TABLE bronze_ch.ge_ffp_arbres_remarquables
  ADD COLUMN IF NOT EXISTS content_key text,
  ADD COLUMN IF NOT EXISTS last_run_id uuid;

-- ── 3. Clear the stale generation left by the globalid rotation ──────────
-- Operator-approved. These 594 rows are the previous publication of the
-- remarquables layer, already superseded by an identical set under new GUIDs.
DELETE FROM bronze_ch.ge_ffp_arbres_remarquables WHERE deleted_at IS NOT NULL;

-- ── 4. Backfill, then collapse duplicates onto one row per key ───────────
UPDATE bronze_ch.ge_sipv_arbre_isole
   SET content_key = public.ge_trees_content_key(geom, nom_complet, circonference_1m)
 WHERE content_key IS NULL;
UPDATE bronze_ch.ge_ffp_arbres_remarquables
   SET content_key = public.ge_trees_content_key(geom, espece, diametre_tronc)
 WHERE content_key IS NULL;

-- Keep the lowest objectid of each duplicate set: deterministic, and the lower
-- id is the original of the double-published pairs.
DELETE FROM bronze_ch.ge_sipv_arbre_isole a
 USING bronze_ch.ge_sipv_arbre_isole b
 WHERE a.content_key = b.content_key AND a.objectid > b.objectid;
DELETE FROM bronze_ch.ge_ffp_arbres_remarquables a
 USING bronze_ch.ge_ffp_arbres_remarquables b
 WHERE a.content_key = b.content_key AND a.objectid > b.objectid;

-- ── 5. Swap the primary key ──────────────────────────────────────────────
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.table_constraints
             WHERE table_schema='bronze_ch' AND table_name='ge_sipv_arbre_isole'
               AND constraint_type='PRIMARY KEY') THEN
    ALTER TABLE bronze_ch.ge_sipv_arbre_isole DROP CONSTRAINT ge_sipv_arbre_isole_pkey;
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.table_constraints
             WHERE table_schema='bronze_ch' AND table_name='ge_ffp_arbres_remarquables'
               AND constraint_type='PRIMARY KEY') THEN
    ALTER TABLE bronze_ch.ge_ffp_arbres_remarquables DROP CONSTRAINT ge_ffp_arbres_remarquables_pkey;
  END IF;
END $$;

ALTER TABLE bronze_ch.ge_sipv_arbre_isole
  ALTER COLUMN content_key SET NOT NULL,
  ADD PRIMARY KEY (content_key);
ALTER TABLE bronze_ch.ge_ffp_arbres_remarquables
  ALTER COLUMN content_key SET NOT NULL,
  ADD PRIMARY KEY (content_key);

COMMENT ON COLUMN bronze_ch.ge_sipv_arbre_isole.globalid IS
  'Source GUID, kept for provenance only. NOT the key: SITG regenerates it on '
  'wholesale republish (proved on the remarquables layer, bug f69a9dcb), and '
  'the SITG metadata states OBJECTID is not a permanent identifier either.';
COMMENT ON COLUMN bronze_ch.ge_ffp_arbres_remarquables.globalid IS
  'Source GUID, kept for provenance only. NOT the key: measured ZERO overlap '
  'across two consecutive publications of this layer (bug f69a9dcb).';

CREATE INDEX IF NOT EXISTS ge_sipv_arbre_isole_run_ix
  ON bronze_ch.ge_sipv_arbre_isole (last_run_id);
CREATE INDEX IF NOT EXISTS ge_ffp_arbres_remarquables_run_ix
  ON bronze_ch.ge_ffp_arbres_remarquables (last_run_id);

-- ── 6. Report ────────────────────────────────────────────────────────────
DO $$
DECLARE i_n int; r_n int;
BEGIN
  SELECT count(*) INTO i_n FROM bronze_ch.ge_sipv_arbre_isole;
  SELECT count(*) INTO r_n FROM bronze_ch.ge_ffp_arbres_remarquables;
  RAISE NOTICE 're-keyed: isole % rows, remarquables % rows', i_n, r_n;
END $$;
