-- ═══════════════════════════════════════════════════════════════════════════
-- SITG tree cadastre — promote and distribute, chained after the parser
--
-- Run on re-LLM with -v ON_ERROR_STOP=1. Order is fixed:
--   1. rebuild gold from bronze
--   2. assert conservation and the invariants that matter legally
--   3. distribute to lamap_db ref via Branch A
--   4. assert the consumer matches
--
-- Statements are top level, NOT wrapped in a DO block, because
-- gold_ch.sync_full_refresh COMMITs internally and PostgreSQL forbids that
-- inside an atomic block.
-- ═══════════════════════════════════════════════════════════════════════════

\set ON_ERROR_STOP on

-- ── 1. Rebuild gold ──────────────────────────────────────────────────────
CALL gold_ch.refresh_trees_cadastre();

-- ── 2. Assertions before anything leaves re-LLM ──────────────────────────
DO $$
DECLARE
  v_gold   int;
  v_bronze int;
  v_rmq    int;
  v_states int;
BEGIN
  SELECT count(*) INTO v_gold FROM gold_ch.trees_cadastre;
  SELECT (SELECT count(*) FROM bronze_ch.ge_sipv_arbre_isole WHERE deleted_at IS NULL)
       + (SELECT count(*) FROM bronze_ch.ge_ffp_arbres_remarquables WHERE deleted_at IS NULL)
    INTO v_bronze;

  IF v_gold <> v_bronze THEN
    RAISE EXCEPTION 'conservation failed: gold % rows, bronze live % rows', v_gold, v_bronze;
  END IF;

  -- Remarkability must never be inferred from size. If this count ever tracks
  -- the circumference distribution instead of staying near the ~206 OCAN
  -- publishes, something has started deriving it.
  SELECT count(*) INTO v_rmq FROM gold_ch.trees_cadastre WHERE is_remarquable;
  IF v_rmq > 1000 THEN
    RAISE EXCEPTION 'is_remarquable = % rows, far above the ~400 expected (206 ICA + 208 approved FFP). Remarkability is being derived, not carried.', v_rmq;
  END IF;

  -- The felling flag must stay tri-state. If NULLs vanish, something collapsed
  -- "unmeasured" into "below threshold", which would read as permission to fell.
  SELECT count(DISTINCT coalesce(requires_felling_authorisation::text,'null'))
    INTO v_states FROM gold_ch.trees_cadastre;
  IF v_states < 3 THEN
    RAISE EXCEPTION 'requires_felling_authorisation collapsed to % state(s); it must remain true/false/NULL', v_states;
  END IF;

  RAISE NOTICE 'gold OK: % rows, % remarkable, tri-state intact', v_gold, v_rmq;
END $$;

-- ── 3. Distribute ────────────────────────────────────────────────────────
CALL gold_ch.sync_trees_cadastre();

-- ── 4. Assert the consumer matches ───────────────────────────────────────
DO $$
DECLARE
  v_gold int;
  v_ref  int;
BEGIN
  SELECT count(*) INTO v_gold FROM gold_ch.trees_cadastre;
  SELECT count(*) INTO v_ref  FROM lamap_db_foreign.trees_cadastre;
  IF v_gold <> v_ref THEN
    RAISE EXCEPTION 'distribution mismatch: gold % rows, lamap_db ref % rows', v_gold, v_ref;
  END IF;
  RAISE NOTICE 'lamap_db ref.trees_cadastre matches gold: % rows', v_ref;
END $$;
