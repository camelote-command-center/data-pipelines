-- ============================================================================
-- Repair the Branch A wrappers so their cron jobs work
-- ============================================================================
-- I introduced this on 2026-08-06. The wrappers were written with SET clauses in
-- the definition (proconfig). gold_ch.sync_full_refresh COMMITs internally, and
-- a procedure carrying ANY proconfig cannot commit, so every wrapper failed with
-- "invalid transaction termination". The repair runs were done by calling
-- sync_full_refresh directly, which hid the fact that crons 78, 79 and 80 would
-- have failed on the 3rd of next month.
--
-- gold_ch.run_sync_proc has always avoided this: it carries no proconfig and
-- sets its timeout with set_config() inside the body. Same pattern here.
--
-- TWO constraints apply to any procedure that CALLs a committing procedure:
--   1. No proconfig. A SET clause in the definition blocks the nested COMMIT.
--      Use set_config() inside the body instead, as run_sync_proc does. This
--      overrides the usual guidance about putting statement_timeout in the
--      definition clause for pg_cron callers; that guidance only holds for
--      procedures that never commit.
--   2. No SECURITY DEFINER. Transaction control is not permitted in a
--      security-definer context. gold_ch.run_sync_proc and
--      gold_ch.sync_full_refresh are both security-invoker for this reason, and
--      these wrappers now match. They are reachable only by the roles that could
--      already call sync_full_refresh directly, so this removes no protection.
-- ============================================================================

CREATE OR REPLACE PROCEDURE gold_ch.sync_ge_rdppf_synthese()
LANGUAGE plpgsql
AS $procedure$
DECLARE v int := 0;
BEGIN
  PERFORM set_config('search_path', 'gold_ch, bronze_ch, public', false);
  PERFORM set_config('statement_timeout', '3600000', false);
  CALL gold_ch.sync_full_refresh('bronze_ch', 'ge_rdppf_synthese', 'ge_rdppf_synthese',
                                 'lamap_db_server', 'lamap_db_foreign', 'lamap_db', NULL, v);
  RAISE NOTICE 'sync_ge_rdppf_synthese: % rows written', v;
END;$procedure$;

CREATE OR REPLACE PROCEDURE gold_ch.sync_ge_cad_batiments()
LANGUAGE plpgsql
AS $procedure$
DECLARE v int := 0;
BEGIN
  PERFORM set_config('search_path', 'gold_ch, bronze_ch, public', false);
  PERFORM set_config('statement_timeout', '3600000', false);
  CALL gold_ch.sync_full_refresh('bronze_ch', 'ge_cad_batiments', 'ge_cad_batiments',
                                 'lamap_db_server', 'lamap_db_foreign', 'lamap_db', NULL, v);
  RAISE NOTICE 'sync_ge_cad_batiments: % rows written', v;
END;$procedure$;

CREATE OR REPLACE PROCEDURE gold_ch.sync_ge_cad_batiments_souterrains()
LANGUAGE plpgsql
AS $procedure$
DECLARE v int := 0;
BEGIN
  PERFORM set_config('search_path', 'gold_ch, bronze_ch, public', false);
  PERFORM set_config('statement_timeout', '3600000', false);
  CALL gold_ch.sync_full_refresh('bronze_ch', 'ge_cad_batiments_souterrains', 'ge_cad_batiments_souterrains',
                                 'lamap_db_server', 'lamap_db_foreign', 'lamap_db', NULL, v);
  RAISE NOTICE 'sync_ge_cad_batiments_souterrains: % rows written', v;
END;$procedure$;
