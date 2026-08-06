-- ============================================================================
-- Move the three heavy syncs onto gold_ch.sync_full_refresh Branch A
-- ============================================================================
-- The cross-FDW `UPDATE lamap_db_foreign.X t ... FROM bronze_ch.X s` shape
-- cannot push a predicate that references a remote column, so EXPLAIN resolves
-- to a Foreign Scan over the WHOLE remote table followed by one remote UPDATE
-- per changed row. At 8'108 rows that finishes in ~3 minutes; at 74'436 and
-- 83'018 it does not finish at all.
--
-- A sync that cannot finish is not merely slow: it under-repairs SILENTLY,
-- which is the same defect class this audit exists to remove, and it makes a
-- raising drift detector meaningless because the process cannot pass.
--
-- Branch A stages the source into ref._staging_<target> in one bulk insert and
-- then does the UPSERT entirely on lamap_db via dblink_exec. It moved 73'000
-- forest rows in seconds at this same scale.
--
-- ge_cad_batiments and ge_cad_batiments_souterrains needed a single-column key
-- for ON CONFLICT: business_key is a STORED generated column
-- (no_comm || '/' || no_batiment) with a unique index, on both databases. It is
-- excluded from column_list because a generated column cannot be inserted into.
-- ============================================================================

CREATE OR REPLACE PROCEDURE gold_ch.sync_ge_rdppf_synthese()
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'gold_ch', 'bronze_ch', 'public'
SET statement_timeout TO '3600s'
AS $procedure$
DECLARE v int := 0;
BEGIN
  CALL gold_ch.sync_full_refresh('bronze_ch', 'ge_rdppf_synthese', 'ge_rdppf_synthese',
                                 'lamap_db_server', 'lamap_db_foreign', 'lamap_db',
                                 NULL, v);
  RAISE NOTICE 'sync_ge_rdppf_synthese: % rows written', v;
END;$procedure$;

CREATE OR REPLACE PROCEDURE gold_ch.sync_ge_cad_batiments()
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'gold_ch', 'bronze_ch', 'public'
SET statement_timeout TO '3600s'
AS $procedure$
DECLARE v int := 0;
BEGIN
  CALL gold_ch.sync_full_refresh('bronze_ch', 'ge_cad_batiments', 'ge_cad_batiments',
                                 'lamap_db_server', 'lamap_db_foreign', 'lamap_db',
                                 NULL, v);
  RAISE NOTICE 'sync_ge_cad_batiments: % rows written', v;
END;$procedure$;

CREATE OR REPLACE PROCEDURE gold_ch.sync_ge_cad_batiments_souterrains()
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'gold_ch', 'bronze_ch', 'public'
SET statement_timeout TO '3600s'
AS $procedure$
DECLARE v int := 0;
BEGIN
  CALL gold_ch.sync_full_refresh('bronze_ch', 'ge_cad_batiments_souterrains', 'ge_cad_batiments_souterrains',
                                 'lamap_db_server', 'lamap_db_foreign', 'lamap_db',
                                 NULL, v);
  RAISE NOTICE 'sync_ge_cad_batiments_souterrains: % rows written', v;
END;$procedure$;

