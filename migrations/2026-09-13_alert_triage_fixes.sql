-- ============================================================================
-- 2026-09-13 — Fixes from the Telegram alert triage (each verified REAL before fixing)
-- ============================================================================
-- Triage classified every alert as REAL / ALREADY-FIXED / BY-DESIGN / OBSOLETE with evidence.
-- Only the REAL ones are changed here. Backups: lamap_db backup.fn_defs_20260913 (*@pre-20260913),
-- scratch copies of foncier has_active_sub. NO data deleted anywhere.
-- ============================================================================

-- ---------------------------------------------------------------- foncier-geneve
-- REAL: public.has_active_sub(uid) was EXECUTE-able by anon and answered for ANY uid (subscription
-- enumeration). Its only callers are 7 sub_gate RLS policies (role authenticated) passing auth.uid().
-- Verified after: subscriber -> true for self, false for another id; subscriber reads a gated table,
-- a non-subscriber reads 0 rows; anon -> permission denied.
CREATE OR REPLACE FUNCTION public.has_active_sub(uid uuid)
 RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER SET search_path TO 'public', 'pg_temp'
AS $function$
  SELECT (uid = (SELECT auth.uid()) OR (SELECT auth.role()) = 'service_role')
     AND EXISTS (
       SELECT 1 FROM public.subscribers s
       WHERE s.user_id = uid AND s.status = 'active' AND s.current_period_end > now()
     );
$function$;
REVOKE EXECUTE ON FUNCTION public.has_active_sub(uuid) FROM anon, PUBLIC;
-- ROLLBACK: previous body = the same without the first predicate; GRANT EXECUTE ... TO anon.

-- ---------------------------------------------------------------- lamap-db (security)
-- REAL: same enumeration in public.has_valid_subscription (no policy/frontend caller) + the 5 functions the
-- scanner flagged run SECURITY DEFINER with the caller's search_path. The set path equals anon/authenticated's
-- effective path (public, silver) plus extensions and pg_temp last, so resolution is unchanged.
-- NOT done: the other ~160 SECURITY DEFINER functions without search_path on lamap-db (needs per-function review).
-- (has_valid_subscription body: early RETURN false unless $1 = auth.uid() or service_role)
REVOKE EXECUTE ON FUNCTION public.has_valid_subscription(uuid) FROM anon, PUBLIC;
ALTER FUNCTION public.has_valid_subscription(uuid)       SET search_path = public, silver, extensions, pg_temp;
ALTER FUNCTION public.get_street_geometry(text,text)     SET search_path = public, silver, extensions, pg_temp;
ALTER FUNCTION public.increment_campaign_views(uuid)     SET search_path = public, silver, extensions, pg_temp;
ALTER FUNCTION public.increment_campaign_conversions(uuid) SET search_path = public, silver, extensions, pg_temp;
ALTER FUNCTION public.create_trial_subscription()        SET search_path = public, silver, extensions, pg_temp;

-- ---------------------------------------------------------------- pixxels-data
-- REAL: scratch table public._tg (32 rows of supplier/category data) had no RLS, anon SELECT and
-- authenticated full write. No reader anywhere.
ALTER TABLE public._tg ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public._tg FROM anon, authenticated;
-- CORRECTED the same day: dataset 204 was first deprecated as "NocoDB stopped", which was wrong — the feed was
-- MOVED to Gary's rousseau_5 (re-LLM crons 111 transactions / 112 listings, houses+apartments from immobilier).
-- Row kept active, renamed, and cron 112 now stamps it via rpc/update_dataset_last_acquired when 111 also succeeded.
UPDATE datasets SET status = 'active', name = 'Gary feed to rousseau_5 (transactions + listings; ex-NocoDB nightly sync)'
 WHERE code = 'relm_nocodb_sync_nightly';
-- REAL: Pixxels Communities collector timed out (cc_collect_metrics measured 8-13 s vs timeout 5 s;
-- cron.job_run_details 85k rows / 227 MB, no index, owned by supabase_admin). Timeout raised; log
-- retention NOT applied (would delete cron history — needs founder approval).
UPDATE db_health_targets SET timeout_ms = 30000 WHERE label = 'Pixxels Communities (db)';

-- ---------------------------------------------------------------- studio
-- REAL (setup never finished): metrics enabled 2026-09-06 but cc_collect_metrics never installed (44/44 PGRST202).
-- Installed: schema monitoring + monitoring.config (RLS on, no anon/authenticated) with metrics_token copied from
-- pixxels-data vault 'metrics_dispatch_token' (not written here), and public.cc_collect_metrics(text, text[])
-- identical to pixxels-communities, EXECUTE to service_role only.

-- ---------------------------------------------------------------- lamap-db (pipelines)
-- ALREADY-FIXED but never closed: freshness:sad (one-day breach 2026-09-01) and freshness:transactions
-- (2026-09-11). public.pipeline_watchdog_run now POSTs fn_ops_resolve('freshness:<pipe>') for every healthy pipe.
-- BY-DESIGN noise: transactions event threshold 8d fired every Friday 07:15 before the Friday 20:00 import
-- -> 12d (one weekly cycle + the Monday retries). SAD stays 14d.
-- REAL: 611/615/521/639 queued behind 607 (06:30, 41-57 min), all started at its end and hit the 15-min DB
-- statement_timeout every day (inventory_counts stale since 2026-08-17). -> own 3600s timeout, staggered after 607.
SELECT cron.alter_job(611, schedule := '45 7 * * *', command := 'SET statement_timeout TO ''3600s''; SELECT public.refresh_mv_entity_derived();');
SELECT cron.alter_job(615, schedule := '0 8 * * *',  command := 'SET statement_timeout TO ''3600s''; SELECT public.refresh_mv_plots_default_count_cache();');
SELECT cron.alter_job(521, schedule := '10 8 * * *', command := 'SET statement_timeout TO ''3600s''; SELECT refresh_inventory_counts();');
SELECT cron.alter_job(639, schedule := '20 8 * * *', command := 'SET statement_timeout TO ''3600s''; SELECT * FROM public.assert_plot_geometry_coverage();');
-- REAL: 522 weekly died at exactly its own 1800s limit twice.
SELECT cron.alter_job(522, command := 'SET statement_timeout = ''3600s''; SELECT public.refresh_unit_owner_mapping();');
-- ROLLBACK: previous schedules 35 6 / 50 6 / 5 7 / 0 7 * * *, commands without the SET.

-- ---------------------------------------------------------------- lamap-lbi
-- REAL (new today): migration 20260913055634 created mv_broker_observed_v2_uq on expressions (COALESCE),
-- which REFRESH ... CONCURRENTLY cannot use -> job 26 refresh-lbi-annonces failed, none of its 5 views refreshed.
-- Same uniqueness on plain columns with NULLS NOT DISTINCT (created before the old one was dropped).
CREATE UNIQUE INDEX CONCURRENTLY mv_broker_observed_v2_uq_cols ON lbi_app.mv_broker_observed_v2
  (agency_name, scope, canton, commune_canonical_bfs, offer_scope) NULLS NOT DISTINCT;
DROP INDEX CONCURRENTLY lbi_app.mv_broker_observed_v2_uq;
ALTER INDEX lbi_app.mv_broker_observed_v2_uq_cols RENAME TO mv_broker_observed_v2_uq;

-- ---------------------------------------------------------------- re-LLM
-- REAL: vd_batiment_rcb / vd_zone_affectation / vd_degre_sensibilite_bruit cannot finish as one process on
-- GitHub (1 feature per ArcGIS request) -> sharded jobs in .github/workflows/vd_enrichment.yml.
-- Run-log rows left 'running' by killed processes are closed (not deleted):
UPDATE bronze_ch.vd_enrichment_runs SET status = 'failed', finished_at = coalesce(finished_at, now()),
       error_message = coalesce(error_message, 'process killed (job timeout / superseded); closed 2026-09-13')
 WHERE status = 'running' AND started_at < now() - interval '1 day';
