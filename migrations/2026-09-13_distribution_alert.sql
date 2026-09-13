-- ============================================================================
-- 2026-09-13 — Distribution alert: a frozen consumer pipe can no longer stay silent
-- ============================================================================
-- Why: foncier (5 weeks) and lamap-lbi link_entity_transactions (5 weeks) were frozen while
-- every cron job reported success. gold_ch.sync_full_refresh logged each REFUSED run to
-- gold_ch.sync_log / gold_ch.sync_failure_log, but nothing read those logs and pushed.
-- (gold_ch.sync_drift_alarm covers content drift on 3 hand-registered targets and raises
-- inside cron, where nobody looks.)
--
-- What it checks, every day at 07:00 UTC (after daily 05:00, foncier 05:02, drift 06:45):
--   failing  — every (source, consumer) pair whose LATEST attempt in 35 days failed, from
--              sync_log + sync_failure_log, plus 'running' rows older than 6h (stuck).
--   stale    — every enabled pair (sync_registry x lamap_db/lamap-lbi/lamap-crm, and
--              foncier_sync_registry x foncier) with no success inside its frequency window
--              (daily 36h, weekly 8d, monthly 32d, quarterly 95d) — catches a runner that
--              stopped firing, which leaves no failure row at all.
--   silent   — no sync_log activity at all for 36h (the distributor itself is down).
-- Pairs declared in gold_ch.sync_target_scope (distribute=false, with evidence) are skipped.
--
-- Output: one Command Center item per consumer, dedup key 'distribution:<target_db>'
-- (prefix not owned by fn_ops_collect, so it is never swept). severity=critical when a pipe
-- that used to deliver is now frozen, warn when it never delivered. When a consumer comes
-- back clean the item is auto-resolved (fn_ops_resolve) and re-opens if it breaks again.
-- Every run, clean ones included, is recorded in gold_ch.distribution_alert_runs.
--
-- ROLLBACK: SELECT cron.unschedule('distribution-alert-daily'); function + run table additive.
-- ============================================================================
BEGIN;

CREATE TABLE IF NOT EXISTS gold_ch.distribution_alert_runs (
  id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  ran_at        timestamptz NOT NULL DEFAULT now(),
  status        text        NOT NULL CHECK (status IN ('clean','alerted')),
  findings      jsonb       NOT NULL,
  open_keys     text[]      NOT NULL DEFAULT '{}',
  resolved_keys text[]      NOT NULL DEFAULT '{}',
  pushed        boolean     NOT NULL,
  duration_ms   integer
);

CREATE OR REPLACE FUNCTION gold_ch.distribution_alert_run(p_push boolean DEFAULT true)
RETURNS jsonb
LANGUAGE plpgsql
SET search_path TO 'gold_ch', 'public', 'pg_catalog'
AS $fn$
DECLARE
  v_t0        timestamptz := clock_timestamp();
  v_cam_key   text := (SELECT decrypted_secret FROM vault.decrypted_secrets WHERE name = 'sr_pixxels_data');
  v_rellm     uuid := 'c1fd061c-8e0f-4c82-97fc-24fce67a1571';
  v_findings  jsonb := '[]'::jsonb;
  v_open      text[] := '{}';
  v_prev_open text[];
  v_resolved  text[] := '{}';
  v_last_act  timestamptz;
  f           jsonb;
  k           text;
  v_req       bigint;
BEGIN
  IF p_push AND v_cam_key IS NULL THEN
    RAISE EXCEPTION 'distribution_alert_run: vault secret sr_pixxels_data missing — cannot push';
  END IF;

  WITH ev AS (
    SELECT source_table, target_db, started_at AS at,
           CASE WHEN status = 'running' AND started_at < now() - interval '6 hours' THEN 'stuck' ELSE status END AS status
    FROM gold_ch.sync_log
    WHERE started_at > now() - interval '35 days' AND status IN ('success','failed','running')
    UNION ALL
    SELECT source_table, target_db, failed_at, 'failed'
    FROM gold_ch.sync_failure_log
    WHERE failed_at > now() - interval '35 days'
  ), latest AS (
    SELECT DISTINCT ON (source_table, target_db) source_table, target_db, status, at
    FROM ev WHERE status <> 'running'
    ORDER BY source_table, target_db, at DESC
  ), pairs AS (
    SELECT r.source_schema || '.' || r.source_table AS source_table, t.target_db, r.frequency
    FROM gold_ch.sync_registry r
    CROSS JOIN (VALUES ('lamap_db'), ('lamap-lbi'), ('lamap-crm')) t(target_db)
    WHERE r.enabled
    UNION
    SELECT f.source_schema || '.' || f.source_table, 'foncier', f.frequency
    FROM gold_ch.foncier_sync_registry f WHERE f.enabled
    UNION
    -- pairs outside the registries (bespoke procedures) that failed recently
    SELECT l.source_table, l.target_db, 'daily' FROM latest l WHERE l.status IN ('failed','stuck')
  ), scoped AS (
    SELECT DISTINCT ON (p.source_table, p.target_db) p.*
    FROM pairs p
    WHERE NOT EXISTS (SELECT 1 FROM gold_ch.sync_target_scope sc
                      WHERE sc.source_schema || '.' || sc.source_table = p.source_table
                        AND sc.target_db = p.target_db AND NOT sc.distribute)
    ORDER BY p.source_table, p.target_db,
             array_position(ARRAY['daily','weekly','monthly','quarterly'], p.frequency) DESC
  ), st AS (
    SELECT s.source_table, s.target_db, s.frequency, l.status AS latest_status, l.at AS latest_at,
           ok.last_ok,
           (SELECT e.error_message FROM gold_ch.sync_log e
             WHERE e.source_table = s.source_table AND e.target_db = s.target_db AND e.status = 'failed'
             ORDER BY e.started_at DESC LIMIT 1) AS last_error,
           CASE s.frequency WHEN 'daily' THEN interval '36 hours' WHEN 'weekly' THEN interval '8 days'
                            WHEN 'monthly' THEN interval '32 days' ELSE interval '95 days' END AS win
    FROM scoped s
    LEFT JOIN latest l USING (source_table, target_db)
    LEFT JOIN LATERAL (SELECT max(started_at) AS last_ok FROM gold_ch.sync_log x
                       WHERE x.source_table = s.source_table AND x.target_db = s.target_db
                         AND x.status = 'success') ok ON true
  ), bad AS (
    SELECT st.*,
           CASE WHEN latest_status IN ('failed','stuck') THEN latest_status ELSE 'stale' END AS kind
    FROM st
    WHERE latest_status IN ('failed','stuck')
       OR last_ok IS NULL OR last_ok < now() - win
  )
  SELECT coalesce(jsonb_agg(g ORDER BY g->>'target_db'), '[]'::jsonb) INTO v_findings
  FROM (
    SELECT jsonb_build_object(
      'target_db', target_db,
      'severity', CASE WHEN bool_or(last_ok IS NOT NULL) THEN 'critical' ELSE 'warn' END,
      'failing', count(*) FILTER (WHERE kind <> 'stale'),
      'stale', count(*) FILTER (WHERE kind = 'stale'),
      'frozen', count(*) FILTER (WHERE last_ok IS NOT NULL),
      'oldest_last_ok', min(last_ok)::date,
      'pipes', jsonb_agg(jsonb_build_object(
                 'source', source_table, 'kind', kind, 'frequency', frequency,
                 'last_ok', last_ok::date, 'last_attempt', latest_at,
                 'error', left(regexp_replace(coalesce(last_error, 'no sync_log trace'), '\s+', ' ', 'g'), 160))
               ORDER BY (last_ok IS NULL), last_ok, source_table)
    ) AS g, target_db
    FROM bad GROUP BY target_db
  ) q;

  SELECT max(started_at) INTO v_last_act FROM gold_ch.sync_log;
  IF v_last_act IS NULL OR v_last_act < now() - interval '36 hours' THEN
    v_findings := v_findings || jsonb_build_object(
      'target_db', 'distributor', 'severity', 'critical', 'failing', 0, 'stale', 0, 'frozen', 0,
      'pipes', '[]'::jsonb, 'note', format('no gold_ch.sync_log activity since %s', coalesce(v_last_act::text, 'ever')));
  END IF;

  SELECT coalesce(array_agg('distribution:' || (x->>'target_db')), '{}') INTO v_open
  FROM jsonb_array_elements(v_findings) x;

  SELECT open_keys INTO v_prev_open
  FROM gold_ch.distribution_alert_runs WHERE pushed ORDER BY ran_at DESC LIMIT 1;
  v_resolved := ARRAY(SELECT unnest(coalesce(v_prev_open, '{}')) EXCEPT SELECT unnest(v_open));

  IF p_push THEN
    FOR f IN SELECT * FROM jsonb_array_elements(v_findings) LOOP
      SELECT net.http_post(
        url := 'https://dxugbpeacnorjunpljih.supabase.co/rest/v1/rpc/fn_ops_capture',
        headers := jsonb_build_object('apikey', v_cam_key, 'Authorization', 'Bearer ' || v_cam_key,
                                      'Content-Type', 'application/json'),
        body := jsonb_build_object(
          'p_dedup_key', 'distribution:' || (f->>'target_db'),
          'p_source', 'distribution_alert',
          'p_title', CASE WHEN f->>'target_db' = 'distributor' THEN 'Warehouse distributor silent: ' || (f->>'note')
                     ELSE format('%s: %s distribution pipe(s) failing, %s stale — %s frozen, oldest last delivery %s',
                                 f->>'target_db', f->>'failing', f->>'stale', f->>'frozen',
                                 coalesce(f->>'oldest_last_ok', 'never')) END,
          'p_detail', (SELECT string_agg(format('%s [%s, last ok %s]: %s', p->>'source', p->>'kind',
                                                coalesce(p->>'last_ok', 'never'), p->>'error'), E'\n')
                       FROM (SELECT p FROM jsonb_array_elements(f->'pipes') p LIMIT 25) z)
                      || E'\n\nRe-LLM: gold_ch.distribution_alert_runs (full list), gold_ch.sync_log. '
                      || 'A pair that should never be distributed is declared in gold_ch.sync_target_scope with evidence.',
          'p_severity', f->>'severity',
          'p_startup_id', v_rellm, 'p_target', f->>'target_db', 'p_metric', 'distribution')
      ) INTO v_req;
    END LOOP;
    FOREACH k IN ARRAY v_resolved LOOP
      SELECT net.http_post(
        url := 'https://dxugbpeacnorjunpljih.supabase.co/rest/v1/rpc/fn_ops_resolve',
        headers := jsonb_build_object('apikey', v_cam_key, 'Authorization', 'Bearer ' || v_cam_key,
                                      'Content-Type', 'application/json'),
        body := jsonb_build_object('p_dedup_key', k)
      ) INTO v_req;
    END LOOP;
  END IF;

  INSERT INTO gold_ch.distribution_alert_runs (status, findings, open_keys, resolved_keys, pushed, duration_ms)
  VALUES (CASE WHEN jsonb_array_length(v_findings) > 0 THEN 'alerted' ELSE 'clean' END,
          v_findings, v_open, CASE WHEN p_push THEN v_resolved ELSE '{}' END, p_push,
          (extract(epoch FROM clock_timestamp() - v_t0) * 1000)::int);

  RETURN jsonb_build_object('status', CASE WHEN jsonb_array_length(v_findings) > 0 THEN 'alerted' ELSE 'clean' END,
                            'open', v_open, 'resolved', CASE WHEN p_push THEN v_resolved ELSE '{}' END,
                            'pushed', p_push, 'findings', v_findings);
END $fn$;

COMMENT ON FUNCTION gold_ch.distribution_alert_run(boolean) IS
  'Daily: every warehouse->consumer pipe (lamap_db, lamap-lbi, lamap-crm, foncier) whose latest attempt failed, '
  'is stuck, or has not delivered inside its frequency window. One Command Center item per consumer '
  '(distribution:<target_db>), auto-resolved when clean. Out-of-scope pairs: gold_ch.sync_target_scope.';

COMMIT;

-- Schedule (applied separately):
--   SELECT cron.schedule('distribution-alert-daily', '0 7 * * *', 'SELECT gold_ch.distribution_alert_run();');
