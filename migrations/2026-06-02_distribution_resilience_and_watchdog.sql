-- ============================================================================
-- Distribution resilience + freshness watchdog (re-llm + lamap_db + camelote)
-- 2026-06-02. Fixes the silent re-llm->lamap_db distribution stall (bug 667d75b2
-- + Brief-E cron composition). Incident c2fcc6d7. Black Box: sad_pipeline_freshness
-- v2 5c89baa4, alert_automation_self_healing v2 0f001184,
-- pipeline_distribution_freshness_watchdog v1 238e6b04.
-- NOTE: orchestration fns public.pipeline_watchdog_run / gold_ch.pipeline_watchdog_run
--   embed lamap+camelote service-role JWTs (cron-404 pattern) — REDACTED here;
--   recover live via pg_get_functiondef. Cron layout: re-llm 21@04:30 refresh,
--   51@05:00 sync, 52@06:00 producer-watchdog; lamap 655@07:15 consumer-watchdog.
-- ============================================================================

-- ===== re-llm: per-matview isolation (refresh_daily_matviews + failure log) =====
-- 2.2 — per-matview isolation: failure log + no-RAISE refresh_daily_matviews
CREATE TABLE IF NOT EXISTS gold_ch.refresh_failure_log (
  id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  mv_name   text NOT NULL,
  failed_at timestamptz NOT NULL DEFAULT now(),
  sqlstate  text,
  sqlerrm   text,
  context   text
);
CREATE INDEX IF NOT EXISTS idx_refresh_failure_log_failed_at ON gold_ch.refresh_failure_log(failed_at DESC);

DROP FUNCTION IF EXISTS gold_ch.refresh_daily_matviews();
CREATE FUNCTION gold_ch.refresh_daily_matviews()
 RETURNS TABLE(succeeded int, failed int, failed_names text[])
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'gold_ch', 'silver_ch', 'public'
 SET statement_timeout TO '3600000'
AS $function$
DECLARE
  v_start      timestamptz;
  mv           text;
  v_ok         int := 0;
  v_fail       int := 0;
  v_failed     text[] := ARRAY[]::text[];
  v_state      text;
  v_msg        text;
  matviews CONSTANT text[] := ARRAY[
    'silver_ch.event_sad',
    'silver_ch.event_transactions',
    'silver_ch.event_transaction_parties',
    'silver_ch.link_plot_listings',
    'silver_ch.link_plot_sad',
    'silver_ch.link_plot_transactions',
    'silver_ch.listing_active',
    'silver_ch.listing_group_best_url',
    'gold_ch.core_listings',
    'gold_ch.core_sad',
    'gold_ch.core_transactions'
  ];
BEGIN
  FOREACH mv IN ARRAY matviews LOOP
    v_start := clock_timestamp();
    BEGIN
      EXECUTE format('REFRESH MATERIALIZED VIEW %s', mv);
      v_ok := v_ok + 1;
      RAISE NOTICE 'refresh_daily_matviews: % OK in %ms', mv,
        (extract(epoch FROM clock_timestamp()-v_start)*1000)::int;
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT;
      v_fail := v_fail + 1;
      v_failed := v_failed || mv;
      -- runs in the OUTER txn after the subtxn rollback, so it persists
      INSERT INTO gold_ch.refresh_failure_log(mv_name, sqlstate, sqlerrm, context)
      VALUES (mv, v_state, v_msg, 'refresh_daily_matviews');
      RAISE WARNING 'refresh_daily_matviews: % FAILED [%]: %', mv, v_state, v_msg;
    END;
  END LOOP;
  -- NEVER RAISE: one poison matview must not freeze the other ten or abort the cron.
  -- Failures are surfaced via gold_ch.refresh_failure_log + the pipeline watchdog.
  succeeded := v_ok; failed := v_fail; failed_names := v_failed;
  RETURN NEXT;
END;
$function$;

-- ===== re-llm: per-pipe isolation (run_sync_proc + sync_failure_log) =====
-- 2.3 — per-pipe isolation: sync failure log + no-RAISE run_sync_proc
CREATE TABLE IF NOT EXISTS gold_ch.sync_failure_log (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  source_table text NOT NULL,
  target_db   text NOT NULL,
  sync_mode   text,
  failed_at   timestamptz NOT NULL DEFAULT now(),
  detail      text,
  context     text
);
CREATE INDEX IF NOT EXISTS idx_sync_failure_log_failed_at ON gold_ch.sync_failure_log(failed_at DESC);

CREATE OR REPLACE PROCEDURE gold_ch.run_sync_proc(IN p_frequency text)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  rec           RECORD;
  v_rows        INTEGER;
  v_full_source TEXT;
  v_failures    INTEGER := 0;
  v_total       INTEGER := 0;
  v_failed_list TEXT := '';
  targets CONSTANT TEXT[][] := ARRAY[
    ['lamap_crm_server', 'crm_foreign',      'lamap-crm'],
    ['lamap_lbi_server', 'lbi_foreign',      'lamap-lbi'],
    ['lamap_db_server',  'lamap_db_foreign', 'lamap_db']
  ];
  t TEXT[];
BEGIN
  PERFORM set_config('statement_timeout', '3600000', false);

  FOR rec IN
    SELECT r.source_schema, r.source_table, r.target_table, r.pk_column,
           r.sync_mode, r.delta_column
    FROM gold_ch.sync_registry r
    WHERE r.frequency = p_frequency AND r.enabled = true
    ORDER BY r.source_table
  LOOP
    FOREACH t SLICE 1 IN ARRAY targets LOOP
      v_full_source := rec.source_schema || '.' || rec.source_table;
      v_total := v_total + 1;

      IF rec.sync_mode = 'delta' THEN
        v_rows := 0;
        CALL gold_ch.sync_delta(rec.source_schema, rec.source_table, rec.target_table,
                                rec.pk_column, t[2], t[3], rec.delta_column, v_rows);
        PERFORM set_config('statement_timeout', '3600000', false);
        IF v_rows = -1 THEN
          v_failures := v_failures + 1;
          v_failed_list := v_failed_list || v_full_source || '->' || t[3] || '(delta -1); ';
          INSERT INTO gold_ch.sync_failure_log(source_table,target_db,sync_mode,detail,context)
          VALUES (v_full_source, t[3], 'delta', 'sync_delta returned -1 (see gold_ch.sync_log)', 'run_sync_proc:'||p_frequency);
        ELSE
          RAISE NOTICE 'run_sync_proc(%): %->% delta upserted % rows', p_frequency, v_full_source, t[3], v_rows;
        END IF;
      ELSE
        v_rows := 0;
        CALL gold_ch.sync_full_refresh(rec.source_schema, rec.source_table, rec.target_table,
                                       t[1], t[2], t[3], NULL::text[], v_rows);
        COMMIT;
        PERFORM set_config('statement_timeout', '3600000', false);
        IF v_rows = -1 THEN
          v_failures := v_failures + 1;
          v_failed_list := v_failed_list || v_full_source || '->' || t[3] || '(full_refresh -1); ';
          INSERT INTO gold_ch.sync_failure_log(source_table,target_db,sync_mode,detail,context)
          VALUES (v_full_source, t[3], 'full_refresh', 'sync_full_refresh returned -1 (see gold_ch.sync_log)', 'run_sync_proc:'||p_frequency);
        ELSE
          RAISE NOTICE 'run_sync_proc(%): %->% full_refresh did % rows', p_frequency, v_full_source, t[3], v_rows;
        END IF;
      END IF;
    END LOOP;
  END LOOP;

  PERFORM set_config('statement_timeout', '600000', false);

  -- NEVER RAISE: a failed pipe must not abort the others. Failures are logged to
  -- gold_ch.sync_failure_log + gold_ch.sync_log and surfaced by the pipeline watchdog.
  IF v_failures > 0 THEN
    RAISE WARNING 'run_sync_proc(%): % of % pipe-targets failed (logged): %', p_frequency, v_failures, v_total, v_failed_list;
  ELSE
    RAISE NOTICE 'run_sync_proc(%): all % pipe-targets ok', p_frequency, v_total;
  END IF;
END;
$procedure$;

-- ===== re-llm: sync_delta staging/upsert timeout raised 5m->30m / 30m->60m =====
-- (literal edits to gold_ch.sync_delta; see pg_get_functiondef for full body)

-- ===== re-llm: producer watchdog =====
-- 3.2 producer-side watchdog: source has rows the sync hasn't picked up for >24h,
-- plus any refresh/sync failure-log entries in the last 36h.
CREATE OR REPLACE FUNCTION gold_ch.pipeline_source_freshness_check()
RETURNS TABLE(pipe_name text, source_max timestamptz, last_successful_sync_at timestamptz,
              unsynced_lag interval, is_stale boolean, recent_failures int, notes text)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = gold_ch, public, pg_catalog
AS $fn$
DECLARE
  r RECORD;
  v_src_max timestamptz;
  v_last    timestamptz;
  v_fail    int;
  pipes CONSTANT text[][] := ARRAY[
    ['listings',     'gold_ch.v_listings_full',     'last_seen_at'],
    ['transactions', 'gold_ch.v_transactions_full', 'updated_at'],
    ['sad',          'gold_ch.v_sad_full',          'updated_at']
  ];
  p text[];
BEGIN
  FOREACH p SLICE 1 IN ARRAY pipes LOOP
    EXECUTE format('SELECT max(%I) FROM %s', p[3], p[2]) INTO v_src_max;
    SELECT max(finished_at) INTO v_last FROM gold_ch.sync_log
      WHERE source_table = p[2] AND target_db='lamap_db' AND status='success';
    SELECT count(*) INTO v_fail FROM gold_ch.sync_failure_log
      WHERE source_table = p[2] AND target_db='lamap_db' AND failed_at > now()-interval '36 hours';
    pipe_name := p[1];
    source_max := v_src_max;
    last_successful_sync_at := v_last;
    unsynced_lag := CASE WHEN v_last IS NULL THEN NULL ELSE v_src_max - v_last END;
    -- stale if source has data the sync hasn't carried for >24h, or never synced, or recent failures
    is_stale := (v_last IS NULL) OR (v_src_max > v_last + interval '24 hours') OR (v_fail > 0);
    recent_failures := v_fail;
    notes := format('source=%s delta_col=%s; stale if unsynced>24h or sync_failure_log>0', p[2], p[3]);
    RETURN NEXT;
  END LOOP;
END;
$fn$;
GRANT EXECUTE ON FUNCTION gold_ch.pipeline_source_freshness_check() TO anon, authenticated, service_role;

-- ===== lamap_db: consumer watchdog + trend log =====
-- 3.4 trend snapshot log
CREATE TABLE IF NOT EXISTS platform.pipeline_freshness_log (
  snapshot_date         date NOT NULL DEFAULT current_date,
  pipe_name             text NOT NULL,
  last_event_date       date,
  last_upstream_sync_at timestamptz,
  actual_lag            interval,
  is_stale              boolean,
  PRIMARY KEY (snapshot_date, pipe_name)
);
CREATE INDEX IF NOT EXISTS idx_pipeline_freshness_log_pipe ON platform.pipeline_freshness_log(pipe_name, snapshot_date DESC);

-- 3.1 consumer-side freshness check (keys on ref-side sync/ingestion timestamps, never event date)
CREATE OR REPLACE FUNCTION public.pipeline_freshness_check()
RETURNS TABLE(pipe_name text, last_event_date date, last_upstream_sync_at timestamptz,
              expected_max_lag interval, actual_lag interval, is_stale boolean, notes text)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public, ref, pg_catalog
AS $fn$
  -- listings: daily scrape+sync; key on ref.listings.last_seen_at
  SELECT 'listings'::text,
         max(last_seen_at)::date,
         max(last_seen_at),
         interval '36 hours',
         now() - max(last_seen_at),
         now() - max(last_seen_at) > interval '36 hours',
         'ref.listings.last_seen_at; daily cadence (re-llm refresh 04:30 + sync 05:00 + lamap refresh 06:37)'
  FROM ref.listings
  UNION ALL
  -- transactions: FAO weekly data, but synced daily -> key on ref.transactions.updated_at (sync time)
  SELECT 'transactions'::text,
         max(publication_date),
         max(updated_at),
         interval '48 hours',
         now() - max(updated_at),
         now() - max(updated_at) > interval '48 hours',
         'ref.transactions.updated_at (sync time, advances each daily sync); event publication_date lags ~weekly and is NOT the staleness signal'
  FROM ref.transactions
  UNION ALL
  -- sad: key on ref.sad.updated_at (sync time); date_depot lags ~10d so not used for staleness
  SELECT 'sad'::text,
         max(date_depot),
         max(updated_at),
         interval '48 hours',
         now() - max(updated_at),
         now() - max(updated_at) > interval '48 hours',
         'ref.sad.updated_at (sync time); date_depot lags ~10d and is NOT the staleness signal'
  FROM ref.sad
$fn$;
GRANT EXECUTE ON FUNCTION public.pipeline_freshness_check() TO anon, authenticated, service_role;

-- ===== camelote/pixxels: incident dedup-upsert RPC =====
-- camelote/pixxels: dedup-UPSERT incident into the Bugs dashboard.
-- Dedup key: (project='lamap', open, '[pipeline-watchdog] <pipe>' marker). Increments
-- recurrence_count (occurrence/Day-N) instead of inserting duplicates. Documented in platform.standards.
CREATE OR REPLACE FUNCTION public.log_pipeline_stall(p_pipe text, p_lag text, p_detail text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_catalog AS $$
DECLARE v_id uuid; v_occ int;
BEGIN
  SELECT id, recurrence_count+1 INTO v_id, v_occ FROM bugs
  WHERE project='lamap' AND status='open' AND description LIKE '[severity=high][pipeline-watchdog] '||p_pipe||':%'
  ORDER BY created_at LIMIT 1;
  IF v_id IS NOT NULL THEN
    UPDATE bugs SET recurrence_count=v_occ, updated_at=now(),
      fix_applied='[watchdog] occurrence '||v_occ||', last_seen '||now()::text||'; lag '||p_lag
    WHERE id=v_id;
    RETURN jsonb_build_object('bug_id',v_id,'occurrence',v_occ,'action','updated');
  END IF;
  INSERT INTO bugs (project, description, source, root_cause, status, recurrence_count, reported_at)
  VALUES ('lamap','[severity=high][pipeline-watchdog] '||p_pipe||': '||p_detail,'pipeline-watchdog',
          'upstream re-llm -> ref.* distribution stall for '||p_pipe||' (lag '||p_lag||')','open',1,CURRENT_DATE)
  RETURNING id INTO v_id;
  RETURN jsonb_build_object('bug_id',v_id,'occurrence',1,'action','inserted');
END; $$;
GRANT EXECUTE ON FUNCTION public.log_pipeline_stall(text,text,text) TO service_role;

-- ===== lamap_db: platform.standards documentation =====
INSERT INTO platform.standards (category, rule_key, rule_text, correct_call, incorrect_call, applies_to, severity) VALUES
('monitoring','pipeline_distribution_freshness_watchdog',
 'Daily watchdog defends the re-llm->lamap_db distribution. lamap consumer check public.pipeline_freshness_check() (cron 655 @07:15) + re-llm producer check gold_ch.pipeline_source_freshness_check() (cron 52 @06:00). On is_stale it fires BOTH channels via gold_ch/public.pipeline_watchdog_run(): (A) email via Edge Function pipeline-watchdog-alert (thin Resend adapter; recipient HARDCODED info@lamap.ch, NEVER payload-driven; reuses project RESEND_API_KEY + sender "LaMap Alertes <alertes@lamap.ch>"; payload {stale_pipes,summary,environment,triggered_at,is_forced_test}); (B) incident on camelote via rpc public.log_pipeline_stall(). Thresholds key on SYNC/INGESTION time (never event date): listings max(ref.listings.last_seen_at)>36h; transactions max(ref.transactions.updated_at)>48h; sad max(ref.sad.updated_at)>48h. Trend snapshot -> platform.pipeline_freshness_log. Forced-break test: SELECT pipeline_watchdog_run(true) (sends [TEST] email).',
 'SELECT public.pipeline_watchdog_run();  -- both channels, daily snapshot',
 'Do NOT add a payload "to:" field to pipeline-watchdog-alert (open-relay shape); do NOT create a new Resend key/vault secret; do NOT key thresholds on event-date (publication_date/date_depot lag is real-world noise, not a failure signal).',
 ARRAY['re-llm','lamap_db','distribution','monitoring'],'must'),
('monitoring','pipeline_stall_incident_dedup_key',
 'log_pipeline_stall (camelote/pixxels) UPSERTs into bugs deduped on (project=lamap, status=open, description LIKE [severity=high][pipeline-watchdog] <pipe>:%). Existing open row -> recurrence_count+1 (occurrence/Day-N) + updated_at + last_seen; else insert. Prevents day-3 dashboard noise on a persistent stall.',
 'SELECT public.log_pipeline_stall(p_pipe,p_lag,p_detail);',
 'Do NOT INSERT a new bug row per day for the same ongoing stall.',
 ARRAY['camelote_data','monitoring'],'must'),
('tech','relm_distribution_sole_statement_crons',
 'The re-llm refresh and sync MUST be SEPARATE sole-statement crons. Composing SELECT refresh_daily_matviews(); CALL run_sync_proc() in one command reintroduces "invalid transaction termination" (the CALL COMMIT inside an implicit txn) — the 2026-06 stall. Layout: job 21 refresh-daily-matviews "30 4 * * *" (SELECT-only); job 51 relm_sync_daily_all "0 5 * * *" (CALL run_sync_proc(daily), sole statement); jobs 47/48/49 redundant Fri/Mon sync; job 52 relm_pipeline_watchdog "0 6 * * *". Ordering: re-llm refresh 04:30 -> sync 05:00 -> lamap consumer refresh 06:37 -> watchdog 07:15.',
 'cron.schedule(name, sched, ''CALL gold_ch.run_sync_proc(''''daily'''');'');  -- sole statement',
 'SET ...; SELECT gold_ch.refresh_daily_matviews(); CALL gold_ch.run_sync_proc(''daily''); -- COMMIT fails (invalid transaction termination)',
 ARRAY['re-llm','distribution','crons'],'blocker');

-- ===== Orchestration fns (REDACTED keys) — public/gold_ch.pipeline_watchdog_run =====
-- Bodies embed service JWTs; recover live via pg_get_functiondef. Shape: snapshot
-- into platform.pipeline_freshness_log, then on is_stale fire net.http_post to the
-- pipeline-watchdog-alert edge fn (email) + camelote rpc log_pipeline_stall (incident).
