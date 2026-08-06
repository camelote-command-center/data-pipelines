-- ============================================================================
-- Standing drift detector for the re-LLM to consumer sync chain
-- ============================================================================
-- Bug 177de4c5: ref.ge_rdppf_synthese had drifted from source while row counts
-- AND max(updated_at) were identical, so every check we ran was blind to it.
-- The platform-wide audit then found the same defect on four more tables.
--
-- This is the check that would have caught it: per-table content comparison,
-- source vs destination, recorded every run and raised on.
--
-- It is deliberately wired to FAIL THE CRON rather than write to a new alert
-- channel. A failed pg_cron job is already picked up by cron_failed_24h in
-- fn_db_metrics_cycle and surfaces through the existing Telegram tier, and per
-- operations/monitoring every Tier-2 event auto-logs a bugs row. Inventing a
-- second notification path for this would repeat the mistake that got the
-- Telegram bug-report channel retired the day it was built.
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold_ch.sync_drift_log (
  id                bigserial PRIMARY KEY,
  checked_at        timestamptz NOT NULL DEFAULT now(),
  source_rel        text NOT NULL,
  foreign_rel       text NOT NULL,
  common_cols       int,
  source_rows       bigint,
  target_rows       bigint,
  missing_in_target bigint,
  extra_in_target   bigint,
  drifted           bigint,
  error             text
);

CREATE INDEX IF NOT EXISTS sync_drift_log_checked_idx
  ON gold_ch.sync_drift_log (checked_at DESC);
CREATE INDEX IF NOT EXISTS sync_drift_log_bad_idx
  ON gold_ch.sync_drift_log (source_rel, checked_at DESC)
  WHERE drifted > 0 OR missing_in_target > 0 OR extra_in_target > 0 OR error IS NOT NULL;

-- ---------------------------------------------------------------------------
-- The check
-- ---------------------------------------------------------------------------
-- p_raise = true  -> raises on the first problem found, after logging them all.
-- p_raise = false -> logs only. Use this for an ad-hoc look without failing a job.
--
-- globalid and objectid are excluded from the comparison by default: SITG
-- regenerates both on every republication and documents them as unstable
-- identifiers, so including them makes the check fire on every SITG release
-- regardless of whether any real value moved. They are still SYNCED, they are
-- just not evidence of a defect.
-- LOGS ONLY. It does not raise, and it must not: a RAISE aborts the transaction
-- and discards the very sync_drift_log rows the run just wrote, leaving the log
-- empty exactly on the runs that found something. An in-procedure COMMIT cannot
-- rescue that either, because the per-target exception handler puts the
-- procedure in a context where COMMIT is refused.
-- The alarm is therefore a SECOND cron job calling gold_ch.sync_drift_alarm(),
-- which reads the committed log and raises. Two jobs, two transactions: the log
-- survives and the failure still reaches Telegram via cron_failed_24h.
CREATE OR REPLACE PROCEDURE gold_ch.assert_no_sync_drift(p_raise boolean DEFAULT true)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  t   RECORD;
  d   RECORD;
  bad int := 0;
  msg text := '';
BEGIN
  FOR t IN SELECT * FROM gold_ch.sync_drift_targets WHERE enabled ORDER BY source_rel LOOP
    BEGIN
      -- Exclusions are the list justified in the platform audit: SITG
      -- regenerates globalid and objectid on every republication, and id /
      -- created_at / business_key / content_hash are local bookkeeping. Leaving
      -- globalid and objectid in is what made the first rdppf hash report
      -- 74'251 drifted rows instead of the real 106.
      SELECT * INTO d
      FROM gold_ch.sync_drift_check(t.source_rel, t.foreign_rel, t.key_column,
                                    ARRAY['globalid','objectid','id','created_at',
                                          'business_key','content_hash']
                                    || t.exclude_columns);

      INSERT INTO gold_ch.sync_drift_log
        (source_rel, foreign_rel, common_cols, source_rows, target_rows,
         missing_in_target, extra_in_target, drifted)
      VALUES (t.source_rel, t.foreign_rel, d.common_cols, d.source_rows, d.target_rows,
              d.missing_in_target, d.extra_in_target, d.drifted);

      IF d.drifted > 0 OR d.missing_in_target > 0
         OR d.extra_in_target > t.accepted_extra THEN
        bad := bad + 1;
        msg := msg || format('%s: %s drifted, %s missing, %s extra. ',
                             t.source_rel, d.drifted, d.missing_in_target, d.extra_in_target);
      END IF;

    EXCEPTION WHEN OTHERS THEN
      -- A target that cannot be checked is itself a finding: never swallow it.
      INSERT INTO gold_ch.sync_drift_log (source_rel, foreign_rel, error)
      VALUES (t.source_rel, t.foreign_rel, SQLERRM);
      bad := bad + 1;
      msg := msg || format('%s: CHECK FAILED (%s). ', t.source_rel, SQLERRM);
    END;
  END LOOP;

  IF bad > 0 AND p_raise THEN
    RAISE EXCEPTION 'sync drift detected on % target(s): %', bad, msg;
  ELSIF bad > 0 THEN
    RAISE WARNING 'sync drift detected on % target(s): %', bad, msg;
  ELSE
    RAISE NOTICE 'sync drift check clean across % target(s)',
      (SELECT count(*) FROM gold_ch.sync_drift_targets WHERE enabled);
  END IF;
END;
$$;

REVOKE ALL ON PROCEDURE gold_ch.assert_no_sync_drift(boolean) FROM PUBLIC, anon, authenticated;

-- ---------------------------------------------------------------------------
-- Latest state, one row per target
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold_ch.v_sync_drift_latest AS
SELECT DISTINCT ON (source_rel)
  source_rel, foreign_rel, checked_at, source_rows, target_rows,
  missing_in_target, extra_in_target, drifted, error,
  (coalesce(drifted,0) = 0 AND coalesce(missing_in_target,0) = 0
   AND coalesce(extra_in_target,0) = 0 AND error IS NULL) AS clean
FROM gold_ch.sync_drift_log
ORDER BY source_rel, checked_at DESC;

-- ---------------------------------------------------------------------------
-- Schedule: daily, after the 05:00 relm_sync_daily_all cron has finished.
-- ---------------------------------------------------------------------------
SELECT cron.unschedule('sync-drift-check-daily')
WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'sync-drift-check-daily');

-- RAISING. The drift this detector exists to catch is repaired: all ten targets
-- report zero drifted rows, and the only remaining differences are destination
-- rows with no source row, recorded per target in accepted_extra. A failed
-- pg_cron job is picked up by cron_failed_24h and reaches Telegram through the
-- existing tier, auto-logging a bug.
SELECT cron.schedule(
  'sync-drift-check-daily',
  '30 6 * * *',
  $cron$CALL gold_ch.assert_no_sync_drift(false);$cron$
);

-- ---------------------------------------------------------------------------
-- The alarm: a separate job, so the log written above is already committed.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION gold_ch.sync_drift_alarm()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'gold_ch', 'public', 'pg_catalog'
AS $fn$
DECLARE
  v_bad int;
  v_msg text;
BEGIN
  SELECT count(*), coalesce(string_agg(
           format('%s: %s drifted, %s missing, %s extra%s',
                  source_rel, drifted, missing_in_target, extra_in_target,
                  coalesce(' ('||error||')','')), '; '), '')
    INTO v_bad, v_msg
  FROM gold_ch.v_sync_drift_latest
  WHERE NOT clean
    AND checked_at > now() - interval '2 days';   -- a stale log is itself a failure

  IF (SELECT max(checked_at) FROM gold_ch.sync_drift_log) < now() - interval '2 days' THEN
    RAISE EXCEPTION 'sync drift check has not run in over 2 days';
  END IF;

  IF v_bad > 0 THEN
    RAISE EXCEPTION 'sync drift on % target(s): %', v_bad, v_msg;
  END IF;
END $fn$;

REVOKE ALL ON FUNCTION gold_ch.sync_drift_alarm() FROM PUBLIC, anon, authenticated;

SELECT cron.unschedule('sync-drift-alarm-daily')
WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'sync-drift-alarm-daily');

SELECT cron.schedule(
  'sync-drift-alarm-daily',
  '45 6 * * *',
  $cron$SELECT gold_ch.sync_drift_alarm();$cron$
);
