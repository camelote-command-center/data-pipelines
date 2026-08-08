-- ═══════════════════════════════════════════════════════════════════════════
-- Liveness check for quarterly ref.* tables, on the existing alarm path
--
-- WHY
--   The forest chain fires on 1 January 2027. Between now and then a scheduling
--   defect is invisible: the drift detector compares gold against ref and both
--   would simply stay unchanged, which reads as "clean". Drift catches a sync
--   that ran and diverged; it cannot catch a pipeline that never ran at all.
--
--   So: assert that quarterly ref tables have been touched within roughly one
--   quarter plus a margin. If the January run does not fire, this goes red in
--   days instead of at the April run.
--
-- NO NEW CHANNEL. This extends gold_ch.sync_drift_alarm(), already scheduled as
-- cron 108 at 06:45 daily and already the thing that raises. One more reason
-- for it to raise, not a second alarm to wire up and forget.
--
-- TABLE-DRIVEN, NOT HARDCODED (platform.standards #269). The target list is a
-- table, and its rows are DERIVED from sync_drift_targets rather than typed out,
-- so a forest layer added later cannot be silently omitted the way four
-- sitg_geo_layers datasets were.
-- ═══════════════════════════════════════════════════════════════════════════

\set ON_ERROR_STOP on

CREATE TABLE IF NOT EXISTS gold_ch.ref_freshness_targets (
  foreign_rel   text PRIMARY KEY,
  ts_column     text NOT NULL,
  max_age_days  int  NOT NULL,
  enabled       boolean NOT NULL DEFAULT true,
  note          text
);

COMMENT ON TABLE gold_ch.ref_freshness_targets IS
  'Tables whose staleness is asserted daily by gold_ch.sync_drift_alarm(). '
  'Catches a pipeline that never ran, which drift detection cannot: if neither '
  'side moves, gold and ref agree and drift reads clean. max_age_days should be '
  'the cadence plus a margin, never the cadence exactly.';

-- Rows derived from the drift targets, with the timestamp column resolved from
-- the catalogue rather than assumed. 100 days = one quarter (92) plus a week of
-- slack for a late or retried run.
INSERT INTO gold_ch.ref_freshness_targets (foreign_rel, ts_column, max_age_days, note)
SELECT t.foreign_rel,
       COALESCE(
         (SELECT c.column_name FROM information_schema.columns c
           WHERE c.table_schema='lamap_db_foreign'
             AND c.table_name = split_part(t.foreign_rel,'.',2)
             AND c.column_name IN ('updated_at','computed_at')
           ORDER BY CASE c.column_name WHEN 'updated_at' THEN 0 ELSE 1 END
           LIMIT 1)),
       100,
       'quarterly forest chain (sitg_forest.yml, cron 0 3 1 1,4,7,10 *)'
FROM gold_ch.sync_drift_targets t
WHERE t.enabled
  AND (t.foreign_rel ~* 'forest')
  AND EXISTS (SELECT 1 FROM information_schema.columns c
               WHERE c.table_schema='lamap_db_foreign'
                 AND c.table_name = split_part(t.foreign_rel,'.',2)
                 AND c.column_name IN ('updated_at','computed_at'))
ON CONFLICT (foreign_rel) DO UPDATE
  SET ts_column = EXCLUDED.ts_column, max_age_days = EXCLUDED.max_age_days;

-- ---------------------------------------------------------------------------
-- Staleness probe. Separate from the alarm so it can be read on demand without
-- raising, which is how you check the board rather than trip it.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION gold_ch.ref_freshness_check()
RETURNS TABLE(foreign_rel text, last_touched timestamptz, age_days numeric,
              max_age_days int, stale boolean)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO 'gold_ch', 'public', 'pg_catalog'
SET statement_timeout TO '600s'
AS $function$
DECLARE r record; v_ts timestamptz;
BEGIN
  FOR r IN SELECT * FROM gold_ch.ref_freshness_targets WHERE enabled ORDER BY 1 LOOP
    EXECUTE format('SELECT max(%I) FROM %s', r.ts_column, r.foreign_rel) INTO v_ts;
    foreign_rel  := r.foreign_rel;
    last_touched := v_ts;
    age_days     := CASE WHEN v_ts IS NULL THEN NULL
                         ELSE round(extract(epoch FROM now() - v_ts)/86400.0, 1) END;
    max_age_days := r.max_age_days;
    -- A NULL timestamp counts as stale. "No rows / never stamped" is exactly
    -- the state a pipeline that never ran would leave behind, and treating it
    -- as fresh would defeat the whole check.
    stale        := (v_ts IS NULL) OR (v_ts < now() - make_interval(days => r.max_age_days));
    RETURN NEXT;
  END LOOP;
END;
$function$;

-- ---------------------------------------------------------------------------
-- Fold it into the alarm that already runs (cron 108, 06:45 daily).
-- The existing drift and last-run assertions are preserved verbatim; this adds
-- a third reason to raise.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION gold_ch.sync_drift_alarm()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'gold_ch', 'public', 'pg_catalog'
AS $function$
DECLARE
  v_bad int; v_msg text; v_last timestamptz; v_stale int; v_smsg text;
  v_problems text[] := '{}';
BEGIN
  -- BOTH conditions are evaluated before anything raises. An earlier version of
  -- this raised on drift immediately, which made the freshness assertion
  -- unreachable: drift is currently non-zero on several long-standing targets,
  -- so the liveness check that exists to catch a pipeline never firing would
  -- itself never have run. A red alarm must not hide a second, different red.
  SELECT count(*), coalesce(string_agg(format('%s: %s drifted, %s missing, %s extra%s',
           v.source_rel, v.drifted, v.missing_in_target, v.extra_in_target,
           coalesce(' ('||v.error||')','')), '; '), '')
    INTO v_bad, v_msg
  FROM gold_ch.v_sync_drift_latest v
  JOIN gold_ch.sync_drift_targets t
    ON t.source_rel=v.source_rel AND t.foreign_rel=v.foreign_rel AND t.enabled
  WHERE NOT v.clean;

  SELECT count(*), coalesce(string_agg(format('%s last touched %s (%s days, limit %s)',
           f.foreign_rel, coalesce(f.last_touched::date::text,'never'),
           coalesce(f.age_days::text,'n/a'), f.max_age_days), '; '), '')
    INTO v_stale, v_smsg
  FROM gold_ch.ref_freshness_check() f WHERE f.stale;

  SELECT max(checked_at) INTO v_last FROM gold_ch.sync_drift_log;

  IF v_last IS NULL OR v_last < now() - interval '2 days' THEN
    v_problems := v_problems || format('sync drift check has not run in over 2 days (last: %s)', v_last);
  END IF;
  IF v_bad > 0 THEN
    v_problems := v_problems || format('sync drift on %s target(s): %s', v_bad, v_msg);
  END IF;
  -- Liveness: a pipeline that never fired leaves gold and ref both unchanged,
  -- so drift stays clean and says nothing. This is the only check that notices.
  IF v_stale > 0 THEN
    v_problems := v_problems || format('ref freshness: %s table(s) past their refresh window: %s', v_stale, v_smsg);
  END IF;

  IF array_length(v_problems, 1) > 0 THEN
    RAISE EXCEPTION '%', array_to_string(v_problems, ' || ');
  END IF;
END;
$function$;

-- Report current state without raising.
SELECT foreign_rel, last_touched::date, age_days, max_age_days, stale
FROM gold_ch.ref_freshness_check() ORDER BY 1;
