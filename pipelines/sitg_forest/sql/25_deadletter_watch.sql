-- ============================================================================
-- Watch the dead-letter queue on a channel that cannot share its failure mode
-- ============================================================================
-- v_notify_dead_letters is the evidence that an alert was lost. Nothing read it.
-- A silent-failure detector that fails silently is not a detector.
--
-- THE CIRCULARITY THIS AVOIDS. The obvious wiring is to alert on dead letters
-- through fn_db_health_notify. That routes through notify_outbox, the very thing
-- being reported on. If Telegram is down, the alert is lost AND the alarm about
-- the lost alert is lost with it, and both land in the same dead-letter queue
-- nobody reads. So this raise deliberately does NOT use the outbox.
--
-- TWO INDEPENDENT SIGNALS, neither of which is the outbox:
--   1. A bugs row on pixxels_data. No HTTP at all, so no shared failure mode
--      with Telegram. It surfaces wherever bugs already surface and is the
--      durable record.
--   2. A raised exception, which fails the cron job. That is picked up by
--      cron_failed_24h in fn_db_metrics_cycle. Note this eventually reaches
--      Telegram too, but only after the bugs row already exists, so a total
--      HTTP outage still leaves the finding recorded.
--
-- The bugs row is written FIRST and in its own statement, before the raise, so
-- the raise cannot roll it away. Same lesson as assert_no_sync_drift.
-- ============================================================================

CREATE OR REPLACE FUNCTION public.fn_notify_deadletter_check(p_window interval DEFAULT interval '24 hours')
RETURNS TABLE (dead_count bigint, oldest timestamptz, bug_id uuid)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $$
DECLARE
  v_n      bigint;
  v_oldest timestamptz;
  v_bug    uuid;
  v_desc   text;
BEGIN
  SELECT count(*), min(created_at) INTO v_n, v_oldest
  FROM public.notify_outbox
  WHERE status = 'dead' AND created_at > now() - p_window;

  IF v_n = 0 THEN
    RETURN QUERY SELECT 0::bigint, NULL::timestamptz, NULL::uuid;
    RETURN;
  END IF;

  v_desc := format(
    '[auto:notify_deadletter] %s notification(s) were never delivered in the last %s, oldest %s. '
    'These are alerts nobody saw. Inspect public.v_notify_dead_letters on pixxels_data. '
    'Each row records the message, the attempt count and the last error.',
    v_n, p_window, v_oldest);

  -- Deduped: one open bug per day, recurrence bumped rather than a new row each run.
  SELECT id INTO v_bug FROM public.bugs
  WHERE source = 'notify-deadletter-autolog' AND status = 'open'
    AND created_at > now() - interval '24 hours'
  LIMIT 1;

  IF v_bug IS NULL THEN
    INSERT INTO public.bugs (project, description, source, root_cause, status, reported_at)
    VALUES ('pixxels-cc', v_desc, 'notify-deadletter-autolog',
            'A notification exhausted its retries in public.notify_outbox. pg_net has no retry of its own; the outbox adds bounded retry, and anything past max_attempts lands here.',
            'open', current_date)
    RETURNING id INTO v_bug;
  ELSE
    UPDATE public.bugs
       SET recurrence_count = coalesce(recurrence_count, 0) + 1,
           description = v_desc,
           updated_at = now()
     WHERE id = v_bug;
  END IF;

  RETURN QUERY SELECT v_n, v_oldest, v_bug;
END $$;

REVOKE ALL ON FUNCTION public.fn_notify_deadletter_check(interval) FROM PUBLIC, anon, authenticated;

COMMENT ON FUNCTION public.fn_notify_deadletter_check(interval) IS
  'Records a bugs row when notifications have died undelivered. Deliberately does NOT send through notify_outbox: an outage would otherwise take out both the alert and the alarm about the lost alert.';

-- ---------------------------------------------------------------------------
-- The alarm. Separate function, separate cron, so the bugs row above is already
-- committed before anything raises.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.fn_notify_deadletter_alarm(p_window interval DEFAULT interval '24 hours')
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $$
DECLARE v_n bigint;
BEGIN
  SELECT count(*) INTO v_n
  FROM public.notify_outbox
  WHERE status = 'dead' AND created_at > now() - p_window;

  IF v_n > 0 THEN
    RAISE EXCEPTION
      'notify_outbox has % undelivered notification(s) in the last %. See public.v_notify_dead_letters.',
      v_n, p_window;
  END IF;
END $$;

REVOKE ALL ON FUNCTION public.fn_notify_deadletter_alarm(interval) FROM PUBLIC, anon, authenticated;

SELECT cron.unschedule('notify-deadletter-check')
WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'notify-deadletter-check');
SELECT cron.unschedule('notify-deadletter-alarm')
WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'notify-deadletter-alarm');

SELECT cron.schedule('notify-deadletter-check', '20 7 * * *',
                     $cron$SELECT public.fn_notify_deadletter_check();$cron$);
SELECT cron.schedule('notify-deadletter-alarm', '25 7 * * *',
                     $cron$SELECT public.fn_notify_deadletter_alarm();$cron$);
