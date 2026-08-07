-- ============================================================================
-- Notification outbox: bounded retry with backoff, and a visible dead letter
-- ============================================================================
-- pg_net fires and forgets. A transient failure loses the alert silently, and
-- nothing downstream can tell "no alert because nothing was wrong" from "no
-- alert because the POST failed". Measured on 2026-08-06: one deliberate
-- cron_failed_24h alert was lost to an SSL connect error, roughly 1 request in
-- 20 over three days.
--
-- This is the same failure class as bug 36c6e4bd (fn_db_health_notify inherited
-- pg_net's 5s default timeout and dropped alerts). That fix raised the timeout;
-- it did not add a retry, so the class stayed open.
--
-- Every Telegram send now lands in public.notify_outbox first. A cron resolves
-- each attempt against net._http_response, retries with backoff up to
-- max_attempts, and marks anything past that DEAD. A dead row is a visible
-- artefact, not a silence.
-- ============================================================================

CREATE TABLE IF NOT EXISTS public.notify_outbox (
  id              bigserial PRIMARY KEY,
  channel         text        NOT NULL DEFAULT 'telegram',
  chat_secret     text        NOT NULL,
  message         text        NOT NULL,
  status          text        NOT NULL DEFAULT 'pending',   -- pending | sent | dead
  attempts        int         NOT NULL DEFAULT 0,
  max_attempts    int         NOT NULL DEFAULT 5,
  request_id      bigint,
  last_error      text,
  created_at      timestamptz NOT NULL DEFAULT now(),
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  sent_at         timestamptz,
  CONSTRAINT notify_outbox_status_chk CHECK (status IN ('pending','sent','dead'))
);

CREATE INDEX IF NOT EXISTS notify_outbox_due_idx
  ON public.notify_outbox (next_attempt_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS notify_outbox_dead_idx
  ON public.notify_outbox (created_at DESC) WHERE status = 'dead';

COMMENT ON TABLE public.notify_outbox IS
  'Every outbound Telegram notification. Exists because pg_net does not retry: a transient SSL error silently loses an alert, and silence is indistinguishable from "nothing was wrong". Rows in status=dead are alerts that were never delivered; see public.v_notify_dead_letters.';

-- ---------------------------------------------------------------------------
-- Post one attempt. Returns the pg_net request id.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.fn_notify_attempt(p_row public.notify_outbox)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $$
DECLARE
  v_token text;
  v_chat  text;
  v_req   bigint;
BEGIN
  SELECT decrypted_secret INTO v_token FROM vault.decrypted_secrets WHERE name = 'telegram_bot_token';
  SELECT decrypted_secret INTO v_chat  FROM vault.decrypted_secrets WHERE name = p_row.chat_secret;
  IF v_token IS NULL OR v_chat IS NULL THEN
    RETURN NULL;
  END IF;

  SELECT net.http_post(
    url     := 'https://api.telegram.org/bot' || v_token || '/sendMessage',
    body    := jsonb_build_object('chat_id', v_chat, 'text', p_row.message,
                                  'parse_mode', 'HTML', 'disable_web_page_preview', true),
    headers := jsonb_build_object('Content-Type', 'application/json'),
    timeout_milliseconds := 15000
  ) INTO v_req;

  RETURN v_req;
END $$;

-- ---------------------------------------------------------------------------
-- Enqueue and make the first attempt immediately.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.fn_notify_send(p_text text, p_chat_secret text DEFAULT 'telegram_chat_id')
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $$
DECLARE
  v_row public.notify_outbox;
  v_req bigint;
BEGIN
  INSERT INTO public.notify_outbox (chat_secret, message, attempts, next_attempt_at, last_attempt_at)
  VALUES (p_chat_secret, p_text, 1, now(), now())
  RETURNING * INTO v_row;

  v_req := public.fn_notify_attempt(v_row);

  UPDATE public.notify_outbox
     SET request_id = v_req,
         -- first retry one minute out; the cycle applies the backoff from there
         next_attempt_at = now() + interval '1 minute',
         last_error = CASE WHEN v_req IS NULL THEN 'vault secret missing' ELSE NULL END,
         status = CASE WHEN v_req IS NULL THEN 'dead' ELSE 'pending' END
   WHERE id = v_row.id;

  RETURN v_row.id;
END $$;

-- ---------------------------------------------------------------------------
-- Resolve outstanding attempts, retry with backoff, dead-letter the exhausted.
-- ---------------------------------------------------------------------------
-- Backoff is 2^attempts minutes, capped at 60. statement_timeout lives in the
-- definition clause because pg_cron sessions inherit the database-level value
-- and SET LOCAL inside the body would be overridden.
CREATE OR REPLACE FUNCTION public.fn_notify_retry_cycle()
RETURNS TABLE (resolved_sent int, retried int, dead_lettered int)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
SET statement_timeout TO '120s'
AS $$
DECLARE
  r        public.notify_outbox;
  v_status int;
  v_err    text;
  v_sent   int := 0;
  v_retry  int := 0;
  v_dead   int := 0;
  v_req    bigint;
BEGIN
  FOR r IN
    SELECT * FROM public.notify_outbox
    WHERE status = 'pending' AND next_attempt_at <= now()
    ORDER BY id
    LIMIT 200
  LOOP
    v_status := NULL; v_err := NULL;

    IF r.request_id IS NOT NULL THEN
      SELECT status_code, error_msg INTO v_status, v_err
      FROM net._http_response WHERE id = r.request_id;
    END IF;

    IF v_status BETWEEN 200 AND 299 THEN
      UPDATE public.notify_outbox
         SET status = 'sent', sent_at = now(), last_error = NULL
       WHERE id = r.id;
      v_sent := v_sent + 1;

    ELSIF v_status IS NULL AND v_err IS NULL AND r.request_id IS NOT NULL
          AND r.last_attempt_at > now() - interval '2 minutes' THEN
      -- Still in flight. Keyed on last_attempt_at, NOT created_at: pg_net prunes
      -- _http_response, so an old row whose response has aged out would look
      -- in-flight forever if judged by when it was first created.
      NULL;

    ELSIF r.attempts >= r.max_attempts THEN
      UPDATE public.notify_outbox
         SET status = 'dead',
             last_error = coalesce(v_err, 'http ' || coalesce(v_status::text, 'no response'))
       WHERE id = r.id;
      v_dead := v_dead + 1;

    ELSE
      v_req := public.fn_notify_attempt(r);
      UPDATE public.notify_outbox
         SET attempts        = r.attempts + 1,
             request_id      = v_req,
             last_attempt_at = now(),
             last_error      = coalesce(v_err, 'http ' || coalesce(v_status::text, 'no response')),
             next_attempt_at = now() + (least(power(2, r.attempts + 1)::int, 60) || ' minutes')::interval
       WHERE id = r.id;
      v_retry := v_retry + 1;
    END IF;
  END LOOP;

  RETURN QUERY SELECT v_sent, v_retry, v_dead;
END $$;

-- ---------------------------------------------------------------------------
-- Dead letters. A row here is an alert nobody ever saw.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.v_notify_dead_letters AS
SELECT id, channel, chat_secret, left(message, 200) AS message, attempts,
       last_error, created_at
FROM public.notify_outbox
WHERE status = 'dead'
ORDER BY created_at DESC;

REVOKE ALL ON FUNCTION public.fn_notify_attempt(public.notify_outbox)      FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.fn_notify_send(text, text)                   FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.fn_notify_retry_cycle()                      FROM PUBLIC, anon, authenticated;

-- ---------------------------------------------------------------------------
-- Route the two existing notifiers through the outbox. Signatures unchanged, so
-- every caller keeps working.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.fn_db_health_notify(p_text text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $$
BEGIN
  PERFORM public.fn_notify_send(p_text, 'telegram_chat_id');
END $$;

CREATE OR REPLACE FUNCTION public.fn_signups_notify(p_text text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $$
BEGIN
  PERFORM public.fn_notify_send(p_text, 'telegram_signups_chat_id');
END $$;

SELECT cron.unschedule('notify-retry-cycle')
WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'notify-retry-cycle');

SELECT cron.schedule('notify-retry-cycle', '*/5 * * * *',
                     $cron$SELECT public.fn_notify_retry_cycle();$cron$);
