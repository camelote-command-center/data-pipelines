-- 2026-08-17 — safe_cast: pure parser for CI, attribution for wrappers (re-LLM)
--
-- PROBLEM 1 — a test harness could write to the production quarantine.
-- pipelines/transactions-fao/date-parse.test.ts asserted safe_cast.to_date via
-- public.assert_safe_cast_to_date. to_date quarantines on failure, so any probe
-- value that failed to parse would leave a real row in safe_cast.quarantine and
-- production data quality would reflect CI traffic.
--
-- (For the record: the 255 'unattributed' rows on the board were NOT from CI.
-- The harness only ever probed '02.07.2026', '15.07.2026' and '2026-07-02',
-- all of which parse. Those rows were ad-hoc diagnostic queries run against
-- production on 2026-08-17 in two bursts, 10:58 and 11:13, plus 2 from the
-- regression run of this very migration. All 255 were deleted; none pre-dated
-- that day, so no recurring caller produces them.)
--
-- FIX: all parsing moves into safe_cast.parse_date(), which is IMMUTABLE and
-- contains no INSERT. safe_cast.to_date() keeps its exact public behaviour but
-- now holds no parsing logic of its own — it delegates, range-clamps and
-- quarantines. public.probe_safe_cast_to_date() exposes the pure parser over
-- PostgREST for CI. Because to_date has no parser of its own, asserting the
-- probe remains an equivalent regression guard for bugs f3538ed1 and 817d1453.
-- An unparseable value makes parse_date RAISE, which surfaces as a failed build
-- rather than a silent write.
--
-- PROBLEM 2 — the assert_ wrappers took only (raw), so every caller landed in
-- the 'unattributed' bucket.
--
-- FIX: 4-argument OVERLOADS. Adding DEFAULTs to the existing 1-arg name would
-- make assert_safe_cast_to_int('x') ambiguous ("function is not unique"), and
-- dropping the 1-arg form is blocked by silver_ch.v_ge_commune_roster depending
-- on it. Distinct arities resolve unambiguously, so every existing single-arg
-- caller keeps working and attribution stays OPTIONAL: a caller that omits it
-- still quarantines, just without a label. Losing the label beats losing the row.

-- Pure date parser. NO side effects: no quarantine insert, no writes at all.
-- Raises on an unparseable value; safe_cast.to_date() catches that and is the
-- only place that quarantines. Keeping ALL parsing here means to_date holds no
-- parsing logic of its own, so a CI probe against parse_date is an equivalent
-- regression guard — and cannot write to production.
CREATE OR REPLACE FUNCTION safe_cast.parse_date(raw text)
 RETURNS date
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
DECLARE
  t text;
  ascii_t text;
  mon int;
  mname text;
BEGIN
  IF raw IS NULL OR btrim(raw) = '' THEN RETURN NULL; END IF;
  t := btrim(raw);
  IF t ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN
    RETURN to_date(t, 'DD.MM.YYYY');   -- European dotted date, parsed day-first
  END IF;
  -- French long-form date, e.g. '07 aout 2026', '14 août 2026', '1er mai 2026'.
  -- Accents are folded to ASCII so 'août'/'aout' and 'décembre'/'decembre' match.
  ascii_t := translate(lower(t), 'àáâäãéèêëíìîïóòôöõúùûüç', 'aaaaaeeeeiiiiooooouuuuc');
  IF ascii_t ~ '^\d{1,2}(er)?\s+[a-z]+\s+\d{4}$' THEN
    mname := (regexp_match(ascii_t, '^\d{1,2}(?:er)?\s+([a-z]+)\s+\d{4}$'))[1];
    mon := CASE mname
             WHEN 'janvier' THEN 1  WHEN 'fevrier'  THEN 2  WHEN 'mars'      THEN 3
             WHEN 'avril'   THEN 4  WHEN 'mai'      THEN 5  WHEN 'juin'      THEN 6
             WHEN 'juillet' THEN 7  WHEN 'aout'     THEN 8  WHEN 'septembre' THEN 9
             WHEN 'octobre' THEN 10 WHEN 'novembre' THEN 11 WHEN 'decembre'  THEN 12
           END;
    IF mon IS NULL THEN
      RAISE EXCEPTION 'unrecognised french month %', mname;
    END IF;
    RETURN to_date(regexp_replace(ascii_t, '^(\d{1,2})(?:er)?\s+[a-z]+\s+(\d{4})$',
                                  '\1 ' || mon::text || ' \2'), 'DD MM YYYY');
  END IF;
  RETURN t::date;                      -- ISO and everything else
END $function$;

-- Quarantining wrapper. Holds NO parsing logic — that all lives in
-- safe_cast.parse_date(), which is pure. This function's only jobs are the
-- range clamp and the quarantine bookkeeping.
CREATE OR REPLACE FUNCTION safe_cast.to_date(raw text, src_table text DEFAULT NULL::text, src_pk text DEFAULT NULL::text, col text DEFAULT NULL::text)
 RETURNS date
 LANGUAGE plpgsql
AS $function$
DECLARE d date;
BEGIN
  IF raw IS NULL OR btrim(raw)='' THEN RETURN NULL; END IF;
  BEGIN
    d := safe_cast.parse_date(raw);
    -- clamp absurd future/past sentinels (e.g. 2070-06-06) to NULL + quarantine
    IF d > (now()::date + interval '2 years') OR d < date '1800-01-01' THEN
      INSERT INTO safe_cast.quarantine(source_table,source_pk,column_name,expected_type,raw_value)
      VALUES (COALESCE(src_table,'?'),src_pk,COALESCE(col,'?'),'date(out-of-range)',raw);
      RETURN NULL;
    END IF;
    RETURN d;
  EXCEPTION WHEN OTHERS THEN
    INSERT INTO safe_cast.quarantine(source_table,source_pk,column_name,expected_type,raw_value)
    VALUES (COALESCE(src_table,'?'),src_pk,COALESCE(col,'?'),'date',raw);
    RETURN NULL;
  END;
END $function$;

CREATE OR REPLACE FUNCTION public.probe_safe_cast_to_date(raw text)
 RETURNS date LANGUAGE sql IMMUTABLE SECURITY INVOKER
 SET search_path TO 'public', 'pg_catalog'
AS $function$ SELECT safe_cast.parse_date(raw); $function$;

GRANT EXECUTE ON FUNCTION public.probe_safe_cast_to_date(text) TO service_role, authenticated, anon;

-- 4-arg overloads (1-arg forms retained and delegating)
CREATE OR REPLACE FUNCTION public.assert_safe_cast_to_date(raw text, src_table text, src_pk text, col text)
 RETURNS date LANGUAGE sql SECURITY DEFINER SET search_path TO 'public','pg_catalog'
AS $function$ SELECT safe_cast.to_date(raw, src_table, src_pk, col); $function$;

CREATE OR REPLACE FUNCTION public.assert_safe_cast_to_int(raw text, src_table text, src_pk text, col text)
 RETURNS integer LANGUAGE sql SECURITY DEFINER SET search_path TO 'public','pg_catalog'
AS $function$ SELECT safe_cast.to_int(raw, src_table, src_pk, col); $function$;

-- The only in-database caller of a wrapper, now attributed.
CREATE OR REPLACE VIEW silver_ch.v_ge_commune_roster AS
 SELECT assert_safe_cast_to_int((no_com_federal)::text,
                                'bronze_ch.ge_cad_communes'::text,
                                (no_com_federal)::text,
                                'no_com_federal'::text) AS commune_bfs,
        CASE WHEN (count(*) > 1) THEN split_part(min((commune)::text), '-'::text, 1)
             ELSE min((commune)::text) END AS commune_name,
        count(*) AS cadastral_subdivisions
   FROM bronze_ch.ge_cad_communes
  WHERE (no_com_federal IS NOT NULL)
  GROUP BY no_com_federal;
