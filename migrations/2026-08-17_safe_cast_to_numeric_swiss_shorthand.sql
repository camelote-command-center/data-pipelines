-- 2026-08-17 — safe_cast.to_numeric: accept Swiss price shorthand (re-LLM)
--
-- '575000.- CHF' and '1’180000.-' are real numbers wearing Swiss notation, but
-- they were quarantining. The existing body already stripped apostrophes (both
-- the straight and the typographic one), nbsp and plain spaces, and mapped a
-- comma to the decimal point; what it did not handle was a currency token and
-- the trailing '.-' meaning "and no centimes".
--
-- Order matters: strip the currency token FIRST, then the trailing '.-',
-- otherwise '575000.- CHF' still ends in a letter when the dash rule runs.
--
-- Deliberately NOT extended to the non-price legal vocabulary (donation,
-- succession, ...) or to lot labels — those are classified in the FAO LDTR
-- ingest, not in this shared caster. See
-- 2026-08-17_fao_ldtr_non_price_vocabulary.sql.

CREATE OR REPLACE FUNCTION safe_cast.to_numeric(raw text, src_table text DEFAULT NULL::text, src_pk text DEFAULT NULL::text, col text DEFAULT NULL::text)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
DECLARE t text;
BEGIN
  IF raw IS NULL OR btrim(raw)='' THEN RETURN NULL; END IF;
  BEGIN
    -- unchanged: strip Swiss thousands separators (straight + typographic apostrophe),
    -- non-breaking and plain spaces, then treat a comma as the decimal point.
    t := replace(replace(replace(replace(replace(raw,'''',''),'’',''),chr(160),''),' ',''),',','.');
    -- added 2026-08-17: Swiss price shorthand that is still a real number.
    --   '575000.- CHF' -> 575000     '1’180000.-' -> 1180000
    -- Currency token first, then the trailing '.-' / '.–' meaning "and no centimes".
    t := regexp_replace(t, '(?i)(chf|sfr\.?|frs?\.)', '', 'g');
    t := regexp_replace(t, '\.[-–]$', '');
    RETURN t::numeric;
  EXCEPTION WHEN OTHERS THEN
    INSERT INTO safe_cast.quarantine(source_table,source_pk,column_name,expected_type,raw_value)
    VALUES (COALESCE(src_table,'?'),src_pk,COALESCE(col,'?'),'numeric',raw);
    RETURN NULL;
  END;
END $function$;
