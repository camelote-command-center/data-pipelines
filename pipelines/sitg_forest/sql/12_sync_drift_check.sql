-- ============================================================================
-- Content-level drift detection between a re-LLM source and a consumer copy
-- ============================================================================
-- Built after bug 177de4c5: ref.ge_rdppf_synthese on lamap_db had drifted from
-- bronze_ch.ge_rdppf_synthese on 11 rows while row counts AND max(updated_at)
-- were identical on both sides. Every freshness check we run compares exactly
-- those two things, so the drift was invisible for as long as it existed.
--
-- This compares the actual VALUES, column by column, and is the only check that
-- can catch a sync whose change-predicate never fired.
--
-- Comparison rules that matter:
--   * Columns are the INTERSECTION of both relations, ordered by name so the
--     hash is stable regardless of physical column order.
--   * Every value is cast to text before hashing. For geometry this is
--     deliberate: the PostGIS = operator compares bounding boxes, so two
--     genuinely different shapes with the same envelope compare equal.
--   * NULL is folded to a sentinel that cannot occur in the data, so
--     NULL and the empty string stay distinguishable.
-- ============================================================================

CREATE OR REPLACE FUNCTION gold_ch.sync_drift_check(
  p_source_rel  text,
  p_foreign_rel text,
  p_key         text,   -- column name OR any SQL expression yielding a unique key
  p_exclude     text[] DEFAULT '{}'
)
RETURNS TABLE (
  common_cols       int,
  source_rows       bigint,
  target_rows       bigint,
  missing_in_target bigint,
  extra_in_target   bigint,
  drifted           bigint
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO 'gold_ch', 'public', 'pg_catalog'
SET statement_timeout TO '1800s'
AS $$
DECLARE
  v_cols   text[];
  v_hash   text;
  v_sql    text;
BEGIN
  SELECT array_agg(a.attname ORDER BY a.attname) INTO v_cols
  FROM pg_attribute a
  WHERE a.attrelid = p_source_rel::regclass AND a.attnum > 0 AND NOT a.attisdropped
    AND a.attname <> p_key
    AND NOT (a.attname = ANY (p_exclude))
    AND EXISTS (
      SELECT 1 FROM pg_attribute b
      WHERE b.attrelid = p_foreign_rel::regclass AND b.attnum > 0 AND NOT b.attisdropped
        AND b.attname = a.attname
    );

  IF v_cols IS NULL OR array_length(v_cols, 1) IS NULL THEN
    RAISE EXCEPTION 'sync_drift_check: no common comparable columns between % and %',
      p_source_rel, p_foreign_rel;
  END IF;

  -- md5 over every common column, NULL folded to a sentinel byte sequence.
  SELECT 'md5(' || string_agg(
           format('coalesce(%I::text, ''\x00NULL'')', c), ' || ''|'' || '
           ORDER BY c) || ')'
    INTO v_hash
  FROM unnest(v_cols) c;

  -- p_key is used as an EXPRESSION, not an identifier, so a composite business
  -- key can be passed as (no_comm || '/' || no_batiment). Re-keying the two
  -- batiments syncs onto their business key made that necessary.
  v_sql := format($q$
    WITH s AS (SELECT %1$s AS k, %2$s AS h FROM %3$s),
         t AS (SELECT %1$s AS k, %2$s AS h FROM %4$s)
    SELECT %5$s::int,
           (SELECT count(*) FROM s),
           (SELECT count(*) FROM t),
           count(*) FILTER (WHERE t.k IS NULL),
           count(*) FILTER (WHERE s.k IS NULL),
           count(*) FILTER (WHERE s.k IS NOT NULL AND t.k IS NOT NULL AND s.h IS DISTINCT FROM t.h)
    FROM s FULL OUTER JOIN t ON s.k = t.k
  $q$, p_key, v_hash, p_source_rel, p_foreign_rel, array_length(v_cols, 1));

  RETURN QUERY EXECUTE v_sql;
END;
$$;

REVOKE ALL ON FUNCTION gold_ch.sync_drift_check(text, text, text, text[]) FROM PUBLIC, anon, authenticated;

COMMENT ON FUNCTION gold_ch.sync_drift_check(text, text, text, text[]) IS
  'Value-level drift between a re-LLM source relation and a consumer copy reached over FDW. Row counts and max(updated_at) cannot detect the failure this exists for: see bug 177de4c5, where 11 rows differed with both of those identical.';

-- ---------------------------------------------------------------------------
-- The registry of timestamp-gated syncs, so the audit and the standing check
-- run over the same list.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold_ch.sync_drift_targets (
  source_rel   text NOT NULL,
  foreign_rel  text NOT NULL,
  key_column   text NOT NULL,
  procedure    text,
  enabled      boolean NOT NULL DEFAULT true,
  PRIMARY KEY (source_rel, foreign_rel)
);

INSERT INTO gold_ch.sync_drift_targets (source_rel, foreign_rel, key_column, procedure) VALUES
  ('bronze_ch.fao_ldtr',                     'lamap_db_foreign.fao_ldtr',                     'id',     'sync_fao_ldtr'),
  ('bronze_ch.ge_cad_adresses',              'lamap_db_foreign.ge_cad_adresses',              'idpadr', 'sync_ge_cad_adresses'),
  ('bronze_ch.ge_cad_batiments',             'lamap_db_foreign.ge_cad_batiments',             'id',     'sync_ge_cad_batiments'),
  ('bronze_ch.ge_cad_batiments_souterrains', 'lamap_db_foreign.ge_cad_batiments_souterrains', 'id',     'sync_ge_cad_batiments_souterrains'),
  ('bronze_ch.ge_cad_communes',              'lamap_db_foreign.ge_cad_communes',              'id',     'sync_ge_cad_communes'),
  ('gold_ch.v_ddp_full',                     'lamap_db_foreign.ddp',                          'egrid',  'sync_ge_cad_ddp_rich'),
  ('bronze_ch.ge_gol_sites_pollues',         'lamap_db_foreign.ge_gol_sites_pollues',         'id',     'sync_ge_gol_sites_pollues'),
  ('bronze_ch.ge_rdppf_synthese',            'lamap_db_foreign.ge_rdppf_synthese',            'id',     'sync_ge_rdppf_synthese'),
  ('bronze_ch.ge_sit_surelevation',          'lamap_db_foreign.ge_sit_surelevation',          'id',     'sync_ge_sit_surelevation'),
  ('bronze_ch.uspi_knowledge',               'lamap_db_foreign.uspi_knowledge',               'id',     'sync_uspi_knowledge')
ON CONFLICT (source_rel, foreign_rel) DO NOTHING;
