-- ============================================================================
-- ge_cad_adresses: a unique projection, without deleting from bronze
-- ============================================================================
-- idpadr is unique at the DESTINATION (54'705 of 54'705) but duplicated at the
-- SOURCE (54'667 rows, 54'662 distinct). Branch A needs a unique key, so the
-- source is projected rather than deleted from. Nothing is removed from
-- bronze_ch: the duplicates are a source-side ingest artefact and deleting
-- production rows to satisfy a sync would be the wrong trade.
--
-- DETERMINISTIC ORDERING IS THE WHOLE POINT. DISTINCT ON without a total order
-- picks an arbitrary row, and the winner can change between runs even when the
-- data has not. That is precisely the silent-drift class this work removed, so
-- the ORDER BY is fully specified: idpadr, then id ascending. The lowest id is
-- the earliest ingest of that address and is stable across re-ingests.
--
-- ONE ADDRESS IS EXCLUDED ENTIRELY, NOT RESOLVED.
-- idpadr 260629140440 (Chemin Plein-Sud 40) carries TWO DIFFERENT egids at
-- source: 295531927 (id 5270) and 295531928 (id 326658). That is one address
-- bound to two different buildings. We have no basis to prefer either, and
-- picking the lower id would be a coin flip presented as a decision. The row is
-- therefore left out of the projection so the sync does not touch the
-- destination row at all: a stale address is better than an address confidently
-- bound to the wrong building. Same principle as forest_constraint_source,
-- where an inference must never present itself as authoritative.
-- Raised with SITG; see gold_ch.ge_cad_adresses_exceptions.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Known exceptions, kept as data rather than as a comment
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold_ch.ge_cad_adresses_exceptions (
  idpadr        text PRIMARY KEY,
  reason        text NOT NULL,
  competing     jsonb,
  raised_with   text,
  noted_at      timestamptz NOT NULL DEFAULT now()
);

INSERT INTO gold_ch.ge_cad_adresses_exceptions (idpadr, reason, competing, raised_with)
VALUES ('260629140440',
        'Two source rows share this idpadr but carry different egid values: the same address bound to two different buildings. Excluded from the sync projection rather than resolved, because there is no basis to prefer either and a wrong building binding is worse than a stale address.',
        '{"address":"Chemin Plein-Sud 40","rows":[{"id":5270,"egid":295531927},{"id":326658,"egid":295531928}]}'::jsonb,
        'SITG (draft query prepared, not sent)')
ON CONFLICT (idpadr) DO UPDATE
  SET reason = EXCLUDED.reason, competing = EXCLUDED.competing, raised_with = EXCLUDED.raised_with;

-- ---------------------------------------------------------------------------
-- The projection
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold_ch.v_ge_cad_adresses_full AS
SELECT DISTINCT ON (a.idpadr)
  a.idpadr,
  a.adresse,
  a.egid,
  a.code_voie,
  a.id_girec,
  a.no_batiment,
  a.nom_npa,
  a.updated_at
FROM bronze_ch.ge_cad_adresses a
WHERE NOT EXISTS (
  SELECT 1 FROM gold_ch.ge_cad_adresses_exceptions x WHERE x.idpadr = a.idpadr
)
ORDER BY a.idpadr, a.id;   -- total order: same row wins on every run

COMMENT ON VIEW gold_ch.v_ge_cad_adresses_full IS
  'Unique-by-idpadr projection of bronze_ch.ge_cad_adresses for Branch A. ORDER BY is total (idpadr, id) so the winning row is identical on every run. Rows listed in gold_ch.ge_cad_adresses_exceptions are omitted entirely so the sync leaves their destination rows untouched.';

-- ---------------------------------------------------------------------------
-- Collapse logging: if this moves off its expected value, the source changed
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold_ch.ge_cad_adresses_projection_log (
  id            bigserial PRIMARY KEY,
  run_at        timestamptz NOT NULL DEFAULT now(),
  source_rows   bigint,
  projected     bigint,
  collapsed     bigint,
  excluded_rows bigint
);

CREATE OR REPLACE FUNCTION gold_ch.log_ge_cad_adresses_projection()
RETURNS TABLE (source_rows bigint, projected bigint, collapsed bigint, excluded_rows bigint)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'gold_ch', 'bronze_ch', 'public', 'pg_catalog'
AS $$
DECLARE
  v_src bigint; v_proj bigint; v_exc bigint; v_coll bigint;
BEGIN
  SELECT count(*) INTO v_src  FROM bronze_ch.ge_cad_adresses;
  SELECT count(*) INTO v_proj FROM gold_ch.v_ge_cad_adresses_full;
  SELECT count(*) INTO v_exc  FROM bronze_ch.ge_cad_adresses a
    WHERE EXISTS (SELECT 1 FROM gold_ch.ge_cad_adresses_exceptions x WHERE x.idpadr = a.idpadr);

  -- rows the DISTINCT ON discarded, excluding those removed by the exception list
  v_coll := v_src - v_exc - v_proj;

  INSERT INTO gold_ch.ge_cad_adresses_projection_log (source_rows, projected, collapsed, excluded_rows)
  VALUES (v_src, v_proj, v_coll, v_exc);

  IF v_coll <> 4 THEN
    RAISE WARNING 'ge_cad_adresses projection collapsed % rows, expected 4. The source duplicate set has changed and needs review.', v_coll;
  END IF;

  RETURN QUERY SELECT v_src, v_proj, v_coll, v_exc;
END $$;

COMMENT ON FUNCTION gold_ch.log_ge_cad_adresses_projection() IS
  'Records how many rows the unique projection discarded. Expected 4 (five duplicated idpadr values, one of which is excluded outright). Any other number means the source duplicate set moved and must be looked at before trusting the sync.';

REVOKE ALL ON FUNCTION gold_ch.log_ge_cad_adresses_projection() FROM PUBLIC, anon, authenticated;
