-- ============================================================
-- bronze_ch.ge_cad_batiments / ge_cad_batiments_souterrains — stop treating
-- objectid as an identity. APPLY ON re-LLM before deploying the matching import.py.
--
-- WHY
--   Both tables were re-keyed onto business_key (no_comm || '/' || no_batiment,
--   UNIQUE) in Aug 2026 (bug 177de4c5), but the parser kept upserting ON CONFLICT
--   (objectid) and two UNIQUE indexes on objectid stayed. Between the 2026-10-02
--   and 2026-11-01 runs SITG renumbered the layers: 82,658 of 82,910 hors-sol and
--   7,697 of 8,100 sous-sol buildings came back under a different objectid. Every
--   upsert then landed on some other building's row and hit
--   ge_cad_batiments_bkey_uidx (23505), so both layers wrote 0 rows.
--
--   The parser now upserts ON CONFLICT (business_key). objectid must not be UNIQUE
--   any more: while a run walks the renumbered layer, a building takes an objectid
--   another row still holds until that row's own batch arrives.
-- ============================================================

\set ON_ERROR_STOP on

BEGIN;

DROP INDEX IF EXISTS bronze_ch.idx_ge_cad_batiments_objectid;
DROP INDEX IF EXISTS bronze_ch.idx_ge_cad_batiments_unique;
CREATE INDEX IF NOT EXISTS idx_ge_cad_batiments_objectid
  ON bronze_ch.ge_cad_batiments (objectid);

DROP INDEX IF EXISTS bronze_ch.idx_ge_cad_batiments_souterrains_objectid;
DROP INDEX IF EXISTS bronze_ch.idx_ge_cad_batiments_souterrains_unique;
CREATE INDEX IF NOT EXISTS idx_ge_cad_batiments_souterrains_objectid
  ON bronze_ch.ge_cad_batiments_souterrains (objectid);

COMMIT;
