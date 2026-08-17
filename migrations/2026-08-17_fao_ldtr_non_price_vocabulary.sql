-- 2026-08-17 — FAO LDTR: stop quarantining non-price transfer types (re-LLM)
--
-- WHY: bronze_ch.fao_ldtr.prix is populated from the notice's "Prix de vente"
-- field, which sometimes carries a legal transfer type instead of a figure:
-- the flat was donated, inherited or bequeathed, so there is no price. The only
-- cast site, silver_ch.event_transactions, calls
--   safe_cast.to_numeric(l.prix, 'bronze_ch.fao_ldtr', NULL, 'prix')
-- which quarantined every one of them on EVERY matview refresh. 449 rows had
-- accumulated from 11 source notices. That is not a failed parse, so the
-- quarantine item could never reach zero and the check was being taught to lie.
--
-- DECISION (Ilan, 2026-08-17): classify the vocabulary at the INGEST, before
-- the cast. Not in the watchdog — suppressing a known-good case there would
-- make the quarantine dishonest.
--
-- Full vocabulary observed in bronze_ch.fao_ldtr + safe_cast.quarantine over
-- 2026-06-07 .. 2026-08-17 (7 distinct values, 11 live source rows):
--   donation                       (x6 live)
--   Donation d'actions             -> stored "Donation dactions"
--   Succession                     (x1 live)
--   Succession sans soulte         (x2 live)
--   Succession sans soultes        (x1, 2026-06-07 only)
--   liquidation d'une succession   -> stored "liquidation dune succession"
--   Délivrance de legs             (x1 live)
-- The parser strips apostrophes (.replace(/'/gm,'')), which is why the stored
-- forms lose them; the classifier normalises accents AND apostrophes so both
-- spellings match.
--
-- NOT generalised to "contains no digits": an unrecognised non-numeric shape
-- must still quarantine so genuinely new formats stay visible.
--
-- WHY A SIDE TABLE and not a column on bronze_ch.fao_ldtr:
--   gold_ch.sync_fao_ldtr() distributes to Lamap with
--     INSERT INTO lamap_db_foreign.fao_ldtr SELECT s.* FROM bronze_ch.fao_ldtr s ...
--   A new column on fao_ldtr would make s.* wider than the 16-column foreign
--   table and break FAO LDTR distribution to Lamap production on the next new
--   row. Do not add columns to bronze_ch.fao_ldtr until that SELECT s.* is
--   replaced with an explicit column list.

CREATE TABLE IF NOT EXISTS bronze_ch.fao_ldtr_price_absent (
  affaire      text PRIMARY KEY,
  raw_value    text NOT NULL,
  reason       text NOT NULL,
  recorded_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fao_ldtr_price_absent_reason
  ON bronze_ch.fao_ldtr_price_absent (reason);

GRANT SELECT, INSERT, UPDATE, DELETE ON bronze_ch.fao_ldtr_price_absent TO service_role;

-- Backfill the 11 existing notices, then blank their prix so the cast never
-- sees them. Same normalisation as pipelines/fao-ldtr/price-classifier.ts.
WITH norm AS (
  SELECT affaire, prix,
         regexp_replace(
           translate(lower(prix), 'àáâäãéèêëíìîïóòôöõúùûüç''’', 'aaaaaeeeeiiiiooooouuuuc'),
           '\s+', ' ', 'g') AS n
  FROM bronze_ch.fao_ldtr
  WHERE prix IS NOT NULL AND btrim(prix) <> ''
)
INSERT INTO bronze_ch.fao_ldtr_price_absent (affaire, raw_value, reason)
SELECT affaire, prix,
       CASE WHEN n ~ '\mdonation\M'   THEN 'donation'
            WHEN n ~ '\msuccession\M' THEN 'succession'
            WHEN n ~ '\mlegs\M'       THEN 'delivrance_de_legs'
            WHEN n ~ '\mpartage\M'    THEN 'partage' END
FROM norm
WHERE n !~ '\d'
  AND (n ~ '\mdonation\M' OR n ~ '\msuccession\M' OR n ~ '\mlegs\M' OR n ~ '\mpartage\M')
ON CONFLICT (affaire) DO UPDATE
  SET raw_value = excluded.raw_value, reason = excluded.reason, recorded_at = now();

UPDATE bronze_ch.fao_ldtr l
SET prix = NULL, updated_at = now()
FROM bronze_ch.fao_ldtr_price_absent p
WHERE p.affaire = l.affaire AND l.prix IS NOT NULL;

-- Stale quarantine rows for those 11 notices were removed (449 rows): under the
-- new design they were never failures, and the facts they recorded now live in
-- bronze_ch.fao_ldtr_price_absent. The three 'lot n° X.XX : NNNNNNN' rows were
-- deliberately LEFT quarantined — extracting a price from a lot label is domain
-- parsing that belongs in the ingest, tracked as separate work.
