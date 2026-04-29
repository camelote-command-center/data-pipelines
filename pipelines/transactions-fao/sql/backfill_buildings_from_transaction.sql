-- Backfill bronze_ch.fao_transactions.buildings from raw transaction text
--
-- Context:
--   The Claude-Sonnet parser only ran on FAO rows ingested via the current
--   structured-format pipeline (mid-2025 onwards). 102,868 historical rows
--   (date_of_transaction 2005-2024) were backfilled into bronze in early
--   2026 with `buildings = NULL` even though the raw transaction text
--   contains extractable parcel references in 60-70% of rows.
--
--   This script extracts those references via regex and writes them back
--   to `buildings` in the same JSON shape the parser produces. The live
--   parser is unchanged — it remains correct for new rows.
--
-- Patterns handled:
--   1. Modern prefix:  (B-F|PPE|COP|DDP|DP|VIC|Bien-fonds|Unite de PPE/COP/DDP)
--                      <commune>, NN/NNNN [-NN]
--   2. Old colon:      immeuble NN:NNNN  or  NN:NNNN fe NN
--
-- Schema produced (same as the Claude parser):
--   [{"building_types":[...],"building_commune":"Onex",
--     "building_id":"34/2615","feuillet_number":null|"34",
--     "pourmille_number":null,"parts_cop":null,"premises":[]}]
--
-- Idempotent: only touches rows with buildings IS NULL OR buildings = ''.
-- Precision-first: rows where the regex finds nothing are left NULL.
-- Multiple matches per transaction are de-duplicated by (building_id,
-- building_commune) and aggregated into the JSON array.
--
-- Rollback:
--   UPDATE bronze_ch.fao_transactions SET buildings = NULL
--     WHERE buildings LIKE '%"_backfill_v1":true%';
--
-- Smoke-test before commit:
--   BEGIN; <run script>; SELECT COUNT(*) FILTER (WHERE buildings IS NOT NULL)
--     FROM bronze_ch.fao_transactions; ROLLBACK;
-- ---------------------------------------------------------------------------

WITH null_rows AS (
  SELECT id, commune, transaction
  FROM bronze_ch.fao_transactions
  WHERE (buildings IS NULL OR buildings = '')
    AND transaction IS NOT NULL
    AND transaction <> ''
),

-- Pattern A: prefix-led references.
-- Captured groups:
--   1 = prefix verbatim (used to derive building_type)
--   2 = commune name (may contain hyphens, accents, apostrophes)
--   3 = commune number
--   4 = parcel id
--   5 = optional feuillet (PPE suffix "-NN")
pattern_a AS (
  SELECT
    nr.id,
    m[1]      AS raw_prefix,
    m[2]      AS commune_name,
    m[3]      AS commune_num,
    m[4]      AS parcel_id,
    NULLIF(m[5], '') AS feuillet
  FROM null_rows nr,
       regexp_matches(
         nr.transaction,
         -- (?: prefix ) \s+ ( commune ) [\s,] ( NN ) / ( NNNN ) (?: - ( NN ) )?
         -- Commune accepts letters (including accents), spaces, hyphens, apostrophes.
         '(?:^|[^\w\-])(B-F|b-f|PPE|COP|DDP|DP|dp|VIC|Bien-fonds?|Unit[eé]s?\s+de\s+(?:PPE|COP|DDP|DP))\s+([A-Za-zÀ-ÖØ-öø-ÿ][\w\-''\s]*?)[\s,]\s*(\d{1,3})/(\d{1,6})(?:-(\d{1,4}))?',
         'g'
       ) AS m
),

-- Pattern B: old colon format. immeuble NN:NNNN [fe NN]
pattern_b AS (
  SELECT
    nr.id,
    NULL::text AS raw_prefix,
    nr.commune AS commune_name,   -- old format relies on the row's own commune
    m[1]       AS commune_num,
    m[2]       AS parcel_id,
    NULLIF(m[3], '') AS feuillet
  FROM null_rows nr,
       regexp_matches(
         nr.transaction,
         '(?:immeuble|bien[\s\-]fonds?)\s+(\d{1,3}):(\d{1,6})(?:\s+fe\s*(\d{1,4}))?',
         'gi'
       ) AS m
),

-- Pattern C: bare colon NN:NNNN fe NN (no preceding "immeuble" keyword)
pattern_c AS (
  SELECT
    nr.id,
    NULL::text AS raw_prefix,
    nr.commune AS commune_name,
    m[1]       AS commune_num,
    m[2]       AS parcel_id,
    NULLIF(m[3], '') AS feuillet
  FROM null_rows nr,
       regexp_matches(
         nr.transaction,
         '(?<!\d)(\d{1,3}):(\d{1,6})\s+fe\s*(\d{1,4})',
         'g'
       ) AS m
),

all_matches AS (
  SELECT * FROM pattern_a
  UNION ALL
  SELECT * FROM pattern_b
  UNION ALL
  SELECT * FROM pattern_c
),

cleaned AS (
  -- Normalize prefix → building_types[]; trim commune; dedupe per (id, building_id, commune)
  SELECT DISTINCT ON (id, building_id, building_commune)
    id,
    CASE
      WHEN raw_prefix IS NULL THEN NULL
      WHEN raw_prefix ~* '^(B-F|b-f)$'              THEN ARRAY['B-F']
      WHEN raw_prefix ~* '^PPE$'                    THEN ARRAY['PPE']
      WHEN raw_prefix ~* '^COP$'                    THEN ARRAY['COP']
      WHEN raw_prefix ~* '^(DDP|DP|dp)$'            THEN ARRAY['DDP']
      WHEN raw_prefix ~* '^VIC$'                    THEN ARRAY['VIC']
      WHEN raw_prefix ~* '^Bien-fonds?$'            THEN ARRAY['B-F']
      WHEN raw_prefix ~* '^Unit'                    THEN ARRAY['PPE']
      ELSE ARRAY[raw_prefix]
    END                                                   AS building_types,
    NULLIF(trim(BOTH ' ,-' FROM commune_name), '')       AS building_commune,
    commune_num || '/' || parcel_id                       AS building_id,
    feuillet                                              AS feuillet_number
  FROM all_matches
  WHERE commune_num IS NOT NULL
    AND parcel_id   IS NOT NULL
    AND parcel_id::int > 0
    AND commune_num::int > 0
    AND length(commune_num) <= 3       -- guard against false positives
),

aggregated AS (
  SELECT id,
         jsonb_agg(
           jsonb_build_object(
             'building_types',    building_types,
             'building_commune',  building_commune,
             'building_id',       building_id,
             'feuillet_number',   feuillet_number,
             'pourmille_number',  NULL,
             'parts_cop',         NULL,
             'premises',          '[]'::jsonb,
             '_backfill_v1',      true
           )
           ORDER BY building_id, building_commune
         ) AS buildings_jsonb
  FROM cleaned
  GROUP BY id
)

UPDATE bronze_ch.fao_transactions f
SET buildings = a.buildings_jsonb::text,
    updated_at = NOW()
FROM aggregated a
WHERE f.id = a.id
  AND (f.buildings IS NULL OR f.buildings = '');

-- Coverage report
\echo ''
\echo '=== Post-backfill coverage ==='
SELECT
  COUNT(*) AS total,
  COUNT(*) FILTER (WHERE buildings IS NOT NULL AND buildings <> '') AS with_buildings,
  ROUND(100.0 * COUNT(*) FILTER (WHERE buildings IS NOT NULL AND buildings <> '') / COUNT(*), 2) AS pct,
  COUNT(*) FILTER (WHERE buildings LIKE '%"_backfill_v1":true%') AS from_this_backfill
FROM bronze_ch.fao_transactions;
