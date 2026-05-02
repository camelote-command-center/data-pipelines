-- v0.5.2 / urbanisation v2: promoteur-focused 6-category schema migration.
-- Applied to re-LLM. TRUNCATEs the v1 (empty anyway) data, swaps CHECK, adds
-- alternate_categories + match_keywords + map_freshness_year columns.

ALTER TABLE silver_ch.pdcom_urbanisation
  DROP CONSTRAINT IF EXISTS category_key_valid;

ALTER TABLE silver_ch.pdcom_urbanisation
  ADD CONSTRAINT category_key_valid CHECK (category_key IN (
    'zone_5',
    'densification_accrue',
    'plq_a_etablir',
    'plq_realise',
    'a_proteger',
    'exploitable'
  ));

ALTER TABLE silver_ch.pdcom_urbanisation
  ADD COLUMN IF NOT EXISTS alternate_categories text[] DEFAULT '{}',
  ADD COLUMN IF NOT EXISTS match_keywords text[] DEFAULT '{}',
  ADD COLUMN IF NOT EXISTS map_freshness_year integer;

TRUNCATE TABLE silver_ch.pdcom_urbanisation;

COMMENT ON COLUMN silver_ch.pdcom_urbanisation.alternate_categories IS
  'Other category_keys this polygon also matched, in priority order. The most-specific match wins as category_key.';
COMMENT ON COLUMN silver_ch.pdcom_urbanisation.match_keywords IS
  'Substrings from the canonical keyword lists that triggered the match. For human eyeball/audit.';
COMMENT ON COLUMN silver_ch.pdcom_urbanisation.map_freshness_year IS
  'Year extracted from the source PDF filename (2018, 2020, 2026, etc.) — used to pick the freshest map per commune.';
