-- FAO Multi — add country/language/admin_level_1 metadata columns
-- Forward-compat with re-LLM v2 multi-domain taxonomy briefing (2026-04-27).
-- All FAO publications are Geneva canton, French-language. Defaults backfill
-- existing rows automatically.
--
-- Rollback:
--   ALTER TABLE bronze_ch.ge_fao_publications
--     DROP COLUMN admin_level_1,
--     DROP COLUMN language,
--     DROP COLUMN country;

ALTER TABLE bronze_ch.ge_fao_publications
  ADD COLUMN IF NOT EXISTS country       TEXT NOT NULL DEFAULT 'ch',
  ADD COLUMN IF NOT EXISTS language      TEXT NOT NULL DEFAULT 'fr',
  ADD COLUMN IF NOT EXISTS admin_level_1 TEXT NOT NULL DEFAULT 'GE';

CREATE INDEX IF NOT EXISTS idx_fao_publications_country_admin
  ON bronze_ch.ge_fao_publications (country, admin_level_1);
