-- OpenPLZ — Swiss postal codes / address taxonomy
-- Source: openplzapi.org (REST, no auth, OGD)
-- One row per entity at any granularity (canton, district, commune, locality).
-- Streets deferred to phase 2 (would need ~5K per-locality fetches).
--
-- Rollback: DROP TABLE IF EXISTS bronze_ch.openplz_records;

CREATE SCHEMA IF NOT EXISTS bronze_ch;

CREATE TABLE IF NOT EXISTS bronze_ch.openplz_records (
  id              BIGSERIAL PRIMARY KEY,
  level           TEXT NOT NULL,          -- 'canton' | 'district' | 'commune' | 'locality'
  natural_key     TEXT NOT NULL,          -- canton.key | district.key | commune.key | "{postal_code}|{name}|{commune.key}" for locality
  name            TEXT,
  short_name      TEXT,
  postal_code     TEXT,
  canton_short    TEXT,
  district_key    TEXT,
  commune_key     TEXT,
  attributes      JSONB NOT NULL,
  country         TEXT NOT NULL DEFAULT 'ch',
  admin_level_1   TEXT,                   -- canton short (GE, VD, ...)
  admin_level_2   TEXT,                   -- commune name when applicable
  fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT openplz_records_level_key_unique UNIQUE (level, natural_key)
);

CREATE INDEX IF NOT EXISTS idx_openplz_records_level
  ON bronze_ch.openplz_records (level);
CREATE INDEX IF NOT EXISTS idx_openplz_records_canton
  ON bronze_ch.openplz_records (canton_short);
CREATE INDEX IF NOT EXISTS idx_openplz_records_postal
  ON bronze_ch.openplz_records (postal_code) WHERE postal_code IS NOT NULL;

CREATE OR REPLACE FUNCTION bronze_ch._openplz_records_touch()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_openplz_records_touch ON bronze_ch.openplz_records;
CREATE TRIGGER trg_openplz_records_touch
  BEFORE UPDATE ON bronze_ch.openplz_records
  FOR EACH ROW EXECUTE FUNCTION bronze_ch._openplz_records_touch();
