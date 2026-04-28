-- opendata.fr.ch — Fribourg cantonal open data (OpenDataSoft v2)
-- Source: opendata.fr.ch/api/explore/v2.1
-- Two tables: dataset metadata + records (one row per ODS record).
--
-- Rollback: DROP TABLE IF EXISTS bronze_ch.opendata_fr_records, bronze_ch.opendata_fr_datasets;

CREATE SCHEMA IF NOT EXISTS bronze_ch;

-- 1. Dataset metadata (~110 datasets)
CREATE TABLE IF NOT EXISTS bronze_ch.opendata_fr_datasets (
  id              BIGSERIAL PRIMARY KEY,
  dataset_id      TEXT NOT NULL UNIQUE,
  dataset_uid     TEXT,
  has_records     BOOLEAN,
  metas           JSONB,                  -- title, description, theme, keyword, publisher, license, ...
  fields          JSONB,                  -- field schema
  features        TEXT[],
  attributes      JSONB NOT NULL,         -- full catalog row
  country         TEXT NOT NULL DEFAULT 'ch',
  admin_level_1   TEXT NOT NULL DEFAULT 'FR',
  fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_opendata_fr_datasets_meta_gin ON bronze_ch.opendata_fr_datasets USING GIN (metas);

-- 2. Records (one row per ODS record across all datasets)
CREATE TABLE IF NOT EXISTS bronze_ch.opendata_fr_records (
  id              BIGSERIAL PRIMARY KEY,
  dataset_id      TEXT NOT NULL,
  record_id       TEXT,                   -- ODS recordid when present
  natural_key     TEXT NOT NULL,          -- record_id when present, else md5 of attributes
  attributes      JSONB NOT NULL,
  country         TEXT NOT NULL DEFAULT 'ch',
  admin_level_1   TEXT NOT NULL DEFAULT 'FR',
  admin_level_2   TEXT,                   -- commune name extracted when available
  fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT opendata_fr_records_dataset_key_unique UNIQUE (dataset_id, natural_key)
);
CREATE INDEX IF NOT EXISTS idx_opendata_fr_records_dataset ON bronze_ch.opendata_fr_records (dataset_id);
CREATE INDEX IF NOT EXISTS idx_opendata_fr_records_attrs_gin ON bronze_ch.opendata_fr_records USING GIN (attributes);

CREATE OR REPLACE FUNCTION bronze_ch._opendata_fr_touch()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = now(); RETURN NEW; END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_opendata_fr_datasets_touch ON bronze_ch.opendata_fr_datasets;
CREATE TRIGGER trg_opendata_fr_datasets_touch
  BEFORE UPDATE ON bronze_ch.opendata_fr_datasets
  FOR EACH ROW EXECUTE FUNCTION bronze_ch._opendata_fr_touch();

DROP TRIGGER IF EXISTS trg_opendata_fr_records_touch ON bronze_ch.opendata_fr_records;
CREATE TRIGGER trg_opendata_fr_records_touch
  BEFORE UPDATE ON bronze_ch.opendata_fr_records
  FOR EACH ROW EXECUTE FUNCTION bronze_ch._opendata_fr_touch();
