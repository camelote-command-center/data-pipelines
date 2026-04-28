-- OpenHolidays — Swiss public + school holidays per canton
-- Source: openholidaysapi.org (REST, OGD, no auth)
--
-- Rollback: DROP TABLE IF EXISTS bronze_ch.openholidays;

CREATE SCHEMA IF NOT EXISTS bronze_ch;

CREATE TABLE IF NOT EXISTS bronze_ch.openholidays (
  id              UUID PRIMARY KEY,        -- API-provided UUID
  type            TEXT NOT NULL,           -- 'Public' | 'School' | 'Bank' | 'Optional'
  start_date      DATE NOT NULL,
  end_date        DATE NOT NULL,
  name_en         TEXT,
  name_fr         TEXT,
  name_de         TEXT,
  name_it         TEXT,
  nationwide      BOOLEAN NOT NULL,
  subdivisions    TEXT[],                  -- e.g. {CH-GE, CH-VD}
  attributes      JSONB NOT NULL,
  country         TEXT NOT NULL DEFAULT 'ch',
  fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_openholidays_type ON bronze_ch.openholidays (type);
CREATE INDEX IF NOT EXISTS idx_openholidays_dates ON bronze_ch.openholidays (start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_openholidays_subdivs ON bronze_ch.openholidays USING GIN (subdivisions);

CREATE OR REPLACE FUNCTION bronze_ch._openholidays_touch()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = now(); RETURN NEW; END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_openholidays_touch ON bronze_ch.openholidays;
CREATE TRIGGER trg_openholidays_touch
  BEFORE UPDATE ON bronze_ch.openholidays
  FOR EACH ROW EXECUTE FUNCTION bronze_ch._openholidays_touch();
