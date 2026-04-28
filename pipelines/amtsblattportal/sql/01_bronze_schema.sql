-- amtsblattportal.ch — Swiss federal SOGC + cantonal Amtsblätter publications
-- Source: amtsblattportal.ch/api/v1/publications.json (REST, OGD, no auth)
-- Volume: 2.7M+ historical publications across ~150 rubrics + 23 cantonal tenants.
--
-- Rollback: DROP TABLE IF EXISTS bronze_ch.amtsblatt_publications;

CREATE SCHEMA IF NOT EXISTS bronze_ch;

CREATE TABLE IF NOT EXISTS bronze_ch.amtsblatt_publications (
  id                    UUID PRIMARY KEY,             -- API meta.id (UUID)
  publication_number    TEXT,
  publication_state     TEXT,
  publication_date      DATE NOT NULL,
  expiration_date       DATE,
  rubric                TEXT,
  sub_rubric            TEXT,
  primary_tenant_code   TEXT,                         -- 'shab', 'kabge', 'kabvd', etc.
  cantons               TEXT[],
  language              TEXT,
  title_de              TEXT,
  title_fr              TEXT,
  title_it              TEXT,
  title_en              TEXT,
  registration_office   TEXT,                         -- displayName for fast filtering
  attributes            JSONB NOT NULL,
  country               TEXT NOT NULL DEFAULT 'ch',
  fetched_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_amtsblatt_pubdate ON bronze_ch.amtsblatt_publications (publication_date DESC);
CREATE INDEX IF NOT EXISTS idx_amtsblatt_tenant ON bronze_ch.amtsblatt_publications (primary_tenant_code);
CREATE INDEX IF NOT EXISTS idx_amtsblatt_rubric ON bronze_ch.amtsblatt_publications (rubric, sub_rubric);
CREATE INDEX IF NOT EXISTS idx_amtsblatt_cantons ON bronze_ch.amtsblatt_publications USING GIN (cantons);
CREATE INDEX IF NOT EXISTS idx_amtsblatt_attrs ON bronze_ch.amtsblatt_publications USING GIN (attributes);

CREATE OR REPLACE FUNCTION bronze_ch._amtsblatt_touch()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = now(); RETURN NEW; END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_amtsblatt_touch ON bronze_ch.amtsblatt_publications;
CREATE TRIGGER trg_amtsblatt_touch
  BEFORE UPDATE ON bronze_ch.amtsblatt_publications
  FOR EACH ROW EXECUTE FUNCTION bronze_ch._amtsblatt_touch();
