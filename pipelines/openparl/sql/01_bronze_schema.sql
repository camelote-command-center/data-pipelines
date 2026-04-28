-- OpenParlData — Swiss parliamentary data (federal, cantonal, communal)
-- Source: api.openparldata.ch/v1 (REST, CC-BY, no auth)
-- Phase 1: ingest ALL ~2,411 bodies (national context) + Suisse-romande indexed
-- bodies' affairs. Persons/meetings/agendas/votings can be layered in later.
--
-- Rollback: DROP TABLE IF EXISTS bronze_ch.openparl_records;

CREATE SCHEMA IF NOT EXISTS bronze_ch;

CREATE TABLE IF NOT EXISTS bronze_ch.openparl_records (
  id              BIGSERIAL PRIMARY KEY,
  record_type     TEXT NOT NULL,        -- 'body' | 'affair' | 'voting' | 'person' | 'meeting' | 'agenda'
  api_id          BIGINT NOT NULL,      -- the API's `id` field
  body_id         BIGINT,               -- which body this record belongs to (null for bodies)
  canton_key      TEXT,                 -- ISO canton (GE, VD, NE, JU, FR, VS, ...)
  body_key        TEXT,
  body_type       TEXT,                 -- 'municipality' | 'canton' | 'national'
  title           TEXT,
  attributes      JSONB NOT NULL,
  country         TEXT NOT NULL DEFAULT 'ch',
  language        TEXT,
  admin_level_1   TEXT,                 -- canton_key
  admin_level_2   TEXT,                 -- body name when type='municipality'
  fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT openparl_records_type_id_unique UNIQUE (record_type, api_id)
);

CREATE INDEX IF NOT EXISTS idx_openparl_records_type ON bronze_ch.openparl_records (record_type);
CREATE INDEX IF NOT EXISTS idx_openparl_records_canton ON bronze_ch.openparl_records (canton_key);
CREATE INDEX IF NOT EXISTS idx_openparl_records_body ON bronze_ch.openparl_records (body_id) WHERE body_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_openparl_records_attrs ON bronze_ch.openparl_records USING GIN (attributes);

CREATE OR REPLACE FUNCTION bronze_ch._openparl_records_touch()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = now(); RETURN NEW; END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_openparl_records_touch ON bronze_ch.openparl_records;
CREATE TRIGGER trg_openparl_records_touch
  BEFORE UPDATE ON bronze_ch.openparl_records
  FOR EACH ROW EXECUTE FUNCTION bronze_ch._openparl_records_touch();
