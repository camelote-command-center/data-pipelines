-- FAO Multi — generic bronze table for fao.ge.ch rubriques 72/91/97/135/136/213/217/322/84/90
-- One row per <li> entry. Field names vary per rubrique → captured in `fields` JSONB.
-- Target: re-llm bronze_ch (znrvddgmczdqoucmykij)
--
-- Rollback:
--   DROP TABLE IF EXISTS bronze_ch.ge_fao_publications;

CREATE SCHEMA IF NOT EXISTS bronze_ch;

CREATE TABLE IF NOT EXISTS bronze_ch.ge_fao_publications (
  id                BIGSERIAL PRIMARY KEY,
  rubrique          INT NOT NULL,
  affaire           TEXT,
  publication_date  DATE,
  title             TEXT,
  section           TEXT,
  subsection        TEXT,
  raw_text          TEXT,
  fields            JSONB,
  -- Composite natural key: prefer affaire, fall back to md5(raw_text) so re-runs
  -- on rubriques that don't carry an affaire number stay idempotent.
  dedup_key         TEXT GENERATED ALWAYS AS (
                      COALESCE(NULLIF(affaire, ''), md5(COALESCE(raw_text, '')))
                    ) STORED,
  fetched_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT ge_fao_publications_rubrique_dedup_key UNIQUE (rubrique, dedup_key)
);

CREATE INDEX IF NOT EXISTS idx_fao_publications_rubrique_date
  ON bronze_ch.ge_fao_publications (rubrique, publication_date DESC);

CREATE INDEX IF NOT EXISTS idx_fao_publications_publication_date
  ON bronze_ch.ge_fao_publications (publication_date DESC);

CREATE INDEX IF NOT EXISTS idx_fao_publications_affaire
  ON bronze_ch.ge_fao_publications (affaire)
  WHERE affaire IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_fao_publications_fields_gin
  ON bronze_ch.ge_fao_publications USING GIN (fields);

-- Auto-bump updated_at
CREATE OR REPLACE FUNCTION bronze_ch._fao_publications_touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_fao_publications_touch_updated_at
  ON bronze_ch.ge_fao_publications;

CREATE TRIGGER trg_fao_publications_touch_updated_at
  BEFORE UPDATE ON bronze_ch.ge_fao_publications
  FOR EACH ROW EXECUTE FUNCTION bronze_ch._fao_publications_touch_updated_at();
