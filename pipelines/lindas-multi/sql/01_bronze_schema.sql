-- LINDAS Multi — generic bronze table for Linked Data Switzerland datasets
-- Sources: register.ld.admin.ch, politics.ld.admin.ch, energy.ld.admin.ch,
-- culture.ld.admin.ch (descriptors), all queryable via SPARQL at lindas.admin.ch/query.
--
-- One row per RDF subject. Predicates → values land in `properties` JSONB.
-- For multi-valued predicates (e.g. multilingual labels), the value is a JSON array.
--
-- Rollback:
--   DROP TABLE IF EXISTS bronze_ch.lindas_observations;

CREATE SCHEMA IF NOT EXISTS bronze_ch;

CREATE TABLE IF NOT EXISTS bronze_ch.lindas_observations (
  id                BIGSERIAL PRIMARY KEY,
  dataset_slug      TEXT NOT NULL,         -- 'curia', 'termdat', 'bfe_ogd115_gest_bilanz', ...
  dataset_iri       TEXT NOT NULL,         -- the user-facing descriptor URL
  graph_iri         TEXT,                  -- the SPARQL named graph
  sparql_endpoint   TEXT,                  -- where data was fetched from
  subject_iri       TEXT NOT NULL,         -- ?s in (?s ?p ?o), the entity IRI
  properties        JSONB NOT NULL,        -- {predicate_iri: value-or-array} pairs
  tags              TEXT[],                -- coarse pre-populated tags (energy, parliament, ...)
  country           TEXT NOT NULL DEFAULT 'ch',
  language          TEXT,                  -- where derivable per row
  admin_level_1     TEXT NOT NULL DEFAULT 'CH',  -- federal-level data
  fetched_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT lindas_observations_dataset_subject_unique
    UNIQUE (dataset_slug, subject_iri)
);

CREATE INDEX IF NOT EXISTS idx_lindas_observations_dataset
  ON bronze_ch.lindas_observations (dataset_slug);

CREATE INDEX IF NOT EXISTS idx_lindas_observations_graph
  ON bronze_ch.lindas_observations (graph_iri);

CREATE INDEX IF NOT EXISTS idx_lindas_observations_properties_gin
  ON bronze_ch.lindas_observations USING GIN (properties);

CREATE INDEX IF NOT EXISTS idx_lindas_observations_tags_gin
  ON bronze_ch.lindas_observations USING GIN (tags);

CREATE INDEX IF NOT EXISTS idx_lindas_observations_country_admin
  ON bronze_ch.lindas_observations (country, admin_level_1);

-- Auto-bump updated_at
CREATE OR REPLACE FUNCTION bronze_ch._lindas_observations_touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_lindas_observations_touch_updated_at
  ON bronze_ch.lindas_observations;

CREATE TRIGGER trg_lindas_observations_touch_updated_at
  BEFORE UPDATE ON bronze_ch.lindas_observations
  FOR EACH ROW EXECUTE FUNCTION bronze_ch._lindas_observations_touch_updated_at();
