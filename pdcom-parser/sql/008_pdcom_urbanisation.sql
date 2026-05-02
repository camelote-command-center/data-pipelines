-- v0.5 / urbanisation track: focused-extraction silver table on re-LLM.
-- Separate from v0.4's silver_ch.pdcom_zones matview. No matview, no gold, no FDW.
-- Per-PDF DELETE+INSERT idempotency via pdf_sha256 column.
-- Apply to re-LLM (znrvddgmczdqoucmykij).

CREATE TABLE IF NOT EXISTS silver_ch.pdcom_urbanisation (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  commune_bfs     integer NOT NULL,             -- federal BFS
  commune_name    text NOT NULL,
  category_key    text NOT NULL,                -- one of 6 canonical buckets
  category_label  text NOT NULL,                -- canonical display label (consistent across communes)
  raw_label       text NOT NULL,                -- as captured from PDF legend
  source_color    text NOT NULL,                -- hex
  source_pdf      text NOT NULL,                -- filename
  source_page     integer NOT NULL,
  source_url      text,                          -- joined from bronze_ch.pdcom_sources by sha256, NULL if not in bronze
  pdf_sha256      text NOT NULL,                -- enables idempotent per-PDF DELETE+INSERT
  geometry        geometry(MultiPolygon, 2056) NOT NULL,  -- LV95
  area_m2         numeric GENERATED ALWAYS AS (ST_Area(geometry)) STORED,
  confidence      numeric NOT NULL,             -- 0-1, per-feature
  extracted_at    timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT category_key_valid CHECK (category_key IN (
    'plq_a_etablir_existant',
    'mz_court_moyen_terme',
    'mz_moyen_long_terme',
    'densification_accrue',
    'a_proteger_a_menager',
    'planification_imperative'
  ))
);

CREATE INDEX IF NOT EXISTS idx_pdcom_urb_geometry ON silver_ch.pdcom_urbanisation USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_pdcom_urb_commune  ON silver_ch.pdcom_urbanisation (commune_bfs);
CREATE INDEX IF NOT EXISTS idx_pdcom_urb_category ON silver_ch.pdcom_urbanisation (category_key);
CREATE INDEX IF NOT EXISTS idx_pdcom_urb_pdf_sha  ON silver_ch.pdcom_urbanisation (pdf_sha256);

COMMENT ON TABLE silver_ch.pdcom_urbanisation IS
  'v0.5 focused urbanisation extraction. Polygons mapped to 6 canonical zoning categories from small (1-2 page) PDCom carte-de-synthèse / strategie-d-evolution PDFs. Parallel to silver_ch.pdcom_zones (v0.4); merge/replace decision deferred.';

COMMENT ON COLUMN silver_ch.pdcom_urbanisation.category_key IS
  'Canonical bucket: plq_a_etablir_existant | mz_court_moyen_terme | mz_moyen_long_terme | densification_accrue | a_proteger_a_menager | planification_imperative';

COMMENT ON COLUMN silver_ch.pdcom_urbanisation.confidence IS
  'Per-feature confidence: page georef confidence × commune-clip ratio. Tier 1 label match starts at 0.95, Tier 2 at 0.80-0.94.';
