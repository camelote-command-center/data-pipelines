-- Apply to re-LLM (znrvddgmczdqoucmykij).
-- Geometry in LV95 (SRID 2056). commune_bfs matches lamap_db ref.* convention.
-- Schema: bronze_ch (country-partitioned medallion).

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS bronze_ch;

CREATE TABLE IF NOT EXISTS bronze_ch.pdcom_sources (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    commune_bfs integer NOT NULL,
    commune_name text NOT NULL,
    canton_code text NOT NULL DEFAULT 'GE',
    source_url text,
    source_path text,
    pdf_filename text NOT NULL,
    pdf_sha256 text NOT NULL,
    pdf_size_bytes bigint,
    pdf_page_count integer,
    adoption_date date,
    downloaded_at timestamptz DEFAULT now(),
    parsed_at timestamptz,
    parser_version text,
    manifest_json jsonb,
    UNIQUE (commune_bfs, pdf_sha256)
);
CREATE INDEX IF NOT EXISTS idx_pdcom_sources_commune ON bronze_ch.pdcom_sources (commune_bfs);

CREATE TABLE IF NOT EXISTS bronze_ch.pdcom_pages (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id uuid NOT NULL REFERENCES bronze_ch.pdcom_sources(id) ON DELETE CASCADE,
    page_number integer NOT NULL,
    page_type text NOT NULL CHECK (page_type IN ('map','text','cover','toc','unknown')),
    map_theme text,
    map_title text,
    legend_json jsonb,
    drawing_count integer,
    has_raster boolean,
    georef_confidence numeric(4,3),
    extraction_status text CHECK (extraction_status IN ('ok','legend_failed','low_confidence','skipped','extract_failed')),
    UNIQUE (source_id, page_number)
);
CREATE INDEX IF NOT EXISTS idx_pdcom_pages_source ON bronze_ch.pdcom_pages (source_id);
CREATE INDEX IF NOT EXISTS idx_pdcom_pages_theme ON bronze_ch.pdcom_pages (map_theme);

CREATE TABLE IF NOT EXISTS bronze_ch.pdcom_features (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    page_id uuid NOT NULL REFERENCES bronze_ch.pdcom_pages(id) ON DELETE CASCADE,
    commune_bfs integer NOT NULL,
    map_theme text NOT NULL,
    layer_label text NOT NULL,
    layer_slug text NOT NULL,
    source_color text NOT NULL,
    fill_type text NOT NULL CHECK (fill_type IN ('solid','stroke_only','hatch')),
    geometry geometry(Geometry, 2056) NOT NULL,
    georef_confidence numeric(4,3),
    properties jsonb,
    extracted_at timestamptz DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_pdcom_features_page ON bronze_ch.pdcom_features (page_id);
CREATE INDEX IF NOT EXISTS idx_pdcom_features_commune_theme ON bronze_ch.pdcom_features (commune_bfs, map_theme);
CREATE INDEX IF NOT EXISTS idx_pdcom_features_slug ON bronze_ch.pdcom_features (layer_slug);
CREATE INDEX IF NOT EXISTS idx_pdcom_features_geom ON bronze_ch.pdcom_features USING GIST (geometry);
