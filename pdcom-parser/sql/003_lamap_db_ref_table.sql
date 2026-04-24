-- Apply to lamap_db (fckdwddgtdbvhzloejni).
-- WGS84 (SRID 4326) — lamap_db geometry convention.
-- Populated via re-LLM FDW push (gold_ch.sync_registry → run_sync() → lamap_db_foreign.pdcom_zones → ref.pdcom_zones).

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS ref;

CREATE TABLE IF NOT EXISTS ref.pdcom_zones (
    id uuid PRIMARY KEY,
    commune_bfs integer NOT NULL,
    commune_name text,
    map_theme text NOT NULL,
    layer_slug text NOT NULL,
    layer_label text NOT NULL,
    source_color text,
    fill_type text,
    geometry geometry(Geometry, 4326) NOT NULL,
    georef_confidence numeric(4,3),
    adoption_date date,
    extracted_at timestamptz,
    updated_at timestamptz DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ref_pdcom_zones_geom ON ref.pdcom_zones USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_ref_pdcom_zones_commune_theme ON ref.pdcom_zones (commune_bfs, map_theme);
CREATE INDEX IF NOT EXISTS idx_ref_pdcom_zones_slug ON ref.pdcom_zones (layer_slug);

GRANT SELECT ON ref.pdcom_zones TO anon, authenticated, service_role;
