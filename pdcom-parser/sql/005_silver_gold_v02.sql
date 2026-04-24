-- v0.2: restore 0.8 silver gate (spec §5 — do NOT lower), add source_url + source_page_number.
-- Apply to re-LLM (znrvddgmczdqoucmykij).

DROP MATERIALIZED VIEW IF EXISTS gold_ch.pdcom_zones CASCADE;
DROP MATERIALIZED VIEW IF EXISTS silver_ch.pdcom_zones CASCADE;

CREATE MATERIALIZED VIEW silver_ch.pdcom_zones AS
SELECT
    f.id,
    f.commune_bfs,
    s.commune_name,
    f.map_theme,
    f.layer_slug,
    f.layer_label,
    f.source_color,
    f.fill_type,
    ST_MakeValid(f.geometry) AS geometry,       -- LV95 (SRID 2056)
    f.georef_confidence,
    s.adoption_date,
    f.extracted_at,
    s.source_url,                               -- NEW in v0.2
    p.page_number AS source_page_number         -- NEW in v0.2
FROM bronze_ch.pdcom_features f
JOIN bronze_ch.pdcom_pages p ON f.page_id = p.id
JOIN bronze_ch.pdcom_sources s ON p.source_id = s.id
WHERE f.georef_confidence >= 0.8        -- v0.2: restored from 0.5 back to spec default
  AND p.extraction_status = 'ok';

CREATE UNIQUE INDEX idx_silver_pdcom_zones_pk ON silver_ch.pdcom_zones (id);
CREATE INDEX idx_silver_pdcom_zones_geom ON silver_ch.pdcom_zones USING GIST (geometry);
CREATE INDEX idx_silver_pdcom_zones_commune_theme ON silver_ch.pdcom_zones (commune_bfs, map_theme);
CREATE INDEX idx_silver_pdcom_zones_slug ON silver_ch.pdcom_zones (layer_slug);

CREATE MATERIALIZED VIEW gold_ch.pdcom_zones AS
SELECT
    id, commune_bfs, commune_name, map_theme, layer_slug, layer_label,
    source_color, fill_type,
    ST_Transform(geometry, 4326) AS geometry,   -- WGS84 at boundary
    georef_confidence, adoption_date, extracted_at,
    now() AS updated_at,                        -- ensures column-count match with foreign table
    source_url, source_page_number
FROM silver_ch.pdcom_zones;

CREATE UNIQUE INDEX idx_gold_pdcom_zones_pk ON gold_ch.pdcom_zones (id);
CREATE INDEX idx_gold_pdcom_zones_geom ON gold_ch.pdcom_zones USING GIST (geometry);
CREATE INDEX idx_gold_pdcom_zones_commune_theme ON gold_ch.pdcom_zones (commune_bfs, map_theme);
CREATE INDEX idx_gold_pdcom_zones_slug ON gold_ch.pdcom_zones (layer_slug);

CREATE OR REPLACE FUNCTION gold_ch.refresh_pdcom_zones()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = gold_ch, silver_ch, public
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY silver_ch.pdcom_zones;
    REFRESH MATERIALIZED VIEW CONCURRENTLY gold_ch.pdcom_zones;
END;
$$;

-- Rebuild foreign table on re-LLM so lamap_db_foreign.pdcom_zones exposes the new cols
DROP FOREIGN TABLE IF EXISTS lamap_db_foreign.pdcom_zones;
CREATE FOREIGN TABLE lamap_db_foreign.pdcom_zones (
    id uuid NOT NULL,
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
    updated_at timestamptz,
    source_url text,
    source_page_number integer
) SERVER lamap_db_server OPTIONS (schema_name 'ref', table_name 'pdcom_zones');
