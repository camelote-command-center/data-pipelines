-- Apply to re-LLM. Silver matview in LV95 (gate: confidence ≥ 0.8, extraction ok).
-- Gold matview in WGS84 for distribution (LV95 → 4326 transform happens here, at boundary).

CREATE SCHEMA IF NOT EXISTS silver_ch;
CREATE SCHEMA IF NOT EXISTS gold_ch;

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
    ST_MakeValid(f.geometry) AS geometry,      -- LV95 (SRID 2056)
    f.georef_confidence,
    s.adoption_date,
    f.extracted_at
FROM bronze_ch.pdcom_features f
JOIN bronze_ch.pdcom_pages p ON f.page_id = p.id
JOIN bronze_ch.pdcom_sources s ON p.source_id = s.id
WHERE f.georef_confidence >= 0.8
  AND p.extraction_status = 'ok';

CREATE UNIQUE INDEX IF NOT EXISTS idx_silver_pdcom_zones_pk ON silver_ch.pdcom_zones (id);
CREATE INDEX IF NOT EXISTS idx_silver_pdcom_zones_geom ON silver_ch.pdcom_zones USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_silver_pdcom_zones_commune_theme ON silver_ch.pdcom_zones (commune_bfs, map_theme);
CREATE INDEX IF NOT EXISTS idx_silver_pdcom_zones_slug ON silver_ch.pdcom_zones (layer_slug);

-- Gold: distribution source. WGS84. run_sync() copies this to lamap_db.ref.pdcom_zones.
DROP MATERIALIZED VIEW IF EXISTS gold_ch.pdcom_zones CASCADE;
CREATE MATERIALIZED VIEW gold_ch.pdcom_zones AS
SELECT
    id,
    commune_bfs,
    commune_name,
    map_theme,
    layer_slug,
    layer_label,
    source_color,
    fill_type,
    ST_Transform(geometry, 4326) AS geometry,    -- WGS84 at distribution boundary
    georef_confidence,
    adoption_date,
    extracted_at
FROM silver_ch.pdcom_zones;

CREATE UNIQUE INDEX IF NOT EXISTS idx_gold_pdcom_zones_pk ON gold_ch.pdcom_zones (id);
CREATE INDEX IF NOT EXISTS idx_gold_pdcom_zones_geom ON gold_ch.pdcom_zones USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_gold_pdcom_zones_commune_theme ON gold_ch.pdcom_zones (commune_bfs, map_theme);
CREATE INDEX IF NOT EXISTS idx_gold_pdcom_zones_slug ON gold_ch.pdcom_zones (layer_slug);

-- Refresh helper: called after bronze loads finish.
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

-- Register in sync_registry: weekly full_refresh to lamap_db (and crm/lbi, though harmless for them).
INSERT INTO gold_ch.sync_registry (source_schema, source_table, target_table, pk_column, sync_mode, frequency, enabled)
VALUES ('gold_ch', 'pdcom_zones', 'pdcom_zones', 'id', 'full_refresh', 'weekly', true)
ON CONFLICT (source_schema, source_table, target_table) DO UPDATE
SET pk_column    = EXCLUDED.pk_column,
    sync_mode    = EXCLUDED.sync_mode,
    frequency    = EXCLUDED.frequency,
    enabled      = EXCLUDED.enabled;
