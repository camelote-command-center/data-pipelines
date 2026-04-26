-- v0.4: silver_ch.pdcom_zones label quality cleanup.
-- Applied to re-LLM (znrvddgmczdqoucmykij). Distribution to lamap_db is a
-- separate step after Ilan reviews — this migration ONLY touches re-LLM matviews.
--
-- Changes vs v0.2:
-- 1) Drop garbage labels (numbers/percentages/measurements, lonely parens).
-- 2) Drop truncated labels (end with dangling connector word — parser fix in v0.4
--    prevents new ones; this drops the legacy bronze rows).
-- 3) Drop GE place/neighborhood names captured as legend labels.
-- 4) Drop Lancy basemap noise (commerce, leisure, equipment usage data).
-- 5) Apply per-label theme overrides (botanical → elements_naturels, etc.).
-- 6) Apply conservative typo fixes (Résau → Réseau).
-- 7) Drop map_theme = 'unknown' rows (the rescue via theme overrides comes first).
-- 8) Cross-page geometry dedup: if the same physical zone appears on multiple
--    pages with different themes, keep the highest-confidence entry per
--    (commune_bfs, geometry_hash, normalized_label).
--
-- Quality gates that MUST hold (unchanged):
--   - georef_confidence >= 0.8 (silver gate)
--   - extraction_status = 'ok'

DROP MATERIALIZED VIEW IF EXISTS gold_ch.pdcom_zones CASCADE;
DROP MATERIALIZED VIEW IF EXISTS silver_ch.pdcom_zones CASCADE;

CREATE MATERIALIZED VIEW silver_ch.pdcom_zones AS
WITH base AS (
    SELECT
        f.id, f.commune_bfs, s.commune_name, f.map_theme,
        f.layer_slug, f.layer_label, f.source_color, f.fill_type,
        ST_MakeValid(f.geometry) AS geometry,
        f.georef_confidence, s.adoption_date, f.extracted_at,
        s.source_url, p.page_number AS source_page_number
    FROM bronze_ch.pdcom_features f
    JOIN bronze_ch.pdcom_pages p ON f.page_id = p.id
    JOIN bronze_ch.pdcom_sources s ON p.source_id = s.id
    WHERE f.georef_confidence >= 0.8
      AND p.extraction_status = 'ok'
),
cleaned AS (
    SELECT
        id, commune_bfs, commune_name,
        map_theme,
        layer_slug,
        -- v0.4: strip leading bullets/dashes and trailing separators that the
        -- v0.3.1 parser left when it added continuation-line joins (the join
        -- bypassed the original cleanup step). Belt-and-suspenders with the
        -- v0.4 parser fix.
        TRIM(BOTH ' -–—'
          FROM regexp_replace(
            regexp_replace(
              regexp_replace(layer_label, '^[\s\-/–—>·•]+', '', 'g'),
              '[\s\-/–—]+$', '', 'g'
            ),
            -- typo fixes
            '\bRésau\b', 'Réseau', 'g'
          )
        ) AS layer_label,
        source_color, fill_type, geometry, georef_confidence,
        adoption_date, extracted_at, source_url, source_page_number
    FROM base
),
filtered AS (
    SELECT *
    FROM cleaned
    WHERE
        -- Drop garbage labels: pure numbers/percentages/measurements
        layer_label !~ '^\s*\d+(?:[\.,]\d+)?\s*(?:%|cm|m|m2|m²)?(?:\s*à\s*\d+(?:[\.,]\d+)?\s*(?:%|cm|m|m2|m²)?)?\s*$'
        -- Lonely paren or bracket
        AND layer_label !~ '^[\)\]]\s*$'
        AND layer_label !~ '^\s*[\(\[].{0,3}$'
        -- Truncated: ends with connector word
        AND layer_label !~* '\s+(à|de|du|des|et|en|au|aux|par|pour|sur|sous|la|le|les|liée?s?\s+à)\s*$'
        -- v0.4: paragraph-shaped labels (A3 gate patterns: sentence break, ends-with-colon, leading bullet/dash, numeric range)
        AND layer_label !~ '\. [A-Z]'
        AND layer_label !~ ':$'
        AND layer_label !~ '^>'
        AND layer_label !~ '^[-–]\s'
        AND layer_label !~ '^\d+(\.\d+)?\s*[-–]\s*\d+'
        -- Place/neighborhood names (lowercased compare)
        AND lower(layer_label) NOT IN (
            'aubépine','aubepine',
            'oltramare',
            'genthod-bellevue','genthod bellevue',
            'creux-de-genthod','creux de genthod',
            'chambésy','chambesy',
            'bellevue-mollies','bellevue mollies',
            'mont-blanc','mont blanc',
            'pont-de-drize','pont de drize',
            'fontenette',
            'sgv',
            'campagne de st-georges','campagne de st georges'
        )
        -- Lancy-specific basemap blocklist (commune_bfs=12624)
        AND NOT (
            commune_bfs = 12624 AND (
                lower(layer_label) ILIKE '%supermarché%' OR
                lower(layer_label) ILIKE '%supermarche%' OR
                lower(layer_label) ILIKE '%boulang%' OR
                lower(layer_label) ILIKE '%boucherie%' OR
                lower(layer_label) ILIKE '%kiosque%' OR
                lower(layer_label) ILIKE '%tea-room%' OR
                lower(layer_label) ILIKE 'restaurants%' OR
                lower(layer_label) ILIKE 'restaurant - %' OR
                lower(layer_label) ILIKE '%lieu de culte%' OR
                lower(layer_label) ILIKE '%rdv amoureux%' OR
                lower(layer_label) ILIKE '%lieu des loisirs%' OR
                lower(layer_label) ILIKE '%parc préféré%' OR
                lower(layer_label) ILIKE '%case pmr%' OR
                lower(layer_label) ILIKE '%incinération%' OR
                lower(layer_label) ILIKE '%commerce de détail%' OR
                lower(layer_label) ILIKE '%type de locaux%' OR
                lower(layer_label) ILIKE '%bâtiment ou équipement%' OR
                lower(layer_label) ILIKE '%pratique associative%' OR
                lower(layer_label) ILIKE '%pratique libre%' OR
                lower(layer_label) ILIKE 'orientations de la stratégie%' OR
                lower(layer_label) ILIKE 'escrime' OR
                lower(layer_label) ILIKE 'vallon de cours d%' OR
                lower(layer_label) ILIKE 'clinique, hôpital%' OR
                lower(layer_label) ILIKE 'école, crèche%' OR
                lower(layer_label) = 'habitation' OR
                lower(layer_label) = 'bureau'
            )
        )
),
themed AS (
    -- Per-label theme override (botanical/mobility/affectation/patrimoine).
    -- Applied AFTER filtering so we don't waste work on dropped rows.
    SELECT
        id, commune_bfs, commune_name,
        CASE
            WHEN layer_label ~* '\m(forêt|forets|forêts|chêne|chenes|chênes|hêtre|tilleul|érable|peuplier|saule|saules|bois|bosquet|bosquets|arborisation|haie|haies|arbre|arbres|verger|vergers|cordon\s+boisé|continuité\s+végétale|végétation)\M'
                THEN 'elements_naturels'
            WHEN map_theme = 'unknown' AND layer_label ~* '\m(piéton|pietons|piétons|cycliste|cyclistes|cheminement|axe\s+routier|voirie|tpg|tram|bus|gare|halte)\M'
                THEN 'mobilite'
            WHEN map_theme = 'unknown' AND layer_label ~* '\m(plq|zone\s+à\s+bâtir|zone\s+a\s+batir|secteur\s+d.acquisition|périmètre\s+de\s+développement|zone\s+de\s+développement)\M'
                THEN 'affectation'
            WHEN map_theme = 'unknown' AND layer_label ~* '\m(monument|patrimoine|recensement|relevé|isos|icomos|classé|classée|inventaire|protégé|protégée)\M'
                THEN 'patrimoine_bati'
            ELSE map_theme
        END AS map_theme,
        layer_slug, layer_label, source_color, fill_type, geometry,
        georef_confidence, adoption_date, extracted_at, source_url, source_page_number
    FROM filtered
),
no_unknown AS (
    -- Drop residual unknown-theme rows (rescue via overrides above failed).
    SELECT * FROM themed WHERE map_theme <> 'unknown'
),
deduped AS (
    -- Cross-page geometry dedup: same physical zone captured on multiple pages.
    -- Keep highest-confidence row per (commune_bfs, geometry_hash, normalized_label).
    SELECT DISTINCT ON (commune_bfs, md5(ST_AsBinary(geometry)), lower(layer_label))
        id, commune_bfs, commune_name, map_theme, layer_slug, layer_label,
        source_color, fill_type, geometry, georef_confidence,
        adoption_date, extracted_at, source_url, source_page_number
    FROM no_unknown
    ORDER BY commune_bfs, md5(ST_AsBinary(geometry)), lower(layer_label),
             georef_confidence DESC, extracted_at DESC, id
)
SELECT * FROM deduped;

CREATE UNIQUE INDEX idx_silver_pdcom_zones_pk ON silver_ch.pdcom_zones (id);
CREATE INDEX idx_silver_pdcom_zones_geom ON silver_ch.pdcom_zones USING GIST (geometry);
CREATE INDEX idx_silver_pdcom_zones_commune_theme ON silver_ch.pdcom_zones (commune_bfs, map_theme);
CREATE INDEX idx_silver_pdcom_zones_slug ON silver_ch.pdcom_zones (layer_slug);

CREATE MATERIALIZED VIEW gold_ch.pdcom_zones AS
SELECT
    id, commune_bfs, commune_name, map_theme, layer_slug, layer_label,
    source_color, fill_type,
    ST_Transform(geometry, 4326) AS geometry,
    georef_confidence, adoption_date, extracted_at,
    now() AS updated_at,
    source_url, source_page_number
FROM silver_ch.pdcom_zones;

CREATE UNIQUE INDEX idx_gold_pdcom_zones_pk ON gold_ch.pdcom_zones (id);
CREATE INDEX idx_gold_pdcom_zones_geom ON gold_ch.pdcom_zones USING GIST (geometry);
CREATE INDEX idx_gold_pdcom_zones_commune_theme ON gold_ch.pdcom_zones (commune_bfs, map_theme);
CREATE INDEX idx_gold_pdcom_zones_slug ON gold_ch.pdcom_zones (layer_slug);

-- Recreate the foreign table on re-LLM (lamap_db_foreign.pdcom_zones).
-- Schema unchanged from v0.2 — this is a no-op recreate to ensure FDW points to
-- the new gold matview after the CASCADE drop.
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
