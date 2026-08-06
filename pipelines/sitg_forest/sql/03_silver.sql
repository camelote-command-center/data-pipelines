-- ============================================================================
-- Geneva forest layers — silver on re-LLM
-- ============================================================================
-- Follows platform.standards / silver_promotion_pattern_ge_overlays:
--   bronze_ch.ge_<x>  ->  silver_ch.cadastral_<x>  ->  gold_ch.v_<x>_full  ->  ref.cadastral_<x>
--
-- Deviation from that pattern, deliberately: the standard's silver carries only
-- ST_Transform(geometry, 4326). These layers additionally carry geom_2056,
-- because every forest computation in Phase 2 is metric (areas in m2, distances
-- in m) and must never be done in 4326. The 4326 column remains the one that
-- travels to ref.* and the map; geom_2056 is what Phase 2 joins on.
--
-- Geometry is repaired here, not in bronze. Bronze holds the source verbatim.
-- Repair counts are reported by silver_ch.v_ge_forest_geometry_audit.
--
-- COMMUNE is layer-specific and the brief's "commune integers resolved against
-- the roster" only holds for the two RDPPF distance layers:
--   RDPPF_DISTANCES_FORET_S / _L  COMMUNE is a federal BFS number (6601-6645).
--                                 All 856 / 845 rows resolve against the
--                                 roster. Measured 2026-08-06: 0 unmatched.
--   FFP_LISIERES_FORESTIERES      COMMUNE is free text holding NAMES, and is
--                                 frequently multi-valued or not a commune at
--                                 all: 'Chene-Bougeries, Chene-Bourg, Thonex,
--                                 Vandoeuvres', 'Avully et Chancy',
--                                 'Vernier + Meyrin', 'Chene-Bourg/Bougeries',
--                                 'Corsier; Anieres', 'Commune de Lancy',
--                                 'Geneve Plainpalais' (a City-of-Geneva
--                                 sub-quarter, not a commune).
--                                 no_commune is therefore resolved ONLY when
--                                 the text maps to exactly one roster commune.
--                                 Otherwise it is NULL and commune_resolution
--                                 records why. Guessing here would silently
--                                 attribute a forest procedure to the wrong
--                                 commune.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS silver_ch;

-- ---------------------------------------------------------------------------
-- Commune name resolution helper
-- ---------------------------------------------------------------------------
-- Returns the federal BFS number when the free-text commune resolves to
-- exactly one roster commune, otherwise NULL. Multi-valued strings are split
-- on every separator observed in the live data: comma, semicolon, slash, plus,
-- and the French conjunction ' et '.
CREATE OR REPLACE FUNCTION silver_ch.ge_forest_commune_tokens(p_text text)
RETURNS text[]
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
  SELECT array_agg(t)
  FROM (
    SELECT btrim(tok) AS t
    FROM regexp_split_to_table(
           regexp_replace(coalesce(p_text, ''), '\s+et\s+', '|', 'gi'),
           '[|,;/+]'
         ) AS tok
    WHERE btrim(tok) <> ''
  ) s;
$$;

CREATE OR REPLACE FUNCTION silver_ch.ge_forest_resolve_commune(p_text text)
RETURNS integer
LANGUAGE sql
STABLE
PARALLEL SAFE
AS $$
  WITH toks AS (
    SELECT unnest(silver_ch.ge_forest_commune_tokens(p_text)) AS t
  ),
  hits AS (
    SELECT DISTINCT c.no_com_federal::int AS bfs
    FROM toks
    JOIN bronze_ch.ge_cad_communes c
      ON lower(public.unaccent(c.commune)) = lower(public.unaccent(toks.t))
  )
  SELECT CASE WHEN count(*) = 1 THEN min(bfs) ELSE NULL END FROM hits;
$$;

CREATE OR REPLACE FUNCTION silver_ch.ge_forest_commune_resolution(p_text text)
RETURNS text
LANGUAGE sql
STABLE
PARALLEL SAFE
AS $$
  WITH toks AS (
    SELECT unnest(silver_ch.ge_forest_commune_tokens(p_text)) AS t
  ),
  hits AS (
    SELECT DISTINCT c.no_com_federal::int AS bfs
    FROM toks
    JOIN bronze_ch.ge_cad_communes c
      ON lower(public.unaccent(c.commune)) = lower(public.unaccent(toks.t))
  )
  SELECT CASE
           WHEN p_text IS NULL OR btrim(p_text) = '' THEN 'absent'
           WHEN (SELECT count(*) FROM hits) = 1 THEN 'single'
           WHEN (SELECT count(*) FROM hits) > 1 THEN 'multiple'
           ELSE 'unmatched'
         END;
$$;

-- ---------------------------------------------------------------------------
-- Canonical commune roster, one row per federal BFS number
-- ---------------------------------------------------------------------------
-- bronze_ch.ge_cad_communes has 48 rows but only 45 distinct federal numbers:
-- BFS 6621 (Ville de Geneve) is split into four CADASTRAL sub-communes
-- (Geneve-Cite, Geneve-Eaux-Vives, Geneve-Petit-Saconnex, Geneve-Plainpalais).
-- Joining the RDPPF layers straight onto no_com_federal therefore fans every
-- Ville-de-Geneve row out four times: the first build of
-- cadastral_forest_distance_s produced 1'027 rows from 856 bronze rows and the
-- unique index refused it. This view collapses the subdivisions back to the
-- commune, which is what a BFS number actually identifies.
CREATE OR REPLACE VIEW silver_ch.v_ge_commune_roster AS
SELECT
  no_com_federal::int                       AS commune_bfs,
  CASE WHEN count(*) > 1
       THEN split_part(min(commune), '-', 1)   -- 'Geneve-Cite' -> 'Geneve'
       ELSE min(commune)
  END                                       AS commune_name,
  count(*)                                  AS cadastral_subdivisions
FROM bronze_ch.ge_cad_communes
WHERE no_com_federal IS NOT NULL
GROUP BY no_com_federal;

-- ---------------------------------------------------------------------------
-- Validity repair helpers
-- ---------------------------------------------------------------------------
-- ST_MakeValid on a self-intersecting ring frequently returns a
-- GeometryCollection mixing polygons with the degenerate lines and points that
-- fall out of the repair. Assigning that straight to a MultiPolygon column
-- fails with "Geometry type (GeometryCollection) does not match column type".
-- ST_CollectionExtract keeps only the dimension we actually want: 3 for
-- polygons, 2 for lines. The discarded fragments are zero-area or zero-length
-- and carry no forest extent.
CREATE OR REPLACE FUNCTION silver_ch.ge_forest_valid_polygon(p_geom public.geometry)
RETURNS public.geometry(MultiPolygon, 2056)
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
  SELECT public.ST_Multi(public.ST_CollectionExtract(public.ST_MakeValid(p_geom), 3))::public.geometry(MultiPolygon, 2056);
$$;

CREATE OR REPLACE FUNCTION silver_ch.ge_forest_valid_line(p_geom public.geometry)
RETURNS public.geometry(MultiLineString, 2056)
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
  SELECT public.ST_Multi(public.ST_CollectionExtract(public.ST_MakeValid(p_geom), 2))::public.geometry(MultiLineString, 2056);
$$;

-- ---------------------------------------------------------------------------
-- 1. Cadastre forestier
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS silver_ch.cadastral_forest_cadastre CASCADE;
CREATE MATERIALIZED VIEW silver_ch.cadastral_forest_cadastre AS
SELECT
  b.geom_hash,
  b.objectid,
  b.remarque,
  b.shape_area                                                    AS source_area_m2,
  b.shape_length                                                  AS source_length_m,
  ST_Area(silver_ch.ge_forest_valid_polygon(b.geometry))                               AS area_m2,
  silver_ch.ge_forest_valid_polygon(b.geometry) AS geom_2056,
  ST_Transform(silver_ch.ge_forest_valid_polygon(b.geometry), 4326)::geometry(MultiPolygon,4326) AS geometry,
  'GE'::text                                                      AS canton_code,
  b.raw_data,
  now()                                                           AS updated_at
FROM bronze_ch.ge_ffp_cadastre_foret b
WHERE b.deleted_at IS NULL
  AND b.geometry IS NOT NULL;

CREATE UNIQUE INDEX cadastral_forest_cadastre_pk
  ON silver_ch.cadastral_forest_cadastre (geom_hash);
CREATE INDEX cadastral_forest_cadastre_gix
  ON silver_ch.cadastral_forest_cadastre USING GIST (geometry);
CREATE INDEX cadastral_forest_cadastre_gix_2056
  ON silver_ch.cadastral_forest_cadastre USING GIST (geom_2056);

-- ---------------------------------------------------------------------------
-- 2. RDPPF distances forêt — surface
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS silver_ch.cadastral_forest_distance_s CASCADE;
CREATE MATERIALIZED VIEW silver_ch.cadastral_forest_distance_s AS
SELECT
  b.erebid,
  b.geom_hash,
  b.objectid,
  b.commune                                                       AS commune_bfs,
  c.commune_name,
  b.statut_juridique,
  b.entree_en_force_date,
  b.lien_document,
  b.lien_plan,
  b.date_maj,
  b.shape_area                                                    AS source_area_m2,
  ST_Area(silver_ch.ge_forest_valid_polygon(b.geometry))                               AS area_m2,
  silver_ch.ge_forest_valid_polygon(b.geometry) AS geom_2056,
  ST_Transform(silver_ch.ge_forest_valid_polygon(b.geometry), 4326)::geometry(MultiPolygon,4326) AS geometry,
  'GE'::text                                                      AS canton_code,
  b.raw_data,
  now()                                                           AS updated_at
FROM bronze_ch.ge_rdppf_distances_foret_s b
LEFT JOIN silver_ch.v_ge_commune_roster c ON c.commune_bfs = b.commune
WHERE b.deleted_at IS NULL
  AND b.geometry IS NOT NULL;

CREATE UNIQUE INDEX cadastral_forest_distance_s_pk
  ON silver_ch.cadastral_forest_distance_s (erebid, geom_hash);
CREATE INDEX cadastral_forest_distance_s_gix
  ON silver_ch.cadastral_forest_distance_s USING GIST (geometry);
CREATE INDEX cadastral_forest_distance_s_gix_2056
  ON silver_ch.cadastral_forest_distance_s USING GIST (geom_2056);

-- ---------------------------------------------------------------------------
-- 3. RDPPF distances forêt — ligne
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS silver_ch.cadastral_forest_distance_l CASCADE;
CREATE MATERIALIZED VIEW silver_ch.cadastral_forest_distance_l AS
SELECT
  b.erebid,
  b.geom_hash,
  b.objectid,
  b.commune                                                          AS commune_bfs,
  c.commune_name,
  b.statut_juridique,
  b.entree_en_force_date,
  b.lien_document,
  b.lien_plan,
  b.date_maj,
  b.shape_length                                                     AS source_length_m,
  ST_Length(silver_ch.ge_forest_valid_line(b.geometry))                                AS length_m,
  silver_ch.ge_forest_valid_line(b.geometry) AS geom_2056,
  ST_Transform(silver_ch.ge_forest_valid_line(b.geometry), 4326)::geometry(MultiLineString,4326) AS geometry,
  'GE'::text                                                         AS canton_code,
  b.raw_data,
  now()                                                              AS updated_at
FROM bronze_ch.ge_rdppf_distances_foret_l b
LEFT JOIN silver_ch.v_ge_commune_roster c ON c.commune_bfs = b.commune
WHERE b.deleted_at IS NULL
  AND b.geometry IS NOT NULL;

CREATE UNIQUE INDEX cadastral_forest_distance_l_pk
  ON silver_ch.cadastral_forest_distance_l (erebid, geom_hash);
CREATE INDEX cadastral_forest_distance_l_gix
  ON silver_ch.cadastral_forest_distance_l USING GIST (geometry);
CREATE INDEX cadastral_forest_distance_l_gix_2056
  ON silver_ch.cadastral_forest_distance_l USING GIST (geom_2056);

-- ---------------------------------------------------------------------------
-- 4. Lisières forestières
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS silver_ch.cadastral_forest_lisieres CASCADE;
CREATE MATERIALIZED VIEW silver_ch.cadastral_forest_lisieres AS
SELECT
  b.id_dossier_key,
  b.geom_hash,
  b.id_dossier,
  b.objectid,
  b.type_procedure,
  b.commune                                                          AS commune_raw,
  silver_ch.ge_forest_resolve_commune(b.commune)                     AS commune_bfs,
  silver_ch.ge_forest_commune_resolution(b.commune)                  AS commune_resolution,
  b.parcelles                                                        AS parcelles_raw,
  b.num_autor,
  b.mz_plq,
  b.relev_etat,
  b.relev_date,
  b.etape_procedure,
  b.etat_dossier,
  b.statut_juridique,
  b.dec_natfor,
  -- Decision "forest by nature" recorded as a boolean alongside the source text
  CASE lower(coalesce(b.dec_natfor, ''))
    WHEN 'oui' THEN true
    WHEN 'non' THEN false
    ELSE NULL
  END                                                                AS dec_natfor_bool,
  b.fao_requete_date,
  b.fao_decision_date,
  b.entree_en_force_date,
  -- A procedure counts as in force only once entree_en_force_date has passed.
  (b.entree_en_force_date IS NOT NULL AND b.entree_en_force_date <= current_date)
                                                                     AS in_force,
  b.recours,
  b.rdppf_statut,
  b.lien_document,
  b.shape_length                                                     AS source_length_m,
  ST_Length(silver_ch.ge_forest_valid_line(b.geometry))                                AS length_m,
  silver_ch.ge_forest_valid_line(b.geometry) AS geom_2056,
  ST_Transform(silver_ch.ge_forest_valid_line(b.geometry), 4326)::geometry(MultiLineString,4326) AS geometry,
  'GE'::text                                                         AS canton_code,
  b.raw_data,
  now()                                                              AS updated_at
FROM bronze_ch.ge_ffp_lisieres_forestieres b
WHERE b.deleted_at IS NULL
  AND b.geometry IS NOT NULL;

CREATE UNIQUE INDEX cadastral_forest_lisieres_pk
  ON silver_ch.cadastral_forest_lisieres (id_dossier_key, geom_hash);
CREATE INDEX cadastral_forest_lisieres_gix
  ON silver_ch.cadastral_forest_lisieres USING GIST (geometry);
CREATE INDEX cadastral_forest_lisieres_gix_2056
  ON silver_ch.cadastral_forest_lisieres USING GIST (geom_2056);
CREATE INDEX cadastral_forest_lisieres_num_autor_idx
  ON silver_ch.cadastral_forest_lisieres (num_autor) WHERE num_autor IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 5. Fonction plan directeur forestier (optional layer)
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS silver_ch.cadastral_forest_fonction CASCADE;
CREATE MATERIALIZED VIEW silver_ch.cadastral_forest_fonction AS
SELECT
  b.geom_hash,
  b.objectid,
  b.type                                                          AS fonction_type,
  b.shape_area                                                    AS source_area_m2,
  ST_Area(silver_ch.ge_forest_valid_polygon(b.geometry))                               AS area_m2,
  silver_ch.ge_forest_valid_polygon(b.geometry) AS geom_2056,
  ST_Transform(silver_ch.ge_forest_valid_polygon(b.geometry), 4326)::geometry(MultiPolygon,4326) AS geometry,
  'GE'::text                                                      AS canton_code,
  b.raw_data,
  now()                                                           AS updated_at
FROM bronze_ch.ge_ffp_fonction_pdf b
WHERE b.deleted_at IS NULL
  AND b.geometry IS NOT NULL;

CREATE UNIQUE INDEX cadastral_forest_fonction_pk
  ON silver_ch.cadastral_forest_fonction (geom_hash);
CREATE INDEX cadastral_forest_fonction_gix
  ON silver_ch.cadastral_forest_fonction USING GIST (geometry);
CREATE INDEX cadastral_forest_fonction_gix_2056
  ON silver_ch.cadastral_forest_fonction USING GIST (geom_2056);

-- ---------------------------------------------------------------------------
-- Geometry validity audit (verification item 4)
-- ---------------------------------------------------------------------------
-- "Before" is measured on bronze, which holds the source verbatim. "After" is
-- measured on silver, where ST_MakeValid has been applied. After must be zero.
CREATE OR REPLACE VIEW silver_ch.v_ge_forest_geometry_audit AS
SELECT 'cadastral_forest_cadastre'::text AS layer,
       (SELECT count(*) FROM bronze_ch.ge_ffp_cadastre_foret
         WHERE geometry IS NOT NULL AND NOT ST_IsValid(geometry))          AS invalid_before,
       (SELECT count(*) FROM silver_ch.cadastral_forest_cadastre
         WHERE NOT ST_IsValid(geom_2056))                                  AS invalid_after,
       (SELECT count(*) FROM silver_ch.cadastral_forest_cadastre)          AS rows_silver
UNION ALL
SELECT 'cadastral_forest_distance_s',
       (SELECT count(*) FROM bronze_ch.ge_rdppf_distances_foret_s
         WHERE geometry IS NOT NULL AND NOT ST_IsValid(geometry)),
       (SELECT count(*) FROM silver_ch.cadastral_forest_distance_s
         WHERE NOT ST_IsValid(geom_2056)),
       (SELECT count(*) FROM silver_ch.cadastral_forest_distance_s)
UNION ALL
SELECT 'cadastral_forest_distance_l',
       (SELECT count(*) FROM bronze_ch.ge_rdppf_distances_foret_l
         WHERE geometry IS NOT NULL AND NOT ST_IsValid(geometry)),
       (SELECT count(*) FROM silver_ch.cadastral_forest_distance_l
         WHERE NOT ST_IsValid(geom_2056)),
       (SELECT count(*) FROM silver_ch.cadastral_forest_distance_l)
UNION ALL
SELECT 'cadastral_forest_lisieres',
       (SELECT count(*) FROM bronze_ch.ge_ffp_lisieres_forestieres
         WHERE geometry IS NOT NULL AND NOT ST_IsValid(geometry)),
       (SELECT count(*) FROM silver_ch.cadastral_forest_lisieres
         WHERE NOT ST_IsValid(geom_2056)),
       (SELECT count(*) FROM silver_ch.cadastral_forest_lisieres)
UNION ALL
SELECT 'cadastral_forest_fonction',
       (SELECT count(*) FROM bronze_ch.ge_ffp_fonction_pdf
         WHERE geometry IS NOT NULL AND NOT ST_IsValid(geometry)),
       (SELECT count(*) FROM silver_ch.cadastral_forest_fonction
         WHERE NOT ST_IsValid(geom_2056)),
       (SELECT count(*) FROM silver_ch.cadastral_forest_fonction);
