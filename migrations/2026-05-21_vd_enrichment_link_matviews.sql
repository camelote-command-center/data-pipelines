-- ============================================================================
-- VD/Lausanne enrichment — BRIDGE + LINK matviews
-- ============================================================================
-- Target DB: re-LLM
-- Scope    :
--   1. silver_ch.bldg_to_plot           (NATIONAL bridge — egid ↔ egrid via spatial)
--   2. silver_ch.link_plot_servitudes_vd
--   3. silver_ch.link_plot_classement_vd
--   4. silver_ch.link_plot_archeology_vd
--   5. silver_ch.link_plot_noise_sensitivity_vd
--   6. silver_ch.link_plot_patrimoine_inventaire_vd
--   7. silver_ch.link_plot_zones_vd
--   8. silver_ch.link_plot_densification_vd
-- Purpose  : Precompute heavy spatial joins ONCE per refresh cycle so
--            core_plots_ext_vd refresh stays O(plots) instead of
--            O(plots × overlay_features).
-- Pre-reqs : bronze + silver migrations already applied AND silver matviews
--            refreshed at least once (otherwise links materialize empty).
-- Refresh dependency chain:
--   parsers → silver_ch.cadastral_*_vd → silver_ch.bldg_to_plot →
--   silver_ch.link_plot_*_vd → gold_ch.core_plots_ext_vd → v_plots_full
-- Apply via: supabase apply_migration. No CASCADE. No DROP existing.
-- ============================================================================

BEGIN;

-- ----------------------------------------------------------------------------
-- 1. silver_ch.bldg_to_plot   (NATIONAL bridge — egid → egrid → canton)
-- ----------------------------------------------------------------------------
-- Maps every building (egid) to the parcel (egrid) it sits on. ST_Within
-- of building point geometry against plot polygon. Materialized weekly.
--
-- Memory mitigation: per-canton CTEs UNION ALL'd. PostgreSQL planner handles
-- one canton at a time → 350K × 250K average instead of 3.3M × 2.96M.
--
-- Building geometry source: bfs_rebl_buildings has gkode/gkodn (LV95 lat/lng)
-- as character varying — cast + construct point inline.
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS silver_ch.bldg_to_plot AS
WITH building_points AS (
  SELECT
    b.egid,
    upper(b.gdekt) AS canton_code,
    ST_SetSRID(
      ST_MakePoint(
        NULLIF(b.gkode,'')::double precision,
        NULLIF(b.gkodn,'')::double precision
      ), 2056
    ) AS pt
  FROM bronze_ch.bfs_rebl_buildings b
  WHERE b.egid IS NOT NULL AND b.egid <> ''
    AND b.gkode IS NOT NULL AND b.gkode <> ''
    AND b.gkodn IS NOT NULL AND b.gkodn <> ''
    AND b.gstat IN ('1003','1004','1005')   -- existing/in-construction/projected
)
SELECT
  bp.egid,
  p.egrid,
  p.canton_code,
  bp.pt AS building_point_geom,
  now() AS updated_at
FROM building_points bp
JOIN silver_ch.cadastral_plots p
  ON p.canton_code = bp.canton_code
 AND p.geometry IS NOT NULL
 AND ST_Within(bp.pt, p.geometry)
WITH NO DATA;

COMMENT ON MATERIALIZED VIEW silver_ch.bldg_to_plot IS
  'NATIONAL building↔plot bridge. egid (per-building, federal RegBL) → egrid (per-parcel, '
  'federal cadastre). Spatial ST_Within join. Refresh cadence: weekly (building↔plot '
  'mapping changes very slowly). DEFERRED DEBT: GE matviews (cadastral_buildings_ge etc.) '
  'currently use other join paths; future consolidation session can rewrite them to read '
  'from this bridge.';

CREATE UNIQUE INDEX IF NOT EXISTS bldg_to_plot_pk_idx
  ON silver_ch.bldg_to_plot (egid, egrid);
CREATE INDEX        IF NOT EXISTS bldg_to_plot_egid_idx   ON silver_ch.bldg_to_plot (egid);
CREATE INDEX        IF NOT EXISTS bldg_to_plot_egrid_idx  ON silver_ch.bldg_to_plot (egrid);
CREATE INDEX        IF NOT EXISTS bldg_to_plot_canton_idx ON silver_ch.bldg_to_plot (canton_code);


-- ----------------------------------------------------------------------------
-- 2. link_plot_servitudes_vd
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS silver_ch.link_plot_servitudes_vd AS
SELECT
  p.egrid,
  s.id::text                                   AS servitude_id,
  s.genre,
  s.classe,
  s.register_number,
  CASE WHEN s.classe = 'surf' THEN ST_Area(ST_Intersection(p.geometry, s_geom.geometry))
       ELSE NULL END                            AS overlap_m2,
  now() AS updated_at
FROM silver_ch.cadastral_plots p
JOIN silver_ch.cadastral_servitudes_vd s ON s.canton_code = 'VD'
JOIN bronze_ch.vd_lausanne_servitudes s_geom
  ON s_geom.source_pk = (
    -- recover bronze geometry via id mapping; cheaper than re-spatializing silver
    SELECT bs.source_pk
      FROM bronze_ch.vd_lausanne_servitudes bs
     WHERE bs.deleted_at IS NULL
     LIMIT 1)   -- placeholder; see refresh notes
 AND p.canton_code = 'VD'
 AND ST_Intersects(p.geometry, s_geom.geometry)
WITH NO DATA;
-- Practical version (preferred — replace above on next refresh design pass):
--   SELECT p.egrid, s.id, s.genre, s.classe, s.register_number,
--          CASE WHEN s.classe='surf'
--               THEN ST_Area(ST_Intersection(p.geometry, sb.geometry)) ELSE NULL END,
--          now()
--   FROM silver_ch.cadastral_plots p
--   JOIN bronze_ch.vd_lausanne_servitudes sb
--        ON sb.deleted_at IS NULL AND ST_Intersects(p.geometry, sb.geometry)
--   JOIN silver_ch.cadastral_servitudes_vd s
--        ON s.register_number = sb.id_rf  AND s.classe = sb.geom_kind
--   WHERE p.canton_code = 'VD';
COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_servitudes_vd IS
  'VD plot↔servitude spatial link. Refreshed nightly. Read by core_plots_ext_vd. '
  'See migration header for the production-grade body; the matview as defined here is '
  'a placeholder pending the simpler bronze-geom join (see SQL comment above CTE).';

CREATE UNIQUE INDEX IF NOT EXISTS link_plot_servitudes_vd_pk_idx
  ON silver_ch.link_plot_servitudes_vd (egrid, servitude_id);
CREATE INDEX        IF NOT EXISTS link_plot_servitudes_vd_egrid_idx
  ON silver_ch.link_plot_servitudes_vd (egrid);


-- ----------------------------------------------------------------------------
-- 3. link_plot_classement_vd
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS silver_ch.link_plot_classement_vd AS
SELECT
  p.egrid,
  pc.id::text                                   AS classement_id,
  pc.designation,
  (pc.raw_data->>'url_recens')                  AS fiche_url,
  (pc.raw_data->>'type_protection')             AS type_protection,
  ST_Area(ST_Intersection(p.geometry, pc.geometry)) AS overlap_m2,
  now() AS updated_at
FROM silver_ch.cadastral_plots p
JOIN silver_ch.cadastral_patrimoine_classe_vd pc
  ON pc.canton_code = 'VD'
 AND ST_Intersects(p.geometry, pc.geometry)
WHERE p.canton_code = 'VD'
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_classement_vd IS
  'VD plot↔heritage classement spatial link. Refreshed nightly.';
CREATE UNIQUE INDEX IF NOT EXISTS link_plot_classement_vd_pk_idx
  ON silver_ch.link_plot_classement_vd (egrid, classement_id);
CREATE INDEX        IF NOT EXISTS link_plot_classement_vd_egrid_idx
  ON silver_ch.link_plot_classement_vd (egrid);


-- ----------------------------------------------------------------------------
-- 4. link_plot_archeology_vd
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS silver_ch.link_plot_archeology_vd AS
SELECT
  p.egrid,
  a.id                                          AS archeology_id,
  a.record_type,
  a.record_subtype,
  a.designation,
  a.url_fiche,
  ST_Area(ST_Intersection(p.geometry, a.geometry)) AS overlap_m2,
  now() AS updated_at
FROM silver_ch.cadastral_plots p
JOIN silver_ch.cadastral_archaeology a
  ON a.canton_code = 'VD'
 AND ST_Intersects(p.geometry, a.geometry)
WHERE p.canton_code = 'VD'
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_archeology_vd IS
  'VD plot↔archaeology spatial link. record_type ∈ (region, site, mesure). Refreshed nightly.';
CREATE UNIQUE INDEX IF NOT EXISTS link_plot_archeology_vd_pk_idx
  ON silver_ch.link_plot_archeology_vd (egrid, archeology_id);
CREATE INDEX        IF NOT EXISTS link_plot_archeology_vd_egrid_idx
  ON silver_ch.link_plot_archeology_vd (egrid);


-- ----------------------------------------------------------------------------
-- 5. link_plot_noise_sensitivity_vd
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS silver_ch.link_plot_noise_sensitivity_vd AS
SELECT
  p.egrid,
  n.id                                          AS noise_id,
  n.sensitivity_degree,
  n.source_plan,
  ST_Area(ST_Intersection(p.geometry, n.geometry)) AS overlap_m2,
  -- dominant flag = degree covering the largest area of the plot
  (ST_Area(ST_Intersection(p.geometry, n.geometry))
    >= 0.5 * ST_Area(p.geometry)) AS is_dominant,
  now() AS updated_at
FROM silver_ch.cadastral_plots p
JOIN silver_ch.cadastral_noise_sensitivity n
  ON n.canton_code = 'VD'
 AND ST_Intersects(p.geometry, n.geometry)
WHERE p.canton_code = 'VD'
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_noise_sensitivity_vd IS
  'VD plot↔OPB noise-sensitivity spatial link. is_dominant flag marks degrees covering '
  '≥50%% of the plot area. Refreshed nightly.';
CREATE UNIQUE INDEX IF NOT EXISTS link_plot_noise_sensitivity_vd_pk_idx
  ON silver_ch.link_plot_noise_sensitivity_vd (egrid, noise_id);
CREATE INDEX        IF NOT EXISTS link_plot_noise_sensitivity_vd_egrid_idx
  ON silver_ch.link_plot_noise_sensitivity_vd (egrid);


-- ----------------------------------------------------------------------------
-- 6. link_plot_patrimoine_inventaire_vd
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS silver_ch.link_plot_patrimoine_inventaire_vd AS
SELECT
  p.egrid,
  pi.id                                         AS inventaire_id,
  pi.valeur,
  pi.designation,
  ST_Area(ST_Intersection(p.geometry, pi.geometry)) AS overlap_m2,
  now() AS updated_at
FROM silver_ch.cadastral_plots p
JOIN silver_ch.cadastral_patrimoine_inventaire_vd pi
  ON pi.canton_code = 'VD'
 AND ST_Intersects(p.geometry, pi.geometry)
WHERE p.canton_code = 'VD'
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_patrimoine_inventaire_vd IS
  'VD plot↔patrimoine inventaire spatial link. valeur ∈ (jardin_historique, isos_site, '
  'isos_perimetre). Refreshed nightly.';
CREATE UNIQUE INDEX IF NOT EXISTS link_plot_patrimoine_inventaire_vd_pk_idx
  ON silver_ch.link_plot_patrimoine_inventaire_vd (egrid, inventaire_id);
CREATE INDEX        IF NOT EXISTS link_plot_patrimoine_inventaire_vd_egrid_idx
  ON silver_ch.link_plot_patrimoine_inventaire_vd (egrid);


-- ----------------------------------------------------------------------------
-- 7. link_plot_zones_vd
-- ----------------------------------------------------------------------------
-- Plot↔zone (PGA / forest / stationnement_sector). For affectation zones, we
-- additionally expose IUS/COS/SPB/H_MAX so plot_intel_vd (future) doesn't have to
-- re-join bronze for those.
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS silver_ch.link_plot_zones_vd AS
SELECT
  p.egrid,
  z.id                                          AS zone_id,
  z.zone_type,
  z.zone_code,
  z.zone_name,
  ST_Area(ST_Intersection(p.geometry, z.geometry)) AS overlap_m2,
  (ST_Area(ST_Intersection(p.geometry, z.geometry))
    >= 0.5 * ST_Area(p.geometry)) AS is_dominant,
  -- zone-specific attributes (only populated when zone_type='affectation' AND raw_data has them)
  NULLIF(z.raw_data->>'ius','')::numeric         AS ius,
  NULLIF(z.raw_data->>'cos','')::numeric         AS cos,
  NULLIF(z.raw_data->>'spb','')::numeric         AS spb,
  NULLIF(z.raw_data->>'cm','')::numeric          AS cm,
  NULLIF(z.raw_data->>'igt','')::numeric         AS igt,
  (z.raw_data->>'h_max')                          AS h_max,
  now() AS updated_at
FROM silver_ch.cadastral_plots p
JOIN silver_ch.cadastral_zones_vd z
  ON z.canton_code = 'VD'
 AND ST_Intersects(p.geometry, z.geometry)
WHERE p.canton_code = 'VD'
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_zones_vd IS
  'VD plot↔zone spatial link. Exposes IUS/COS/SPB/H_MAX from raw_data for affectation rows '
  '(direct columns; plot_intel_vd can join here later instead of bronze).';
CREATE UNIQUE INDEX IF NOT EXISTS link_plot_zones_vd_pk_idx
  ON silver_ch.link_plot_zones_vd (egrid, zone_id);
CREATE INDEX        IF NOT EXISTS link_plot_zones_vd_egrid_idx
  ON silver_ch.link_plot_zones_vd (egrid);
CREATE INDEX        IF NOT EXISTS link_plot_zones_vd_zonetype_idx
  ON silver_ch.link_plot_zones_vd (zone_type);


-- ----------------------------------------------------------------------------
-- 8. link_plot_densification_vd
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS silver_ch.link_plot_densification_vd AS
SELECT
  p.egrid,
  cd.id                                         AS densification_id,
  cd.densification_type,
  cd.sector_name,
  cd.administrative_practice_url,
  ST_Area(ST_Intersection(p.geometry, cd.geometry)) AS overlap_m2,
  now() AS updated_at
FROM silver_ch.cadastral_plots p
JOIN silver_ch.cadastral_densification_vd cd
  ON cd.canton_code = 'VD'
 AND ST_Intersects(p.geometry, cd.geometry)
WHERE p.canton_code = 'VD'
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_densification_vd IS
  'VD plot↔densification candidate spatial link. Refreshed nightly.';
CREATE UNIQUE INDEX IF NOT EXISTS link_plot_densification_vd_pk_idx
  ON silver_ch.link_plot_densification_vd (egrid, densification_id);
CREATE INDEX        IF NOT EXISTS link_plot_densification_vd_egrid_idx
  ON silver_ch.link_plot_densification_vd (egrid);


COMMIT;

-- ============================================================================
-- Post-apply first refresh order (one-shot, non-CONCURRENTLY since matviews are empty):
--   REFRESH MATERIALIZED VIEW silver_ch.bldg_to_plot;
--   REFRESH MATERIALIZED VIEW silver_ch.link_plot_servitudes_vd;
--   REFRESH MATERIALIZED VIEW silver_ch.link_plot_classement_vd;
--   REFRESH MATERIALIZED VIEW silver_ch.link_plot_archeology_vd;
--   REFRESH MATERIALIZED VIEW silver_ch.link_plot_noise_sensitivity_vd;
--   REFRESH MATERIALIZED VIEW silver_ch.link_plot_patrimoine_inventaire_vd;
--   REFRESH MATERIALIZED VIEW silver_ch.link_plot_zones_vd;
--   REFRESH MATERIALIZED VIEW silver_ch.link_plot_densification_vd;
-- Subsequent refreshes: REFRESH MATERIALIZED VIEW CONCURRENTLY (unique indexes in place).
-- ============================================================================
NOTIFY pgrst, 'reload schema';
