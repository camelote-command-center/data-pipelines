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
--
-- SRID convention discovered during initial apply (fix recorded here):
--   silver_ch.cadastral_plots.geometry   = EPSG:4326 (WGS84 lat/lng)
--   bronze_ch.vd_* / bfs_rebl_buildings  = EPSG:2056 (LV95 meters)
--   ST_Within / ST_Intersects across SRIDs raises in PostGIS.
--   Fix: ST_Transform inputs to 4326 inside the spatial predicates. Store
--        original 2056 geometry where stored, for downstream meter-unit ops.
-- ============================================================================

BEGIN;

-- ----------------------------------------------------------------------------
-- 1. silver_ch.bldg_to_plot   (NATIONAL bridge — egid → egrid → canton)
-- ----------------------------------------------------------------------------
-- Building point built in EPSG:2056 (LV95 meters, RegBL gkode/gkodn). Transformed
-- to 4326 inline for the ST_Within against cadastral_plots (4326). The stored
-- building_point_geom remains in 2056 so downstream distance ops (ST_DWithin in
-- meters) work without re-projection. Memory mitigation: planner handles per-canton
-- batches naturally via the canton_code equality join.
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
    ) AS pt_2056
  FROM bronze_ch.bfs_rebl_buildings b
  WHERE b.egid IS NOT NULL AND b.egid <> ''
    AND b.gkode IS NOT NULL AND b.gkode <> ''
    AND b.gkodn IS NOT NULL AND b.gkodn <> ''
    AND b.gstat IN ('1003','1004','1005')   -- existing/in-construction/projected
),
building_points_4326 AS (
  SELECT egid, canton_code, pt_2056,
         ST_Transform(pt_2056, 4326) AS pt_4326
    FROM building_points
)
SELECT
  bp.egid,
  p.egrid,
  p.canton_code,
  bp.pt_2056 AS building_point_geom,
  now() AS updated_at
FROM building_points_4326 bp
JOIN silver_ch.cadastral_plots p
  ON p.canton_code = bp.canton_code
 AND p.geometry IS NOT NULL
 AND ST_Within(bp.pt_4326, p.geometry)
WITH NO DATA;

COMMENT ON MATERIALIZED VIEW silver_ch.bldg_to_plot IS
  'NATIONAL building↔plot bridge. egid (per-building, federal RegBL) → egrid (per-parcel, '
  'federal cadastre). Spatial ST_Within join (point transformed to 4326 inline; plot is '
  '4326). Stored building_point_geom is EPSG:2056 (LV95 meters) for downstream distance ops. '
  'Refresh cadence: weekly. DEFERRED DEBT: GE matviews currently use other join paths; '
  'future consolidation can rewrite them to read from this bridge.';

CREATE UNIQUE INDEX IF NOT EXISTS bldg_to_plot_pk_idx
  ON silver_ch.bldg_to_plot (egid, egrid);
CREATE INDEX        IF NOT EXISTS bldg_to_plot_egid_idx   ON silver_ch.bldg_to_plot (egid);
CREATE INDEX        IF NOT EXISTS bldg_to_plot_egrid_idx  ON silver_ch.bldg_to_plot (egrid);
CREATE INDEX        IF NOT EXISTS bldg_to_plot_canton_idx ON silver_ch.bldg_to_plot (canton_code);


-- ----------------------------------------------------------------------------
-- 2. link_plot_servitudes_vd
-- ----------------------------------------------------------------------------
-- cadastral_plots is 4326; vd_lausanne_servitudes is 2056. ST_Transform the
-- bronze geometry to 4326 for the ST_Intersects. For area, transform plot to
-- 2056 (meter units) so ST_Area returns m².
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS silver_ch.link_plot_servitudes_vd AS
SELECT
  p.egrid,
  s.id::text                                        AS servitude_id,
  s.genre,
  s.classe,
  s.register_number,
  CASE WHEN s.classe = 'surf'
       THEN ST_Area(ST_Intersection(ST_Transform(p.geometry, 2056), sb.geometry))::numeric
       ELSE NULL END                                  AS overlap_m2,
  now() AS updated_at
FROM silver_ch.cadastral_plots p
JOIN bronze_ch.vd_lausanne_servitudes sb
  ON sb.deleted_at IS NULL
 AND ST_Intersects(p.geometry, ST_Transform(sb.geometry, 4326))
JOIN silver_ch.cadastral_servitudes_vd s
  ON s.register_number = sb.id_rf
 AND s.classe          = sb.geom_kind
WHERE p.canton_code = 'VD'
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_servitudes_vd IS
  'VD plot↔servitude spatial link. Refreshed nightly. cadastral_plots is 4326, bronze '
  'geometry is 2056; ST_Transform inline. overlap_m2 computed in 2056 (meters).';

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
  pc.id::text                                        AS classement_id,
  pc.designation,
  (pc.raw_data->>'url_recens')                        AS fiche_url,
  (pc.raw_data->>'type_protection')                   AS type_protection,
  ST_Area(ST_Intersection(ST_Transform(p.geometry, 2056), pc.geometry))::numeric AS overlap_m2,
  now() AS updated_at
FROM silver_ch.cadastral_plots p
JOIN silver_ch.cadastral_patrimoine_classe_vd pc
  ON pc.canton_code = 'VD'
 AND ST_Intersects(p.geometry, ST_Transform(pc.geometry, 4326))
WHERE p.canton_code = 'VD'
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_classement_vd IS
  'VD plot↔heritage classement spatial link. overlap_m2 in 2056 (meters).';
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
  a.id                                               AS archeology_id,
  a.record_type,
  a.record_subtype,
  a.designation,
  a.url_fiche,
  ST_Area(ST_Intersection(ST_Transform(p.geometry, 2056), a.geometry))::numeric AS overlap_m2,
  now() AS updated_at
FROM silver_ch.cadastral_plots p
JOIN silver_ch.cadastral_archaeology a
  ON a.canton_code = 'VD'
 AND ST_Intersects(p.geometry, ST_Transform(a.geometry, 4326))
WHERE p.canton_code = 'VD'
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_archeology_vd IS
  'VD plot↔archaeology spatial link. record_type ∈ (region, site, mesure). overlap_m2 in 2056.';
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
  n.id                                               AS noise_id,
  n.sensitivity_degree,
  n.source_plan,
  ST_Area(ST_Intersection(ST_Transform(p.geometry, 2056), n.geometry))::numeric AS overlap_m2,
  (ST_Area(ST_Intersection(ST_Transform(p.geometry, 2056), n.geometry))
    >= 0.5 * ST_Area(ST_Transform(p.geometry, 2056))) AS is_dominant,
  now() AS updated_at
FROM silver_ch.cadastral_plots p
JOIN silver_ch.cadastral_noise_sensitivity n
  ON n.canton_code = 'VD'
 AND ST_Intersects(p.geometry, ST_Transform(n.geometry, 4326))
WHERE p.canton_code = 'VD'
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_noise_sensitivity_vd IS
  'VD plot↔OPB noise-sensitivity spatial link. is_dominant flag = degrees covering ≥50%% '
  'of the plot area. Areas computed in 2056 (meters).';
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
  pi.id                                              AS inventaire_id,
  pi.valeur,
  pi.designation,
  ST_Area(ST_Intersection(ST_Transform(p.geometry, 2056), pi.geometry))::numeric AS overlap_m2,
  now() AS updated_at
FROM silver_ch.cadastral_plots p
JOIN silver_ch.cadastral_patrimoine_inventaire_vd pi
  ON pi.canton_code = 'VD'
 AND ST_Intersects(p.geometry, ST_Transform(pi.geometry, 4326))
WHERE p.canton_code = 'VD'
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_patrimoine_inventaire_vd IS
  'VD plot↔patrimoine inventaire spatial link. valeur ∈ (jardin_historique, isos_site, '
  'isos_perimetre). overlap_m2 in 2056.';
CREATE UNIQUE INDEX IF NOT EXISTS link_plot_patrimoine_inventaire_vd_pk_idx
  ON silver_ch.link_plot_patrimoine_inventaire_vd (egrid, inventaire_id);
CREATE INDEX        IF NOT EXISTS link_plot_patrimoine_inventaire_vd_egrid_idx
  ON silver_ch.link_plot_patrimoine_inventaire_vd (egrid);


-- ----------------------------------------------------------------------------
-- 7. link_plot_zones_vd
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS silver_ch.link_plot_zones_vd AS
SELECT
  p.egrid,
  z.id                                               AS zone_id,
  z.zone_type,
  z.zone_code,
  z.zone_name,
  ST_Area(ST_Intersection(ST_Transform(p.geometry, 2056), z.geometry))::numeric AS overlap_m2,
  (ST_Area(ST_Intersection(ST_Transform(p.geometry, 2056), z.geometry))
    >= 0.5 * ST_Area(ST_Transform(p.geometry, 2056))) AS is_dominant,
  NULLIF(z.raw_data->>'ius','')::numeric             AS ius,
  NULLIF(z.raw_data->>'cos','')::numeric             AS cos,
  NULLIF(z.raw_data->>'spb','')::numeric             AS spb,
  NULLIF(z.raw_data->>'cm','')::numeric              AS cm,
  NULLIF(z.raw_data->>'igt','')::numeric             AS igt,
  (z.raw_data->>'h_max')                              AS h_max,
  now() AS updated_at
FROM silver_ch.cadastral_plots p
JOIN silver_ch.cadastral_zones_vd z
  ON z.canton_code = 'VD'
 AND ST_Intersects(p.geometry, ST_Transform(z.geometry, 4326))
WHERE p.canton_code = 'VD'
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_zones_vd IS
  'VD plot↔zone spatial link. Exposes IUS/COS/SPB/H_MAX from raw_data for affectation rows. '
  'overlap_m2 in 2056 (meters).';
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
  cd.id                                              AS densification_id,
  cd.densification_type,
  cd.sector_name,
  cd.administrative_practice_url,
  ST_Area(ST_Intersection(ST_Transform(p.geometry, 2056), cd.geometry))::numeric AS overlap_m2,
  now() AS updated_at
FROM silver_ch.cadastral_plots p
JOIN silver_ch.cadastral_densification_vd cd
  ON cd.canton_code = 'VD'
 AND ST_Intersects(p.geometry, ST_Transform(cd.geometry, 4326))
WHERE p.canton_code = 'VD'
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_densification_vd IS
  'VD plot↔densification candidate spatial link. overlap_m2 in 2056.';
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
--
-- Sanity assertion (added 2026-05-21 after the v1 silent SRID-mismatch incident):
--   The step-4 wrapper applies a DO block asserting silver_ch.bldg_to_plot >= 2.5M rows
--   after the first REFRESH. If the matview body silently yields too few rows again, the
--   transaction rolls back instead of leaving us with empty data masquerading as success.
-- ============================================================================
NOTIFY pgrst, 'reload schema';
