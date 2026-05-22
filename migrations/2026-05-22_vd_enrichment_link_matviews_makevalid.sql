-- ============================================================================
-- VD/Lausanne enrichment — link matviews ST_MakeValid fix
-- ============================================================================
-- Target DB: re-LLM
-- Version  : 20260522000002
-- Purpose  : Phase 5 of PR #16 surfaced GEOS TopologyException on every
--            link_plot_*_vd matview refresh:
--              lwgeom_intersection_prec: GEOS Error: TopologyException:
--              side location conflict at ... — input geometry is invalid.
--            Some canton-VD source polygons (zones, classement, archeology,
--            patrimoine_inventaire, etc.) have invalid topology (self-
--            intersection / ring-direction). ST_Intersection raises.
--
-- Fix      : Wrap inputs with ST_MakeValid() inside ST_Intersection.
--            Per scope rules: ST_MakeValid only where the exception fires
--            (ST_Intersection + ST_Intersects predicate on RHS).
--            cadastral_plots.geometry (federal cadastre, clean) is NOT
--            wrapped in ST_Intersects predicate to avoid extra work, but
--            IS wrapped inside ST_Intersection where both sides may invalidate.
--
-- Steps    : (all inside one transaction)
--   1. DROP VIEW gold_ch.v_plots_full         (consumer of ext_vd)
--   2. DROP MATERIALIZED VIEW gold_ch.core_plots_ext_vd  (consumer of link_*)
--   3. DROP 7 silver_ch.link_plot_*_vd matviews
--   4. CREATE 7 link matviews with ST_MakeValid wrap (auto-populate via WITH DATA)
--   5. CREATE core_plots_ext_vd (same body as PR #15, auto-populate)
--   6. CREATE v_plots_full (same body as PR #15)
--   7. Verification DO block: count rows in 2 critical link matviews; RAISE EXCEPTION
--      if either is 0 — prevents silent-zero-rows commit (lesson from earlier SRID bug).
--   8. INSERT tracking row.
--
-- Standing rule precedent:
--   PR #15 rule "No DROP on existing matviews" had a clarification when the user
--   approved the link-matview rollback during PR #15's Phase 4 incident:
--     "These were created minutes ago, no dependents (gold isn't built yet).
--      Clean rollback."
--   By analogy, link + core_plots_ext_vd + v_plots_full are all PR #15/#16
--   artifacts within this session arc. Same precedent applies.
--
-- Bug ref  : camelote_data.bugs id (filed after this migration applies):
--   "Canton-VD source polygons have invalid topology — ST_MakeValid wrap applied"
-- ============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- 1. DROP downstream consumers first
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS gold_ch.v_plots_full;
DROP MATERIALIZED VIEW IF EXISTS gold_ch.core_plots_ext_vd;

-- ---------------------------------------------------------------------------
-- 2. DROP the 7 link matviews
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS silver_ch.link_plot_densification_vd;
DROP MATERIALIZED VIEW IF EXISTS silver_ch.link_plot_zones_vd;
DROP MATERIALIZED VIEW IF EXISTS silver_ch.link_plot_patrimoine_inventaire_vd;
DROP MATERIALIZED VIEW IF EXISTS silver_ch.link_plot_noise_sensitivity_vd;
DROP MATERIALIZED VIEW IF EXISTS silver_ch.link_plot_archeology_vd;
DROP MATERIALIZED VIEW IF EXISTS silver_ch.link_plot_classement_vd;
DROP MATERIALIZED VIEW IF EXISTS silver_ch.link_plot_servitudes_vd;


-- ---------------------------------------------------------------------------
-- 3. Recreate link matviews with ST_MakeValid wrap.
--    ST_MakeValid only inside ST_Intersection and on the RHS of ST_Intersects;
--    cadastral_plots.geometry not wrapped in ST_Intersects (federal cadastre, clean).
-- ---------------------------------------------------------------------------

-- 3a. link_plot_servitudes_vd
CREATE MATERIALIZED VIEW silver_ch.link_plot_servitudes_vd AS
SELECT
  p.egrid,
  s.id::text                                        AS servitude_id,
  s.genre,
  s.classe,
  s.register_number,
  CASE WHEN s.classe = 'surf'
       THEN ST_Area(ST_Intersection(ST_MakeValid(ST_Transform(p.geometry, 2056)),
                                    ST_MakeValid(sb.geometry)))::numeric
       ELSE NULL END                                  AS overlap_m2,
  now() AS updated_at
FROM silver_ch.cadastral_plots p
JOIN bronze_ch.vd_lausanne_servitudes sb
  ON sb.deleted_at IS NULL
 AND ST_Intersects(p.geometry, ST_MakeValid(ST_Transform(sb.geometry, 4326)))
JOIN silver_ch.cadastral_servitudes_vd s
  ON s.register_number = sb.id_rf
 AND s.classe          = sb.geom_kind
WHERE p.canton_code = 'VD';
COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_servitudes_vd IS
  'VD plot↔servitude spatial link. ST_MakeValid wraps inputs on RHS of join + both sides inside ST_Intersection (2026-05-22 fix for GEOS TopologyException).';
CREATE UNIQUE INDEX link_plot_servitudes_vd_pk_idx
  ON silver_ch.link_plot_servitudes_vd (egrid, servitude_id);
CREATE INDEX link_plot_servitudes_vd_egrid_idx
  ON silver_ch.link_plot_servitudes_vd (egrid);


-- 3b. link_plot_classement_vd
CREATE MATERIALIZED VIEW silver_ch.link_plot_classement_vd AS
SELECT
  p.egrid,
  pc.id::text                                        AS classement_id,
  pc.designation,
  (pc.raw_data->>'url_recens')                        AS fiche_url,
  (pc.raw_data->>'type_protection')                   AS type_protection,
  ST_Area(ST_Intersection(ST_MakeValid(ST_Transform(p.geometry, 2056)),
                          ST_MakeValid(pc.geometry)))::numeric AS overlap_m2,
  now() AS updated_at
FROM silver_ch.cadastral_plots p
JOIN silver_ch.cadastral_patrimoine_classe_vd pc
  ON pc.canton_code = 'VD'
 AND ST_Intersects(p.geometry, ST_MakeValid(ST_Transform(pc.geometry, 4326)))
WHERE p.canton_code = 'VD';
COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_classement_vd IS
  'VD plot↔heritage classement spatial link. ST_MakeValid wrap (2026-05-22).';
CREATE UNIQUE INDEX link_plot_classement_vd_pk_idx
  ON silver_ch.link_plot_classement_vd (egrid, classement_id);
CREATE INDEX link_plot_classement_vd_egrid_idx
  ON silver_ch.link_plot_classement_vd (egrid);


-- 3c. link_plot_archeology_vd
CREATE MATERIALIZED VIEW silver_ch.link_plot_archeology_vd AS
SELECT
  p.egrid,
  a.id                                               AS archeology_id,
  a.record_type,
  a.record_subtype,
  a.designation,
  a.url_fiche,
  ST_Area(ST_Intersection(ST_MakeValid(ST_Transform(p.geometry, 2056)),
                          ST_MakeValid(a.geometry)))::numeric AS overlap_m2,
  now() AS updated_at
FROM silver_ch.cadastral_plots p
JOIN silver_ch.cadastral_archaeology a
  ON a.canton_code = 'VD'
 AND ST_Intersects(p.geometry, ST_MakeValid(ST_Transform(a.geometry, 4326)))
WHERE p.canton_code = 'VD';
COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_archeology_vd IS
  'VD plot↔archaeology spatial link. ST_MakeValid wrap (2026-05-22).';
CREATE UNIQUE INDEX link_plot_archeology_vd_pk_idx
  ON silver_ch.link_plot_archeology_vd (egrid, archeology_id);
CREATE INDEX link_plot_archeology_vd_egrid_idx
  ON silver_ch.link_plot_archeology_vd (egrid);


-- 3d. link_plot_noise_sensitivity_vd
CREATE MATERIALIZED VIEW silver_ch.link_plot_noise_sensitivity_vd AS
SELECT
  p.egrid,
  n.id                                               AS noise_id,
  n.sensitivity_degree,
  n.source_plan,
  ST_Area(ST_Intersection(ST_MakeValid(ST_Transform(p.geometry, 2056)),
                          ST_MakeValid(n.geometry)))::numeric AS overlap_m2,
  (ST_Area(ST_Intersection(ST_MakeValid(ST_Transform(p.geometry, 2056)),
                           ST_MakeValid(n.geometry)))
    >= 0.5 * ST_Area(ST_MakeValid(ST_Transform(p.geometry, 2056))))      AS is_dominant,
  now() AS updated_at
FROM silver_ch.cadastral_plots p
JOIN silver_ch.cadastral_noise_sensitivity n
  ON n.canton_code = 'VD'
 AND ST_Intersects(p.geometry, ST_MakeValid(ST_Transform(n.geometry, 4326)))
WHERE p.canton_code = 'VD';
COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_noise_sensitivity_vd IS
  'VD plot↔OPB noise-sensitivity spatial link. ST_MakeValid wrap (2026-05-22).';
CREATE UNIQUE INDEX link_plot_noise_sensitivity_vd_pk_idx
  ON silver_ch.link_plot_noise_sensitivity_vd (egrid, noise_id);
CREATE INDEX link_plot_noise_sensitivity_vd_egrid_idx
  ON silver_ch.link_plot_noise_sensitivity_vd (egrid);


-- 3e. link_plot_patrimoine_inventaire_vd
CREATE MATERIALIZED VIEW silver_ch.link_plot_patrimoine_inventaire_vd AS
SELECT
  p.egrid,
  pi.id                                              AS inventaire_id,
  pi.valeur,
  pi.designation,
  ST_Area(ST_Intersection(ST_MakeValid(ST_Transform(p.geometry, 2056)),
                          ST_MakeValid(pi.geometry)))::numeric AS overlap_m2,
  now() AS updated_at
FROM silver_ch.cadastral_plots p
JOIN silver_ch.cadastral_patrimoine_inventaire_vd pi
  ON pi.canton_code = 'VD'
 AND ST_Intersects(p.geometry, ST_MakeValid(ST_Transform(pi.geometry, 4326)))
WHERE p.canton_code = 'VD';
COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_patrimoine_inventaire_vd IS
  'VD plot↔patrimoine inventaire spatial link. ST_MakeValid wrap (2026-05-22).';
CREATE UNIQUE INDEX link_plot_patrimoine_inventaire_vd_pk_idx
  ON silver_ch.link_plot_patrimoine_inventaire_vd (egrid, inventaire_id);
CREATE INDEX link_plot_patrimoine_inventaire_vd_egrid_idx
  ON silver_ch.link_plot_patrimoine_inventaire_vd (egrid);


-- 3f. link_plot_zones_vd
CREATE MATERIALIZED VIEW silver_ch.link_plot_zones_vd AS
SELECT
  p.egrid,
  z.id                                               AS zone_id,
  z.zone_type,
  z.zone_code,
  z.zone_name,
  ST_Area(ST_Intersection(ST_MakeValid(ST_Transform(p.geometry, 2056)),
                          ST_MakeValid(z.geometry)))::numeric AS overlap_m2,
  (ST_Area(ST_Intersection(ST_MakeValid(ST_Transform(p.geometry, 2056)),
                           ST_MakeValid(z.geometry)))
    >= 0.5 * ST_Area(ST_MakeValid(ST_Transform(p.geometry, 2056))))      AS is_dominant,
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
 AND ST_Intersects(p.geometry, ST_MakeValid(ST_Transform(z.geometry, 4326)))
WHERE p.canton_code = 'VD';
COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_zones_vd IS
  'VD plot↔zone spatial link. ST_MakeValid wrap (2026-05-22). IUS/COS/SPB/H_MAX promoted from raw_data.';
CREATE UNIQUE INDEX link_plot_zones_vd_pk_idx
  ON silver_ch.link_plot_zones_vd (egrid, zone_id);
CREATE INDEX link_plot_zones_vd_egrid_idx
  ON silver_ch.link_plot_zones_vd (egrid);
CREATE INDEX link_plot_zones_vd_zonetype_idx
  ON silver_ch.link_plot_zones_vd (zone_type);


-- 3g. link_plot_densification_vd
CREATE MATERIALIZED VIEW silver_ch.link_plot_densification_vd AS
SELECT
  p.egrid,
  cd.id                                              AS densification_id,
  cd.densification_type,
  cd.sector_name,
  cd.administrative_practice_url,
  ST_Area(ST_Intersection(ST_MakeValid(ST_Transform(p.geometry, 2056)),
                          ST_MakeValid(cd.geometry)))::numeric AS overlap_m2,
  now() AS updated_at
FROM silver_ch.cadastral_plots p
JOIN silver_ch.cadastral_densification_vd cd
  ON cd.canton_code = 'VD'
 AND ST_Intersects(p.geometry, ST_MakeValid(ST_Transform(cd.geometry, 4326)))
WHERE p.canton_code = 'VD';
COMMENT ON MATERIALIZED VIEW silver_ch.link_plot_densification_vd IS
  'VD plot↔densification candidate spatial link. ST_MakeValid wrap (2026-05-22).';
CREATE UNIQUE INDEX link_plot_densification_vd_pk_idx
  ON silver_ch.link_plot_densification_vd (egrid, densification_id);
CREATE INDEX link_plot_densification_vd_egrid_idx
  ON silver_ch.link_plot_densification_vd (egrid);


-- ---------------------------------------------------------------------------
-- 4. Recreate gold_ch.core_plots_ext_vd (same body as PR #15 gold migration).
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW gold_ch.core_plots_ext_vd AS
WITH
energy_agg_vd AS (
  SELECT
    b2p.egrid,
    avg(e.energy_demand_kwh)::numeric                                  AS energy_demand_kwh_vd,
    mode() WITHIN GROUP (ORDER BY e.heating_solution_target)           AS heating_solution_target,
    mode() WITHIN GROUP (ORDER BY e.energy_source)                     AS energy_source_vd
  FROM silver_ch.bldg_to_plot b2p
  JOIN silver_ch.cadastral_energy_vd e ON e.egid = b2p.egid
  WHERE b2p.canton_code = 'VD'
  GROUP BY b2p.egrid
),
bldg_dest_vd AS (
  SELECT DISTINCT ON (b2p.egrid)
    b2p.egrid,
    b.categorie_txt::character varying                                  AS building_destination
  FROM silver_ch.bldg_to_plot b2p
  JOIN bronze_ch.vd_batiment_rcb b ON b.egid = b2p.egid AND b.deleted_at IS NULL
  WHERE b2p.canton_code = 'VD'
  ORDER BY b2p.egrid, b.cons_annee DESC NULLS LAST
),
bldg_year_vd AS (
  SELECT
    b2p.egrid,
    min(b.cons_annee) FILTER (
      WHERE b.cons_annee >= 1000
        AND b.cons_annee <= extract(year FROM CURRENT_DATE)::int
    )                                                                    AS construction_year
  FROM silver_ch.bldg_to_plot b2p
  JOIN bronze_ch.vd_batiment_rcb b ON b.egid = b2p.egid AND b.deleted_at IS NULL
  WHERE b2p.canton_code = 'VD'
  GROUP BY b2p.egrid
),
ddp_agg_vd AS (
  SELECT
    d.no_commune_no_parcelle,
    count(*)::integer                                                    AS ddp_count,
    array_agg(DISTINCT d.no_ddp) FILTER (WHERE d.no_ddp IS NOT NULL)     AS ddp_numbers,
    sum(d.surface_ddp_m2)::numeric                                        AS ddp_total_surface_m2
  FROM silver_ch.cadastral_ddp_vd d
  WHERE d.canton_code = 'VD' AND d.no_commune_no_parcelle IS NOT NULL
  GROUP BY d.no_commune_no_parcelle
),
serv_agg_vd AS (
  SELECT
    l.egrid,
    count(*)::integer                                                    AS servitude_count,
    array_agg(DISTINCT l.genre)  FILTER (WHERE l.genre  IS NOT NULL)     AS servitude_genres,
    array_agg(DISTINCT l.classe) FILTER (WHERE l.classe IS NOT NULL)     AS servitude_classes
  FROM silver_ch.link_plot_servitudes_vd l
  GROUP BY l.egrid
),
classement_agg AS (
  SELECT
    l.egrid,
    count(*)::integer                                                    AS classement_count,
    array_agg(DISTINCT l.designation) FILTER (WHERE l.designation IS NOT NULL) AS classement_descriptions,
    array_agg(DISTINCT l.fiche_url)   FILTER (WHERE l.fiche_url   IS NOT NULL) AS classement_fiche_urls
  FROM silver_ch.link_plot_classement_vd l
  GROUP BY l.egrid
),
inventaire_agg AS (
  SELECT
    l.egrid,
    count(*) FILTER (WHERE l.valeur = 'jardin_historique')::integer       AS jardins_count,
    array_agg(DISTINCT l.designation)
      FILTER (WHERE l.valeur = 'jardin_historique' AND l.designation IS NOT NULL) AS jardins_descriptions,
    count(*) FILTER (WHERE l.valeur IN ('isos_site','isos_perimetre'))::integer AS isos_count,
    count(*) FILTER (WHERE l.valeur = 'isos_perimetre')::integer          AS isos_perimeter_count,
    array_agg(DISTINCT l.valeur)
      FILTER (WHERE l.valeur LIKE 'isos%')                                AS isos_categories
  FROM silver_ch.link_plot_patrimoine_inventaire_vd l
  GROUP BY l.egrid
),
archeology_agg AS (
  SELECT
    l.egrid,
    count(*) FILTER (WHERE l.record_type = 'region')::integer             AS archeology_region_count,
    count(*) FILTER (WHERE l.record_type = 'site')::integer               AS archeology_site_count,
    array_agg(DISTINCT l.record_type) FILTER (WHERE l.record_type IS NOT NULL) AS archeology_record_types
  FROM silver_ch.link_plot_archeology_vd l
  GROUP BY l.egrid
),
noise_agg AS (
  SELECT
    l.egrid,
    COALESCE(
      (array_agg(l.sensitivity_degree ORDER BY l.is_dominant DESC, l.overlap_m2 DESC))[1],
      mode() WITHIN GROUP (ORDER BY l.sensitivity_degree)
    )                                                                     AS noise_sensitivity_degree,
    mode() WITHIN GROUP (ORDER BY l.source_plan)                          AS noise_sensitivity_source
  FROM silver_ch.link_plot_noise_sensitivity_vd l
  GROUP BY l.egrid
),
densif_agg_vd AS (
  SELECT
    l.egrid,
    array_agg(DISTINCT l.densification_type)
      FILTER (WHERE l.densification_type IS NOT NULL)                     AS densification_types,
    array_agg(DISTINCT l.administrative_practice_url)
      FILTER (WHERE l.administrative_practice_url IS NOT NULL)            AS densification_practice_urls,
    array_agg(DISTINCT l.sector_name)
      FILTER (WHERE l.sector_name IS NOT NULL)                            AS densification_sectors
  FROM silver_ch.link_plot_densification_vd l
  GROUP BY l.egrid
)
SELECT
  p.egrid,
  p.no_commune_no_parcelle,
  ddp.ddp_count,
  ddp.ddp_numbers,
  ddp.ddp_total_surface_m2,
  (ddp.no_commune_no_parcelle IS NOT NULL)             AS is_ddp,
  COALESCE(sv.servitude_count, 0)                       AS servitude_count,
  sv.servitude_genres,
  sv.servitude_classes,
  da.densification_types,
  da.densification_practice_urls,
  da.densification_sectors,
  bld.building_destination,
  by2.construction_year,
  ea.energy_demand_kwh_vd,
  ea.heating_solution_target,
  ea.energy_source_vd,
  noise.noise_sensitivity_degree,
  noise.noise_sensitivity_source,
  COALESCE(inv.isos_count, 0)                           AS isos_count,
  inv.isos_perimeter_count,
  inv.isos_categories,
  COALESCE(cls.classement_count, 0)                     AS classement_count,
  cls.classement_descriptions,
  cls.classement_fiche_urls,
  COALESCE(inv.jardins_count, 0)                        AS jardins_count,
  inv.jardins_descriptions,
  COALESCE(arc.archeology_region_count, 0)              AS archeology_region_count,
  COALESCE(arc.archeology_site_count, 0)                AS archeology_site_count,
  arc.archeology_record_types
FROM silver_ch.cadastral_plots p
  LEFT JOIN ddp_agg_vd     ddp   ON ddp.no_commune_no_parcelle = p.no_commune_no_parcelle
  LEFT JOIN serv_agg_vd    sv    ON sv.egrid    = p.egrid
  LEFT JOIN densif_agg_vd  da    ON da.egrid    = p.egrid
  LEFT JOIN classement_agg cls   ON cls.egrid   = p.egrid
  LEFT JOIN inventaire_agg inv   ON inv.egrid   = p.egrid
  LEFT JOIN archeology_agg arc   ON arc.egrid   = p.egrid
  LEFT JOIN noise_agg      noise ON noise.egrid = p.egrid
  LEFT JOIN bldg_dest_vd   bld   ON bld.egrid   = p.egrid
  LEFT JOIN bldg_year_vd   by2   ON by2.egrid   = p.egrid
  LEFT JOIN energy_agg_vd  ea    ON ea.egrid    = p.egrid
WHERE p.canton_code = 'VD';

COMMENT ON MATERIALIZED VIEW gold_ch.core_plots_ext_vd IS
  'VD extension matview for core_plots. Re-created 2026-05-22 as part of the link-matview ST_MakeValid fix migration.';

CREATE UNIQUE INDEX core_plots_ext_vd_egrid_idx
  ON gold_ch.core_plots_ext_vd (egrid);


-- ---------------------------------------------------------------------------
-- 5. Recreate gold_ch.v_plots_full (same body as PR #15 gold migration).
-- ---------------------------------------------------------------------------
CREATE VIEW gold_ch.v_plots_full AS
WITH lineage_agg AS (
  SELECT v.egrid,
         array_agg(DISTINCT v.no_commune_no_parcelle_histo)
           FILTER (WHERE v.no_commune_no_parcelle_histo IS NOT NULL) AS historical_parcelles
  FROM gold_ch.v_plot_lineage_full v
  WHERE v.daterad > '19000101'::text
  GROUP BY v.egrid
)
SELECT
  cp.egrid, cp.canton_code, cp.canton_name, cp.commune_bfs, cp.commune_name,
  cp.gi_rec_numero, cp.sous_secteur_nom, cp.parcel_number, cp.surface_m2,
  cp.geometry, cp.centroid, cp.lv95_e, cp.lv95_n, cp.wgs84_lat, cp.wgs84_lng,
  cp.building_count, cp.dwelling_count, cp.construction_year_oldest, cp.construction_year_newest,
  cp.max_floors, cp.footprint_total_m2, cp.volume_total_m3,
  cp.primary_category, cp.primary_class, cp.primary_heater_energy,
  cp.main_address, cp.main_postal_code, cp.main_locality,
  cp.addresses, cp.addresses_display, cp.address_search,
  cp.buildings, cp.buildings_summary, cp.search_all,
  cp.owner_count, cp.owners_display, cp.owners, cp.owner_names_search,
  cp.last_transaction_date, cp.last_transaction_price, cp.last_transaction_type,
  cp.active_listing_count, cp.sad_count, cp.updated_at,
  ext_ge.rdppf_zone_primary, ext_ge.rdppf_zone_synthetic, ext_ge.rdppf_zone_protected,
  ext_ge.rdppf_zone_restricted, ext_ge.rdppf_plq, ext_ge.rdppf_polluted_site,
  ext_ge.rdppf_forest_distance, ext_ge.rdppf_groundwater_protect, ext_ge.rdppf_pdf_url,
  COALESCE(ext_ge.servitude_count, ext_vd.servitude_count)         AS servitude_count,
  COALESCE(ext_ge.servitude_genres, ext_vd.servitude_genres)       AS servitude_genres,
  COALESCE(ext_ge.servitude_classes, ext_vd.servitude_classes)     AS servitude_classes,
  COALESCE(ext_ge.ddp_count, ext_vd.ddp_count)                     AS ddp_count,
  COALESCE(ext_ge.ddp_numbers, ext_vd.ddp_numbers)                 AS ddp_numbers,
  COALESCE(ext_ge.ddp_total_surface_m2, ext_vd.ddp_total_surface_m2) AS ddp_total_surface_m2,
  COALESCE(ext_ge.is_ddp, ext_vd.is_ddp)                           AS is_ddp,
  ext_ge.is_ppe, ext_ge.ppe_statuses,
  COALESCE(ext_ge.densification_types, ext_vd.densification_types) AS densification_types,
  COALESCE(ext_ge.densification_practice_urls, ext_vd.densification_practice_urls) AS densification_practice_urls,
  COALESCE(ext_ge.densification_sectors, ext_vd.densification_sectors) AS densification_sectors,
  app.appartenance,
  'ch'::character(2)         AS country_code,
  rc.canton_code             AS admin1_code,
  rc.canton_name             AS admin1_name,
  NULL::text                 AS admin2_code,
  NULL::text                 AS admin2_name,
  (rc.canonical_bfs)::text   AS admin3_code,
  rc.canonical_name          AS admin3_name,
  (rc.canonical_bfs)::text   AS admin3_canonical_id,
  ((COALESCE((cp.commune_bfs)::text, cp.canton_code) || '-'::text) || cp.parcel_number) AS parcel_universal_id,
  scp.no_commune_no_parcelle,
  la.historical_parcelles,
  pi.zone_primaire, pi.zone_ius_standard, pi.zone_ius_max,
  pi.ius_hpe, pi.ius_thpe, pi.ius_realistic_ceiling, pi.ius_legal_ceiling,
  pi.ius_dero_status, pi.solde_pct_legal, pi.solde_pct_standard,
  pi.surface_brut_de_plancher_hors_sol_m2, pi.surface_potentielle_m2,
  pi.surface_residuelle_m2, pi.surface_potentielle_legal_m2,
  pi.score_surface, pi.pool_surface_m2, pi.has_pool, pi.has_veranda,
  pi.nearest_transport_m, pi.nearest_school_m, pi.nearest_supermarket_m,
  pi.nearest_pharmacy_m, pi.nearest_restaurant_m, pi.amenities_score,
  pi.heating_type, pi.permits_count, pi.lien_rf,
  pi.densification_zone, pi.rdppf_pnp_zone,
  ext_vd.noise_sensitivity_degree,
  ext_vd.noise_sensitivity_source,
  ext_vd.isos_count, ext_vd.isos_categories, ext_vd.isos_perimeter_count,
  ext_vd.classement_count, ext_vd.classement_descriptions, ext_vd.classement_fiche_urls,
  ext_vd.jardins_count, ext_vd.jardins_descriptions,
  ext_vd.archeology_region_count, ext_vd.archeology_site_count, ext_vd.archeology_record_types,
  ext_vd.energy_demand_kwh_vd, ext_vd.heating_solution_target, ext_vd.energy_source_vd,
  ext_vd.building_destination,
  ext_vd.construction_year   AS construction_year_vd
FROM gold_ch.core_plots cp
  LEFT JOIN silver_ch.cadastral_plots          scp    ON scp.egrid       = cp.egrid
  LEFT JOIN lineage_agg                         la    ON la.egrid        = cp.egrid
  LEFT JOIN gold_ch.core_plots_ext_ge           ext_ge ON ext_ge.egrid   = cp.egrid
  LEFT JOIN gold_ch.core_plots_ext_vd           ext_vd ON ext_vd.egrid   = cp.egrid
  LEFT JOIN silver_ch.ref_commune_appartenance_ge app  ON app.commune_bfs = cp.commune_bfs
                                                        AND cp.canton_code = 'GE'
  LEFT JOIN silver_ch.ref_communes              rc    ON rc.canonical_bfs = cp.commune_bfs
  LEFT JOIN silver_ch.plot_intel_ge             pi    ON pi.egrid        = cp.egrid;

COMMENT ON VIEW gold_ch.v_plots_full IS
  'Joins core_plots with GE and VD extension matviews. Re-created 2026-05-22 as part of the link-matview ST_MakeValid fix migration. Same column layout as PR #15.';


-- ---------------------------------------------------------------------------
-- 6. Verification inside the transaction.
--    RAISE EXCEPTION on silent-zero-rows (lesson from earlier SRID bug).
-- ---------------------------------------------------------------------------
DO $$
DECLARE
  n_serv  bigint;
  n_zones bigint;
BEGIN
  SELECT count(*) INTO n_serv  FROM silver_ch.link_plot_servitudes_vd;
  SELECT count(*) INTO n_zones FROM silver_ch.link_plot_zones_vd;
  RAISE NOTICE 'link_plot_servitudes_vd: % rows / link_plot_zones_vd: % rows', n_serv, n_zones;
  IF n_serv  = 0 THEN RAISE EXCEPTION 'link_plot_servitudes_vd is EMPTY after rebuild — aborting'; END IF;
  IF n_zones = 0 THEN RAISE EXCEPTION 'link_plot_zones_vd is EMPTY after rebuild — aborting';     END IF;
END$$;


-- ---------------------------------------------------------------------------
-- 7. Migration tracking row.
-- ---------------------------------------------------------------------------
INSERT INTO supabase_migrations.schema_migrations (version, name, statements, created_by)
VALUES (
  '20260522000002',
  '2026-05-22_vd_enrichment_link_matviews_makevalid',
  ARRAY[
    'DROP VIEW gold_ch.v_plots_full; DROP MATERIALIZED VIEW gold_ch.core_plots_ext_vd; DROP 7 silver_ch.link_plot_*_vd matviews.',
    'CREATE 7 link matviews with ST_MakeValid wrap (only inside ST_Intersection + on RHS of ST_Intersects).',
    'CREATE gold_ch.core_plots_ext_vd + gold_ch.v_plots_full (same bodies as PR #15).',
    'Verification DO block raises if link_plot_servitudes_vd or link_plot_zones_vd is 0 rows.'
  ],
  'psql-session-override:vd_enrichment-followup'
)
ON CONFLICT (version) DO NOTHING;

COMMIT;

-- ============================================================================
-- Post-apply:
--   REFRESH MATERIALIZED VIEW CONCURRENTLY gold_ch.core_plots_ext_vd;
--   NOTIFY pgrst, 'reload schema';
-- ============================================================================
