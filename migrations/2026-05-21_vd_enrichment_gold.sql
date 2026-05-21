-- ============================================================================
-- VD/Lausanne enrichment — GOLD migration  (rewritten to use bridge + link matviews)
-- ============================================================================
-- Target DB: re-LLM
-- Scope    : gold_ch.core_plots_ext_vd  +  REPLACE gold_ch.v_plots_full
-- Pattern  : All joins keyed on egrid. No runtime ST_Intersects. Per-building
--            data joined via silver_ch.bldg_to_plot bridge. Spatial overlay
--            data joined via silver_ch.link_plot_*_vd matviews.
-- V1 drops :  9 rdppf_*, 2 land_price_*, 2 ppe_*, 2 idc_*, type_propriete
-- V1 adds  :  noise_sensitivity_*, isos_*, classement_*, jardins_*, archeology_*,
--             energy_demand_kwh_vd, heating_solution_target, energy_source_vd
-- Pre-reqs :  bronze + silver + link_matviews migrations applied AND refreshed
--             at least once.
-- ============================================================================

BEGIN;

-- ----------------------------------------------------------------------------
-- 1. gold_ch.core_plots_ext_vd
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS gold_ch.core_plots_ext_vd AS
WITH
-- per-building → per-plot aggregations, joined through the bridge
energy_agg_vd AS (
  SELECT
    b2p.egrid,
    avg(e.energy_demand_kwh)::numeric                          AS energy_demand_kwh_vd,
    mode() WITHIN GROUP (ORDER BY e.heating_solution_target)   AS heating_solution_target,
    mode() WITHIN GROUP (ORDER BY e.energy_source)             AS energy_source_vd
  FROM silver_ch.bldg_to_plot b2p
  JOIN silver_ch.cadastral_energy_vd e ON e.egid = b2p.egid
  WHERE b2p.canton_code = 'VD'
  GROUP BY b2p.egrid
),
bldg_dest_vd AS (
  SELECT DISTINCT ON (b2p.egrid)
    b2p.egrid,
    b.categorie_txt::character varying AS building_destination   -- type-compat with ext_ge
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
    ) AS construction_year
  FROM silver_ch.bldg_to_plot b2p
  JOIN bronze_ch.vd_batiment_rcb b ON b.egid = b2p.egid AND b.deleted_at IS NULL
  WHERE b2p.canton_code = 'VD'
  GROUP BY b2p.egrid
),
-- DDP aggregation (link via no_commune_no_parcelle)
ddp_agg_vd AS (
  SELECT
    d.no_commune_no_parcelle,
    count(*)::integer                                                                  AS ddp_count,
    array_agg(DISTINCT d.no_ddp) FILTER (WHERE d.no_ddp IS NOT NULL)                   AS ddp_numbers,
    sum(d.surface_ddp_m2)::numeric                                                      AS ddp_total_surface_m2
  FROM silver_ch.cadastral_ddp_vd d
  WHERE d.canton_code = 'VD' AND d.no_commune_no_parcelle IS NOT NULL
  GROUP BY d.no_commune_no_parcelle
),
-- Spatial aggregations: read from link matviews, NOT live ST_Intersects
serv_agg_vd AS (
  SELECT
    l.egrid,
    count(*)::integer                                                                  AS servitude_count,
    array_agg(DISTINCT l.genre)   FILTER (WHERE l.genre   IS NOT NULL)                 AS servitude_genres,
    array_agg(DISTINCT l.classe)  FILTER (WHERE l.classe  IS NOT NULL)                 AS servitude_classes
  FROM silver_ch.link_plot_servitudes_vd l
  GROUP BY l.egrid
),
classement_agg AS (
  SELECT
    l.egrid,
    count(*)::integer                                                                  AS classement_count,
    array_agg(DISTINCT l.designation) FILTER (WHERE l.designation IS NOT NULL)         AS classement_descriptions,
    array_agg(DISTINCT l.fiche_url)   FILTER (WHERE l.fiche_url   IS NOT NULL)         AS classement_fiche_urls
  FROM silver_ch.link_plot_classement_vd l
  GROUP BY l.egrid
),
inventaire_agg AS (
  SELECT
    l.egrid,
    count(*) FILTER (WHERE l.valeur = 'jardin_historique')::integer                    AS jardins_count,
    array_agg(DISTINCT l.designation)
      FILTER (WHERE l.valeur = 'jardin_historique' AND l.designation IS NOT NULL)      AS jardins_descriptions,
    count(*) FILTER (WHERE l.valeur IN ('isos_site','isos_perimetre'))::integer        AS isos_count,
    count(*) FILTER (WHERE l.valeur = 'isos_perimetre')::integer                       AS isos_perimeter_count,
    array_agg(DISTINCT l.valeur)
      FILTER (WHERE l.valeur LIKE 'isos%')                                              AS isos_categories
  FROM silver_ch.link_plot_patrimoine_inventaire_vd l
  GROUP BY l.egrid
),
archeology_agg AS (
  SELECT
    l.egrid,
    count(*) FILTER (WHERE l.record_type = 'region')::integer                          AS archeology_region_count,
    count(*) FILTER (WHERE l.record_type = 'site')::integer                            AS archeology_site_count,
    array_agg(DISTINCT l.record_type) FILTER (WHERE l.record_type IS NOT NULL)         AS archeology_record_types
  FROM silver_ch.link_plot_archeology_vd l
  GROUP BY l.egrid
),
noise_agg AS (
  SELECT
    l.egrid,
    -- prefer dominant degree when one covers ≥50% of the plot; else mode()
    COALESCE(
      (array_agg(l.sensitivity_degree ORDER BY l.is_dominant DESC, l.overlap_m2 DESC))[1],
      mode() WITHIN GROUP (ORDER BY l.sensitivity_degree)
    )                                                                                  AS noise_sensitivity_degree,
    mode() WITHIN GROUP (ORDER BY l.source_plan)                                       AS noise_sensitivity_source
  FROM silver_ch.link_plot_noise_sensitivity_vd l
  GROUP BY l.egrid
),
densif_agg_vd AS (
  SELECT
    l.egrid,
    array_agg(DISTINCT l.densification_type)
      FILTER (WHERE l.densification_type IS NOT NULL)                                  AS densification_types,
    array_agg(DISTINCT l.administrative_practice_url)
      FILTER (WHERE l.administrative_practice_url IS NOT NULL)                         AS densification_practice_urls,
    array_agg(DISTINCT l.sector_name)
      FILTER (WHERE l.sector_name IS NOT NULL)                                         AS densification_sectors
  FROM silver_ch.link_plot_densification_vd l
  GROUP BY l.egrid
),
centroids AS (
  SELECT
    p.egrid,
    ST_X(ST_Centroid(p.geometry))::double precision AS centroid_lon,
    ST_Y(ST_Centroid(p.geometry))::double precision AS centroid_lat
  FROM gold_ch.core_plots p
  WHERE p.canton_code = 'VD' AND p.geometry IS NOT NULL
)
SELECT
  p.egrid,
  p.no_commune_no_parcelle,
  -- ── DDP (mirror of _ge shape) ──
  ddp.ddp_count,
  ddp.ddp_numbers,
  ddp.ddp_total_surface_m2,
  (ddp.no_commune_no_parcelle IS NOT NULL)             AS is_ddp,
  -- ── Servitudes (mirror of _ge shape) ──
  COALESCE(sv.servitude_count, 0)                      AS servitude_count,
  sv.servitude_genres,
  sv.servitude_classes,
  -- ── Densification (mirror of _ge shape) ──
  da.densification_types,
  da.densification_practice_urls,
  da.densification_sectors,
  -- ── Building destination + construction year (joined via bldg_to_plot bridge) ──
  bld.building_destination,                            -- character varying (type-compat with ext_ge)
  by2.construction_year,
  ct.centroid_lon,
  ct.centroid_lat,
  -- ── Energy (VD-specific units; distinct from IDC; joined via bldg_to_plot) ──
  ea.energy_demand_kwh_vd,
  ea.heating_solution_target,
  ea.energy_source_vd,
  -- ── VD-only additive columns ──
  noise.noise_sensitivity_degree,
  noise.noise_sensitivity_source,
  COALESCE(inv.isos_count, 0)                          AS isos_count,
  inv.isos_perimeter_count,
  inv.isos_categories,
  COALESCE(cls.classement_count, 0)                    AS classement_count,
  cls.classement_descriptions,
  cls.classement_fiche_urls,
  COALESCE(inv.jardins_count, 0)                       AS jardins_count,
  inv.jardins_descriptions,
  COALESCE(arc.archeology_region_count, 0)             AS archeology_region_count,
  COALESCE(arc.archeology_site_count, 0)               AS archeology_site_count,
  arc.archeology_record_types
FROM silver_ch.cadastral_plots p
  LEFT JOIN ddp_agg_vd       ddp   ON ddp.no_commune_no_parcelle = p.no_commune_no_parcelle
  LEFT JOIN serv_agg_vd      sv    ON sv.egrid    = p.egrid
  LEFT JOIN densif_agg_vd    da    ON da.egrid    = p.egrid
  LEFT JOIN classement_agg   cls   ON cls.egrid   = p.egrid
  LEFT JOIN inventaire_agg   inv   ON inv.egrid   = p.egrid
  LEFT JOIN archeology_agg   arc   ON arc.egrid   = p.egrid
  LEFT JOIN noise_agg        noise ON noise.egrid = p.egrid
  LEFT JOIN bldg_dest_vd     bld   ON bld.egrid   = p.egrid
  LEFT JOIN bldg_year_vd     by2   ON by2.egrid   = p.egrid
  LEFT JOIN energy_agg_vd    ea    ON ea.egrid    = p.egrid
  LEFT JOIN centroids        ct    ON ct.egrid    = p.egrid
WHERE p.canton_code = 'VD'
WITH NO DATA;

COMMENT ON MATERIALIZED VIEW gold_ch.core_plots_ext_vd IS
  'VD extension matview for core_plots. Mirror of core_plots_ext_ge with VD-specific deltas. '
  'All spatial joins precomputed in silver_ch.link_plot_*_vd matviews. Per-building data joined '
  'via NATIONAL bridge silver_ch.bldg_to_plot. '
  'V1 drops: 9 rdppf_*, indicative_land_price_m2, estimated_land_value, is_ppe, ppe_statuses, '
  'idc_avg, idc_bracket, type_propriete — sources not in scope. '
  'V1 adds: noise_sensitivity_*, isos_*, classement_*, jardins_*, archeology_*, energy_demand_kwh_vd, '
  'heating_solution_target, energy_source_vd. '
  'KNOWN DEBT: building_destination is RAW value from RCB categorie_txt; ext_ge uses '
  'ge_cad_batiments.destination — different vocabularies. Normalization deferred to '
  'silver_ch.ref_building_categories (separate session).';

CREATE UNIQUE INDEX IF NOT EXISTS core_plots_ext_vd_egrid_idx
  ON gold_ch.core_plots_ext_vd (egrid);


-- ----------------------------------------------------------------------------
-- 2. gold_ch.v_plots_full — REPLACE with ext_vd join + COALESCE shared concepts
-- ----------------------------------------------------------------------------
-- CREATE OR REPLACE VIEW is non-destructive (regular view, not matview).
-- Adds LEFT JOIN to ext_vd, COALESCEs shared concepts (canton_code in core_plots
-- guarantees only one side populated per row), adds VD-only columns.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold_ch.v_plots_full AS
WITH lineage_agg AS (
  SELECT v.egrid,
         array_agg(DISTINCT v.no_commune_no_parcelle_histo)
           FILTER (WHERE v.no_commune_no_parcelle_histo IS NOT NULL) AS historical_parcelles
  FROM gold_ch.v_plot_lineage_full v
  WHERE v.daterad > '19000101'::text
  GROUP BY v.egrid
)
SELECT
  cp.egrid,
  cp.canton_code,
  cp.canton_name,
  cp.commune_bfs,
  cp.commune_name,
  cp.gi_rec_numero,
  cp.sous_secteur_nom,
  cp.parcel_number,
  cp.surface_m2,
  cp.geometry,
  cp.centroid,
  cp.lv95_e,
  cp.lv95_n,
  cp.wgs84_lat,
  cp.wgs84_lng,
  cp.building_count,
  cp.dwelling_count,
  cp.construction_year_oldest,
  cp.construction_year_newest,
  cp.max_floors,
  cp.footprint_total_m2,
  cp.volume_total_m3,
  cp.primary_category,
  cp.primary_class,
  cp.primary_heater_energy,
  cp.main_address,
  cp.main_postal_code,
  cp.main_locality,
  cp.addresses,
  cp.addresses_display,
  cp.address_search,
  cp.buildings,
  cp.buildings_summary,
  cp.search_all,
  cp.owner_count,
  cp.owners_display,
  cp.owners,
  cp.owner_names_search,
  cp.last_transaction_date,
  cp.last_transaction_price,
  cp.last_transaction_type,
  cp.active_listing_count,
  cp.sad_count,
  cp.updated_at,
  -- ── GE-only columns (only ext_ge populates these; no VD source) ──
  ext_ge.rdppf_zone_primary,
  ext_ge.rdppf_zone_synthetic,
  ext_ge.rdppf_zone_protected,
  ext_ge.rdppf_zone_restricted,
  ext_ge.rdppf_plq,
  ext_ge.rdppf_polluted_site,
  ext_ge.rdppf_forest_distance,
  ext_ge.rdppf_groundwater_protect,
  ext_ge.rdppf_pdf_url,
  ext_ge.indicative_land_price_m2,
  ext_ge.estimated_land_value,
  ext_ge.is_ppe,
  ext_ge.ppe_statuses,
  ext_ge.idc_avg,
  ext_ge.idc_bracket,
  ext_ge.type_propriete,
  -- ── Shared concepts: COALESCE ext_ge then ext_vd ──
  COALESCE(ext_ge.servitude_count,             ext_vd.servitude_count)               AS servitude_count,
  COALESCE(ext_ge.servitude_genres,            ext_vd.servitude_genres)              AS servitude_genres,
  COALESCE(ext_ge.servitude_classes,           ext_vd.servitude_classes)             AS servitude_classes,
  COALESCE(ext_ge.ddp_count,                   ext_vd.ddp_count)                     AS ddp_count,
  COALESCE(ext_ge.ddp_numbers,                 ext_vd.ddp_numbers)                   AS ddp_numbers,
  COALESCE(ext_ge.ddp_total_surface_m2,        ext_vd.ddp_total_surface_m2)          AS ddp_total_surface_m2,
  COALESCE(ext_ge.is_ddp,                      ext_vd.is_ddp)                        AS is_ddp,
  COALESCE(ext_ge.densification_types,         ext_vd.densification_types)           AS densification_types,
  COALESCE(ext_ge.densification_practice_urls, ext_vd.densification_practice_urls)   AS densification_practice_urls,
  COALESCE(ext_ge.densification_sectors,       ext_vd.densification_sectors)         AS densification_sectors,
  ext_ge.mutation_count,                       -- VD has no mutations source yet
  ext_ge.last_mutation_date,
  COALESCE(ext_ge.historical_parcelles,        la.historical_parcelles)              AS historical_parcelles,
  COALESCE(ext_ge.construction_year,           ext_vd.construction_year)             AS construction_year,
  COALESCE(ext_ge.centroid_lon,                ext_vd.centroid_lon)                  AS centroid_lon,
  COALESCE(ext_ge.centroid_lat,                ext_vd.centroid_lat)                  AS centroid_lat,
  COALESCE(ext_ge.building_destination,        ext_vd.building_destination)          AS building_destination,
    -- KNOWN DEBT: building_destination — different source vocabularies (ge_cad_batiments.destination
    -- vs vd_batiment_rcb.categorie_txt). Surfaced raw. Normalization layer silver_ch.ref_building_categories
    -- is a separate deferred task.
  -- ── VD-only additive columns ──
  ext_vd.noise_sensitivity_degree,
  ext_vd.noise_sensitivity_source,
  ext_vd.isos_count,
  ext_vd.isos_categories,
  ext_vd.isos_perimeter_count,
  ext_vd.classement_count,
  ext_vd.classement_descriptions,
  ext_vd.classement_fiche_urls,
  ext_vd.jardins_count,
  ext_vd.jardins_descriptions,
  ext_vd.archeology_region_count,
  ext_vd.archeology_site_count,
  ext_vd.archeology_record_types,
  ext_vd.energy_demand_kwh_vd,
  ext_vd.heating_solution_target,
  ext_vd.energy_source_vd,
  -- ── geographic/ref pass-through (unchanged from prior viewdef) ──
  app.appartenance,
  'ch'::character(2)             AS country_code,
  rc.canton_code                  AS admin1_code,
  rc.canton_name                  AS admin1_name,
  NULL::text                      AS admin2_code,
  NULL::text                      AS admin2_name,
  (rc.canonical_bfs)::text        AS admin3_code,
  rc.canonical_name               AS admin3_name,
  (rc.canonical_bfs)::text        AS admin3_canonical_id,
  ((COALESCE((cp.commune_bfs)::text, cp.canton_code) || '-'::text) || cp.parcel_number) AS parcel_universal_id,
  scp.no_commune_no_parcelle,
  -- _v2 shadow added 2026-05-21; legacy column retained until consumer audit complete;
  -- drop in follow-up session.
  la.historical_parcelles         AS historical_parcelles_v2,
  pi.zone_primaire,
  pi.zone_ius_standard,
  pi.zone_ius_max,
  pi.ius_hpe,
  pi.ius_thpe,
  pi.ius_realistic_ceiling,
  pi.ius_legal_ceiling,
  pi.ius_dero_status,
  pi.solde_pct_legal,
  pi.solde_pct_standard,
  pi.surface_brut_de_plancher_hors_sol_m2,
  pi.surface_potentielle_m2,
  pi.surface_residuelle_m2,
  pi.surface_potentielle_legal_m2,
  pi.score_surface,
  pi.pool_surface_m2,
  pi.has_pool,
  pi.has_veranda,
  pi.nearest_transport_m,
  pi.nearest_school_m,
  pi.nearest_supermarket_m,
  pi.nearest_pharmacy_m,
  pi.nearest_restaurant_m,
  pi.amenities_score,
  pi.heating_type,
  pi.permits_count,
  pi.lien_rf,
  pi.densification_zone,
  pi.rdppf_pnp_zone
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
  'Joins core_plots with GE and VD extension matviews. COALESCE on shared concepts; '
  'VD-only and GE-only columns surfaced separately. building_destination is RAW per-canton '
  'vocabulary — normalization deferred to silver_ch.ref_building_categories.';

COMMIT;

NOTIFY pgrst, 'reload schema';

-- ============================================================================
-- Refresh dependency chain (now resolved):
--   bronze parsers (per-VPS cron) →
--   silver_ch.cadastral_*_vd (matview refresh cron) →
--   silver_ch.bldg_to_plot (weekly) →
--   silver_ch.link_plot_*_vd (nightly) →
--   gold_ch.core_plots_ext_vd (nightly) →
--   gold_ch.v_plots_full (regular view; reads matviews live)
-- All matview refreshes use CONCURRENTLY after the first non-CONCURRENTLY seed.
-- ============================================================================
