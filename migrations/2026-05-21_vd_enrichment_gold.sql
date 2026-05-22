-- ============================================================================
-- VD/Lausanne enrichment — GOLD migration  (CREATE-OR-REPLACE-VIEW-safe)
-- ============================================================================
-- Target DB: re-LLM
-- Scope    : gold_ch.core_plots_ext_vd  +  REPLACE gold_ch.v_plots_full
-- v_plots_full SAFETY: columns 1-106 of the existing view are preserved in
--   EXACT name, type, and order. Expressions for shared concepts (servitude_*,
--   ddp_*, is_ddp, densification_*) become COALESCE(ext_ge, ext_vd) — same
--   column name/type, same position. VD-only additions (16 cols) appended at
--   positions 107+. No reorder. No drop.
-- ============================================================================

BEGIN;

-- ----------------------------------------------------------------------------
-- 1. gold_ch.core_plots_ext_vd  (mirror of core_plots_ext_ge w/ VD deltas)
-- ----------------------------------------------------------------------------
-- Reads silver_ch.cadastral_plots, silver_ch.bldg_to_plot (NATIONAL bridge),
-- silver_ch.cadastral_*_vd, and silver_ch.link_plot_*_vd. All spatial joins
-- precomputed; this matview does only egrid-keyed aggregations.
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS gold_ch.core_plots_ext_vd AS
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
WHERE p.canton_code = 'VD'
WITH NO DATA;

COMMENT ON MATERIALIZED VIEW gold_ch.core_plots_ext_vd IS
  'VD extension matview for core_plots. Mirror of core_plots_ext_ge with VD-specific deltas. '
  'Per-building data joined via silver_ch.bldg_to_plot (NATIONAL bridge). Spatial overlays via '
  'silver_ch.link_plot_*_vd. RDPPF / land_prices / PPE / IDC deferred (no VD source). '
  'KNOWN DEBT: building_destination raw VD vocabulary (categorie_txt) vs GE vocabulary '
  '(destination); normalization in silver_ch.ref_building_categories is a future session.';

CREATE UNIQUE INDEX IF NOT EXISTS core_plots_ext_vd_egrid_idx
  ON gold_ch.core_plots_ext_vd (egrid);


-- ----------------------------------------------------------------------------
-- 2. gold_ch.v_plots_full  (REPLACE)
-- ----------------------------------------------------------------------------
-- COLUMNS 1-106 preserved in exact name/type/position from the existing view.
-- Shared-concept expressions (servitude_count, servitude_genres, servitude_classes,
-- ddp_count, ddp_numbers, ddp_total_surface_m2, is_ddp, densification_types,
-- densification_practice_urls, densification_sectors) change FROM ext_ge.x
-- TO COALESCE(ext_ge.x, ext_vd.x) — same name, same type, same position.
-- COLUMNS 107-122 are new VD-only additions, strictly appended.
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
  /*  1 */ cp.egrid,
  /*  2 */ cp.canton_code,
  /*  3 */ cp.canton_name,
  /*  4 */ cp.commune_bfs,
  /*  5 */ cp.commune_name,
  /*  6 */ cp.gi_rec_numero,
  /*  7 */ cp.sous_secteur_nom,
  /*  8 */ cp.parcel_number,
  /*  9 */ cp.surface_m2,
  /* 10 */ cp.geometry,
  /* 11 */ cp.centroid,
  /* 12 */ cp.lv95_e,
  /* 13 */ cp.lv95_n,
  /* 14 */ cp.wgs84_lat,
  /* 15 */ cp.wgs84_lng,
  /* 16 */ cp.building_count,
  /* 17 */ cp.dwelling_count,
  /* 18 */ cp.construction_year_oldest,
  /* 19 */ cp.construction_year_newest,
  /* 20 */ cp.max_floors,
  /* 21 */ cp.footprint_total_m2,
  /* 22 */ cp.volume_total_m3,
  /* 23 */ cp.primary_category,
  /* 24 */ cp.primary_class,
  /* 25 */ cp.primary_heater_energy,
  /* 26 */ cp.main_address,
  /* 27 */ cp.main_postal_code,
  /* 28 */ cp.main_locality,
  /* 29 */ cp.addresses,
  /* 30 */ cp.addresses_display,
  /* 31 */ cp.address_search,
  /* 32 */ cp.buildings,
  /* 33 */ cp.buildings_summary,
  /* 34 */ cp.search_all,
  /* 35 */ cp.owner_count,
  /* 36 */ cp.owners_display,
  /* 37 */ cp.owners,
  /* 38 */ cp.owner_names_search,
  /* 39 */ cp.last_transaction_date,
  /* 40 */ cp.last_transaction_price,
  /* 41 */ cp.last_transaction_type,
  /* 42 */ cp.active_listing_count,
  /* 43 */ cp.sad_count,
  /* 44 */ cp.updated_at,
  /* 45 */ ext_ge.rdppf_zone_primary,
  /* 46 */ ext_ge.rdppf_zone_synthetic,
  /* 47 */ ext_ge.rdppf_zone_protected,
  /* 48 */ ext_ge.rdppf_zone_restricted,
  /* 49 */ ext_ge.rdppf_plq,
  /* 50 */ ext_ge.rdppf_polluted_site,
  /* 51 */ ext_ge.rdppf_forest_distance,
  /* 52 */ ext_ge.rdppf_groundwater_protect,
  /* 53 */ ext_ge.rdppf_pdf_url,
  /* 54 */ COALESCE(ext_ge.servitude_count,             ext_vd.servitude_count)             AS servitude_count,
  /* 55 */ COALESCE(ext_ge.servitude_genres,            ext_vd.servitude_genres)            AS servitude_genres,
  /* 56 */ COALESCE(ext_ge.servitude_classes,           ext_vd.servitude_classes)           AS servitude_classes,
  /* 57 */ COALESCE(ext_ge.ddp_count,                   ext_vd.ddp_count)                   AS ddp_count,
  /* 58 */ COALESCE(ext_ge.ddp_numbers,                 ext_vd.ddp_numbers)                 AS ddp_numbers,
  /* 59 */ COALESCE(ext_ge.ddp_total_surface_m2,        ext_vd.ddp_total_surface_m2)        AS ddp_total_surface_m2,
  /* 60 */ COALESCE(ext_ge.is_ddp,                      ext_vd.is_ddp)                      AS is_ddp,
  /* 61 */ ext_ge.is_ppe,
  /* 62 */ ext_ge.ppe_statuses,
  /* 63 */ COALESCE(ext_ge.densification_types,         ext_vd.densification_types)         AS densification_types,
  /* 64 */ COALESCE(ext_ge.densification_practice_urls, ext_vd.densification_practice_urls) AS densification_practice_urls,
  /* 65 */ COALESCE(ext_ge.densification_sectors,       ext_vd.densification_sectors)       AS densification_sectors,
  /* 66 */ app.appartenance,
  /* 67 */ 'ch'::character(2)         AS country_code,
  /* 68 */ rc.canton_code             AS admin1_code,
  /* 69 */ rc.canton_name             AS admin1_name,
  /* 70 */ NULL::text                 AS admin2_code,
  /* 71 */ NULL::text                 AS admin2_name,
  /* 72 */ (rc.canonical_bfs)::text   AS admin3_code,
  /* 73 */ rc.canonical_name          AS admin3_name,
  /* 74 */ (rc.canonical_bfs)::text   AS admin3_canonical_id,
  /* 75 */ ((COALESCE((cp.commune_bfs)::text, cp.canton_code) || '-'::text) || cp.parcel_number) AS parcel_universal_id,
  /* 76 */ scp.no_commune_no_parcelle,
  /* 77 */ la.historical_parcelles,
  /* 78 */ pi.zone_primaire,
  /* 79 */ pi.zone_ius_standard,
  /* 80 */ pi.zone_ius_max,
  /* 81 */ pi.ius_hpe,
  /* 82 */ pi.ius_thpe,
  /* 83 */ pi.ius_realistic_ceiling,
  /* 84 */ pi.ius_legal_ceiling,
  /* 85 */ pi.ius_dero_status,
  /* 86 */ pi.solde_pct_legal,
  /* 87 */ pi.solde_pct_standard,
  /* 88 */ pi.surface_brut_de_plancher_hors_sol_m2,
  /* 89 */ pi.surface_potentielle_m2,
  /* 90 */ pi.surface_residuelle_m2,
  /* 91 */ pi.surface_potentielle_legal_m2,
  /* 92 */ pi.score_surface,
  /* 93 */ pi.pool_surface_m2,
  /* 94 */ pi.has_pool,
  /* 95 */ pi.has_veranda,
  /* 96 */ pi.nearest_transport_m,
  /* 97 */ pi.nearest_school_m,
  /* 98 */ pi.nearest_supermarket_m,
  /* 99 */ pi.nearest_pharmacy_m,
  /*100 */ pi.nearest_restaurant_m,
  /*101 */ pi.amenities_score,
  /*102 */ pi.heating_type,
  /*103 */ pi.permits_count,
  /*104 */ pi.lien_rf,
  /*105 */ pi.densification_zone,
  /*106 */ pi.rdppf_pnp_zone,
  -- ─── VD-only additions appended at positions 107+ ───
  /*107 */ ext_vd.noise_sensitivity_degree,
  /*108 */ ext_vd.noise_sensitivity_source,
  /*109 */ ext_vd.isos_count,
  /*110 */ ext_vd.isos_categories,
  /*111 */ ext_vd.isos_perimeter_count,
  /*112 */ ext_vd.classement_count,
  /*113 */ ext_vd.classement_descriptions,
  /*114 */ ext_vd.classement_fiche_urls,
  /*115 */ ext_vd.jardins_count,
  /*116 */ ext_vd.jardins_descriptions,
  /*117 */ ext_vd.archeology_region_count,
  /*118 */ ext_vd.archeology_site_count,
  /*119 */ ext_vd.archeology_record_types,
  /*120 */ ext_vd.energy_demand_kwh_vd,
  /*121 */ ext_vd.heating_solution_target,
  /*122 */ ext_vd.energy_source_vd,
  /*123 */ ext_vd.building_destination,
  /*124 */ ext_vd.construction_year   AS construction_year_vd
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
  'Joins core_plots with GE and VD extension matviews. Columns 1-106: identical to prior view '
  '(shared concepts now COALESCE ext_ge/ext_vd, same name/type, same position). Columns 107-124: '
  'VD-only additions appended. KNOWN DEBT: ext_vd.building_destination uses RCB categorie_txt '
  'vocabulary; ext_ge.building_destination uses ge_cad_batiments.destination — not normalized.';

COMMIT;

NOTIFY pgrst, 'reload schema';

-- ============================================================================
-- Step-5 wrapper apply pattern:
--   1. Tx-1 (this BEGIN/COMMIT): CREATE core_plots_ext_vd WITH NO DATA, indexes,
--      then CREATE OR REPLACE VIEW v_plots_full. COMMIT.
--   2. Stmt: REFRESH MATERIALIZED VIEW gold_ch.core_plots_ext_vd
--      (non-CONCURRENTLY first time).
--   3. Tx-2: INSERT schema_migrations row, COMMIT.
-- Tracking semantics: row only after refresh succeeds (matches the step-4 fix).
-- ============================================================================
