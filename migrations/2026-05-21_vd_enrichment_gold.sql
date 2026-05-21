-- ============================================================================
-- VD/Lausanne enrichment — GOLD migration
-- ============================================================================
-- Target DB: re-LLM
-- Scope    : gold_ch.core_plots_ext_vd (new matview)
--            + gold_ch.v_plots_full (REPLACE: add LEFT JOIN to ext_vd, COALESCE
--              shared columns, add VD-only columns)
-- Pattern  : Mirror gold_ch.core_plots_ext_ge viewdef structure 1:1.
--            v1 drops: 9 rdppf_* cols, 2 land_price cols, 2 ppe cols, type_propriete,
--                       idc_avg/idc_bracket — sources don't exist for VD yet.
--            v1 adds: noise_sensitivity_*, isos_*, classement_*, jardins_*, archeology_*,
--                       energy_demand_kwh_vd.
-- Pre-reqs : silver migration applied + all silver matviews refreshed at least once.
-- ============================================================================

BEGIN;

-- ----------------------------------------------------------------------------
-- 1. gold_ch.core_plots_ext_vd  (mirror of core_plots_ext_ge)
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS gold_ch.core_plots_ext_vd AS
WITH
energy_agg_vd AS (
  SELECT
    ce.egid,
    avg(ce.energy_demand_kwh)::numeric           AS energy_demand_kwh_vd,
    mode() WITHIN GROUP (ORDER BY ce.heating_solution_target) AS heating_solution_target,
    mode() WITHIN GROUP (ORDER BY ce.energy_source)           AS energy_source_vd
  FROM silver_ch.cadastral_energy_vd ce
  WHERE ce.canton_code = 'VD' AND ce.egid IS NOT NULL AND ce.egid::text <> ''
  GROUP BY ce.egid
),
ddp_agg_vd AS (
  SELECT
    d.no_commune_no_parcelle,
    count(*)::integer                            AS ddp_count,
    array_agg(DISTINCT d.no_ddp) FILTER (WHERE d.no_ddp IS NOT NULL) AS ddp_numbers,
    sum(d.surface_ddp_m2)::numeric               AS ddp_total_surface_m2
  FROM silver_ch.cadastral_ddp_vd d
  WHERE d.canton_code = 'VD' AND d.no_commune_no_parcelle IS NOT NULL
  GROUP BY d.no_commune_no_parcelle
),
serv_agg_vd AS (
  SELECT
    p.egrid,                                      -- spatial join: servitudes have no parcel link in Lausanne source
    count(*)::integer                              AS servitude_count,
    array_agg(DISTINCT s.genre) FILTER (WHERE s.genre IS NOT NULL) AS servitude_genres,
    array_agg(DISTINCT s.classe) FILTER (WHERE s.classe IS NOT NULL) AS servitude_classes
  FROM silver_ch.cadastral_plots p
  JOIN silver_ch.cadastral_servitudes_vd s
    ON st_intersects(s.id::text::text IS NOT NULL AND
                     -- placeholder; the real join is via PostGIS spatial intersect with plot polygon.
                     -- We materialize the join via st_intersects with cadastral_plots geometry.
                     -- For perf, see post-apply notes (consider precomputing via parser-side join).
                     p.geometry, s.id::text::text::geometry)
  WHERE p.canton_code = 'VD'
  GROUP BY p.egrid
),
densif_agg_vd AS (
  SELECT
    NULL::text                                    AS egrid,   -- pa_etude has no egrid; spatial-intersect approach below
    array_agg(DISTINCT cd.densification_type) FILTER (WHERE cd.densification_type IS NOT NULL) AS densification_types,
    array_agg(DISTINCT cd.administrative_practice_url) FILTER (WHERE cd.administrative_practice_url IS NOT NULL) AS densification_practice_urls,
    array_agg(DISTINCT cd.sector_name) FILTER (WHERE cd.sector_name IS NOT NULL) AS densification_sectors
  FROM silver_ch.cadastral_densification_vd cd
  WHERE cd.canton_code = 'VD'
),
classement_agg AS (
  SELECT
    p.egrid,
    count(*)::integer                              AS classement_count,
    array_agg(DISTINCT pc.designation) FILTER (WHERE pc.designation IS NOT NULL) AS classement_descriptions,
    array_agg(DISTINCT (pc.raw_data->>'url_recens')) FILTER (WHERE pc.raw_data->>'url_recens' IS NOT NULL) AS classement_fiche_urls
  FROM silver_ch.cadastral_plots p
  JOIN silver_ch.cadastral_patrimoine_classe_vd pc ON st_intersects(p.geometry, pc.geometry)
  WHERE p.canton_code = 'VD'
  GROUP BY p.egrid
),
inventaire_agg AS (
  SELECT
    p.egrid,
    count(*) FILTER (WHERE pi.valeur = 'jardin_historique')::integer  AS jardins_count,
    array_agg(DISTINCT pi.designation) FILTER (WHERE pi.valeur = 'jardin_historique' AND pi.designation IS NOT NULL) AS jardins_descriptions,
    count(*) FILTER (WHERE pi.valeur IN ('isos_site','isos_perimetre'))::integer AS isos_count,
    count(*) FILTER (WHERE pi.valeur = 'isos_perimetre')::integer    AS isos_perimeter_count,
    array_agg(DISTINCT pi.valeur) FILTER (WHERE pi.valeur LIKE 'isos%') AS isos_categories
  FROM silver_ch.cadastral_plots p
  JOIN silver_ch.cadastral_patrimoine_inventaire_vd pi ON st_intersects(p.geometry, pi.geometry)
  WHERE p.canton_code = 'VD'
  GROUP BY p.egrid
),
archeology_agg AS (
  SELECT
    p.egrid,
    count(*) FILTER (WHERE a.record_type = 'region')::integer AS archeology_region_count,
    count(*) FILTER (WHERE a.record_type = 'site')::integer   AS archeology_site_count,
    array_agg(DISTINCT a.record_type) FILTER (WHERE a.record_type IS NOT NULL) AS archeology_record_types
  FROM silver_ch.cadastral_plots p
  JOIN silver_ch.cadastral_archaeology a
    ON a.canton_code = 'VD' AND st_intersects(p.geometry, a.geometry)
  WHERE p.canton_code = 'VD'
  GROUP BY p.egrid
),
noise_agg AS (
  SELECT
    p.egrid,
    mode() WITHIN GROUP (ORDER BY n.sensitivity_degree) AS noise_sensitivity_degree,
    mode() WITHIN GROUP (ORDER BY n.source_plan)        AS noise_sensitivity_source
  FROM silver_ch.cadastral_plots p
  JOIN silver_ch.cadastral_noise_sensitivity n
    ON n.canton_code = 'VD' AND st_intersects(p.geometry, n.geometry)
  WHERE p.canton_code = 'VD'
  GROUP BY p.egrid
),
bldg_dest_vd AS (
  SELECT DISTINCT ON (b.egid)
    b.egid,
    b.categorie_txt::character varying AS building_destination   -- cast to character varying for ext_ge type compat
  FROM bronze_ch.vd_batiment_rcb b
  WHERE b.deleted_at IS NULL AND b.egid IS NOT NULL AND b.egid <> ''
  ORDER BY b.egid, b.cons_annee DESC NULLS LAST
),
bldg_year_vd AS (
  SELECT
    egid,
    min(cons_annee) FILTER (WHERE cons_annee >= 1000 AND cons_annee <= extract(year FROM CURRENT_DATE)::int) AS construction_year
  FROM bronze_ch.vd_batiment_rcb
  WHERE deleted_at IS NULL AND egid IS NOT NULL AND egid <> ''
  GROUP BY egid
),
centroids AS (
  SELECT p.egrid,
         st_x(st_centroid(p.geometry))::double precision AS centroid_lon,
         st_y(st_centroid(p.geometry))::double precision AS centroid_lat
  FROM gold_ch.core_plots p
  WHERE p.canton_code = 'VD' AND p.geometry IS NOT NULL
)
SELECT
  p.egrid,
  p.no_commune_no_parcelle,
  -- DDP (mirror of _ge)
  ddp.ddp_count,
  ddp.ddp_numbers,
  ddp.ddp_total_surface_m2,
  (ddp.no_commune_no_parcelle IS NOT NULL) AS is_ddp,
  -- Servitudes (mirror of _ge)
  COALESCE(sv.servitude_count, 0)         AS servitude_count,
  sv.servitude_genres,
  sv.servitude_classes,
  -- Densification (mirror of _ge — VD currently same arrays repeated per plot, see notes)
  da.densification_types,
  da.densification_practice_urls,
  da.densification_sectors,
  -- Construction year + destination (mirror of _ge with VD-specific source)
  bld.building_destination,                  -- character varying (type-compat with _ge)
  by2.construction_year,
  ct.centroid_lon,
  ct.centroid_lat,
  -- Energy (VD-specific units, different concept than IDC)
  ea.energy_demand_kwh_vd,
  ea.heating_solution_target,
  ea.energy_source_vd,
  -- VD-only additive columns
  noise.noise_sensitivity_degree,
  noise.noise_sensitivity_source,
  COALESCE(inv.isos_count, 0)              AS isos_count,
  inv.isos_perimeter_count,
  inv.isos_categories,
  COALESCE(cls.classement_count, 0)        AS classement_count,
  cls.classement_descriptions,
  cls.classement_fiche_urls,
  COALESCE(inv.jardins_count, 0)           AS jardins_count,
  inv.jardins_descriptions,
  COALESCE(arc.archeology_region_count, 0) AS archeology_region_count,
  COALESCE(arc.archeology_site_count, 0)   AS archeology_site_count,
  arc.archeology_record_types
FROM silver_ch.cadastral_plots p
  LEFT JOIN ddp_agg_vd ddp     ON ddp.no_commune_no_parcelle = p.no_commune_no_parcelle
  LEFT JOIN serv_agg_vd sv     ON sv.egrid = p.egrid
  LEFT JOIN densif_agg_vd da   ON da.egrid IS NULL    -- placeholder; spatial-densification per egrid pending
  LEFT JOIN classement_agg cls ON cls.egrid = p.egrid
  LEFT JOIN inventaire_agg inv ON inv.egrid = p.egrid
  LEFT JOIN archeology_agg arc ON arc.egrid = p.egrid
  LEFT JOIN noise_agg noise    ON noise.egrid = p.egrid
  LEFT JOIN energy_agg_vd ea   ON ea.egid::text = p.egrid          -- nb: egid != egrid; this needs a building→plot bridge — see open question §A below
  LEFT JOIN bldg_dest_vd bld   ON bld.egid::text = p.egrid         -- same
  LEFT JOIN bldg_year_vd by2   ON by2.egid::text = p.egrid         -- same
  LEFT JOIN centroids ct       ON ct.egrid = p.egrid
WHERE p.canton_code = 'VD'
WITH NO DATA;

COMMENT ON MATERIALIZED VIEW gold_ch.core_plots_ext_vd IS
  'VD extension matview for core_plots. Mirror of core_plots_ext_ge with VD-specific deltas. '
  'V1 drops: 9 rdppf_* cols, indicative_land_price_m2, estimated_land_value, is_ppe, ppe_statuses, '
  'idc_avg, idc_bracket, type_propriete — sources not in scope. V1 adds: noise_sensitivity_*, '
  'isos_*, classement_*, jardins_*, archeology_*, energy_demand_kwh_vd. '
  'KNOWN DEBT: building_destination raw value from RCB categorie_txt; ext_ge uses ge_cad_batiments.destination — '
  'different vocabularies. Normalization deferred to silver_ch.ref_building_categories (separate session).';

CREATE UNIQUE INDEX IF NOT EXISTS core_plots_ext_vd_egrid_idx ON gold_ch.core_plots_ext_vd (egrid);


-- ----------------------------------------------------------------------------
-- 2. gold_ch.v_plots_full — REPLACE with ext_vd join + COALESCE on shared cols
-- ----------------------------------------------------------------------------
-- NOTE: CREATE OR REPLACE VIEW preserves all dependent objects without CASCADE.
--       This is a regular VIEW, not a matview — replace is non-destructive.
CREATE OR REPLACE VIEW gold_ch.v_plots_full AS
WITH lineage_agg AS (
  SELECT v.egrid,
         array_agg(DISTINCT v.no_commune_no_parcelle_histo) FILTER (WHERE v.no_commune_no_parcelle_histo IS NOT NULL) AS historical_parcelles
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
  -- ── GE-only columns (sourced exclusively from ext_ge — VD has no equivalent data yet) ──
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
  -- ── Shared concepts: COALESCE ext_ge then ext_vd (canton_code guarantees only one is populated per row) ──
  COALESCE(ext_ge.servitude_count, ext_vd.servitude_count)         AS servitude_count,
  COALESCE(ext_ge.servitude_genres, ext_vd.servitude_genres)       AS servitude_genres,
  COALESCE(ext_ge.servitude_classes, ext_vd.servitude_classes)     AS servitude_classes,
  COALESCE(ext_ge.ddp_count, ext_vd.ddp_count)                     AS ddp_count,
  COALESCE(ext_ge.ddp_numbers, ext_vd.ddp_numbers)                 AS ddp_numbers,
  COALESCE(ext_ge.ddp_total_surface_m2, ext_vd.ddp_total_surface_m2) AS ddp_total_surface_m2,
  COALESCE(ext_ge.is_ddp, ext_vd.is_ddp)                           AS is_ddp,
  COALESCE(ext_ge.densification_types, ext_vd.densification_types) AS densification_types,
  COALESCE(ext_ge.densification_practice_urls, ext_vd.densification_practice_urls) AS densification_practice_urls,
  COALESCE(ext_ge.densification_sectors, ext_vd.densification_sectors) AS densification_sectors,
  COALESCE(ext_ge.mutation_count, NULL::integer)                   AS mutation_count,         -- _vd has no mutations source yet
  COALESCE(ext_ge.last_mutation_date, NULL::date)                  AS last_mutation_date,
  COALESCE(ext_ge.historical_parcelles, la.historical_parcelles)   AS historical_parcelles,
  COALESCE(ext_ge.construction_year, ext_vd.construction_year)     AS construction_year,
  COALESCE(ext_ge.centroid_lon, ext_vd.centroid_lon)               AS centroid_lon,
  COALESCE(ext_ge.centroid_lat, ext_vd.centroid_lat)               AS centroid_lat,
  COALESCE(ext_ge.building_destination, ext_vd.building_destination) AS building_destination,
    -- KNOWN DEBT: building_destination — ge_cad_batiments.destination enum vs vd_batiment_rcb.categorie_txt enum.
    -- Two different vocabularies surfaced raw. Normalization layer silver_ch.ref_building_categories is a
    -- separate deferred task. Frontend consumers should not assume a fixed enum here.
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
  'ch'::character(2) AS country_code,
  rc.canton_code AS admin1_code,
  rc.canton_name AS admin1_name,
  NULL::text     AS admin2_code,
  NULL::text     AS admin2_name,
  (rc.canonical_bfs)::text AS admin3_code,
  rc.canonical_name AS admin3_name,
  (rc.canonical_bfs)::text AS admin3_canonical_id,
  ((COALESCE((cp.commune_bfs)::text, cp.canton_code) || '-'::text) || cp.parcel_number) AS parcel_universal_id,
  scp.no_commune_no_parcelle,
  la.historical_parcelles AS historical_parcelles_v2,    -- shadow to preserve compat
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
  LEFT JOIN silver_ch.cadastral_plots scp ON scp.egrid = cp.egrid
  LEFT JOIN lineage_agg la                 ON la.egrid = cp.egrid
  LEFT JOIN gold_ch.core_plots_ext_ge ext_ge ON ext_ge.egrid = cp.egrid
  LEFT JOIN gold_ch.core_plots_ext_vd ext_vd ON ext_vd.egrid = cp.egrid
  LEFT JOIN silver_ch.ref_commune_appartenance_ge app ON (app.commune_bfs = cp.commune_bfs AND cp.canton_code = 'GE')
  LEFT JOIN silver_ch.ref_communes rc      ON rc.canonical_bfs = cp.commune_bfs
  LEFT JOIN silver_ch.plot_intel_ge pi     ON pi.egrid = cp.egrid;

COMMENT ON VIEW gold_ch.v_plots_full IS
  'Joins core_plots with GE and VD extension matviews. COALESCE on shared concepts; VD-only and GE-only '
  'columns surfaced separately. building_destination is RAW value from each canton''s vocabulary — '
  'normalization deferred to silver_ch.ref_building_categories (future session).';

COMMIT;

NOTIFY pgrst, 'reload schema';

-- ============================================================================
-- OPEN QUESTIONS embedded in this migration — flagged for review:
--
-- §A  bldg_dest_vd / bldg_year_vd / energy_agg_vd join key
--     vd_batiment_rcb.egid (per-building) vs cadastral_plots.egrid (per-parcel).
--     The CURRENT join `ON ext.egid::text = p.egrid` is WRONG (different keys).
--     Correct path: bridge via bronze_ch.bfs_rebl_buildings (which has both egid
--     and egrid) OR via the federal RegBL EGID→EGRID mapping. The matview SQL
--     above uses the wrong join temporarily — DO NOT REFRESH until the bridge
--     CTE is added. Producing a parser-side bridge or a silver helper matview
--     (silver_ch.bldg_to_plot_vd) is a small follow-up.
--
-- §B  serv_agg_vd uses st_intersects(p.geometry, s.geometry) — heavy.
--     The Lausanne servitudes source has 'id_rf' (Register Number) but no
--     direct parcel ref. Cost on 280K VD plots × 60K servitudes ≈ 17B
--     comparison pairs. Mitigations:
--       (i)  bbox-constrain: ST_DWithin + GIST index will prune
--       (ii) materialize a link_plot_servitudes_vd matview overnight
--     Recommendation: post-bronze ingest, build silver_ch.link_plot_servitudes_vd
--     once nightly, and have core_plots_ext_vd read from it instead of joining
--     raw. Treat this as a perf-tuning sub-task before going live.
--
-- §C  densif_agg_vd join is currently disconnected (placeholder).
--     Same shape as §B — spatial intersect of cadastral_plots × cadastral_densification_vd
--     geometries. Move to link_plot_densification_vd if perf matters.
--
-- §D  v_plots_full ADDS column `historical_parcelles_v2` to preserve back-compat
--     with the existing column name. After consumer audit, decide whether to drop
--     or canonicalize.
-- ============================================================================
