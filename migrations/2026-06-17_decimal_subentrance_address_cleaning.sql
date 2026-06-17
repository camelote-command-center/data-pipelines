-- ============================================================================
-- 2026-06-17 — Kill decimal sub-entrance (.N) addresses in the v2 plot pipeline
-- ----------------------------------------------------------------------------
-- BUG: plots showed Swiss building-registry sub-entrance addresses ("Baie 10.1",
--   "7.1"). The ".N" code must NEVER show; letter suffixes ("6a") are legit and kept.
--   Fixed once 2026-02-23 (old gold.refresh_plots_registry_step2_addresses) but the
--   filter did not survive the architecture-v2 migration -> 460,492 ref.plots rows dirty.
--
-- ROOT CAUSE: v2 builds addresses upstream (re-LLM core_plots -> v_plots_full -> FDW
--   -> lamap ref.plots); the sub-entrance filter was not ported.
--   *** is_official is UNRELIABLE across cantons (GE flags .N NULL, ZH flags .N TRUE) ***
--   -> use the regex `full_address !~ '\.\d+,'` nationwide, NOT is_official.
--
-- FIX:
--  Part A (re-LLM, permanent): CREATE OR REPLACE gold_ch.v_plots_full -- regex element-
--    filter on addresses/addresses_display (the ref.plots FDW sync source; NO core_plots
--    cascade). See section 1.
--  Part B (lamap, going-forward): public.upsert_mv_plots rebuilds addresses_display/
--    addresses_rf from ref.addresses regex-filtered, COALESCE(clean, raw). See section 2.
--  Backfill of pre-existing rows: server-side helper proc (section 3); lamap large-statement
--    backfills MUST run server-side (client/pooler connection drops on big UPDATEs). The
--    nightly full-refresh also self-heals ref.plots from the now-clean v_plots_full.
--
-- VERIFIED: plot 16/7389 / egrid CH968765636495 -> Baie 4,5,6,6a,7,8,10,12 (.N gone, 6a kept).
-- Black Box: plot_address_sub_entrance_cleaning v1 (4da94fd7).
-- ============================================================================

-- ====== 1. re-LLM: gold_ch.v_plots_full (run on re-LLM znrvddgmczdqoucmykij) ======
CREATE OR REPLACE VIEW gold_ch.v_plots_full AS
 WITH lineage_agg AS (
         SELECT v.egrid,
            array_agg(DISTINCT v.no_commune_no_parcelle_histo) FILTER (WHERE (v.no_commune_no_parcelle_histo IS NOT NULL)) AS historical_parcelles
           FROM gold_ch.v_plot_lineage_full v
          WHERE (v.daterad > '19000101'::text)
          GROUP BY v.egrid
        ), ov_agg AS (
         SELECT o.egrid,
            (count(*))::integer AS owner_count,
            string_agg(o.new_owner_name, ' | '::text ORDER BY o.new_owner_name) AS owners_display,
            jsonb_agg(jsonb_build_object('name', o.new_owner_name, 'entity_id', o.new_entity_id, 'dob', NULL::date, 'type', 'transaction_derived'::text) ORDER BY o.new_owner_name) AS owners,
            string_agg(o.new_owner_name, ' '::text) AS owner_names_search,
            max(o.updated_at) AS max_updated
           FROM silver_ch.plot_owner_overrides o
          WHERE (o.status = 'active'::text)
          GROUP BY o.egrid
        )
 SELECT cp.egrid,
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
    regexp_replace(cp.main_address, '(\d+)\.\d[0-9.]*[a-z]?\s*$'::text, '\1'::text, 'i'::text) AS main_address,
    cp.main_postal_code,
    cp.main_locality,
    COALESCE((SELECT array_agg(e ORDER BY e) FROM unnest(cp.addresses) e WHERE e !~ '\.\d+,'), cp.addresses) AS addresses,
    COALESCE((SELECT string_agg(e, ' | ' ORDER BY e) FROM unnest(string_to_array(cp.addresses_display, ' | ')) e WHERE e !~ '\.\d+,'), cp.addresses_display) AS addresses_display,
    cp.address_search,
    cp.buildings,
    cp.buildings_summary,
    cp.search_all,
    COALESCE(ov.owner_count, cp.owner_count) AS owner_count,
    COALESCE(ov.owners_display, cp.owners_display) AS owners_display,
    COALESCE(ov.owners, cp.owners) AS owners,
    COALESCE(ov.owner_names_search, cp.owner_names_search) AS owner_names_search,
    cp.last_transaction_date,
    cp.last_transaction_price,
    cp.last_transaction_type,
    cp.active_listing_count,
    cp.sad_count,
    GREATEST(cp.updated_at, ov.max_updated) AS updated_at,
    ext_ge.rdppf_zone_primary,
    ext_ge.rdppf_zone_synthetic,
    ext_ge.rdppf_zone_protected,
    ext_ge.rdppf_zone_restricted,
    ext_ge.rdppf_plq,
    ext_ge.rdppf_polluted_site,
    ext_ge.rdppf_forest_distance,
    ext_ge.rdppf_groundwater_protect,
    ext_ge.rdppf_pdf_url,
    COALESCE(ext_ge.servitude_count, ext_vd.servitude_count) AS servitude_count,
    COALESCE(ext_ge.servitude_genres, ext_vd.servitude_genres) AS servitude_genres,
    COALESCE(ext_ge.servitude_classes, ext_vd.servitude_classes) AS servitude_classes,
    COALESCE(ext_ge.ddp_count, ext_vd.ddp_count) AS ddp_count,
    COALESCE(ext_ge.ddp_numbers, ext_vd.ddp_numbers) AS ddp_numbers,
    COALESCE(ext_ge.ddp_total_surface_m2, ext_vd.ddp_total_surface_m2) AS ddp_total_surface_m2,
    COALESCE(ext_ge.is_ddp, ext_vd.is_ddp) AS is_ddp,
    ext_ge.is_ppe,
    ext_ge.ppe_statuses,
    COALESCE(ext_ge.densification_types, ext_vd.densification_types) AS densification_types,
    COALESCE(ext_ge.densification_practice_urls, ext_vd.densification_practice_urls) AS densification_practice_urls,
    COALESCE(ext_ge.densification_sectors, ext_vd.densification_sectors) AS densification_sectors,
    app.appartenance,
    'ch'::character(2) AS country_code,
    rc.canton_code AS admin1_code,
    rc.canton_name AS admin1_name,
    NULL::text AS admin2_code,
    NULL::text AS admin2_name,
    (rc.canonical_bfs)::text AS admin3_code,
    rc.canonical_name AS admin3_name,
    (rc.canonical_bfs)::text AS admin3_canonical_id,
    ((COALESCE((cp.commune_bfs)::text, cp.canton_code) || '-'::text) || cp.parcel_number) AS parcel_universal_id,
    scp.no_commune_no_parcelle,
    la.historical_parcelles,
    pi.zone_primaire,
        CASE
            WHEN (reg.zone_regime = 'lgzd_development'::text) THEN NULL::numeric
            ELSE pi.zone_ius_standard
        END AS zone_ius_standard,
        CASE
            WHEN (reg.zone_regime = 'lgzd_development'::text) THEN NULL::numeric
            ELSE pi.zone_ius_max
        END AS zone_ius_max,
        CASE
            WHEN (reg.zone_regime = 'lgzd_development'::text) THEN NULL::numeric
            ELSE pi.ius_hpe
        END AS ius_hpe,
        CASE
            WHEN (reg.zone_regime = 'lgzd_development'::text) THEN NULL::numeric
            ELSE pi.ius_thpe
        END AS ius_thpe,
        CASE
            WHEN (reg.zone_regime = 'lgzd_development'::text) THEN NULL::numeric
            ELSE pi.ius_realistic_ceiling
        END AS ius_realistic_ceiling,
        CASE
            WHEN (reg.zone_regime = 'lgzd_development'::text) THEN NULL::numeric
            ELSE pi.ius_legal_ceiling
        END AS ius_legal_ceiling,
        CASE
            WHEN (reg.zone_regime = 'lgzd_development'::text) THEN NULL::text
            ELSE pi.ius_dero_status
        END AS ius_dero_status,
        CASE
            WHEN (reg.zone_regime = 'lgzd_development'::text) THEN NULL::numeric
            ELSE pi.solde_pct_legal
        END AS solde_pct_legal,
        CASE
            WHEN (reg.zone_regime = 'lgzd_development'::text) THEN NULL::numeric
            ELSE pi.solde_pct_standard
        END AS solde_pct_standard,
    pi.surface_brut_de_plancher_hors_sol_m2,
        CASE
            WHEN (reg.zone_regime = 'lgzd_development'::text) THEN NULL::numeric
            ELSE pi.surface_potentielle_m2
        END AS surface_potentielle_m2,
        CASE
            WHEN (reg.zone_regime = 'lgzd_development'::text) THEN NULL::numeric
            ELSE pi.surface_residuelle_m2
        END AS surface_residuelle_m2,
        CASE
            WHEN (reg.zone_regime = 'lgzd_development'::text) THEN NULL::numeric
            ELSE pi.surface_potentielle_legal_m2
        END AS surface_potentielle_legal_m2,
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
    pi.rdppf_pnp_zone,
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
    ext_vd.building_destination,
    ext_vd.construction_year AS construction_year_vd,
    reg.zone_regime,
    reg.dev_zone_code,
    reg.dev_id_min,
    reg.plq_linked,
    reg.dev_status
   FROM (((((((((gold_ch.core_plots cp
     LEFT JOIN silver_ch.cadastral_plots scp ON ((scp.egrid = cp.egrid)))
     LEFT JOIN lineage_agg la ON ((la.egrid = cp.egrid)))
     LEFT JOIN ov_agg ov ON ((ov.egrid = cp.egrid)))
     LEFT JOIN gold_ch.core_plots_ext_ge ext_ge ON ((ext_ge.egrid = cp.egrid)))
     LEFT JOIN gold_ch.core_plots_ext_vd ext_vd ON ((ext_vd.egrid = cp.egrid)))
     LEFT JOIN silver_ch.ref_commune_appartenance_ge app ON (((app.commune_bfs = cp.commune_bfs) AND (cp.canton_code = 'GE'::text))))
     LEFT JOIN silver_ch.ref_communes rc ON ((rc.canonical_bfs = cp.commune_bfs)))
     LEFT JOIN silver_ch.plot_intel_ge pi ON ((pi.egrid = cp.egrid)))
     LEFT JOIN silver_ch.plot_dev_regime_ge reg ON (((reg.egrid)::text = cp.egrid)));

-- ====== 2. lamap_db: public.upsert_mv_plots (run on lamap fckdwddgtdbvhzloejni) ======
CREATE OR REPLACE FUNCTION public.upsert_mv_plots(p_since timestamp with time zone DEFAULT NULL::timestamp with time zone)
 RETURNS integer
 LANGUAGE plpgsql
 SET statement_timeout TO '90min'
AS $function$

DECLARE
  v_since timestamptz := COALESCE(p_since, now() - interval '36 hours');
  v_changed_egrids text[];
  v_count integer;
BEGIN
  SELECT array_agg(DISTINCT egrid) INTO v_changed_egrids
  FROM (
    SELECT egrid FROM ref.plots WHERE updated_at > v_since AND egrid IS NOT NULL
    UNION SELECT egrid FROM ref.buildings WHERE updated_at > v_since AND egrid IS NOT NULL
    UNION SELECT egrid FROM ref.addresses WHERE updated_at > v_since AND egrid IS NOT NULL
    UNION SELECT l.egrid FROM ref.link_plot_sad l JOIN ref.sad s ON s.sad_id = l.sad_id WHERE s.updated_at > v_since AND l.egrid IS NOT NULL
    UNION SELECT l.egrid FROM ref.link_plot_listings l JOIN ref.listings li ON li.listing_id = l.listing_id WHERE li.updated_at > v_since AND l.egrid IS NOT NULL
  ) s WHERE egrid IS NOT NULL;

  IF v_changed_egrids IS NULL OR array_length(v_changed_egrids, 1) IS NULL THEN
    RETURN 0;
  END IF;

  WITH
  addresses_agg AS (
    SELECT a.egrid,
      array_agg(DISTINCT a.street_name) FILTER (WHERE a.street_name IS NOT NULL) AS street_names,
      array_agg(DISTINCT a.postal_code) FILTER (WHERE a.postal_code IS NOT NULL) AS npa_array,
      bool_or(a.is_official) AS has_official_address,
      (array_agg(a.full_address ORDER BY a.is_official DESC NULLS LAST))[1] AS official_address,
      string_agg(a.full_address, ' | ' ORDER BY a.full_address) FILTER (WHERE a.full_address !~ '\.\d+,') AS addresses_display_clean,
      array_agg(a.full_address ORDER BY a.full_address) FILTER (WHERE a.full_address !~ '\.\d+,') AS addresses_clean
    FROM ref.addresses a WHERE a.egrid = ANY(v_changed_egrids)
    GROUP BY a.egrid
  ),
  buildings_agg AS (
    SELECT
      b.egrid,
      count(*)::integer AS buildings_count_computed,
      sum(b.footprint_m2) AS footprint_total_m2,
      min(b.construction_year) FILTER (WHERE b.construction_year IS NOT NULL) AS construction_year_min,
      max(b.construction_year) FILTER (WHERE b.construction_year IS NOT NULL) AS construction_year_max,
      avg(b.construction_year) FILTER (WHERE b.construction_year IS NOT NULL)::integer AS construction_year_avg,
      (array_agg(b.construction_year_estimated ORDER BY b.construction_year_estimated ASC NULLS LAST))[1] AS construction_year_estimated,
      (array_agg(b.construction_period_label   ORDER BY b.construction_year_estimated ASC NULLS LAST))[1] AS construction_period_label,
      max(b.renovation_year) FILTER (WHERE b.renovation_year IS NOT NULL) AS renovation_year_max,
      bool_or(b.class_label IN ('Maison à 3+ logements', 'Habitat communautaire')) AS has_apartments,
      bool_or(b.class_label IN ('Maison à 1 logement', 'Maison à 2 logements')) AS has_houses,
      bool_or(b.class_label IN (
        'Immeuble de bureaux','Bâtiment commercial','Bâtiment industriel',
        'Hôtel','Salle de sport','Bât. enseign./recherche',
        'Bât. à usage cult./récr.','Édifice cult./religieux',
        'Autre hébergement','Transport/communication','Réserv./silo/entrepôt'
      )) AS has_commercial,
      max(b.floors_below_ground) FILTER (WHERE b.floors_below_ground IS NOT NULL) AS floors_below_ground_max
    FROM ref.buildings b WHERE b.egrid = ANY(v_changed_egrids)
    GROUP BY b.egrid
  ),
  sad_agg AS (
    SELECT l.egrid,
      max(s.date_depot) AS sad_last_date,
      count(*) FILTER (WHERE s.status IN (
        'pendant','EN_INSTRUCTION','INSTRUCTION','INSTRUCTION DA','EN_SYNTHESE',
        'ENREGISTRE','ENREGISTREMENT','ACCEPTE','ACCEPTE DA','ADOPTE','DECIDE',
        'apres_decision','published','A L''ENQUETE','OPPOSITION','RECOURS','RECOURS DA',
        'PREAVIS CM','CHANTIER','SUIVI_CHANTIER','MISE_EN_SERVICE','VA','RENVOYE',
        'EN SUSPENS','EN SUSPENS DA','PROCEDURE AMS','REFERENDUM'
      ))::integer AS sad_active_count,
      array_agg(DISTINCT s.permit_type_full) FILTER (WHERE s.permit_type_full IS NOT NULL) AS sad_categories
    FROM ref.link_plot_sad l
    JOIN ref.sad s ON s.sad_id = l.sad_id
    WHERE l.egrid = ANY(v_changed_egrids)
    GROUP BY l.egrid
  ),
  /* PATCH 2026-05-23: cadastral_protection PK is (canton_code, ident_dn, parcel_number),
     not egrid — 2766 egrids have N>1 rows. Aggregate to 1:1 by egrid using bool_or
     (semantics: any protection flag → plot is protected). */
  cadastral_protection_agg AS (
    SELECT egrid, bool_or(in_bln) AS in_bln
    FROM ref.cadastral_protection
    WHERE egrid IS NOT NULL AND egrid = ANY(v_changed_egrids)
    GROUP BY egrid
  ),
  /* PATCH 2026-05-23: cadastral_buildings has same N:1 shape (2766 dup egrids).
     Aggregate floor area with sum (total floor area across egrid's buildings). */
  cadastral_buildings_agg AS (
    SELECT egrid, sum(total_floor_area_m2) AS total_floor_area_m2
    FROM ref.cadastral_buildings
    WHERE egrid IS NOT NULL AND egrid = ANY(v_changed_egrids)
    GROUP BY egrid
  )
  INSERT INTO public.mv_plots (
    egrid, no_commune_no_parcelle, parcel_universal_id, historical_parcelles, canton_code, canton_name,
    commune_bfs, commune_name, npa, surface_m2,
    construction_year_oldest, construction_year_newest,
    owners, owners_display, owner_count, owner_names_search,
    primary_heater_energy, primary_category, primary_class,
    zone_affectation, zone_synthetique, rdppf_plq, rdppf_zone_protegee, rdppf_inconstructible,
    sad_count, building_count, dwelling_count,
    addresses_display, last_transaction_date, last_transaction_price,
    idc_avg, idc_bracket, gi_rec_numero,
    geometry, centroid, geometry_lv95, centroid_lv95,
    lv95_e, lv95_n, wgs84_lat, wgs84_lng,
    street_names, has_official_address, official_address,
    buildings_count_computed, footprint_total_m2,
    construction_year_min, construction_year_max, construction_year_avg, renovation_year_max,
    construction_year_estimated, construction_period_label,
    has_apartments, has_houses, has_commercial,
    sad_last_date, sad_active_count, sad_categories,
    has_pdcom,
    zone_primaire, zone_ius_standard, zone_ius_max, ius_hpe, ius_thpe,
    ius_realistic_ceiling, ius_legal_ceiling, ius_dero_status,
    solde_pct_legal, solde_pct_standard,
    surface_brut_de_plancher_hors_sol_m2, surface_potentielle_m2, surface_residuelle_m2,
    surface_potentielle_legal_m2, score_surface, pool_surface_m2,
    has_pool, has_veranda, volume_total_m3,
    nearest_transport_m, nearest_school_m, nearest_supermarket_m,
    nearest_pharmacy_m, nearest_restaurant_m, amenities_score,
    heating_type, typologie, sous_secteur_nom, permits_count,
    addresses_rf, extrait_rdppf_pdf, rdppf_reglements_speciaux, rdppf_sites_pollues, rdppf_forets,
    lien_rf, appartenance, densification_zone, typologie_categorie, rdppf_pnp_zone,
    floors_below_ground_max,
    in_bln, total_floor_area_m2,
    updated_at, matview_version
  )
  SELECT
    p.egrid,
    p.no_commune_no_parcelle,
    p.parcel_universal_id,
    p.historical_parcelles,
    p.canton_code, p.canton_name,
    p.commune_bfs, p.commune_name,
    COALESCE(aa.npa_array,
             CASE WHEN p.main_postal_code IS NOT NULL
                  THEN ARRAY[p.main_postal_code]::text[] END),
    p.surface_m2, p.construction_year_oldest, p.construction_year_newest,
    p.owners, p.owners_display, COALESCE(p.owner_count, 0), p.owner_names_search,
    p.primary_heater_energy, p.primary_category, p.primary_class,
    p.zone_affectation, p.zone_synthetique, p.rdppf_plq, p.rdppf_zone_protegee, p.rdppf_inconstructible,
    p.sad_count, p.building_count, p.dwelling_count,
    COALESCE(aa.addresses_display_clean, p.addresses_display) AS addresses_display, p.last_transaction_date, p.last_transaction_price,
    p.idc_avg, p.idc_bracket, p.gi_rec_numero,
    ST_SetSRID(p.geometry, 4326),
    ST_SetSRID(p.centroid, 4326),
    ST_Transform(ST_SetSRID(p.geometry, 4326), 2056),
    ST_Transform(ST_SetSRID(p.centroid, 4326), 2056),
    p.lv95_e, p.lv95_n, p.wgs84_lat, p.wgs84_lng,
    aa.street_names, aa.has_official_address, aa.official_address,
    ba.buildings_count_computed, ba.footprint_total_m2,
    ba.construction_year_min, ba.construction_year_max, ba.construction_year_avg, ba.renovation_year_max,
    ba.construction_year_estimated, ba.construction_period_label,
    COALESCE(ba.has_apartments, false), COALESCE(ba.has_houses, false), COALESCE(ba.has_commercial, false),
    sa.sad_last_date, sa.sad_active_count, sa.sad_categories,
    COALESCE(pp.has_pdcom, false),
    p.zone_primaire, p.zone_ius_standard, p.zone_ius_max,
    p.ius_hpe, p.ius_thpe,
    p.ius_realistic_ceiling, p.ius_legal_ceiling, p.ius_dero_status,
    p.solde_pct_legal, p.solde_pct_standard,
    p.surface_brut_de_plancher_hors_sol_m2, p.surface_potentielle_m2, p.surface_residuelle_m2,
    p.surface_potentielle_legal_m2, p.score_surface, p.pool_surface_m2,
    COALESCE(p.has_pool, false), COALESCE(p.has_veranda, false), p.volume_total_m3,
    p.nearest_transport_m, p.nearest_school_m, p.nearest_supermarket_m,
    p.nearest_pharmacy_m, p.nearest_restaurant_m, p.amenities_score,
    COALESCE(p.heating_type, p.primary_heater_energy) AS heating_type,
    COALESCE(p.typologie, p.primary_category) AS typologie,
    p.sous_secteur_nom,
    p.permits_count,
    COALESCE(aa.addresses_clean, p.addresses) AS addresses, p.extrait_rdppf_pdf, p.rdppf_reglements_speciaux, p.rdppf_sites_pollues, p.rdppf_forets,
    p.lien_rf, p.appartenance, p.densification_zone, p.typologie_categorie, p.rdppf_pnp_zone,
    ba.floors_below_ground_max,
    cp.in_bln,
    cb.total_floor_area_m2,
    now(),
    'v1.9-2026-05-19-writer-authoritative-stamp+constr_period'::text
  FROM ref.plots p
  LEFT JOIN addresses_agg aa ON aa.egrid = p.egrid
  LEFT JOIN buildings_agg ba ON ba.egrid = p.egrid
  LEFT JOIN sad_agg sa ON sa.egrid = p.egrid
  LEFT JOIN ref.plot_pdcom_summary pp ON pp.egrid = p.egrid
  LEFT JOIN cadastral_buildings_agg cb ON cb.egrid = p.egrid
  LEFT JOIN cadastral_protection_agg cp ON cp.egrid = p.egrid
  WHERE p.egrid = ANY(v_changed_egrids)
  ON CONFLICT (egrid) DO UPDATE SET
    no_commune_no_parcelle = EXCLUDED.no_commune_no_parcelle,
    parcel_universal_id = EXCLUDED.parcel_universal_id,
    historical_parcelles = EXCLUDED.historical_parcelles,
    canton_code = EXCLUDED.canton_code,
    canton_name = EXCLUDED.canton_name,
    commune_bfs = EXCLUDED.commune_bfs,
    commune_name = EXCLUDED.commune_name,
    npa = EXCLUDED.npa,
    surface_m2 = EXCLUDED.surface_m2,
    construction_year_oldest = EXCLUDED.construction_year_oldest,
    construction_year_newest = EXCLUDED.construction_year_newest,
    owners = EXCLUDED.owners,
    owners_display = EXCLUDED.owners_display,
    owner_count = EXCLUDED.owner_count,
    owner_names_search = EXCLUDED.owner_names_search,
    primary_heater_energy = EXCLUDED.primary_heater_energy,
    primary_category = EXCLUDED.primary_category,
    primary_class = EXCLUDED.primary_class,
    zone_affectation = EXCLUDED.zone_affectation,
    zone_synthetique = EXCLUDED.zone_synthetique,
    rdppf_plq = EXCLUDED.rdppf_plq,
    rdppf_zone_protegee = EXCLUDED.rdppf_zone_protegee,
    rdppf_inconstructible = EXCLUDED.rdppf_inconstructible,
    sad_count = EXCLUDED.sad_count,
    building_count = EXCLUDED.building_count,
    dwelling_count = EXCLUDED.dwelling_count,
    addresses_display = EXCLUDED.addresses_display,
    last_transaction_date = EXCLUDED.last_transaction_date,
    last_transaction_price = EXCLUDED.last_transaction_price,
    idc_avg = EXCLUDED.idc_avg,
    idc_bracket = EXCLUDED.idc_bracket,
    gi_rec_numero = EXCLUDED.gi_rec_numero,
    geometry = EXCLUDED.geometry,
    centroid = EXCLUDED.centroid,
    geometry_lv95 = EXCLUDED.geometry_lv95,
    centroid_lv95 = EXCLUDED.centroid_lv95,
    lv95_e = EXCLUDED.lv95_e,
    lv95_n = EXCLUDED.lv95_n,
    wgs84_lat = EXCLUDED.wgs84_lat,
    wgs84_lng = EXCLUDED.wgs84_lng,
    street_names = EXCLUDED.street_names,
    has_official_address = EXCLUDED.has_official_address,
    official_address = EXCLUDED.official_address,
    buildings_count_computed = EXCLUDED.buildings_count_computed,
    footprint_total_m2 = EXCLUDED.footprint_total_m2,
    construction_year_min = EXCLUDED.construction_year_min,
    construction_year_max = EXCLUDED.construction_year_max,
    construction_year_avg = EXCLUDED.construction_year_avg,
    construction_year_estimated = EXCLUDED.construction_year_estimated,
    construction_period_label   = EXCLUDED.construction_period_label,
    renovation_year_max = EXCLUDED.renovation_year_max,
    has_apartments = EXCLUDED.has_apartments,
    has_houses = EXCLUDED.has_houses,
    has_commercial = EXCLUDED.has_commercial,
    sad_last_date = EXCLUDED.sad_last_date,
    sad_active_count = EXCLUDED.sad_active_count,
    sad_categories = EXCLUDED.sad_categories,
    has_pdcom = EXCLUDED.has_pdcom,
    zone_primaire = EXCLUDED.zone_primaire,
    zone_ius_standard = EXCLUDED.zone_ius_standard,
    zone_ius_max = EXCLUDED.zone_ius_max,
    ius_hpe = EXCLUDED.ius_hpe,
    ius_thpe = EXCLUDED.ius_thpe,
    ius_realistic_ceiling = EXCLUDED.ius_realistic_ceiling,
    ius_legal_ceiling = EXCLUDED.ius_legal_ceiling,
    ius_dero_status = EXCLUDED.ius_dero_status,
    solde_pct_legal = EXCLUDED.solde_pct_legal,
    solde_pct_standard = EXCLUDED.solde_pct_standard,
    surface_brut_de_plancher_hors_sol_m2 = EXCLUDED.surface_brut_de_plancher_hors_sol_m2,
    surface_potentielle_m2 = EXCLUDED.surface_potentielle_m2,
    surface_residuelle_m2 = EXCLUDED.surface_residuelle_m2,
    surface_potentielle_legal_m2 = EXCLUDED.surface_potentielle_legal_m2,
    score_surface = EXCLUDED.score_surface,
    pool_surface_m2 = EXCLUDED.pool_surface_m2,
    has_pool = EXCLUDED.has_pool,
    has_veranda = EXCLUDED.has_veranda,
    volume_total_m3 = EXCLUDED.volume_total_m3,
    nearest_transport_m = EXCLUDED.nearest_transport_m,
    nearest_school_m = EXCLUDED.nearest_school_m,
    nearest_supermarket_m = EXCLUDED.nearest_supermarket_m,
    nearest_pharmacy_m = EXCLUDED.nearest_pharmacy_m,
    nearest_restaurant_m = EXCLUDED.nearest_restaurant_m,
    amenities_score = EXCLUDED.amenities_score,
    heating_type = EXCLUDED.heating_type,
    typologie = EXCLUDED.typologie,
    sous_secteur_nom = EXCLUDED.sous_secteur_nom,
    permits_count = EXCLUDED.permits_count,
    addresses_rf = EXCLUDED.addresses_rf,
    extrait_rdppf_pdf = EXCLUDED.extrait_rdppf_pdf,
    rdppf_reglements_speciaux = EXCLUDED.rdppf_reglements_speciaux,
    rdppf_sites_pollues = EXCLUDED.rdppf_sites_pollues,
    rdppf_forets = EXCLUDED.rdppf_forets,
    lien_rf = EXCLUDED.lien_rf,
    appartenance = EXCLUDED.appartenance,
    densification_zone = EXCLUDED.densification_zone,
    typologie_categorie = EXCLUDED.typologie_categorie,
    rdppf_pnp_zone = EXCLUDED.rdppf_pnp_zone,
    floors_below_ground_max = EXCLUDED.floors_below_ground_max,
    in_bln = EXCLUDED.in_bln,
    total_floor_area_m2 = EXCLUDED.total_floor_area_m2,
    matview_version = EXCLUDED.matview_version,
    updated_at = now();

  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END;
$function$


-- ====== 3. lamap_db: one-time backfill helper (server-side, idempotent) ======
CREATE OR REPLACE PROCEDURE public.clean_dotted_addresses_full_260617()
LANGUAGE plpgsql AS $$
DECLARE iters int := 0;
BEGIN
  SET statement_timeout='0';
  DROP TABLE IF EXISTS public._dirty_work;
  CREATE TABLE public._dirty_work AS
    SELECT egrid FROM ref.plots WHERE addresses_display ~ '\.\d+,'
    UNION SELECT egrid FROM public.mv_plots WHERE addresses_display ~ '\.\d+,';
  CREATE INDEX ON public._dirty_work(egrid);
  COMMIT;
  LOOP
    iters := iters + 1; EXIT WHEN iters > 400;
    DROP TABLE IF EXISTS _b;
    CREATE TEMP TABLE _b AS SELECT egrid FROM public._dirty_work LIMIT 4000;
    EXIT WHEN (SELECT count(*) FROM _b) = 0;
    BEGIN
      WITH clean AS (SELECT a.egrid,
          string_agg(a.full_address,' | ' ORDER BY a.full_address) FILTER (WHERE a.full_address !~ '\.\d+,') AS disp,
          array_agg(a.full_address ORDER BY a.full_address)        FILTER (WHERE a.full_address !~ '\.\d+,') AS arr
        FROM ref.addresses a WHERE a.egrid IN (SELECT egrid FROM _b) GROUP BY a.egrid)
      UPDATE ref.plots p SET addresses_display=c.disp, addresses=c.arr FROM clean c WHERE p.egrid=c.egrid AND c.disp IS NOT NULL;
      WITH clean AS (SELECT a.egrid,
          string_agg(a.full_address,' | ' ORDER BY a.full_address) FILTER (WHERE a.full_address !~ '\.\d+,') AS disp,
          array_agg(a.full_address ORDER BY a.full_address)        FILTER (WHERE a.full_address !~ '\.\d+,') AS arr
        FROM ref.addresses a WHERE a.egrid IN (SELECT egrid FROM _b) GROUP BY a.egrid)
      UPDATE public.mv_plots m SET addresses_display=c.disp, addresses_rf=c.arr FROM clean c WHERE m.egrid=c.egrid AND c.disp IS NOT NULL;
      DELETE FROM public._dirty_work d USING _b WHERE d.egrid=_b.egrid;
    EXCEPTION WHEN deadlock_detected OR lock_not_available THEN NULL;
    END;
    COMMIT;
  END LOOP;
  DROP TABLE IF EXISTS public._dirty_work;
END $$;
