-- TEMPORARY HELPER: per-canton mv_plots repaint
-- One-time use after gold rebuild + ref.* sync. DROP after Phase 5 completes.
-- Mirrors public.upsert_mv_plots() logic but filters by canton (or NULL bucket).
-- Uses COMMIT to bound transaction size and avoid pooler/proxy idle drops.

CREATE OR REPLACE PROCEDURE public.repaint_mv_plots_per_canton(p_canton text)
LANGUAGE plpgsql AS $proc$
DECLARE
  v_changed_egrids text[];
  v_count integer;
  v_label text;
BEGIN
  v_label := COALESCE(p_canton, '__NULL__');

  -- Build egrid set from ref.plots filtered by canton (or IS NULL bucket)
  IF p_canton IS NULL THEN
    SELECT array_agg(egrid) INTO v_changed_egrids
    FROM ref.plots WHERE canton_code IS NULL AND egrid IS NOT NULL;
  ELSE
    SELECT array_agg(egrid) INTO v_changed_egrids
    FROM ref.plots WHERE canton_code = p_canton AND egrid IS NOT NULL;
  END IF;

  IF v_changed_egrids IS NULL OR array_length(v_changed_egrids, 1) IS NULL THEN
    RAISE NOTICE 'repaint_mv_plots_per_canton(%): no egrids', v_label;
    RETURN;
  END IF;

  RAISE NOTICE 'repaint_mv_plots_per_canton(%): processing % egrids', v_label, array_length(v_changed_egrids, 1);

  WITH
  addresses_agg AS (
    SELECT a.egrid,
      array_agg(DISTINCT a.street_name) FILTER (WHERE a.street_name IS NOT NULL) AS street_names,
      array_agg(DISTINCT a.postal_code) FILTER (WHERE a.postal_code IS NOT NULL) AS npa_array,
      bool_or(a.is_official) AS has_official_address,
      (array_agg(a.full_address ORDER BY a.is_official DESC NULLS LAST))[1] AS official_address
    FROM ref.addresses a WHERE a.egrid = ANY(v_changed_egrids)
    GROUP BY a.egrid
  ),
  buildings_agg AS (
    SELECT
      b.egrid,
      count(*)::integer AS buildings_count_computed,
      sum(b.footprint_m2) AS footprint_total_m2,
      min(b.construction_year) AS construction_year_min,
      max(b.construction_year) AS construction_year_max,
      avg(b.construction_year)::integer AS construction_year_avg,
      max(b.renovation_year) AS renovation_year_max,
      max(b.floors_below_ground) AS floors_below_ground_max,
      bool_or(b.class_label IN ('Maison à 3+ logements', 'Habitat communautaire')) AS has_apartments,
      bool_or(b.class_label IN ('Maison à 1 logement', 'Maison à 2 logements')) AS has_houses,
      bool_or(b.class_label IN (
        'Immeuble de bureaux','Bâtiment commercial','Bâtiment industriel',
        'Hôtel','Salle de sport','Bât. enseign./recherche',
        'Bât. à usage cult./récr.','Édifice cult./religieux',
        'Autre hébergement','Transport/communication','Réserv./silo/entrepôt'
      )) AS has_commercial
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
  )
  INSERT INTO public.mv_plots (
    egrid, no_commune_no_parcelle, historical_parcelles, canton_code, canton_name,
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
    updated_at, matview_version
  )
  SELECT
    p.egrid, p.no_commune_no_parcelle, p.historical_parcelles, p.canton_code, p.canton_name,
    p.commune_bfs, COALESCE(NULLIF(p.commune_name, ''), spc.commune_name) AS commune_name,
    COALESCE(aa.npa_array,
             CASE WHEN p.main_postal_code IS NOT NULL
                  THEN ARRAY[p.main_postal_code]::text[] END),
    p.surface_m2, p.construction_year_oldest, p.construction_year_newest,
    COALESCE(p.owners, gpr.owners),
    COALESCE(p.owners_display, gpr.owners_display),
    COALESCE(NULLIF(p.owner_count, 0), gpr.owner_count, 0),
    COALESCE(p.owner_names_search, gpr.owners_display),
    p.primary_heater_energy, p.primary_category, p.primary_class,
    p.zone_affectation, p.zone_synthetique, p.rdppf_plq, p.rdppf_zone_protegee, p.rdppf_inconstructible,
    p.sad_count, p.building_count, p.dwelling_count,
    p.addresses_display, p.last_transaction_date, p.last_transaction_price,
    p.idc_avg, p.idc_bracket, p.gi_rec_numero,
    ST_SetSRID(p.geometry, 4326),
    ST_SetSRID(p.centroid, 4326),
    ST_Transform(ST_SetSRID(p.geometry, 4326), 2056),
    ST_Transform(ST_SetSRID(p.centroid, 4326), 2056),
    p.lv95_e, p.lv95_n, p.wgs84_lat, p.wgs84_lng,
    aa.street_names, aa.has_official_address, aa.official_address,
    ba.buildings_count_computed, ba.footprint_total_m2,
    ba.construction_year_min, ba.construction_year_max, ba.construction_year_avg, ba.renovation_year_max,
    COALESCE(ba.has_apartments, false), COALESCE(ba.has_houses, false), COALESCE(ba.has_commercial, false),
    sa.sad_last_date, sa.sad_active_count, sa.sad_categories,
    COALESCE(pp.has_pdcom, false),
    spc.zone_primaire, spc.zone_ius_standard, spc.zone_ius_max,
    spc.ius_hpe, spc.ius_thpe,
    spc.ius_realistic_ceiling, spc.ius_legal_ceiling, spc.ius_dero_status,
    spc.solde_pct_legal, spc.solde_pct_standard,
    gpr.surface_brut_de_plancher_hors_sol_m2, gpr.surface_potentielle_m2, gpr.surface_residuelle_m2,
    gpr.surface_potentielle_legal_m2, gpr.score_surface, gpr.pool_surface_m2,
    COALESCE(gpr.has_pool, false), COALESCE(gpr.has_veranda, false), gpr.volume_total_m3,
    gpr.nearest_transport_m, gpr.nearest_school_m, gpr.nearest_supermarket_m,
    gpr.nearest_pharmacy_m, gpr.nearest_restaurant_m, gpr.amenities_score,
    COALESCE(gpr.heating_type, p.primary_heater_energy),
    COALESCE(gpr.typologie, p.primary_category),
    COALESCE(p.sous_secteur_nom, gpr.sous_secteur_nom),
    gpr.permits_count,
    p.addresses, p.extrait_rdppf_pdf, p.rdppf_reglements_speciaux, p.rdppf_sites_pollues, p.rdppf_forets,
    gpr.lien_rf, gpr.appartenance, gpr.densification_zone, gpr.typologie_categorie, gpr.rdppf_pnp_zone,
    ba.floors_below_ground_max,
    now(),
    'v1.2-2026-05-01'::text
  FROM ref.plots p
  LEFT JOIN addresses_agg aa ON aa.egrid = p.egrid
  LEFT JOIN buildings_agg ba ON ba.egrid = p.egrid
  LEFT JOIN sad_agg sa ON sa.egrid = p.egrid
  LEFT JOIN ref.plot_pdcom_summary pp ON pp.egrid = p.egrid
  LEFT JOIN silver.plots_cad spc ON spc.egrid = p.egrid
  LEFT JOIN gold.plots_registry gpr ON gpr.egrid = p.egrid
  WHERE p.egrid = ANY(v_changed_egrids)
  ON CONFLICT (egrid) DO UPDATE SET
    no_commune_no_parcelle = EXCLUDED.no_commune_no_parcelle,
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
    updated_at = now();


  GET DIAGNOSTICS v_count = ROW_COUNT;
  RAISE NOTICE 'repaint_mv_plots_per_canton(%): upserted % rows', v_label, v_count;

  COMMIT;
END;
$proc$;
