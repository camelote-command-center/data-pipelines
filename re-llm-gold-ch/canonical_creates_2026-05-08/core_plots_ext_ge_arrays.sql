-- core_plots_ext_ge — REBUILD with array_agg() for 7 multi-value columns
-- Fixes type mismatch: lamap-db ref.plots expects text[] for these 7 columns
--   servitude_genres, servitude_classes
--   densification_types, densification_practice_urls, densification_sectors
--   ddp_numbers
--   ppe_statuses
-- All other columns + CTEs unchanged from previous version.

CREATE MATERIALIZED VIEW gold_ch.core_plots_ext_ge AS
WITH energy_agg AS (
    SELECT cb.egrid,
        avg(cb.idc_avg_3years) AS idc_avg,
        mode() WITHIN GROUP (ORDER BY cb.idc_bracket_3years) AS idc_bracket
    FROM gold_ch.core_buildings cb
    WHERE cb.egrid IS NOT NULL AND cb.egrid::text <> '' AND cb.canton_code::text = 'GE'
    GROUP BY cb.egrid
), ppe_agg AS (
    -- CHANGED: was DISTINCT ON returning scalar statut. Now aggregates all distinct statuses per parcelle.
    SELECT no_commune_no_parcelle,
        array_agg(DISTINCT statut) FILTER (WHERE statut IS NOT NULL) AS ppe_statuses
    FROM silver_ch.cadastral_ppe
    WHERE canton_code = 'GE' AND no_commune_no_parcelle IS NOT NULL
    GROUP BY no_commune_no_parcelle
), ddp_agg AS (
    SELECT cadastral_ddp.no_commune_no_parcelle,
        count(*)::integer AS ddp_count,
        -- CHANGED: string_agg → array_agg
        array_agg(DISTINCT cadastral_ddp.no_ddp) FILTER (WHERE cadastral_ddp.no_ddp IS NOT NULL) AS ddp_numbers,
        sum(cadastral_ddp.surface_ddp_m2) AS ddp_total_surface_m2
    FROM silver_ch.cadastral_ddp
    WHERE cadastral_ddp.canton_code = 'GE' AND cadastral_ddp.no_commune_no_parcelle IS NOT NULL
    GROUP BY cadastral_ddp.no_commune_no_parcelle
), serv_agg AS (
    SELECT cadastral_servitudes.no_commune_no_parcelle,
        count(*)::integer AS servitudes_count,
        -- CHANGED: string_agg → array_agg (×2)
        array_agg(DISTINCT cadastral_servitudes.genre) FILTER (WHERE cadastral_servitudes.genre IS NOT NULL) AS servitude_genres,
        array_agg(DISTINCT cadastral_servitudes.classe) FILTER (WHERE cadastral_servitudes.classe IS NOT NULL) AS servitude_classes
    FROM silver_ch.cadastral_servitudes
    WHERE cadastral_servitudes.canton_code = 'GE'
    GROUP BY cadastral_servitudes.no_commune_no_parcelle
), mut_agg AS (
    SELECT cadastral_mutations.egrid,
        count(*)::integer AS mutation_count,
        max(cadastral_mutations.radiation_date) AS last_mutation_date,
        array_agg(DISTINCT cadastral_mutations.no_commune_no_parcelle_histo) FILTER (WHERE cadastral_mutations.no_commune_no_parcelle_histo IS NOT NULL) AS historical_parcelles
    FROM silver_ch.cadastral_mutations
    WHERE cadastral_mutations.canton_code = 'GE'
    GROUP BY cadastral_mutations.egrid
), land_prices AS (
    SELECT DISTINCT ON (cadastral_land_prices.commune) cadastral_land_prices.commune,
        cadastral_land_prices.price_avg_m2
    FROM silver_ch.cadastral_land_prices
    WHERE cadastral_land_prices.canton_code = 'GE'
    ORDER BY cadastral_land_prices.commune, cadastral_land_prices.year DESC
), densif_agg AS (
    SELECT cadastral_densification.egrid,
        -- CHANGED: string_agg → array_agg (×3)
        array_agg(DISTINCT cadastral_densification.densification_type) FILTER (WHERE cadastral_densification.densification_type IS NOT NULL) AS densification_types,
        array_agg(DISTINCT cadastral_densification.administrative_practice_url) FILTER (WHERE cadastral_densification.administrative_practice_url IS NOT NULL) AS densification_practice_urls,
        array_agg(DISTINCT cadastral_densification.sector_name) FILTER (WHERE cadastral_densification.sector_name IS NOT NULL) AS densification_sectors
    FROM silver_ch.cadastral_densification
    WHERE cadastral_densification.canton_code = 'GE' AND cadastral_densification.egrid IS NOT NULL
    GROUP BY cadastral_densification.egrid
), first_addr AS (
    SELECT DISTINCT ON (cb.egrid) cb.egrid,
        cb.commune_name AS bldg_commune_name
    FROM gold_ch.core_buildings cb
    WHERE cb.egrid IS NOT NULL AND cb.egrid::text <> '' AND cb.canton_code::text = 'GE'
    ORDER BY cb.egrid, cb.egid
), bldg_dest AS (
    SELECT DISTINCT ON (cb.egrid) cb.egrid,
        gb.destination AS building_destination
    FROM gold_ch.core_buildings cb
    JOIN bronze_ch.ge_cad_batiments gb ON gb.egid::text = cb.egid::text
    WHERE cb.canton_code::text = 'GE' AND cb.egrid IS NOT NULL AND cb.egrid::text <> '' AND gb.destination IS NOT NULL
    ORDER BY cb.egrid, COALESCE(cb.footprint_m2, 0::numeric) DESC, cb.egid
), bldg_year AS (
    SELECT cb.egrid,
        min(cb.construction_year) FILTER (WHERE cb.construction_year >= 1000 AND cb.construction_year <= EXTRACT(year FROM CURRENT_DATE)::integer) AS construction_year
    FROM gold_ch.core_buildings cb
    WHERE cb.canton_code::text = 'GE' AND cb.egrid IS NOT NULL AND cb.egrid::text <> ''
    GROUP BY cb.egrid
), centroids AS (
    SELECT core_plots.egrid,
        st_x(st_centroid(core_plots.geometry)) AS centroid_lon,
        st_y(st_centroid(core_plots.geometry)) AS centroid_lat
    FROM gold_ch.core_plots
    WHERE core_plots.canton_code = 'GE' AND core_plots.geometry IS NOT NULL
)
SELECT p.egrid,
    p.no_commune_no_parcelle,
    r.genre AS type_propriete,
    r.zones_affect_prim AS rdppf_zone_primary,
    r.zones_affect_synt AS rdppf_zone_synthetic,
    r.plq AS rdppf_plq,
    r.pnp AS rdppf_pnp,
    r.zone_protegee AS rdppf_zone_protected,
    r.sites_inconstructibles AS rdppf_zone_restricted,
    r.reglements_speciaux AS rdppf_reglements_speciaux,
    r.cadastre_site_pollues AS rdppf_polluted_site,
    r.limites_foret_statiques AS rdppf_forest_distance,
    r.zones_protec_eaux_sout AS rdppf_groundwater_protect,
    r.extrait_rdppf_pdf AS rdppf_pdf_url,
    ppe.no_commune_no_parcelle IS NOT NULL AS is_ppe,
    ppe.ppe_statuses,
    ddp.no_commune_no_parcelle IS NOT NULL AS is_ddp,
    ddp.ddp_count,
    ddp.ddp_numbers,
    ddp.ddp_total_surface_m2,
    ea.idc_avg,
    ea.idc_bracket,
    da.densification_types,
    da.densification_practice_urls,
    da.densification_sectors,
    lp.price_avg_m2 AS indicative_land_price_m2,
    CASE
        WHEN p.surface_m2 IS NOT NULL AND lp.price_avg_m2 IS NOT NULL THEN round(p.surface_m2 * lp.price_avg_m2)
        ELSE NULL::numeric
    END AS estimated_land_value,
    COALESCE(sv.servitudes_count, 0) AS servitude_count,
    sv.servitude_genres,
    sv.servitude_classes,
    ma.mutation_count,
    ma.last_mutation_date,
    ma.historical_parcelles,
    bd.building_destination,
    by2.construction_year,
    ct.centroid_lon,
    ct.centroid_lat
FROM silver_ch.cadastral_plots p
LEFT JOIN first_addr fa ON fa.egrid::text = p.egrid
LEFT JOIN silver_ch.cadastral_rdppf r ON r.egrid = p.egrid
LEFT JOIN ppe_agg ppe ON ppe.no_commune_no_parcelle = p.no_commune_no_parcelle
LEFT JOIN ddp_agg ddp ON ddp.no_commune_no_parcelle = p.no_commune_no_parcelle
LEFT JOIN energy_agg ea ON ea.egrid::text = p.egrid
LEFT JOIN densif_agg da ON da.egrid = p.egrid
LEFT JOIN land_prices lp ON lp.commune = COALESCE(NULLIF(p.commune_name, ''), fa.bldg_commune_name::text)
LEFT JOIN serv_agg sv ON sv.no_commune_no_parcelle = p.no_commune_no_parcelle
LEFT JOIN mut_agg ma ON ma.egrid = p.egrid
LEFT JOIN bldg_dest bd ON bd.egrid::text = p.egrid
LEFT JOIN bldg_year by2 ON by2.egrid::text = p.egrid
LEFT JOIN centroids ct ON ct.egrid = p.egrid
WHERE p.canton_code = 'GE';
