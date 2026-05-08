-- gold_ch.core_plots_ext_ge
CREATE MATERIALIZED VIEW gold_ch.core_plots_ext_ge AS
 WITH energy_agg AS (
         SELECT b.egrid,
            avg(en.idc_avg_3years) AS idc_avg,
            mode() WITHIN GROUP (ORDER BY en.idc_bracket_3years) AS idc_bracket
           FROM (silver_ch.cadastral_buildings b
             JOIN silver_ch.cadastral_energy en ON (((en.egid)::text = (b.egid)::text)))
          WHERE ((b.egrid IS NOT NULL) AND (b.egrid <> $1::text) AND ((b.canton_code)::text = $2::text))
          GROUP BY b.egrid
        ), ppe_agg AS (
         SELECT DISTINCT ON (cadastral_ppe.no_commune_no_parcelle) cadastral_ppe.no_commune_no_parcelle,
            cadastral_ppe.statut
           FROM silver_ch.cadastral_ppe
          WHERE ((cadastral_ppe.canton_code = $3::text) AND (cadastral_ppe.no_commune_no_parcelle IS NOT NULL))
          ORDER BY cadastral_ppe.no_commune_no_parcelle, cadastral_ppe.id
        ), ddp_agg AS (
         SELECT DISTINCT cadastral_ddp.no_commune_no_parcelle
           FROM silver_ch.cadastral_ddp
          WHERE ((cadastral_ddp.canton_code = $4::text) AND (cadastral_ddp.no_commune_no_parcelle IS NOT NULL))
        ), serv_agg AS (
         SELECT cadastral_servitudes.no_commune_no_parcelle,
            (count(*))::integer AS servitudes_count
           FROM silver_ch.cadastral_servitudes
          WHERE (cadastral_servitudes.canton_code = $5::text)
          GROUP BY cadastral_servitudes.no_commune_no_parcelle
        ), mut_agg AS (
         SELECT cadastral_mutations.egrid,
            (count(*))::integer AS mutation_count,
            max(cadastral_mutations.radiation_date) AS last_mutation_date,
            array_agg(DISTINCT cadastral_mutations.no_commune_no_parcelle_histo) FILTER (WHERE (cadastral_mutations.no_commune_no_parcelle_histo IS NOT NULL)) AS historical_parcelles
           FROM silver_ch.cadastral_mutations
          WHERE (cadastral_mutations.canton_code = $6::text)
          GROUP BY cadastral_mutations.egrid
        ), land_prices AS (
         SELECT DISTINCT ON (cadastral_land_prices.commune) cadastral_land_prices.commune,
            cadastral_land_prices.price_avg_m2
           FROM silver_ch.cadastral_land_prices
          WHERE (cadastral_land_prices.canton_code = $7::text)
          ORDER BY cadastral_land_prices.commune, cadastral_land_prices.year DESC
        ), densif_agg AS (
         SELECT DISTINCT ON (cadastral_densification.egrid) cadastral_densification.egrid,
            cadastral_densification.densification_type
           FROM silver_ch.cadastral_densification
          WHERE ((cadastral_densification.canton_code = $8::text) AND (cadastral_densification.egrid IS NOT NULL))
          ORDER BY cadastral_densification.egrid, cadastral_densification.id
        ), first_addr AS (
         SELECT DISTINCT ON (b.egrid) b.egrid,
            b.commune_name AS bldg_commune_name
           FROM silver_ch.cadastral_buildings b
          WHERE ((b.egrid IS NOT NULL) AND (b.egrid <> $9::text) AND ((b.canton_code)::text = $10::text))
          ORDER BY b.egrid, b.egid
        ), bldg_dest AS (
         SELECT DISTINCT ON (b.egrid) b.egrid,
            b.ge_destination AS building_destination
           FROM silver_ch.cadastral_buildings b
          WHERE (((b.canton_code)::text = $11::text) AND (b.egrid IS NOT NULL) AND (b.egrid <> $12::text) AND (b.ge_destination IS NOT NULL))
          ORDER BY b.egrid, COALESCE(b.footprint_m2, ($13)::numeric) DESC, b.egid
        ), bldg_year AS (
         SELECT b.egrid,
            min(b.construction_year) FILTER (WHERE ((b.construction_year >= $14) AND (b.construction_year <= ((EXTRACT($15 FROM CURRENT_DATE))::integer + $16)))) AS construction_year
           FROM silver_ch.cadastral_buildings b
          WHERE (((b.canton_code)::text = $17::text) AND (b.egrid IS NOT NULL) AND (b.egrid <> $18::text))
          GROUP BY b.egrid
        ), centroids AS (
         SELECT core_plots.egrid,
            st_x(st_centroid(core_plots.geometry)) AS centroid_lon,
            st_y(st_centroid(core_plots.geometry)) AS centroid_lat
           FROM gold_ch.core_plots
          WHERE ((core_plots.canton_code = $19::text) AND (core_plots.geometry IS NOT NULL))
        )
 SELECT p.egrid,
    p.no_commune_no_parcelle,
    r.genre AS type_propriete,
    r.zones_affect_prim AS zone_affectation,
    r.zones_affect_synt AS zone_synthetique,
    r.plq AS rdppf_plq,
    r.pnp AS rdppf_pnp,
    r.zone_protegee AS rdppf_zone_protegee,
    r.sites_inconstructibles AS rdppf_inconstructible,
    r.reglements_speciaux AS rdppf_reglements_speciaux,
    r.cadastre_site_pollues AS rdppf_sites_pollues,
    r.limites_foret_statiques AS rdppf_forets,
    r.extrait_rdppf_pdf,
    (ppe.no_commune_no_parcelle IS NOT NULL) AS is_ppe,
    ppe.statut AS ppe_statut,
    (ddp.no_commune_no_parcelle IS NOT NULL) AS has_ddp,
    ea.idc_avg,
    ea.idc_bracket,
    da.densification_type,
    lp.price_avg_m2 AS indicative_land_price_m2,
        CASE
            WHEN ((p.surface_m2 IS NOT NULL) AND (lp.price_avg_m2 IS NOT NULL)) THEN round((p.surface_m2 * lp.price_avg_m2))
            ELSE $20::numeric
        END AS estimated_land_value,
    COALESCE(sv.servitudes_count, $21) AS servitudes_count,
    ma.mutation_count,
    ma.last_mutation_date,
    ma.historical_parcelles,
    bd.building_destination,
    by2.construction_year,
    ct.centroid_lon,
    ct.centroid_lat,
    $22::text AS sous_secteur_nom
   FROM ((((((((((((silver_ch.cadastral_plots p
     LEFT JOIN first_addr fa ON ((fa.egrid = p.egrid)))
     LEFT JOIN silver_ch.cadastral_rdppf r ON ((r.egrid = p.egrid)))
     LEFT JOIN ppe_agg ppe ON ((ppe.no_commune_no_parcelle = p.no_commune_no_parcelle)))
     LEFT JOIN ddp_agg ddp ON ((ddp.no_commune_no_parcelle = p.no_commune_no_parcelle)))
     LEFT JOIN energy_agg ea ON ((ea.egrid = p.egrid)))
     LEFT JOIN densif_agg da ON ((da.egrid = p.egrid)))
     LEFT JOIN land_prices lp ON ((lp.commune = COALESCE(NULLIF(p.commune_name, $23::text), fa.bldg_commune_name))))
     LEFT JOIN serv_agg sv ON ((sv.no_commune_no_parcelle = p.no_commune_no_parcelle)))
     LEFT JOIN mut_agg ma ON ((ma.egrid = p.egrid)))
     LEFT JOIN bldg_dest bd ON ((bd.egrid = p.egrid)))
     LEFT JOIN bldg_year by2 ON ((by2.egrid = p.egrid)))
     LEFT JOIN centroids ct ON ((ct.egrid = p.egrid)))
  WHERE (p.canton_code = $24::text)
