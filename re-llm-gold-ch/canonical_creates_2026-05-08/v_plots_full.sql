CREATE OR REPLACE VIEW gold_ch.v_plots_full AS
SELECT cp.egrid,cp.canton_code,cp.canton_name,cp.commune_bfs,cp.commune_name,cp.gi_rec_numero,cp.sous_secteur_nom,cp.parcel_number,cp.surface_m2,cp.geometry,cp.centroid,cp.lv95_e,cp.lv95_n,cp.wgs84_lat,cp.wgs84_lng,cp.building_count,cp.dwelling_count,cp.construction_year_oldest,cp.construction_year_newest,cp.max_floors,cp.footprint_total_m2,cp.volume_total_m3,cp.primary_category,cp.primary_class,cp.primary_heater_energy,cp.main_address,cp.main_postal_code,cp.main_locality,cp.addresses,cp.addresses_display,cp.address_search,cp.buildings,cp.buildings_summary,cp.search_all,cp.owner_count,cp.owners_display,cp.owners,cp.owner_names_search,cp.last_transaction_date,cp.last_transaction_price,cp.last_transaction_type,cp.active_listing_count,cp.sad_count,cp.updated_at,ext_ge.rdppf_zone_primary,ext_ge.rdppf_zone_synthetic,ext_ge.rdppf_zone_protected,ext_ge.rdppf_zone_restricted,ext_ge.rdppf_plq,ext_ge.rdppf_polluted_site,ext_ge.rdppf_forest_distance,ext_ge.rdppf_groundwater_protect,ext_ge.rdppf_pdf_url,ext_ge.servitude_count,ext_ge.servitude_genres,ext_ge.servitude_classes,ext_ge.ddp_count,ext_ge.ddp_numbers,ext_ge.ddp_total_surface_m2,ext_ge.is_ddp,ext_ge.is_ppe,ext_ge.ppe_statuses,ext_ge.densification_types,ext_ge.densification_practice_urls,ext_ge.densification_sectors, app.appartenance,
       'ch'::char(2) AS country_code,
       lower(rc.canton_code) AS admin1_code,
       rc.canton_name AS admin1_name,
       NULL::text AS admin2_code,
       NULL::text AS admin2_name,
       rc.canonical_bfs::text AS admin3_code,
       rc.canonical_name AS admin3_name,
       rc.canonical_bfs::text AS admin3_canonical_id
FROM gold_ch.core_plots cp
LEFT JOIN gold_ch.core_plots_ext_ge ext_ge ON ext_ge.egrid = cp.egrid
LEFT JOIN silver_ch.ref_commune_appartenance_ge app
       ON app.commune_bfs = cp.commune_bfs AND cp.canton_code = 'GE'
LEFT JOIN silver_ch.ref_communes rc
       ON rc.canonical_bfs = cp.commune_bfs
