CREATE OR REPLACE VIEW gold_ch.v_buildings_full AS
SELECT cb.egid,cb.egrid,cb.canton_code,cb.canton_name,cb.commune_bfs,cb.commune_name,cb.category_code,cb.category_label,cb.class_code,cb.class_label,cb.status_code,cb.status_label,cb.construction_year,cb.construction_year_estimated,cb.construction_period_label,cb.construction_year_source,cb.renovation_year,cb.demolition_year,cb.floors_above_ground,cb.floors_below_ground,cb.dwelling_count,cb.footprint_m2,cb.volume_m3,cb.heater_type_1_label,cb.heater_energy_1_label,cb.heater_type_2_label,cb.heater_energy_2_label,cb.hot_water_type_1_label,cb.hot_water_energy_1_label,cb.hot_water_type_2_label,cb.hot_water_energy_2_label,cb.footprint_geometry,cb.wgs84_point,cb.lv95_e,cb.lv95_n,cb.idc_last_value,cb.idc_last_year,cb.idc_avg_3years,cb.idc_bracket_3years,cb.idc_bracket_1year,cb.minergie_standard,cb.minergie_certificate,cb.minergie_ebf_m2,cb.idc_sre_m2,cb.updated_at,ext_ge.ge_destination,ext_ge.ge_nomenclature,ext_ge.ge_nomen_classe,ext_ge.ge_egrid_liste,
       'ch'::char(2) AS country_code,
       lower(rc.canton_code) AS admin1_code,
       rc.canton_name AS admin1_name,
       NULL::text AS admin2_code,
       NULL::text AS admin2_name,
       rc.canonical_bfs::text AS admin3_code,
       rc.canonical_name AS admin3_name,
       rc.canonical_bfs::text AS admin3_canonical_id
FROM gold_ch.core_buildings cb
LEFT JOIN gold_ch.core_buildings_ext_ge ext_ge ON ext_ge.egid::text = cb.egid::text
LEFT JOIN silver_ch.ref_communes rc ON rc.canonical_bfs = cb.commune_bfs
