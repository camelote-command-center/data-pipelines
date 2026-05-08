CREATE OR REPLACE VIEW gold_ch.v_units_full AS
SELECT u.egid,u.ewid,u.edid,u.egrid,u.canton_code,u.canton_name,u.commune_bfs,u.commune_name,u.unit_number,u.entrance_number,u.floor_code,u.floor_label,u.description,u.is_multi_dwelling,u.construction_year,u.demolition_year,u.status_code,u.status_label,u.surface_m2,u.rooms,u.has_kitchen,u.updated_at,
       'ch'::char(2) AS country_code,
       lower(rc.canton_code) AS admin1_code,
       rc.canton_name AS admin1_name,
       NULL::text AS admin2_code,
       NULL::text AS admin2_name,
       rc.canonical_bfs::text AS admin3_code,
       rc.canonical_name AS admin3_name,
       rc.canonical_bfs::text AS admin3_canonical_id
FROM gold_ch.core_units u
LEFT JOIN silver_ch.ref_communes rc ON rc.canonical_bfs = u.commune_bfs
