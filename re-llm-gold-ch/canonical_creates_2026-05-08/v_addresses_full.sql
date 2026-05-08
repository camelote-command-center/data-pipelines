CREATE OR REPLACE VIEW gold_ch.v_addresses_full AS
SELECT a.edid,a.egid,a.egrid,a.egaid,a.street_name,a.street_number,a.postal_code,a.locality,a.is_official,a.canton_code,a.canton_name,a.commune_bfs,a.commune_name,a.wgs84_point,a.lv95_e,a.lv95_n,a.full_address,a.fts,a.updated_at,
       'ch'::char(2) AS country_code,
       lower(rc.canton_code) AS admin1_code,
       rc.canton_name AS admin1_name,
       NULL::text AS admin2_code,
       NULL::text AS admin2_name,
       rc.canonical_bfs::text AS admin3_code,
       rc.canonical_name AS admin3_name,
       rc.canonical_bfs::text AS admin3_canonical_id
FROM gold_ch.core_addresses a
LEFT JOIN silver_ch.ref_communes rc ON rc.canonical_bfs = a.commune_bfs
