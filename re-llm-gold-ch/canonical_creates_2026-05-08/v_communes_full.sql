CREATE OR REPLACE VIEW gold_ch.v_communes_full AS
SELECT c.commune_bfs,c.commune_name,c.canton_code,c.canton_name,c.npa_list,c.geometry,c.centroid,c.area_km2,c.population,c.pop_year,c.transaction_count,c.listing_count,c.sad_count,c.has_transactions,c.has_listings,c.has_sads,c.benchmark_sale_price_m2,c.benchmark_rent_m2,c.updated_at,
       'ch'::char(2) AS country_code,
       lower(rc.canton_code) AS admin1_code,
       rc.canton_name AS admin1_name,
       NULL::text AS admin2_code,
       NULL::text AS admin2_name,
       rc.canonical_bfs::text AS admin3_code,
       rc.canonical_name AS admin3_name,
       rc.canonical_bfs::text AS admin3_canonical_id
FROM gold_ch.core_communes c
LEFT JOIN silver_ch.ref_communes rc ON rc.canonical_bfs = c.commune_bfs
