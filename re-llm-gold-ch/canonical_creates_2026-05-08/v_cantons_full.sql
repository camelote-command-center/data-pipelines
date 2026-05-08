CREATE OR REPLACE VIEW gold_ch.v_cantons_full AS
SELECT c.canton_code,c.canton_name,c.canton_name_de,c.canton_name_it,c.commune_count,c.total_population,c.total_transactions,c.total_listings,c.total_sads,c.geometry,c.has_data,c.updated_at,
       'ch'::char(2) AS country_code,
       lower(c.canton_code) AS admin1_code,
       c.canton_name AS admin1_name,
       NULL::text AS admin2_code,
       NULL::text AS admin2_name,
       NULL::text AS admin3_code,
       NULL::text AS admin3_name,
       NULL::text AS admin3_canonical_id
FROM gold_ch.core_cantons c
