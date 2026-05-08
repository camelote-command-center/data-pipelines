-- gold_ch.core_cantons
CREATE MATERIALIZED VIEW gold_ch.core_cantons AS
 WITH canton_stats AS (
         SELECT core_communes.canton_code,
            count(*) AS commune_count,
            sum(
                CASE
                    WHEN core_communes.has_transactions THEN $1
                    ELSE $2
                END) AS communes_with_transactions,
            sum(
                CASE
                    WHEN core_communes.has_listings THEN $3
                    ELSE $4
                END) AS communes_with_listings,
            sum(core_communes.population) AS total_population,
            sum(core_communes.transaction_count) AS total_transactions,
            sum(core_communes.listing_count) AS total_listings,
            sum(core_communes.sad_count) AS total_sads
           FROM gold_ch.core_communes
          GROUP BY core_communes.canton_code
        ), canton_geom AS (
         SELECT core_communes.canton_code,
            st_union(core_communes.geometry) AS geometry
           FROM gold_ch.core_communes
          WHERE (core_communes.geometry IS NOT NULL)
          GROUP BY core_communes.canton_code
        )
 SELECT c.code AS canton_code,
    c.name_fr AS canton_name,
    c.name_de AS canton_name_de,
    c.name_it AS canton_name_it,
    cs.commune_count,
    cs.total_population,
    cs.total_transactions,
    cs.total_listings,
    cs.total_sads,
    cg.geometry,
    ((cs.total_transactions > ($5)::numeric) OR (cs.total_listings > $6)) AS has_data,
    now() AS updated_at
   FROM ((ref.cantons c
     LEFT JOIN canton_stats cs ON ((cs.canton_code = c.code)))
     LEFT JOIN canton_geom cg ON ((cg.canton_code = c.code)))
