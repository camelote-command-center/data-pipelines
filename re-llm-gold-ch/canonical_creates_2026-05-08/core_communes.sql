CREATE MATERIALIZED VIEW gold_ch.core_communes AS
WITH npa_agg AS (
  SELECT commune_bfs, array_agg(DISTINCT npa ORDER BY npa) AS npa_list
  FROM silver_ch.ref_npa_commune
  WHERE commune_bfs IS NOT NULL AND npa IS NOT NULL
  GROUP BY commune_bfs
),
pop_latest AS (
  SELECT DISTINCT ON (bfs_commune_number)
    bfs_commune_number, total_population AS population, year AS pop_year
  FROM silver_ch.market_population
  WHERE total_population IS NOT NULL
  ORDER BY bfs_commune_number, year DESC
),
tx_agg AS (
  SELECT commune_number::int AS bfs, count(*)::bigint AS n
  FROM gold_ch.core_transactions
  WHERE commune_number IS NOT NULL AND commune_number ~ $1
  GROUP BY commune_number::int
),
listing_agg AS (
  -- core_listings has npa, not commune_bfs — aggregate via ref_npa_commune bridge
  SELECT npa_c.commune_bfs AS bfs, count(*)::int AS n
  FROM gold_ch.core_listings cl
  JOIN silver_ch.ref_npa_commune npa_c ON npa_c.npa = cl.npa
  WHERE cl.npa IS NOT NULL AND npa_c.commune_bfs IS NOT NULL
  GROUP BY npa_c.commune_bfs
),
sad_agg AS (
  -- core_sad has 'commune' name + 'communes_all' array, NOT a numeric BFS — best to match via name
  SELECT fc.bfs_nr AS bfs, count(*)::bigint AS n
  FROM gold_ch.core_sad cs
  JOIN bronze_ch.federal_communes fc ON lower(fc.name) = lower(cs.commune)
  WHERE cs.commune IS NOT NULL
  GROUP BY fc.bfs_nr
),
bench_per_commune AS (
  SELECT npa_c.commune_bfs,
    percentile_cont($2) WITHIN GROUP (ORDER BY bs.estimated_sale_price_m2) AS sale_m2,
    percentile_cont($3) WITHIN GROUP (ORDER BY br.estimated_rent_price_m2_monthly) AS rent_m2
  FROM silver_ch.ref_npa_commune npa_c
  LEFT JOIN gold_ch.core_benchmark_sales bs   ON bs.npa = npa_c.npa AND bs.property_type=$4
  LEFT JOIN gold_ch.core_benchmark_rentals br ON br.npa = npa_c.npa AND br.property_type=$5
  WHERE npa_c.commune_bfs IS NOT NULL
  GROUP BY npa_c.commune_bfs
)
SELECT
  fc.bfs_nr AS commune_bfs,
  fc.name AS commune_name,
  fc.canton_code,
  rc.name AS canton_name,
  npa.npa_list,
  fc.geometry,
  ST_Centroid(fc.geometry) AS centroid,
  fc.area_km2,
  COALESCE(pl.population, fc.population) AS population,
  pl.pop_year,
  COALESCE(tx.n, $6) AS transaction_count,
  COALESCE(la.n, $7) AS listing_count,
  COALESCE(sa.n, $8) AS sad_count,
  COALESCE(tx.n, $9) > $10 AS has_transactions,
  COALESCE(la.n, $11) > $12 AS has_listings,
  COALESCE(sa.n, $13) > $14 AS has_sads,
  bp.sale_m2::numeric AS benchmark_sale_price_m2,
  bp.rent_m2::numeric AS benchmark_rent_m2,
  now() AS updated_at
FROM bronze_ch.federal_communes fc
LEFT JOIN ref.cantons rc ON rc.code = fc.canton_code
LEFT JOIN npa_agg npa ON npa.commune_bfs = fc.bfs_nr
LEFT JOIN pop_latest pl ON pl.bfs_commune_number = fc.bfs_nr
LEFT JOIN tx_agg tx ON tx.bfs = fc.bfs_nr
LEFT JOIN listing_agg la ON la.bfs = fc.bfs_nr
LEFT JOIN sad_agg sa ON sa.bfs = fc.bfs_nr
LEFT JOIN bench_per_commune bp ON bp.commune_bfs = fc.bfs_nr
