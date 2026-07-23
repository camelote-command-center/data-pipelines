-- core_filter_options: De-parameterized from pg_stat_statements canonical
-- 36 UNION ALL blocks (33 canonical + 3 enhancements) producing dynamic filter options
-- Column contract: product, domain, filter_type, filter_value, filter_label, parent_value, sort_order, count, is_active
-- Must UNION ALL with gold_ch.static_filter_options via v_filter_options_full
--
-- Column name fixes from canonical:
--   v_plots_full.zone_synthetique → v_plots_full.rdppf_zone_synthetic
--   v_plots_full.type_propriete  → core_plots_ext_ge.type_propriete (not in v_plots_full)
--   core_plots_ext_ge.densification_type → core_plots_ext_ge.densification_types
--
-- REFRESH dependency chain:
--   core_buildings → core_plots, core_plots_ext_ge → (v_plots_full) → core_filter_options

CREATE MATERIALIZED VIEW gold_ch.core_filter_options AS

-- ═══════════════════════════════════════════
-- TRANSACTION domain (4 blocks — includes enhancement: property_type)
-- ═══════════════════════════════════════════

-- 1. Transaction | Canton
SELECT 'lamap'::text AS product,
    'transaction'::text AS domain,
    'canton'::text AS filter_type,
    t.canton_code AS filter_value,
    c.name_fr AS filter_label,
    NULL::text AS parent_value,
    (row_number() OVER (ORDER BY c.name_fr))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_transactions t
JOIN ref.cantons c ON c.code = t.canton_code
WHERE t.canton_code IS NOT NULL
GROUP BY t.canton_code, c.name_fr

UNION ALL

-- 2. Transaction | Commune
SELECT 'lamap'::text AS product,
    'transaction'::text AS domain,
    'commune'::text AS filter_type,
    core_transactions.commune AS filter_value,
    core_transactions.commune AS filter_label,
    core_transactions.canton_code AS parent_value,
    (row_number() OVER (ORDER BY core_transactions.commune))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_transactions
WHERE core_transactions.commune IS NOT NULL
GROUP BY core_transactions.commune, core_transactions.canton_code

UNION ALL

-- 3. Transaction | Year
SELECT 'lamap'::text AS product,
    'transaction'::text AS domain,
    'year'::text AS filter_type,
    (EXTRACT(year FROM core_transactions.transaction_date)::integer)::text AS filter_value,
    (EXTRACT(year FROM core_transactions.transaction_date)::integer)::text AS filter_label,
    NULL::text AS parent_value,
    EXTRACT(year FROM core_transactions.transaction_date)::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_transactions
WHERE core_transactions.transaction_date IS NOT NULL
GROUP BY EXTRACT(year FROM core_transactions.transaction_date)

UNION ALL

-- 4. Transaction | Property Type (ENHANCEMENT: replaces 13 NULL-count static entries)
SELECT 'lamap'::text AS product,
    'transaction'::text AS domain,
    'property_type'::text AS filter_type,
    core_transactions.property_type AS filter_value,
    core_transactions.property_type AS filter_label,
    NULL::text AS parent_value,
    (row_number() OVER (ORDER BY count(*) DESC))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_transactions
WHERE core_transactions.property_type IS NOT NULL
GROUP BY core_transactions.property_type

UNION ALL

-- ═══════════════════════════════════════════
-- LISTING domain (7 blocks)
-- ═══════════════════════════════════════════

-- 4. Listing | Canton (from market_listing_stats)
SELECT 'lamap'::text AS product,
    'listing'::text AS domain,
    'canton'::text AS filter_type,
    nc.canton_code AS filter_value,
    c.name_fr AS filter_label,
    NULL::text AS parent_value,
    (row_number() OVER (ORDER BY c.name_fr))::integer AS sort_order,
    (sum(ls.listing_count))::integer AS count,
    true AS is_active
FROM silver_ch.market_listing_stats ls
JOIN silver_ch.ref_npa_commune nc ON nc.npa = ls.npa
JOIN ref.cantons c ON c.code = nc.canton_code::text
GROUP BY nc.canton_code, c.name_fr

UNION ALL

-- 5. Listing | Commune (from market_listing_stats)
SELECT 'lamap'::text AS product,
    'listing'::text AS domain,
    'commune'::text AS filter_type,
    nc.commune_name AS filter_value,
    nc.commune_name AS filter_label,
    nc.canton_code AS parent_value,
    (row_number() OVER (ORDER BY nc.commune_name))::integer AS sort_order,
    (sum(ls.listing_count))::integer AS count,
    true AS is_active
FROM silver_ch.market_listing_stats ls
JOIN silver_ch.ref_npa_commune nc ON nc.npa = ls.npa
GROUP BY nc.commune_name, nc.canton_code

UNION ALL

-- 6. Listing | Property Type
-- Labels: identity (lowercase) matching current consumer data
SELECT 'lamap'::text AS product,
    'listing'::text AS domain,
    'property_type'::text AS filter_type,
    core_listings.property_type AS filter_value,
    core_listings.property_type AS filter_label,
    NULL::text AS parent_value,
    (row_number() OVER (ORDER BY core_listings.property_type))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_listings
WHERE core_listings.property_type IS NOT NULL
GROUP BY core_listings.property_type

UNION ALL

-- 7. Listing | Offer Type
-- Labels: initcap (rent→Rent, sale→Sale) matching current consumer data
SELECT 'lamap'::text AS product,
    'listing'::text AS domain,
    'offer_type'::text AS filter_type,
    core_listings.offer_type AS filter_value,
    CASE core_listings.offer_type
        WHEN 'rent' THEN 'Rent'
        WHEN 'sale' THEN 'Sale'
        ELSE core_listings.offer_type
    END AS filter_label,
    NULL::text AS parent_value,
    CASE core_listings.offer_type
        WHEN 'rent' THEN 1
        ELSE 2
    END AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_listings
GROUP BY core_listings.offer_type

UNION ALL

-- 8. Listing | Room Category
SELECT 'lamap'::text AS product,
    'listing'::text AS domain,
    'room_category'::text AS filter_type,
    core_listings.room_category AS filter_value,
    core_listings.room_category AS filter_label,
    NULL::text AS parent_value,
    CASE core_listings.room_category
        WHEN '1' THEN 1
        WHEN '2' THEN 2
        WHEN '3' THEN 3
        WHEN '4' THEN 4
        WHEN '5+' THEN 5
        ELSE 99
    END AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_listings
WHERE core_listings.room_category IS NOT NULL
GROUP BY core_listings.room_category

UNION ALL

-- 9. Listing | Price Category
SELECT 'lamap'::text AS product,
    'listing'::text AS domain,
    'price_category'::text AS filter_type,
    core_listings.price_category AS filter_value,
    core_listings.price_category AS filter_label,
    NULL::text AS parent_value,
    (row_number() OVER (ORDER BY min(core_listings.price)))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_listings
WHERE core_listings.price_category IS NOT NULL
GROUP BY core_listings.price_category

UNION ALL

-- 10. Listing | Source
SELECT 'lamap'::text AS product,
    'listing'::text AS domain,
    'source'::text AS filter_type,
    core_listings.source AS filter_value,
    initcap(replace(core_listings.source, '_', ' ')) AS filter_label,
    NULL::text AS parent_value,
    (row_number() OVER (ORDER BY count(*) DESC))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_listings
WHERE core_listings.source IS NOT NULL
GROUP BY core_listings.source

UNION ALL

-- ═══════════════════════════════════════════
-- PLOT domain (9 blocks — includes enhancement: heating_type)
-- ═══════════════════════════════════════════

-- 11. Plot | Canton
SELECT 'lamap'::text AS product,
    'plot'::text AS domain,
    'canton'::text AS filter_type,
    p.canton_code AS filter_value,
    c.name_fr AS filter_label,
    NULL::text AS parent_value,
    (row_number() OVER (ORDER BY c.name_fr))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.v_plots_full p
JOIN ref.cantons c ON c.code = p.canton_code
WHERE p.canton_code IS NOT NULL
GROUP BY p.canton_code, c.name_fr

UNION ALL

-- 12. Plot | Commune
SELECT 'lamap'::text AS product,
    'plot'::text AS domain,
    'commune'::text AS filter_type,
    v_plots_full.commune_name AS filter_value,
    v_plots_full.commune_name AS filter_label,
    v_plots_full.canton_code AS parent_value,
    (row_number() OVER (ORDER BY v_plots_full.commune_name))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.v_plots_full
WHERE v_plots_full.commune_name IS NOT NULL AND v_plots_full.commune_name <> ''
GROUP BY v_plots_full.commune_name, v_plots_full.canton_code

UNION ALL

-- 13. Plot | NPA (postal code)
SELECT 'lamap'::text AS product,
    'plot'::text AS domain,
    'npa'::text AS filter_type,
    v_plots_full.main_postal_code AS filter_value,
    v_plots_full.main_postal_code AS filter_label,
    NULL::text AS parent_value,
    (v_plots_full.main_postal_code)::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.v_plots_full
WHERE v_plots_full.main_postal_code IS NOT NULL AND v_plots_full.main_postal_code <> ''
GROUP BY v_plots_full.main_postal_code

UNION ALL

-- 14. Plot | Type Usage (from primary_category)
SELECT 'lamap'::text AS product,
    'plot'::text AS domain,
    'type_usage'::text AS filter_type,
    v_plots_full.primary_category AS filter_value,
    v_plots_full.primary_category AS filter_label,
    NULL::text AS parent_value,
    (row_number() OVER (ORDER BY count(*) DESC))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.v_plots_full
WHERE v_plots_full.primary_category IS NOT NULL
GROUP BY v_plots_full.primary_category

UNION ALL

-- 15. Plot | Zone (FIXED: zone_synthetique → rdppf_zone_synthetic)
SELECT 'lamap'::text AS product,
    'plot'::text AS domain,
    'zone'::text AS filter_type,
    v_plots_full.rdppf_zone_synthetic AS filter_value,
    v_plots_full.rdppf_zone_synthetic AS filter_label,
    NULL::text AS parent_value,
    (row_number() OVER (ORDER BY v_plots_full.rdppf_zone_synthetic))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.v_plots_full
WHERE v_plots_full.rdppf_zone_synthetic IS NOT NULL
GROUP BY v_plots_full.rdppf_zone_synthetic

UNION ALL

-- 16. Plot | Type Propriété (FIXED: source changed from v_plots_full to core_plots_ext_ge)
SELECT 'lamap'::text AS product,
    'plot'::text AS domain,
    'type_propriete'::text AS filter_type,
    core_plots_ext_ge.type_propriete AS filter_value,
    core_plots_ext_ge.type_propriete AS filter_label,
    NULL::text AS parent_value,
    (row_number() OVER (ORDER BY count(*) DESC))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_plots_ext_ge
WHERE core_plots_ext_ge.type_propriete IS NOT NULL
GROUP BY core_plots_ext_ge.type_propriete

UNION ALL

-- 17. Plot | Densification Type (FIXED: densification_types is text[], unnest)
SELECT 'lamap'::text AS product,
    'plot'::text AS domain,
    'densification_type'::text AS filter_type,
    dt AS filter_value,
    dt AS filter_label,
    NULL::text AS parent_value,
    (row_number() OVER (ORDER BY count(*) DESC))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_plots_ext_ge, unnest(core_plots_ext_ge.densification_types) AS dt
WHERE core_plots_ext_ge.densification_types IS NOT NULL
GROUP BY dt

UNION ALL

-- 18. Plot | IDC Bracket
SELECT 'lamap'::text AS product,
    'plot'::text AS domain,
    'idc_bracket'::text AS filter_type,
    core_plots_ext_ge.idc_bracket AS filter_value,
    core_plots_ext_ge.idc_bracket AS filter_label,
    NULL::text AS parent_value,
    (row_number() OVER (ORDER BY core_plots_ext_ge.idc_bracket))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_plots_ext_ge
WHERE core_plots_ext_ge.idc_bracket IS NOT NULL
GROUP BY core_plots_ext_ge.idc_bracket

UNION ALL

-- 19. Plot | Heating Type (ENHANCEMENT: plot-level heating energy, counts distinct plots)
-- Source: core_buildings.heater_energy_1_label aggregated by egrid (plot)
SELECT 'lamap'::text AS product,
    'plot'::text AS domain,
    'heating_type'::text AS filter_type,
    cb.heater_energy_1_label AS filter_value,
    cb.heater_energy_1_label AS filter_label,
    NULL::text AS parent_value,
    (row_number() OVER (ORDER BY count(DISTINCT cb.egrid) DESC))::integer AS sort_order,
    count(DISTINCT cb.egrid)::integer AS count,
    true AS is_active
FROM gold_ch.core_buildings cb
WHERE cb.egrid IS NOT NULL AND cb.egrid <> '' AND cb.heater_energy_1_label IS NOT NULL
GROUP BY cb.heater_energy_1_label

UNION ALL

-- ═══════════════════════════════════════════
-- SAD domain (7 blocks)
-- ═══════════════════════════════════════════

-- 19. SAD | Canton
SELECT 'lamap'::text AS product,
    'sad'::text AS domain,
    'canton'::text AS filter_type,
    s.canton_code AS filter_value,
    c.name_fr AS filter_label,
    NULL::text AS parent_value,
    (row_number() OVER (ORDER BY c.name_fr))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_sad s
JOIN ref.cantons c ON c.code = s.canton_code
WHERE s.canton_code IS NOT NULL
GROUP BY s.canton_code, c.name_fr

UNION ALL

-- 20. SAD | Commune
SELECT 'lamap'::text AS product,
    'sad'::text AS domain,
    'commune'::text AS filter_type,
    s.commune AS filter_value,
    s.commune AS filter_label,
    s.canton_code AS parent_value,
    (row_number() OVER (ORDER BY s.commune))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_sad s
WHERE s.commune IS NOT NULL
GROUP BY s.commune, s.canton_code

UNION ALL

-- 21. SAD | Type Dossier (JOINs ref.permit_type_taxonomy for canonical labels)
SELECT 'lamap'::text AS product,
    'sad'::text AS domain,
    'type_dossier'::text AS filter_type,
    COALESCE(t.canonical_value, s.permit_type::text) AS filter_value,
    COALESCE(t.canonical_label, s.permit_type::text) AS filter_label,
    NULL::text AS parent_value,
    (row_number() OVER (ORDER BY COALESCE(t.canonical_label, s.permit_type::text)))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_sad s
LEFT JOIN ref.permit_type_taxonomy t
       ON t.canton_code = s.canton_code AND t.raw_value = s.permit_type::text
WHERE s.permit_type IS NOT NULL AND s.permit_type::text <> ''
GROUP BY COALESCE(t.canonical_value, s.permit_type::text),
         COALESCE(t.canonical_label, s.permit_type::text)

UNION ALL

-- 22. SAD | Statut (JOINs ref.status_taxonomy for canonical labels)
SELECT 'lamap'::text AS product,
    'sad'::text AS domain,
    'statut'::text AS filter_type,
    COALESCE(t.canonical_value, s.status) AS filter_value,
    COALESCE(t.canonical_label, s.status) AS filter_label,
    NULL::text AS parent_value,
    (row_number() OVER (ORDER BY COALESCE(t.canonical_label, s.status)))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_sad s
LEFT JOIN ref.status_taxonomy t
       ON t.canton_code = s.canton_code AND t.raw_value = s.status
WHERE s.status IS NOT NULL
GROUP BY COALESCE(t.canonical_value, s.status),
         COALESCE(t.canonical_label, s.status)

UNION ALL

-- 23. SAD | Year
SELECT 'lamap'::text AS product,
    'sad'::text AS domain,
    'year'::text AS filter_type,
    (EXTRACT(year FROM s.date_depot)::integer)::text AS filter_value,
    (EXTRACT(year FROM s.date_depot)::integer)::text AS filter_label,
    NULL::text AS parent_value,
    EXTRACT(year FROM s.date_depot)::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_sad s
WHERE s.date_depot IS NOT NULL
GROUP BY EXTRACT(year FROM s.date_depot)

UNION ALL

-- 24. SAD | Type Opération
SELECT 'lamap'::text AS product,
    'sad'::text AS domain,
    'type_operation'::text AS filter_type,
    s.type_operation AS filter_value,
    s.type_operation AS filter_label,
    NULL::text AS parent_value,
    (row_number() OVER (ORDER BY s.type_operation))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_sad s
WHERE s.type_operation IS NOT NULL AND s.type_operation <> ''
GROUP BY s.type_operation

UNION ALL

-- 25. SAD | Opération
SELECT 'lamap'::text AS product,
    'sad'::text AS domain,
    'operation'::text AS filter_type,
    s.operation AS filter_value,
    s.operation AS filter_label,
    NULL::text AS parent_value,
    (row_number() OVER (ORDER BY s.operation))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_sad s
WHERE s.operation IS NOT NULL AND s.operation <> ''
GROUP BY s.operation

UNION ALL

-- ═══════════════════════════════════════════
-- BUILDING domain (6 blocks — includes enhancement: destination)
-- ═══════════════════════════════════════════

-- 26. Building | Category Label
SELECT 'lamap'::text AS product,
    'building'::text AS domain,
    'category_label'::text AS filter_type,
    core_buildings.category_label::text AS filter_value,
    core_buildings.category_label::text AS filter_label,
    NULL::text AS parent_value,
    (row_number() OVER (ORDER BY count(*) DESC))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_buildings
WHERE core_buildings.category_label IS NOT NULL
GROUP BY core_buildings.category_label

UNION ALL

-- 27. Building | Class Label
SELECT 'lamap'::text AS product,
    'building'::text AS domain,
    'class_label'::text AS filter_type,
    core_buildings.class_label::text AS filter_value,
    core_buildings.class_label::text AS filter_label,
    NULL::text AS parent_value,
    (row_number() OVER (ORDER BY count(*) DESC))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_buildings
WHERE core_buildings.class_label IS NOT NULL
GROUP BY core_buildings.class_label

UNION ALL

-- 28. Building | Heater Energy Label
SELECT 'lamap'::text AS product,
    'building'::text AS domain,
    'heater_energy_1_label'::text AS filter_type,
    core_buildings.heater_energy_1_label::text AS filter_value,
    core_buildings.heater_energy_1_label::text AS filter_label,
    NULL::text AS parent_value,
    (row_number() OVER (ORDER BY count(*) DESC))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_buildings
WHERE core_buildings.heater_energy_1_label IS NOT NULL
GROUP BY core_buildings.heater_energy_1_label

UNION ALL

-- 29. Building | Heating Type Label
SELECT 'lamap'::text AS product,
    'building'::text AS domain,
    'heating_type_1_label'::text AS filter_type,
    core_buildings.heater_type_1_label::text AS filter_value,
    core_buildings.heater_type_1_label::text AS filter_label,
    NULL::text AS parent_value,
    (row_number() OVER (ORDER BY count(*) DESC))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_buildings
WHERE core_buildings.heater_type_1_label IS NOT NULL
GROUP BY core_buildings.heater_type_1_label

UNION ALL

-- 30. Building | Hot Water Energy Label
SELECT 'lamap'::text AS product,
    'building'::text AS domain,
    'hot_water_energy_1_label'::text AS filter_type,
    core_buildings.hot_water_energy_1_label::text AS filter_value,
    core_buildings.hot_water_energy_1_label::text AS filter_label,
    NULL::text AS parent_value,
    (row_number() OVER (ORDER BY count(*) DESC))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_buildings
WHERE core_buildings.hot_water_energy_1_label IS NOT NULL
GROUP BY core_buildings.hot_water_energy_1_label

UNION ALL

-- 31. Building | Destination (ENHANCEMENT: replaces 114 NULL-count static entries)
-- Source: core_plots_ext_ge.building_destination (GE only, 60.9% plot coverage)
SELECT 'lamap'::text AS product,
    'building'::text AS domain,
    'destination'::text AS filter_type,
    core_plots_ext_ge.building_destination AS filter_value,
    core_plots_ext_ge.building_destination AS filter_label,
    NULL::text AS parent_value,
    (row_number() OVER (ORDER BY core_plots_ext_ge.building_destination))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_plots_ext_ge
WHERE core_plots_ext_ge.building_destination IS NOT NULL
GROUP BY core_plots_ext_ge.building_destination

UNION ALL

-- ═══════════════════════════════════════════
-- ENTITY domain (2 blocks)
-- ═══════════════════════════════════════════

-- 31. Entity | Entity Type
SELECT 'lamap'::text AS product,
    'entity'::text AS domain,
    'entity_type'::text AS filter_type,
    core_entities.entity_type AS filter_value,
    initcap(replace(core_entities.entity_type, '_', ' ')) AS filter_label,
    NULL::text AS parent_value,
    CASE core_entities.entity_type
        WHEN 'person' THEN 1
        WHEN 'company' THEN 2
        WHEN 'foundation' THEN 3
        WHEN 'public_body' THEN 4
        WHEN 'unknown' THEN 5
        ELSE 99
    END AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_entities
WHERE core_entities.entity_type IS NOT NULL
GROUP BY core_entities.entity_type

UNION ALL

-- 32. Entity | Canton
SELECT 'lamap'::text AS product,
    'entity'::text AS domain,
    'canton'::text AS filter_type,
    e.canton_code AS filter_value,
    c.name_fr AS filter_label,
    NULL::text AS parent_value,
    (row_number() OVER (ORDER BY c.name_fr))::integer AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_entities e
JOIN ref.cantons c ON c.code = e.canton_code
WHERE e.canton_code IS NOT NULL
GROUP BY e.canton_code, c.name_fr

UNION ALL

-- ═══════════════════════════════════════════
-- UNIT domain (1 block)
-- ═══════════════════════════════════════════

-- 33. Unit | Rooms (bucketed: 1, 1.5, 2, 2.5, ... 5.5, 6+)
SELECT 'lamap'::text AS product,
    'unit'::text AS domain,
    'rooms'::text AS filter_type,
    CASE
        WHEN core_units.rooms >= 6::numeric THEN '6+'::text
        ELSE core_units.rooms::text
    END AS filter_value,
    CASE
        WHEN core_units.rooms >= 6::numeric THEN '6+'::text
        ELSE core_units.rooms::text
    END AS filter_label,
    NULL::text AS parent_value,
    CASE
        WHEN core_units.rooms >= 6::numeric THEN 60
        ELSE (core_units.rooms * 10::numeric)::integer
    END AS sort_order,
    (count(*))::integer AS count,
    true AS is_active
FROM gold_ch.core_units
WHERE core_units.rooms IS NOT NULL
GROUP BY
    CASE
        WHEN core_units.rooms >= 6::numeric THEN '6+'::text
        ELSE core_units.rooms::text
    END,
    CASE
        WHEN core_units.rooms >= 6::numeric THEN 60
        ELSE (core_units.rooms * 10::numeric)::integer
    END
;
