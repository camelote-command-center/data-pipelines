CREATE MATERIALIZED VIEW gold_ch.core_filter_options AS
 SELECT $1::text AS product,
    $2::text AS domain,
    $3::text AS filter_type,
    t.canton_code AS filter_value,
    c.name_fr AS filter_label,
    $4::text AS parent_value,
    (row_number() OVER (ORDER BY c.name_fr))::integer AS sort_order,
    (count(*))::integer AS count,
    $5 AS is_active
   FROM (gold_ch.core_transactions t
     JOIN ref.cantons c ON ((c.code = t.canton_code)))
  WHERE (t.canton_code IS NOT NULL)
  GROUP BY t.canton_code, c.name_fr
UNION ALL
 SELECT $6::text AS product,
    $7::text AS domain,
    $8::text AS filter_type,
    core_transactions.commune AS filter_value,
    core_transactions.commune AS filter_label,
    core_transactions.canton_code AS parent_value,
    (row_number() OVER (ORDER BY core_transactions.commune))::integer AS sort_order,
    (count(*))::integer AS count,
    $9 AS is_active
   FROM gold_ch.core_transactions
  WHERE (core_transactions.commune IS NOT NULL)
  GROUP BY core_transactions.commune, core_transactions.canton_code
UNION ALL
 SELECT $10::text AS product,
    $11::text AS domain,
    $12::text AS filter_type,
    ((EXTRACT($13 FROM core_transactions.transaction_date))::integer)::text AS filter_value,
    ((EXTRACT($14 FROM core_transactions.transaction_date))::integer)::text AS filter_label,
    $15::text AS parent_value,
    (EXTRACT($16 FROM core_transactions.transaction_date))::integer AS sort_order,
    (count(*))::integer AS count,
    $17 AS is_active
   FROM gold_ch.core_transactions
  WHERE (core_transactions.transaction_date IS NOT NULL)
  GROUP BY (EXTRACT($18 FROM core_transactions.transaction_date))
UNION ALL
 SELECT $19::text AS product,
    $20::text AS domain,
    $21::text AS filter_type,
    nc.canton_code AS filter_value,
    c.name_fr AS filter_label,
    $22::text AS parent_value,
    (row_number() OVER (ORDER BY c.name_fr))::integer AS sort_order,
    (sum(ls.listing_count))::integer AS count,
    $23 AS is_active
   FROM ((silver_ch.market_listing_stats ls
     JOIN silver_ch.ref_npa_commune nc ON ((nc.npa = ls.npa)))
     JOIN ref.cantons c ON ((c.code = (nc.canton_code)::text)))
  GROUP BY nc.canton_code, c.name_fr
UNION ALL
 SELECT $24::text AS product,
    $25::text AS domain,
    $26::text AS filter_type,
    nc.commune_name AS filter_value,
    nc.commune_name AS filter_label,
    nc.canton_code AS parent_value,
    (row_number() OVER (ORDER BY nc.commune_name))::integer AS sort_order,
    (sum(ls.listing_count))::integer AS count,
    $27 AS is_active
   FROM (silver_ch.market_listing_stats ls
     JOIN silver_ch.ref_npa_commune nc ON ((nc.npa = ls.npa)))
  GROUP BY nc.commune_name, nc.canton_code
UNION ALL
 SELECT $28::text AS product,
    $29::text AS domain,
    $30::text AS filter_type,
    core_listings.property_type AS filter_value,
        CASE core_listings.property_type
            WHEN $31::text THEN $32::text
            WHEN $33::text THEN $34::text
            WHEN $35::text THEN $36::text
            WHEN $37::text THEN $38::text
            WHEN $39::text THEN $40::text
            WHEN $41::text THEN $42::text
            WHEN $43::text THEN $44::text
            WHEN $45::text THEN $46::text
            WHEN $47::text THEN $48::text
            WHEN $49::text THEN $50::text
            ELSE core_listings.property_type
        END AS filter_label,
    $51::text AS parent_value,
        CASE core_listings.property_type
            WHEN $52::text THEN $53
            WHEN $54::text THEN $55
            WHEN $56::text THEN $57
            WHEN $58::text THEN $59
            WHEN $60::text THEN $61
            WHEN $62::text THEN $63
            ELSE $64
        END AS sort_order,
    (count(*))::integer AS count,
    $65 AS is_active
   FROM gold_ch.core_listings
  WHERE (core_listings.property_type IS NOT NULL)
  GROUP BY core_listings.property_type
UNION ALL
 SELECT $66::text AS product,
    $67::text AS domain,
    $68::text AS filter_type,
    core_listings.offer_type AS filter_value,
        CASE core_listings.offer_type
            WHEN $69::text THEN $70::text
            WHEN $71::text THEN $72::text
            ELSE core_listings.offer_type
        END AS filter_label,
    $73::text AS parent_value,
        CASE core_listings.offer_type
            WHEN $74::text THEN $75
            ELSE $76
        END AS sort_order,
    (count(*))::integer AS count,
    $77 AS is_active
   FROM gold_ch.core_listings
  GROUP BY core_listings.offer_type
UNION ALL
 SELECT $78::text AS product,
    $79::text AS domain,
    $80::text AS filter_type,
    core_listings.room_category AS filter_value,
    core_listings.room_category AS filter_label,
    $81::text AS parent_value,
        CASE core_listings.room_category
            WHEN $82::text THEN $83
            WHEN $84::text THEN $85
            WHEN $86::text THEN $87
            WHEN $88::text THEN $89
            WHEN $90::text THEN $91
            ELSE $92
        END AS sort_order,
    (count(*))::integer AS count,
    $93 AS is_active
   FROM gold_ch.core_listings
  WHERE (core_listings.room_category IS NOT NULL)
  GROUP BY core_listings.room_category
UNION ALL
 SELECT $94::text AS product,
    $95::text AS domain,
    $96::text AS filter_type,
    core_listings.price_category AS filter_value,
    core_listings.price_category AS filter_label,
    $97::text AS parent_value,
    (row_number() OVER (ORDER BY (min(core_listings.price))))::integer AS sort_order,
    (count(*))::integer AS count,
    $98 AS is_active
   FROM gold_ch.core_listings
  WHERE (core_listings.price_category IS NOT NULL)
  GROUP BY core_listings.price_category
UNION ALL
 SELECT $99::text AS product,
    $100::text AS domain,
    $101::text AS filter_type,
    core_listings.source AS filter_value,
    initcap(replace(core_listings.source, $102::text, $103::text)) AS filter_label,
    $104::text AS parent_value,
    (row_number() OVER (ORDER BY (count(*)) DESC))::integer AS sort_order,
    (count(*))::integer AS count,
    $105 AS is_active
   FROM gold_ch.core_listings
  WHERE (core_listings.source IS NOT NULL)
  GROUP BY core_listings.source
UNION ALL
 SELECT $106::text AS product,
    $107::text AS domain,
    $108::text AS filter_type,
    p.canton_code AS filter_value,
    c.name_fr AS filter_label,
    $109::text AS parent_value,
    (row_number() OVER (ORDER BY c.name_fr))::integer AS sort_order,
    (count(*))::integer AS count,
    $110 AS is_active
   FROM (gold_ch.v_plots_full p
     JOIN ref.cantons c ON ((c.code = p.canton_code)))
  WHERE (p.canton_code IS NOT NULL)
  GROUP BY p.canton_code, c.name_fr
UNION ALL
 SELECT $111::text AS product,
    $112::text AS domain,
    $113::text AS filter_type,
    v_plots_full.commune_name AS filter_value,
    v_plots_full.commune_name AS filter_label,
    v_plots_full.canton_code AS parent_value,
    (row_number() OVER (ORDER BY v_plots_full.commune_name))::integer AS sort_order,
    (count(*))::integer AS count,
    $114 AS is_active
   FROM gold_ch.v_plots_full
  WHERE ((v_plots_full.commune_name IS NOT NULL) AND (v_plots_full.commune_name <> $115::text))
  GROUP BY v_plots_full.commune_name, v_plots_full.canton_code
UNION ALL
 SELECT $116::text AS product,
    $117::text AS domain,
    $118::text AS filter_type,
    v_plots_full.main_postal_code AS filter_value,
    v_plots_full.main_postal_code AS filter_label,
    $119::text AS parent_value,
    (v_plots_full.main_postal_code)::integer AS sort_order,
    (count(*))::integer AS count,
    $120 AS is_active
   FROM gold_ch.v_plots_full
  WHERE ((v_plots_full.main_postal_code IS NOT NULL) AND (v_plots_full.main_postal_code <> $121::text))
  GROUP BY v_plots_full.main_postal_code
UNION ALL
 SELECT $122::text AS product,
    $123::text AS domain,
    $124::text AS filter_type,
    v_plots_full.primary_category AS filter_value,
    v_plots_full.primary_category AS filter_label,
    $125::text AS parent_value,
    (row_number() OVER (ORDER BY (count(*)) DESC))::integer AS sort_order,
    (count(*))::integer AS count,
    $126 AS is_active
   FROM gold_ch.v_plots_full
  WHERE (v_plots_full.primary_category IS NOT NULL)
  GROUP BY v_plots_full.primary_category
UNION ALL
 SELECT $127::text AS product,
    $128::text AS domain,
    $129::text AS filter_type,
    v_plots_full.zone_synthetique AS filter_value,
    v_plots_full.zone_synthetique AS filter_label,
    $130::text AS parent_value,
    (row_number() OVER (ORDER BY v_plots_full.zone_synthetique))::integer AS sort_order,
    (count(*))::integer AS count,
    $131 AS is_active
   FROM gold_ch.v_plots_full
  WHERE (v_plots_full.zone_synthetique IS NOT NULL)
  GROUP BY v_plots_full.zone_synthetique
UNION ALL
 SELECT $132::text AS product,
    $133::text AS domain,
    $134::text AS filter_type,
    v_plots_full.type_propriete AS filter_value,
    v_plots_full.type_propriete AS filter_label,
    $135::text AS parent_value,
    (row_number() OVER (ORDER BY (count(*)) DESC))::integer AS sort_order,
    (count(*))::integer AS count,
    $136 AS is_active
   FROM gold_ch.v_plots_full
  WHERE (v_plots_full.type_propriete IS NOT NULL)
  GROUP BY v_plots_full.type_propriete
UNION ALL
 SELECT $137::text AS product,
    $138::text AS domain,
    $139::text AS filter_type,
    core_plots_ext_ge.densification_type AS filter_value,
    core_plots_ext_ge.densification_type AS filter_label,
    $140::text AS parent_value,
    (row_number() OVER (ORDER BY (count(*)) DESC))::integer AS sort_order,
    (count(*))::integer AS count,
    $141 AS is_active
   FROM gold_ch.core_plots_ext_ge
  WHERE (core_plots_ext_ge.densification_type IS NOT NULL)
  GROUP BY core_plots_ext_ge.densification_type
UNION ALL
 SELECT $142::text AS product,
    $143::text AS domain,
    $144::text AS filter_type,
    core_plots_ext_ge.idc_bracket AS filter_value,
    core_plots_ext_ge.idc_bracket AS filter_label,
    $145::text AS parent_value,
    (row_number() OVER (ORDER BY core_plots_ext_ge.idc_bracket))::integer AS sort_order,
    (count(*))::integer AS count,
    $146 AS is_active
   FROM gold_ch.core_plots_ext_ge
  WHERE (core_plots_ext_ge.idc_bracket IS NOT NULL)
  GROUP BY core_plots_ext_ge.idc_bracket
UNION ALL
 SELECT $147::text AS product,
    $148::text AS domain,
    $149::text AS filter_type,
    s.canton_code AS filter_value,
    c.name_fr AS filter_label,
    $150::text AS parent_value,
    (row_number() OVER (ORDER BY c.name_fr))::integer AS sort_order,
    (count(*))::integer AS count,
    $151 AS is_active
   FROM (gold_ch.core_sad s
     JOIN ref.cantons c ON ((c.code = s.canton_code)))
  WHERE (s.canton_code IS NOT NULL)
  GROUP BY s.canton_code, c.name_fr
UNION ALL
 SELECT $152::text AS product,
    $153::text AS domain,
    $154::text AS filter_type,
    s.commune AS filter_value,
    s.commune AS filter_label,
    s.canton_code AS parent_value,
    (row_number() OVER (ORDER BY s.commune))::integer AS sort_order,
    (count(*))::integer AS count,
    $155 AS is_active
   FROM gold_ch.core_sad s
  WHERE (s.commune IS NOT NULL)
  GROUP BY s.commune, s.canton_code
UNION ALL
 -- SAD: type_dossier — JOINs ref.permit_type_taxonomy for canonical labels (regression #4 fix)
 SELECT $156::text AS product,
    $157::text AS domain,
    $158::text AS filter_type,
    COALESCE(t.canonical_value, (s.permit_type)::text) AS filter_value,
    COALESCE(t.canonical_label, (s.permit_type)::text) AS filter_label,
    $159::text AS parent_value,
    (row_number() OVER (ORDER BY COALESCE(t.canonical_label, (s.permit_type)::text)))::integer AS sort_order,
    (count(*))::integer AS count,
    $160 AS is_active
   FROM gold_ch.core_sad s
   LEFT JOIN ref.permit_type_taxonomy t
          ON t.canton_code = s.canton_code AND t.raw_value = (s.permit_type)::text
  WHERE (s.permit_type IS NOT NULL AND (s.permit_type)::text <> $161::text)
  GROUP BY COALESCE(t.canonical_value, (s.permit_type)::text),
           COALESCE(t.canonical_label, (s.permit_type)::text)
UNION ALL
 -- SAD: statut — JOINs ref.status_taxonomy for canonical labels (regression #5 dropdown fix)
 SELECT $162::text AS product,
    $163::text AS domain,
    $164::text AS filter_type,
    COALESCE(t.canonical_value, s.status) AS filter_value,
    COALESCE(t.canonical_label, s.status) AS filter_label,
    $165::text AS parent_value,
    (row_number() OVER (ORDER BY COALESCE(t.canonical_label, s.status)))::integer AS sort_order,
    (count(*))::integer AS count,
    $166 AS is_active
   FROM gold_ch.core_sad s
   LEFT JOIN ref.status_taxonomy t
          ON t.canton_code = s.canton_code AND t.raw_value = s.status
  WHERE (s.status IS NOT NULL)
  GROUP BY COALESCE(t.canonical_value, s.status),
           COALESCE(t.canonical_label, s.status)
UNION ALL
 SELECT $167::text AS product,
    $168::text AS domain,
    $169::text AS filter_type,
    ((EXTRACT($170 FROM s.date_depot))::integer)::text AS filter_value,
    ((EXTRACT($171 FROM s.date_depot))::integer)::text AS filter_label,
    $172::text AS parent_value,
    (EXTRACT($173 FROM s.date_depot))::integer AS sort_order,
    (count(*))::integer AS count,
    $174 AS is_active
   FROM gold_ch.core_sad s
  WHERE (s.date_depot IS NOT NULL)
  GROUP BY (EXTRACT($175 FROM s.date_depot))
UNION ALL
 -- NEW SAD: type_operation (regression: useSADFilterOptions requests this, gets empty)
 SELECT $176::text AS product,
    $177::text AS domain,
    $178::text AS filter_type,
    s.type_operation AS filter_value,
    s.type_operation AS filter_label,
    $179::text AS parent_value,
    (row_number() OVER (ORDER BY s.type_operation))::integer AS sort_order,
    (count(*))::integer AS count,
    $180 AS is_active
   FROM gold_ch.core_sad s
  WHERE (s.type_operation IS NOT NULL AND s.type_operation <> $181::text)
  GROUP BY s.type_operation
UNION ALL
 -- NEW SAD: operation
 SELECT $182::text AS product,
    $183::text AS domain,
    $184::text AS filter_type,
    s.operation AS filter_value,
    s.operation AS filter_label,
    $185::text AS parent_value,
    (row_number() OVER (ORDER BY s.operation))::integer AS sort_order,
    (count(*))::integer AS count,
    $186 AS is_active
   FROM gold_ch.core_sad s
  WHERE (s.operation IS NOT NULL AND s.operation <> $187::text)
  GROUP BY s.operation
UNION ALL
 SELECT $188::text AS product,
    $189::text AS domain,
    $190::text AS filter_type,
    (core_buildings.category_label)::text AS filter_value,
    (core_buildings.category_label)::text AS filter_label,
    $191::text AS parent_value,
    (row_number() OVER (ORDER BY (count(*)) DESC))::integer AS sort_order,
    (count(*))::integer AS count,
    $192 AS is_active
   FROM gold_ch.core_buildings
  WHERE (core_buildings.category_label IS NOT NULL)
  GROUP BY core_buildings.category_label
UNION ALL
 SELECT $193::text AS product,
    $194::text AS domain,
    $195::text AS filter_type,
    (core_buildings.class_label)::text AS filter_value,
    (core_buildings.class_label)::text AS filter_label,
    $196::text AS parent_value,
    (row_number() OVER (ORDER BY (count(*)) DESC))::integer AS sort_order,
    (count(*))::integer AS count,
    $197 AS is_active
   FROM gold_ch.core_buildings
  WHERE (core_buildings.class_label IS NOT NULL)
  GROUP BY core_buildings.class_label
UNION ALL
 SELECT $198::text AS product,
    $199::text AS domain,
    $200::text AS filter_type,
    (core_buildings.heater_energy_1_label)::text AS filter_value,
    (core_buildings.heater_energy_1_label)::text AS filter_label,
    $201::text AS parent_value,
    (row_number() OVER (ORDER BY (count(*)) DESC))::integer AS sort_order,
    (count(*))::integer AS count,
    $202 AS is_active
   FROM gold_ch.core_buildings
  WHERE (core_buildings.heater_energy_1_label IS NOT NULL)
  GROUP BY core_buildings.heater_energy_1_label
UNION ALL
 SELECT $203::text AS product,
    $204::text AS domain,
    $205::text AS filter_type,
    (core_buildings.heater_type_1_label)::text AS filter_value,
    (core_buildings.heater_type_1_label)::text AS filter_label,
    $206::text AS parent_value,
    (row_number() OVER (ORDER BY (count(*)) DESC))::integer AS sort_order,
    (count(*))::integer AS count,
    $207 AS is_active
   FROM gold_ch.core_buildings
  WHERE (core_buildings.heater_type_1_label IS NOT NULL)
  GROUP BY core_buildings.heater_type_1_label
UNION ALL
 SELECT $208::text AS product,
    $209::text AS domain,
    $210::text AS filter_type,
    (core_buildings.hot_water_energy_1_label)::text AS filter_value,
    (core_buildings.hot_water_energy_1_label)::text AS filter_label,
    $211::text AS parent_value,
    (row_number() OVER (ORDER BY (count(*)) DESC))::integer AS sort_order,
    (count(*))::integer AS count,
    $212 AS is_active
   FROM gold_ch.core_buildings
  WHERE (core_buildings.hot_water_energy_1_label IS NOT NULL)
  GROUP BY core_buildings.hot_water_energy_1_label
UNION ALL
 SELECT $213::text AS product,
    $214::text AS domain,
    $215::text AS filter_type,
    core_entities.entity_type AS filter_value,
        CASE core_entities.entity_type
            WHEN $216::text THEN $217::text
            WHEN $218::text THEN $219::text
            WHEN $220::text THEN $221::text
            WHEN $222::text THEN $223::text
            WHEN $224::text THEN $225::text
            WHEN $226::text THEN $227::text
            ELSE core_entities.entity_type
        END AS filter_label,
    $228::text AS parent_value,
        CASE core_entities.entity_type
            WHEN $229::text THEN $230
            WHEN $231::text THEN $232
            WHEN $233::text THEN $234
            WHEN $235::text THEN $236
            WHEN $237::text THEN $238
            ELSE $239
        END AS sort_order,
    (count(*))::integer AS count,
    $240 AS is_active
   FROM gold_ch.core_entities
  WHERE (core_entities.entity_type IS NOT NULL)
  GROUP BY core_entities.entity_type
UNION ALL
 SELECT $241::text AS product,
    $242::text AS domain,
    $243::text AS filter_type,
    e.canton_code AS filter_value,
    c.name_fr AS filter_label,
    $244::text AS parent_value,
    (row_number() OVER (ORDER BY c.name_fr))::integer AS sort_order,
    (count(*))::integer AS count,
    $245 AS is_active
   FROM (gold_ch.core_entities e
     JOIN ref.cantons c ON ((c.code = e.canton_code)))
  WHERE (e.canton_code IS NOT NULL)
  GROUP BY e.canton_code, c.name_fr
UNION ALL
 SELECT $246::text AS product,
    $247::text AS domain,
    $248::text AS filter_type,
        CASE
            WHEN (core_units.rooms >= ($249)::numeric) THEN $250::text
            ELSE (core_units.rooms)::text
        END AS filter_value,
        CASE
            WHEN (core_units.rooms >= ($251)::numeric) THEN $252::text
            ELSE (core_units.rooms)::text
        END AS filter_label,
    $253::text AS parent_value,
        CASE
            WHEN (core_units.rooms >= ($254)::numeric) THEN $255
            ELSE ((core_units.rooms * ($256)::numeric))::integer
        END AS sort_order,
    (count(*))::integer AS count,
    $257 AS is_active
   FROM gold_ch.core_units
  WHERE (core_units.rooms IS NOT NULL)
  GROUP BY
        CASE
            WHEN (core_units.rooms >= (6)::numeric) THEN '6+'::text
            ELSE (core_units.rooms)::text
        END,
        CASE
            WHEN (core_units.rooms >= (6)::numeric) THEN 60
            ELSE ((core_units.rooms * (10)::numeric))::integer
        END
