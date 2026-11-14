-- 2026-11-14: PPE from the notice text (word PPE or a PPE unit number) or the PPE plot list when the text does not sell the whole plot; COP is never PPE.

BEGIN;

SET LOCAL statement_timeout = '60min';

DROP MATERIALIZED VIEW gold_ch.core_transactions CASCADE;

CREATE MATERIALIZED VIEW gold_ch.core_transactions AS
 WITH party_agg AS (
         SELECT etp.transaction_id,
            array_agg(etp.party_name ORDER BY etp.party_name) FILTER (WHERE etp.party_role = 'buyer'::text) AS buyer_names,
            array_agg(etp.party_name ORDER BY etp.party_name) FILTER (WHERE etp.party_role = 'seller'::text) AS seller_names,
            array_agg(DISTINCT etp.entity_id) FILTER (WHERE etp.party_role = 'buyer'::text AND etp.entity_id IS NOT NULL) AS buyer_entity_ids,
            array_agg(DISTINCT etp.entity_id) FILTER (WHERE etp.party_role = 'seller'::text AND etp.entity_id IS NOT NULL) AS seller_entity_ids,
            jsonb_agg(jsonb_build_object('name', etp.party_name, 'entity_id', etp.entity_id, 'entity_type', ce.entity_type, 'legal_form', ce.legal_form, 'ide_uid_formatted', ce.ide_uid_formatted, 'head_office', ce.head_office, 'canton_code', ce.canton_code, 'transaction_count', ce.transaction_count) ORDER BY etp.party_name) FILTER (WHERE etp.party_role = 'buyer'::text) AS parties_buyers,
            jsonb_agg(jsonb_build_object('name', etp.party_name, 'entity_id', etp.entity_id, 'entity_type', ce.entity_type, 'legal_form', ce.legal_form, 'ide_uid_formatted', ce.ide_uid_formatted, 'head_office', ce.head_office, 'canton_code', ce.canton_code, 'transaction_count', ce.transaction_count) ORDER BY etp.party_name) FILTER (WHERE etp.party_role = 'seller'::text) AS parties_sellers
           FROM silver_ch.event_transaction_parties etp
             LEFT JOIN gold_ch.core_entities ce ON ce.entity_id = etp.entity_id
          GROUP BY etp.transaction_id
        ), plot_match AS (
         SELECT t_1.transaction_id,
            p.egrid,
            p.surface_m2 AS plot_surface_m2,
            p.no_commune_no_parcelle,
            pi.typologie AS plot_typologie,
            tg.type_usage AS plot_type_usage
           FROM silver_ch.event_transactions t_1,
            LATERAL unnest(t_1.plot_references) ref(plot_ref)
             JOIN silver_ch.cadastral_plots p ON p.no_commune_no_parcelle = ref.plot_ref AND p.canton_code = 'GE'::text
             LEFT JOIN silver_ch.plot_intel_ge pi ON pi.egrid = p.egrid
             LEFT JOIN silver_ch.typologies_ge tg ON tg.nom_typologie_clean = pi.typologie
          WHERE t_1.plot_references IS NOT NULL
        ), plot_first AS (
         SELECT DISTINCT ON (plot_match.transaction_id) plot_match.transaction_id,
            plot_match.egrid,
            plot_match.plot_surface_m2
           FROM plot_match
          ORDER BY plot_match.transaction_id, plot_match.egrid
        ), plot_egrids AS (
         SELECT plot_match.transaction_id,
            array_agg(DISTINCT plot_match.egrid) AS egrids
           FROM plot_match
          GROUP BY plot_match.transaction_id
        ), plot_ppe AS (
         SELECT DISTINCT plot_match.transaction_id
           FROM plot_match
             JOIN silver_ch.cadastral_ppe cppe ON cppe.no_commune_no_parcelle = plot_match.no_commune_no_parcelle
        ), plot_typology AS (
         SELECT plot_match.transaction_id,
            mode() WITHIN GROUP (ORDER BY plot_match.plot_typologie) AS resolved_typology,
            mode() WITHIN GROUP (ORDER BY plot_match.plot_type_usage) AS resolved_type_usage
           FROM plot_match
          GROUP BY plot_match.transaction_id
        )
 SELECT t.transaction_id,
    t.affaire_number,
    t.source_id,
    t.transaction_date,
    t.publication_date,
    t.transaction_type,
    t.transaction_type_list,
    t.price,
    t.commune,
    t.commune_number,
    t.canton_code,
    COALESCE(t.plot_references, pre.plot_references) AS plot_references,
    COALESCE(pe.egrids, pre.egrids) AS egrids,
    COALESCE(pf.egrid, pre.primary_egrid) AS primary_egrid,
    pf.plot_surface_m2,
        CASE
            WHEN t.price IS NOT NULL AND t.price > 0::numeric AND COALESCE(t.surface_m2, pf.plot_surface_m2) IS NOT NULL AND COALESCE(t.surface_m2, pf.plot_surface_m2) > 0::numeric THEN round(t.price / COALESCE(t.surface_m2, pf.plot_surface_m2), 2)
            ELSE t.price_per_m2
        END AS price_per_m2,
        CASE
            WHEN t.transaction_text ~* '\mPPE\M|\mUE\s*\d|\m\d{1,2}\.\d{2,3}\s+(partie\s+)?(appartement|studio|attique|duplex|triplex|loft|box|parking|place|garage|cave|grenier|bureaux?|local|locaux|arcade|dépôt|depot|chambre|hall|magasin|atelier|cabinet|salle|restaurant|réduit)\M'::text OR ppp.transaction_id IS NOT NULL AND COALESCE(t.transaction_text, ''::text) !~* '\m(B-F|COP|DDP)\M|bien-fonds|part de copropri|\m\d+:\d+\s+fe\M'::text THEN true
            WHEN t.source_table = 'fao_transactions'::text THEN false
            ELSE t.ppe_indicator
        END AS ppe_indicator,
    t.ppe_pourmille_raw,
    t.buildings_raw,
    t.transaction_text,
    t.buyer_count,
    t.seller_count,
    t.buyers_display,
    t.sellers_display,
    pa.buyer_names,
    pa.seller_names,
    pa.buyer_entity_ids,
    pa.seller_entity_ids,
    pa.parties_buyers,
    pa.parties_sellers,
    gold_ch.normalize_property_type(COALESCE(
        CASE
            WHEN t.source_table = 'fao_transactions'::text THEN
            CASE
                WHEN t.transaction_text ~* '\mPPE\M|\mUE\s*\d|\m\d{1,2}\.\d{2,3}\s+(partie\s+)?(appartement|studio|attique|duplex|triplex|loft|box|parking|place|garage|cave|grenier|bureaux?|local|locaux|arcade|dépôt|depot|chambre|hall|magasin|atelier|cabinet|salle|restaurant|réduit)\M'::text OR ppp.transaction_id IS NOT NULL AND COALESCE(t.transaction_text, ''::text) !~* '\m(B-F|COP|DDP)\M|bien-fonds|part de copropri|\m\d+:\d+\s+fe\M'::text THEN 'apartment'::text
                WHEN t.is_ldtr = true THEN 'apartment'::text
                WHEN ft.resolved_typology = ANY (ARRAY['Habitation Un Logement'::text, 'Hab Deux Logements'::text]) THEN 'house'::text
                WHEN t.transaction_text ~* '\m(maison individuelle|villa|chalet)\M'::text THEN 'house'::text
                WHEN ft.resolved_typology = 'Hab Plusieurs Logements'::text THEN 'building'::text
                WHEN t.transaction_text ~* '\mimmeuble locatif\M'::text THEN 'building'::text
                WHEN ft.resolved_type_usage = 'Commercial'::text THEN 'commercial'::text
                WHEN t.transaction_text ~* '\m(bureau|arcade|local commercial|restaurant|hôtel|hotel|usine|atelier|dépôt|depot|hangar)\M'::text THEN 'commercial'::text
                WHEN ft.resolved_typology = ANY (ARRAY['Garage'::text, 'Garage Privé'::text, 'Parking Public'::text]) THEN 'parking'::text
                WHEN t.transaction_text ~* '\m(place de parc|box no|parking)\M'::text AND t.price < 200000::numeric THEN 'parking'::text
                WHEN ft.resolved_typology = ANY (ARRAY['Terrain Vierge'::text, 'Bois et Forêt'::text]) THEN 'land'::text
                WHEN t.transaction_text ~* '\m(parcelle non bâtie|terrain à bâtir|terrain vierge)\M'::text THEN 'land'::text
                WHEN ft.resolved_typology = 'Terrain Agricole'::text THEN 'agricultural'::text
                WHEN ft.resolved_type_usage = 'Agricole'::text THEN 'agricultural'::text
                ELSE NULL::text
            END
            WHEN t.source_table = 'fao_ldtr'::text THEN 'apartment'::text
            ELSE t.property_type
        END, llm.property_type)) AS property_type,
    t.surface_m2,
    t.previous_transaction_date,
    t.address,
    t.is_ldtr,
    t.ldtr_units,
    t.source_table,
    to_tsvector('french'::regconfig, (((((((COALESCE(t.buyers_display, ''::text) || ' '::text) || COALESCE(t.sellers_display, ''::text)) || ' '::text) || COALESCE(t.commune, ''::text)) || ' '::text) || COALESCE(t.transaction_type, ''::text)) || ' '::text) || COALESCE(t.address, ''::text)) AS fts_transaction,
    t.updated_at
   FROM silver_ch.event_transactions t
     LEFT JOIN party_agg pa ON pa.transaction_id::text = t.transaction_id::text
     LEFT JOIN plot_first pf ON pf.transaction_id::text = t.transaction_id::text
     LEFT JOIN plot_egrids pe ON pe.transaction_id::text = t.transaction_id::text
     LEFT JOIN plot_typology ft ON ft.transaction_id::text = t.transaction_id::text
     LEFT JOIN plot_ppe ppp ON ppp.transaction_id::text = t.transaction_id::text
     LEFT JOIN silver_ch.transactions_property_type_llm llm ON llm.transaction_id = t.transaction_id::text
     LEFT JOIN silver_ch.transaction_plotref_extract pre ON pre.transaction_id = t.transaction_id::text;
CREATE INDEX idx_core_txn_buyer_entities ON gold_ch.core_transactions USING gin (buyer_entity_ids) WHERE (buyer_entity_ids IS NOT NULL);
CREATE INDEX idx_core_txn_canton_commune ON gold_ch.core_transactions USING btree (canton_code, commune);
CREATE INDEX idx_core_txn_date ON gold_ch.core_transactions USING btree (transaction_date);
CREATE INDEX idx_core_txn_egrid ON gold_ch.core_transactions USING btree (primary_egrid) WHERE (primary_egrid IS NOT NULL);
CREATE INDEX idx_core_txn_fts ON gold_ch.core_transactions USING gin (fts_transaction);
CREATE UNIQUE INDEX idx_core_txn_id ON gold_ch.core_transactions USING btree (transaction_id);
CREATE INDEX idx_core_txn_plots ON gold_ch.core_transactions USING gin (plot_references);
CREATE INDEX idx_core_txn_seller_entities ON gold_ch.core_transactions USING gin (seller_entity_ids) WHERE (seller_entity_ids IS NOT NULL);
CREATE INDEX idx_core_txn_type ON gold_ch.core_transactions USING btree (transaction_type);
ALTER MATERIALIZED VIEW gold_ch.core_transactions OWNER TO postgres;
GRANT INSERT ON gold_ch.core_transactions TO service_role;
GRANT SELECT ON gold_ch.core_transactions TO service_role;
GRANT UPDATE ON gold_ch.core_transactions TO service_role;
GRANT DELETE ON gold_ch.core_transactions TO service_role;
GRANT TRUNCATE ON gold_ch.core_transactions TO service_role;
GRANT REFERENCES ON gold_ch.core_transactions TO service_role;
GRANT TRIGGER ON gold_ch.core_transactions TO service_role;
GRANT MAINTAIN ON gold_ch.core_transactions TO service_role;

CREATE VIEW gold_ch.v_transactions_full AS
 SELECT t.transaction_id,
    t.affaire_number,
    t.source_id,
    t.transaction_date,
    t.publication_date,
    t.transaction_type,
    t.transaction_type_list,
    t.price,
    t.commune,
    t.commune_number,
    t.canton_code,
    t.plot_references,
    t.egrids,
    t.primary_egrid,
    t.plot_surface_m2,
    t.price_per_m2,
    t.ppe_indicator,
    t.ppe_pourmille_raw,
    t.buildings_raw,
    t.transaction_text,
    t.buyer_count,
    t.seller_count,
    t.buyers_display,
    t.sellers_display,
    t.buyer_names,
    t.seller_names,
    t.buyer_entity_ids,
    t.seller_entity_ids,
    t.property_type,
    t.surface_m2,
    t.previous_transaction_date,
    t.address,
    t.is_ldtr,
    t.ldtr_units,
    t.source_table,
    t.fts_transaction,
    t.updated_at,
    COALESCE(sub.canonical_bfs, cl.canonical_bfs) AS commune_canonical_bfs,
    'ch'::character(2) AS country_code,
    cl.canton_code AS admin1_code,
    cl.canton_name AS admin1_name,
    NULL::text AS admin2_code,
    NULL::text AS admin2_name,
    COALESCE(sub.canonical_bfs, cl.canonical_bfs)::text AS admin3_code,
    COALESCE(sub.canonical_name, cl.canonical_name) AS admin3_name,
    COALESCE(sub.canonical_bfs, cl.canonical_bfs)::text AS admin3_canonical_id,
    t.parties_buyers,
    t.parties_sellers,
    ppe.ppe_lot,
    ppe.ppe_millieme,
    ppe.lot_type,
    ppe.unit_surface_m2,
    ppe.unit_surface_source,
    ppe.ppe_lot_count,
    ppe.ppe_lots,
    'fao'::text AS source,
    COALESCE(lf.footprint_stable, true) AS footprint_stable,
    COALESCE(lf.lineage_uncertain, false) AS lineage_uncertain
   FROM gold_ch.core_transactions t
     LEFT JOIN silver_ch.ref_city_lookup cl ON cl.city_norm = silver_ch.unaccent_lower(lower(t.commune)) AND (t.canton_code IS NULL OR cl.canton_code = t.canton_code)
     LEFT JOIN silver_ch.ref_communes sub ON cl.canonical_bfs = 6621 AND sub.parent_canonical_bfs = 6621 AND sub.no_commune =
        CASE
            WHEN t.commune_number ~ '^\d+$'::text THEN safe_cast.to_int(t.commune_number, 'silver_ch.event_transactions'::text, NULL::text, 'commune_number'::text)
            ELSE NULL::integer
        END
     LEFT JOIN silver_ch.transaction_ppe_summary ppe ON ppe.transaction_id = t.transaction_id::text
     LEFT JOIN silver_ch.transaction_lineage_flags lf ON lf.transaction_id = t.transaction_id::text;
ALTER VIEW gold_ch.v_transactions_full OWNER TO postgres;
GRANT INSERT ON gold_ch.v_transactions_full TO service_role;
GRANT SELECT ON gold_ch.v_transactions_full TO service_role;
GRANT UPDATE ON gold_ch.v_transactions_full TO service_role;
GRANT DELETE ON gold_ch.v_transactions_full TO service_role;
GRANT TRUNCATE ON gold_ch.v_transactions_full TO service_role;
GRANT REFERENCES ON gold_ch.v_transactions_full TO service_role;
GRANT TRIGGER ON gold_ch.v_transactions_full TO service_role;
GRANT MAINTAIN ON gold_ch.v_transactions_full TO service_role;

CREATE MATERIALIZED VIEW gold_ch.core_house_price_m2 AS
 WITH units AS (
         SELECT core_units.egrid,
            count(*)::integer AS n_dwellings,
            sum(core_units.surface_m2) AS habitable_surface_m2
           FROM gold_ch.core_units
          WHERE core_units.canton_code::text = 'GE'::text AND core_units.surface_m2 > 0::numeric AND core_units.egrid IS NOT NULL
          GROUP BY core_units.egrid
        ), addr AS (
         SELECT core_addresses.egrid,
            min(core_addresses.street_name) AS street_name
           FROM gold_ch.core_addresses
          WHERE core_addresses.canton_code::text = 'GE'::text AND core_addresses.egrid IS NOT NULL AND core_addresses.street_name IS NOT NULL
          GROUP BY core_addresses.egrid
         HAVING count(DISTINCT core_addresses.street_name) = 1
        ), base AS (
         SELECT t.transaction_id,
            t.commune_canonical_bfs AS commune_bfs,
            NULLIF(t.commune, ''::text) AS commune_name,
            p.sous_secteur_nom,
            a.street_name,
            t.transaction_date,
            t.price,
            u.habitable_surface_m2,
            u.n_dwellings,
            round(t.price / u.habitable_surface_m2) AS price_per_m2
           FROM gold_ch.v_transactions_full t
             JOIN units u ON u.egrid::text = t.primary_egrid
             LEFT JOIN gold_ch.core_plots p ON p.egrid = t.primary_egrid
             LEFT JOIN addr a ON a.egrid::text = t.primary_egrid
          WHERE array_length(t.egrids, 1) = 1 AND t.price > 0::numeric AND t.property_type = 'house'::text AND u.habitable_surface_m2 > 0::numeric AND round(t.price / u.habitable_surface_m2) >= 1000::numeric AND round(t.price / u.habitable_surface_m2) <= 50000::numeric
        ), win(window_label, cutoff) AS (
         VALUES ('24m'::text,CURRENT_DATE - '2 years'::interval), ('60m'::text,CURRENT_DATE - '5 years'::interval), ('all'::text,'1900-01-01 00:00:00'::timestamp without time zone)
        ), scoped AS (
         SELECT w.window_label,
            b.transaction_id,
            b.commune_bfs,
            b.commune_name,
            b.sous_secteur_nom,
            b.street_name,
            b.transaction_date,
            b.price,
            b.habitable_surface_m2,
            b.n_dwellings,
            b.price_per_m2
           FROM base b
             CROSS JOIN win w
          WHERE w.window_label = 'all'::text OR b.transaction_date >= w.cutoff
        ), agg AS (
         SELECT scoped.window_label,
            'street'::text AS level,
            scoped.commune_bfs,
            max(scoped.commune_name) AS commune_name,
            scoped.sous_secteur_nom,
            scoped.street_name,
            count(*)::integer AS n_sales,
            round(percentile_cont(0.5::double precision) WITHIN GROUP (ORDER BY (scoped.price_per_m2::double precision)))::numeric AS median_price_m2,
            round(percentile_cont(0.25::double precision) WITHIN GROUP (ORDER BY (scoped.price_per_m2::double precision)))::numeric AS p25_price_m2,
            round(percentile_cont(0.75::double precision) WITHIN GROUP (ORDER BY (scoped.price_per_m2::double precision)))::numeric AS p75_price_m2,
            min(scoped.transaction_date) AS first_sale,
            max(scoped.transaction_date) AS last_sale
           FROM scoped
          WHERE scoped.street_name IS NOT NULL AND scoped.sous_secteur_nom IS NOT NULL AND scoped.commune_bfs IS NOT NULL
          GROUP BY scoped.window_label, scoped.commune_bfs, scoped.sous_secteur_nom, scoped.street_name
         HAVING count(*) >= 3
        UNION ALL
         SELECT scoped.window_label,
            'girec'::text AS text,
            scoped.commune_bfs,
            max(scoped.commune_name) AS max,
            scoped.sous_secteur_nom,
            NULL::text AS text,
            count(*)::integer AS count,
            round(percentile_cont(0.5::double precision) WITHIN GROUP (ORDER BY (scoped.price_per_m2::double precision)))::numeric AS round,
            round(percentile_cont(0.25::double precision) WITHIN GROUP (ORDER BY (scoped.price_per_m2::double precision)))::numeric AS round,
            round(percentile_cont(0.75::double precision) WITHIN GROUP (ORDER BY (scoped.price_per_m2::double precision)))::numeric AS round,
            min(scoped.transaction_date) AS min,
            max(scoped.transaction_date) AS max
           FROM scoped
          WHERE scoped.sous_secteur_nom IS NOT NULL AND scoped.commune_bfs IS NOT NULL
          GROUP BY scoped.window_label, scoped.commune_bfs, scoped.sous_secteur_nom
         HAVING count(*) >= 3
        UNION ALL
         SELECT scoped.window_label,
            'commune'::text AS text,
            scoped.commune_bfs,
            max(scoped.commune_name) AS max,
            NULL::text AS text,
            NULL::text AS text,
            count(*)::integer AS count,
            round(percentile_cont(0.5::double precision) WITHIN GROUP (ORDER BY (scoped.price_per_m2::double precision)))::numeric AS round,
            round(percentile_cont(0.25::double precision) WITHIN GROUP (ORDER BY (scoped.price_per_m2::double precision)))::numeric AS round,
            round(percentile_cont(0.75::double precision) WITHIN GROUP (ORDER BY (scoped.price_per_m2::double precision)))::numeric AS round,
            min(scoped.transaction_date) AS min,
            max(scoped.transaction_date) AS max
           FROM scoped
          WHERE scoped.commune_bfs IS NOT NULL
          GROUP BY scoped.window_label, scoped.commune_bfs
         HAVING count(*) >= 3
        UNION ALL
         SELECT scoped.window_label,
            'canton'::text AS text,
            NULL::integer AS int4,
            'Genève'::text AS text,
            NULL::text AS text,
            NULL::text AS text,
            count(*)::integer AS count,
            round(percentile_cont(0.5::double precision) WITHIN GROUP (ORDER BY (scoped.price_per_m2::double precision)))::numeric AS round,
            round(percentile_cont(0.25::double precision) WITHIN GROUP (ORDER BY (scoped.price_per_m2::double precision)))::numeric AS round,
            round(percentile_cont(0.75::double precision) WITHIN GROUP (ORDER BY (scoped.price_per_m2::double precision)))::numeric AS round,
            min(scoped.transaction_date) AS min,
            max(scoped.transaction_date) AS max
           FROM scoped
          GROUP BY scoped.window_label
         HAVING count(*) >= 3
        )
 SELECT window_label,
    level,
    commune_bfs,
    commune_name,
    sous_secteur_nom,
    street_name,
    COALESCE(commune_bfs, '-1'::integer) AS commune_bfs_key,
    COALESCE(sous_secteur_nom, ''::text) AS sous_secteur_key,
    COALESCE(street_name, ''::text) AS street_key,
    n_sales,
    median_price_m2,
    p25_price_m2,
    p75_price_m2,
        CASE
            WHEN n_sales >= 20 THEN 'high'::text
            WHEN n_sales >= 5 THEN 'medium'::text
            ELSE 'low'::text
        END AS confidence,
    first_sale,
    last_sale,
    'house'::text AS property_type,
    'GE'::text AS canton_code,
    'transactions_surveyed_habitable'::text AS price_source,
    now() AS computed_at
   FROM agg;
CREATE INDEX ix_core_house_price_m2_lookup ON gold_ch.core_house_price_m2 USING btree (window_label, level, commune_bfs);
CREATE UNIQUE INDEX ux_core_house_price_m2 ON gold_ch.core_house_price_m2 USING btree (window_label, level, commune_bfs_key, sous_secteur_key, street_key);
ALTER MATERIALIZED VIEW gold_ch.core_house_price_m2 OWNER TO postgres;
COMMENT ON MATERIALIZED VIEW gold_ch.core_house_price_m2 IS 'P1 (2026-07-15): REAL habitable price/m2 for GE villa transactions = prix / SURVEYED dwelling surface (core_units), NOT terrain. Houses only — apartments are not buildable at unit level (0/18884 PPE lot<->unit crosswalk; pourmille is a value share, not surface) per audits/2026-07-14e. Base = clean single-plot (excludes ~11% bundle deals), property_type=house, clamped 1000-50000 CHF/m2 (same bound as core_benchmark_sales listings tier; keeps 96.26%). Bridge = primary_egrid -> core_units.egrid, 82.4% of 20221 clean villas resolve (77.7% to exactly one dwelling). Grain = (window_label, level, commune_bfs, sous_secteur_nom, street_name); levels street>girec>commune>canton for parent fallback; min 3 sales per cell so a 2-sale median is never emitted. Windows 24m/60m/all let the consumer trade recency against depth (villa CHF/m2 rose ~60% 2017->2026, so ''all'' must NOT be read as a current price). Feeds the P2 canonical benchmark house sale leg. Do NOT use core_plots.commune_name (empty string on 99.1% of GE plots) — commune comes from the transaction.';
GRANT INSERT ON gold_ch.core_house_price_m2 TO service_role;
GRANT SELECT ON gold_ch.core_house_price_m2 TO service_role;
GRANT UPDATE ON gold_ch.core_house_price_m2 TO service_role;
GRANT DELETE ON gold_ch.core_house_price_m2 TO service_role;
GRANT TRUNCATE ON gold_ch.core_house_price_m2 TO service_role;
GRANT REFERENCES ON gold_ch.core_house_price_m2 TO service_role;
GRANT TRIGGER ON gold_ch.core_house_price_m2 TO service_role;
GRANT MAINTAIN ON gold_ch.core_house_price_m2 TO service_role;

CREATE VIEW gold_ch.v_foncier_transactions AS
 SELECT transaction_id,
    affaire_number,
    source_id,
    transaction_date,
    publication_date,
    transaction_type,
    transaction_type_list,
    price,
    commune,
    commune_number,
    canton_code,
    plot_references,
    egrids,
    primary_egrid,
    plot_surface_m2,
    price_per_m2,
    ppe_indicator,
    ppe_pourmille_raw,
    buildings_raw,
    transaction_text,
    buyer_count,
    seller_count,
    buyers_display,
    sellers_display,
    buyer_names,
    seller_names,
    property_type,
    surface_m2,
    previous_transaction_date,
    address,
    is_ldtr,
    ldtr_units,
    source_table,
    fts_transaction,
    updated_at,
    commune_canonical_bfs,
    country_code,
    admin1_code,
    admin1_name,
    admin2_code,
    admin2_name,
    admin3_code,
    admin3_name,
    admin3_canonical_id,
        CASE
            WHEN parties_buyers IS NULL THEN NULL::jsonb
            ELSE COALESCE(( SELECT jsonb_agg(jsonb_build_object('name', e.value ->> 'name'::text, 'entity_type', e.value ->> 'entity_type'::text) ORDER BY e.ord) AS jsonb_agg
               FROM jsonb_array_elements(t.parties_buyers) WITH ORDINALITY e(value, ord)), '[]'::jsonb)
        END AS parties_buyers,
        CASE
            WHEN parties_sellers IS NULL THEN NULL::jsonb
            ELSE COALESCE(( SELECT jsonb_agg(jsonb_build_object('name', e.value ->> 'name'::text, 'entity_type', e.value ->> 'entity_type'::text) ORDER BY e.ord) AS jsonb_agg
               FROM jsonb_array_elements(t.parties_sellers) WITH ORDINALITY e(value, ord)), '[]'::jsonb)
        END AS parties_sellers
   FROM gold_ch.v_transactions_full t
  WHERE canton_code = 'GE'::text AND transaction_date >= (CURRENT_DATE - '5 years'::interval);
ALTER VIEW gold_ch.v_foncier_transactions OWNER TO postgres;
GRANT INSERT ON gold_ch.v_foncier_transactions TO service_role;
GRANT SELECT ON gold_ch.v_foncier_transactions TO service_role;
GRANT UPDATE ON gold_ch.v_foncier_transactions TO service_role;
GRANT DELETE ON gold_ch.v_foncier_transactions TO service_role;
GRANT TRUNCATE ON gold_ch.v_foncier_transactions TO service_role;
GRANT REFERENCES ON gold_ch.v_foncier_transactions TO service_role;
GRANT TRIGGER ON gold_ch.v_foncier_transactions TO service_role;
GRANT MAINTAIN ON gold_ch.v_foncier_transactions TO service_role;

CREATE MATERIALIZED VIEW gold_ch.core_benchmark_v2 AS
 WITH spine AS (
         SELECT DISTINCT core_addresses.commune_bfs,
            core_addresses.commune_name,
            core_addresses.canton_code
           FROM gold_ch.core_addresses
          WHERE core_addresses.commune_bfs IS NOT NULL AND core_addresses.commune_name IS NOT NULL AND core_addresses.canton_code IS NOT NULL
        ), npa_w AS (
         SELECT core_addresses.commune_bfs,
            core_addresses.postal_code AS npa,
            count(*)::numeric AS addr_n
           FROM gold_ch.core_addresses
          WHERE core_addresses.commune_bfs IS NOT NULL AND core_addresses.postal_code IS NOT NULL
          GROUP BY core_addresses.commune_bfs, core_addresses.postal_code
        ), ra AS (
         SELECT s.commune_bfs,
            s.canton_code,
            ep.property_type AS asset,
            avg(ep.sale_price_per_m2) FILTER (WHERE ep.sale_price_per_m2 >= 1000::numeric AND ep.sale_price_per_m2 <= 50000::numeric) AS sale_m2
           FROM silver_ch.market_external_prices ep
             JOIN spine s ON lower(s.commune_name) = lower(ep.commune_name)
          WHERE ep.source ~~ 'realadvisor%'::text
          GROUP BY s.commune_bfs, s.canton_code, ep.property_type
        ), oc AS (
         SELECT s.commune_bfs,
            s.canton_code,
            o.asset,
            avg(o.price_median) FILTER (WHERE o.deal_type = 'sale'::text AND o.price_median >= 1000::numeric AND o.price_median <= 50000::numeric) AS sale_m2,
            avg(o.price_median) FILTER (WHERE o.deal_type = 'rent'::text AND o.price_median >= 5::numeric AND o.price_median <= 200::numeric) AS rent_m2
           FROM silver_ch.ocstat_prices o
             JOIN spine s ON lower(s.commune_name) = lower(o.commune_name) AND s.canton_code::text = 'GE'::text
          WHERE o.geo_level = 'commune'::text AND o.value_scope = 'own'::text AND o.is_price_per_m2 AND NOT o.is_ambiguous_grain AND o.market_type = 'free_market'::text
          GROUP BY s.commune_bfs, s.canton_code, o.asset
        ), tx AS (
         SELECT h.commune_bfs,
            'GE'::text AS canton_code,
            'house'::text AS asset,
            h.median_price_m2 AS sale_m2,
            h.n_sales
           FROM gold_ch.core_house_price_m2 h
          WHERE h.level = 'commune'::text AND h.window_label = '60m'::text AND h.median_price_m2 >= 1000::numeric AND h.median_price_m2 <= 50000::numeric
        ), ph AS (
         SELECT w.commune_bfs,
            ep.property_type AS asset,
            sum(ep.sale_price_per_m2 * w.addr_n) FILTER (WHERE ep.sale_price_per_m2 >= 1000::numeric AND ep.sale_price_per_m2 <= 50000::numeric) / NULLIF(sum(w.addr_n) FILTER (WHERE ep.sale_price_per_m2 >= 1000::numeric AND ep.sale_price_per_m2 <= 50000::numeric), 0::numeric) AS sale_m2,
            sum(ep.rent_price_per_m2_monthly * w.addr_n) FILTER (WHERE ep.rent_price_per_m2_monthly >= 5::numeric AND ep.rent_price_per_m2_monthly <= 200::numeric) / NULLIF(sum(w.addr_n) FILTER (WHERE ep.rent_price_per_m2_monthly >= 5::numeric AND ep.rent_price_per_m2_monthly <= 200::numeric), 0::numeric) AS rent_m2
           FROM silver_ch.market_external_prices ep
             JOIN npa_w w ON w.npa = ep.npa
          WHERE ep.source = 'pricehubble'::text
          GROUP BY w.commune_bfs, ep.property_type
        ), li AS (
         SELECT w.commune_bfs,
                CASE
                    WHEN ls.property_type = ANY (ARRAY['house'::text, 'maison'::text]) THEN 'house'::text
                    ELSE 'apartment'::text
                END AS asset,
            sum(ls.median_price_per_m2 * w.addr_n::double precision) FILTER (WHERE ls.median_price_per_m2 >= 1000::double precision AND ls.median_price_per_m2 <= 50000::double precision) / NULLIF(sum(w.addr_n) FILTER (WHERE ls.median_price_per_m2 >= 1000::double precision AND ls.median_price_per_m2 <= 50000::double precision), 0::numeric)::double precision AS sale_m2,
            sum(ls.listing_count) FILTER (WHERE ls.median_price_per_m2 >= 1000::double precision AND ls.median_price_per_m2 <= 50000::double precision) AS listing_n
           FROM silver_ch.market_listing_stats ls
             JOIN npa_w w ON w.npa = ls.npa
          WHERE (ls.offer_type = ANY (ARRAY['sale'::text, 'achat'::text])) AND ls.median_price_per_m2 IS NOT NULL
          GROUP BY w.commune_bfs, (
                CASE
                    WHEN ls.property_type = ANY (ARRAY['house'::text, 'maison'::text]) THEN 'house'::text
                    ELSE 'apartment'::text
                END)
        ), bfs AS (
         SELECT bfs_average_rents.canton_code,
            avg(bfs_average_rents.average_rent_chf) / 70.0 AS rent_m2
           FROM bronze_ch.bfs_average_rents
          WHERE bfs_average_rents.canton_code <> 'CH'::text AND bfs_average_rents.rooms = 3
          GROUP BY bfs_average_rents.canton_code
        ), grid AS (
         SELECT s.commune_bfs,
            s.commune_name,
            s.canton_code,
            a.asset,
            d.deal_type
           FROM spine s
             CROSS JOIN ( VALUES ('apartment'::text), ('house'::text)) a(asset)
             CROSS JOIN ( VALUES ('sale'::text), ('rent'::text)) d(deal_type)
        ), vals AS (
         SELECT g.commune_bfs,
            g.commune_name,
            g.canton_code,
            g.asset,
            g.deal_type,
            g.canton_code::text = 'GE'::text AS is_ge,
                CASE
                    WHEN g.deal_type = 'sale'::text THEN ra.sale_m2
                    ELSE NULL::numeric
                END AS v_ra,
                CASE
                    WHEN g.deal_type = 'sale'::text THEN oc.sale_m2
                    ELSE oc.rent_m2
                END AS v_oc,
                CASE
                    WHEN g.deal_type = 'sale'::text THEN tx.sale_m2
                    ELSE NULL::numeric
                END AS v_tx,
                CASE
                    WHEN g.deal_type = 'sale'::text THEN ph.sale_m2
                    ELSE ph.rent_m2
                END AS v_ph,
                CASE
                    WHEN g.deal_type = 'sale'::text THEN li.sale_m2
                    ELSE NULL::double precision
                END AS v_li,
                CASE
                    WHEN g.deal_type = 'rent'::text THEN bfs.rent_m2
                    ELSE NULL::numeric
                END AS v_bfs,
            tx.n_sales,
            li.listing_n
           FROM grid g
             LEFT JOIN ra ON ra.commune_bfs = g.commune_bfs AND ra.asset = g.asset
             LEFT JOIN oc ON oc.commune_bfs = g.commune_bfs AND oc.asset = g.asset
             LEFT JOIN tx ON tx.commune_bfs = g.commune_bfs AND tx.asset = g.asset AND g.deal_type = 'sale'::text
             LEFT JOIN ph ON ph.commune_bfs = g.commune_bfs AND ph.asset = g.asset
             LEFT JOIN li ON li.commune_bfs = g.commune_bfs AND li.asset = g.asset
             LEFT JOIN bfs ON bfs.canton_code = g.canton_code::text
        ), w AS (
         SELECT v.commune_bfs,
            v.commune_name,
            v.canton_code,
            v.asset,
            v.deal_type,
            v.is_ge,
            v.v_ra,
            v.v_oc,
            v.v_tx,
            v.v_ph,
            v.v_li,
            v.v_bfs,
            v.n_sales,
            v.listing_n,
                CASE
                    WHEN v.v_tx IS NULL THEN 0::numeric
                    WHEN v.deal_type = 'sale'::text AND v.asset = 'house'::text AND v.is_ge THEN 0.55
                    ELSE 0::numeric
                END AS w_tx,
                CASE
                    WHEN v.v_ra IS NULL THEN 0::numeric
                    WHEN v.asset = 'house'::text AND v.is_ge THEN 0.25
                    WHEN v.asset = 'house'::text THEN 0.55
                    WHEN v.asset = 'apartment'::text AND v.is_ge THEN 0.30
                    ELSE 0.50
                END AS w_ra,
                CASE
                    WHEN v.v_oc IS NULL THEN 0::numeric
                    WHEN v.deal_type = 'sale'::text AND v.is_ge THEN 0.15
                    WHEN v.deal_type = 'rent'::text AND v.is_ge THEN 0.40
                    ELSE 0::numeric
                END AS w_oc,
                CASE
                    WHEN v.v_ph IS NULL THEN 0::numeric
                    WHEN v.deal_type = 'sale'::text AND v.is_ge THEN 0.10
                    WHEN v.deal_type = 'sale'::text THEN 0.30
                    WHEN v.deal_type = 'rent'::text AND v.is_ge THEN 0.45
                    ELSE 0.70
                END AS w_ph,
                CASE
                    WHEN v.v_li IS NULL THEN 0::numeric
                    WHEN v.asset = 'apartment'::text AND v.is_ge THEN 0.35
                    WHEN v.asset = 'house'::text AND v.is_ge THEN 0.10
                    ELSE 0.15
                END AS w_li,
                CASE
                    WHEN v.v_bfs IS NULL THEN 0::numeric
                    WHEN v.is_ge THEN 0.15
                    ELSE 0.30
                END AS w_bfs
           FROM vals v
        ), blend AS (
         SELECT w.commune_bfs,
            w.commune_name,
            w.canton_code,
            w.asset,
            w.deal_type,
            w.is_ge,
            w.v_ra,
            w.v_oc,
            w.v_tx,
            w.v_ph,
            w.v_li,
            w.v_bfs,
            w.n_sales,
            w.listing_n,
            w.w_tx,
            w.w_ra,
            w.w_oc,
            w.w_ph,
            w.w_li,
            w.w_bfs,
            w.w_tx + w.w_ra + w.w_oc + w.w_ph + w.w_li + w.w_bfs AS w_tot,
            (COALESCE(w.v_tx * w.w_tx, 0::numeric) + COALESCE(w.v_ra * w.w_ra, 0::numeric) + COALESCE(w.v_oc * w.w_oc, 0::numeric) + COALESCE(w.v_ph * w.w_ph, 0::numeric))::double precision + COALESCE(w.v_li * w.w_li::double precision, 0::double precision) + COALESCE(w.v_bfs * w.w_bfs, 0::numeric)::double precision AS w_sum,
            (w.v_tx IS NOT NULL)::integer + (w.v_ra IS NOT NULL)::integer + (w.v_oc IS NOT NULL)::integer + (w.v_ph IS NOT NULL)::integer + (w.v_li IS NOT NULL)::integer + (w.v_bfs IS NOT NULL)::integer AS src_n
           FROM w
        ), final AS (
         SELECT b.commune_bfs,
            b.commune_name,
            b.canton_code,
            b.asset,
            b.deal_type,
            b.is_ge,
            b.v_ra,
            b.v_oc,
            b.v_tx,
            b.v_ph,
            b.v_li,
            b.v_bfs,
            b.n_sales,
            b.listing_n,
            b.w_tx,
            b.w_ra,
            b.w_oc,
            b.w_ph,
            b.w_li,
            b.w_bfs,
            b.w_tot,
            b.w_sum,
            b.src_n,
            (b.w_sum / NULLIF(b.w_tot, 0::numeric)::double precision)::numeric AS px,
                CASE
                    WHEN (b.v_tx IS NOT NULL OR b.v_ra IS NOT NULL OR b.v_oc IS NOT NULL) AND (b.v_ph IS NOT NULL OR b.v_li IS NOT NULL) THEN 'mixed'::text
                    WHEN b.v_tx IS NOT NULL OR b.v_ra IS NOT NULL OR b.v_oc IS NOT NULL THEN 'native_commune'::text
                    WHEN b.v_ph IS NOT NULL OR b.v_li IS NOT NULL THEN 'apportioned_from_npa'::text
                    ELSE 'canton_fallback'::text
                END AS gb
           FROM blend b
          WHERE b.w_tot > 0::numeric AND b.w_sum IS NOT NULL
        )
 SELECT 'commune'::text AS level,
    f.commune_bfs,
    f.commune_name,
    NULL::text AS sous_secteur_nom,
    NULL::text AS street_name,
    f.commune_bfs AS commune_bfs_key,
    ''::text AS sous_secteur_key,
    ''::text AS street_key,
    f.canton_code,
    f.asset AS property_type,
    f.deal_type,
    round(f.px, 2) AS blended_price_m2,
    jsonb_strip_nulls(jsonb_build_object('transactions',
        CASE
            WHEN f.v_tx IS NOT NULL THEN jsonb_build_object('value', round(f.v_tx, 2), 'weight', f.w_tx, 'n', f.n_sales, 'grain', 'commune_native')
            ELSE NULL::jsonb
        END, 'realadvisor',
        CASE
            WHEN f.v_ra IS NOT NULL THEN jsonb_build_object('value', round(f.v_ra, 2), 'weight', f.w_ra, 'grain', 'commune_native')
            ELSE NULL::jsonb
        END, 'ocstat',
        CASE
            WHEN f.v_oc IS NOT NULL THEN jsonb_build_object('value', round(f.v_oc, 2), 'weight', f.w_oc, 'grain', 'commune_native')
            ELSE NULL::jsonb
        END, 'pricehubble',
        CASE
            WHEN f.v_ph IS NOT NULL THEN jsonb_build_object('value', round(f.v_ph, 2), 'weight', f.w_ph, 'grain', 'apportioned_from_npa')
            ELSE NULL::jsonb
        END, 'listings',
        CASE
            WHEN f.v_li IS NOT NULL THEN jsonb_build_object('value', round(f.v_li::numeric, 2), 'weight', f.w_li, 'n', f.listing_n, 'grain', 'apportioned_from_npa')
            ELSE NULL::jsonb
        END, 'bfs',
        CASE
            WHEN f.v_bfs IS NOT NULL THEN jsonb_build_object('value', round(f.v_bfs, 2), 'weight', f.w_bfs, 'grain', 'canton')
            ELSE NULL::jsonb
        END)) AS sources,
    f.src_n AS source_count,
    f.n_sales,
    f.gb AS geo_basis,
    f.v_tx IS NOT NULL AS has_transaction_leg,
    f.is_ge AS is_full_stack,
        CASE
            WHEN f.gb = 'canton_fallback'::text THEN 'low'::text
            WHEN f.v_tx IS NOT NULL AND f.n_sales >= 20 THEN 'high'::text
            WHEN f.src_n >= 3 THEN 'high'::text
            WHEN f.src_n = 2 THEN 'medium'::text
            ELSE 'low'::text
        END AS confidence,
    'canonical_blend'::text AS price_source,
    now() AS computed_at
   FROM final f
  WHERE f.deal_type = 'sale'::text AND f.px >= 1000::numeric AND f.px <= 50000::numeric OR f.deal_type = 'rent'::text AND f.px >= 5::numeric AND f.px <= 200::numeric
UNION ALL
 SELECT h.level,
    h.commune_bfs,
    h.commune_name,
    h.sous_secteur_nom,
    h.street_name,
    h.commune_bfs AS commune_bfs_key,
    COALESCE(h.sous_secteur_nom, ''::text) AS sous_secteur_key,
    COALESCE(h.street_name, ''::text) AS street_key,
    'GE'::text AS canton_code,
    'house'::text AS property_type,
    'sale'::text AS deal_type,
    h.median_price_m2 AS blended_price_m2,
    jsonb_build_object('transactions', jsonb_build_object('value', h.median_price_m2, 'weight', 1.0, 'n', h.n_sales, 'grain', h.level || '_native'::text)) AS sources,
    1 AS source_count,
    h.n_sales,
    'native_commune'::text AS geo_basis,
    true AS has_transaction_leg,
    true AS is_full_stack,
    h.confidence,
    'transactions_only'::text AS price_source,
    now() AS computed_at
   FROM gold_ch.core_house_price_m2 h
  WHERE h.window_label = '60m'::text AND (h.level = ANY (ARRAY['girec'::text, 'street'::text])) AND h.median_price_m2 >= 1000::numeric AND h.median_price_m2 <= 50000::numeric;
CREATE INDEX ix_core_benchmark_v2_lookup ON gold_ch.core_benchmark_v2 USING btree (canton_code, commune_bfs, property_type, deal_type);
CREATE UNIQUE INDEX ux_core_benchmark_v2 ON gold_ch.core_benchmark_v2 USING btree (level, commune_bfs_key, sous_secteur_key, street_key, property_type, deal_type);
ALTER MATERIALIZED VIEW gold_ch.core_benchmark_v2 OWNER TO postgres;
COMMENT ON MATERIALIZED VIEW gold_ch.core_benchmark_v2 IS 'C1 THE CANONICAL BENCHMARK (2026-07-15, rent-crossover fix applied). Commune spine (core_addresses) + GIREC/street projections (transactions/P1 only). REAL weighted blend, renormalised over present sources. RENT SOURCES = pricehubble + ocstat + bfs ONLY: RealAdvisor is EXCLUDED from rent because bug a99d05dc (maison.rent_m2 contains appartement.sales_m2) is live in silver_ch.market_external_prices (Genève house rent field = 15494, the apartment sale price; RA rent medians apt 556 / house 11331 vs pricehubble apt 19 / house 22), and market_listing_stats has ZERO rent rows (100% offer_type=sale) so listings cannot serve rent either. RA and listings remain first-class SALE sources. Per-source clamps: sale 1000-50000 (core_benchmark_sales/P1 convention), rent 5-200 (core_benchmark_rentals convention); plus an OUTPUT sanity gate on the blended value in the same bands, so nothing implausible ships whatever the inputs do. confidence: canton_fallback is forced to low (one canton average spread across communes is never high). Ultra-prime land-heavy sale cells are truncated by the 50000 clamp, NOT segmented — a segment key does not exist (class_label is RegBL typology, not quality). DOES NOT REPLACE core_benchmark_sales/_rentals — those keep serving until Claude repoints.';
GRANT INSERT ON gold_ch.core_benchmark_v2 TO service_role;
GRANT SELECT ON gold_ch.core_benchmark_v2 TO service_role;
GRANT UPDATE ON gold_ch.core_benchmark_v2 TO service_role;
GRANT DELETE ON gold_ch.core_benchmark_v2 TO service_role;
GRANT TRUNCATE ON gold_ch.core_benchmark_v2 TO service_role;
GRANT REFERENCES ON gold_ch.core_benchmark_v2 TO service_role;
GRANT TRIGGER ON gold_ch.core_benchmark_v2 TO service_role;
GRANT MAINTAIN ON gold_ch.core_benchmark_v2 TO service_role;

CREATE VIEW gold_ch.v_house_price_m2_full AS
 SELECT window_label,
    level,
    commune_bfs,
    commune_name,
    sous_secteur_nom,
    street_name,
    n_sales,
    median_price_m2,
    p25_price_m2,
    p75_price_m2,
    confidence,
    first_sale,
    last_sale,
    property_type,
    canton_code,
    price_source,
    computed_at
   FROM gold_ch.core_house_price_m2;
ALTER VIEW gold_ch.v_house_price_m2_full OWNER TO postgres;
GRANT INSERT ON gold_ch.v_house_price_m2_full TO service_role;
GRANT SELECT ON gold_ch.v_house_price_m2_full TO service_role;
GRANT UPDATE ON gold_ch.v_house_price_m2_full TO service_role;
GRANT DELETE ON gold_ch.v_house_price_m2_full TO service_role;
GRANT TRUNCATE ON gold_ch.v_house_price_m2_full TO service_role;
GRANT REFERENCES ON gold_ch.v_house_price_m2_full TO service_role;
GRANT TRIGGER ON gold_ch.v_house_price_m2_full TO service_role;
GRANT MAINTAIN ON gold_ch.v_house_price_m2_full TO service_role;

CREATE MATERIALIZED VIEW gold_ch.core_communes AS
 WITH rc AS (
         SELECT r_1.canonical_bfs,
            r_1.canonical_name,
            r_1.canton_code,
            r_1.canton_name,
            r_1.no_commune,
            r_1.is_sub_commune,
            r_1.parent_canonical_bfs,
            r_1.aliases,
            r_1.population,
            r_1.area_km2,
            r_1.appartenance,
            r_1.geometry,
            r_1.npa_list,
            r_1.source_notes,
            r_1.imported_at,
            r_1.updated_at,
            r_1.canonical_name_norm,
            r_1.aliases_norm,
            unaccent(lower(btrim(r_1.canonical_name))) AS k_name,
            unaccent(lower(btrim(regexp_replace(r_1.canonical_name, '\s*\([A-Z]{2}\)\s*$'::text, ''::text)))) AS k_stripped
           FROM silver_ch.ref_communes r_1
        ), keys AS (
         SELECT rc_1.canton_code,
            rc_1.k_name AS k,
            rc_1.canonical_bfs,
            1 AS prio
           FROM rc rc_1
        UNION ALL
         SELECT rc_1.canton_code,
            unaccent(lower(btrim(a.v))) AS unaccent,
            rc_1.canonical_bfs,
            2
           FROM rc rc_1
             CROSS JOIN LATERAL unnest(rc_1.aliases) a(v)
        UNION ALL
         SELECT rc_1.canton_code,
            rc_1.k_stripped,
            rc_1.canonical_bfs,
            3
           FROM rc rc_1
          WHERE rc_1.k_stripped <> rc_1.k_name
        ), key_map AS (
         SELECT DISTINCT ON (keys.canton_code, keys.k) keys.canton_code,
            keys.k,
            keys.canonical_bfs
           FROM keys
          ORDER BY keys.canton_code, keys.k, keys.prio, keys.canonical_bfs
        ), tx_names AS (
         SELECT core_transactions.canton_code,
            unaccent(lower(btrim(regexp_replace(core_transactions.commune, '\s*\([A-Z]{2}\)\s*$'::text, ''::text)))) AS k,
            count(*) AS n
           FROM gold_ch.core_transactions
          WHERE core_transactions.commune IS NOT NULL
          GROUP BY core_transactions.canton_code, (unaccent(lower(btrim(regexp_replace(core_transactions.commune, '\s*\([A-Z]{2}\)\s*$'::text, ''::text)))))
        ), sad_names AS (
         SELECT core_sad.canton_code,
            unaccent(lower(btrim(regexp_replace(core_sad.commune, '\s*\([A-Z]{2}\)\s*$'::text, ''::text)))) AS k,
            count(*) AS n
           FROM gold_ch.core_sad
          WHERE core_sad.commune IS NOT NULL
          GROUP BY core_sad.canton_code, (unaccent(lower(btrim(regexp_replace(core_sad.commune, '\s*\([A-Z]{2}\)\s*$'::text, ''::text)))))
        ), tx AS (
         SELECT km.canonical_bfs AS bfs,
            sum(t.n)::bigint AS n
           FROM tx_names t
             JOIN key_map km USING (canton_code, k)
          GROUP BY km.canonical_bfs
        ), sad AS (
         SELECT km.canonical_bfs AS bfs,
            sum(s.n)::bigint AS n
           FROM sad_names s
             JOIN key_map km USING (canton_code, k)
          GROUP BY km.canonical_bfs
        ), lst AS (
         SELECT x.commune_bfs AS bfs,
            count(*) AS n
           FROM gold_ch.core_listings cl
             JOIN silver_ch.ref_npa_commune x ON x.npa = cl.npa
          WHERE cl.npa IS NOT NULL AND x.commune_bfs IS NOT NULL
          GROUP BY x.commune_bfs
        ), own AS (
         SELECT rc_1.canonical_bfs AS bfs,
            rc_1.parent_canonical_bfs AS parent,
            COALESCE(tx.n, 0::bigint) AS tx_n,
            COALESCE(sad.n, 0::bigint) AS sad_n,
            COALESCE(lst.n, 0::bigint) AS lst_n
           FROM rc rc_1
             LEFT JOIN tx ON tx.bfs = rc_1.canonical_bfs
             LEFT JOIN sad ON sad.bfs = rc_1.canonical_bfs
             LEFT JOIN lst ON lst.bfs = rc_1.canonical_bfs
        ), rolled AS (
         SELECT o.bfs,
            o.tx_n::numeric + COALESCE(( SELECT sum(c.tx_n) AS sum
                   FROM own c
                  WHERE c.parent = o.bfs), 0::numeric) AS tx_n,
            o.sad_n::numeric + COALESCE(( SELECT sum(c.sad_n) AS sum
                   FROM own c
                  WHERE c.parent = o.bfs), 0::numeric) AS sad_n,
            o.lst_n::numeric + COALESCE(( SELECT sum(c.lst_n) AS sum
                   FROM own c
                  WHERE c.parent = o.bfs), 0::numeric) AS lst_n
           FROM own o
        ), bench AS (
         SELECT core_benchmark_v2.commune_bfs,
            max(core_benchmark_v2.blended_price_m2) FILTER (WHERE core_benchmark_v2.deal_type = 'sale'::text AND core_benchmark_v2.property_type = 'apartment'::text) AS sale_m2,
            max(core_benchmark_v2.blended_price_m2) FILTER (WHERE core_benchmark_v2.deal_type = 'rent'::text AND core_benchmark_v2.property_type = 'apartment'::text) AS rent_m2
           FROM gold_ch.core_benchmark_v2
          WHERE core_benchmark_v2.level = 'commune'::text
          GROUP BY core_benchmark_v2.commune_bfs
        ), pop AS (
         SELECT market_population.bfs_commune_number AS bfs,
            max(market_population.year) AS pop_year
           FROM silver_ch.market_population
          WHERE market_population.total_population IS NOT NULL
          GROUP BY market_population.bfs_commune_number
        )
 SELECT rc.canonical_bfs AS commune_bfs,
    regexp_replace(rc.canonical_name, '\s*\([A-Z]{2}\)\s*$'::text, ''::text) AS commune_name,
    rc.canton_code,
    rc.canton_name,
    rc.npa_list,
    st_transform(rc.geometry, 4326)::geometry(MultiPolygon,4326) AS geometry,
    st_pointonsurface(st_transform(rc.geometry, 4326)) AS centroid,
    COALESCE(rc.area_km2, round((st_area(rc.geometry) / '1000000'::numeric::double precision)::numeric, 3)) AS area_km2,
    rc.population,
        CASE
            WHEN rc.population IS NOT NULL THEN pop.pop_year
            ELSE NULL::integer
        END AS pop_year,
    r.tx_n::bigint AS transaction_count,
    r.lst_n::integer AS listing_count,
    r.sad_n::bigint AS sad_count,
    r.tx_n > 0::numeric AS has_transactions,
    r.lst_n > 0::numeric AS has_listings,
    r.sad_n > 0::numeric AS has_sads,
    round(b.sale_m2, 2) AS benchmark_sale_price_m2,
    round(b.rent_m2, 2) AS benchmark_rent_m2,
    now() AS updated_at,
    'ch'::character(2) AS country_code,
    rc.canton_code AS admin1_code,
    rc.canton_name AS admin1_name,
    NULL::text AS admin2_code,
    NULL::text AS admin2_name,
    rc.canonical_bfs::text AS admin3_code,
    regexp_replace(rc.canonical_name, '\s*\([A-Z]{2}\)\s*$'::text, ''::text) AS admin3_name,
    rc.canonical_bfs::text AS admin3_canonical_id,
    rc.aliases,
    rc.appartenance,
    rc.is_sub_commune,
    rc.parent_canonical_bfs AS parent_commune_bfs
   FROM rc
     JOIN rolled r ON r.bfs = rc.canonical_bfs
     LEFT JOIN bench b ON b.commune_bfs = rc.canonical_bfs
     LEFT JOIN pop ON pop.bfs = rc.canonical_bfs;
CREATE UNIQUE INDEX core_communes_bfs_uidx ON gold_ch.core_communes USING btree (commune_bfs);
ALTER MATERIALIZED VIEW gold_ch.core_communes OWNER TO postgres;
GRANT INSERT ON gold_ch.core_communes TO service_role;
GRANT SELECT ON gold_ch.core_communes TO service_role;
GRANT UPDATE ON gold_ch.core_communes TO service_role;
GRANT DELETE ON gold_ch.core_communes TO service_role;
GRANT TRUNCATE ON gold_ch.core_communes TO service_role;
GRANT REFERENCES ON gold_ch.core_communes TO service_role;
GRANT TRIGGER ON gold_ch.core_communes TO service_role;
GRANT MAINTAIN ON gold_ch.core_communes TO service_role;

CREATE VIEW gold_ch.v_benchmark_v2_full AS
 SELECT level,
    commune_bfs,
    commune_name,
    sous_secteur_nom,
    street_name,
    commune_bfs_key,
    sous_secteur_key,
    street_key,
    canton_code,
    property_type,
    deal_type,
    blended_price_m2,
    sources,
    source_count,
    n_sales,
    geo_basis,
    has_transaction_leg,
    is_full_stack,
    confidence,
    price_source,
    computed_at
   FROM gold_ch.core_benchmark_v2;
ALTER VIEW gold_ch.v_benchmark_v2_full OWNER TO postgres;
GRANT INSERT ON gold_ch.v_benchmark_v2_full TO service_role;
GRANT SELECT ON gold_ch.v_benchmark_v2_full TO service_role;
GRANT UPDATE ON gold_ch.v_benchmark_v2_full TO service_role;
GRANT DELETE ON gold_ch.v_benchmark_v2_full TO service_role;
GRANT TRUNCATE ON gold_ch.v_benchmark_v2_full TO service_role;
GRANT REFERENCES ON gold_ch.v_benchmark_v2_full TO service_role;
GRANT TRIGGER ON gold_ch.v_benchmark_v2_full TO service_role;
GRANT MAINTAIN ON gold_ch.v_benchmark_v2_full TO service_role;

CREATE VIEW gold_ch.v_communes_full AS
 SELECT commune_bfs,
    commune_name,
    canton_code,
    canton_name,
    npa_list,
    geometry,
    centroid,
    area_km2,
    population,
    pop_year,
    transaction_count,
    listing_count,
    sad_count,
    has_transactions,
    has_listings,
    has_sads,
    benchmark_sale_price_m2,
    benchmark_rent_m2,
    updated_at,
    country_code,
    admin1_code,
    admin1_name,
    admin2_code,
    admin2_name,
    admin3_code,
    admin3_name,
    admin3_canonical_id,
    aliases,
    appartenance,
    is_sub_commune,
    parent_commune_bfs
   FROM gold_ch.core_communes;
ALTER VIEW gold_ch.v_communes_full OWNER TO postgres;
GRANT INSERT ON gold_ch.v_communes_full TO service_role;
GRANT SELECT ON gold_ch.v_communes_full TO service_role;
GRANT UPDATE ON gold_ch.v_communes_full TO service_role;
GRANT DELETE ON gold_ch.v_communes_full TO service_role;
GRANT TRUNCATE ON gold_ch.v_communes_full TO service_role;
GRANT REFERENCES ON gold_ch.v_communes_full TO service_role;
GRANT TRIGGER ON gold_ch.v_communes_full TO service_role;
GRANT MAINTAIN ON gold_ch.v_communes_full TO service_role;

COMMIT;