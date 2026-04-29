-- Remove the silver-side regex fallback CTE branch from
-- silver_ch.event_transactions.fao_plots (added 2026-04-29 ~13:00 UTC as a
-- consumer-side workaround while bronze coverage was 2%).
--
-- Bronze is now canonical: bronze_ch.fao_transactions.buildings carries the
-- recovered parcel data (66.30% coverage, up from 2.22%) thanks to the
-- pure-SQL backfill committed earlier today (commit 5a73664). The silver-side
-- regex is now redundant — it was catching the same patterns now persisted
-- in bronze, plus a handful of edge cases.
--
-- This script:
--   1. Drops silver_ch.event_transactions WITH CASCADE (drops
--      silver_ch.link_plot_transactions and gold_ch.core_transactions too).
--   2. Recreates silver_ch.event_transactions WITHOUT the UNION ALL regex
--      branch in the fao_plots CTE.
--   3. Recreates silver_ch.link_plot_transactions and gold_ch.core_transactions
--      verbatim from their pre-existing definitions.
--   4. Recreates all 17 indexes that existed across the three matviews.
--   5. REFRESHes the chain.
--
-- Verification: silver/gold plot coverage must stay ≥66,000 rows. If it drops
-- below 66K, bronze was missing cases the regex was catching — restore the
-- regex branch and investigate.
--
-- Rollback: re-add the UNION ALL branch from the pre-change matview definition
-- (saved at the top of this commit's diff in the data-pipelines repo).

BEGIN;

DROP MATERIALIZED VIEW silver_ch.event_transactions CASCADE;

CREATE MATERIALIZED VIEW silver_ch.event_transactions AS
 WITH ldtr_agg AS (
         SELECT COALESCE(f.affaire_number, f.source_id::character varying) AS fao_key,
            jsonb_agg(DISTINCT jsonb_build_object('permit_number', l.affaire, 'lot_key', l.lot_key, 'address', l.address, 'buyer', replace(replace(l.acheteur::text, '<u>'::text, ''::text), '</u>'::text, ''::text), 'seller', replace(replace(l.vendeur::text, '<u>'::text, ''::text), '</u>'::text, ''::text), 'price', l.prix)) AS ldtr_units
           FROM bronze_ch.fao_ldtr l
             JOIN bronze_ch.fao_transactions f ON l.commune::text = f.commune::text AND l.transaction_date = f.date_of_transaction::date
          GROUP BY (COALESCE(f.affaire_number, f.source_id::character varying))
        ), fao_parties AS (
         SELECT f.id,
            string_agg(DISTINCT b.elem ->> 'name'::text, ', '::text) AS buyers_display,
            string_agg(DISTINCT s.elem ->> 'name'::text, ', '::text) AS sellers_display,
            count(DISTINCT b.elem ->> 'name'::text) AS buyer_count,
            count(DISTINCT s.elem ->> 'name'::text) AS seller_count
           FROM bronze_ch.fao_transactions f
             LEFT JOIN LATERAL jsonb_array_elements(f.new_owner_s) b(elem) ON true
             LEFT JOIN LATERAL jsonb_array_elements(f.old_owner_s) s(elem) ON true
          GROUP BY f.id
        ), fao_plots AS (
         SELECT f.id,
            array_agg(DISTINCT b.elem ->> 'building_id'::text) FILTER (WHERE (b.elem ->> 'building_id'::text) IS NOT NULL) AS plot_references
           FROM bronze_ch.fao_transactions f,
            LATERAL jsonb_array_elements(f.buildings::jsonb) b(elem)
          WHERE f.buildings IS NOT NULL AND f.buildings::text <> ''::text
          GROUP BY f.id
        ), fao AS (
         SELECT COALESCE(f.affaire_number, f.source_id::character varying) AS transaction_id,
            f.affaire_number,
            f.source_id,
            f.date_of_transaction::date AS transaction_date,
            f.fao_publication_date::date AS publication_date,
            lower(TRIM(BOTH FROM f.type_of_transaction)) AS transaction_type,
                CASE
                    WHEN f.type_clean_list IS NOT NULL THEN ARRAY( SELECT jsonb_array_elements_text(f.type_clean_list) AS jsonb_array_elements_text)
                    ELSE NULL::text[]
                END AS transaction_type_list,
                CASE
                    WHEN f.price IS NOT NULL AND f.price::text <> ''::text THEN replace(f.price::text, ','::text, '.'::text)::numeric
                    ELSE NULL::numeric
                END AS price,
            NULLIF(TRIM(BOTH FROM f.commune), ''::text) AS commune,
            NULLIF(TRIM(BOTH FROM f.commune_number), ''::text) AS commune_number,
            'GE'::text AS canton_code,
            fp.plot_references,
                CASE
                    WHEN f.ppe_indicator::text = 'Yes'::text THEN true
                    WHEN f.ppe_indicator::text = 'No'::text THEN false
                    ELSE NULL::boolean
                END AS ppe_indicator,
            NULLIF(TRIM(BOTH FROM f.cumulative_pourmille_ppe), ''::text) AS ppe_pourmille_raw,
                CASE
                    WHEN f.buildings IS NOT NULL AND f.buildings::text <> ''::text THEN f.buildings::jsonb
                    ELSE NULL::jsonb
                END AS buildings_raw,
            NULLIF(TRIM(BOTH FROM f.transaction), ''::text) AS transaction_text,
            COALESCE(fp2.buyer_count, 0::bigint)::integer AS buyer_count,
            COALESCE(fp2.seller_count, 0::bigint)::integer AS seller_count,
            fp2.buyers_display,
            fp2.sellers_display,
            NULL::text AS property_type,
            NULL::numeric AS surface_m2,
            NULL::numeric AS price_per_m2,
            NULL::date AS previous_transaction_date,
            NULL::text AS address,
            la.ldtr_units IS NOT NULL AS is_ldtr,
            la.ldtr_units,
            'fao_transactions'::text AS source_table,
            now() AS updated_at
           FROM bronze_ch.fao_transactions f
             LEFT JOIN fao_parties fp2 ON f.id = fp2.id
             LEFT JOIN fao_plots fp ON f.id = fp.id
             LEFT JOIN ldtr_agg la ON COALESCE(f.affaire_number, f.source_id::character varying)::text = la.fao_key::text
        ), "national" AS (
         SELECT DISTINCT ON (transactions_national.source_id) 'nat:'::text || transactions_national.source_id AS transaction_id,
            NULL::text AS affaire_number,
            transactions_national.source_id,
            transactions_national.transaction_date,
            NULL::date AS publication_date,
            lower(TRIM(BOTH FROM transactions_national.reason)) AS transaction_type,
            NULL::text[] AS transaction_type_list,
            transactions_national.price,
            NULL::text AS commune,
            NULL::text AS commune_number,
            transactions_national.canton AS canton_code,
            NULL::text[] AS plot_references,
            NULL::boolean AS ppe_indicator,
            NULL::text AS ppe_pourmille_raw,
            NULL::jsonb AS buildings_raw,
            NULL::text AS transaction_text,
            COALESCE(transactions_national.nb_buyers, 0) AS buyer_count,
            COALESCE(transactions_national.nb_sellers, 0) AS seller_count,
            NULLIF(TRIM(BOTH FROM transactions_national.buyers), ''::text) AS buyers_display,
            NULLIF(TRIM(BOTH FROM transactions_national.sellers), ''::text) AS sellers_display,
            NULLIF(TRIM(BOTH FROM transactions_national.property_type), ''::text) AS property_type,
            transactions_national.surface_m2,
            transactions_national.price_per_m2,
            transactions_national.previous_transaction_date,
            NULLIF(TRIM(BOTH FROM transactions_national.address), ''::text) AS address,
            false AS is_ldtr,
            NULL::jsonb AS ldtr_units,
            'transactions_national'::text AS source_table,
            now() AS updated_at
           FROM bronze_ch.transactions_national
          ORDER BY transactions_national.source_id, transactions_national.id
        ), unmatched_ldtr AS (
         SELECT 'ldtr:'::text || l.affaire::text AS transaction_id,
            NULL::text AS affaire_number,
            NULL::text AS source_id,
            l.transaction_date,
            l.date_de_parution_au_rf AS publication_date,
            'vente ldtr'::text AS transaction_type,
            ARRAY['Vente LDTR'::text] AS transaction_type_list,
                CASE
                    WHEN l.prix IS NOT NULL AND l.prix::text <> ''::text THEN replace(l.prix::text, ','::text, '.'::text)::numeric
                    ELSE NULL::numeric
                END AS price,
            NULLIF(TRIM(BOTH FROM l.commune), ''::text) AS commune,
            NULL::text AS commune_number,
            'GE'::text AS canton_code,
            NULL::text[] AS plot_references,
            NULL::boolean AS ppe_indicator,
            NULL::text AS ppe_pourmille_raw,
            NULL::jsonb AS buildings_raw,
            NULLIF(TRIM(BOTH FROM l.transaction), ''::text) AS transaction_text,
            jsonb_array_length(COALESCE(l.acheteur_list, '[]'::jsonb)) AS buyer_count,
            jsonb_array_length(COALESCE(l.vendeur_list, '[]'::jsonb)) AS seller_count,
            replace(replace(NULLIF(TRIM(BOTH FROM l.acheteur), ''::text), '<u>'::text, ''::text), '</u>'::text, ''::text) AS buyers_display,
            replace(replace(NULLIF(TRIM(BOTH FROM l.vendeur), ''::text), '<u>'::text, ''::text), '</u>'::text, ''::text) AS sellers_display,
            NULL::text AS property_type,
            NULL::numeric AS surface_m2,
            NULL::numeric AS price_per_m2,
            NULL::date AS previous_transaction_date,
            NULLIF(TRIM(BOTH FROM l.address), ''::text) AS address,
            true AS is_ldtr,
            jsonb_build_array(jsonb_build_object('permit_number', l.affaire, 'lot_key', l.lot_key, 'address', l.address, 'buyer', replace(replace(l.acheteur::text, '<u>'::text, ''::text), '</u>'::text, ''::text), 'seller', replace(replace(l.vendeur::text, '<u>'::text, ''::text), '</u>'::text, ''::text), 'price', l.prix)) AS ldtr_units,
            'fao_ldtr'::text AS source_table,
            now() AS updated_at
           FROM bronze_ch.fao_ldtr l
          WHERE NOT (EXISTS ( SELECT 1
                   FROM bronze_ch.fao_transactions f
                  WHERE l.commune::text = f.commune::text AND l.transaction_date = f.date_of_transaction::date))
        )
 SELECT fao.transaction_id,
    fao.affaire_number,
    fao.source_id,
    fao.transaction_date,
    fao.publication_date,
    fao.transaction_type,
    fao.transaction_type_list,
    fao.price,
    fao.commune,
    fao.commune_number,
    fao.canton_code,
    fao.plot_references,
    fao.ppe_indicator,
    fao.ppe_pourmille_raw,
    fao.buildings_raw,
    fao.transaction_text,
    fao.buyer_count,
    fao.seller_count,
    fao.buyers_display,
    fao.sellers_display,
    fao.property_type,
    fao.surface_m2,
    fao.price_per_m2,
    fao.previous_transaction_date,
    fao.address,
    fao.is_ldtr,
    fao.ldtr_units,
    fao.source_table,
    fao.updated_at
   FROM fao
UNION ALL
 SELECT "national".transaction_id,
    "national".affaire_number,
    "national".source_id,
    "national".transaction_date,
    "national".publication_date,
    "national".transaction_type,
    "national".transaction_type_list,
    "national".price,
    "national".commune,
    "national".commune_number,
    "national".canton_code,
    "national".plot_references,
    "national".ppe_indicator,
    "national".ppe_pourmille_raw,
    "national".buildings_raw,
    "national".transaction_text,
    "national".buyer_count,
    "national".seller_count,
    "national".buyers_display,
    "national".sellers_display,
    "national".property_type,
    "national".surface_m2,
    "national".price_per_m2,
    "national".previous_transaction_date,
    "national".address,
    "national".is_ldtr,
    "national".ldtr_units,
    "national".source_table,
    "national".updated_at
   FROM "national"
UNION ALL
 SELECT unmatched_ldtr.transaction_id,
    unmatched_ldtr.affaire_number,
    unmatched_ldtr.source_id,
    unmatched_ldtr.transaction_date,
    unmatched_ldtr.publication_date,
    unmatched_ldtr.transaction_type,
    unmatched_ldtr.transaction_type_list,
    unmatched_ldtr.price,
    unmatched_ldtr.commune,
    unmatched_ldtr.commune_number,
    unmatched_ldtr.canton_code,
    unmatched_ldtr.plot_references,
    unmatched_ldtr.ppe_indicator,
    unmatched_ldtr.ppe_pourmille_raw,
    unmatched_ldtr.buildings_raw,
    unmatched_ldtr.transaction_text,
    unmatched_ldtr.buyer_count,
    unmatched_ldtr.seller_count,
    unmatched_ldtr.buyers_display,
    unmatched_ldtr.sellers_display,
    unmatched_ldtr.property_type,
    unmatched_ldtr.surface_m2,
    unmatched_ldtr.price_per_m2,
    unmatched_ldtr.previous_transaction_date,
    unmatched_ldtr.address,
    unmatched_ldtr.is_ldtr,
    unmatched_ldtr.ldtr_units,
    unmatched_ldtr.source_table,
    unmatched_ldtr.updated_at
   FROM unmatched_ldtr;;

CREATE MATERIALIZED VIEW silver_ch.link_plot_transactions AS
 SELECT cp.egrid,
    t.transaction_id,
    ref.plot_ref AS no_commune_no_parcelle
   FROM silver_ch.event_transactions t,
    LATERAL unnest(t.plot_references) ref(plot_ref)
     JOIN silver_ch.cadastral_plots cp ON cp.no_commune_no_parcelle = ref.plot_ref
  WHERE t.plot_references IS NOT NULL AND array_length(t.plot_references, 1) > 0;;

CREATE MATERIALIZED VIEW gold_ch.core_transactions AS
 WITH party_agg AS (
         SELECT event_transaction_parties.transaction_id,
            array_agg(event_transaction_parties.party_name ORDER BY event_transaction_parties.party_name) FILTER (WHERE event_transaction_parties.party_role = 'buyer'::text) AS buyer_names,
            array_agg(event_transaction_parties.party_name ORDER BY event_transaction_parties.party_name) FILTER (WHERE event_transaction_parties.party_role = 'seller'::text) AS seller_names,
            array_agg(DISTINCT event_transaction_parties.entity_id) FILTER (WHERE event_transaction_parties.party_role = 'buyer'::text AND event_transaction_parties.entity_id IS NOT NULL) AS buyer_entity_ids,
            array_agg(DISTINCT event_transaction_parties.entity_id) FILTER (WHERE event_transaction_parties.party_role = 'seller'::text AND event_transaction_parties.entity_id IS NOT NULL) AS seller_entity_ids
           FROM silver_ch.event_transaction_parties
          GROUP BY event_transaction_parties.transaction_id
        ), plot_match AS (
         SELECT t_1.transaction_id,
            p.egrid,
            p.surface_m2 AS plot_surface_m2,
            p.no_commune_no_parcelle
           FROM silver_ch.event_transactions t_1,
            LATERAL unnest(t_1.plot_references) ref(plot_ref)
             JOIN silver_ch.cadastral_plots p ON p.no_commune_no_parcelle = ref.plot_ref AND p.canton_code = 'GE'::text
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
    t.plot_references,
    pe.egrids,
    pf.egrid AS primary_egrid,
    pf.plot_surface_m2,
        CASE
            WHEN t.price IS NOT NULL AND t.price > 0::numeric AND COALESCE(t.surface_m2, pf.plot_surface_m2) IS NOT NULL AND COALESCE(t.surface_m2, pf.plot_surface_m2) > 0::numeric THEN round(t.price / COALESCE(t.surface_m2, pf.plot_surface_m2), 2)
            ELSE t.price_per_m2
        END AS price_per_m2,
    t.ppe_indicator,
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
    t.property_type,
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
     LEFT JOIN plot_egrids pe ON pe.transaction_id::text = t.transaction_id::text;;

-- Indexes on silver_ch.event_transactions
CREATE UNIQUE INDEX idx_event_transactions_id ON silver_ch.event_transactions USING btree (transaction_id);
CREATE INDEX idx_event_transactions_canton_commune ON silver_ch.event_transactions USING btree (canton_code, commune);
CREATE INDEX idx_event_transactions_date ON silver_ch.event_transactions USING btree (transaction_date);
CREATE INDEX idx_event_transactions_plots ON silver_ch.event_transactions USING gin (plot_references);
CREATE INDEX idx_event_transactions_type ON silver_ch.event_transactions USING btree (transaction_type);

-- Indexes on silver_ch.link_plot_transactions
CREATE UNIQUE INDEX idx_link_plot_tx_uniq ON silver_ch.link_plot_transactions USING btree (egrid, transaction_id);
CREATE INDEX idx_link_plot_tx_egrid ON silver_ch.link_plot_transactions USING btree (egrid);
CREATE INDEX idx_link_plot_tx_txid ON silver_ch.link_plot_transactions USING btree (transaction_id);

-- Indexes on gold_ch.core_transactions
CREATE UNIQUE INDEX idx_core_txn_id ON gold_ch.core_transactions USING btree (transaction_id);
CREATE INDEX idx_core_txn_canton_commune ON gold_ch.core_transactions USING btree (canton_code, commune);
CREATE INDEX idx_core_txn_date ON gold_ch.core_transactions USING btree (transaction_date);
CREATE INDEX idx_core_txn_egrid ON gold_ch.core_transactions USING btree (primary_egrid) WHERE (primary_egrid IS NOT NULL);
CREATE INDEX idx_core_txn_type ON gold_ch.core_transactions USING btree (transaction_type);
CREATE INDEX idx_core_txn_buyer_entities ON gold_ch.core_transactions USING gin (buyer_entity_ids) WHERE (buyer_entity_ids IS NOT NULL);
CREATE INDEX idx_core_txn_seller_entities ON gold_ch.core_transactions USING gin (seller_entity_ids) WHERE (seller_entity_ids IS NOT NULL);
CREATE INDEX idx_core_txn_plots ON gold_ch.core_transactions USING gin (plot_references);
CREATE INDEX idx_core_txn_fts ON gold_ch.core_transactions USING gin (fts_transaction);

COMMIT;
