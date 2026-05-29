-- ============================================================================
-- Geneva sub-quartier canonicalization — GOLD migration (CREATE-OR-REPLACE-VIEW)
-- ============================================================================
-- Target DB: re-LLM (project znrvddgmczdqoucmykij)
-- Scope    : REPLACE gold_ch.v_transactions_full
--
-- Purpose
--   Policy (platform.standards rule_key data_use_canonical_commune): for the
--   City of Geneva, always resolve to one of the four cadastral sub-quartiers
--   when the section is known, falling back to commune Genève (BFS 6621) only
--   when it is not.
--
--     Section (commune_number) | Canonical BFS | Name
--     ------------------------ | ------------- | ---------------------
--     21                       | 66211         | Genève-Cité
--     22                       | 66212         | Genève-Eaux-Vives
--     23                       | 66213         | Genève-Petit-Saconnex
--     24                       | 66214         | Genève-Plainpalais
--
--   The previous definition derived commune_canonical_bfs purely by name-joining
--   core_transactions to silver_ch.ref_city_lookup, which mapped "Genève" -> 6621
--   and discarded commune_number. This adds a section-aware override join to
--   silver_ch.ref_communes (parent_canonical_bfs = 6621, no_commune = section).
--
-- Effect when applied (verified live 2026-05-28):
--   23,908 city rows moved 6621 -> 66211..66214; 0 already-correct rows disturbed;
--   0 non-Geneva rows touched (119,465 total). Sub-quartier counts:
--   66211=4575, 66212=7356, 66213=7301, 66214=5769.
--
-- Gotcha (cast safety):
--   commune_number can be multi-section text like '16, 48'. The ::int cast MUST
--   be guarded inside the CASE so the planner cannot reorder it ahead of the
--   regex predicate and error on non-numeric values.
--
-- This file captures the already-live definition so the repo does not drift from
-- the DB. CREATE OR REPLACE preserves column name/type/order — non-destructive.
-- ============================================================================

BEGIN;

CREATE OR REPLACE VIEW gold_ch.v_transactions_full AS
 SELECT t.transaction_id, t.affaire_number, t.source_id, t.transaction_date,
    t.publication_date, t.transaction_type, t.transaction_type_list, t.price,
    t.commune, t.commune_number, t.canton_code, t.plot_references, t.egrids,
    t.primary_egrid, t.plot_surface_m2, t.price_per_m2, t.ppe_indicator,
    t.ppe_pourmille_raw, t.buildings_raw, t.transaction_text, t.buyer_count,
    t.seller_count, t.buyers_display, t.sellers_display, t.buyer_names,
    t.seller_names, t.buyer_entity_ids, t.seller_entity_ids, t.property_type,
    t.surface_m2, t.previous_transaction_date, t.address, t.is_ldtr,
    t.ldtr_units, t.source_table, t.fts_transaction, t.updated_at,
    COALESCE(sub.canonical_bfs, cl.canonical_bfs) AS commune_canonical_bfs,
    'ch'::character(2) AS country_code,
    cl.canton_code AS admin1_code, cl.canton_name AS admin1_name,
    NULL::text AS admin2_code, NULL::text AS admin2_name,
    COALESCE(sub.canonical_bfs, cl.canonical_bfs)::text AS admin3_code,
    COALESCE(sub.canonical_name, cl.canonical_name)   AS admin3_name,
    COALESCE(sub.canonical_bfs, cl.canonical_bfs)::text AS admin3_canonical_id
   FROM gold_ch.core_transactions t
     LEFT JOIN silver_ch.ref_city_lookup cl
            ON cl.city_norm = silver_ch.unaccent_lower(lower(t.commune))
           AND (t.canton_code IS NULL OR cl.canton_code = t.canton_code)
     LEFT JOIN silver_ch.ref_communes sub
            ON cl.canonical_bfs = 6621
           AND sub.parent_canonical_bfs = 6621
           AND sub.no_commune = CASE WHEN t.commune_number ~ '^[0-9]+$'
                                     THEN t.commune_number::int END;

COMMIT;
