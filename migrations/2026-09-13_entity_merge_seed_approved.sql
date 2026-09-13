-- ============================================================================
-- 2026-09-13 — Seed silver_ch.entity_merge_decisions with the approved company merges
-- ============================================================================
-- Operator approved "apply the 118 company merges", then required the result to be
-- durable ("build it bulletproof first"). Of the 118 candidates, 109 pass the strict
-- evidence rule below and are seeded ACTIVE; 9 are seeded as REVIEW (not enforced).
--
-- RULE firm_name_unique_uid_v1 — all must hold:
--   * the stripped name ends in a legal form whose firm name must be unique across
--     Switzerland (CO art. 951: SA/AG, Sàrl/GmbH, cooperative) — so a different
--     domicile means a seat move or deed-time domicile, not a second company;
--   * the stripped name matches the registered entity name exactly (case/accent-insensitive);
--   * the canonical entity carries a registered UID (CHE-…);
--   * exactly ONE UID-bearing entity has that name. (Name-keyed land-registry shadows
--     without a UID and with zero transactions do not count as competitors — e.g. AYOM SA,
--     NIC SA, Cronos Finance SA each have one.)
-- Domicile-vs-seat was deliberately NOT used: ref_communes keeps fused communes and their
-- former parts as separate BFS numbers, so it flags Bulle vs La Tour-de-Trême as different.
--
-- canonical_name = the spelling the entity's existing transactions already use (majority),
-- so the name-text seller filter collapses onto the existing entry.
-- ============================================================================

INSERT INTO silver_ch.entity_merge_decisions
  (alias_name, alias_key, canonical_name, canonical_entity_id, domicile, scope, rule, evidence, status, decided_by)
WITH spell AS (
  SELECT entity_id, party_name,
         row_number() OVER (PARTITION BY entity_id ORDER BY count(*) DESC, party_name) AS rk
  FROM silver_ch.event_transaction_parties
  WHERE party_name NOT LIKE '%,%' AND entity_id IS NOT NULL
  GROUP BY entity_id, party_name
),
uidn AS (
  SELECT name_normalized, count(*) AS uid_entities
  FROM silver_ch.entity_master
  WHERE coalesce(ide_uid,'') <> '' AND entity_type IN ('company','foundation','association','government')
  GROUP BY 1
),
v AS (
  SELECT t.*, s.party_name AS majority_spelling,
         coalesce(u.uid_entities, 0) AS uid_entities,
         btrim(regexp_replace(btrim(regexp_replace(t.alias_name, '^.*,', '')), '^(à|au|aux|in|im|von|zu|bei)\s+', '', 'i')) AS domicile_text
  FROM backup._t1_verdicts_20260913 t
  LEFT JOIN spell s ON s.entity_id = t.canonical_entity_id AND s.rk = 1
  LEFT JOIN silver_ch.entity_master em ON em.entity_id = t.canonical_entity_id
  LEFT JOIN uidn u ON u.name_normalized = em.name_normalized
)
SELECT
  v.alias_name,
  lower(public.unaccent('public.unaccent'::regdictionary, btrim(v.alias_name))),
  coalesce(v.majority_spelling, v.canonical_head),
  v.canonical_entity_id,
  nullif(v.domicile_text, ''),
  v.src,
  'firm_name_unique_uid_v1',
  jsonb_build_object(
    'entity_name', v.entity_name, 'uid', v.uid, 'entity_seat', v.head_office,
    'unique_legal_form', v.unique_form, 'exact_name', v.exact_name,
    'uid_bearing_entities_with_name', v.uid_entities, 'all_entities_with_name', v.same_name_entities,
    'party_rows', v.rows, 'source_candidates', 'backup._t1_verdicts_20260913'),
  CASE WHEN v.entity_name IS NOT NULL
        AND coalesce(v.unique_form, false)
        AND coalesce(v.ide_uid, '') <> ''
        AND coalesce(v.exact_name, false)
        AND v.uid_entities = 1
       THEN 'active' ELSE 'review' END,
  'claude-code — operator-approved 2026-09-13 ("apply the 118 company merges", "build it bulletproof first")'
FROM (
  -- Case variants of one alias ("XXL Box Sàrl, …" / "XXL BOX Sàrl, …") share one alias_key.
  -- Matching is case/accent-insensitive, so ONE decision covers every spelling.
  SELECT DISTINCT ON (lower(public.unaccent('public.unaccent'::regdictionary, btrim(alias_name))), src) *
  FROM v
  ORDER BY lower(public.unaccent('public.unaccent'::regdictionary, btrim(alias_name))), src, rows DESC, alias_name
) v;
