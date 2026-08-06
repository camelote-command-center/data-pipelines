-- ============================================================================
-- FFP_LISIERES_FORESTIERES.PARCELLES — free-text parcel list, normalised
-- ============================================================================
-- PARCELLES is a String(200) free-text field listing the parcels a forest
-- boundary procedure affects. It is messy in ways worth recording, because the
-- rules below were derived from the live data, not guessed. Every pattern here
-- was measured against all 1'315 rows on 2026-08-06.
--
-- SEPARATORS observed:  ;  ,  /  +  :  -  .  whitespace  ' et '  ' en '
--   The hyphen matters and is easy to miss: '449-855-1178-1580' is four
--   parcels, not one identifier. Geneva parcel numbers are plain integers, so
--   treating '-' as a separator is safe.
--
-- DOMAINE PUBLIC prefixes, all seen in the wild and all meaning the same thing:
--   'dp6412', 'dp 6392', 'DDP1844', 'DDP 15936', 'dpco 2585',
--   'DP cantonal 2754', '6331 (dp communal)'
--   These are normalised to DP<digits> BEFORE splitting, so that splitting on
--   whitespace cannot separate the prefix from its number and silently demote
--   a domaine-public parcel to an ordinary one.
--
-- PARENTHETICALS are stripped before parsing and kept in annotation. They carry
--   history rather than current parcels: '(ancien 7593-7592)', '(dp communal)',
--   '( DDP 2500)'. Parsing '(ancien ...)' as current parcels would attach a
--   forest procedure to parcels it no longer affects.
--
-- SENTINELS are values that name no parcel at all: 'voir liste', 'diverses',
--   'voir cartouche plan', 'Voir plan', 'voir ci-dessous', 'Selon liste',
--   'Divers Secteurs', 'mutation'. They are KEPT with parse_status='sentinel'
--   rather than dropped, so a reader can see the procedure exists and that its
--   parcel list has to be read off the plan.
--
-- NOTHING IS DROPPED. Every token lands with a parse_status:
--   parsed | sentinel | unparseable | absent
-- Measured outcome over 2'738 tokens: 2'298 plain parcels, 138 domaine public,
-- 1 with a letter suffix, 218 sentinel, 22 unparseable (99.2% classified).
-- The 22 are orphan words left by annotations ('Secteurs' from 'Divers
-- Secteurs', 'Zone'/'B' from '=Zone B'), not parcel numbers.
--
-- COMMUNE is inherited from the parent lisiere row and is NULL unless that row
-- resolved to exactly one commune. See 03_silver.sql for why.
-- ============================================================================

DROP MATERIALIZED VIEW IF EXISTS silver_ch.cadastral_forest_lisieres_parcelles CASCADE;

CREATE MATERIALIZED VIEW silver_ch.cadastral_forest_lisieres_parcelles AS
WITH base AS (
  SELECT
    l.id_dossier_key,
    l.geom_hash,
    l.id_dossier,
    l.parcelles_raw,
    l.commune_bfs,
    l.commune_resolution,
    -- Keep what the parentheses said, then take them out of the parse.
    NULLIF(btrim(array_to_string(
      ARRAY(SELECT (regexp_matches(coalesce(l.parcelles_raw, ''), '\(([^)]*)\)', 'g'))[1]),
      '; ')), '')                                                    AS annotation,
    regexp_replace(
      regexp_replace(coalesce(l.parcelles_raw, ''), '\([^)]*\)', ' ', 'g'),
      '(d\.?d\.?p|dp)\s*(co|cantonal|communal)?\s*([0-9]+)', 'DP\3', 'gi'
    )                                                                AS normalised
  FROM silver_ch.cadastral_forest_lisieres l
),
split AS (
  SELECT
    b.*,
    t.ord,
    btrim(regexp_replace(t.tok, '^[^0-9A-Za-z]+|[^0-9A-Za-z]+$', '', 'g')) AS token
  FROM base b
  LEFT JOIN LATERAL regexp_split_to_table(
      regexp_replace(b.normalised, '\s+(et|en)\s+', '|', 'gi'),
      '[|,;/+:\-.[:space:]]+'
    ) WITH ORDINALITY AS t(tok, ord) ON true
)
SELECT
  s.id_dossier_key,
  s.geom_hash,
  s.id_dossier,
  s.ord                                                              AS token_ordinal,
  s.parcelles_raw,
  s.annotation,
  NULLIF(s.token, '')                                                AS token,
  CASE
    WHEN s.token ~ '^DP[0-9]+$'       THEN substring(s.token from 3)::integer
    WHEN s.token ~ '^[0-9]+$'         THEN s.token::integer
    WHEN s.token ~ '^[0-9]+[A-Za-z]$' THEN substring(s.token from '^[0-9]+')::integer
    ELSE NULL
  END                                                                AS no_parcelle,
  CASE WHEN s.token ~ '^[0-9]+[A-Za-z]$'
       THEN substring(s.token from '[A-Za-z]$') END                  AS parcelle_suffix,
  (s.token ~ '^DP[0-9]+$')                                           AS is_domaine_public,
  s.commune_bfs                                                      AS no_commune,
  s.commune_resolution,
  CASE
    WHEN coalesce(btrim(s.parcelles_raw), '') = ''                   THEN 'absent'
    WHEN s.token IS NULL OR s.token = ''                             THEN 'absent'
    WHEN s.token ~ '^DP[0-9]+$'
      OR s.token ~ '^[0-9]+$'
      OR s.token ~ '^[0-9]+[A-Za-z]$'                                THEN 'parsed'
    WHEN s.token ~* '^(voir.*|divers.*|selon.*|liste|dessous|mutation|cartouche|plan|ci)$'
                                                                     THEN 'sentinel'
    ELSE 'unparseable'
  END                                                                AS parse_status,
  'GE'::text                                                         AS canton_code,
  now()                                                              AS updated_at
FROM split s;

-- Unique key for REFRESH ... CONCURRENTLY. token_ordinal disambiguates repeated
-- tokens inside one PARCELLES string.
CREATE UNIQUE INDEX cadastral_forest_lisieres_parcelles_pk
  ON silver_ch.cadastral_forest_lisieres_parcelles (id_dossier_key, geom_hash, token_ordinal);
CREATE INDEX cadastral_forest_lisieres_parcelles_no_parcelle_idx
  ON silver_ch.cadastral_forest_lisieres_parcelles (no_commune, no_parcelle)
  WHERE no_parcelle IS NOT NULL;
CREATE INDEX cadastral_forest_lisieres_parcelles_dossier_idx
  ON silver_ch.cadastral_forest_lisieres_parcelles (id_dossier);
CREATE INDEX cadastral_forest_lisieres_parcelles_status_idx
  ON silver_ch.cadastral_forest_lisieres_parcelles (parse_status);

-- ---------------------------------------------------------------------------
-- Parse-rate report (verification item 6)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver_ch.v_ge_forest_parcelles_audit AS
SELECT
  parse_status,
  count(*)                                     AS tokens,
  count(DISTINCT id_dossier)                   AS dossiers,
  round(100.0 * count(*) / NULLIF(sum(count(*)) OVER (), 0), 2) AS pct
FROM silver_ch.cadastral_forest_lisieres_parcelles
GROUP BY parse_status
ORDER BY tokens DESC;
