-- =============================================================================
-- Bern (+ generalizable) succession / ownerless-plot event stream
-- Source: amtsblattportal.ch OGD REST API (already ingested as metadata into
--         bronze_ch.amtsblatt_publications by pipelines/amtsblattportal).
-- This pipeline (pipelines/amtsblatt-be) is a DOWNSTREAM parse layer: it fetches
-- the per-publication *content* XML (which the list API omits) and distils it
-- into estate-grain events. It does NOT re-ingest metadata and does NOT touch
-- bronze_ch.amtsblatt_publications (owned by the sibling pipeline).
--
-- Scope (confirmed 2026-07-16):
--   * cantonal  tenant=kabbe rubric TE-BE : Erbenaufruf (TE-BE20, Art.555 ZGB),
--     Testamentseröffnung (TE-BE10), öff. Inventar (TE-BE60/70),
--     Erbschaft an Gemeinwesen (TE-BE90)
--   * federal   tenant=shab  rubric KK, addition=refusedLegacy, canton⊇BE :
--     ausgeschlagene Erbschaften in konkursamtlicher Liquidation
--
-- Rollback:
--   DROP FUNCTION IF EXISTS public.refresh_succession_candidates();
--   DROP MATERIALIZED VIEW IF EXISTS bronze_ch.succession_notice_candidates;
--   DROP TABLE IF EXISTS bronze_ch.succession_events;
--   DROP TABLE IF EXISTS bronze_ch.succession_notice_raw;
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS bronze_ch;

-- -----------------------------------------------------------------------------
-- Candidate matview: which already-ingested publications are succession notices.
-- MATERIALIZED because the shab arm regex-scans 2.4M rows — too slow for a
-- per-request PostgREST read; the scan runs once at refresh instead.
-- Canton-parameterized (bonus §8): the cantonal arm keys off the TE-<CC> rubric
-- family so another canton is added by extending the tenant/rubric patterns, not
-- by a rewrite. event_type here is a PROVISIONAL label; the TS parser confirms
-- ausschlagung vs liquidation from the content-level <addition> flag.
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS bronze_ch.succession_notice_candidates AS
WITH base AS (
  SELECT
    p.id,
    p.primary_tenant_code                                       AS tenant,
    p.rubric,
    p.sub_rubric,
    p.publication_date,
    p.language,
    COALESCE(p.title_de, p.title_fr, p.title_it, p.title_en)    AS title,
    p.cantons,
    -- canton: cantonal tenants encode it as kab<cc>; federal shab carries it in cantons[]
    CASE
      WHEN p.primary_tenant_code LIKE 'kab%' THEN upper(substring(p.primary_tenant_code from 4 for 2))
      ELSE NULL
    END                                                         AS tenant_canton
  FROM bronze_ch.amtsblatt_publications p
)
-- Cantonal succession family (TE-<CC>*)
SELECT
  b.id, b.tenant, b.tenant_canton AS canton, b.rubric, b.sub_rubric,
  b.publication_date, b.language, b.title,
  CASE b.sub_rubric
    WHEN 'TE-BE20' THEN 'erbenruf'
    WHEN 'TE-BE10' THEN 'testament'
    WHEN 'TE-BE60' THEN 'inventar'
    WHEN 'TE-BE70' THEN 'inventar'
    WHEN 'TE-BE90' THEN 'escheat'
    ELSE 'other'
  END AS event_type
FROM base b
WHERE b.tenant = 'kabbe'
  AND b.rubric = 'TE-BE'
  AND b.sub_rubric IN ('TE-BE10','TE-BE20','TE-BE60','TE-BE70','TE-BE90')

UNION ALL

-- Federal repudiated estates in liquidation (SHAB Konkurs, canton BE).
-- Pre-filtered on the title marker; the parser re-confirms via <addition>.
SELECT
  b.id, b.tenant, 'BE' AS canton, b.rubric, b.sub_rubric,
  b.publication_date, b.language, b.title,
  'ausschlagung' AS event_type
FROM base b
WHERE b.tenant = 'shab'
  AND b.rubric = 'KK'
  AND 'BE' = ANY(b.cantons)
  AND COALESCE(b.title, '') ~* '(ausgeschlagene?\s+erbschaft|succession\s+r[ée]pudi|eredit[àa]\s+.*rinunci)';

CREATE UNIQUE INDEX IF NOT EXISTS idx_succ_cand_id     ON bronze_ch.succession_notice_candidates (id);
CREATE INDEX        IF NOT EXISTS idx_succ_cand_canton ON bronze_ch.succession_notice_candidates (canton, publication_date DESC);

-- Refresh helper (SECURITY DEFINER + raised timeout so the 2.4M-row scan is not
-- killed by the caller-role statement_timeout). Called by the pipeline at start.
CREATE OR REPLACE FUNCTION public.refresh_succession_candidates()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET statement_timeout TO '180s'
AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY bronze_ch.succession_notice_candidates;
END;
$$;

-- -----------------------------------------------------------------------------
-- Raw content layer: one row per fetched publication (idempotent on id).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze_ch.succession_notice_raw (
  publication_id     UUID PRIMARY KEY,          -- amtsblatt meta.id
  tenant             TEXT NOT NULL,
  canton             TEXT NOT NULL,
  rubric             TEXT,
  sub_rubric         TEXT,
  event_type         TEXT NOT NULL,             -- confirmed by parser
  publication_date   DATE NOT NULL,
  language           TEXT,
  title              TEXT,
  source_url         TEXT NOT NULL,
  addition           TEXT,                      -- e.g. 'refusedLegacy'
  content_json       JSONB,                     -- parsed content fields
  raw_xml            TEXT,                      -- full XML payload (audit)
  parse_status       TEXT NOT NULL DEFAULT 'ok',
  fetched_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_succ_raw_canton_evt   ON bronze_ch.succession_notice_raw (canton, event_type);
CREATE INDEX IF NOT EXISTS idx_succ_raw_pubdate      ON bronze_ch.succession_notice_raw (publication_date DESC);
CREATE INDEX IF NOT EXISTS idx_succ_raw_tenant       ON bronze_ch.succession_notice_raw (tenant);

-- -----------------------------------------------------------------------------
-- Event layer: estate grain, deduped across an estate's republications.
-- Dedupe key (brief §5): (canton, event_type, deceased_name, deceased_dob, authority).
-- Rows are (re)built by aggregating succession_notice_raw — additive, never lossy.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze_ch.succession_events (
  id                        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  canton                    TEXT NOT NULL DEFAULT 'BE',
  event_type                TEXT NOT NULL,          -- erbenruf|ausschlagung|liquidation|dereliktion|escheat|testament|inventar|other
  repudiation_scope         TEXT NOT NULL DEFAULT 'not_applicable', -- konkursamtliche_liquidation | unknown | not_applicable
  deceased_name             TEXT,                   -- "prename surname"
  deceased_prename          TEXT,
  deceased_surname          TEXT,
  deceased_dob              DATE,
  deceased_dod              DATE,                   -- date of death (or detection)
  deceased_last_domicile    TEXT,
  deceased_heimatort        TEXT,                   -- placeOfOrigin
  authority                 TEXT,
  authority_municipality_id TEXT,
  deadline_date             DATE,                   -- entryDeadline (latest known)
  first_publication_date    DATE,
  latest_publication_date   DATE,
  publication_dates         DATE[],                 -- Erbenruf publishes 3×; KK across lifecycle
  languages                 TEXT[],
  rubric                    TEXT,
  sub_rubrics               TEXT[],                 -- all sub-rubrics seen for this estate
  refused_legacy            BOOLEAN NOT NULL DEFAULT false,
  publication_ids           UUID[],                 -- rolled-up raw publication ids
  source_urls               TEXT[],
  primary_source_url        TEXT,                   -- latest notice
  notice_count              INT NOT NULL DEFAULT 1,
  body_text                 TEXT,                   -- callInheritance/probate/arrangement (latest)
  linked_egrid              TEXT,                   -- NULLABLE — never fabricated; set only on a positive match
  dedupe_key                TEXT NOT NULL UNIQUE,
  created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_succ_evt_canton_type ON bronze_ch.succession_events (canton, event_type);
CREATE INDEX IF NOT EXISTS idx_succ_evt_surname     ON bronze_ch.succession_events (lower(deceased_surname));
CREATE INDEX IF NOT EXISTS idx_succ_evt_latest      ON bronze_ch.succession_events (latest_publication_date DESC);
CREATE INDEX IF NOT EXISTS idx_succ_evt_deadline    ON bronze_ch.succession_events (deadline_date);
-- idempotent for pre-existing installs (CREATE TABLE IF NOT EXISTS won't add columns)
ALTER TABLE bronze_ch.succession_events ADD COLUMN IF NOT EXISTS repudiation_scope TEXT NOT NULL DEFAULT 'not_applicable';

-- Alert feed: Erbenrufe + repudiated estates in konkursamtliche Liquidation,
-- recency-ordered. EXACT predicate: event_type='erbenruf' OR (event_type='ausschlagung'
-- AND repudiation_scope='konkursamtliche_liquidation'). It excludes testament/inventar/
-- escheat/unknown (119 of 6,867). It does NOT filter on domicile (only 10 events lack one).
-- ⚠️ NOT a verified all-heirs-repudiation list — see repudiation_scope docs.
-- linked_egrid is carried through NULL (parcel linking is a separate manual/LBI step).
CREATE OR REPLACE VIEW bronze_ch.succession_actionable AS
SELECT
  deceased_name, deceased_dob, deceased_last_domicile, deceased_heimatort,
  authority, event_type AS category, repudiation_scope,
  deceased_dod, deadline_date, first_publication_date, latest_publication_date,
  notice_count, sub_rubrics, linked_egrid, primary_source_url
FROM bronze_ch.succession_events
WHERE (event_type = 'erbenruf')
   OR (event_type = 'ausschlagung' AND repudiation_scope = 'konkursamtliche_liquidation')
ORDER BY latest_publication_date DESC NULLS LAST;

CREATE OR REPLACE FUNCTION bronze_ch._succession_touch()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = now(); RETURN NEW; END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_succession_touch ON bronze_ch.succession_events;
CREATE TRIGGER trg_succession_touch
  BEFORE UPDATE ON bronze_ch.succession_events
  FOR EACH ROW EXECUTE FUNCTION bronze_ch._succession_touch();
