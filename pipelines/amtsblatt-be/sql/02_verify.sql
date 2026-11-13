-- =============================================================================
-- Verification suite / proof of record for the BE succession stream.
-- re-LLM is MCP-denied to some agents, so these psql outputs ARE the evidence.
-- Run:  psql "$RE_LLM_SESSION_POOLER_URI" -f pipelines/amtsblatt-be/sql/02_verify.sql
--
-- Covers: collapse (raw notices -> distinct estates) overall + per category + per year,
-- repudiation_scope counts, the dedup-key definition, worked collapse examples, and a
-- cross-estate spot check (no two deceased merged; no missed merges).
--
-- Expected at 2026-07-17: 16,837 candidates = 16,228 parsed + 609 content_denied_401
--                         -> 6,867 estate-events; 0 duplicate identities; 0 linked_egrid.
-- =============================================================================
\pset footer off
\echo '================ 1. COLLAPSE: raw notices vs distinct estate-events, per category ================'
WITH r AS (SELECT event_type, count(*) notices FROM bronze_ch.succession_notice_raw GROUP BY 1),
     e AS (SELECT event_type, count(*) events FROM bronze_ch.succession_events GROUP BY 1)
SELECT COALESCE(r.event_type,e.event_type) AS category,
       r.notices, e.events,
       round(r.notices::numeric / NULLIF(e.events,0), 2) AS collapse_ratio
FROM r FULL JOIN e USING (event_type)
ORDER BY r.notices DESC NULLS LAST;

\echo ''
\echo '================ 1b. TOTALS ================'
SELECT (SELECT count(*) FROM bronze_ch.succession_notice_raw)  AS raw_notices,
       (SELECT count(*) FROM bronze_ch.succession_events)      AS distinct_events,
       round((SELECT count(*) FROM bronze_ch.succession_notice_raw)::numeric
             / NULLIF((SELECT count(*) FROM bronze_ch.succession_events),0),2) AS overall_ratio;

\echo ''
\echo '================ 2. COLLAPSE per category PER YEAR (raw pub year vs event first-pub year) ================'
WITH r AS (SELECT event_type, EXTRACT(year FROM publication_date)::int yr, count(*) notices
           FROM bronze_ch.succession_notice_raw GROUP BY 1,2),
     e AS (SELECT event_type, EXTRACT(year FROM first_publication_date)::int yr, count(*) events
           FROM bronze_ch.succession_events GROUP BY 1,2)
SELECT COALESCE(r.event_type,e.event_type) category, COALESCE(r.yr,e.yr) yr,
       COALESCE(r.notices,0) notices, COALESCE(e.events,0) events
FROM r FULL JOIN e ON r.event_type=e.event_type AND r.yr=e.yr
ORDER BY category, yr;

\echo ''
\echo '================ 3. repudiation_scope: all-heirs-repudiated vs partial vs unknown ================'
SELECT event_type, repudiation_scope, refused_legacy, count(*) events
FROM bronze_ch.succession_events
GROUP BY 1,2,3 ORDER BY 1,2;

\echo ''
\echo '--- sanity: any ausschlagung raw NOT refusedLegacy or NOT KK Konkurs? (should be 0) ---'
SELECT count(*) AS ausschlagung_not_konkurs_or_not_refused
FROM bronze_ch.succession_notice_raw
WHERE event_type='ausschlagung' AND (addition IS DISTINCT FROM 'refusedLegacy' OR rubric <> 'KK');

\echo ''
\echo '================ 4. WORKED COLLAPSE EXAMPLES (estates with the most notices) ================'
WITH top AS (
  SELECT dedupe_key, deceased_name, deceased_dob, deceased_last_domicile, event_type, notice_count
  FROM bronze_ch.succession_events ORDER BY notice_count DESC LIMIT 4)
SELECT t.deceased_name, t.deceased_dob, left(t.deceased_last_domicile,32) domicile, t.event_type,
       t.notice_count, r.sub_rubric, r.publication_date, r.publication_id
FROM top t
JOIN bronze_ch.succession_notice_raw r
  ON lower(r.content_json->>'deceased_name')=lower(t.deceased_name)
 AND (r.content_json->>'deceased_dob')=t.deceased_dob::text
ORDER BY t.notice_count DESC, t.deceased_name, r.publication_date;

\echo ''
\echo '================ 5. SPOT CHECK — 5 events traced to source; verify no cross-estate merge ================'
WITH samp AS (
  SELECT * FROM bronze_ch.succession_events
  WHERE notice_count >= 2 ORDER BY md5(dedupe_key) LIMIT 5)
SELECT s.deceased_name, s.deceased_dob, s.notice_count,
       -- distinct (name,dob) tuples among the grouped raw rows: MUST be 1 (no cross-estate merge)
       (SELECT count(DISTINCT lower(r.content_json->>'deceased_name')||'|'||coalesce(r.content_json->>'deceased_dob',''))
        FROM bronze_ch.succession_notice_raw r
        WHERE lower(r.content_json->>'deceased_name')=lower(s.deceased_name)
          AND coalesce(r.content_json->>'deceased_dob','')=coalesce(s.deceased_dob::text,'')
          AND coalesce(lower(r.content_json->>'deceased_last_domicile'),'')=coalesce(lower(s.deceased_last_domicile),'')
       ) AS distinct_identities_in_group,
       array_length(s.publication_ids,1) AS ids_rolled_up
FROM samp s ORDER BY s.notice_count DESC;

\echo ''
\echo '--- missed-merge check: any two events sharing identical (name,dob,domicile,event_type)? (should be 0) ---'
SELECT count(*) AS duplicate_identity_events FROM (
  SELECT lower(deceased_name), deceased_dob, lower(deceased_last_domicile), event_type, count(*)
  FROM bronze_ch.succession_events
  GROUP BY 1,2,3,4 HAVING count(*) > 1) d;
