-- 2026-09-17 — Person entity dedup by date of birth (RE-LLM)
--
-- Why: duplicate person entities (same person under name variants, e.g. an
-- extra given name, hyphen/accent differences, or seeded twice from the land
-- registry and the Lamap lift) keep the owner resolver ambiguous.
--
-- Measured before writing this (active persons, 138,047 parseable names):
--   15,070 pairs with compatible names (same family name, one given-name set
--   contained in the other). 1,826 have both dates of birth: 468 equal,
--   1,358 different (mostly 15+ years apart: parent/child namesakes, not
--   typos). Names alone are therefore NOT a merge signal.
--   Address evidence was tested and rejected as a merge signal:
--     * persons carry no address in entities_person_extra / RF head_office;
--     * FAO street addresses exist for only 3,426 person entities and are
--       shared by 3 candidate pairs;
--     * sharing an owned plot is unsafe: identical-name co-owners with both
--       dates known have DIFFERENT dates 189 times vs 120 equal.
--
-- Rule (only this is applied):
--   two ACTIVE person entities are the same person when
--     family keys are equal (silver_ch.fn_person_name_parts) AND
--     one given-name set is contained in the other AND
--     both dates of birth are known and equal.
--   Connected pairs form a cluster; a cluster is applied only when every
--   member pair is name-compatible (no "Hans Peter" + "Hans Rudolf" chains).
--   Canonical member: ge_registre_foncier > ge_ddp_owners > lamap_lift > other,
--   then most given names, then oldest created_at.
--
-- Effect (non-destructive; nothing is deleted):
--   * silver_ch.entity_matches: one row per member -> canonical
--     (source 'dob_rule_20260917').
--   * silver_ch.entities: cluster_id = canonical id on every member;
--     non-canonical members status 'merged', metadata.merged_into.
--   * References re-pointed merged -> canonical: plot_owner_overrides
--     (new_entity_id, old_entity_id), transaction_party_links.resolved_entity_id,
--     party_name_resolution_cache.resolved_entity_id, plot_owners_lifted.entity_id.
--   * populate_plot_owner_overrides / populate_transaction_party_links:
--     name-based "attach created entity" step follows a merged entity to its
--     canonical id instead of attaching the merged id.
--   Not changed: link_plot_owners (matview over source registry ids),
--   entity_relationships, entity_crosswalk_lamap, gold_ch.core_entities
--   (merged entities stay listed downstream until consumers honour status).
--
-- Rollback (restores everything this file changed):
--   BEGIN;
--   UPDATE silver_ch.entities e SET status=b.status, cluster_id=b.cluster_id, metadata=b.metadata, updated_at=now()
--     FROM backup.person_dedup_entities_20260917 b WHERE b.entity_id=e.entity_id;
--   UPDATE silver_ch.plot_owner_overrides o SET new_entity_id=b.new_entity_id, old_entity_id=b.old_entity_id
--     FROM backup.person_dedup_overrides_20260917 b WHERE b.egrid=o.egrid AND b.new_owner_name=o.new_owner_name;
--   UPDATE silver_ch.transaction_party_links t SET resolved_entity_id=b.resolved_entity_id
--     FROM backup.person_dedup_party_links_20260917 b
--     WHERE (b.transaction_id,b.party_role,b.party_index,b.parse_idx)=(t.transaction_id,t.party_role,t.party_index,t.parse_idx);
--   UPDATE silver_ch.party_name_resolution_cache c SET resolved_entity_id=b.resolved_entity_id
--     FROM backup.person_dedup_name_cache_20260917 b WHERE b.cache_key=c.cache_key;
--   UPDATE silver_ch.plot_owners_lifted l SET entity_id=b.entity_id
--     FROM backup.person_dedup_lifted_20260917 b WHERE b.id=l.id;
--   -- entity_matches rows: leave as history or mark: UPDATE silver_ch.entity_matches SET reason=reason||' [reverted]' WHERE source='dob_rule_20260917';
--   -- functions: re-create from backup.fn_defs_20260913 fn '<name>@pre-person-dedup'.
--   COMMIT;

BEGIN;
SET LOCAL statement_timeout = '900s';

INSERT INTO backup.fn_defs_20260913 (fn, md5, definition)
SELECT p.proname || '@pre-person-dedup', md5(pg_get_functiondef(p.oid)), pg_get_functiondef(p.oid)
FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'silver_ch' AND p.proname IN ('populate_plot_owner_overrides', 'populate_transaction_party_links')
  AND NOT EXISTS (SELECT 1 FROM backup.fn_defs_20260913 b WHERE b.fn = p.proname || '@pre-person-dedup');

-- 1) candidate persons with a date of birth
CREATE TEMP TABLE _p ON COMMIT DROP AS
SELECT e.entity_id, e.canonical_name, e.created_at, e.source_tables[1] AS src,
       (silver_ch.fn_person_name_parts(e.canonical_name)).*, x.date_of_birth AS dob
FROM silver_ch.entities e
JOIN silver_ch.entities_person_extra x USING (entity_id)
WHERE e.entity_type = 'person' AND e.status = 'active' AND x.date_of_birth IS NOT NULL;
DELETE FROM _p WHERE family_key IS NULL OR family_key = '' OR cardinality(given) = 0;
CREATE INDEX ON _p (family_key, dob);

-- 2) same-person edges
CREATE TEMP TABLE _edge ON COMMIT DROP AS
SELECT a.entity_id AS a_id, b.entity_id AS b_id
FROM _p a JOIN _p b
  ON a.family_key = b.family_key AND a.dob = b.dob AND a.entity_id < b.entity_id
 AND (a.given <@ b.given OR b.given <@ a.given);

-- 3) connected components (label = smallest member id)
CREATE TEMP TABLE _lab ON COMMIT DROP AS
SELECT id AS entity_id, id AS lab FROM (SELECT a_id AS id FROM _edge UNION SELECT b_id FROM _edge) s;
DO $$
DECLARE n int;
BEGIN
  LOOP
    WITH nb AS (
      SELECT l.entity_id, min(l2.lab::text)::uuid AS m
      FROM _lab l
      JOIN _edge e ON e.a_id = l.entity_id OR e.b_id = l.entity_id
      JOIN _lab l2 ON l2.entity_id = CASE WHEN e.a_id = l.entity_id THEN e.b_id ELSE e.a_id END
      GROUP BY l.entity_id)
    UPDATE _lab l SET lab = nb.m FROM nb WHERE nb.entity_id = l.entity_id AND nb.m < l.lab;
    GET DIAGNOSTICS n = ROW_COUNT;
    EXIT WHEN n = 0;
  END LOOP;
END $$;

-- 4) keep clusters whose members are all pairwise name-compatible; pick canonical
CREATE TEMP TABLE _cluster ON COMMIT DROP AS
WITH m AS (SELECT l.lab, p.* FROM _lab l JOIN _p p USING (entity_id)),
bad AS (
  SELECT DISTINCT a.lab FROM m a JOIN m b ON a.lab = b.lab AND a.entity_id < b.entity_id
  WHERE NOT (a.given <@ b.given OR b.given <@ a.given)),
ranked AS (
  SELECT m.*, row_number() OVER (PARTITION BY m.lab ORDER BY
           CASE m.src WHEN 'ge_registre_foncier' THEN 0 WHEN 'ge_ddp_owners' THEN 1 WHEN 'lamap_lift' THEN 2 ELSE 3 END,
           cardinality(m.given) DESC, m.created_at, m.entity_id) AS rk
  FROM m WHERE m.lab NOT IN (SELECT lab FROM bad))
SELECT r.entity_id, c.entity_id AS canonical_id, r.canonical_name, c.canonical_name AS canonical_name_target, r.dob, r.src
FROM ranked r JOIN ranked c ON c.lab = r.lab AND c.rk = 1;

CREATE TEMP TABLE _map ON COMMIT DROP AS
SELECT entity_id AS merged_id, canonical_id FROM _cluster WHERE entity_id <> canonical_id;

-- 5) backups of every row touched
CREATE TABLE backup.person_dedup_entities_20260917 AS
SELECT e.* FROM silver_ch.entities e WHERE e.entity_id IN (SELECT entity_id FROM _cluster);
CREATE TABLE backup.person_dedup_overrides_20260917 AS
SELECT o.* FROM silver_ch.plot_owner_overrides o
WHERE o.new_entity_id IN (SELECT merged_id FROM _map) OR o.old_entity_id IN (SELECT merged_id FROM _map);
CREATE TABLE backup.person_dedup_party_links_20260917 AS
SELECT t.* FROM silver_ch.transaction_party_links t WHERE t.resolved_entity_id IN (SELECT merged_id FROM _map);
CREATE TABLE backup.person_dedup_name_cache_20260917 AS
SELECT c.* FROM silver_ch.party_name_resolution_cache c WHERE c.resolved_entity_id IN (SELECT merged_id FROM _map);
CREATE TABLE backup.person_dedup_lifted_20260917 AS
SELECT l.* FROM silver_ch.plot_owners_lifted l WHERE l.entity_id IN (SELECT merged_id FROM _map);

-- 6) decision records
INSERT INTO silver_ch.entity_matches (entity_a_id, entity_b_id, cluster_id, confidence, reason, evidence, source, reviewer, reviewed_at)
SELECT c.entity_id, c.canonical_id, c.canonical_id, 0.990, 'same_date_of_birth_compatible_names',
       jsonb_build_object('date_of_birth', c.dob, 'merged_name', c.canonical_name, 'canonical_name', c.canonical_name_target,
                          'merged_source', c.src, 'rule', 'family_key equal + given-name subset + equal DOB'),
       'dob_rule_20260917', 'claude-code', now()
FROM _cluster c WHERE c.entity_id <> c.canonical_id
ON CONFLICT DO NOTHING;

-- 7) entities
UPDATE silver_ch.entities e SET cluster_id = c.canonical_id, updated_at = now()
FROM _cluster c WHERE c.entity_id = e.entity_id AND c.entity_id = c.canonical_id;
UPDATE silver_ch.entities e
SET cluster_id = m.canonical_id, status = 'merged', updated_at = now(),
    metadata = coalesce(e.metadata, '{}'::jsonb) || jsonb_build_object('merged_into', m.canonical_id, 'merge_rule', 'dob_rule_20260917')
FROM _map m WHERE m.merged_id = e.entity_id;

-- 8) re-point references
UPDATE silver_ch.plot_owner_overrides o SET new_entity_id = m.canonical_id, updated_at = now()
FROM _map m WHERE o.new_entity_id = m.merged_id;
UPDATE silver_ch.plot_owner_overrides o SET old_entity_id = m.canonical_id, updated_at = now()
FROM _map m WHERE o.old_entity_id = m.merged_id;
UPDATE silver_ch.transaction_party_links t SET resolved_entity_id = m.canonical_id, updated_at = now()
FROM _map m WHERE t.resolved_entity_id = m.merged_id;
UPDATE silver_ch.party_name_resolution_cache c SET resolved_entity_id = m.canonical_id
FROM _map m WHERE c.resolved_entity_id = m.merged_id;
UPDATE silver_ch.plot_owners_lifted l SET entity_id = m.canonical_id
FROM _map m WHERE l.entity_id = m.merged_id;

-- 9) attach steps follow merges
DO $$
DECLARE d text; d2 text;
BEGIN
  d := pg_get_functiondef('silver_ch.populate_plot_owner_overrides'::regproc);
  d2 := replace(d, 'UPDATE _resolved r SET resolved_entity_id = e.entity_id',
                   'UPDATE _resolved r SET resolved_entity_id = CASE WHEN e.status = ''merged'' THEN e.cluster_id ELSE e.entity_id END');
  IF d2 = d THEN RAISE EXCEPTION 'populate_plot_owner_overrides attach step not found'; END IF;
  EXECUTE d2;

  d := pg_get_functiondef('silver_ch.populate_transaction_party_links'::regproc);
  d2 := replace(d, 'SET decision = ''created'', resolved_entity_id = e.entity_id',
                   'SET decision = ''created'', resolved_entity_id = CASE WHEN e.status = ''merged'' THEN e.cluster_id ELSE e.entity_id END');
  IF d2 = d THEN RAISE EXCEPTION 'populate_transaction_party_links attach step not found'; END IF;
  EXECUTE d2;
END $$;

SELECT (SELECT count(*) FROM _edge) AS edges,
       (SELECT count(DISTINCT canonical_id) FROM _cluster) AS clusters,
       (SELECT count(*) FROM _map) AS merged_entities,
       (SELECT count(DISTINCT l.lab) FROM _lab l) - (SELECT count(DISTINCT canonical_id) FROM _cluster) AS clusters_skipped_incompatible,
       (SELECT count(*) FROM backup.person_dedup_overrides_20260917) AS overrides_repointed,
       (SELECT count(*) FROM backup.person_dedup_party_links_20260917) AS party_links_repointed,
       (SELECT count(*) FROM backup.person_dedup_name_cache_20260917) AS cache_repointed,
       (SELECT count(*) FROM backup.person_dedup_lifted_20260917) AS lifted_repointed;

COMMIT;
