-- ============================================================================
-- 2026-09-13 — Entity merge authority (re-LLM)
-- ============================================================================
-- WHY THIS EXISTS
-- Entity merges have been applied at least five times, in five different places
-- (re-LLM resolver cache/links, re-LLM lift_merge_review, lamap_db
-- entities_canonical, lamap_db lbi_entity_aliases_v1 companies and persons).
-- Every one was downstream and lived in a single consumer, so:
--   * a merge on lamap_db never reached lamap-lbi, rousseau_5 or foncier;
--   * re-LLM kept regenerating the fragmented names every night (the resolver
--     even mints a NEW entity for a glued name such as "AYOM SA, CHENE-BOUGERIES");
--   * nothing checked that a merge still held, so merges decayed silently.
-- And the UI seller filter groups by NAME TEXT (core_transactions seller_names /
-- parties_sellers.name come straight from etp.party_name), so unifying entity_id
-- alone would not collapse the filter.
--
-- THE CONTRACT
--   1. ONE authority, upstream: silver_ch.entity_merge_decisions.
--   2. Decisions are enforced on the SOURCE name (bronze), so display AND
--      resolution follow, and every consumer inherits it through normal
--      distribution. No matview is redefined (event_transaction_parties has 11
--      dependents including core_transactions and the GE pricing stack).
--   3. Enforcement is idempotent and re-runs before every matview refresh, so a
--      re-ingest that reverts a name is healed on the next refresh.
--   4. Nothing is ever deleted. Decisions are revoked, not removed; a revocation
--      restores the exact pre-image recorded in entity_merge_applications.
--   5. A watchdog (separate migration) verifies every active decision in re-LLM
--      and in every consumer, and alerts the Command Center when one does not hold.
--
-- ROLLBACK: see bottom. Never DROP ... CASCADE.
-- ============================================================================

BEGIN;
SET LOCAL lock_timeout = '30s';

-- ── 1. The authority ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver_ch.entity_merge_decisions (
  id                   bigserial PRIMARY KEY,
  alias_name           text        NOT NULL,   -- fragmented name exactly as stored, e.g. 'AYOM SA, CHENE-BOUGERIES'
  alias_key            text        NOT NULL,   -- lower(unaccent(btrim(alias_name))) — the match key
  canonical_name       text        NOT NULL,   -- name written instead: the majority spelling already used for the entity
  canonical_entity_id  uuid        NOT NULL,   -- registered entity the name must resolve to
  domicile             text,                   -- locality split off the alias; moved into city only where a city field exists and is empty
  scope                text        NOT NULL CHECK (scope IN ('fao_transactions','transactions_national')),
  rule                 text        NOT NULL,
  evidence             jsonb       NOT NULL DEFAULT '{}'::jsonb,
  status               text        NOT NULL DEFAULT 'active' CHECK (status IN ('active','review','revoked')),
  decided_by           text        NOT NULL,
  decided_at           timestamptz NOT NULL DEFAULT now(),
  revoked_at           timestamptz,
  revoked_by           text,
  revoked_reason       text,
  CONSTRAINT emd_revoked_consistent CHECK ((status = 'revoked') = (revoked_at IS NOT NULL)),
  -- 2-arg unaccent pins the dictionary: the 1-arg form resolves it via search_path and
  -- would fail for an INSERT from a session whose search_path lacks public.
  CONSTRAINT emd_key_matches_alias  CHECK (alias_key = lower(public.unaccent('public.unaccent'::regdictionary, btrim(alias_name)))),
  CONSTRAINT emd_no_self_alias      CHECK (alias_key <> lower(public.unaccent('public.unaccent'::regdictionary, btrim(canonical_name))))
);
-- One active decision per alias per scope. Review/revoked rows may repeat.
CREATE UNIQUE INDEX IF NOT EXISTS entity_merge_decisions_active_uq
  ON silver_ch.entity_merge_decisions (alias_key, scope) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS entity_merge_decisions_entity_idx
  ON silver_ch.entity_merge_decisions (canonical_entity_id);

COMMENT ON TABLE silver_ch.entity_merge_decisions IS
  'THE single authority for entity merges (2026-09-13). Every merge is recorded here once, enforced on the bronze '
  'name by silver_ch.apply_entity_merge_decisions() before each matview refresh, and verified in re-LLM and every '
  'consumer by gold_ch.entity_merge_watchdog_run(). Rows are never deleted: revoke via '
  'silver_ch.revoke_entity_merge_decision(). Do NOT add merges in consumer databases.';

-- ── 2. Audit log of every change the enforcement made (enables exact revert) ─
CREATE TABLE IF NOT EXISTS silver_ch.entity_merge_applications (
  id              bigserial PRIMARY KEY,
  decision_id     bigint      NOT NULL REFERENCES silver_ch.entity_merge_decisions(id),
  run_id          uuid        NOT NULL,
  target_table    text        NOT NULL,     -- bronze_ch.fao_transactions | bronze_ch.transactions_national
  target_row_id   bigint      NOT NULL,     -- bronze primary key
  target_field    text        NOT NULL,     -- old_owner_s | new_owner_s | buyers | sellers
  element_index   int,                      -- 1-based position inside the owner array (FAO only)
  before_value    jsonb       NOT NULL,
  after_value     jsonb       NOT NULL,
  applied_at      timestamptz NOT NULL DEFAULT now(),
  reverted_at     timestamptz
);
CREATE INDEX IF NOT EXISTS entity_merge_applications_decision_idx ON silver_ch.entity_merge_applications (decision_id);
CREATE INDEX IF NOT EXISTS entity_merge_applications_target_idx   ON silver_ch.entity_merge_applications (target_table, target_row_id);

-- ── 3. Structural NO-DELETE guarantee on both tables ────────────────────────
CREATE OR REPLACE FUNCTION silver_ch.entity_merge_forbid_removal()
RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
  RAISE EXCEPTION '% on %.% is forbidden: entity merge records are never removed. Revoke with silver_ch.revoke_entity_merge_decision(id, by, reason).',
    TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME;
END $fn$;

DROP TRIGGER IF EXISTS emd_no_delete   ON silver_ch.entity_merge_decisions;
DROP TRIGGER IF EXISTS emd_no_truncate ON silver_ch.entity_merge_decisions;
DROP TRIGGER IF EXISTS ema_no_delete   ON silver_ch.entity_merge_applications;
DROP TRIGGER IF EXISTS ema_no_truncate ON silver_ch.entity_merge_applications;
CREATE TRIGGER emd_no_delete   BEFORE DELETE   ON silver_ch.entity_merge_decisions    FOR EACH ROW       EXECUTE FUNCTION silver_ch.entity_merge_forbid_removal();
CREATE TRIGGER emd_no_truncate BEFORE TRUNCATE ON silver_ch.entity_merge_decisions    FOR EACH STATEMENT EXECUTE FUNCTION silver_ch.entity_merge_forbid_removal();
CREATE TRIGGER ema_no_delete   BEFORE DELETE   ON silver_ch.entity_merge_applications FOR EACH ROW       EXECUTE FUNCTION silver_ch.entity_merge_forbid_removal();
CREATE TRIGGER ema_no_truncate BEFORE TRUNCATE ON silver_ch.entity_merge_applications FOR EACH STATEMENT EXECUTE FUNCTION silver_ch.entity_merge_forbid_removal();

-- ── 4. Enforcement: idempotent, source-level ────────────────────────────────
CREATE OR REPLACE FUNCTION silver_ch.apply_entity_merge_decisions()
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'silver_ch', 'bronze_ch', 'public'
AS $fn$
DECLARE
  v_run   uuid := gen_random_uuid();
  v_fao   int  := 0;
  v_nat   int  := 0;
  r       record;
  v_new   jsonb;
BEGIN
  -- FAO: rewrite matching owner elements inside old_owner_s / new_owner_s.
  --   name     -> canonical_name
  --   city     -> domicile, ONLY when city is null/empty (never overwrite an address)
  --   name_raw -> the original string, kept inside the element for audit
  FOR r IN
    WITH d AS (
      SELECT id, alias_key, canonical_name, domicile
      FROM silver_ch.entity_merge_decisions
      WHERE status = 'active' AND scope = 'fao_transactions'
    )
    SELECT f.id AS row_id, s.side, s.arr,
           array_agg(DISTINCT d.id) AS decision_ids
    FROM bronze_ch.fao_transactions f
    CROSS JOIN LATERAL (VALUES ('old_owner_s', f.old_owner_s), ('new_owner_s', f.new_owner_s)) s(side, arr)
    CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(s.arr) = 'array' THEN s.arr ELSE '[]'::jsonb END) e(elem)
    JOIN d ON d.alias_key = lower(public.unaccent(btrim(e.elem->>'name')))
    GROUP BY f.id, s.side, s.arr
  LOOP
    -- rebuild the array, element by element, logging each change
    SELECT jsonb_agg(
             CASE WHEN d.id IS NULL THEN x.elem
                  ELSE (x.elem - 'name' - 'city')
                       || jsonb_build_object(
                            'name', d.canonical_name,
                            'city', CASE WHEN nullif(btrim(coalesce(x.elem->>'city','')),'') IS NULL
                                         THEN to_jsonb(d.domicile) ELSE x.elem->'city' END,
                            'name_raw', coalesce(x.elem->'name_raw', x.elem->'name'))
             END ORDER BY x.ord)
      INTO v_new
    FROM jsonb_array_elements(r.arr) WITH ORDINALITY x(elem, ord)
    LEFT JOIN silver_ch.entity_merge_decisions d
      ON d.status = 'active' AND d.scope = 'fao_transactions'
     AND d.alias_key = lower(public.unaccent(btrim(x.elem->>'name')));

    INSERT INTO silver_ch.entity_merge_applications
      (decision_id, run_id, target_table, target_row_id, target_field, element_index, before_value, after_value)
    SELECT d.id, v_run, 'bronze_ch.fao_transactions', r.row_id, r.side, x.ord::int, x.elem, v_new -> (x.ord::int - 1)
    FROM jsonb_array_elements(r.arr) WITH ORDINALITY x(elem, ord)
    JOIN silver_ch.entity_merge_decisions d
      ON d.status = 'active' AND d.scope = 'fao_transactions'
     AND d.alias_key = lower(public.unaccent(btrim(x.elem->>'name')));

    EXECUTE format('UPDATE bronze_ch.fao_transactions SET %I = $1, updated_at = now() WHERE id = $2', r.side)
      USING v_new, r.row_id;
  END LOOP;
  SELECT count(*) INTO v_fao FROM silver_ch.entity_merge_applications
   WHERE run_id = v_run AND target_table = 'bronze_ch.fao_transactions';

  -- National: the whole buyers/sellers string is one party in event_transaction_parties.
  -- ONE update per row: two UPDATE CTEs touching the same tuple in one statement would
  -- silently apply only one of them (a row whose buyer AND seller are both aliases).
  WITH d AS (
    SELECT id, alias_key, canonical_name FROM silver_ch.entity_merge_decisions
    WHERE status = 'active' AND scope = 'transactions_national'
  ),
  cand AS (
    SELECT n.id AS row_id, n.buyers, n.sellers,
           db.id AS b_dec, db.canonical_name AS b_new,
           ds.id AS s_dec, ds.canonical_name AS s_new
    FROM bronze_ch.transactions_national n
    LEFT JOIN d db ON db.alias_key = lower(public.unaccent(btrim(n.buyers)))
    LEFT JOIN d ds ON ds.alias_key = lower(public.unaccent(btrim(n.sellers)))
    WHERE db.id IS NOT NULL OR ds.id IS NOT NULL
  ),
  logged AS (
    INSERT INTO silver_ch.entity_merge_applications
      (decision_id, run_id, target_table, target_row_id, target_field, before_value, after_value)
    SELECT b_dec, v_run, 'bronze_ch.transactions_national', row_id, 'buyers',  to_jsonb(buyers),  to_jsonb(b_new) FROM cand WHERE b_dec IS NOT NULL
    UNION ALL
    SELECT s_dec, v_run, 'bronze_ch.transactions_national', row_id, 'sellers', to_jsonb(sellers), to_jsonb(s_new) FROM cand WHERE s_dec IS NOT NULL
    RETURNING 1
  ),
  upd AS (
    UPDATE bronze_ch.transactions_national n
       SET buyers  = coalesce(c.b_new, n.buyers),
           sellers = coalesce(c.s_new, n.sellers),
           updated_at = now()
      FROM cand c WHERE c.row_id = n.id
    RETURNING 1
  )
  SELECT (SELECT count(*) FROM logged) INTO v_nat;

  RETURN jsonb_build_object('run_id', v_run, 'fao_elements_rewritten', v_fao, 'national_fields_rewritten', v_nat,
                            'active_decisions', (SELECT count(*) FROM silver_ch.entity_merge_decisions WHERE status='active'));
END $fn$;

COMMENT ON FUNCTION silver_ch.apply_entity_merge_decisions() IS
  'Idempotent enforcement of silver_ch.entity_merge_decisions on the bronze source names. Runs before every matview '
  'refresh (pre-hook in gold_ch.refresh_daily_matviews), so a re-ingest that reverts a name is healed. Logs every change '
  'to silver_ch.entity_merge_applications. A second run with no new reversions is a no-op.';

-- ── 5. Revocation: restore the exact pre-image, never delete ───────────────
CREATE OR REPLACE FUNCTION silver_ch.revoke_entity_merge_decision(p_id bigint, p_by text, p_reason text)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'silver_ch', 'bronze_ch', 'public'
AS $fn$
DECLARE a record; v_restored int := 0; v_skipped int := 0; v_arr jsonb;
BEGIN
  IF coalesce(btrim(p_reason),'') = '' THEN RAISE EXCEPTION 'a revocation reason is required'; END IF;
  UPDATE silver_ch.entity_merge_decisions
     SET status='revoked', revoked_at=now(), revoked_by=p_by, revoked_reason=p_reason
   WHERE id = p_id AND status <> 'revoked';
  IF NOT FOUND THEN RAISE EXCEPTION 'decision % not found or already revoked', p_id; END IF;

  FOR a IN SELECT * FROM silver_ch.entity_merge_applications
            WHERE decision_id = p_id AND reverted_at IS NULL ORDER BY id DESC LOOP
    IF a.target_table = 'bronze_ch.fao_transactions' THEN
      EXECUTE format('SELECT %I FROM bronze_ch.fao_transactions WHERE id=$1', a.target_field) INTO v_arr USING a.target_row_id;
      -- restore only if the element is still what we wrote (someone may have changed it since)
      IF v_arr -> (a.element_index - 1) = a.after_value THEN
        EXECUTE format('UPDATE bronze_ch.fao_transactions SET %I = jsonb_set(%I, $1, $2), updated_at=now() WHERE id=$3',
                       a.target_field, a.target_field)
          USING ARRAY[(a.element_index - 1)::text], a.before_value, a.target_row_id;
        v_restored := v_restored + 1;
      ELSE v_skipped := v_skipped + 1; END IF;
    ELSE
      EXECUTE format('UPDATE bronze_ch.transactions_national SET %I = $1, updated_at=now() WHERE id=$2 AND %I = $3',
                     a.target_field, a.target_field)
        USING a.before_value #>> '{}', a.target_row_id, a.after_value #>> '{}';
      IF FOUND THEN v_restored := v_restored + 1; ELSE v_skipped := v_skipped + 1; END IF;
    END IF;
    UPDATE silver_ch.entity_merge_applications SET reverted_at = now() WHERE id = a.id;
  END LOOP;
  RETURN jsonb_build_object('decision_id', p_id, 'restored', v_restored, 'skipped_changed_since', v_skipped);
END $fn$;

COMMIT;

-- ============================================================================
-- ROLLBACK (only if the whole mechanism must go; revoke individual merges instead)
-- 1. revoke every active decision (restores all bronze pre-images):
--      SELECT silver_ch.revoke_entity_merge_decision(id, 'rollback', 'mechanism rollback')
--      FROM silver_ch.entity_merge_decisions WHERE status='active';
-- 2. remove the pre-hook from gold_ch.refresh_daily_matviews (restore the definition
--    with md5 04fbe530137c98973318ae3b1a81038d, kept in backup.fn_defs_20260913).
-- 3. the tables stay (audit history); disable by revoking decisions, not by DROP.
-- ============================================================================
