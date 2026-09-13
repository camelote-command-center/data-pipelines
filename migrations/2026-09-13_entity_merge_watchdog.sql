-- ============================================================================
-- 2026-09-13 — Entity merge watchdog (re-LLM)
-- ============================================================================
-- The part that makes merges bulletproof: it does not assume a merge still holds,
-- it checks every night, in the warehouse AND in every consumer, and pushes a
-- Command Center alert when one does not.
--
-- Dedup prefix 'entity_merge:' is deliberately NOT in pixxels_data fn_ops_clear's
-- owned-prefix set, so fn_ops_collect() cannot auto-resolve these alerts (the
-- 2026-08-09 bug that silently wiped remote pushes). A human closes them.
--
-- Consumers are verified through the existing foreign tables, filtered by the
-- transaction ids the merges actually touched (pushed down by postgres_fdw), so the
-- check is exact and cheap. Every run is recorded in gold_ch.entity_merge_watchdog_runs,
-- clean runs included — "no alert" is provably "checked and fine", not "never ran".
-- ============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS gold_ch.entity_merge_watchdog_runs (
  id          bigserial PRIMARY KEY,
  run_at      timestamptz NOT NULL DEFAULT now(),
  status      text        NOT NULL CHECK (status IN ('clean','alerted','error')),
  findings    jsonb       NOT NULL,
  duration_ms int
);

CREATE OR REPLACE FUNCTION gold_ch.entity_merge_watchdog_run(p_lookback_hours int DEFAULT 48, p_push boolean DEFAULT true)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'gold_ch', 'silver_ch', 'bronze_ch', 'public'
AS $fn$
DECLARE
  v_t0        timestamptz := clock_timestamp();
  v_cam_key   text := (SELECT decrypted_secret FROM vault.decrypted_secrets WHERE name = 'sr_pixxels_data');
  v_rellm     uuid := 'c1fd061c-8e0f-4c82-97fc-24fce67a1571';  -- pixxels_data startups.id for RE-LLM
  v_findings  jsonb := '[]'::jsonb;
  v_touched   text[];
  v_n         int;
  v_samples   text[];
  f           jsonb;
  v_req       bigint;
  v_status    text;
  -- street/address words that are never a domicile even where a commune shares the name
  -- ("Rue" FR, "Port" BE) — must match STREET_WORDS in pipelines/transactions-fao/owners.ts
  v_street_words text[] := ARRAY['rue','port','route','rte','chemin','ch','avenue','av','place','pl','quai','boulevard','bd',
                                 'pont','ruelle','impasse','allee','promenade','sentier','strasse','weg','gasse','platz'];
BEGIN
  -- transaction ids (as event_transaction_parties / consumers key them) touched by active merges
  SELECT array_agg(DISTINCT tx) INTO v_touched FROM (
    SELECT coalesce(ft.affaire_number, ft.source_id::text) AS tx
    FROM silver_ch.entity_merge_applications a
    JOIN silver_ch.entity_merge_decisions d ON d.id = a.decision_id AND d.status = 'active'
    JOIN bronze_ch.fao_transactions ft ON ft.id = a.target_row_id
    WHERE a.target_table = 'bronze_ch.fao_transactions' AND a.reverted_at IS NULL
    UNION
    SELECT 'nat:' || n.source_id
    FROM silver_ch.entity_merge_applications a
    JOIN silver_ch.entity_merge_decisions d ON d.id = a.decision_id AND d.status = 'active'
    JOIN bronze_ch.transactions_national n ON n.id = a.target_row_id
    WHERE a.target_table = 'bronze_ch.transactions_national' AND a.reverted_at IS NULL
  ) x;
  v_touched := coalesce(v_touched, ARRAY[]::text[]);

  -- 1. CRITICAL — an active alias is still present in bronze (enforcement did not run / failed)
  SELECT count(*), (array_agg(DISTINCT nm))[1:5] INTO v_n, v_samples FROM (
    SELECT e->>'name' AS nm
    FROM bronze_ch.fao_transactions ft,
         jsonb_array_elements(coalesce(CASE WHEN jsonb_typeof(ft.old_owner_s)='array' THEN ft.old_owner_s END,'[]')
                            ||coalesce(CASE WHEN jsonb_typeof(ft.new_owner_s)='array' THEN ft.new_owner_s END,'[]')) e
    JOIN silver_ch.entity_merge_decisions d ON d.status='active' AND d.scope='fao_transactions'
     AND d.alias_key = lower(public.unaccent(btrim(e->>'name')))
    UNION ALL
    SELECT d.alias_name FROM bronze_ch.transactions_national n
    JOIN silver_ch.entity_merge_decisions d ON d.status='active' AND d.scope='transactions_national'
     AND d.alias_key IN (lower(public.unaccent(btrim(n.buyers))), lower(public.unaccent(btrim(n.sellers))))
  ) z;
  IF v_n > 0 THEN v_findings := v_findings || jsonb_build_object('check','not_enforced_bronze','severity','critical','n',v_n,'samples',v_samples,
    'title', format('%s merged alias(es) still present in bronze — enforcement did not run or failed', v_n)); END IF;

  -- 2. CRITICAL — an active alias still appears as a party in the warehouse
  SELECT count(*), (array_agg(DISTINCT p.party_name))[1:5] INTO v_n, v_samples
  FROM silver_ch.event_transaction_parties p
  JOIN silver_ch.entity_merge_decisions d ON d.status='active' AND d.alias_key = lower(public.unaccent(btrim(p.party_name)));
  IF v_n > 0 THEN v_findings := v_findings || jsonb_build_object('check','not_honoured_warehouse','severity','critical','n',v_n,'samples',v_samples,
    'title', format('%s party row(s) in event_transaction_parties still use a merged alias', v_n)); END IF;

  -- 3. CRITICAL — on the touched transactions, the canonical name resolves to the WRONG entity
  SELECT count(*), (array_agg(DISTINCT p.party_name || ' -> ' || coalesce(p.entity_id::text,'NULL')))[1:5] INTO v_n, v_samples
  FROM silver_ch.event_transaction_parties p
  JOIN silver_ch.entity_merge_decisions d
    ON d.status='active' AND lower(public.unaccent(btrim(d.canonical_name))) = lower(public.unaccent(btrim(p.party_name)))
  WHERE p.transaction_id::text = ANY (v_touched)
    AND p.entity_id IS DISTINCT FROM d.canonical_entity_id;
  IF v_n > 0 THEN v_findings := v_findings || jsonb_build_object('check','wrong_entity_warehouse','severity','critical','n',v_n,'samples',v_samples,
    'title', format('%s merged party row(s) resolve to the wrong entity', v_n)); END IF;

  -- 4. CRITICAL — the canonical entity of an active decision no longer exists
  SELECT count(*), (array_agg(d.canonical_name))[1:5] INTO v_n, v_samples
  FROM silver_ch.entity_merge_decisions d
  WHERE d.status='active' AND NOT EXISTS (SELECT 1 FROM silver_ch.entity_master em WHERE em.entity_id = d.canonical_entity_id);
  IF v_n > 0 THEN v_findings := v_findings || jsonb_build_object('check','canonical_entity_missing','severity','critical','n',v_n,'samples',v_samples,
    'title', format('%s active merge(s) point at an entity that no longer exists', v_n)); END IF;

  -- 5. CRITICAL — a consumer still shows a merged alias on a touched transaction
  IF array_length(v_touched, 1) > 0 THEN
    BEGIN
      SELECT count(*), (array_agg(DISTINCT nm))[1:5] INTO v_n, v_samples FROM (
        SELECT 'lamap_db: ' || x AS nm
        FROM lamap_db_foreign.transactions t, unnest(coalesce(t.seller_names,'{}') || coalesce(t.buyer_names,'{}')) x
        JOIN silver_ch.entity_merge_decisions d ON d.status='active' AND d.alias_key = lower(public.unaccent(btrim(x)))
        WHERE t.transaction_id = ANY (v_touched)
        UNION ALL
        SELECT 'lamap-lbi: ' || x
        FROM lbi_foreign.transactions t, unnest(coalesce(t.seller_names,'{}') || coalesce(t.buyer_names,'{}')) x
        JOIN silver_ch.entity_merge_decisions d ON d.status='active' AND d.alias_key = lower(public.unaccent(btrim(x)))
        WHERE t.transaction_id = ANY (v_touched)
        UNION ALL
        SELECT 'rousseau_5: ' || x
        FROM rousseau5_foreign.transactions t, unnest(coalesce(t.seller_names,'{}') || coalesce(t.buyer_names,'{}')) x
        JOIN silver_ch.entity_merge_decisions d ON d.status='active' AND d.alias_key = lower(public.unaccent(btrim(x)))
        WHERE t.transaction_id = ANY (v_touched)
      ) z;
      IF v_n > 0 THEN v_findings := v_findings || jsonb_build_object('check','not_honoured_consumer','severity','critical','n',v_n,'samples',v_samples,
        'title', format('%s merged alias(es) still visible in a consumer database', v_n)); END IF;
    EXCEPTION WHEN OTHERS THEN
      -- a consumer we cannot reach is itself a finding, never a silent pass
      v_findings := v_findings || jsonb_build_object('check','consumer_unreachable','severity','critical','n',1,
        'samples', ARRAY[SQLERRM], 'title','Entity merge watchdog could not read a consumer database: ' || left(SQLERRM,120));
    END;
  END IF;

  -- 6. WARN — the FAO parser produced NEW fragmented party names (glued domicile / bare locality)
  SELECT count(*), (array_agg(DISTINCT nm))[1:5] INTO v_n, v_samples FROM (
    SELECT e->>'name' AS nm
    FROM bronze_ch.fao_transactions ft,
         jsonb_array_elements(coalesce(CASE WHEN jsonb_typeof(ft.old_owner_s)='array' THEN ft.old_owner_s END,'[]')
                            ||coalesce(CASE WHEN jsonb_typeof(ft.new_owner_s)='array' THEN ft.new_owner_s END,'[]')) e
    WHERE ft.created_at > now() - make_interval(hours => p_lookback_hours)
      AND (
        -- bare locality as a party
        (lower(public.unaccent(btrim(e->>'name'))) <> ALL (v_street_words)
         AND EXISTS (SELECT 1 FROM silver_ch.ref_communes rc
                WHERE rc.canonical_name_norm = lower(public.unaccent(btrim(e->>'name')))
                   OR lower(public.unaccent(btrim(e->>'name'))) = ANY (rc.aliases_norm)))
        OR
        -- "NAME, LOCALITY" with no city
        (e->>'name' LIKE '%,%' AND nullif(btrim(coalesce(e->>'city','')),'') IS NULL
         AND lower(public.unaccent(btrim(regexp_replace(e->>'name','^.*,','')))) <> ALL (v_street_words)
         AND EXISTS (SELECT 1 FROM silver_ch.ref_communes rc
                     WHERE rc.canonical_name_norm = lower(public.unaccent(btrim(regexp_replace(e->>'name','^.*,',''))))
                        OR lower(public.unaccent(btrim(regexp_replace(e->>'name','^.*,','')))) = ANY (rc.aliases_norm)))
      )
  ) z;
  IF v_n > 0 THEN v_findings := v_findings || jsonb_build_object('check','new_fragmented_fao_names','severity','warn','n',v_n,'samples',v_samples,
    'title', format('FAO parser produced %s new fragmented party name(s) in the last %sh', v_n, p_lookback_hours)); END IF;

  -- 7. INFO — review decisions waiting for a human (never enforced until approved)
  SELECT count(*), (array_agg(alias_name ORDER BY alias_name))[1:5] INTO v_n, v_samples
  FROM silver_ch.entity_merge_decisions WHERE status = 'review';
  IF v_n > 0 THEN v_findings := v_findings || jsonb_build_object('check','review_pending','severity','info','n',v_n,'samples',v_samples,
    'title', format('%s entity merge candidate(s) awaiting human review', v_n)); END IF;

  v_status := CASE WHEN EXISTS (SELECT 1 FROM jsonb_array_elements(v_findings) x WHERE x->>'severity' IN ('critical','warn'))
                   THEN 'alerted' ELSE 'clean' END;

  IF p_push THEN
    FOR f IN SELECT * FROM jsonb_array_elements(v_findings) LOOP
      SELECT net.http_post(
        url := 'https://dxugbpeacnorjunpljih.supabase.co/rest/v1/rpc/fn_ops_capture',
        headers := jsonb_build_object('apikey',v_cam_key,'Authorization','Bearer '||v_cam_key,'Content-Type','application/json'),
        body := jsonb_build_object(
          'p_dedup_key', 'entity_merge:' || (f->>'check'),
          'p_source', 'entity_merge_watchdog',
          'p_title', f->>'title',
          'p_detail', format('n=%s. Samples: %s. Authority: silver_ch.entity_merge_decisions (re-LLM). Enforcement: silver_ch.apply_entity_merge_decisions() pre-hook in gold_ch.refresh_daily_matviews.',
                             f->>'n', f->'samples'),
          'p_severity', f->>'severity',
          'p_startup_id', v_rellm, 'p_target', 'entity_merge', 'p_metric', f->>'check')
      ) INTO v_req;
    END LOOP;
  END IF;

  INSERT INTO gold_ch.entity_merge_watchdog_runs (status, findings, duration_ms)
  VALUES (v_status, v_findings, (extract(epoch FROM clock_timestamp() - v_t0) * 1000)::int);

  RETURN jsonb_build_object('status', v_status, 'touched_transactions', array_length(v_touched,1),
                            'findings', v_findings, 'pushed', p_push);
END $fn$;

COMMENT ON FUNCTION gold_ch.entity_merge_watchdog_run(int, boolean) IS
  'Nightly verification that every active silver_ch.entity_merge_decisions row holds in bronze, the warehouse, and '
  'every consumer (lamap_db, lamap-lbi, rousseau_5). Pushes Command Center alerts under dedup prefix entity_merge: '
  '(not auto-cleared). Every run is recorded in gold_ch.entity_merge_watchdog_runs, clean runs included.';

COMMIT;

-- Schedule (applied separately): daily 06:15 UTC, after refresh (04:30), distribution (05:00) and rousseau_5 (05:30).
--   SELECT cron.schedule('entity-merge-watchdog-daily', '15 6 * * *', 'SELECT gold_ch.entity_merge_watchdog_run();');
-- ROLLBACK: SELECT cron.unschedule('entity-merge-watchdog-daily'); the function and run table are additive.
