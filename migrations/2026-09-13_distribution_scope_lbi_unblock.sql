-- ============================================================================
-- 2026-09-13 — Distribution scope + lamap-lbi unblock
-- ============================================================================
-- Context: foncier and LBI entity links were frozen for 5 weeks while cron stayed green
-- (sync_full_refresh refused them; nothing alerted). Auditing every pipe failing into
-- lamap-lbi (44 in 35 days) showed three different things mixed in one log:
--
--  (a) 34 pairs were never a distribution: run_sync_proc pushes EVERY sync_registry row to
--      all three consumers, but lamap-lbi has no table for them (cadastral_*, sad_national,
--      listing_dom, ...), no function or view reads them, and none ever succeeded. They
--      failed on every run and buried the real freezes. -> gold_ch.sync_target_scope declares
--      them (evidence computed live via dblink below); run_sync_proc skips declared pairs.
--  (b) plots / buildings on LBI are empty BY DESIGN: 2026-07-06 decision (change_log
--      23d469db, db2de8b8) serves LBI from the curated ref.plots_registry (73k rows, fed from
--      lamap_db) instead of 9.8 GB of ref.plots. Declared, not refilled.
--  (c) real freezes, fixed here:
--      - silver_ch.link_plot_listings -> LBI: REFUSED since 2026-08-06 (210,885 stale rows).
--        Keyless junction -> Branch C atomic swap, exactly like lamap_db.
--      - gold_ch.v_core_entities_enriched -> LBI: the Branch C swap TRUNCATEs ref.entities,
--        which LBI now refuses (FKs from lbi_app.special_situation_* tables; rolled back
--        2026-09-13, live intact). -> Branch A upsert on the unique idx_ref_entities_id.
--      - Branch B (legacy) failures left NO sync_log row: the 'running' row is INSERTed inside
--        the exception block and rolled back with it, so the handler's UPDATE matched nothing.
--        Now the handler INSERTs the failure with SQLERRM.
--
-- NO DELETIONS: scope rows are never deleted (trigger); re-enable a pair with
-- UPDATE ... SET distribute = true. Upserts and swaps never remove consumer rows by key.
--
-- ROLLBACK (re-LLM): restore gold_ch.sync_full_refresh and gold_ch.run_sync_proc from
-- backup.fn_defs_20260913 (fn = 'sync_full_refresh@pre-distribution-scope' /
-- 'run_sync_proc@pre-distribution-scope'). The scope table and staging objects are inert
-- without the new procedure bodies.
-- ============================================================================

-- ---------------------------------------------------------------- (1) lamap-lbi
-- CREATE TABLE IF NOT EXISTS ref._staging_link_plot_listings (LIKE ref.link_plot_listings INCLUDING DEFAULTS);

-- ---------------------------------------------------------------- (2) re-LLM
BEGIN;

INSERT INTO backup.fn_defs_20260913 (fn, md5, definition)
SELECT p.proname || '@pre-distribution-scope', md5(pg_get_functiondef(p.oid)), pg_get_functiondef(p.oid)
FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'gold_ch' AND p.proname IN ('sync_full_refresh', 'run_sync_proc');

IMPORT FOREIGN SCHEMA ref LIMIT TO (_staging_link_plot_listings) FROM SERVER lamap_lbi_server INTO lbi_foreign;

CREATE TABLE IF NOT EXISTS gold_ch.sync_target_scope (
  source_schema text        NOT NULL,
  source_table  text        NOT NULL,
  target_db     text        NOT NULL CHECK (target_db IN ('lamap_db','lamap-lbi','lamap-crm')),
  distribute    boolean     NOT NULL DEFAULT false,
  reason        text        NOT NULL CHECK (reason IN ('target_absent_on_consumer','by_design_not_distributed')),
  evidence      jsonb       NOT NULL,
  decided_by    text        NOT NULL,
  decided_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source_schema, source_table, target_db)
);
COMMENT ON TABLE gold_ch.sync_target_scope IS
  'Pairs gold_ch.run_sync_proc must NOT push (distribute=false), each with evidence. Anything not '
  'listed is distributed and any failure is alerted by gold_ch.distribution_alert_run(). Never '
  'delete a row: set distribute=true to re-enable.';

CREATE OR REPLACE FUNCTION gold_ch.sync_target_scope_no_delete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'gold_ch.sync_target_scope rows are never deleted; UPDATE ... SET distribute = true instead';
END $$;
DROP TRIGGER IF EXISTS sync_target_scope_no_delete ON gold_ch.sync_target_scope;
CREATE TRIGGER sync_target_scope_no_delete BEFORE DELETE ON gold_ch.sync_target_scope
  FOR EACH ROW EXECUTE FUNCTION gold_ch.sync_target_scope_no_delete();
DROP TRIGGER IF EXISTS sync_target_scope_no_truncate ON gold_ch.sync_target_scope;
CREATE TRIGGER sync_target_scope_no_truncate BEFORE TRUNCATE ON gold_ch.sync_target_scope
  FOR EACH STATEMENT EXECUTE FUNCTION gold_ch.sync_target_scope_no_delete();

-- (a) evidence computed live on lamap-lbi, not typed by hand
INSERT INTO gold_ch.sync_target_scope (source_schema, source_table, target_db, distribute, reason, evidence, decided_by)
SELECT r.source_schema, r.source_table, 'lamap-lbi', false, 'target_absent_on_consumer',
       jsonb_build_object('target_table', r.target_table, 'consumer_table_exists', false,
                          'successful_syncs_ever', 0, 'consumer_functions_mentioning_name', x.fn_mentions,
                          'verified_at', now()),
       'claude-code (Session System 2.4.3)'
FROM gold_ch.sync_registry r
CROSS JOIN LATERAL dblink('lamap_lbi_server', format(
  $q$SELECT to_regclass(%L) IS NOT NULL,
            (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
             WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND p.prosrc ~ %L)$q$,
  'ref.' || quote_ident(r.target_table), '\m' || r.target_table || '\M')) AS x(table_exists boolean, fn_mentions bigint)
WHERE NOT x.table_exists
  AND NOT EXISTS (SELECT 1 FROM gold_ch.sync_log l
                  WHERE l.source_table = r.source_schema || '.' || r.source_table
                    AND l.target_db = 'lamap-lbi' AND l.status = 'success')
ON CONFLICT DO NOTHING;

-- (b) by design
INSERT INTO gold_ch.sync_target_scope (source_schema, source_table, target_db, distribute, reason, evidence, decided_by)
VALUES
 ('gold_ch','v_plots_full','lamap-lbi', false, 'by_design_not_distributed',
  jsonb_build_object('target_table','plots','decision_change_log',jsonb_build_array('23d469db-eeaf-44f3-86ba-70ff008173f2','db2de8b8-4739-4741-8b1e-7f3e19441719'),
    'note','LBI serves plots from ref.plots_registry (73k curated rows, fed from lamap_db), not 9.8 GB ref.plots; ref.plots on LBI has been empty since before 2026-07-06; last warehouse success 2026-05-06; REFUSED since.'),
  'claude-code (Session System 2.4.3)'),
 ('gold_ch','v_buildings_full','lamap-lbi', false, 'by_design_not_distributed',
  jsonb_build_object('target_table','buildings','decision_change_log',jsonb_build_array('23d469db-eeaf-44f3-86ba-70ff008173f2'),
    'note','ref.buildings on LBI empty (0 rows); legacy TRUNCATE path (allow_legacy_truncate) failed silently every month since 2026-07-01; a success would TRUNCATE and load 3.3M rows into LBI. Not read by any LBI table-qualified query.'),
  'claude-code (Session System 2.4.3)')
ON CONFLICT DO NOTHING;
CREATE OR REPLACE PROCEDURE gold_ch.run_sync_proc(IN p_frequency text)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  rec           RECORD;
  v_rows        INTEGER;
  v_full_source TEXT;
  v_failures    INTEGER := 0;
  v_total       INTEGER := 0;
  v_failed_list TEXT := '';
  v_skipped     INTEGER := 0;
  targets CONSTANT TEXT[][] := ARRAY[
    ['lamap_crm_server', 'crm_foreign',      'lamap-crm'],
    ['lamap_lbi_server', 'lbi_foreign',      'lamap-lbi'],
    ['lamap_db_server',  'lamap_db_foreign', 'lamap_db']
  ];
  t TEXT[];
BEGIN
  PERFORM set_config('statement_timeout', '3600000', false);

  FOR rec IN
    SELECT r.source_schema, r.source_table, r.target_table, r.pk_column,
           r.sync_mode, r.delta_column
    FROM gold_ch.sync_registry r
    WHERE r.frequency = p_frequency AND r.enabled = true
    ORDER BY r.source_table
  LOOP
    FOREACH t SLICE 1 IN ARRAY targets LOOP
      v_full_source := rec.source_schema || '.' || rec.source_table;
      -- 2026-09-13: every registry row used to be pushed to all three consumers. Pairs that were
      -- never a distribution (no table on the consumer) failed on every run and buried the real
      -- freezes. gold_ch.sync_target_scope declares them, with evidence; nothing else is skipped.
      IF EXISTS (SELECT 1 FROM gold_ch.sync_target_scope s
                 WHERE s.source_schema = rec.source_schema AND s.source_table = rec.source_table
                   AND s.target_db = t[3] AND NOT s.distribute) THEN
        v_skipped := v_skipped + 1;
        CONTINUE;
      END IF;
      v_total := v_total + 1;

      IF rec.sync_mode = 'delta' THEN
        v_rows := 0;
        CALL gold_ch.sync_delta(rec.source_schema, rec.source_table, rec.target_table,
                                rec.pk_column, t[2], t[3], rec.delta_column, v_rows);
        PERFORM set_config('statement_timeout', '3600000', false);
        IF v_rows = -1 THEN
          v_failures := v_failures + 1;
          v_failed_list := v_failed_list || v_full_source || '->' || t[3] || '(delta -1); ';
          INSERT INTO gold_ch.sync_failure_log(source_table,target_db,sync_mode,detail,context)
          VALUES (v_full_source, t[3], 'delta', 'sync_delta returned -1 (see gold_ch.sync_log)', 'run_sync_proc:'||p_frequency);
        ELSE
          RAISE NOTICE 'run_sync_proc(%): %->% delta upserted % rows', p_frequency, v_full_source, t[3], v_rows;
        END IF;
      ELSE
        v_rows := 0;
        CALL gold_ch.sync_full_refresh(rec.source_schema, rec.source_table, rec.target_table,
                                       t[1], t[2], t[3], NULL::text[], v_rows);
        COMMIT;
        PERFORM set_config('statement_timeout', '3600000', false);
        IF v_rows = -1 THEN
          v_failures := v_failures + 1;
          v_failed_list := v_failed_list || v_full_source || '->' || t[3] || '(full_refresh -1); ';
          INSERT INTO gold_ch.sync_failure_log(source_table,target_db,sync_mode,detail,context)
          VALUES (v_full_source, t[3], 'full_refresh', 'sync_full_refresh returned -1 (see gold_ch.sync_log)', 'run_sync_proc:'||p_frequency);
        ELSE
          RAISE NOTICE 'run_sync_proc(%): %->% full_refresh did % rows', p_frequency, v_full_source, t[3], v_rows;
        END IF;
      END IF;
    END LOOP;
  END LOOP;

  PERFORM set_config('statement_timeout', '600000', false);

  -- NEVER RAISE: a failed pipe must not abort the others. Failures are logged to
  -- gold_ch.sync_failure_log + gold_ch.sync_log and surfaced by the pipeline watchdog.
  IF v_failures > 0 THEN
    RAISE WARNING 'run_sync_proc(%): % of % pipe-targets failed (logged), % out of scope: %', p_frequency, v_failures, v_total, v_skipped, v_failed_list;
  ELSE
    RAISE NOTICE 'run_sync_proc(%): all % pipe-targets ok, % out of scope', p_frequency, v_total, v_skipped;
  END IF;
END;
$procedure$

;
CREATE OR REPLACE PROCEDURE gold_ch.sync_full_refresh(IN p_source_schema text, IN p_source_table text, IN p_target_table text, IN p_target_server text, IN p_foreign_schema text, IN p_target_db text, IN p_column_list text[] DEFAULT NULL::text[], INOUT p_rows_affected integer DEFAULT 0)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  v_count       INTEGER := 0;
  v_log_id      BIGINT;
  v_full_source TEXT;
  v_cols        TEXT;
  v_cols_qual   TEXT;
  v_in_allowlist     boolean := false;
  v_legacy_ok        boolean := false;
  v_pk_column        text;
  v_registry_cols    text[];
  v_registry_pk_seen text;
  v_staging_table    text;
  v_set_list         text;
  v_per_target_protect_cols text[];
  v_distinct_pred    text;
  v_status           text;
  v_error            text := NULL;
  i int;
  c_allowlist CONSTANT text[][] := ARRAY[
    ARRAY['gold_ch','v_addresses_full','addresses','lamap-lbi'],
    ARRAY['gold_ch','v_plots_full','plots','lamap_db'],
    ARRAY['gold_ch','v_chantiers_full','cadastral_chantiers','lamap_db'],
    ARRAY['gold_ch','v_patrimoine_classe_full','cadastral_patrimoine_classe','lamap_db'],
    ARRAY['gold_ch','v_patrimoine_inventaire_full','cadastral_patrimoine_inventaire','lamap_db'],
    ARRAY['gold_ch','v_buildings_geo_full','buildings_geo','lamap_db'],
    ARRAY['gold_ch','v_rdppf_pnp_full','cadastral_rdppf_pnp','lamap_db'],
    ARRAY['gold_ch','v_federal_communes_full','federal_communes','lamap_db'],
    ARRAY['gold_ch','v_girec_full','cadastral_girec','lamap_db'],
    ARRAY['gold_ch','v_agricultural_full','cadastral_agricultural','lamap_db'],
    ARRAY['gold_ch','v_bike_full','cadastral_bike','lamap_db'],
    ARRAY['gold_ch','v_solaire_full','cadastral_solar_panels','lamap_db'],
    ARRAY['gold_ch','v_sad_national_full','sad_national','lamap_db'],
    ARRAY['gold_ch','v_plots_ge_full_geom_full','plots_ge_full_geom','lamap_db'],
    ARRAY['gold_ch','v_communes_ge_geo_full','communes_ge_geo','lamap_db'],
    ARRAY['gold_ch','v_federal_cadastral_parcels_full','federal_cadastral_parcels','lamap_db'],
    ARRAY['gold_ch','v_transactions_full','transactions','lamap_db'],
    ARRAY['gold_ch','v_core_entities_enriched',       'entities',    'lamap_db'],
    ARRAY['gold_ch','v_pricehubble_full','pricehubble','lamap_db'],
    -- Geneva forest layers, added 2026-08-06. Allowlisted from birth: these
    -- tables have no legacy TRUNCATE behaviour to cut over from, and the brief
    -- that created them forbids TRUNCATE on live ref.* outright.
    ARRAY['gold_ch','v_forest_cadastre_full','cadastral_forest_cadastre','lamap_db'],
    ARRAY['gold_ch','v_forest_distance_s_full','cadastral_forest_distance_s','lamap_db'],
    ARRAY['gold_ch','v_forest_distance_l_full','cadastral_forest_distance_l','lamap_db'],
    ARRAY['gold_ch','v_forest_lisieres_full','cadastral_forest_lisieres','lamap_db'],
    ARRAY['gold_ch','v_forest_lisieres_parcelles_full','cadastral_forest_lisieres_parcelles','lamap_db'],
    ARRAY['gold_ch','v_forest_fonction_full','cadastral_forest_fonction','lamap_db'],
    ARRAY['gold_ch','v_plot_forest_constraints_full','plot_forest_constraints','lamap_db'],
    -- SITG tree cadastre, added 2026-08-07. Allowlisted from birth: new table,
    -- no legacy TRUNCATE behaviour to cut over from.
    ARRAY['gold_ch','v_trees_cadastre_full','trees_cadastre','lamap_db'],
    -- 2026-08-06: the three heavy bespoke syncs migrated off cross-FDW
    -- UPDATE ... FROM. Their predicate could not be pushed down, so each run
    -- fetched the whole remote table and then issued one remote UPDATE per
    -- changed row. At 74k and 83k rows that never finished, which under-repairs
    -- SILENTLY: the same defect class this allowlist exists to remove.
    ARRAY['bronze_ch','ge_rdppf_synthese','ge_rdppf_synthese','lamap_db'],
    ARRAY['bronze_ch','ge_cad_batiments','ge_cad_batiments','lamap_db'],
    ARRAY['bronze_ch','ge_cad_batiments_souterrains','ge_cad_batiments_souterrains','lamap_db']

,
    ARRAY['gold_ch','v_ge_cad_adresses_full','ge_cad_adresses','lamap_db'],
    -- 2026-09-13: foncier (foncier-geneve). Refused since the fail-closed guard landed (frozen at
    -- 2026-08-06). Keyed tables -> Branch A non-destructive upsert. Key and column list come from
    -- gold_ch.foncier_sync_registry (see the Branch A lookup), which this procedure never read.
    ARRAY['gold_ch','v_foncier_transactions','transactions','foncier'],
    ARRAY['gold_ch','v_foncier_sad','sad','foncier'],
    ARRAY['gold_ch','v_foncier_listings','listings','foncier'],
    ARRAY['gold_ch','v_foncier_plots','plots','foncier'],
    -- 2026-09-13: entities->LBI (was Branch C; see c_swaplist). Unique idx_ref_entities_id on LBI.
    ARRAY['gold_ch','v_core_entities_enriched','entities','lamap-lbi']  ];
  -- 2026-05-19 Phase 7d incident: COALESCE-protect seeded cols (Amendment-1 §2 manifest, 27 cols).
  -- TEXT cols: NULLIF(EXCLUDED.col,'') treats empty-string as missing.
  -- NUMERIC/INTEGER/JSONB cols: plain COALESCE(EXCLUDED.col, target.col).
  -- typologie + typologie_categorie included even though not currently in v_plots_full
  -- column_list — guards against future additions wiping them.
  c_coalesce_text_cols CONSTANT text[] := ARRAY[
    'appartenance','commune_name','heating_type','ius_dero_status','lien_rf',
    'owner_names_search','owners_display','sous_secteur_nom','typologie',
    'typologie_categorie','zone_primaire'
  ];
  c_coalesce_other_cols CONSTANT text[] := ARRAY[
    'ius_hpe','ius_legal_ceiling','ius_realistic_ceiling','ius_thpe','owner_count',
    'owners','pool_surface_m2','solde_pct_legal','solde_pct_standard',
    'surface_brut_de_plancher_hors_sol_m2','surface_potentielle_legal_m2',
    'surface_potentielle_m2','surface_residuelle_m2','volume_total_m3',
    'zone_ius_max','zone_ius_standard'
  ];
  -- 2026-05-19 §5b Bucket D fix: cols where target (legacy-seeded value) takes precedence
  -- over EXCLUDED (re-LLM emits different format / different content).
  -- zone_primaire: legacy carries '5 (100%)' / multi-zone strings; re-LLM emits bare prefix.
  -- densification_zone: legacy + re-LLM diverge on naming for 67 GE rows.
  -- Pattern: col = COALESCE(ref.plots.col, EXCLUDED.col) -- target wins; re-LLM only fills NULL.
  c_target_priority_cols CONSTANT text[] := ARRAY[
    'zone_primaire','densification_zone'
  ];
  -- 2026-06-05 Step 2 wave-2 β-a: per-tuple COALESCE protection. Scoped so
  -- adding a protected column to one allowlist target does NOT change any
  -- other target's SET clause. Empty array for unlisted tuples → no-op pass-through.
  c_coalesce_other_per_target CONSTANT text[][] := ARRAY[
    -- (source_schema, source_table, target_table, target_db, col)
    ARRAY['gold_ch','v_transactions_full','transactions','lamap_db','parties_buyers'],
    ARRAY['gold_ch','v_transactions_full','transactions','lamap_db','parties_sellers'],
    ARRAY['gold_ch','v_core_entities_enriched',       'entities',    'lamap_db','last_transaction_date']
  ];
  -- 2026-06-12 Branch C: atomic remote staging-swap for keyless/junction tables.
  -- Failure mode = remote rollback (TRUNCATE undone) -> live data intact. PK-free.
  v_use_swap boolean := false;
  c_swaplist CONSTANT text[][] := ARRAY[
    -- 2026-09-13: entities->LBI moved to Branch A (c_allowlist). The swap TRUNCATEs ref.entities, which
    -- LBI now forbids: lbi_app.special_situation_legal_event_entity_links_v1 and
    -- special_situation_registry_uid_evidence_v1 hold FOREIGN KEYs to it (swap rolled back 2026-09-13).
    ARRAY['silver_ch','link_plot_sad','link_plot_sad','lamap-lbi'],            -- proof (non-live)
    ARRAY['silver_ch','event_transaction_parties','link_entity_transactions','lamap_db'],
    ARRAY['silver_ch','event_transaction_parties','link_entity_transactions','lamap-lbi'],  -- 2026-09-13: LBI link_entity_transactions was never cut over to a non-destructive path; refused (frozen) since 2026-08-06
    ARRAY['silver_ch','link_plot_listings','link_plot_listings','lamap_db'],
    ARRAY['silver_ch','link_plot_listings','link_plot_listings','lamap-lbi'],  -- 2026-09-13: refused (frozen) since 2026-08-06; keyless junction, same as lamap_db
    ARRAY['silver_ch','link_plot_sad','link_plot_sad','lamap_db'],
    ARRAY['silver_ch','link_plot_transactions','link_plot_transactions','lamap_db']
  ];
BEGIN
  -- Inside-body config to mirror sync_delta. The procedure has NO proconfig
  -- SET clauses (would block COMMIT) and NO SECURITY DEFINER (same).
  PERFORM set_config('statement_timeout', '7200000', true);

  v_full_source := p_source_schema || '.' || p_source_table;

  -- ===== Branch C: atomic remote staging-swap (keyless/junction tables) =====
  FOR i IN 1 .. coalesce(array_length(c_swaplist, 1), 0) LOOP
    IF c_swaplist[i][1] = p_source_schema AND c_swaplist[i][2] = p_source_table
       AND c_swaplist[i][3] = p_target_table AND c_swaplist[i][4] = p_target_db THEN
      v_use_swap := true; EXIT;
    END IF;
  END LOOP;

  IF v_use_swap THEN
    IF p_column_list IS NULL OR array_length(p_column_list,1) IS NULL THEN
      SELECT array_agg(a.attname ORDER BY a.attnum) INTO p_column_list
      FROM pg_attribute a JOIN pg_class c ON a.attrelid=c.oid JOIN pg_namespace n ON c.relnamespace=n.oid
      WHERE n.nspname=p_source_schema AND c.relname=p_source_table AND a.attnum>0 AND NOT a.attisdropped;
    END IF;
    v_cols := array_to_string(ARRAY(SELECT quote_ident(c) FROM unnest(p_column_list) c), ', ');
    v_staging_table := '_staging_' || p_target_table;
    INSERT INTO gold_ch.sync_log (source_table, target_db, sync_mode, started_at, status)
    VALUES (v_full_source, p_target_db, 'atomic_swap', clock_timestamp(), 'running') RETURNING id INTO v_log_id;
    -- 1) load staging on consumer (via FDW). NO subtransaction here so we can COMMIT.
    PERFORM dblink_exec(p_target_server, format('SET statement_timeout=''7200000''; TRUNCATE ref.%I;', v_staging_table));
    EXECUTE format('INSERT INTO %I.%I (%s) SELECT %s FROM %I.%I', p_foreign_schema, v_staging_table, v_cols, v_cols, p_source_schema, p_source_table);
    GET DIAGNOSTICS v_count = ROW_COUNT;
    -- 2) safety: never swap an empty staging into a live table
    IF v_count = 0 THEN
      UPDATE gold_ch.sync_log SET status='failed', error_message='staging loaded 0 rows; refused', finished_at=clock_timestamp() WHERE id=v_log_id;
      p_rows_affected := -1; RETURN;
    END IF;
    -- 3) COMMIT so the just-loaded staging rows are visible to the separate swap connection
    COMMIT;
    PERFORM set_config('statement_timeout', '7200000', true);
    -- 4) ATOMIC swap on a NAMED dblink connection (transaction spans calls; ROLLBACK on error undoes TRUNCATE -> live intact)
    PERFORM dblink_connect('lbi_swapconn', p_target_server);
    PERFORM dblink_exec('lbi_swapconn', 'BEGIN');
    v_status := dblink_exec('lbi_swapconn',
      format('SET LOCAL statement_timeout=''7200000''; TRUNCATE ref.%I; INSERT INTO ref.%I (%s) SELECT %s FROM ref.%I;',
             p_target_table, p_target_table, v_cols, v_cols, v_staging_table), false);
    IF v_status IS NULL OR v_status = 'ERROR' THEN
      v_error := dblink_error_message('lbi_swapconn');
      PERFORM dblink_exec('lbi_swapconn', 'ROLLBACK', false);
      PERFORM dblink_disconnect('lbi_swapconn');
      UPDATE gold_ch.sync_log SET status='failed', error_message='swap rolled back (live intact): '||COALESCE(v_error,'?'), finished_at=clock_timestamp() WHERE id=v_log_id;
      RAISE WARNING 'Branch C swap ROLLED BACK %->% (live data intact): %', v_full_source, p_target_db, v_error;
      p_rows_affected := -1; RETURN;
    END IF;
    PERFORM dblink_exec('lbi_swapconn', 'COMMIT');
    PERFORM dblink_disconnect('lbi_swapconn');
    BEGIN PERFORM dblink_exec(p_target_server, format('TRUNCATE ref.%I;', v_staging_table)); EXCEPTION WHEN OTHERS THEN NULL; END;
    UPDATE gold_ch.sync_log SET status='success', rows_affected=v_count, finished_at=clock_timestamp() WHERE id=v_log_id;
    RAISE NOTICE 'Branch C swap %->%: % rows (atomic, named conn)', v_full_source, p_target_db, v_count;
    p_rows_affected := v_count; RETURN;
  END IF;

  FOR i IN 1 .. coalesce(array_length(c_allowlist, 1), 0) LOOP
    IF c_allowlist[i][1] = p_source_schema
       AND c_allowlist[i][2] = p_source_table
       AND c_allowlist[i][3] = p_target_table
       AND c_allowlist[i][4] = p_target_db THEN
      v_in_allowlist := true;
      EXIT;
    END IF;
  END LOOP;

  IF NOT v_in_allowlist THEN
    -- FAIL CLOSED. Branch B TRUNCATEs the LIVE target table. Reaching it by
    -- accident truncated ref.ge_cad_adresses on 2026-08-07: the procedure
    -- emitted a NOTICE and proceeded. A NOTICE is not a guard, so the legacy
    -- path now requires an EXPLICIT opt-in per target.
    SELECT r.pk_column, coalesce(r.allow_legacy_truncate, false)
      INTO v_registry_pk_seen, v_legacy_ok
    FROM gold_ch.sync_registry r
    WHERE r.source_schema = p_source_schema
      AND r.source_table  = p_source_table
      AND r.target_table  = p_target_table
    LIMIT 1;

    IF NOT coalesce(v_legacy_ok, false) THEN
      -- FAIL CLOSED, but through the orchestrator's CONTRACT instead of a RAISE.
      -- 2026-08-09: this used to RAISE EXCEPTION. gold_ch.run_sync_proc states
      -- "NEVER RAISE: a failed pipe must not abort the others" and detects failure via
      -- p_rows_affected = -1, so raising bypassed that and aborted the WHOLE daily run at
      -- the first refused tuple. Consequence: one CRM-bound target took Lamap's
      -- transactions/sad/listings distribution down from 2026-08-06 and the job failed
      -- every day after. The refusal is UNCHANGED - nothing is truncated - only the way it
      -- is reported. It also cannot simply be wrapped by the caller: this procedure does
      -- its own COMMIT, and COMMIT inside a block with an EXCEPTION clause raises
      -- 'invalid transaction termination'.
      INSERT INTO gold_ch.sync_log (source_table, target_db, sync_mode, started_at, finished_at, status, error_message)
      VALUES (v_full_source, p_target_db, 'full_refresh', clock_timestamp(), clock_timestamp(), 'failed',
              format('REFUSED: %s->%s.%s is not in c_allowlist and its sync_registry row does not set allow_legacy_truncate. Branch B would TRUNCATE the live target. Add the tuple to c_allowlist (correct fix), or set allow_legacy_truncate=true only if this target genuinely still needs the legacy path.',
                     v_full_source, p_target_db, p_target_table));
      RAISE WARNING 'sync_full_refresh REFUSED (skipped, not aborted): %->%.%',
        v_full_source, p_target_db, p_target_table;
      p_rows_affected := -1;
      RETURN;
    END IF;  RAISE WARNING
      'sync_full_refresh: %->% is on the legacy TRUNCATE path by explicit opt-in (pk=%). Cut it over to Branch A.',
      v_full_source, p_target_db, coalesce(v_registry_pk_seen, 'unknown');
  END IF;

  IF v_in_allowlist THEN
    -- 2026-09-13: foncier's key and column list live in gold_ch.foncier_sync_registry
    -- (run_foncier_sync reads it, but always passes a NULL column list). gold_ch.sync_registry
    -- still wins for every other target; the fallback is scoped to p_target_db = 'foncier'.
    SELECT x.pk_column, x.column_list
      INTO v_pk_column, v_registry_cols
    FROM (
      SELECT r.pk_column, r.column_list, 1 AS prio
      FROM gold_ch.sync_registry r
      WHERE r.source_schema = p_source_schema
        AND r.source_table  = p_source_table
        AND r.target_table  = p_target_table
      UNION ALL
      SELECT f.pk_column, f.column_list, 2
      FROM gold_ch.foncier_sync_registry f
      WHERE p_target_db = 'foncier'
        AND f.source_schema = p_source_schema
        AND f.source_table  = p_source_table
        AND f.target_table  = p_target_table
    ) x
    ORDER BY x.prio
    LIMIT 1;

    IF v_pk_column IS NULL
       OR v_registry_cols IS NULL
       OR array_length(v_registry_cols, 1) IS NULL THEN
      INSERT INTO gold_ch.sync_log
        (source_table, target_db, sync_mode, started_at, finished_at, status, error_message)
      VALUES
        (v_full_source, p_target_db, 'full_refresh',
         clock_timestamp(), clock_timestamp(), 'failed',
         'Branch A: registry row missing pk_column or column_list');
      COMMIT;
      PERFORM set_config('statement_timeout', '7200000', true);
      RAISE WARNING 'sync_full_refresh (Branch A) %->%: registry row missing pk_column or column_list',
                    v_full_source, p_target_db;
      p_rows_affected := -1;
      RETURN;
    END IF;

    v_staging_table := '_staging_' || p_target_table;
    v_cols      := array_to_string(ARRAY(SELECT quote_ident(c) FROM unnest(v_registry_cols) c), ', ');
    v_cols_qual := v_cols;

    -- 2026-06-05 β-a: resolve per-tuple protection list for this sync target.
    -- Empty array for unlisted tuples; CASE branch then no-ops.
    v_per_target_protect_cols := ARRAY(
      SELECT c_coalesce_other_per_target[s.idx][5]
      FROM generate_subscripts(c_coalesce_other_per_target, 1) AS s(idx)
      WHERE c_coalesce_other_per_target[s.idx][1] = p_source_schema
        AND c_coalesce_other_per_target[s.idx][2] = p_source_table
        AND c_coalesce_other_per_target[s.idx][3] = p_target_table
        AND c_coalesce_other_per_target[s.idx][4] = p_target_db
    );

    -- 2026-05-19 Phase 7d incident: COALESCE-protect seeded cols (see DECLARE constants).
    SELECT string_agg(
      CASE
        -- 2026-06-05 β-a: per-tuple COALESCE protection (highest precedence).
        -- Empty list (most tuples) → branch is always FALSE → fall through unchanged.
        WHEN c = ANY (COALESCE(v_per_target_protect_cols, ARRAY[]::text[])) THEN
          format('%I = COALESCE(EXCLUDED.%I, %I.%I.%I)',
                 c, c, 'ref', p_target_table, c)
        WHEN c = ANY (c_target_priority_cols) THEN
          -- 2026-05-19 §5b Bucket D: target legacy-seed wins; re-LLM only fills NULL.
          format('%I = COALESCE(%I.%I.%I, EXCLUDED.%I)',
                 c, 'ref', p_target_table, c, c)
        WHEN c = ANY (c_coalesce_text_cols) THEN
          format('%I = COALESCE(NULLIF(EXCLUDED.%I, ''''), %I.%I.%I)',
                 c, c, 'ref', p_target_table, c)
        WHEN c = 'owner_count' THEN
          -- 2026-05-19 incident follow-up: v1.6 treated owner_count=0 as missing
          format('%I = COALESCE(NULLIF(EXCLUDED.%I, 0), %I.%I.%I)',
                 c, c, 'ref', p_target_table, c)
        WHEN c = ANY (c_coalesce_other_cols) THEN
          format('%I = COALESCE(EXCLUDED.%I, %I.%I.%I)',
                 c, c, 'ref', p_target_table, c)
        ELSE
          format('%I = EXCLUDED.%I', c, c)
      END,
      ', ' ORDER BY ord
    ) INTO v_set_list
    FROM unnest(v_registry_cols) WITH ORDINALITY AS u(c, ord)
    WHERE c <> v_pk_column;

    SELECT
      '(' || string_agg(
               CASE WHEN c IN ('geometry','centroid')
                    THEN format('%I.%I::text', p_target_table, c)
                    ELSE format('%I.%I',       p_target_table, c)
               END,
               ', ' ORDER BY ord) || ')'
      || ' IS DISTINCT FROM '
      || '(' || string_agg(
                  CASE WHEN c IN ('geometry','centroid')
                       THEN format('EXCLUDED.%I::text', c)
                       ELSE format('EXCLUDED.%I',       c)
                  END,
                  ', ' ORDER BY ord) || ')'
      INTO v_distinct_pred
    FROM unnest(v_registry_cols) WITH ORDINALITY AS u(c, ord)
    WHERE c <> v_pk_column
      AND c <> 'updated_at';

    v_distinct_pred := COALESCE(v_distinct_pred, 'true');

    INSERT INTO gold_ch.sync_log (source_table, target_db, sync_mode, started_at, status)
    VALUES (v_full_source, p_target_db, 'full_refresh', clock_timestamp(), 'running')
    RETURNING id INTO v_log_id;
    COMMIT;
    PERFORM set_config('statement_timeout', '7200000', true);

    BEGIN
      PERFORM dblink_exec(p_target_server, format(
        'SET statement_timeout = ''7200000''; TRUNCATE ref.%I;', v_staging_table
      ));
    EXCEPTION WHEN OTHERS THEN
      v_error := SQLERRM;
    END;
    IF v_error IS NOT NULL THEN
      UPDATE gold_ch.sync_log SET
        finished_at = clock_timestamp(), status = 'failed',
        error_message = 'Branch A phase 1 (leading staging TRUNCATE): ' || v_error
      WHERE id = v_log_id;
      COMMIT;
      PERFORM set_config('statement_timeout', '7200000', true);
      RAISE WARNING 'sync_full_refresh (Branch A phase 1) FAILED %->%: %', v_full_source, p_target_db, v_error;
      p_rows_affected := -1;
      RETURN;
    END IF;

    BEGIN
      EXECUTE format(
        'INSERT INTO %I.%I (%s) SELECT %s FROM %I.%I',
        p_foreign_schema, v_staging_table, v_cols,
        v_cols_qual,
        p_source_schema, p_source_table
      );
    EXCEPTION WHEN OTHERS THEN
      v_error := SQLERRM;
    END;
    IF v_error IS NOT NULL THEN
      UPDATE gold_ch.sync_log SET
        finished_at = clock_timestamp(), status = 'failed',
        error_message = 'Branch A phase 2 (FDW staging load): ' || v_error
      WHERE id = v_log_id;
      COMMIT;
      PERFORM set_config('statement_timeout', '7200000', true);
      RAISE WARNING 'sync_full_refresh (Branch A phase 2) FAILED %->%: %', v_full_source, p_target_db, v_error;
      p_rows_affected := -1;
      RETURN;
    END IF;

    COMMIT;
    PERFORM set_config('statement_timeout', '7200000', true);

    BEGIN
      v_status := dblink_exec(p_target_server, format(
        'SET statement_timeout = ''7200000''; INSERT INTO ref.%I (%s) SELECT %s FROM ref.%I ON CONFLICT (%I) DO UPDATE SET %s WHERE %s;',
        p_target_table, v_cols, v_cols_qual, v_staging_table,
        v_pk_column, v_set_list, v_distinct_pred
      ));
      v_count := COALESCE(NULLIF(split_part(v_status, ' ', 3), ''), '0')::integer;
    EXCEPTION WHEN OTHERS THEN
      v_error := SQLERRM;
    END;
    IF v_error IS NOT NULL THEN
      UPDATE gold_ch.sync_log SET
        finished_at = clock_timestamp(), status = 'failed',
        error_message = 'Branch A phase 3 (upsert): ' || v_error
      WHERE id = v_log_id;
      COMMIT;
      PERFORM set_config('statement_timeout', '7200000', true);
      RAISE WARNING 'sync_full_refresh (Branch A phase 3) FAILED %->%: %', v_full_source, p_target_db, v_error;
      p_rows_affected := -1;
      RETURN;
    END IF;

    BEGIN
      PERFORM dblink_exec(p_target_server, format(
        'SET statement_timeout = ''120000''; TRUNCATE ref.%I;', v_staging_table
      ));
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'sync_full_refresh (Branch A) trailing staging TRUNCATE skipped (non-fatal): %', SQLERRM;
    END;

    UPDATE gold_ch.sync_log SET
      finished_at = clock_timestamp(), rows_affected = v_count, status = 'success'
    WHERE id = v_log_id;
    COMMIT;
    PERFORM set_config('statement_timeout', '7200000', true);

    RAISE NOTICE 'sync_full_refresh (Branch A, non-destructive) %->%: % rows written by upsert (% cols, pk=%, dblink_status=%)',
                 v_full_source, p_target_db, v_count, array_length(v_registry_cols,1), v_pk_column, v_status;
    p_rows_affected := v_count;
    RETURN;
  END IF;

  BEGIN
    IF p_column_list IS NULL OR array_length(p_column_list,1) IS NULL THEN
      SELECT array_agg(a.attname ORDER BY a.attnum)
        INTO p_column_list
      FROM pg_attribute a
      JOIN pg_class c ON a.attrelid=c.oid
      JOIN pg_namespace n ON c.relnamespace=n.oid
      WHERE n.nspname = p_source_schema AND c.relname = p_source_table
        AND a.attnum > 0 AND NOT a.attisdropped;
    END IF;

    IF p_column_list IS NULL OR array_length(p_column_list,1) IS NULL THEN
      RAISE EXCEPTION 'sync_full_refresh: cannot resolve column list for %.%', p_source_schema, p_source_table;
    END IF;

    v_cols := array_to_string(ARRAY(SELECT quote_ident(c) FROM unnest(p_column_list) c), ', ');
    v_cols_qual := v_cols;

    INSERT INTO gold_ch.sync_log (source_table, target_db, sync_mode, started_at, status)
    VALUES (v_full_source, p_target_db, 'full_refresh', clock_timestamp(), 'running')
    RETURNING id INTO v_log_id;

    PERFORM dblink_exec(p_target_server, format(
      'SET statement_timeout = ''7200000''; TRUNCATE ref.%I;', p_target_table
    ));

    EXECUTE format(
      'INSERT INTO %I.%I (%s) SELECT %s FROM %I.%I',
      p_foreign_schema, p_target_table, v_cols,
      v_cols_qual,
      p_source_schema, p_source_table
    );
    GET DIAGNOSTICS v_count = ROW_COUNT;

    UPDATE gold_ch.sync_log SET
      finished_at = clock_timestamp(), rows_affected = v_count, status = 'success'
    WHERE id = v_log_id;

    RAISE NOTICE 'sync_full_refresh %->%: % rows (% cols)', v_full_source, p_target_db, v_count, array_length(p_column_list,1);
    p_rows_affected := v_count;
    RETURN;
  EXCEPTION WHEN OTHERS THEN
    -- 2026-09-13: this was an UPDATE of the 'running' row, but that row was INSERTed inside this
    -- same block and is rolled back with it, so the UPDATE matched nothing and every Branch B
    -- failure left NO trace in sync_log (communes, buildings, listing_dom to LBI/CRM for months).
    INSERT INTO gold_ch.sync_log (source_table, target_db, sync_mode, started_at, finished_at, status, error_message)
    VALUES (v_full_source, p_target_db, 'full_refresh', clock_timestamp(), clock_timestamp(), 'failed',
            'Branch B (legacy): ' || SQLERRM);
    RAISE WARNING 'sync_full_refresh FAILED %->%: %', v_full_source, p_target_db, SQLERRM;
    p_rows_affected := -1;
    RETURN;
  END;
END;
$procedure$

;
COMMIT;
