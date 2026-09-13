-- 2026-09-13: add the entity-merge pre-hook to gold_ch.refresh_daily_matviews.
-- Only change vs the previous definition (md5 04fbe530137c98973318ae3b1a81038d): the isolated
-- PERFORM silver_ch.apply_entity_merge_decisions() block before the FOREACH loop.
-- ROLLBACK: re-run the definition stored in backup.fn_defs_20260913 (fn='gold_ch.refresh_daily_matviews()').

CREATE OR REPLACE FUNCTION gold_ch.refresh_daily_matviews()
 RETURNS TABLE(succeeded integer, failed integer, failed_names text[])
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'gold_ch', 'silver_ch', 'public'
 SET statement_timeout TO '3600000'
AS $function$
DECLARE
  v_start      timestamptz;
  mv           text;
  v_ok         int := 0;
  v_fail       int := 0;
  v_failed     text[] := ARRAY[]::text[];
  v_state      text;
  v_msg        text;
  matviews CONSTANT text[] := ARRAY[
    'silver_ch.event_sad',
    'silver_ch.sad_parcelle_geom',  -- G 2026-07-24: parcelle-derived SAD geometry, refresh with the SAD chain
    'silver_ch.event_transactions',
    'silver_ch.event_transaction_parties',
    'silver_ch.link_plot_listings',
    'silver_ch.link_plot_sad',
    'silver_ch.link_plot_transactions',
    'silver_ch.listing_active',
    'silver_ch.listing_group_best_url',
    'gold_ch.core_listings',
    'gold_ch.core_sad',
    'gold_ch.core_transactions'
  ];
BEGIN
  -- Pre-hook (2026-09-13): enforce silver_ch.entity_merge_decisions on the bronze source
  -- names BEFORE any matview reads them, so every refresh (nightly + mid-day) carries the
  -- merges and a re-ingest that reverted a name is healed. Isolated: a failure is logged
  -- to refresh_failure_log and the refresh continues, exactly like the post-refresh hooks.
  BEGIN
    PERFORM silver_ch.apply_entity_merge_decisions();
    RAISE NOTICE 'refresh_daily_matviews: apply_entity_merge_decisions OK';
  EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT;
    INSERT INTO gold_ch.refresh_failure_log(mv_name, sqlstate, sqlerrm, context)
    VALUES ('silver_ch.apply_entity_merge_decisions', v_state, v_msg, 'refresh_daily_matviews:pre-hook');
    RAISE WARNING 'refresh_daily_matviews: apply_entity_merge_decisions FAILED [%]: %', v_state, v_msg;
  END;

  FOREACH mv IN ARRAY matviews LOOP
    v_start := clock_timestamp();
    BEGIN
      EXECUTE format('REFRESH MATERIALIZED VIEW %s', mv);
      v_ok := v_ok + 1;
      RAISE NOTICE 'refresh_daily_matviews: % OK in %ms', mv,
        (extract(epoch FROM clock_timestamp()-v_start)*1000)::int;

      -- Step 1 hook: after ETP refresh, repopulate the resolution queue.
      IF mv = 'silver_ch.event_transaction_parties' THEN
        BEGIN
          PERFORM silver_ch.populate_party_resolution_queue();
          RAISE NOTICE 'refresh_daily_matviews: party_resolution_queue refreshed';
        EXCEPTION WHEN OTHERS THEN
          GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT;
          INSERT INTO gold_ch.refresh_failure_log(mv_name, sqlstate, sqlerrm, context)
          VALUES ('silver_ch.populate_party_resolution_queue', v_state, v_msg, 'refresh_daily_matviews:hook');
          RAISE WARNING 'refresh_daily_matviews: populate_party_resolution_queue FAILED [%]: %', v_state, v_msg;
        END;
      END IF;

      -- Step 2 hook: after core_transactions refresh, (re)extract per-unit PPE detail
      -- so the daily sync pushes fresh ppe_lot/millieme/unit_surface to consumers.
      IF mv = 'gold_ch.core_transactions' THEN
        BEGIN
          PERFORM silver_ch.refresh_ppe_extract();
          RAISE NOTICE 'refresh_daily_matviews: refresh_ppe_extract OK';
          PERFORM silver_ch.refresh_accessory_medians();
          PERFORM silver_ch.refresh_bundle_comps();
          PERFORM silver_ch.refresh_transaction_lineage_flags();
          RAISE NOTICE 'refresh_daily_matviews: bundle_comps + lineage_flags OK';
        EXCEPTION WHEN OTHERS THEN
          GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT;
          INSERT INTO gold_ch.refresh_failure_log(mv_name, sqlstate, sqlerrm, context)
          VALUES ('silver_ch.refresh_ppe_extract', v_state, v_msg, 'refresh_daily_matviews:hook');
          RAISE WARNING 'refresh_daily_matviews: refresh_ppe_extract FAILED [%]: %', v_state, v_msg;
        END;
      END IF;

    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT;
      v_fail := v_fail + 1;
      v_failed := v_failed || mv;
      INSERT INTO gold_ch.refresh_failure_log(mv_name, sqlstate, sqlerrm, context)
      VALUES (mv, v_state, v_msg, 'refresh_daily_matviews');
      RAISE WARNING 'refresh_daily_matviews: % FAILED [%]: %', mv, v_state, v_msg;
    END;
  END LOOP;
  succeeded := v_ok; failed := v_fail; failed_names := v_failed;
  RETURN NEXT;
END;
$function$

;
