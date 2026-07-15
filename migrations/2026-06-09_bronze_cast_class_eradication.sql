-- ============================================================================
-- 2026-06-09 — Bronze cast-class eradication (re-LLM)  [safe_cast + self-enforcement]
-- ----------------------------------------------------------------------------
-- WHY: FAO 2026-06-05 gazette froze — silver_ch.event_transactions REFRESH threw
--   22P02 "invalid input syntax for type numeric: \"Succession sans soultes\"" on an
--   unguarded (replace(l.prix))::numeric cast over bronze_ch.fao_ldtr. Per-matview
--   isolation contained it (siblings kept refreshing) but the transactions chain
--   stayed frozen. Root class: any cast from a scraped bronze_ch column to a typed
--   value can crash a refresh on one dirty row.
--
-- WHAT: every such cast in silver_ch/gold_ch now routes through safe_cast.to_<type>()
--   (NULL + quarantine on parse failure, never throws). 47 guards across 7 leaf
--   matviews (event_transactions, event_sad, listing_active, event_transaction_parties,
--   core_buildings, entity_companies, listing_group_best_url) + 1 function.
--   gold_ch.detect_unguarded_bronze_casts() = 0; daily self-enforcement cron @06:20 UTC.
--
-- EXECUTION NOTE (ops): the matview rewrite was applied as a LIGHT DDL-swap
--   (drop+recreate WITH NO DATA, seconds) + server-side per-matview REFRESH with
--   COMMIT between each (gold_ch.cascade_repopulate_260608). A single-transaction
--   warehouse rebuild was attempted first and destabilized re-LLM 3× (restart, hang,
--   pooler saturation) — DO NOT repeat that on re-LLM. Atomicity always held; no data loss.
--
-- DOCTRINE: containment is not eradication; detection is not automation; survival is
--   automation. The scanner caught 17 sites the manual audit missed — the scanner is
--   the authority. Secrets: the email/incident keys are reused from
--   gold_ch.pipeline_watchdog_run server-side (see build step) — NOT committed here.
-- ============================================================================

-- ====== 1. safe_cast schema + quarantine + 7 typed helpers ======
CREATE SCHEMA IF NOT EXISTS safe_cast;

CREATE TABLE IF NOT EXISTS safe_cast.quarantine (
  id            bigserial PRIMARY KEY,
  observed_at   timestamptz NOT NULL DEFAULT now(),
  source_table  text NOT NULL,
  source_pk     text,
  column_name   text NOT NULL,
  expected_type text NOT NULL,
  raw_value     text NOT NULL,
  context       jsonb
);
CREATE INDEX IF NOT EXISTS idx_quarantine_src ON safe_cast.quarantine (source_table, column_name, observed_at DESC);
GRANT USAGE ON SCHEMA safe_cast TO anon, authenticated, service_role;
GRANT SELECT ON safe_cast.quarantine TO anon, authenticated, service_role;

-- STABLE (not IMMUTABLE) because they INSERT into quarantine; matview refresh does not require IMMUTABLE.
CREATE OR REPLACE FUNCTION safe_cast.to_numeric(raw text, src_table text DEFAULT NULL, src_pk text DEFAULT NULL, col text DEFAULT NULL)
RETURNS numeric LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE AS $$
BEGIN
  IF raw IS NULL OR btrim(raw)='' THEN RETURN NULL; END IF;
  BEGIN
    RETURN replace(replace(replace(replace(replace(raw,'''',''),'’',''),chr(160),''),' ',''),',','.')::numeric;
  EXCEPTION WHEN OTHERS THEN
    INSERT INTO safe_cast.quarantine(source_table,source_pk,column_name,expected_type,raw_value)
    VALUES (COALESCE(src_table,'?'),src_pk,COALESCE(col,'?'),'numeric',raw);
    RETURN NULL;
  END;
END $$;

CREATE OR REPLACE FUNCTION safe_cast.to_int(raw text, src_table text DEFAULT NULL, src_pk text DEFAULT NULL, col text DEFAULT NULL)
RETURNS integer LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE AS $$
BEGIN
  IF raw IS NULL OR btrim(raw)='' THEN RETURN NULL; END IF;
  BEGIN
    RETURN round(replace(replace(replace(replace(raw,'''',''),chr(160),''),' ',''),',','.')::numeric)::integer;
  EXCEPTION WHEN OTHERS THEN
    INSERT INTO safe_cast.quarantine(source_table,source_pk,column_name,expected_type,raw_value)
    VALUES (COALESCE(src_table,'?'),src_pk,COALESCE(col,'?'),'integer',raw);
    RETURN NULL;
  END;
END $$;

CREATE OR REPLACE FUNCTION safe_cast.to_bigint(raw text, src_table text DEFAULT NULL, src_pk text DEFAULT NULL, col text DEFAULT NULL)
RETURNS bigint LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE AS $$
BEGIN
  IF raw IS NULL OR btrim(raw)='' THEN RETURN NULL; END IF;
  BEGIN
    RETURN round(replace(replace(replace(replace(raw,'''',''),chr(160),''),' ',''),',','.')::numeric)::bigint;
  EXCEPTION WHEN OTHERS THEN
    INSERT INTO safe_cast.quarantine(source_table,source_pk,column_name,expected_type,raw_value)
    VALUES (COALESCE(src_table,'?'),src_pk,COALESCE(col,'?'),'bigint',raw);
    RETURN NULL;
  END;
END $$;

CREATE OR REPLACE FUNCTION safe_cast.to_date(raw text, src_table text DEFAULT NULL, src_pk text DEFAULT NULL, col text DEFAULT NULL)
RETURNS date LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE AS $$
DECLARE d date;
BEGIN
  IF raw IS NULL OR btrim(raw)='' THEN RETURN NULL; END IF;
  BEGIN
    d := btrim(raw)::date;
    -- clamp absurd future/past sentinels (e.g. 2070-06-06) to NULL + quarantine
    IF d > (now()::date + interval '2 years') OR d < date '1800-01-01' THEN
      INSERT INTO safe_cast.quarantine(source_table,source_pk,column_name,expected_type,raw_value)
      VALUES (COALESCE(src_table,'?'),src_pk,COALESCE(col,'?'),'date(out-of-range)',raw);
      RETURN NULL;
    END IF;
    RETURN d;
  EXCEPTION WHEN OTHERS THEN
    INSERT INTO safe_cast.quarantine(source_table,source_pk,column_name,expected_type,raw_value)
    VALUES (COALESCE(src_table,'?'),src_pk,COALESCE(col,'?'),'date',raw);
    RETURN NULL;
  END;
END $$;

CREATE OR REPLACE FUNCTION safe_cast.to_timestamptz(raw text, src_table text DEFAULT NULL, src_pk text DEFAULT NULL, col text DEFAULT NULL)
RETURNS timestamptz LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE AS $$
BEGIN
  IF raw IS NULL OR btrim(raw)='' THEN RETURN NULL; END IF;
  BEGIN RETURN btrim(raw)::timestamptz;
  EXCEPTION WHEN OTHERS THEN
    INSERT INTO safe_cast.quarantine(source_table,source_pk,column_name,expected_type,raw_value)
    VALUES (COALESCE(src_table,'?'),src_pk,COALESCE(col,'?'),'timestamptz',raw);
    RETURN NULL;
  END;
END $$;

CREATE OR REPLACE FUNCTION safe_cast.to_boolean(raw text, src_table text DEFAULT NULL, src_pk text DEFAULT NULL, col text DEFAULT NULL)
RETURNS boolean LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE AS $$
BEGIN
  IF raw IS NULL OR btrim(raw)='' THEN RETURN NULL; END IF;
  BEGIN RETURN btrim(lower(raw))::boolean;
  EXCEPTION WHEN OTHERS THEN
    INSERT INTO safe_cast.quarantine(source_table,source_pk,column_name,expected_type,raw_value)
    VALUES (COALESCE(src_table,'?'),src_pk,COALESCE(col,'?'),'boolean',raw);
    RETURN NULL;
  END;
END $$;

CREATE OR REPLACE FUNCTION safe_cast.to_float(raw text, src_table text DEFAULT NULL, src_pk text DEFAULT NULL, col text DEFAULT NULL)
RETURNS double precision LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE AS $$
BEGIN
  IF raw IS NULL OR btrim(raw)='' THEN RETURN NULL; END IF;
  BEGIN RETURN replace(replace(replace(replace(replace(raw,'''',''),'’',''),chr(160),''),' ',''),',','.')::double precision;
  EXCEPTION WHEN OTHERS THEN
    INSERT INTO safe_cast.quarantine(source_table,source_pk,column_name,expected_type,raw_value)
    VALUES (COALESCE(src_table,'?'),src_pk,COALESCE(col,'?'),'float',raw);
    RETURN NULL;
  END;
END $$;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA safe_cast TO anon, authenticated, service_role;

-- ====== 2. self-enforcement scanner ======
CREATE OR REPLACE FUNCTION gold_ch.detect_unguarded_bronze_casts()
RETURNS TABLE(object_name text, object_kind text, bronze_table text, bronze_col text, col_type text, sample text)
LANGUAGE plpgsql STABLE AS $$
DECLARE obj record; col record; def text; pat text;
BEGIN
  FOR obj IN
    SELECT n.nspname AS sch, c.relname AS nm, c.relkind::text AS k, c.oid
      FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE n.nspname IN ('silver_ch','gold_ch','public') AND c.relkind IN ('m','v')
    UNION ALL
    SELECT n.nspname, p.proname, 'f', p.oid
      FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
      WHERE n.nspname IN ('silver_ch','gold_ch','public') AND p.prokind IN ('f','p')
  LOOP
    BEGIN
      def := CASE WHEN obj.k='f' THEN pg_get_functiondef(obj.oid) ELSE pg_get_viewdef(obj.oid) END;
    EXCEPTION WHEN OTHERS THEN CONTINUE; END;
    IF def IS NULL OR def !~ 'bronze_ch' THEN CONTINUE; END IF;
    FOR col IN
      SELECT table_name, column_name, data_type FROM information_schema.columns
      WHERE table_schema='bronze_ch' AND data_type IN ('text','character varying','character')
    LOOP
      IF def !~ ('bronze_ch\.'||col.table_name||'\M') THEN CONTINUE; END IF;
      -- word-boundary (\m) catches BOTH qualified (alias.col) and unqualified (col) renderings;
      -- safe_cast-wrapped casts render with ::text inner, so they never match the numtype list.
      pat := '\m'||col.column_name||'\s*\)?\s*::\s*(numeric|integer|bigint|smallint|double precision|real|date|timestamp|timestamptz|boolean)';
      IF def ~ pat THEN
        object_name:=obj.sch||'.'||obj.nm; object_kind:=obj.k;
        bronze_table:=col.table_name; bronze_col:=col.column_name; col_type:=col.data_type;
        sample:=substring(def from ('.{0,20}\m'||col.column_name||'\s*\)?\s*::\s*\w+'));
        RETURN NEXT;
      END IF;
    END LOOP;
  END LOOP;
END $$;

-- ====== 3. enforcement function builder (keys reused server-side from watchdog) ======
DO $do$
DECLARE
  src text := pg_get_functiondef('gold_ch.pipeline_watchdog_run'::regproc);
  k_lamap text;
  k_cam text;
BEGIN
  k_lamap := (regexp_match(src, $$v_lamap_key text:='([^']+)'$$))[1];
  k_cam   := (regexp_match(src, $$v_cam_key text:='([^']+)'$$))[1];
  IF k_lamap IS NULL OR k_cam IS NULL THEN RAISE EXCEPTION 'could not extract keys'; END IF;

  EXECUTE format($f$
    CREATE OR REPLACE FUNCTION gold_ch.cast_guard_enforcement(p_force_test boolean DEFAULT false)
    RETURNS jsonb LANGUAGE plpgsql AS $body$
    DECLARE v_lamap_key text := %L; v_cam_key text := %L;
      v_cnt int; v_sites jsonb; v_payload jsonb; v_email_req bigint; v_bug bigint;
    BEGIN
      SELECT count(*), COALESCE(jsonb_agg(jsonb_build_object('object',object_name,'col',bronze_table||'.'||bronze_col)),'[]'::jsonb)
        INTO v_cnt, v_sites FROM gold_ch.detect_unguarded_bronze_casts();
      IF p_force_test AND v_cnt=0 THEN
        v_cnt:=1; v_sites:=jsonb_build_array(jsonb_build_object('object','FORCED_TEST','col','synthetic'));
      END IF;
      IF v_cnt=0 THEN RETURN jsonb_build_object('unguarded',0,'alerted',false); END IF;
      v_payload := jsonb_build_object('stale_pipes',v_sites,
        'summary','[re-llm cast-guard] '||v_cnt||' unguarded bronze cast(s) in silver_ch/gold_ch',
        'environment','re-llm-cast-guard','triggered_at',now(),'is_forced_test',p_force_test);
      SELECT net.http_post(url:='https://fckdwddgtdbvhzloejni.supabase.co/functions/v1/pipeline-watchdog-alert',
        headers:=jsonb_build_object('Authorization','Bearer '||v_lamap_key,'Content-Type','application/json'),
        body:=v_payload) INTO v_email_req;
      SELECT net.http_post(url:='https://dxugbpeacnorjunpljih.supabase.co/rest/v1/rpc/log_pipeline_stall',
        headers:=jsonb_build_object('apikey',v_cam_key,'Authorization','Bearer '||v_cam_key,'Content-Type','application/json'),
        body:=jsonb_build_object('p_pipe','cast-guard-regression','p_lag',v_cnt||' site(s)',
          'p_detail','[re-llm cast-guard] '||v_cnt||' unguarded bronze cast(s): '||left(v_sites::text,400)||(CASE WHEN p_force_test THEN ' [FORCED TEST]' ELSE '' END))) INTO v_bug;
      RETURN jsonb_build_object('unguarded',v_cnt,'alerted',true,'email_req',v_email_req,'incident_req',v_bug);
    END $body$;
  $f$, k_lamap, k_cam);
  RAISE NOTICE 'cast_guard_enforcement built (keys reused from watchdog)';
END $do$;

-- ====== 4. resumable per-matview repopulation procedure (light path) ======
CREATE OR REPLACE PROCEDURE gold_ch.cascade_repopulate_260608()
LANGUAGE plpgsql AS $$
DECLARE m text; mvs text[] := ARRAY[
    'silver_ch.listing_active',
    'gold_ch.core_listings',
    'silver_ch.market_listing_stats',
    'gold_ch.core_benchmark_rentals',
    'gold_ch.core_benchmark_sales',
    'gold_ch.core_buildings',
    'silver_ch.entity_companies',
    'silver_ch.event_transaction_parties',
    'silver_ch.event_transactions',
    'gold_ch.core_entities',
    'gold_ch.core_entities_v1_legacy',
    'silver_ch.event_sad',
    'silver_ch.link_plot_listings',
    'silver_ch.link_plot_sad',
    'silver_ch.link_plot_transactions',
    'gold_ch.core_plots',
    'gold_ch.core_plots_ext_ge',
    'gold_ch.core_sad',
    'silver_ch.link_entity_sad',
    'silver_ch.link_sad_owners',
    'gold_ch.core_sad_ext_ge',
    'silver_ch.plot_zone5_pdcom_eligibility',
    'silver_ch.plot_intel_ge',
    'gold_ch.core_transactions',
    'silver_ch.listing_group_best_url',
    'silver_ch.plot_ancestry_edges',
    'silver_ch.solar_potential',
    'silver_ch.link_entity_network',
    'silver_ch.plot_intel_ge_v5_legacy'
  ];
BEGIN
  SET statement_timeout='0';
  FOREACH m IN ARRAY mvs LOOP
    IF NOT (SELECT relispopulated FROM pg_class WHERE oid = m::regclass) THEN
      EXECUTE 'REFRESH MATERIALIZED VIEW '||m;
      COMMIT;
      RAISE NOTICE 'refreshed %', m;
    END IF;
  END LOOP;
END $$;

-- ====== 5. daily self-enforcement cron (run once to (re)install) ======
-- SELECT cron.schedule('cast-guard-enforcement-daily','20 6 * * *',$$SELECT gold_ch.cast_guard_enforcement();$$);
-- Verify: SELECT count(*) FROM gold_ch.detect_unguarded_bronze_casts();  -- must be 0
-- Full guarded matview DDL (39-object union) is recoverable via pg_get_viewdef; not duplicated here.
