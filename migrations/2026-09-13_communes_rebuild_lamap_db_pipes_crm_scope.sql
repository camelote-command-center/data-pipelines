-- ============================================================================
-- 2026-09-13 — Rebuild communes; unblock lamap_db addresses + rdppf_pnp; declare the lamap-crm lane
-- ============================================================================
-- Surfaced by gold_ch.distribution_alert_run() on its first run.
--
-- (1) COMMUNES. gold_ch.v_communes_full and gold_ch.core_communes no longer existed (core_communes gone
--     by 2026-06-08, the view after 2026-07-23; no change_log). Every consumer kept the 2026-06-02
--     snapshot. The only archived definition (db51871, 2026-05-08) is known-broken (2026-06-15 cantons
--     attempt: it mixes federal BFS / FAO / GE commune codes). Rebuilt from silver_ch.ref_communes and
--     VALIDATED against the intact 2026-06-02 snapshot on lamap-lbi (lbi_foreign.communes):
--       - 2,135 rows, 0 missing; 18 static columns identical on all rows; geometry and centroid
--         byte-identical (ST_Transform(geometry,4326), ST_PointOnSurface).
--       - transaction_count corr 0.9895, no commune below its June value; listing_count corr 0.997.
--     Deliberate corrections of June defects (documented, not reproduced):
--       - commune text is matched per canton with the deterministic steps of silver_ch.resolve_commune
--         (exact / alias / canton-suffix strip; no trigram guess, no review-queue writes). June matched
--         the exact name only: Carouge, Corsier, Le Grand-Saconnex had 0 transactions and 0 SAD (now 3,058
--         / 1,101 / 2,647 tx); FR homonyms got VD/NE SAD counts (Cugy FR 24 -> 11).
--       - sub-communes roll up into their parent: Genève SAD 0 -> 59,878 (all city permits are filed
--         under Genève-Cité / -Plainpalais / -Petit-Saconnex / -Eaux-Vives).
--       - benchmark_*_m2 from gold_ch.core_benchmark_v2 (commune level, apartment, blended) — the old
--         core_benchmark_sales/rentals no longer exist; June sale values correlate 0.82 with v2.
--       - area_km2 was NULL on every row; now ST_Area of the official geometry.
--     Refresh: cron core-communes-refresh-daily 01:30 UTC (old refresher cron 27 is inactive).
--     Distribution: Branch A upsert on unique commune_bfs (was legacy TRUNCATE) to lamap_db + lamap-lbi.
--     lamap-crm reads communes from lamap_db over FDW, so it follows lamap_db.
--
-- (2) lamap_db addresses: REFUSED since 2026-09-01 — only the lamap-lbi tuple was allowlisted.
--     lamap_db ref.addresses had no unique key: 3,428,649 rows, 0 duplicate egaid ->
--     CREATE UNIQUE INDEX CONCURRENTLY ux_ref_addresses_egaid; Branch A.
--
-- (3) lamap_db cadastral_rdppf_pnp: allowlisted but the registry row had no column_list since
--     2026-05-15 ("Branch A: registry row missing pk_column or column_list"). Filled with the 22 columns
--     common to the source view and the target (identical sets).
--
-- (4) lamap-crm: all 79 registry pairs declared by_design_not_distributed. CRM migration 20260713160000
--     (change_log 9f12a85a) dropped its 30 local ref.* tables and schema ref; CRM reads Lamap data over
--     its own FDW lamap_db_ro. Pushing re-creates nothing and fails every run. Bug ac984b64.
--
-- NO DELETIONS: upserts only; scope rows are never deleted (trigger).
-- ROLLBACK: sync_full_refresh from backup.fn_defs_20260913 'sync_full_refresh@pre-communes';
--   UPDATE gold_ch.sync_target_scope SET distribute=true WHERE target_db='lamap-crm';
--   SELECT cron.unschedule('core-communes-refresh-daily'); the matview/view/staging/index are additive.
-- ============================================================================

-- ---------------------------------------------------------------- lamap_db
-- SET statement_timeout='900s';
-- CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ux_ref_addresses_egaid ON ref.addresses (egaid);
-- CREATE TABLE IF NOT EXISTS ref._staging_addresses (LIKE ref.addresses INCLUDING DEFAULTS);
-- CREATE TABLE IF NOT EXISTS ref._staging_communes (LIKE ref.communes INCLUDING DEFAULTS);
-- CREATE TABLE IF NOT EXISTS ref._staging_cadastral_rdppf_pnp (LIKE ref.cadastral_rdppf_pnp INCLUDING DEFAULTS);
-- ---------------------------------------------------------------- lamap-lbi
-- CREATE TABLE IF NOT EXISTS ref._staging_communes (LIKE ref.communes INCLUDING DEFAULTS);
-- ---------------------------------------------------------------- re-LLM
-- IMPORT FOREIGN SCHEMA ref LIMIT TO (_staging_addresses, _staging_communes, _staging_cadastral_rdppf_pnp) FROM SERVER lamap_db_server INTO lamap_db_foreign;
-- IMPORT FOREIGN SCHEMA ref LIMIT TO (_staging_communes) FROM SERVER lamap_lbi_server INTO lbi_foreign;

CREATE MATERIALIZED VIEW gold_ch.core_communes AS
WITH rc AS (
  SELECT r.*,
         public.unaccent(lower(btrim(r.canonical_name))) AS k_name,
         public.unaccent(lower(btrim(regexp_replace(r.canonical_name, '\s*\([A-Z]{2}\)\s*$', '')))) AS k_stripped
  FROM silver_ch.ref_communes r
), keys AS (
  -- the deterministic steps of silver_ch.resolve_commune (exact name, alias, canton-suffix strip);
  -- no trigram guess and no review-queue side effect
  SELECT canton_code, k_name AS k, canonical_bfs, 1 AS prio FROM rc
  UNION ALL
  SELECT rc.canton_code, public.unaccent(lower(btrim(a.v))), rc.canonical_bfs, 2 FROM rc CROSS JOIN LATERAL unnest(rc.aliases) a(v)
  UNION ALL
  SELECT canton_code, k_stripped, canonical_bfs, 3 FROM rc WHERE k_stripped <> k_name
), key_map AS (
  SELECT DISTINCT ON (canton_code, k) canton_code, k, canonical_bfs
  FROM keys ORDER BY canton_code, k, prio, canonical_bfs
), tx_names AS (
  SELECT canton_code, public.unaccent(lower(btrim(regexp_replace(commune, '\s*\([A-Z]{2}\)\s*$', '')))) AS k, count(*) AS n
  FROM gold_ch.core_transactions WHERE commune IS NOT NULL GROUP BY 1, 2
), sad_names AS (
  SELECT canton_code, public.unaccent(lower(btrim(regexp_replace(commune, '\s*\([A-Z]{2}\)\s*$', '')))) AS k, count(*) AS n
  FROM gold_ch.core_sad WHERE commune IS NOT NULL GROUP BY 1, 2
), tx AS (
  SELECT km.canonical_bfs AS bfs, sum(t.n)::bigint AS n FROM tx_names t JOIN key_map km USING (canton_code, k) GROUP BY 1
), sad AS (
  SELECT km.canonical_bfs AS bfs, sum(s.n)::bigint AS n FROM sad_names s JOIN key_map km USING (canton_code, k) GROUP BY 1
), lst AS (
  -- listings carry an NPA, not a commune: NPA -> commune bridge (an NPA spanning communes counts in each)
  SELECT x.commune_bfs AS bfs, count(*)::bigint AS n
  FROM gold_ch.core_listings cl JOIN silver_ch.ref_npa_commune x ON x.npa = cl.npa
  WHERE cl.npa IS NOT NULL AND x.commune_bfs IS NOT NULL GROUP BY 1
), own AS (
  SELECT rc.canonical_bfs AS bfs, rc.parent_canonical_bfs AS parent,
         coalesce(tx.n, 0) AS tx_n, coalesce(sad.n, 0) AS sad_n, coalesce(lst.n, 0) AS lst_n
  FROM rc LEFT JOIN tx ON tx.bfs = rc.canonical_bfs LEFT JOIN sad ON sad.bfs = rc.canonical_bfs LEFT JOIN lst ON lst.bfs = rc.canonical_bfs
), rolled AS (
  -- a sub-commune (Genève-Cité ...) is part of its parent: the parent counts its own rows plus its children's
  SELECT o.bfs,
         o.tx_n  + coalesce((SELECT sum(c.tx_n)  FROM own c WHERE c.parent = o.bfs), 0) AS tx_n,
         o.sad_n + coalesce((SELECT sum(c.sad_n) FROM own c WHERE c.parent = o.bfs), 0) AS sad_n,
         o.lst_n + coalesce((SELECT sum(c.lst_n) FROM own c WHERE c.parent = o.bfs), 0) AS lst_n
  FROM own o
), bench AS (
  SELECT commune_bfs,
         max(blended_price_m2) FILTER (WHERE deal_type = 'sale' AND property_type = 'apartment') AS sale_m2,
         max(blended_price_m2) FILTER (WHERE deal_type = 'rent' AND property_type = 'apartment') AS rent_m2
  FROM gold_ch.core_benchmark_v2 WHERE level = 'commune' GROUP BY 1
), pop AS (
  SELECT bfs_commune_number AS bfs, max(year) AS pop_year
  FROM silver_ch.market_population WHERE total_population IS NOT NULL GROUP BY 1
)
SELECT
  rc.canonical_bfs                                                        AS commune_bfs,
  regexp_replace(rc.canonical_name, '\s*\([A-Z]{2}\)\s*$', '')            AS commune_name,
  rc.canton_code,
  rc.canton_name,
  rc.npa_list,
  ST_Transform(rc.geometry, 4326)::geometry(MultiPolygon, 4326)           AS geometry,
  ST_PointOnSurface(ST_Transform(rc.geometry, 4326))                      AS centroid,
  coalesce(rc.area_km2, round((ST_Area(rc.geometry) / 1e6)::numeric, 3))  AS area_km2,
  rc.population,
  CASE WHEN rc.population IS NOT NULL THEN pop.pop_year END::integer      AS pop_year,
  r.tx_n::bigint                                                          AS transaction_count,
  r.lst_n::integer                                                        AS listing_count,
  r.sad_n::bigint                                                         AS sad_count,
  r.tx_n  > 0                                                             AS has_transactions,
  r.lst_n > 0                                                             AS has_listings,
  r.sad_n > 0                                                             AS has_sads,
  round(b.sale_m2, 2)                                                     AS benchmark_sale_price_m2,
  round(b.rent_m2, 2)                                                     AS benchmark_rent_m2,
  now()                                                                   AS updated_at,
  'ch'::char(2)                                                           AS country_code,
  rc.canton_code                                                          AS admin1_code,
  rc.canton_name                                                          AS admin1_name,
  NULL::text                                                              AS admin2_code,
  NULL::text                                                              AS admin2_name,
  rc.canonical_bfs::text                                                  AS admin3_code,
  regexp_replace(rc.canonical_name, '\s*\([A-Z]{2}\)\s*$', '')            AS admin3_name,
  rc.canonical_bfs::text                                                  AS admin3_canonical_id,
  rc.aliases,
  rc.appartenance,
  rc.is_sub_commune,
  rc.parent_canonical_bfs                                                 AS parent_commune_bfs
FROM rc
JOIN rolled r ON r.bfs = rc.canonical_bfs
LEFT JOIN bench b ON b.commune_bfs = rc.canonical_bfs
LEFT JOIN pop ON pop.bfs = rc.canonical_bfs;
CREATE UNIQUE INDEX core_communes_bfs_uidx ON gold_ch.core_communes (commune_bfs);
CREATE OR REPLACE VIEW gold_ch.v_communes_full AS SELECT * FROM gold_ch.core_communes;
GRANT SELECT ON gold_ch.core_communes, gold_ch.v_communes_full TO service_role;

BEGIN;
INSERT INTO backup.fn_defs_20260913 (fn, md5, definition)
SELECT 'sync_full_refresh@pre-communes', md5(pg_get_functiondef('gold_ch.sync_full_refresh'::regproc)), pg_get_functiondef('gold_ch.sync_full_refresh'::regproc);

UPDATE gold_ch.sync_registry SET pk_column = 'commune_bfs', allow_legacy_truncate = false
 WHERE source_schema = 'gold_ch' AND source_table = 'v_communes_full' AND target_table = 'communes';
UPDATE gold_ch.sync_registry
   SET column_list = ARRAY['id','no_plan','indice','commune','lieu','nom_zone','dt_adopt','statut_jur','type_plan','lien_loi','lien_plan','lien_sad','lien_reglement','l_dj_comp','l_inf_comp','erebid','site','date_maj','date_dj_co','canton_code','geometry','updated_at']
 WHERE source_schema = 'gold_ch' AND source_table = 'v_rdppf_pnp_full' AND target_table = 'cadastral_rdppf_pnp' AND column_list IS NULL;

-- lamap-crm: the whole push lane is retired by design (CRM migration 20260713160000 dropped schema ref;
-- CRM reads Lamap data over its own FDW lamap_db_ro -> lamap_ref.*). Declare every pair, with evidence.
INSERT INTO gold_ch.sync_target_scope (source_schema, source_table, target_db, distribute, reason, evidence, decided_by)
SELECT r.source_schema, r.source_table, 'lamap-crm', false, 'by_design_not_distributed',
       jsonb_build_object('target_table', r.target_table,
         'decision_change_log', '9f12a85a-21aa-49fd-b9c9-dafe79c27d11',
         'bug', 'ac984b64',
         'note', 'lamap-crm dropped all 30 local ref.* tables + schema ref on 2026-07-13 (P4b); it reads Lamap data live from lamap_db over FDW lamap_db_ro (lamap_ref: communes, filter_options, solar_potential, user_*). Every push to lamap-crm fails with schema "ref" does not exist since 2026-07-14.',
         'verified_at', now()),
       'claude-code (Session System 2.4.3)'
FROM gold_ch.sync_registry r
ON CONFLICT DO NOTHING;

-- communes refresh: the old nightly refresher (cron 27 refresh_gold_warehouse) is inactive
UPDATE gold_ch.refresh_manifest
   SET depends_on = ARRAY['silver_ch.ref_communes','silver_ch.ref_npa_commune','silver_ch.market_population','gold_ch.core_transactions','gold_ch.core_sad','gold_ch.core_listings','gold_ch.core_benchmark_v2'],
       refresh_schedule = '30 1 * * * (cron core-communes-refresh-daily)',
       last_refresh_at = now(), last_refresh_status = 'success', last_row_count = 2135, is_active = true
 WHERE schema_name = 'gold_ch' AND view_name = 'core_communes';
COMMIT;

-- sync_full_refresh: 3 tuples appended to c_allowlist (full definition below)
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
    ARRAY['gold_ch','v_core_entities_enriched','entities','lamap-lbi'],
    -- 2026-09-13: communes rebuilt (gold_ch.core_communes; the old objects were dropped). Unique commune_bfs on both.
    ARRAY['gold_ch','v_communes_full','communes','lamap_db'],
    ARRAY['gold_ch','v_communes_full','communes','lamap-lbi'],
    -- 2026-09-13: addresses -> lamap_db REFUSED since 2026-09-01 (only the LBI tuple was allowlisted).
    -- ux_ref_addresses_egaid created on lamap_db (3,428,649 rows, 0 duplicate egaid).
    ARRAY['gold_ch','v_addresses_full','addresses','lamap_db']  ];
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

SELECT cron.schedule('core-communes-refresh-daily', '30 1 * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY gold_ch.core_communes;');
