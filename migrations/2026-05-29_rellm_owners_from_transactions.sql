-- ============================================================================
-- Owner-from-transactions — re-LLM PRODUCER (Branch B: ported upstream)
-- ============================================================================
-- Target DB : re-LLM (project znrvddgmczdqoucmykij)  [producer]
-- Consumer  : lamap_db (fckdwddgtdbvhzloejni) via existing v_plots_full -> plots push
-- Status    : Phase 2 build COMPLETE & LIVE; V1 canary passed 2026-05-29.
--             Phase 3 (lamap_db legacy decommission: cron job 450, gold.plots_registry,
--             legacy silver.entities dedup) NOT done — gated on sign-off (checkpoint c).
--
-- Problem
--   "Latest buyer becomes the new owner" was applied on lamap_db into the orphaned
--   gold.plots_registry (dead after the v2 warehouse split). Frontend reads owners
--   from mv_plots/ref.*, fed by the re-LLM push, which still carried the RF cadastral
--   owner. => owners were stale (showed sellers).
--
-- Fix (this migration, on re-LLM)
--   Two-path owner model upstream, feeding the existing plots push (no new path):
--     * silver_ch.plot_owner_overrides  (egrid-keyed transaction-derived owners)
--     * silver_ch.plots_missing_owners  (Path B RF-fallback worklist)
--     * silver_ch.populate_plot_owner_overrides(p_egrid) — latest tx per egrid,
--       SELLER-MATCH GUARD (override only when latest tx seller ⊆ current RF owner),
--       buyers resolved via REUSED silver_ch.fn_resolve_buyer_to_entity (tiered,
--       normalized, legal-form-aware, never blind-inserts); exact/fuzzy->link,
--       create->create entity (NOT EXISTS guard, idempotent), ambiguous/defer->
--       keep name + enqueue Path B. Parties read from event_transactions.
--       buyers_display/sellers_display (transaction_party_links only ~10% backfilled).
--     * silver_ch.retire_plot_owner_overrides() — retire superseded (newer tx).
--     * gold_ch.v_plots_full: override-wins COALESCE on the 4 owner cols + a fresh
--       updated_at = GREATEST(cp.updated_at, ov.max_updated) watermark so the lamap
--       delta upsert_mv_plots picks overridden egrids up. core_plots UNTOUCHED.
--
-- Verified live 2026-05-29 (canary 17/209 egrid CH185965638963, tx 2025/12050/0):
--   v_plots_full/ref.plots/mv_plots owners_display='DHR SA'; get_plot_ownership_
--   timeline_v2('17/209') current_owners={DHR SA}, is_current_owner=t on 2025-12-17.
--   Backfill: 1,344 egrids overridden / 1,936 active rows (1,620 entity-linked,
--   316 name-only -> Path B) / 739 entities created. Idempotent (run3 = 0/0/0; V8 0 new).
--   Push COALESCE verified owner cols EXCLUDED-wins (no revert, no TRUNCATE of ref.plots).
--
-- NOTE: the plots push (gold_ch.sync_full_refresh, monthly cron job 6) is the delivery
--   path. Add the re-LLM cron chain FAO -> retire -> populate -> push (not in this file).
-- ============================================================================

BEGIN;

-- ---- Step A: storage --------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver_ch.plot_owner_overrides (
  egrid              text NOT NULL,
  old_owner_name     text,
  old_entity_id      uuid,
  new_owner_name     text NOT NULL,
  new_entity_id      uuid,
  transaction_id     text,
  transaction_date   date,
  status             text NOT NULL DEFAULT 'active',
  confidence         numeric,
  resolver_decision  text,
  confirmed_at       timestamptz,
  created_at         timestamptz NOT NULL DEFAULT now(),
  updated_at         timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (egrid, new_owner_name),
  CONSTRAINT plot_owner_overrides_status_chk CHECK (status IN ('active','retired'))
);
CREATE INDEX IF NOT EXISTS idx_plot_owner_overrides_egrid_active
  ON silver_ch.plot_owner_overrides(egrid) WHERE status='active';

CREATE TABLE IF NOT EXISTS silver_ch.plots_missing_owners (
  egrid           text NOT NULL,
  buyer_name      text NOT NULL,
  transaction_id  text,
  reason          text NOT NULL,
  status          text NOT NULL DEFAULT 'pending',
  created_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (egrid, buyer_name),
  CONSTRAINT plots_missing_owners_reason_chk CHECK (reason IN ('ambiguous','unmatched')),
  CONSTRAINT plots_missing_owners_status_chk CHECK (status IN ('pending','rf_confirmed'))
);

-- ---- Step B: producer functions --------------------------------------------
CREATE OR REPLACE FUNCTION silver_ch.populate_plot_owner_overrides(p_egrid text DEFAULT NULL)
RETURNS TABLE(overrides_upserted int, entities_created int, enqueued_missing int, egrids_guard_passed int)
LANGUAGE plpgsql
SET search_path TO 'silver_ch','public','pg_catalog'
SET statement_timeout TO '1200s'
AS $fn$
DECLARE
  v_over int := 0; v_ent int := 0; v_miss int := 0; v_eg int := 0;
BEGIN
  -- 1) latest qualifying transaction per egrid (achat with both buyers & sellers)
  CREATE TEMP TABLE _latest ON COMMIT DROP AS
  SELECT DISTINCT ON (lt.egrid)
         lt.egrid, t.transaction_id, t.transaction_date, t.publication_date,
         t.buyers_display, t.sellers_display
  FROM silver_ch.link_plot_transactions lt
  JOIN silver_ch.event_transactions t ON t.transaction_id = lt.transaction_id
  WHERE (p_egrid IS NULL OR lt.egrid = p_egrid)
    AND t.transaction_type = 'achat'
    AND COALESCE(t.buyers_display,'') <> ''
    AND COALESCE(t.sellers_display,'') <> ''
  ORDER BY lt.egrid, t.transaction_date DESC NULLS LAST, t.publication_date DESC NULLS LAST;

  -- 2) seller-match guard: >=1 seller's normalized words are a subset of a current RF owner's words
  CREATE TEMP TABLE _guard ON COMMIT DROP AS
  WITH sellers AS (
    SELECT l.egrid, l.transaction_id, l.transaction_date, l.buyers_display,
           array_remove(string_to_array(regexp_replace(lower(public.unaccent(btrim(s))),'[^a-z0-9 ]',' ','g'),' '),'') AS seller_words
    FROM _latest l, unnest(string_to_array(l.sellers_display, ', ')) s
  ),
  owners AS (
    SELECT o.egrid, o.owner_name, o.entity_id,
           array_remove(string_to_array(regexp_replace(lower(public.unaccent(o.owner_name_normalized)),'[^a-z0-9 ]',' ','g'),' '),'') AS owner_words
    FROM silver_ch.link_plot_owners o
    WHERE p_egrid IS NULL OR o.egrid = p_egrid
  )
  SELECT DISTINCT ON (s.egrid)
         s.egrid, s.transaction_id, s.transaction_date, s.buyers_display,
         o.owner_name AS old_owner_name, o.entity_id AS old_entity_id
  FROM sellers s
  JOIN owners o ON o.egrid = s.egrid
   AND array_length(s.seller_words,1) >= 1
   AND s.seller_words <@ o.owner_words
  ORDER BY s.egrid, o.owner_name;
  GET DIAGNOSTICS v_eg = ROW_COUNT;

  -- 3) explode buyers and resolve each via the shared tiered resolver
  CREATE TEMP TABLE _resolved ON COMMIT DROP AS
  SELECT g.egrid, g.transaction_id, g.transaction_date, g.old_owner_name, g.old_entity_id,
         btrim(b) AS buyer_name,
         r.decision, r.entity_id AS resolved_entity_id, r.confidence,
         (silver_ch.fn_normalize_owner_name(btrim(b))).is_person AS is_person
  FROM _guard g,
       unnest(string_to_array(g.buyers_display, ', ')) b
       CROSS JOIN LATERAL silver_ch.fn_resolve_buyer_to_entity(btrim(b)) r
  WHERE btrim(b) <> '';

  -- 4) create entities for confident 'create' decisions (idempotent: NOT EXISTS guard)
  WITH to_create AS (
    SELECT DISTINCT buyer_name,
           CASE WHEN is_person THEN 'person' ELSE 'company' END AS et,
           lower(public.unaccent(buyer_name)) AS nn
    FROM _resolved WHERE decision = 'create'
  ), ins AS (
    INSERT INTO silver_ch.entities
      (entity_id, entity_type, canonical_name, name_normalized, name_normalized_unaccented,
       legal_form_class, status, source_tables, first_seen_at, last_seen_at, created_at, updated_at)
    SELECT gen_random_uuid(), tc.et, tc.buyer_name, tc.nn, tc.nn,
           (silver_ch.fn_normalize_owner_name(tc.buyer_name)).legal_form_class,
           'active', ARRAY['plot_owner_overrides'], now(), now(), now(), now()
    FROM to_create tc
    WHERE NOT EXISTS (SELECT 1 FROM silver_ch.entities e WHERE e.entity_type = tc.et AND e.name_normalized = tc.nn)
    RETURNING 1
  ) SELECT count(*) INTO v_ent FROM ins;

  -- 5) attach entity_id for create decisions (now resolvable by normalized name + type)
  UPDATE _resolved r SET resolved_entity_id = e.entity_id
  FROM silver_ch.entities e
  WHERE r.decision = 'create' AND r.resolved_entity_id IS NULL
    AND e.name_normalized = lower(public.unaccent(r.buyer_name))
    AND e.entity_type = CASE WHEN r.is_person THEN 'person' ELSE 'company' END;

  -- 6) upsert active overrides (name always kept; entity_id NULL for ambiguous/defer)
  WITH up AS (
    INSERT INTO silver_ch.plot_owner_overrides
      (egrid, old_owner_name, old_entity_id, new_owner_name, new_entity_id,
       transaction_id, transaction_date, status, confidence, resolver_decision, updated_at)
    SELECT egrid, old_owner_name, old_entity_id, buyer_name, resolved_entity_id,
           transaction_id, transaction_date, 'active', confidence, decision, now()
    FROM _resolved
    ON CONFLICT (egrid, new_owner_name) DO UPDATE SET
      new_entity_id     = EXCLUDED.new_entity_id,
      old_owner_name    = EXCLUDED.old_owner_name,
      old_entity_id     = EXCLUDED.old_entity_id,
      transaction_id    = EXCLUDED.transaction_id,
      transaction_date  = EXCLUDED.transaction_date,
      status            = 'active',
      confidence        = EXCLUDED.confidence,
      resolver_decision = EXCLUDED.resolver_decision,
      updated_at        = now()
    WHERE silver_ch.plot_owner_overrides.new_entity_id   IS DISTINCT FROM EXCLUDED.new_entity_id
       OR silver_ch.plot_owner_overrides.transaction_id  IS DISTINCT FROM EXCLUDED.transaction_id
       OR silver_ch.plot_owner_overrides.status          <> 'active'
       OR silver_ch.plot_owner_overrides.resolver_decision IS DISTINCT FROM EXCLUDED.resolver_decision
    RETURNING 1
  ) SELECT count(*) INTO v_over FROM up;

  -- 7) retire stale active overrides for processed egrids (buyer no longer in latest tx)
  UPDATE silver_ch.plot_owner_overrides o SET status='retired', updated_at=now()
  WHERE o.status='active'
    AND o.egrid IN (SELECT egrid FROM _guard)
    AND NOT EXISTS (SELECT 1 FROM _resolved r WHERE r.egrid=o.egrid AND r.buyer_name=o.new_owner_name);

  -- 8) Path B worklist: ambiguous/unmatched buyers (name retained, not force-assigned)
  WITH mq AS (
    INSERT INTO silver_ch.plots_missing_owners (egrid, buyer_name, transaction_id, reason, status)
    SELECT egrid, buyer_name, transaction_id,
           CASE WHEN decision='ambiguous_defer' THEN 'ambiguous' ELSE 'unmatched' END, 'pending'
    FROM _resolved WHERE decision IN ('ambiguous_defer','defer')
    ON CONFLICT (egrid, buyer_name) DO NOTHING
    RETURNING 1
  ) SELECT count(*) INTO v_miss FROM mq;

  overrides_upserted := v_over; entities_created := v_ent; enqueued_missing := v_miss; egrids_guard_passed := v_eg;
  RETURN NEXT;
END;
$fn$;

CREATE OR REPLACE FUNCTION silver_ch.retire_plot_owner_overrides()
RETURNS integer
LANGUAGE plpgsql
SET search_path TO 'silver_ch','public','pg_catalog'
SET statement_timeout TO '600s'
AS $fn$
DECLARE v_n int;
BEGIN
  -- retire active overrides whose transaction_id is not the egrid's current latest qualifying achat
  WITH latest AS (
    SELECT DISTINCT ON (lt.egrid) lt.egrid, t.transaction_id
    FROM silver_ch.link_plot_transactions lt
    JOIN silver_ch.event_transactions t ON t.transaction_id = lt.transaction_id
    WHERE t.transaction_type='achat' AND COALESCE(t.buyers_display,'')<>'' AND COALESCE(t.sellers_display,'')<>''
    ORDER BY lt.egrid, t.transaction_date DESC NULLS LAST, t.publication_date DESC NULLS LAST
  )
  UPDATE silver_ch.plot_owner_overrides o SET status='retired', updated_at=now()
  FROM latest l
  WHERE o.egrid = l.egrid AND o.status='active' AND o.transaction_id IS DISTINCT FROM l.transaction_id;
  GET DIAGNOSTICS v_n = ROW_COUNT;
  RETURN v_n;
END;
$fn$;

-- ---- Step C: injection (gold_ch.v_plots_full owner override) ----------------
CREATE OR REPLACE VIEW gold_ch.v_plots_full AS
 WITH lineage_agg AS (
         SELECT v.egrid,
            array_agg(DISTINCT v.no_commune_no_parcelle_histo) FILTER (WHERE (v.no_commune_no_parcelle_histo IS NOT NULL)) AS historical_parcelles
           FROM gold_ch.v_plot_lineage_full v
          WHERE (v.daterad > '19000101'::text)
          GROUP BY v.egrid
        ), ov_agg AS (
         -- 2026-05-29 owner-from-transactions override (silver_ch.plot_owner_overrides, active rows).
         -- Override-wins over RF (core_plots) for the 4 owner columns; same shapes. core_plots untouched.
         SELECT o.egrid,
            (count(*))::integer AS owner_count,
            string_agg(o.new_owner_name, ' | '::text ORDER BY o.new_owner_name) AS owners_display,
            jsonb_agg(jsonb_build_object('name', o.new_owner_name, 'entity_id', o.new_entity_id, 'dob', NULL::date, 'type', 'transaction_derived'::text) ORDER BY o.new_owner_name) AS owners,
            string_agg(o.new_owner_name, ' '::text) AS owner_names_search,
            max(o.updated_at) AS max_updated
           FROM silver_ch.plot_owner_overrides o
          WHERE o.status = 'active'
          GROUP BY o.egrid
        )
 SELECT cp.egrid,
    cp.canton_code,
    cp.canton_name,
    cp.commune_bfs,
    cp.commune_name,
    cp.gi_rec_numero,
    cp.sous_secteur_nom,
    cp.parcel_number,
    cp.surface_m2,
    cp.geometry,
    cp.centroid,
    cp.lv95_e,
    cp.lv95_n,
    cp.wgs84_lat,
    cp.wgs84_lng,
    cp.building_count,
    cp.dwelling_count,
    cp.construction_year_oldest,
    cp.construction_year_newest,
    cp.max_floors,
    cp.footprint_total_m2,
    cp.volume_total_m3,
    cp.primary_category,
    cp.primary_class,
    cp.primary_heater_energy,
    cp.main_address,
    cp.main_postal_code,
    cp.main_locality,
    cp.addresses,
    cp.addresses_display,
    cp.address_search,
    cp.buildings,
    cp.buildings_summary,
    cp.search_all,
    COALESCE(ov.owner_count, cp.owner_count) AS owner_count,
    COALESCE(ov.owners_display, cp.owners_display) AS owners_display,
    COALESCE(ov.owners, cp.owners) AS owners,
    COALESCE(ov.owner_names_search, cp.owner_names_search) AS owner_names_search,
    cp.last_transaction_date,
    cp.last_transaction_price,
    cp.last_transaction_type,
    cp.active_listing_count,
    cp.sad_count,
    GREATEST(cp.updated_at, ov.max_updated) AS updated_at,
    ext_ge.rdppf_zone_primary,
    ext_ge.rdppf_zone_synthetic,
    ext_ge.rdppf_zone_protected,
    ext_ge.rdppf_zone_restricted,
    ext_ge.rdppf_plq,
    ext_ge.rdppf_polluted_site,
    ext_ge.rdppf_forest_distance,
    ext_ge.rdppf_groundwater_protect,
    ext_ge.rdppf_pdf_url,
    COALESCE(ext_ge.servitude_count, ext_vd.servitude_count) AS servitude_count,
    COALESCE(ext_ge.servitude_genres, ext_vd.servitude_genres) AS servitude_genres,
    COALESCE(ext_ge.servitude_classes, ext_vd.servitude_classes) AS servitude_classes,
    COALESCE(ext_ge.ddp_count, ext_vd.ddp_count) AS ddp_count,
    COALESCE(ext_ge.ddp_numbers, ext_vd.ddp_numbers) AS ddp_numbers,
    COALESCE(ext_ge.ddp_total_surface_m2, ext_vd.ddp_total_surface_m2) AS ddp_total_surface_m2,
    COALESCE(ext_ge.is_ddp, ext_vd.is_ddp) AS is_ddp,
    ext_ge.is_ppe,
    ext_ge.ppe_statuses,
    COALESCE(ext_ge.densification_types, ext_vd.densification_types) AS densification_types,
    COALESCE(ext_ge.densification_practice_urls, ext_vd.densification_practice_urls) AS densification_practice_urls,
    COALESCE(ext_ge.densification_sectors, ext_vd.densification_sectors) AS densification_sectors,
    app.appartenance,
    'ch'::character(2) AS country_code,
    rc.canton_code AS admin1_code,
    rc.canton_name AS admin1_name,
    NULL::text AS admin2_code,
    NULL::text AS admin2_name,
    (rc.canonical_bfs)::text AS admin3_code,
    rc.canonical_name AS admin3_name,
    (rc.canonical_bfs)::text AS admin3_canonical_id,
    ((COALESCE((cp.commune_bfs)::text, cp.canton_code) || '-'::text) || cp.parcel_number) AS parcel_universal_id,
    scp.no_commune_no_parcelle,
    la.historical_parcelles,
    pi.zone_primaire,
    pi.zone_ius_standard,
    pi.zone_ius_max,
    pi.ius_hpe,
    pi.ius_thpe,
    pi.ius_realistic_ceiling,
    pi.ius_legal_ceiling,
    pi.ius_dero_status,
    pi.solde_pct_legal,
    pi.solde_pct_standard,
    pi.surface_brut_de_plancher_hors_sol_m2,
    pi.surface_potentielle_m2,
    pi.surface_residuelle_m2,
    pi.surface_potentielle_legal_m2,
    pi.score_surface,
    pi.pool_surface_m2,
    pi.has_pool,
    pi.has_veranda,
    pi.nearest_transport_m,
    pi.nearest_school_m,
    pi.nearest_supermarket_m,
    pi.nearest_pharmacy_m,
    pi.nearest_restaurant_m,
    pi.amenities_score,
    pi.heating_type,
    pi.permits_count,
    pi.lien_rf,
    pi.densification_zone,
    pi.rdppf_pnp_zone,
    ext_vd.noise_sensitivity_degree,
    ext_vd.noise_sensitivity_source,
    ext_vd.isos_count,
    ext_vd.isos_categories,
    ext_vd.isos_perimeter_count,
    ext_vd.classement_count,
    ext_vd.classement_descriptions,
    ext_vd.classement_fiche_urls,
    ext_vd.jardins_count,
    ext_vd.jardins_descriptions,
    ext_vd.archeology_region_count,
    ext_vd.archeology_site_count,
    ext_vd.archeology_record_types,
    ext_vd.energy_demand_kwh_vd,
    ext_vd.heating_solution_target,
    ext_vd.energy_source_vd,
    ext_vd.building_destination,
    ext_vd.construction_year AS construction_year_vd
   FROM ((((((((gold_ch.core_plots cp
     LEFT JOIN silver_ch.cadastral_plots scp ON ((scp.egrid = cp.egrid)))
     LEFT JOIN lineage_agg la ON ((la.egrid = cp.egrid)))
     LEFT JOIN ov_agg ov ON ((ov.egrid = cp.egrid)))
     LEFT JOIN gold_ch.core_plots_ext_ge ext_ge ON ((ext_ge.egrid = cp.egrid)))
     LEFT JOIN gold_ch.core_plots_ext_vd ext_vd ON ((ext_vd.egrid = cp.egrid)))
     LEFT JOIN silver_ch.ref_commune_appartenance_ge app ON (((app.commune_bfs = cp.commune_bfs) AND (cp.canton_code = 'GE'::text))))
     LEFT JOIN silver_ch.ref_communes rc ON ((rc.canonical_bfs = cp.commune_bfs)))
     LEFT JOIN silver_ch.plot_intel_ge pi ON ((pi.egrid = cp.egrid)));

COMMIT;
