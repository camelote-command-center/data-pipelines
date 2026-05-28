-- ============================================================================
-- Petites-annonces (listings) commune canonicalization — re-llm ROOT FIX
-- ============================================================================
-- Target DB : re-llm (project znrvddgmczdqoucmykij)  [canonical source]
--
-- Problem
--   /app/petites-annonces showed garbage commune values ("Geneva", "1532 Fétigny
--   FR", "6986 Curio TI"). The listings feed bypassed silver_ch.resolve_commune:
--   gold_ch.v_listings_full derived admin3 purely by joining core_listings.city
--   to silver_ch.ref_city_lookup, which left 52% of active listings unresolved
--   (114,798 / 219,183) and let raw scraped strings through downstream.
--
-- Root fix (this migration)
--   1. Enhance silver_ch.resolve_commune with a pre-normalization step:
--        - strip a trailing bare canton token ("Fetigny FR" -> hint canton FR)
--          (only when the 2-letter token is a real canton code),
--        - strip a leading 4-digit NPA ("1532 Fetigny" -> "Fetigny"),
--      then run the existing exact/alias/strip-(XX)/trigram/log pipeline.
--   2. Add the English alias "Geneva" -> 6621 in silver_ch.ref_communes
--      (aliases + aliases_norm). ("Geneve" already resolved via unaccent.)
--   3. Persistent resolution cache silver_ch.listing_city_resolution, populated
--      by resolve_commune for distinct listing cities NOT covered by
--      ref_city_lookup; refreshed by silver_ch.refresh_listing_city_resolution()
--      (route new ingestion through this).
--   4. NPA fallback matview silver_ch.ref_npa_top_commune (top commune per NPA by
--      population) for free-text listings whose city won't resolve but NPA is
--      present. Geneva NPAs -> 6621 parent (the correct free-text fallback; no
--      section is known, so never a sub-quartier).
--   5. gold_ch.v_listings_full now COALESCEs the three tiers:
--        ref_city_lookup -> listing_city_resolution -> ref_npa_top_commune.
--
-- Effect (verified live 2026-05-28): active-listing admin3 coverage 48% -> 98.6%
--   (216,090 / 219,183). "1532 Fétigny FR"->Fétigny, "6986 Curio TI"->Curio,
--   "Geneva"/"GENEVE"->Genève. Distinct-city recovery on the previously
--   unresolved set ~41% (city) + NPA fallback closes most of the rest.
--   Unresolved inputs continue to log to silver_ch.commune_resolution_review
--   (the alias-curation feedback loop).
--
-- Downstream (NOT done here):
--   * The re-llm -> lamap_db listings sync must run to propagate the improved
--     admin3 into lamap ref.listings (then v_commune_canonical_gaps('annonces')
--     drops from ~127k to ~1.4%). Do NOT trigger the full sync ad hoc — it has
--     destructive branches; let the scheduled job run.
--   * lamap shared filter API already has an 'annonces' branch reading
--     ref.listings.admin3_canonical_id (clean; excludes NULL/garbage).
--   * Wire refresh_listing_city_resolution() + ref_npa_top_commune refresh into
--     the listings ingestion cron (left to the pipeline owner).
-- ============================================================================

BEGIN;

-- 1 + 2 applied live: see silver_ch.resolve_commune body (pre-parse step 0) and
-- the ref_communes update below.
UPDATE silver_ch.ref_communes
SET aliases      = (SELECT array_agg(DISTINCT x) FROM unnest(aliases || ARRAY['Geneva']) x),
    aliases_norm = (SELECT array_agg(DISTINCT public.unaccent(lower(x))) FROM unnest(aliases || ARRAY['Geneva']) x)
WHERE canonical_bfs = 6621
  AND NOT ('Geneva' = ANY(aliases));

-- 3. resolution cache + refresh function
CREATE TABLE IF NOT EXISTS silver_ch.listing_city_resolution (
  city_norm       text PRIMARY KEY,
  canonical_bfs   integer,
  canonical_name  text,
  canton_code     text,
  canton_name     text,
  resolved_at     timestamptz NOT NULL DEFAULT now()
);
GRANT SELECT ON silver_ch.listing_city_resolution TO anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION silver_ch.refresh_listing_city_resolution()
RETURNS integer
LANGUAGE plpgsql SECURITY DEFINER
SET search_path TO 'silver_ch','public','pg_catalog'
SET statement_timeout TO '600s'
AS $$
DECLARE v_added integer;
BEGIN
  WITH cand AS (
    SELECT DISTINCT l.city AS raw_city, silver_ch.unaccent_lower(lower(l.city)) AS city_norm
    FROM gold_ch.core_listings l
    WHERE l.city IS NOT NULL AND btrim(l.city) <> ''
  ),
  ins AS (
    SELECT DISTINCT ON (c.city_norm) c.city_norm, rc.canonical_bfs, rc.canonical_name, rc.canton_code, rc.canton_name
    FROM cand c
    LEFT JOIN silver_ch.ref_city_lookup cl ON cl.city_norm = c.city_norm
    LEFT JOIN silver_ch.listing_city_resolution lr ON lr.city_norm = c.city_norm
    CROSS JOIN LATERAL (SELECT silver_ch.resolve_commune(c.raw_city) AS bfs) r
    LEFT JOIN silver_ch.ref_communes rc ON rc.canonical_bfs = r.bfs
    WHERE cl.canonical_bfs IS NULL AND lr.city_norm IS NULL AND r.bfs IS NOT NULL
    ORDER BY c.city_norm
  )
  INSERT INTO silver_ch.listing_city_resolution(city_norm,canonical_bfs,canonical_name,canton_code,canton_name)
  SELECT city_norm,canonical_bfs,canonical_name,canton_code,canton_name FROM ins
  ON CONFLICT (city_norm) DO NOTHING;
  GET DIAGNOSTICS v_added = ROW_COUNT;
  RETURN v_added;
END;
$$;
-- backfill: SELECT silver_ch.refresh_listing_city_resolution();  (added 2791 rows live)

-- 4. NPA -> top commune (by population) fallback
CREATE MATERIALIZED VIEW IF NOT EXISTS silver_ch.ref_npa_top_commune AS
SELECT npa,
       (array_agg(canonical_bfs  ORDER BY population DESC NULLS LAST))[1] AS canonical_bfs,
       (array_agg(canonical_name ORDER BY population DESC NULLS LAST))[1] AS canonical_name,
       (array_agg(canton_code    ORDER BY population DESC NULLS LAST))[1] AS canton_code,
       (array_agg(canton_name    ORDER BY population DESC NULLS LAST))[1] AS canton_name
FROM silver_ch.ref_communes c, LATERAL unnest(c.npa_list) AS u(npa)
WHERE NULLIF(c.canonical_name,'') IS NOT NULL AND NOT COALESCE(c.is_sub_commune,false)
GROUP BY npa
WITH DATA;
CREATE UNIQUE INDEX IF NOT EXISTS ref_npa_top_commune_pk ON silver_ch.ref_npa_top_commune(npa);
GRANT SELECT ON silver_ch.ref_npa_top_commune TO anon, authenticated, service_role;

-- 5. v_listings_full: COALESCE city-lookup -> resolution-cache -> npa-fallback
CREATE OR REPLACE VIEW gold_ch.v_listings_full AS
 SELECT l.listing_id, l.source, l.source_id, l.title, l.description,
    COALESCE(l.url, gbu.best_url) AS url,
    l.offer_type, l.property_type, l.property_type_raw, l.price, l.price_unit,
    l.rent_net, l.rent_charges, l.rent_gross, l.currency, l.rooms,
    l.surface_living_m2, l.surface_usable_m2, l.surface_land_m2, l.floor,
    l.npa, l.city, l.street, l.canton, l.latitude, l.longitude, l.agency_name,
    l.construction_year, l.renovation_year, l.images, l.features, l.is_furnished,
    l.publishing_status, l.is_active, l.first_seen_at, l.last_seen_at,
    l.source_priority, l.dedup_group_hash, l.price_per_m2, l.has_price,
    l.has_coordinates, l.room_category, l.price_category, l.fts_listing,
    l.geometry, l.updated_at,
    'ch'::character(2) AS country_code,
    COALESCE(cl.canton_code, lcr.canton_code, npc.canton_code)             AS admin1_code,
    COALESCE(cl.canton_name, lcr.canton_name, npc.canton_name)             AS admin1_name,
    NULL::text AS admin2_code,
    NULL::text AS admin2_name,
    (COALESCE(cl.canonical_bfs, lcr.canonical_bfs, npc.canonical_bfs))::text AS admin3_code,
    COALESCE(cl.canonical_name, lcr.canonical_name, npc.canonical_name)      AS admin3_name,
    (COALESCE(cl.canonical_bfs, lcr.canonical_bfs, npc.canonical_bfs))::text AS admin3_canonical_id
   FROM ((((gold_ch.core_listings l
     LEFT JOIN silver_ch.ref_city_lookup cl ON cl.city_norm = silver_ch.unaccent_lower(lower(l.city)))
     LEFT JOIN silver_ch.listing_city_resolution lcr ON lcr.city_norm = silver_ch.unaccent_lower(lower(l.city)))
     LEFT JOIN silver_ch.ref_npa_top_commune npc ON npc.npa = l.npa)
     LEFT JOIN silver_ch.listing_group_best_url gbu ON gbu.dedup_group_hash = l.dedup_group_hash);

COMMIT;

-- ----------------------------------------------------------------------------
-- silver_ch.resolve_commune enhanced body (applied live; captured for the repo):
-- adds step 0 (strip trailing canton token -> hint; strip leading NPA) before
-- the existing exact/alias/strip-(XX)/trigram/log steps. See live definition.
-- ----------------------------------------------------------------------------
