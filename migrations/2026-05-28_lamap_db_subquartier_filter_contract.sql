-- ============================================================================
-- Geneva sub-quartier filter contract — lamap_db (CONSUMER)
-- ============================================================================
-- Target DB : lamap_db (project fckdwddgtdbvhzloejni)
-- Scope     : Uniform commune-options + sub-facets API for the filter UI of
--             two pages: /app/registre-proprietaires and /app/sad.
--
-- Why
--   These pages showed "Genève" as a single commune and their cascading
--   postcode / GIREC filters came up empty for it. Root cause: filters read
--   from the wrong grain — postcode was fed from ref.communes.npa_list (NULL by
--   design for the four sub-quartiers) and GIREC was keyed on the parent /
--   commune name, which never matches a sub-quartier.
--
--   Policy (mirrors the gold_ch.v_transactions_full canonicalization): for the
--   City of Geneva, resolve to one of the four cadastral sub-quartiers whenever
--   the section is known, falling back to commune Genève (6621) only when not:
--     66211 Genève-Cité | 66212 Genève-Eaux-Vives
--     66213 Genève-Petit-Saconnex | 66214 Genève-Plainpalais  (parent 6621)
--
-- Design
--   * Every facet level is driven from the address/plot grain, NEVER from the
--     commune reference table's npa_list.
--   * GIREC -> sub-quartier is a true hierarchy (id_girec rolls up to a cadastral
--     section = sub-quartier) -> a genuine cascade child.
--   * NPA -> sub-quartier is many-to-many (NPAs straddle boundaries) -> treat as
--     a co-facet that narrows results, NOT a strict nested child.
--   * GIREC is Geneva-specific: get_*_facets() returns an empty girec list for
--     non-GE communes (UI disables that facet when empty).
--
-- API (uniform per page)
--   RPC 1  get_<page>_commune_options()
--          RETURNS (commune_canonical_bfs int, canonical_name text,
--                   parent_bfs int, is_sub_commune bool, n bigint)
--   RPC 2  get_<page>_facets(p_commune_bfs int)
--          RETURNS (facet text, value text, label text, n bigint)
--          facet in ('npa','girec')
--
-- Definition of done (verified live 2026-05-28):
--   get_registre_commune_options() -> 66211..66214 present (n 2986/1895/2775/2635)
--   get_registre_facets(66211)     -> npa 7 values, girec 31 values
--   get_sad_commune_options()      -> 66211..66214 present (n 18595/11397/13939/15947)
--   get_sad_facets(66211)          -> npa 7 values, girec 31 values
--   Timings: registre opts 2ms / facets 24ms; sad opts 62ms / facets 207ms.
--
-- NOTE: the transactions page (/app/transactions) is intentionally NOT covered
--   here — ref.transactions stays Genève-only until the Friday job-43
--   propagation and mv_transactions has no commune_canonical_bfs yet. Do not
--   wire its filters until both are resolved (open product decision #1).
-- ============================================================================

BEGIN;

-- ----------------------------------------------------------------------------
-- registre-proprietaires : base = public.mv_plots (3.7M rows)
-- ----------------------------------------------------------------------------
-- Geneva-city plots have commune_bfs=6621 with the sub-quartier name carried in
-- commune_name ('Genève-Cité', ...). The options aggregate over the full plots
-- matview is ~40s, so it is cached in a small matview refreshed alongside the
-- other plot-derived matviews. Facets are a live, fast filtered scan.

DROP MATERIALIZED VIEW IF EXISTS public.mv_registre_commune_options;
CREATE MATERIALIZED VIEW public.mv_registre_commune_options AS
WITH derived AS (
  SELECT COALESCE(sub.commune_bfs, mp.commune_bfs)                   AS commune_canonical_bfs,
         COALESCE(sub.commune_name, c.commune_name, mp.commune_name) AS canonical_name,
         COALESCE(sub.parent_commune_bfs, c.parent_commune_bfs)      AS parent_bfs,
         COALESCE(sub.is_sub_commune, c.is_sub_commune, false)       AS is_sub_commune,
         mp.canton_code
  FROM public.mv_plots mp
  LEFT JOIN ref.communes c   ON c.commune_bfs = mp.commune_bfs
  LEFT JOIN ref.communes sub ON mp.commune_bfs = 6621
                            AND sub.parent_commune_bfs = 6621
                            AND sub.commune_name = mp.commune_name
  WHERE mp.commune_bfs IS NOT NULL
)
SELECT commune_canonical_bfs,
       max(canonical_name)     AS canonical_name,
       max(parent_bfs)         AS parent_bfs,
       bool_or(is_sub_commune) AS is_sub_commune,
       max(canton_code)        AS canton_code,
       count(*)                AS n
FROM derived
GROUP BY commune_canonical_bfs
WITH DATA;

-- Unique index required for REFRESH ... CONCURRENTLY.
CREATE UNIQUE INDEX mv_registre_commune_options_pk
  ON public.mv_registre_commune_options (commune_canonical_bfs);

GRANT SELECT ON public.mv_registre_commune_options TO anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.get_registre_commune_options()
RETURNS TABLE(commune_canonical_bfs int, canonical_name text, parent_bfs int,
              is_sub_commune boolean, n bigint)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = public, ref
AS $$
  SELECT commune_canonical_bfs, canonical_name, parent_bfs, is_sub_commune, n
  FROM public.mv_registre_commune_options
  WHERE canonical_name IS NOT NULL
  ORDER BY canonical_name;
$$;
GRANT EXECUTE ON FUNCTION public.get_registre_commune_options() TO anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.get_registre_facets(p_commune_bfs int)
RETURNS TABLE(facet text, value text, label text, n bigint)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = public, ref
AS $$
  WITH tgt AS (
    SELECT commune_bfs, commune_name, parent_commune_bfs, is_sub_commune
    FROM ref.communes WHERE commune_bfs = p_commune_bfs
  ),
  base AS (
    SELECT mp.npa, mp.gi_rec_numero, mp.sous_secteur_nom
    FROM public.mv_plots mp, tgt
    WHERE (tgt.is_sub_commune
             AND mp.commune_bfs = tgt.parent_commune_bfs
             AND mp.commune_name = tgt.commune_name)
       OR (NOT tgt.is_sub_commune AND mp.commune_bfs = tgt.commune_bfs)
  ),
  npa_facet AS (
    SELECT 'npa'::text AS facet, n.npa::text AS value, n.npa::text AS label, count(*) AS n
    FROM base b, LATERAL unnest(b.npa) AS n(npa)
    WHERE n.npa IS NOT NULL AND n.npa <> ''
    GROUP BY n.npa
  ),
  girec_facet AS (
    SELECT 'girec'::text AS facet, b.gi_rec_numero AS value,
           max(b.sous_secteur_nom) AS label, count(*) AS n
    FROM base b
    WHERE b.gi_rec_numero IS NOT NULL AND b.gi_rec_numero <> ''
    GROUP BY b.gi_rec_numero
  )
  SELECT * FROM npa_facet
  UNION ALL
  SELECT * FROM girec_facet
  ORDER BY facet, n DESC, value;
$$;
GRANT EXECUTE ON FUNCTION public.get_registre_facets(int) TO anon, authenticated, service_role;

-- Append the options-cache refresh to the existing plot-derived refresh job.
-- (Full function re-declared so the repo captures the live definition.)
CREATE OR REPLACE FUNCTION public.refresh_mv_plots_derived()
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET statement_timeout TO '1200s'
 SET search_path TO 'public'
AS $function$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_plot_owners;
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_plot_owners_autocomplete;
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_plot_communes;
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_plot_addresses;
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_plot_buildings;
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_plot_units;
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_plot_listings;
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_plot_transactions_agg;
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_registre_commune_options;
END; $function$;

-- ----------------------------------------------------------------------------
-- sad : base = ref.sad (245k rows) — already carries commune_canonical_bfs
-- ----------------------------------------------------------------------------
-- Options aggregate directly over ref.sad (fast with the new index). Facets join
-- ref.sad -> mv_plots on the first parcel of a possibly multi-parcel reference
-- ('21/1731;1572;7707' -> '21/1731') to pick up npa + girec from the plot grain.

CREATE INDEX IF NOT EXISTS idx_ref_sad_commune_canonical_bfs
  ON ref.sad (commune_canonical_bfs);

CREATE OR REPLACE FUNCTION public.get_sad_commune_options()
RETURNS TABLE(commune_canonical_bfs int, canonical_name text, parent_bfs int,
              is_sub_commune boolean, n bigint)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = public, ref
AS $$
  SELECT s.commune_canonical_bfs,
         max(c.commune_name)                       AS canonical_name,
         max(c.parent_commune_bfs)                 AS parent_bfs,
         bool_or(COALESCE(c.is_sub_commune,false)) AS is_sub_commune,
         count(*)                                  AS n
  FROM ref.sad s
  JOIN ref.communes c ON c.commune_bfs = s.commune_canonical_bfs
  WHERE s.commune_canonical_bfs IS NOT NULL
  GROUP BY s.commune_canonical_bfs
  ORDER BY 2;
$$;
GRANT EXECUTE ON FUNCTION public.get_sad_commune_options() TO anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.get_sad_facets(p_commune_bfs int)
RETURNS TABLE(facet text, value text, label text, n bigint)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = public, ref
AS $$
  WITH base AS (
    SELECT mp.npa, mp.gi_rec_numero, mp.sous_secteur_nom
    FROM ref.sad s
    JOIN public.mv_plots mp
      ON mp.no_commune_no_parcelle = split_part(s.no_commune_no_parcelle, ';', 1)
    WHERE s.commune_canonical_bfs = p_commune_bfs
  ),
  npa_facet AS (
    SELECT 'npa'::text AS facet, n.npa::text AS value, n.npa::text AS label, count(*) AS n
    FROM base b, LATERAL unnest(b.npa) AS n(npa)
    WHERE n.npa IS NOT NULL AND n.npa <> ''
    GROUP BY n.npa
  ),
  girec_facet AS (
    SELECT 'girec'::text AS facet, b.gi_rec_numero AS value,
           max(b.sous_secteur_nom) AS label, count(*) AS n
    FROM base b
    WHERE b.gi_rec_numero IS NOT NULL AND b.gi_rec_numero <> ''
    GROUP BY b.gi_rec_numero
  )
  SELECT * FROM npa_facet
  UNION ALL
  SELECT * FROM girec_facet
  ORDER BY facet, n DESC, value;
$$;
GRANT EXECUTE ON FUNCTION public.get_sad_facets(int) TO anon, authenticated, service_role;

COMMIT;
