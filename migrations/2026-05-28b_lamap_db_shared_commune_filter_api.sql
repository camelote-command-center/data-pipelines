-- ============================================================================
-- Shared commune filter API — lamap_db (CONSUMER)
-- ============================================================================
-- Target DB : lamap_db (project fckdwddgtdbvhzloejni)
-- Supersedes: the per-page RPCs from 2026-05-28_lamap_db_subquartier_filter_contract.sql
--             (get_registre_* / get_sad_*) which are DROPPED here.
--
-- Why
--   The commune filter was wired per page, causing drift (different sort, no
--   canton scoping, transactions stuck on parent grain). Consolidate onto ONE
--   shared, dataset+canton-parameterized API bound to a single FE filter
--   component. Implements:
--     * Item 1 — communes sort strictly alphabetically (no sub-quartier pinning);
--                the four Geneva sub-quartiers fall naturally under "Genève-…".
--     * Item 2 — canton scoping, default GE, with a get_cantons() selector source.
--     * Structural — single API across pages.
--
-- Datasets
--   'registre' (base mv_plots, via mv_registre_commune_options cache) — READY
--   'sad'      (base ref.sad)                                          — READY
--   'transactions' / 'annonces'                                        — GATED
--     (transactions: mv_transactions has no commune_canonical_bfs and the
--      ref.transactions propagation has not landed; annonces: commune still
--      un-canonicalized at the mv_listings grain). Both RAISE until enabled.
--
-- Verified live 2026-05-28:
--   get_commune_options('registre','GE') -> Genève, Genève-Cité, Genève-Eaux-Vives,
--     Genève-Petit-Saconnex, Genève-Plainpalais, Genthod, Onex, Vernier ... (alpha)
--   get_commune_options('sad','GE')      -> 48 communes incl. the 4 sub-quartiers
--   get_facets('registre',66211) / get_facets('sad',66211) -> npa 7, girec 31
--   get_cantons('registre') / ('sad')    -> distinct cantons w/ names + counts
--   get_*('transactions'|'annonces', ...) -> RAISE 'not enabled yet'
-- ============================================================================

BEGIN;

DROP FUNCTION IF EXISTS public.get_registre_commune_options();
DROP FUNCTION IF EXISTS public.get_registre_facets(int);
DROP FUNCTION IF EXISTS public.get_sad_commune_options();
DROP FUNCTION IF EXISTS public.get_sad_facets(int);

-- ----------------------------------------------------------------------------
-- get_cantons(p_dataset) : distinct cantons present in a dataset, for the
-- canton selector. Returns (canton_code, canton_name, n).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_cantons(p_dataset text DEFAULT 'registre')
RETURNS TABLE(canton_code text, canton_name text, n bigint)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = public, ref
AS $$
BEGIN
  IF p_dataset = 'registre' THEN
    RETURN QUERY
      SELECT o.canton_code,
             (SELECT max(c.canton_name) FROM ref.communes c WHERE c.canton_code = o.canton_code),
             sum(o.n)::bigint
      FROM public.mv_registre_commune_options o
      WHERE o.canton_code IS NOT NULL
      GROUP BY o.canton_code ORDER BY 2;
  ELSIF p_dataset = 'sad' THEN
    RETURN QUERY
      SELECT c.canton_code, max(c.canton_name), count(*)::bigint
      FROM ref.sad s JOIN ref.communes c ON c.commune_bfs = s.commune_canonical_bfs
      WHERE c.canton_code IS NOT NULL
      GROUP BY c.canton_code ORDER BY 2;
  ELSE
    RAISE EXCEPTION 'get_cantons: dataset % not enabled yet', p_dataset;
  END IF;
END;
$$;
GRANT EXECUTE ON FUNCTION public.get_cantons(text) TO anon, authenticated, service_role;

-- ----------------------------------------------------------------------------
-- get_commune_options(p_dataset, p_canton DEFAULT 'GE')
--   Canonical communes for a dataset, scoped to a canton, ORDER BY name only.
--   p_canton NULL => all cantons.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_commune_options(p_dataset text, p_canton text DEFAULT 'GE')
RETURNS TABLE(commune_canonical_bfs int, canonical_name text, parent_bfs int,
              is_sub_commune boolean, n bigint)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = public, ref
AS $$
BEGIN
  IF p_dataset = 'registre' THEN
    RETURN QUERY
      SELECT o.commune_canonical_bfs, o.canonical_name, o.parent_bfs, o.is_sub_commune, o.n
      FROM public.mv_registre_commune_options o
      WHERE o.canonical_name IS NOT NULL
        AND (p_canton IS NULL OR o.canton_code = p_canton)
      ORDER BY o.canonical_name;
  ELSIF p_dataset = 'sad' THEN
    RETURN QUERY
      SELECT s.commune_canonical_bfs,
             max(c.commune_name),
             max(c.parent_commune_bfs),
             bool_or(COALESCE(c.is_sub_commune,false)),
             count(*)
      FROM ref.sad s JOIN ref.communes c ON c.commune_bfs = s.commune_canonical_bfs
      WHERE s.commune_canonical_bfs IS NOT NULL
        AND (p_canton IS NULL OR c.canton_code = p_canton)
      GROUP BY s.commune_canonical_bfs
      ORDER BY 2;
  ELSE
    RAISE EXCEPTION 'get_commune_options: dataset % not enabled yet (transactions gated on propagation; annonces gated on canonicalization)', p_dataset;
  END IF;
END;
$$;
GRANT EXECUTE ON FUNCTION public.get_commune_options(text,text) TO anon, authenticated, service_role;

-- ----------------------------------------------------------------------------
-- get_facets(p_dataset, p_commune_bfs) : npa (co-facet) + girec (hierarchical
--   child) for a selected canonical commune, sourced from the address/plot
--   grain (never commune.npa_list). girec is empty for non-GE communes.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_facets(p_dataset text, p_commune_bfs int)
RETURNS TABLE(facet text, value text, label text, n bigint)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = public, ref
AS $$
BEGIN
  IF p_dataset = 'registre' THEN
    RETURN QUERY
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
      )
      SELECT 'npa'::text, n.npa::text, n.npa::text, count(*)
        FROM base b, LATERAL unnest(b.npa) AS n(npa)
        WHERE n.npa IS NOT NULL AND n.npa <> '' GROUP BY n.npa
      UNION ALL
      SELECT 'girec'::text, b.gi_rec_numero, max(b.sous_secteur_nom), count(*)
        FROM base b WHERE b.gi_rec_numero IS NOT NULL AND b.gi_rec_numero <> ''
        GROUP BY b.gi_rec_numero
      ORDER BY 1, 4 DESC, 2;
  ELSIF p_dataset = 'sad' THEN
    RETURN QUERY
      WITH base AS (
        SELECT mp.npa, mp.gi_rec_numero, mp.sous_secteur_nom
        FROM ref.sad s
        JOIN public.mv_plots mp
          ON mp.no_commune_no_parcelle = split_part(s.no_commune_no_parcelle, ';', 1)
        WHERE s.commune_canonical_bfs = p_commune_bfs
      )
      SELECT 'npa'::text, n.npa::text, n.npa::text, count(*)
        FROM base b, LATERAL unnest(b.npa) AS n(npa)
        WHERE n.npa IS NOT NULL AND n.npa <> '' GROUP BY n.npa
      UNION ALL
      SELECT 'girec'::text, b.gi_rec_numero, max(b.sous_secteur_nom), count(*)
        FROM base b WHERE b.gi_rec_numero IS NOT NULL AND b.gi_rec_numero <> ''
        GROUP BY b.gi_rec_numero
      ORDER BY 1, 4 DESC, 2;
  ELSE
    RAISE EXCEPTION 'get_facets: dataset % not enabled yet', p_dataset;
  END IF;
END;
$$;
GRANT EXECUTE ON FUNCTION public.get_facets(text,int) TO anon, authenticated, service_role;

COMMIT;
