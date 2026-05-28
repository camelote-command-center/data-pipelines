-- ============================================================================
-- Transactions sub-quartier commune options — lamap_db (CONSUMER)
-- ============================================================================
-- Target DB : lamap_db (project fckdwddgtdbvhzloejni)
-- Depends on: 2026-05-28b_lamap_db_shared_commune_filter_api.sql (shared API)
--
-- Why
--   /app/transactions showed only parent "Genève" in its commune filter while
--   registre + sad show the four sub-quartiers. Root cause: mv_transactions has
--   no commune_canonical_bfs, so its commune source aggregated on raw commune
--   text -> "Genève".
--
-- Decision (2026-05-28): DERIVE NOW, do NOT rebuild mv_transactions.
--   ref.transactions.commune_number carries the cadastral section, and the
--   section->sub-quartier mapping is fixed cadastral fact (66190 + section:
--   21->66211, 22->66212, 23->66213, 24->66214). We derive the canonical bfs in
--   a small cache matview instead of a heavy DROP+CREATE of mv_transactions.
--   The derivation is cast-safe (nested CASE guards the ::int) and idempotent:
--   once the Friday job-43 propagation lands in ref.transactions (moving the
--   23,908 city rows from 6621 to 66211..66214), the same expression yields the
--   identical result. Derived Geneva counts match the upstream v_transactions_full
--   effect exactly: 66211=4575, 66212=7356, 66213=7301, 66214=5769.
--
-- Facets/result caveat (propagation gap):
--   get_facets('transactions', bfs) is name-mapped over gold.transactions_fao,
--   whose commune arrays are still mostly bare "Genève" until propagation. So
--   sub-quartier facet/result COUNTS will be lower than the (complete, derived)
--   option count until Friday; they converge automatically afterwards.
-- ============================================================================

BEGIN;

DROP MATERIALIZED VIEW IF EXISTS public.mv_transactions_commune_options;
CREATE MATERIALIZED VIEW public.mv_transactions_commune_options AS
WITH derived AS (
  SELECT CASE WHEN t.commune_number ~ '^[0-9]+$' THEN
              CASE WHEN t.commune_number::int IN (21,22,23,24)
                    AND t.commune_canonical_bfs IN (6621,66211,66212,66213,66214)
                   THEN 66190 + t.commune_number::int
                   ELSE t.commune_canonical_bfs END
              ELSE t.commune_canonical_bfs END AS eff_bfs
  FROM ref.transactions t
  WHERE t.commune_canonical_bfs IS NOT NULL
)
SELECT d.eff_bfs                        AS commune_canonical_bfs,
       c.commune_name                   AS canonical_name,
       c.parent_commune_bfs             AS parent_bfs,
       COALESCE(c.is_sub_commune,false) AS is_sub_commune,
       c.canton_code                    AS canton_code,
       count(*)                         AS n
FROM derived d
LEFT JOIN ref.communes c ON c.commune_bfs = d.eff_bfs
GROUP BY 1,2,3,4,5
WITH DATA;

CREATE UNIQUE INDEX mv_transactions_commune_options_pk
  ON public.mv_transactions_commune_options (commune_canonical_bfs);
GRANT SELECT ON public.mv_transactions_commune_options TO anon, authenticated, service_role;

-- The shared get_cantons / get_commune_options / get_facets functions (from the
-- 2026-05-28b migration) gain a 'transactions' branch:
--   * get_cantons('transactions')          -> over the cache (GE-only FAO dataset)
--   * get_commune_options('transactions',c) -> over the cache, alphabetical
--   * get_facets('transactions', bfs)       -> name-mapped over gold.transactions_fao
-- (Re-applied here so the repo captures the live bodies.)

CREATE OR REPLACE FUNCTION public.get_cantons(p_dataset text DEFAULT 'registre')
RETURNS TABLE(canton_code text, canton_name text, n bigint)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = public, ref
AS $$
BEGIN
  IF p_dataset = 'registre' THEN
    RETURN QUERY SELECT o.canton_code,
        (SELECT max(c.canton_name) FROM ref.communes c WHERE c.canton_code=o.canton_code), sum(o.n)::bigint
      FROM public.mv_registre_commune_options o WHERE o.canton_code IS NOT NULL GROUP BY o.canton_code ORDER BY 2;
  ELSIF p_dataset = 'transactions' THEN
    RETURN QUERY SELECT o.canton_code,
        (SELECT max(c.canton_name) FROM ref.communes c WHERE c.canton_code=o.canton_code), sum(o.n)::bigint
      FROM public.mv_transactions_commune_options o WHERE o.canton_code IS NOT NULL GROUP BY o.canton_code ORDER BY 2;
  ELSIF p_dataset = 'sad' THEN
    RETURN QUERY SELECT c.canton_code, max(c.canton_name), count(*)::bigint
      FROM ref.sad s JOIN ref.communes c ON c.commune_bfs=s.commune_canonical_bfs
      WHERE c.canton_code IS NOT NULL GROUP BY c.canton_code ORDER BY 2;
  ELSE RAISE EXCEPTION 'get_cantons: dataset % not enabled yet', p_dataset; END IF;
END; $$;
GRANT EXECUTE ON FUNCTION public.get_cantons(text) TO anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.get_commune_options(p_dataset text, p_canton text DEFAULT 'GE')
RETURNS TABLE(commune_canonical_bfs int, canonical_name text, parent_bfs int, is_sub_commune boolean, n bigint)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = public, ref
AS $$
BEGIN
  IF p_dataset = 'registre' THEN
    RETURN QUERY SELECT o.commune_canonical_bfs,o.canonical_name,o.parent_bfs,o.is_sub_commune,o.n
      FROM public.mv_registre_commune_options o
      WHERE o.canonical_name IS NOT NULL AND (p_canton IS NULL OR o.canton_code=p_canton) ORDER BY o.canonical_name;
  ELSIF p_dataset = 'transactions' THEN
    RETURN QUERY SELECT o.commune_canonical_bfs,o.canonical_name,o.parent_bfs,o.is_sub_commune,o.n
      FROM public.mv_transactions_commune_options o
      WHERE o.canonical_name IS NOT NULL AND (p_canton IS NULL OR o.canton_code=p_canton) ORDER BY o.canonical_name;
  ELSIF p_dataset = 'sad' THEN
    RETURN QUERY SELECT s.commune_canonical_bfs,max(c.commune_name),max(c.parent_commune_bfs),
        bool_or(COALESCE(c.is_sub_commune,false)),count(*)
      FROM ref.sad s JOIN ref.communes c ON c.commune_bfs=s.commune_canonical_bfs
      WHERE s.commune_canonical_bfs IS NOT NULL AND (p_canton IS NULL OR c.canton_code=p_canton)
      GROUP BY s.commune_canonical_bfs ORDER BY 2;
  ELSE RAISE EXCEPTION 'get_commune_options: dataset % not enabled yet (annonces gated on canonicalization)', p_dataset; END IF;
END; $$;
GRANT EXECUTE ON FUNCTION public.get_commune_options(text,text) TO anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.get_facets(p_dataset text, p_commune_bfs int)
RETURNS TABLE(facet text, value text, label text, n bigint)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = public, ref
AS $$
DECLARE v_name text;
BEGIN
  IF p_dataset = 'registre' THEN
    RETURN QUERY
      WITH tgt AS (SELECT commune_bfs,commune_name,parent_commune_bfs,is_sub_commune FROM ref.communes WHERE commune_bfs=p_commune_bfs),
      base AS (SELECT mp.npa,mp.gi_rec_numero,mp.sous_secteur_nom FROM public.mv_plots mp, tgt
               WHERE (tgt.is_sub_commune AND mp.commune_bfs=tgt.parent_commune_bfs AND mp.commune_name=tgt.commune_name)
                  OR (NOT tgt.is_sub_commune AND mp.commune_bfs=tgt.commune_bfs))
      SELECT 'npa'::text,n.npa::text,n.npa::text,count(*) FROM base b,LATERAL unnest(b.npa) n(npa)
        WHERE n.npa IS NOT NULL AND n.npa<>'' GROUP BY n.npa
      UNION ALL
      SELECT 'girec'::text,b.gi_rec_numero,max(b.sous_secteur_nom),count(*) FROM base b
        WHERE b.gi_rec_numero IS NOT NULL AND b.gi_rec_numero<>'' GROUP BY b.gi_rec_numero
      ORDER BY 1,4 DESC,2;
  ELSIF p_dataset = 'sad' THEN
    RETURN QUERY
      WITH base AS (SELECT mp.npa,mp.gi_rec_numero,mp.sous_secteur_nom FROM ref.sad s
        JOIN public.mv_plots mp ON mp.no_commune_no_parcelle=split_part(s.no_commune_no_parcelle,';',1)
        WHERE s.commune_canonical_bfs=p_commune_bfs)
      SELECT 'npa'::text,n.npa::text,n.npa::text,count(*) FROM base b,LATERAL unnest(b.npa) n(npa)
        WHERE n.npa IS NOT NULL AND n.npa<>'' GROUP BY n.npa
      UNION ALL
      SELECT 'girec'::text,b.gi_rec_numero,max(b.sous_secteur_nom),count(*) FROM base b
        WHERE b.gi_rec_numero IS NOT NULL AND b.gi_rec_numero<>'' GROUP BY b.gi_rec_numero
      ORDER BY 1,4 DESC,2;
  ELSIF p_dataset = 'transactions' THEN
    SELECT commune_name INTO v_name FROM ref.communes WHERE commune_bfs=p_commune_bfs;
    RETURN QUERY
      WITH base AS (SELECT t.no_postal, t.sous_secteurs FROM gold.transactions_fao t
                    WHERE v_name IS NOT NULL AND t.communes && ARRAY[v_name])
      SELECT 'npa'::text, p.npa::text, p.npa::text, count(*) FROM base b,LATERAL unnest(b.no_postal) p(npa)
        WHERE p.npa IS NOT NULL AND p.npa::text<>'' GROUP BY p.npa
      UNION ALL
      SELECT 'girec'::text, s.ss, s.ss, count(*) FROM base b,LATERAL unnest(b.sous_secteurs) s(ss)
        WHERE s.ss IS NOT NULL AND s.ss<>'' GROUP BY s.ss
      ORDER BY 1,4 DESC,2;
  ELSE RAISE EXCEPTION 'get_facets: dataset % not enabled yet', p_dataset; END IF;
END; $$;
GRANT EXECUTE ON FUNCTION public.get_facets(text,int) TO anon, authenticated, service_role;

COMMIT;

-- Refresh wiring: public.refresh_mv_transactions() gains a best-effort
-- REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_transactions_commune_options
-- (applied live; see that function's body).
