-- Apply to lamap_db. Public-facing function (ref not exposed to PostgREST).
-- Returns PDCom zones overlapping a plot identified by egrid.

CREATE OR REPLACE FUNCTION public.get_pdcom_zones_for_plot(p_egrid text)
RETURNS TABLE (
    map_theme text,
    layer_slug text,
    layer_label text,
    overlap_pct numeric,
    source_color text,
    adoption_date date
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public, ref, pg_temp
AS $$
    -- Note: ref.plots.commune_bfs uses FAO municipal codes (e.g. 6612)
    -- while ref.communes.commune_bfs (and ref.pdcom_zones.commune_bfs)
    -- uses federal BFS codes (e.g. 11431). Join through ref.communes
    -- by name to pre-filter spatially to the right commune.
    SELECT
        z.map_theme,
        z.layer_slug,
        z.layer_label,
        ROUND(
            (ST_Area(ST_Intersection(p.geometry, z.geometry)::geography)
             / NULLIF(ST_Area(p.geometry::geography), 0) * 100)::numeric,
            2
        ) AS overlap_pct,
        z.source_color,
        z.adoption_date
    FROM ref.plots p
    JOIN ref.communes c ON c.commune_name = p.commune_name
    JOIN ref.pdcom_zones z ON z.commune_bfs = c.commune_bfs
    WHERE p.egrid = p_egrid
      AND ST_Intersects(p.geometry, z.geometry)
    ORDER BY overlap_pct DESC NULLS LAST, z.map_theme, z.layer_slug;
$$;
GRANT EXECUTE ON FUNCTION public.get_pdcom_zones_for_plot(text) TO anon, authenticated, service_role;

-- Register in platform.standards (single source of truth for data access rules).
-- Schema: (category, rule_key, rule_text, correct_call, incorrect_call, applies_to[], severity)
INSERT INTO platform.standards (category, rule_key, rule_text, correct_call, incorrect_call, applies_to, severity)
VALUES (
    'data_access',
    'pdcom_zones_for_plot',
    'Use public.get_pdcom_zones_for_plot(p_egrid) to retrieve PDCom master-plan zones overlapping a plot. Returns theme, slug, label, overlap %, color, adoption date. Data lives in ref.pdcom_zones (WGS84), synced from re-LLM gold_ch.pdcom_zones via the standard sync_registry mechanism. Do not query ref.pdcom_zones directly from the frontend — ref is not PostgREST-exposed.',
    'supabase.rpc(''get_pdcom_zones_for_plot'', { p_egrid: ''CH...'' })',
    'supabase.from(''pdcom_zones'').select(''*'')  -- fails: ref not exposed',
    ARRAY['lamap'],
    'error'
)
ON CONFLICT (rule_key) DO UPDATE
  SET rule_text      = EXCLUDED.rule_text,
      correct_call   = EXCLUDED.correct_call,
      incorrect_call = EXCLUDED.incorrect_call,
      applies_to     = EXCLUDED.applies_to,
      severity       = EXCLUDED.severity,
      updated_at     = now();

NOTIFY pgrst, 'reload schema';
