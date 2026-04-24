-- v0.2: add source_url + source_page_number to ref.pdcom_zones + update public function.
-- Apply to lamap_db (fckdwddgtdbvhzloejni).

ALTER TABLE ref.pdcom_zones
    ADD COLUMN IF NOT EXISTS source_url text,
    ADD COLUMN IF NOT EXISTS source_page_number integer;

DROP FUNCTION IF EXISTS public.get_pdcom_zones_for_plot(text);

CREATE FUNCTION public.get_pdcom_zones_for_plot(p_egrid text)
RETURNS TABLE (
    map_theme text,
    layer_slug text,
    layer_label text,
    overlap_pct numeric,
    source_color text,
    adoption_date date,
    source_url text,
    source_page_number integer
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
        z.adoption_date,
        z.source_url,
        z.source_page_number
    FROM ref.plots p
    JOIN ref.communes c ON c.commune_name = p.commune_name
    JOIN ref.pdcom_zones z ON z.commune_bfs = c.commune_bfs
    WHERE p.egrid = p_egrid
      AND ST_Intersects(p.geometry, z.geometry)
    ORDER BY overlap_pct DESC NULLS LAST, z.map_theme, z.layer_slug;
$$;
GRANT EXECUTE ON FUNCTION public.get_pdcom_zones_for_plot(text) TO anon, authenticated, service_role;

-- Update platform.standards row to reflect new return columns.
UPDATE platform.standards
SET rule_text = 'Use public.get_pdcom_zones_for_plot(p_egrid) to retrieve PDCom master-plan zones overlapping a plot. Returns theme, slug, label, overlap %, color, adoption date, source_url (PDCom PDF), source_page_number. Data lives in ref.pdcom_zones (WGS84), synced from re-LLM gold_ch.pdcom_zones via the standard sync_registry mechanism. Do not query ref.pdcom_zones directly from the frontend — ref is not PostgREST-exposed.',
    updated_at = now()
WHERE rule_key = 'pdcom_zones_for_plot';

NOTIFY pgrst, 'reload schema';
