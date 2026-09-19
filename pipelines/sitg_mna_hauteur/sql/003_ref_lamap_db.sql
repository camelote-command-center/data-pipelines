-- APPLY ON lamap_db (BEFORE 002's FDW push runs). PURE-REF read path for building roof heights.
CREATE TABLE IF NOT EXISTS ref.building_roof_heights (
  egid bigint PRIMARY KEY, no_commune integer, height_m numeric(6,2), height_source text NOT NULL,
  mna_p95_m numeric(6,2), mna_max_m numeric(6,2), bati3d_height_m numeric(6,2), roof_area_m2 numeric(12,2),
  footprints integer NOT NULL, source_vintage text NOT NULL, computed_at timestamptz NOT NULL
);
COMMENT ON TABLE ref.building_roof_heights IS 'FDW copy of gold_ch.building_roof_heights on re-LLM (daily, gold_ch.sync_building_roof_heights). Never write here.';
GRANT SELECT ON ref.building_roof_heights TO anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.get_building_roof_height(p_egid bigint) RETURNS jsonb
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public, pg_temp AS $$
  SELECT to_jsonb(t) FROM (
    SELECT b.egid, b.height_m, b.height_source, b.mna_p95_m, b.mna_max_m, b.bati3d_height_m, b.roof_area_m2,
           b.footprints, b.source_vintage, b.computed_at
    FROM ref.building_roof_heights b WHERE b.egid = p_egid) t;
$$;
REVOKE ALL ON FUNCTION public.get_building_roof_height(bigint) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.get_building_roof_height(bigint) TO anon, authenticated, service_role;

INSERT INTO platform.standards (category, rule_key, rule_text, correct_call, incorrect_call, applies_to, severity)
VALUES ('rpc_contract', 'building_roof_height_rpc',
 'Roof height of a Geneva building (m above ground) is read via public.get_building_roof_height(p_egid bigint) -> jsonb. SECURITY DEFINER, search_path=public,pg_temp, EXECUTE to anon+authenticated+service_role. PURE-REF: reads ONLY ref.building_roof_heights, the daily FDW copy of gold_ch.building_roof_heights on re-LLM (source of truth; never write to ref). height_m = SITG 20 cm height model 2025 (p95 of the roof); where it disagrees with bati3d LOD2 by >10 m (4.6 %, mostly sheds under tree crowns) or reads < 2 m, bati3d is used and height_source = ''bati3d''; with neither, height_m is NULL and height_source = ''none''. The 3D city tiles carry the same value as the roof_height_m feature property.',
 'const { data } = await supabase.rpc(''get_building_roof_height'', { p_egid }); // data.height_m, data.height_source (''mna_2025''|''bati3d''|''none''); null = no measurement',
 '.from(''building_roof_heights'') on ref, or computing heights client-side from 3D tiles', '{lamap}', 'info')
ON CONFLICT DO NOTHING;
