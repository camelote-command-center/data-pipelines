-- APPLY ON lamap_db. One call per click in the photomesh 3D view: what is at this point?
-- The SITG photomesh (I3S) carries no attributes, so identity comes from the position:
--   building = footprint containing the point (public.buildings_geo) + roof height (ref.building_roof_heights)
--   tree     = nearest ref.plot_trees trunk whose crown (radius + 0.5 m) covers the point, only when NOT on a building
--   plot     = resolve_plot_by_point (same as the 2D map)
-- kind: 'building' | 'tree' | 'ground'. Heights are the 2025 measurements (SITG height model), not the 2019 mesh.
CREATE OR REPLACE FUNCTION public.get_3d_feature_at(p_lon double precision, p_lat double precision)
RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = public, ref, pg_catalog
SET plan_cache_mode = force_custom_plan
AS $$
DECLARE
  pt geometry := ST_Transform(ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326), 2056);
  b jsonb; t jsonb; p jsonb;
BEGIN
  SELECT jsonb_build_object('egid', g.egid, 'height_m', h.height_m, 'height_source', h.height_source)
    INTO b
  FROM public.buildings_geo g
  LEFT JOIN ref.building_roof_heights h ON h.egid = g.egid
  WHERE g.geometry && pt AND ST_Contains(g.geometry, pt)
  ORDER BY ST_Area(g.geometry) ASC LIMIT 1;

  IF b IS NULL THEN
    SELECT jsonb_build_object('height_m', round(t0.height_m::numeric, 2), 'crown_m', round(2 * t0.crown_radius_m::numeric, 2),
                              'lon', ST_X(ST_Transform(t0.geom, 4326)), 'lat', ST_Y(ST_Transform(t0.geom, 4326)),
                              'egrid', t0.egrid, 'source', t0.source)
      INTO t
    FROM ref.plot_trees t0
    WHERE t0.geom && ST_Expand(pt, 10) AND ST_DWithin(t0.geom, pt, t0.crown_radius_m + 0.5)
    ORDER BY ST_Distance(t0.geom, pt) / (t0.crown_radius_m + 0.5) ASC LIMIT 1;
  END IF;

  SELECT to_jsonb(r) INTO p FROM public.resolve_plot_by_point(p_lon, p_lat) r;

  RETURN jsonb_build_object('kind', CASE WHEN b IS NOT NULL THEN 'building' WHEN t IS NOT NULL THEN 'tree' ELSE 'ground' END,
                            'building', b, 'tree', t, 'plot', p);
END $$;
REVOKE ALL ON FUNCTION public.get_3d_feature_at(double precision, double precision) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.get_3d_feature_at(double precision, double precision) TO anon, authenticated, service_role;

INSERT INTO platform.standards (category, rule_key, rule_text, correct_call, incorrect_call, applies_to, severity)
SELECT 'map_layers', 'get_3d_feature_at_rpc',
 'Click identity in the 3D view (SITG photomesh has no attributes): public.get_3d_feature_at(p_lon, p_lat) -> jsonb {kind building|tree|ground, building {egid, height_m, height_source}, tree {height_m, crown_m, lon, lat, egrid, source}, plot (resolve_plot_by_point row)}. SECURITY DEFINER, EXECUTE anon+authenticated+service_role. Reads public.buildings_geo, ref.building_roof_heights, ref.plot_trees (SITG height model 2025, source mna-2025). Heights are 2025 measurements, not read off the 2019 mesh (the mesh reads trees 0.6-2 m low). Pass the lon/lat of scene.pickPosition, not of the screen ray on the ellipsoid.',
 'const { data } = await supabase.rpc(''get_3d_feature_at'', { p_lon, p_lat }); // data.kind, data.tree?.height_m, data.building?.egid, data.plot?.egrid',
 'reading heights from the mesh for trees; separate RPC calls per click', '{lamap}', 'info'
WHERE NOT EXISTS (SELECT 1 FROM platform.standards WHERE rule_key = 'get_3d_feature_at_rpc');
