-- signed divergence sum, computed about (local min corner - offset).
-- Normalising to the min corner keeps coordinates ~O(100) so float error stays tiny;
-- the offset is applied AFTER that, so it is NOT normalised away.
CREATE OR REPLACE FUNCTION bronze_ch.fn_mesh_volume_signed(
  g geometry, dx double precision DEFAULT 0, dy double precision DEFAULT 0, dz double precision DEFAULT 0)
RETURNS double precision LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $fn$
WITH b AS (SELECT ST_Translate(g, dx-ST_XMin(g), dy-ST_YMin(g), dz-ST_ZMin(g)) AS gg),
faces AS (SELECT (ST_Dump(gg)).geom AS f FROM b),
rings AS (SELECT ST_ExteriorRing(f) AS r FROM faces WHERE ST_NPoints(ST_ExteriorRing(f)) >= 4),
tri AS (SELECT ST_PointN(r,1) AS a, ST_PointN(r,i) AS p, ST_PointN(r,i+1) AS q
        FROM rings, LATERAL generate_series(2, ST_NPoints(r)-2) AS i)
SELECT coalesce(sum(
    ( ST_X(a)*(ST_Y(p)*ST_Z(q) - ST_Y(q)*ST_Z(p))
    - ST_Y(a)*(ST_X(p)*ST_Z(q) - ST_X(q)*ST_Z(p))
    + ST_Z(a)*(ST_X(p)*ST_Y(q) - ST_X(q)*ST_Y(p)) ) / 6.0), 0)
FROM tri;
$fn$;

CREATE OR REPLACE FUNCTION bronze_ch.fn_mesh_volume(g geometry)
RETURNS double precision LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$fn$ SELECT abs(bronze_ch.fn_mesh_volume_signed(g,0,0,0)); $fn$;

CREATE OR REPLACE FUNCTION bronze_ch.fn_mesh_is_closed(g geometry, tol double precision DEFAULT 0.01)
RETURNS boolean LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$fn$ SELECT abs( bronze_ch.fn_mesh_volume_signed(g,0,0,0)
               - bronze_ch.fn_mesh_volume_signed(g,977,1291,613) ) <= tol; $fn$;

