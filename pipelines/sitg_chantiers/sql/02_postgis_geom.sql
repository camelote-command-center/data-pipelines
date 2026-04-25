-- PostGIS geometry layer for spatial joins across all bronze_ch datasets.
--
-- ABSOLUTE RULE (portfolio-wide): every dataset that has spatial data
-- MUST expose a real PostGIS `geom geometry(*, 4326)` column with a GIST
-- index, so plots/parcelles can be matched against any other layer
-- (chantiers, SAD permits, transactions, PDCom, etc.).
--
-- Implementation:
--   • bronze_ch.arcgis_to_geom(text)  — converts ArcGIS REST JSON
--     ({x,y} or {rings:...} or {paths:...}) to PostGIS geometry in 4326.
--   • bronze_ch._sync_geom_from_geometry() — trigger that auto-fills
--     `geom` whenever `geometry` (text JSON) is inserted/updated.
--   • Per table: `geom` column + GIST index + trigger.
--   • Backfill in same transaction.
--   • bronze_ch.parcels_intersecting_geom(geometry) — generic spatial
--     join helper usable from any other dataset.
--   • bronze_ch.parcels_affected_by_chantier(text) — convenience wrapper.

-- ──────────────────────────────────────────────────────────────
-- 1. Conversion function: ArcGIS REST JSON → PostGIS geometry
-- ──────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION bronze_ch.arcgis_to_geom(arcgis_json text)
RETURNS geometry
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  j jsonb;
BEGIN
  IF arcgis_json IS NULL OR arcgis_json = '' THEN
    RETURN NULL;
  END IF;

  BEGIN
    j := arcgis_json::jsonb;
  EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
  END;

  -- Point: {"x":..., "y":...}
  IF j ? 'x' AND j ? 'y' THEN
    RETURN ST_SetSRID(
      ST_MakePoint((j->>'x')::float8, (j->>'y')::float8),
      4326
    );
  END IF;

  -- Polygon: {"rings":[[[x,y],...]]} — rings 2+ are holes.
  -- ArcGIS does not enforce a winding order, so we ST_MakeValid then
  -- force CCW outer rings to keep PostGIS geography-area calculations sane.
  IF j ? 'rings' THEN
    DECLARE g geometry;
    BEGIN
      g := ST_GeomFromGeoJSON(jsonb_build_object('type','Polygon','coordinates', j->'rings')::text);
      g := ST_SetSRID(g, 4326);
      g := ST_MakeValid(g);
      g := ST_ForcePolygonCCW(g);
      RETURN g;
    END;
  END IF;

  -- Polyline: {"paths":[[[x,y],...]]}
  IF j ? 'paths' THEN
    RETURN ST_SetSRID(
      ST_GeomFromGeoJSON(
        jsonb_build_object('type','MultiLineString','coordinates', j->'paths')::text
      ),
      4326
    );
  END IF;

  RETURN NULL;
EXCEPTION WHEN OTHERS THEN
  RETURN NULL;
END;
$$;

-- ──────────────────────────────────────────────────────────────
-- 2. Generic trigger that keeps `geom` in sync with `geometry`
-- ──────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION bronze_ch._sync_geom_from_geometry()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.geom := bronze_ch.arcgis_to_geom(NEW.geometry);
  RETURN NEW;
END;
$$;

-- ──────────────────────────────────────────────────────────────
-- 3. Apply to chantiers tables (new in this session)
-- ──────────────────────────────────────────────────────────────
ALTER TABLE bronze_ch.ge_infomob_chantier_point
  ADD COLUMN IF NOT EXISTS geom geometry(Geometry, 4326);
CREATE INDEX IF NOT EXISTS idx_infomob_chantier_point_geom
  ON bronze_ch.ge_infomob_chantier_point USING GIST (geom);
DROP TRIGGER IF EXISTS trg_infomob_chantier_point_geom
  ON bronze_ch.ge_infomob_chantier_point;
CREATE TRIGGER trg_infomob_chantier_point_geom
  BEFORE INSERT OR UPDATE OF geometry ON bronze_ch.ge_infomob_chantier_point
  FOR EACH ROW EXECUTE FUNCTION bronze_ch._sync_geom_from_geometry();

ALTER TABLE bronze_ch.ge_pcmob_chantier_consult
  ADD COLUMN IF NOT EXISTS geom geometry(Geometry, 4326);
CREATE INDEX IF NOT EXISTS idx_pcmob_chantier_consult_geom
  ON bronze_ch.ge_pcmob_chantier_consult USING GIST (geom);
DROP TRIGGER IF EXISTS trg_pcmob_chantier_consult_geom
  ON bronze_ch.ge_pcmob_chantier_consult;
CREATE TRIGGER trg_pcmob_chantier_consult_geom
  BEFORE INSERT OR UPDATE OF geometry ON bronze_ch.ge_pcmob_chantier_consult
  FOR EACH ROW EXECUTE FUNCTION bronze_ch._sync_geom_from_geometry();

-- ──────────────────────────────────────────────────────────────
-- 4. Apply to existing reference tables (parcelles, batiments)
-- ──────────────────────────────────────────────────────────────
ALTER TABLE bronze_ch.ge_cad_parcelles
  ADD COLUMN IF NOT EXISTS geom geometry(Geometry, 4326);
CREATE INDEX IF NOT EXISTS idx_ge_cad_parcelles_geom
  ON bronze_ch.ge_cad_parcelles USING GIST (geom);
DROP TRIGGER IF EXISTS trg_ge_cad_parcelles_geom ON bronze_ch.ge_cad_parcelles;
CREATE TRIGGER trg_ge_cad_parcelles_geom
  BEFORE INSERT OR UPDATE OF geometry ON bronze_ch.ge_cad_parcelles
  FOR EACH ROW EXECUTE FUNCTION bronze_ch._sync_geom_from_geometry();

ALTER TABLE bronze_ch.ge_cad_batiments
  ADD COLUMN IF NOT EXISTS geom geometry(Geometry, 4326);
CREATE INDEX IF NOT EXISTS idx_ge_cad_batiments_geom
  ON bronze_ch.ge_cad_batiments USING GIST (geom);
DROP TRIGGER IF EXISTS trg_ge_cad_batiments_geom ON bronze_ch.ge_cad_batiments;
CREATE TRIGGER trg_ge_cad_batiments_geom
  BEFORE INSERT OR UPDATE OF geometry ON bronze_ch.ge_cad_batiments
  FOR EACH ROW EXECUTE FUNCTION bronze_ch._sync_geom_from_geometry();

-- ──────────────────────────────────────────────────────────────
-- 5. Backfill `geom` from existing `geometry` text on all tables
-- ──────────────────────────────────────────────────────────────
UPDATE bronze_ch.ge_infomob_chantier_point
  SET geom = bronze_ch.arcgis_to_geom(geometry)
  WHERE geom IS NULL AND geometry IS NOT NULL;

UPDATE bronze_ch.ge_pcmob_chantier_consult
  SET geom = bronze_ch.arcgis_to_geom(geometry)
  WHERE geom IS NULL AND geometry IS NOT NULL;

UPDATE bronze_ch.ge_cad_parcelles
  SET geom = bronze_ch.arcgis_to_geom(geometry)
  WHERE geom IS NULL AND geometry IS NOT NULL;

UPDATE bronze_ch.ge_cad_batiments
  SET geom = bronze_ch.arcgis_to_geom(geometry)
  WHERE geom IS NULL AND geometry IS NOT NULL;

-- ──────────────────────────────────────────────────────────────
-- 6. Generic spatial-join helpers (usable from any dataset)
-- ──────────────────────────────────────────────────────────────

-- Find parcelles intersecting any input geometry.
CREATE OR REPLACE FUNCTION bronze_ch.parcels_intersecting_geom(input_geom geometry)
RETURNS TABLE (
  parcel_id integer,
  objectid varchar,
  egrid varchar,
  commune varchar,
  no_parcelle varchar,
  surface varchar,
  geom geometry
)
LANGUAGE sql
STABLE
AS $$
  SELECT id, objectid, egrid, commune, no_parcelle, surface, geom
  FROM bronze_ch.ge_cad_parcelles
  WHERE geom IS NOT NULL
    AND ST_Intersects(geom, input_geom);
$$;

-- Find buildings intersecting any input geometry.
CREATE OR REPLACE FUNCTION bronze_ch.buildings_intersecting_geom(input_geom geometry)
RETURNS TABLE (
  building_id integer,
  objectid varchar,
  egid varchar,
  commune varchar,
  no_batiment varchar,
  destination varchar,
  geom geometry
)
LANGUAGE sql
STABLE
AS $$
  SELECT id, objectid, egid, commune, no_batiment, destination, geom
  FROM bronze_ch.ge_cad_batiments
  WHERE geom IS NOT NULL
    AND ST_Intersects(geom, input_geom);
$$;

-- Convenience: parcels affected by a chantier (PCMOB polygon by objectid).
CREATE OR REPLACE FUNCTION bronze_ch.parcels_affected_by_chantier(chantier_objectid text)
RETURNS TABLE (
  parcel_id integer,
  egrid varchar,
  commune varchar,
  no_parcelle varchar,
  parcel_surface_m2 double precision,
  intersection_area_m2 double precision,
  pct_of_parcel_affected numeric
)
LANGUAGE sql
STABLE
AS $$
  WITH c AS (
    SELECT geom FROM bronze_ch.ge_pcmob_chantier_consult
    WHERE objectid = chantier_objectid AND geom IS NOT NULL
    LIMIT 1
  )
  SELECT
    p.id,
    p.egrid,
    p.commune,
    p.no_parcelle,
    ST_Area(p.geom::geography) AS parcel_surface_m2,
    ST_Area(ST_Intersection(p.geom, c.geom)::geography) AS intersection_area_m2,
    ROUND(
      (ST_Area(ST_Intersection(p.geom, c.geom)::geography)
       / NULLIF(ST_Area(p.geom::geography), 0) * 100)::numeric,
      2
    ) AS pct_of_parcel_affected
  FROM bronze_ch.ge_cad_parcelles p, c
  WHERE p.geom IS NOT NULL
    AND ST_Intersects(p.geom, c.geom)
  ORDER BY intersection_area_m2 DESC;
$$;
