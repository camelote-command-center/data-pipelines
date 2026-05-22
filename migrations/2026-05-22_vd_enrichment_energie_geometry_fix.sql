-- ============================================================================
-- VD/Lausanne enrichment — vd_lausanne_energie_cad_bati.geometry type fix
-- ============================================================================
-- Records the in-flight ALTER applied during PR #16 Phase 4 backfill.
-- The original PR #15 bronze migration typed geometry as Point; the Lausanne WFS
-- source actually returns MultiPolygon features (building-footprint polygons,
-- not centroid points). Backfill failed with InvalidParameterValue until the
-- column was widened to geometry(Geometry, 2056).
--
-- The live ALTER was applied 2026-05-22 mid-Phase 4. This migration records
-- it for reproducibility (no-op on the live DB; tracking row is the point).
--
-- Bug filed in camelote_data.bugs: see "vd_lausanne_energie_cad_bati.geometry
-- typed Point in PR#15" for full context + future-prevention notes.
-- ============================================================================

BEGIN;

ALTER TABLE bronze_ch.vd_lausanne_energie_cad_bati
  ALTER COLUMN geometry TYPE geometry(Geometry, 2056) USING geometry::geometry(Geometry, 2056);

INSERT INTO supabase_migrations.schema_migrations (version, name, statements, created_by)
VALUES (
  '20260522000001',
  '2026-05-22_vd_enrichment_energie_geometry_fix',
  ARRAY[
    $stmt$ALTER TABLE bronze_ch.vd_lausanne_energie_cad_bati ALTER COLUMN geometry TYPE geometry(Geometry, 2056) USING geometry::geometry(Geometry, 2056);$stmt$
  ],
  'vd_enrichment-followup'
)
ON CONFLICT (version) DO NOTHING;

COMMIT;

NOTIFY pgrst, 'reload schema';
