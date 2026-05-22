-- ============================================================================
-- VD/Lausanne enrichment — federal_cadastral_parcels VD subset ST_MakeValid pass
-- ============================================================================
-- Target DB: re-LLM
-- Version  : 20260522000003
-- Purpose  : Link matview refreshes still failed after the 20260522000002 bronze
--            pass. Diagnostic SELECT NOT ST_IsValid(geometry) FROM
--            silver_ch.cadastral_plots WHERE canton_code='VD' surfaced 724 of
--            284,015 VD plots with invalid topology.
--
--            silver_ch.cadastral_plots reads from bronze_ch.federal_cadastral_parcels
--            (national, ~3M rows). The assumption "federal cadastre = clean" was
--            wrong — the VD slice has 724 invalid polygons. The link matview
--            ST_Intersection raises against any of these.
--
-- Scope    : VD subset only (724 rows). National scope not in scope here; same
--            fix can be applied later if/when other canton-VD-style work surfaces
--            similar issues for other cantons.
--
-- Shape-preserving: ST_MakeValid does not change which area a polygon represents
--            — only metadata (self-intersections resolved, ring orientation
--            normalized). Downstream GE matviews (plot_intel_ge etc) that
--            already work against the current cadastral_plots will see identical
--            spatial relationships, just cleaner topology.
-- ============================================================================

BEGIN;
SET LOCAL statement_timeout = '1800s';

DO $$
DECLARE updated_count bigint;
BEGIN
  -- ST_MakeValid on severely-broken polygons can produce GeometryCollection
  -- (mixed dim). ST_CollectionExtract(_, 3) extracts polygon parts only, so
  -- the result fits the geometry(MultiPolygon, 2056) column.
  UPDATE bronze_ch.federal_cadastral_parcels
     SET geometry = ST_CollectionExtract(ST_MakeValid(geometry), 3)::geometry(MultiPolygon, 2056)
   WHERE canton_code = 'VD'
     AND geometry IS NOT NULL
     AND NOT ST_IsValid(geometry);
  GET DIAGNOSTICS updated_count = ROW_COUNT;
  RAISE NOTICE 'bronze_ch.federal_cadastral_parcels VD subset: % rows ST_MakeValid''d', updated_count;
END$$;

INSERT INTO supabase_migrations.schema_migrations (version, name, statements, created_by)
VALUES (
  '20260522000003',
  '2026-05-22_vd_enrichment_federal_plots_makevalid',
  ARRAY['ST_MakeValid applied to invalid VD subset of bronze_ch.federal_cadastral_parcels (~724 rows expected)'],
  'vd_enrichment-followup'
)
ON CONFLICT (version) DO NOTHING;

COMMIT;
