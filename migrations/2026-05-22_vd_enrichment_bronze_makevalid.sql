-- ============================================================================
-- VD/Lausanne enrichment — bronze polygon ST_MakeValid pass
-- ============================================================================
-- Target DB: re-LLM
-- Version  : 20260522000002 (reuses the slot — the prior DROP+recreate attempt
--                            rolled back atomically; no migration row landed)
-- Purpose  : Mitigate GEOS TopologyException raised by silver link_plot_*_vd
--            ST_Intersection on canton-VD source polygons (zones, classement,
--            archeology, patrimoine_inventaire, etc.) with invalid topology.
--
-- Approach : ST_MakeValid the bronze geometry IN PLACE. Shape-preserving
--            (same represented polygon, only topology metadata changes:
--             self-intersections resolved, ring orientation normalized).
--            Bronze "rawness" at the data level preserved.
--            v_plots_full + GE downstream untouched.
--
-- Future-proof: parser ingest code also gets an ST_MakeValid wrap (separate
--               commit) so future runs don't re-introduce invalid topology.
-- ============================================================================

BEGIN;

DO $$
DECLARE
  t record;
  updated_count bigint;
  total_updated bigint := 0;
BEGIN
  FOR t IN
    SELECT unnest(ARRAY[
      'bronze_ch.vd_zone_affectation',
      'bronze_ch.vd_limite_foret',
      'bronze_ch.vd_batiment_projete',
      'bronze_ch.vd_degre_sensibilite_bruit',
      'bronze_ch.vd_classement',
      'bronze_ch.vd_jardin_historique',
      'bronze_ch.vd_isos',
      'bronze_ch.vd_region_archeologique',
      'bronze_ch.vd_lausanne_archeologie',
      'bronze_ch.vd_lausanne_ddp',
      'bronze_ch.vd_lausanne_dp',
      'bronze_ch.vd_lausanne_pa_etude',
      'bronze_ch.vd_lausanne_parking_sectors',
      'bronze_ch.vd_lausanne_energie_cad_bati'
    ]) AS tbl
  LOOP
    EXECUTE format(
      'UPDATE %s SET geometry = ST_MakeValid(geometry) '
      'WHERE NOT ST_IsValid(geometry) AND geometry IS NOT NULL',
      t.tbl
    );
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    RAISE NOTICE '%: % rows ST_MakeValid''d', t.tbl, updated_count;
    total_updated := total_updated + updated_count;
  END LOOP;
  RAISE NOTICE 'TOTAL: % invalid geometries fixed', total_updated;
END$$;

INSERT INTO supabase_migrations.schema_migrations (version, name, statements, created_by)
VALUES (
  '20260522000002',
  '2026-05-22_vd_enrichment_bronze_makevalid',
  ARRAY['ST_MakeValid applied to 14 bronze polygon tables; see migration body'],
  'vd_enrichment-followup'
)
ON CONFLICT (version) DO NOTHING;

COMMIT;
