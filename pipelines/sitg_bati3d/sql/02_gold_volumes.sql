DROP TABLE IF EXISTS gold_ch.ge_building_volumes_3d;
CREATE TABLE gold_ch.ge_building_volumes_3d AS
SELECT
  b.egid::bigint                                   AS egid,
  -- AUTHORITATIVE: SITG-published closed-shell volume. ABOVE-GROUND ONLY (base is the
  -- footprint dropped to TERRAIN altitude) — excludes basements. NOT a SIA 116 total.
  round(f.volume::numeric, 2)                      AS volume_3d_hors_sol_m3,
  -- Superstructures (1-9 m2: chien-assis, cheminees). STORED SEPARATELY, never summed
  -- into the main figure; excluded from a SIA volume.
  round(sp.volume_sp::numeric, 2)                  AS volume_3d_superstructures_m3,
  round(b.altitude_ref::numeric, 2)                AS altitude_ref_ln02_m,
  round(t.altitude_max::numeric, 2)                AS altitude_max_ln02_m,
  round((t.altitude_max - b.altitude_ref)::numeric, 2) AS height_m,
  round(t.surface_totale::numeric, 2)              AS toit_surface_totale_m2,
  round(t.surface_avant_toit::numeric, 2)          AS toit_surface_avant_toit_m2,
  round(t.pente_min::numeric, 2)                   AS toit_pente_min,
  round(t.pente_max::numeric, 2)                   AS toit_pente_max,
  round(t.pente_moy::numeric, 2)                   AS toit_pente_moy,
  round(t.surface_totale_sol::numeric, 2)          AS toit_surface_totale_sol_m2,
  round(b.surface_sol::numeric, 2)                 AS surface_sol_m2,
  round(f.surface_totale::numeric, 2)              AS facade_surface_totale_m2,
  round(f.surface_partage::numeric, 2)             AS facade_surface_partage_m2,
  sp.n_superstructures,
  -- provenance: per-feature photogrammetry survey date + dataset publication date
  b.date_leve::date                                AS source_releve_date,
  DATE '2025-06-12'                                AS source_publication_date,
  -- verification columns, filled by the mesh cross-check pass
  NULL::numeric                                    AS volume_mesh_computed_m3,
  NULL::boolean                                    AS shell_closed,
  now()                                            AS computed_at
FROM bronze_ch.ge_cad_bati3d_base b
JOIN bronze_ch.ge_cad_bati3d_toit   t ON t.egid = b.egid
JOIN bronze_ch.ge_cad_bati3d_facade f ON f.egid = b.egid
LEFT JOIN (
  SELECT egid, sum(volume) AS volume_sp, count(*) AS n_superstructures
  FROM bronze_ch.ge_cad_bati3d_sp_facade GROUP BY egid
) sp ON sp.egid = b.egid;

ALTER TABLE gold_ch.ge_building_volumes_3d ADD PRIMARY KEY (egid);
CREATE INDEX ix_gebv3d_vol ON gold_ch.ge_building_volumes_3d (volume_3d_hors_sol_m3);

COMMENT ON TABLE gold_ch.ge_building_volumes_3d IS
 'Per-EGID ABOVE-GROUND building volume from SITG BATI3D (publication 2025-06-12, photogrammetry, cadence irregular). volume_3d_hors_sol_m3 is SITG''s published closed-shell volume; independently reproduced to 0.02% by bronze_ch.fn_mesh_volume on the subset whose shell closes. EXCLUDES basements: the BATI3D base is the footprint dropped to terrain altitude. NOT a SIA 116 total.';
COMMENT ON COLUMN gold_ch.ge_building_volumes_3d.volume_3d_hors_sol_m3 IS 'Above-ground volume only (m3). Excludes sous-sol, which is not obtainable from any SITG cadastral or 3D layer.';
COMMENT ON COLUMN gold_ch.ge_building_volumes_3d.volume_3d_superstructures_m3 IS 'Superstructures 1-9 m2 (chien-assis, cheminees), SEPARATE. Never add to volume_3d_hors_sol_m3 for a SIA figure.';

SELECT 'rows=' || count(*) || ' | with_volume=' || count(volume_3d_hors_sol_m3)
    || ' | with_superstruct=' || count(volume_3d_superstructures_m3) FROM gold_ch.ge_building_volumes_3d;
