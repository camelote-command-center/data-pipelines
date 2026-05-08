CREATE MATERIALIZED VIEW gold_ch.core_units AS
SELECT
  cu.egid,
  cu.ewid,
  cu.edid,
  cb.egrid,
  cu.canton_code,
  cu.canton_name,
  cu.commune_bfs,
  cu.commune_name,
  cu.unit_number,
  cu.entrance_number,
  cu.floor_code,
  cu.floor_label,
  cu.description,
  cu.is_multi_dwelling,
  cu.construction_year,
  cu.demolition_year,
  cu.status_code,
  cu.status_label,
  cu.surface_m2,
  cu.rooms,
  cu.has_kitchen,
  cu.updated_at
FROM silver_ch.cadastral_units cu
LEFT JOIN silver_ch.cadastral_buildings cb ON cb.egid = cu.egid
