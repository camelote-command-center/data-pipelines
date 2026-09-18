-- SITG MNA HAUTEUR 2025-03 (20 cm height-above-ground model) -> per-parcel and per-building
-- height statistics. Additive: three NEW tables, nothing existing is altered.
-- Rollback: DROP TABLE bronze_ch.ge_mna_hauteur_parcel_stats, bronze_ch.ge_mna_hauteur_building_heights,
--           bronze_ch.ge_mna_hauteur_runs;

CREATE TABLE IF NOT EXISTS bronze_ch.ge_mna_hauteur_runs (
  run_id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  started_at           timestamptz NOT NULL DEFAULT now(),
  finished_at          timestamptz,
  source_url           text NOT NULL,
  source_bytes         bigint,
  source_last_modified timestamptz,
  tiles                integer,
  parcels              integer,
  buildings            integer,
  status               text NOT NULL DEFAULT 'running',
  github_run_url       text
);

CREATE TABLE IF NOT EXISTS bronze_ch.ge_mna_hauteur_parcel_stats (
  egrid                text PRIMARY KEY,
  no_commune           integer,
  no_parcelle          integer,
  canopy_cover_pct     numeric(6,3) NOT NULL CHECK (canopy_cover_pct BETWEEN 0 AND 100),
  veg_height_p95_m     numeric(5,2),
  veg_height_max_m     numeric(5,2),
  veg_height_mean_m    numeric(5,2),
  vegetated_area_m2    numeric(12,2) NOT NULL,
  built_area_m2        numeric(12,2) NOT NULL,
  built_height_max_m   numeric(6,2),
  parcel_area_m2       numeric(12,2) NOT NULL,
  source_vintage       text NOT NULL DEFAULT '2025-03',
  source_last_modified timestamptz,
  run_id               uuid REFERENCES bronze_ch.ge_mna_hauteur_runs(run_id),
  computed_at          timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze_ch.ge_mna_hauteur_building_heights (
  footprint_id         integer PRIMARY KEY,          -- bronze_ch.ge_buildings_geo.id
  egid                 integer,
  no_commune           integer,
  no_parcelle          integer,
  roof_area_m2         numeric(12,2) NOT NULL,
  height_max_m         numeric(6,2),
  height_p95_m         numeric(6,2),
  height_p50_m         numeric(6,2),
  height_mean_m        numeric(6,2),
  source_vintage       text NOT NULL DEFAULT '2025-03',
  source_last_modified timestamptz,
  run_id               uuid REFERENCES bronze_ch.ge_mna_hauteur_runs(run_id),
  computed_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ge_mna_hauteur_building_heights_egid_idx ON bronze_ch.ge_mna_hauteur_building_heights (egid);

COMMENT ON TABLE bronze_ch.ge_mna_hauteur_parcel_stats IS
'Per-parcel height statistics from SITG MNA_HAUTEUR_2025_03 (MNS-MNT, 20 cm pixels, LiDAR flown at NIGHT 21 Mar-1 Apr 2025 = LEAF-OFF: deciduous canopy cover reads LOW versus summer). Vegetation columns (canopy_cover_pct, veg_*) exclude buildings: mask = bronze_ch.ge_buildings_geo buffered 1.5 m (footprints are the ground outline, roofs overhang). 3 m threshold separates trees from grass/hedges/vehicles; veg_* computed over the >=3 m mask only. built_* = footprint shrunk 0.4 m (drops edge pixels mixing roof and ground). Parallel to, NOT a replacement for, gold_ch.plot_canopy_stats (swisstopo, 50 cm, leaf-on/off mixed). Compare before any swap.';
COMMENT ON TABLE bronze_ch.ge_mna_hauteur_building_heights IS
'Per-footprint roof heights above ground from SITG MNA_HAUTEUR_2025_03 (20 cm). Pixels = footprint shrunk 0.4 m (ST_Buffer -0.4; original footprint if that empties it). height_* are metres ABOVE GROUND, not LN02 altitudes. Independent of bronze_ch.ge_cad_bati3d_* (photogrammetry) — use to cross-check.';

ALTER TABLE bronze_ch.ge_mna_hauteur_runs             ENABLE ROW LEVEL SECURITY;
ALTER TABLE bronze_ch.ge_mna_hauteur_parcel_stats     ENABLE ROW LEVEL SECURITY;
ALTER TABLE bronze_ch.ge_mna_hauteur_building_heights ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON bronze_ch.ge_mna_hauteur_runs, bronze_ch.ge_mna_hauteur_parcel_stats,
              bronze_ch.ge_mna_hauteur_building_heights FROM anon, authenticated;
