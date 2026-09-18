-- SITG LiDAR 2025 — commune-scoped mirror manifest. Additive: one NEW table.
-- Rollback: DROP TABLE bronze_ch.ge_lidar_2025_tiles;
CREATE TABLE IF NOT EXISTS bronze_ch.ge_lidar_2025_tiles (
  tile_key          text PRIMARY KEY,               -- '<E>_<N>' = SW corner, 250 m tile, LV95
  x_min             integer NOT NULL,
  y_min             integer NOT NULL,
  communes          integer[] NOT NULL,             -- no_commune values the tile was fetched for
  source_url        text NOT NULL,
  source_bytes      bigint,
  points            bigint NOT NULL,
  density_pts_m2    numeric(8,1) NOT NULL,
  z_min             numeric(8,2),
  z_max             numeric(8,2),
  has_rgb           boolean NOT NULL,
  class_counts      jsonb NOT NULL,                 -- {"2": n, "6": n, "16": n, ...}
  r2_key            text NOT NULL,                  -- COPC mirror in camelote-backups
  copc_bytes        bigint NOT NULL,
  version           text NOT NULL DEFAULT 'v1',
  acquired_at       timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE bronze_ch.ge_lidar_2025_tiles IS
'SITG LiDAR 2025 (flown at night 21 Mar-1 Apr 2025, Riegl VQ-1560II-S), mirrored per commune as COPC to R2 camelote-backups/lamap-2025/lidar/sitg-2025/<version>/<tile>.copc.laz. NEVER the whole canton: 5,505 tiles x ~381 MB = ~2.1 TB. MEASURED vs the catalogue (2026-09-18, Genève-Cité): files are LAS 1.2 / PDRF 3 WITH RGB (catalogue says 1.4 / PDRF 1); density ~153 pts/m2 (catalogue: min 100); CLASS 16 IS STREET/PAVEMENT SURFACE, NOT NOISE — height above ground 0.01 +/- 0.05 m, spread across every street, 26.5 % of an old-town tile. Treat it as ground; filtering it as noise punches a hole in every street.';
ALTER TABLE bronze_ch.ge_lidar_2025_tiles ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON bronze_ch.ge_lidar_2025_tiles FROM anon, authenticated;
