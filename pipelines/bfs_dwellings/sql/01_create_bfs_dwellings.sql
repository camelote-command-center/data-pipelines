-- bronze_ch.bfs_dwellings
-- Total dwellings per (year, commune) from BFS dataflow CH1.GWS:DF_GWS_REG5
-- ("Dwellings by canton/municipality, building category, number of rooms, and construction period").
-- Year coverage in source: 2010-2024 (annual GWS register-based).
--
-- Conflict key: (year, bfs_commune_number) — bare-column UNIQUE for PostgREST on_conflict.

CREATE TABLE IF NOT EXISTS bronze_ch.bfs_dwellings (
    id                 bigserial PRIMARY KEY,
    year               integer NOT NULL,
    bfs_commune_number integer NOT NULL,
    commune_name       text,
    canton_code        text,
    total_dwellings    integer,
    source             text DEFAULT 'BFS',
    created_at         timestamptz DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_bfs_dwellings_year_commune
    ON bronze_ch.bfs_dwellings (year, bfs_commune_number);

CREATE INDEX IF NOT EXISTS idx_bfs_dwellings_canton
    ON bronze_ch.bfs_dwellings (canton_code, year);
