-- Migrate bronze_ch.bfs_vacancy_rates from canton-level to commune-level
--
-- Background:
--   The pre-2026-05 schema was designed around canton-level data with a 3-col
--   COALESCE expression unique index that PostgREST cannot match via on_conflict
--   (since on_conflict only accepts plain column names, not expressions).
--   See ~/work/bfs-vacancy-fix-2026-05-04/REPORT.md for the diagnostics.
--
--   The new parser (commit before this one) fetches commune-level data from
--   BFS SDMX dataflow CH1.LWZ:DF_LWZ_1 and produces (year, bfs_commune_number)
--   as the natural unique key.
--
-- Pre-flight checks (verified 2026-05-04 before this migration):
--   * SELECT count(*) FROM bronze_ch.bfs_vacancy_rates  -> 0 rows
--   * pg_proc, pg_views, pg_depend show ZERO consumers (no functions/views/
--     matviews/cron jobs reference this table)
--   * gold_ch.sync_registry: not registered as source or target
--   * Therefore: dropping the index, changing NULL constraints, and rebuilding
--     the data is safe.
--
-- This script is idempotent: re-running is a no-op once applied.

BEGIN;

-- 1. Drop the COALESCE-expression unique index that PostgREST could never satisfy
DROP INDEX IF EXISTS bronze_ch.idx_bfs_vacancy_rates_uq_vacancy;

-- 2. Make bfs_commune_number NOT NULL (every row from BFS commune-level dataset has it)
ALTER TABLE bronze_ch.bfs_vacancy_rates
    ALTER COLUMN bfs_commune_number SET NOT NULL;

-- 3. Make canton_code nullable (parser populates it from a bfs_population lookup
--    which may be empty during initial seeding, and BFS commune-level data does
--    not directly carry canton_code in the row itself)
ALTER TABLE bronze_ch.bfs_vacancy_rates
    ALTER COLUMN canton_code DROP NOT NULL;

-- 4. Add the natural 2-col unique index on plain columns so PostgREST
--    `on_conflict=year,bfs_commune_number` works
CREATE UNIQUE INDEX IF NOT EXISTS idx_bfs_vacancy_rates_year_commune
    ON bronze_ch.bfs_vacancy_rates (year, bfs_commune_number);

COMMIT;

-- Verify:
--   \d bronze_ch.bfs_vacancy_rates
--   Should show: bfs_commune_number NOT NULL; canton_code nullable;
--                idx_bfs_vacancy_rates_year_commune UNIQUE on (year, bfs_commune_number);
--                no idx_bfs_vacancy_rates_uq_vacancy.
