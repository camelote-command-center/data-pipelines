-- ============================================================================
-- camelote_data — add `host` column to public.datasets
-- ============================================================================
-- Target DB: camelote-data (project ref: dxugbpeacnorjunpljih)
-- Scope    : ALTER public.datasets ADD COLUMN host text. Distinguishes VPS-hosted
--            parsers from GH-Actions-hosted parsers in the watchdog/auto-fix system.
-- Pre-reqs : None.
-- Backfill : Existing rows get host='github-actions' (current default for the
--            existing fleet). New VD-enrichment rows insert with host=<vps-host>.
-- ============================================================================

BEGIN;

ALTER TABLE public.datasets
  ADD COLUMN IF NOT EXISTS host text;

COMMENT ON COLUMN public.datasets.host IS
  'Execution host identifier. Values: ''github-actions'' for repo-hosted workflows, '
  '''vps-145.223.82.190'' / ''vps-31.97.122.135'' / ''vps-46.202.153.114'' for the '
  'distributed parser fleet. NULL means unspecified — assume GH Actions for legacy rows.';

-- Backfill legacy rows
UPDATE public.datasets
   SET host = 'github-actions'
 WHERE host IS NULL
   AND workflow_file IS NOT NULL;     -- only rows that clearly had a GH workflow

CREATE INDEX IF NOT EXISTS idx_datasets_host ON public.datasets (host);

COMMIT;

NOTIFY pgrst, 'reload schema';
