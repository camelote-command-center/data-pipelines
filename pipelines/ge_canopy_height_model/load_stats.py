#!/usr/bin/env python3
"""
Stage 3 — load merged statistics into gold_ch.plot_canopy_stats and push
to ref.plot_canopy_stats on lamap_db.

UPSERT, never TRUNCATE (doctrine). The load goes through a TEMP staging
table so a partial run can never leave gold_ch half-empty: rows land in
staging, then one INSERT ... ON CONFLICT DO UPDATE promotes them.

The FDW push is gold_ch.sync_plot_canopy_stats(), which does its own
UPDATE-then-INSERT against lamap_db_foreign.plot_canopy_stats.
"""
import os, subprocess, sys

PG = os.environ.get("RE_LLM_PG_URI")
if not PG:
    sys.exit("RE_LLM_PG_URI not set")
TSV = sys.argv[1] if len(sys.argv) > 1 else "work/canopy_stats.tsv"
if not os.path.exists(TSV):
    sys.exit(f"{TSV} not found — run merge_stats.py first")

COLS = ("egrid,no_commune,no_parcelle,canopy_cover_pct,height_p95_m,height_max_m,"
        "height_mean_m,vegetated_area_m2,parcel_area_m2,polygon_area_m2,"
        "dsm_year,dtm_year,vintage_mixed")

sql = f"""
\\set ON_ERROR_STOP on
BEGIN;
CREATE TEMP TABLE _stg (LIKE gold_ch.plot_canopy_stats INCLUDING DEFAULTS) ON COMMIT DROP;
ALTER TABLE _stg DROP COLUMN computed_at;
\\copy _stg ({COLS}) FROM '{TSV}' WITH (FORMAT text, NULL '\\N')
INSERT INTO gold_ch.plot_canopy_stats AS t ({COLS}, computed_at)
SELECT {COLS}, now() FROM _stg
ON CONFLICT (egrid) DO UPDATE SET
  no_commune=EXCLUDED.no_commune, no_parcelle=EXCLUDED.no_parcelle,
  canopy_cover_pct=EXCLUDED.canopy_cover_pct, height_p95_m=EXCLUDED.height_p95_m,
  height_max_m=EXCLUDED.height_max_m, height_mean_m=EXCLUDED.height_mean_m,
  vegetated_area_m2=EXCLUDED.vegetated_area_m2, parcel_area_m2=EXCLUDED.parcel_area_m2,
  polygon_area_m2=EXCLUDED.polygon_area_m2, dsm_year=EXCLUDED.dsm_year,
  dtm_year=EXCLUDED.dtm_year, vintage_mixed=EXCLUDED.vintage_mixed,
  computed_at=EXCLUDED.computed_at;
COMMIT;
SELECT 'gold_ch.plot_canopy_stats rows = '||count(*) FROM gold_ch.plot_canopy_stats;
CALL gold_ch.sync_plot_canopy_stats();
SELECT 'ref.plot_canopy_stats rows = '||count(*) FROM lamap_db_foreign.plot_canopy_stats;
"""
r = subprocess.run(["psql", PG, "-q", "-v", "ON_ERROR_STOP=1"],
                   input=sql, text=True, capture_output=True,
                   env={**os.environ, "PGOPTIONS": "-c client_min_messages=warning"})
print(r.stdout)
if r.returncode != 0:
    print(r.stderr[:2000], file=sys.stderr)
    sys.exit(r.returncode)
