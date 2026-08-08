#!/usr/bin/env python3
"""
Stage 4 — load a CHM tree-detection vintage into gold_ch.plot_trees and push to
ref.plot_trees on lamap_db.

This is the step that never existed. The 2026-07-24 load of 1'592'275 trees was
performed ad hoc in a session (recorded in the Command Center change_log, not in
this repo), which is why gold_ch.plot_trees froze at chm-v1 while the canopy
STATS beside it refresh through load_stats.py. Same shape as load_stats.py, so
the two halves of the CHM output are now loaded the same way. Bug 2acfe744.

A VINTAGE IS A REPLACEMENT, NOT AN UPDATE
    trees_cadastre can be keyed on content across publications because SITG
    republishes the same surveyed values. A LiDAR detection cannot: between CHM
    vintages a real tree shifts by centimetres, grows, and may appear or vanish.
    There is no stable cross-vintage identity, and matching on one would merge
    measurements from different years into a single row.

    So content_key gives idempotency WITHIN a vintage (reloading chm-v2 updates
    its own rows rather than duplicating them), and a NEW vintage is loaded
    alongside, verified, then the superseded generation is snapshotted and
    removed as a whole. UNIQUE (source, content_key) enforces exactly that.

    The snapshot-then-remove is deliberate and planned, not discovered: loading
    a new generation against a non-destructive sync is what left
    ref.trees_cadastre holding 479'507 rows earlier in the same week.

THE HEIGHT FLOOR IS ASSERTED, NOT APPLIED
    The original load silently dropped 1008 sub-3 m rows. Commit d865fca moved
    that floor into the detector itself ("so no load-time patch is needed next
    CHM vintage"). Re-applying it here would hide a detector regression, so this
    checks and REFUSES instead. If sub-floor rows ever reappear, that is a
    detection bug and the load should stop.

Usage:
    load_trees.py work/plot_trees.csv --vintage chm-v2
    load_trees.py work/plot_trees.csv --vintage chm-v2 --keep-previous

Environment:
    RE_LLM_PG_URI   re-LLM connection string
"""
import argparse
import os
import subprocess
import sys
from datetime import date

VEG_MIN_M = 3.0        # must match the detector's floor
MIN_EXPECTED_ROWS = 500_000   # a GE-wide run is ~1.6M; far below that is a broken run


def psql(pg: str, sql: str, quiet: bool = False) -> str:
    r = subprocess.run(
        ["psql", pg, "-v", "ON_ERROR_STOP=1", "-q", "-tA"],
        input=sql, text=True, capture_output=True,
        env={**os.environ, "PGOPTIONS": "-c client_min_messages=warning"},
    )
    if r.returncode != 0:
        print(r.stdout, file=sys.stderr)
        print(r.stderr[:3000], file=sys.stderr)
        raise SystemExit(f"psql failed ({r.returncode})")
    if not quiet and r.stdout.strip():
        print(r.stdout.rstrip())
    return r.stdout


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("csv", nargs="?", default="work/plot_trees.csv")
    ap.add_argument("--vintage", required=True,
                    help="CHM vintage tag, e.g. chm-v2. Must not already be live.")
    ap.add_argument("--keep-previous", action="store_true",
                    help="Load the new vintage but do NOT remove the superseded one. "
                         "Leaves two generations live; use only to inspect a diff.")
    args = ap.parse_args()

    pg = os.environ.get("RE_LLM_PG_URI")
    if not pg:
        return int(bool(sys.stderr.write("RE_LLM_PG_URI not set\n"))) or 1
    if not os.path.exists(args.csv):
        return int(bool(sys.stderr.write(f"{args.csv} not found — run detection first\n"))) or 1

    stamp = date.today().strftime("%Y%m%d")
    backup = f"backup.plot_trees_superseded_{stamp}"

    print(f"  vintage: {args.vintage}")
    print(f"  csv:     {args.csv} ({os.path.getsize(args.csv)/1e6:.1f} MB)")

    before = psql(pg, "SELECT source||' '||count(*) FROM gold_ch.plot_trees "
                      "GROUP BY source ORDER BY source;", quiet=True).strip()
    print(f"  gold before: {before or '(empty)'}")

    # ── Stage + promote, one transaction ─────────────────────────────────
    load_sql = f"""
\\set ON_ERROR_STOP on
BEGIN;
CREATE TEMP TABLE _stg (
  egrid text, e_lv95 double precision, n_lv95 double precision,
  height_m real, crown_radius_m real, tile text
) ON COMMIT DROP;
\\copy _stg (egrid,e_lv95,n_lv95,height_m,crown_radius_m,tile) FROM '{args.csv}' WITH (FORMAT csv, HEADER true)

-- Refuse a detector regression rather than silently patching it (see docstring).
DO $$
DECLARE v_low int; v_n int;
BEGIN
  SELECT count(*) FILTER (WHERE height_m < {VEG_MIN_M}), count(*) INTO v_low, v_n FROM _stg;
  IF v_n < {MIN_EXPECTED_ROWS} THEN
    RAISE EXCEPTION 'only % staged rows, below the % floor — detection run looks truncated', v_n, {MIN_EXPECTED_ROWS};
  END IF;
  IF v_low > 0 THEN
    RAISE EXCEPTION '% staged rows are below the % m vegetation floor. The detector '
                    'is supposed to enforce this at the peak (commit d865fca); patching '
                    'it here would hide the regression.', v_low, {VEG_MIN_M};
  END IF;
  RAISE NOTICE 'staged % rows, all at or above the % m floor', v_n, {VEG_MIN_M};
END $$;

-- id is GENERATED ALWAYS AS IDENTITY: it must not be supplied, and it carries
-- no meaning across runs. Ordering the insert by tile then position keeps the
-- generated ids spatially coherent, which makes tile-range scans on the
-- consumer cheaper, but nothing depends on the values.
INSERT INTO gold_ch.plot_trees AS t
  (content_key, egrid, geom, height_m, crown_radius_m, tile, source, computed_at)
SELECT
  md5(round(s.e_lv95::numeric,2)::text||','||round(s.n_lv95::numeric,2)::text
      ||'|'||s.height_m::text||'|'||s.crown_radius_m::text),
  nullif(btrim(s.egrid), ''),
  ST_SetSRID(ST_MakePoint(s.e_lv95, s.n_lv95), 2056),
  s.height_m, s.crown_radius_m, s.tile, '{args.vintage}', now()
FROM _stg s
ORDER BY s.tile, s.e_lv95, s.n_lv95
ON CONFLICT (source, content_key) DO UPDATE SET
  egrid          = EXCLUDED.egrid,
  geom           = EXCLUDED.geom,
  height_m       = EXCLUDED.height_m,
  crown_radius_m = EXCLUDED.crown_radius_m,
  tile           = EXCLUDED.tile;
  -- computed_at is deliberately NOT updated on conflict. It records when the
  -- VINTAGE was computed, not when it was last loaded, so re-running the same
  -- vintage must not move it. A genuinely new vintage is an INSERT, where
  -- now() applies. (Learned the hard way: the first run of this loader
  -- overwrote chm-v1's original 2026-07-24 13:19:32 with the reload time.)
COMMIT;
SELECT 'loaded '||count(*)||' rows for {args.vintage}'
  FROM gold_ch.plot_trees WHERE source = '{args.vintage}';
"""
    psql(pg, load_sql)

    # ── Snapshot then remove the superseded generation ───────────────────
    if args.keep_previous:
        print("  --keep-previous: superseded vintages left in place (two generations live)")
    else:
        psql(pg, f"""
\\set ON_ERROR_STOP on
CREATE SCHEMA IF NOT EXISTS backup;
DROP TABLE IF EXISTS {backup};
CREATE TABLE {backup} AS
  SELECT now() AS snapshot_at, * FROM gold_ch.plot_trees WHERE source <> '{args.vintage}';
SELECT 'snapshotted '||count(*)||' superseded rows to {backup}' FROM {backup};
DELETE FROM gold_ch.plot_trees WHERE source <> '{args.vintage}';
SELECT 'gold now holds '||count(*)||' rows, sources: '||
       coalesce(string_agg(DISTINCT source, ','), '(none)') FROM gold_ch.plot_trees;
""")

    # ── Push to ref and assert the consumer matches ──────────────────────
    # sync_plot_trees() is DELETE-then-INSERT against the foreign table, in one
    # transaction, so ref becomes an exact mirror of gold and readers never see
    # a partial state. Safe here because ref.plot_trees is a pure mirror with no
    # destination-only rows -- unlike ref.ge_cad_adresses, where a truncate cost
    # 43 rows that existed only on the destination (incident 4e77cead).
    psql(pg, """
\\set ON_ERROR_STOP on
CALL gold_ch.sync_plot_trees();
DO $$
DECLARE g bigint; r bigint;
BEGIN
  SELECT count(*) INTO g FROM gold_ch.plot_trees;
  SELECT count(*) INTO r FROM lamap_db_foreign.plot_trees;
  IF g <> r THEN
    RAISE EXCEPTION 'distribution mismatch: gold % rows, ref % rows', g, r;
  END IF;
  RAISE NOTICE 'ref.plot_trees matches gold: % rows', r;
END $$;
SELECT 'ref.plot_trees = '||count(*)||' rows' FROM lamap_db_foreign.plot_trees;
""")
    print("  done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
