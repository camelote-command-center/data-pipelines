# GE Canopy Height Model — swisstopo DSM − DTM → per-parcel canopy statistics

Produces a 0.5 m canopy height model for canton Genève and reduces it to
one row of canopy statistics per cadastral parcel.

```
CHM = swissSURFACE3D Raster (DSM, surface incl. vegetation + buildings)
    − swissALTI3D          (DTM, bare earth)
```

Both products are **LN02 metres, EPSG:2056, 0.5 m**. Because the CHM is a
difference of two LN02 rasters the datum cancels, so the result is a true
height above ground. **There is no geoid correction anywhere in this
pipeline and none is needed.**

## Output

| Target | What |
|---|---|
| `camelote-backups/lamap-2025/chm/v1/{easting}-{northing}.tif` | 357 CHM COGs, UInt16 ×10 (0–500 = 0.0–50.0 m), nodata 65535, DEFLATE, ~3.4 MB each |
| `gold_ch.plot_canopy_stats` (re-LLM) | source of truth, one row per parcel |
| `ref.plot_canopy_stats` (lamap_db) | daily FDW copy — **never write here** |
| `public.get_plot_canopy_stats(p_egrid text)` (lamap_db) | the only frontend read path |

Documented as `platform.standards` rule_key `plot_canopy_stats_rpc`.

## Stages

```bash
export RE_LLM_PG_URI='postgresql://…'          # re-LLM session pooler

python prepare_inputs.py                        # tiles.json + parcels.gpkg + buildings.gpkg
python chm_pipeline.py --shard 0 --nshards 8    # ×8, in parallel
python merge_stats.py                           # partials/*.npz → work/canopy_stats.tsv
python load_stats.py work/canopy_stats.tsv      # → gold_ch, then FDW push to ref
python sample_chm.py --file points.tsv          # ground-truth check: E<TAB>N<TAB>expected_m
```

⚠️ **The FDW push is for INCREMENTS, not the initial load.** `gold_ch.sync_plot_canopy_stats()`
runs row-by-row across postgres_fdw: the first 73k-row push exceeded its 900 s
`statement_timeout`. In steady state it is fine (~40 s) because the UPDATE is gated on
`computed_at`, so nothing changes between annual rebuilds. For a **first** load, or after a
full recompute, COPY `canopy_stats.tsv` straight into `ref.plot_canopy_stats` on lamap_db with
`INSERT … ON CONFLICT DO UPDATE` — same UPSERT semantics, executed locally, **2.7 s**. Then set
`ref.computed_at` equal to gold's, or the daily gate will never match and every run will try to
update all 73k rows across the wire.

The GitHub Actions workflow `ge_canopy_height_model.yml` wires exactly
this: `prepare` → 8-way `tiles` matrix → `reduce`.

## Three decisions that are load-bearing

**1. Buildings are masked, buffered by 1.5 m.** A canopy model cannot tell
a tree from a building — both are "surface above bare earth". In central
Geneva **64.9% of the ≥3 m area is building**, so unmasked, `height_p95_m`
reports building height and the whole product is wrong exactly where
promoteurs work.

The 1.5 m buffer is not cosmetic. Cadastral footprints are the **ground**
outline and roofs overhang them, so an unbuffered mask leaves a rim of
roof edge that survives as tall vegetation. Measured on tile `2500-1117`:
the rim is **41,875 m² reading ≥3 m at mean height 14.5 m** — a 15 m-tall
"hedge" ringing every block, plausible enough to pass review.

**2. Mask first, then clamp.** Post-mask the urban max falls to ~36 m,
comfortably inside the 50 m ceiling, so the clamp becomes a genuine
anomaly detector rather than a clip on real towers. Tiles still exceeding
50 m after masking are logged `suspect`.

**3. Newest DSM paired with nearest DTM, not same-year.** swisstopo runs
the two products on offset cycles. Over Geneva: DSM {2016, 2019, 2023,
2025}, DTM {2019, 2021}. Requiring a shared year yields only **328 of 357
tiles** and pins the canopy to 2019.

Measured on one tile:

| | median Δ | pixels moving >0.5 m |
|---|---|---|
| DTM 2021 − DTM 2019 | +0.085 m | **3.25%** |
| DSM 2025 − DSM 2019 | −0.030 m (mean +0.422) | **46.6%** |

Bare earth is static; the canopy is not. So the DSM vintage governs
accuracy and the DTM vintage barely matters — take the newest DSM, pair
the nearest DTM, cover all 357 tiles. `dsm_year` and `dtm_year` are
carried per parcel (modal where a parcel spans tiles) with a
`vintage_mixed` flag, because a single `source_year` column would lie.

## p95 across tile boundaries

Parcels straddle tiles, and a per-tile p95 averaged across tiles is wrong.
Each tile emits a **500-bin × 0.1 m histogram per parcel** over the ≥3 m
mask, plus `sum`, `count` and `max`. `merge_stats.py` sums the bin counts
and reads p95 off the cumulative distribution — exact to the bin width,
trivially mergeable, ~1 KB per parcel.

## The 3 m threshold

Separates trees from grass, hedges and vehicles. Every statistic
(`canopy_cover_pct`, `height_p95_m`, `height_mean_m`, `vegetated_area_m2`,
`height_max_m`) is computed over the ≥3 m mask only. It is stated in the
table comment, the column comments, the RPC comment and the standards row,
and echoed back to callers as `veg_threshold_m`.

## Gotchas that cost time

- **Hold GDAL datasets in a variable.** `gdal.Open(p).GetRasterBand(1).ReadAsArray()`
  lets the dataset be garbage-collected mid-expression and crashes with a
  `TypeError` deep in `gdal_array`.
- **`psql | grep` truncates large single-line output.** A 380 KB GeoJSON came
  back clipped at the head. Use `psql -o file` with
  `PGOPTIONS='-c client_min_messages=error'`.
- **Download, don't `/vsicurl/`.** Measured 1.7× faster (10.2 s vs 17.3 s per
  pair) because we read 100% of both rasters — COG range-reads only pay off
  for windowed access.
- **R2 objects are IMMUTABLE and the token has no DELETE.** `camelote-backups`
  enforces an object-lock policy, so re-uploading an existing key returns
  HTTP 409 `ObjectLockedByBucketPolicy`. `--force` alone therefore fails on
  every tile that already shipped — recompute with a **new version prefix**:
  `python chm_pipeline.py --force --version v2`. This makes each version an
  immutable snapshot, which is a reasonable property for a derived artefact:
  `v1` stays byte-for-byte what the published statistics were computed from.
- **5 parcels have ring self-intersections** and need `ST_MakeValid`
  (`prepare_inputs.py` does it): `CH376385826546`, `CH476387946555`,
  `CH557265856370`, `CH608963736534`, `CH676385596513`. Areas are preserved.

## Not in scope

The public XYZ display-tile pyramid is **deferred** pending a public R2
bucket and hostname — the current token cannot create buckets and
`platform.standards` 218/219 currently specify Supabase edge functions for
tiles, so an R2-served raster is a new pattern needing its own rule.
swissSURFACE3D LiDAR point cloud is a separate track.
