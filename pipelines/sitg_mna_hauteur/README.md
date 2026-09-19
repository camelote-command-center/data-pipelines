# sitg_mna_hauteur — SITG MNA Hauteur 2025 (20 cm) → per-parcel and per-building heights

Source: `MNA_HAUTEUR_2025_03`, **one canton-wide Cloud-Optimized GeoTIFF**, 20 cm, Float32, EPSG:2056,
13 GB, MNS − MNT (height above ground), from LiDAR flown **at night 21 Mar – 1 Apr 2025 (leaf-off)**.
Read in place with HTTP range requests — never downloaded whole.

```
prepare.py   HEAD the COG; exit skip=true if Last-Modified unchanged (unless FORCE); build tiles.json,
             parcels.gpkg (global pid), bldg_buf.gpkg (+1.5 m), bldg_fp.gpkg (−0.4 m, id/egid)
worker.py    <shard> <nshards>: 1 km windows -> raw accumulators (.npz): counts, sums, maxima, fixed-bin histograms
merge_load.py merge -> upsert bronze_ch.ge_mna_hauteur_parcel_stats + _building_heights -> PATCH per dataset code
```
Test on one commune: `COMMUNE=21 python prepare.py && python worker.py 0 1 && COMMUNE=21 python merge_load.py`
(partial runs publish only that commune's rows and never PATCH freshness).

## Decisions and traps (measured 2026-09-18)
- **Source defect:** isolated pixels at ~377 m (16 px on the Eaux-Vives lakeshore, 2500999.8/1117884.8) where the
  terrain model had a hole, so MNS − MNT leaked the *absolute altitude*. Values > 200 m are dropped as no-data and
  counted per run. Nothing in Geneva stands 200 m above ground.
- **Buildings masked for vegetation** with `ge_buildings_geo` +1.5 m (roofs overhang footprints), 3 m tree threshold.
  Building heights use the footprint −0.4 m to drop roof/ground edge pixels.
- **p95 from merged histograms**, never from per-tile summaries (parcels straddle tiles).
- **Validation, Genève-Cité:** canopy cover 5.45 % vs 5.46 % in `gold_ch.plot_canopy_stats`, correlation 0.976, tree
  maxima within 0.27 m — leaf-off costs almost nothing here. Building heights vs bati3d photogrammetry (2,532 EGIDs):
  median +1.23 m (LiDAR max includes chimneys), 67.8 % within 3 m, 8.7 % off > 10 m (open question: multi-footprint EGIDs).
- Parallel to the CHM, **not a replacement**. Compare canton-wide before any swap.
- Hold every GDAL/OGR dataset in a variable — chained `Open(..).GetLayer(0)` frees it mid-expression and crashes.

## Promotion to the app (2026-09-19) — `sql/002_gold_promote.sql` (re-LLM), `sql/003_ref_lamap_db.sql` (lamap_db)

| from (bronze) | to (gold, re-LLM) | lamap_db (FDW copy, never write) | app read path |
|---|---|---|---|
| `ge_mna_hauteur_parcel_stats` | `gold_ch.plot_canopy_stats` | `ref.plot_canopy_stats` | `get_plot_canopy_stats(p_egrid)` (unchanged contract) |
| `ge_mna_hauteur_building_heights` | `gold_ch.building_roof_heights` | `ref.building_roof_heights` | `get_building_roof_height(p_egid)` (new, `platform.standards` `building_roof_height_rpc`) |

- **Canopy**: the drawer switched from the 0.5 m swisstopo CHM (mixed 2019–2025 vintages) to this 20 cm, single-flight
  model. Measured before the switch on 61,203 parcels: cover |diff| 1.22 points, max-height correlation 0.985. The 18
  parcels whose height-model max exceeds 50 m (the altitude-leak defect shows below the 200 m cap too) are not
  promoted and keep their CHM row. `ge_canopy_height_model/load_stats.py` no longer overwrites promoted parcels.
  Backups: `gold_ch._bak_plot_canopy_stats_20260919` (re-LLM), `backup.ref_plot_canopy_stats_20260919` (lamap_db).
- **Building height** = height-model p95 of the roof. bati3d LOD2 is used instead when they disagree by > 10 m (sheds
  under tree crowns: the height model sees the canopy) or the height model reads < 2 m; with neither, NULL /
  `height_source = 'none'`. 2026-09-19: 77,254 `mna_2025`, 5,433 `bati3d`, 191 `none`. p95 because max is off by
  > 10 m on 6.9 % (antennas, cranes, overhanging trees) vs 4.6 % for p95.
- **Push**: daily crons 101 / `sync_building_roof_heights` carry increments (~40 s no-ops). A new flight changes every
  row, and one FDW statement cannot push ~80k rows inside 900 s, so a full `merge_load.py` run promotes and then
  pushes commune by commune (`CALL gold_ch.sync_…(<no_commune>)`), and asserts gold = ref counts. Single-commune runs
  never promote. `SKIP_PROMOTE=1` stops after bronze.
- First load was a direct COPY into `ref` (2 s), with `computed_at` copied from gold so the daily gates match.
