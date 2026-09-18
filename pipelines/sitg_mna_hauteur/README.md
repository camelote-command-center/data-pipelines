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
