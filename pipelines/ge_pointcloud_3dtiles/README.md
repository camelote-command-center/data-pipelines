# GE point cloud → 3D Tiles (swissSURFACE3D LiDAR for the Cesium 3D view)

Converts swisstopo swissSURFACE3D **2025 COPC** into Cesium-consumable 3D
Tiles carrying **height above ground, ASPRS classification and true colour**
per point — the four things the 3D view needs (photoreal geometry, height
ramp, class filter, hover tooltip) come from one dataset.

Replaces the swissTLM3D vegetation tileset (`VEGETATION_URL`,
`src/components/app/map3d/Cesium3DView.tsx`), which is generic tree models
on surveyed positions.

> **Branch note.** The Cesium 3D view lives on **`lovable_dev`** in
> `lamap-2025/lamap-2025`. `main` is ~9,400 commits behind (head `84752088`,
> 2025-11-21) and `develop` does not have it either. Grepping `main` or
> `develop` for `Cesium3DView` returns nothing and leads to the wrong
> conclusion that no renderer exists. Verified 2026-07-23 via the GitHub API.

## Pipeline

```bash
python pilot.py both          # COPC -> attributed LAZ, both densities
python tileset.py             # per-commune 3D Tiles + batch-table assertions
rclone copy pilot_tilesets/<density>/<commune> \
  r2-lamap-3d:lamap-3d-public/pointcloud/v1/<density>/<commune>
```

Per tile: `readers.copc` → `filters.hag_dem` → `filters.colorization` →
`filters.range` (drop class 7 noise) → optional `filters.decimation` →
`writers.las` PDRF 7 + `HeightAboveGround` extra dim.

## Decisions, each measured not assumed

**HAG — use `filters.hag_dem`.** A raw point carries only absolute `Z`;
colouring by `Z` would make a lawn on a hilltop read as "tall". Benchmarked
on 23.74 M points (tile 2502-1117):

| filter | time | ground p50 | veg p95 | veg max |
|---|---|---|---|---|
| **`hag_dem`** | **17.6 s** | +0.000 m | 23.74 | 40.14 |
| `hag_nn` | 47.2 s | +0.000 m | 23.75 | 40.08 |
| `hag_delaunay` | 101.6 s | +0.000 m | 23.75 | 40.24 |

All three agree to **~0.15 m**. `hag_dem` wins on speed *and* robustness: it
takes ground from **swissALTI3D**, not from the cloud's own class-2 points,
so it does not degrade where ground is occluded (dense canopy, courtyards) —
exactly where the other two are weakest. ⚠️ `hag_dem` **floors at 0**;
`hag_nn` permits negatives (−7.61 m seen). Absence of negative HAG is
expected, not a bug.

**Classification — the documented scheme does NOT hold in the 2025 vintage.**
Measured: class 2 ground 42.9%, class 3 "low vegetation" **39.1%**, class 6
building 11.9%, **class 26 undocumented 4.6%**, class 1 1.6%, class 9 water
0.02%, class 17 bridge 0.01%. **Classes 4 and 5 do not exist**, and class 3
reaches **40.1 m above ground** — every tree is "low vegetation". So:
- vegetation tiers must come from **HAG thresholds**, not class lookup
- **class 26** is unresolved — hide by default, expose as a toggle, do not
  discard. Ask `geodata@swisstopo.ch` for the 2025 spec
- **class 6** is kept in the LAZ and filtered in Cesium via
  `${classification}`, so swissBUILDINGS3D stays the default building
  representation and the toggle is free

**Colorisation is mandatory.** Source is PDRF 6, 30-byte records — **no RGB**.
`ch.swisstopo.swissimage-dop10` **2023 at 0.1 m** is `LAYOUT=COG`, on the
**same 1 km LV95 grid**, readable via `/vsicurl/`: 0.8 s to read, 1.1 s to
sample 23.7 M points. ⚠️ Ortho 2023 vs cloud 2025 — a tree felled in 2024 is
coloured from imagery that still shows it.

**Stay on 3D Tiles 1.0 / `pnts`.** 1.1 was tested and rejected: py3dtiles
12.1.1 **crashes** (`KeyError: 'heightaboveground'`, no `tileset.json`) on
`--extra-fields HeightAboveGround`, and **silently drops the field** when
lowercased. What does arrive is `POSITION`, `COLOR_0`, `_CLASSIFICATION` as a
raw glTF vertex attribute with **no `EXT_structural_metadata`** and no schema
— which Cesium's declarative `Cesium3DTileStyle` cannot address via
`${classification}`. So 1.1 costs the height ramp *and* the class filter.
`pnts` carries both in the batch table. Revisit when py3dtiles emits
structural metadata.

**`pnts` triples the data.** It is uncompressed: **20.1 bytes/point** against
6.6 in COPC. One tile, 150 MB COPC in → **476.3 MB out (303% of input)**,
2,685 files, 8 LOD levels, 45.4 s. py3dtiles has **no thinning option** and
retains **100.0%** of points across the pyramid, so decimation must happen
upstream in PDAL.

**Per-commune tilesets, not canton-wide.** Independently buildable and
fixable; a canton build is a single-machine 4-hour job over 41 GB that must
succeed as one unit. Storage is less of a constraint than it looks: 3D Tiles
is LOD-driven, pnts files are median 171 KB, and a street-level viewport
pulls a few dozen — single-digit MB, not the tile total.

## Two silent-failure traps

1. **`--extra-fields` is case-sensitive and only warns.** `Classification`
   was dropped with a warning while `classification` worked, producing a
   tileset that looks fine until the styling does nothing. `tileset.py`
   **asserts on the batch table after every conversion** and fails the job.
2. **Resume guards must verify the byte count, not just existence.** A killed
   run left a truncated COPC (113.7 MB of an expected 145.5 MB); PDAL read
   it, emitted 2 KB, and **exited 0**. `pilot.py` verifies against the STAC
   `Content-Length` and asserts the output is non-trivial.

## Source extent

**329 of 357 GE tiles have LiDAR**, 41.54 GB COPC, mean 126.3 MB, measured
density **23.7 pts/m²** → ~6.27 B points canton-wide. The 28 tiles without
are all north-east — **open Lake Geneva**, where swisstopo does not fly.
⚠️ A "307 tiles / 38.6 GB" figure comes from the narrower bbox
`6.0,46.15,6.31,46.33`; use the CHM's `5.95,46.13,6.32,46.37`.

## Cross-validation

The Cologny tree, three independent sources: CHM raster **39.17 m** ·
radial-profile crown test · swissSURFACE3D LiDAR **p99 39.18 m**, classified
vegetation. HAG is carried through `pnts` verbatim (source and tileset agree
exactly on min/max/p99.99). See `pipelines/ge_canopy_height_model/`.

## Credentials

`rclone` remote `r2-lamap-3d` → bucket `lamap-3d-public`. Credentials live in
the operator's rclone config and **must never be committed**. Public read is
served from the bucket's R2.dev development URL with CORS allowing GET/HEAD
from any origin and exposing `ETag, Content-Length, Content-Range,
Accept-Ranges` — verified from live response headers, not assumed.
