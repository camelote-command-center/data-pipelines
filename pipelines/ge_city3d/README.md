# ge_city3d — realistic 3D city per commune → 3D Tiles for the Cesium view

Not a parser: a build tool over data the parsers already ingest. First commune: **Genève-Cité (21)** —
61 tiles, 2,863 buildings, 8,315 trees, ~357 MB. Built locally 2026-09-18 in ~15 min (4 processes).

```bash
export RE_LLM_PG_URI=...            # session pooler URI from the supabase-registry
python city_commune.py 21 out/                 # all 250 m cells touching the commune (+ 1 km LOD parents)
python city_commune.py 21 out/ 2500500_1117500 # selected tiles (tileset.json merges every tiles_*.json)
python -m http.server 8765  # then open cesium_view.html?ts=out/tileset.json — click a tree or a building for its height
```
Inputs: `bronze_ch.ge_cad_bati3d_*` (buildings), `bronze_ch.bfs_rebl_buildings` (façade style), SWISSIMAGE 10 cm
2023 (roofs + tree greenness, range requests), SITG MNA_HAUTEUR 2025 20 cm COG (trees, range requests), swissALTI3D
(tree base altitude), `gold_ch.building_roof_heights` (building height property). **No LiDAR needed since v4** — the
same builder runs for any Geneva commune. Tree detections are cached per 250 m cell in `<out>_trees/` (`TREE_CACHE`).

## What makes it look real — and what did not
- **Buildings from bati3d LOD2 planar faces, flat shading.** Draping the LiDAR over footprints (v1) melted roofs into blobs,
  streaked the eaves and made cardboard walls. Rejected by the operator on sight.
- **Roofs textured with SWISSIMAGE** (planar projection), **façades procedural and RegBL-driven**: `gbaup` → style
  (belle époque / inter-war / post-war / curtain wall), `gastw` → floor height, `gklas` 1211/1220/1230 or pre-1945 →
  ground-floor shopfront; colour varies per EGID within a style.
- **Trees from the SITG 20 cm height model** (`trees_mna.py`, v4 — was LiDAR class 5 up to v3): canopy peaks ≥ 4 m and
  ≥ 3.5 m apart, off building footprints (+1.5 m), kept only where SWISSIMAGE is green (ExG ≥ 0.04) and the surface is
  not jagged (local σ ≤ 2.5 m). Crown radius from neighbour spacing. See v4 below for the check against the LiDAR.

## ⚠️ Height datum — measured, contradicts older notes
swisstopo's Cesium terrain (`3d.geo.admin.ch/ch.swisstopo.terrain.3d`) stores **LN02 heights as ellipsoidal heights**
(≤ 4 cm vs swissALTI3D at three points). So position = LV95 → WGS84 lon/lat, height = LN02 **as is**. Applying the
real geoid (+49.8 m) made every building float 50 m high.

## v2 (2026-09-19) — built for the live view, driven by profiling
`profiling/` rebuilds the app's own Cesium3DView layer stack (lovable_dev 5653bb2b) in a standalone page and flies one
fixed path over Genève-Cité in a fresh headless Chrome per scenario (real GPU via ANGLE/Metal, empty cache):
`python profiling/run_prof.py "" buildings "buildings,trees,parcels" "buildings,pc" city` (serve this folder on :8765,
open prof.html?key=<public anon key>). Measured on an M1:

| scenario | fps | p95 frame | slow frames | note |
|---|---|---|---|---|
| terrain + SWISSIMAGE | 60 | 18 ms | 0 | baseline |
| app default (swissBUILDINGS3D + tree billboards + parcel outlines) | 50–51 | 25–27 ms | 12 | fine |
| + LiDAR point cloud (8 blocks, eye-dome lighting) | **25** | **99 ms** | **370** | the real frame killer; never finished loading in 30 s |
| city v1 (merged trees, 45 textures/tile) | 33 | 47 ms | 44 | 531 MB GPU, 140 MB download |
| city v2 (instanced trees, LOD, JPEG) | 41–44 | 29–31 ms | 36–45 | 77 MB download |
| **city v3 (v2 + neutral façades with tint, 16 draw calls/tile)** | **49.3** | **28 ms** | 36 | 87 MB download — **on par with the app default (49.6 fps, same run)** |

What changed in v2, each measured:
- **Trees via `EXT_mesh_gpu_instancing`** (3 low-poly variants, ~250 triangles) instead of ~1,600 merged triangles each.
- **EGID per building** via `EXT_mesh_features` + `EXT_structural_metadata` → `scene.pick(...).getProperty('egid')` works.
- **Base slabs** close buildings from below ("Voir sous le sol").
- **Levels of detail**: 1 km parents with 1 m roofs (REPLACE, geometricError 10 m) over 250 m leaves with 20 cm roofs.
  In 3D Tiles a child's `transform` is relative to its parent's, so leaves carry `inv(parent) @ leaf`.
- **Neutral façade textures + per-vertex tint (COLOR_0)**: draw calls per tile 47 → 16 at identical look.
- **Buildings UNLIT (no normals)**. With normals, Cesium applies real sun shading and every façade facing away from the
  sun goes dark; v1 had no normals by accident and that unlit look is what the operator approved. Trees keep normals.
- Every triangle is re-oriented after clipping (walls outward, roofs up, bases down): shapely clipping can flip rings.
- Note: Cesium's GPU memory figure fills up to `cacheBytes` (512 MiB default) regardless of what is on screen — lower
  `cacheBytes` in the app for small devices rather than reading ~500 MB as a leak.

Integration settings for the app: `maximumScreenSpaceError` 16 (the preview pages use 8, which loads more 20 cm
leaves than needed; a wide commune view at 8 reached 736 MB before settling) and `cacheBytes` ~256–384 MB on small devices.
Genève-Cité v3: 9 × 1 km parents + 61 × 250 m leaves, 2,863 buildings, 8,315 trees, 133 MB on disk.

## v4 (2026-09-19) — trees and heights from the 2025 height model, one-click measurements
Why: the unified 3D layer must cover the canton. LiDAR class 5 needs the raw point cloud (~2.1 TB for GE); the SITG
height model (MNA_HAUTEUR_2025_03 = surface − terrain, same March 2025 flight) is one 13 GB COG read by range requests.

**Checked against the LiDAR trees on 6 Genève-Cité tiles** (`validate_trees.py` logic, 651 LiDAR trees):

| detector | detections | real (have a LiDAR twin ≤ 2 m) | LiDAR trees found | height vs LiDAR |
|---|---|---|---|---|
| height-model peaks only | 876 | 59 % | 80 % | — |
| **+ greenness + roughness filter (shipped)** | **519** | **92 %** | **73 %** | median −0.16 m, 90 % within 0.40 m |

Most of the 27 % not found are small trees merged into a neighbour's crown; the 8 % extra are mostly hedges/shrubs ≥ 4 m.
Poles, awnings and structure edges scored ExG +0.00 vs +0.28 for crowns — that is what the filter removes.

- **Tree models measure true**: the 3 variants are normalised so the crown top is exactly at the scaled height and the
  crown radius exactly 0.5 × scale — measuring a tree with a height tool returns the detected height (variant 2 was
  6 % short and 21 % too wide before).
- **One-click height**: each tree instance carries `height_m` / `crown_m` (`EXT_instance_features` → property table
  `tree`); each building carries `egid` + `roof_height_m` (= `gold_ch.building_roof_heights.height_m`, the same value
  the drawer RPC `get_building_roof_height` returns; −1 = none). `scene.pick(pos).getProperty('height_m')`.
- **Every 250 m cell touching the commune** gets a tile (parks/woods hold trees but no buildings); trees are clipped to
  the commune so neighbouring communes never draw the same tree twice.
- 1 km parents reuse the leaves' cached detections (trees ≥ 8 m) — no second read of the rasters.

## Canton build (2026-09-19) — live at `https://3d.lamap.ch/city/v2/canton/tileset.json`
`run_canton.sh` (48 communes, 4 in parallel, shared per-cell tree cache) + `make_root.py`: **80,890 buildings,
912,526 trees, 5,612 tiles, 3.3 GB**, ~3 h on an M1. Three fixes found while verifying, all measured:
- **One property table per GLB** (`glb_writer`, `convert_single_table.py` for tiles built before): CesiumJS picks through a
  single table per model, so with separate building/tree tables, roof clicks returned nothing or a tree's height.
  Now `kind` 0/1, `egid`, `height_m` (roof or tree), `crown_m`. Checked: roof EGID 2039227 → 26.88 m = `get_building_roof_height`.
- **Per-km draw range** (`wrap_parents.py`, in the builder): each 1 km parent sits under an empty tile with its own box
  and geometricError 60 (≈ 2.9 km at MSSE 16). Before, a commune's parents were drawn whenever the camera was inside
  the commune's bounding sphere — over every neighbour. Flight over Genève-Cité: **43 → 56 fps** (p95 43 → 22 ms);
  served from 3d.lamap.ch: 54 fps, p95 24 ms. One commune alone: 59.5 fps.
- **Per-process temp files** in `trees_mna.py` (parallel builds overwrote each other's footprint mask).
- App settings: MSSE 16, keep Cesium's default cache — `cacheBytes` 384 MB + 128 MB overflow measured *slower*
  (36 fps, re-loading). The profiler's net/GPU MB figures are capped (250 resource-timing entries); trust frame times.

## Photomesh view (2026-09-19) — the look for the app: SITG Photomaillage 3D 2019 + Lamap 2025 measurements
SITG `3D_PHOTOMESH_2019_05` (Accès libre, level A; credit "© SITG") is streamed as I3S straight from SITG's ArcGIS
Online SceneServer (`Mesh3D_GENEVE_2019`, WGS84, EGM96 heights). CesiumJS `I3SDataProvider` reads it as is.
Measured on Genève-Cité (`cesium_view_photomesh.html`):
- **Alignment**: mesh ground vs swisstopo terrain −0.4 … 0 m with heights used as is (same no-geoid convention).
- **Buildings** measured on the mesh are within ~0.5 m of `building_roof_heights`; **trees read 0.6–2 m low** (2019
  vintage + photogrammetry rounds crowns) — so tree heights shown on click come from the 2025 data, not the mesh.
- **Unlit** (`CustomShader` UNLIT): the photos carry daylight; Cesium's sun made the mesh dark at dusk.
- **Globe clipped inside the canton** (`ClippingPolygonCollection`, outer rings of `pub/canton_outline.geojson`, inset 5 m):
  inside, the mesh is the ground; outside, terrain + basemap as before.
- **Parcels** (clamped GeoJSON / ground polylines) drape onto the mesh (classification BOTH).
- **Click identity without an extra layer**: `get_3d_feature_at(lon, lat)` (lamap_db, `sql/001_…`) → building (EGID +
  2025 roof height) / tree (2025 height + crown) / plot, ~30 ms as anon. An invisible 3D tree layer was tried first:
  pickable at alpha 0.01 but the parcel classification painted it, and it cost ~10 fps.
- fps on the Cité flight: mesh alone 60, our v4 model 54–56, app default ~50.
- Trees: `ref.plot_trees` now holds the 912,526 SITG-height-model trees (`source = mna-2025`, loaded with
  `ge_canopy_height_model/load_trees.py`; chm-v1 snapshotted to `backup.plot_trees_superseded_20260919`).
  `load_trees.py` refuses to let a `chm-*` vintage replace `mna-*` trees without `--allow-chm-over-mna`.

## Known limits / next
- Façade textures are procedural, not photographic.
- Canton build: ~5,000 cells; detection ~10–15 s per cell (network-bound on the three COGs).
