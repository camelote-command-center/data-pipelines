# ge_city3d — realistic 3D city per commune → 3D Tiles for the Cesium view

Not a parser: a build tool over data the parsers already ingest. First commune: **Genève-Cité (21)** —
61 tiles, 2,863 buildings, 8,315 trees, ~357 MB. Built locally 2026-09-18 in ~15 min (4 processes).

```bash
export RE_LLM_PG_URI=...            # session pooler URI from the supabase-registry
python city_commune.py 21 <copc_dir> out/                 # all tiles of the commune
python city_commune.py 21 <copc_dir> out/ 2500500_1117500 # selected tiles (tileset.json merges every tiles_*.json)
python -m http.server 8765  # then open cesium_view.html (expects out/ at ./commune21/)
```
Inputs: `bronze_ch.ge_cad_bati3d_*` (buildings), `bronze_ch.bfs_rebl_buildings` (façade style), SWISSIMAGE 10 cm
2023 (roofs, read by range requests), SITG LiDAR 2025 COPC mirrored by `sitg_lidar_2025` (trees, terrain for tree heights).

## What makes it look real — and what did not
- **Buildings from bati3d LOD2 planar faces, flat shading.** Draping the LiDAR over footprints (v1) melted roofs into blobs,
  streaked the eaves and made cardboard walls. Rejected by the operator on sight.
- **Roofs textured with SWISSIMAGE** (planar projection), **façades procedural and RegBL-driven**: `gbaup` → style
  (belle époque / inter-war / post-war / curtain wall), `gastw` → floor height, `gklas` 1211/1220/1230 or pre-1945 →
  ground-floor shopfront; colour varies per EGID within a style.
- **Trees from LiDAR class 5**: canopy peaks ≥ 4 m and ≥ 3 m apart, off building footprints; crown radius from
  neighbour spacing; trunk + 5 jittered crown blobs.

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
| city v2 (instanced trees, LOD, JPEG) | 41 | 31 ms | 45 | 77 MB download |

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

## Known limits / next
- Tiles merge geometry by material, so **per-building picking (EGID → drawer) is not wired yet** — needs
  EXT_mesh_features feature IDs per building.
- No LOD: every tile is full detail. Tree-heavy tiles reach ~11 MB — lighten tree meshes, add a coarse LOD.
- Façade textures are procedural, not photographic.
