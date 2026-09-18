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

## Known limits / next
- Tiles merge geometry by material, so **per-building picking (EGID → drawer) is not wired yet** — needs
  EXT_mesh_features feature IDs per building.
- No LOD: every tile is full detail. Tree-heavy tiles reach ~11 MB — lighten tree meshes, add a coarse LOD.
- Façade textures are procedural, not photographic.
