# sitg_lidar_2025 — SITG LiDAR 2025, commune-scoped COPC mirror

On demand, **never the canton** (5,505 × 250 m tiles × ~381 MB ≈ 2.1 TB). Dispatch with a commune list:
`sitg_lidar_2025.yml` → `communes=21`. Output: COPC per tile on R2
`camelote-backups/lamap-2025/lidar/sitg-2025/<version>/<E>_<N>.copc.laz` + one row per tile in
`bronze_ch.ge_lidar_2025_tiles` (points, density, class counts, RGB, R2 key).

```
lidar.py plan <communes>        resolve the URL list from the SITG page (its filename carries a hash), list tiles
lidar.py fetch <shard> <n>      download .las.zip -> PDAL writers.copc (326 MB -> 88 MB, 3.7x) -> R2 -> stats row
lidar.py load                   upsert manifest -> PATCH ge_lidar_2025_tiles
```
R2 `camelote-backups` is **write-once** (HTTP 409 on overwrite): recompute with `version=v2`.

## The catalogue is wrong — measured on Genève-Cité, 2026-09-18
| catalogue says | reality |
|---|---|
| LAS 1.4 / PDRF 1 | **LAS 1.2 / PDRF 3, with RGB** |
| ≥ 100 pts/m² | **~153 pts/m²** (9.59 M points on one 250 m tile) |
| class 16 = "Bruit" | **class 16 = street / pavement surface** — height above ground 0.01 ± 0.05 m, evenly spread over every street, 26.5 % of an old-town tile |

**Treat class 16 as ground.** Filtering it as noise punches a hole in every street. Tile classes on the old-town
tile 2500500_1117500: building 6 = 56.9 %, streets 16 = 26.5 %, ground 2 = 9.1 %, high vegetation 5 = 5.1 %.
Unlike swissSURFACE3D 2025, **classes 4 and 5 exist**, so vegetation tiers come straight from the classification.
Genève-Cité: 74 tiles intersect, 70 exist (the 4 others are open water), ~6 GB of COPC.
