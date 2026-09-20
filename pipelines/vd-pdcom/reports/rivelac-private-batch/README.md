# Rivelac: two indicative outlines for private review

The batch reviews five source maps and retains three as exceptions: Les Fourches (2.9), Les Marais (2.10) and Nestlé (4). Closing a color mask merges narrow roads and omits building-edge regions in those maps. They are not admitted by this module.

Two outlines can now be positioned for private review:

| Site | Source page | Meaning | Outline | Source table |
|---|---:|---|---:|---:|
| Pré-des-Fourches, Noville (2.4) | 61 | Proposed new productive activity zone; current activity-zone area 0 ha | 69,156.9 m² | Future 7 ha |
| Pré-Neuf, Villeneuve (2.11) | 80 | Densification of a largely built mixed-activity site | 151,642.7 m² | Current/future 15.3 ha |

These are full indicative zone outlines, not available land or legal parcel boundaries. Source updated 30 January 2025; exact-version cantonal approval and later currentness remain unresolved. Implementation horizons are source scenarios, not completion promises.

## Alignment and boundaries

The unchanged SIFT/RANSAC transforms use an official EPSG:2056 cadastral reference image. Raw descriptor holdout RMSEs are poor because some descriptors match the wrong buildings; all original matches and errors remain in `evidence.json`. Identity review was performed before calculating the selected control errors. For Noville an additional southern building corner was read from independent source/reference grids before its residual was calculated; the building's changed northern footprint was excluded by identity, not residual. No refit was performed.

Six identified Noville controls have RMSE 2.373 m, maximum 4.931 m. Four identified Villeneuve controls have RMSE 1.115 m, maximum 1.580 m. Each outline is fully inside its identified-control convex hull. This is an image alignment check, not surveyed accuracy.

Noville's outer edge is manually traced across hatch endpoints. Villeneuve uses a visually reviewed external color contour with a 3×3 closing operation to reconnect internal boundary strokes and retain enclosed buildings. Kernel sizes 5 and 7 alter that outline by only 36 and 51 square pixels respectively. This method is not automatically applied to other maps. Both outlines were visually overlaid on the official reference. Areas were compared with the source table but never scaled to force agreement.

## Parcel and delivery limits

44 official reference records are frozen with exact EWKB alongside display GeoJSON (11 Noville, 33 Villeneuve). Positive-area intersections produce 4 and 15 pairs respectively, all land objects, including six small public-road edge intersections. The Noville outline intersects approximately 61.8% of parcel 1133; the whole parcel is not promoted as a reserve. Road-edge intersections remain flagged by the existing 10 m heuristic, which is not an accuracy confidence interval.

The parser checks the exact source ID/hash, restricts admission to these two sites, rejects changed reference membership/geometry/type and preserves prior source evidence. The existing private receiver route enforces `review_required` / `internal_review_only`, exact row parity and valid EPSG:4326 geometry. Commune completion and `delivery_status` remain unchanged. A repeated persistence run must reproduce the same sector IDs and parcel pairs.

The initial persistence attempt rejected the frozen GeoJSON because its default nine-decimal serialization changed coordinates by up to 5.3e-10 m. No sector was written by that attempt. The reference snapshots now retain exact EWKB and strict equality remains enforced; no tolerance was added. The failed monitoring attempt remains linked to its successful retry.
