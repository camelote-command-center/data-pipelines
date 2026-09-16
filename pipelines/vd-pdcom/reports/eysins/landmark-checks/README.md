# Eysins current footprint and outer-area checks

The PR86 historical-map transform remains **research only / parcel matching not ready**. This review leaves it unchanged and preserves the reasons that automatic nearest-building matching cannot yet provide the missing controls.

## What was checked

The 690 original official footprint objects are retained with OBJECTID, EGID, commune BFS and building number. ESRI rings are combined by symmetric difference, checked for valid geometry, and transformed through the registered RE-LLM PostGIS route from EPSG:2056 to EPSG:21781. `reference-polygons-lv03.json` records that method and the original response hash. No new web dataset or runtime rows are introduced.

Using the unchanged PR86 transform, all 25 previously selected source components were checked against **filled polygons**, retaining candidate identities. This distinguishes points inside a building from distances to its boundary. Eleven centers are inside a footprint; twenty are within 5 m of a polygon. Ten components have multiple footprints within 5 m (38, 45, 50–56 and 61), making a nearest-object identity assumption unsafe. These components were previously used in exploratory work and are not independent controls.

Five centers remain outside the 5 m band:

| Component | Polygon distance | Nearest OBJECTID | EGID | Interpretation |
|---|---:|---:|---:|---|
| 30 | 28.278 m | 46533816 | absent | SDAN has a long black shape beside the road; no corresponding current footprint at its center. |
| 41 | 32.804 m | 46527291 | absent | L-shaped dark mark lies on the orange patch beside the junction; building identity is unestablished. |
| 48 | 8.567 m | 46431198 | 809321 | Small black source shape and nearby present outlines do not provide a confirmed matching corner. |
| 59 | 5.336 m | 46390073 | 809391 | Source component spans a large dark compound; several current outlines subdivide the vicinity. |
| 63 | 8.471 m | 46481829 | 3174965 | Small rectangular source shape and current outline are displaced; cause unestablished. |

These are **nearest candidates, not verified building identities**. Neither demolition, reconstruction, symbol error nor geometric misalignment is established as the cause. No component was silently removed, no reference was snapped, and no transform was adjusted to improve this diagnostic.

## Visual review

The full current-footprint overlay and all five context triptychs were inspected. The contexts retain SDAN alone, candidate-warped PGA plus current outlines, and SDAN plus current outlines. Pink lines represent the current reference snapshot, not source planning boundaries. Pixel axes remain source coordinates.

Outer-area side-by-side images (`north-`, `east-`, `south-correspondence-review.png`) were also inspected. The north has roads/field lines but much of the proposed envelope is covered by graphic overprints. The east shows changed or non-equivalent industrial building patterns, with no confirmed distributed corner pair established in this review. Southern field and road context is visually compatible, but is not an independently measured control. **No new manual control pair or independent accuracy claim is accepted.**

SDAN PDF20 names the orange/blue/hatched concepts but gives no legend establishing each black component as a cadastral building. PDF21 describes a strategic sector and two long-term alternatives; the extracted black shapes must therefore remain unverified until identified. The historical 1,000 residents/400 jobs applies to the overall Eysins group, not individual polygons or current capacity.

## Next work

Use identifiable road/cadastral junctions or a better official georeferenced historical base to establish distributed controls, especially north/east. Do not iterate nearest-point fitting again. Preserve the frozen current reference IDs and resolve source object semantics before accepting corners. Eysins remains an explicit unresolved commune; these research artifacts create no delivered sectors or parcels.

## Verification

Run `python verify.py` with the PR86 research environment (OpenCV 5.0.0.93, PyMuPDF 1.28.2, NumPy 2.5.3, Pillow, Shapely). It checks source hashes, original/reference identity parity, valid geometries, exact replay and the explicit release gates. No runtime dependency or workflow changes are required.
