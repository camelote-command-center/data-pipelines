# Yvonand boundary and alignment research — 16 September 2026

Exact source7 SHA d4fecb86752e2f2cb2c3359e835e9555694979b88288bb50795080ec349a1a2b, source document b8fc92d6-c4a6-5a3c-8fee-dfa2b11de848 page1. Still research, no sector or parcel delivery.

## Four previously invalid contours

`reconstruct_evenodd.py` reproduces the actual even-odd PDF fill rule by noding original straight segments, polygonizing and testing face parity. No coordinate displacement, buffer, snapping or arbitrary repair. Original commands remain in the PR103 artifact. All four now have valid paint geometry. Drawing41 retains two faces including a small self-crossing sliver;737 a single area with a retraced zero-area spur;783 two faces;915 one face. OpenCV raw-path even-odd rasterization versus reconstructed faces at12px/point has zero differing interior pixels; differences remain within a one-pixel raw boundary (37/1024/23/4 respectively). The long retraced spur explains737's boundary-only differences. This resolves paint representation, not planning semantics or enclosing projected-area boundaries.

## Official references and exploratory translation

Acquired 1,655 current official layer22 footprints within Yvonand's municipal bounding envelope (includes neighbours). Count query matches full unique OBJECTID count; all geometries valid/nonempty. Query URL/time/response and stored-file SHA recorded. These are footprint records, not necessarily individual buildings or dwellings; EGID may be null.

Source extraction yields1,620 black filled path candidates; they include buildings AND symbols. Fixed printed1:5000 gives1.7638888889m/PDFpoint. North-up translation estimate is E=2544324.804072453,N=1184695.317947909, with display-y inverted. No rotation/shear/scale fitted. Training source paths use drawing_index modulo5 !=0 and area>70m². Reserved source paths never participate in histogram selection or median refinement. Histogram uses shape dimensions/area; translation refinement uses training proximity/area. Source identifiers/algorithm/parameters are retained.

457 training matches,457 unique target OBJECTIDs: centroid RMSE0.3673m, median footprint IoU0.9631. Of248 reserved source candidates,114 pass the proximity/area correspondence gate;134 remain unmatched. Reserved114 target OBJECTIDs are unique and disjoint from training; centroid RMSE0.5241m, median IoU0.9617.107/114 have IoU≥0.85;104/114 Hausdorff≤2m. Maximum reserved Hausdorff7.486m, maximum training11.592m. Full results include every accepted correspondence, including shape discrepancies, without refitting to reserved points.

Four spatially spread reserved examples and the worst-IoU example were visually checked against the source composite. Outlying worst example has a footprint difference, not an excuse to retune the fit. Counts/RMSE refer only to correspondence-filtered matches, NOT all248 reserved candidates. This is geometric consistency evidence, not surveyed accuracy or historical building identity. Subsequent changes, duplicated/altered footprints and symbol classification still require review. Keep translation provisional until sector-local coverage and all applicable discrepancies are assessed.

Reproduce in order: `source_black_paths.py`, `coarse_alignment.py`, `check_shapes.py`, `reconstruct_evenodd.py` with source7 locally under `../source-review/`. The first script checks source hash. Snapshot current-footprints.geojson and receipt are versioned. `check_shapes.py` produces the final augmented alignment JSON and contact sheet.

Next: identify enclosing boundaries of proposed hatch areas, assign existing/projected meanings, test each sector's coverage by reserved controls, review currentness/constraints, and only then create scoped private sector/parcel candidates. The1,620 black paths and204 colored paths are not sector counts. No new workflow dispatch was needed for this research.
