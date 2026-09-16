# Mortaigue overview: grid calibration and independent reference checks

The November 2025 inquiry map's **1:5,000 overview** can be calibrated from its native coordinate grid. This work does not calibrate the separate 1:1,000 detail panel or establish final planning approval.

## Method and reproducibility

`analyse.py` hash-checks the PDF and both previously acquired official-reference snapshots. Grid drawings 805/806 locate eastings 2,546,000 and 2,546,500; drawing 807 locates northing 1,183,500. Labels were visually read in the overview. The 500 m horizontal interval defines scale; north-up orientation and equal vertical scale are assumed because only one horizontal gridline is available. No building or parcel reference is used to fit, refine, rotate, snap or rescale the transform.

The resulting scale is 1.7643331608915913 metres per PDF point. The transformation is:

```
E = 1.7643331608915913 * x + 2544440.0380865084
N = -1.7643331608915913 * y + 1183779.8532407347
```

Gray source footprint paths are then checked against the 16 September 2026 official layer-22 snapshot already retained in `../boundary-research/`. Before any matching, 24 paths crossing the panel bounds and seven paths overpainted by the title/scale boxes are excluded. The latter were identified during visual review; raw geometry underneath a caption is not counted as a visible control. All exclusion indices/reasons remain in `results.json`.

## Results and limits

- All **230 remaining candidates** have unique nearest reference OIDs within 10 m; no unmatched or duplicate matches. This count supersedes the initial 237 raw-path count before caption masking.
- Centroid RMSE **0.0774 m**, median IoU **0.9766**, maximum Hausdorff distance **0.0846 m**. No matches fall below IoU 0.85 or exceed 2 m Hausdorff distance.
- These are checks independent of transform fitting, **not independently surveyed accuracy**. The map and API footprints can share cadastral source data. The comparisons test coordinate recovery and source agreement.
- Five checks (four spatial extremes and lowest IoU) were visually reviewed. `render_checks.py` renders source plus magenta reference outlines; the blue centroid hull is a coverage diagnostic, not an accuracy envelope.
- **12.39%** of the AGP contour lies outside that centroid hull. Its nearest check centroid is 51.43 m from the contour. Strong overall reference agreement does not remove the extrapolation limit.

The transformed AGP paint contour has area **10,235.59 m²**, whereas the source parcel table states **10,411 m²**. The **175.41 m² (1.68%) discrepancy remains unresolved**. Neither the scale nor the contour was adjusted to force agreement.

Against the hash-verified layer-21 snapshot in `../proposed-perimeters/`, intersections greater than 1 m² are **10,213.91 m² with parcel 326** (EGRID CH974597878390) and **21.67 m² with cantonal DP 1051** (EGRID CH238397457650). The latter is a retained edge discrepancy, not another opportunity. No ownership data is requested. These are two research intersections, not runtime parcel candidates.

## Delivery decision

Save the calibrated geometry and checks only in the existing Yvonand research record. The contour denotes **proposed protected agricultural land for ecological compensation**, not buildable capacity. Final decisions, the area/edge discrepancy, original Mordagne plan, revised PDCom and historical thematic reconciliation remain outstanding. No private runtime sector, public sector, or commune-completion claim is added.
