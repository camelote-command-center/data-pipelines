# Eysins SDAN–PGA correspondence candidate

Research only: **not ready for parcel matching or receiver delivery**. This establishes a reproducible bridge between the SDAN 2006 illustration and the adjacent historical PGA. It does not establish current planning rights or complete commune coverage.

## Sources and coordinates

`inputs.json` gates every source by SHA-256. SDAN PDF page 20 / xref 115 is the original 524×469 image. PGA PDF `../source-update/5650969.pdf` / xref 4 is rotated clockwise and resized to 1000×1414, exactly the coordinates used by `../pga-grid/calibration.json`. Image coordinates have a top-left origin and downward y. `correspondence.json` explicitly contains LV03 / EPSG:21781 coordinate arrays, not RFC 7946 GeoJSON.

## Method and evidence

`match.py` detects SIFT descriptors, applies a two-neighbor distance ratio, mutual nearest-descriptor matching, and 2-pixel source/target spatial deduplication. Similarity RANSAC uses a fixed seed, 5 target-pixel threshold, 10,000 iterations and 0.999 confidence. The target mask covers the village. Matching APIs follow the [OpenCV matcher documentation](https://docs.opencv.org/5.0/main_modules/classcv_1_1BFMatcher.html).

The strict grayscale ratio 0.7 proposal has 22 pairs / 10 inliers. Ratios 0.8 and 0.9 yield 59/17 and 204/28, with broadly consistent transforms. Three dark-mask alternatives yield only 2–3 inliers and are rejected. Earlier one-way matches collapsed onto a single target and are not evidence of alignment; mutual matching and spatial deduplication prevent that failure here.

The strict proposal is composed with the previously calibrated PGA printed grid. Its in-sample correspondence residual is **2.515 m RMSE**. This is image fit consistency, **not independent geographic accuracy**. The PGA grid's separate 1.948 m axis-coordinate check RMSE is additional evidence with different meaning; these numbers are not combined into an accuracy claim.

`landmark-contexts.png` was visually reviewed: the strict matches show corresponding road, parcel and building patterns. Descriptor locations are context centers, not surveyed corners. Three matches cluster in the southwest neighborhood. `historical-map-overlay.png` was visually reviewed: the seven outlined envelopes occupy plausible historical Chise, Nipy, Terre Bonne and Sous Cor areas.

## Coverage and sensitivity

| Envelope | Outside strict inlier hull | Maximum shift among three grayscale fits |
|---|---:|---:|
| E1 | 95.5% | 6.05 m |
| E2 | 97.7% | 2.79 m |
| E3 | 4.6% | 3.66 m |
| E4 | 100% | 4.71 m |
| E5 | 100% | 4.02 m |
| E6 | 100% | 7.82 m |
| E7 | 100% | 2.95 m |

Most illustrated areas extrapolate beyond the matched landmarks. Ratio variants reuse the same images and descriptors; their spread is a sensitivity check, not an uncertainty bound or held-out validation. No variant was selected to improve agreement with current buildings.

As a separate, untuned diagnostic, the 25 building-like components previously selected in PR82 are transformed and compared with the 690 frozen current official footprint outlines. Twenty centers are within 5 m of an edge, but component IDs 30 and 41 are 28.28 and 32.80 m away. IDs 48, 59 and 63 are also beyond 5 m. Historical changes, symbol selection and wrong identities remain unresolved. Edge distance does not identify a building, distinguish inside/outside, or measure point accuracy. These reused components are not independent controls.

## Next gate

Identify distributed, explicit corresponding corners around the northern and eastern envelopes; reserve checks before any further fitting. Resolve current footprint identities and the conspicuous outliers. Only then assess commune attribution, cadastral intersections and buffer sensitivity for a private review candidate. Preserve the 2006 strategic/indicative semantics and E7's optional long-term status. No new runtime sectors, parcel candidates, public data or completed commune were created by this report.

## Replay

Research environment: Python 3.13, opencv-python-headless 5.0.0.93, NumPy 2.5.3, PyMuPDF 1.28.2, Pillow and Shapely. OpenCV is a research-only dependency; the parser workflow is unchanged.

Run `python verify.py` in this directory. Verification checks frozen inputs, outputs and exact replay, valid seven envelopes, explicit delivery gates and the failed alternatives. Research coordinate arrays are deliberately separate from runtime ingestion.
