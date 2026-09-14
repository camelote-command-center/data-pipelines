# Prangins: official footprint review — 14 September 2026

Seven pre-existing holdout outliers checked against the official Vaud API/APIGeo MapServer layer 22 (Batiments), without refitting the frozen alignment. Each 160 m query window has an independently reconciled feature count; no truncation was accepted. Reference coordinates and saved source-building geometry are EPSG:2056. Query URLs, response SHA-256 and fetched date are retained per window. These hashes identify the fetched response; the JSON artifacts add computed distances and are not byte-identical raw responses.

The old references are registered building location points, not cadastral footprint centroids. A large nearest-point residual cannot be interpreted directly as positional error. The PDF is from 2013 and the footprint service was queried in 2026; temporal or representation differences are possible but their causes are not established.

| Holdout | Old point residual (m) | Point-to-nearest footprint (m) | Best polygon IoU |
|---|---:|---:|---:|
| 0 | 500.14 | 0.00 | 0.4731 |
| 3 | 19.96 | 0.00 | 0.0353 |
| 15 | 13.54 | 9.33 | 0.0117 |
| 22 | 13.12 | 7.30 | 0.0000 |
| 72 | 11.46 | 4.46 | 0.0000 |
| 88 | 32.43 | 0.00 | 0.1510 |
| 169 | 12.96 | 7.05 | 0.0000 |

The source centroids for 0, 3 and 88 lie inside current footprints, but that does not establish matching footprint shape. Holdout 0 is in Nyon, outside the original Prangins-only point reference set. For 15, 22, 72 and 169, current footprint geometry has little or no overlap with the selected source building. All seven overlays were visually inspected: neighbouring outlines often align while individual source/current shapes differ. Do not infer demolition, construction dates or a general precision bound from this evidence.

Magenta outlines are current official footprints; blue circles identify heldout PDF centroids. The background is the original source PDF. IoU compares the selected PDF building polygon with individual service features, which may represent different building parts; it is diagnostic only. These outliers were selected by prior residual, so they are not an unbiased accuracy sample.

All 15 Prangins sectors remain review_required. The 10 m parcel boundary flag remains a heuristic. No public release or commune completion follows from this review. Next: check parcel boundaries and category semantics independently and assess source/current building differences before release.

## Reproduce

Run `python pipelines/vd-pdcom/reports/prangins/footprint-review/reproduce.py /path/to/Prangins.pdf` with numpy, shapely and PyMuPDF. The PDF SHA is checked before processing; reference snapshots are frozen. This rewrites the summary and seven overlays using the existing alignment; it makes no network or database calls.
