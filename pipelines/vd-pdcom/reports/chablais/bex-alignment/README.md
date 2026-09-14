# Bex PUM.7 detailed inset alignment

Source: Chablais PA4 programme of measures, PDF page 76 (printed 70), document `e9288af6-8ecb-5be2-a07e-0581b0a3b068`, SHA256 `7d2fa9b86cb8bb49757eb935bd0735ff3a1476aebc8de39aed4cd39d905f42d5`. The sole 809 × 979 pixel inset is extracted only after checksum verification. The source explicitly requires further perimeter refinement; alignment never converts it into a cadastral/legal boundary.

## Method and result

Two building-pigment targets (RGB 157/154/149 and 126/55/49; Euclidean distance <14) yield 228 connected-component centroids after excluding edge-touching, <30-pixel and >5000-pixel components. Coordinates are source pixels with Y flipped. Component centroids are approximate building references, not identified surveyed points.

The registered RE-LLM `bronze_ch.vd_batiment_rcb` supplies 1,581 active EGID/location points in LV95 envelope (2566000,1121300)–(2567700,1123500). Sorted EGID input is frozen in `reference-points.json`. The envelope was localized with the official geo.admin.ch address search for Avenue de la Gare 68, Bex; that address point is not a fitted control.

Every fifth source point is withheld **before** fitting: 182 training, 46 holdout. FFT coarse rotation/scale/translation search followed by robust nearest-point optimization uses training points only. The best solution has 175/182 training points within 5 m; the next coarse candidate has 33. Scale is 0.8618162 m/source pixel and rotation −0.0001655 radians. Full parameters/origins are in `alignment.json`.

Held-out nearest registered-point distances: 42/46 under 5 m; median 0.653 m, p90 3.413 m, RMSE 2.044 m. These are fit diagnostics, not surveyed positional accuracy or independent point identities. Search selection and a shared cadastral provenance limit independence.

## Footprint check

416 official Vaud layer22 building footprints were independently downloaded in the fitted envelope; a count-only query reconciles the response. Query URLs, response hash and four >=5 m holdout residuals are saved in `footprint-review.json`; geometry is frozen in `official-footprints.geojson` (LV95).

All four outlier crops and the full overlay were visually reviewed. Three source centroids fall inside current footprints; the fourth is 1.160 m outside. The overlay shows generally corresponding buildings, while some footprints have changed shape/extent. No demolition, temporal cause or complete shape equivalence is inferred. In particular, containment does not establish building-shape agreement, and a concave footprint may place its source centroid outside.

No planning polygons, parcel joins or receiver rows were created. Next: extract a clearly qualified candidate outline from the detailed inset, preserve source version/perimeter limitations, check actual commune intersection, and perform parcel sensitivity QA before any release.

## Reproduce

```sh
python prepare.py --pdf /path/to/verified-pa4-measures.pdf
python fit.py
python overlay.py --pdf /path/to/verified-pa4-measures.pdf
```

Run with NumPy 2.5.3, SciPy 1.18.1, PyMuPDF 1.28.2, Matplotlib and Shapely. Source-point extraction and full fitting were reproduced from the frozen inputs. Raw point lists are explicitly source pixels / LV95 respectively; they must not be used as longitude/latitude.
