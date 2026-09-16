# Lutry land-use map: printed-grid calibration and legend

Part 2, PDF page 8, is the historical land-use map. PDF page 11 (printed 51) defines its categories. The approved dossier date remains 28 February 2000; no later parcel rights or remaining capacity are inferred.

## Raster provenance

The map consists of **15 embedded raster objects** (one monochrome background plus multiple color tiles), not a single complete JPEG. `calibration.json` records every xref, size, filter and PDF placement. `page-8.png` is a complete 4× PDF composite, 2380×3368 pixels, rendered from SHA-gated bytes using PyMuPDF 1.28.2. Coordinates in `controls.json` refer to that composite, top-left origin with y downward. `page-11.png` preserves the full legend page. Original legend assets xref99 and xref100 are extracted without alteration.

## Printed grid

Four manually read grid intersections fit an affine transform to LV03 (EPSG:21781); the CRS is inferred from the printed Swiss coordinate values and Lutry location, not embedded metadata. Two separately designated intersections were frozen before fitting and held out. Their **2D grid residual RMSE is 5.567 m**, maximum **6.276 m**. This is internal scan/grid consistency, not independent surveyed or cadastral accuracy. No controls were moved after seeing the result. Manual reading and scan deformation remain relevant; only two checks limit confidence.

The affine matrix maps composite pixels `(x, y, 1)` to `(E, N)`:

```
E = 1.562471272716*x - 0.013896435333*y + 541294.302194469
N = -0.001194585527*x - 1.546178052842*y + 154753.906482043
```

Grid contexts and the control overlay were visually inspected. A separately retrieved current commune boundary, transformed from the registered RE-LLM reference in EPSG:2056 to EPSG:21781, was overlaid only for context. It broadly follows the map's municipal footprint; its lake portion extends beyond the rendered page. The current boundary was not used to fit or adjust the map. This visual agreement is not a quantified accuracy or boundary-completeness claim.

## Semantics

`semantic-review.json` preserves **15 destination symbols** and **15 broad-area labels (A–O)**. They are two different classification systems. Legend color alone is insufficient: similar magenta appears in housing, activities and technical equipment. Strategic reserve uses black vertical hatching; sports/leisure uses colored striping. Undefined destination is not a development entitlement. Area G's “recent urbanization extension” describes a historical existing fringe rather than future empty land.

The map and full legend were visually reviewed together. Exact native symbol crops identify which evidence supports each label. No source polygon has yet been digitized, and no reserve area, current potential or parcel count is claimed.

## Verification and next work

`verify.py` checks source and frozen artifact hashes, exact composite/calibration replay, raster composition, controls, legend crop bounds and research release gates. No runtime dependency or workflow change.

Next: identify and digitize strategic-reserve and relevant urbanization envelopes with local legend context; reconcile the action index and later planning. Then obtain current reference/parcel evidence and assess boundary sensitivity. The grid transform is research input, not permission to deliver unreviewed polygons. This work creates no runtime sectors, parcels or completed commune.
