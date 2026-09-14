# Épalinges urbanisation map calibration and selected spatial candidates

Exact source SHA: bd095d80d2284073a93ef7e108f89b083d32fb361c97b689809522b7b8f64c86.
PDF page99, boundary drawing2 (1,169 lines and two cubic curves), matches the
commune's outline. Fit a similarity transform to current BFS5584 boundary in
LV95, using600 alternating perimeter samples;600 interleaved samples are held
out. Residual median0.313m,p952.301m,RMSE1.685m,max15.939m. These are **boundary
fit diagnostics**, not independent surveyed accuracy. The fitted scale is
4.30343m/PDF-point; use calibration rather than the printed scale alone.

North/centre/south overlays compare current RCB building **location points**
(magenta) against the mapped buildings. Reviewed overlays show broadly consistent
placement; no numerical point-control accuracy or parcel eligibility is claimed.
Calibration reference coordinates are explicitly EPSG:2056. Stored production
candidate geometries are EPSG:4326 with the existing GiST indexes.

Six filled source paths match the legend's medium-density residential color AND
its effective group opacity0.44. The same color at0.80 is another legend category,
so fill RGB alone is unsafe. Extraction retains group context, requires Normal
blend modes, clips using the actual rectangular clipping paths, rejects multiple
contours and invalid geometry, and adaptively flattens cubic curves to0.03PDF-point
chord-deviation tolerance. Source strokes and other overprinted measures/categories
are not promoted as part of these six fill polygons. Original map boundaries are
indicative. Source approval includes the reservation recorded in the parent report.

`medium-density-lv95.geojson` stores the six reproducible selected geometries;
`epalinges_pilot.py` replays them only for the matching document and hash, creates
parcel intersections and leaves all outputs review_required. A10m boundary flag
is a review heuristic, not a proven error bound. No receiver release or complete
commune coverage. Independent QA, remaining categories and receiver integration
are still required.

Reproduce with Python, numpy, scipy, shapely and PyMuPDF installed:
`python fit_boundary.py --pdf /path/to/exact-source.pdf`
`python extract_medium_density.py --pdf /path/to/exact-source.pdf`
