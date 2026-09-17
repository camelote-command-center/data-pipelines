# Bellevue / Borné-Nau: native hatch-support contours

Four native colored paint paths (drawing indices **0, 1, 64, 97**) recovered from the August 2011 Grandson sheet 7, SHA256 `1f349cf0dcf2d9f5fb7e13ccddaee37fa041c223c4c95834fef990c9765a360a`. These underlie the visible white diagonal hatching whose legend denotes new zoning in the short/medium term. The selection was manually checked against the rendered map and overlaid outlines.

Cubic curves and line commands are preserved exactly in unrotated PDF coordinates. Diagnostic polygons use 32 subdivisions per cubic and are valid; diagnostic areas are square PDF points, **not square metres**. The extractor verifies the source hash and reproduces `contours.json` byte for byte.

Blue path 1 overpaints part of orange path 0. Raw areas are not additive, and the outlines are not net development perimeters. No deductions for subsequent symbols, roads, landscape requirements or existing structures have been applied. Alternative-use circles remain distinct from the hatch-support areas. This is four paint paths, not four confirmed opportunities.

`overlay.png`/`overlay.pdf` show the selected boundaries. The source page has PDF rotation 90 degrees, and its north arrow is not aligned with display-up. No geographic transform, parcel join or runtime sector has been produced. Independent alignment, per-category overlay reconciliation, currentness and exact-version approval remain required. Historical indicative 1990 cadastral base and 2011 planning date remain material limits.
