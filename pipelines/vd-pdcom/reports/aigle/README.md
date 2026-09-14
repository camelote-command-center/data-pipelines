# Aigle urbanisation source paths

Exact source SHA is enforced by extract_source_paths.py. Legend PDF18 and map PDF19 identify nine yellow “Densifier” paths and two orange “Préparer la densification” paths. These are proposed planning actions, not parcel rights or assigned numeric density values. Roman-numeral indicative density symbols are separate annotations and are not automatically assigned to polygons.

The map contains 22 matching paths, but 11 are entirely hidden by their clipping context. All three ancestor rectangular clips and page bounds are applied before retaining geometry. This avoids duplicate/misplaced polygons from reused map content. Curves flatten to0.03PDF-point chord deviation; multiple contours, unsupported operators, invalid geometry and unsupported clips fail closed. Normal/Multiply blend modes are accepted and retained; they affect appearance rather than the source path boundary. The orange group has effective opacity about0.8.

selected-paths.png was visually checked against the original map; blue boundaries trace the selected source categories. Other hatching/categories and Fontanney inset are not extracted. source-paths.json explicitly uses PDF coordinates (top-left origin, points), not geographic coordinates or a GIS feature collection. No PostGIS sector or parcel intersection is created until calibrated. Source approval fields remain blank as recorded in document_reviews.json.

Reproduce: `python pipelines/vd-pdcom/reports/aigle/extract_source_paths.py --pdf /path/to/source.pdf --output /path/to/source-paths.json` with numpy, shapely and PyMuPDF. Next: calibrate the main map against independent official reference geography, verify heldout alignment and preserve indicative semantics before private receiver delivery.
