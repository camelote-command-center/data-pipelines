# Eysins SDAN indicative pixel outlines

Seven source-specific contours were manually digitized from the original 524×469 image (PDF20, printed16, image xref115) and visually checked as an overlay: five orange medium-density housing patches, one blue activities/shops envelope, and one hatched possible long-term housing option. These are illustrated color regions, not seven legally defined planning sectors.

Coordinates are original image pixels with the origin at top left and y increasing downward. No geographic CRS, commune intersection or parcel matching has been performed. The 2006 strategic estimates of about 1,000 new residents/400 jobs apply across the described Eysins development sectors and must not be allocated to individual shapes or presented as current remaining capacity.

E1 excludes the conspicuous white southeast indentation but retains internal basemap details. E2/E3 are narrow strips with uncertain road edges/endpoints; E4/E5 have faded paint/building overprints. E6 is a gross blue envelope retaining internal uncolored separators. E7 follows the colored hatch envelope, not the dashed line extending south. Per-shape limitations are preserved in `digitization.json`. Hatching uses the Eysins local legend; the different Prangins meaning must not be imported.

Run `python verify.py` to check source SHA/dimensions, valid closed polygons, image bounds, disjointness and category separation, then reproduce the source overlay and 35 area-sensitivity scenarios (−2,−1,0,+1,+2 pixels). These buffers are illustrative, not measured positional errors or metres. Raw vertices remain explicit and are never silently repaired. `frozen-hashes.json` records reviewed artifact hashes for repeat verification.

Next establish explicit correspondences against official reference geometry, retain separate controls, inspect alignment residuals and current commune attribution. Seek higher-resolution official sources where the source edges are too ambiguous. No geographic runtime sectors, parcel links or receiver records were created; existing private 40/2267 and coverage totals51PDFs/33communes are unchanged.
