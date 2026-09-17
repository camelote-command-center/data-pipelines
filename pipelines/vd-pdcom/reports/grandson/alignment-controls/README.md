# Grandson official alignment references

Acquired **1,604 official building footprint objects** from Vaud APIGeo layer 22 with `NO_COM_FED=5561`, in EPSG:2056. Counts before and after agree, OBJECTIDs are unique, and the service reports no truncation. There are 91 null EGIDs; these are retained rather than silently dropped. The timestamp, exact query, selected attributes and snapshot SHA256 are in `manifest.json`. No ownership fields were queried.

Prepared **393 historical gray-fill candidates** from Bellevue/Borné-Nau sheet 7. Of 459 paths with the exact selected gray fill, 66 were excluded and every reason is retained. The conservative display rectangle excludes the legend and edge areas. Candidate polygons are valid; cubic curves use 32 subdivisions. Exact JSON replay passed. The source overlay was visually checked. Gray-fill selection alone does not establish building identity or exhaustive coverage.

These are two separate evidence sets, not 393 successful matches. **No geographic fit or accuracy claim has been made.** Next identify correspondences, separate fitting points from reserved validation points before optimisation, and retain unmatched or changed buildings. The historic base is indicative and adapted from 1990; present-day references may differ. No sectors, parcel links, currentness or approval statuses were advanced.
