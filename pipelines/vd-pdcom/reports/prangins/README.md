# Prangins spatial pilot — 2026-09-13

This is a **review-required pilot**, not complete PDCom coverage or a legal
eligibility determination. No consumer release has occurred.

Source: official Prangins 2013 synthesis PDF linked in ../../pilot_sources.json,
SHA256 a4079a88201b716ede0793150102306f8b1faddb55c391a7c2c1696244a96d02.
Selected original layer: `densification`; pale-yellow fill corresponds to
“Densification à étudier des zones de faible densité”. Fifteen simple valid
polygon paths, total about674781m². Other categories remain unextracted.

## Independent alignment experiment

975 PDF building centroids from the BATI layer;1372 current reference building
centroids from RE-LLM bronze_ch.vd_batiment_rcb, spatially selected with the
Prangins swiss_communes_geo boundary. Reference points and identifiers are in
reference-buildings.json. No ownership data was accessed.

Hold out every fifth PDF centroid before fitting. Coarse FFT correlation and
robust similarity registration use the remaining780 points. Select the solution
using training matches, then evaluate the195 held-out points:

-188/195 nearest reference buildings within10m.
-Median nearest-building distance0.534m;90th percentile2.067m.
-Full, **untrimmed RMSE35.998m**. Seven large mismatches remain; these have NOT
 been proven to be demolished buildings or measured map errors.

These are nearest-neighbour pattern-matching statistics, not surveyed checkpoint
accuracy. Source-map boundary precision remains unknown. Codex visually inspected
three overlays (north,centre,east): mapped buildings align well in these samples;
the review is not an exhaustive check of every sector edge.

Reproduce with the exact source PDF:
`python align.py --pdf /path/to/Prangins.pdf --output alignment-new.json`.
Dependencies used: PyMuPDF1.28.2,numpy2.5.3,scipy1.18.1,shapely2.1.2;
the existing pdcom-parser path decoder also imports its normal dependencies.
The output parameters apply to Y-flipped PDF coordinates relative to pdf_origin,
then rotation/scale, translation and reference_origin in LV95.

## Persisted pilot result

RE-LLM bronze_ch.vd_pdcom_sectors:15 valid WGS84 geometries with GIST index,
source page1, source document ID ad153736-58f1-533f-8cca-e41524f9fe12,
full alignment evidence, review_status=review_required. source_precision_m and
validated_by remain NULL; no false precision or full validation claim.

Intersected with1012 indexed silver_ch.cadastral_plots for federal code5725:
541 sector/parcel pairs,527 distinct EGRIDs. Each candidate retains overlap area,
parcel area,fraction,and real WGS84 intersection geometry in
bronze_ch.vd_pdcom_parcel_candidates.382 parcels touch the10m sector-boundary
review buffer. That buffer is a review heuristic, **not an established error bound**.
Even a large overlap means only that a strategic area overlaps the parcel.

The broad gold views timed out at20s; the indexed cadastral source succeeded.
No production query/index changes were made to resolve that read-path issue.

## Outstanding

Reconcile seven matching outliers;check full spatial alignment and sector edges;
interpret remaining Prangins categories;complete Lausanne/Morges georeferencing
and hatching interpretation;verify applicable versions;register and validate
receiver delivery. Commune states remain candidate_vectors/not_ready;zero
communes are resolved. The other297 current VD communes remain not_searched.


### Outlier review, 2026-09-14

The reference is RCB **building location points**, not footprint centroids (the
registered geometry column is POINT, EPSG:2056). Earlier centroid terminology
was incorrect. Consequently the centroid-to-nearest-location-point diagnostic
is not a surveyed map alignment error. The original untrimmed metric remains
unchanged rather than silently removing adverse observations.

All seven held-out outliers now have individual PDF overlays: magenta circles
are current RCB locations; the blue circle is the held-out PDF polygon centroid.
Holdout 0 lies in Nyon, outside the Prangins-only reference subset. Its nearest
unrestricted RCB point (EGID9029479) is7.108m away; the old500.140m result was a
reference-coverage mismatch. The other six remain in Prangins. The overlays show
location-point/shape differences, but do not independently establish whether
these reflect changed buildings, cartographic generalisation, or extraction
subparts. The release remains review_required.

`holdout-outliers.json` records point coordinates, commune membership, nearest
RCB point distances and building metadata. `holdout-{index}.png` preserves each
visual check. Official Prangins PDCom page rechecked14September2026 and still
states entry into force2013. No full-commune or parcel-eligibility claim made.
