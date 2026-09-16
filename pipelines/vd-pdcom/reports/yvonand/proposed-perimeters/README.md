# Yvonand proposed hatching: historical cadastral units, not sector perimeters

Eight native closed black-stroke subpaths are recovered exactly from source7. These are cadastral outlines under proposed hatching, NOT eight complete planning sectors or net developable areas. Selection is non-exhaustive: original search considered closed subpaths >2000 square PDF points, with .27–.72 overlap fraction in the four extracted colors;4495/9 was excluded from the final set. The final eight were visually reviewed in source-contours.jpg; extract_contours.py uses explicit reviewed indices and checks the PDF hash.

| Native drawing/subpath | Visual interpretation / remaining boundary issue |
|---|---|
|4502/4|Marais de Mordagne: purple/orange projected bands overlap and a green projected area occupies the west. Different purple stripe endings show this cannot be reduced to one industrial polygon. The enclosing cadastral outline is recovered, not a separation of uses.|
|4536/4|Orange proposed hatching, green public-space symbol and adjoining solid orange; outline does not delimit only the proposed portion.|
|4552/2|Oche Vulliamy: projected orange area with a thin southern extension beyond the hatch envelope and a northern portion adjoining solid orange.|
|4569/2|Orange projected hatching; cadastral subdivision within a larger hatch area.|
|4576/1|Au Rebut: orange projected band within a larger area; boundary symbols/road context retained.|
|4576/2|Au Rebut/Treysala: projected orange area includes existing building symbols.|
|4586/0|Grand Clos: orange projected hatching and explicit proposed green-space symbol; not all housing capacity.|
|4588/5|Clos du Four: orange proposed hatching plus green-space markings/overlap; retain both.|

The 2007 objectives already call for mixed use near Mordagne (10.1b) and coordinated/phased legalisation, green pockets and density limits (8.1–8.3). That thematic correspondence does not establish today's zoning or convert the cadastral contour into an adopted sector boundary.

## Alignment and current parcel research

Applied the fixed-scale translation from PR104 without retuning. All eight contours lie inside the hull of reserved source/reference matches. Each has11–31 reserved matches within300m. Mordagne has17 nearby checks, no IoU<.85/Hausdorff>2m discrepancies and maximum centroid residual0.171m. Au Rebut4576/1 has21 such checks, no shape discrepancies, maximum0.196m. Other units have1–4 nearby footprint discrepancies; those remain in the GeoJSON properties. This is geometric consistency, not surveyed accuracy.

Acquired745 current official layer21 parcel references in BFS5939 intersecting the combined contour bounding box. Count, unique EGRID, geometry validity, BFS identity and saved-file SHA checked. Research intersections >1m² yield43 contour–parcel pairs/38 EGRIDs:32 private-parcel-kind,10 communal-public-domain and1 cantonal-public-domain pairs.29 pairs cover <1% of the current parcel; keep them as possible boundary/sliver effects, not additional opportunities. No personal ownership information was requested.

The dominant current matches are326 (Mordagne),125/2654/2655 (4536/4),150,122,1472,1470,1752 and166. These are measured overlaps, not proved historical parcel lineage. Several contours closely match current parcels while others have splits/edge differences. A1% filter alone does not resolve slivers: small road/parcels can exceed1% with tiny absolute area. All raw >1m² pairs are retained with area and fraction.

`research-contours-lv95.geojson` is explicitly research geography; `research-intersections.json` is not the runtime parcel-candidate table. Reproduce with extract_contours.py then analyse.py; source hash and snapshot hash/count assertions guard the inputs. Runtime/source metadata and earlier research remain separate.

Next resolve actual thematic hatch envelopes and existing/projected splits (especially4536/4,4552/2 and mixed Mordagne), review subsequent plans/currentness and sector-local discrepancies, then define a scoped private-delivery contract. No runtime sectors, receiver rows, public release or completion claims in this milestone. Corpus remains61PDFs/35sourcecommunes;private40sectors/2267pairs.
