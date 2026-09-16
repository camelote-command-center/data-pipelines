# Yvonand: zoning-map evidence and approval limits

Four official adjacent planning PDFs (62 pages) were acquired from the commune's construction-regulations page. These add context to the 2008 PDCom; they do not increase the PDCom corpus or delivered sector counts. Exact URLs, source hashes and acquisition times are in `sources.json`.

## Verified observations

- The PGA sheet uploaded in May 2026 is the historical plan bearing cantonal approval **13 July 1977**. Its upload date is not an approval date. The proposed 2026 regulation independently lists that historical date (PDF page 54). The sheet alone is not a consolidated inventory of subsequent local plans.
- Both PACom maps are inquiry material. The 1:2,000 Villages sheet is plan 1/6, phase ENQ, drawn 29 January 2026 and printed 25 February 2026. Its approval and effective-date fields are blank. The 1:5,000 communal sheet also has blank approval fields. The regulation cover is dated 2 March 2026 and says Enquête publique. None establishes subsequent adoption or entry into force.
- Parcel 326 at Marais de Mordagne is visibly split: its eastern portion is purple and labelled ACA B; its western portion is white and separated by a red dashed line. Adjacent parcel 327 is CEN C, not ACA B. This supports the earlier report's partial-parcel proposal; **32,499 m² is the report's stated area, not a new measurement**. The historical 43,214 m² PDCom cadastral contour must not be reused as that proposal's boundary.
- **Legend correction:** on the 1:2,000 sheet the red dashed line is literally “Périmètre des plans d'affectation légalisés”; the 1:5,000 legend says “Périmètre du plan d'affectation communal”. Do not flatten these into one scope label. The 1:2,000 legend abbreviates economic-activities B as ACA, while map labels and regulation use ACA B. Proposed colouring does not establish current legal rights.
- Regulation PDF page 20 identifies ACA B and contains proposed parameters, including IVB 5 m³/m², ISB 0.50 and height 8 m. These are draft source statements, not effective parcel entitlements or calculated capacity. PDF page 54 lists the 1977 PGA and 1993 Mordagne plan among plans to be abrogated upon the new plan taking effect; that wording is not evidence the abrogation has occurred.
- The 1:2,000 sheet includes separate La Mauguettaz, Les Vursys and Frouye insets. A single page-to-ground transform cannot be applied across them. Both PACom PDFs contain vector drawings despite having no extractable native text; this does not prove every coloured zone has an available closed vector contour.

## Evidence and validation

All four SHA-256 hashes and page counts were checked. Approval/title blocks, whole-sheet context, Mordagne crop and the 1:2,000 legend were visually reviewed. `crops.json` records PDF point rectangles and render scales. `selected-page-text.json` retains regulation pages 1, 4, 5, 20, 21 and 54. Full PDFs remain local and are reproducible from the manifest; targeted visual evidence is committed.

## Remaining gates

The separate Mortaigue map and final decisions, revised PDCom and older Mordagne plan geometry remain unresolved. Thematic envelopes, inset-specific georeferencing and independent checks are still required before private geographic delivery. Yvonand remains downloaded/not_ready; no sector or parcel candidate was created. This is a research milestone only.
