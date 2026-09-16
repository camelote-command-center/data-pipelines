# Lutry dossier directeur: official acquisition and first source review

**Subsequent review:** [approval exceptions and missing annexes](../reserve-review/README.md) qualify the general approval statement. Three named sectors had approval suspended; later resolution is unverified.

BFS **5606**. Three official linked PDFs, **44 + 21 + 21 = 86 scanned pages**. This adds a source commune, not a completed commune or delivered parcel layer.

## Provenance and status

The [municipal planning page](https://www.lutry.ch/vivre-a-lutry/urbanisme-et-constructions/amenagement-du-territoire/planifications-communales), saved as `landing.html`, states that the dossier has been in force since **28 February 2000** and links all three parts. The page displays an update date of 5 August 2026; that is a page date, not a new plan approval. Source URLs, byte counts, hashes and stable document IDs are in `sources.json`. Acquisition was on 16 September 2026.

Part 1 PDF page 3 was visually inspected at readable resolution: municipal executive approval **27 July 1998**, consultation **2 October–2 November 1998**, council adoption **10 May 1999**, departmental approval date stamp **28 February 2000**. Signature lines are blank. The preparation date **24 July 1998** is distinct. The municipal listing corroborates the approval status; this is not a claim that the scan carries signatures.

The page separately discusses a PACom I revision and later consultation. This is an adjacent land-use procedure, not evidence that the entire director dossier was replaced. Neither later detailed plans nor current parcel entitlements were exhaustively checked. The energy plan and PGA links are excluded from this PDCom acquisition.

## Scan and map inventory

All 86 pages have zero extracted native text, zero vector paths and no PDF viewport georeference detected by the existing inspector. Contact sheets for all three parts were visually inspected. OCR/geographic work remains pending; scan acquisition does not imply map validation.

Part 1 contains introductory material, objectives and schematic diagrams. Part 2 PDF pages **6, 8, 9, 12, 14 and 16** contain the main map sequence. Page 8 is land use, with its detailed legend on page 11; both were inspected at readable resolution. Part 3 PDF pages **1, 5, 7–11 and 13** contain mobility map/detail panels; exact theme-level review remains pending. Pages 16–19 contain the action/intervention index. Do not confuse PDF page numbers with printed continuous page numbers.

The land-use map includes printed coordinate/grid markings, which may support later calibration, but no geographic transform has been established. The legend differentiates housing density, activities, **strategic reserve**, viticulture, agriculture, forest, equipment, recreation and undefined destination. These colors represent different planning categories, not uniformly development opportunities. In particular, the historic “recent urbanization extension” description is not an assertion of current unused capacity.

Part 2 page 5 describes coordinating plans across land use, site, collective equipment and transport. It also notes that the transport chapter transposes an earlier 1991 circulation plan with updates. Version context must remain attached to later extractions.

## OCR and next step

`ocr/` holds raw French Tesseract OCR for 12 selected approval, definition, legend and action-index pages. The text is a research aid, not an authoritative transcription, and does not overwrite native PDF inspection. No numeric rules or parcel geometries were normalized. The full 86-page OCR remains pending.

Next: review the land-use categories and action index together, extract full-resolution raster maps, calibrate printed grid controls with unused checks, and only then assess candidate geometry and current cadastral intersections. Preserve strategic reserve separately from built housing, agriculture and protection categories.

## Discovery fix and verification

The bounded crawler recognized “plan directeur communal” but missed **“dossier directeur”**, the municipality's own name for this plan. The synonym now participates in candidate detection/navigation; source/version review remains required. The real saved landing-page regression recovers exactly the three dossier PDFs and excludes the nearby PGA and energy files. The complete targeted suite passes **47 tests**.

`verify.py` checks PDF hashes, page inventory, source-manifest membership, approval-review evidence and OCR page provenance. Parts 2/3 rely on the official three-part listing and part 1's approval evidence; no approval page is fabricated within those continuations. No new sector/parcel/receiver release is authorized by this report.
