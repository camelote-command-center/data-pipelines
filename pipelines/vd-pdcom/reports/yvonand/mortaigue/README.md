# Yvonand: Mortaigue dossier recovered

The official municipal category page exposes a new **14 September 2026** item, preavis **2026/13**, labelled **Déposé**. Following the live page, rather than cached search snippets, recovered four adjacent planning sources: a 14-page adoption proposal, one map, a 17-page regulation and a 51-page report (83 pages total). Source hashes and exact URLs are in `sources.json`. These are not additional PDCom documents.

## Status and scope

The signed municipal proposal asks the council to adopt the plan and resolve six remaining objections; its final conclusion expressly reserves departmental approval. It is not a council resolution or proof of entry into force. Its page 8 reports an inquiry from 12 November to 11 December 2025. The attached map/regulation/report are the November 2025 inquiry versions; map approval and effective-date fields remain blank. Final adoption, any subsequent amendments and entry into force remain unverified.

The missing western portion of parcel 326 is now explained: the Mortaigue proposal places it in **zone agricole protégée 16 LAT (AGP)** for ecological compensation. Article 10 on regulation page 7 prohibits construction while allowing qualifying arrangements serving the zone's purpose, subject to federal provisions. The eastern portion's proposed ACA B treatment is in the separate PACom dossier reviewed in PR107. Neither source supports treating the entire historical PDCom contour as industrial development capacity.

The map's parcel table states **10,411 m²** of parcel 326 within Mortaigue and **60,479 m²** for the whole plan. These are source statements, not newly measured areas; do not add them to the PACom report's 32,499 m² as an exact cadastral reconciliation. The report explicitly coordinates the two separate planning perimeters (page 6). Pages 9 and 11 describe the older Mordagne public-use/clearance areas and the proposed ecological compensation. These descriptions do not replace the missing original 1993 map.

## Reproducible map extraction

The sheet has two distinct panels: **1:5,000 overview** containing western parcel 326, and **1:1,000 detail** containing the sporting site. The title's 1:1,000 must not be applied to the overview. The PDF contains 1,718 vector drawings and no embedded raster images despite having zero extractable native text.

`extract_agp.py` hash-checks the exact source and extracts drawing 98 (sequence 99), a valid closed 12-segment native contour beneath the AGP red hatching. `agp-contour.json` is in PDF points only. It is not geographic output, a private runtime sector, or a parcel entitlement. Its overlay was visually checked against the coloured source outline. No ground transform or parcel intersection was attempted.

## Verification and next work

Four source hashes/page counts were checked; all 14 proposal pages were OCR processed locally. Retained OCR remains explicitly unverified text; the proposal's final conclusions and map scope/title were visually reviewed. Selected native report/regulation pages support semantics and procedural distinctions. The 51-page report lists 21 annexes but does not contain those annexes; the dossier must not be described as fully acquired.

Next: validate per-panel alignment with independent controls, reconcile the proposal boundaries with historical PDCom thematic extents, and obtain the revised PDCom/original Mordagne plan and final decisions. Yvonand remains downloaded/not_ready. No runtime or public delivery change.
