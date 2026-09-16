# Veytaux scan, approval and amendment review

The existing official 32-page PDF is entirely scanned: each page has one raster, zero native characters and zero vector paths. Its source SHA is frozen in `ocr-manifest.json`. All32pages now have raw French research OCR (25,779characters), including blank/noisy pages. This is not a fully verified transcription or canonical text delivery.

PDF4 was visually checked: municipal executive9September1993, consultation30October–30November1992, council adoption6December1993, and signed/stamped Council of State approval27April1994. Later legal currency remains unverified.

PDF30–31 were visually checked and contain ten adopted council amendments. `case-index.json` links the eleven named cases to their PDF pages and amendments. In particular Sonchaux amendment10 qualifies the development objectives and returns a large part to alpine zoning. Do not extract opportunities from the earlier illustration without the amendment. The eleven cases are not eleven validated development polygons.

Contact sheets cover the entire document. Main synthesis mapPDF11 and case locatorPDF15 precede case maps/indicative illustrationsPDF16–19 and28–29. Their separate extents require individual calibration and legend review. No geometry, parcel intersection, receiver release or commune completion is asserted.

Reproduce rawOCR using `ocr.py` in the recorded PyMuPDF/Tesseract environment. Next extract original map images with placement provenance, read legends, then establish explicit independent calibration controls.
