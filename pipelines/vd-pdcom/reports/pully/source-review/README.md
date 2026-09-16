# Pully: source scope before parcel extraction

Reviewed 16 September 2026. Existing RE-LLM document `98ffb71a-e9e2-5643-9baf-93c42967cb30`, SHA256 `02a96fd6036e51c77e45ab188168e1c451d40cef8b71204e1271151431173d6d`, 73 PDF pages. This adds evidence to an existing source, not another covered commune.

## Approval has a page-level restriction

PDF 71 (printed 69) explicitly states **only numbered grey pages are subject to approval**. It gives municipal executive approval 22 August 1995, consultation 14 September–13 October 1995, council adoption 22 November 1995, and Council of State approval 6 March 1996. Names are typed; this copy has no visible handwritten signatures on the approval page. The general 19 October 1995 footer is not the cantonal approval date.

The restriction is visible in the page image but omitted by both the existing extractable text layer and fresh French OCR. It is manually recorded in the version-specific source review. All 12 identified maps have white backgrounds; they are not covered by a blanket assertion of approved map geometry. Their associated grey objective/measure pages must accompany any interpretation.

PDF 27 (printed 25) records a cantonal qualification excluding cantonal transport objectives, especially motorway connections, from the stated objectives. Incorporated council-amendment footnotes occur on PDF 13 (O13), 21 (O1), 22 (M1–M4), and 59 (M6/fourth bullet). These references are not an exhaustive legal audit.

## Current source evidence

The municipal [planning page](https://www.pully.ch/pully-pratique/urbanisme-environnement/amenagement-du-territoire/) links this PDF. The [Pully 2040 page](https://www.pully.ch/pully-officiel/projets-municipaux/pully-2040), dated 2 March 2026, calls the linked 1996 document the current PDCom and describes its revision. Retrieved HTML, final URLs, timestamps and hashes are retained in `web-evidence.json`. This does not establish current rights for any parcel or the state of later detailed plans.

## Reusable extraction

- All 73 pages have an existing extractable OCR-like text layer: 86,256 characters. Zero vector paths. `native-text.txt` preserves that layer; existing runtime `text` is unchanged.
- Fresh raw French Tesseract OCR: 84,682 characters, page hashes, engine version and reproduction script. It is an alternative unverified transcription, not a substitute for image review or a receiver text delivery.
- Twelve complete map composites, PDF pages 37–41, 48, 53–54, 61–63, 70. They comprise **524 image/stencil layers**. Extracting only the colour background loses linework and symbols. `render_maps.py` renders all layers with PDF rotation at 3 pixels per display point; the manifest records layer hashes, placements and rotation. Render scale is not ground accuracy.
- Five contact sheets cover every page. `case-index.json` links four broad strategic spaces to their text and map pages, and separately records four additional attention areas. These are not eight parcel polygons.
- PDF 66 calls for preparation of an intercommunal plan for the Paudèze valley. The illustrative project map on PDF 70 is not proof that such a plan was adopted.

## Next gate

Read the map legends at source resolution, relate selected historical categories to approved objectives, check later detailed planning, then acquire explicit stable controls and reserve independent checks. There is no geographic fit, sector polygon, parcel intersection or receiver release in this review. Pully remains incomplete. Prior private delivery totals are unchanged.

Run `python verify.py` in this directory to verify source/OCR/composite/layer/web-evidence hashes. Run `python ocr.py` or `python render_maps.py` to reproduce derived assets; engine versions may affect bytes.
