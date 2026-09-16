# Veytaux original map assets and semantics

Eight original embedded map/page images extracted byte-for-byte from the frozen32page source. `manifest.json` records source hash, xref, asset hash, dimensions, PDF placement and page rotation. Re-run `extract.py` to reproduce. All eight originals visually inspected; zero native paths/text and no embedded VP georeference.

Critical orientation: PDF16,18,28 use180-degree page rotation. Their raw image bytes appear upside down, whereas PDF rendering is upright. Compose placement and page rotation before transferring controls or boundaries. The synthesis map north symbol points left, not upward.

`semantics.json` transcribes16 legend entries, retaining the two-line non-buildable open-space label as one symbol. Housing, mixed-use, study perimeters, woodland, public equipment and circulation actions must remain separate; repeated similar hatch colors cannot identify opportunities on their own. The June1992 synthesis has a0–150metre scale bar but no usable labelled national-grid coordinates were established. It is not calibrated.

The locatorPDF15 shows cases1–10. Case11Sonchaux is represented by an off-map directional arrow, so no eleventh polygon can be extracted there. The separate Sonchaux context/schema requires its own transform. PDF17,19,29 already incorporate the corresponding adopted amendment wording3,4,10; this corrects the earlier implication that the Sonchaux narrative was unamended. Preserve the narrative, amendment list and indicative drawing together.

No source polygons, geographic transform, parcel intersections, receiver release or commune completion added. Next: digitize explicitly scoped study outlines in displayed-image coordinates; identify stable reference features before fitting, reserve independent check points, and preserve scan/version uncertainty.
