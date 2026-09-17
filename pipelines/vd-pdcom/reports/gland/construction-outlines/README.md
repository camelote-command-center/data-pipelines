# Communet construction envelopes in source coordinates

Ten blue construction-perimeter outlines manually traced and visually checked: A1, A2, B1–B5, C1–C2 and D. All polygons valid with zero pairwise positive-area overlap. Coordinates remain PDF points. This is the later PPA, not the original PDCom map. Dashed SILO parking, underground, activity/front lines are distinct and not included.

The separate Article9 capacity rules apply per construction envelope; these polygons do not express net land, parcels, existing footprints or remaining capacity. Existing buildings are visibly present in B5. No parcel intersections or runtime geometry created.

Coverage check against the convex hull of PR129 building-corner checks: eight envelopes lie entirely outside it; B1 approximately80.9% outside and C1 approximately44.2%. This exposes extrapolation, not an accuracy measurement. Source geometry can be reviewed now while geographic release remains withheld.

extract.py replays traces, validation, coverage and annotated image with the exact hashed source PDF from ../sector-currentness. Prior affine is not adjusted. All ten overlays visually checked at native crop scale. Manual stroke precision and currentness limitations remain explicit in outlines.json.
