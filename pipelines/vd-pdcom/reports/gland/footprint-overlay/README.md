# Communet current footprints versus historical envelopes

Research overlay in EPSG21781 using the unchanged provisional affine and official PR128 snapshot. All ten source envelopes intersect current aboveground buildings: 27 distinct aboveground footprints, plus six underground envelope-footprint pairs retained separately. Aboveground union fractions range26.4–72.6%. These fractions are footprint coverage, **not remaining capacity or consumed floor-area rights**.

Every positive overlap is preserved. Aboveground counts using >1m² overlap remain unchanged for illustrative inward/outward2m buffers; this is not positional confidence. Underground features are excluded from aboveground statistics. Valid reference polygons and envelope polygons checked. Original and later-plan distinctions, diagnostic control selection and extrapolation remain explicit.

analyze.py reproduces all results with NumPy/Shapely and the committed source/reference snapshots. No new datum conversion, geographic runtime record, parcel allocation or receiver delivery. Next use these identity-linked overlaps to investigate temporal lineage/currentness and retain uncertainty; do not infer development potential by subtracting building coverage from envelope area.
