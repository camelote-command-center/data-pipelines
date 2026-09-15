# Morges project-hatch review

The40 yellow base paths are not40development sectors. Matching the orange vertical project hatch yields **six fully hatched underlying base paths** (5558,5761,7935,8039,8192,8238), **two partly hatched paths** (8261,8641), and32without matching project hatch detected. This is source semantics research, not validated sector coverage.

Clipping changes the result: unbounded strokes visually touch neighboring paths. review.py intersects each selected stroke/thin fill with its active clipping paths and excludes a0.5PDF-point base boundary strip. This removes false matches on5556,5757,5759,5915,8226. Five full domains have illustrative support ratios0.996–1.005; partials0.427/0.301. Ratios are diagnostics, not exact area fractions. Path8238 uses a live PDF tiling pattern: its off-page prototype is not map geometry. ResourceP16/xref185 has the same pattern stream as legendP35/xref194; the parent clipping path matches the base domain. The script preserves both parent domains and fails on unrecognized shapes.

All40 source crops and8positive outline overlays were visually reviewed. Horizontal brown lines mean heritage interest according to the legend, not project density; green dots/other marks are distinct overprints. Path8192 includes gray/purple/transport overlays. The two partial outlines must not be delivered whole; source clipping and overprint boundaries need separate extraction.

Run `python review.py --pdf /path/to/exact-map.pdf --output /tmp/morges-hatching`. SHA-gated, exact repeat verified. Source PyMuPDF1.28.2; Shapely2.1.2. Frozen support.json and semantic-review.json preserve diagnostic units and stable operation/checkpoint IDs. The original curve extraction remains an earlier research stage; this review refines its semantics.

Next: independent geographic alignment for the full-hatch candidates, partial-boundary extraction and overprint review, then official parcel matching. No sector/parcel/receiver writes in this milestone; historical2012approval and later-currentness limits remain.
