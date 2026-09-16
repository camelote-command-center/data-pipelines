# Yvonand urbanisation: paint paths and semantic constraints

Source 7, exact hash d4fecb86752e2f2cb2c3359e835e9555694979b88288bb50795080ec349a1a2b, page 1. Reproduce with `extract.py --pdf <source-7.pdf> --out <directory>`. This is research, not a new runtime sector extractor.

204 visible valid paint paths: 94 orange, 82 yellow, 11 purple, 17 cyan. Three selected-color paths are fully excluded by clipping/page furniture. Four invalid contours are retained with their source commands, not repaired: drawing indexes 41,737,783,915. Purple41 is a substantive industrial-area outline; do not call extraction complete. All selected paths retain source commands, sequence IDs, clipping, color and area in PDF points. Page rotation90 is applied explicitly after clipping in native coordinates. Conservative page-furniture exclusions are recorded. Overlay visually checked against original map.

These are NOT 204 planning sectors. Several projected areas are encoded as individual colored hatch fragments. The overlay exposes their fragmentation. Later overpainting is not subtracted and fragments must not be summed into capacity or parcel counts. Resolve outer boundaries and existing/projected classification first. Four-color selection excludes commercial, green, natural, hamlet and arrow symbols; it is intentionally not full-map coverage.

Legend interpretation: orange medium-density habitat; yellow low-to-medium-density habitat; purple industry/craft; cyan public construction. Numeric density, current buildability and final zoning are not established. Same thematic color appears in existing and projected columns; color alone cannot distinguish them.

Objectives document 121a9d59-239c-5d79-a323-baa06467a789 (SHA4b1e3a8a9692494d9981a16e1b912636ede4d12dd8a20048ea052bd80f4b5a7d), PDF7–9 visually checked:
- 8.1/8.1a prioritise village densification and already legalised zones, with an inventory of actually buildable parcels still called for.
- 8.1b conditions future residential allocation of intermediate zones on need and village structure; coordinated local planning is contemplated before rezoning.
- 8.1c reserves Condémines only for possible long-term expansion; arrows are not buildable parcel perimeters.
- 8.2 Goilles expansion requires phased legalisation through plans.
- 8.3a limits densification of block interiors to preserve green pockets.
- 10.1b calls for reconsidering west-of-village land between RC402c and railway, including a medium-density mixed zone and coordination with Goilles. This is not evidence that rezoning occurred.

These are thematic cross-references, not independent approval proof for the objectives volume or one-to-one assignments to paint paths. Next: recover proposed outer boundaries, identify stable cadastral/building controls with reserved independent checks, and investigate subsequent plans. No geographic transform, intersections, receiver or public release in this milestone.
