# Morges official parcel QA

Six full-base hatch outlines intersect62sector–parcel pairs /61distinct EGRIDs. Per source drawing:5558=18,5761=17,7935=2,8039=1,8192=4,8238=20. Official reference query:399unique count-checked objects in a bounded envelope under2km per axis, Morges5642only. URL/response hashes, attributes and geometry retained.

Distinct objects:55private land parcels,5communal public-domain land parcels,1DDPsurface right. DDP CH900945208367 belongs to path8238. Land CH798388424597 intersects both7935and8039; this is a shared parcel across distinct source shapes. Rights can overlap land; counts/areas are not exclusive-area totals.

All61EGRIDs exist in the silver mirror with correct5642attribution. Six official/mirror intersection keysets match exactly; geometric identity was not established by this comparison. No federal mirror writes.

Thirty scenarios reproduced offline: buffers−10,−5,0,+5,+10m produce53/55/62/74/90pairs and52/54/61/72/84distinct objects. These are illustrative sensitivity, not error bounds. No base overlap is under1m²; four below1%parcel area remain in the evidence (1.12–9.51m²). Tiny floating-point fraction above1 on the fully covered DDP is preserved in research output, not legal precision.

Six overlays visually checked. Map alignment/footprint differences, later source currentness, path8192overprints and0.0834m²commune-edge discrepancy remain unresolved. Two partially hatched source paths remain withheld. This milestone creates research evidence only, not62delivered pairs or completed commune coverage.

Run check.py for official refresh, verify.py for offline30scenario replay, overlay.py for figures. Canonical report/operation IDs in runtime-receipt.json. Next scoped official-reference private pilot, explicit overprint and boundary flags, repeat idempotency and registered receiver parity; preserve current review-only status.
