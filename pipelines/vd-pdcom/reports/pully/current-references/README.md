# Boverattes current references and PDL access gap

16 September 2026. Continuation of PR100's historical planning-context review, linked to Pully PDCom document `98ffb71a-e9e2-5643-9baf-93c42967cb30`.

## Current official geometry acquired

The eight numbers in Boverattes regulation PDF5/article4 all return current Vaud cadastral objects for Pully (BFS5590). Their current EGRIDs, official areas and geometries are preserved from APIGeo layer21 with URL, retrieval time, hash and independent count check. This proves current number matches, **not historical lineage or unchanged boundaries**.

A bounded layer22 query over their envelope returns 57 footprint features. Replayed intersections greater than1m² produce 21 distinct footprint records across the eight parcels. Sixteen have distinct non-null EGIDs; five have none. These are not counts of homes or necessarily separate occupied buildings. Parcel2034 alone intersects ten footprint records; its official area is14,256m². Both official-area totals across the eight matched parcels are24,341m², but some individual MO/RF areas differ.

The reference plot uses current official coordinates only. It is not an alignment of the1996 PDCom or the later PPA, and neither unbuilt residual area nor remaining development capacity is estimated. The eight matched land parcels are not a complete inventory of surface rights. The1m² cutoff filters tiny intersections; it is not a validated occupancy criterion.

## Exact PDL remains missing

Two indexed official minutes URLs (2007 and2009) returnedHTTP404. The current council page lists archive years2018–2025 and directs earlier requests to the municipal archives. The retrieved page and failed requests are retained; no contact was sent. Search snippets are leads, not acquired adopted-plan evidence. The prior2007 proposal404 is recorded in `../map-semantics/boverattes-context.json`.

Boverattes should therefore remain a historical lead with demonstrated subsequent implementation and unresolved residual capacity. The referenced adopted PDL, applicable option-selection publication, later amendments and historical cadastral continuity still need evidence. These research references do not advance commune completion or add receiver sectors.

`verify.py` checks file/source hashes, identities/counts/geometry validity and recomputes all21 intersections. `plot.py` reproduces the current-reference image. Geometry is EPSG:2056. No owner fields were requested.
