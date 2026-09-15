# Bex PUM.7 official-reference route

The PDCom parser has its own scoped official parcel-reference snapshots; it does not fabricate rows in the federal cadastral mirror. Each snapshot records the exact document, current commune, bounded official layer21 query, count-only request, response hash, timestamps and LV95 envelope. Reference rows carry EGRID, official attributes and valid MultiPolygon2056 geometry with GiST indexing. Snapshot/EGRID foreign keys bind candidate pairs to their actual reference evidence. Tables are private with RLS.

The Bex SHA/document gate replays the selected radius2 outline from `reports/chablais/bex-outline`. Each full/manual/annual acquisition fetches current official references. Initial candidate insertion uses their actual intersections and checks Bex-only geographic attribution. On repeat, unchanged parcel keys/reference geometries are idempotent; a changed key set or reference geometry fails for review instead of silently replacing the reviewed geometry. Historical snapshots are retained. No source-area, approval or positional uncertainty is removed.

Cadastral type refresh accepts a current, foreign-key-backed official reference when the silver mirror is missing. If mirror and reference disagree, reference identity is unresolved, or the fresh official response disagrees, it fails. Fresh type evidence includes reference snapshot IDs. It does not infer identity from a loosely attached JSON attribute or a sector's document-wide commune list.

The 10m boundary flag remains an illustrative review heuristic. The sheet's perimeter-refinement warning and +1018m² area discrepancy remain in sector evidence. Output stays `review_required` / `internal_review_only`; nothing advances a commune to completed or becomes publicly released.

## Operations

- Migration: `20260915022509_vd_pdcom_official_parcel_references.sql`.
- Acquisition hook: `acquire_sources.persist` → `bex_pilot.persist` → `official_references.refresh`.
- Existing full/annual workflow then runs `cadastral_types.refresh` and registered private Lamap delivery.
- New reference tables are owned by the existing Pixxels VD PDCom parser scope; no independent unmonitored cron or mirror writer is introduced.
- Missing/changed references remain explicit failures for review, with source snapshot evidence preserved.

## Verified first delivery

One Bex sector /65intersections now use the official reference route. Initial and repeat ingestion return65pairs with the same snapshot and no duplicates. All1,086 EGRIDs have fresh official types:1,059land objects and27surface rights, across1,107pairs. Private Lamap full-row parity:33sectors/1,107pairs, zero mismatch. Independent receiver checks confirm Bex-only attribution,65land intersections, review_required/internal_review_only, RLS and no anon/authenticated read grants. Source snapshot136geometries all intersect the requested envelope. Previous1,042pairs remain present. Zero completed communes/public releases.

A first ALTER-based draft hit a long-running reader's lock; it rolled back. The final migration uses a separate FK-backed link table and applied without interrupting that reader or altering the existing candidate table.
