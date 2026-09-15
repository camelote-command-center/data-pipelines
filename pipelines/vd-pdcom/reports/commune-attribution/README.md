# Shared planning-document commune attribution

A planning document may cover Aigle, Bex and Ollon while a particular sector is entirely in Bex. The review view previously copied the whole document membership into every sector. The migration now intersects each sector with current declared member communes and requires a two-dimensional interior overlap. Source membership remains separately preserved in `bronze_ch.vd_pdcom_document_communes`.

Boundary touching alone does not assign a commune. Topology is compared in the sector's stored CRS (4326), transforming the official reference boundary once. Testing revealed that transforming a reference-derived test polygon 2056→4326→2056 introduced 6.667 m² of numerical overlap along the Bex/Ollon boundary. Comparing both in 4326 fixes that case without an arbitrary area cutoff. The 2056 bounding-box predicate remains a coarse index filter.

No geographic match returns an empty array, never a fallback to all document members. `review_delivery.validate_attribution` rejects empty/null commune arrays before any receiver upsert. Geometry outside the declared document scope is not newly attributed; source scope and boundary QA remain separate.

`check.py` exercises the actual view with synthetic sectors in one rolled-back database transaction:

- Bex geometry → Bex only.
- Ollon geometry → Ollon only.
- Bex/Ollon union → both.
- Aigle geometry touching neighbours → Aigle only.
- Prangins geometry against the Chablais source → empty, rejected by delivery preflight.

These fixtures never reach a receiver and are verified absent afterward. Run with the established protected `RE_LLM_DB_URL` route. The production view migration was applied after those tests; all 32 existing sector rows have the same complete-row hash as before. Scoped private delivery verifies 32 sectors and 1,042 parcel pairs with no mismatch. No public layers or completed-commune states change.

This prerequisite enables a future Bex candidate from the intercommunal source without falsely assigning it to Aigle/Ollon. The Bex outline and parcel QA remain pending.
