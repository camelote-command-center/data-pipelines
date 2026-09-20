# Registered source candidate reconciliation — 20 September 2026

Source acquisition registered documents but left their discovery landing links pending. The annual acquisition handler now reconciles matching pending links within its document transaction and records document IDs/hashes in per-commune discovery evidence.

A rollback trial and committed backfill resolved 11 existing landing candidates across 10 communes. Pending links changed from 42 to 31; accepted from 118 to 129; rejected stayed 27. A repeat changed zero rows. Existing document, document-membership, commune, sector and parcel-candidate row hashes stayed identical.

Only exact registered PDF URLs or registered landing URLs, normalized for surrounding whitespace and a trailing slash, with matching commune membership qualify. Malformed relative links, filename-only similarities, query variants and other communes remain unresolved. Rejected decisions are preserved.

This is source-link review. No additional source commune, extracted/aligned map, validated sector or receiver row was produced. The source corpus remains 147 PDFs and 243 communes, with 57 source gaps and zero completed communes. Last verified private delivery remains 42 sectors, 2,286 parcel pairs and 2,264 distinct EGRIDs.

The first trial accidentally reused an older research operation ID and hit the existing unique audit constraint; it rolled back without changing source decisions or the older audit. The distinct automatic-reconciliation operation in verification.json passed rollback, commit and repeat checks.
