# Prangins parcel intersection QA — 14 September 2026

Current RE-LLM cadastral source has 1,012 distinct Prangins EGRIDs and no duplicate EGRIDs. Recomputing positive-area intersections finds the same 541 sector–parcel pairs as stored: zero missing or extra pairs. Recomputed areas differ by over 0.01 m² on 217 pairs, with maximum absolute difference 0.172 m². The cause is not established; no exact area-equality claim is made and no stored values were overwritten.

Of 541 nominal pairs, 15 intersect by less than 1 m² and 55 by less than 1% of parcel area. These remain candidates, not statements that an entire parcel is covered or eligible.

Eroding each sector by 10 m yields 449 intersecting pairs; expanding by 10 m yields 694. This is an illustrative sensitivity calculation, not a confidence interval or measured accuracy bound. Pair counts may count the same EGRID in multiple sectors. Current cadastre is not necessarily the cadastre shown on the 2013 plan. The source planning boundaries are indicative.

All sectors remain review_required. Before release, review the small and boundary intersections and the intended category meaning; do not auto-accept candidates based only on positive overlap. Existing replay inserts use ON CONFLICT DO NOTHING, so future cadastral changes need explicit reconciliation with history rather than an assumption of freshness.

## Reproduce

With a registered RE-LLM PostgreSQL connection supplied through DATABASE_URL, run `python pipelines/vd-pdcom/reports/prangins/parcel-review/check.py /path/to/output.json`. Dependencies: psycopg2. The script uses a read-only transaction and a 60-second statement timeout. It reads the current database, so later cadastral revisions can change the result. No credentials or connection URLs are written to the report.
