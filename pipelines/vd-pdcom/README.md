# Vaud PDCom coverage and spatial pilot

Owner: RE-LLM, operated and monitored through Pixxels. This is a dedicated
PDCom effort, separate from the nationwide regulation-document corpus.

## Coverage contract

The denominator is the OFS snapshot for the execution date, not the number of
PDFs discovered. On 2026-09-13 the official hierarchy yields 300 VD communes.
Use `BfsCode`, never `HistoricalCode`; follow Parent up to canton VD. The older
`bronze_ch.federal_communes` table is not a trustworthy current VD census.

`bronze_ch.vd_pdcom_communes` retains one row per federal code, including retired
communes. Each successful census adds new communes, preserves existing progress,
and retires absent codes without deleting history. Large roster changes stop for
review. Retired-source territorial correspondence still requires review; never
inherit validated status automatically into a merged municipality.

Every current commune starts `not_searched / pending / not_ready`. Discovery,
extraction and delivery are separate. `not_found` means search inconclusive;
`confirmed_no_plan` requires affirmative official evidence. An intercommunal
document links through `vd_pdcom_document_communes` to each covered commune;
finding one does not prove whole-commune coverage. Multiple documents and byte
versions are retained. Annual acquisition plus manual Run Now is supported.

`bronze_ch.vd_pdcom_coverage` is the complete queryable tracking view. The full
commune CSV and summary are saved in each GitHub Actions artifact and counts in
Pixxels acquisition_logs.error_details. A successful census/acquisition run does
NOT mean all communes have been extracted: report.coverage_complete stays false
until a separate validated release is implemented. No false 300/300 completion.

## Pilot and quality gates

Reviewed official source manifest: Lausanne, Morges, Prangins synthesis maps.
Download checksummed PDFs, inspect text and vector paths, and preserve provenance.
This first stage does not publish consumer parcel assignments. A Prangins
geographic pilot is stored separately as review_required:15 sectors and541
sector/parcel intersections (527 distinct parcels). See reports/prangins/README.md
for complete alignment evidence, including seven outliers and untrimmed RMSE.

For spatial release: prefer native GIS/GeoPDF coordinates; otherwise independent
control points, measured residuals, and visual overlay validation. Do NOT reuse
the old `georef.py` map-bbox/commune-bbox confidence floor as positional accuracy.
Do not apply Geneva zone-5 eligibility logic to Vaud. Schematic map precision and
alignment error must accompany parcel intersections; no automatic whole-parcel
classification from a small overlap. A new PDF hash requires renewed validation.

`vd_pdcom_sectors` provides real WGS84 geom and GIST index; review_status
separates pilot candidates from validated output. source_precision_m remains
NULL when unknown. `vd_pdcom_parcel_candidates` contains review-required
geographic overlaps. PDF page-space paths belong to inspection artifacts, never geom.
Delivery status becomes verified only after field/geometry readback at the
registered receiver. Existing GE datasets and syncs are not modified.

Run: `python pipelines/vd-pdcom/parser.py --persist --monitor` using registered
RE_LLM_DB_URL / CMD_URL / CMD_KEY secrets. Without flags, inspection is local.
The migration is additive; rollback is pause scheduling and retain audit data.

Remaining scope: canton-wide source search, pilot vector extraction and measured
alignment, review of legal versions, parcel linkage and receiver delivery,
then progressively resolve the 300 commune backlog. Neither three pilot sources
nor successful downloads close that backlog.

## Resumable canton-wide discovery (2026-09-14)

Discovery also reads bounded, literal `window.open` document links on table rows
(such as SDOL), without executing JavaScript. Dynamic handlers remain excluded.
Intercommunal and localised director plans are discovery candidates; document
scope, version and relevance still require review before acquisition/coverage.
Reviewed large compilations may declare `max_pages` (1–1,000; default 500).
The 792-page 2022 PDi-OL municipal programmes use 800 pages and 120 MB, with
vector enumeration disabled. Their schedules are indicative, not parcel rights.


`discovery.py --limit 30 --workers 4 --monitor` matches the current OFS roster
against UCV's full municipality directory, resolves its official website field,
and crawls up to 12 planning-related HTML pages per commune. It records candidate
PDCom links with their referring page and page hashes. A candidate is **not** an
accepted/current plan; regulations alone are not treated as PDCom evidence.

The workflow resumes 30 due communes hourly, sharing its existing concurrency
group. The annual September run still refreshes the census and reviewed pilot
PDFs. Manual `mode=discovery` resumes discovery without downloading the pilot
again; `mode=full` runs both. Successful searches become due for refresh after
365 days; blocked websites retry after seven days. Empty/limited searches enter
`manual_search_required` and require a second research pass; they never become
`confirmed_no_plan`. The initial backlog has priority over retries.

Runtime records live in `bronze_ch.vd_pdcom_discovery_queue`,
`vd_pdcom_discovery_attempts`, and `vd_pdcom_source_candidates`. One queue row per
current commune, an advisory lock, and per-commune commits make interruptions
resumable without losing earlier results. Existing document, extraction and
receiver validation states are preserved. Pixxels acquisition details include
queue counts and candidate totals; an acquisition success does not mean coverage
or receiver delivery is complete. Evidence artifacts are retained for 90 days.

### Additional official sources

`reviewed_sources.json` supplies official PDF links reviewed for acquisition.
`acquire_sources.py --persist` hashes each PDF, inventories every page's text and
vector-path count, saves document/commune provenance and retains original files
in workflow artifacts. Full manual and annual runs acquire these documents.
A reviewed link does not establish plan approval, currency or spatial coverage:
`plan_status` remains unverified/consultation pending document-specific review.
Existing validated/candidate extraction states and delivery states are preserved.

Registered documents also resolve matching pending discovery candidates in the same
transaction, with a per-commune discovery audit linking document IDs and hashes.
Only exact PDF URLs and registered landing URLs (whitespace/trailing slash normalized)
with matching commune membership qualify. Rejected links, malformed relative URLs,
and other communes remain untouched. This changes source-link review only.

### Source review gates

`document_reviews.json` binds approval evidence and reviewed map-page references
to an exact document ID and SHA. `reviews.py` applies this metadata after source
persistence; changed bytes cannot inherit an earlier approval. Reviews preserve
cantonal reservations and indicative map limits, and do not advance commune
extraction/delivery validation. Épalinges cahier II page3 records approval on
5July2023 with a reservation concerning chapter2.2/pages21–22; map pages99–102
are indexed for the next spatial-extraction step. Page numbers are PDF indices.

### Shared plans and second-pass sources

The reviewed manifest includes Gland's two published PDCom documents, Montreux's
18 published PDCom chapters, and the PDi-OL version identified by SDOL as adopted
25August2021. Its cover independently names the same eight current communes as
the official OFS district roster. `commune_bfs_list` links that single versioned
document to all eight communes. The parser verifies current roster membership,
rejects duplicate identities and requires intercommunal scope for shared sources.
Source possession does not establish complete extraction or receiver delivery.
Official landing pages remain in the manifest for source/version review.

## Private Lamap review delivery

Pixxels `vd_pdcom_review_delivery` tracks a private review copy, separately from
coverage/acquisition. `review_delivery.py` uses the existing registered
`lamap_db_server` FDW route to upsert only `ref.vd_pdcom_review_sectors` and
`ref.vd_pdcom_review_parcels`. It preserves source hashes, plan status, reservations,
validation evidence and boundary-review flags. Every row is explicitly
`internal_review_only`. Anonymous/authenticated SELECT is revoked and RLS enabled.
It does not write released PDCom layers or advance commune delivery completion.

Deployment order: apply `sql/lamap_review_receiver.sql` on registered Lamap;
apply the additive review-view migration on RE-LLM; apply
`sql/rellm_review_foreign.sql` there. Register both source and Lamap destination in
Pixxels. Existing server/user mappings remain unchanged. Hourly/full workflow runs
copy and compare every source/receiver row; any missing/extra/different row or
invalid/non-4326 geometry fails the delivery. Empty sources fail closed. No
DELETE/TRUNCATE or global sync-registry changes. Repeated delivery is idempotent.
This provides receiver-side review data; it is not a substitute for final QA and
publication approval criteria in the master task.

Reviewed sources can set `enumerate_vectors: false` to inventory text and page
metadata without expanding PDF drawing operations. Unknown vector counts stay
null with `vector_inventory_status=not_evaluated`; this is not a zero-vector or
validated-map claim. Existing sources retain vector enumeration by default.
The six Alpes vaudoises tourism documents use this mode; the 228 MB explanatory
report has a source-specific 250 MB download limit. Their 15 commune links cover
regional tourism above the upper vineyard limit, not complete communal PDComs.
Source review metadata persists on newly encountered versions. Existing inspected
versions and manual research remain preserved. These sources participate in the
existing annual/full acquisition, without an additional workflow or schedule.

### Reviewed archive sources

Official ZIP bundles may register an exact `archive_member` alongside `pdf_url` (the archive URL). Only that reviewed PDF is read, in memory. The download retains the existing public URL/redirect/deadline checks; `max_archive_bytes` limits the archive and `max_bytes` limits the uncompressed PDF (both default100MB). Missing, duplicate, encrypted, oversized and non-PDF members fail acquisition. The first page inspection records archive URL/hash, exact member name and extracted PDF hash. A changed member name requires review; the parser never chooses an alternative automatically.

The September2025 Bourg-en-Lavaux source is a pre-council edition from an official annex bundle. Council6October2025 adopted the PDCom **as modified**: exact amendment linkage and cantonal approval remain unresolved. Arc-en-Ciel and Cocagne-Buyere are partial local plans, not full communal coverage; retain their version/currentness notes. Five reviewed source additions are included in the existing annual acquisition route without another workflow.

### Replaying reviewed source batches

`reviewed_batches.py` reapplies registered inspection annotations only when document ID, SHA-256 and page count match the reviewed bytes. The current four-document batch preserves the Chablais activity source roles, 14 operational fiches and 126 reviewed area/job cells, plus Jouxtens's 18 map-page classifications and signed reservations. Existing page text and unrelated review metadata remain intact; document-level annotations are supported by both review paths. Changed PDF bytes receive no old batch annotations. Replay does not set plan status, geographic validation, commune completion or receiver eligibility.

The three Chablais PDFs are included in the existing annual reviewed-source manifest. Jouxtens replay is registered for its already acquired bytes, but its browser-only acquisition remains an exception and is not added to the unattended manifest. Nyon native DWF decoding is still a separate historical research artifact pending alignment, semantics and production integration.

## Private historical site scenarios

The existing private Lamap delivery step also copies
`gold_ch.v_vd_pdcom_review_site_scenarios` to
`ref.vd_pdcom_review_site_scenarios` through `lamap_db_server`.
It exposes 102 reviewed SDRM source-table scenarios for 51 sites in eight communes.
These are historical inhabitants-plus-jobs scenarios, **not current housing
capacity, validated sectors or parcel assignments**. Source blanks, page references,
word boxes, source dates, arithmetic conflicts and limits remain attached.

Provision once, in order: `sql/lamap_review_site_scenarios.sql` on Lamap,
then `sql/rellm_review_site_scenarios.sql` and
`sql/rellm_review_site_scenarios_foreign.sql` on RE-LLM. Both source and receiver
are private; receiver RLS is enabled and public/anon/authenticated grants revoked.
The source view selects only the reviewed document ID/hash. Changed bytes or a
missing/duplicate source inventory fail delivery without deleting prior evidence.
The `(document_id, site_id, scenario)` key makes repeat delivery idempotent.
Full-row readback checks preserve nulls and reject mismatches or extra receiver rows.

The existing scheduled/manual delivery step records the scenario result separately
from spatial counts. No new workflow or recurrence is introduced. Rollback is to
remove the scenario call from the delivery entry point; retain the private evidence
tables and leave all existing spatial delivery untouched.

## Read-only release preflight

`python pipelines/vd-pdcom/release_preflight.py --output preflight.json` uses
`RE_LLM_DB_URL` in a database-enforced read-only transaction. It inventories all
current communes and each sector's recorded validation, validator, source
precision, source approval status, geometry, boundary-review flags and full-row
private receiver parity. It never changes completion or releases data. A clean
private receiver copy is not final delivery. Even zero listed blockers is only a
necessary-gate report: independent positional/semantic review, exact current
version and reservations, exhaustive commune map/category coverage, and a
registered final release receiver with verified readback remain required.
