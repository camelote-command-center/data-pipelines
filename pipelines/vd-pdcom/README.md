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
