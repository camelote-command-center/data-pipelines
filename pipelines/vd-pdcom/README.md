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
This first stage does not yet publish geographic sectors or parcel assignments.

For spatial release: prefer native GIS/GeoPDF coordinates; otherwise independent
control points, measured residuals, and visual overlay validation. Do NOT reuse
the old `georef.py` map-bbox/commune-bbox confidence floor as positional accuracy.
Do not apply Geneva zone-5 eligibility logic to Vaud. Schematic map precision and
alignment error must accompany parcel intersections; no automatic whole-parcel
classification from a small overlap. A new PDF hash requires renewed validation.

`vd_pdcom_sectors` provides a real WGS84 geom and GIST index for future validated
output. PDF page-space paths belong to inspection artifacts, never geom.
Delivery status becomes verified only after field/geometry readback at the
registered receiver. Existing GE datasets and syncs are not modified.

Run: `python pipelines/vd-pdcom/parser.py --persist --monitor` using registered
RE_LLM_DB_URL / CMD_URL / CMD_KEY secrets. Without flags, inspection is local.
The migration is additive; rollback is pause scheduling and retain audit data.

Remaining scope: canton-wide source search, pilot vector extraction and measured
alignment, review of legal versions, parcel linkage and receiver delivery,
then progressively resolve the 300 commune backlog. Neither three pilot sources
nor successful downloads close that backlog.
