# Swiss planning documents — Pixxels parser

Datasets `ch_planning_documents` (#267, catalog) and `ch_planning_document_text`
(#269, PDF/OCR) inventory official planning
references and acquires versioned PDF text. This is an acquisition layer for the
Swiss regulation corpus; it does **not** claim 2,000 communes, normalized numerical
building rules, legal completeness, or new PDCom polygon coverage.

## Operating contract

- Owner/landing database: RE-LLM, registered centrally in Pixxels `datasets`.
- Sources: public geodienste NPL 1.2 STAC/INTERLIS exports (German/French models),
  plus Zürich's official ÖREB register, **Nutzungsplanung allgemein**.
- Code: this folder; workflow `.github/workflows/ch_planning_documents.yml`.
- Schedule: yearly, 15 September 02:20 UTC; registry expectation 365 days + 30 grace.
- Bounded runs (2026-09-14): national text/OCR does not fit one GitHub job (the 2026-09-13 run was
  killed at the 350-min limit after ~500 URLs of ~28k). Every run has a time budget (default 290 min):
  no new document starts after it, in-flight documents get 20 more minutes, one document may take at most
  30 min (`document_time_limit`, retried weekly). A continuation (`--resume`, every 6 h at :50) extracts
  against the latest stored catalog without re-discovery and is a silent no-op when nothing is due.
  Never-attempted references go first; `error` / `needs_ocr` references are retried at most weekly.
  A run stopped by its budget after real progress is logged `partial` (dataset stays active, freshness is
  not advanced); a run that starts nothing while work is due fails. `complete` still requires zero
  outstanding references in the catalog.
  Command Center Run Now dispatches the workflow without required inputs.
- Acquisition outcomes: `acquisition_logs`, exact dataset metadata PATCH, and a
  GitHub run report. Empty/changed source contracts fail loudly. Failed or bounded text
  runs never become an annual text success. A verified national catalog can succeed
  independently of text extraction failures. The parser restores prior verified freshness
  after the legacy trigger-pipeline dispatch timestamp side effect.
- `complete` means the configured acquisition ran without source/download errors;
  it never means national legal coverage. Inspect the canton coverage and extraction
  outcome fields, including `restricted`, `not_published`, `no_document_records`,
  `missing_url`, `needs_ocr`, and `html_unverified`.
- Workflow failures before Python starts are also reported. A cancelled runner can
  leave a running acquisition log; check the GitHub run before diagnosing a hang.

## Storage and distribution

`bronze_ch.planning_document_runs` stores scope and outcome. `planning_document_sources`
stores official references with source-provided commune IDs and legal status.
`planning_document_versions` stores byte hashes, page text, extraction method/status,
final URL, and a knowledge document pointer. Sources sharing a URL share its byte
version and download; `source_id` on a version is its first observed reference,
while all `sources.current_version_id` links describe its reference coverage.

Only successfully extracted PDFs enter existing `knowledge_ch.documents/chunks`.
Knowledge IDs are deterministic per source URL + byte hash; page numbers survive
chunking. Prior byte versions remain auditable; old knowledge documents are marked
inactive, never deleted. No changes to existing Geneva custom PDCom or spatial tables.
The raw source URL remains available; binary PDFs are not separately archived.

Knowledge documents/chunks already flow through `gold_ch.lia_sync_manifest` to
Lamap `lia.knowledge_documents` and `lia.knowledge_chunks`. Acquisition recurrence
is independent of the existing daily delivery. The central destination registry
allows one row per receiver; the primary table is registered and this document
specifies its companion chunks. Other receivers must be explicitly registered.

The existing LIA sync routine truncates entire receiver tables. Do not invoke it
merely to test this parser. Verify the normal scheduled delivery or perform a
scoped, non-destructive upsert of the new parser's rows after inspecting the exact
receiver columns. Never infer receiver delivery from the acquisition timestamp.

Bulk knowledge inserts disable only `classify_on_insert`, restore it in the same
transaction, and leave classification pending for the existing batch backfill.
Taxonomy validation stays enabled. No per-row LLM calls or new AI service secrets.

## Extraction and limits

PDF text uses pypdf layout extraction. The workflow installs local Tesseract OCR
(DE/FR/IT/EN) and Poppler; image pages with insufficient text get OCR with bounded
subprocess time and image dimensions. Persisted page metadata records the method.
Documents still lacking readable text remain `needs_ocr`; arbitrary HTML remains
`html_unverified` and is not promoted as legal text. OCR is not a verified numeric
rule extractor. Rule/zone interpretation with article-level validation is a
separate downstream stage.

Commune numbers come only from source fields / Zürich's displayed BFS labels,
never internal option IDs, document IDs, filenames, or guessed names. Missing BFS
stays NULL. Source legal status is retained verbatim, including repealed/draft
states. Search consumers must distinguish historical documents from current law.

NW/OW/VD downloads require authorization and are not requested. Missing cantons
and exports with no document references are listed in every national report.
Existing sources for those territories are not replaced by this parser.

## Development and recovery

```sh
pip install -r pipelines/ch-planning-documents/requirements.txt
python -m unittest discover -s pipelines/ch-planning-documents/tests -v
python pipelines/ch-planning-documents/parser.py --dry-run --report catalog.json
# Auth is injected via RE_LLM_DB_URL, CMD_URL, CMD_KEY. Never write keys to files.
python pipelines/ch-planning-documents/parser.py
```

`--cantons`, `--max-documents`, and `--catalog-only` are bounded development options:
they produce an incomplete run, not annual freshness. `--no-monitor` is for an
explicitly logged local acceptance pilot only; ordinary runs must report to Pixxels.
`--cache-dir` reuses local XTF fixtures and must not be used for scheduled freshness.
Default runs always fetch the current catalog; successful downloads younger than
365 days are reused. Reruns continue pending/error URLs without duplicating bytes.

Schema: `supabase/migrations/20260913145910_ch_planning_documents.sql` (RE-LLM only).
Rollback is to disable this workflow and pause this dataset, retaining audit data.
Never truncate or delete existing knowledge or receiver tables for rollback.

## Reviewed municipal VD corpus bridge

`vd_pdcom_text.py` is a separately scoped bridge for an explicitly reviewed approved
municipal PDCom PDF already registered in `bronze_ch.vd_pdcom_documents`. It reads
hash-bound local bytes (including PDFs exceeding the national fetch cap); it does
not change national discovery, download limits, extraction or publisher defaults.
`source=vd_pdcom_municipal` and the actual municipal publisher preserve provenance.
The first reviewed source is `reports/lausanne-approved-text/review.json`.

`build_bundle(pdf_path, review)` retains every physical page, exact native text,
empty/image-only status and original page citations. Only nonempty native text
becomes searchable chunks. Page status and signed approval scope, historic
statistical limits and lack of spatial qualification accompany documents and
all chunks. `partial_native_text` is deliberately not complete visual/OCR coverage;
`ingestion_status=completed` means the native-text ingestion operation completed.
Image-only gaps remain explicit, including approval images reviewed separately.

`persist(connection, bundle, operation_id, pdf_path)` rebuilds from exact PDF
bytes before writing, checks current registered URL/hash/status/page count/BFS,
and verifies same-ID provenance on replay. It does not commit. Rehearse twice in
one transaction and roll back; check no rows remain and classifiers are restored.
Unexpected classifier state and an active older version of the same URL fail
closed pending explicit reviewed supersession. No unrelated source is deactivated.

`deliver(connection, document_id)` checks the existing active LIA sync manifest
and exact source/foreign columns, then updates/inserts only that document and its
chunks. It never calls the global truncating sync or expands geographic routes.
Direct Lamap full-field readback remains required after commit. The companion
`monitor` function uses the registered text dataset's acquisition log, reporting
this bounded run as `partial`; it restores legacy log-trigger changes to the
exact locked dataset freshness/count/status fields in the same transaction,
then verifies them. It never advances national freshness or coverage. Source and receiver commits must be reported separately on failure;
exact replay safely resumes the same source operation/document IDs with a new
monitor-attempt UUID; prior failure/completion logs are retained unchanged.

The bridge requires PyMuPDF from the existing `vd-pdcom` runtime in addition to
this parser's dependencies. Unit tests cover approval scope, provenance conflicts,
exact bytes, physical-page gaps and deterministic replay. Its caller must preserve
the approved review file and the same operation ID; supplements require their own
review and cannot inherit main-document approval. This delivers searchable source
text, not extracted legal rules, current capacity, qualified geometry or commune
completion.

Reproducible invocation (inject registered routes as environment variables; never
write their values to reports). Default rehearses source and optional receiver
twice in one transaction and rolls back. Add `--commit` only for a reviewed live
operation; this requires `PIXXELS_DB_URL` and a distinct `--monitor-id UUID`,
and writes a scoped acquisition log. For retry keep `--operation-id` stable, use
a new monitor ID, and preserve the previous log rather than resetting its status.

```sh
python pipelines/ch-planning-documents/vd_pdcom_text.py \
  --pdf /approved/project/path/approved.pdf \
  --review pipelines/ch-planning-documents/reports/lausanne-approved-text/review.json \
  --operation-id EXISTING_OPERATION_UUID --deliver \
  --report /approved/project/path/text-receipt.json
```

`RE_LLM_DB_URL` is the registered source session-pooler route. Receiver access uses
its already registered FDW; the command does not accept an arbitrary receiver.
`PIXXELS_DB_URL` is the registered monitor route. Run outside Desktop/Documents;
keep reports and PDF under the approved local project root. A failed commit/monitor
acknowledgment remains an explicit failure requiring scoped readback, never an
assumed successful receiver delivery.
