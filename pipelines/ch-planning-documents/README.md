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
