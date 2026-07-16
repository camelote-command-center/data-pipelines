# amtsblatt-be — Bern succession / ownerless-plot event stream

Distils Swiss gazette **succession notices for canton Bern** into an estate-grain
event table used as an **early-warning / alerting signal** for estates heading
toward vacancy or reversion to the state.

## ⚠️ Scope — read this first

- **This is an event/alerting stream, not a "claim a free plot" pipeline.** It
  surfaces estates that *precede* a plot going ownerless (absent heirs, repudiated
  estates in liquidation, estates falling to the community). It does **not** list
  occupiable herrenlos parcels.
- **Event → parcel linking is a separate, manual/LBI step.** Notices name the
  deceased and the authority; they almost never name the immeuble. `linked_egrid`
  is **always NULL** here and is only ever set by a downstream positive match
  (GRUDIS manual lookup, or owner-name match once BE plots carry owners). **Never
  fabricate a parcel link.**
- **Bern's private-occupation route for derelict plots is narrowing** — a March 2026
  reform gives communes a pre-emption right. The value is the *signal*, surfaced early.
- **GRUDIS public is not scraped** (AGOV/BE-Login gated, serial-query-protected).
  The only machine-parseable channel is the gazette.

## What it does (no re-ingest)

The gazette metadata is **already ingested** by the sibling pipeline
`pipelines/amtsblattportal` into `bronze_ch.amtsblatt_publications` (2.4M rows,
`kabbe` = Bern). This pipeline is a **downstream parse layer**:

1. **Select** succession candidates from the metadata via the matview
   `bronze_ch.succession_notice_candidates` (canton-parameterized).
2. **Fetch** each notice's **content XML** (`/api/v1/publications/{id}/xml`) — the
   list API omits the body — and store it in `bronze_ch.succession_notice_raw`.
   Parsing is **deterministic** against the gazette's published XSDs (no LLM needed;
   see "Parsing" below).
3. **Build** estate-grain rows in `bronze_ch.succession_events`, deduping an
   estate's republications (Erbenruf publishes 3×; a repudiated estate spans
   KK01→02→04→06) into one event.

### Sources (two tenants, one platform — amtsblattportal.ch OGD REST API)

| Signal | Tenant | Rubric | event_type |
|---|---|---|---|
| Erbenaufruf (Art. 555 ZGB — call to absent heirs) | `kabbe` | TE-BE20 | `erbenruf` |
| Testamentseröffnung | `kabbe` | TE-BE10 | `testament` |
| Öffentliches Inventar (Rechnungsruf / Auflage) | `kabbe` | TE-BE60/70 | `inventar` |
| Erbschaft an ein Gemeinwesen (estate → state) | `kabbe` | TE-BE90 | `escheat` |
| **Ausgeschlagene Erbschaft in konkursamtl. Liquidation** | `shab` | KK01–06 (`addition=refusedLegacy`) | `ausschlagung` |

The repudiation→liquidation signal — the brief's "all-heirs-repudiated estates" —
lives in **federal SHAB filtered to canton BE**, not the cantonal Amtsblatt. It is
the dominant volume (~16.5k publications vs ~370 cantonal).

## Parsing

The content endpoint returns **schema-validated XML** (e.g. `TE-BE20-export.xsd`).
`lib/parse.ts` extracts fields **deterministically** — cheaper, testable, and more
accurate than LLM extraction. Both content shapes (`<testator>` for TE-*,
`<debtor><person>` for KK) share a person sub-structure. Fixture regression tests
in `parse.test.ts` cover every rubric and both languages (DE/FR).

> Optional Sonnet enrichment (parent names, marital status from the free-text
> `<callInheritance>`/`<probate>`) is a future add-on — not required for any
> structured field, so it is intentionally not wired in.

## Access / politeness

- Uses the **sanctioned OGD REST API** (`amtsblattportal.ch/api/v1/…`, no auth).
- `robots.txt` is `Disallow: /` — that governs the HTML UI, not the documented data
  API (the sibling ingest already uses it in production). We stay polite: 300 ms
  between content fetches, retry with backoff on 429/5xx.

## Run

```bash
# one-shot full-archive backfill (idempotent; skips already-fetched notices)
cd pipelines/_shared && npm ci && cd -
cd pipelines/amtsblatt-be && npm ci
npx tsx run.ts                    # fetch content → build events

# scoped / incremental
SINCE=2026-01-01 npx tsx run.ts   # only notices on/after a date
MAX=100 npx tsx fetch-content.ts  # cap a run
CANTON=BE npx tsx build-events.ts # rebuild events from raw only

# tests (offline, no network/DB)
npm test
```

Required env: `RE_LLM_SUPABASE_URL`, `RE_LLM_SUPABASE_SERVICE_ROLE_KEY`.

## Deployment

`.github/workflows/amtsblatt_be.yml` — weekly, **Thursday 06:00 UTC** (after the
Wednesday Bern Amtsblatt release and after the sibling `amtsblatt.yml` daily ingest
has landed the new metadata). `workflow_dispatch` runs the archive backfill.

## Generalizing to other cantons (bonus §8)

The candidate matview and `mapEventType` key off the `TE-<CC>` rubric family and the
`shab` KK `refusedLegacy` flag, both canton-parameterized. Adding a canton = extend
the matview's tenant/rubric predicates and pass `CANTON=<CC>`; no rewrite.
