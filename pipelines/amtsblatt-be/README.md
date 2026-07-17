# amtsblatt-be — Bern distressed-estate / succession event stream

Distils Swiss gazette **succession notices for canton Bern** into an estate-grain
event table used as an **early-warning / alerting signal**.

## ⚠️ What this actually is (read first)

- **It is a distressed-estate *liquidation* feed, not an ownerless-plot inventory.**
  The volume is almost entirely **repudiated estates going to konkursamtliche
  Liquidation** (all nearest heirs declined the inheritance, Art. 573 ZGB), plus a
  thin cantonal layer of Erbenrufe / inventories.
- **Genuine ownerless / escheat events are essentially nonexistent in BE**: the
  cantonal "Erbschaft an ein Gemeinwesen" rubric has **1 notice in the entire
  digital archive** (since 2020). Do not present this stream as a supply of
  herrenlos parcels.
- **This is an event/alerting stream, not a "claim a free plot" pipeline.** It
  surfaces estates that *may precede* a plot changing hands under distress. It does
  **not** list occupiable parcels.

### 🔚 Arc close — the original goal is resolved: BE is NOT an acquisition play

This work began as a hunt for **biens sans maître** to acquire. That goal is **closed as
not viable in BE**, on evidence:

| blocker | evidence |
|---|---|
| Escheat effectively doesn't happen | `TE-BE90` = **1 notice** in the entire digital archive (since 2020) |
| Events can't be linked to parcels | re-LLM has **472,671 BE plots** but **0 BE owners** — nothing to match an estate against |
| The owner registry isn't sweepable | GRUDIS public is AGOV/BE-Login gated + serial-query-protected — single manual lookups only |
| The legal route is narrowing | March 2026 reform gives communes a **pre-emption right** over derelict plots |

**What actually exists** is a **distressed-estate alerting feed** keyed by *deceased name
+ last domicile*, triageable **by commune**. Its value is an **LBI outreach signal**, not
ownerless-land acquisition. The **event→parcel link remains the real gate and stays
deferred/manual** — `linked_egrid` is NULL on all 6,867 and is never fabricated.
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

The repudiation→liquidation signal — the "all-heirs-repudiated estates" — lives in
**federal SHAB filtered to canton BE**, not the cantonal Amtsblatt. It is the
dominant volume (~16.5k publications vs ~370 cantonal).

### `repudiation_scope` — what the source states, and what it does not

Values: `konkursamtliche_liquidation` (6,745) · `unknown` (4) · `not_applicable` (118).

`konkursamtliche_liquidation` records **two facts the notice actually carries**: the
estate was **repudiated** ("ausgeschlagene Erbschaft" / "succession répudiée" — 100% of
KK notices) and it is in **official liquidation** (rubric KK, authority = Konkursamt /
Office des faillites).

⚠️ **It is NOT a verified "all heirs repudiated" classification.** Art. 573 ZGB makes
all-nearest-heirs repudiation the legal *precondition* for konkursamtliche Liquidation,
so that is a reasonable entailment — but it is an **inference, not a source fact**.
Measured over the corpus: **0 of 15,928** KK notices cite Art. 573, and **0** contain any
"sämtliche / alle Erben / tous les héritiers" statement. Do not present the feed as a
confirmed all-heirs list.

⚠️ **The flag does no discriminating work inside the KK set** (`refusedLegacy` on
15,924/15,928 ≈ 99.97%). Its filtering value is **between categories** — it excludes
testament/inventar/escheat/unknown — not within them. `partial` is **structurally
unobservable** here: a partial ausschlagung never triggers official liquidation, so it is
never published in the KK rubric. That is why no event carries it — not because we tested
and found none.

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
