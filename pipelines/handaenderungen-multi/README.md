# handaenderungen-multi

One canton-parameterized parser for cantonal **Handänderungen** (published property-transfer
notices) → `bronze_ch.transactions_national` (re-LLM) → FDW streamed UPSERT →
`ref.transactions_national` (lamap_db), canton-stamped. Plus a déréliction / sans-maître flag.

**Cantons in scope: LU, SZ.** (VS is a documented NO-GO — see below.)

## Lane separation
- **This session:** `transactions_national` rows for **LU / SZ** only, repo dir `handaenderungen-multi`.
- **SG (paused):** `transactions_national` (SG), publikationen.sg.ch — do not touch.
- **BE (active):** `succession_events` (BE), amtsblattportal.ch — different table, do not touch.

Cantons are disjoint values on the shared table, so no row collision.

## Architecture
```
adapter.discover ─▶ parse (regex → Sonnet fallback) ─▶ ownerless flag ─▶ normalize
                                                                            │
                                    bronze_ch.transactions_national (re-LLM)◀┘
                                                     │ syncToLamap (client-side streamed UPSERT)
                                    ref.transactions_national (lamap_db, canton='LU'|'SZ')
```
- `src/parse.ts` — deterministic regex parser (primary) + Claude `claude-sonnet-5` fallback
  for messy blocks. Grammar: `<sellers> an <buyers>, Nr. <parcel>, <address>, <desc>, <surface|WQ>`.
- `src/ownerless.ts` — `is_ownerless_event` = Aneignung/Okkupation, a public-body party
  (Kanton/Gemeinde/Bürgergemeinde/Korporation/Genossame), or text `herrenlos`/`Dereliktion`/`Eigentumsaufgabe`.
- `src/normalize.ts` — → `bronze_ch.transactions_national` (20 cols). Rich fields
  (grundbuchkreis, parcel_number, ownership_form, quote, is_ownerless_event, per-party domiciles,
  raw_text) → `raw_data` jsonb, mirroring the `fo_fr_ch` (Fribourg) convention. `price` NULL by design.
  Natural key **UNIQUE(source_id, canton)**; `source_id` is a stable hash of
  `(canton, source_organ, parcel_number, buyer_names, publication_date)` ⇒ idempotent UPSERT.
- `src/reconcile.ts` — best-effort `(canton, commune≈grundbuchkreis, parcel_number) → egrid`
  via lamap_db `ref.plots` (direct pg — `ref.*` isn't REST-exposed). Resolved egrid → `raw_data.egrid`
  → the generated `ref.transactions_national.egrid` column. **Join downstream on egrid, never
  no_commune_no_parcelle** (GE-only, `c655f036`).
- `src/load.ts` + `src/lamap-pg.ts` — bronze UPSERT to re-LLM (supabase-js; `bronze_ch` REST-exposed) +
  **client-side streamed UPSERT to lamap_db over a direct pg connection** (`ref.*` is NOT PostgREST-exposed,
  so REST can't write it — this is the FR/VD "streamed UPSERT, never dblink" path; conn string in
  `LAMAP_DB_URI`). Writes BOTH `ref.transactions_national` (canonical, raw_data + generated
  `is_ownerless_event`/`egrid`) and `public.transactions_national_data` (serving twin `ask_lamap` reads,
  lean projection: `source_file→source_system`, `reason→type_transaction`). UPSERT COALESCEs so NULL
  never overwrites; keyed `(source_id, canton)`.

### Load-path DDL (applied to lamap_db, `sql/`)
The load path was **verified live against lamap_db** (`fckdwddgtdbvhzloejni`), not just dry-run:
- `2026-07-16_transactions_national_load_key.sql` — adds `UNIQUE(source_id, canton)` to `ref.transactions_national`
  and `public.transactions_national_data` (0 existing violations, 0 NULL source_id — the ON CONFLICT key did
  not previously exist, so the first live write would have thrown); adds STORED generated columns
  `is_ownerless_event boolean` + `egrid text` over `raw_data` (queryable). `NOTIFY pgrst`.
- `2026-07-16b_transactions_national_id_default.sql` — both `id` columns had no default/identity
  (`ref.id` nullable, `ref` has no PK — its real key is now `(source_id,canton)`; `public._data.id` NOT NULL
  PK). Adds a shared `public.transactions_national_id_seq` default so the streamed UPSERT inserts without
  managing ids. Additive. `NOTIFY pgrst`.

**Grain (stated):** `source_id` is one-per-transaction-record — a stable hash of
`(canton, source_organ, parcel_number, buyer_names, publication_date)`. Verified: existing FR/VD/VS rows are
1 source_id per row; the composite key is `(source_id, canton)` because source_id strings collide across cantons.

**Live verification** (`tests/load.live.test.ts`, run via `verify_load.sh`): insert 4 SZ fixture rows (incl. 1
`is_ownerless_event=true`) → `WHERE is_ownerless_event=true`=1 and `egrid` queryable → re-run stays 4/4/4
(idempotent) → purge to 0/0/0. Writes to a net-new canton and cleans up (isolated per-canton deletes, RESTRICT).
- `src/adapters/{schwyz,lucerne}.ts` — per-source `discover()`. Fixture path works today;
  the **live fetch path is a stub that throws** until a compliant source is wired (see Access).

## Run
```bash
npm test                                        # deterministic parser tests (no network/LLM)
npx tsx run.ts --canton SZ --fixtures ./fixtures # dry run over committed fixtures
npx tsx run.ts --canton SZ --fixtures ./fixtures --load   # + bronze upsert + lamap sync
npx tsx run.ts --canton SZ                       # live — throws (access-gated)
```
Env for live/load: `RE_LLM_SUPABASE_URL`, `RE_LLM_SUPABASE_SERVICE_ROLE_KEY`,
`SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY` (lamap_db), `ANTHROPIC_API_KEY` (LLM fallback),
optional `HANDAENDERUNGEN_MODEL` (default `claude-sonnet-5`).

## Access & compliance — the blocker (recon 2026-07-16)
Recon-first, per the SG lesson: **robots checked BEFORE any scrape; nothing scraped against a Disallow.**
Every Swiss cantonal gazette in scope disallows generic scraping; the compliant path is a
subscription/PDF-Abo or a permitted API. **No open bulk API exists today for LU/SZ.**

### SZ — Schwyz — BUILDABLE, strongest legal case
- **Legal ✅ CONFIRMED open** — Kleine Anfrage KA 16/25: *"Die Aneignung eines im Grundbuch als
  herrenlos bezeichneten Grundstücks ist weder nach Bundes- noch nach kantonalem Recht ausgeschlossen"*;
  SZ has no rule auto-vesting derelinquished land in canton/Bezirk/Gemeinde. A private person
  (Jonas Lauwiner) appropriated ownerless SZ plots, published in the Amtsblatt. Sans-maître is live.
- **Feed:** cantonal *Amtsblatt des Kantons Schwyz* (weekly, issue-numbered), on amtsblatt.sz.ch
  since 1 Jan 2026. **Locate resolved: cantonal**, not Bezirk (Einsiedler Anzeiger etc. carry other content).
- **Access ⛔** robots `Disallow: /`. Compliant path = **"Gesamtausgabe als PDF" Abo** or a
  Handänderungen Filter/Such-Abo (email/PDF, free reader registration) → ingest the delivered PDF.
  Wire in `src/adapters/schwyz.ts` `discover()` live path.
- Baseline plots present (SZ 51,652, keys 100%) ⇒ egrid reconciliation feasible.

### LU — Lucerne — BUILDABLE, durable target = amtsblattportal
- **Legal ✅ open** — EGZGB (SRL 200) has no reversion clause; 2002 LU parliamentary answer on
  herrenlose Grundstücke. Sans-maître is live.
- **Feed:** Luzerner Kantonsblatt (weekly Sat), rubric «Grundstückübertragungen», **no Kaufpreis**.
  Online reader free (the Galledia fee is print-only).
- **Access ⚠️** reader-host (www.kantonsblatt.lu.ch, Galledia) robots **UNVERIFIED at recon** (TLS
  failure) — do NOT scrape until confirmed. **Durable path: LU migrates to amtsblattportal.ch (SECO,
  free, REST API) ~2028** — reuse the BE lane's amtsblattportal ingest. Wire in `src/adapters/lucerne.ts`.
- Baseline plots present (LU 105,146, keys 100%) ⇒ egrid reconciliation feasible.

### VS — Valais — NO-GO (recon note only, no adapter)
- **Legal ❌** LACC (RS 211.1) **Art. 162**: *"Les immeubles sans maître sont la propriété de la
  commune… Ils ne peuvent être occupés par un tiers qu'avec son autorisation."* Ownerless immeubles
  vest in the commune; private occupation needs authorization ⇒ **sans-maître angle dead**.
- **Feed:** Bulletin officiel (bulletinvalaiswallis.ch, crawlable, Crawl-delay 10) has **no confirmed
  Handänderungen rubric**; the owner portal is login-gated + 10/day = not bulk-ingestible. VS's
  earlier `transactions_national` rows came from a one-off Excel import (`import-vs-excel.ts`), not a feed.
- **Verdict:** no build. If a genuine transfer rubric is later confirmed in the crawlable BO, a
  transaction-only feed (no ownerless value) could be reconsidered.

## Deploy
Scheduled workflows aligned to each organ's cadence (LU weekly Sat, SZ weekly) + a one-shot archive
backfill on first run — **enabled once a compliant source is wired** into the adapter live paths.
Until then `.github/workflows/handaenderungen-multi-ci.yml` runs the fixture tests on every push.

## Fixtures
`fixtures/{schwyz,lucerne}/*.json` — `{ pub, blocks[] }`. **Synthetic**, derived from the documented
record grammar (real PDF fixtures require the compliant source). Replace with real samples once access lands.
