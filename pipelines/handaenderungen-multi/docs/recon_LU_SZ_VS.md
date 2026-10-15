# Recon — Handänderungen LU / SZ / VS (repo: handaenderungen-multi)

Date 2026-07-16 · Claude Code (Opus 4.8). Recon-first per SG lesson: **all robots checked BEFORE any scrape; nothing scraped against a Disallow.**

## Shared target (coordinated w/ SG lane)
- re-LLM `bronze_ch.transactions_national` (20 cols) → FDW streamed UPSERT → lamap_db `ref.transactions_national`, `canton` stamp. Triplet: `public.transactions_national_data` + `ref.transactions_national` + `ask_lamap.transactions_national` (view).
- Disjoint canton values (LU/SZ/VS) vs SG (paused) vs BE (`succession_events`, different table — untouched). No row collision.
- Rich fields (seller/buyer_domicile[], ownership_form ME/GE/StWE/BR/SR, quote/StWE-WQ, parcel_number, grundbuchkreis/source_organ, is_ownerless_event) → `raw_data` jsonb. `price` NULL by source design. Same convention as FR `fo_fr_ch` sibling.
- **Baseline federal plots PRESENT for all 3** (lamap_db ref.plots, keys 100% populated): LU 105,146 · SZ 51,652 · VS 614,372; egrid + parcel_number + commune_name + commune_bfs all populated ⇒ (parcel_number+commune)→egrid reconciliation feasible. (Do NOT use no_commune_no_parcelle — GE-only, unreliable non-GE.)

## LU — Lucerne  →  BUILDABLE (durable target = amtsblattportal), access to confirm
- **Legal ✅ OPEN.** EGZGB SRL Nr. 200 has NO reversion clause (unlike FR/BS/GL); 2002 LU parliamentary answer on herrenlose Grundstücke. Sans-maître holds. (Edge: land newly formed from water/accretion → canton; irrelevant to normal dérélictions.)
- **Feed:** Luzerner Kantonsblatt, weekly (Sat), rubric «Grundstückübertragungen» / «Handänderungen von luzernischen Grundstücken». **No Kaufpreis** (price NULL). Online reader free to read (Galledia produces print; print sub Fr.102/yr is print-only).
- **Access ⚠️ UNCONFIRMED.** `kantonsblatt.lu.ch` (no robots.txt of its own) 301-redirects to the lu.ch info page. Real reader host `www.kantonsblatt.lu.ch` (Galledia platform) failed TLS from sandbox — **robots posture of the actual reader NOT verified; must check before scraping.** Given SG/SZ/amtsblattportal all Disallow generic bots, do not assume open.
- **Strategic:** LU migrates to **amtsblattportal.ch** (SECO, free for readers, **REST API** import/export) from ~2028. Build the LU adapter shaped for amtsblattportal (reuse BE lane's ingest layer when available); interim = free reader IF robots permits, else subscription/consent.

## SZ — Schwyz  →  BUILDABLE, strongest legal case; access = PDF subscription
- **Legal ✅ CONFIRMED OPEN (authoritative).** Kleine Anfrage KA 16/25 (Sicherheitsdept.): *"Die Aneignung eines im Grundbuch als herrenlos bezeichneten Grundstücks ist weder nach Bundesrecht noch nach kantonalem Recht ausgeschlossen"*; SZ has NO rule auto-vesting derelinquished land in Kanton/Bezirk/Gemeinde. A private person (Jonas Lauwiner) appropriated ownerless SZ plots, published in the Amtsblatt. SZ EGZGB (SRSZ 210.100) — no reversion clause (grep confirms). **Sans-maître fully alive.**
- **Feed:** Handänderungen/Aneignungen in the **cantonal** «Amtsblatt des Kantons Schwyz» (weekly, issue-numbered, e.g. "Ausgabe Nr. 12"). Ownership forms ME/GE/StWE/BR/SR per Verordnung über die Veröffentlichung von Eigentumsübertragungen 1993. **Locate resolved: cantonal**, not Bezirk (Bezirk organs like Einsiedler Anzeiger carry other content). Confirm whether Kaufpreis published (likely not).
- **Access ⛔ robots `Disallow: /`** on amtsblatt.sz.ch (digital since 1 Jan 2026). Free to read + full-text search in-browser, but **no scraping, no RSS/API.** Compliant path per the platform reader-guide: **"Gesamtausgabe als PDF abonnieren"** (scheduled full-edition PDF) or a **Filter/Such-Abo** on "Handänderungen" (email intervals). Requires a (free) reader registration — user action. Ingest the delivered PDF/email.

## VS — Valais  →  NO-GO (recon note only)
- **Legal ❌ sans-maître DEAD.** LACC (RS 211.1) **Art. 162**: *"Les immeubles sans maître sont la propriété de la commune sur le territoire de laquelle ils se trouvent. Ils ne peuvent être occupés par un tiers qu'avec son autorisation."* Ownerless immeubles → commune property; occupation needs commune authorization ⇒ private appropriation excluded. (As the brief predicted.)
- **Feed:** Bulletin officiel `bo-vs.ch` → `bulletinvalaiswallis.ch` (crawlable: robots `Crawl-delay: 10`, no Disallow; electronic since 1 Mar 2023, daily, free, FR/DE). BUT **no confirmed name-by-name Handänderungen / transfer rubric** — evidence weak/ambiguous (search conflated it with droits-de-mutation tax rules). VS's real property channel is the **owner portal** (AGOV/SwissID login, 10 searches/day, serial-protected) = verify-one-plot, NOT bulk-ingestible.
- **Verdict:** No build. Sans-maître angle gone; transaction-feed value unconfirmed and probably absent from the free BO. Residual: IF a genuine transfer rubric is later found in the crawlable BO, a transaction-only feed (no ownerless value) could be reconsidered — not now.

## The recurring wall (SG → LU → SZ)
Swiss cantonal gazettes broadly **Disallow generic scraping**; compliant access = an **email/PDF subscription** (SG, SZ) or a **future/official API** (amtsblattportal, LU 2028). None offers an open bulk API *today* for LU/SZ. ⇒ The parser core + adapters can be built now against fixtures, but **live ingest is access-gated pending subscriptions/consent** (user action), same as SG.

## Build recommendation
1. **SZ first** on merit (legally strongest + confirmed cantonal feed + ownership-form data) — access via Gesamtausgabe-PDF Abo.
2. **LU** shaped for amtsblattportal (durable), interim via free reader if its robots permits, else subscription.
3. **VS: no build** — recon note delivered; go = no.
4. Shared canton-parameterized core + per-source adapter; parse w/ Sonnet (reuse transactions-fao); fixtures + regression per source; dedupe `(canton, source_organ, parcel_number, buyer_names, publication_date)`, UPSERT additive.
