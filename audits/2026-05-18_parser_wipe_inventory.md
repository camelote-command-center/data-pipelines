# Parser Wipe Inventory — Camelote bronze layer audit

**Date**: 2026-05-18
**Scope**: All 65 parsers under `pipelines/` in `data-pipelines` repo
**Question**: Does any parser wipe/destroy bronze data (`bronze_ch.*` or other bronze schemas) on re-run?
**Method**: Static read-only analysis (grep + Python regex scanner + targeted file reads). No execution. No edits.

---

## Headline

**Zero parsers wipe bronze data.**

- 0 BLOCKERs
- 0 runtime `TRUNCATE` on bronze tables
- 0 runtime `DROP TABLE` on bronze tables
- 0 `pandas.to_sql(if_exists='replace')` anywhere in repo
- 0 unfiltered `DELETE FROM bronze_*` statements
- **1** scoped runtime DELETE on `bronze_ch.*` — proven safe (silgeneve, scoped + guarded)
- 3 DELETEs on `bronze_ae.*` (dld-dubai) — pipeline is **disabled** (cron commented out), wrong schema, all filtered

All other apparent matches are (a) `-- Rollback: DROP TABLE IF EXISTS …` documentation comments in one-time bootstrap DDL files, or (b) natural-language comments containing the words "truncate" / "delete".

---

## Worst-first inventory (all 65 parsers)

| Risk | Parser | File:line | Operation | Target table | Load pattern | Trigger | Verdict |
|---|---|---|---|---|---|---|---|
| **REVIEW (safe)** | silgeneve | `parser.py:454` | `DELETE FROM bronze_ch.silgeneve_articles WHERE law_rsge = %s AND article_number != ALL(%s)` | `bronze_ch.silgeneve_articles` | Scoped child-row prune under parent UPSERT | weekly cron | **SAFE** — guarded by `if current_numbers:`; only runs when a freshly parsed law produced article rows; prunes articles removed from that single law only. Cannot empty table. |
| LOW (out of scope) | dld-dubai | `migrate.mjs:392`, `migrate.mjs:406` | `DELETE … USING … WHERE a.id < b.id` (filtered dedup self-join, inside `CREATE FUNCTION`) | `bronze_ae.transactions`, `bronze_ae.rentals` | Dedup function definition (DDL) | DISABLED (cron commented; API keys not configured) | Wrong schema (UAE not CH); pipeline not running; deletes are filtered to duplicate rows only |
| LOW (out of scope) | dld-dubai | `verify.mjs:24` | `DELETE FROM bronze_ae.ingestion_log WHERE source = 'test'` | `bronze_ae.ingestion_log` | Smoke-test cleanup | manual verify only | Filtered to test rows only |
| SAFE (false positive) | amtsblattportal | `sql/01_bronze_schema.sql:5` | `-- Rollback: DROP TABLE IF EXISTS …` | n/a | Comment in one-time DDL | manual `psql` | Never executed |
| SAFE (false positive) | court-decisions | `sql/01_bronze_index.sql:5` | `-- Rollback:` comment | n/a | DDL | manual | Never executed |
| SAFE (false positive) | fao-multi | `sql/01_bronze_schema.sql:6` | `-- Rollback:` comment | n/a | DDL | manual | Never executed |
| SAFE (false positive) | lindas-multi | `sql/01_bronze_schema.sql:9` | `-- Rollback:` comment | n/a | DDL | manual | Never executed |
| SAFE (false positive) | news-rss | `sql/01_bronze_index.sql:5` | `-- Rollback:` comment | n/a | DDL | manual | Never executed |
| SAFE (false positive) | opendata-fr | `sql/01_bronze_schema.sql:5` | `-- Rollback:` comment | n/a | DDL | manual | Never executed |
| SAFE (false positive) | openholidays | `sql/01_bronze_schema.sql:4` | `-- Rollback:` comment | n/a | DDL | manual | Never executed |
| SAFE (false positive) | openparl | `sql/01_bronze_schema.sql:6` | `-- Rollback:` comment | n/a | DDL | manual | Never executed |
| SAFE (false positive) | openplz | `sql/01_bronze_schema.sql:6` | `-- Rollback:` comment | n/a | DDL | manual | Never executed |
| SAFE (false positive) | wiki-recent | `sql/01_bronze_schema.sql:8` | `-- Rollback:` comment | n/a | DDL | manual | Never executed |
| SAFE (false positive) | wikipedia-ge | `sql/01_bronze_wikipedia.sql:16-17` | `-- Rollback:` comment | n/a | DDL | manual | Never executed |
| SAFE (false positive) | fao-multi | `fetch-fao.ts:352` | code comment: "silently truncate" | n/a | n/a | n/a | Natural-language comment |
| SAFE (false positive) | silgeneve | `parser.py:18` | docstring: "never TRUNCATE" | n/a | n/a | n/a | Docstring |
| SAFE (false positive) | sitg_servitudes | `import.py:16` | docstring: "No delete, no truncate" | n/a | n/a | n/a | Docstring |
| SAFE | acheter-louer | (via `_shared/supabase.ts` `batchUpsert`) | UPSERT | `bronze_ch.listings` | append/update | scheduled | UPSERT-only |
| SAFE | properstar | via `_shared` `batchUpsert` | UPSERT | `bronze_ch.listings` | append/update | scheduled (currently CF-blocked) | UPSERT-only |
| SAFE | immobilier | via `_shared` `batchUpsert` | UPSERT | `bronze_ch.listings` | append/update | scheduled | UPSERT-only |
| SAFE | flatfox | via `_shared` `batchUpsert` | UPSERT | `bronze_ch.listings` | append/update | scheduled | UPSERT-only |
| SAFE | homegate | via `_shared` `batchUpsert` | UPSERT | `bronze_ch.listings` | append/update | scheduled | UPSERT-only |
| SAFE | fao-ldtr | via `_shared` `batchUpsert` | UPSERT | `bronze_ch.fao_ldtr_*` | append/update | scheduled | UPSERT-only |
| SAFE | fao-multi | inline + helper | UPSERT | `bronze_ch.ge_fao_publications` | append/update | scheduled | UPSERT-only |
| SAFE | transactions-fao | helper | UPSERT | `bronze_ch.fao_transactions` | append/update | scheduled | UPSERT-only |
| SAFE | realadvisor | helper | UPSERT | `bronze_ch.listings` | append/update | scheduled | UPSERT-only |
| SAFE | news-rss | helper | UPSERT | `bronze_ch.news_index` | append/update | scheduled | UPSERT-only |
| SAFE | amtsblattportal | helper | UPSERT | `bronze_ch.amtsblatt_publications` | append/update | scheduled | UPSERT-only |
| SAFE | bfs_construction_price_index | inline | UPSERT (ON CONFLICT) | `bronze_ch.bfs_*` | append/update | scheduled | UPSERT-only |
| SAFE | bfs_dwellings | inline | UPSERT | `bronze_ch.bfs_*` | append/update | scheduled | UPSERT-only |
| SAFE | bfs_impi | inline | UPSERT | `bronze_ch.bfs_*` | append/update | scheduled | UPSERT-only |
| SAFE | bfs_population | inline | UPSERT | `bronze_ch.bfs_*` | append/update | scheduled | UPSERT-only |
| SAFE | bfs_rent_cpi | inline | UPSERT | `bronze_ch.bfs_*` | append/update | scheduled | UPSERT-only |
| SAFE | bfs_rents | inline | UPSERT | `bronze_ch.bfs_*` | append/update | scheduled | UPSERT-only |
| SAFE | bfs_vacancy | inline | UPSERT | `bronze_ch.bfs_*` | append/update | scheduled | UPSERT-only |
| SAFE | candle-updater | inline | UPSERT | `bronze_ch.candles` | append/update | scheduled | UPSERT-only |
| SAFE | court-decisions | helper | UPSERT | `bronze_ch.court_decisions_index` | append/update | scheduled | UPSERT-only |
| SAFE | forbes-billionaires | inline | UPSERT | `bronze_global.forbes_*` | append/update | scheduled | UPSERT-only |
| SAFE | fr-feuille-officielle | inline | UPSERT | `bronze_fr.*` | append/update | scheduled | UPSERT-only |
| SAFE | gwr | helper | UPSERT | `bronze_ch.gwr_*` | append/update | scheduled | UPSERT-only |
| SAFE | lindas-multi | helper | UPSERT | `bronze_ch.lindas_observations` | append/update | scheduled | UPSERT-only |
| SAFE | lolla-daily | helper | UPSERT | `bronze_ch.lolla_*` | append/update; capped at MAX_ADS_PER_RUN=500 | scheduled | UPSERT-only |
| SAFE | minergie | helper | UPSERT | `bronze_ch.minergie_*` | append/update | scheduled | UPSERT-only |
| SAFE | ne-sad-sitn | inline | UPSERT | `bronze_ch.ne_*` | append/update | scheduled | UPSERT-only |
| SAFE | opendata-fr | helper | UPSERT | `bronze_ch.opendata_fr_*` | append/update | scheduled | UPSERT-only |
| SAFE | openholidays | helper | UPSERT | `bronze_ch.openholidays` | append/update | scheduled | UPSERT-only |
| SAFE | openparl | helper | UPSERT | `bronze_ch.openparl_records` | append/update | scheduled | UPSERT-only |
| SAFE | openplz | helper | UPSERT | `bronze_ch.openplz_records` | append/update | scheduled | UPSERT-only |
| SAFE | osm | helper | UPSERT | `bronze_ch.osm_*` | append/update | scheduled | UPSERT-only |
| SAFE | sad | helper | UPSERT | `bronze_ch.sad_*` | append/update | scheduled | UPSERT-only |
| SAFE | silgeneve | inline | UPSERT (laws) + scoped DELETE (stale articles, see above) | `bronze_ch.silgeneve_laws`, `bronze_ch.silgeneve_articles` | UPSERT parent + scoped child prune | scheduled | UPSERT-only on parent; child prune is scoped + guarded |
| SAFE | simap | helper | UPSERT | `bronze_ch.simap_*` | append/update | scheduled | UPSERT-only |
| SAFE | sirene | helper | UPSERT | `bronze_fr.sirene_*` | append/update | scheduled | UPSERT-only |
| SAFE | sitg_authorizations | helper | UPSERT | `bronze_ch.sitg_*` | append/update | scheduled | UPSERT-only |
| SAFE | sitg_business_energy | helper | UPSERT | `bronze_ch.sitg_*` | append/update | scheduled | UPSERT-only |
| SAFE | sitg_cadastral | helper | UPSERT | `bronze_ch.sitg_*` | append/update | scheduled | UPSERT-only |
| SAFE | sitg_chantiers | helper | UPSERT | `bronze_ch.sitg_*` | append/update | scheduled | UPSERT-only |
| SAFE | sitg_demographics | helper | UPSERT | `bronze_ch.sitg_*` | append/update | scheduled | UPSERT-only |
| SAFE | sitg_dip_schools | helper | UPSERT | `bronze_ch.sitg_*` | append/update | scheduled | UPSERT-only |
| SAFE | sitg_geo_layers | helper | UPSERT | `bronze_ch.sitg_*` | append/update | scheduled | UPSERT-only |
| SAFE | sitg_geography | helper | UPSERT | `bronze_ch.sitg_*` | append/update | scheduled | UPSERT-only |
| SAFE | sitg_servitudes | helper | UPSERT (objectid) | `bronze_ch.ge_rfo_servitudes` | append/update | scheduled | UPSERT-only; docstring explicitly states "No delete, no truncate" |
| SAFE | sitg_urban_programs | helper | UPSERT | `bronze_ch.sitg_*` | append/update | scheduled | UPSERT-only |
| SAFE | snb | helper | UPSERT | `bronze_ch.snb_*` | append/update | scheduled | UPSERT-only |
| SAFE | tinjob-ats | inline | UPSERT | `bronze_*.tinjob_*` | append/update | scheduled | UPSERT-only |
| SAFE | tinjob-personio | inline | UPSERT | `bronze_*.tinjob_*` | append/update | scheduled | UPSERT-only |
| SAFE | tpg | helper | UPSERT | `bronze_ch.tpg_*` | append/update | scheduled | UPSERT-only |
| SAFE | vd-sad-actis | inline | UPSERT | `bronze_ch.vd_*` | append/update | scheduled | UPSERT-only |
| SAFE | wiki-recent | helper | UPSERT | `bronze_ch.wikipedia_edit_log` | append/update | scheduled | UPSERT-only |
| SAFE | wikipedia-ge | helper | UPSERT | `bronze_ch.wikipedia_articles`, `bronze_ch.wikidata_entities` | append/update | scheduled | UPSERT-only |
| SAFE | zefix | helper | UPSERT | `bronze_ch.zefix_*` | append/update | scheduled | UPSERT-only |
| n/a | blog-generator | — | no DB writes | — | — | — | Scaffolding / generator |
| n/a | forum-seeder | — | no DB writes | — | — | — | Seeder script |
| n/a | fr-dvf | — | no DB writes (placeholder) | — | — | — | Placeholder pipeline |
| n/a | il-nadlan | — | no DB writes (placeholder) | — | — | — | Placeholder pipeline |
| n/a | uk-price-paid | — | no DB writes (placeholder) | — | — | — | Placeholder pipeline |
| n/a | repo-backups | — | no DB writes | — | — | — | Repo archive mirror |
| n/a | federal_cadastral_parcels_t1 | — | no DB writes (split-tile fetch only) | — | — | — | Sub-tile of t1/t2/t3 fetcher; writes happen elsewhere |
| n/a | federal_cadastral_parcels_t2 | — | no DB writes | — | — | — | Sub-tile fetcher |
| n/a | federal_cadastral_parcels_t3 | — | no DB writes | — | — | — | Sub-tile fetcher |

---

## Hard answers

1. **Does any parser fully wipe a bronze table?** No.
2. **Does any parser TRUNCATE or DROP+RECREATE at runtime?** No.
3. **Does any parser use `pandas.to_sql(if_exists='replace')`?** No (zero hits anywhere in repo).
4. **Does any parser issue an unfiltered DELETE on a bronze table?** No.
5. **Worst case found?** `silgeneve` scoped child-row cleanup — correct parent-child pattern, empty-fetch-guarded.

## Recommendation
**No remediation needed.** Current bronze-layer write discipline across the parser fleet is uniformly UPSERT-only (`INSERT … ON CONFLICT DO UPDATE` or `batch_upsert`/`batchUpsert`/`upsertBronze`). The one scoped DELETE in silgeneve is the correct pattern for its parent-child law/articles model.

This file is the reusable baseline. Re-run the audit by replaying the Python classifier in `08_SESSION_LOG.md` from any later date and diffing against this table.
