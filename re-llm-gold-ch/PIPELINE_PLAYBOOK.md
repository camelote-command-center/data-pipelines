# gold_ch Pipeline Playbook

**Purpose**: canonical recovery & operations guide for the re-LLM `gold_ch` schema and its propagation to consumer DBs (lamap-db, lamap-crm, lamap-lbi).

**Author**: derived from the 2026-05-09 incident-recovery session (post 2026-05-06 parallel-CC destruction event).

**Read this first** when:
- gold_ch matviews are corrupt, missing, or stale
- A consumer DB (lamap-db `ref.*` / `mv_plots`) shows wrong row counts or missing cantons
- You're onboarding a new operator who needs to understand the medallion → consumer flow

---

## 1. Architecture overview

```
bronze_ch.*           ← raw ingest (parsers write here)
   │
   ↓
silver_ch.*           ← cleaned, normalized matviews + reference tables
   │     (ref_communes, cadastral_buildings, cadastral_plots,
   │      market_listing_stats, link_*, listing_active, event_*)
   ↓
gold_ch.core_*        ← business-shaped matviews (8 core objects)
   │     (core_buildings, core_plots, core_plots_ext_ge, core_units,
   │      core_addresses, core_communes, core_cantons, core_filter_options)
   ↓
gold_ch.v_*_full      ← VIEWS adding standard geo columns
   │                    (country_code, admin1_*, admin2_*, admin3_*)
   ↓
gold_ch.sync_full_refresh()  ← TRUNCATE+INSERT via FDW
   │
   ↓
lamap-db ref.*        ← consumer's frozen snapshot
   │
   ↓
public.upsert_mv_plots() / mv_plots ← consumer's denormalized view
   │
   ↓
PostgREST → Lovable frontend
```

**Three consumer DBs** are wired in `gold_ch.run_sync_proc()`:
- `lamap_db_server` / `lamap_db_foreign` / target_db `lamap_db`
- `lamap_crm_server` / `crm_foreign` / target_db `lamap-crm`
- `lamap_lbi_server` / `lbi_foreign` / target_db `lamap-lbi`

Calling `sync_full_refresh()` directly with one set of params syncs **only that one** consumer.

---

## 2. The 8 canonical gold_ch matviews

| # | Matview | Source(s) | ~Rows | Refresh time |
|---|---------|-----------|-------|--------------|
| 1 | `core_buildings` | silver_ch.cadastral_buildings, .cadastral_entrances + ref.cantons | 3.35M | ~30s regular |
| 2 | `core_units` | silver_ch.cadastral_units | 4.96M | ~40s regular |
| 3 | `core_addresses` | silver_ch.cadastral_entrances | 3.43M | ~42s regular |
| 4 | `core_communes` | silver_ch.ref_npa_commune, .market_population, ref.federal_communes (depends on core_transactions/listings/sad for counts) | 2.1K | <1s CONCURRENTLY |
| 5 | `core_cantons` | ref.cantons, gold_ch.core_communes | 26 | <1s CONCURRENTLY |
| 6 | `core_plots` | silver_ch.cadastral_plots, .cadastral_entrances, .link_plot_*, .event_transactions, .listing_active + **gold_ch.core_buildings** | 2.96M | ~3-5min regular |
| 7 | `core_plots_ext_ge` | silver_ch.cadastral_rdppf, .cadastral_ddp, .cadastral_servitudes, .cadastral_densification, .cadastral_ppe, .cadastral_mutations, .cadastral_land_prices + **gold_ch.core_buildings** + bronze_ch.ge_cad_batiments | 73K (GE only) | ~30-60s regular |
| 8 | `core_filter_options` | core_transactions, core_listings, core_sad, core_buildings, core_entities, core_units, core_plots_ext_ge, **v_plots_full** + ref.cantons, ref.permit_type_taxonomy, ref.status_taxonomy, silver_ch.market_listing_stats | ~11.2K | ~30-60s CONCURRENTLY |

**Refresh order matters** — see §4.

### 2.1 Critical column-type contracts (from lamap-db `ref.plots`)

These columns must be `text[]` arrays in `core_plots_ext_ge` and `v_plots_full`. **Using `string_agg(..., ', ')` instead of `array_agg(...)` produces `text` and breaks the FDW INSERT** (canonical 2026-05-09 failure mode):

| Column | Type | Source CTE in core_plots_ext_ge |
|--------|------|---------------------------------|
| `servitude_genres` | text[] | `serv_agg` from cadastral_servitudes.genre |
| `servitude_classes` | text[] | `serv_agg` from cadastral_servitudes.classe |
| `densification_types` | text[] | `densif_agg` from cadastral_densification.densification_type |
| `densification_practice_urls` | text[] | `densif_agg` from cadastral_densification.administrative_practice_url |
| `densification_sectors` | text[] | `densif_agg` from cadastral_densification.sector_name |
| `ddp_numbers` | text[] | `ddp_agg` from cadastral_ddp.no_ddp |
| `ppe_statuses` | text[] | `ppe_agg` from cadastral_ppe.statut (must use `array_agg(DISTINCT statut)`, not `DISTINCT ON ... statut`) |

The `core_filter_options` block for `plot | densification_type` must `unnest(densification_types)` — see `core_filter_options_deparam.sql` block 17.

### 2.2 Core_plots silver-redesign bridge

Silver redesigned `cadastral_buildings` from building-level to parcel-level. **gold_ch.core_buildings is now the bridge** — used in core_plots' 4 CTEs (`bldg_summary`, `first_addr`, `addr_agg`, `bldg_json`) instead of silver directly. This creates a hard refresh dependency: **core_buildings must refresh before core_plots**.

---

## 3. Two views over the matviews

| View | Wraps | Adds |
|------|-------|------|
| `gold_ch.v_plots_full` | core_plots LEFT JOIN core_plots_ext_ge LEFT JOIN ref_commune_appartenance_ge LEFT JOIN ref_communes | country_code, admin1_*, admin2_*, admin3_* (8 standard geo columns) |
| `gold_ch.v_filter_options_full` | core_filter_options UNION ALL static_filter_options | static config rows (admin/crm/lbi entries with NULL counts) |

There are 7 other v_*_full views (units, addresses, buildings, communes, cantons, sad, transactions) that wrap their matviews + add the geo standard columns. See `sync_registry`.

**Convention**: `admin1_code` = uppercase canton_code from ref_communes JOIN. **Lower-cased `admin1_code` is a bug**.

---

## 4. Refresh order & cron

### 4.1 Dependency-correct refresh order

```
Wave 1 (no interdeps):
  core_buildings, core_units, core_addresses, core_communes, core_cantons

Wave 2 (depends on core_buildings from Wave 1):
  core_plots → THEN core_plots_ext_ge   [order matters within wave]

Wave 3 (depends on Wave 1 + Wave 2 + daily silver matviews):
  core_filter_options
```

### 4.2 REFRESH method per matview

| Matview | Method | Why |
|---------|--------|-----|
| core_buildings | `REFRESH MATERIALIZED VIEW` (regular) | CONCURRENTLY DELETE phase times out at 600s on 3M+ rows |
| core_units | regular | Same |
| core_addresses | regular | Same |
| core_plots | regular | Same — historical CONCURRENTLY took 37min, regular takes ~3min |
| core_plots_ext_ge | regular | 73K rows; CONCURRENTLY works but adds no value |
| core_communes | `REFRESH ... CONCURRENTLY` | <1s, frequently consumed |
| core_cantons | CONCURRENTLY | <1s |
| core_filter_options | CONCURRENTLY | ~30s, has unique index on (product, domain, filter_type, filter_value, COALESCE(parent_value, '')) |

### 4.3 Cron schedule

| Time UTC | Day | Job | Purpose |
|----------|-----|-----|---------|
| **01:00** | **daily** | `gold_ch.refresh_gold_warehouse()` | All 8 matviews, Waves 1+2+3 |
| 02:00 | 1st of month | `run_sync_proc('monthly')` | Full-refresh sync of monthly entries |
| 03:00 | Sunday | `run_sync_proc('weekly')` | Full-refresh sync of weekly entries |
| 04:30 | Mon-Fri | `gold_ch.refresh_daily_matviews()` | silver event/link + core_listings/sad/transactions |
| 05:00 | Mon-Fri | `run_sync_proc('daily')` | Delta sync of daily entries |

**Disabled (replaced 2026-05-09)**: jobs 13 (`refresh_core_plots`) and 14 (`refresh_core_plots_ext_ge`) — both used CONCURRENTLY and timed out.

`refresh_gold_warehouse()` writes to `gold_ch.refresh_manifest` after each REFRESH (last_refresh_at, last_refresh_duration_s, last_refresh_status, last_row_count) for monitoring continuity.

---

## 5. Sync registry

`gold_ch.sync_registry` maps each `v_*_full` view to a target `ref.<table>` on every consumer DB. Schema:

| Column | Notes |
|--------|-------|
| source_schema | `gold_ch`, `silver_ch`, or `knowledge_ch` |
| source_table | The view name |
| target_table | The `ref.*` table on consumers |
| pk_column | NULL for full_refresh, set for delta |
| sync_mode | `full_refresh` or `delta` |
| frequency | `daily` / `weekly` / `monthly` |
| enabled | bool |
| delta_column | typically `updated_at` |
| column_list | text[] — optional explicit column shape for FDW-safe inserts |

`run_sync_proc(p_frequency)` iterates the registry and calls `sync_full_refresh()` (or `sync_delta()`) once per (entry × consumer) pair.

### 5.1 sync_full_refresh()

**Signature** (7-param, preferred — has overload-collision with 6-param so always pass `p_column_list := NULL` when calling directly):

```sql
SELECT gold_ch.sync_full_refresh(
  p_source_schema  => 'gold_ch',
  p_source_table   => 'v_plots_full',
  p_target_table   => 'plots',
  p_target_server  => 'lamap_db_server',
  p_foreign_schema => 'lamap_db_foreign',
  p_target_db      => 'lamap_db',
  p_column_list    => NULL  -- derives from pg_attribute, schema-drift safe
);
```

**Critical behavior**: TRUNCATE on the target runs via `dblink_exec` in its **own committed transaction** before the FDW INSERT. **If the INSERT fails, the target is left empty** — there is no rollback path. Always test FDW-side type compatibility before triggering a full sync (see §6).

### 5.2 Single-target vs all-targets

`run_sync_proc('daily')` syncs to all 3 consumers. To sync to **one consumer only** (e.g. lamap-db), call `sync_full_refresh()` directly with that consumer's params. Phase 4 of the 2026-05-09 recovery used this pattern — CRM and LBI were intentionally untouched.

---

## 6. Recovery procedure (the 2026-05-09 playbook, generalized)

Use this when gold_ch matviews are corrupt or missing, e.g. after a parallel-CC incident.

### 6.1 Pre-flight

1. Snapshot what currently exists:
   ```sql
   SELECT schemaname, matviewname FROM pg_matviews WHERE schemaname='gold_ch';
   SELECT schemaname, viewname    FROM pg_views    WHERE schemaname='gold_ch';
   ```
2. Capture canonical SQL from `pg_stat_statements` BEFORE any restart (it clears on restart):
   ```sql
   SELECT query FROM pg_stat_statements WHERE query ILIKE 'CREATE MATERIALIZED VIEW gold_ch.%';
   ```
   Save outputs to `canonical_creates_<DATE>/`. **5 of the 8 matviews are parameterized** (literals replaced with `$1`, `$2`, …) — they need de-parameterization before execution.
3. Don't trust earlier "silver redesign" theories — verify column names directly:
   ```sql
   SELECT column_name FROM information_schema.columns
   WHERE table_schema='silver_ch' AND table_name='cadastral_buildings';
   ```

### 6.2 Build order (smallest, most-depended-upon first)

1. **core_units** (no gold deps)
2. **core_addresses** (no gold deps)
3. **core_communes** (depends on core_transactions/listings/sad — these are in the daily cron, refresh them first if stale)
4. **core_cantons** (depends on core_communes)
5. **core_buildings** (no gold deps; bridge for core_plots)
6. **core_plots** (depends on core_buildings)
7. **core_plots_ext_ge** (depends on core_buildings)
8. **core_filter_options** (depends on all of the above + v_plots_full + daily matviews)

For each, enforce these hard rules:
- **No `DROP CASCADE`**, no modifying silver_ch.
- One object per iteration. Surface CREATE SQL for review before execute.
- After CREATE, verify row counts match expectations.
- Test `REFRESH MATERIALIZED VIEW` (regular). If it fails, fix before moving on.
- After every successful build, also recreate the matching `v_*_full` view + `CREATE UNIQUE INDEX` if it has one.

### 6.3 De-parameterization tips

`pg_stat_statements` normalizes literals to `$N`. To recover:
- **Boolean flags**: usually `is_active = true`
- **EXTRACT**: `EXTRACT($N FROM date_col)` → `EXTRACT(year FROM date_col)`
- **Domain/filter_type strings**: cross-reference the consumer's existing taxonomy (e.g. `lamap-db ref.filter_options` for filter blocks)
- **CASE WHEN labels**: query the live data — if filter_value = filter_label, identity mapping; if `Rent`/`Sale` style, `initcap()`
- **Empty-string filters**: `WHERE col <> ''` (very common pattern)

### 6.4 Type-safety check before sync

**Always** verify FDW type compatibility before running `sync_full_refresh()`:

```sql
-- on gold (source)
SELECT a.attname, format_type(a.atttypid, a.atttypmod)
FROM pg_attribute a JOIN pg_class c ON a.attrelid=c.oid
JOIN pg_namespace n ON c.relnamespace=n.oid
WHERE n.nspname='gold_ch' AND c.relname='<source_view>'
ORDER BY a.attnum;

-- on consumer (target)
SELECT column_name, data_type, udt_name
FROM information_schema.columns
WHERE table_schema='ref' AND table_name='<target>'
ORDER BY ordinal_position;
```

Compare. **`text` vs `text[]` mismatches are the canonical failure** (see §2.1).

### 6.5 Sync sequence (small first)

Order to minimize blast radius if anything still goes wrong:
1. cantons (~26 rows)
2. communes (~2K)
3. filter_options (~11K)
4. units (~5M)
5. addresses (~3.4M)
6. buildings (~3.35M)
7. plots (~3M, heaviest — jsonb columns + geometry)

After each: verify lamap-db `ref.<table>` row count matches gold source.

**Hard rule**: SEQUENTIAL ONLY. Pooler-drop bug 6206ee23 has 5+ recurrences from external Python parallel sync. Keep this in-database, one at a time.

### 6.6 mv_plots repaint on consumer

After sync, `public.upsert_mv_plots()` is the canonical path. **But** when ALL plot rows are "new" (e.g. after a TRUNCATE+INSERT with reset `updated_at`), the function builds a 3M-element `v_changed_egrids` array and the single huge INSERT exceeds the proxy/pooler idle window (~3min observed) before the 15-min statement_timeout.

**Workaround pattern** (2026-05-09 Phase 5):

1. Create a temporary procedure that mirrors `upsert_mv_plots()`'s INSERT block but **filters by `canton_code`** instead of by `egrid = ANY(v_changed_egrids)`.
2. Use `COMMIT` inside the procedure (PG11+ supports COMMIT in PL/pgSQL).
3. Iterate cantons ascending size order so problems surface on small ones first.
4. After all 21 cantons land: `refresh_mv_plots_default_count_cache()` + `NOTIFY pgrst, 'reload schema'`.
5. **Drop the temp procedure** — keep `upsert_mv_plots()` canonical.

The procedure template lives at `canonical_creates_2026-05-08/repaint_helper.sql` (commit-amended after the 2026-05-09 session).

**Footnote — pooler drops on the LAST canton are normal.** The 2026-05-09 run lost its psql client connection during BE (the largest canton) but BE's data had already committed inside the procedure. psql exits with code 2 because its socket died, but server-side state is correct. **Always verify the final canton's row count directly** via a fresh psql connection before declaring failure — don't trust the loop's exit code alone.

### 6.7 Verification matrix

After any rebuild + sync:

| Check | Query (lamap-db) | Expected |
|-------|-------|----------|
| Total rows | `SELECT count(*) FROM ref.plots` | matches gold v_plots_full count |
| All cantons | `SELECT count(DISTINCT canton_code) FROM ref.plots` | 21 (CH cantons with cadastral coverage) |
| VD/NE/JU/AG present | `SELECT canton_code, count(*) FROM ref.plots GROUP BY 1` | all 4 with non-zero counts |
| filter_options non-empty | `SELECT count(*) FROM ref.filter_options` | ~11K (459 static + ~10.7K dynamic) |
| building/destination has counts | `SELECT count FROM ref.filter_options WHERE domain='building' AND filter_type='destination' LIMIT 1` | non-NULL count |
| mv_plots matches ref.plots | `SELECT count(*) FROM mv_plots` vs `SELECT count(*) FROM ref.plots` | within ~10 rows (slight drift from orphans is expected — see §7) |

---

## 7. Known issues & open bugs

| ID | Severity | Summary |
|----|----------|---------|
| 804817a3-…ad1eb | P3 | silver_ch.ref_communes coverage gap → 495K plots have NULL admin1_code (mostly Suisse alémanique) |
| 99a0188a-…cdeb0a | P3 | mv_plots retains orphan rows when upstream egrids disappear (`upsert_mv_plots` doesn't delete) |
| 04bbaab1-…36cb3b | P? | gold_ch.core_buildings_ext_ge is missing — referenced in canonical v_buildings_full but not rebuilt |
| 8dc6fe67-…61e95539 | P? | core_cantons uses `ref.cantons.name_fr` while core_communes uses `name` — language inconsistency |

Search `public.bugs` on `dxugbpeacnorjunpljih` for current state.

---

## 8. File map

```
data-pipelines/re-llm-gold-ch/
├── PIPELINE_PLAYBOOK.md                          ← this file
└── canonical_creates_2026-05-08/
    ├── README.md                                 ← origin notes for the captured SQL
    ├── core_buildings.sql                        ← parameterized, 45 refs
    ├── core_cantons.sql                          ← parameterized
    ├── core_communes.sql                         ← parameterized
    ├── core_filter_options.sql                   ← parameterized, 240 refs (deprecated, use _deparam)
    ├── core_filter_options_deparam.sql           ← USE THIS — 36 blocks de-parameterized
    ├── core_plots.sql                            ← non-parameterized
    ├── core_plots_ext_ge.sql                     ← original (string_agg version, do not use)
    ├── core_plots_ext_ge_arrays.sql              ← USE THIS — array_agg version, type-correct
    ├── core_plots_raw.sql                        ← raw pg_stat dump, formatting artifacts
    ├── core_units.sql                            ← non-parameterized
    ├── v_addresses_full.sql ... v_units_full.sql ← view contracts
```

---

## 9. Hard rules (bake into every recovery)

1. **No `DROP CASCADE`**, no modifying `silver_ch` objects.
2. **One object per iteration** — surface SQL before execute, verify row counts after.
3. **Sequential sync only** — no parallel `sync_full_refresh()` calls. Pooler-drop bug 6206ee23.
4. **Single-target sync first** (lamap-db only) before fanning out to CRM/LBI.
5. **Type-check before TRUNCATE** — `text` vs `text[]` mismatches leave the consumer EMPTY because TRUNCATE commits before the INSERT runs.
6. **Heavy matviews use regular `REFRESH`**, not CONCURRENTLY. CONCURRENTLY's DELETE phase doesn't fit in 600s on multi-million row sets.
7. **`v_changed_egrids` arrays don't scale to 3M elements.** Chunk by canton (or by hash) when the entire dataset is "new".
8. **Always write to `refresh_manifest`** in any new refresh function so monitoring continuity is preserved.
9. **`admin1_code` is uppercase**. Lower-cased = bug.
10. **Only delete from `mv_plots` deliberately** — `upsert_mv_plots()` doesn't sync deletions.

---

*Last updated: 2026-05-09 — initial canonical playbook from gold_ch incident-recovery session.*
