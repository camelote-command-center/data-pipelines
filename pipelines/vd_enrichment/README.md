# vd_enrichment — VD/Lausanne canton-VD + federal enrichment

19 datasets ingesting into `re-llm` `bronze_ch.*`, feeding 10 new `_vd` silver matviews +
3 national silver matviews + 1 new `gold_ch.core_plots_ext_vd` matview.

## File layout

```
pipelines/vd_enrichment/
├── README.md                       ← this file
├── PARSERS_SKELETON.md             ← how to write the 18 sister parsers
├── discovery_vd.json               ← discovery artifact (frozen 2026-05-21)
├── v2_attr_mapping.json            ← per-attribute mapping, source-of-truth
├── camelote_datasets_seed.sql      ← 19 registration rows for camelote_data
├── deploy.sh                       ← idempotent VPS deployment
├── vps1.parsers.txt                ← cadastre group (6 parsers)
├── vps2.parsers.txt                ← amenagement group (10 parsers)
├── vps3.parsers.txt                ← federal+energy group (2 parsers, +1 quarterly)
├── _shared/
│   ├── arcgis_query.py             ← canton-VD ArcGIS REST iterator
│   ├── wfs_query.py                ← Lausanne WFS iterator
│   ├── federal_query.py            ← GeoAdmin api3 iterator
│   └── upsert.py                   ← bronze UPSERT + soft-delete
├── vd_batiment_rcb/run.py          ← FULL example parser
└── <18 other parser dirs>/run.py   ← skeleton — see PARSERS_SKELETON.md
```

Migrations (in `data-pipelines/migrations/`), in apply order:
1. `2026-05-21_camelote_datasets_host.sql`         (camelote_data — adds `host` column)
2. `2026-05-21_vd_enrichment_bronze.sql`           (re-llm — 18 bronze tables + 1 runs table)
3. `2026-05-21_vd_enrichment_silver.sql`           (re-llm — 13 silver matviews, `WITH NO DATA`)
4. `2026-05-21_vd_enrichment_link_matviews.sql`    (re-llm — `silver_ch.bldg_to_plot` national bridge + 7 `link_plot_*_vd`)
5. `2026-05-21_vd_enrichment_gold.sql`             (re-llm — `core_plots_ext_vd` + `v_plots_full` REPLACE)

## Apply order (locked, do not skip / do not combine)

1. **Camelote schema extension** (`apply_migration` against camelote_data).
2. **Bronze migration** (`apply_migration` against re-llm). Verify with `\d bronze_ch.vd_batiment_rcb`.
3. **Camelote dataset seed** (manual SQL against camelote_data): `camelote_datasets_seed.sql`. Verify 19 rows.
4. **VPS deploy** (3 hosts): `./deploy.sh vps1 vps1.parsers.txt` etc. Each leaves cron *unreloaded*. Dry-runs only.
5. **Populate `/opt/lamap/.env` on each VPS** with `SUPABASE_DB_URI` (session_pooler_uri from registry — never `db.<ref>.supabase.co`).
6. **Reload cron**: `ssh root@<vps> 'systemctl reload cron'`. Wait one cadence OR trigger a parser manually. Verify bronze rows.
7. **Silver migration**. Creates matviews `WITH NO DATA`.
8. **Silver first refresh** (non-CONCURRENTLY): refresh each of the 13 `_vd` matviews + 3 national matviews. Confirm row counts > 0.
9. **Link matviews migration**. Creates bridge + 7 link matviews `WITH NO DATA`.
10. **Link first refresh** (non-CONCURRENTLY): `REFRESH MATERIALIZED VIEW silver_ch.bldg_to_plot;` first (heaviest, ~3.3M × per-canton plots), then the 7 `link_plot_*_vd`.
11. **Gold migration**. Creates `core_plots_ext_vd` `WITH NO DATA` + REPLACE `v_plots_full`.
12. **Gold first refresh**: `REFRESH MATERIALIZED VIEW gold_ch.core_plots_ext_vd;`.
13. **Verify**: `SELECT count(*), count(noise_sensitivity_degree) FROM gold_ch.v_plots_full WHERE canton_code = 'VD';`
14. **Ongoing refreshes** via matview refresh scheduler — use `CONCURRENTLY` (unique indexes built in 8/10/12).

**If any step fails, STOP.** Do not proceed. Surface to operator.

## Refresh dependency chain (ongoing operation)

```
parsers (VPS cron, monthly)
  ↓
silver_ch.cadastral_*_vd        (matview refresh, daily)
  ↓
silver_ch.bldg_to_plot          (matview refresh, weekly — bridge changes slowly)
  ↓
silver_ch.link_plot_*_vd        (matview refresh, nightly)
  ↓
gold_ch.core_plots_ext_vd       (matview refresh, nightly)
  ↓
gold_ch.v_plots_full            (regular view, reads matviews live)
```

All refreshes `CONCURRENTLY` after the first non-CONCURRENTLY seed.

## Deferred debt (NOT in this session)

- **`silver_ch.bldg_to_plot` is national** — eventual `core_plots_ext_ge` rewrite to read from it is a small consolidation win; out of scope this session.
- **`building_destination` vocabulary** — `categorie_txt` (VD) vs `destination` (GE) — raw values surfaced; `silver_ch.ref_building_categories` is a future session.
- **`historical_parcelles_v2` shadow** — kept until consumer audit; drop in follow-up session.
- **RDPPF for VD** — separate scoped sub-plan; `cadastral_rdppf` matview itself needs redesign.

## Hard rules (enforced by code + reviewed in migrations)

- Bronze DDL only via Supabase `apply_migration` (never raw psycopg2 for DDL).
- No CASCADE, no DROP, no RENAME on existing matviews.
- UPSERT only; soft-delete via `deleted_at`.
- All matview unique indexes created BEFORE any REFRESH CONCURRENTLY.
- `NOTIFY pgrst, 'reload schema'` after every DDL migration.
- VPS credentials only in `/opt/lamap/.env` (chmod 600), never committed.
- VPS repo is pull-only — never `git push` from a VPS.
