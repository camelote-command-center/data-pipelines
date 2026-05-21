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

Migrations (in `data-pipelines/migrations/`):
- `2026-05-21_vd_enrichment_bronze.sql`  — 18 bronze tables + 1 runs table
- `2026-05-21_vd_enrichment_silver.sql`  — 13 silver matviews
- `2026-05-21_vd_enrichment_gold.sql`    — `core_plots_ext_vd` + `v_plots_full` patch
- `2026-05-21_camelote_datasets_host.sql`— adds `host` column to camelote_data.datasets

## Apply order

1. **Camelote schema extension** (camelote_data): `2026-05-21_camelote_datasets_host.sql`
2. **Bronze migration** (re-llm): `2026-05-21_vd_enrichment_bronze.sql`
3. **Camelote seed** (camelote_data): `camelote_datasets_seed.sql`
4. **VPS deploy** (3 hosts): `./deploy.sh vps1 vps1.parsers.txt` etc. Each leaves cron *unreloaded*.
5. **Populate `.env` on each VPS** with `SUPABASE_DB_URI` (session_pooler_uri from registry).
6. **Reload cron**: `ssh root@<vps> 'systemctl reload cron'`
7. **Wait one cadence** OR trigger manually: `python3 -m pipelines.vd_enrichment.vd_batiment_rcb.run` to backfill bronze.
8. **Silver migration** (re-llm): `2026-05-21_vd_enrichment_silver.sql` — creates matviews WITH NO DATA.
9. **First refresh**: `REFRESH MATERIALIZED VIEW silver_ch.cadastral_buildings_vd;` (non-CONCURRENTLY first time, ×13).
10. **Gold migration** (re-llm): `2026-05-21_vd_enrichment_gold.sql`. Resolve `§A bldg_to_plot bridge` (see migration header) before refreshing `core_plots_ext_vd`.
11. **`REFRESH MATERIALIZED VIEW gold_ch.core_plots_ext_vd`** + verify `v_plots_full` returns VD rows with `ext_vd.*` populated.
12. Ongoing refreshes via existing matview refresh scheduler — use `CONCURRENTLY` (unique indexes built in steps 8/10).

## Open items (must resolve before step 11)

- **§A** — `cadastral_energy_vd` and `bldg_dest_vd` joins use `egid::text = p.egrid` which is WRONG (per-building vs per-plot key). Need a bridge CTE/matview. See `gold` migration header.
- **§B / §C** — spatial-intersect aggregations in `core_plots_ext_vd` are heavy. Plan: materialize `silver_ch.link_plot_servitudes_vd`, `link_plot_densification_vd` as nightly matviews.
- **§D** — `v_plots_full` adds `historical_parcelles_v2` to preserve back-compat. Decide whether to canonicalize after consumer audit.
- **building_destination known debt** — `categorie_txt` (VD) vs `destination` (GE) — different vocabularies surfaced raw. Normalization layer `silver_ch.ref_building_categories` deferred.

## Hard rules (enforced by code + reviewed in migrations)

- Bronze DDL only via Supabase `apply_migration` (never raw psycopg2 for DDL).
- No CASCADE, no DROP, no RENAME on existing matviews.
- UPSERT only; soft-delete via `deleted_at`.
- All matview unique indexes created BEFORE any REFRESH CONCURRENTLY.
- `NOTIFY pgrst, 'reload schema'` after every DDL migration.
- VPS credentials only in `/opt/lamap/.env` (chmod 600), never committed.
- VPS repo is pull-only — never `git push` from a VPS.
