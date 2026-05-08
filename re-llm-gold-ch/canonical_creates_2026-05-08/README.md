# gold_ch Canonical CREATE SQL — Recovery from pg_stat_statements

**Recovery date:** 2026-05-08 ~10:00 UTC+1
**Source:** `pg_stat_statements` on re-LLM (`znrvddgmczdqoucmykij`), extracted before server restart
**Context:** 15 gold_ch objects were destroyed on 2026-05-06 by a parallel CC session (documented in `forensics-2026-05-07.md`). This extraction recovered the exact CREATE statements from PostgreSQL's query cache.

## Objects covered (14 of 15)

### Materialized views (8)
| File | Object | Parameterized? |
|------|--------|----------------|
| `core_buildings.sql` | `gold_ch.core_buildings` | Yes (45 refs) |
| `core_cantons.sql` | `gold_ch.core_cantons` | Yes (5 refs) |
| `core_communes.sql` | `gold_ch.core_communes` | Yes (11 refs) |
| `core_filter_options.sql` | `gold_ch.core_filter_options` | Yes (218 refs) |
| `core_plots.sql` | `gold_ch.core_plots` | No |
| `core_plots_ext_ge.sql` | `gold_ch.core_plots_ext_ge` | Yes (18 refs) |
| `core_plots_raw.sql` | `gold_ch.core_plots` (raw pg_stat dump) | No (alternate format) |
| `core_units.sql` | `gold_ch.core_units` | No |

### Views (7)
| File | Object | Parameterized? |
|------|--------|----------------|
| `v_addresses_full.sql` | `gold_ch.v_addresses_full` | No |
| `v_buildings_full.sql` | `gold_ch.v_buildings_full` | No |
| `v_cantons_full.sql` | `gold_ch.v_cantons_full` | No |
| `v_communes_full.sql` | `gold_ch.v_communes_full` | No |
| `v_filter_options_full.sql` | `gold_ch.v_filter_options_full` | No |
| `v_plots_full.sql` | `gold_ch.v_plots_full` | No |
| `v_units_full.sql` | `gold_ch.v_units_full` | No |

## Missing object (1 of 15)

**`gold_ch.core_addresses`** — not captured in pg_stat_statements. Needs manual reconstruction from:
- `silver_ch.cadastral_entrances` (source table)
- `v_addresses_full.sql` (output column list is known)
- Peer matviews (`core_buildings`, `core_units`) for pattern reference

## About parameterized constants

`pg_stat_statements` normalizes literal values to `$1`, `$2`, etc. Five matviews have these placeholders that must be de-parameterized before execution. The constants are typically:
- String literals for category labels (e.g., canton names, building classes, zone types)
- Numeric constants for BFS ranges
- Boolean flags

De-parameterization requires cross-referencing with silver_ch data and the views that consume these matviews.

## CRITICAL NOTE: "Silver redesign" myth correction

Earlier sessions theorized that silver column names had drifted from what gold expected. This is **FALSE**. The captured SQL confirms:
- `silver_ch.cadastral_buildings.dwelling_count` ✓
- `silver_ch.cadastral_buildings.construction_year` ✓ (not `year_built`)
- `silver_ch.cadastral_buildings.footprint_m2` ✓
- `silver_ch.cadastral_plots.surface_m2` ✓
- `silver_ch.cadastral_buildings.class_label` ✓ (not `building_class`)

The column names in silver are the canonical ones. No renaming needed. Use these SQL files as-is (after de-parameterization).

## Notes on core_plots_raw.sql vs core_plots.sql

- `core_plots_raw.sql` (40KB): Raw pg_stat_statements dump with formatting artifacts
- `core_plots.sql` (6.5KB): Cleaned/reformatted version of the same query

Both represent `gold_ch.core_plots`. Use `core_plots.sql` for execution.
