# Parser skeletons — 18 remaining datasets

All parsers follow the same shape as `vd_batiment_rcb/run.py`. Three variants based on source.

## Variant A — canton-VD ArcGIS REST (9 datasets)

`vd_zone_affectation`, `vd_limite_foret`, `vd_batiment_projete`, `vd_degre_sensibilite_bruit`,
`vd_classement` (multi-layer), `vd_jardin_historique`, `vd_isos` (multi-layer),
`vd_region_archeologique`, `vd_ddp_pts`.

Diff from `vd_batiment_rcb`:
- `cfg = ArcGISConfig(layer_id=<N>, layer_name=<source_layer>, page_size=1000)`
- `COLUMNS` tuple matches each table's schema in the bronze migration
- `_row_from_feature` maps source attrs → bronze cols per `v2_attr_mapping.json`
- For multi-layer bronzes (vd_classement, vd_isos), iterate `for source_layer, layer_id in MULTI_LAYERS:` and run the loop per layer; soft-delete pass at end uses `source_layer_values=[<all_layers>]`.

## Variant B — Lausanne WFS (8 datasets)

`vd_lausanne_ddp`, `vd_lausanne_dp`, `vd_lausanne_servitudes` (multi-layer),
`vd_lausanne_pa_etude`, `vd_lausanne_parking_sectors`, `vd_lausanne_archeologie` (multi-layer),
`vd_lausanne_energie_cad_bati`.

Diff from `vd_batiment_rcb`:
- Import `wfs_query` instead of `arcgis_query`
- `cfg = WFSConfig(layer_name=<source_layer>, srs=2056)`
- `_row_from_feature` reads `f["properties"]` (not `f["attributes"]`) and `f["geometry"]`
- WKT via `geojson_geom_to_ewkt(geom)`
- Multi-layer: same approach as Variant A
- Source PK: `f.get("id") or f.get("properties",{}).get("gml:id")`

For `vd_lausanne_servitudes`, derive `geom_kind` from `source_layer` suffix:
```python
geom_kind = source_layer.rsplit("_", 1)[-1]  # 'line' | 'point' | 'surf'
```

For `vd_lausanne_archeologie`, derive `record_type`, `record_subtype`, `geom_kind`:
```python
parts = source_layer.split("_")
# 'amenagement_rec_arch_<subtype>_<geom>'
record_subtype = parts[3]       # 'notes' | 'mesures'
record_type = 'site' if 'site' in parts[-1] or 'point' in parts[-1] or 'surf' in parts[-1] else 'mesure'
geom_kind = parts[-1] if parts[-1] in ('point','site','surf') else 'surf'
# For mesures_pbc / mesures_plan_class: record_subtype = 'pbc' or 'plan_class', record_type='mesure'
if parts[-1] in ('pbc','plan_class'):
    record_subtype = parts[-1]
    record_type = 'mesure'
    geom_kind = 'surf'
```

## Variant C — Federal GeoAdmin (1 dataset)

`federal_bav_transit`.

Diff from `vd_batiment_rcb`:
- Import `federal_query` (`iter_features(FederalConfig(layer='ch.bav.haltestellen-oev'))`)
- Spatial-join `canton_code` per-row inside parser:
  ```python
  with conn.cursor() as cur:
      cur.execute("""
          SELECT canton_code FROM bronze_ch.federal_communes
           WHERE ST_Contains(geometry, ST_GeomFromEWKT(%s)) LIMIT 1
      """, (wkt,))
      row = cur.fetchone()
      canton_code = row[0] if row else None
  ```
- Cron: monthly on VPS3 PLUS quarterly schedule line (UPSERT semantics make the dual-cadence safe).

## Field maps — see `v2_attr_mapping.json`

For each parser:
1. Pull the bronze table's row from `v2_attr_mapping.json`
2. Generate `COLUMNS` tuple from the `fields[]` array (`bronze_col` values)
3. Generate `_row_from_feature` mapping (`src` → `bronze_col`)

A helper script that does this auto-generation is in `_shared/generate_parser_from_mapping.py`
(skeleton — to be authored before deploy).

## Testing checklist per parser

Before adding to cron:
- [ ] `--dry-run --limit 5` succeeds, prints feature schema match
- [ ] `--limit 100` against bronze writes 100 rows, schema matches table
- [ ] Re-run without `--limit`: idempotent (rerun = same row count, last_seen_at advances)
- [ ] Soft-delete pass: temporarily delete one source row, rerun, confirm `deleted_at` set
- [ ] Restore: source row returns next cycle, confirm `deleted_at` reset to NULL
