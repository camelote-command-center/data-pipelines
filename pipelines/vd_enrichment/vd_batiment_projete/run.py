#!/usr/bin/env python3
"""
vd_batiment_projete — canton-VD projected buildings (cadastral). Layer 274 (polygon).
Source raw_data → jsonb; only OBJECTID + geometry promoted to columns.
"""
import json, sys
from pipelines.vd_enrichment._shared.arcgis_query import esri_to_wkt
from pipelines.vd_enrichment._shared.arcgis_parser_base import LayerSpec, run_arcgis_parser

SOURCE_LAYER = "vd.batiment_projete"
TABLE = "bronze_ch.vd_batiment_projete"
PK_COLS = ("source_layer", "arcgis_objectid")
COLUMNS = ("raw_data", "arcgis_objectid", "geometry",
           "source_layer", "canton_code", "first_seen_at")


def row_mapper(f, source_layer, run_started_at):
    a = f.get("attributes") or {}
    oid = a.get("OBJECTID")
    if oid is None:
        return None
    return (
        json.dumps(a, ensure_ascii=False, default=str),
        oid,
        esri_to_wkt(f.get("geometry")),
        source_layer, "VD", run_started_at,
    )


if __name__ == "__main__":
    sys.exit(run_arcgis_parser(
        dataset_code="vd_batiment_projete",
        table=TABLE, pk_cols=PK_COLS, columns=COLUMNS,
        layers=[LayerSpec(layer_id=274, source_layer=SOURCE_LAYER)],
        row_mapper=row_mapper,
        parser_module="vd_batiment_projete",
    ))
