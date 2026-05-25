#!/usr/bin/env python3
"""vd_region_archeologique — canton-VD archaeological regions. Layer 320."""
import json, sys
from pipelines.vd_enrichment._shared.arcgis_query import esri_to_wkt
from pipelines.vd_enrichment._shared.arcgis_parser_base import LayerSpec, run_arcgis_parser

SOURCE_LAYER = "vd.region_archeologique"
TABLE = "bronze_ch.vd_region_archeologique"
PK_COLS = ("source_layer", "arcgis_objectid")
COLUMNS = ("nom_region", "raw_data", "arcgis_objectid", "geometry",
           "source_layer", "canton_code", "first_seen_at")


def row_mapper(f, source_layer, run_started_at):
    a = f.get("attributes") or {}
    oid = a.get("OBJECTID")
    if oid is None:
        return None
    return (
        a.get("NOM_REGION") or a.get("nom_region") or a.get("DESIGNATION"),
        json.dumps(a, ensure_ascii=False, default=str),
        oid, esri_to_wkt(f.get("geometry")),
        source_layer, "VD", run_started_at,
    )


if __name__ == "__main__":
    sys.exit(run_arcgis_parser(
        dataset_code="vd_region_archeologique",
        table=TABLE, pk_cols=PK_COLS, columns=COLUMNS,
        layers=[LayerSpec(layer_id=320, source_layer=SOURCE_LAYER)],
        row_mapper=row_mapper,
        parser_module="vd_region_archeologique",
    ))
