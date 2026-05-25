#!/usr/bin/env python3
"""vd_degre_sensibilite_bruit — canton-VD OPB noise sensitivity (DS I-IV). Layer 461."""
import json, sys
from pipelines.vd_enrichment._shared.arcgis_query import esri_to_wkt
from pipelines.vd_enrichment._shared.arcgis_parser_base import LayerSpec, run_arcgis_parser

SOURCE_LAYER = "vd.degre_sensibilite_bruit"
TABLE = "bronze_ch.vd_degre_sensibilite_bruit"
PK_COLS = ("source_layer", "arcgis_objectid")
COLUMNS = ("ds_degre", "source_plan", "description", "raw_data",
           "arcgis_objectid", "geometry",
           "source_layer", "canton_code", "first_seen_at")


def row_mapper(f, source_layer, run_started_at):
    a = f.get("attributes") or {}
    oid = a.get("OBJECTID")
    if oid is None:
        return None
    return (
        a.get("DS_DEGRE") or a.get("DEGRE") or a.get("DS"),
        a.get("SOURCE_PLAN") or a.get("PLAN") or a.get("PLAN_LEGAL"),
        a.get("DESCRIPTION") or a.get("DESIGNATION"),
        json.dumps(a, ensure_ascii=False, default=str),
        oid, esri_to_wkt(f.get("geometry")),
        source_layer, "VD", run_started_at,
    )


if __name__ == "__main__":
    sys.exit(run_arcgis_parser(
        dataset_code="vd_degre_sensibilite_bruit",
        table=TABLE, pk_cols=PK_COLS, columns=COLUMNS,
        layers=[LayerSpec(layer_id=461, source_layer=SOURCE_LAYER)],
        row_mapper=row_mapper,
        parser_module="vd_degre_sensibilite_bruit",
    ))
