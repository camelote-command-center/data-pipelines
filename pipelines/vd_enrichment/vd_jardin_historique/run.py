#!/usr/bin/env python3
"""vd_jardin_historique — canton-VD historic gardens. Layer 403 (47 source fields → raw_data jsonb)."""
import json, sys
from datetime import datetime
from pipelines.vd_enrichment._shared.arcgis_query import esri_to_wkt
from pipelines.vd_enrichment._shared.arcgis_parser_base import LayerSpec, run_arcgis_parser

SOURCE_LAYER = "vd.jardin_historique"
TABLE = "bronze_ch.vd_jardin_historique"
PK_COLS = ("source_layer", "arcgis_objectid")
COLUMNS = ("designation", "valeur", "date_classement", "raw_data",
           "arcgis_objectid", "geometry",
           "source_layer", "canton_code", "first_seen_at")


def _epoch_ms_to_ts(v):
    if v is None or v == "":
        return None
    try:
        return datetime.utcfromtimestamp(int(v) / 1000).isoformat() + "+00:00"
    except (TypeError, ValueError):
        return None


def row_mapper(f, source_layer, run_started_at):
    a = f.get("attributes") or {}
    oid = a.get("OBJECTID")
    if oid is None:
        return None
    return (
        a.get("DESIGNATION") or a.get("NOM") or a.get("NOM_JARDIN"),
        a.get("VALEUR") or a.get("CATEGORIE"),
        _epoch_ms_to_ts(a.get("DATE_CLASSEMENT") or a.get("DATE_CLASS")),
        json.dumps(a, ensure_ascii=False, default=str),
        oid, esri_to_wkt(f.get("geometry")),
        source_layer, "VD", run_started_at,
    )


if __name__ == "__main__":
    sys.exit(run_arcgis_parser(
        dataset_code="vd_jardin_historique",
        table=TABLE, pk_cols=PK_COLS, columns=COLUMNS,
        layers=[LayerSpec(layer_id=403, source_layer=SOURCE_LAYER)],
        row_mapper=row_mapper,
        parser_module="vd_jardin_historique",
    ))
