#!/usr/bin/env python3
"""
vd_classement — canton-VD heritage classement (2 source layers consolidated).
  /398 vd.plan_classement                       — fiche + url_recens fields
  /161 vd.arrete_decision_classement_perimetre  — numero_arrete + date_arrete + type_protection
"""
import json, sys
from datetime import datetime
from pipelines.vd_enrichment._shared.arcgis_query import esri_to_wkt
from pipelines.vd_enrichment._shared.arcgis_parser_base import LayerSpec, run_arcgis_parser

TABLE = "bronze_ch.vd_classement"
PK_COLS = ("source_layer", "arcgis_objectid")
COLUMNS = ("id_objet_recense", "commune", "description",
           "fiche", "url_recens",
           "numero_arrete", "date_arrete", "type_protection",
           "raw_data", "arcgis_objectid", "geometry",
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
    is_plan = source_layer == "vd.plan_classement"
    return (
        a.get("ID_OBJET_RECENSE"),
        a.get("COMMUNE"),
        a.get("DESCRIPTION"),
        a.get("FICHE") if is_plan else None,
        a.get("URL_RECENS") if is_plan else None,
        None if is_plan else a.get("NUMERO_ARRETE"),
        None if is_plan else _epoch_ms_to_ts(a.get("DATE_ARRETE")),
        None if is_plan else a.get("TYPE_PROTECTION"),
        json.dumps(a, ensure_ascii=False, default=str),
        oid, esri_to_wkt(f.get("geometry")),
        source_layer, "VD", run_started_at,
    )


if __name__ == "__main__":
    sys.exit(run_arcgis_parser(
        dataset_code="vd_classement",
        table=TABLE, pk_cols=PK_COLS, columns=COLUMNS,
        layers=[
            LayerSpec(layer_id=398, source_layer="vd.plan_classement"),
            LayerSpec(layer_id=161, source_layer="vd.arrete_decision_classement_perimetre"),
        ],
        row_mapper=row_mapper,
        parser_module="vd_classement",
    ))
