#!/usr/bin/env python3
"""
vd_isos — federal ISOS sites + perimeters, canton-VD published (2 layers).
  /481 vd.site_fonde_sur_isos       — Point geometry
  /404 vd.perimetre_fonde_sur_isos  — Polygon geometry
"""
import json, sys
from pipelines.vd_enrichment._shared.arcgis_query import esri_to_wkt
from pipelines.vd_enrichment._shared.arcgis_parser_base import LayerSpec, run_arcgis_parser

TABLE = "bronze_ch.vd_isos"
PK_COLS = ("source_layer", "arcgis_objectid")
COLUMNS = ("isos_categorie", "designation", "raw_data",
           "arcgis_objectid", "geometry",
           "source_layer", "canton_code", "first_seen_at")


def row_mapper(f, source_layer, run_started_at):
    a = f.get("attributes") or {}
    oid = a.get("OBJECTID")
    if oid is None:
        return None
    return (
        a.get("CATEGORIE") or a.get("ISOS_CATEGORIE") or a.get("CAT"),
        a.get("DESIGNATION") or a.get("NOM") or a.get("DENOMINATION"),
        json.dumps(a, ensure_ascii=False, default=str),
        oid, esri_to_wkt(f.get("geometry")),
        source_layer, "VD", run_started_at,
    )


if __name__ == "__main__":
    sys.exit(run_arcgis_parser(
        dataset_code="vd_isos",
        table=TABLE, pk_cols=PK_COLS, columns=COLUMNS,
        layers=[
            LayerSpec(layer_id=481, source_layer="vd.site_fonde_sur_isos"),
            LayerSpec(layer_id=404, source_layer="vd.perimetre_fonde_sur_isos"),
        ],
        row_mapper=row_mapper,
        parser_module="vd_isos",
    ))
