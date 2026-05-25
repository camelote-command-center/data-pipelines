#!/usr/bin/env python3
"""vd_lausanne_parking_sectors — Lausanne WFS parking policy sectors (commune-specific)."""
import sys
from pipelines.vd_enrichment._shared.wfs_query import geojson_geom_to_ewkt
from pipelines.vd_enrichment._shared.wfs_parser_base import WFSLayerSpec, run_wfs_parser

TABLE = "bronze_ch.vd_lausanne_parking_sectors"
PK_COLS = ("source_layer", "source_pk")
COLUMNS = ("secteur", "source_pk", "geometry",
           "source_layer", "canton_code", "first_seen_at")


def row_mapper(f, source_layer, run_started_at):
    p = f.get("properties") or {}
    source_pk = f.get("id") or p.get("gml:id")
    if not source_pk:
        return None
    return (
        p.get("secteur"),
        str(source_pk),
        geojson_geom_to_ewkt(f.get("geometry"), srid=2056),
        source_layer, "VD", run_started_at,
    )


if __name__ == "__main__":
    sys.exit(run_wfs_parser(
        dataset_code="vd_lausanne_parking_sectors",
        table=TABLE, pk_cols=PK_COLS, columns=COLUMNS,
        layers=[WFSLayerSpec(source_layer="amenagement_pga_sect_stationnement")],
        row_mapper=row_mapper,
        parser_module="vd_lausanne_parking_sectors",
    ))
