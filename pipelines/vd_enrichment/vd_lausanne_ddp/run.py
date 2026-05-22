#!/usr/bin/env python3
"""vd_lausanne_ddp — Lausanne WFS bdcad_bf_parc_pol_ddp (DDP polygons, 8 fields)."""
import sys
from pipelines.vd_enrichment._shared.wfs_query import geojson_geom_to_ewkt
from pipelines.vd_enrichment._shared.wfs_parser_base import WFSLayerSpec, run_wfs_parser

TABLE = "bronze_ch.vd_lausanne_ddp"
PK_COLS = ("source_layer", "source_pk")
COLUMNS = ("no_parc", "type_txt", "no_commune", "commune_name",
           "proprietaire", "surface_m2", "href_geomatik",
           "source_pk", "geometry",
           "source_layer", "canton_code", "first_seen_at")


def row_mapper(f, source_layer, run_started_at):
    p = f.get("properties") or {}
    source_pk = f.get("id") or p.get("gml:id")
    if not source_pk:
        return None
    no_com = p.get("numcom")
    try:
        no_com = int(no_com) if no_com not in (None, "") else None
    except (TypeError, ValueError):
        no_com = None
    surf = p.get("surface")
    try:
        surf = float(surf) if surf not in (None, "") else None
    except (TypeError, ValueError):
        surf = None
    return (
        p.get("no_parc"),
        p.get("type"),
        no_com,
        p.get("nom_com"),
        p.get("proprio"),
        surf,
        p.get("href_go"),
        str(source_pk),
        geojson_geom_to_ewkt(f.get("geometry"), srid=2056),
        source_layer, "VD", run_started_at,
    )


if __name__ == "__main__":
    sys.exit(run_wfs_parser(
        dataset_code="vd_lausanne_ddp",
        table=TABLE, pk_cols=PK_COLS, columns=COLUMNS,
        layers=[WFSLayerSpec(source_layer="bdcad_bf_parc_pol_ddp")],
        row_mapper=row_mapper,
        parser_module="vd_lausanne_ddp",
    ))
