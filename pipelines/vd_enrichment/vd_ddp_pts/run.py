#!/usr/bin/env python3
"""vd_ddp_pts — canton-VD DDP centroids (mensuration officielle). Layer 430, MultiPoint."""
import sys
from pipelines.vd_enrichment._shared.arcgis_query import esri_to_wkt
from pipelines.vd_enrichment._shared.arcgis_parser_base import LayerSpec, run_arcgis_parser

SOURCE_LAYER = "vd.ddp_source_mens_officielle"
TABLE = "bronze_ch.vd_ddp_pts"
PK_COLS = ("source_layer", "arcgis_objectid")
COLUMNS = ("numero", "egris_egrid", "genre_txt", "arcgis_objectid", "geometry",
           "source_layer", "canton_code", "first_seen_at")


def row_mapper(f, source_layer, run_started_at):
    a = f.get("attributes") or {}
    oid = a.get("OBJECTID")
    if oid is None:
        return None
    return (
        a.get("NUMERO"),
        a.get("EGRIS_EGRID"),
        a.get("GENRE_TXT"),
        oid,
        esri_to_wkt(f.get("geometry")),
        source_layer, "VD", run_started_at,
    )


if __name__ == "__main__":
    sys.exit(run_arcgis_parser(
        dataset_code="vd_ddp_pts",
        table=TABLE, pk_cols=PK_COLS, columns=COLUMNS,
        layers=[LayerSpec(layer_id=430, source_layer=SOURCE_LAYER)],
        row_mapper=row_mapper,
        parser_module="vd_ddp_pts",
    ))
