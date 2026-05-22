#!/usr/bin/env python3
"""vd_lausanne_energie_cad_bati — Lausanne WFS per-building energy cadastre (10 fields, point geom)."""
import sys
from pipelines.vd_enrichment._shared.wfs_query import geojson_geom_to_ewkt
from pipelines.vd_enrichment._shared.wfs_parser_base import WFSLayerSpec, run_wfs_parser

TABLE = "bronze_ch.vd_lausanne_energie_cad_bati"
PK_COLS = ("source_layer", "source_pk")
COLUMNS = ("egid", "rue", "no_rue", "no_parcelle",
           "solution_heat", "horizon_heat", "besoins_kwh",
           "vecteur_actuel", "detail_solution_heat",
           "source_pk", "geometry",
           "source_layer", "canton_code", "first_seen_at")


def row_mapper(f, source_layer, run_started_at):
    p = f.get("properties") or {}
    source_pk = f.get("id") or p.get("gml:id")
    if not source_pk:
        return None
    besoins = p.get("besoins_kw")
    try:
        besoins = float(besoins) if besoins not in (None, "") else None
    except (TypeError, ValueError):
        besoins = None
    return (
        p.get("egid"),
        p.get("rue"),
        p.get("no_rue"),
        p.get("no_parcell"),
        p.get("solution_h"),
        p.get("horizon_ht"),
        besoins,
        p.get("vecteur_ac"),
        p.get("det_sol_h"),
        str(source_pk),
        geojson_geom_to_ewkt(f.get("geometry"), srid=2056),
        source_layer, "VD", run_started_at,
    )


if __name__ == "__main__":
    sys.exit(run_wfs_parser(
        dataset_code="vd_lausanne_energie_cad_bati",
        table=TABLE, pk_cols=PK_COLS, columns=COLUMNS,
        layers=[WFSLayerSpec(source_layer="energie_cad_bati")],
        row_mapper=row_mapper,
        parser_module="vd_lausanne_energie_cad_bati",
    ))
