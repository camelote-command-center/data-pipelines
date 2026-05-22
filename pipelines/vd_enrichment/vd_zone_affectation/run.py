#!/usr/bin/env python3
"""
vd_zone_affectation — canton-VD PGA zones. Layer 36 (polygon, 26 attrs).
Promotes IUS/COS/SPB/CM/IGT/H_MAX + zone codes/labels to columns; bronze table
has explicit columns for all 26 source fields (see migration body).
"""
from datetime import datetime
import sys
from pipelines.vd_enrichment._shared.arcgis_query import esri_to_wkt
from pipelines.vd_enrichment._shared.arcgis_parser_base import LayerSpec, run_arcgis_parser

SOURCE_LAYER = "vd.zone_affectation"
TABLE = "bronze_ch.vd_zone_affectation"
PK_COLS = ("source_layer", "arcgis_objectid")
COLUMNS = (
    "code_vd_n2", "designation_vd_n2", "designation_vd_n1",
    "code_ch", "designation_ch", "code_com", "designation_com",
    "abreviation", "statut_juridique",
    "date_entree_vigueur", "date_fin", "force_obligatoire",
    "ius_type", "ius_value", "ius", "cos", "spb", "cm", "igt", "h_max",
    "normat", "symbole", "sous_theme", "perimetre_m", "surface_m2",
    "arcgis_objectid", "geometry",
    "source_layer", "canton_code", "first_seen_at",
)


def _epoch_ms_to_ts(v):
    """ArcGIS returns Date fields as milliseconds since epoch (or null)."""
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
        a.get("CODE_VD_N2"), a.get("DESIGNATION_VD_N2"), a.get("DESIGNATION_VD_N1"),
        a.get("CODE_CH"), a.get("DESIGNATION_CH"),
        a.get("CODE_COM"), a.get("DESIGNATION_COM"),
        a.get("ABREVIATION"), a.get("STATUT_JURIDIQUE"),
        _epoch_ms_to_ts(a.get("DATE_EV")), a.get("DATE_FIN"), a.get("FORCE_OBLIGATOIRE"),
        a.get("INDICE_UTILISATION_TYPE"), a.get("INDICE_UTILISATION"),
        a.get("IUS"), a.get("COS"), a.get("SPB"), a.get("CM"), a.get("IGT"), a.get("H_MAX"),
        a.get("NORMAT"), a.get("SYMBOLE"), a.get("SOUS_THEME"),
        a.get("PERIMETRE"), a.get("SURFACE"),
        oid, esri_to_wkt(f.get("geometry")),
        source_layer, "VD", run_started_at,
    )


if __name__ == "__main__":
    sys.exit(run_arcgis_parser(
        dataset_code="vd_zone_affectation",
        table=TABLE, pk_cols=PK_COLS, columns=COLUMNS,
        layers=[LayerSpec(layer_id=36, source_layer=SOURCE_LAYER)],
        row_mapper=row_mapper,
        parser_module="vd_zone_affectation",
    ))
