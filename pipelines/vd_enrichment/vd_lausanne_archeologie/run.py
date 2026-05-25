#!/usr/bin/env python3
"""
vd_lausanne_archeologie — Lausanne WFS archaeology, 8 sub-layers consolidated.

source_layer suffix decodes to (record_type, record_subtype, geom_kind):

  amenagement_rec_arch_notes_point      → site / notes / point
  amenagement_rec_arch_notes_site       → site / notes / site
  amenagement_rec_arch_notes_surf       → site / notes / surf
  amenagement_rec_arch_mesures_pbc      → mesure / pbc / surf
  amenagement_rec_arch_mesures_plan_class → mesure / plan_class / surf
  amenagement_rec_arch_mesures_point    → mesure / mesures / point
  amenagement_rec_arch_mesures_site     → mesure / mesures / site
  amenagement_rec_arch_mesures_surf     → mesure / mesures / surf
"""
import sys
from pipelines.vd_enrichment._shared.wfs_query import geojson_geom_to_ewkt
from pipelines.vd_enrichment._shared.wfs_parser_base import WFSLayerSpec, run_wfs_parser

TABLE = "bronze_ch.vd_lausanne_archeologie"
PK_COLS = ("source_layer", "source_pk")
COLUMNS = ("description", "mesure", "note_detail", "note_carto",
           "url_fiche", "url_carte",
           "source_pk", "geometry",
           "source_layer", "record_type", "record_subtype", "geom_kind",
           "canton_code", "first_seen_at")

SOURCE_LAYERS = [
    "amenagement_rec_arch_notes_point",
    "amenagement_rec_arch_notes_site",
    "amenagement_rec_arch_notes_surf",
    "amenagement_rec_arch_mesures_pbc",
    "amenagement_rec_arch_mesures_plan_class",
    "amenagement_rec_arch_mesures_point",
    "amenagement_rec_arch_mesures_site",
    "amenagement_rec_arch_mesures_surf",
]


def _decode_layer(source_layer: str):
    """
    Return (record_type, record_subtype, geom_kind).
    'notes' layers are sites; 'mesures' layers are mesures; pbc and plan_class
    are surface-geometry mesures with their own subtype names.
    """
    if "_notes_" in source_layer:
        record_type = "site"
        # last segment after _notes_ is the geom_kind (point|site|surf)
        record_subtype = "notes"
        geom_kind = source_layer.split("_notes_", 1)[1]
    elif "_mesures_" in source_layer:
        record_type = "mesure"
        tail = source_layer.split("_mesures_", 1)[1]
        if tail in ("pbc", "plan_class"):
            record_subtype = tail
            geom_kind = "surf"
        else:
            record_subtype = "mesures"
            geom_kind = tail
    else:
        record_type, record_subtype, geom_kind = "site", "notes", "surf"
    return record_type, record_subtype, geom_kind


def row_mapper(f, source_layer, run_started_at):
    p = f.get("properties") or {}
    source_pk = f.get("id") or p.get("gml:id")
    if not source_pk:
        return None
    record_type, record_subtype, geom_kind = _decode_layer(source_layer)
    return (
        p.get("descriptio"),
        p.get("mesure"),
        p.get("note_detai"),
        p.get("note_carto"),
        p.get("url_fiche"),
        p.get("url_carte"),
        str(source_pk),
        geojson_geom_to_ewkt(f.get("geometry"), srid=2056),
        source_layer, record_type, record_subtype, geom_kind,
        "VD", run_started_at,
    )


if __name__ == "__main__":
    sys.exit(run_wfs_parser(
        dataset_code="vd_lausanne_archeologie",
        table=TABLE, pk_cols=PK_COLS, columns=COLUMNS,
        layers=[WFSLayerSpec(source_layer=L) for L in SOURCE_LAYERS],
        row_mapper=row_mapper,
        parser_module="vd_lausanne_archeologie",
    ))
