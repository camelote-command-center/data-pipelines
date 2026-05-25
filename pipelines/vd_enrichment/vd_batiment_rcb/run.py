#!/usr/bin/env python3
"""
vd_batiment_rcb — canton-VD building cadastre (RCB). Layer 39 (point, 22 attrs).

Migrated to the shared run_arcgis_parser driver so it supports --where for
chunked parallel runs. canton-VD ArcGIS has maxRecordCount=1 service-wide;
without sharding a single-process run takes 30+ hours for the ~240K rows.
With 10× sharded parallelism on VPS1 + HTTP keep-alive, ~80-130 min.
"""
import sys
from pipelines.vd_enrichment._shared.arcgis_query import esri_to_wkt
from pipelines.vd_enrichment._shared.arcgis_parser_base import LayerSpec, run_arcgis_parser

SOURCE_LAYER = "vd.batiment_rcb"
TABLE = "bronze_ch.vd_batiment_rcb"
PK_COLS = ("source_layer", "arcgis_objectid")
COLUMNS = (
    "egid", "no_cadastr", "categorie_txt", "classe_txt", "statut_txt",
    "cons_annee", "cons_perio_txt", "surface_m2", "nb_niv_tot",
    "chauf1_sys_txt", "chauf1_nrg_txt",
    "chauf2_sys_txt", "chauf2_nrg_txt",
    "eau1_sys_txt",   "eau1_nrg_txt",
    "eau2_sys_txt",   "eau2_nrg_txt",
    "sre_m2", "abri_pci", "no_camac",
    "arcgis_objectid", "geometry",
    "source_layer", "canton_code", "first_seen_at",
)


def row_mapper(f, source_layer, run_started_at):
    a = f.get("attributes") or {}
    oid = a.get("OBJECTID")
    if oid is None:
        return None
    return (
        a.get("EGID"), a.get("NO_CADASTR"), a.get("CATEGORIE_TXT"), a.get("CLASSE_TXT"),
        a.get("STATUT_TXT"), a.get("CONS_ANNEE"), a.get("CONS_PERIO_TXT"),
        a.get("SURFACE"), a.get("NB_NIV_TOT"),
        a.get("CHAUF1_SYS_TXT"), a.get("CHAUF1_NRG_TXT"),
        a.get("CHAUF2_SYS_TXT"), a.get("CHAUF2_NRG_TXT"),
        a.get("EAU1_SYS_TXT"),   a.get("EAU1_NRG_TXT"),
        a.get("EAU2_SYS_TXT"),   a.get("EAU2_NRG_TXT"),
        a.get("SRE"), a.get("ABRI_PCI"), a.get("NO_CAMAC"),
        oid,
        esri_to_wkt(f.get("geometry")),
        source_layer, "VD", run_started_at,
    )


if __name__ == "__main__":
    sys.exit(run_arcgis_parser(
        dataset_code="vd_batiment_rcb",
        table=TABLE, pk_cols=PK_COLS, columns=COLUMNS,
        layers=[LayerSpec(layer_id=39, source_layer=SOURCE_LAYER)],
        row_mapper=row_mapper,
        parser_module="vd_batiment_rcb",
    ))
