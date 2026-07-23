#!/usr/bin/env python3
"""
SITG BATI3D -> re-LLM bronze_ch (6 layers) + per-EGID above-ground volume aggregate.

Ingest is GDB -> ogr2ogr -> PostGIS. NEVER CSV: multipatch geometry does not survive
CSV export, and without the geometry there is no volume.

CRS is EPSG:2056 (CH1903+/LV95) and stays that way. Altitudes are LN02 metres above
sea level, NOT heights above ground: building height = ALTITUDE_MAX - ALTITUDE_REF.
Do not reproject to 4326 -- Z would become degrees and every volume would be garbage.

Publication cadence is "irreguliere" (last 2025-06-12, photogrammetry). Never present
the output as live data; source_publication_date + per-feature date_leve are carried
through to the consumer.

Metadata is PATCHed PER DATASET CODE, not by workflow_file. sitg_cadastral.yml stamps
all 8 of its datasets with one timestamp, which means a fresh last_acquired_at proves
nothing about any individual layer. We do not repeat that.
"""
import os
import subprocess
import sys
import urllib.request
import zipfile
import json
import tempfile

SITG_OPENDATA = "https://ge.ch/sitg/geodata/SITG/OPENDATA/{layer}-GDB.zip"

# layer -> (dataset_code, bronze table)
LAYERS = [
    ("CAD_BATI3D_BASE",         "ge_cad_bati3d_base",       "ge_cad_bati3d_base"),
    ("CAD_BATI3D_BASIC_FACADE", "ge_cad_bati3d_facade",     "ge_cad_bati3d_facade"),
    ("CAD_BATI3D_BASIC_TOIT",   "ge_cad_bati3d_toit",       "ge_cad_bati3d_toit"),
    ("CAD_BATI3D_SP_FACADE",    "ge_cad_bati3d_sp_facade",  "ge_cad_bati3d_sp_facade"),
    ("CAD_BATI3D_SP_TOIT",      "ge_cad_bati3d_sp_toit",    "ge_cad_bati3d_sp_toit"),
    ("CAD_BATI3D_COUVERTS",     "ge_cad_bati3d_couverts",   "ge_cad_bati3d_couverts"),
]

SCHEMA = os.environ.get("RE_LLM_SCHEMA", "bronze_ch")
PGURI = os.environ.get("RE_LLM_PG_URI")          # session pooler URI
CMD_URL = os.environ.get("CAMELOTE_DATA_SUPABASE_URL")
CMD_KEY = os.environ.get("CAMELOTE_DATA_SUPABASE_SERVICE_KEY")


def fetch(layer: str, workdir: str) -> str:
    url = SITG_OPENDATA.format(layer=layer)
    zp = os.path.join(workdir, f"{layer}.zip")
    print(f"  fetching {url}")
    urllib.request.urlretrieve(url, zp)
    with zipfile.ZipFile(zp) as z:
        z.extractall(os.path.join(workdir, layer))
    for root, dirs, _ in os.walk(os.path.join(workdir, layer)):
        for d in dirs:
            if d.endswith(".gdb"):
                return os.path.join(root, d)
    raise RuntimeError(f"no .gdb found in {layer}")


def load(gdb: str, table: str) -> int:
    """ogr2ogr GDB -> PostGIS. Multipatch lands as MultiPolygonZ."""
    cmd = [
        "ogr2ogr", "-f", "PostgreSQL", f"PG:{PGURI}", gdb,
        "-nln", table, "-lco", f"SCHEMA={SCHEMA}",
        "-lco", "GEOMETRY_NAME=geom", "-lco", "FID=objectid", "-lco", "PRECISION=NO",
        "-lco", "OVERWRITE=YES", "-nlt", "MULTIPOLYGONZ", "-a_srs", "EPSG:2056",
        "--config", "PG_USE_COPY", "YES", "-gt", "20000",
    ]
    subprocess.run(cmd, check=True)
    out = subprocess.run(
        ["psql", PGURI, "-tAc", f"SELECT count(*) FROM {SCHEMA}.{table}"],
        check=True, capture_output=True, text=True)
    return int(out.stdout.strip())


def patch_dataset(code: str, count: int, error: str | None = None) -> None:
    """PATCH pixxels_data.datasets BY CODE -- deliberately not by workflow_file."""
    if not (CMD_URL and CMD_KEY):
        print(f"  [skip PATCH {code}: no command-center credentials]")
        return
    import datetime
    if error:
        payload = {"last_error": error}
    else:
        payload = {
            "last_acquired_at": datetime.datetime.now(datetime.UTC)
                                 .strftime("%Y-%m-%dT%H:%M:%SZ"),
            "record_count": count,
            "last_error": None,
        }
    req = urllib.request.Request(
        f"{CMD_URL}/rest/v1/datasets?code=eq.{code}",
        data=json.dumps(payload).encode(), method="PATCH",
        headers={"apikey": CMD_KEY, "Authorization": f"Bearer {CMD_KEY}",
                 "Content-Type": "application/json", "Prefer": "return=minimal"})
    with urllib.request.urlopen(req, timeout=60) as r:
        print(f"  PATCH {code} -> {r.status}")


def rebuild_aggregate() -> None:
    """Rebuild gold_ch.ge_building_volumes_3d from the freshly loaded layers."""
    sql_path = os.path.join(os.path.dirname(__file__), "sql", "02_gold_volumes.sql")
    subprocess.run(["psql", PGURI, "-v", "ON_ERROR_STOP=1", "-f", sql_path], check=True)


def main() -> int:
    if not PGURI:
        print("ERROR: RE_LLM_PG_URI required")
        return 1
    only = sys.argv[1] if len(sys.argv) > 1 else None
    failed = []
    with tempfile.TemporaryDirectory() as workdir:
        for layer, code, table in LAYERS:
            if only and only != code:
                continue
            print(f"[{layer}]")
            try:
                n = load(fetch(layer, workdir), table)
                print(f"  {table}: {n} rows")
                patch_dataset(code, n)
            except Exception as e:                      # noqa: BLE001
                print(f"  FAILED {layer}: {e}")
                patch_dataset(code, 0, error=str(e)[:500])
                failed.append(layer)
    if failed:
        print(f"FAILED layers: {failed}")
        return 1
    rebuild_aggregate()
    print("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
