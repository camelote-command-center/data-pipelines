"""
Reusable helper for SITG CSV ZIP data downloads.

Handles:
  - Download ZIP file from SITG open data
  - Extract CSV from ZIP
  - Parse CSV with semicolon delimiter
  - Field name mapping (to snake_case, matching existing JS parsers)
  - Value normalisation (stringify + whitespace collapse)

Usage:
    from shared.sitg_csv import fetch_csv_features

    records = fetch_csv_features(
        csv_url="https://ge.ch/sitg/geodata/SITG/OPENDATA/CAD_DDP-CSV.zip",
    )

Each record is a dict with snake_case keys matching the existing bronze table
columns (produced by the legacy JS parsers).  CSV sources do NOT include
geometry — use the ArcGIS helper if geometry is needed.
"""

import csv
import io
import zipfile

import requests

from shared.sitg_arcgis import key_to_snake_case, format_value


def fetch_csv_features(csv_url: str) -> list[dict]:
    """
    Download a SITG CSV ZIP and return records as list of dicts.

    Args:
        csv_url: URL to the CSV ZIP file (e.g. .../CAD_DDP-CSV.zip)

    Returns:
        List of dicts with snake_case keys and normalised values.
    """
    print(f"  Downloading CSV ZIP...")
    resp = requests.get(csv_url, timeout=300, stream=True)
    resp.raise_for_status()

    content = resp.content
    print(f"  Download complete: {len(content) / 1024 / 1024:.1f} MB")

    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        # Find the CSV file inside the ZIP
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError(f"No CSV file found in ZIP. Contents: {zf.namelist()}")

        csv_name = csv_names[0]
        print(f"  Extracting: {csv_name}")

        with zf.open(csv_name) as f:
            # Use utf-8-sig to handle BOM if present
            text = io.TextIOWrapper(f, encoding="utf-8-sig")
            reader = csv.DictReader(text, delimiter=";")

            records: list[dict] = []
            for row in reader:
                record: dict = {}
                for k, v in row.items():
                    if k is None:
                        continue
                    record[key_to_snake_case(k)] = format_value(v)
                records.append(record)

    print(f"  CSV records: {len(records):,}")
    return records
