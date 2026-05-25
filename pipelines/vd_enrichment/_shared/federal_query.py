"""
Federal GeoAdmin bulk dataset iterator (STAC + CSV path).

For ch.bav.haltestellen-oev (and similar federal datasets distributed via
data.geo.admin.ch), the discovery path is:

  1. STAC item:   https://data.geo.admin.ch/api/stac/v0.9/collections/<layer>/items
  2. Pick an asset (we use <name>_2056_fr.csv.zip — CSV in EPSG:2056, French labels)
  3. Download + unzip + iterate CSV rows

This replaces the broken /find-endpoint approach used in PR #15
(/find requires non-empty searchText; cannot bulk-extract).

Usage:
  cfg = FederalCSVConfig(
      stac_collection="ch.bav.haltestellen-oev",
      asset_glob="*_2056_fr.csv.zip",
      csv_filename="PointExploitation.csv",
  )
  for row in iter_csv_rows(cfg):
      ...  # row is dict {column: value}
"""
from __future__ import annotations
import csv
import fnmatch
import io
import json
import logging
import os
import tempfile
import time
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from typing import Iterator

log = logging.getLogger(__name__)

STAC_BASE = "https://data.geo.admin.ch/api/stac/v0.9/collections"


@dataclass
class FederalCSVConfig:
    stac_collection: str             # e.g. "ch.bav.haltestellen-oev"
    asset_glob: str                  # e.g. "*_2056_fr.csv.zip"
    csv_filename: str                # name inside the zip, e.g. "PointExploitation.csv"


def _http_get_bytes(url: str, timeout: int = 120, max_retries: int = 6) -> bytes:
    last_err = None
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return r.read()
        except Exception as e:
            last_err = e
            time.sleep(min(60, 2 ** attempt))
    raise RuntimeError(f"federal GET failed: {last_err}  url={url}")


def resolve_asset_url(cfg: FederalCSVConfig) -> str:
    """Walk STAC items → assets and find the first matching asset_glob."""
    items_url = f"{STAC_BASE}/{cfg.stac_collection}/items"
    items_resp = json.loads(_http_get_bytes(items_url))
    for item in items_resp.get("features", []):
        for name, asset in item.get("assets", {}).items():
            if fnmatch.fnmatch(name, cfg.asset_glob):
                return asset.get("href")
    raise RuntimeError(f"No asset matches {cfg.asset_glob!r} in collection {cfg.stac_collection}")


def iter_csv_rows(cfg: FederalCSVConfig) -> Iterator[dict]:
    """
    Resolve STAC asset, download zip, extract the target CSV, yield rows as dicts.
    Yields nothing if csv_filename isn't in the zip.
    """
    asset_url = resolve_asset_url(cfg)
    log.info(f"federal_query: downloading {asset_url}")
    zip_bytes = _http_get_bytes(asset_url, timeout=180)
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = zf.namelist()
        if cfg.csv_filename not in names:
            raise RuntimeError(f"{cfg.csv_filename} not in zip; available: {names}")
        with zf.open(cfg.csv_filename) as raw:
            # CSV is UTF-8 with BOM (xtf_id header starts with ﻿)
            text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
            reader = csv.DictReader(text)
            for row in reader:
                yield row
