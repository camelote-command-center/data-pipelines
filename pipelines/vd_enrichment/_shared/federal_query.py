"""
Federal GeoAdmin iterator.

TODO (deferred to follow-up PR): replace /find endpoint with the correct bulk-extract path.
  The /find endpoint requires `searchText` to be non-empty (returns HTTP 400 with empty text),
  so it can't be used to bulk-extract all features in a layer. Correct paths to evaluate:
    (a) Direct dataset download: https://data.geo.admin.ch/<layer>/<layer>/data.{csv,gpkg,geojson}
    (b) MapServer/<layerId>/query?where=1=1&outFields=*&f=geojson   (REST query)
    (c) Federal WFS at https://wms.geo.admin.ch/?service=WFS
  Discovered via VPS3 dry-run 2026-05-22 (HTTP 400 from /find).
  Until fixed, federal_bav_transit parser is non-functional. Not on this PR's critical path
  (deferred to follow-up PR per scope adjustment).

Used for: ch.bav.haltestellen-oev (currently broken — see TODO above).

Pattern: GeoAdmin /find endpoint with searchField=* (returns all features matching a
bbox, paged via offset). For national coverage, we walk CH bbox grid because /find
caps at 200 features per request.

Bbox (EPSG:2056 LV95): 2,485,000–2,834,000 E × 1,074,000–1,296,000 N.
Grid cell: 50km × 50km gives ~7×5=35 cells, well within rate budget for quarterly cadence.
"""
from __future__ import annotations
import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Iterator


GEOADMIN_FIND = "https://api3.geo.admin.ch/rest/services/api/MapServer/find"
CH_BBOX_LV95 = (2_485_000, 1_074_000, 2_834_000, 1_296_000)  # (xmin, ymin, xmax, ymax)
GRID_CELL_M = 50_000


@dataclass
class FederalConfig:
    layer: str                                # e.g., "ch.bav.haltestellen-oev"
    search_field: str = "*"
    geometry_format: str = "geojson"


def _http_get_json(url, timeout=45, max_retries=6):
    last_err = None
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return json.loads(r.read())
        except Exception as e:
            last_err = e
            time.sleep(min(60, 2 ** attempt))
    raise RuntimeError(f"GeoAdmin GET failed: {last_err}  url={url}")


def iter_features(cfg: FederalConfig) -> Iterator[dict]:
    """
    Walk CH bbox grid and yield each feature exactly once.
    Deduplication via a seen-set keyed on layer-specific PK (didok for BAV).
    """
    xmin, ymin, xmax, ymax = CH_BBOX_LV95
    seen_ids: set[str] = set()
    for x0 in range(xmin, xmax, GRID_CELL_M):
        for y0 in range(ymin, ymax, GRID_CELL_M):
            x1 = min(x0 + GRID_CELL_M, xmax)
            y1 = min(y0 + GRID_CELL_M, ymax)
            params = {
                "layer": cfg.layer,
                "searchField": cfg.search_field,
                "searchText": "",                # empty = all features in bbox
                "returnGeometry": "true",
                "geometryFormat": cfg.geometry_format,
                "geometry": f"{x0},{y0},{x1},{y1}",
                "geometryType": "esriGeometryEnvelope",
                "sr": "2056",
                "imageDisplay": "1024,1024,96",
                "mapExtent": f"{x0},{y0},{x1},{y1}",
                "tolerance": "0",
                "lang": "fr",
            }
            url = GEOADMIN_FIND + "?" + urllib.parse.urlencode(params)
            resp = _http_get_json(url)
            for f in resp.get("results", []) or []:
                attrs = f.get("attributes", {}) or {}
                pk = attrs.get("didok") or attrs.get("number") or f.get("featureId")
                if pk and str(pk) in seen_ids:
                    continue
                if pk:
                    seen_ids.add(str(pk))
                yield f
            time.sleep(0.2)   # courtesy rate limit
