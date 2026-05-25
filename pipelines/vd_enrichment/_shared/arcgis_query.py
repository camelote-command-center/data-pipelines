"""
ArcGIS REST /query pageable iterator for canton-VD wmsVD MapServer.

Important: canton-VD ArcGIS has service-level maxRecordCount=1 — every /query
response returns exactly ONE feature regardless of resultRecordCount. The
service ignores our requested page size. For 240K-row layers this means 240K
HTTP requests. Mitigations in this module:

  - HTTP keep-alive via a module-level requests.Session (TLS handshake reused
    across all requests; saves ~250ms per request).
  - Each ArcGISConfig accepts a `where` clause so callers can chunk by
    OBJECTID range/modulo and run multiple parser processes in parallel.

Yields features as Python dicts: {'attributes': {...}, 'geometry': {...}}.
Geometry comes back as ArcGIS JSON; convert to WKT via esri_to_wkt().
"""
from __future__ import annotations
import json
import time
import urllib.parse
from dataclasses import dataclass
from typing import Iterator

try:
    import requests
    _USE_REQUESTS = True
    _SESSION = requests.Session()
    _SESSION.headers.update({"Accept-Encoding": "gzip"})
except ImportError:
    import urllib.request, urllib.error
    _USE_REQUESTS = False
    _SESSION = None


ARCGIS_BASE = "https://agsgc.map.vd.ch/agsgc/rest/services/OGC/wmsVD/MapServer"
DEFAULT_PAGE_SIZE = 1000
DEFAULT_MAX_RETRIES = 6


@dataclass
class ArcGISConfig:
    layer_id: int
    layer_name: str
    out_sr: int = 2056
    page_size: int = DEFAULT_PAGE_SIZE
    where: str = "1=1"


def _http_get_json(url: str, timeout: int = 30, max_retries: int = DEFAULT_MAX_RETRIES) -> dict:
    """GET with HTTP keep-alive when requests is available, fallback to urllib."""
    last_err = None
    for attempt in range(max_retries):
        try:
            if _USE_REQUESTS:
                r = _SESSION.get(url, timeout=timeout)
                r.raise_for_status()
                return r.json()
            else:
                import urllib.request
                with urllib.request.urlopen(url, timeout=timeout) as r:
                    return json.loads(r.read())
        except Exception as e:
            last_err = e
            backoff = min(60, 2 ** attempt)
            time.sleep(backoff)
    raise RuntimeError(f"ArcGIS GET failed after {max_retries} attempts: {last_err}  url={url}")


def iter_features(cfg: ArcGISConfig) -> Iterator[dict]:
    """
    Iterate all features matching cfg.where. Pagination uses resultOffset.
    Because the canton-VD service caps at 1 feature per response, each loop
    iteration is one feature. exceededTransferLimit=True is the normal case.
    """
    offset = 0
    while True:
        params = {
            "where": cfg.where,
            "outFields": "*",
            "returnGeometry": "true",
            "outSR": str(cfg.out_sr),
            "resultRecordCount": str(cfg.page_size),
            "resultOffset": str(offset),
            "f": "json",
        }
        url = f"{ARCGIS_BASE}/{cfg.layer_id}/query?" + urllib.parse.urlencode(params)
        resp = _http_get_json(url)
        feats = resp.get("features", []) or []
        for f in feats:
            yield f
        if not feats:
            break
        offset += len(feats)
        # Stop iterating when server signals end of dataset
        if not resp.get("exceededTransferLimit", False) and len(feats) < cfg.page_size:
            break


def esri_to_wkt(geom: dict | None, geom_type_hint: str | None = None) -> str | None:
    """
    Convert ArcGIS JSON geometry to PostGIS-ingestible EWKT (SRID=2056).
    Supports point / multipoint / polyline / polygon. Promotes single shapes
    to MULTI* where bronze columns expect MULTI types.
    """
    if not geom:
        return None
    if "x" in geom and "y" in geom:
        return f"SRID=2056;POINT({geom['x']} {geom['y']})"
    if "points" in geom:
        pts = ", ".join(f"{p[0]} {p[1]}" for p in geom["points"])
        return f"SRID=2056;MULTIPOINT({pts})"
    if "paths" in geom:
        lines = []
        for path in geom["paths"]:
            coords = ", ".join(f"{p[0]} {p[1]}" for p in path)
            lines.append(f"({coords})")
        return f"SRID=2056;MULTILINESTRING({','.join(lines)})"
    if "rings" in geom:
        polys = []
        for ring in geom["rings"]:
            coords = ", ".join(f"{p[0]} {p[1]}" for p in ring)
            polys.append(f"(({coords}))")
        return f"SRID=2056;MULTIPOLYGON({','.join(polys)})"
    return None
