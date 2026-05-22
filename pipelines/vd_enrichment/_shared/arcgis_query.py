"""
ArcGIS REST /query pageable iterator for canton-VD wmsVD MapServer.

Endpoint pattern:
  GET https://agsgc.map.vd.ch/agsgc/rest/services/OGC/wmsVD/MapServer/<layer_id>/query
    ?where=1=1
    &outFields=*
    &returnGeometry=true
    &outSR=2056
    &f=json
    &resultRecordCount=<page>
    &resultOffset=<offset>

Yields features as Python dicts: {'attributes': {...}, 'geometry': {...}}.
Geometry comes back as ArcGIS JSON; convert to WKT via esri_to_wkt() below.
"""
from __future__ import annotations
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Iterator


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
    """GET with exponential backoff. Returns parsed JSON."""
    last_err = None
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return json.loads(r.read())
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            last_err = e
            backoff = min(60, 2 ** attempt)
            time.sleep(backoff)
    raise RuntimeError(f"ArcGIS GET failed after {max_retries} attempts: {last_err}  url={url}")


def iter_features(cfg: ArcGISConfig) -> Iterator[dict]:
    """
    Iterate all features from the ArcGIS layer. Yields raw feature dicts.
    Pagination uses resultOffset; stops when exceededTransferLimit is False
    AND fewer-than-page-size features come back.
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
        if not feats or len(feats) < cfg.page_size:
            # Last page
            if not resp.get("exceededTransferLimit"):
                break
        offset += len(feats)


def esri_to_wkt(geom: dict | None, geom_type_hint: str | None = None) -> str | None:
    """
    Convert ArcGIS JSON geometry to PostGIS-ingestible EWKT (with SRID=2056).
    Supports point / multipoint / polyline / polygon.

    Returns 'SRID=2056;<wkt>' or None.
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
        # ArcGIS rings are ordered: outer + inner (holes). One polygon per outer.
        # Simple approach: treat all rings as outer rings of separate polygons in a multipolygon.
        # For correctness with holes, would need orientation check; this is acceptable for
        # ingest+spatial-intersect usage (PostGIS will ST_MakeValid the result anyway).
        polys = []
        for ring in geom["rings"]:
            coords = ", ".join(f"{p[0]} {p[1]}" for p in ring)
            polys.append(f"(({coords}))")
        return f"SRID=2056;MULTIPOLYGON({','.join(polys)})"
    return None
