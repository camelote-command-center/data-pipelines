"""
Lausanne WFS GetFeature iterator.

Endpoint:
  https://map.lausanne.ch/mapserv_proxy?ogcserver=source+for+image%2Fpng
    &SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature
    &TYPENAME=ms:<layer_name>
    &OUTPUTFORMAT=geojson
    &SRSNAME=EPSG:2056

Returns GeoJSON FeatureCollection. Lausanne MapServer doesn't support WFS 2.0
StartIndex pagination reliably, so we pull the whole layer in one request per
target layer. All target layers are small (<60K features); per-pull payload
typically ≤30MB. If a layer ever grows beyond comfortable single-pull size,
add BBOX-paged pulls.
"""
from __future__ import annotations
import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Iterator


CACHE_VERSION = "0a66991b66614d7ba6fc1d68508038ca"   # from /themes endpoint; update if stale
WFS_BASE = (
    "https://map.lausanne.ch/mapserv_proxy"
    f"?ogcserver=source+for+image%2Fpng&cache_version={CACHE_VERSION}"
)


@dataclass
class WFSConfig:
    layer_name: str
    srs: int = 2056
    max_features: int | None = None


def fetch_feature_collection(cfg: WFSConfig, timeout: int = 60, max_retries: int = 6) -> dict:
    params = {
        "SERVICE": "WFS",
        "VERSION": "1.1.0",
        "REQUEST": "GetFeature",
        "TYPENAME": f"ms:{cfg.layer_name}",
        "OUTPUTFORMAT": "geojson",
        "SRSNAME": f"EPSG:{cfg.srs}",
    }
    if cfg.max_features:
        params["MAXFEATURES"] = str(cfg.max_features)
    url = WFS_BASE + "&" + urllib.parse.urlencode(params)
    last_err = None
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return json.loads(r.read())
        except Exception as e:
            last_err = e
            time.sleep(min(60, 2 ** attempt))
    raise RuntimeError(f"WFS GetFeature failed: {last_err}  url={url}")


def iter_features(cfg: WFSConfig) -> Iterator[dict]:
    fc = fetch_feature_collection(cfg)
    for f in fc.get("features", []) or []:
        yield f


def geojson_geom_to_ewkt(geom: dict | None, srid: int = 2056) -> str | None:
    """
    Convert GeoJSON geometry to EWKT. PostGIS can also accept GeoJSON directly via
    ST_GeomFromGeoJSON(), but text EWKT keeps the parser+driver path simpler.
    """
    if not geom:
        return None
    t = geom.get("type")
    c = geom.get("coordinates")
    if t == "Point":
        return f"SRID={srid};POINT({c[0]} {c[1]})"
    if t == "MultiPoint":
        pts = ", ".join(f"{p[0]} {p[1]}" for p in c)
        return f"SRID={srid};MULTIPOINT({pts})"
    if t == "LineString":
        coords = ", ".join(f"{p[0]} {p[1]}" for p in c)
        return f"SRID={srid};LINESTRING({coords})"
    if t == "MultiLineString":
        lines = ", ".join("(" + ", ".join(f"{p[0]} {p[1]}" for p in line) + ")" for line in c)
        return f"SRID={srid};MULTILINESTRING({lines})"
    if t == "Polygon":
        rings = ", ".join("(" + ", ".join(f"{p[0]} {p[1]}" for p in ring) + ")" for ring in c)
        return f"SRID={srid};POLYGON({rings})"
    if t == "MultiPolygon":
        polys = []
        for poly in c:
            rings = ", ".join("(" + ", ".join(f"{p[0]} {p[1]}" for p in ring) + ")" for ring in poly)
            polys.append(f"({rings})")
        return f"SRID={srid};MULTIPOLYGON({','.join(polys)})"
    return None
