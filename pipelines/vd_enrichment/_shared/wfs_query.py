"""
Lausanne WFS GetFeature iterator — GML 3.1.1 parser path.

Why GML, not GeoJSON: map.lausanne.ch's MapServer doesn't permit
OUTPUTFORMAT=geojson for several layers (servitudes, energie, etc.), even
though they appear in /themes with full attribute schemas. Default GML output
works for every public layer. Parsed with xml.etree.ElementTree (stdlib).

Endpoint (no OUTPUTFORMAT specified → default GML 3.1.1):
  https://map.lausanne.ch/mapserv_proxy?ogcserver=source+for+image%2Fpng
    &SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature
    &TYPENAME=ms:<layer_name>
    &SRSNAME=EPSG:2056
    &MAXFEATURES=<n>  (optional)

Yielded shape per feature (matches the prior GeoJSON-style dict):
  {
    "id":         <gml:id, e.g. "bdcad_bf_parc_pol_ddp.20226009">,
    "properties": { "no_parc": "21029", "type": "DDP", ... },
    "geometry":   { "type": "Polygon" | "MultiPolygon" | ... ,
                    "coordinates": [...] }
  }

The geometry dict mirrors GeoJSON exactly so `geojson_geom_to_ewkt(geom)`
keeps working unchanged.

If a layer truly returns an empty FeatureCollection (`<gml:Null>missing</gml:Null>`),
that's a data/auth issue at Lausanne, not a parser issue — yields nothing and
the run.py soft-delete pass treats it as "no rows present this cycle".
"""
from __future__ import annotations
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Iterator


CACHE_VERSION = "0a66991b66614d7ba6fc1d68508038ca"   # from /themes endpoint; update if stale
WFS_BASE = (
    "https://map.lausanne.ch/mapserv_proxy"
    f"?ogcserver=source+for+image%2Fpng&cache_version={CACHE_VERSION}"
)

NS = {
    "ms":  "http://mapserver.gis.umn.edu/mapserver",
    "gml": "http://www.opengis.net/gml",
    "wfs": "http://www.opengis.net/wfs",
}


@dataclass
class WFSConfig:
    layer_name: str
    srs: int = 2056
    max_features: int | None = None


def _http_get_bytes(url: str, timeout: int = 90, max_retries: int = 6) -> bytes:
    last_err = None
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return r.read()
        except Exception as e:
            last_err = e
            time.sleep(min(60, 2 ** attempt))
    raise RuntimeError(f"WFS GetFeature failed: {last_err}  url={url}")


def fetch_xml(cfg: WFSConfig) -> bytes:
    """GET the WFS GetFeature response as raw bytes (GML XML)."""
    params = {
        "SERVICE":   "WFS",
        "VERSION":   "1.1.0",
        "REQUEST":   "GetFeature",
        "TYPENAME":  f"ms:{cfg.layer_name}",
        "SRSNAME":   f"EPSG:{cfg.srs}",
    }
    if cfg.max_features:
        params["MAXFEATURES"] = str(cfg.max_features)
    url = WFS_BASE + "&" + urllib.parse.urlencode(params)
    return _http_get_bytes(url)


def _local(tag: str) -> str:
    """Strip XML namespace prefix from an ET tag: '{http://...}name' -> 'name'."""
    return tag.split("}", 1)[1] if "}" in tag else tag


def _parse_pos_list(text: str, dim: int = 2) -> list[list[float]]:
    """'x1 y1 x2 y2 ...' -> [[x1, y1], [x2, y2], ...]"""
    flat = [float(t) for t in text.split()]
    return [flat[i:i+dim] for i in range(0, len(flat), dim)]


def _parse_geometry(geom_elem: ET.Element) -> dict | None:
    """
    Parse a <ms:geom> child (or any GML geometry container) into a GeoJSON-shape dict.
    Supports Point, MultiPoint, LineString, MultiLineString, Polygon, MultiPolygon.
    """
    # Find the actual gml geometry inside the wrapper (often <ms:geom><gml:Polygon>...)
    for child in geom_elem.iter():
        local = _local(child.tag)
        if local == "Point":
            pos = child.find("gml:pos", NS)
            if pos is not None and pos.text:
                coords = [float(v) for v in pos.text.split()]
                return {"type": "Point", "coordinates": coords[:2]}
        elif local == "MultiPoint":
            pts = []
            for m in child.findall(".//gml:Point/gml:pos", NS):
                if m.text:
                    coords = [float(v) for v in m.text.split()]
                    pts.append(coords[:2])
            if pts:
                return {"type": "MultiPoint", "coordinates": pts}
        elif local == "LineString":
            pos = child.find("gml:posList", NS)
            if pos is not None and pos.text:
                return {"type": "LineString", "coordinates": _parse_pos_list(pos.text)}
        elif local == "MultiLineString":
            lines = []
            for ls in child.findall(".//gml:LineString/gml:posList", NS):
                if ls.text:
                    lines.append(_parse_pos_list(ls.text))
            if lines:
                return {"type": "MultiLineString", "coordinates": lines}
        elif local == "MultiCurve":   # WFS 1.1 alt for MultiLineString
            lines = []
            for ls in child.findall(".//gml:LineStringSegment/gml:posList", NS):
                if ls.text:
                    lines.append(_parse_pos_list(ls.text))
            for ls in child.findall(".//gml:LineString/gml:posList", NS):
                if ls.text:
                    lines.append(_parse_pos_list(ls.text))
            if lines:
                return {"type": "MultiLineString", "coordinates": lines}
        elif local == "Polygon":
            rings = []
            ext = child.find("gml:exterior/gml:LinearRing/gml:posList", NS)
            if ext is not None and ext.text:
                rings.append(_parse_pos_list(ext.text))
            for inn in child.findall("gml:interior/gml:LinearRing/gml:posList", NS):
                if inn.text:
                    rings.append(_parse_pos_list(inn.text))
            if rings:
                return {"type": "Polygon", "coordinates": rings}
        elif local == "MultiPolygon" or local == "MultiSurface":
            polys = []
            for poly in child.findall(".//gml:Polygon", NS) + child.findall(".//gml:surfaceMember/gml:Polygon", NS):
                rings = []
                ext = poly.find("gml:exterior/gml:LinearRing/gml:posList", NS)
                if ext is not None and ext.text:
                    rings.append(_parse_pos_list(ext.text))
                for inn in poly.findall("gml:interior/gml:LinearRing/gml:posList", NS):
                    if inn.text:
                        rings.append(_parse_pos_list(inn.text))
                if rings:
                    polys.append(rings)
            if polys:
                return {"type": "MultiPolygon", "coordinates": polys}
    return None


def iter_features(cfg: WFSConfig) -> Iterator[dict]:
    """
    Iterate features as dicts {'id', 'properties', 'geometry'}.
    Empty FeatureCollection yields nothing (no error raised).
    """
    body = fetch_xml(cfg)
    root = ET.fromstring(body)
    for member in root.findall("gml:featureMember", NS):
        # Each member contains exactly one child — the feature element <ms:<layer_name>>
        for feat in member:
            gml_id = feat.attrib.get(f"{{{NS['gml']}}}id")
            props: dict[str, str | None] = {}
            geometry = None
            for child in feat:
                local = _local(child.tag)
                if local == "boundedBy":
                    continue   # ignore per-feature bbox
                # geometry wrapper: typically <ms:geom> or <ms:msGeometry>; can also be named
                # after the geom column. We detect by presence of a gml:* child.
                has_gml_child = any(_local(c.tag) in (
                    "Point","MultiPoint","LineString","MultiLineString","MultiCurve",
                    "Polygon","MultiPolygon","MultiSurface"
                ) for c in child)
                if has_gml_child:
                    geometry = _parse_geometry(child)
                    continue
                # otherwise treat as scalar attribute
                txt = (child.text or "").strip()
                props[local] = txt if txt else None
            yield {"id": gml_id, "properties": props, "geometry": geometry}


def geojson_geom_to_ewkt(geom: dict | None, srid: int = 2056) -> str | None:
    """
    Convert GeoJSON-shape geometry to PostGIS EWKT. Same API as the prior
    GeoJSON path; the GML parser produces the same dict shape.
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


# Back-compat alias for any caller that imported fetch_feature_collection
def fetch_feature_collection(cfg: WFSConfig) -> dict:
    """Legacy shim: returns GeoJSON-shape FeatureCollection."""
    feats = list(iter_features(cfg))
    return {"type": "FeatureCollection", "features": feats}
