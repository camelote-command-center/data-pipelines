from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from shapely.geometry import mapping


def _to_feature(geom, props: dict) -> dict:
    return {"type": "Feature", "geometry": mapping(geom), "properties": props}


def write_layer_geojson(path: Path, entry: dict, layer_slug: str, epsg: int = 2056) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    geoms = entry.get("geoms", [])
    features = []
    for g in geoms:
        if g is None or getattr(g, "is_empty", False):
            continue
        features.append(_to_feature(g, {
            "layer_slug": layer_slug,
            "label": entry.get("label"),
            "color": entry.get("color"),
            "fill_type": entry.get("fill_type"),
        }))
    fc = {
        "type": "FeatureCollection",
        "crs": {"type": "name", "properties": {"name": f"EPSG:{epsg}"}},
        "features": features,
    }
    with path.open("w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False)
    return len(features)


def write_manifest(path: Path, manifest: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2, default=str)


def write_combined_geojson(path: Path, all_entries: list[dict], epsg: int = 2056) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    features = []
    for entry in all_entries:
        for g in entry.get("geoms", []):
            if g is None or getattr(g, "is_empty", False):
                continue
            features.append(_to_feature(g, {
                "layer_slug": entry.get("slug"),
                "label": entry.get("label"),
                "color": entry.get("color"),
                "fill_type": entry.get("fill_type"),
                "map_theme": entry.get("map_theme"),
                "page_number": entry.get("page_number"),
            }))
    fc = {
        "type": "FeatureCollection",
        "crs": {"type": "name", "properties": {"name": f"EPSG:{epsg}"}},
        "features": features,
    }
    with path.open("w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False)
    return len(features)
