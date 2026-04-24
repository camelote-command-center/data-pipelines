from __future__ import annotations

import math
from typing import Iterable

import fitz
from shapely.geometry import LineString, MultiLineString, MultiPolygon, Point, Polygon, box
from shapely.ops import unary_union


MIN_FEATURE_AREA_M2 = 200.0
MAX_FEATURE_AREA_FRAC_OF_COMMUNE = 0.30


def _colors_match(a, b, tol: int = 2) -> bool:
    if a is None or b is None:
        return False
    for ac, bc in zip(a[:3], b[:3]):
        ai = int(round(ac * 255)) if 0 <= ac <= 1 else int(ac)
        bi = int(round(bc * 255)) if 0 <= bc <= 1 else int(bc)
        if abs(ai - bi) > tol:
            return False
    return True


def _flip_y(pt, page_h: float):
    return (pt[0], page_h - pt[1])


def _flatten_bezier(p0, p1, p2, p3, samples: int = 10):
    pts = []
    for i in range(samples + 1):
        t = i / samples
        u = 1 - t
        x = u**3 * p0[0] + 3 * u**2 * t * p1[0] + 3 * u * t**2 * p2[0] + t**3 * p3[0]
        y = u**3 * p0[1] + 3 * u**2 * t * p1[1] + 3 * u * t**2 * p2[1] + t**3 * p3[1]
        pts.append((x, y))
    return pts


def _items_to_subpaths(items, page_h: float) -> list[list[tuple[float, float]]]:
    """Convert PyMuPDF drawing items to lists of vertex sequences. Y-flipped to standard."""
    subpaths: list[list[tuple[float, float]]] = []
    current: list[tuple[float, float]] = []

    def flush():
        nonlocal current
        if len(current) >= 2:
            subpaths.append(current)
        current = []

    for it in items:
        op = it[0]
        if op == "m":
            flush()
            pt = _flip_y((it[1].x, it[1].y), page_h)
            current = [pt]
        elif op == "l":
            p0 = _flip_y((it[1].x, it[1].y), page_h)
            p1 = _flip_y((it[2].x, it[2].y), page_h)
            if not current:
                current = [p0]
            elif current[-1] != p0:
                current.append(p0)
            current.append(p1)
        elif op == "c":
            p0 = _flip_y((it[1].x, it[1].y), page_h)
            p1 = _flip_y((it[2].x, it[2].y), page_h)
            p2 = _flip_y((it[3].x, it[3].y), page_h)
            p3 = _flip_y((it[4].x, it[4].y), page_h)
            if not current:
                current = [p0]
            elif current[-1] != p0:
                current.append(p0)
            pts = _flatten_bezier(p0, p1, p2, p3)
            current.extend(pts[1:])
        elif op == "re":
            flush()
            rect = it[1]
            x0, y0, x1, y1 = rect.x0, rect.y0, rect.x1, rect.y1
            pts = [
                _flip_y((x0, y0), page_h),
                _flip_y((x1, y0), page_h),
                _flip_y((x1, y1), page_h),
                _flip_y((x0, y1), page_h),
                _flip_y((x0, y0), page_h),
            ]
            subpaths.append(pts)
            current = []
        elif op == "qu":  # quadratic Bezier variant used by some PDFs
            pass
    flush()
    return subpaths


def drawing_to_polygon(drawing: dict, page_h: float, min_area: float = 1.0):
    items = drawing.get("items") or []
    if not items:
        return None
    subpaths = _items_to_subpaths(items, page_h)
    if not subpaths:
        return None
    polys = []
    for sp in subpaths:
        if len(sp) < 3:
            continue
        if sp[0] != sp[-1]:
            sp = list(sp) + [sp[0]]
        try:
            poly = Polygon(sp).buffer(0)
        except Exception:
            continue
        if poly.is_empty:
            continue
        if poly.geom_type == "Polygon" and poly.area >= min_area:
            polys.append(poly)
        elif poly.geom_type == "MultiPolygon":
            for g in poly.geoms:
                if g.area >= min_area:
                    polys.append(g)
    if not polys:
        return None
    if len(polys) == 1:
        return polys[0]
    try:
        return MultiPolygon(polys)
    except Exception:
        return polys[0]


def drawing_to_lines(drawing: dict, page_h: float, min_length: float = 1.0) -> list[LineString]:
    items = drawing.get("items") or []
    subpaths = _items_to_subpaths(items, page_h)
    lines: list[LineString] = []
    for sp in subpaths:
        if len(sp) < 2:
            continue
        try:
            ls = LineString(sp)
            if ls.length >= min_length:
                lines.append(ls)
        except Exception:
            continue
    return lines


def _map_box_yflip(map_bbox: list[float], page_h: float):
    x0, y0, x1, y1 = map_bbox
    # Y is flipped once for layers but map_bbox was reported in PDF coords.
    # Flip Y to match.
    ny0 = page_h - y1
    ny1 = page_h - y0
    return box(x0, ny0, x1, ny1)


def clip_and_score_layers(
    layers_lv95: dict[str, dict],
    commune_boundary_lv95: Polygon,
    page_conf: float,
) -> list[dict]:
    """Clip every polygon in layers_lv95 to the commune boundary (Fix 1),
    drop features violating world-coordinate area filters (Fix 2),
    and compute per-feature confidence = page_conf * clip_ratio (Fix 4).

    Returns flat list of feature dicts ready for DB insert / GeoJSON export.
    Each feature carries `clip_ratio`, `page_conf`, `original_area_m2`,
    `clipped_area_m2` in its `properties` so downstream can audit.
    """
    commune_area_m2 = commune_boundary_lv95.area  # LV95 is metric (metres)
    max_area = commune_area_m2 * MAX_FEATURE_AREA_FRAC_OF_COMMUNE

    features: list[dict] = []
    dropped_log: list[dict] = []

    for slug, entry in layers_lv95.items():
        fill_type = entry.get("fill_type", "solid")
        label = entry.get("label") or slug
        color = entry.get("color")

        for g in entry.get("geoms", []):
            if g is None or getattr(g, "is_empty", False):
                continue

            if fill_type == "stroke_only":
                # LineString: intersect with commune, skip area filters.
                try:
                    clipped = g.intersection(commune_boundary_lv95)
                except Exception:
                    continue
                if clipped.is_empty:
                    continue
                original_len = g.length
                clipped_len = clipped.length if hasattr(clipped, "length") else 0.0
                if original_len <= 0 or clipped_len <= 0:
                    continue
                clip_ratio = clipped_len / original_len if original_len > 0 else 0.0
                # Skip if almost nothing remains (mostly outside)
                if clipped_len < 1.0:
                    dropped_log.append({"slug": slug, "reason": "clipped_len<1m", "label": label})
                    continue
                feature_conf = round(page_conf * clip_ratio, 3)
                # Normalize to list of LineStrings
                geoms_out = []
                if clipped.geom_type == "LineString":
                    geoms_out = [clipped]
                elif clipped.geom_type == "MultiLineString":
                    geoms_out = list(clipped.geoms)
                elif hasattr(clipped, "geoms"):
                    geoms_out = [gg for gg in clipped.geoms if gg.geom_type in ("LineString", "MultiLineString")]
                for geom_out in geoms_out:
                    features.append({
                        "slug": slug, "label": label, "fill_type": fill_type, "color": color,
                        "geom": geom_out,
                        "confidence": feature_conf,
                        "properties": {
                            "page_conf": round(page_conf, 3),
                            "clip_ratio": round(clip_ratio, 3),
                            "original_length_m": round(original_len, 2),
                            "clipped_length_m": round(clipped_len, 2),
                        },
                    })
            else:
                # Polygonal feature (solid or hatch-derived): full clip + area filters.
                try:
                    original_area = g.area
                    clipped = g.intersection(commune_boundary_lv95)
                except Exception:
                    continue
                if clipped.is_empty:
                    dropped_log.append({"slug": slug, "reason": "zero_intersection", "label": label})
                    continue
                clipped_area = clipped.area
                if original_area <= 0:
                    continue
                clip_ratio = clipped_area / original_area

                # Area filters (Fix 2)
                if clipped_area < MIN_FEATURE_AREA_M2:
                    dropped_log.append({
                        "slug": slug, "reason": "below_min_area",
                        "area_m2": round(clipped_area, 1), "label": label,
                    })
                    continue
                if clipped_area > max_area:
                    dropped_log.append({
                        "slug": slug, "reason": "above_max_area",
                        "area_m2": round(clipped_area, 1),
                        "pct_of_commune": round(100 * clipped_area / commune_area_m2, 1),
                        "label": label,
                    })
                    continue

                feature_conf = round(page_conf * clip_ratio, 3)

                # Normalize to Polygons
                geoms_out = []
                if clipped.geom_type == "Polygon":
                    geoms_out = [clipped]
                elif clipped.geom_type == "MultiPolygon":
                    geoms_out = list(clipped.geoms)
                elif hasattr(clipped, "geoms"):
                    geoms_out = [gg for gg in clipped.geoms if gg.geom_type in ("Polygon", "MultiPolygon")]
                for geom_out in geoms_out:
                    if geom_out.area < MIN_FEATURE_AREA_M2:
                        continue
                    features.append({
                        "slug": slug, "label": label, "fill_type": fill_type, "color": color,
                        "geom": geom_out,
                        "confidence": feature_conf,
                        "properties": {
                            "page_conf": round(page_conf, 3),
                            "clip_ratio": round(clip_ratio, 3),
                            "original_area_m2": round(original_area, 1),
                            "clipped_area_m2": round(clipped_area, 1),
                        },
                    })

    return features, dropped_log


def extract_layers(page: fitz.Page, legend: dict, max_area_frac: float = 0.35, min_area: float = 4.0) -> dict[str, dict]:
    page_h = page.rect.height
    drawings = page.get_drawings()
    map_clip = _map_box_yflip(legend["map_bbox"], page_h) if legend.get("map_bbox") else None
    map_area = map_clip.area if map_clip is not None else (page.rect.width * page_h)
    max_poly_area = map_area * max_area_frac

    out: dict[str, dict] = {}
    for entry in legend.get("entries", []):
        slug = entry["slug"]
        target_rgb = entry.get("fill_color")
        if target_rgb is None:
            continue
        fill_type = entry["fill_type"]

        polys: list = []
        lines: list = []

        for d in drawings:
            rect = d.get("rect")
            if rect is not None and legend.get("legend_bbox"):
                lb = legend["legend_bbox"]
                dr = fitz.Rect(rect)
                lr = fitz.Rect(lb[0], lb[1], lb[2], lb[3])
                if lr.contains(dr):
                    continue
            if fill_type in ("solid", "hatch"):
                fill = d.get("fill")
                if not _colors_match(fill, target_rgb, tol=3):
                    continue
                shp = drawing_to_polygon(d, page_h)
                if shp is None:
                    continue
                if shp.geom_type == "Polygon":
                    polys.append(shp)
                elif shp.geom_type == "MultiPolygon":
                    polys.extend(list(shp.geoms))
            elif fill_type == "stroke_only":
                col = d.get("color") or d.get("fill")
                if not _colors_match(col, target_rgb, tol=3):
                    continue
                for ls in drawing_to_lines(d, page_h):
                    lines.append(ls)

        if fill_type == "solid":
            # Area filters: drop tiny noise + page-wide backgrounds
            polys = [p for p in polys if p.area >= min_area and p.area <= max_poly_area]
            if polys and map_clip is not None:
                polys = [p.intersection(map_clip) for p in polys]
                polys = [p for p in polys if not p.is_empty and p.area >= min_area]
            if polys:
                try:
                    merged = unary_union(polys)
                except Exception:
                    merged = None
                if merged is None or merged.is_empty:
                    pass
                elif merged.geom_type == "Polygon":
                    polys = [merged]
                elif merged.geom_type == "MultiPolygon":
                    polys = list(merged.geoms)
            out[slug] = {
                "fill_type": fill_type,
                "label": entry["label"],
                "color": entry["fill_color_hex"],
                "geoms": polys,
            }
        elif fill_type == "stroke_only":
            if map_clip is not None:
                lines = [ls.intersection(map_clip) for ls in lines]
                lines = [ls for ls in lines if not ls.is_empty and ls.length >= 1.0]
            out[slug] = {
                "fill_type": fill_type,
                "label": entry["label"],
                "color": entry["fill_color_hex"],
                "geoms": lines,
            }
        else:  # hatch — handed off to hatch.py caller
            out[slug] = {
                "fill_type": fill_type,
                "label": entry["label"],
                "color": entry["fill_color_hex"],
                "geoms": polys,
                "raw_drawings": [d for d in drawings if _colors_match(d.get("color") or d.get("fill"), target_rgb, tol=3)],
            }

    return out
