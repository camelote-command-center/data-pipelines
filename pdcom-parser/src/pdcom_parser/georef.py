from __future__ import annotations

from typing import Any

import fitz
from shapely.affinity import affine_transform
from shapely.geometry import Polygon, shape
from shapely.ops import unary_union


def _drawings_to_closed_polys(drawings, page_h: float, min_area_pct: float = 0.02, page_area: float = 1.0) -> list[tuple[float, Polygon]]:
    """Return [(stroke_width, polygon)] for closed paths with meaningful area."""
    from .extract import drawing_to_polygon

    out = []
    for d in drawings:
        col = d.get("color")
        if col is None:
            continue
        width = d.get("width") or 0.0
        if width < 0.6:
            continue
        poly = drawing_to_polygon(d, page_h, min_area=10.0)
        if poly is None or poly.is_empty:
            continue
        if poly.geom_type == "MultiPolygon":
            poly = max(poly.geoms, key=lambda g: g.area)
        if poly.area < page_area * min_area_pct:
            continue
        out.append((width, poly))
    return out


def _excludes_page_edge(poly: Polygon, page_rect: fitz.Rect, page_h: float, margin: float = 4.0) -> bool:
    minx, miny, maxx, maxy = poly.bounds
    # page_rect is PDF-coord. poly is Y-flipped so y=0 is bottom.
    if minx < margin and miny < margin and maxx > page_rect.width - margin and maxy > page_h - margin:
        return False
    return True


def detect_commune_boundary(page: fitz.Page) -> Polygon | None:
    page_h = page.rect.height
    page_area = page.rect.width * page.rect.height
    drawings = page.get_drawings()
    candidates = _drawings_to_closed_polys(drawings, page_h, min_area_pct=0.02, page_area=page_area)
    if not candidates:
        return None
    # Sort by stroke width desc then area desc
    candidates.sort(key=lambda t: (-t[0], -t[1].area))
    for width, poly in candidates:
        if _excludes_page_edge(poly, page.rect, page_h):
            return poly
    return candidates[0][1]


def _compute_transform(pdf_poly: Polygon, target_poly_lv95: Polygon) -> tuple[list[float], float]:
    """Return shapely affine params [a, b, d, e, xoff, yoff] for scale+translate
    (non-uniform), plus a confidence score.

    Confidence metric: IoU-like ratio between transformed bbox and target bbox.
    Cleaner than Hausdorff which depends on shape noise along the boundary."""
    pminx, pminy, pmaxx, pmaxy = pdf_poly.bounds
    tminx, tminy, tmaxx, tmaxy = target_poly_lv95.bounds
    pw = max(pmaxx - pminx, 1e-6)
    ph = max(pmaxy - pminy, 1e-6)
    tw = tmaxx - tminx
    th = tmaxy - tminy
    # Non-uniform scale: fit each axis independently.
    sx = tw / pw
    sy = th / ph
    pcx = (pminx + pmaxx) / 2
    pcy = (pminy + pmaxy) / 2
    tcx = (tminx + tmaxx) / 2
    tcy = (tminy + tmaxy) / 2
    a = sx
    e = sy
    xoff = tcx - sx * pcx
    yoff = tcy - sy * pcy
    params = [a, 0.0, 0.0, e, xoff, yoff]
    transformed = affine_transform(pdf_poly, params)

    # Confidence: IoU of transformed vs target.
    try:
        inter = transformed.intersection(target_poly_lv95).area
        union = transformed.union(target_poly_lv95).area
        iou = inter / union if union > 0 else 0.0
    except Exception:
        iou = 0.0
    conf = max(0.0, min(1.0, iou))
    return params, conf


def _compute_transform_bbox(
    pdf_map_bbox_yflip: tuple[float, float, float, float],
    target_lv95_bbox: tuple[float, float, float, float],
) -> list[float]:
    """Fit PDF map-bbox to target LV95 bbox with non-uniform scale + translate."""
    pminx, pminy, pmaxx, pmaxy = pdf_map_bbox_yflip
    tminx, tminy, tmaxx, tmaxy = target_lv95_bbox
    pw = max(pmaxx - pminx, 1e-6)
    ph = max(pmaxy - pminy, 1e-6)
    tw = tmaxx - tminx
    th = tmaxy - tminy
    sx = tw / pw
    sy = th / ph
    pcx = (pminx + pmaxx) / 2
    pcy = (pminy + pmaxy) / 2
    tcx = (tminx + tmaxx) / 2
    tcy = (tminy + tmaxy) / 2
    xoff = tcx - sx * pcx
    yoff = tcy - sy * pcy
    return [sx, 0.0, 0.0, sy, xoff, yoff]


def georeference(
    layers_pdf: dict[str, dict],
    commune_boundary_lv95: Polygon,
    page: fitz.Page,
    map_bbox: tuple[float, float, float, float] | None = None,
) -> tuple[dict[str, dict], float]:
    """Georeference PDF-space layer geometries into LV95.

    Strategy:
    - Primary: use legend's map_bbox (Y-flipped) as the PDF-space frame; fit to
      commune bbox in LV95. Fast, robust, low-precision.
    - If map_bbox is None, fall back to commune-boundary detection + transform
      fitting on the boundary polygon itself.
    """
    page_h = page.rect.height
    target_bbox = commune_boundary_lv95.bounds

    if map_bbox is not None:
        x0, y0, x1, y1 = map_bbox
        # Y-flip map_bbox (legend gave PDF-down coords, layers are Y-up)
        ny0 = page_h - y1
        ny1 = page_h - y0
        params = _compute_transform_bbox((x0, ny0, x1, ny1), target_bbox)
        # Confidence: we trust map_bbox as PDF-frame → LV95-bbox. Score reflects
        # the aspect-ratio agreement (1.0 if perfectly matching).
        pw = x1 - x0
        ph = y1 - y0
        tw = target_bbox[2] - target_bbox[0]
        th = target_bbox[3] - target_bbox[1]
        if pw > 0 and ph > 0 and tw > 0 and th > 0:
            pdf_ratio = pw / ph
            target_ratio = tw / th
            ratio_agreement = min(pdf_ratio, target_ratio) / max(pdf_ratio, target_ratio)
        else:
            ratio_agreement = 0.0
        conf = 0.5 + 0.4 * ratio_agreement  # [0.5, 0.9] range
    else:
        pdf_boundary = detect_commune_boundary(page)
        if pdf_boundary is None:
            return layers_pdf, 0.0
        params, conf = _compute_transform(pdf_boundary, commune_boundary_lv95)

    out = {}
    for slug, entry in layers_pdf.items():
        new_geoms = []
        for g in entry.get("geoms", []):
            if g is None or getattr(g, "is_empty", False):
                continue
            try:
                new_geoms.append(affine_transform(g, params))
            except Exception:
                continue
        out[slug] = {**entry, "geoms": new_geoms}
    return out, conf
