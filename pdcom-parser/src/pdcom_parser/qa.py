"""QA image renderer — overlays extracted zones on the commune boundary (LV95).
Saves as PNG. No display required (matplotlib 'Agg' backend)."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.patches import Polygon as MplPolygon, PathPatch
from matplotlib.path import Path as MplPath
from shapely.geometry import Polygon, MultiPolygon, LineString, MultiLineString


def _poly_to_patch(poly: Polygon, color: str, alpha: float = 0.55) -> MplPolygon | None:
    if poly is None or poly.is_empty:
        return None
    exterior = list(poly.exterior.coords)
    if len(exterior) < 3:
        return None
    codes = [MplPath.MOVETO] + [MplPath.LINETO] * (len(exterior) - 2) + [MplPath.CLOSEPOLY]
    verts = list(exterior)
    for interior in poly.interiors:
        ring = list(interior.coords)
        if len(ring) < 3:
            continue
        verts.extend(ring)
        codes.extend([MplPath.MOVETO] + [MplPath.LINETO] * (len(ring) - 2) + [MplPath.CLOSEPOLY])
    try:
        path = MplPath(verts, codes)
    except Exception:
        return None
    return PathPatch(path, facecolor=color, edgecolor="none", alpha=alpha)


def render_qa_image(
    commune_boundary_lv95: Polygon | MultiPolygon,
    features: list[dict],
    out_path: Path,
    title: str = "",
    max_width_px: int = 1800,
) -> Path:
    """`features` = list of {geom, color, label, fill_type}."""
    minx, miny, maxx, maxy = commune_boundary_lv95.bounds
    width_m = maxx - minx
    height_m = maxy - miny
    aspect = height_m / max(width_m, 1)
    fig_w = 12
    fig_h = max(4, fig_w * aspect)
    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=150)
    ax.set_aspect("equal")

    # 1. Commune outline
    boundary_polys = [commune_boundary_lv95] if commune_boundary_lv95.geom_type == "Polygon" else list(commune_boundary_lv95.geoms)
    for bp in boundary_polys:
        # Light fill
        patch = _poly_to_patch(bp, "#f0f0f0", alpha=1.0)
        if patch is not None:
            ax.add_patch(patch)
        x, y = bp.exterior.xy
        ax.plot(list(x), list(y), color="#333", linewidth=1.5)

    # 2. Feature polygons by color
    for f in features:
        g = f["geom"]
        color = f.get("color") or "#808080"
        fill_type = f.get("fill_type", "solid")
        if g is None or getattr(g, "is_empty", False):
            continue
        if fill_type == "stroke_only":
            # Lines
            if g.geom_type == "LineString":
                x, y = g.xy
                ax.plot(list(x), list(y), color=color, linewidth=1.2)
            elif g.geom_type == "MultiLineString":
                for geom in g.geoms:
                    x, y = geom.xy
                    ax.plot(list(x), list(y), color=color, linewidth=1.2)
            continue
        if g.geom_type == "Polygon":
            p = _poly_to_patch(g, color)
            if p is not None:
                ax.add_patch(p)
        elif g.geom_type == "MultiPolygon":
            for poly in g.geoms:
                p = _poly_to_patch(poly, color)
                if p is not None:
                    ax.add_patch(p)

    ax.set_xlim(minx - 50, maxx + 50)
    ax.set_ylim(miny - 50, maxy + 50)
    ax.set_title(title, fontsize=12)
    ax.set_xlabel("LV95 Easting (m)")
    ax.set_ylabel("LV95 Northing (m)")
    ax.grid(True, linewidth=0.3, alpha=0.3)

    # Minimal legend
    if features:
        seen = set()
        handles = []
        labels = []
        for f in features:
            key = (f.get("color"), f.get("label"))
            if key in seen:
                continue
            seen.add(key)
            handles.append(plt.Rectangle((0, 0), 1, 1, facecolor=f.get("color", "#808080"), alpha=0.55))
            lbl = f.get("label") or f.get("slug", "?")
            labels.append(lbl[:50])
            if len(handles) >= 12:
                break
        ax.legend(handles, labels, loc="center left", bbox_to_anchor=(1.01, 0.5), fontsize=7, frameon=False)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return out_path
