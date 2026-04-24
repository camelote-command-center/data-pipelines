from __future__ import annotations

from shapely.geometry import LineString, MultiPoint, Polygon
from shapely.ops import unary_union


def _cluster_endpoints(points: list[tuple[float, float]], eps: float = 8.0) -> list[list[tuple[float, float]]]:
    """Simple union-find based clustering — avoids sklearn dep."""
    n = len(points)
    parent = list(range(n))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    # O(n^2) — fine for swatch-sized clusters (typ. <2k endpoints)
    for i in range(n):
        xi, yi = points[i]
        for j in range(i + 1, n):
            xj, yj = points[j]
            if (xi - xj) ** 2 + (yi - yj) ** 2 <= eps * eps:
                union(i, j)

    groups: dict[int, list[tuple[float, float]]] = {}
    for i in range(n):
        r = find(i)
        groups.setdefault(r, []).append(points[i])
    return list(groups.values())


_MAX_HATCH_LINES = 600  # cap for O(n²) clustering; 600 endpoints → 360k comparisons ≈ 2s


def recover_hatch_zones(lines: list[LineString], eps_pt: float = 8.0, min_cluster_lines: int = 4, ratio: float = 0.15) -> list[Polygon]:
    """Given all stroke lines of one color, cluster endpoints and return concave-hull polygons.

    Capped at _MAX_HATCH_LINES: if more, bail (pages with thousands of stroke-matched
    lines are usually basemap noise, not a real hatched zone)."""
    if not lines:
        return []
    if len(lines) > _MAX_HATCH_LINES:
        return []
    pts = []
    owner = []
    for li, ls in enumerate(lines):
        coords = list(ls.coords)
        if not coords:
            continue
        pts.append(coords[0])
        owner.append(li)
        pts.append(coords[-1])
        owner.append(li)

    clusters = _cluster_endpoints(pts, eps=eps_pt)
    polys: list[Polygon] = []
    for cluster in clusters:
        if len(cluster) < min_cluster_lines * 2:
            continue
        mp = MultiPoint(cluster)
        try:
            hull = mp.concave_hull(ratio=ratio) if hasattr(mp, "concave_hull") else mp.convex_hull
        except Exception:
            hull = mp.convex_hull
        if hull.is_empty:
            continue
        if hull.geom_type == "Polygon":
            polys.append(hull)
        elif hull.geom_type == "MultiPolygon":
            polys.extend(list(hull.geoms))
    if not polys:
        return []
    try:
        merged = unary_union(polys)
    except Exception:
        return polys
    if merged.geom_type == "Polygon":
        return [merged]
    return list(merged.geoms)
