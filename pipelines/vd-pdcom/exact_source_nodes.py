"""Preserve existing native vertex/edge junctions before coordinate transforms.

This is source-space noding, not geometry repair. Only exact existing vertices
are inserted into exactly collinear edges. No tolerance, snapping, intersections,
new coordinates, union or transform is applied here. Callers still own all
source identity, semantics, alignment and delivery gates.
"""
from fractions import Fraction
import math

from shapely.geometry import MultiPolygon, Polygon, mapping, shape


def _point(value):
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        raise ValueError('native_node_2d_coordinate_required')
    if any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) for v in value):
        raise ValueError('native_node_finite_coordinate_required')
    return tuple(value)


def _twice_area(ring):
    return sum(Fraction(a[0])*Fraction(b[1])-Fraction(b[0])*Fraction(a[1])
               for a,b in zip(ring,ring[1:]))


def node_existing_vertices(geometries):
    """Return noded simple Polygon supports and an exact insertion ledger.

    Scope deliberately excludes holes, curves, invalid or overlapping source
    collections. Those require their own source/operator review. Input geometry
    dictionaries are never modified.
    """
    rings=[]
    for geometry in geometries:
        if geometry.get('type') != 'Polygon' or len(geometry.get('coordinates', [])) != 1:
            raise ValueError('native_node_simple_polygon_required')
        ring=[_point(v) for v in geometry['coordinates'][0]]
        if len(ring)<4 or ring[0]!=ring[-1] or any(a==b for a,b in zip(ring,ring[1:])):
            raise ValueError('native_node_closed_nondegenerate_ring_required')
        rings.append(ring)
    if not rings or not MultiPolygon([Polygon(r) for r in rings]).is_valid:
        raise ValueError('native_node_valid_source_collection_required')
    vertices=set(v for ring in rings for v in ring[:-1])
    result=[];ledger=[]
    for polygon_index,ring in enumerate(rings):
        noded=[]
        for edge_index,(start,end) in enumerate(zip(ring,ring[1:])):
            x,y=map(Fraction,start);dx,dy=Fraction(end[0])-x,Fraction(end[1])-y
            inside=[]
            for vertex in vertices:
                vx,vy=Fraction(vertex[0])-x,Fraction(vertex[1])-y
                if vx*dy != vy*dx:
                    continue
                t=vx/dx if dx else vy/dy
                if 0<t<1:
                    inside.append((t,vertex))
            noded.append(start)
            for t,vertex in sorted(inside):
                noded.append(vertex)
                ledger.append({'polygon_index':polygon_index,'edge_index':edge_index,
                               'existing_source_vertex':list(vertex),'exact_rational_parameter':str(t)})
        noded.append(noded[0])
        if _twice_area(noded)!=_twice_area(ring) or not Polygon(noded).equals(Polygon(ring)):
            raise ValueError('native_node_source_support_changed')
        result.append(mapping(Polygon(noded)))
    return {'geometries':result,'insertions':ledger}
