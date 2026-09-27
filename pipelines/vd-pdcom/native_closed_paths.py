"""Decode one explicitly closed native PDF contour without inferred closures."""
import math
from shapely.geometry import Polygon,box

def flatten_cubic(p0,p1,p2,p3,tolerance=.03,depth=0):
    if not 0<tolerance<=.03:raise ValueError('native_curve_tolerance')
    def distance(p,a,b):
        dx,dy=b[0]-a[0],b[1]-a[1];n=dx*dx+dy*dy
        t=max(0,min(1,((p[0]-a[0])*dx+(p[1]-a[1])*dy)/n)) if n else 0
        return math.hypot(p[0]-a[0]-t*dx,p[1]-a[1]-t*dy)
    if max(distance(p1,p0,p3),distance(p2,p0,p3))<=tolerance:return [list(p0),list(p3)]
    if depth>=20:raise ValueError('native_curve_subdivision_limit')
    def mid(a,b):return [(a[0]+b[0])/2,(a[1]+b[1])/2]
    a=mid(p0,p1);b=mid(p1,p2);c=mid(p2,p3);d=mid(a,b);e=mid(b,c);m=mid(d,e)
    return flatten_cubic(p0,a,d,m,tolerance,depth+1)[:-1]+flatten_cubic(m,e,c,p3,tolerance,depth+1)

def closed_polygon(items,tolerance=.03):
    if len(items)==1 and items[0][0]=='re':
        g=box(*items[0][1])
    else:
        pts=[]
        for it in items:
            if it[0] not in ('l','c'):raise ValueError('native_unsupported_operator')
            start=list(it[1]);end=list(it[-1])
            if pts and math.dist(pts[-1],start)>1e-8:raise ValueError('native_disconnected_contour')
            if not pts:pts=[start]
            pts.extend([end] if it[0]=='l' else flatten_cubic(*it[1:],tolerance=tolerance)[1:])
        if len(pts)<4 or math.dist(pts[0],pts[-1])>1e-8:raise ValueError('native_open_contour')
        g=Polygon(pts)
    if g.is_empty or not g.is_valid or g.area<=0:raise ValueError('native_invalid_contour')
    return g
