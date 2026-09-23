"""Source glyph anchors, never cadastral location estimates or filled surfaces."""
import numpy as np
from shapely.geometry import Point,LineString,Polygon

def anchor(path,rule):
    support=path['support']
    if rule=='crossing_centerline_review':
        v=np.array(support['vertices'],dtype=float)
        if v.shape!=(4,2) or np.linalg.norm(v[0]+v[2]-v[1]-v[3])>=.01:raise ValueError('symbol_incomplete_parallelogram')
        g=Polygon(v);pt=LineString([v[0],v[2]]).intersection(LineString([v[1],v[3]]))
        if not g.is_valid or pt.geom_type!='Point' or not g.contains(pt):raise ValueError('symbol_invalid_diagonal_anchor')
        return pt
    if rule not in ('star_symbol_center','ring_symbol_center','outer_circle_center'):raise ValueError('symbol_unreviewed_anchor_rule')
    s=support[0];v=np.array(s['start_vertices'],dtype=float);centre=v.mean(axis=0);r=np.linalg.norm(v-centre,axis=1)
    if rule=='star_symbol_center':
        if v.shape!=(10,2) or s['operators']!=['l']*10 or max(np.ptp(r[::2]),np.ptp(r[1::2]))>=.02 or min(r[::2])<=max(r[1::2]):raise ValueError('symbol_nonregular_star')
        # Both alternating vertex sets must be equally spaced five-point rings.
        for ring in (v[::2],v[1::2]):
            angles=np.sort(np.arctan2(ring[:,1]-centre[1],ring[:,0]-centre[0]));steps=np.diff(np.r_[angles,angles[0]+2*np.pi])
            if np.max(abs(steps-2*np.pi/5))>.01:raise ValueError('symbol_star_angles')
    else:
        if v.shape!=(4,2) or s['operators']!=['c']*4 or np.ptp(r)>=.01 or np.linalg.norm(v[0]+v[2]-v[1]-v[3])>=.01:raise ValueError('symbol_non_circular_quarters')
    if not np.isfinite(centre).all():raise ValueError('symbol_nonfinite')
    return Point(centre)
