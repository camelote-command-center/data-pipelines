"""Render unchanged source with reference footprint outlines using the grid fit."""
import json
from pathlib import Path
import pymupdf
from shapely.geometry import shape, MultiPoint
from shapely.affinity import affine_transform

ROOT = Path(__file__).resolve().parent
r=json.loads((ROOT/'results.json').read_text())
refs=json.loads((ROOT.parent/'boundary-research/current-footprints.geojson').read_text())['features']
by_id={f['properties']['OBJECTID']:shape(f['geometry']) for f in refs}
s,_,_,_,tx,ty=r['matrix'];inverse=[1/s,0,0,-1/s,-tx/s,ty/s]
doc=pymupdf.open(ROOT.parent/'mortaigue/plan-1000.pdf');page=doc[0]
for check in r['checks']:
    g=affine_transform(by_id[check['nearest_reference']['OBJECTID']],inverse)
    for poly in ([g] if g.geom_type=='Polygon' else g.geoms):
        page.draw_polyline([pymupdf.Point(x,y) for x,y in poly.exterior.coords],color=(1,0,1),width=.25)
# Coverage hull is diagnostic only, never an accuracy envelope.
hull=MultiPoint([shape(x['source_pdf_geometry']).centroid for x in r['checks']]).convex_hull
page.draw_polyline([pymupdf.Point(x,y) for x,y in hull.exterior.coords],color=(0,.2,1),width=.5)
page.get_pixmap(matrix=pymupdf.Matrix(2,2),clip=pymupdf.Rect(620,15,1395,390)).save(ROOT/'overview-reference-overlay.png')
selected=[]
for axis,fn in [(0,min),(0,max),(1,min),(1,max)]:
    c=fn(r['checks'],key=lambda c:list(shape(c['source_pdf_geometry']).centroid.coords)[0][axis])
    if c['drawing_index'] not in selected:selected.append(c['drawing_index'])
worst=min(r['checks'],key=lambda c:c['iou'])['drawing_index']
if worst not in selected:selected.append(worst)
out=pymupdf.open()
for i in selected:
    c=next(c for c in r['checks'] if c['drawing_index']==i)
    b=shape(c['source_pdf_geometry']).bounds
    clip=pymupdf.Rect(b)+(-4,-4,4,4)
    pix=page.get_pixmap(matrix=pymupdf.Matrix(8,8),clip=clip)
    dest=out.new_page(width=500,height=380)
    dest.insert_text((15,20),f"drawing {i} / OID {c['nearest_reference']['OBJECTID']} / IoU {c['iou']:.4f}")
    dest.insert_image(pymupdf.Rect(10,35,490,370),pixmap=pix)
out.save(ROOT/'selected-reference-checks.pdf')
(ROOT/'visual-review-selection.json').write_text(json.dumps({'drawing_indices':selected,'selection':'spatial extremes and lowest IoU; not used in fit','reference_outline':'magenta','centroid_hull':'blue; not an accuracy envelope'},indent=2)+'\n')
print('Selected',selected)
