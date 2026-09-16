"""Untuned footprint-containment review of the frozen historical-map proposal."""
import hashlib
import io
import json
from pathlib import Path

import cv2
import numpy as np
from PIL import Image, ImageDraw
import pymupdf
from shapely.geometry import Point, shape

P = Path(__file__).resolve().parent
for name, digest in json.loads((P / 'inputs.json').read_text()).items():
    assert hashlib.sha256((P / name).read_bytes()).hexdigest() == digest, name
proposal = json.loads((P / '../map-correspondence/correspondence.json').read_text())
T = np.array(proposal['source_to_lv03_affine'])
M = np.array(proposal['source_to_pga_review_affine'])
refs = json.loads((P / 'reference-polygons-lv03.json').read_text())
assert refs['crs'] == 'EPSG:21781'
features = refs['features']
polys = [shape(f['geometry']) for f in features]
assert len(polys) == 690 and all(p.is_valid and not p.is_empty for p in polys)
source = Image.open(P / '../../nyon/sdan-maps/images/eysins-urban.jpeg').convert('RGB')
components = json.loads((P / '../alignment/black-components.json').read_text())
selected = {r['component_id'] for r in proposal['reference_edge_checks']}
checks = []
for comp in components:
    if comp['id'] not in selected:
        continue
    world = np.r_[comp['point'], 1] @ T.T
    point = Point(world)
    order = sorted(range(len(polys)), key=lambda i: (polys[i].distance(point), features[i]['attributes']['OBJECTID']))
    nearby = [i for i in order if polys[i].distance(point) <= 5]
    nearest = order[0]
    checks.append({
        'component_id': comp['id'], 'source_pixel': comp['point'], 'point_lv03': world.tolist(),
        'nearest_polygon_distance_m': polys[nearest].distance(point),
        'nearest_polygon_attributes': features[nearest]['attributes'],
        'containing_footprints': [features[i]['attributes'] for i in nearby if polys[i].covers(point)],
        'footprints_within_5m': [features[i]['attributes'] for i in nearby],
        'identity_verified': False,
    })
result = {
    'method': 'Frozen PR86 transform; no refitting. Centroid containment and distance to filled official polygons, not distance to their outlines.',
    'checks': checks,
    'summary': {
        'selected_components': len(checks),
        'inside_current_footprint': sum(bool(r['containing_footprints']) for r in checks),
        'within_5m_of_polygon': sum(r['nearest_polygon_distance_m'] <= 5 for r in checks),
        'outside_5m_component_ids': [r['component_id'] for r in checks if r['nearest_polygon_distance_m'] > 5],
        'multiple_footprints_within_5m_component_ids': [r['component_id'] for r in checks if len(r['footprints_within_5m']) > 1],
    },
    'independent_accuracy_verified': False,
    'parcel_matching_ready': False,
    'receiver_release': 'not_ready',
}
(P / 'qa.json').write_text(json.dumps(result, indent=2) + '\n')
with pymupdf.open(P / '../source-update/5650969.pdf') as pdf:
    im = Image.open(io.BytesIO(pdf.extract_image(4)['image'])).transpose(Image.Transpose.ROTATE_270).resize((1000, 1414))
pga = Image.fromarray(cv2.warpAffine(np.array(im), cv2.invertAffineTransform(M), source.size,
                                   flags=cv2.INTER_CUBIC, borderValue=(255, 255, 255)))
current = source.copy()
inv = np.linalg.inv(T[:, :2])
for base in [current, pga]:
    draw = ImageDraw.Draw(base)
    for polygon in polys:
        parts = [polygon] if polygon.geom_type == 'Polygon' else list(polygon.geoms)
        for part in parts:
            for ring in [part.exterior, *part.interiors]:
                xy = (np.array(ring.coords) - T[:, 2]) @ inv.T
                draw.line([tuple(v) for v in xy], fill='magenta', width=1)
current.resize((1048, 938)).save(P / 'current-footprint-overlay.png')
# Review the five previously flagged edge-distance cases without changing the transform.
canvas = Image.new('RGB', (900, 1350), 'white')
draw = ImageDraw.Draw(canvas)
byid = {r['id']: r for r in components}
for row, component_id in enumerate([30, 41, 48, 59, 63]):
    x, y = np.rint(byid[component_id]['point']).astype(int)
    for col, base in enumerate([source, pga, current]):
        patch = base.crop((x-25, y-25, x+25, y+25)).resize((250, 250))
        canvas.paste(patch, (col*300, row*270+20))
        draw.text((col*300, row*270), f'{component_id}: '+['SDAN', 'PGA + current', 'SDAN + current'][col], fill='black')
canvas.save(P / 'flagged-component-contexts.png')
print(json.dumps(result['summary']))
# Original source beside the fixed candidate-warped PGA, without fitted corrections.
plain_pga = Image.fromarray(cv2.warpAffine(np.array(im), cv2.invertAffineTransform(M), source.size,
                                         flags=cv2.INTER_CUBIC, borderValue=(255, 255, 255)))
for name, box in [('east', (280, 65, 510, 245)), ('north', (65, 0, 295, 220)), ('south', (180, 265, 340, 435))]:
    w, h = (box[2]-box[0])*3, (box[3]-box[1])*3
    canvas = Image.new('RGB', (w*2, h), 'white')
    for col, base in enumerate([source, plain_pga]):
        canvas.paste(base.crop(box).resize((w, h)), (col*w, 0))
    draw = ImageDraw.Draw(canvas)
    for offset in [0, w]:
        for x in range(box[0], box[2], 20):
            xx = (x-box[0])*3+offset
            draw.line((xx, 0, xx, h), fill='#999999')
            draw.text((xx, 1), str(x), fill='red')
        for y in range(box[1], box[3], 20):
            yy = (y-box[1])*3
            draw.line((offset, yy, offset+w, yy), fill='#999999')
            draw.text((offset, yy), str(y), fill='red')
    canvas.save(P / f'{name}-correspondence-review.png')
