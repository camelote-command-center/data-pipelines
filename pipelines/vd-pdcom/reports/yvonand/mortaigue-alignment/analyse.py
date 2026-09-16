"""Grid-only Mortaigue overview calibration; independent current-footprint checks."""
import argparse, hashlib, json, math
from pathlib import Path
import numpy as np
import pymupdf
from shapely.geometry import Polygon, shape, mapping, box, MultiPoint
from shapely.affinity import affine_transform

HASH = '4de692dfdbfa3e3ba280d97950d0d770ec744756ca33b853346f47b745cf3f1a'

def analyse(pdf, reference, contour):
    assert hashlib.sha256(pdf.read_bytes()).hexdigest() == HASH
    doc = pymupdf.open(pdf)
    paths = doc[0].get_drawings()
    # Literal labels visually read from the overview, with native line positions.
    x1 = paths[805]['rect'].x0
    x2 = paths[806]['rect'].x0
    y1 = paths[807]['rect'].y0
    assert paths[805]['rect'].width == paths[806]['rect'].width == 0
    assert paths[807]['rect'].height == 0
    scale = 500 / (x2 - x1)
    matrix = [scale, 0, 0, -scale, 2546000 - scale*x1, 1183500 + scale*y1]
    # Same x/y scale and no rotation assumed from cartographic grid. Only one
    # northing line is visible, so no independent vertical grid spacing exists.
    panel = box(625.6592407226562,24.604793548583984,1375.93798828125,371.9512939453125)
    assert hashlib.sha256(reference.read_bytes()).hexdigest() == json.loads((reference.parent/'reference-receipt.json').read_text())['file_sha256']
    refs = json.loads(reference.read_text())['features']
    polygons = [shape(f['geometry']) for f in refs]
    centroids = np.array([[g.centroid.x,g.centroid.y] for g in polygons])
    checks=[]; excluded=[]
    for i,p in enumerate(paths[:805]):
        if not p['fill'] or max(abs(v-0.6980392336845398) for v in p['fill'])>1e-5:
            continue
        if not panel.covers(box(*p['rect'])):
            excluded.append({'drawing_index':i,'reason':'not wholly inside overview panel'})
            continue
        points=[];reason=None
        for item in p['items']:
            if item[0]!='l':reason='non-line path';break
            start,end=list(item[1]),list(item[2])
            if not points:points.append(start)
            if start!=points[-1]:reason='discontinuous path';break
            points.append(end)
        if reason or len(points)<3:
            excluded.append({'drawing_index':i,'reason':reason or 'too few points'});continue
        # Fill semantics close the ring even when closePath is false.
        g=Polygon(points)
        if not g.is_valid:
            excluded.append({'drawing_index':i,'reason':'invalid polygon'});continue
        # Later white title/scale boxes hide raw source paths. Exclude any
        # positive-area overlap before selecting independent visible checks.
        masks = [box(*paths[j]['rect']) for j in [1565,1650]]
        if any(g.intersection(mask).area > 0 for mask in masks):
            excluded.append({'drawing_index':i,'reason':'overpainted by overview title/scale box'})
            continue
        ground=affine_transform(g,matrix)
        distances=np.linalg.norm(centroids-[ground.centroid.x,ground.centroid.y],axis=1)
        j=int(np.argmin(distances));distance=float(distances[j]);r=polygons[j]
        accepted=distance<=10
        checks.append({'drawing_index':i,'source_pdf_geometry':mapping(g),'source_lv95_geometry':mapping(ground),'source_area_m2':ground.area,'nearest_reference':refs[j]['properties'],'centroid_distance_m':distance,'within_10m':accepted,'iou':ground.intersection(r).area/ground.union(r).area if accepted else None,'hausdorff_m':ground.hausdorff_distance(r) if accepted else None})
    matches=[x for x in checks if x['within_10m']]
    counts={}
    for x in matches:
        key=x['nearest_reference']['OBJECTID'];counts[key]=counts.get(key,0)+1
    target=affine_transform(shape(json.loads(contour.read_text())['geometry']),matrix)
    parcel_path=reference.parent.parent/'proposed-perimeters/current-parcels.geojson'
    assert hashlib.sha256(parcel_path.read_bytes()).hexdigest() == json.loads((parcel_path.parent/'reference-receipt.json').read_text())['file_sha256']
    parcels=json.loads(parcel_path.read_text())['features'];intersections=[]
    for f in parcels:
        r=shape(f['geometry']);a=target.intersection(r).area
        if a>1:intersections.append({'properties':f['properties'],'intersection_m2':a,'fraction_of_agp':a/target.area,'fraction_of_current_parcel':a/r.area})
    hull=MultiPoint([shape(x['source_lv95_geometry']).centroid for x in matches]).convex_hull
    summary={'candidate_count':len(checks),'excluded_count':len(excluded),'matches_within_10m':len(matches),'unmatched':len(checks)-len(matches),'unique_reference_oids':len(counts),'duplicate_reference_oids':[k for k,v in counts.items() if v>1], 'centroid_rmse_m':math.sqrt(sum(x['centroid_distance_m']**2 for x in matches)/len(matches)), 'median_iou':float(np.median([x['iou'] for x in matches])), 'iou_below_085':sum(x['iou']<.85 for x in matches),'hausdorff_over_2m':sum(x['hausdorff_m']>2 for x in matches),'max_hausdorff_m':max(x['hausdorff_m'] for x in matches),'agp_area_m2':target.area,'source_stated_area_m2':10411,'agp_fraction_outside_check_hull':target.difference(hull).area/target.area,'agp_nearest_check_centroid_m':min(target.distance(shape(x['source_lv95_geometry']).centroid) for x in matches)}
    summary['area_difference_from_stated_m2'] = target.area - 10411
    summary['area_difference_from_stated_percent'] = 100*(target.area/10411-1)
    return {'source_sha256':HASH,'reference_sha256':hashlib.sha256(reference.read_bytes()).hexdigest(),'parcel_reference_sha256':hashlib.sha256(parcel_path.read_bytes()).hexdigest(),'fit_method':'grid only; no reference-footprint fitting, snapping or correction','grid':{'x_2546000':x1,'x_2546500':x2,'y_1183500':y1,'assumption':'north-up equal x/y scale; only one northing gridline'},'matrix':matrix,'summary':summary,'checks':checks,'excluded':excluded,'agp_lv95_geometry':mapping(target),'research_intersections':intersections,'release':'research_only; currentness and spatial extrapolation limits retained'}

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--pdf',type=Path,required=True);parser.add_argument('--reference',type=Path,required=True);parser.add_argument('--contour',type=Path,required=True);parser.add_argument('--out',type=Path,required=True);a=parser.parse_args();result=analyse(a.pdf,a.reference,a.contour);a.out.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n');print(json.dumps(result['summary'],indent=2))
