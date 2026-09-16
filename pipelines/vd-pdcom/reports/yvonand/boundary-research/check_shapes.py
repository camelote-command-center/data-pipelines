import pathlib,json,numpy as np,pymupdf as f
from shapely.geometry import shape
from shapely.affinity import affine_transform
from PIL import Image,ImageDraw,ImageFont
p=pathlib.Path(__file__).resolve().parent;j=json.load(open(p/'exploratory-alignment.json'));s={x['drawing_index']:shape(x['geometry_pdf_display_points']) for x in json.load(open(p/'black-path-candidates.json'))['candidates']};t={x['properties']['OBJECTID']:shape(x['geometry']) for x in json.load(open(p/'current-footprints.geojson'))['features']};sc=j['scale_metres_per_pdf_point'];tx,ty=j['translation'];seen={}
for row in j['matches']:
 g=affine_transform(s[row['source_drawing_index']],[sc,0,0,-sc,tx,ty]);h=t[row['reference_OBJECTID']];row['iou']=g.intersection(h).area/g.union(h).area;row['hausdorff_m']=g.hausdorff_distance(h)
 seen.setdefault(row['role'],set());assert row['reference_OBJECTID'] not in seen[row['role']];seen[row['role']].add(row['reference_OBJECTID'])
assert not seen['training']&seen['reserved']
for role in ['training','reserved']:
 rows=[r for r in j['matches'] if r['role']==role];j[role+'_shape_check']={'matches':len(rows),'iou_ge_085':sum(r['iou']>=.85 for r in rows),'hausdorff_le_2m':sum(r['hausdorff_m']<=2 for r in rows),'median_iou':float(np.median([r['iou'] for r in rows])),'maximum_hausdorff_m':max(r['hausdorff_m'] for r in rows)}
j['reserved_unmatched_candidates']=j['reserved_source_candidates']-j['reserved_proximity_area_matches'];(p/'exploratory-alignment.json').write_text(json.dumps(j,indent=2));print(j['training_shape_check'],j['reserved_shape_check'])
# Four spatially spread reserved examples plus worst shape correspondence; selection is for visual inspection only.
rows=[r for r in j['matches'] if r['role']=='reserved'];samples=[]
for axis,sign in [(0,-1),(0,1),(1,-1),(1,1)]:
 row=max(rows,key=lambda r:sign*list(s[r['source_drawing_index']].centroid.coords)[0][axis]);
 if row not in samples:samples.append(row)
samples.append(min(rows,key=lambda r:r['iou']));page=f.open(p.parent/'source-review/source-7.pdf')[0];font=ImageFont.truetype('/System/Library/Fonts/Supplemental/Arial.ttf',18);panels=[]
for row in samples:
 g=s[row['source_drawing_index']];x,y=g.centroid.coords[0];clip=f.Rect(x-75,y-75,x+75,y+75);px=page.get_pixmap(matrix=f.Matrix(3,3),clip=clip);im=Image.frombytes('RGB',[px.width,px.height],px.samples);dr=ImageDraw.Draw(im)
 h=affine_transform(t[row['reference_OBJECTID']],[1/sc,0,0,-1/sc,-tx/sc,ty/sc]);polys=[h] if h.geom_type=='Polygon' else list(h.geoms)
 for poly in polys:dr.line([((xx-clip.x0)*3,(yy-clip.y0)*3) for xx,yy in poly.exterior.coords],fill='#0065ff',width=4)
 panel=Image.new('RGB',(500,520),'white');panel.paste(im,(20,60));di=ImageDraw.Draw(panel);di.text((12,8),f"Reserved path {row['source_drawing_index']} / OID {row['reference_OBJECTID']}",font=font,fill='black');di.text((12,32),f"IoU {row['iou']:.3f}; centroid {row['distance_m']:.2f}m",font=font,fill='black');panels.append(panel)
canvas=Image.new('RGB',(1000,520*((len(panels)+1)//2)),'white')
for i,im in enumerate(panels):canvas.paste(im,((i%2)*500,(i//2)*520))
canvas.save(p/'reserved-footprint-checks.jpg',quality=93)
