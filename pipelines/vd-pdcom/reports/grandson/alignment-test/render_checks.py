import pathlib,json,numpy as np,pymupdf
p=pathlib.Path(__file__).resolve().parent;r=json.loads((p/'results.json').read_text());src={x['drawing_index']:x for x in json.loads((p.parent/'alignment-controls/source-candidates.json').read_text())['candidates']};refs={x['attributes']['OBJECTID']:x for x in json.loads((p.parent/'alignment-controls/official-footprints.json').read_text())['features']};accepted=[x for x in r['reserved'] if x['accepted_correspondence']];chosen=[]
for key in [lambda x:src[x['drawing_index']]['centroid_pdf'][0],lambda x:-src[x['drawing_index']]['centroid_pdf'][0],lambda x:src[x['drawing_index']]['centroid_pdf'][1],lambda x:-src[x['drawing_index']]['centroid_pdf'][1],lambda x:x['iou'],lambda x:-x['iou']]:
 x=min(accepted,key=key)
 if x not in chosen:chosen.append(x)
a,b,d,e,tx,ty=r['affine_pdf_to_lv95'];inv=np.linalg.inv([[a,b],[d,e]]);review=pymupdf.open()
for x in chosen:
 doc=pymupdf.open(p.parent/'source-review/07.pdf');q=doc[0];q.set_rotation(0);idx=x['drawing_index'];path=src[idx]['geometry_pdf_coordinates']['coordinates'][0];allpoints=list(path)
 sh=q.new_shape();sh.draw_polyline(path);sh.finish(color=(0,1,0),width=.8,closePath=True);sh.commit()
 for ring in refs[x['nearest_oid']]['geometry']['rings']:
  pts=(np.array(ring)-[tx,ty])@inv.T;allpoints.extend(pts.tolist());sh=q.new_shape();sh.draw_polyline(pts.tolist());sh.finish(color=(1,0,1),width=.8,closePath=True);sh.commit()
 pts=np.array(allpoints);rect=pymupdf.Rect(pts[:,0].min()-10,pts[:,1].min()-10,pts[:,0].max()+10,pts[:,1].max()+10)&q.rect;pix=q.get_pixmap(matrix=pymupdf.Matrix(5,5),clip=rect);pix.save(p/f'check-{idx}.png');page=review.new_page(width=650,height=650);page.insert_text((20,25),f"Drawing {idx}; official OID {x['nearest_oid']}; IoU {x['iou']:.3f}; centroid {x['distance_m']:.2f}m",fontsize=11);page.insert_text((20,45),'Green: source polygon. Magenta: official footprint. Fixed transform; no correction.',fontsize=10);page.insert_image(pymupdf.Rect(20,65,630,630),pixmap=pix)
review.save(p/'checks.pdf');(p/'visual-checks.json').write_text(json.dumps(chosen,indent=2));print([(x['drawing_index'],x['iou']) for x in chosen])
