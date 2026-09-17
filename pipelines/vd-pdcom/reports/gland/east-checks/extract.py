import pathlib,json,numpy as np,pymupdf
p=pathlib.Path(__file__).resolve().parent.parent;q=p/'east-checks';q.mkdir(exist_ok=True)
a=np.array(json.loads((p/'grid-controls/results.json').read_text())['coefficients_pdf_x_y_1_to_lv03']);inv=np.linalg.inv(a[:2]);d=pymupdf.open(p/'sector-currentness/communet-plan.pdf')
boxes={'east':[1600,340,1670,770],'northwest':[605,195,770,295]}
for name,box in boxes.items():
 d[0].get_pixmap(matrix=pymupdf.Matrix(3,3),clip=pymupdf.Rect(box)).save(q/f'{name}.png')
 found=[]
 for f in json.loads((p/'reference-controls/footprints-lv03.json').read_text())['features']:
  xy=(np.array(f['geometry']['rings'][0])-a[2])@inv;center=xy[:-1].mean(axis=0)
  if box[0]<center[0]<box[2] and box[1]<center[1]<box[3]:found.append({'attributes':f['attributes'],'pdf_ring':xy.tolist(),'lv03_ring':f['geometry']['rings'][0]})
 (q/f'{name}-reference.json').write_text(json.dumps(found,indent=2));print(name,[(x['attributes'],x['pdf_ring']) for x in found])
(q/'crop-boxes.json').write_text(json.dumps(boxes,indent=2))
