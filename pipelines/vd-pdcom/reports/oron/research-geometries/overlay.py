import pathlib,json,numpy as np,pymupdf
p=pathlib.Path(__file__).resolve().parent.parent;q=p/'research-geometries';r=json.loads((q/'review.json').read_text());inv=np.linalg.inv(np.array(r['composed_pdf77_to_lv95']));refs={f['attributes']['OBJECTID']:f for f in json.loads((p/'official-controls/footprints.json').read_text())['features']};traces=json.loads((p/'densification-candidates/traces.json').read_text())['features']
for row,f in zip(r['features'],traces):
 d=pymupdf.open(str(p/'recovered-source/report.pdf'));pg=d[76]
 for match in row['aboveground_matches']:
  for ring in refs[match['objectid']]['geometry']['rings']:
   xy=np.c_[np.array(ring),np.ones(len(ring))]@inv.T;pg.draw_polyline([pymupdf.Point(*x[:2]) for x in xy],color=(0,.5,1),width=.2)
 pg.draw_polyline([pymupdf.Point(*x) for x in f['ring']],color=(1,0,1),width=.4);xy=np.array(f['ring']);lo=xy.min(axis=0)-5;hi=xy.max(axis=0)+5;pg.get_pixmap(matrix=pymupdf.Matrix(4,4),clip=pymupdf.Rect(*lo,*hi)).save(q/(f['id']+'.png'))
