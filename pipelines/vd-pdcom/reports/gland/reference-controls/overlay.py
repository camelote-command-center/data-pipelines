import pathlib,json,numpy as np,pymupdf
from PIL import Image,ImageDraw
p=pathlib.Path(__file__).resolve().parent.parent
a=np.array(json.loads((p/'grid-controls/results.json').read_text())['coefficients_pdf_x_y_1_to_lv03']);inv=np.linalg.inv(a[:2]);d=pymupdf.open(p/'sector-currentness/communet-plan.pdf');page=d[0]
features=json.loads((p/'reference-controls/footprints-lv03.json').read_text())['features']
for name,box in [('west',[700,300,1200,920]),('south',[1100,750,1800,920])]:
 clip=pymupdf.Rect(box);pix=page.get_pixmap(matrix=pymupdf.Matrix(1.5,1.5),clip=clip);im=Image.frombytes('RGB',[pix.width,pix.height],pix.samples);draw=ImageDraw.Draw(im)
 for f in features:
  for ring in f['geometry']['rings']:
   xy=(np.array(ring)-a[2])@inv
   pts=[((x-box[0])*1.5,(y-box[1])*1.5) for x,y in xy]
   draw.line(pts,fill=(255,0,200),width=2)
 im.save(p/'reference-controls'/f'{name}-overlay.png')
