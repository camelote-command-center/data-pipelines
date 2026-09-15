import numpy as np,json,pathlib
from PIL import Image,ImageDraw
from scipy.ndimage import label,find_objects
p=pathlib.Path(__file__).resolve().parent;im=Image.open(p.parents[1]/'nyon/sdan-maps/images/eysins-urban.jpeg').convert('RGB');a=np.asarray(im);mask=(a.max(axis=2)<85);lab,n=label(mask);rows=[];draw=ImageDraw.Draw(im)
for i,sl in enumerate(find_objects(lab),1):
 if sl is None:continue
 sub=lab[sl]==i;size=sub.sum();h,w=sub.shape
 if not (12<=size<=300 and 3<=h<=32 and 3<=w<=32 and size/(h*w)>.3):continue
 y,x=np.nonzero(sub);pt=[float(x.mean()+sl[1].start),float(y.mean()+sl[0].start)];j=len(rows);rows.append({'id':j,'point':pt,'pixels':int(size),'bbox':[sl[1].start,sl[0].start,sl[1].stop,sl[0].stop]});draw.text(tuple(pt),str(j),fill='red',stroke_width=1,stroke_fill='white')
p.joinpath('black-components.json').write_text(json.dumps(rows,indent=2)+'\n');im.resize((1048,938)).save(p/'components-overlay.png');print(len(rows))
