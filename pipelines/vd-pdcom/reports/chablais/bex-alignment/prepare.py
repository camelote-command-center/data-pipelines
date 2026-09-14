import argparse,hashlib,json,pathlib,numpy as np,pymupdf as f
from scipy import ndimage
root=pathlib.Path(__file__).resolve().parent
p=argparse.ArgumentParser();p.add_argument('--pdf',type=pathlib.Path,required=True);args=p.parse_args()
if hashlib.sha256(args.pdf.read_bytes()).hexdigest()!='7d2fa9b86cb8bb49757eb935bd0735ff3a1476aebc8de39aed4cd39d905f42d5':raise ValueError('source_sha_mismatch')
with f.open(args.pdf) as doc:
 images=[im for im in doc[75].get_images() if im[2:4]==(809,979)]
 if len(images)!=1:raise ValueError('unexpected_inset')
 p=f.Pixmap(doc,images[0][0])
a=np.frombuffer(p.samples,dtype=np.uint8).reshape(p.height,p.width,3).astype(float)
mask=(np.linalg.norm(a-[157,154,149],axis=2)<14)|(np.linalg.norm(a-[126,55,49],axis=2)<14)
l,n=ndimage.label(mask,np.ones((3,3)));sizes=np.bincount(l.ravel());points=[];meta=[]
for idx,sl in enumerate(ndimage.find_objects(l),1):
 ys,xs=sl
 if sizes[idx]<30 or sizes[idx]>5000 or xs.start==0 or ys.start==0 or xs.stop==p.width or ys.stop==p.height:continue
 y,x=ndimage.center_of_mass(mask,l,idx);points.append([x,-y]);meta.append({'id':idx,'pixel_count':int(sizes[idx]),'centroid_top_left':[x,y]})
(root/'source-points.json').write_text(json.dumps(points));(root/'source-components.json').write_text(json.dumps(meta));print(len(points))
