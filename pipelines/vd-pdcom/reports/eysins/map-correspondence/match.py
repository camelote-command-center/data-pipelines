from pathlib import Path
import cv2,numpy as np,json,hashlib,io,fitz
from PIL import ImageDraw
from shapely.geometry import MultiPoint,Point,Polygon,shape
from PIL import Image
p=Path(__file__).resolve().parent
for name,digest in json.loads((p/'inputs.json').read_text()).items():
 assert hashlib.sha256((p/name).read_bytes()).hexdigest()==digest,name
assert cv2.__version__=='5.0.0'
a=cv2.imread(str(p/'../../nyon/sdan-maps/images/eysins-urban.jpeg'),0)
with fitz.open(p/'../source-update/5650969.pdf') as pdf:
 im=Image.open(io.BytesIO(pdf.extract_image(4)['image'])).transpose(Image.Transpose.ROTATE_270).resize((1000,1414))
b=cv2.cvtColor(np.array(im),cv2.COLOR_RGB2GRAY);mask=np.zeros_like(b);mask[450:850,300:900]=255

out=[]
for mode in ['gray','dark']:
 aa=a if mode=='gray' else cv2.GaussianBlur((a<95).astype('uint8')*255,(3,3),.6)
 bb=b if mode=='gray' else cv2.GaussianBlur((b<65).astype('uint8')*255,(3,3),.6)
 sift=cv2.SIFT_create(nfeatures=5000,contrastThreshold=.02);ka,da=sift.detectAndCompute(aa,None);kb,db=sift.detectAndCompute(bb,mask);matches=cv2.BFMatcher().knnMatch(da,db,k=2)
 for ratio in [.7,.8,.9]:
  reverse=cv2.BFMatcher().match(db,da)
  candidates=sorted([m for m,n in matches if m.distance<ratio*n.distance and reverse[m.trainIdx].trainIdx==m.queryIdx],key=lambda m:m.distance)
  good=[]
  for m in candidates:
   if all(np.linalg.norm(np.array(ka[m.queryIdx].pt)-ka[v.queryIdx].pt)>2 and np.linalg.norm(np.array(kb[m.trainIdx].pt)-kb[v.trainIdx].pt)>2 for v in good):good.append(m)
  X=np.float32([ka[m.queryIdx].pt for m in good]);Y=np.float32([kb[m.trainIdx].pt for m in good]);cv2.setRNGSeed(5716)
  if len(good)<3:print(mode,ratio,len(good));continue
  M,inliers=cv2.estimateAffinePartial2D(X,Y,method=cv2.RANSAC,ransacReprojThreshold=5,maxIters=10000,confidence=.999,refineIters=20);count=int(inliers.sum());r={'mode':mode,'ratio':ratio,'candidate_count':len(good),'inliers':count,'matrix':M.tolist(),'matches':[{'source':x.tolist(),'target':y.tolist(),'inlier':bool(v)} for x,y,v in zip(X,Y,inliers[:,0])]};out.append(r);print(mode,ratio,len(good),count,M)
(p/'matches.json').write_text(json.dumps(out,indent=2)+'\n')

selected=out[0];assert selected['mode']=='gray' and selected['ratio']==.7
M=np.array(selected['matrix']);A=np.array(json.loads((p/'../pga-grid/calibration.json').read_text())['affine_pixel_to_lv03']);T=A@np.vstack([M,[0,0,1]])
matched=[r for r in selected['matches'] if r['inlier']]
X=np.array([r['source'] for r in matched]);Y=np.array([r['target'] for r in matched]);pred=np.c_[X,np.ones(len(X))]@M.T;delta=(pred-Y)@A[:,:2].T
hull=MultiPoint(X).convex_hull
rings=json.loads((p/'../pixel-boundaries/digitization.json').read_text())['features'];features=[]
for r in rings:
 xy=np.array(r['ring_pixels']);world=np.c_[xy,np.ones(len(xy))]@T.T;poly=Polygon(world);src=Polygon(xy)
 assert poly.is_valid and poly.area>0
 variants=[]
 for fit in out[:3]:
  matrix=np.array(fit['matrix']);variants.append(np.linalg.norm((np.c_[xy,np.ones(len(xy))]@(matrix-M).T)@A[:,:2].T,axis=1))
 features.append({'id':r['id'],'category':r['category'],'ring_lv03':world.tolist(),'gross_illustrated_area_m2':poly.area,'fraction_outside_inlier_hull':1-src.intersection(hull).area/src.area,'max_displacement_across_gray_ratio_fits_m':float(np.max(variants)),'limitation':r['limitation']})
lines=shape(json.loads((p/'../pga-grid/footprint-lines-lv03.json').read_text())['geometry'])
component_ids=[24,27,28,30,33,34,38,41,43,45,47,48,50,51,52,53,54,55,56,58,59,60,61,63,64]
checks=[]
for c in json.loads((p/'../alignment/black-components.json').read_text()):
 if c['id'] not in component_ids:continue
 point=np.r_[c['point'],1]@T.T
 checks.append({'component_id':c['id'],'point_lv03':point.tolist(),'nearest_current_footprint_edge_m':Point(point).distance(lines)})
result={'status':'exploratory_historical_map_correspondence','source_to_pga_review_affine':M.tolist(),'source_to_lv03_affine':T.tolist(),'fit_inlier_count':len(matched),'fit_inlier_rmse_m':float(np.sqrt(np.mean(np.sum(delta**2,axis=1)))),'fit_inlier_max_m':float(np.max(np.linalg.norm(delta,axis=1))),'reference_edge_checks':checks,'features':features,'independent_accuracy_verified':False,'parcel_matching_ready':False,'receiver_release':'not_ready','limits':['SIFT fit residuals are in-sample image agreement, not geographic accuracy.','Ratio variants reuse descriptors and are sensitivity checks, not independent validation.','Current footprint edge distances have no confirmed building identity and are not point accuracy.','PGA printed-grid calibration uncertainty remains additional.','Northern and eastern outlines extrapolate beyond the fitted landmark hull.','Seven historical illustrated envelopes are not legal sectors or current developable land.']}
(p/'correspondence.json').write_text(json.dumps(result,indent=2)+'\n')
canvas=im.convert('RGB');draw=ImageDraw.Draw(canvas)
for r in rings:
 xy=np.c_[r['ring_pixels'],np.ones(len(r['ring_pixels']))]@M.T;draw.line([tuple(v) for v in xy],fill='red',width=2);center=xy[:-1].mean(axis=0);draw.text(tuple(center),r['id'],fill='red',stroke_width=1,stroke_fill='white')
for index,r in enumerate(selected['matches']):
 if not r['inlier']:continue
 x,y=r['target'];draw.ellipse((x-4,y-4,x+4,y+4),outline='blue',width=2);draw.text((x+5,y),str(index),fill='blue')
canvas.crop((300,250,1000,900)).save(p/'historical-map-overlay.png')
source=Image.open(p/'../../nyon/sdan-maps/images/eysins-urban.jpeg').convert('RGB');canvas=Image.new('RGB',(620,190*5),'white');draw=ImageDraw.Draw(canvas)
for n,r in enumerate(matched):
 col=n%2;row=n//2
 for j,(base,pt) in enumerate([(source,r['source']),(im,r['target'])]):
  x,y=pt;crop=base.crop((round(x)-30,round(y)-30,round(x)+30,round(y)+30)).resize((150,150));canvas.paste(crop,(col*310+j*150,row*190+25));cx=col*310+j*150+75;cy=row*190+100;draw.line((cx-8,cy,cx+8,cy),fill='red');draw.line((cx,cy-8,cx,cy+8),fill='red')
 draw.text((col*310,row*190+5),f"Inlier {n+1}: SDAN / PGA",fill='black')
canvas.save(p/'landmark-contexts.png')
print(json.dumps({'fit_rmse_m':result['fit_inlier_rmse_m'],'extrapolation':{r['id']:round(r['fraction_outside_inlier_hull'],3) for r in features},'edge_checks_under_5m':sum(r['nearest_current_footprint_edge_m']<5 for r in checks)}))
