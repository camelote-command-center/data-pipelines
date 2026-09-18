import pathlib,json,hashlib,numpy as np,pymupdf,cv2
p=pathlib.Path(__file__).resolve().parent.parent;q=p/'intermap-registration';q.mkdir(exist_ok=True);src=p/'recovered-source/report.pdf';raw=src.read_bytes();assert hashlib.sha256(raw).hexdigest()=='f6c73677a1940ff8529d795c60f375b2270a5f124ad946bf6bb14ca987eb7680';d=pymupdf.open(stream=raw,filetype='pdf');imgs=[];keys=[];desc=[];sift=cv2.SIFT_create(nfeatures=8000)
for n in [77,177]:
 pix=d[n-1].get_pixmap(matrix=pymupdf.Matrix(2,2),colorspace=pymupdf.csGRAY);im=np.frombuffer(pix.samples,np.uint8).reshape(pix.height,pix.width);mask=np.zeros_like(im);mask[50:-50,50:-50]=255;k,de=sift.detectAndCompute(im,mask);imgs.append(im);keys.append(k);desc.append(de)
match=cv2.BFMatcher().knnMatch(desc[0],desc[1],k=2);pairs=[]
for first,second in match:
 if first.distance<.7*second.distance:
  x=list(keys[0][first.queryIdx].pt);y=list(keys[1][first.trainIdx].pt);digest=hashlib.sha256(json.dumps([x,y]).encode()).hexdigest();pairs.append({'source_pixel':x,'target_pixel':y,'role':'check' if int(digest[:8],16)%3==0 else 'fit','distance_ratio':first.distance/second.distance})
train=[v for v in pairs if v['role']=='fit'];cv2.setRNGSeed(5805);matrix,inliers=cv2.estimateAffine2D(np.float32([v['source_pixel'] for v in train]),np.float32([v['target_pixel'] for v in train]),method=cv2.RANSAC,ransacReprojThreshold=3,maxIters=5000,confidence=.999,refineIters=10);assert matrix is not None
for v in pairs:v['error_target_pixels']=float(np.linalg.norm(matrix@np.r_[v['source_pixel'],1]-v['target_pixel']))
checks=[v for v in pairs if v['role']=='check'];near=[v for v in checks if v['error_target_pixels']<=3]
pdf=matrix.copy();pdf[:,2]/=2
r={'source_sha256':hashlib.sha256(raw).hexdigest(),'source_page':77,'target_page':177,'render_scale':2,'algorithm':'SIFT8000, ratio<0.7; SHA256 coordinate-pair modulo3 reserved checks before RANSAC fit; RNGseed5805, train-only RANSAC3px, no post-check adjustment.','opencv_version':cv2.__version__,'matches':len(pairs),'fit_count':len(train),'fit_inliers':int(inliers.sum()),'check_count':len(checks),'check_within_3px':len(near),'all_check_median_pixels':float(np.median([v['error_target_pixels'] for v in checks])),'accepted_check_rmse_pixels':float(np.sqrt(np.mean([v['error_target_pixels']**2 for v in near]))),'affine_pdf77_to_pdf177':pdf.tolist(),'status':'provisional_intermap_registration_not_ground_accuracy','limits':['SIFT correspondences may share features/correlated locations; checks are not independent ground controls.','All check residuals preserved including rejected descriptor matches.','Composed geographic accuracy inherits unresolved environment-map reference/generalisation error.','No sector geographic geometry or runtime delivery.']}
(q/'algorithm-result.json').write_text(json.dumps(r,indent=2));(q/'matches.json').write_text(json.dumps(pairs,indent=2));print(r)
# Three source-sector centers compared on both maps; no geometry delivery.
tr=json.loads((p/'densification-candidates/traces.json').read_text())['features']
for i,f in enumerate(tr):
 xy=np.mean(f['ring'][:-1],axis=0);target=pdf@np.r_[xy,1]
 for tag,n,c in [('source',77,xy),('target',177,target)]:
  clip=pymupdf.Rect(c[0]-25,c[1]-25,c[0]+25,c[1]+25);d[n-1].get_pixmap(matrix=pymupdf.Matrix(4,4),clip=clip).save(q/f'sector-{i+1}-{tag}.png')
