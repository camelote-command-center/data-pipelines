"""Extract PA5 indicative outline in image pixels only; no geographic transform."""
import argparse,hashlib,json,pathlib,numpy as np,pymupdf as fitz
from scipy import ndimage
from shapely.geometry import box,mapping
from shapely.ops import unary_union
root=pathlib.Path(__file__).resolve().parent
p=argparse.ArgumentParser();p.add_argument('--pdf',type=pathlib.Path,required=True);args=p.parse_args()
sha='b71c426bd9ea2e3aff936f52d2bf5e8dc84144468093803dfd98d921bae47c73'
if hashlib.sha256(args.pdf.read_bytes()).hexdigest()!=sha:raise ValueError('source_sha_mismatch')
with fitz.open(args.pdf) as doc:
 ims=[i for i in doc[69].get_images() if i[2:4]==(834,707)]
 if len(ims)!=1:raise ValueError('unexpected_inset')
 pix=fitz.Pixmap(doc,ims[0][0]);rgb=np.frombuffer(pix.samples,dtype=np.uint8).reshape(pix.height,pix.width,3).astype(float)
# Include pink boundary plus dark red interior; holes infer continuity through overprints.
raw=(rgb[:,:,0]>1.5*rgb[:,:,1])&(rgb[:,:,0]-rgb[:,:,2]>35)
variants=[]
for radius in [1,2,3]:
 yy,xx=np.mgrid[-radius:radius+1,-radius:radius+1];closed=ndimage.binary_closing(raw,structure=xx*xx+yy*yy<=radius*radius);lab,n=ndimage.label(closed);sizes=np.bincount(lab.ravel());sizes[0]=0;filled=ndimage.binary_fill_holes(lab==sizes.argmax());boxes=[]
 for y,row in enumerate(filled):
  d=np.diff(np.r_[False,row,False].astype(int));boxes.extend(box(int(a),y,int(b),y+1) for a,b in zip(np.flatnonzero(d==1),np.flatnonzero(d==-1)))
 g=unary_union(boxes).simplify(.5,preserve_topology=True)
 if not g.is_valid or g.geom_type!='Polygon':raise ValueError('invalid_outline')
 variants.append({'closing_radius_pixels':radius,'area_pixels':g.area,'geometry_pixels':mapping(g)})
result={'source_sha256':sha,'pdf_page':70,'inset_pixels':[834,707],'coordinate_system':'image_pixels_top_left_NOT_geographic','status':'research_only_alignment_rejected','selection':'Pink outer boundary included, not a cadastral boundary; hole filling and closing are explicit inferences.','variants':variants}
(root/'pixel-outlines.json').write_text(json.dumps(result,indent=2)+'\n')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
fig,ax=plt.subplots(figsize=(10,9));ax.imshow(rgb.astype(np.uint8))
for v,col in zip(variants,['yellow','cyan','blue']):
 q=np.array(v['geometry_pixels']['coordinates'][0]);ax.plot(q[:,0],q[:,1],color=col,lw=.8,label=str(v['closing_radius_pixels'])+'px closing')
ax.legend();ax.set_title('PA5 Bex: pixel outline only — geographic alignment rejected');fig.tight_layout();fig.savefig(root/'pixel-outline-review.png',dpi=120);plt.close(fig)
print([(v['closing_radius_pixels'],v['area_pixels']) for v in variants])
