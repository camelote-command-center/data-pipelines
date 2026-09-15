"""Medium-density base paths; existing/proposed hatch semantics remain unresolved."""
import argparse,pathlib,json,hashlib,math
import pymupdf as f
from shapely.geometry import Polygon,mapping
root=pathlib.Path(__file__).resolve().parent
SHA='9ac3058bc9f837696f69886815f54e65dab848bd67e62df90deb476cebd85300'
def curve(a,b,c,d,tol,depth=0):
 def distance(p):
  dx,dy=d[0]-a[0],d[1]-a[1];n=math.hypot(dx,dy)
  if not n:return math.dist(a,p)
  u=max(0,min(1,((p[0]-a[0])*dx+(p[1]-a[1])*dy)/(n*n)))
  return math.dist(p,(a[0]+u*dx,a[1]+u*dy))
 if max(distance(b),distance(c))<=tol:return [d]
 if depth>=20:raise ValueError('curve_subdivision_limit')
 mid=lambda p,q:((p[0]+q[0])/2,(p[1]+q[1])/2)
 ab=mid(a,b);bc=mid(b,c);cd=mid(c,d);abc=mid(ab,bc);bcd=mid(bc,cd);m=mid(abc,bcd)
 return curve(a,ab,abc,m,tol,depth+1)+curve(m,bcd,cd,d,tol,depth+1)
def rings(items,tol):
 result=[];run=[]
 for item in items:
  if item[0] not in ('l','c'):raise ValueError('unexpected_base_command')
  a=tuple(item[1]);end=tuple(item[-1])
  if run and math.dist(run[-1],a)>.001:result.append(run);run=[]
  if not run:run.append(a)
  run+=([end] if item[0]=='l' else curve(*[tuple(x) for x in item[1:]],tol))
 if run:result.append(run)
 return result
def extract(pdf,out):
 if hashlib.sha256(pdf.read_bytes()).hexdigest()!=SHA:raise ValueError('source_sha_mismatch')
 doc=f.open(pdf);pg=doc[0];ds=pg.get_drawings();color=(1.,.8909895420074463,.6494392156600952)
 selected=[(i,d) for i,d in enumerate(ds) if d['fill'] and max(abs(a-b) for a,b in zip(d['fill'],color))<1e-6 and d['rect'].x0>650]
 if len(selected)!=40:raise ValueError('unexpected_medium_density_path_count')
 allvariants={}
 for tol in [.1,.25,.5]:
  parts=[]
  for i,d in selected:
   rr=rings(d['items'],tol);g=Polygon(rr[0]) if len(rr)==1 else None
   parts.append({'drawing_index':i,'source_command_count':len(d['items']),'rings_pdf_points':rr,'ring_count':len(rr),'valid_single_polygon':g is not None and g.is_valid,'area_pdf_points_squared':g.area if g else None,'geometry_pdf_points':mapping(g) if g is not None and g.is_valid else None,'temporal_status':'unresolved_existing_vs_proposed','scope':'medium-density base fill only; overprints/clipping not resolved'})
  allvariants[str(tol)]=parts
 result={'source_sha256':SHA,'document_id':'b4deac7f-9349-5463-b2f5-0afdd2bf78e6','pdf_page':1,'coordinate_system':'PDF_points_top_left_NOT_geographic','legend_rgb':color,'legend_text':'Habitat moyenne densité, à restructurer / urbaniser','legend_columns':'existant / projet; same base fill, project adds hatching','detailed_legend_document':'118c86c3-15ed-531e-aeb1-ca151e5284ee','detailed_legend_page':6,'status':'research_only_not_growth_sectors','selected_tolerance_pdf_points':.25,'variants':allvariants,'limitations':['Curve flattening sensitivity, not spatial accuracy.','Raw base paths may contain existing housing and projected urbanisation.','Hatching, overprints, geographic alignment and currentness unresolved.','2023mobility approval does not authorize land-use classification.']}
 out.mkdir(parents=True,exist_ok=True);(out/'extraction.json').write_text(json.dumps(result,indent=2)+'\n');pix=pg.get_pixmap(matrix=f.Matrix(.5,.5));pix.save(str(out/'source.png'))
 import matplotlib
 matplotlib.use('Agg')
 import matplotlib.pyplot as plt
 from PIL import Image
 fig,ax=plt.subplots(figsize=(14,10));ax.imshow(Image.open(out/'source.png'),extent=[0,pg.rect.width,pg.rect.height,0])
 for part in allvariants['0.25']:
  for ring in part['rings_pdf_points']:
   x,y=zip(*ring);ax.plot(x,y,color='blue',lw=.7)
 ax.set_xlim(700,2450);ax.set_ylim(1650,550);ax.set_title('Morges medium-density base paths — existing/proposed NOT yet resolved');fig.tight_layout();fig.savefig(out/'review.png',dpi=110);plt.close(fig);doc.close();return result
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--pdf',type=pathlib.Path,required=True);p.add_argument('--output',type=pathlib.Path,default=root);a=p.parse_args();r=extract(a.pdf,a.output);print({k:{'parts':len(v),'valid':sum(x['valid_single_polygon'] for x in v)} for k,v in r['variants'].items()})
