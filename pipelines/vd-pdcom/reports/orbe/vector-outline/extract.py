"""Orbe vector evidence in PDF points; intentionally not geographic geometry."""
import argparse,hashlib,json,pathlib,math
import pymupdf as f
from shapely.geometry import Polygon,mapping
ROOT=pathlib.Path(__file__).resolve().parent
SHA='8b78d42eb83210b6b63404dd8b4270fedd15d1b7e3d65d2a403d6edcee654a3c'
def runs(items):
 result=[];run=[]
 for item in items:
  if item[0]!='l':raise ValueError('unexpected_path_command')
  _,a,b=item
  if run and math.dist(run[-1],a)>.001:result.append(run);run=[]
  if not run:run.append(list(a))
  run.append(list(b))
 if run:result.append(run)
 return result
def extract(pdf,out):
 if hashlib.sha256(pdf.read_bytes()).hexdigest()!=SHA:raise ValueError('source_sha_mismatch')
 doc=f.open(pdf);page=doc[0];ds=page.get_drawings()
 reds=[(i,d) for i,d in enumerate(ds) if d['color']==(1.,0.,0.) and len(d['items'])>100]
 if len(reds)!=2:raise ValueError('unexpected_red_path_count')
 main=[x for x in reds if 750<x[1]['rect'].x0 and x[1]['rect'].x1<1500]
 inset=[x for x in reds if x[1]['rect'].x0>1500]
 if len(main)!=1 or len(inset)!=1:raise ValueError('main_inset_ambiguous')
 chains=runs(main[0][1]['items'])
 if len(chains)!=2 or any(math.dist(c[0],c[-1])>.001 for c in chains):raise ValueError('boundary_not_two_closed_rings')
 polys=sorted([Polygon(c) for c in chains],key=lambda p:p.area)
 if not all(p.is_valid for p in polys) or not polys[1].contains(polys[0]):raise ValueError('boundary_rings_not_nested')
 inner,outer=polys;band=outer.difference(inner)
 # Verify matching even-odd red fill: exporter draws band then outlines its edges.
 fill=ds[main[0][0]-1]
 if not fill['even_odd'] or fill['rect']!=main[0][1]['rect'] or abs(fill['fill'][0]-.866667)>.00001:raise ValueError('expected_evenodd_band_missing')
 pink=[]
 for i,d in enumerate(ds):
  color=d['fill'];rect=d['rect']
  if not color or max(abs(a-b) for a,b in zip(color,(.937255,.560784,.847059)))>.00001 or not (750<rect.x0 and rect.x1<1500):continue
  for chain in runs(d['items']):
   poly=Polygon(chain) if len(chain)>=3 else None
   # Filled PDF paths implicitly close; preserve that fact and tiny fragments.
   pink.append({'drawing_index':i,'raw_points':chain,'implicit_close':chain[0]!=chain[-1], 'valid_polygon':bool(poly is not None and poly.is_valid and poly.area>0),'area_pdf_points_squared':poly.area if poly else 0,'geometry_pdf_points':mapping(poly) if poly is not None and poly.is_valid and poly.area>0 else None})
 result={'source_sha256':SHA,'pdf_page':1,'coordinate_system':'PDF_points_top_left_NOT_geographic','status':'research_only','priority_densification':{'semantic_territories':1,'drawing_index':main[0][0],'inner':mapping(inner),'outer':mapping(outer),'band':mapping(band),'inner_area_pdf_points_squared':inner.area,'outer_area_pdf_points_squared':outer.area,'band_area_pdf_points_squared':band.area,'interpretation':'Two nested rings bound one cartographic band. Neither is an independently validated legal or cadastral boundary.'},'excluded_inset':{'drawing_index':inset[0][0],'reason':'Separate centre map1:5000 versus main1:10000; clipped duplicate depiction, not an extra territory.'},'medium_term_mixed_use':{'legend_rgb':[.937255,.560784,.847059],'source_path_parts':pink,'interpretation':'Unmerged source fill fragments, not counted as final planning sectors. Tiny fragments retained; semantic grouping and clipping review pending.'},'limits':['No geographic alignment or parcel joins.','Exact-version approval remains unverified.','Other legend categories, including orange hatching, are not yet extracted.']}
 out.mkdir(parents=True,exist_ok=True);(out/'extraction.json').write_text(json.dumps(result,indent=2)+'\n')
 pix=page.get_pixmap(matrix=f.Matrix(.6,.6));pix.save(str(out/'source-overview.png'))
 import matplotlib
 matplotlib.use('Agg')
 import matplotlib.pyplot as plt
 from PIL import Image
 fig,ax=plt.subplots(figsize=(10,12));ax.imshow(Image.open(out/'source-overview.png'),extent=[0,page.rect.width,page.rect.height,0])
 for g,col,label in [(outer,'cyan','Outer band edge'),(inner,'blue','Inner band edge')]:
  x,y=g.exterior.xy;ax.plot(x,y,color=col,lw=.8,label=label)
 for part in pink:
  if part['geometry_pdf_points']:
   q=Polygon(part['geometry_pdf_points']['coordinates'][0]);x,y=q.exterior.xy;ax.plot(x,y,color='magenta',lw=1)
 ax.set_xlim(750,1400);ax.set_ylim(1150,400);ax.legend();ax.set_title('Orbe main map: one banded territory + mixed-use source parts\nPDF coordinates only; inset excluded');fig.tight_layout();fig.savefig(out/'extraction-review.png',dpi=130);plt.close(fig);doc.close();return result
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--pdf',type=pathlib.Path,required=True);p.add_argument('--output',type=pathlib.Path,default=ROOT);a=p.parse_args();r=extract(a.pdf,a.output);print(json.dumps({'territories':r['priority_densification']['semantic_territories'],'mixed_parts':len(r['medium_term_mixed_use']['source_path_parts']),'valid_parts':sum(p['valid_polygon'] for p in r['medium_term_mixed_use']['source_path_parts'])}))
