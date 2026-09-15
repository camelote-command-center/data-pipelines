"""Clipping-aware evidence of orange project hatching, not geographic release."""
import pathlib,json,hashlib,sys,argparse
import pymupdf as fitz
from shapely.geometry import Polygon,LineString,box,shape,mapping
from shapely.ops import unary_union
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]/'medium-density'))
from extract import rings,SHA
ORANGE=(.9703211784362793,.5791866779327393,.11464103311300278)
def polygon(d):
 items=d['items']
 if len(items)==1 and items[0][0]=='re':return box(*items[0][1])
 rr=rings(items,.1)
 if len(rr)!=1:raise ValueError('compound_clip_requires_review')
 g=Polygon(rr[0]);assert g.is_valid,'invalid_clip';return g

def run(pdf,output):
 assert hashlib.sha256(pdf.read_bytes()).hexdigest()==SHA
 pg=fitz.open(pdf)[0];drawings=pg.get_drawings(extended=True);clips={};lines=[];special=[];filled=[]
 for i,d in enumerate(drawings):
  level=d['level'];clips={k:v for k,v in clips.items() if k<level}
  if d['type']=='clip':clips[level]=(i,d);continue
  color=d.get('color') if d['type']=='s' else d.get('fill')
  if not color or max(abs(a-b) for a,b in zip(color,ORANGE))>1e-6:continue
  r=d['rect'];items=d['items']
  if d['type']=='s' and len(items)==1 and items[0][0]=='l' and abs(r.width)<.02 and abs(d['width']-.5)<.001:
   if r.x1<0:
    # Pattern tile coordinates are not main-map geometry. Its parent clipping
    # path supplies the domain; two occurrences must be the source and legend.
    outer=[(j,c) for j,c in clips.values() if c['scissor'].x0>0 and c['scissor'].x1>0]
    assert len(outer)==1,'unrecognized_pattern_parent'
    j,c=outer[0];g=polygon(c);special.append(dict(extended_index=i,clip_index=j,domain=mapping(g)));continue
   g=LineString([tuple(items[0][1]),tuple(items[0][2])])
   for _,c in clips.values():g=g.intersection(polygon(c))
   if not g.is_empty:lines.append((i,g))
  elif d['type']=='f' and len(items)<=4 and .45<=r.width<=.55 and r.height>=3:
   g=polygon(d)
   for _,c in clips.values():g=g.intersection(polygon(c))
   if not g.is_empty:filled.append((i,g))
 parts=json.loads((pathlib.Path(__file__).resolve().parents[1]/'medium-density/extraction.json').read_text())['variants']['0.25'];result=[]
 for p in parts:
  base=shape(p['geometry_pdf_points']);interior=base.buffer(-.5);lh=[(i,g.intersection(interior)) for i,g in lines if g.intersects(interior)];fh=[(i,g.intersection(interior)) for i,g in filled if g.intersects(interior)];sp=[s for s in special if shape(s['domain']).intersection(interior).area>1]
  length=sum(g.length for _,g in lh);area=sum(g.area for _,g in fh)
  result.append(dict(drawing_index=p['drawing_index'],clipped_line_count=sum(g.length>0 for _,g in lh),clipped_line_length_pdf_points=length,thin_fill_count=sum(g.area>0 for _,g in fh),thin_fill_area_pdf_points_squared=area,pattern_domain_count=len(sp),pattern_domain_fraction=sum(shape(s['domain']).intersection(base).area for s in sp)/base.area,illustrative_hatch_support_ratio=(length*3+area*6)/interior.area,limitations='Support ratio is a diagnostic only, not exact project area; later overprints and fragment grouping still require review.'))
 out=dict(source_sha256=SHA,pdf_page=1,coordinate_system='PDF_points_top_left_NOT_geographic',clipping_tolerance_pdf_points=.1,base_boundary_exclusion_pdf_points=.5,orange_vertical_strokes=len(lines),orange_thin_fills=len(filled),pattern_domains=special,paths=result)
 output.mkdir(parents=True,exist_ok=True);(output/'support.json').write_text(json.dumps(out,indent=2)+'\n');return out
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--pdf',required=True,type=pathlib.Path);p.add_argument('--output',required=True,type=pathlib.Path);a=p.parse_args();o=run(a.pdf,a.output)
 for r in o['paths']:print(r['drawing_index'],r['clipped_line_count'],r['thin_fill_count'],round(r['illustrative_hatch_support_ratio'],3),r['pattern_domain_count'])
