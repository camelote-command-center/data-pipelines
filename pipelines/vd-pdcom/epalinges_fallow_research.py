"""Reproduce held Épalinges source-junction extraction; never persist or qualify."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import fitz
from shapely.affinity import affine_transform
from shapely.geometry import MultiPolygon, Polygon, mapping, shape
from exact_source_nodes import node_existing_vertices

REVIEW=Path(__file__).parent/'reports/epalinges-fallow-junction/review.json'
REVIEW_SHA='9b667afad7439c08ed0522833a89bbc6280e9d94c38f871c5058d9d4f0777643'


def _normal(value):
    if isinstance(value,dict):return {k:_normal(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)):return [_normal(v) for v in value]
    if isinstance(value,(fitz.Point,fitz.Rect,fitz.Quad)):return list(value)
    return value


def extract(source_pdf):
    """Check original operators/ancestry, node existing vertices, apply old frame.

    Geographic coordinates in review.json are separate independently checked
    PostGIS evidence. This offline command regenerates source-space and LV95
    extraction only; it neither calls a database nor assumes source accuracy.
    """
    review_bytes=REVIEW.read_bytes()
    if hashlib.sha256(review_bytes).hexdigest()!=REVIEW_SHA:raise ValueError('fallow_review_changed')
    review=json.loads(review_bytes);raw=Path(source_pdf).read_bytes()
    if hashlib.sha256(raw).hexdigest()!=review['source_sha256']:
        raise ValueError('fallow_source_hash')
    with fitz.open(stream=raw,filetype='pdf') as doc:
        drawings=doc[101].get_drawings(extended=True)
    stack=[];proof=[]
    for i,drawing in enumerate(drawings):
        while stack and stack[-1][1]['level']>=drawing['level']:stack.pop()
        if drawing['type'] in ('clip','group'):
            stack.append((i,drawing));continue
        if i not in (46,47,48):continue
        if not all(item[0]=='l' for item in drawing['items']):raise ValueError('fallow_original_line_operator')
        points=[tuple(drawing['items'][0][1])]+[tuple(item[2]) for item in drawing['items']]
        if points[0]!=points[-1]:raise ValueError('fallow_original_closed_support')
        frozen=review['junction_diagnostic']['paths'][i-46]
        if not Polygon(points).equals(shape(frozen['original_pdf'])):raise ValueError('fallow_original_support_changed')
        proof.append({'extended_index':i,'original_drawing':_normal(drawing),
                      'full_ancestor_chain':[{'extended_index':n,'drawing':_normal(d)} for n,d in stack],
                      'original_support_equal_frozen':True})
    if proof!=review['native_source_proof']['paths']:raise ValueError('fallow_native_ancestry_changed')
    originals=[p['original_pdf'] for p in review['junction_diagnostic']['paths']]
    noded=node_existing_vertices(originals)
    a=review['alignment'];angle,scale,tx,ty=a['parameters'];c,s=math.cos(angle),math.sin(angle)
    p,q=a['pdf_origin_y_flipped'],a['lv95_origin'];h=841.8900146484375
    m=[scale*c,scale*s,scale*s,-scale*c,q[0]+tx-scale*c*p[0]-scale*s*(h-p[1]),q[1]+ty-scale*s*p[0]+scale*c*(h-p[1])]
    transfer=review['page_transfer']['pdf_to_page99']
    lv=[affine_transform(affine_transform(shape(g),transfer),m) for g in noded['geometries']]
    for source,g,frozen in zip(noded['geometries'],lv,review['junction_diagnostic']['paths']):
        if not shape(source).equals_exact(shape(frozen['noded_pdf']),0) or not g.equals_exact(shape(frozen['noded_lv95']),0):
            raise ValueError('fallow_reproduction_changed')
    collection=MultiPolygon(lv)
    if not collection.is_valid:raise ValueError('fallow_invalid_transformed_collection')
    return {'state':'held_research_only_not_delivery_eligible','source_sha256':review['source_sha256'],
            'page':102,'path_ids':review['path_ids'],'source_precision_m':None,'qualification':False,
            'exact_insertions':noded['insertions'],'noded_source_geometries':noded['geometries'],
            'geometry_lv95':mapping(collection),'native_source_proof':review['native_source_proof'],
            'remaining_gates':review['remaining_gates'],'geographic_hold':review['geographic_hold'],
            'no_database_actions':True}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-pdf',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if not args.output.is_absolute():parser.error('--output must be an absolute artifact path')
    result=extract(args.source_pdf)
    args.output.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print('Reproduced 3 held native paths with one exact existing-vertex insertion; no database actions.')

if __name__=='__main__':main()
