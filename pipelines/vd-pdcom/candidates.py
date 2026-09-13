"""Reviewed PDF-layer selections. Page coordinates only; NOT parcel-ready data.

Templates are tied to exact source hashes. Changing PDF bytes invalidates the
selection until reviewed, even if a layer still has the same name.
"""
import hashlib
import pymupdf as fitz

TEMPLATES={
 5586:('ea2b910b722ba904600e64939905a4dee3847cd160e3d5730d0138cd1595546e',
       'PERIMETRE SITES MAJEURS','Sites majeurs de mutation urbaine'),
 5725:('a4079a88201b716ede0793150102306f8b1faddb55c391a7c2c1696244a96d02',
       'densification','Densification à étudier des zones de faible densité'),
 5642:('9ac3058bc9f837696f69886815f54e65dab848bd67e62df90deb476cebd85300',
       'Calque 1',None),
}


def close_color(a,b,tolerance=.002):
    return a is not None and all(abs(x-y)<tolerance for x,y in zip(a,b))


def serial(value):
    if isinstance(value,(fitz.Point,fitz.Rect,fitz.Quad)):return list(value)
    if isinstance(value,(list,tuple)):return [serial(x) for x in value]
    return value


def extract(data,bfs):
    sha,layer,label=TEMPLATES[bfs]
    if hashlib.sha256(data).hexdigest()!=sha:
        return {'status':'template_review_required','paths':[]}
    result=[]
    with fitz.open(stream=data,filetype='pdf') as doc:
        page=doc[0]
        for index,d in enumerate(page.get_drawings()):
            if d.get('layer')!=layer or d.get('fill') is None:continue
            if bfs==5725 and not close_color(d['fill'],(.996002,.963363,.786709)):continue
            if bfs==5642:
                # Exclude the legend, and retain existing/project distinction as
                # unresolved: identical base fills are overprinted with hatching.
                if d['rect'].x0<680:continue
                label=None
                for rgb,text in [((.900252,.750896,.618265),'Centre ville, à restructurer / densifier'),
                                 ((.980041,.748684,.718975),'Extension du centre, à restructurer / densifier'),
                                 ((1,.89099,.649439),'Habitat moyenne densité, à restructurer / urbaniser')]:
                    if close_color(d['fill'],rgb):label=text;break
                if label is None:continue
            result.append({'page_number':1,'drawing_index':index,'layer':layer,'label':label,
                           'rect':list(d['rect']),'fill':list(d['fill']),
                           'even_odd':d.get('even_odd'),'close_path':d.get('closePath'),
                           'items':serial(d['items'])})
    return {'status':'candidate_vectors','coordinate_system':'PDF points, origin top-left, NOT geographic',
            'validation_status':'requires_georeferencing_and_spatial_QA',
            'semantic_limit':'Selected pilot categories only; not complete plan coverage. Morges hatching existing/project unresolved.',
            'paths':result}
