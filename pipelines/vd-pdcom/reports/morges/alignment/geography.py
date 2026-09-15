"""Apply the exploratory transform to six full-base hatch research outlines."""
import json,pathlib,numpy as np
from shapely.geometry import shape,mapping
from shapely.ops import transform
p=pathlib.Path(__file__).resolve().parent;a=json.loads((p/'alignment.json').read_text());angle,scale,tx,ty=a['parameters'];R=np.array([[np.cos(angle),-np.sin(angle)],[np.sin(angle),np.cos(angle)]]);P0=np.array(a['source_origin_y_flipped']);Q0=np.array(a['reference_origin_lv95']);src=json.loads((p.parent/'medium-density/extraction.json').read_text());review=json.loads((p.parent/'hatching/semantic-review.json').read_text());commune=shape(json.loads((p/'commune.geojson').read_text()));features=[]
def geo(x,y,z=None):
 q=(np.column_stack([x,-np.asarray(y)])-P0)@R.T*scale+[tx,ty]+Q0;return q[:,0],q[:,1]
for part in src['variants']['0.25']:
 if part['drawing_index'] not in review['full_base_hatch_paths']:continue
 g=transform(geo,shape(part['geometry_pdf_points']));assert g.is_valid
 prop=dict(drawing_index=part['drawing_index'],area_m2=g.area,outside_morges_m2=g.difference(commune).area,status='research_only_alignment_not_surveyed',overprints='pending; base hatch outline only',source_approval='2012-10-10; later currentness unresolved');features.append(dict(type='Feature',properties=prop,geometry=mapping(g)))
(p/'full-hatch-lv95.geojson').write_text(json.dumps(dict(type='FeatureCollection',coordinate_system='EPSG:2056',features=features)))
