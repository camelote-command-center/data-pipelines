import pathlib,json,numpy as np
from shapely.geometry import Polygon,mapping
from shapely.ops import unary_union
p=pathlib.Path(__file__).resolve().parent.parent;q=p/'footprint-overlay';q.mkdir(exist_ok=True)
a=np.array(json.loads((p/'grid-controls/results.json').read_text())['coefficients_pdf_x_y_1_to_lv03']);cover={x['label']:x for x in json.loads((p/'east-checks/review.json').read_text())['combined']['coverage']}
refs=json.loads((p/'reference-controls/footprints-lv03.json').read_text())['features'];buildings=[]
for f in refs:
 rings=f['geometry']['rings'];g=Polygon(rings[0],rings[1:]);assert g.is_valid
 buildings.append((f['attributes'],g))
rows=[]
for f in json.loads((p/'construction-outlines/outlines.json').read_text())['features']:
 xy=np.array(f['pdf_ring']);xy=np.c_[xy,np.ones(len(xy))]@a;g=Polygon(xy);assert g.is_valid
 matches=[]
 for attrs,b in buildings:
  overlap=g.intersection(b).area
  if overlap>0:
   matches.append({**attrs,'overlap_m2':overlap,'footprint_m2':b.area,'footprint_overlap_fraction':overlap/b.area,'above_ground':attrs['GENRE_TXT']=='bâtiment'})
 union=unary_union([b.intersection(g) for attrs,b in buildings if attrs['GENRE_TXT']=='bâtiment' and b.intersects(g)])
 sensitivity=[]
 for radius in [-2,0,2]:
  test=g.buffer(radius);ids=[attrs['OBJECTID'] for attrs,b in buildings if attrs['GENRE_TXT']=='bâtiment' and test.intersection(b).area>1]
  sensitivity.append({'illustrative_buffer_m':radius,'aboveground_ids_overlap_gt_1m2':ids,'count':len(ids)})
 rows.append({'label':f['label'],'crs':'EPSG:21781','geometry':mapping(g),'envelope_area_m2':g.area,'coverage':cover[f['label']],'footprint_matches':matches,'aboveground_union_overlap_m2':union.area,'aboveground_union_fraction':union.area/g.area,'sensitivity':sensitivity})
result={'status':'research_only_not_runtime_or_capacity','coordinate_method':'Frozen PR127 PDF-to-LV03 affine, no refitting; official PR128 reference explicitly requested in EPSG21781. No datum transformation or LV95/4326 conversion.','sources':{'map_sha256':'68f36cb156489e4166d61cb6d0938691d0dcfe489fb666ed05a58dbc98ac77b7','reference_sha256':'8559303dbd9d112afb0c2dab344b273ffb5db98b23cc608df29c6e26406e6f82'},'envelopes':rows,'limits':['Footprint occupancy is not used floor area, unused rights or residual capacity.','Original PDCom polygons and later PPA envelopes remain separate.','Historical/current lineage and any demolition/reconstruction dates not established.','Positive intersections may be raster/alignment edge artifacts; all retained, with separate >1m2 sensitivity.','Plus/minus2m is illustrative, not a confidence interval.','Underground buildings excluded from aboveground unions/counts, retained separately among matches.','Review statuses and extrapolation flags remain unresolved.']}
(q/'analysis.json').write_text(json.dumps(result,ensure_ascii=False,indent=2));print([(x['label'],len(x['footprint_matches']),round(x['aboveground_union_fraction'],3),[a['count'] for a in x['sensitivity']]) for x in rows])
