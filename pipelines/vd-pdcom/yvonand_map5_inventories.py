"""Three partial historical map5 inventory supports, private review only."""
import gzip,hashlib,json,uuid,math,fitz,collections
from pathlib import Path
from psycopg2.extras import Json
from shapely.geometry import Polygon,shape,MultiPoint,LineString
from shapely.ops import unary_union,polygonize
from shapely.affinity import affine_transform
from official_references import refresh
from cadastral_types import KINDS
DOC='df9afa21-6c06-56ee-85fb-bc8004ee01f0'
SHA='fc9b35b51f8120f4445131e29b31b48c5b987fc3f32fab3cf1d9301aa9663d5c'
BATCH_SHA='c6f188ec1694e43ce99f4c92c53bad8b4cde7fc167c0ba63631708526630798a'
ROOT=Path(__file__).parent/'reports/yvonand-map5-inventories'
KEYS={'blue','orange','red'}
LABELS={'blue':'Recensement architecturale (note 1 à 3)','orange':'Périmètres archéologiques','red':'Biotope du Moulin'}
WARNING='Les périmètres des différents inventaires figurent à titre indicatif ; seuls les plans originaux font foi.'
def sector_id(key):
    if key not in KEYS:raise ValueError('yvonand_map5_scope')
    return str(uuid.uuid5(uuid.NAMESPACE_URL,DOC+'#yvonand-map5-inventories-v1#'+key))
def fail(ok,name):
    if not ok:raise ValueError('yvonand_map5_'+name)
def native_polygon(r,matrix):
    items=r['items']
    if len(items)==1 and items[0][0]=='qu':
        q=items[0][1];pts=[q[0],q[1],q[3],q[2],q[0]]
    else:
        pts=[items[0][1]]
        for item in items:
            fail(item[0]=='l'and item[1]==pts[-1],'literal_chain');pts.append(item[2])
        fail(pts[-1]==pts[0],'explicit_closure')
    g=Polygon([list(fitz.Point(p)*fitz.Matrix(*matrix))for p in pts]);fail(g.is_valid and g.area>0,'native_valid');return g

def validate(b):
    fail((b['document_id'],b['source_sha256'])==(DOC,SHA),'source')
    fail(b['source_precision_m']is None and b['publication_status']=='internal_review_only'and b['review_status']=='review_required','private_unknown_precision')
    fail(b['municipal_currentness']['classification']=='historical_current_applicability_unestablished','currentness')
    n=b['source_native_contract'];fail(n['literal_inventory_warning']==WARNING,'inventory_authority')
    fail(len(n['components'])==66 and len(n['selected_denominator'])==70 and len(n['exceptions'])==4 and {c['extended_index']for c in n['exceptions']}=={3807,3820,4148,24429},'native_denominator')
    raw=gzip.decompress((ROOT/'map-5-full-paint.json.gz').read_bytes());fail(hashlib.sha256(raw).hexdigest()==n['full_paint_ledger']['sha256'],'paint_hash');ledger=json.loads(raw);rows=ledger['records'];fail(len(rows)==24739 and ledger['source_sha256']==SHA,'paint_scope')
    selected={r['extended_index']for r in rows if r.get('fill')==[0,0,1]or(r['type']=='clip'and r['level']==1)}
    fail({c['extended_index']for c in n['selected_denominator']}==selected=={c['extended_index']for c in n['components']+n['exceptions']}and b['native_exceptions']==n['exceptions'],'all_selected_accounted')
    for c in n['exceptions']:fail(c['source_native_record']==rows[c['extended_index']],'unchanged_exception')
    reg=b['registration'];frame=reg['frame'];fail(frame['scale']==.5 and frame['translation']==[418.08001708984375,310.3800048828125] and frame['exemplar_source7_path']==1342 and frame['exemplar_source5_path']==841,'frozen_frame')
    source7raw=(ROOT.parent/'yvonand-existing-vocations/areas.json').read_bytes();fail(hashlib.sha256(source7raw).hexdigest()=='b994b84628118286d6795bbbb000d666cef59ba0420d225d1c340efff99bf506','original_control_ledger');old=json.loads(source7raw);oldcontrols={c['source_drawing_index']:c for c in old['reviewed_control_identities']['selected']}
    aa,bb,cc,dd,ee,ff=old['affine'];tx,ty=frame['translation'];aff=[aa/.5,bb/.5,cc/.5,dd/.5,ee-(aa*tx+bb*ty)/.5,ff-(cc*tx+dd*ty)/.5];fail(b['affine']==reg['source5_affine']==aff,'affine')
    controls=reg['controls'];fail(len(controls)==19 and {c['source7_path']for c in controls}==set(oldcontrols),'all19_controls');reserved=[]
    for c in controls:
        oldc=oldcontrols[c['source7_path']];fail(c['role']==oldc['role']and c['reference_geometry']==oldc['reference_geometry']and c['reference_attributes']==oldc['reference_attributes'],'control_identity')
        source_record=next(r for r in rows if r['drawing_index']==c['source5_path']);items=source_record['items']
        if c['source5_path']in [1132,2499]:
            fail(source_record['even_odd']and all(z[0]=='l'for z in items),'control_evenodd');edges=[(tuple(z[1]),tuple(z[2]))for z in items];counts=collections.Counter(tuple(sorted(z))for z in edges);repeat=[(k,v)for k,v in counts.items()if v%2==0];fail(len(repeat)==1 and repeat[0][1]==2,'one_exact_retrace');edge=repeat[0][0];fail(edges.count(edge)==edges.count(tuple(reversed(edge)))==1,'reverse_edge_pair');faces=list(polygonize([LineString(k)for k,v in counts.items()if v%2]));fail(len(faces)==1 and faces[0].is_valid and faces[0].area==abs(Polygon([z[0]for z in edges]).area),'exact_evenodd_face');literal=Polygon([list(fitz.Point(v)*fitz.Matrix(*ledger['rotation_matrix']))for v in faces[0].exterior.coords])
        else:literal=native_polygon(source_record,ledger['rotation_matrix'])
        fail(literal.equals(shape(c['geometry_display'])),'native_control_geometry')
        g=shape(c['geometry_display']);world=affine_transform(g,aff);res=world.centroid.distance(shape(c['reference_geometry']).centroid)
        fail(world.equals_exact(shape(c['geometry_lv95']),0)and res==c['centroid_residual_m'],'recomputed_control')
        if c['role']=='reserved':reserved.append(c)
    diag=b['source5_reserved_diagnostic'];fail(len(reserved)==8 and diag['controls']==reserved and (diag['original_denominator'],diag['original_matched'],diag['original_unmatched'])==(248,114,134),'reserved_denominators');rmse=math.sqrt(sum(c['centroid_residual_m']**2 for c in reserved)/8);fail(diag['rmse_m']==reg['same8_reserved_filtered_rmse_m']==rmse,'source5_diagnostic')
    hull=MultiPoint([shape(c['geometry_display']).centroid for c in controls if c['role']=='training']).convex_hull;fail(hull.equals_exact(shape(reg['training_hull_display']),0),'hull')
    boundary=json.loads((ROOT.parent/'yvonand-commercial-support/registered-commune-boundary.json').read_text());commune=unary_union([shape(f['geometry'])for f in boundary['features']]);accepted={};held=set()
    for c in n['components']:
        r=rows[c['extended_index']];fail(r==c['source_native_record']and not c['visible_paint_area'],'native_commands');g=native_polygon(r,ledger['rotation_matrix']);fail(g.equals_exact(shape(c['geometry_display_points']),0),'native_geometry')
        if c['support_kind']=='outer_pattern_clip_support':fail(c['paint_children']==rows[c['extended_index']+1:c['extended_index']+3] and r['type']=='clip'and r['level']==1,'pattern_semantics')
        else:fail(r['fill']==[0,0,1]and r['fill_opacity']==1 and r['level']==1,'blue_paint')
        if hull.covers(g)and commune.covers(affine_transform(g,aff)):accepted[c['extended_index']]=c
        else:held.add(c['extended_index'])
    fail(len(accepted)==37 and len(held)==29 and {c['extended_index']for c in b['whole_geometry_holds']}==held,'whole_holds')
    raw=gzip.decompress((ROOT/'official-complete-reference.geojson.gz').read_bytes());fail(hashlib.sha256(raw).hexdigest()==b['official_reference']['sha256'],'reference_hash');refs=json.loads(raw)['features'];byid={r['properties']['EGRID']:r for r in refs};fail(len(refs)==len(byid)==b['official_reference']['count']==666 and b['official_reference']['count_verified'],'reference_count')
    for r in refs:fail(shape(r['geometry']).is_valid and r['properties']['NO_COM_FED']==5939 and r['properties']['GENRE_TXT']in KINDS,'reference_identity')
    fail(len(b['features'])==3 and {f['key']for f in b['features']}==KEYS,'three_categories');seen=set();total=0
    for f in b['features']:
        key=f['key'];fail(f['label']==f['literal_legend']==LABELS[key]and not f['category_complete'],'literal_partial');gs=[]
        ids=[c['extended_index']for c in f['components']];fail(ids==f['path_ids'] and len(set(ids))==len(ids),'path_ids')
        for c in f['components']:
            idx=c['extended_index'];fail(idx in accepted and accepted[idx]['category']==key and c['native_support']==accepted[idx],'accepted_support');g=affine_transform(shape(accepted[idx]['geometry_display_points']),aff);fail(g.equals_exact(shape(c['geometry_lv95']),0)and c['whole_hull_covered']and c['whole_registered_commune_covered'],'whole_geometry');gs.append(g);seen.add(idx)
        g=shape(f['geometry_lv95']);fail(g.is_valid and g.equals(unary_union(gs))and g.equals_exact(shape(f['geometry_lv95_research_only']),0),'union');pairs=f['expected_pairs'];expect={k:r for k,r in byid.items()if g.intersection(shape(r['geometry'])).area>0};fail(len(pairs)==len(expect)=={'blue':36,'orange':55,'red':1}[key]and {p['egrid']for p in pairs}==set(expect),'all_pairs')
        for p in pairs:
            rr=expect[p['egrid']];ref=shape(rr['geometry']);i=g.intersection(ref);a=rr['properties'];fail(p['attributes']==a and p['geometry']==rr['geometry']and i.equals_exact(shape(p['overlap_geometry_lv95']),0),'pair_geometry');fail((p['overlap_m2'],p['parcel_area_m2'],p['overlap_fraction'])==(i.area,ref.area,min(1,i.area/ref.area)),'pair_areas');fail(p['boundary_review_required']==(ref.distance(g.boundary)<=10)and p['uncertainty_buffer_m']==10 and p['contextual_only'],'pair_flags');kind='ddp_superficie'if a['GENRE_TXT']=='DDP superficie'else'ddp_source'if a['GENRE_TXT']=='DDP source'else'bien_fonds';fail(p['cadastral_object_kind']==kind,'object_kind')
        total+=len(pairs)
    fail(seen==set(accepted)and total==92,'complete_accepted_batch')
    fail(b['independent_native_qa']['status'].startswith('accepted')and b['independent_registration_qa']['decision']=='fixed_source5_frame_and_whole_hull_checks_independently_verified'and b['independent_parcel_qa']['decision']=='accept_three_partial_historical_inventory_categories_for_private_review_adapter_preparation','independent_reviews')
    return b

def load_artifacts():
    raw=(ROOT/'areas.json').read_bytes();fail(hashlib.sha256(raw).hexdigest()==BATCH_SHA,'changed_batch_requires_review');return validate(json.loads(raw))
def persist(conn,document_id,sha):
    if str(document_id)!=DOC or sha!=SHA:return None
    with conn.cursor() as c:
        c.execute('SELECT sha256,page_count FROM bronze_ch.vd_pdcom_documents WHERE id=%s',(DOC,));d=c.fetchone()
        if not d or d[0]!=SHA or d[1]!=1:raise ValueError('yvonand_map5_stored_source')
    b=load_artifacts();refs=[refresh(conn,DOC,5939,r['bounds']) for r in b['references']];results=[]
    with conn.cursor() as c:
        c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_references a JOIN bronze_ch.vd_pdcom_parcel_references z USING(egrid) WHERE a.snapshot_id=ANY(%s::uuid[]) AND z.snapshot_id=ANY(%s::uuid[]) AND a.snapshot_id<z.snapshot_id AND (NOT ST_Equals(a.geom,z.geom) OR a.commune_bfs IS DISTINCT FROM z.commune_bfs OR a.official_attributes IS DISTINCT FROM z.official_attributes)''',([x['snapshot_id'] for x in refs],[x['snapshot_id'] for x in refs]))
        if c.fetchone()[0]:raise ValueError('yvonand_map5_conflicting_tile_reference')
    for f in b['features']:
        sid=sector_id(f['key']);geom=json.dumps(f['geometry_lv95']);label='Yvonand 2008 — inventaire indicatif partiel, surpeints conservés ; seuls les plans originaux font foi — '+f['label']
        frozen=[{'egrid':x['egrid'],'kind':x['attributes']['GENRE_TXT'],'geometry':x['geometry']} for x in f['expected_pairs']]
        evidence={'source_sha256':SHA,'source_path_ids':f['path_ids'],'path_identity_kind':'extended PDF drawing/clip index','source_category':f['literal_legend'],'source_transform':b['affine'],'source_precision_m':None,'source_precision':'unknown','review_status':'review_required','publication_status':'internal_review_only','category_complete':False,'inventory_authority_warning':WARNING,'source_policy_status':'historical_current_applicability_unestablished','paint_semantics':'Whole original authored fill or outer pattern clip support; later overpaint retained. Pattern tile clip is not a map boundary. No visible-area, exclusive-area, zoning, ownership or building-rights claim. No Existant/Projete distinction in original map5 legend.','municipal_currentness':b['municipal_currentness'],'source5_reserved_diagnostic':b['source5_reserved_diagnostic'],'alignment_scalar_semantics':'Actual same8 reserved source5 control centroid residual RMSE; original248/114/134 denominator retained. Filtered diagnostic only, not precision or global accuracy.','registration':b['registration'],'whole_component_training_hull':{str(c['extended_index']):c['whole_hull_covered']for c in f['components']},'whole_component_registered_commune':{str(c['extended_index']):c['whole_registered_commune_covered']for c in f['components']},'native_invalid_held_extended_indexes':[3807,3820,4148],'whole_hull_held_extended_indexes':[c['extended_index']for c in b['whole_geometry_holds']],'boundary_buffer':'10m review heuristic, not measured error','official_references':refs,'batch_sha256':BATCH_SHA,'grouping':'One partial collection per literal inventory category','independent_native_review':b['independent_native_qa'],'independent_registration_review':b['independent_registration_qa'],'independent_parcel_review':b['independent_parcel_qa']}
        with conn,conn.cursor() as c:
            c.execute("SET LOCAL statement_timeout='90s'");c.execute('SELECT pg_advisory_xact_lock(593900505)')
            c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_references WHERE snapshot_id=ANY(%s::uuid[]) AND geom && ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) AND NOT ST_IsValid(geom)''',([x['snapshot_id'] for x in refs],geom))
            if c.fetchone()[0]:raise ValueError('yvonand_map5_invalid_current_reference')
            c.execute('DROP TABLE IF EXISTS pg_temp.yvonand_map5_pairs')
            c.execute('''CREATE TEMP TABLE yvonand_map5_pairs ON COMMIT DROP AS WITH g AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) geom),r AS MATERIALIZED (SELECT DISTINCT ON(egrid) * FROM bronze_ch.vd_pdcom_parcel_references WHERE snapshot_id=ANY(%s::uuid[]) AND ST_IsValid(geom) ORDER BY egrid,snapshot_id)
            SELECT r.*,ST_Intersection(r.geom,g.geom) overlap,ST_Area(r.geom) area,ST_DWithin(r.geom,ST_Boundary(g.geom),10) edge,CASE r.official_attributes->>'GENRE_TXT' WHEN 'DDP superficie' THEN 'ddp_superficie' WHEN 'DDP source' THEN 'ddp_source' WHEN 'parcelle privée' THEN 'bien_fonds' WHEN 'DP communal' THEN 'bien_fonds' WHEN 'DP cantonal' THEN 'bien_fonds' ELSE 'unknown' END object_kind FROM r,g WHERE ST_Intersects(r.geom,g.geom) AND ST_Area(ST_Intersection(r.geom,g.geom))>0''',(geom,[x['snapshot_id'] for x in refs]))
            c.execute('SELECT egrid FROM yvonand_map5_pairs');keys={x[0] for x in c.fetchall()}
            if keys!={x['egrid'] for x in frozen}:raise ValueError('yvonand_map5_current_membership_changed')
            c.execute('''WITH f AS (SELECT x->>'egrid' egrid,x->>'kind' kind,ST_SetSRID(ST_GeomFromGeoJSON(x->'geometry'),2056) geom FROM jsonb_array_elements(%s::jsonb) x) SELECT count(*) FROM yvonand_map5_pairs n LEFT JOIN f USING(egrid) WHERE f.egrid IS NULL OR NOT ST_Equals(n.geom,f.geom) OR (n.official_attributes->>'GENRE_TXT') IS DISTINCT FROM f.kind''',(Json(frozen),))
            if c.fetchone()[0]:raise ValueError('yvonand_map5_current_geometry_changed')
            c.execute('SELECT ST_IsValid(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)),(SELECT bool_and(ST_IsValid(ST_Transform(overlap,4326))) FROM yvonand_map5_pairs)',(geom,))
            if c.fetchone()!=(True,True):raise ValueError('yvonand_map5_transformed_geometry_invalid')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_sectors(id,document_id,page_number,label,geom,alignment_rmse_m,validation_evidence,review_status) VALUES(%s,%s,%s,%s,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326),%s,%s,'review_required') ON CONFLICT DO NOTHING''',(sid,DOC,f['page'],label,geom,b['source5_reserved_diagnostic']['rmse_m'],Json(evidence)))
            c.execute("SELECT label,review_status,source_precision_m,validated_by,ST_Equals(geom,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)),alignment_rmse_m=%s FROM bronze_ch.vd_pdcom_sectors WHERE id=%s",(geom,b['source5_reserved_diagnostic']['rmse_m'],sid))
            if c.fetchone()!=(label,'review_required',None,None,True,True):raise ValueError('yvonand_map5_existing_sector_changed')
            c.execute('SELECT commune_bfs FROM gold_ch.v_vd_pdcom_review_sectors WHERE id=%s',(sid,))
            if c.fetchone()[0]!=[5939]:raise ValueError('yvonand_map5_attribution')
            c.execute('SELECT egrid FROM bronze_ch.vd_pdcom_parcel_candidates WHERE sector_id=%s',(sid,));old={x[0] for x in c.fetchall()}
            if old and old!=keys:raise ValueError('yvonand_map5_existing_membership')
            c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_candidates p JOIN yvonand_map5_pairs n USING(egrid) WHERE p.sector_id=%s AND (NOT ST_Equals(p.geom,ST_Transform(n.overlap,4326)) OR p.overlap_m2 IS DISTINCT FROM ST_Area(n.overlap) OR p.parcel_area_m2 IS DISTINCT FROM n.area OR p.overlap_fraction IS DISTINCT FROM LEAST(1,ST_Area(n.overlap)/n.area) OR p.boundary_review_required IS DISTINCT FROM n.edge OR p.uncertainty_buffer_m IS DISTINCT FROM 10 OR p.cadastral_object_kind IS DISTINCT FROM n.object_kind)''',(sid,))
            if c.fetchone()[0]:raise ValueError('yvonand_map5_existing_values')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_parcel_candidates(sector_id,egrid,overlap_m2,parcel_area_m2,overlap_fraction,boundary_review_required,uncertainty_buffer_m,geom,cadastral_object_kind,cadastral_type_evidence) SELECT %s,n.egrid,ST_Area(n.overlap),n.area,LEAST(1,ST_Area(n.overlap)/n.area),n.edge,10,ST_Transform(n.overlap,4326),n.object_kind,jsonb_build_object('source_url',s.query_url,'response_sha256',s.response_sha256,'reference_snapshot_ids',jsonb_build_array(n.snapshot_id),'official_attributes',n.official_attributes,'status','matched') FROM yvonand_map5_pairs n JOIN bronze_ch.vd_pdcom_parcel_reference_snapshots s ON s.id=n.snapshot_id ON CONFLICT DO NOTHING''',(sid,))
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_candidate_parcel_references(sector_id,egrid,snapshot_id) SELECT %s,egrid,snapshot_id FROM yvonand_map5_pairs ON CONFLICT(sector_id,egrid) DO UPDATE SET snapshot_id=EXCLUDED.snapshot_id''',(sid,))
            c.execute('SELECT validation_evidence FROM bronze_ch.vd_pdcom_sectors WHERE id=%s',(sid,))
            if c.fetchone()[0]!=evidence:raise ValueError('yvonand_map5_existing_evidence_changed')
        results.append({'key':f['key'],'sector_id':sid,'parcel_pairs':len(keys)})
    return {'collections':len(results),'parcel_pairs':sum(r['parcel_pairs'] for r in results),'candidates':results,'publication_status':'internal_review_only','review_status':'review_required'}
