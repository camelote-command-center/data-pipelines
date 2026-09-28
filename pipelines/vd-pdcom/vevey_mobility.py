"""Reviewed approved Vevey mobility native path supports, private and partial."""
import json,hashlib,uuid,math
from pathlib import Path
from shapely.geometry import shape,MultiLineString,MultiPoint
from shapely.affinity import affine_transform
from psycopg2.extras import Json
from native_closed_paths import closed_polygon,flatten_cubic
DOC='9d0a37f4-0265-59b3-a2f6-b21b57ae5414'
SHA='196c5b690494ce19ade545af7519fa9dc8b413666bbcb90b69cca769c92054f3'
BATCH_SHA='7e59e7340b3a432bb62714c603434c5879c8bc35b5d9fc0a1ea3deb82c869415'
PATHS={'bus': [61488, 61494, 61506, 61514, 61528, 61546, 61548, 61554, 61560, 61576, 61584, 61586, 61592], 'cycle': [61362, 61364, 61370, 61376, 61388, 61394, 61400, 61406, 61418, 61426, 61428, 61440, 61442, 61444, 61446, 61452, 61458, 61470, 61476], 'modal_distribution_possible_motor_closure': [61332], 'moderated': [61348, 61351, 61352, 61353, 61354], 'multimodal': [61327, 61328, 61331, 61333, 61335], 'pedestrian': [61600, 61606, 61609, 61612, 61615, 61621, 61624, 61627], 'traffic': [61341, 61349, 61355]}

def sector_id(key):
    if key not in PATHS:raise ValueError('mobility_category')
    return str(uuid.uuid5(uuid.NAMESPACE_URL,DOC+'#native-mobility87-v1#'+key))

def native_lines(items):
    paths=[];current=[]
    for it in items:
        if it[0] not in ('l','c'):raise ValueError('mobility_operator')
        pts=[it[1],it[2]] if it[0]=='l' else flatten_cubic(*it[1:],tolerance=.003)
        if current and math.dist(current[-1],pts[0])>1e-8:paths.append(current);current=[]
        current.extend(pts if not current else pts[1:])
    if current:paths.append(current)
    return MultiLineString(paths)

def combine(paths):
    gs=[shape(p['ground_geometry']) for p in paths]
    return MultiLineString([list(line.coords) for g in gs for line in (list(g.geoms) if g.geom_type=='MultiLineString' else [g])])

def validate(b):
    if (b['document_id'],b['source_sha256'],b['source_plan_status'],b['source_scope_communes'])!=(DOC,SHA,'approved',[5890]):raise ValueError('mobility_source')
    if b['source_precision_m'] is not None or b['publication_status']!='internal_review_only' or b['review_status']!='review_required' or b['parcel_links_allowed'] or b['buffer_allowed'] or b['complete_map']:raise ValueError('mobility_private_limits')
    sem=b['literal_semantic_review']
    if sem['geographic_acceptance']!='accepted_private_native_cartographic_supports_only' or sem['qualified_release_authorized'] or sem['commune_complete']:raise ValueError('mobility_semantics')
    if set(sem['delivered_class_keys'])!=set(PATHS) or sem['not_processed_classes']!=['parking_core','parking_outer','calmed_access','motorway']:raise ValueError('mobility_acceptance_scope')
    if b['curve_tolerance_pdfpoint']!=.003 or len(b['features'])!=7 or {f['key'] for f in b['features']}!=set(PATHS):raise ValueError('mobility_batch_scope')
    cc=b['frozen_controls']['controls'];train=[c for c in cc if c['role']=='train'];held=[c for c in cc if c['role']=='held']
    if [c['source_ordinary_index'] for c in train]!=[1398,2291,2773,3671,1983,4026] or [c['source_ordinary_index'] for c in held]!=[1668,3810,4518,2080,871]:raise ValueError('mobility_frozen_partition')
    hull=MultiPoint([c['source_centroid'] for c in train]).convex_hull;m=b['fit']['matrix'];o=b['fit']['offset'];affine=[m[0][0],m[0][1],m[1][0],m[1][1],*o];res=[]
    for c in cc:
        if math.dist(shape(c['source_geometry']).centroid.coords[0],c['source_centroid'])>1e-10 or math.dist(shape(c['reference_geometry']).centroid.coords[0],c['ground_centroid'])>1e-8:raise ValueError('mobility_control_geometry')
        x,y=c['source_centroid'];err=math.dist([m[0][0]*x+m[0][1]*y+o[0],m[1][0]*x+m[1][1]*y+o[1]],c['ground_centroid'])
        if c['role']=='held':res.append(err)
    if max(res)>=5 or abs(math.sqrt(sum(x*x for x in res)/5)-b['alignment']['holdout_rmse_m'])>1e-8:raise ValueError('mobility_alignment')
    clips={x['index']:closed_polygon(x['items']) for x in b['source_native_proof']['clips']};commune=shape(b['commune_reference']['geometry'])
    ledger=b['source_native_proof']['candidates']
    if [sum(x['disposition']==s for x in ledger) for s in ['eligible','whole_path_geographic_hold','zero_length_style_dot','legend_or_page_furniture']]!=[54,26,76,12]:raise ValueError('mobility_full_accounting')
    for f in b['features']:
        if [p['index'] for p in f['source_paths']]!=PATHS[f['key']] or f['page']!=87:raise ValueError('mobility_path_membership')
        for p in f['source_paths']:
            g=native_lines(p['native_items'])
            if not g.equals_exact(shape(p['geometry']),1e-10) or not hull.covers(g) or g.length<=0:raise ValueError('mobility_whole_native')
            visible=g
            for idx in p['clip_identity']['active_clips']:visible=visible.intersection(clips[idx])
            if not visible.equals_exact(shape(p['source_visible_geometry']),1e-10):raise ValueError('mobility_source_clip')
            if not visible.equals(g):raise ValueError('mobility_whole_visible_support')
            ground=affine_transform(visible,affine)
            if not ground.equals_exact(shape(p['ground_geometry']),1e-8) or not commune.covers(ground):raise ValueError('mobility_ground_scope')
            if p['index']==61332 and p['dashes']!='[ 4.49 3.739 ] 0':raise ValueError('mobility_closure_style')
        g=combine(f['source_paths'])
        if not g.is_valid or not g.equals_exact(shape(f['geometry_lv95']),1e-8):raise ValueError('mobility_group_geometry')
    return b

def load_artifacts():
    raw=(Path(__file__).parent/'reports/vevey-mobility/batch.json').read_bytes()
    if hashlib.sha256(raw).hexdigest()!=BATCH_SHA:raise ValueError('mobility_changed_batch_requires_review')
    return validate(json.loads(raw))

def persist(conn,document_id,sha):
    if str(document_id)!=DOC or sha!=SHA:return None
    b=load_artifacts();out=[]
    with conn,conn.cursor() as c:
        c.execute("SET LOCAL statement_timeout='90s'");c.execute('SELECT pg_advisory_xact_lock(589000387)')
        c.execute('SELECT sha256,page_count,plan_status FROM bronze_ch.vd_pdcom_documents WHERE id=%s',(DOC,))
        if c.fetchone()!=(SHA,152,'approved'):raise ValueError('mobility_stored_source')
        for f in b['features']:
            sid=sector_id(f['key']);g=json.dumps(f['geometry_lv95']);label='Vevey — support cartographique de mobilité — '+f['label']
            evidence=dict(feature_kind='source_native_line',source_sha256=SHA,source_path_ids=[str(p['index']) for p in f['source_paths']],source_category=f['label'],page_number=87,semantic_contract=b['literal_semantic_review'],native_source_paths=f['source_paths'],source_transform=b['fit'],coordinate_scope='whole original paths within frozen training hull and official Vevey',alignment=b['alignment'],source_precision='unknown',source_precision_m=None,review_status='review_required',publication_status='internal_review_only',grouping='Partial source-native policy class; individual paths are not separate sectors or built routes',parcel_association='none: non-area source feature',geometry_processing=b['representation'],batch_sha256=BATCH_SHA,artifact='pipelines/vd-pdcom/reports/vevey-mobility',source_plan_status='approved',source_scope_communes=[5890],held_source_paths=[p['index'] for p in b['source_native_proof']['candidates'] if p['disposition']=='whole_path_geographic_hold'],complete_map=False)
            c.execute('SELECT ST_IsValid(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326))',(g,))
            if c.fetchone()[0] is not True:raise ValueError('mobility_invalid_4326')
            c.execute("""INSERT INTO bronze_ch.vd_pdcom_sectors(id,document_id,page_number,label,geom,alignment_rmse_m,validation_evidence,review_status) VALUES(%s,%s,87,%s,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326),%s,%s,'review_required') ON CONFLICT DO NOTHING""",(sid,DOC,label,g,b['alignment']['holdout_rmse_m'],Json(evidence)))
            c.execute("""SELECT document_id::text,label,review_status,source_precision_m,validated_by,validation_evidence,ST_Equals(geom,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)) FROM bronze_ch.vd_pdcom_sectors WHERE id=%s""",(g,sid))
            if c.fetchone()!=(DOC,label,'review_required',None,None,evidence,True):raise ValueError('mobility_existing_state_changed')
            c.execute('SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_candidates WHERE sector_id=%s',(sid,))
            if c.fetchone()[0]:raise ValueError('mobility_unexpected_parcels')
            c.execute('SELECT commune_bfs,validation_evidence FROM gold_ch.v_vd_pdcom_review_sectors WHERE id=%s',(sid,));communes,ve=c.fetchone()
            if communes!=[5890] or 'native_attribution' not in ve:raise ValueError('mobility_attribution')
            out.append(dict(sector_id=sid,key=f['key'],feature_kind='source_native_line',source_paths=len(f['source_paths']),commune_bfs=communes))
    return dict(collections=len(out),source_paths=54,parcel_pairs=0,candidates=out,publication_status='internal_review_only',review_status='review_required')
