"""Reviewed approved Vevey ground-floor activity native wavy paths, private and partial."""
import json,hashlib,uuid,math
from pathlib import Path
from shapely.geometry import shape,MultiLineString,MultiPoint
from shapely.affinity import affine_transform
from psycopg2.extras import Json
from native_closed_paths import closed_polygon,flatten_cubic
DOC='9d0a37f4-0265-59b3-a2f6-b21b57ae5414'
SHA='196c5b690494ce19ade545af7519fa9dc8b413666bbcb90b69cca769c92054f3'
BATCH_SHA='10c36263ea39bbcce21170b7c272450be3bde390e347f121862cee044d644c80'
PATHS={'active_ground_floor':[95078,95079]}

def sector_id(key):
    if key not in PATHS:raise ValueError('active_ground_floor_category')
    return str(uuid.uuid5(uuid.NAMESPACE_URL,DOC+'#native-active-ground-floor67-v1#'+key))

def native_lines(items):
    paths=[];current=[]
    for it in items:
        if it[0] not in ('l','c'):raise ValueError('active_ground_floor_operator')
        pts=[it[1],it[2]] if it[0]=='l' else flatten_cubic(*it[1:],tolerance=.003)
        if current and math.dist(current[-1],pts[0])>1e-8:paths.append(current);current=[]
        current.extend(pts if not current else pts[1:])
    if current:paths.append(current)
    return MultiLineString(paths)

def combine(paths):
    gs=[shape(p['ground_geometry']) for p in paths]
    return MultiLineString([list(line.coords) for g in gs for line in (list(g.geoms) if g.geom_type=='MultiLineString' else [g])])

def validate(b):
    if (b['document_id'],b['source_sha256'],b['source_plan_status'],b['source_scope_communes'])!=(DOC,SHA,'approved',[5890]):raise ValueError('active_ground_floor_source')
    if b['source_precision_m'] is not None or b['publication_status']!='internal_review_only' or b['review_status']!='review_required' or b['parcel_links_allowed'] or b['buffer_allowed'] or b['complete_map']:raise ValueError('active_ground_floor_private_limits')
    sem=b['literal_semantic_review']
    if sem['geographic_acceptance']!='accepted_private_native_cartographic_supports_only' or sem['qualified_release_authorized'] or sem['commune_complete']:raise ValueError('active_ground_floor_semantics')
    if sem['decision']!='semantic_mapping_accepted_for_bounded_private_native_lines_only':raise ValueError('active_ground_floor_semantic_review_pending')
    if sem['delivered_class_keys']!=['active_ground_floor'] or sem['eligible_ids']!=[95078,95079] or sem['held_ids']!=[95080,95081] or sem['legend_excluded_ids']!=[96164] or sem['parcel_associations_allowed']:raise ValueError('active_ground_floor_acceptance_scope')
    if b['curve_tolerance_pdfpoint']!=.003 or len(b['features'])!=1 or {f['key'] for f in b['features']}!=set(PATHS):raise ValueError('active_ground_floor_batch_scope')
    cc=b['frozen_controls']['controls'];train=[c for c in cc if c['role']=='train'];held=[c for c in cc if c['role']=='held']
    if [c['source_ordinary_index'] for c in train]!=[1388,2354,2931,3927,2047,4281] or [c['source_ordinary_index'] for c in held]!=[1705,4065,4821,2143,865]:raise ValueError('active_ground_floor_frozen_partition')
    hull=MultiPoint([c['source_centroid'] for c in train]).convex_hull;m=b['fit']['matrix'];o=b['fit']['offset'];affine=[m[0][0],m[0][1],m[1][0],m[1][1],*o];res=[]
    for c in cc:
        if math.dist(shape(c['source_geometry']).centroid.coords[0],c['source_centroid'])>1e-10 or math.dist(shape(c['reference_geometry']).centroid.coords[0],c['ground_centroid'])>1e-8:raise ValueError('active_ground_floor_control_geometry')
        x,y=c['source_centroid'];err=math.dist([m[0][0]*x+m[0][1]*y+o[0],m[1][0]*x+m[1][1]*y+o[1]],c['ground_centroid'])
        if c['role']=='held':res.append(err)
    if max(res)>=5 or abs(math.sqrt(sum(x*x for x in res)/5)-b['alignment']['holdout_rmse_m'])>1e-8:raise ValueError('active_ground_floor_alignment')
    clips={x['index']:closed_polygon(x['items']) for x in b['source_native_proof']['clips']};commune=shape(b['commune_reference']['geometry'])
    state=b['source_native_proof']['raw_graphics_state_proof']
    if not state['independent_lexer_tracks_original_q_Q_and_gs'] or not state['all_source_strokes_CA_1_SMask_None_BM_Normal']:raise ValueError('active_ground_floor_unknown_mask_state')
    for row in state['records']:
        gs=row['graphics_state_before_source_q']
        if (gs['CA'],gs['SMask'],gs['BM'])!=('1','/None','/Normal'):raise ValueError('active_ground_floor_mask_or_opacity_changed')
    ledger=b['source_native_proof']['candidates']
    if [sum(x['disposition']==status for x in ledger) for status in ['eligible','whole_path_geographic_hold','legend']]!=[2,2,1]:raise ValueError('active_ground_floor_full_accounting')
    for f in b['features']:
        if [p['index'] for p in f['source_paths']]!=PATHS[f['key']] or f['page']!=67:raise ValueError('active_ground_floor_path_membership')
        for p in f['source_paths']:
            op=p['native_operator'];proof=p['raw_stroke_proof']
            if op['color']!=[.9343404173851013,.19400320947170258,.17949187755584717] or op['width']!=.8799999952316284 or op['dashes']!='[] 0' or op['stroke_opacity']!=1.0 or op['type']!='s' or op['fill'] is not None:raise ValueError('active_ground_floor_native_style')
            if not proof['matched_native_stroke'] or proof['source_content_xref']!=14427 or not proof['raw_stroke_statement'].endswith('S\nQ'):raise ValueError('active_ground_floor_raw_stroke')
            if p['clip_identity']['full_native_ancestry']!=[0,87229]:raise ValueError('active_ground_floor_source_ancestry')
            g=native_lines(p['native_items'])
            if not g.equals_exact(shape(p['geometry']),1e-10) or not hull.covers(g) or g.length<=0:raise ValueError('active_ground_floor_whole_native')
            visible=g
            for idx in p['clip_identity']['active_clips']:visible=visible.intersection(clips[idx])
            if not visible.equals_exact(shape(p['source_visible_geometry']),1e-10):raise ValueError('active_ground_floor_source_clip')
            if not visible.equals(g):raise ValueError('active_ground_floor_whole_visible_support')
            ground=affine_transform(visible,affine)
            if not ground.equals_exact(shape(p['ground_geometry']),1e-8) or not commune.covers(ground):raise ValueError('active_ground_floor_ground_scope')
        g=combine(f['source_paths'])
        if not g.is_valid or not g.equals_exact(shape(f['geometry_lv95']),1e-8):raise ValueError('active_ground_floor_group_geometry')
    return b

def load_artifacts():
    raw=(Path(__file__).parent/'reports/vevey-active-ground-floor/batch.json').read_bytes()
    if hashlib.sha256(raw).hexdigest()!=BATCH_SHA:raise ValueError('active_ground_floor_changed_batch_requires_review')
    return validate(json.loads(raw))

def persist(conn,document_id,sha):
    if str(document_id)!=DOC or sha!=SHA:return None
    b=load_artifacts();out=[]
    with conn,conn.cursor() as c:
        c.execute("SET LOCAL statement_timeout='90s'");c.execute('SELECT pg_advisory_xact_lock(589001167)')
        c.execute('SELECT sha256,page_count,plan_status FROM bronze_ch.vd_pdcom_documents WHERE id=%s',(DOC,))
        if c.fetchone()!=(SHA,152,'approved'):raise ValueError('active_ground_floor_stored_source')
        for f in b['features']:
            sid=sector_id(f['key']);g=json.dumps(f['geometry_lv95']);label='Vevey — support cartographique partiel — '+f['label']
            evidence=dict(feature_kind='source_native_line',source_sha256=SHA,source_path_ids=[str(p['index']) for p in f['source_paths']],source_category=f['label'],legend_excluded_native_ids=[96164],raw_graphics_state_proof=b['source_native_proof']['raw_graphics_state_proof'],native_graphics_states=b['source_native_proof']['native_graphics_states'],native_pdf_clips=b['source_native_proof']['clips'],all_class_accounting=b['literal_semantic_review']['partial_scope'] if 'partial_scope' in b['literal_semantic_review'] else b['native_class_review'],page_number=67,semantic_contract=b['literal_semantic_review'],native_source_paths=f['source_paths'],source_transform=b['fit'],coordinate_scope='whole original paths within frozen training hull and official Vevey',alignment=b['alignment'],source_precision='unknown',source_precision_m=None,review_status='review_required',publication_status='internal_review_only',grouping='Partial source-native policy class; individual paths are not separate sectors or built routes',parcel_association='none: non-area source feature',geometry_processing=b['representation'],batch_sha256=BATCH_SHA,artifact='pipelines/vd-pdcom/reports/vevey-active-ground-floor',source_plan_status='approved',source_scope_communes=[5890],held_source_paths=[p['index'] for p in b['source_native_proof']['candidates'] if p['disposition']=='whole_path_geographic_hold'],complete_map=False)
            c.execute('SELECT ST_IsValid(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326))',(g,))
            if c.fetchone()[0] is not True:raise ValueError('active_ground_floor_invalid_4326')
            c.execute("""INSERT INTO bronze_ch.vd_pdcom_sectors(id,document_id,page_number,label,geom,alignment_rmse_m,validation_evidence,review_status) VALUES(%s,%s,67,%s,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326),%s,%s,'review_required') ON CONFLICT DO NOTHING""",(sid,DOC,label,g,b['alignment']['holdout_rmse_m'],Json(evidence)))
            c.execute("""SELECT document_id::text,label,review_status,source_precision_m,validated_by,validation_evidence,ST_Equals(geom,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)) FROM bronze_ch.vd_pdcom_sectors WHERE id=%s""",(g,sid))
            if c.fetchone()!=(DOC,label,'review_required',None,None,evidence,True):raise ValueError('active_ground_floor_existing_state_changed')
            c.execute('SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_candidates WHERE sector_id=%s',(sid,))
            if c.fetchone()[0]:raise ValueError('active_ground_floor_unexpected_parcels')
            c.execute('SELECT commune_bfs,validation_evidence FROM gold_ch.v_vd_pdcom_review_sectors WHERE id=%s',(sid,));communes,ve=c.fetchone()
            if communes!=[5890] or 'native_attribution' not in ve:raise ValueError('active_ground_floor_attribution')
            out.append(dict(sector_id=sid,key=f['key'],feature_kind='source_native_line',source_paths=len(f['source_paths']),commune_bfs=communes))
    return dict(collections=len(out),source_paths=2,parcel_pairs=0,candidates=out,publication_status='internal_review_only',review_status='review_required')
