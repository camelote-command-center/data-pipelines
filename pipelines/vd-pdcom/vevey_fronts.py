"""Reviewed approved Vevey fronts native path supports, private and partial."""
import json,hashlib,uuid,math
from pathlib import Path
from shapely.geometry import shape,MultiLineString,MultiPoint
from shapely.affinity import affine_transform
from psycopg2.extras import Json
from native_closed_paths import closed_polygon,flatten_cubic
DOC='9d0a37f4-0265-59b3-a2f6-b21b57ae5414'
SHA='196c5b690494ce19ade545af7519fa9dc8b413666bbcb90b69cca769c92054f3'
BATCH_SHA='4ca5fc2cd8e610618c2d66ec67c985960e9a07f6f8263a29b8ffa1039fa2e321'
PATHS={'continuous_front': [94859, 94860, 94861, 94862, 94863, 94864, 94865, 94866, 94867, 94869, 94870, 94871, 94872, 94873, 94874, 94875, 94876, 94877, 94878, 94879, 94880, 94881, 94882, 94883, 94884, 94885, 94886, 94887, 94889, 94890, 94891, 94892, 94893, 94896, 94897, 94898, 94899, 94900, 94901, 94902, 94903, 94904, 94905, 94906, 94907, 94908, 94909, 94910, 94911, 94912, 94913, 94914, 94915, 94916, 94917, 94918, 94919, 94920, 94921, 94922, 94923, 94924, 94925, 94928, 94929], 'discontinuous_front': [94930, 94931, 94932, 94933, 94934, 94935, 94936, 94937, 94938, 94939, 94940, 94941, 94942, 94943, 94944, 94945, 94946, 94947, 94948, 94949, 94950, 94951, 94952, 94953, 94954, 94955, 94956, 94957, 94958, 94959, 94960, 94961, 94962, 94963, 94964, 94965, 94966, 94967, 94968, 94969, 94970, 94971, 94972, 94973, 94974, 94975, 94976, 94977, 94978, 94979, 94980, 94981, 94982, 94983, 94984, 94985, 94986, 94987, 94988, 94989, 94990, 94991, 94992, 94993, 94994, 94995, 94996, 94997, 94998, 94999, 95000, 95001, 95002, 95003, 95004, 95005, 95006, 95007, 95008, 95009, 95010, 95011, 95012, 95013, 95014, 95015, 95016, 95017, 95018, 95019, 95020, 95021, 95022, 95023, 95024, 95025, 95026, 95027, 95028, 95029, 95032, 95033, 95034, 95035, 95036, 95037, 95038, 95039, 95040, 95041, 95042, 95043, 95044, 95045, 95046, 95047, 95048, 95049, 95050, 95051, 95052, 95053, 95054, 95055, 95056, 95057, 95058, 95059, 95060, 95061, 95062, 95063, 95064, 95065, 95066, 95067, 95068, 95069, 95070, 95071, 95072, 95073, 95074, 95075, 95076, 95077], 'pedestrian_continuity': [95129, 95133, 95135, 95137, 95141, 95145, 95149, 95153, 95161]}

def sector_id(key):
    if key not in PATHS:raise ValueError('fronts_category')
    return str(uuid.uuid5(uuid.NAMESPACE_URL,DOC+'#native-fronts67-v1#'+key))

def native_lines(items):
    paths=[];current=[]
    for it in items:
        if it[0] not in ('l','c'):raise ValueError('fronts_operator')
        pts=[it[1],it[2]] if it[0]=='l' else flatten_cubic(*it[1:],tolerance=.003)
        if current and math.dist(current[-1],pts[0])>1e-8:paths.append(current);current=[]
        current.extend(pts if not current else pts[1:])
    if current:paths.append(current)
    return MultiLineString(paths)

def combine(paths):
    gs=[shape(p['ground_geometry']) for p in paths]
    return MultiLineString([list(line.coords) for g in gs for line in (list(g.geoms) if g.geom_type=='MultiLineString' else [g])])

def validate(b):
    if (b['document_id'],b['source_sha256'],b['source_plan_status'],b['source_scope_communes'])!=(DOC,SHA,'approved',[5890]):raise ValueError('fronts_source')
    if b['source_precision_m'] is not None or b['publication_status']!='internal_review_only' or b['review_status']!='review_required' or b['parcel_links_allowed'] or b['buffer_allowed'] or b['complete_map']:raise ValueError('fronts_private_limits')
    sem=b['literal_semantic_review']
    if sem['geographic_acceptance']!='accepted_private_native_cartographic_supports_only' or sem['qualified_release_authorized'] or sem['commune_complete']:raise ValueError('fronts_semantics')
    if set(sem['delivered_class_keys'])!=set(PATHS) or sem['not_processed_classes']!=['historic_dense','villas_collective','overall_plan','collective_open','mixed_program','equipment_site','relay_polarity','active_ground_floor']:raise ValueError('fronts_acceptance_scope')
    if b['curve_tolerance_pdfpoint']!=.003 or len(b['features'])!=3 or {f['key'] for f in b['features']}!=set(PATHS):raise ValueError('fronts_batch_scope')
    cc=b['frozen_controls']['controls'];train=[c for c in cc if c['role']=='train'];held=[c for c in cc if c['role']=='held']
    if [c['source_ordinary_index'] for c in train]!=[1388,2354,2931,3927,2047,4281] or [c['source_ordinary_index'] for c in held]!=[1705,4065,4821,2143,865]:raise ValueError('fronts_frozen_partition')
    hull=MultiPoint([c['source_centroid'] for c in train]).convex_hull;m=b['fit']['matrix'];o=b['fit']['offset'];affine=[m[0][0],m[0][1],m[1][0],m[1][1],*o];res=[]
    for c in cc:
        if math.dist(shape(c['source_geometry']).centroid.coords[0],c['source_centroid'])>1e-10 or math.dist(shape(c['reference_geometry']).centroid.coords[0],c['ground_centroid'])>1e-8:raise ValueError('fronts_control_geometry')
        x,y=c['source_centroid'];err=math.dist([m[0][0]*x+m[0][1]*y+o[0],m[1][0]*x+m[1][1]*y+o[1]],c['ground_centroid'])
        if c['role']=='held':res.append(err)
    if max(res)>=5 or abs(math.sqrt(sum(x*x for x in res)/5)-b['alignment']['holdout_rmse_m'])>1e-8:raise ValueError('fronts_alignment')
    clips={x['index']:closed_polygon(x['items']) for x in b['source_native_proof']['clips']};commune=shape(b['commune_reference']['geometry'])
    ledger=b['source_native_proof']['candidates']
    if [sum(x['disposition']==s for x in ledger) for s in ['eligible','whole_path_geographic_hold','zero_length_style_dot','legend_or_page_furniture','other_style_commune_boundary','unsupported_operator']]!=[220,7,16,5,1,1]:raise ValueError('fronts_full_accounting')
    for f in b['features']:
        if [p['index'] for p in f['source_paths']]!=PATHS[f['key']] or f['page']!=67:raise ValueError('fronts_path_membership')
        for p in f['source_paths']:
            g=native_lines(p['native_items'])
            if not g.equals_exact(shape(p['geometry']),1e-10) or not hull.covers(g) or g.length<=0:raise ValueError('fronts_whole_native')
            visible=g
            for idx in p['clip_identity']['active_clips']:visible=visible.intersection(clips[idx])
            if not visible.equals_exact(shape(p['source_visible_geometry']),1e-10):raise ValueError('fronts_source_clip')
            if not visible.equals(g):raise ValueError('fronts_whole_visible_support')
            ground=affine_transform(visible,affine)
            if not ground.equals_exact(shape(p['ground_geometry']),1e-8) or not commune.covers(ground):raise ValueError('fronts_ground_scope')
            if p['index']==67813:raise ValueError('fronts_boundary_not_front')
        g=combine(f['source_paths'])
        if not g.is_valid or not g.equals_exact(shape(f['geometry_lv95']),1e-8):raise ValueError('fronts_group_geometry')
    return b

def load_artifacts():
    raw=(Path(__file__).parent/'reports/vevey-fronts/batch.json').read_bytes()
    if hashlib.sha256(raw).hexdigest()!=BATCH_SHA:raise ValueError('fronts_changed_batch_requires_review')
    return validate(json.loads(raw))

def persist(conn,document_id,sha):
    if str(document_id)!=DOC or sha!=SHA:return None
    b=load_artifacts();out=[]
    with conn,conn.cursor() as c:
        c.execute("SET LOCAL statement_timeout='90s'");c.execute('SELECT pg_advisory_xact_lock(589000667)')
        c.execute('SELECT sha256,page_count,plan_status FROM bronze_ch.vd_pdcom_documents WHERE id=%s',(DOC,))
        if c.fetchone()!=(SHA,152,'approved'):raise ValueError('fronts_stored_source')
        for f in b['features']:
            sid=sector_id(f['key']);g=json.dumps(f['geometry_lv95']);label='Vevey — support cartographique partiel — '+f['label']
            evidence=dict(feature_kind='source_native_line',source_sha256=SHA,source_path_ids=[str(p['index']) for p in f['source_paths']],source_category=f['label'],excluded_boundary_native_ids=[67813],unsupported_native_ids=[94868],all_class_accounting=b['literal_semantic_review']['partial_scope'] if 'partial_scope' in b['literal_semantic_review'] else b['native_class_review'],page_number=67,semantic_contract=b['literal_semantic_review'],native_source_paths=f['source_paths'],source_transform=b['fit'],coordinate_scope='whole original paths within frozen training hull and official Vevey',alignment=b['alignment'],source_precision='unknown',source_precision_m=None,review_status='review_required',publication_status='internal_review_only',grouping='Partial source-native policy class; individual paths are not separate sectors or built routes',parcel_association='none: non-area source feature',geometry_processing=b['representation'],batch_sha256=BATCH_SHA,artifact='pipelines/vd-pdcom/reports/vevey-fronts',source_plan_status='approved',source_scope_communes=[5890],held_source_paths=[p['index'] for p in b['source_native_proof']['candidates'] if p['disposition']=='whole_path_geographic_hold'],complete_map=False)
            c.execute('SELECT ST_IsValid(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326))',(g,))
            if c.fetchone()[0] is not True:raise ValueError('fronts_invalid_4326')
            c.execute("""INSERT INTO bronze_ch.vd_pdcom_sectors(id,document_id,page_number,label,geom,alignment_rmse_m,validation_evidence,review_status) VALUES(%s,%s,67,%s,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326),%s,%s,'review_required') ON CONFLICT DO NOTHING""",(sid,DOC,label,g,b['alignment']['holdout_rmse_m'],Json(evidence)))
            c.execute("""SELECT document_id::text,label,review_status,source_precision_m,validated_by,validation_evidence,ST_Equals(geom,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)) FROM bronze_ch.vd_pdcom_sectors WHERE id=%s""",(g,sid))
            if c.fetchone()!=(DOC,label,'review_required',None,None,evidence,True):raise ValueError('fronts_existing_state_changed')
            c.execute('SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_candidates WHERE sector_id=%s',(sid,))
            if c.fetchone()[0]:raise ValueError('fronts_unexpected_parcels')
            c.execute('SELECT commune_bfs,validation_evidence FROM gold_ch.v_vd_pdcom_review_sectors WHERE id=%s',(sid,));communes,ve=c.fetchone()
            if communes!=[5890] or 'native_attribution' not in ve:raise ValueError('fronts_attribution')
            out.append(dict(sector_id=sid,key=f['key'],feature_kind='source_native_line',source_paths=len(f['source_paths']),commune_bfs=communes))
    return dict(collections=len(out),source_paths=220,parcel_pairs=0,candidates=out,publication_status='internal_review_only',review_status='review_required')
