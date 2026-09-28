"""Private original Vevey calmed-access cartographic symbol footprints; no parcels."""
import json,hashlib,uuid,math
from pathlib import Path
from shapely.geometry import shape,MultiPoint
from shapely.ops import unary_union
from shapely.affinity import affine_transform
from psycopg2.extras import Json
from native_closed_paths import closed_polygon
from vevey_mobility import load_artifacts as approved_frame
DOC='9d0a37f4-0265-59b3-a2f6-b21b57ae5414'
SHA='196c5b690494ce19ade545af7519fa9dc8b413666bbcb90b69cca769c92054f3'
BATCH_SHA='96361e57ef34b89349a1d2036da4754f8ceb2bafdbc77f157182d1d379ff6844'
PATHS={'calmed_access':[61047, 61049, 61051, 61052, 61053, 61054, 61055, 61056, 61057, 61058, 61059, 61060, 61061, 61063, 61064, 61065, 61066, 61067, 61068, 61069, 61070, 61071, 61072, 61073, 61074, 61075, 61086, 61093, 61095, 61097, 61098, 61099, 61100, 61101, 61104, 61111, 61112, 61113, 61114, 61115, 61116, 61117, 61119, 61120, 61121, 61122, 61123, 61124, 61125, 61127, 61130, 61132, 61133, 61134, 61135, 61136, 61137, 61138, 61139, 61140, 61145, 61148, 61149, 61150, 61153, 61154, 61155, 61156, 61158, 61159, 61162, 61163, 61164, 61165, 61166, 61168, 61169, 61170, 61171, 61172, 61173, 61174, 61175, 61177, 61179, 61180, 61181, 61182, 61183, 61184, 61185, 61186, 61187, 61188, 61189, 61190, 61191, 61192, 61193, 61194, 61195, 61196, 61197, 61198, 61199, 61200, 61201, 61202, 61203, 61204, 61205, 61206, 61207, 61210, 61211, 61213, 61214, 61215, 61216, 61217, 61218, 61219, 61220, 61221, 61222, 61223, 61224, 61225, 61226, 61227, 61228, 61229, 61230, 61231, 61232, 61233, 61234, 61236, 61239, 61240, 61241, 61242, 61243, 61244, 61245, 61246, 61247, 61248, 61249, 61250, 61251, 61252, 61253, 61254, 61257, 61258, 61259, 61260, 61261, 61262, 61263, 61264, 61265, 61266, 61267, 61268, 61269, 61270, 61271, 61273, 61274, 61275, 61276, 61277, 61278, 61279, 61280, 61281, 61282, 61283, 61285, 61287, 61289, 61291, 61292, 61293, 61294, 61295, 61296, 61297, 61298, 61299, 61300, 61301, 61302, 61304, 61305, 61306, 61307, 61308, 61309, 61314, 61315, 61316, 61317, 61318, 61319, 61320, 61321, 61323, 61326]}

def sector_id(key):
    if key not in PATHS:raise ValueError('access_category')
    return str(uuid.uuid5(uuid.NAMESPACE_URL,DOC+'#native-access87-v1#'+key))

def validate(b):
    if (b['document_id'],b['source_sha256'],b['source_plan_status'],b['source_scope_communes'])!=(DOC,SHA,'approved',[5890]):raise ValueError('access_source')
    if b['source_precision_m'] is not None or b['publication_status']!='internal_review_only' or b['review_status']!='review_required' or b['parcel_links_allowed'] or b['buffer_allowed'] or b['complete_map']:raise ValueError('access_private_limits')
    old=approved_frame()
    if any(b[k]!=old[k] for k in ['frozen_controls','fit','commune_reference']):raise ValueError('access_frozen_frame')
    if (b['alignment']['holdout_rmse_m'],b['alignment']['holdout_max_m'])!=(old['alignment']['holdout_rmse_m'],old['alignment']['holdout_max_m']):raise ValueError('access_alignment_claim')
    sem=b['literal_semantic_review']
    if sem['delivered_class_keys']!=['calmed_access'] or sem['native_candidate_mapping_pending'] or sem['parcel_links_allowed'] or sem['current_regulation']:raise ValueError('access_semantic_scope')
    limits=sem['limits']
    if limits['source_precision_m'] is not None or any(limits[k] for k in ['qualification','municipal_completion','map_completion']):raise ValueError('access_semantic_limits')
    if b['curve_tolerance_pdfpoint']!=.003 or len(b['features'])!=1 or b['features'][0]['key']!='calmed_access':raise ValueError('access_batch_scope')
    cc=b['frozen_controls']['controls'];hull=MultiPoint([c['source_centroid'] for c in cc if c['role']=='train']).convex_hull;m=b['fit']['matrix'];o=b['fit']['offset'];affine=[m[0][0],m[0][1],m[1][0],m[1][1],*o]
    clips={x['index']:closed_polygon(x['items']) for x in b['source_native_proof']['clips']};commune=shape(b['commune_reference']['geometry']);ledger=b['source_native_proof']['candidates']
    if [sum(x['disposition']==s for x in ledger) for s in ['eligible','whole_support_hold','native_topology_hold','legend']]!=[211,53,7,1]:raise ValueError('access_full_accounting')
    f=b['features'][0]
    if [p['index'] for p in f['source_paths']]!=PATHS['calmed_access'] or f['page']!=87:raise ValueError('access_path_membership')
    if f['source_paths']!=[p for p in ledger if p['disposition']=='eligible']:raise ValueError('access_ledger_membership')
    gs=[]
    for p in f['source_paths']:
        if p['fill']!=[.9453421831130981,.8111848831176758,.4923475980758667] or p['color'] is not None:raise ValueError('access_literal_style')
        g=closed_polygon(p['native_items'],tolerance=.003)
        if not g.equals_exact(shape(p['geometry']),1e-10) or not hull.covers(g):raise ValueError('access_whole_native')
        visible=g
        for idx in p['clip_identity']['active_clips']:visible=visible.intersection(clips[idx])
        if not visible.equals(g):raise ValueError('access_whole_visible_support')
        ground=affine_transform(g,affine)
        if not ground.equals_exact(shape(p['ground_geometry']),1e-8) or not commune.covers(ground):raise ValueError('access_ground_scope')
        gs.append(ground)
    combined=unary_union(gs);given=shape(f['geometry_lv95'])
    if not given.is_valid or not combined.equals_exact(given,1e-8) or combined.symmetric_difference(given).area!=0:raise ValueError('access_exact_set_union')
    return b

def load_artifacts():
    raw=(Path(__file__).parent/'reports/vevey-access/batch.json').read_bytes()
    if hashlib.sha256(raw).hexdigest()!=BATCH_SHA:raise ValueError('access_changed_batch_requires_review')
    return validate(json.loads(raw))

def persist(conn,document_id,sha):
    if str(document_id)!=DOC or sha!=SHA:return None
    b=load_artifacts();out=[]
    with conn,conn.cursor() as c:
        c.execute("SET LOCAL statement_timeout='90s'");c.execute('SELECT pg_advisory_xact_lock(589000487)')
        c.execute('SELECT sha256,page_count,plan_status FROM bronze_ch.vd_pdcom_documents WHERE id=%s',(DOC,))
        if c.fetchone()!=(SHA,152,'approved'):raise ValueError('access_stored_source')
        for f in b['features']:
            sid=sector_id(f['key']);g=json.dumps(f['geometry_lv95']);label='Vevey — empreintes cartographiques de desserte apaisée — '+f['label']
            evidence=dict(feature_kind='source_native_cartographic_area',source_sha256=SHA,source_path_ids=[str(p['index']) for p in f['source_paths']],source_category=f['label'],page_number=87,semantic_contract=b['literal_semantic_review'],native_source_paths=f['source_paths'],source_transform=b['fit'],coordinate_scope='whole original paths within frozen training hull and official Vevey',alignment=b['alignment'],source_precision='unknown',source_precision_m=None,review_status='review_required',publication_status='internal_review_only',grouping='Exact set union of partial source-native cartographic street symbols; not legal road widths or parcel sectors',parcel_association='none: cartographic street symbol footprints are not parcel policy boundaries',geometry_processing=b['representation'],batch_sha256=BATCH_SHA,artifact='pipelines/vd-pdcom/reports/vevey-access',source_plan_status='approved',source_scope_communes=[5890],held_source_paths=[p['index'] for p in b['source_native_proof']['candidates'] if p['disposition']=='whole_support_hold'],native_topology_held_source_paths=[p['index'] for p in b['source_native_proof']['candidates'] if p['disposition']=='native_topology_hold'],delivered_class_keys=['calmed_access'],current_regulation=False,parcel_links_allowed=False,complete_map=False)
            c.execute('SELECT ST_IsValid(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326))',(g,))
            if c.fetchone()[0] is not True:raise ValueError('access_invalid_4326')
            c.execute("""INSERT INTO bronze_ch.vd_pdcom_sectors(id,document_id,page_number,label,geom,alignment_rmse_m,validation_evidence,review_status) VALUES(%s,%s,87,%s,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326),%s,%s,'review_required') ON CONFLICT DO NOTHING""",(sid,DOC,label,g,b['alignment']['holdout_rmse_m'],Json(evidence)))
            c.execute("""SELECT document_id::text,label,review_status,source_precision_m,validated_by,validation_evidence,ST_Equals(geom,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)) FROM bronze_ch.vd_pdcom_sectors WHERE id=%s""",(g,sid))
            if c.fetchone()!=(DOC,label,'review_required',None,None,evidence,True):raise ValueError('access_existing_state_changed')
            c.execute('SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_candidates WHERE sector_id=%s',(sid,))
            if c.fetchone()[0]:raise ValueError('access_unexpected_parcels')
            c.execute('SELECT commune_bfs,validation_evidence FROM gold_ch.v_vd_pdcom_review_sectors WHERE id=%s',(sid,));communes,ve=c.fetchone()
            if communes!=[5890] or ve!=evidence:raise ValueError('access_attribution')
            out.append(dict(sector_id=sid,key=f['key'],feature_kind='source_native_cartographic_area',source_paths=len(f['source_paths']),commune_bfs=communes))
    return dict(collections=len(out),source_paths=211,parcel_pairs=0,candidates=out,publication_status='internal_review_only',review_status='review_required')
