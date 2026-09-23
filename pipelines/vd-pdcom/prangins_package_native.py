"""Pinned historical source lines/point anchors on the existing private route."""
import json,hashlib,uuid
from pathlib import Path
from shapely.geometry import shape,MultiLineString,MultiPoint
from shapely.affinity import affine_transform
from psycopg2.extras import Json
DOC='3336e2a0-6986-5e4b-903b-149dc8d71394'
SHA='a70f0043ed910266197e4619a1d33c2d4c111cc8408dff164b98d5cf138b433c'
BATCH_SHA='08aa3aed4b3d5a154795be398b1f1ef02381758a28c47b48d757f25029257b8c'
def sector_id(key):return str(uuid.uuid5(uuid.NAMESPACE_URL,DOC+'#source-native-2026-09-23#'+str(key)))
def combine(paths,kind):
    gs=[shape(p['geometry_lv95']) for p in paths]
    if kind=='source_native_line':
        if any(g.geom_type not in ('LineString','MultiLineString') for g in gs):raise ValueError('native_line_type')
        return MultiLineString([list(line.coords) for g in gs for line in (list(g.geoms) if g.geom_type=='MultiLineString' else [g])])
    if kind=='source_native_point':
        if any(g.geom_type!='Point' for g in gs):raise ValueError('native_point_type')
        return MultiPoint(gs)
    raise ValueError('native_feature_kind')
def validate(b):
    if (b['document_id'],b['source_sha256'])!=(DOC,SHA):raise ValueError('native_source')
    if b['publication_status']!='internal_review_only' or b['source_precision_m'] is not None:raise ValueError('native_private_precision')
    from prangins_package_alignment import validate_alignment
    validate_alignment(dict(b,features=[dict(source_paths=f['paths']) for f in b['features']]))
    allow={str(x['source_ledger_index']):x for x in b['semantic_allowlist']};seen=set()
    if {f['key'] for f in b['features']}!=set(allow) or len(b['features'])!=len(allow):raise ValueError('native_category_scope')
    for f in b['features']:
        a=allow[f['key']];ids=[p['path_id'] for p in f['paths']]
        if ids!=a['source_path_ids'] or seen.intersection(ids) or len(set(ids))!=len(ids):raise ValueError('native_source_membership')
        seen.update(ids)
        if f['label']!=a['source_label'] or f['feature_kind']!=a['feature_kind']:raise ValueError('native_semantics')
        if a['parcel_links_allowed'] is not False or a['buffer_allowed'] is not False:raise ValueError('native_no_parcels_or_buffer')
        for p in f['paths']:
            g=shape(p['geometry_lv95']);orig=shape(p['geometry_base_pdf'])
            from shapely.affinity import translate
            if not orig.equals_exact(translate(shape(p['geometry_pdf']),-f['translation'][0],-f['translation'][1]),1e-7):raise ValueError('package_source_frame')
            if not g.equals_exact(affine_transform(orig,b['affine']),1e-7):raise ValueError('native_source_transform')
            if g.is_empty or not g.is_valid:raise ValueError('native_invalid_source')
            if f['feature_kind']=='source_native_point':
                rays=p['rays'];common=set(map(tuple,rays[0]))&set(map(tuple,rays[1]))
                if len(rays)!=2 or len(common)!=1 or tuple(orig.coords[0])!=next(iter(common)):raise ValueError('native_anchor')
        g=combine(f['paths'],f['feature_kind'])
        if not shape(f['geometry_lv95']).equals_exact(g,1e-7):raise ValueError('native_collection')
    return b
def load_artifacts():
    raw=(Path(__file__).parent/'reports/prangins-package-maps/native.json').read_bytes()
    if hashlib.sha256(raw).hexdigest()!=BATCH_SHA:raise ValueError('native_changed_source_requires_review')
    return validate(json.loads(raw))
def persist(conn,document_id,sha):
    if str(document_id)!=DOC or sha!=SHA:return None
    b=load_artifacts();out=[]
    with conn,conn.cursor() as c:
        c.execute("SET LOCAL statement_timeout='90s'");c.execute('SELECT pg_advisory_xact_lock(572500301)')
        c.execute('SELECT sha256,page_count FROM bronze_ch.vd_pdcom_documents WHERE id=%s',(DOC,));stored=c.fetchone()
        if not stored or stored[0]!=SHA or not stored[1] or stored[1]<75:raise ValueError('native_stored_source')
        for f in b['features']:
            sid=sector_id(f['key']);g=json.dumps(f['geometry_lv95']);label='Prangins — tracé/symbole indicatif historique — '+f['label'];sem=next(a for a in b['semantic_allowlist'] if str(a['source_ledger_index'])==f['key'])
            evidence=dict(feature_kind=f['feature_kind'],source_sha256=SHA,source_path_ids=[p['path_id'] for p in f['paths']],source_category=f['label'],legend_label_id=sem['label_id'],source_ledger_index=sem['source_ledger_index'],page_number=f['page'],semantic_contract=sem,source_transform=b['affine'],source_frame_translation=f['translation'],coordinate_scope='whole original paths covered by independent training hull',alignment=b['alignment'],source_precision='unknown',source_precision_m=None,review_status='review_required',publication_status='internal_review_only',grouping='Source-native category collection; paths and symbol anchors are not parcels, zoning areas or measured legal boundaries',parcel_association='none: non-area source feature',geometry_processing='Original page-specific native supports with separately verified source frame and ground fit; strict training hull; no buffers or parcel links',batch_sha256=BATCH_SHA,artifact='pipelines/vd-pdcom/reports/prangins-package-maps')
            c.execute('SELECT ST_IsValid(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326))',(g,))
            if c.fetchone()[0] is not True:raise ValueError('native_invalid_4326')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_sectors(id,document_id,page_number,label,geom,alignment_rmse_m,validation_evidence,review_status)
                VALUES(%s,%s,%s,%s,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326),%s,%s,'review_required') ON CONFLICT DO NOTHING''',(sid,DOC,f['page'],label,g,b['alignment']['holdout_rmse_m'],Json(evidence)))
            c.execute('''SELECT document_id::text,label,review_status,source_precision_m,validated_by,validation_evidence,
                ST_Equals(geom,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)) FROM bronze_ch.vd_pdcom_sectors WHERE id=%s''',(g,sid))
            if c.fetchone()!=(DOC,label,'review_required',None,None,evidence,True):raise ValueError('native_existing_state_changed')
            c.execute('SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_candidates WHERE sector_id=%s',(sid,))
            if c.fetchone()[0]:raise ValueError('native_unexpected_parcel_rows')
            c.execute('SELECT commune_bfs,validation_evidence FROM gold_ch.v_vd_pdcom_review_sectors WHERE id=%s',(sid,));communes,ve=c.fetchone()
            if not communes or 'native_attribution' not in ve:raise ValueError('native_attribution_unresolved')
            out.append(dict(sector_id=sid,key=f['key'],feature_kind=f['feature_kind'],source_paths=len(f['paths']),commune_bfs=communes))
    return dict(collections=len(out),source_paths=sum(x['source_paths'] for x in out),parcel_pairs=0,candidates=out,publication_status='internal_review_only',review_status='review_required')
