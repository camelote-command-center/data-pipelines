"""Sixteen hash-bound historical SDRM source contours for existing private review routes.

No current capacity, legal precision, public release or commune completion is inferred.
"""
import hashlib,json,math,uuid
from pathlib import Path
from psycopg2.extras import Json
from shapely.geometry import shape
from shapely.affinity import affine_transform
from official_references import refresh
from cadastral_types import KINDS
DOC='860247ca-7943-5cfd-a948-7f1d70fed5d9'
SHA='a267218ebe407b251ddf4d8e5d13c89b1ff68b2a41086d60476ad8bad66f3b67'
BATCH_SHA='306aced8b8e6b464fb0cae19cb3ce086ac8c049b801c89f59925ac372dadb16e'
SITES={'D2','E1','E2','E3','Ei1','L2','L3','L4','L5','L6','L7','P1','P2','P3','P4','P5'}
SEMANTICS={'landscape_internal_issues','landscape_wider_issues','allocation_procedure_state2011'}

def sector_id(site):
    if site not in SITES:raise ValueError('sdrm_unreviewed_site')
    return str(uuid.uuid5(uuid.NAMESPACE_URL,DOC+'#site'+site+'#private-historical-outline2026-09-22'))

def validate_artifacts(b):
    if (b['document_id'],b['source_sha256'],b['page_number'])!=(DOC,SHA,13):raise ValueError('sdrm_source_scope')
    fs=b['features'];fit=b['fit'];controls=fit['controls']
    if len(fs)!=16 or {x['site_id'] for x in fs}!=SITES:raise ValueError('sdrm_site_scope')
    if len(controls)!=8 or sum(c['role']=='train' for c in controls)!=4 or sum(c['role']=='holdout' for c in controls)!=4:raise ValueError('sdrm_control_split')
    held=[c for c in controls if c['role']=='holdout']
    if not all(math.isfinite(c['error_m']) and 0<=c['error_m']<=10 for c in held):raise ValueError('sdrm_control_gate')
    rmse=math.sqrt(sum(c['error_m']**2 for c in held)/4)
    if abs(rmse-fit['holdout_rmse_m'])>1e-9 or not fit['screen_passed']:raise ValueError('sdrm_control_rmse')
    hull=shape(b['control_hull_pdf']);a,bb,e,n=fit['coefficient_a_b_e_n'];pairs=0
    for f in fs:
        source=shape(f['source_geometry_pdf']);geo=shape(f['geometry_lv95'])
        if not source.is_valid or not geo.is_valid or source.is_empty or geo.is_empty or not hull.covers(source):raise ValueError('sdrm_geometry_support')
        if not geo.equals_exact(affine_transform(source,[a,bb,bb,-a,e,n]),1e-7):raise ValueError('sdrm_geometry_transform')
        if abs((geo.area/f['source_index_area_m2']-1)*100)>5:raise ValueError('sdrm_source_area')
        if (f['source_semantic_class'] not in SEMANTICS or f['status']!='private_historical_source_contour_review_required' or f['publication_status']!='internal_review_only'):raise ValueError('sdrm_source_semantics')
        seen=set()
        for snap in f['references']:
            if snap['invalid_reference_geometries'] or not snap['pairs']:raise ValueError('sdrm_reference_invalid')
            for p in snap['pairs']:
                at=p['attributes'];eg=at['EGRID'];g=shape(p['reference_geometry_lv95'])
                if eg in seen or at['NO_COM_FED']!=snap['bfs'] or KINDS.get(at['GENRE_TXT'])!='bien_fonds' or not g.is_valid:raise ValueError('sdrm_reference_identity')
                if not g.intersects(geo) or g.intersection(geo).area<=0:raise ValueError('sdrm_reference_overlap')
                seen.add(eg);pairs+=1
    if pairs!=139:raise ValueError('sdrm_pair_scope')
    return b

def load_artifacts():
    root=Path(__file__).parent/'reports/sdrm-private-batch';raw=(root/'batch.json').read_bytes()
    if hashlib.sha256(raw).hexdigest()!=BATCH_SHA:raise ValueError('sdrm_batch_changed_requires_review')
    b=validate_artifacts(json.loads(raw))
    if hashlib.sha256((root/'reviewed-controls.json').read_bytes()).hexdigest()!=b['fit']['frozen_control_sha256']:raise ValueError('sdrm_control_evidence_changed')
    return b

def persist(conn,document_id,sha):
    if str(document_id)!=DOC or sha!=SHA:return None
    with conn.cursor() as c:
        c.execute('SELECT sha256,page_count FROM bronze_ch.vd_pdcom_documents WHERE id=%s',(DOC,))
        stored=c.fetchone()
        if not stored or stored[0]!=SHA or not stored[1] or stored[1]<13:raise ValueError('sdrm_stored_source_mismatch')
    b=load_artifacts();results=[]
    for f in b['features']:
        site=f['site_id'];sid=sector_id(site);geom=json.dumps(f['geometry_lv95']);refs=[];frozen=[]
        for snap in f['references']:
            current=refresh(conn,DOC,snap['bfs'],snap['bounds_lv95']);refs.append(current)
            frozen.extend({'egrid':p['attributes']['EGRID'],'kind':p['attributes']['GENRE_TXT'],'geometry':p['reference_geometry_lv95']} for p in snap['pairs'])
        label='SDRM '+site+' — historique 2014 — '+f['source_semantic_class']
        evidence={'source_document_id':DOC,'source_sha256':SHA,'page_number':13,'source_path_index':f['source_path_index'],'source_map_date':'2014-07-10','source_geometry_kind':'Historical full source contour; not legal boundary, net available land or current capacity','source_semantic_class':f['source_semantic_class'],'source_precision':f['source_precision'],'source_index_area_m2':f['source_index_area_m2'],'transformed_area_m2':f['transformed_area_m2'],'source_area_difference_percent':f['area_difference_percent'],'boundary_review':f['boundary_review'],'alignment':b['fit'],'complete_control_hull_support':True,'official_references':refs,'currentness':b['approval_currentness'],'validation_status':'review_required','publication_status':'internal_review_only','buffer_limit':'10m boundary heuristic, not confidence interval or boundary accuracy','artifact':'pipelines/vd-pdcom/reports/sdrm-private-batch','batch_sha256':BATCH_SHA}
        with conn,conn.cursor() as c:
            c.execute("SET LOCAL statement_timeout='90s'");c.execute('SELECT pg_advisory_xact_lock(572500301)')
            c.execute('DROP TABLE IF EXISTS pg_temp.sdrm_current_pairs')
            c.execute('''CREATE TEMP TABLE sdrm_current_pairs ON COMMIT DROP AS WITH g AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) geom)
            SELECT r.*,ST_Intersection(r.geom,g.geom) overlap,ST_Area(r.geom) area,ST_DWithin(r.geom,ST_Boundary(g.geom),10) edge
            FROM bronze_ch.vd_pdcom_parcel_references r,g WHERE snapshot_id=ANY(%s::uuid[]) AND ST_Intersects(r.geom,g.geom) AND ST_Area(ST_Intersection(r.geom,g.geom))>0''',(geom,[x['snapshot_id'] for x in refs]))
            c.execute('SELECT egrid FROM sdrm_current_pairs');keys=[x[0] for x in c.fetchall()]
            if len(keys)!=len(set(keys)) or set(keys)!={x['egrid'] for x in frozen}:raise ValueError('sdrm_reference_membership_changed_requires_review')
            c.execute('''WITH f AS (SELECT x->>'egrid' egrid,x->>'kind' kind,ST_SetSRID(ST_GeomFromGeoJSON(x->'geometry'),2056) geom FROM jsonb_array_elements(%s::jsonb) x)
            SELECT count(*) FROM sdrm_current_pairs n LEFT JOIN f USING(egrid) WHERE f.egrid IS NULL OR NOT ST_Equals(n.geom,f.geom) OR (n.official_attributes->>'GENRE_TXT') IS DISTINCT FROM f.kind''',(Json(frozen),))
            if c.fetchone()[0]:raise ValueError('sdrm_reference_geometry_changed_requires_review')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_sectors(id,document_id,page_number,label,geom,alignment_rmse_m,validation_evidence,review_status)
            VALUES(%s,%s,13,%s,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326),%s,%s,'review_required') ON CONFLICT DO NOTHING''',(sid,DOC,label,geom,b['fit']['holdout_rmse_m'],Json(evidence)))
            c.execute('SELECT commune_bfs FROM gold_ch.v_vd_pdcom_review_sectors WHERE id=%s',(sid,))
            if c.fetchone()[0]!=sorted(x['bfs'] for x in f['references']):raise ValueError('sdrm_current_commune_attribution_mismatch')
            c.execute("SELECT label,review_status,ST_Equals(geom,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)) FROM bronze_ch.vd_pdcom_sectors WHERE id=%s",(geom,sid))
            if c.fetchone()!=(label,'review_required',True):raise ValueError('sdrm_existing_sector_changed_requires_review')
            c.execute('SELECT egrid FROM bronze_ch.vd_pdcom_parcel_candidates WHERE sector_id=%s',(sid,));old={x[0] for x in c.fetchall()}
            if old and old!=set(keys):raise ValueError('sdrm_existing_pair_membership_conflict')
            c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_candidates p JOIN sdrm_current_pairs n USING(egrid) WHERE p.sector_id=%s AND (NOT ST_Equals(p.geom,ST_Transform(n.overlap,4326)) OR p.overlap_m2 IS DISTINCT FROM ST_Area(n.overlap) OR p.parcel_area_m2 IS DISTINCT FROM n.area OR p.overlap_fraction IS DISTINCT FROM LEAST(1,ST_Area(n.overlap)/n.area) OR p.boundary_review_required IS DISTINCT FROM n.edge OR p.uncertainty_buffer_m IS DISTINCT FROM 10 OR p.cadastral_object_kind IS DISTINCT FROM 'bien_fonds')''',(sid,))
            if c.fetchone()[0]:raise ValueError('sdrm_existing_pair_geometry_conflict')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_parcel_candidates(sector_id,egrid,overlap_m2,parcel_area_m2,overlap_fraction,boundary_review_required,uncertainty_buffer_m,geom,cadastral_object_kind,cadastral_type_evidence)
            SELECT %s,n.egrid,ST_Area(n.overlap),n.area,LEAST(1,ST_Area(n.overlap)/n.area),n.edge,10,ST_Transform(n.overlap,4326),'bien_fonds',
            jsonb_build_object('source_url',s.query_url,'response_sha256',s.response_sha256,'reference_snapshot_ids',jsonb_build_array(n.snapshot_id),'official_attributes',n.official_attributes,'status','matched')
            FROM sdrm_current_pairs n JOIN bronze_ch.vd_pdcom_parcel_reference_snapshots s ON s.id=n.snapshot_id ON CONFLICT DO NOTHING''',(sid,))
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_candidate_parcel_references(sector_id,egrid,snapshot_id) SELECT %s,egrid,snapshot_id FROM sdrm_current_pairs ON CONFLICT(sector_id,egrid) DO UPDATE SET snapshot_id=EXCLUDED.snapshot_id''',(sid,))
            c.execute("UPDATE bronze_ch.vd_pdcom_sectors SET validation_evidence=%s WHERE id=%s AND review_status='review_required'",(Json(evidence),sid))
        results.append({'site_id':site,'sector_id':sid,'parcel_pairs':len(keys),'references':refs})
    return {'sectors':len(results),'parcel_pairs':sum(x['parcel_pairs'] for x in results),'candidates':results,'status':'review_required','publication_status':'internal_review_only'}
