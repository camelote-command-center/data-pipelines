"""Four source-reviewed Nord-vaudois activity outlines, delivered only to private review routes."""
import json,uuid
from pathlib import Path
from psycopg2.extras import Json
from official_references import refresh
from cadastral_types import KINDS
DOC='95594a29-225b-5388-943a-8e45c423d7c5'
SHA='3b37f3c821d16573023f225211c80c0b3f87ee45df726724c6ca3edf2778e1e9'
SITES={'47':(5552,1),'85':(5903,1),'103':(5921,1),'12':(5929,1)}
def sector_id(code):
    if code not in SITES:raise ValueError('nord_activity_unreviewed_site')
    return str(uuid.uuid5(uuid.NAMESPACE_URL,DOC+'#site'+code+'#private-outline2026-09-21'))
def load_artifacts():
    p=Path(__file__).parent/'reports/nord-activity-private-batch'
    fs=json.loads((p/'features.geojson').read_text())['features'];ev=json.loads((p/'evidence.json').read_text());qa=json.loads((p/'parcel-qa.json').read_text());frozen=json.loads((p/'official-parcels.geojson').read_text())['features']
    if {f['properties']['site_code'] for f in fs}!=set(SITES) or len(fs)!=4:raise ValueError('nord_activity_artifact_scope')
    for f in fs:
        pr=f['properties'];code=pr['site_code'];e=ev[code]
        holdout=e['independent_shape_holdout']
        if ((pr['bfs'],pr['page'])!=SITES[code] or not e['reference_held_out_of_fit']
            or holdout['kind']!='parcelle privée' or holdout['bfs']!=pr['bfs']
            or not holdout['iou']>0.98 or not 0<=holdout['hausdorff_m']<5):
            raise ValueError('nord_activity_control_review')
        if pr['status']!='private_indicative_outline_review_required':raise ValueError('nord_activity_review_status')
    return fs,ev,{q['site_code']:q for q in qa},frozen

def persist(conn,document_id,sha):
    if str(document_id)!=DOC or sha!=SHA:return None
    fs,ev,qa,frozen=load_artifacts();results=[]
    for f in fs:
        pr=f['properties'];code=pr['site_code'];bfs,page=SITES[code];q=qa[code];g=json.dumps(f['geometry']);sid=sector_id(code);ref=refresh(conn,DOC,bfs,q['bounds'])
        evidence={**ev[code],'source_document_id':DOC,'source_sha256':SHA,'page_number':page,'source_geometry_kind':'Indicative full source zone outline, not a legal boundary or net available land','source_precision':'unknown','official_reference':ref,'source_area_m2':q['area_m2'],'area_difference_limit':'Source category footprint only; no stated reserve area comparison or forced scaling.','validation_status':'review_required','publication_status':'internal_review_only','buffer_limit':'10m boundary heuristic; not an accuracy confidence interval. Public-road edge intersections retained.','artifact':'pipelines/vd-pdcom/reports/nord-activity-private-batch'}
        with conn,conn.cursor() as c:
            c.execute("SET LOCAL statement_timeout='90s'")
            c.execute('SELECT pg_advisory_xact_lock(572500301)')
            c.execute('DROP TABLE IF EXISTS pg_temp.nord_activity_current_pairs')
            c.execute('''CREATE TEMP TABLE nord_activity_current_pairs ON COMMIT DROP AS
            WITH g AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) geom)
            SELECT r.*,ST_Intersection(r.geom,g.geom) overlap,ST_Area(r.geom) area,ST_DWithin(r.geom,ST_Boundary(g.geom),10) edge
            FROM bronze_ch.vd_pdcom_parcel_references r,g WHERE snapshot_id=%s AND ST_Intersects(r.geom,g.geom) AND ST_Area(ST_Intersection(r.geom,g.geom))>0''',(g,ref['snapshot_id']))
            c.execute('SELECT egrid FROM nord_activity_current_pairs');actual={x[0] for x in c.fetchall()}
            if actual!={x['egrid'] for x in q['pairs']}:raise ValueError('nord_activity_reference_membership_changed_requires_review')
            c.execute('''WITH frozen AS (SELECT x->'properties'->>'EGRID' egrid,x->'properties'->>'GENRE_TXT' kind,ST_GeomFromEWKB(decode(x->'properties'->>'geom_ewkb','hex')) geom FROM jsonb_array_elements(%s::jsonb) x)
            SELECT count(*) FROM nord_activity_current_pairs n LEFT JOIN frozen f USING(egrid) WHERE f.egrid IS NULL OR NOT ST_Equals(n.geom,f.geom) OR (n.official_attributes->>'GENRE_TXT') IS DISTINCT FROM f.kind''',(Json(frozen),))
            if c.fetchone()[0]:raise ValueError('nord_activity_reference_geometry_changed_requires_review')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_sectors(id,document_id,page_number,label,geom,alignment_rmse_m,validation_evidence,review_status)
            VALUES(%s,%s,%s,%s,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326),%s,%s,'review_required') ON CONFLICT DO NOTHING''',(sid,DOC,page,ev[code]['source_semantics']['label'],g,ev[code]['consistent_control_rmse_m'],Json(evidence)))
            c.execute('SELECT commune_bfs FROM gold_ch.v_vd_pdcom_review_sectors WHERE id=%s',(sid,))
            if c.fetchone()[0]!=[bfs]:raise ValueError('nord_activity_geographic_attribution_mismatch')
            c.execute("SELECT review_status,ST_Equals(geom,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)) FROM bronze_ch.vd_pdcom_sectors WHERE id=%s",(g,sid));status,equal=c.fetchone()
            if status!='review_required' or not equal:raise ValueError('nord_activity_existing_sector_changed_requires_review')
            c.execute('SELECT egrid FROM bronze_ch.vd_pdcom_parcel_candidates WHERE sector_id=%s',(sid,));old={x[0] for x in c.fetchall()}
            if old and old!=actual:raise ValueError('nord_activity_existing_candidate_membership_conflict')
            c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_candidates p JOIN nord_activity_current_pairs n USING(egrid) WHERE p.sector_id=%s AND NOT ST_Equals(p.geom,ST_Transform(n.overlap,4326))''',(sid,))
            if c.fetchone()[0]:raise ValueError('nord_activity_existing_candidate_geometry_conflict')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_parcel_candidates(sector_id,egrid,overlap_m2,parcel_area_m2,overlap_fraction,boundary_review_required,uncertainty_buffer_m,geom,cadastral_object_kind,cadastral_type_evidence)
            SELECT %s,n.egrid,ST_Area(n.overlap),n.area,LEAST(1,ST_Area(n.overlap)/n.area),n.edge,10,ST_Transform(n.overlap,4326),
            COALESCE(%s::jsonb->>(n.official_attributes->>'GENRE_TXT'),'unknown'),
            jsonb_build_object('source_url',s.query_url,'response_sha256',s.response_sha256,'reference_snapshot_ids',jsonb_build_array(n.snapshot_id),'official_attributes',n.official_attributes,'status','matched')
            FROM nord_activity_current_pairs n JOIN bronze_ch.vd_pdcom_parcel_reference_snapshots s ON s.id=n.snapshot_id ON CONFLICT DO NOTHING''',(sid,Json(KINDS)))
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_candidate_parcel_references(sector_id,egrid,snapshot_id) SELECT %s,egrid,snapshot_id FROM nord_activity_current_pairs ON CONFLICT(sector_id,egrid) DO UPDATE SET snapshot_id=EXCLUDED.snapshot_id''',(sid,))
            c.execute("UPDATE bronze_ch.vd_pdcom_sectors SET validation_evidence=%s WHERE id=%s AND review_status='review_required'",(Json(evidence),sid))
            c.execute("UPDATE bronze_ch.vd_pdcom_communes SET extraction_status='candidate_vectors',blocker='Private indicative Nord-vaudois outline only; exact-version approval, currentness and legal-boundary precision unresolved' WHERE commune_bfs=%s",(bfs,))
        results.append({'sector_id':sid,'site_code':code,'parcel_pairs':len(actual),'reference':ref})
    return {'sectors':len(results),'parcel_pairs':sum(x['parcel_pairs'] for x in results),'candidates':results,'status':'review_required'}
