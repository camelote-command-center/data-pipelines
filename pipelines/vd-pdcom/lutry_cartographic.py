"""Pinned approximate historic Lutry map depiction; reservations unresolved, private only."""
import hashlib,json,uuid,math
from pathlib import Path
from psycopg2.extras import Json
from shapely.geometry import shape,Polygon,MultiPoint
from official_references import refresh
from cadastral_types import KINDS
DOC='1146a4f0-9281-5fb9-80c8-462a710229db'
SHA='f2dcfe50c628863dd1bb7fcb6632536cecb67d112b61b28fea0ed7c4e118190d'
BATCH_SHA='04b228e57c57e2aa4c7b7a5f9aad558c6edbe7feb861f73f69c69fe3b48126c3'
KEYS={'undefined_destination'}
def sector_id(key):
    if key not in KEYS:raise ValueError('lutry_unreviewed_category')
    return str(uuid.uuid5(uuid.NAMESPACE_URL,DOC+'#manual-raster-transition-v1#'+key))
def validate(b):
    if (b['document_id'],b['source_sha256'])!=(DOC,SHA):raise ValueError('lutry_source')
    if len(b['features'])!=1 or {f['key'] for f in b['features']}!=KEYS:raise ValueError('lutry_scope')
    if b['publication_status']!='internal_review_only' or b['source_precision_m'] is not None or b['review_status']!='review_required':raise ValueError('lutry_qualification')
    if b['source_plan_status']!='approved_with_reservation' or b['reservation_applicability']!='unresolved_sector_specific_applicability':raise ValueError('lutry_reservations')
    sem=b['literal_semantic_review']
    if sem['reservation_status']!=b['reservation_applicability'] or sem['qualified_release_authorized'] or sem['commune_complete'] or len(sem['corpus_gaps'])!=2:raise ValueError('lutry_literal_limits')
    if b['independent_geographic_decision']!='accepted_private_historical_cartographic_depiction_only':raise ValueError('lutry_qa')
    rep=b['representation']
    if rep!={'method':'manually_reviewed_visible_raster_colour_transition','vertex_count':32,'all_edges_uncertain':True,'exact_visible_fill':False,'exact_policy_extent':False,'green_notch_preserved':True,'snapping':'none','gaps_joined':False,'complete_policy_sector':False}:raise ValueError('lutry_representation')
    controls=b['frozen_grid']['points']
    expected=[('T1','train',[1095,486],[543000,154000]),('T2','train',[1737,487],[544000,154000]),('T3','train',[480,3075],[542000,150000]),('T4','train',[1118,3073],[543000,150000]),('C1','check',[1100,1136],[543000,153000]),('C2','check',[1749,1776],[544000,152000])]
    if [(c['id'],c['split'],c['pixel'],c['lv03']) for c in controls]!=expected:raise ValueError('lutry_frozen_grid')
    hull=MultiPoint([c['pixel'] for c in controls if c['split']=='train']).convex_hull;t=b['trace'];p=shape(t['geometry_pixels']);pts=list(p.exterior.coords)[:-1]
    if not p.is_valid or len(pts)!=32 or not hull.covers(p) or pts!=[tuple(v) for v in t['frozen_trace']['source_pixels']]:raise ValueError('lutry_whole_trace')
    matrix=b['grid_fit']['affine_pixel_to_lv03'];rr=t['official_reframe_responses']
    if len(rr)!=32:raise ValueError('lutry_vertex_count')
    for (x,y),r in zip(pts,rr):
        lv03=[row[0]*x+row[1]*y+row[2] for row in matrix]
        if math.dist(lv03,r['lv03'])>1e-7 or r['lv95']!=[float(r['response']['easting']),float(r['response']['northing'])]:raise ValueError('lutry_exact_conversion')
        if abs(r['lv95'][0]-lv03[0]-2000000)>10 or abs(r['lv95'][1]-lv03[1]-1000000)>10:raise ValueError('lutry_conversion_anomaly')
    g=Polygon([r['lv95'] for r in rr]);f=b['features'][0]
    if not g.is_valid or not g.equals_exact(shape(t['geometry_lv95']),1e-10) or not g.equals_exact(shape(f['geometry_lv95']),1e-10) or f['invalid_reference_envelope_overlap']:raise ValueError('lutry_ground_geometry')
    residual=[]
    for c in controls:
        pred=[row[0]*c['pixel'][0]+row[1]*c['pixel'][1]+row[2] for row in matrix]
        if c['split']=='check':residual.append(math.dist(pred,c['lv03']))
    if abs(math.sqrt(sum(v*v for v in residual)/2)-b['alignment']['holdout_rmse_m'])>1e-8:raise ValueError('lutry_diagnostic')
    if len(f['expected_pairs'])!=32 or len({r['egrid'] for r in f['expected_pairs']})!=32:raise ValueError('lutry_pair_count')
    for r in f['expected_pairs']:
        at=r['attributes'];q=shape(r['reference_geometry'])
        if at['EGRID']!=r['egrid'] or at['NO_COM_FED']!=5606 or at['GENRE_TXT'] not in KINDS or not q.is_valid or g.intersection(q).area<=0:raise ValueError('lutry_reference')
    return b
def load_artifacts():
    raw=(Path(__file__).parent/'reports/lutry-cartographic/batch.json').read_bytes()
    if hashlib.sha256(raw).hexdigest()!=BATCH_SHA:raise ValueError('lutry_changed_batch_requires_review')
    return validate(json.loads(raw))
def persist(conn,document_id,sha):
    if str(document_id)!=DOC or sha!=SHA:return None
    with conn.cursor() as c:
        c.execute('SELECT sha256,page_count,plan_status FROM bronze_ch.vd_pdcom_documents WHERE id=%s',(DOC,));d=c.fetchone()
        if not d or d[0]!=SHA or not d[1] or d[1]<11 or d[2]!='approved_with_reservation':raise ValueError('lutry_cartographic_stored_source')
    b=load_artifacts();refs=[refresh(conn,DOC,5606,r['bounds']) for r in b['references']];results=[]
    with conn.cursor() as c:
        c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_references a JOIN bronze_ch.vd_pdcom_parcel_references z USING(egrid) WHERE a.snapshot_id=ANY(%s::uuid[]) AND z.snapshot_id=ANY(%s::uuid[]) AND a.snapshot_id<z.snapshot_id AND (NOT ST_Equals(a.geom,z.geom) OR a.commune_bfs IS DISTINCT FROM z.commune_bfs OR a.official_attributes IS DISTINCT FROM z.official_attributes)''',([x['snapshot_id'] for x in refs],[x['snapshot_id'] for x in refs]))
        if c.fetchone()[0]:raise ValueError('lutry_cartographic_conflicting_tile_reference')
    for f in b['features']:
        sid=sector_id(f['key']);geom=json.dumps(f['geometry_lv95']);label='Lutry — représentation cartographique historique approximative — '+f['label']
        frozen=[{'egrid':x['egrid'],'kind':x['attributes']['GENRE_TXT'],'geometry':x['reference_geometry']} for x in f['expected_pairs']]
        evidence={'source_sha256':SHA,'source_path_ids':f['path_ids'],'source_category':f['category'],'page_number':8,'grouping':'One approximate manual cartographic depiction, not a complete policysector','semantics':b['semantics'],'reservation':b['reservation'],'reservation_applicability':b['reservation_applicability'],'source_plan_status':b['source_plan_status'],'alignment':b['alignment'],'literal_semantics':b['literal_semantic_review'],'representation':b['representation'],'independent_geographic_decision':b['independent_geographic_decision'],'source_precision':'unknown','source_precision_m':None,'boundary_buffer':'10m review heuristic, not a measured error bound','review_status':'review_required','publication_status':'internal_review_only','reference_scope':'Current official Lutry parcels; contextual intersection only, no parcel-level applicability','official_references':refs,'batch_sha256':BATCH_SHA,'artifact':'pipelines/vd-pdcom/reports/lutry-cartographic','ground_checks':'Unchanged six printed LV03 grid intersections,4train2reserved;32 exact official REFRAME datum conversions independently replayed. Independent current road/watercourse context only, no surveyed accuracy.'}
        with conn,conn.cursor() as c:
            c.execute("SET LOCAL statement_timeout='90s'");c.execute('SELECT pg_advisory_xact_lock(560600301)')
            c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_references WHERE snapshot_id=ANY(%s::uuid[]) AND geom && ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) AND NOT ST_IsValid(geom)''',([x['snapshot_id'] for x in refs],geom))
            if c.fetchone()[0]:raise ValueError('lutry_cartographic_invalid_current_reference')
            c.execute('DROP TABLE IF EXISTS pg_temp.lutry_cartographic_pairs')
            c.execute('''CREATE TEMP TABLE lutry_cartographic_pairs ON COMMIT DROP AS WITH g AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) geom),r AS MATERIALIZED (SELECT DISTINCT ON(egrid) * FROM bronze_ch.vd_pdcom_parcel_references WHERE snapshot_id=ANY(%s::uuid[]) AND ST_IsValid(geom) ORDER BY egrid,snapshot_id)
            SELECT r.*,ST_Intersection(r.geom,g.geom) overlap,ST_Area(r.geom) area,ST_DWithin(r.geom,ST_Boundary(g.geom),10) edge,CASE r.official_attributes->>'GENRE_TXT' WHEN 'DDP superficie' THEN 'ddp_superficie' WHEN 'DDP source' THEN 'ddp_source' WHEN 'parcelle privée' THEN 'bien_fonds' WHEN 'DP communal' THEN 'bien_fonds' WHEN 'DP cantonal' THEN 'bien_fonds' ELSE 'unknown' END object_kind FROM r,g WHERE ST_Intersects(r.geom,g.geom) AND ST_Area(ST_Intersection(r.geom,g.geom))>0''',(geom,[x['snapshot_id'] for x in refs]))
            c.execute('SELECT egrid FROM lutry_cartographic_pairs');keys={x[0] for x in c.fetchall()}
            if keys!={x['egrid'] for x in frozen}:raise ValueError('lutry_cartographic_current_membership_changed')
            c.execute('''WITH f AS (SELECT x->>'egrid' egrid,x->>'kind' kind,ST_SetSRID(ST_GeomFromGeoJSON(x->'geometry'),2056) geom FROM jsonb_array_elements(%s::jsonb) x) SELECT count(*) FROM lutry_cartographic_pairs n LEFT JOIN f USING(egrid) WHERE f.egrid IS NULL OR NOT ST_Equals(n.geom,f.geom) OR (n.official_attributes->>'GENRE_TXT') IS DISTINCT FROM f.kind''',(Json(frozen),))
            if c.fetchone()[0]:raise ValueError('lutry_cartographic_current_geometry_changed')
            c.execute('SELECT ST_IsValid(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)),(SELECT bool_and(ST_IsValid(ST_Transform(overlap,4326))) FROM lutry_cartographic_pairs)',(geom,))
            if c.fetchone()!=(True,True):raise ValueError('lutry_cartographic_transformed_geometry_invalid')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_sectors(id,document_id,page_number,label,geom,alignment_rmse_m,validation_evidence,review_status) VALUES(%s,%s,%s,%s,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326),%s,%s,'review_required') ON CONFLICT DO NOTHING''',(sid,DOC,f['page'],label,geom,b['alignment']['holdout_rmse_m'],Json(evidence)))
            c.execute("SELECT label,review_status,source_precision_m,validated_by,ST_Equals(geom,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)) FROM bronze_ch.vd_pdcom_sectors WHERE id=%s",(geom,sid))
            if c.fetchone()!=(label,'review_required',None,None,True):raise ValueError('lutry_cartographic_existing_sector_changed')
            c.execute('SELECT commune_bfs FROM gold_ch.v_vd_pdcom_review_sectors WHERE id=%s',(sid,))
            if c.fetchone()[0]!=[5606]:raise ValueError('lutry_cartographic_attribution')
            c.execute('SELECT egrid FROM bronze_ch.vd_pdcom_parcel_candidates WHERE sector_id=%s',(sid,));old={x[0] for x in c.fetchall()}
            if old and old!=keys:raise ValueError('lutry_cartographic_existing_membership')
            c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_candidates p JOIN lutry_cartographic_pairs n USING(egrid) WHERE p.sector_id=%s AND (NOT ST_Equals(p.geom,ST_Transform(n.overlap,4326)) OR p.overlap_m2 IS DISTINCT FROM ST_Area(n.overlap) OR p.parcel_area_m2 IS DISTINCT FROM n.area OR p.overlap_fraction IS DISTINCT FROM LEAST(1,ST_Area(n.overlap)/n.area) OR p.boundary_review_required IS DISTINCT FROM n.edge OR p.uncertainty_buffer_m IS DISTINCT FROM 10 OR p.cadastral_object_kind IS DISTINCT FROM n.object_kind)''',(sid,))
            if c.fetchone()[0]:raise ValueError('lutry_cartographic_existing_values')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_parcel_candidates(sector_id,egrid,overlap_m2,parcel_area_m2,overlap_fraction,boundary_review_required,uncertainty_buffer_m,geom,cadastral_object_kind,cadastral_type_evidence) SELECT %s,n.egrid,ST_Area(n.overlap),n.area,LEAST(1,ST_Area(n.overlap)/n.area),n.edge,10,ST_Transform(n.overlap,4326),n.object_kind,jsonb_build_object('source_url',s.query_url,'response_sha256',s.response_sha256,'reference_snapshot_ids',jsonb_build_array(n.snapshot_id),'official_attributes',n.official_attributes,'status','matched') FROM lutry_cartographic_pairs n JOIN bronze_ch.vd_pdcom_parcel_reference_snapshots s ON s.id=n.snapshot_id ON CONFLICT DO NOTHING''',(sid,))
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_candidate_parcel_references(sector_id,egrid,snapshot_id) SELECT %s,egrid,snapshot_id FROM lutry_cartographic_pairs ON CONFLICT(sector_id,egrid) DO UPDATE SET snapshot_id=EXCLUDED.snapshot_id''',(sid,))
            c.execute('UPDATE bronze_ch.vd_pdcom_sectors SET validation_evidence=%s WHERE id=%s',(Json(evidence),sid))
        results.append({'key':f['key'],'sector_id':sid,'parcel_pairs':len(keys)})
    return {'collections':len(results),'parcel_pairs':sum(r['parcel_pairs'] for r in results),'candidates':results,'publication_status':'internal_review_only','review_status':'review_required'}
