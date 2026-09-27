"""Pinned historical raster cartographic supports; private review only."""
import hashlib,json,uuid,math
from pathlib import Path
from psycopg2.extras import Json
from shapely.geometry import shape,Polygon,MultiPoint
from shapely.ops import unary_union
from official_references import refresh
from cadastral_types import KINDS
DOC='ecaca53c-511b-5af4-94b2-7925852fc740'
SHA='86b3c0352167167623ab93ebc0aeafd2957e14c0797867a3e982910e65b227eb'
BATCH_SHA='22d36556929094ef4fe5e838c7d6d10d05fd49fcc2edc99aff4876df4b5697f8'
KEYS={'institutions_northwest','public_equipment_middle'}
def sector_id(key):
    if key not in KEYS:raise ValueError('saint_legier_unreviewed_category')
    return str(uuid.uuid5(uuid.NAMESPACE_URL,DOC+'#raster-fragments-v1#'+key))
def validate(b):
    if (b['document_id'],b['source_sha256'])!=(DOC,SHA):raise ValueError('saint_legier_source')
    if len(b['features'])!=2 or {f['key'] for f in b['features']}!=KEYS:raise ValueError('saint_legier_scope')
    if b['publication_status']!='internal_review_only' or b['source_precision_m'] is not None or not b['predecessor_scope_only']:raise ValueError('saint_legier_qualification')
    if b['independent_geographic_decision']!='accepted_private_historical_raster_support_only':raise ValueError('saint_legier_qa')
    if b['municipal_currentness']['classification']!='historical_not_current_municipal_reference':raise ValueError('saint_legier_currentness')
    rep=b['raster_representation']
    if rep['contour_mode']!='RETR_EXTERNAL' or rep['simplification_epsilon_base_pixels']!=2 or rep['morphology']!='none' or rep['gaps_joined'] or not rep['all_edges_uncertain'] or rep['omitted_interior_color_holes']!={'cyan_461':147,'cyan_825':0,'public_blue_663':2,'public_blue_668':0}:raise ValueError('saint_legier_raster_representation')
    controls=b['frozen_grid']['controls']
    if len(controls)!=6 or len({c['id'] for c in controls})!=6 or sum(c['role']=='train' for c in controls)!=4 or not b['frozen_grid']['roles_frozen_before_first_fit']:raise ValueError('saint_legier_grid')
    for c in controls:
        e,n=map(int,c['id'].replace('printed_grid_','').split('_'))
        if c['ground_xy']!=[e*1000,n*1000] or c['role']!=('held' if e==557 else 'train') or c['ground_crs']!='EPSG:21781':raise ValueError('saint_legier_grid_identity')
    hull=MultiPoint([c['pdf_xy'] for c in controls if c['role']=='train']).convex_hull
    if len(b['landmark_review'])!=8 or sum(c['status']=='unresolved_absent_on_original_map' for c in b['landmark_review'])!=2:raise ValueError('saint_legier_landmark_ledger')
    m=b['grid_fit']['matrix'];paths=b['source_raster_proof'];derived=[]
    if {c['id'] for c in paths}!={'cyan_461','cyan_825','public_blue_663','public_blue_668'}:raise ValueError('saint_legier_raster_ids')
    if len(b['conversion_replay']['checks'])!=118 or not b['conversion_replay']['all_vertices_replayed_sequentially'] or any(c['delta_m']>=1e-6 for c in b['conversion_replay']['checks']):raise ValueError('saint_legier_conversion_replay')
    for c in paths:
        raw=Polygon(c['raw_contour_baseimage_pixels']);approx=Polygon(c['boundary_baseimage_pixels'])
        if not approx.is_valid or raw.hausdorff_distance(approx)>2.000001:raise ValueError('saint_legier_simplification')
        if not hull.covers(MultiPoint([(x*1190.4000244140625/2480,y*841.4400024414062/1753) for x,y in c['raw_contour_baseimage_pixels']])):raise ValueError('saint_legier_raw_hull')
        p=Polygon([(x*1190.4000244140625/2480,y*841.4400024414062/1753) for x,y in c['boundary_baseimage_pixels']])
        if not p.equals_exact(shape(c['geometry_pdf']),1e-10) or not hull.covers(p):raise ValueError('saint_legier_original_whole_hull')
        pts=list(p.exterior.coords)[:-1];rr=c['official_reframe_responses']
        if len(rr)!=len(pts):raise ValueError('saint_legier_vertex_count')
        for (x,y),r in zip(pts,rr):
            lv03=[x*m[0][j]+y*m[1][j]+m[2][j] for j in range(2)]
            if math.dist(lv03,r['lv03'])>1e-7 or r['lv95']!=[float(r['response']['easting']),float(r['response']['northing'])]:raise ValueError('saint_legier_exact_conversion')
            if abs(r['lv95'][0]-lv03[0]-2000000)>2 or abs(r['lv95'][1]-lv03[1]-1000000)>2:raise ValueError('saint_legier_conversion_anomaly')
        g=Polygon([r['lv95'] for r in rr])
        if not g.is_valid or not g.equals_exact(shape(c['geometry_lv95']),1e-10):raise ValueError('saint_legier_ground_geometry')
        derived.append(g)
    byid={c['id']:shape(c['geometry_lv95']) for c in paths}
    expected={'institutions_northwest':{'cyan_461','cyan_825'},'public_equipment_middle':{'public_blue_663','public_blue_668'}}
    for f in b['features']:
        ids=expected[f['key']];g=shape(f['geometry_lv95'])
        if not g.is_valid or not g.equals(unary_union([byid[k] for k in ids])) or set(f['path_ids'])!={'raster-'+k for k in ids} or f['invalid_reference_envelope_overlap']:raise ValueError('saint_legier_collection')
        count={'institutions_northwest':9,'public_equipment_middle':7}[f['key']]
        if len(f['expected_pairs'])!=count or len({r['egrid'] for r in f['expected_pairs']})!=count:raise ValueError('saint_legier_pair_count')
        for r in f['expected_pairs']:
            at=r['attributes'];q=shape(r['geometry'])
            if at['EGRID']!=r['egrid'] or at['NO_COM_FED']!=5892 or at['GENRE_TXT'] not in KINDS or not q.is_valid or g.intersection(q).area<=0:raise ValueError('saint_legier_reference')
    if not {'cyan_880','public_blue_326'}<=set(b['held_exceptions']):raise ValueError('saint_legier_exceptions')
    return b
def load_artifacts():
    raw=(Path(__file__).parent/'reports/saint-legier-remaining/batch.json').read_bytes()
    if hashlib.sha256(raw).hexdigest()!=BATCH_SHA:raise ValueError('saint_legier_changed_batch_requires_review')
    return validate(json.loads(raw))
def persist(conn,document_id,sha):
    if str(document_id)!=DOC or sha!=SHA:return None
    with conn.cursor() as c:
        c.execute('SELECT sha256,page_count FROM bronze_ch.vd_pdcom_documents WHERE id=%s',(DOC,));d=c.fetchone()
        if not d or d[0]!=SHA or not d[1] or d[1]<42:raise ValueError('saint_legier_stored_source')
    b=load_artifacts();refs=[refresh(conn,DOC,5892,r['bounds']) for r in b['references']];results=[]
    with conn.cursor() as c:
        c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_references a JOIN bronze_ch.vd_pdcom_parcel_references z USING(egrid) WHERE a.snapshot_id=ANY(%s::uuid[]) AND z.snapshot_id=ANY(%s::uuid[]) AND a.snapshot_id<z.snapshot_id AND (NOT ST_Equals(a.geom,z.geom) OR a.commune_bfs IS DISTINCT FROM z.commune_bfs OR a.official_attributes IS DISTINCT FROM z.official_attributes)''',([x['snapshot_id'] for x in refs],[x['snapshot_id'] for x in refs]))
        if c.fetchone()[0]:raise ValueError('saint_legier_conflicting_tile_reference')
    for f in b['features']:
        sid=sector_id(f['key']);geom=json.dumps(f['geometry_lv95']);label='Saint-Légier — représentation cartographique historique partielle — '+f['label']
        frozen=[{'egrid':x['egrid'],'kind':x['attributes']['GENRE_TXT'],'geometry':x['geometry']} for x in f['expected_pairs']]
        evidence={'source_sha256':SHA,'source_path_ids':f['path_ids'],'source_category':f['category'],'page_number':42,'grouping':'Partial thematic depiction represented by two disconnected raster paint-component exteriors, not complete physical or policy sectors','semantics':b['semantics'],'reservation':b['reservation'],'alignment':b['alignment'],'literal_semantics':b['literal_semantic_review'],'municipal_currentness':b['municipal_currentness'],'raster_representation':b['raster_representation'],'independent_geographic_decision':b['independent_geographic_decision'],'source_precision':'unknown','source_precision_m':None,'boundary_buffer':'10m review heuristic, not a measured error bound','review_status':'review_required','publication_status':'internal_review_only','reference_scope':'Current official BFS5892 parcels only; historical predecessor support, not merged commune completeness; contextual intersections only','official_references':refs,'batch_sha256':BATCH_SHA,'artifact':'pipelines/vd-pdcom/reports/saint-legier-remaining','ground_checks':'Six labelled original LV03 grid identities fixed before first fit: four train, two held. Six corroborating historic building locations and two unresolved references retained. Exact official REFRAME conversion of118vertices sequentially replayed. No global accuracy claim.'}
        with conn,conn.cursor() as c:
            c.execute("SET LOCAL statement_timeout='90s'");c.execute('SELECT pg_advisory_xact_lock(589200301)')
            c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_references WHERE snapshot_id=ANY(%s::uuid[]) AND geom && ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) AND NOT ST_IsValid(geom)''',([x['snapshot_id'] for x in refs],geom))
            if c.fetchone()[0]:raise ValueError('saint_legier_invalid_current_reference')
            c.execute('DROP TABLE IF EXISTS pg_temp.saint_legier_pairs')
            c.execute('''CREATE TEMP TABLE saint_legier_pairs ON COMMIT DROP AS WITH g AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) geom),r AS MATERIALIZED (SELECT DISTINCT ON(egrid) * FROM bronze_ch.vd_pdcom_parcel_references WHERE snapshot_id=ANY(%s::uuid[]) AND ST_IsValid(geom) ORDER BY egrid,snapshot_id)
            SELECT r.*,ST_Intersection(r.geom,g.geom) overlap,ST_Area(r.geom) area,ST_DWithin(r.geom,ST_Boundary(g.geom),10) edge,CASE r.official_attributes->>'GENRE_TXT' WHEN 'DDP superficie' THEN 'ddp_superficie' WHEN 'DDP source' THEN 'ddp_source' WHEN 'parcelle privée' THEN 'bien_fonds' WHEN 'DP communal' THEN 'bien_fonds' WHEN 'DP cantonal' THEN 'bien_fonds' ELSE 'unknown' END object_kind FROM r,g WHERE ST_Intersects(r.geom,g.geom) AND ST_Area(ST_Intersection(r.geom,g.geom))>0''',(geom,[x['snapshot_id'] for x in refs]))
            c.execute('SELECT egrid FROM saint_legier_pairs');keys={x[0] for x in c.fetchall()}
            if keys!={x['egrid'] for x in frozen}:raise ValueError('saint_legier_current_membership_changed')
            c.execute('''WITH f AS (SELECT x->>'egrid' egrid,x->>'kind' kind,ST_SetSRID(ST_GeomFromGeoJSON(x->'geometry'),2056) geom FROM jsonb_array_elements(%s::jsonb) x) SELECT count(*) FROM saint_legier_pairs n LEFT JOIN f USING(egrid) WHERE f.egrid IS NULL OR NOT ST_Equals(n.geom,f.geom) OR (n.official_attributes->>'GENRE_TXT') IS DISTINCT FROM f.kind''',(Json(frozen),))
            if c.fetchone()[0]:raise ValueError('saint_legier_current_geometry_changed')
            c.execute('SELECT ST_IsValid(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)),(SELECT bool_and(ST_IsValid(ST_Transform(overlap,4326))) FROM saint_legier_pairs)',(geom,))
            if c.fetchone()!=(True,True):raise ValueError('saint_legier_transformed_geometry_invalid')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_sectors(id,document_id,page_number,label,geom,alignment_rmse_m,validation_evidence,review_status) VALUES(%s,%s,%s,%s,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326),%s,%s,'review_required') ON CONFLICT DO NOTHING''',(sid,DOC,f['page'],label,geom,b['alignment']['holdout_rmse_m'],Json(evidence)))
            c.execute("SELECT label,review_status,source_precision_m,validated_by,ST_Equals(geom,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)) FROM bronze_ch.vd_pdcom_sectors WHERE id=%s",(geom,sid))
            if c.fetchone()!=(label,'review_required',None,None,True):raise ValueError('saint_legier_existing_sector_changed')
            c.execute('SELECT commune_bfs FROM gold_ch.v_vd_pdcom_review_sectors WHERE id=%s',(sid,))
            if c.fetchone()[0]!=[5892]:raise ValueError('saint_legier_attribution')
            c.execute('SELECT egrid FROM bronze_ch.vd_pdcom_parcel_candidates WHERE sector_id=%s',(sid,));old={x[0] for x in c.fetchall()}
            if old and old!=keys:raise ValueError('saint_legier_existing_membership')
            c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_candidates p JOIN saint_legier_pairs n USING(egrid) WHERE p.sector_id=%s AND (NOT ST_Equals(p.geom,ST_Transform(n.overlap,4326)) OR p.overlap_m2 IS DISTINCT FROM ST_Area(n.overlap) OR p.parcel_area_m2 IS DISTINCT FROM n.area OR p.overlap_fraction IS DISTINCT FROM LEAST(1,ST_Area(n.overlap)/n.area) OR p.boundary_review_required IS DISTINCT FROM n.edge OR p.uncertainty_buffer_m IS DISTINCT FROM 10 OR p.cadastral_object_kind IS DISTINCT FROM n.object_kind)''',(sid,))
            if c.fetchone()[0]:raise ValueError('saint_legier_existing_values')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_parcel_candidates(sector_id,egrid,overlap_m2,parcel_area_m2,overlap_fraction,boundary_review_required,uncertainty_buffer_m,geom,cadastral_object_kind,cadastral_type_evidence) SELECT %s,n.egrid,ST_Area(n.overlap),n.area,LEAST(1,ST_Area(n.overlap)/n.area),n.edge,10,ST_Transform(n.overlap,4326),n.object_kind,jsonb_build_object('source_url',s.query_url,'response_sha256',s.response_sha256,'reference_snapshot_ids',jsonb_build_array(n.snapshot_id),'official_attributes',n.official_attributes,'status','matched') FROM saint_legier_pairs n JOIN bronze_ch.vd_pdcom_parcel_reference_snapshots s ON s.id=n.snapshot_id ON CONFLICT DO NOTHING''',(sid,))
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_candidate_parcel_references(sector_id,egrid,snapshot_id) SELECT %s,egrid,snapshot_id FROM saint_legier_pairs ON CONFLICT(sector_id,egrid) DO UPDATE SET snapshot_id=EXCLUDED.snapshot_id''',(sid,))
            c.execute('UPDATE bronze_ch.vd_pdcom_sectors SET validation_evidence=%s WHERE id=%s',(Json(evidence),sid))
        results.append({'key':f['key'],'sector_id':sid,'parcel_pairs':len(keys)})
    return {'collections':len(results),'parcel_pairs':sum(r['parcel_pairs'] for r in results),'candidates':results,'publication_status':'internal_review_only','review_status':'review_required'}
