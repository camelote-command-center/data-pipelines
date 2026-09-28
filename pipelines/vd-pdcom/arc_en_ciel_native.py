"""Pinned original Arc-en-Ciel coherent native faces; historical private review only."""
import hashlib,json,uuid,math
from pathlib import Path
from psycopg2.extras import Json
from shapely.geometry import shape,Polygon,MultiPoint,LineString
from shapely.ops import unary_union,polygonize
from official_references import refresh,ARC_CONTEXT
from cadastral_types import KINDS
DOC='e2072261-5391-5a80-9708-001fad9e56fb'
SHA='e55a18076cbc2ca67fe2042fdb76565f0edba400fc72beb444ab70ee7fe7a8c5'
BATCH_SHA='512c44f79e9e285443f2f7e6d05801643184ab7ceb218f4bb50ac5b0cf7a4540'
KEYS={'A1','A2','B1','B2','D1','D2'}
ATTRIBUTION={'A1':[5583,5624],'A2':[5583,5624],'B1':[5583,5624],'B2':[5583],'D1':[5583],'D2':[5583]}
def sector_id(key):
    if key not in KEYS:raise ValueError('arc_unreviewed_category')
    return str(uuid.uuid5(uuid.NAMESPACE_URL,DOC+'#native-coherent-historical-v1#'+key))
def validate(b):
    if (b['document_id'],b['source_sha256'])!=(DOC,SHA):raise ValueError('arc_source')
    if len(b['features'])!=6 or {f['key'] for f in b['features']}!=KEYS:raise ValueError('arc_scope')
    if b['publication_status']!='internal_review_only' or b['source_precision_m'] is not None or b['review_status']!='review_required' or b['source_plan_status']!='unverified':raise ValueError('arc_qualification')
    if b['source_scope_communes']!=[5583,5624] or b['reference_communes']!=[5583,5624,5635]:raise ValueError('arc_communes')
    if b['independent_geographic_decision']!='accepted_private_historical_native_gross_supports_only':raise ValueError('arc_qa')
    sem=b['literal_semantic_review']
    if sem['selected_source_labels']!=['A1','A2','B1','B2','D1','D2'] or sem['geographic_acceptance']!='accepted_private_historical_native_gross_supports_only' or any(sem[k] for k in ['current_housing_or_capacity_claims','current_perimeter_invariance_verified','commune_complete','qualified_release_authorized']):raise ValueError('arc_semantic_limits')
    controls=b['frozen_grid']['controls'];train=[c for c in controls if c['role']=='train'];held=[c for c in controls if c['role']=='held']
    if len(controls)!=255 or len(train)!=4 or len(held)!=251 or not b['frozen_grid']['roles_frozen_before_first_fit']:raise ValueError('arc_grid_partition')
    if {tuple(c['ground_lv03']) for c in train}!={(532450,154600),(532450,156000),(534050,154600),(534050,156000)}:raise ValueError('arc_grid_anchors')
    hull=MultiPoint([c['source_xy_pdf'] for c in train]).convex_hull;matrix=b['grid_fit']['matrix'];residual=[]
    for c in held:
        x,y=c['source_xy_pdf'];pred=[matrix[0][j]*x+matrix[1][j]*y+matrix[2][j] for j in range(2)];residual.append(math.dist(pred,c['ground_lv03']))
    if abs(math.sqrt(sum(v*v for v in residual)/251)-b['alignment']['holdout_rmse_m'])>1e-8:raise ValueError('arc_grid_diagnostic')
    native={x['label']:x for x in b['source_native_proof']['selected']}
    if set(native)!=KEYS:raise ValueError('arc_native_scope')
    for f in b['features']:
        n=native[f['key']];p=shape(n['geometry']);rr=n['official_reframe_responses'];pts=list(p.exterior.coords)[:-1]
        if not p.is_valid or p.interiors or not hull.covers(p) or len(rr)!=len(pts):raise ValueError('arc_whole_native_face')
        lines=[LineString(o['coords']) for o in n['native_support_operators']]
        if not any(q.equals(p) for q in polygonize(unary_union(lines))):raise ValueError('arc_exact_native_network')
        for (x,y),r in zip(pts,rr):
            lv03=[matrix[0][j]*x+matrix[1][j]*y+matrix[2][j] for j in range(2)]
            if math.dist(lv03,r['lv03'])>1e-7 or r['lv95']!=[float(r['response']['easting']),float(r['response']['northing'])]:raise ValueError('arc_exact_conversion')
            if abs(r['lv95'][0]-lv03[0]-2000000)>10 or abs(r['lv95'][1]-lv03[1]-1000000)>10:raise ValueError('arc_conversion_anomaly')
        g=Polygon([r['lv95'] for r in rr])
        if not g.is_valid or not g.equals_exact(shape(n['geometry_lv95']),1e-10) or not g.equals_exact(shape(f['geometry_lv95']),1e-10) or f['invalid_reference_envelope_overlap']:raise ValueError('arc_ground_geometry')
        if len({r['egrid'] for r in f['expected_pairs']})!=len(f['expected_pairs']):raise ValueError('arc_duplicate_pair')
        for r in f['expected_pairs']:
            at=r['attributes'];q=shape(r['reference_geometry'])
            if at['EGRID']!=r['egrid'] or at['NO_COM_FED'] not in b['reference_communes'] or at['GENRE_TXT'] not in KINDS or not q.is_valid or g.intersection(q).area<=0:raise ValueError('arc_reference')
    if sum(len(f['expected_pairs']) for f in b['features'])!=104:raise ValueError('arc_pair_inventory')
    return b
def load_artifacts():
    raw=(Path(__file__).parent/'reports/arc-en-ciel-native/batch.json').read_bytes()
    if hashlib.sha256(raw).hexdigest()!=BATCH_SHA:raise ValueError('arc_changed_batch_requires_review')
    return validate(json.loads(raw))
def persist(conn,document_id,sha):
    if str(document_id)!=DOC or sha!=SHA:return None
    with conn.cursor() as c:
        c.execute('SELECT sha256,page_count,plan_status FROM bronze_ch.vd_pdcom_documents WHERE id=%s',(DOC,));d=c.fetchone()
        if not d or d[0]!=SHA or not d[1] or d[1]<1 or d[2]!='unverified':raise ValueError('arc_en_ciel_native_stored_source')
    b=load_artifacts();refs=[refresh(conn,DOC,r['commune_bfs'],r['bounds'],context_declaration=ARC_CONTEXT if r['commune_bfs']==5635 else None) for r in b['references']];results=[]
    with conn.cursor() as c:
        c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_references a JOIN bronze_ch.vd_pdcom_parcel_references z USING(egrid) WHERE a.snapshot_id=ANY(%s::uuid[]) AND z.snapshot_id=ANY(%s::uuid[]) AND a.snapshot_id<z.snapshot_id AND (NOT ST_Equals(a.geom,z.geom) OR a.commune_bfs IS DISTINCT FROM z.commune_bfs OR a.official_attributes IS DISTINCT FROM z.official_attributes)''',([x['snapshot_id'] for x in refs],[x['snapshot_id'] for x in refs]))
        if c.fetchone()[0]:raise ValueError('arc_en_ciel_native_conflicting_tile_reference')
    for f in b['features']:
        sid=sector_id(f['key']);geom=json.dumps(f['geometry_lv95']);label='Arc-en-Ciel — support historique localisé — '+f['label']
        frozen=[{'egrid':x['egrid'],'kind':x['attributes']['GENRE_TXT'],'geometry':x['reference_geometry']} for x in f['expected_pairs']]
        evidence={'source_sha256':SHA,'source_path_ids':f['path_ids'],'source_category':f['category'],'page_number':1,'grouping':'One original coherent native gross planning face, not net developable land','semantics':b['semantics'],'reservation':b['reservation'],'source_plan_status':b['source_plan_status'],'alignment':b['alignment'],'literal_semantics':b['literal_semantic_review'],'independent_geographic_decision':b['independent_geographic_decision'],'source_precision':'unknown','source_precision_m':None,'boundary_buffer':'10m review heuristic, not a measured error bound','review_status':'review_required','publication_status':'internal_review_only','reference_scope':'Current official cadastral objects from Bussigny, Crissier and neighbouring Ecublens; contextual intersections only, source scope remains Bussigny/Crissier','source_scope_communes':b['source_scope_communes'],'reference_communes':b['reference_communes'],'topology_holds':b['topology_holds'],'official_references':refs,'batch_sha256':BATCH_SHA,'artifact':'pipelines/vd-pdcom/reports/arc-en-ciel-native','ground_checks':'Frozen original printed CAD grid:4 outer corners train,251 counted grid crossings reserved. Intrinsic consistency only, not surveyed accuracy. Exact official REFRAME vertices independently replayed; current cadastral context independently reviewed.'}
        with conn,conn.cursor() as c:
            c.execute("SET LOCAL statement_timeout='90s'");c.execute('SELECT pg_advisory_xact_lock(562400301)')
            c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_references WHERE snapshot_id=ANY(%s::uuid[]) AND geom && ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) AND NOT ST_IsValid(geom)''',([x['snapshot_id'] for x in refs],geom))
            if c.fetchone()[0]:raise ValueError('arc_en_ciel_native_invalid_current_reference')
            c.execute('DROP TABLE IF EXISTS pg_temp.arc_en_ciel_native_pairs')
            c.execute('''CREATE TEMP TABLE arc_en_ciel_native_pairs ON COMMIT DROP AS WITH g AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) geom),r AS MATERIALIZED (SELECT DISTINCT ON(egrid) * FROM bronze_ch.vd_pdcom_parcel_references WHERE snapshot_id=ANY(%s::uuid[]) AND ST_IsValid(geom) ORDER BY egrid,snapshot_id)
            SELECT r.*,ST_Intersection(r.geom,g.geom) overlap,ST_Area(r.geom) area,ST_DWithin(r.geom,ST_Boundary(g.geom),10) edge,CASE r.official_attributes->>'GENRE_TXT' WHEN 'DDP superficie' THEN 'ddp_superficie' WHEN 'DDP source' THEN 'ddp_source' WHEN 'parcelle privée' THEN 'bien_fonds' WHEN 'DP communal' THEN 'bien_fonds' WHEN 'DP cantonal' THEN 'bien_fonds' ELSE 'unknown' END object_kind FROM r,g WHERE ST_Intersects(r.geom,g.geom) AND ST_Area(ST_Intersection(r.geom,g.geom))>0''',(geom,[x['snapshot_id'] for x in refs]))
            c.execute('SELECT egrid FROM arc_en_ciel_native_pairs');keys={x[0] for x in c.fetchall()}
            if keys!={x['egrid'] for x in frozen}:raise ValueError('arc_en_ciel_native_current_membership_changed')
            c.execute('''WITH f AS (SELECT x->>'egrid' egrid,x->>'kind' kind,ST_SetSRID(ST_GeomFromGeoJSON(x->'geometry'),2056) geom FROM jsonb_array_elements(%s::jsonb) x) SELECT count(*) FROM arc_en_ciel_native_pairs n LEFT JOIN f USING(egrid) WHERE f.egrid IS NULL OR NOT ST_Equals(n.geom,f.geom) OR (n.official_attributes->>'GENRE_TXT') IS DISTINCT FROM f.kind''',(Json(frozen),))
            if c.fetchone()[0]:raise ValueError('arc_en_ciel_native_current_geometry_changed')
            c.execute('SELECT ST_IsValid(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)),(SELECT bool_and(ST_IsValid(ST_Transform(overlap,4326))) FROM arc_en_ciel_native_pairs)',(geom,))
            if c.fetchone()!=(True,True):raise ValueError('arc_en_ciel_native_transformed_geometry_invalid')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_sectors(id,document_id,page_number,label,geom,alignment_rmse_m,validation_evidence,review_status) VALUES(%s,%s,%s,%s,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326),%s,%s,'review_required') ON CONFLICT DO NOTHING''',(sid,DOC,f['page'],label,geom,b['alignment']['holdout_rmse_m'],Json(evidence)))
            c.execute("SELECT label,review_status,source_precision_m,validated_by,ST_Equals(geom,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)) FROM bronze_ch.vd_pdcom_sectors WHERE id=%s",(geom,sid))
            if c.fetchone()!=(label,'review_required',None,None,True):raise ValueError('arc_en_ciel_native_existing_sector_changed')
            c.execute('SELECT commune_bfs FROM gold_ch.v_vd_pdcom_review_sectors WHERE id=%s',(sid,))
            if c.fetchone()[0]!=ATTRIBUTION[f['key']]:raise ValueError('arc_en_ciel_native_attribution')
            c.execute('SELECT egrid FROM bronze_ch.vd_pdcom_parcel_candidates WHERE sector_id=%s',(sid,));old={x[0] for x in c.fetchall()}
            if old and old!=keys:raise ValueError('arc_en_ciel_native_existing_membership')
            c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_candidates p JOIN arc_en_ciel_native_pairs n USING(egrid) WHERE p.sector_id=%s AND (NOT ST_Equals(p.geom,ST_Transform(n.overlap,4326)) OR p.overlap_m2 IS DISTINCT FROM ST_Area(n.overlap) OR p.parcel_area_m2 IS DISTINCT FROM n.area OR p.overlap_fraction IS DISTINCT FROM LEAST(1,ST_Area(n.overlap)/n.area) OR p.boundary_review_required IS DISTINCT FROM n.edge OR p.uncertainty_buffer_m IS DISTINCT FROM 10 OR p.cadastral_object_kind IS DISTINCT FROM n.object_kind)''',(sid,))
            if c.fetchone()[0]:raise ValueError('arc_en_ciel_native_existing_values')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_parcel_candidates(sector_id,egrid,overlap_m2,parcel_area_m2,overlap_fraction,boundary_review_required,uncertainty_buffer_m,geom,cadastral_object_kind,cadastral_type_evidence) SELECT %s,n.egrid,ST_Area(n.overlap),n.area,LEAST(1,ST_Area(n.overlap)/n.area),n.edge,10,ST_Transform(n.overlap,4326),n.object_kind,jsonb_build_object('source_url',s.query_url,'response_sha256',s.response_sha256,'reference_snapshot_ids',jsonb_build_array(n.snapshot_id),'official_attributes',n.official_attributes,'status','matched') FROM arc_en_ciel_native_pairs n JOIN bronze_ch.vd_pdcom_parcel_reference_snapshots s ON s.id=n.snapshot_id ON CONFLICT DO NOTHING''',(sid,))
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_candidate_parcel_references(sector_id,egrid,snapshot_id) SELECT %s,egrid,snapshot_id FROM arc_en_ciel_native_pairs ON CONFLICT(sector_id,egrid) DO UPDATE SET snapshot_id=EXCLUDED.snapshot_id''',(sid,))
            c.execute('UPDATE bronze_ch.vd_pdcom_sectors SET validation_evidence=%s WHERE id=%s',(Json(evidence),sid))
        results.append({'key':f['key'],'sector_id':sid,'parcel_pairs':len(keys)})
    return {'collections':len(results),'parcel_pairs':sum(r['parcel_pairs'] for r in results),'candidates':results,'publication_status':'internal_review_only','review_status':'review_required'}
