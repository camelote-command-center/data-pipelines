"""Hash-bound indicative source-layer collections; existing private review only."""
import hashlib,json,uuid,math
from pathlib import Path
from psycopg2.extras import Json
from shapely.geometry import shape
from shapely.ops import unary_union
from shapely.affinity import affine_transform
from official_references import refresh
from cadastral_types import KINDS
DOC='464c3687-bdd2-55b4-9e35-f8ece05099cb'
SHA='8b78d42eb83210b6b63404dd8b4270fedd15d1b7e3d65d2a403d6edcee654a3c'
BATCH_SHA='f60553a2419f0ef7e9898101af486c27fd8ef12db2c0ef07f4b0c865eb55c00b'
KEYS={'infrastructure_adapt','mixed_medium','public_develop'}
OLD_PATHS=set() # Extended indices differ from legacy ordinary-drawing indices; layer gate excludes densification.
def sector_id(key):
    if key not in KEYS:raise ValueError('orbe_unreviewed_category')
    return str(uuid.uuid5(uuid.NAMESPACE_URL,DOC+'#indicative-category-2026-09-22#'+key))
def validate(b):
    if (b['document_id'],b['source_sha256'])!=(DOC,SHA):raise ValueError('orbe_source')
    if len(b['features'])!=3 or {f['key'] for f in b['features']}!=KEYS:raise ValueError('orbe_scope')
    if b['publication_status']!='internal_review_only' or b['source_precision_m'] is not None:raise ValueError('orbe_private_precision')
    seen=set();pairs=0;m=b['affine']
    for f in b['features']:
        ids=set(f['path_ids'])
        for path in f['source_paths']:
            expected=affine_transform(shape(path['geometry_pdf']),m)
            if not shape(path['geometry_lv95']).equals_exact(expected,1e-7):raise ValueError('orbe_page_transform')
        if ids & (seen|OLD_PATHS) or len(ids)!=len(f['path_ids']):raise ValueError('orbe_duplicate_path')
        seen|=ids
        if f['invalid_reference_envelope_overlap']:raise ValueError('orbe_invalid_reference')
        gs=[shape(x['geometry_lv95']) for x in f['source_paths']];g=shape(f['geometry_lv95'])
        if not g.is_valid or g.is_empty or any(not z.is_valid for z in gs) or not g.equals(unary_union(gs)):raise ValueError('orbe_source_union')
        if {x['id'] for x in f['source_paths']}!=ids:raise ValueError('orbe_path_membership')
        if any(pr['category']!=f['category'] or pr['page']!=f['page'] or pr['source_sha256']!=SHA or pr['index'] not in {'infrastructure_adapt':{9363,9364,9508,25623},'mixed_medium':{9491,9493,9526,18671,18672,18673},'public_develop':{18674,18677,18678,18679,18680,18681}}[f['key']] for pr in f['properties']):raise ValueError('orbe_semantics')
        eg=set()
        for row in f['expected_pairs']:
            r=shape(row['geometry']);at=row['attributes']
            if row['egrid'] in eg or at['EGRID']!=row['egrid'] or at['NO_COM_FED']!=5757 or at['GENRE_TXT'] not in KINDS or not r.is_valid:raise ValueError('orbe_reference_identity')
            if g.intersection(r).area<=0:raise ValueError('orbe_reference_overlap')
            eg.add(row['egrid']);pairs+=1
    if pairs!=84:raise ValueError('orbe_pair_count')
    return b
def load_artifacts():
    raw=(Path(__file__).parent/'reports/orbe-thematic/batch.json').read_bytes()
    if hashlib.sha256(raw).hexdigest()!=BATCH_SHA:raise ValueError('orbe_changed_batch_requires_review')
    return validate(json.loads(raw))
def persist(conn,document_id,sha):
    if str(document_id)!=DOC or sha!=SHA:return None
    with conn.cursor() as c:
        c.execute('SELECT sha256,page_count FROM bronze_ch.vd_pdcom_documents WHERE id=%s',(DOC,));d=c.fetchone()
        if not d or d[0]!=SHA or not d[1] or d[1]<1:raise ValueError('orbe_stored_source')
    b=load_artifacts();refs=[refresh(conn,DOC,5757,r['bounds']) for r in b['references']];results=[]
    with conn.cursor() as c:
        c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_references a JOIN bronze_ch.vd_pdcom_parcel_references z USING(egrid) WHERE a.snapshot_id=ANY(%s::uuid[]) AND z.snapshot_id=ANY(%s::uuid[]) AND a.snapshot_id<z.snapshot_id AND (NOT ST_Equals(a.geom,z.geom) OR a.commune_bfs IS DISTINCT FROM z.commune_bfs OR a.official_attributes IS DISTINCT FROM z.official_attributes)''',([x['snapshot_id'] for x in refs],[x['snapshot_id'] for x in refs]))
        if c.fetchone()[0]:raise ValueError('orbe_conflicting_tile_reference')
    for f in b['features']:
        sid=sector_id(f['key']);geom=json.dumps(f['geometry_lv95']);label='Orbe — couche indicative partielle — '+f['label']
        frozen=[{'egrid':x['egrid'],'kind':x['attributes']['GENRE_TXT'],'geometry':x['geometry']} for x in f['expected_pairs']]
        evidence={'source_sha256':SHA,'source_path_ids':f['path_ids'],'source_category':f['category'],'page_number':f['page'],'grouping':'Union of explicitly listed source paths; partial thematic collection, not a physical sector','semantics':b['semantics'],'reservation':b['reservation'],'alignment':b['alignment'],'source_transform':b['affine'],'map_review':'Original page1 transformation unchanged; filled source categories and ancestor clips reviewed. Separate inset excluded; no new fit or precision inference.','source_precision':'unknown','source_precision_m':None,'boundary_buffer':'10m review heuristic, not a measured error bound','review_status':'review_required','publication_status':'internal_review_only','reference_scope':'Current official cadastral objects for source commune5757 only; DDP rights distinct from underlying land, not exclusive area; entire source geometry retained, not clipped','official_references':refs,'batch_sha256':BATCH_SHA,'artifact':'pipelines/vd-pdcom/reports/orbe-thematic'}
        with conn,conn.cursor() as c:
            c.execute("SET LOCAL statement_timeout='90s'");c.execute('SELECT pg_advisory_xact_lock(575700301)')
            c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_references WHERE snapshot_id=ANY(%s::uuid[]) AND geom && ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) AND NOT ST_IsValid(geom)''',([x['snapshot_id'] for x in refs],geom))
            if c.fetchone()[0]:raise ValueError('orbe_invalid_current_reference')
            c.execute('DROP TABLE IF EXISTS pg_temp.orbe_pairs')
            c.execute('''CREATE TEMP TABLE orbe_pairs ON COMMIT DROP AS WITH g AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) geom),r AS MATERIALIZED (SELECT DISTINCT ON(egrid) * FROM bronze_ch.vd_pdcom_parcel_references WHERE snapshot_id=ANY(%s::uuid[]) AND ST_IsValid(geom) ORDER BY egrid,snapshot_id)
            SELECT r.*,ST_Intersection(r.geom,g.geom) overlap,ST_Area(r.geom) area,ST_DWithin(r.geom,ST_Boundary(g.geom),10) edge,CASE r.official_attributes->>'GENRE_TXT' WHEN 'DDP superficie' THEN 'ddp_superficie' WHEN 'DDP source' THEN 'ddp_source' WHEN 'parcelle privée' THEN 'bien_fonds' WHEN 'DP communal' THEN 'bien_fonds' WHEN 'DP cantonal' THEN 'bien_fonds' ELSE 'unknown' END object_kind FROM r,g WHERE ST_Intersects(r.geom,g.geom) AND ST_Area(ST_Intersection(r.geom,g.geom))>0''',(geom,[x['snapshot_id'] for x in refs]))
            c.execute('SELECT egrid FROM orbe_pairs');keys={x[0] for x in c.fetchall()}
            if keys!={x['egrid'] for x in frozen}:raise ValueError('orbe_current_membership_changed')
            c.execute('''WITH f AS (SELECT x->>'egrid' egrid,x->>'kind' kind,ST_SetSRID(ST_GeomFromGeoJSON(x->'geometry'),2056) geom FROM jsonb_array_elements(%s::jsonb) x) SELECT count(*) FROM orbe_pairs n LEFT JOIN f USING(egrid) WHERE f.egrid IS NULL OR NOT ST_Equals(n.geom,f.geom) OR (n.official_attributes->>'GENRE_TXT') IS DISTINCT FROM f.kind''',(Json(frozen),))
            if c.fetchone()[0]:raise ValueError('orbe_current_geometry_changed')
            c.execute('SELECT ST_IsValid(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)),(SELECT bool_and(ST_IsValid(ST_Transform(overlap,4326))) FROM orbe_pairs)',(geom,))
            if c.fetchone()!=(True,True):raise ValueError('orbe_transformed_geometry_invalid')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_sectors(id,document_id,page_number,label,geom,alignment_rmse_m,validation_evidence,review_status) VALUES(%s,%s,%s,%s,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326),%s,%s,'review_required') ON CONFLICT DO NOTHING''',(sid,DOC,f['page'],label,geom,b['alignment']['holdout_rmse_m'],Json(evidence)))
            c.execute("SELECT label,review_status,source_precision_m,validated_by,ST_Equals(geom,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)) FROM bronze_ch.vd_pdcom_sectors WHERE id=%s",(geom,sid))
            if c.fetchone()!=(label,'review_required',None,None,True):raise ValueError('orbe_existing_sector_changed')
            c.execute('SELECT commune_bfs FROM gold_ch.v_vd_pdcom_review_sectors WHERE id=%s',(sid,))
            if c.fetchone()[0]!=[5757]:raise ValueError('orbe_attribution')
            c.execute('SELECT egrid FROM bronze_ch.vd_pdcom_parcel_candidates WHERE sector_id=%s',(sid,));old={x[0] for x in c.fetchall()}
            if old and old!=keys:raise ValueError('orbe_existing_membership')
            c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_candidates p JOIN orbe_pairs n USING(egrid) WHERE p.sector_id=%s AND (NOT ST_Equals(p.geom,ST_Transform(n.overlap,4326)) OR p.overlap_m2 IS DISTINCT FROM ST_Area(n.overlap) OR p.parcel_area_m2 IS DISTINCT FROM n.area OR p.overlap_fraction IS DISTINCT FROM LEAST(1,ST_Area(n.overlap)/n.area) OR p.boundary_review_required IS DISTINCT FROM n.edge OR p.uncertainty_buffer_m IS DISTINCT FROM 10 OR p.cadastral_object_kind IS DISTINCT FROM n.object_kind)''',(sid,))
            if c.fetchone()[0]:raise ValueError('orbe_existing_values')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_parcel_candidates(sector_id,egrid,overlap_m2,parcel_area_m2,overlap_fraction,boundary_review_required,uncertainty_buffer_m,geom,cadastral_object_kind,cadastral_type_evidence) SELECT %s,n.egrid,ST_Area(n.overlap),n.area,LEAST(1,ST_Area(n.overlap)/n.area),n.edge,10,ST_Transform(n.overlap,4326),n.object_kind,jsonb_build_object('source_url',s.query_url,'response_sha256',s.response_sha256,'reference_snapshot_ids',jsonb_build_array(n.snapshot_id),'official_attributes',n.official_attributes,'status','matched') FROM orbe_pairs n JOIN bronze_ch.vd_pdcom_parcel_reference_snapshots s ON s.id=n.snapshot_id ON CONFLICT DO NOTHING''',(sid,))
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_candidate_parcel_references(sector_id,egrid,snapshot_id) SELECT %s,egrid,snapshot_id FROM orbe_pairs ON CONFLICT(sector_id,egrid) DO UPDATE SET snapshot_id=EXCLUDED.snapshot_id''',(sid,))
            c.execute('UPDATE bronze_ch.vd_pdcom_sectors SET validation_evidence=%s WHERE id=%s',(Json(evidence),sid))
        results.append({'key':f['key'],'sector_id':sid,'parcel_pairs':len(keys)})
    return {'collections':len(results),'parcel_pairs':sum(r['parcel_pairs'] for r in results),'candidates':results,'publication_status':'internal_review_only','review_status':'review_required'}
