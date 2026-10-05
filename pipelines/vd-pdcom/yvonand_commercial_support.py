"""One hash-bound historical commercial policy support; private review only."""
import gzip,hashlib,json,uuid,fitz,math
from pathlib import Path
from psycopg2.extras import Json
from shapely.geometry import shape,Polygon,MultiPoint
from shapely.ops import unary_union
from shapely.affinity import affine_transform,translate
from official_references import refresh
from cadastral_types import KINDS
import yvonand_existing_vocations as previous
DOC='567600de-dc15-516c-b511-e33fbebce5eb'
SHA='99e1f03d89478b5815b7b0e9ef0d5c5511cbfd4e48d6ee038ad80ac86c8b0559'
BATCH_SHA='510fabf0b391ca5e8a39930757e236727cbdb9f14190bda2410de87a2574df79'
ROOT=Path(__file__).parent/'reports/yvonand-commercial-support'
LABEL='Activités commerciales et centrales à renforcer'
AFFINE=[1.7638888888888888,0,0,-1.7638888888888888,2544299.8273454974,1184675.315508198]
def sector_id(key='commercial_central_reinforcement'):
    if key!='commercial_central_reinforcement':raise ValueError('yvonand_commercial_scope')
    return str(uuid.uuid5(uuid.NAMESPACE_URL,DOC+'#yvonand-commercial-support-v1#'+key))
def fail(condition,name):
    if not condition:raise ValueError('yvonand_commercial_'+name)
def validate(b):
    fail((b['document_id'],b['source_sha256'])==(DOC,SHA),'source')
    fail(b['publication_status']=='internal_review_only' and b['source_precision_m'] is None,'private_precision')
    fail(b['category_literal']==LABEL and not b['currentness']['current_zoning_established'],'semantics')
    fail(b['affine']==AFFINE,'frozen_transform')
    old=previous.load_artifacts();r=b['registration'];controls=r['controls']
    fail(b['alignment']==old['alignment']==r['source7_alignment_retained'],'prior_failure_denominators')
    fail(r['source7_identity_ledger_denominator']==1620 and r['source3_gray_closed_polygon_denominator']==992,'full_denominators')
    fail(r['source_precision_m'] is None and not r['global_accuracy_claim'] and not r['new_geographic_fit'],'registration_scope')
    fail(len(controls)==19 and len({c['source3_path']for c in controls})==19,'control_count')
    prior={c['source_drawing_index']:c for c in old['reviewed_control_identities']['selected']}
    fail({c['source7_path']for c in controls}==set(prior),'control_identities')
    delta=r['native_translation_source7_to_source3'];fail(delta==[14.160034179687386,-11.3399658203125],'fixed_translation')
    for c in controls:
        p=prior[c['source7_path']];g=shape(c['source3_geometry_display']);ref=shape(c['reference_geometry'])
        fail(c['original_role']==p['role'] and c['reference_attributes']==p['reference_attributes'] and c['reference_geometry']==p['reference_geometry'],'original_identity')
        dist=g.hausdorff_distance(translate(shape(p['source_geometry_pdf']),*delta))
        fail(abs(dist-c['fixed_translation_hausdorff_pt'])<1e-8 and c['normalized_shape_best_distance_pt']<c['normalized_shape_second_distance_pt'],'same_building_shape')
        gg=affine_transform(g,AFFINE);iou=gg.intersection(ref).area/gg.union(ref).area
        fail(iou>=.85 and abs(iou-c['source3_reference_iou'])<1e-8,'official_control')
    diagnostic=b['source3_reserved_diagnostic'];reserved=[c for c in controls if c['original_role']=='reserved']
    fail(len(reserved)==diagnostic['count']==8 and diagnostic['original_reserved_denominator']==248 and diagnostic['original_reserved_matched']==114 and diagnostic['original_reserved_unmatched']==134,'reserved_denominators')
    residuals={c['source3_path']:affine_transform(shape(c['source3_geometry_display']),AFFINE).centroid.distance(shape(c['reference_geometry']).centroid) for c in reserved}
    fail({x['source3_path']for x in diagnostic['controls']}==set(residuals),'all_reserved_retained')
    for x in diagnostic['controls']:fail(abs(x['centroid_residual_m']-residuals[x['source3_path']])<1e-9,'reserved_residual')
    fail(abs(math.sqrt(sum(x*x for x in residuals.values())/8)-diagnostic['rmse_m'])<1e-9 and diagnostic['no_refit_or_reselection'] and not diagnostic['global_accuracy_claim'] and diagnostic['source_precision_m'] is None,'filtered_diagnostic_only')
    fail(b['municipal_currentness']['classification']=='historical_current_applicability_unestablished','currentness_hold')
    train=[c for c in controls if c['original_role']=='training'];fail(len(train)==11,'original_roles')
    hull=MultiPoint([shape(c['source3_geometry_display']).centroid for c in train]).convex_hull
    fail(hull.equals(shape(r['source3_training_hull'])),'training_hull')
    fail(len(b['features'])==2 and {f['drawing_index']for f in b['features']}=={18,65},'complete_source_components')
    raw=gzip.decompress((ROOT/'map-3-full-paint.json.gz').read_bytes());fail(hashlib.sha256(raw).hexdigest()==b['map-3-full-paint.json_sha256'],'full_paint_ledger')
    ledger=json.loads(raw);fail(ledger['source_sha256']==SHA and ledger['page_rotation']==90 and len(ledger['records'])==6149,'source_paint_contract')
    byid={v['drawing_index']:v for v in ledger['records']if v['drawing_index'] is not None}
    fail(len(byid)==5664 and {k for k,v in byid.items()if v.get('fill')==[1,1,0]}=={18,65,5598},'paint_completeness')
    gs=[]
    for f in b['features']:
        p=f['original_paint_record'];fail(p==byid[f['drawing_index']],'original_paint')
        fail(not f['subtraction_or_repair_performed'] and not f['visible_paint_area_claim'],'original_support_only')
        fail(p['fill']==[1,1,0] and p['fill_opacity']==1 and p['even_odd'] and all(a['type']=='group'for a in f['graphics_state_ancestors']),'fill_semantics')
        pts=[p['items'][0][1]]
        for item in p['items']:
            fail(item[0]=='l' and item[1]==pts[-1],'literal_commands');pts.append(item[2])
        fail(pts[0]==pts[-1] and Polygon(pts).equals_exact(shape(f['geometry_native_pdf_points']),0),'literal_closed_path')
        display=Polygon([list(fitz.Point(pt)*fitz.Matrix(*ledger['rotation_matrix'])) for pt in pts]);g=shape(f['geometry_display_pdf_points'])
        fail(display.equals_exact(g,0) and g.is_valid and hull.covers(g),'whole_original_hull')
        expected=affine_transform(g,AFFINE);fail(expected.equals_exact(shape(f['geometry_lv95_research_only']),1e-8),'source_transform');gs.append(expected)
    g=shape(b['geometry_lv95_research_only']);fail(g.is_valid and g.equals(unary_union(gs)),'whole_union')
    boundary=json.loads((ROOT/'registered-commune-boundary.json').read_text());fail(boundary['document_commune_association']['document_id']==DOC and boundary['document_commune_association']['commune_bfs']==5939,'commune_identity')
    fail(unary_union([shape(f['geometry'])for f in boundary['features']]).covers(g),'whole_commune')
    raw=gzip.decompress((ROOT/'official-commercial-parcels.geojson.gz').read_bytes());fail(hashlib.sha256(raw).hexdigest()==b['official-commercial-parcels.geojson_sha256']==b['official_reference']['sha256'],'full_reference')
    refs=json.loads(raw)['features'];fail(len(refs)==208 and b['official_reference']['complete_bbox_count_verified'],'full_reference_count')
    expected={}
    for f in refs:
        a=f['properties'];ref=shape(f['geometry']);fail(ref.is_valid and a['NO_COM_FED']==5939 and a['GENRE_TXT']in KINDS,'reference_identity')
        if g.intersection(ref).area>0:expected[a['EGRID']]=f
    fail(len(expected)==163 and len(b['expected_pairs'])==163 and not b['invalid_reference_envelope_overlap'],'pairs_count')
    fail({x['egrid']for x in b['expected_pairs']}==set(expected),'complete_pairs')
    for x in b['expected_pairs']:
        fail(x['attributes']==expected[x['egrid']]['properties'] and x['geometry']==expected[x['egrid']]['geometry'],'pair_identity')
    return b
def load_artifacts():
    raw=(ROOT/'areas.json').read_bytes()
    fail(hashlib.sha256(raw).hexdigest()==BATCH_SHA,'changed_batch_requires_review')
    return validate(json.loads(raw))
def persist(conn,document_id,sha):
    if str(document_id)!=DOC or sha!=SHA:return None
    with conn.cursor() as c:
        c.execute('SELECT sha256,page_count FROM bronze_ch.vd_pdcom_documents WHERE id=%s',(DOC,));d=c.fetchone()
        if not d or d[0]!=SHA or d[1]!=1:raise ValueError('yvonand_commercial_stored_source')
    b=load_artifacts();refs=[refresh(conn,DOC,5939,r['bounds']) for r in b['references']];results=[]
    with conn.cursor() as c:
        c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_references a JOIN bronze_ch.vd_pdcom_parcel_references z USING(egrid) WHERE a.snapshot_id=ANY(%s::uuid[]) AND z.snapshot_id=ANY(%s::uuid[]) AND a.snapshot_id<z.snapshot_id AND (NOT ST_Equals(a.geom,z.geom) OR a.commune_bfs IS DISTINCT FROM z.commune_bfs OR a.official_attributes IS DISTINCT FROM z.official_attributes)''',([x['snapshot_id'] for x in refs],[x['snapshot_id'] for x in refs]))
        if c.fetchone()[0]:raise ValueError('yvonand_commercial_conflicting_tile_reference')
    for f in [{'key':'commercial_central_reinforcement','geometry_lv95':b['geometry_lv95_research_only'],'label':LABEL,'expected_pairs':b['expected_pairs'],'page':1}]:
        sid=sector_id(f['key']);geom=json.dumps(f['geometry_lv95']);label='Yvonand 2008 — support historique indicatif, surpeints conservés, hors droits à bâtir — '+f['label']
        frozen=[{'egrid':x['egrid'],'kind':x['attributes']['GENRE_TXT'],'geometry':x['geometry']} for x in f['expected_pairs']]
        evidence={'source_sha256':SHA,'source_path_ids':[18,65],'source_category':LABEL,'source_transform':AFFINE,'source_precision_m':None,'source_precision':'unknown','review_status':'review_required','publication_status':'internal_review_only','category_contract':b['category_contract'],'full_paint_ledger':'reports/yvonand-commercial-support/map-3-full-paint.json.gz','paint_semantics':'Original authored yellow support, retaining subsequent public/green/transport overlays; not visible-yellow area or exclusive zoning. No subtraction, clipping, repair or gap inference.','currentness':b['currentness'],'municipal_currentness':b['municipal_currentness'],'source3_reserved_diagnostic':b['source3_reserved_diagnostic'],'alignment_scalar_semantics':'Eight frozen reviewed reserved source3 building centroid residuals only; not all248originalreserved candidates, not thematic precision or unbiased validation. Source3path822 retained at1.7204527m.','alignment':b['alignment'],'registration':b['registration'],'whole_commune':b['registration']['current_registered_commune_whole_outline_check'],'boundary_buffer':'10m review heuristic, not a measured error bound','official_references':refs,'batch_sha256':BATCH_SHA,'grouping':'One literal commercial reinforcement policy collection with two complete native components; map4 is corroboration only','holds':b['holds']}
        with conn,conn.cursor() as c:
            c.execute("SET LOCAL statement_timeout='90s'");c.execute('SELECT pg_advisory_xact_lock(593900302)')
            c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_references WHERE snapshot_id=ANY(%s::uuid[]) AND geom && ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) AND NOT ST_IsValid(geom)''',([x['snapshot_id'] for x in refs],geom))
            if c.fetchone()[0]:raise ValueError('yvonand_commercial_invalid_current_reference')
            c.execute('DROP TABLE IF EXISTS pg_temp.yvonand_commercial_pairs')
            c.execute('''CREATE TEMP TABLE yvonand_commercial_pairs ON COMMIT DROP AS WITH g AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) geom),r AS MATERIALIZED (SELECT DISTINCT ON(egrid) * FROM bronze_ch.vd_pdcom_parcel_references WHERE snapshot_id=ANY(%s::uuid[]) AND ST_IsValid(geom) ORDER BY egrid,snapshot_id)
            SELECT r.*,ST_Intersection(r.geom,g.geom) overlap,ST_Area(r.geom) area,ST_DWithin(r.geom,ST_Boundary(g.geom),10) edge,CASE r.official_attributes->>'GENRE_TXT' WHEN 'DDP superficie' THEN 'ddp_superficie' WHEN 'DDP source' THEN 'ddp_source' WHEN 'parcelle privée' THEN 'bien_fonds' WHEN 'DP communal' THEN 'bien_fonds' WHEN 'DP cantonal' THEN 'bien_fonds' ELSE 'unknown' END object_kind FROM r,g WHERE ST_Intersects(r.geom,g.geom) AND ST_Area(ST_Intersection(r.geom,g.geom))>0''',(geom,[x['snapshot_id'] for x in refs]))
            c.execute('SELECT egrid FROM yvonand_commercial_pairs');keys={x[0] for x in c.fetchall()}
            if keys!={x['egrid'] for x in frozen}:raise ValueError('yvonand_commercial_current_membership_changed')
            c.execute('''WITH f AS (SELECT x->>'egrid' egrid,x->>'kind' kind,ST_SetSRID(ST_GeomFromGeoJSON(x->'geometry'),2056) geom FROM jsonb_array_elements(%s::jsonb) x) SELECT count(*) FROM yvonand_commercial_pairs n LEFT JOIN f USING(egrid) WHERE f.egrid IS NULL OR NOT ST_Equals(n.geom,f.geom) OR (n.official_attributes->>'GENRE_TXT') IS DISTINCT FROM f.kind''',(Json(frozen),))
            if c.fetchone()[0]:raise ValueError('yvonand_commercial_current_geometry_changed')
            c.execute('SELECT ST_IsValid(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)),(SELECT bool_and(ST_IsValid(ST_Transform(overlap,4326))) FROM yvonand_commercial_pairs)',(geom,))
            if c.fetchone()!=(True,True):raise ValueError('yvonand_commercial_transformed_geometry_invalid')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_sectors(id,document_id,page_number,label,geom,alignment_rmse_m,validation_evidence,review_status) VALUES(%s,%s,%s,%s,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326),%s,%s,'review_required') ON CONFLICT DO NOTHING''',(sid,DOC,f['page'],label,geom,b['source3_reserved_diagnostic']['rmse_m'],Json(evidence)))
            c.execute("SELECT label,review_status,source_precision_m,validated_by,ST_Equals(geom,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)) FROM bronze_ch.vd_pdcom_sectors WHERE id=%s",(geom,sid))
            if c.fetchone()!=(label,'review_required',None,None,True):raise ValueError('yvonand_commercial_existing_sector_changed')
            c.execute('SELECT commune_bfs FROM gold_ch.v_vd_pdcom_review_sectors WHERE id=%s',(sid,))
            if c.fetchone()[0]!=[5939]:raise ValueError('yvonand_commercial_attribution')
            c.execute('SELECT egrid FROM bronze_ch.vd_pdcom_parcel_candidates WHERE sector_id=%s',(sid,));old={x[0] for x in c.fetchall()}
            if old and old!=keys:raise ValueError('yvonand_commercial_existing_membership')
            c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_candidates p JOIN yvonand_commercial_pairs n USING(egrid) WHERE p.sector_id=%s AND (NOT ST_Equals(p.geom,ST_Transform(n.overlap,4326)) OR p.overlap_m2 IS DISTINCT FROM ST_Area(n.overlap) OR p.parcel_area_m2 IS DISTINCT FROM n.area OR p.overlap_fraction IS DISTINCT FROM LEAST(1,ST_Area(n.overlap)/n.area) OR p.boundary_review_required IS DISTINCT FROM n.edge OR p.uncertainty_buffer_m IS DISTINCT FROM 10 OR p.cadastral_object_kind IS DISTINCT FROM n.object_kind)''',(sid,))
            if c.fetchone()[0]:raise ValueError('yvonand_commercial_existing_values')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_parcel_candidates(sector_id,egrid,overlap_m2,parcel_area_m2,overlap_fraction,boundary_review_required,uncertainty_buffer_m,geom,cadastral_object_kind,cadastral_type_evidence) SELECT %s,n.egrid,ST_Area(n.overlap),n.area,LEAST(1,ST_Area(n.overlap)/n.area),n.edge,10,ST_Transform(n.overlap,4326),n.object_kind,jsonb_build_object('source_url',s.query_url,'response_sha256',s.response_sha256,'reference_snapshot_ids',jsonb_build_array(n.snapshot_id),'official_attributes',n.official_attributes,'status','matched') FROM yvonand_commercial_pairs n JOIN bronze_ch.vd_pdcom_parcel_reference_snapshots s ON s.id=n.snapshot_id ON CONFLICT DO NOTHING''',(sid,))
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_candidate_parcel_references(sector_id,egrid,snapshot_id) SELECT %s,egrid,snapshot_id FROM yvonand_commercial_pairs ON CONFLICT(sector_id,egrid) DO UPDATE SET snapshot_id=EXCLUDED.snapshot_id''',(sid,))
            c.execute('SELECT validation_evidence FROM bronze_ch.vd_pdcom_sectors WHERE id=%s',(sid,))
            if c.fetchone()[0]!=evidence:raise ValueError('yvonand_commercial_existing_evidence_changed')
        results.append({'key':f['key'],'sector_id':sid,'parcel_pairs':len(keys)})
    return {'collections':len(results),'parcel_pairs':sum(r['parcel_pairs'] for r in results),'candidates':results,'publication_status':'internal_review_only','review_status':'review_required'}
