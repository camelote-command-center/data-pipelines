"""Hash-bound indicative source-layer collections; existing private review only."""
import hashlib,json,uuid,math
from pathlib import Path
from psycopg2.extras import Json
from shapely.geometry import shape
from shapely.ops import unary_union
from shapely.affinity import affine_transform
from official_references import refresh
from cadastral_types import KINDS
DOC='b8fc92d6-c4a6-5a3c-8fee-dfa2b11de848'
SHA='d4fecb86752e2f2cb2c3359e835e9555694979b88288bb50795080ec349a1a2b'
BATCH_SHA='b994b84628118286d6795bbbb000d666cef59ba0420d225d1c340efff99bf506'
KEYS={'existing_medium_habitat','existing_public_utility','existing_industry_craft','existing_low_medium_habitat'}

def sector_id(key):
    if key not in KEYS:raise ValueError('yvonand_unreviewed_category')
    return str(uuid.uuid5(uuid.NAMESPACE_URL,DOC+'#yvonand-existing-vocations-v1#'+key))
def validate(b):
    if (b['document_id'],b['source_sha256'])!=(DOC,SHA):raise ValueError('yvonand_source')
    if b['publication_status']!='internal_review_only' or b['source_precision_m'] is not None:raise ValueError('yvonand_private_precision')
    if b['alignment']['holdout_rmse_m'] is not None or b['alignment']['global_accuracy_claim'] or b['alignment']['fit_changed']:raise ValueError('yvonand_no_accuracy_promotion')
    if b['affine']!=[1.7638888888888888,0,0,-1.7638888888888888,2544324.804072453,1184695.3179479088]:raise ValueError('yvonand_frozen_transform')
    if b['alignment']['prior_filtered_diagnostic_only']['reserved_matched_rmse_m']!=.5241450954613012:raise ValueError('yvonand_frozen_diagnostic')
    if b['native_pdf_contract']['page_rotation']!=90:raise ValueError('yvonand_source_rotation')
    ledger=b['full_source_identity_ledger']['rows']
    if len(ledger)!=1620 or len({x['source_drawing_index'] for x in ledger})!=1620:raise ValueError('yvonand_complete_identity_ledger')
    for r in ledger:
        if r['role']!=('reserved' if r['source_drawing_index']%5==0 else 'training'):raise ValueError('yvonand_original_role')
        if r['original_eligible']!=(shape(r['source_geometry_pdf']).area*b['affine'][0]**2>70):raise ValueError('yvonand_original_eligibility')
    for role,total,matched,bad in [('training',982,457,10),('reserved',248,114,7)]:
        rows=[r for r in ledger if r['role']==role and r['original_eligible']]
        if len(rows)!=total or sum(r['original_match'] is not None for r in rows)!=matched or sum(r['original_match'] is not None and r['original_match']['iou']<.85 for r in rows)!=bad:raise ValueError('yvonand_original_failure_denominators')
    controls=b['reviewed_control_identities']['selected'];byid={r['source_drawing_index']:r for r in ledger}
    if len(controls)!=19 or len({r['best_OBJECTID'] for r in controls})!=19:raise ValueError('yvonand_reviewed_identity_count')
    for r in controls:
        if r!=byid[r['source_drawing_index']] or r['reference_attributes']['OBJECTID']!=r['best_OBJECTID']:raise ValueError('yvonand_identity_correspondence')
        g=affine_transform(shape(r['source_geometry_pdf']),b['affine']);ref=shape(r['reference_geometry'])
        if not g.is_valid or not ref.is_valid or abs(g.intersection(ref).area/g.union(ref).area-r['best_iou'])>1e-8 or r['best_iou']<.85 or r['second_iou']>.25:raise ValueError('yvonand_control_shape_evidence')
    train=[r for r in controls if r['role']=='training']
    if len(train)!=11:raise ValueError('yvonand_reviewed_control_roles')
    from shapely.geometry import MultiPoint
    hull=MultiPoint([shape(r['source_geometry_pdf']).centroid for r in train]).convex_hull
    if b['independent_qa']['decision']!='accepted_for_private_indicative_historical_review_only':raise ValueError('yvonand_independent_decision')
    if len(b['features'])!=4 or {f['key'] for f in b['features']}!=KEYS:raise ValueError('yvonand_scope')
    expected_paths={'existing_medium_habitat':{893,288,284,676},'existing_public_utility':{357},'existing_industry_craft':{887},'existing_low_medium_habitat':{299,293}}
    pairs=0
    for f in b['features']:
        if set(f['path_ids'])!=expected_paths[f['key']] or len(f['source_paths'])!=len(f['path_ids']):raise ValueError('yvonand_native_scope')
        sem=next(c for c in b['literal_semantics']['categories'] if set(c['drawing_indexes'])==set(f['path_ids']))
        if f['label']!=sem['legend'] or f['category']!=sem['legend'] or f['policy_status']['state']!='historical_existing_vocation_not_current_zoning':raise ValueError('yvonand_semantics')
        gs=[]
        for r in f['source_paths']:
            if r['id'] not in f['path_ids'] or r['original_paint_record']['drawing_index']!=r['id']:raise ValueError('yvonand_path_identity')
            g=shape(r['original_paint_record']['geometry_pdf_display_points']);expected=affine_transform(g,b['affine'])
            if not hull.covers(g) or not r['whole_visually_reviewed_training_hull']:raise ValueError('yvonand_whole_hull')
            if not expected.equals_exact(shape(r['geometry_lv95_research_only']),1e-8):raise ValueError('yvonand_transform')
            gs.append(expected)
        g=shape(f['geometry_lv95'])
        if not g.is_valid or g.is_empty or not g.equals(unary_union(gs)):raise ValueError('yvonand_union')
        if f['invalid_reference_envelope_overlap']:raise ValueError('yvonand_invalid_reference')
        seen=set()
        for r in f['expected_pairs']:
            a=r['attributes'];ref=shape(r['geometry'])
            if r['egrid'] in seen or a['EGRID']!=r['egrid'] or a['NO_COM_FED']!=5939 or a['GENRE_TXT'] not in KINDS or not ref.is_valid or g.intersection(ref).area<=0:raise ValueError('yvonand_reference_identity')
            seen.add(r['egrid']);pairs+=1
    if pairs!=163:raise ValueError('yvonand_pair_count')
    return b
def load_artifacts():
    raw=(Path(__file__).parent/'reports/yvonand-existing-vocations/areas.json').read_bytes()
    if hashlib.sha256(raw).hexdigest()!=BATCH_SHA:raise ValueError('yvonand_changed_batch_requires_review')
    return validate(json.loads(raw))
def persist(conn,document_id,sha):
    if str(document_id)!=DOC or sha!=SHA:return None
    with conn.cursor() as c:
        c.execute('SELECT sha256,page_count FROM bronze_ch.vd_pdcom_documents WHERE id=%s',(DOC,));d=c.fetchone()
        if not d or d[0]!=SHA or not d[1] or d[1]<1:raise ValueError('yvonand_stored_source')
    b=load_artifacts();refs=[refresh(conn,DOC,5939,r['bounds']) for r in b['references']];results=[]
    with conn.cursor() as c:
        c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_references a JOIN bronze_ch.vd_pdcom_parcel_references z USING(egrid) WHERE a.snapshot_id=ANY(%s::uuid[]) AND z.snapshot_id=ANY(%s::uuid[]) AND a.snapshot_id<z.snapshot_id AND (NOT ST_Equals(a.geom,z.geom) OR a.commune_bfs IS DISTINCT FROM z.commune_bfs OR a.official_attributes IS DISTINCT FROM z.official_attributes)''',([x['snapshot_id'] for x in refs],[x['snapshot_id'] for x in refs]))
        if c.fetchone()[0]:raise ValueError('yvonand_conflicting_tile_reference')
    for f in b['features']:
        sid=sector_id(f['key']);geom=json.dumps(f['geometry_lv95']);label='Yvonand — couche indicative partielle — '+f['label']
        frozen=[{'egrid':x['egrid'],'kind':x['attributes']['GENRE_TXT'],'geometry':x['geometry']} for x in f['expected_pairs']]
        evidence={'source_sha256':SHA,'source_path_ids':f['path_ids'],'source_category':f['category'],'source_ledger_indices':f['source_ledger_indices'],'literal_source_labels':f['literal_source_labels'],'whole_training_hull':{x['id']:x['whole_visually_reviewed_training_hull'] for x in f['source_paths']},'coordinate_scope':f['coordinate_scope'],'withheld_source_path_ids':b['delivery_scope']['withheld_path_ids'],'source_policy_status':f['policy_status'],'semantic_limitations':f['semantic_limitations'],'legend_label_id':f['label_id'],'page_number':f['page'],'grouping':'Union of explicitly listed source paths; partial thematic collection, not a physical sector','semantics':b['semantics'],'reservation':b['reservation'],'alignment':b['alignment'],'independent_geographic_review':b['independent_qa'],'literal_source_semantics':b['literal_semantics'],'full_control_ledger_artifact':'reports/yvonand-existing-vocations/areas.json','source_transform':b['affine'],'map_review':'Independent original-source90degree displaytransform and19distributed same-building identities establish bounded historical registration only;11reviewed training-hull identities enclose allsource supports. Original982training/248reserved denominator,17shape discrepancies and134reserved unmatched retained. Filtered residuals are not accuracy qualification. No new fit.','source_precision':'unknown','source_precision_m':None,'boundary_buffer':'10m review heuristic, not a measured error bound','review_status':'review_required','publication_status':'internal_review_only','reference_scope':'Current official cadastral objects for source commune5939 only; DDP rights distinct from underlying land, not exclusive area; entire source geometry retained, not clipped','official_references':refs,'batch_sha256':BATCH_SHA,'artifact':'pipelines/vd-pdcom/reports/yvonand-existing-vocations'}
        with conn,conn.cursor() as c:
            c.execute("SET LOCAL statement_timeout='90s'");c.execute('SELECT pg_advisory_xact_lock(593900301)')
            c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_references WHERE snapshot_id=ANY(%s::uuid[]) AND geom && ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) AND NOT ST_IsValid(geom)''',([x['snapshot_id'] for x in refs],geom))
            if c.fetchone()[0]:raise ValueError('yvonand_invalid_current_reference')
            c.execute('DROP TABLE IF EXISTS pg_temp.yvonand_pairs')
            c.execute('''CREATE TEMP TABLE yvonand_pairs ON COMMIT DROP AS WITH g AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) geom),r AS MATERIALIZED (SELECT DISTINCT ON(egrid) * FROM bronze_ch.vd_pdcom_parcel_references WHERE snapshot_id=ANY(%s::uuid[]) AND ST_IsValid(geom) ORDER BY egrid,snapshot_id)
            SELECT r.*,ST_Intersection(r.geom,g.geom) overlap,ST_Area(r.geom) area,ST_DWithin(r.geom,ST_Boundary(g.geom),10) edge,CASE r.official_attributes->>'GENRE_TXT' WHEN 'DDP superficie' THEN 'ddp_superficie' WHEN 'DDP source' THEN 'ddp_source' WHEN 'parcelle privée' THEN 'bien_fonds' WHEN 'DP communal' THEN 'bien_fonds' WHEN 'DP cantonal' THEN 'bien_fonds' ELSE 'unknown' END object_kind FROM r,g WHERE ST_Intersects(r.geom,g.geom) AND ST_Area(ST_Intersection(r.geom,g.geom))>0''',(geom,[x['snapshot_id'] for x in refs]))
            c.execute('SELECT egrid FROM yvonand_pairs');keys={x[0] for x in c.fetchall()}
            if keys!={x['egrid'] for x in frozen}:raise ValueError('yvonand_current_membership_changed')
            c.execute('''WITH f AS (SELECT x->>'egrid' egrid,x->>'kind' kind,ST_SetSRID(ST_GeomFromGeoJSON(x->'geometry'),2056) geom FROM jsonb_array_elements(%s::jsonb) x) SELECT count(*) FROM yvonand_pairs n LEFT JOIN f USING(egrid) WHERE f.egrid IS NULL OR NOT ST_Equals(n.geom,f.geom) OR (n.official_attributes->>'GENRE_TXT') IS DISTINCT FROM f.kind''',(Json(frozen),))
            if c.fetchone()[0]:raise ValueError('yvonand_current_geometry_changed')
            c.execute('SELECT ST_IsValid(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)),(SELECT bool_and(ST_IsValid(ST_Transform(overlap,4326))) FROM yvonand_pairs)',(geom,))
            if c.fetchone()!=(True,True):raise ValueError('yvonand_transformed_geometry_invalid')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_sectors(id,document_id,page_number,label,geom,alignment_rmse_m,validation_evidence,review_status) VALUES(%s,%s,%s,%s,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326),%s,%s,'review_required') ON CONFLICT DO NOTHING''',(sid,DOC,f['page'],label,geom,b['alignment']['prior_filtered_diagnostic_only']['reserved_matched_rmse_m'],Json(evidence)))
            c.execute("SELECT label,review_status,source_precision_m,validated_by,ST_Equals(geom,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)) FROM bronze_ch.vd_pdcom_sectors WHERE id=%s",(geom,sid))
            if c.fetchone()!=(label,'review_required',None,None,True):raise ValueError('yvonand_existing_sector_changed')
            c.execute('SELECT commune_bfs FROM gold_ch.v_vd_pdcom_review_sectors WHERE id=%s',(sid,))
            if c.fetchone()[0]!=[5939]:raise ValueError('yvonand_attribution')
            c.execute('SELECT egrid FROM bronze_ch.vd_pdcom_parcel_candidates WHERE sector_id=%s',(sid,));old={x[0] for x in c.fetchall()}
            if old and old!=keys:raise ValueError('yvonand_existing_membership')
            c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_candidates p JOIN yvonand_pairs n USING(egrid) WHERE p.sector_id=%s AND (NOT ST_Equals(p.geom,ST_Transform(n.overlap,4326)) OR p.overlap_m2 IS DISTINCT FROM ST_Area(n.overlap) OR p.parcel_area_m2 IS DISTINCT FROM n.area OR p.overlap_fraction IS DISTINCT FROM LEAST(1,ST_Area(n.overlap)/n.area) OR p.boundary_review_required IS DISTINCT FROM n.edge OR p.uncertainty_buffer_m IS DISTINCT FROM 10 OR p.cadastral_object_kind IS DISTINCT FROM n.object_kind)''',(sid,))
            if c.fetchone()[0]:raise ValueError('yvonand_existing_values')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_parcel_candidates(sector_id,egrid,overlap_m2,parcel_area_m2,overlap_fraction,boundary_review_required,uncertainty_buffer_m,geom,cadastral_object_kind,cadastral_type_evidence) SELECT %s,n.egrid,ST_Area(n.overlap),n.area,LEAST(1,ST_Area(n.overlap)/n.area),n.edge,10,ST_Transform(n.overlap,4326),n.object_kind,jsonb_build_object('source_url',s.query_url,'response_sha256',s.response_sha256,'reference_snapshot_ids',jsonb_build_array(n.snapshot_id),'official_attributes',n.official_attributes,'status','matched') FROM yvonand_pairs n JOIN bronze_ch.vd_pdcom_parcel_reference_snapshots s ON s.id=n.snapshot_id ON CONFLICT DO NOTHING''',(sid,))
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_candidate_parcel_references(sector_id,egrid,snapshot_id) SELECT %s,egrid,snapshot_id FROM yvonand_pairs ON CONFLICT(sector_id,egrid) DO UPDATE SET snapshot_id=EXCLUDED.snapshot_id''',(sid,))
            c.execute('UPDATE bronze_ch.vd_pdcom_sectors SET validation_evidence=%s WHERE id=%s',(Json(evidence),sid))
        results.append({'key':f['key'],'sector_id':sid,'parcel_pairs':len(keys)})
    return {'collections':len(results),'parcel_pairs':sum(r['parcel_pairs'] for r in results),'candidates':results,'publication_status':'internal_review_only','review_status':'review_required'}
