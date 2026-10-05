"""Two partial historical equipment/green source-support collections, private only."""
import gzip,hashlib,json,uuid,fitz
from pathlib import Path
from psycopg2.extras import Json
from shapely.geometry import Polygon,shape
from shapely.ops import unary_union
from shapely.affinity import affine_transform
from official_references import refresh
from cadastral_types import KINDS
import yvonand_commercial_support as registration_source
DOC='567600de-dc15-516c-b511-e33fbebce5eb'
SHA='99e1f03d89478b5815b7b0e9ef0d5c5511cbfd4e48d6ee038ad80ac86c8b0559'
BATCH_SHA='ff0c049c60f60719ce5b112bca600704aeeb7e762f38b1a2dc4b47da122764a9'
ROOT=Path(__file__).parent/'reports/yvonand-equipment-green'
PATHS={'equipment_public':{23,63,4142,4147,4203,4428,4510,4532,4541,4563,4624,4655,4669,4826,4843,4845,4848,4851,4853,4870,4873,4875,5195,5214,5250,5256},'existing_public_green':{43,73,81,83}}
LABELS={'equipment_public':'Equipement public','existing_public_green':'Espace public de verdure existant'}
KEYS=set(PATHS)
def sector_id(key):
    if key not in KEYS:raise ValueError('yvonand_equipment_green_scope')
    return str(uuid.uuid5(uuid.NAMESPACE_URL,DOC+'#yvonand-equipment-green-supports-v1#'+key))
def fail(ok,name):
    if not ok:raise ValueError('yvonand_equipment_green_'+name)
def validate(b):
    fail((b['document_id'],b['source_sha256'])==(DOC,SHA),'source')
    fail(b['source_precision_m'] is None and b['publication_status']=='internal_review_only' and b['review_status']=='review_required','private_unknown_precision')
    prior=registration_source.load_artifacts()
    for key in ['registration','affine','alignment','source3_reserved_diagnostic','municipal_currentness']:fail(b[key]==prior[key],'unchanged_registration_'+key)
    fail(b['independent_semantic_qa']['decision'].startswith('accepted') and b['independent_parcel_qa']['decision'].startswith('accepted'),'independent_review')
    fail(len(b['features'])==2 and {f['key']for f in b['features']}==KEYS,'two_categories')
    fail(len(b['exceptions'])==5 and {x['drawing_index']for x in b['exceptions']}=={1,55,67,91,94},'five_holds')
    raw=gzip.decompress((registration_source.ROOT/'map-3-full-paint.json.gz').read_bytes());fail(hashlib.sha256(raw).hexdigest()==b['original_source_paint_ledger']['sha256'],'complete_paint_ledger')
    ledger=json.loads(raw);records=ledger['records'];byid={r['drawing_index']:r for r in records if r['drawing_index']is not None}
    fail(len(byid)==5664 and len(records)==6149 and ledger['source_sha256']==SHA and ledger['page_rotation']==90,'source_contract')
    selected={i for i,r in byid.items()if r.get('fill')in [[0,0,1],[0,1,0]]};classified=b['full_selected_fill_classification']
    fail(len(classified)==679 and {r['drawing_index']for r in classified}==selected,'all_blue_green_accounted')
    disposition={r['drawing_index']:r['disposition']for r in classified};hull=shape(b['registration']['source3_training_hull'])
    boundary=json.loads((registration_source.ROOT/'registered-commune-boundary.json').read_text());commune=unary_union([shape(f['geometry'])for f in boundary['features']])
    raw=gzip.decompress((ROOT/'official-complete-reference.geojson.gz').read_bytes());fail(hashlib.sha256(raw).hexdigest()==b['full_reference_sha256']==b['official_reference']['sha256'],'full_reference_hash')
    refs=json.loads(raw)['features'];fail(len(refs)==716 and b['official_reference']['count']==716 and b['official_reference']['count_verified'],'complete_reference_count')
    refsbyid={r['properties']['EGRID']:r for r in refs};fail(len(refsbyid)==716,'unique_reference')
    for r in refs:
        a=r['properties'];fail(shape(r['geometry']).is_valid and a['NO_COM_FED']==5939 and a['GENRE_TXT']in KINDS,'reference_identity')
    pair_total=0
    for f in b['features']:
        key=f['key'];fail(f['literal_legend']==f['label']==LABELS[key] and not f['category_complete'],'literal_partial_category')
        ids=f['path_ids'];fail(len(ids)==len(PATHS[key]) and set(ids)==PATHS[key] and {c['drawing_index']for c in f['components']}==PATHS[key] and len(f['components'])==len(ids),'whole_components')
        legendid=5597 if key=='equipment_public'else 5605;fail(f['legend_record']==byid[legendid],'legend_identity')
        gs=[]
        for c in f['components']:
            idx=c['drawing_index'];r=c['source_record'];fail(r==byid[idx] and disposition[idx].startswith('accepted_original_'),'native_identity')
            fail(not c['source_clipped'] and not c['geometry_repaired'] and not c['visible_fill_area_claim'],'authored_support')
            fail(r['fill']==([0,0,1]if key=='equipment_public'else[0,1,0]) and r['fill_opacity']==1 and all(x['type']=='group'for x in c['graphics_state_ancestors']),'paint_semantics')
            pts=[r['items'][0][1]]
            for item in r['items']:
                fail(item[0]=='l' and item[1]==pts[-1],'literal_chain');pts.append(item[2])
            fail(pts[-1]==pts[0],'explicitly_closed')
            display=Polygon([list(fitz.Point(pt)*fitz.Matrix(*ledger['rotation_matrix']))for pt in pts]);fail(display.is_valid and display.equals_exact(shape(c['geometry_display_pdf']),0),'literal_geometry')
            fail(hull.covers(display) and c['whole_training_hull'],'whole_hull')
            g=affine_transform(display,b['affine']);fail(g.equals_exact(shape(c['geometry_lv95']),0) and commune.covers(g) and c['whole_registered_commune'],'whole_transformed_support');gs.append(g)
        g=shape(f['geometry_lv95']);fail(g.is_valid and g.equals(unary_union(gs)) and g.equals(shape(f['geometry_lv95_research_only'])),'whole_union_not_area_sum')
        pairs=f['expected_pairs'];expect={k:r for k,r in refsbyid.items()if g.intersection(shape(r['geometry'])).area>0}
        fail(len(pairs)==(23 if key=='equipment_public'else 15) and len(pairs)==len(expect) and {p['egrid']for p in pairs}==set(expect),'complete_pair_membership')
        for p in pairs:
            r=expect[p['egrid']];ref=shape(r['geometry']);overlap=g.intersection(ref);a=r['properties']
            fail(p['attributes']==a and p['geometry']==r['geometry'] and overlap.equals_exact(shape(p['overlap_geometry_lv95']),0),'pair_geometry_identity')
            fail(p['overlap_m2']==overlap.area and p['parcel_area_m2']==ref.area and p['overlap_fraction']==min(1,overlap.area/ref.area),'pair_areas')
            fail(p['boundary_review_required']==(ref.distance(g.boundary)<=10) and p['uncertainty_buffer_m']==10 and p['contextual_only'],'review_flags')
            kind='ddp_superficie'if a['GENRE_TXT']=='DDP superficie'else'ddp_source'if a['GENRE_TXT']=='DDP source'else'bien_fonds';fail(p['cadastral_object_kind']==kind,'ddp_kind')
        fail(not f['invalid_reference_envelope_overlap'],'invalid_reference_hold');pair_total+=len(pairs)
    fail(pair_total==38,'pair_total')
    return b
def load_artifacts():
    raw=(ROOT/'areas.json').read_bytes();fail(hashlib.sha256(raw).hexdigest()==BATCH_SHA,'changed_batch_requires_review');return validate(json.loads(raw))
def persist(conn,document_id,sha):
    if str(document_id)!=DOC or sha!=SHA:return None
    with conn.cursor() as c:
        c.execute('SELECT sha256,page_count FROM bronze_ch.vd_pdcom_documents WHERE id=%s',(DOC,));d=c.fetchone()
        if not d or d[0]!=SHA or d[1]!=1:raise ValueError('yvonand_equipment_green_stored_source')
    b=load_artifacts();refs=[refresh(conn,DOC,5939,r['bounds']) for r in b['references']];results=[]
    with conn.cursor() as c:
        c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_references a JOIN bronze_ch.vd_pdcom_parcel_references z USING(egrid) WHERE a.snapshot_id=ANY(%s::uuid[]) AND z.snapshot_id=ANY(%s::uuid[]) AND a.snapshot_id<z.snapshot_id AND (NOT ST_Equals(a.geom,z.geom) OR a.commune_bfs IS DISTINCT FROM z.commune_bfs OR a.official_attributes IS DISTINCT FROM z.official_attributes)''',([x['snapshot_id'] for x in refs],[x['snapshot_id'] for x in refs]))
        if c.fetchone()[0]:raise ValueError('yvonand_equipment_green_conflicting_tile_reference')
    for f in b['features']:
        sid=sector_id(f['key']);geom=json.dumps(f['geometry_lv95']);label='Yvonand 2008 — support historique indicatif, surpeints conservés, hors droits à bâtir — '+f['label']
        frozen=[{'egrid':x['egrid'],'kind':x['attributes']['GENRE_TXT'],'geometry':x['geometry']} for x in f['expected_pairs']]
        evidence={'source_sha256':SHA,'source_path_ids':f['path_ids'],'source_category':f['literal_legend'],'source_transform':b['affine'],'source_precision_m':None,'source_precision':'unknown','review_status':'review_required','publication_status':'internal_review_only','source_policy_status':f['source_policy_status'],'category_complete':False,'semantic_limitations':f['semantics'],'all_five_held_paths':[1,55,67,91,94],'paint_semantics':'Original authored whole solid supports; later overpainting retained; not net visible area, exclusive land use, ownership/access or rights. Equipment component overlaps are unioned, never counted twice; different categories may overlap.','municipal_currentness':b['municipal_currentness'],'source3_reserved_diagnostic':b['source3_reserved_diagnostic'],'alignment_scalar_semantics':'Same eight frozen source3 reserved-control residuals; filtered diagnostic only, original248reserved denominator and1.7204527m outlier retained; no thematic precision.','alignment':b['alignment'],'registration':b['registration'],'registration_inheritance_scope':'Only frozen affine, controls and diagnostic are reused. Any source18/65 support checks inside inherited registration belong to the prior commercial collection. Current component gates are recorded separately.','whole_component_training_hull':{str(c['drawing_index']):c['whole_training_hull']for c in f['components']},'whole_component_registered_commune':{str(c['drawing_index']):c['whole_registered_commune']for c in f['components']},'boundary_buffer':'10m review heuristic, not a measured error bound','official_references':refs,'batch_sha256':BATCH_SHA,'grouping':'One partial collection per literal category, not one physical sector per polygon','object_scope':b['object_scope'],'holds':b['holds'],'independent_semantic_review':b['independent_semantic_qa'],'independent_parcel_review':b['independent_parcel_qa']}
        with conn,conn.cursor() as c:
            c.execute("SET LOCAL statement_timeout='90s'");c.execute('SELECT pg_advisory_xact_lock(593900303)')
            c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_references WHERE snapshot_id=ANY(%s::uuid[]) AND geom && ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) AND NOT ST_IsValid(geom)''',([x['snapshot_id'] for x in refs],geom))
            if c.fetchone()[0]:raise ValueError('yvonand_equipment_green_invalid_current_reference')
            c.execute('DROP TABLE IF EXISTS pg_temp.yvonand_equipment_green_pairs')
            c.execute('''CREATE TEMP TABLE yvonand_equipment_green_pairs ON COMMIT DROP AS WITH g AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) geom),r AS MATERIALIZED (SELECT DISTINCT ON(egrid) * FROM bronze_ch.vd_pdcom_parcel_references WHERE snapshot_id=ANY(%s::uuid[]) AND ST_IsValid(geom) ORDER BY egrid,snapshot_id)
            SELECT r.*,ST_Intersection(r.geom,g.geom) overlap,ST_Area(r.geom) area,ST_DWithin(r.geom,ST_Boundary(g.geom),10) edge,CASE r.official_attributes->>'GENRE_TXT' WHEN 'DDP superficie' THEN 'ddp_superficie' WHEN 'DDP source' THEN 'ddp_source' WHEN 'parcelle privée' THEN 'bien_fonds' WHEN 'DP communal' THEN 'bien_fonds' WHEN 'DP cantonal' THEN 'bien_fonds' ELSE 'unknown' END object_kind FROM r,g WHERE ST_Intersects(r.geom,g.geom) AND ST_Area(ST_Intersection(r.geom,g.geom))>0''',(geom,[x['snapshot_id'] for x in refs]))
            c.execute('SELECT egrid FROM yvonand_equipment_green_pairs');keys={x[0] for x in c.fetchall()}
            if keys!={x['egrid'] for x in frozen}:raise ValueError('yvonand_equipment_green_current_membership_changed')
            c.execute('''WITH f AS (SELECT x->>'egrid' egrid,x->>'kind' kind,ST_SetSRID(ST_GeomFromGeoJSON(x->'geometry'),2056) geom FROM jsonb_array_elements(%s::jsonb) x) SELECT count(*) FROM yvonand_equipment_green_pairs n LEFT JOIN f USING(egrid) WHERE f.egrid IS NULL OR NOT ST_Equals(n.geom,f.geom) OR (n.official_attributes->>'GENRE_TXT') IS DISTINCT FROM f.kind''',(Json(frozen),))
            if c.fetchone()[0]:raise ValueError('yvonand_equipment_green_current_geometry_changed')
            c.execute('SELECT ST_IsValid(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)),(SELECT bool_and(ST_IsValid(ST_Transform(overlap,4326))) FROM yvonand_equipment_green_pairs)',(geom,))
            if c.fetchone()!=(True,True):raise ValueError('yvonand_equipment_green_transformed_geometry_invalid')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_sectors(id,document_id,page_number,label,geom,alignment_rmse_m,validation_evidence,review_status) VALUES(%s,%s,%s,%s,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326),%s,%s,'review_required') ON CONFLICT DO NOTHING''',(sid,DOC,f['page'],label,geom,b['source3_reserved_diagnostic']['rmse_m'],Json(evidence)))
            c.execute("SELECT label,review_status,source_precision_m,validated_by,ST_Equals(geom,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)),alignment_rmse_m=%s FROM bronze_ch.vd_pdcom_sectors WHERE id=%s",(geom,b['source3_reserved_diagnostic']['rmse_m'],sid))
            if c.fetchone()!=(label,'review_required',None,None,True,True):raise ValueError('yvonand_equipment_green_existing_sector_changed')
            c.execute('SELECT commune_bfs FROM gold_ch.v_vd_pdcom_review_sectors WHERE id=%s',(sid,))
            if c.fetchone()[0]!=[5939]:raise ValueError('yvonand_equipment_green_attribution')
            c.execute('SELECT egrid FROM bronze_ch.vd_pdcom_parcel_candidates WHERE sector_id=%s',(sid,));old={x[0] for x in c.fetchall()}
            if old and old!=keys:raise ValueError('yvonand_equipment_green_existing_membership')
            c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_candidates p JOIN yvonand_equipment_green_pairs n USING(egrid) WHERE p.sector_id=%s AND (NOT ST_Equals(p.geom,ST_Transform(n.overlap,4326)) OR p.overlap_m2 IS DISTINCT FROM ST_Area(n.overlap) OR p.parcel_area_m2 IS DISTINCT FROM n.area OR p.overlap_fraction IS DISTINCT FROM LEAST(1,ST_Area(n.overlap)/n.area) OR p.boundary_review_required IS DISTINCT FROM n.edge OR p.uncertainty_buffer_m IS DISTINCT FROM 10 OR p.cadastral_object_kind IS DISTINCT FROM n.object_kind)''',(sid,))
            if c.fetchone()[0]:raise ValueError('yvonand_equipment_green_existing_values')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_parcel_candidates(sector_id,egrid,overlap_m2,parcel_area_m2,overlap_fraction,boundary_review_required,uncertainty_buffer_m,geom,cadastral_object_kind,cadastral_type_evidence) SELECT %s,n.egrid,ST_Area(n.overlap),n.area,LEAST(1,ST_Area(n.overlap)/n.area),n.edge,10,ST_Transform(n.overlap,4326),n.object_kind,jsonb_build_object('source_url',s.query_url,'response_sha256',s.response_sha256,'reference_snapshot_ids',jsonb_build_array(n.snapshot_id),'official_attributes',n.official_attributes,'status','matched') FROM yvonand_equipment_green_pairs n JOIN bronze_ch.vd_pdcom_parcel_reference_snapshots s ON s.id=n.snapshot_id ON CONFLICT DO NOTHING''',(sid,))
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_candidate_parcel_references(sector_id,egrid,snapshot_id) SELECT %s,egrid,snapshot_id FROM yvonand_equipment_green_pairs ON CONFLICT(sector_id,egrid) DO UPDATE SET snapshot_id=EXCLUDED.snapshot_id''',(sid,))
            c.execute('SELECT validation_evidence FROM bronze_ch.vd_pdcom_sectors WHERE id=%s',(sid,))
            if c.fetchone()[0]!=evidence:raise ValueError('yvonand_equipment_green_existing_evidence_changed')
        results.append({'key':f['key'],'sector_id':sid,'parcel_pairs':len(keys)})
    return {'collections':len(results),'parcel_pairs':sum(r['parcel_pairs'] for r in results),'candidates':results,'publication_status':'internal_review_only','review_status':'review_required'}
