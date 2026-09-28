"""Exact approved Vevey historic map supports; private review only."""
import hashlib,json,uuid,math
from pathlib import Path
from psycopg2.extras import Json
from shapely.geometry import shape,MultiPoint,box
from shapely.affinity import affine_transform
from shapely.ops import unary_union
from native_closed_paths import closed_polygon
from official_references import refresh
from cadastral_types import KINDS
DOC='9d0a37f4-0265-59b3-a2f6-b21b57ae5414'
SHA='196c5b690494ce19ade545af7519fa9dc8b413666bbcb90b69cca769c92054f3'
BATCH_SHA='d0304dc7c91022485bb1e44b5cece7d6d858079d090e7c0590fcd2e08330113d'
KEYS={'historic_dense'}
ATTRIBUTION={k:[5890] for k in KEYS}
PATHS={'historic_dense':[92051, 92052, 92053, 92054, 92055, 92056, 92057, 92058, 92059, 92060, 92061, 92062, 92063, 92064, 92065, 92067, 92068, 92069, 92070, 92071, 92072, 92073, 92074, 92075, 92076, 92077, 92078, 92079, 92080, 92081, 92082, 92083, 92084, 92085, 92086, 92087, 92088, 92089, 92090, 92091, 92092, 92093, 92094, 92095, 92096, 92097, 92098, 92099, 92100, 92101, 92102, 92103, 92104, 92105, 92106, 92107, 92108, 92109, 92110, 92111, 92113, 92114, 92115, 92116, 92117, 92118, 92119, 92120, 92121, 92122, 92123, 92124, 92125, 92126, 92127, 92128, 92129, 92130, 92131, 92132, 92133, 92134, 92135, 92136, 92137, 92138, 92139, 92140, 92141, 92142, 92143, 92144, 92145, 92146, 92147, 92148, 92149, 92151, 92152, 92153, 92154, 92155, 92156, 92157, 92158, 92159, 92160, 92161, 92162, 92163, 92164, 92165, 92166, 92167, 92168, 92169, 92171, 92172, 92173, 92174, 92175, 92176, 92177, 92178, 92179, 92180, 92181, 92182, 92183, 92184, 92185, 92188, 92189, 92190, 92191, 92192, 92193, 92194, 92195, 92196, 92197, 92198, 92199, 92200, 92201, 92202, 92203, 92204, 92206, 92209, 92210, 92211, 92213, 92214, 92215, 92216, 92217, 92218, 92219, 92220, 92221, 92223, 92224, 92225, 92226, 92227, 92228, 92229, 92230, 92231, 92232, 92233, 92234, 92235, 92236, 92237, 92238, 92239, 92240, 92241, 92242, 92243, 92244, 92245, 92246, 92248, 92249, 92250, 92251, 92252, 92253, 92254, 92255, 92256, 92257, 92258, 92259, 92260, 92261, 92262, 92263, 92265, 92267, 92268, 92269, 92270, 92271, 92272, 92273, 92274, 92275, 92276, 92277, 92278, 92279, 92280, 92281, 92282, 92283, 92284, 92286, 92287, 92288, 92289, 92290, 92291, 92293, 92294, 92295, 92296, 92297, 92298, 92299, 92300, 92301, 92302, 92303, 92304, 92305, 92306, 92307, 92308, 92309, 92310, 92311, 92312, 92313, 92316, 92317, 92318, 92319, 92320, 92321, 92322, 92323, 92324, 92325, 92326, 92327, 92329, 92330, 92332, 92333, 92334, 92335, 92336, 92337, 92338, 92339, 92340, 92341, 92342, 92343, 92344, 92345, 92346, 92350, 92351, 92352, 92353, 92354, 92355, 92356, 92357, 92358, 92359, 92360, 92361, 92362, 92363, 92364, 92365, 92366, 92367, 92368, 92369, 92370, 92371, 92372, 92373, 92374, 92375, 92376, 92378, 92379, 92380, 92381, 92382, 92383, 92384, 92385, 92387, 92388, 92389, 92390, 92391, 92392, 92393, 92394, 92395, 92396, 92397, 92398, 92399, 92400, 92401, 92402, 92403, 92404, 92405, 92406, 92407, 92408, 92410, 92411, 92412, 92413, 92414, 92415, 92416, 92417, 92418, 92419, 92420, 92421, 92422, 92424, 92425, 92426, 92427, 92428, 92429, 92430, 92431, 92432, 92433, 92434, 92435, 92436, 92437, 92438, 92440, 92441, 92442, 92443, 92444, 92445]}
def sector_id(key):
    if key not in KEYS:raise ValueError('vevey_unreviewed_category')
    return str(uuid.uuid5(uuid.NAMESPACE_URL,DOC+'#native-historic-v1#'+key))
def validate(b):
    if (b['document_id'],b['source_sha256'])!=(DOC,SHA):raise ValueError('vevey_source')
    if b['source_plan_status']!='approved' or b['source_scope_communes']!=[5890] or b['source_precision_m'] is not None or b['publication_status']!='internal_review_only' or b['review_status']!='review_required':raise ValueError('vevey_private_scope')
    sem=b['literal_semantic_review'];decision='accepted_private_native_cartographic_supports_only'
    if sem['geographic_acceptance']!=decision or b['independent_geographic_decision']!=decision or sem['qualified_release_authorized'] or sem['commune_complete'] or sem['native_style_mapping_pending'] or sem['deliveredclass_keys']!=['historic_dense'] or sem['held_ids']!=[92186, 92187, 92208, 92247, 92264, 92285, 92292, 92314, 92315, 92328, 92331, 92348, 92386] or sem['legend_excluded_ids']!=[96146]:raise ValueError('vevey_semantic_scope')
    ledger=b['source_native_proof']['candidates']
    if [sum(p['disposition']==status for p in ledger) for status in ['eligible','whole_support_hold','native_topology_hold','transformed_union_topology_hold']]!=[367, 13, 12, 3]:raise ValueError('vevey_native_partition')
    if sem['transformed_union_topology_hold_ids']!=[92207,92347,92377] or not sem['semantic_mapping']['not_exact_rgb_match'] or not sem['semantic_mapping']['parent_visual_concurrence'] or sem['semantic_mapping']['parent_visual_concurrence_pending']:raise ValueError('vevey_historic_manual_mapping')
    if b['curve_tolerance_pdfpoint']!=.003 or not b['curve_convergence']['same_pair_membership']:raise ValueError('vevey_curve_contract')
    if len(b['features'])!=1 or {f['key'] for f in b['features']}!=KEYS:raise ValueError('vevey_categories')
    cc=b['frozen_controls'];train=[c for c in cc['controls'] if c['role']=='train'];held=[c for c in cc['controls'] if c['role']=='held']
    if [c['source_ordinary_index'] for c in train]!=[1388,2354,2931,3927,2047,4281] or [c['source_ordinary_index'] for c in held]!=[1705,4065,4821,2143,865]:raise ValueError('vevey_frozen_partition')
    hull=MultiPoint([c['source_centroid'] for c in train]).convex_hull;fit=b['fit'];m=fit['matrix'];o=fit['offset'];matrix=[m[0][0],m[0][1],m[1][0],m[1][1],*o];res=[]
    for c in cc['controls']:
        if math.dist(shape(c['source_geometry']).centroid.coords[0],c['source_centroid'])>1e-10 or math.dist(shape(c['reference_geometry']).centroid.coords[0],c['ground_centroid'])>1e-8:raise ValueError('vevey_full_control_geometry')
        x,y=c['source_centroid'];err=math.dist([m[0][0]*x+m[0][1]*y+o[0],m[1][0]*x+m[1][1]*y+o[1]],c['ground_centroid'])
        if c['role']=='held':res.append(err)
    if max(res)>=5 or abs(math.sqrt(sum(x*x for x in res)/5)-b['alignment']['holdout_rmse_m'])>1e-8:raise ValueError('vevey_digital_base_diagnostic')
    needed_clips={i for f in b['features'] for p in f['source_paths'] for i in p['clip_identity']['active_clips']};clips={x['index']:closed_polygon(x['items']) for x in b['source_native_proof']['clips'] if x['index'] in needed_clips};commune=shape(b['commune_reference']['geometry'])
    for f in b['features']:
        if [x['index'] for x in f['source_paths']]!=PATHS[f['key']] or f['page']!=67 or f['invalid_reference_envelope_overlap']:raise ValueError('vevey_native_scope')
        gs=[]
        for p in f['source_paths']:
            native=closed_polygon(p['native_items'],tolerance=.003);original=shape(p['geometry'])
            if p['fill']!=[.47057297825813293,.43956664204597473,.42153048515319824] or p['color'] is not None:raise ValueError('vevey_historic_native_style')
            if not native.equals_exact(original,1e-10) or not hull.covers(original) or not commune.covers(affine_transform(original,matrix)):raise ValueError('vevey_whole_original_source')
            visible=original
            for idx in p['clip_identity']['active_clips']:visible=visible.intersection(clips[idx])
            if abs(original.difference(visible).area-p['source_clip_removed_area_pdfpoint2'])>1e-10:raise ValueError('vevey_historic_clip_accounting')
            if not visible.equals_exact(shape(p['source_visible_geometry']),1e-10):raise ValueError('vevey_exact_source_clip')
            g=affine_transform(visible,matrix)
            if not g.equals_exact(shape(p['ground_geometry']),1e-8) or not commune.covers(g):raise ValueError('vevey_transform_territory')
            gs.append(g)
        g=unary_union(gs)
        if not g.is_valid or not g.equals_exact(shape(f['geometry_lv95']),1e-8):raise ValueError('vevey_group_geometry')
        if len({r['egrid'] for r in f['expected_pairs']})!=len(f['expected_pairs']):raise ValueError('vevey_duplicate_reference')
        for r in f['expected_pairs']:
            at=r['attributes'];q=shape(r['reference_geometry'])
            if at['EGRID']!=r['egrid'] or at['NO_COM_FED']!=5890 or at['GENRE_TXT'] not in KINDS or not q.is_valid or g.intersection(q).area<=0:raise ValueError('vevey_reference')
    if {f['key']:len(f['expected_pairs']) for f in b['features']}!={'historic_dense':369}:raise ValueError('vevey_pair_inventory')
    return b
def load_artifacts():
    raw=(Path(__file__).parent/'reports/vevey-historic/batch.json').read_bytes()
    if hashlib.sha256(raw).hexdigest()!=BATCH_SHA:raise ValueError('vevey_changed_batch_requires_review')
    return validate(json.loads(raw))
def persist(conn,document_id,sha):
    if str(document_id)!=DOC or sha!=SHA:return None
    with conn.cursor() as c:
        c.execute('SELECT sha256,page_count,plan_status FROM bronze_ch.vd_pdcom_documents WHERE id=%s',(DOC,));d=c.fetchone()
        if not d or d[0]!=SHA or not d[1] or d[1]!=152 or d[2]!='approved':raise ValueError('vevey_historic_stored_source')
    b=load_artifacts();refs=[refresh(conn,DOC,r['commune_bfs'],r['bounds'],context_declaration=None) for r in b['references']];results=[]
    with conn.cursor() as c:
        c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_references a JOIN bronze_ch.vd_pdcom_parcel_references z USING(egrid) WHERE a.snapshot_id=ANY(%s::uuid[]) AND z.snapshot_id=ANY(%s::uuid[]) AND a.snapshot_id<z.snapshot_id AND (NOT ST_Equals(a.geom,z.geom) OR a.commune_bfs IS DISTINCT FROM z.commune_bfs OR a.official_attributes IS DISTINCT FROM z.official_attributes)''',([x['snapshot_id'] for x in refs],[x['snapshot_id'] for x in refs]))
        if c.fetchone()[0]:raise ValueError('vevey_historic_conflicting_tile_reference')
    for f in b['features']:
        sid=sector_id(f['key']);geom=json.dumps(f['geometry_lv95']);label='Vevey — support cartographique partiel — '+f['label']
        frozen=[{'egrid':x['egrid'],'kind':x['attributes']['GENRE_TXT'],'geometry':x['reference_geometry']} for x in f['expected_pairs']]
        evidence={'source_sha256':SHA,'source_path_ids':f['path_ids'],'native_source_paths':f['source_paths'],'native_pdf_clips':b['source_native_proof']['clips'],'source_transform':b['fit'],'source_category':f['category'],'page_number':67,'grouping':'Partial historic thematic category of367 original supports; contextual parcels only, no legal designation, development entitlement or capacity. Native PDF clips and .003PDFpoint linearization.','semantics':b['semantics'],'reservation':b['reservation'],'source_plan_status':'approved','alignment':b['alignment'],'literal_semantics':b['literal_semantic_review'],'independent_geographic_decision':b['independent_geographic_decision'],'source_precision':'unknown','source_precision_m':None,'boundary_buffer':'10m review heuristic, not measured accuracy','review_status':'review_required','publication_status':'internal_review_only','source_scope_communes':[5890],'reference_scope':'Vevey current official cadastral objects, contextual intersections only','native_topology_hold_ids':[92066, 92112, 92150, 92170, 92205, 92212, 92222, 92266, 92349, 92409, 92423, 92439],'transformed_union_topology_hold_ids':b['literal_semantic_review']['transformed_union_topology_hold_ids'],'manual_semantic_mapping':b['literal_semantic_review'],'held_source_path_ids':[92186, 92187, 92208, 92247, 92264, 92285, 92292, 92314, 92315, 92328, 92331, 92348, 92386],'legend_excluded_ids':[96146],'official_references':refs,'batch_sha256':BATCH_SHA,'artifact':'pipelines/vd-pdcom/reports/vevey-historic','ground_checks':'Six frozen independently identified original building footprints train; five reserved complete footprints. Millimetric residuals describe digital base-coordinate consistency, not thematic source precision.'}
        with conn,conn.cursor() as c:
            c.execute("SET LOCAL statement_timeout='90s'");c.execute('SELECT pg_advisory_xact_lock(589000767)')
            c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_references WHERE snapshot_id=ANY(%s::uuid[]) AND geom && ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) AND NOT ST_IsValid(geom)''',([x['snapshot_id'] for x in refs],geom))
            if c.fetchone()[0]:raise ValueError('vevey_historic_invalid_current_reference')
            c.execute('DROP TABLE IF EXISTS pg_temp.vevey_historic_pairs')
            c.execute('''CREATE TEMP TABLE vevey_historic_pairs ON COMMIT DROP AS WITH g AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) geom),r AS MATERIALIZED (SELECT DISTINCT ON(egrid) * FROM bronze_ch.vd_pdcom_parcel_references WHERE snapshot_id=ANY(%s::uuid[]) AND ST_IsValid(geom) ORDER BY egrid,snapshot_id)
            SELECT r.*,ST_Intersection(r.geom,g.geom) overlap,ST_Area(r.geom) area,ST_DWithin(r.geom,ST_Boundary(g.geom),10) edge,CASE r.official_attributes->>'GENRE_TXT' WHEN 'DDP superficie' THEN 'ddp_superficie' WHEN 'DDP source' THEN 'ddp_source' WHEN 'parcelle privée' THEN 'bien_fonds' WHEN 'DP communal' THEN 'bien_fonds' WHEN 'DP cantonal' THEN 'bien_fonds' ELSE 'unknown' END object_kind FROM r,g WHERE ST_Intersects(r.geom,g.geom) AND ST_Area(ST_Intersection(r.geom,g.geom))>0''',(geom,[x['snapshot_id'] for x in refs]))
            c.execute('SELECT egrid FROM vevey_historic_pairs');keys={x[0] for x in c.fetchall()}
            if keys!={x['egrid'] for x in frozen}:raise ValueError('vevey_historic_current_membership_changed')
            c.execute('''WITH f AS (SELECT x->>'egrid' egrid,x->>'kind' kind,ST_SetSRID(ST_GeomFromGeoJSON(x->'geometry'),2056) geom FROM jsonb_array_elements(%s::jsonb) x) SELECT count(*) FROM vevey_historic_pairs n LEFT JOIN f USING(egrid) WHERE f.egrid IS NULL OR NOT ST_Equals(n.geom,f.geom) OR (n.official_attributes->>'GENRE_TXT') IS DISTINCT FROM f.kind''',(Json(frozen),))
            if c.fetchone()[0]:raise ValueError('vevey_historic_current_geometry_changed')
            c.execute('SELECT ST_IsValid(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)),(SELECT bool_and(ST_IsValid(ST_Transform(overlap,4326))) FROM vevey_historic_pairs)',(geom,))
            if c.fetchone()!=(True,True):raise ValueError('vevey_historic_transformed_geometry_invalid')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_sectors(id,document_id,page_number,label,geom,alignment_rmse_m,validation_evidence,review_status) VALUES(%s,%s,%s,%s,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326),%s,%s,'review_required') ON CONFLICT DO NOTHING''',(sid,DOC,f['page'],label,geom,b['alignment']['holdout_rmse_m'],Json(evidence)))
            c.execute("SELECT label,review_status,source_precision_m,validated_by,ST_Equals(geom,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)) FROM bronze_ch.vd_pdcom_sectors WHERE id=%s",(geom,sid))
            if c.fetchone()!=(label,'review_required',None,None,True):raise ValueError('vevey_historic_existing_sector_changed')
            c.execute('SELECT commune_bfs FROM gold_ch.v_vd_pdcom_review_sectors WHERE id=%s',(sid,))
            if c.fetchone()[0]!=ATTRIBUTION[f['key']]:raise ValueError('vevey_historic_attribution')
            c.execute('SELECT egrid FROM bronze_ch.vd_pdcom_parcel_candidates WHERE sector_id=%s',(sid,));old={x[0] for x in c.fetchall()}
            if old and old!=keys:raise ValueError('vevey_historic_existing_membership')
            c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_candidates p JOIN vevey_historic_pairs n USING(egrid) WHERE p.sector_id=%s AND (NOT ST_Equals(p.geom,ST_Transform(n.overlap,4326)) OR p.overlap_m2 IS DISTINCT FROM ST_Area(n.overlap) OR p.parcel_area_m2 IS DISTINCT FROM n.area OR p.overlap_fraction IS DISTINCT FROM LEAST(1,ST_Area(n.overlap)/n.area) OR p.boundary_review_required IS DISTINCT FROM n.edge OR p.uncertainty_buffer_m IS DISTINCT FROM 10 OR p.cadastral_object_kind IS DISTINCT FROM n.object_kind)''',(sid,))
            if c.fetchone()[0]:raise ValueError('vevey_historic_existing_values')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_parcel_candidates(sector_id,egrid,overlap_m2,parcel_area_m2,overlap_fraction,boundary_review_required,uncertainty_buffer_m,geom,cadastral_object_kind,cadastral_type_evidence) SELECT %s,n.egrid,ST_Area(n.overlap),n.area,LEAST(1,ST_Area(n.overlap)/n.area),n.edge,10,ST_Transform(n.overlap,4326),n.object_kind,jsonb_build_object('source_url',s.query_url,'response_sha256',s.response_sha256,'reference_snapshot_ids',jsonb_build_array(n.snapshot_id),'official_attributes',n.official_attributes,'status','matched') FROM vevey_historic_pairs n JOIN bronze_ch.vd_pdcom_parcel_reference_snapshots s ON s.id=n.snapshot_id ON CONFLICT DO NOTHING''',(sid,))
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_candidate_parcel_references(sector_id,egrid,snapshot_id) SELECT %s,egrid,snapshot_id FROM vevey_historic_pairs ON CONFLICT(sector_id,egrid) DO UPDATE SET snapshot_id=EXCLUDED.snapshot_id''',(sid,))
            c.execute('UPDATE bronze_ch.vd_pdcom_sectors SET validation_evidence=%s WHERE id=%s',(Json(evidence),sid))
        results.append({'key':f['key'],'sector_id':sid,'parcel_pairs':len(keys)})
    return {'collections':len(results),'parcel_pairs':sum(r['parcel_pairs'] for r in results),'candidates':results,'publication_status':'internal_review_only','review_status':'review_required'}
