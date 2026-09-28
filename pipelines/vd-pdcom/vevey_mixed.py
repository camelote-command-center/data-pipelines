"""Exact approved Vevey mixed map supports; private review only."""
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
BATCH_SHA='be6b4d78a63b02acbb5f0be8e5cb92fedb8ff7d31a5b3fcb2b13768470cfef11'
KEYS={'mixed_program'}
ATTRIBUTION={k:[5890] for k in KEYS}
PATHS={'mixed_program':[94294, 94301, 94308, 94315, 94340, 94353, 94360, 94367, 94381, 94406, 94419, 94426, 94433, 94440, 94447, 94460, 94467, 94474, 94481, 94488, 94495, 94502, 94509, 94516, 94523, 94530, 94537, 94551, 94558, 94571, 94578, 94585, 94592, 94599, 94606, 94613, 94620, 94627, 94640, 94647, 94654, 94679, 94686, 94693, 94700, 94725, 94732, 94739, 94746, 94759, 94766, 94791, 94805, 94812, 94819, 94844, 94851, 94858, 94865, 94872, 94879, 94886, 94893, 94900, 94907, 94914, 94939, 94952, 94959, 94984, 94998, 95023, 95030, 95037, 95044, 95069, 95076, 95083, 95090, 95115, 95122, 95135, 95142, 95167, 95224, 95237, 95244, 95251, 95258, 95265, 95290, 95315, 95322, 95335, 95342, 95367, 95374, 95381, 95388, 95414, 95421, 95471, 95478, 95485, 95505, 95512, 95519, 95526, 95533, 95540, 95553, 95560, 95567, 95599, 95624, 95637, 95662, 95669, 95676, 95683, 95696, 95709, 95734, 95747, 95754, 95767, 95780, 95787, 95812, 95819, 95844, 95851, 95864, 95871, 95878, 95885, 95892, 95906, 95913, 95920, 95927, 95934, 95947, 95954, 95979, 95986, 96011, 96018, 96043, 96056, 96069, 96082, 96089, 96096, 96110, 96117, 96130, 96137, 96162, 96187, 96212, 96219, 96244, 96251, 96258, 96265, 96290, 96297, 96304, 96317, 96324, 96362, 96375, 96382, 96389, 96402, 96427, 96441, 96466, 96473, 96498, 96505, 96530, 96537, 96544, 96551, 96558, 96565, 96572, 96579, 96586, 96593, 96606, 96613, 96638, 96645, 96652, 96679, 96692, 96705, 96712, 96725, 96732, 96745, 96752, 96759, 96766, 96773, 96786, 96831, 96838, 96851, 96876, 96883, 96890, 96915, 96922, 96929, 96936, 96943, 96950, 96957, 96970, 96977, 96984, 97011, 97018, 97025, 97038, 97045, 97052, 97065, 97090, 97115, 97122, 97129, 97142, 97149, 97162, 97169, 97176, 97183, 97190, 97197, 97204, 97217, 97230, 97243, 97256, 97263, 97270, 97295, 97308, 97333, 97340, 97353, 97366, 97379, 97386, 97393, 97407, 97414, 97439, 97464, 97471, 97478, 97491, 97498, 97511, 97524, 97531, 97538, 97545, 97552, 97577, 97602, 97609, 97634, 97647, 97654, 97661, 97675, 97682, 97689, 97714, 97721, 97734, 97759, 97784, 97791, 97798, 97812, 97819, 97826, 97833, 97840, 97847, 97861, 97868, 97875, 97882, 97889, 97896, 97903, 97910, 97917, 97924, 97931, 97938, 97945, 97952, 97987, 97994, 98001, 98008, 98015, 98022, 98029, 98064, 98099, 98106, 98113, 98120, 98127, 98169, 98176, 98183, 98190, 98197, 98204, 98211, 98218, 98225]}
def sector_id(key):
    if key not in KEYS:raise ValueError('vevey_unreviewed_category')
    return str(uuid.uuid5(uuid.NAMESPACE_URL,DOC+'#native-mixed-v1#'+key))
def validate(b):
    if (b['document_id'],b['source_sha256'])!=(DOC,SHA):raise ValueError('vevey_source')
    if b['source_plan_status']!='approved' or b['source_scope_communes']!=[5890] or b['source_precision_m'] is not None or b['publication_status']!='internal_review_only' or b['review_status']!='review_required':raise ValueError('vevey_private_scope')
    sem=b['literal_semantic_review'];decision='accepted_private_native_cartographic_supports_only'
    if sem['geographic_acceptance']!=decision or b['independent_geographic_decision']!=decision or sem['qualified_release_authorized'] or sem['commune_complete'] or sem['native_style_mapping_pending'] or sem['deliveredclass_keys']!=['mixed_program'] or sem['held_ids']!=[95192, 95199, 95446, 95899, 96349, 96434, 96659, 96672, 96799, 96824, 96997, 97400, 97854] or sem['legend_excluded_ids']!=[98845]:raise ValueError('vevey_semantic_scope')
    ledger=b['source_native_proof']['candidates']
    if [sum(p['disposition']==status for p in ledger) for status in ['eligible','whole_support_hold','native_topology_hold','transformed_union_topology_hold']]!=[333, 13, 28, 0]:raise ValueError('vevey_native_partition')
    if sem['ledger_final_ancestry_pin_pending'] or not sem['full_native_paint_ancestry_preserved'] or b['source_native_proof']['native_pattern_resources']['interpretation_pending'] or len(b['source_native_proof']['other_same_color_operators'])!=40:raise ValueError('vevey_mixed_pattern_ancestry')
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
            proof=p['raw_pattern_fill_proof'];components=p['full_pattern_native_components']
            if not proof['matched_raw_fill'] or proof['pattern'] not in ['P2','P3','P4','P5','P6','P7','P8','P9'] or not proof['raw_fill_statement'].endswith('h\nf\nQ') or not p['pattern_class_review']['full_pattern_present']:raise ValueError('vevey_mixed_native_pattern_fill')
            if not any(x.get('fill')==[.9343404173851013,.19400320947170258,.17949187755584717] for x in components) or not any(x.get('fill')==[.5405203104019165,.2225680947303772,.5856565237045288] for x in components):raise ValueError('vevey_mixed_composite_pattern')
            if not native.equals_exact(original,1e-10) or not hull.covers(original) or not commune.covers(affine_transform(original,matrix)):raise ValueError('vevey_whole_original_source')
            visible=original
            for idx in p['clip_identity']['active_clips']:visible=visible.intersection(clips[idx])
            if abs(original.difference(visible).area-p['source_clip_removed_area_pdfpoint2'])>1e-10:raise ValueError('vevey_mixed_clip_accounting')
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
    if {f['key']:len(f['expected_pairs']) for f in b['features']}!={'mixed_program':508}:raise ValueError('vevey_pair_inventory')
    return b
def load_artifacts():
    raw=(Path(__file__).parent/'reports/vevey-mixed/batch.json').read_bytes()
    if hashlib.sha256(raw).hexdigest()!=BATCH_SHA:raise ValueError('vevey_changed_batch_requires_review')
    return validate(json.loads(raw))
def persist(conn,document_id,sha):
    if str(document_id)!=DOC or sha!=SHA:return None
    with conn.cursor() as c:
        c.execute('SELECT sha256,page_count,plan_status FROM bronze_ch.vd_pdcom_documents WHERE id=%s',(DOC,));d=c.fetchone()
        if not d or d[0]!=SHA or not d[1] or d[1]!=152 or d[2]!='approved':raise ValueError('vevey_mixed_stored_source')
    b=load_artifacts();refs=[refresh(conn,DOC,r['commune_bfs'],r['bounds'],context_declaration=None) for r in b['references']];results=[]
    with conn.cursor() as c:
        c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_references a JOIN bronze_ch.vd_pdcom_parcel_references z USING(egrid) WHERE a.snapshot_id=ANY(%s::uuid[]) AND z.snapshot_id=ANY(%s::uuid[]) AND a.snapshot_id<z.snapshot_id AND (NOT ST_Equals(a.geom,z.geom) OR a.commune_bfs IS DISTINCT FROM z.commune_bfs OR a.official_attributes IS DISTINCT FROM z.official_attributes)''',([x['snapshot_id'] for x in refs],[x['snapshot_id'] for x in refs]))
        if c.fetchone()[0]:raise ValueError('vevey_mixed_conflicting_tile_reference')
    for f in b['features']:
        sid=sector_id(f['key']);geom=json.dumps(f['geometry_lv95']);label='Vevey — support cartographique partiel — '+f['label']
        frozen=[{'egrid':x['egrid'],'kind':x['attributes']['GENRE_TXT'],'geometry':x['reference_geometry']} for x in f['expected_pairs']]
        evidence={'source_sha256':SHA,'source_path_ids':f['path_ids'],'native_source_paths':f['source_paths'],'native_pdf_clips':b['source_native_proof']['clips'],'source_transform':b['fit'],'native_pattern_resources':b['source_native_proof']['native_pattern_resources'],'excluded_other_same_colour_operators':[p['ordinary_index'] for p in b['source_native_proof']['other_same_color_operators']],'source_category':f['category'],'page_number':67,'grouping':'Partial mixed thematic category of333 original supports; contextual parcels only, no legal designation, development entitlement or capacity. Native PDF clips and .003PDFpoint linearization.','semantics':b['semantics'],'reservation':b['reservation'],'source_plan_status':'approved','alignment':b['alignment'],'literal_semantics':b['literal_semantic_review'],'independent_geographic_decision':b['independent_geographic_decision'],'source_precision':'unknown','source_precision_m':None,'boundary_buffer':'10m review heuristic, not measured accuracy','review_status':'review_required','publication_status':'internal_review_only','source_scope_communes':[5890],'reference_scope':'Vevey current official cadastral objects, contextual intersections only','native_topology_hold_ids':[94374, 94544, 94798, 94991, 95401, 95498, 95592, 96103, 97004, 97668, 97805, 97959, 97966, 97973, 97980, 98036, 98043, 98050, 98057, 98071, 98078, 98085, 98092, 98134, 98141, 98148, 98155, 98162],'transformed_union_topology_hold_ids':b['literal_semantic_review']['transformed_union_topology_hold_ids'],'manual_semantic_mapping':b['literal_semantic_review'],'held_source_path_ids':[95192, 95199, 95446, 95899, 96349, 96434, 96659, 96672, 96799, 96824, 96997, 97400, 97854],'legend_excluded_ids':[98845],'official_references':refs,'batch_sha256':BATCH_SHA,'artifact':'pipelines/vd-pdcom/reports/vevey-mixed','ground_checks':'Six frozen independently identified original building footprints train; five reserved complete footprints. Millimetric residuals describe digital base-coordinate consistency, not thematic source precision.'}
        with conn,conn.cursor() as c:
            c.execute("SET LOCAL statement_timeout='90s'");c.execute('SELECT pg_advisory_xact_lock(589000867)')
            c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_references WHERE snapshot_id=ANY(%s::uuid[]) AND geom && ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) AND NOT ST_IsValid(geom)''',([x['snapshot_id'] for x in refs],geom))
            if c.fetchone()[0]:raise ValueError('vevey_mixed_invalid_current_reference')
            c.execute('DROP TABLE IF EXISTS pg_temp.vevey_mixed_pairs')
            c.execute('''CREATE TEMP TABLE vevey_mixed_pairs ON COMMIT DROP AS WITH g AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) geom),r AS MATERIALIZED (SELECT DISTINCT ON(egrid) * FROM bronze_ch.vd_pdcom_parcel_references WHERE snapshot_id=ANY(%s::uuid[]) AND ST_IsValid(geom) ORDER BY egrid,snapshot_id)
            SELECT r.*,ST_Intersection(r.geom,g.geom) overlap,ST_Area(r.geom) area,ST_DWithin(r.geom,ST_Boundary(g.geom),10) edge,CASE r.official_attributes->>'GENRE_TXT' WHEN 'DDP superficie' THEN 'ddp_superficie' WHEN 'DDP source' THEN 'ddp_source' WHEN 'parcelle privée' THEN 'bien_fonds' WHEN 'DP communal' THEN 'bien_fonds' WHEN 'DP cantonal' THEN 'bien_fonds' ELSE 'unknown' END object_kind FROM r,g WHERE ST_Intersects(r.geom,g.geom) AND ST_Area(ST_Intersection(r.geom,g.geom))>0''',(geom,[x['snapshot_id'] for x in refs]))
            c.execute('SELECT egrid FROM vevey_mixed_pairs');keys={x[0] for x in c.fetchall()}
            if keys!={x['egrid'] for x in frozen}:raise ValueError('vevey_mixed_current_membership_changed')
            c.execute('''WITH f AS (SELECT x->>'egrid' egrid,x->>'kind' kind,ST_SetSRID(ST_GeomFromGeoJSON(x->'geometry'),2056) geom FROM jsonb_array_elements(%s::jsonb) x) SELECT count(*) FROM vevey_mixed_pairs n LEFT JOIN f USING(egrid) WHERE f.egrid IS NULL OR NOT ST_Equals(n.geom,f.geom) OR (n.official_attributes->>'GENRE_TXT') IS DISTINCT FROM f.kind''',(Json(frozen),))
            if c.fetchone()[0]:raise ValueError('vevey_mixed_current_geometry_changed')
            c.execute('SELECT ST_IsValid(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)),(SELECT bool_and(ST_IsValid(ST_Transform(overlap,4326))) FROM vevey_mixed_pairs)',(geom,))
            if c.fetchone()!=(True,True):raise ValueError('vevey_mixed_transformed_geometry_invalid')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_sectors(id,document_id,page_number,label,geom,alignment_rmse_m,validation_evidence,review_status) VALUES(%s,%s,%s,%s,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326),%s,%s,'review_required') ON CONFLICT DO NOTHING''',(sid,DOC,f['page'],label,geom,b['alignment']['holdout_rmse_m'],Json(evidence)))
            c.execute("SELECT label,review_status,source_precision_m,validated_by,ST_Equals(geom,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)) FROM bronze_ch.vd_pdcom_sectors WHERE id=%s",(geom,sid))
            if c.fetchone()!=(label,'review_required',None,None,True):raise ValueError('vevey_mixed_existing_sector_changed')
            c.execute('SELECT commune_bfs FROM gold_ch.v_vd_pdcom_review_sectors WHERE id=%s',(sid,))
            if c.fetchone()[0]!=ATTRIBUTION[f['key']]:raise ValueError('vevey_mixed_attribution')
            c.execute('SELECT egrid FROM bronze_ch.vd_pdcom_parcel_candidates WHERE sector_id=%s',(sid,));old={x[0] for x in c.fetchall()}
            if old and old!=keys:raise ValueError('vevey_mixed_existing_membership')
            c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_candidates p JOIN vevey_mixed_pairs n USING(egrid) WHERE p.sector_id=%s AND (NOT ST_Equals(p.geom,ST_Transform(n.overlap,4326)) OR p.overlap_m2 IS DISTINCT FROM ST_Area(n.overlap) OR p.parcel_area_m2 IS DISTINCT FROM n.area OR p.overlap_fraction IS DISTINCT FROM LEAST(1,ST_Area(n.overlap)/n.area) OR p.boundary_review_required IS DISTINCT FROM n.edge OR p.uncertainty_buffer_m IS DISTINCT FROM 10 OR p.cadastral_object_kind IS DISTINCT FROM n.object_kind)''',(sid,))
            if c.fetchone()[0]:raise ValueError('vevey_mixed_existing_values')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_parcel_candidates(sector_id,egrid,overlap_m2,parcel_area_m2,overlap_fraction,boundary_review_required,uncertainty_buffer_m,geom,cadastral_object_kind,cadastral_type_evidence) SELECT %s,n.egrid,ST_Area(n.overlap),n.area,LEAST(1,ST_Area(n.overlap)/n.area),n.edge,10,ST_Transform(n.overlap,4326),n.object_kind,jsonb_build_object('source_url',s.query_url,'response_sha256',s.response_sha256,'reference_snapshot_ids',jsonb_build_array(n.snapshot_id),'official_attributes',n.official_attributes,'status','matched') FROM vevey_mixed_pairs n JOIN bronze_ch.vd_pdcom_parcel_reference_snapshots s ON s.id=n.snapshot_id ON CONFLICT DO NOTHING''',(sid,))
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_candidate_parcel_references(sector_id,egrid,snapshot_id) SELECT %s,egrid,snapshot_id FROM vevey_mixed_pairs ON CONFLICT(sector_id,egrid) DO UPDATE SET snapshot_id=EXCLUDED.snapshot_id''',(sid,))
            c.execute('UPDATE bronze_ch.vd_pdcom_sectors SET validation_evidence=%s WHERE id=%s',(Json(evidence),sid))
        results.append({'key':f['key'],'sector_id':sid,'parcel_pairs':len(keys)})
    return {'collections':len(results),'parcel_pairs':sum(r['parcel_pairs'] for r in results),'candidates':results,'publication_status':'internal_review_only','review_status':'review_required'}
