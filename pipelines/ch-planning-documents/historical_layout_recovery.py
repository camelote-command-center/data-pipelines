"""Exact reviewed partial-box append; original PDF versions and old chunks immutable."""
import copy
import hashlib
import json
from pathlib import Path
from psycopg2 import sql
from psycopg2.extras import Json
import historical_reference as historical
import vd_pdcom_text as bridge

ROOT = Path(__file__).parent / 'reports' / 'historical-layout-recovery'
SOURCE_IDS = ('2bcdca02-b3cb-5846-a712-3f538a524557', '896b38ec-fef9-5f88-9ccd-345d17adf737', 'ec1b494f-f6d2-5ed6-8fb6-00bc3f732d28', 'bbdb0fe7-3d42-56dd-9008-c30918c5bd57')
MANIFEST_SHA256 = '2b266d5b8b9963e8c9d90e97045dbe533443d38ed3c1b0f12680fc10cf4adc5d'
FIELDS = ('id', 'document_id', 'chunk_index', 'page_number', 'content', 'metadata')
require = bridge.require
digest = bridge.digest


def load():
    raw = (ROOT / 'manifest.json').read_bytes()
    require(MANIFEST_SHA256 and hashlib.sha256(raw).hexdigest() == MANIFEST_SHA256, 'layout_manifest_not_exact_accepted')
    manifest = json.loads(raw)
    proof = (ROOT / 'independent-content-review.json').read_bytes()
    require(hashlib.sha256(proof).hexdigest() == manifest['independent_review_sha256'], 'layout_independent_review_changed')
    return manifest


def base_bundle(source_id):
    manifest = load()
    pin = manifest['sources'].get(source_id)
    require(pin is not None, 'layout_source_not_reviewed')
    raw = (ROOT.parent / pin['base_directory'] / (pin['slug'] + '-bundle.json')).read_bytes()
    require(hashlib.sha256(raw).hexdigest() == pin['base_bundle_sha256'], 'layout_base_bundle_changed')
    base = json.loads(raw)
    historical.validate_delivery(base['metadata'], base['chunks'])
    require(base['review']['vd_document_id'] == source_id and base['document_id'] == pin['document_id'], 'layout_base_identity_changed')
    return base


def assemble(source_id):
    manifest = load(); base = base_bundle(source_id); pin = manifest['sources'][source_id]
    blocks = [b for b in manifest['blocks'] if b['source_document_id'] == source_id]
    require(bool(blocks), 'layout_no_blocks')
    seen = set(); chunks = []
    for b in blocks:
        require(b['block_id'] not in seen, 'layout_duplicate_block'); seen.add(b['block_id'])
        require(b['source_sha256'] == base['review']['source_sha256'], 'layout_pdf_changed')
        require(b['physical_page'] not in base['review']['historical_reference']['selected_pages'], 'layout_base_page_duplicate')
        require(hashlib.sha256(b['text'].encode()).hexdigest() == b['text_sha256'], 'layout_raw_text_changed')
        x0,y0,x1,y1 = b['bbox_pdf_points']; width,height = b['page_dimensions']
        require(0 <= x0 < x1 <= width and 0 <= y0 < y1 <= height and b['page_rotation'] == (270 if source_id == '896b38ec-fef9-5f88-9ccd-345d17adf737' else 0), 'layout_invalid_crop')
        require(b['confidence'] is None and b['confidence_status'] == 'not_recorded' and b['whole_page_certified'] is False, 'layout_confidence_or_completion')
        require(b['limits'] and b['omitted_region_policy'] and b['reading_order_within_page'] > 0, 'layout_scope_and_order_required')
        for part, start in enumerate(range(0, len(b['text']), 2000)):
            raw = b['text'][start:start+2000]
            caveat = base['review']['historical_reference']['human_readable_caveat'] + '\nPARTIAL SOURCE BOX ' + b['block_id'] + '. Only this coordinate-bound prose is represented; all omitted maps, diagrams, tables and page regions remain held. Raw OCR is uncorrected; no current legal, numeric-rule or parcel-right inference.'
            caveat += ('\n' + b['context_caveat']) if b['context_caveat'] else ''
            metadata = {k:copy.deepcopy(v) for k,v in base['chunks'][0]['metadata'].items() if k not in ('page_provenance','citation','representation','extraction_status')}
            metadata.update(citation=base['review']['source_url']+'#page='+str(b['physical_page']), representation=b['representation'], extraction_status='reviewed_partial_source_box', historical_layout_recovery_revision=MANIFEST_SHA256,
                            block_provenance={k:v for k,v in b.items() if k not in ('text','original_cropped_unsorted_text')}, limits=b['limits'], whole_page_certified=False)
            chunks.append({'id':bridge.uid(base['document_id']+':layout-recovery:'+MANIFEST_SHA256+':'+b['block_id']+':'+str(part)), 'document_id':base['document_id'], 'chunk_index':len(base['chunks'])+len(chunks), 'page_number':b['physical_page'], 'content':caveat+'\n\n'+raw, 'metadata':metadata})
    require(len(chunks)==pin['new_chunks'], 'layout_chunk_count_changed')
    metadata = copy.deepcopy(base['metadata'])
    metadata['historical_layout_recovery'] = {'revision':MANIFEST_SHA256,'base_metadata_sha256':digest(base['metadata']),'base_chunk_count':len(base['chunks']),'additional_chunks':len(chunks),'independent_review_sha256':manifest['independent_review_sha256'],
                                 'base_page_manifest_scope':'Immutable base extraction only; its held pages are not complete search-coverage claims after this partial-box append.',
                                 'partial_page_blocks':[{k:v for k,v in b.items() if k not in ('text','original_cropped_unsorted_text')} for b in blocks],
                                 'all_outside_boxes_held':True,'whole_pages_certified':0,'source_pdf_version_unchanged':True,'current_applicability':'unverified'}
    return {'base':base,'review':base['review'],'document_id':base['document_id'],'source_id':base['source_id'],'version_id':base['version_id'],'metadata':metadata,'new_chunks':chunks,'chunks':base['chunks']+chunks,'blocks':blocks}


def validate_delivery(metadata, chunks):
    source_id = metadata.get('vd_document_id')
    expected = assemble(source_id)
    require(metadata == expected['metadata'], 'layout_document_metadata_changed')
    require(sorted(chunks,key=lambda x:x['chunk_index']) == expected['chunks'], 'layout_chunk_union_changed')


def projected(rows):
    return [{k:r[k] for k in FIELDS} for r in rows]


def read_state(cur, extension, lock=False):
    cur.execute('SELECT source,chunk_count,raw_metadata FROM knowledge_ch.documents WHERE id=%s'+(' FOR UPDATE' if lock else ''),(extension['document_id'],))
    row=cur.fetchone();require(row is not None and row[0]==historical.SOURCE,'layout_existing_source_required')
    cur.execute('SELECT to_jsonb(t) FROM knowledge_ch.chunks t WHERE document_id=%s ORDER BY chunk_index',(extension['document_id'],))
    chunks=projected([r[0] for r in cur.fetchall()])
    base=extension['base']
    if row[1:] == (len(base['chunks']),base['metadata']) and chunks==base['chunks']:return 'base'
    if row[1:] == (len(extension['chunks']),extension['metadata']) and chunks==extension['chunks']:return 'extended'
    raise ValueError('layout_state_not_exact_base_or_extension')


def base_replay(cur, base):
    """Recognize an exact already-extended base acquisition without resetting it."""
    cur.execute('SELECT raw_metadata FROM knowledge_ch.documents WHERE id=%s',(base['document_id'],))
    row=cur.fetchone()
    if not row or not row[0].get('historical_layout_recovery'):return False
    ext=assemble(base['review']['vd_document_id'])
    require(ext['base']==base,'layout_base_replay_bundle_changed')
    verify_bronze(cur,ext)
    require(read_state(cur,ext,lock=True)=='extended','layout_base_replay_state_changed')
    return len(ext['chunks'])


def verify_originals(evidence_dir):
    """Bind reviewed raw boxes to exact PDFs/crops; OCR output is frozen, never rerun."""
    import fitz
    require(fitz.VersionBind=='1.26.7','layout_runtime_changed')
    manifest=load()
    for source_id,pin in manifest['sources'].items():
        ext=assemble(source_id); path=Path(evidence_dir).parent/pin['source_cache_path']
        require(bridge.build_bundle(path,ext['review'])==ext['base'],'layout_original_base_changed')
        with fitz.open(path) as pdf:
            for b in ext['blocks']:
                page=pdf[b['physical_page']-1];rect=fitz.Rect(b['bbox_pdf_points'])
                require([page.rect.width,page.rect.height]==b['page_dimensions'] and page.rotation==b['page_rotation'],'layout_page_geometry_changed')
                require(hashlib.sha256(page.get_text('text').encode()).hexdigest()==b['original_page_text_sha256'],'layout_original_layer_changed')
                crop=page.get_pixmap(matrix=fitz.Matrix(300/72,300/72),clip=rect,alpha=False).tobytes('png')
                require(hashlib.sha256(crop).hexdigest()==b['crop_image_sha256'],'layout_crop_changed')
                require(page.get_text('text',clip=rect,sort=False)==b['original_cropped_unsorted_text'],'layout_unsorted_crop_changed')
                if b['representation']=='coordinate_sorted_existing_uncorrected_pdf_scan_ocr':require(page.get_text('text',clip=rect,sort=True)==b['text'],'layout_embedded_crop_changed')
                else:require(not page.get_text('text').strip(),'layout_ocr_requires_empty_layer')
    return [assemble(s) for s in manifest['sources']]


def verify_bronze(cur, ext):
    base=ext['base'];r=base['review']
    cur.execute('SELECT source_metadata,current_version_id::text,catalog_hash,document_type FROM bronze_ch.planning_document_sources WHERE id=%s',(base['source_id'],))
    require(cur.fetchone()==(r,base['version_id'],digest(r),historical.document_type(r)),'layout_original_source_contract_changed')
    cur.execute('SELECT source_id::text,content_hash,pages,knowledge_document_id::text FROM bronze_ch.planning_document_versions WHERE id=%s',(base['version_id'],))
    require(cur.fetchone()==(base['source_id'],r['source_sha256'],base['pages'],base['document_id']),'layout_original_version_contract_changed')


def persist(conn, extension, operation_id):
    ext=assemble(extension['review']['vd_document_id']);require(ext==extension,'layout_extension_changed')
    r=ext['review']
    with conn.cursor() as cur:
        cur.execute('SELECT sha256,plan_status,page_count,source_url FROM bronze_ch.vd_pdcom_documents WHERE id=%s FOR SHARE',(r['vd_document_id'],))
        require(cur.fetchone()==(r['source_sha256'],'unverified',r['page_count'],r['source_url']),'layout_registered_source_changed')
        historical.validate_registered_mapping(cur,r)
        verify_bronze(cur,ext)
        state=read_state(cur,ext,lock=True)
        bridge.checked_insert(cur,'bronze_ch','planning_document_runs',{'id':str(operation_id),'scope':{'kind':'reviewed_partial_box_append','document_id':r['vd_document_id'],'revision':MANIFEST_SHA256}})
        if state=='base':
            cur.execute("SELECT tgrelid::regclass::text,tgenabled FROM pg_trigger WHERE tgname='classify_on_insert' AND tgrelid IN ('knowledge_ch.documents'::regclass,'knowledge_ch.chunks'::regclass) ORDER BY 1")
            require(cur.fetchall()==[('knowledge_ch.chunks','O'),('knowledge_ch.documents','O')],'layout_classifier_state_changed')
            cur.execute('ALTER TABLE knowledge_ch.chunks DISABLE TRIGGER classify_on_insert')
            for chunk in ext['new_chunks']:bridge.checked_insert(cur,'knowledge_ch','chunks',dict(chunk,domain='real_estate',accessible_to_products=['lamap','lbi']))
            cur.execute('UPDATE knowledge_ch.documents SET chunk_count=%s,raw_metadata=%s WHERE id=%s AND chunk_count=%s AND raw_metadata=%s',
                        (len(ext['chunks']),Json(ext['metadata']),ext['document_id'],len(ext['base']['chunks']),Json(ext['base']['metadata'])))
            require(cur.rowcount==1,'layout_document_compare_and_swap_failed')
            cur.execute('ALTER TABLE knowledge_ch.chunks ENABLE TRIGGER classify_on_insert')
        require(read_state(cur,ext)=='extended','layout_poststate_changed')
    return {'documents_new':0,'chunks_new':len(ext['new_chunks']) if state=='base' else 0,'document_id':ext['document_id'],'version_id':ext['version_id'],'representation_revision':MANIFEST_SHA256,'new_block_count':len(ext['blocks']),'partial_physical_pages':sorted({b['physical_page'] for b in ext['blocks']}),'spatial_increment':0,'national_complete':False,'fully_certified_pages':0}


def deliver(conn, document_id):
    """CAS only receiver metadata/count; insert new chunks, never repair old rows."""
    result={}
    with conn.cursor() as cur:
        cur.execute('SELECT raw_metadata FROM knowledge_ch.documents WHERE id=%s',(document_id,));row=cur.fetchone();require(row is not None,'layout_source_missing')
        ext=assemble(row[0].get('vd_document_id'));require(document_id==ext['document_id'] and read_state(cur,ext)=='extended','layout_delivery_requires_exact_extension')
        for src,dst in [('v_documents_sync','knowledge_documents'),('v_chunks_sync','knowledge_chunks')]:
            cur.execute('SELECT source_schema,source_view,foreign_schema,target_server,target_db,is_active FROM gold_ch.lia_sync_manifest WHERE target_table=%s',(dst,))
            require(cur.fetchall()==[('knowledge_ch',src,'lamap_db_foreign','lamap_db_server','lamap_db',True)],'layout_delivery_route_changed')
        def rows(schema,table,key):
            cur.execute(sql.SQL('SELECT to_jsonb(t) FROM {}.{} t WHERE {}=%s ORDER BY id').format(sql.Identifier(schema),sql.Identifier(table),sql.Identifier(key)),(document_id,));return [v[0] for v in cur.fetchall()]
        source_doc=rows('knowledge_ch','v_documents_sync','id');target_doc=rows('lamap_db_foreign','knowledge_documents','id');require(len(source_doc)==1,'layout_source_document_count')
        target_base=copy.deepcopy(source_doc);target_base[0].update(chunk_count=len(ext['base']['chunks']),raw_metadata=ext['base']['metadata'])
        require(target_doc in (source_doc,target_base),'layout_receiver_document_drift')
        source_chunks=rows('knowledge_ch','v_chunks_sync','document_id');target_chunks=rows('lamap_db_foreign','knowledge_chunks','document_id')
        base_ids={c['id'] for c in ext['base']['chunks']};source_base=[c for c in source_chunks if c['id'] in base_ids]
        require(target_chunks in (source_chunks,source_base),'layout_receiver_old_chunk_drift')
        if target_doc!=source_doc:
            cur.execute('UPDATE lamap_db_foreign.knowledge_documents SET chunk_count=%s,raw_metadata=%s WHERE id=%s AND chunk_count=%s AND raw_metadata=%s',
                        (len(ext['chunks']),Json(ext['metadata']),document_id,len(ext['base']['chunks']),Json(ext['base']['metadata'])))
            require(cur.rowcount==1,'layout_receiver_compare_and_swap_failed')
        if target_chunks!=source_chunks:
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='knowledge_ch' AND table_name='v_chunks_sync' ORDER BY ordinal_position");columns=[v[0] for v in cur.fetchall()]
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='lamap_db_foreign' AND table_name='knowledge_chunks' ORDER BY ordinal_position");require(set(columns)=={v[0] for v in cur.fetchall()},'layout_receiver_columns_changed')
            fields=sql.SQL(',').join(map(sql.Identifier,columns));ids=[c['id'] for c in ext['new_chunks']]
            cur.execute(sql.SQL('INSERT INTO lamap_db_foreign.knowledge_chunks ({}) SELECT {} FROM knowledge_ch.v_chunks_sync WHERE document_id=%s AND id=ANY(%s::uuid[])').format(fields,fields),(document_id,ids))
        require(rows('lamap_db_foreign','knowledge_documents','id')==source_doc and rows('lamap_db_foreign','knowledge_chunks','document_id')==source_chunks,'layout_receiver_poststate_changed')
        result={'knowledge_documents':1,'knowledge_chunks':len(source_chunks)}
    return result
