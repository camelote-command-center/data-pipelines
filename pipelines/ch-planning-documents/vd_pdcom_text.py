"""Scoped municipal PDCom text acquisition; no spatial or national-completion claims.

Caller owns transactions. The national parser remains unchanged. This bridge
uses its deterministic version IDs and existing registered knowledge routes.
"""
import hashlib
import json
import uuid
from pathlib import Path
from urllib.parse import urlparse
from psycopg2 import sql
from psycopg2.extras import Json

SOURCE = 'vd_pdcom_municipal'
NS = uuid.uuid5(uuid.NAMESPACE_URL, 'pixxels:ch-planning-documents:v1')

def uid(value):
    return str(uuid.uuid5(NS, value))

def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()

def require(ok, reason):
    if not ok:
        raise ValueError(reason)

def validate_review(r):
    uuid.UUID(r['vd_document_id'])
    require(r['source_role'] in ('approved_main','approved_amendment'), 'only_reviewed_approved_source_roles_supported')
    if r['source_role']=='approved_amendment':
        scope=r.get('amendment_scope',{})
        require(bool(scope.get('predecessor')) and bool(scope.get('replaced_part')) and bool(scope.get('approved_scope')) and scope.get('replaces_entire_plan') is False, 'explicit_limited_amendment_scope_required')
    require(r['plan_status'] in ('approved', 'approved_with_reservation'), 'approval_required')
    if r['plan_status']=='approved_with_reservation':
        reservations=r.get('reservations')
        require(isinstance(reservations,list) and bool(reservations) and all(isinstance(v,dict) and bool(v.get('scope')) and isinstance(v.get('evidence_pdf_pages'),list) and bool(v['evidence_pdf_pages']) and all(isinstance(n,int) and 1<=n<=r['page_count'] for n in v['evidence_pdf_pages']) for v in reservations), 'explicit_reservation_scope_and_evidence_required')
    require(r['canton'] == 'VD' and isinstance(r['commune_bfs'], int), 'explicit_vd_scope_required')
    require(urlparse(r['source_url']).scheme == 'https' and urlparse(r['source_url']).hostname == r['official_host'], 'official_source_url_mismatch')
    require(r['signed_approval']['visually_verified'] is True and bool(r['signed_approval']['scope']), 'approval_scope_review_required')
    require(r['all_prose_is_binding'] is False and r['spatial_qualification'] is False, 'no_blanket_approval_or_geometry')
    require(bool(r['diagnostic_vintage_limit']) and bool(r['limits']), 'interpretation_limits_required')
    require(r['page_count'] > 0 and len(r['source_sha256']) == 64, 'source_identity_required')

def build_bundle(pdf_path, review):
    import fitz
    validate_review(review)
    path = Path(pdf_path)
    require(hashlib.sha256(path.read_bytes()).hexdigest() == review['source_sha256'], 'pdf_hash_mismatch')
    pages = []
    with fitz.open(path) as pdf:
        require(len(pdf) == review['page_count'], 'physical_page_count_mismatch')
        for i, page in enumerate(pdf):
            text = page.get_text('text')
            status = 'native_text_extracted' if text.strip() else ('not_extracted_image_page' if page.get_images() else 'no_native_text')
            pages.append({'page_number': i+1, 'text': text, 'method': 'pymupdf_native_text', 'status': status,
                          'width': page.rect.width, 'height': page.rect.height})
    return assemble(review, pages, path.stat().st_size)

def assemble(review, pages, byte_count):
    validate_review(review)
    require([p['page_number'] for p in pages] == list(range(1, review['page_count']+1)), 'physical_pages_incomplete_or_reordered')
    require(all(isinstance(p['text'], str) and p['status'] in ('native_text_extracted', 'not_extracted_image_page', 'no_native_text') for p in pages), 'page_status_required')
    url, sha = review['source_url'], review['source_sha256']
    doc = uid('knowledge:'+url+':'+sha)
    provenance = {'parser': SOURCE, 'vd_document_id': review['vd_document_id'], 'content_hash': sha,
                  'commune_bfs': review['commune_bfs'], 'approval_scope': review['signed_approval'],
                  'all_prose_is_binding': False, 'diagnostic_vintage_limit': review['diagnostic_vintage_limit'],
                  'limits': review['limits'], 'spatial_qualification': False, 'commune_complete': False,
                  'source_role': review['source_role'], 'plan_status': review['plan_status'], 'review_sha256': digest(review)}
    if review['source_role']=='approved_amendment':
        provenance['amendment_scope']=review['amendment_scope']
    if review['plan_status']=='approved_with_reservation':
        provenance['reservations']=review['reservations']
    chunks = []
    for p in pages:
        for start in range(0, len(p['text']), 2000):
            text = p['text'][start:start+2000].strip()
            if text:
                i = len(chunks)
                chunks.append({'id': uid(doc+':'+str(i)), 'document_id': doc, 'chunk_index': i, 'content': text,
                               'page_number': p['page_number'], 'metadata': dict(provenance, source_url=url,
                               citation=url+'#page='+str(p['page_number']), extraction_status=p['status'])})
    require(bool(chunks), 'no_searchable_text')
    metadata = dict(provenance, source_url=url, physical_page_count=len(pages),
                    page_manifest=[{k:v for k,v in p.items() if k != 'text'} for p in pages],
                    extraction_status='partial_native_text', map_pages=review['map_pages'],
                    full_visual_map_extraction=False)
    return {'review': review, 'pages': pages, 'byte_count': byte_count, 'chunks': chunks,
            'source_id': uid(SOURCE+':VD:'+review['vd_document_id']),
            'version_id': uid('version:'+url+':'+sha), 'document_id': doc, 'metadata': metadata}

def checked_insert(c, schema, table, row, key='id'):
    cols = list(row)
    values = [Json(row[k]) if isinstance(row[k], (dict, list)) and k not in ('accessible_to_products',) else row[k] for k in cols]
    c.execute(sql.SQL('INSERT INTO {}.{} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING').format(
        sql.Identifier(schema), sql.Identifier(table), sql.SQL(',').join(map(sql.Identifier, cols)),
        sql.SQL(',').join(sql.Placeholder() for _ in cols), sql.Identifier(key)), values)
    c.execute(sql.SQL('SELECT {} FROM {}.{} WHERE {}=%s').format(sql.SQL(',').join(map(sql.Identifier, cols)),
              sql.Identifier(schema), sql.Identifier(table), sql.Identifier(key)), (row[key],))
    saved = c.fetchone()
    require(saved is not None and all(a == b for a,b in zip(saved, row.values())), 'immutable_provenance_conflict:'+table)

def persist(conn, bundle, operation_id, pdf_path):
    """No commits; exact replay verifies provenance. Caller can rollback rehearsal."""
    b, r = bundle, bundle['review']
    require(build_bundle(pdf_path, r) == b, 'bundle_not_exact_pdf_extraction')
    with conn.cursor() as c:
        c.execute("SET LOCAL statement_timeout='90s'")
        c.execute('SELECT sha256,plan_status,page_count,source_url FROM bronze_ch.vd_pdcom_documents WHERE id=%s FOR SHARE', (r['vd_document_id'],))
        require(c.fetchone() == (r['source_sha256'], r['plan_status'], r['page_count'], r['source_url']), 'registered_source_changed')
        c.execute('SELECT 1 FROM bronze_ch.vd_pdcom_document_communes WHERE document_id=%s AND commune_bfs=%s', (r['vd_document_id'],r['commune_bfs']))
        require(c.fetchone() is not None, 'registered_commune_scope_missing')
        c.execute('SELECT id FROM knowledge_ch.documents WHERE original_url=%s AND is_active AND id<>%s', (r['source_url'], b['document_id']))
        require(not c.fetchall(), 'active_prior_version_requires_explicit_reviewed_supersession')
        c.execute('SELECT count(*) FROM knowledge_ch.documents WHERE id=%s',(b['document_id'],));documents_before=c.fetchone()[0]
        c.execute('SELECT count(*) FROM knowledge_ch.chunks WHERE document_id=%s',(b['document_id'],));chunks_before=c.fetchone()[0]
        checked_insert(c,'bronze_ch','planning_document_runs',{'id':str(operation_id),'scope':{'kind':'scoped_municipal_pdcom_text','document_id':r['vd_document_id']}})
        checked_insert(c,'bronze_ch','planning_document_sources',{'id':b['source_id'],'source':SOURCE,'canton_code':'VD','source_key':r['vd_document_id'],'title':r['title'],'document_url':r['source_url'],'commune_bfs':r['commune_bfs'],'language':'fr','legal_status':r['plan_status'],'document_type':'municipal_pdcom','source_metadata':r,'catalog_hash':digest(r),'current_version_id':b['version_id'],'extraction_status':'partial_native_text'})
        checked_insert(c,'bronze_ch','planning_document_versions',{'id':b['version_id'],'source_id':b['source_id'],'content_hash':r['source_sha256'],'final_url':r['source_url'],'content_type':'application/pdf','byte_count':b['byte_count'],'pages':b['pages'],'extraction_status':'partial_native_text','knowledge_document_id':b['document_id']})
        # Only the expensive classifier is suspended; taxonomy remains enabled.
        # Transaction rollback restores trigger state if any assertion fails.
        c.execute("SELECT tgrelid::regclass::text,tgenabled FROM pg_trigger WHERE tgname='classify_on_insert' AND tgrelid IN ('knowledge_ch.documents'::regclass,'knowledge_ch.chunks'::regclass) ORDER BY 1")
        require(c.fetchall() == [('knowledge_ch.chunks','O'),('knowledge_ch.documents','O')], 'classifier_trigger_state_unexpected')
        c.execute('ALTER TABLE knowledge_ch.documents DISABLE TRIGGER classify_on_insert')
        c.execute('ALTER TABLE knowledge_ch.chunks DISABLE TRIGGER classify_on_insert')
        checked_insert(c,'knowledge_ch','documents',{'id':b['document_id'],'title':r['title'],'source':SOURCE,'original_url':r['source_url'],'document_type':'planning_document','publisher':r['publisher'],'language':'fr','country':'CH','canton_code':'VD','ingestion_status':'completed','chunk_count':len(b['chunks']),'is_active':True,'raw_metadata':b['metadata'],'domain':'real_estate','accessible_to_products':['lamap','lbi']})
        for chunk in b['chunks']:
            checked_insert(c,'knowledge_ch','chunks',dict(chunk,domain='real_estate',accessible_to_products=['lamap','lbi']))
        c.execute('SELECT count(*) FROM knowledge_ch.chunks WHERE document_id=%s',(b['document_id'],))
        require(c.fetchone()[0] == len(b['chunks']), 'unexpected_extra_chunks')
        c.execute('ALTER TABLE knowledge_ch.chunks ENABLE TRIGGER classify_on_insert')
        c.execute('ALTER TABLE knowledge_ch.documents ENABLE TRIGGER classify_on_insert')
    return {'documents_new':1-documents_before,'chunks_new':len(b['chunks'])-chunks_before,'document_id':b['document_id'],'version_id':b['version_id'],'physical_pages':len(b['pages']),'searchable_chunks':len(b['chunks']),'empty_native_text_pages':[p['page_number'] for p in b['pages'] if not p['text'].strip()],'spatial_increment':0,'national_complete':False}

def finish_run(conn, operation_id, report):
    """Complete this bounded operation without overwriting an earlier receipt."""
    stored_report={k:v for k,v in report.items() if k not in ('committed','rolled_back','bounded_run_receipt')}
    with conn.cursor() as c:
        c.execute("UPDATE bronze_ch.planning_document_runs SET status='partial',completed_at=now(),report=%s WHERE id=%s AND completed_at IS NULL",(Json(dict(stored_report,national_complete=False)),str(operation_id)))
        c.execute('SELECT status,completed_at,report FROM bronze_ch.planning_document_runs WHERE id=%s',(str(operation_id),));row=c.fetchone()
        require(row and row[0]=='partial' and row[1] is not None and row[2]['document_id']==report['document_id'],'bounded_run_receipt_mismatch')
        return row[2]

def deliver(conn, document_id):
    """Scoped upsert through registered FDW only; never invokes truncate sync."""
    result = {}
    with conn.cursor() as c:
        c.execute("SET LOCAL statement_timeout='120s'")
        c.execute('SELECT source,raw_metadata FROM knowledge_ch.documents WHERE id=%s', (document_id,))
        row=c.fetchone()
        require(row is not None and row[0]==SOURCE and row[1].get('spatial_qualification') is False and row[1].get('all_prose_is_binding') is False, 'scoped_document_contract_required')
        for src,dst,key in [('v_documents_sync','knowledge_documents','id'),('v_chunks_sync','knowledge_chunks','document_id')]:
            c.execute("SELECT source_schema,source_view,foreign_schema,target_server,target_db,is_active FROM gold_ch.lia_sync_manifest WHERE target_table=%s",(dst,))
            require(c.fetchall() == [('knowledge_ch',src,'lamap_db_foreign','lamap_db_server','lamap_db',True)], 'registered_delivery_route_changed')
            c.execute('SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position',('knowledge_ch',src));columns=[x[0] for x in c.fetchall()]
            c.execute('SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position',('lamap_db_foreign',dst));receiver=[x[0] for x in c.fetchall()]
            require(columns and set(columns) == set(receiver), 'receiver_column_contract_changed')
            source=sql.Identifier('knowledge_ch',src);target=sql.Identifier('lamap_db_foreign',dst)
            fields=lambda alias:sql.SQL(',').join(sql.SQL('{}.{}').format(sql.Identifier(alias),sql.Identifier(x)) for x in columns)
            updates=sql.SQL(',').join(sql.SQL('{}=s.{}').format(sql.Identifier(x),sql.Identifier(x)) for x in columns if x!='id')
            c.execute(sql.SQL('UPDATE {} t SET {} FROM {} s WHERE s.{}=%s AND t.{}=%s AND t.id=s.id AND ROW({}) IS DISTINCT FROM ROW({})').format(target,updates,source,sql.Identifier(key),sql.Identifier(key),fields('t'),fields('s')),(document_id,document_id))
            c.execute(sql.SQL('INSERT INTO {} ({}) SELECT {} FROM {} s WHERE s.{}=%s AND NOT EXISTS(SELECT 1 FROM {} t WHERE t.{}=%s AND t.id=s.id)').format(target,sql.SQL(',').join(map(sql.Identifier,columns)),fields('s'),source,sql.Identifier(key),target,sql.Identifier(key)),(document_id,document_id))
            c.execute(sql.SQL('SELECT {} FROM {} s WHERE {}=%s ORDER BY id').format(fields('s'),source,sql.Identifier(key)),(document_id,));source_rows=c.fetchall()
            c.execute(sql.SQL('SELECT {} FROM {} t WHERE {}=%s ORDER BY id').format(fields('t'),target,sql.Identifier(key)),(document_id,));target_rows=c.fetchall()
            require(source_rows==target_rows,'receiver_field_or_scope_mismatch');n=len(source_rows)
            require(n>0,'empty_scoped_source');result[dst]=n
    return result

def monitor(conn, operation_id, phase, report):
    """Bounded acquisition log; compensate legacy trigger without advancing freshness."""
    require(phase in ('running','partial','failed'), 'scoped_monitor_phase')
    with conn.cursor() as c:
        fields=['last_acquired_at','last_db_update_at','next_acquisition_at','record_count','last_error','status','updated_at']
        c.execute(sql.SQL("SELECT id,{} FROM public.datasets WHERE code='ch_planning_document_text' AND status='active' FOR UPDATE").format(sql.SQL(',').join(map(sql.Identifier,fields))))
        rows=c.fetchall();require(len(rows)==1,'text_dataset_registration_missing')
        dataset=str(rows[0][0]);before=rows[0][1:]
        if phase=='running':
            c.execute("INSERT INTO public.acquisition_logs(id,dataset_id,status,triggered_by,notes) VALUES(%s,%s,'running','manual',%s) ON CONFLICT(id) DO NOTHING",(str(operation_id),dataset,'Scoped municipal PDCom approved-corpus bridge; not national completeness'))
            c.execute('SELECT dataset_id::text,status FROM public.acquisition_logs WHERE id=%s',(str(operation_id),));require(c.fetchone()==(dataset,'running'),'monitor_operation_conflict')
        else:
            c.execute("UPDATE public.acquisition_logs SET status=%s,completed_at=now(),records_fetched=%s,records_new=%s,error_details=%s,error_message=%s WHERE id=%s AND dataset_id=%s AND status='running' RETURNING id",(phase,report.get('physical_pages',0),report.get('documents_new',0),Json(dict(report,national_complete=False)),None if phase=='partial' else 'Scoped municipal text bridge failed; inspect sanitized report',str(operation_id),dataset));require(c.fetchone() is not None,'monitor_completion_conflict')
        # Legacy acquisition-log trigger treats partial as national freshness.
        # Restore only its affected fields while holding the exact dataset row.
        setters=sql.SQL(',').join(sql.SQL('{}=%s').format(sql.Identifier(k)) for k in fields)
        c.execute(sql.SQL('UPDATE public.datasets SET {} WHERE id=%s').format(setters),(*before,dataset))
        # The schedule trigger can recompute next_acquisition_at on restoration.
        c.execute('UPDATE public.datasets SET next_acquisition_at=%s WHERE id=%s',(before[2],dataset))
        c.execute(sql.SQL('SELECT {} FROM public.datasets WHERE id=%s').format(sql.SQL(',').join(map(sql.Identifier,fields))),(dataset,))
        require(c.fetchone()==before,'national_dataset_fields_not_restored')


def main():
    import argparse
    import os
    import psycopg2
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--pdf',required=True)
    p.add_argument('--review',required=True)
    p.add_argument('--operation-id',required=True,type=uuid.UUID)
    p.add_argument('--report',required=True)
    p.add_argument('--monitor-id',type=uuid.UUID,help='Distinct attempt UUID required for commit; source operation ID stays stable on retry')
    p.add_argument('--deliver',action='store_true',help='Scoped registered knowledge receiver, never global sync')
    p.add_argument('--commit',action='store_true',help='Commit monitored bounded acquisition; default rehearses twice and rolls back')
    a=p.parse_args()
    require(not a.commit or (a.monitor_id is not None and a.monitor_id != a.operation_id), 'distinct_monitor_attempt_id_required')
    b=build_bundle(a.pdf,json.loads(Path(a.review).read_text()))
    c=psycopg2.connect(os.environ['RE_LLM_DB_URL'],connect_timeout=15)
    mc=None;begun=False;report={'operation_id':str(a.operation_id),'monitor_id':str(a.monitor_id) if a.monitor_id else None,'committed':False}
    try:
        if a.commit:
            mc=psycopg2.connect(os.environ['PIXXELS_DB_URL'],connect_timeout=15)
            monitor(mc,a.monitor_id,'running',{});mc.commit();begun=True
        report.update(persist(c,b,a.operation_id,a.pdf))
        replay=persist(c,b,a.operation_id,a.pdf)
        require(replay['documents_new']==0 and replay['chunks_new']==0 and replay['document_id']==report['document_id'],'persist_replay_mismatch')
        if a.deliver:
            report['receiver']=deliver(c,b['document_id'])
            require(deliver(c,b['document_id'])==report['receiver'],'receiver_replay_mismatch')
        report['bounded_run_receipt']=finish_run(c,a.operation_id,report)
        if a.commit:
            c.commit();report['committed']=True
            monitor(mc,a.monitor_id,'partial',report);mc.commit()
        else:
            c.rollback();report['rolled_back']=True
        Path(a.report).write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps(report))
    except Exception as exc:
        c.rollback()
        # No raw database/connection exception text is stored in a report.
        report['error_type']=type(exc).__name__
        if begun:
            mc.rollback();monitor(mc,a.monitor_id,'failed',report);mc.commit()
        Path(a.report).write_text(json.dumps(report,indent=2)+'\n')
        raise
    finally:
        c.close()
        if mc:mc.close()

if __name__=='__main__':
    main()
