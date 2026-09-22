"""Append verified document-level policy context without reclassifying source geometry."""
import hashlib,json
from pathlib import Path
from psycopg2.extras import Json
DOC='ad153736-58f1-533f-8cca-e41524f9fe12'
SHA='a4079a88201b716ede0793150102306f8b1faddb55c391a7c2c1696244a96d02'
MANIFEST_SHA='210fd495e329826123e24acf3d08e8c34ba06601944f17f054cf86dc4baf2920'
FIELD='later_document_policy_context'
def load():
    raw=(Path(__file__).parent/'reports/prangins-policy/context.json').read_bytes()
    if hashlib.sha256(raw).hexdigest()!=MANIFEST_SHA:raise ValueError('prangins_policy_evidence_changed')
    b=json.loads(raw)
    if b['document_id']!=DOC or b['source_sha256']!=SHA or len(b['rows'])!=23 or len({r['id'] for r in b['rows']})!=23:raise ValueError('prangins_policy_scope')
    p=b['context']
    if p['status']!='verified_2018_policy_change_requires_feature_review' or p['affected_existing_geometry_established'] is not False or p['currentness']!='unresolved':raise ValueError('prangins_policy_overclaim')
    return b
def persist(conn,document_id,sha):
    if str(document_id)!=DOC or sha!=SHA:return None
    b=load();expected={r['id']:r['label'] for r in b['rows']}
    with conn,conn.cursor() as c:
        c.execute('SELECT sha256 FROM bronze_ch.vd_pdcom_documents WHERE id=%s',(DOC,))
        if c.fetchone()!=(SHA,):raise ValueError('prangins_policy_source_changed')
        c.execute('SELECT id::text,label FROM bronze_ch.vd_pdcom_sectors WHERE document_id=%s AND id=ANY(%s::uuid[]) FOR UPDATE',(DOC,list(expected)))
        if dict(c.fetchall())!=expected:raise ValueError('prangins_policy_rows_changed')
        c.execute('''UPDATE bronze_ch.vd_pdcom_sectors SET validation_evidence=jsonb_set(COALESCE(validation_evidence,'{}'::jsonb),ARRAY[%s],%s::jsonb) WHERE document_id=%s AND id=ANY(%s::uuid[]) AND validation_evidence->%s IS DISTINCT FROM %s::jsonb''',(FIELD,Json(b['context']),DOC,list(expected),FIELD,Json(b['context'])))
        changed=c.rowcount
    return {'source_rows':23,'evidence_rows_changed':changed,'geometry_changed':False,'category_status_changed':False}
