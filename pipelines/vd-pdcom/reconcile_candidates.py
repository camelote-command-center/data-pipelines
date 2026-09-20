"""Resolve pending discovery links already represented by registered documents.

This accepts a source link, not document currency, geometry, or commune coverage.
The caller owns the transaction; candidate changes and evidence commit together.
"""
import uuid
from psycopg2.extras import Json, RealDictCursor


def reconcile(conn, document_ids=None, operation_id=None):
    groups = {}
    with conn.cursor(cursor_factory=RealDictCursor) as c:
        c.execute('''SELECT s.id,s.commune_bfs,s.source_url,s.kind
          FROM bronze_ch.vd_pdcom_source_candidates s
          WHERE s.review_status='pending' AND EXISTS (
            SELECT 1 FROM bronze_ch.vd_pdcom_documents d
            JOIN bronze_ch.vd_pdcom_document_communes dc ON dc.document_id=d.id
            WHERE dc.commune_bfs=s.commune_bfs
            AND (%s::uuid[] IS NULL OR d.id=ANY(%s::uuid[]))
            AND (s.source_url=d.source_url OR
              (s.kind='landing' AND rtrim(btrim(s.source_url),'/')=rtrim(btrim(d.landing_url),'/'))))
          ORDER BY s.id FOR UPDATE OF s''',(document_ids,document_ids))
        candidates=c.fetchall()
        for row in candidates:
            c.execute('''SELECT d.id,d.sha256,d.source_url,d.landing_url
              FROM bronze_ch.vd_pdcom_documents d
              JOIN bronze_ch.vd_pdcom_document_communes dc ON dc.document_id=d.id
              WHERE dc.commune_bfs=%s AND (%s::uuid[] IS NULL OR d.id=ANY(%s::uuid[]))
              AND (%s=d.source_url OR
                (%s='landing' AND rtrim(btrim(%s),'/')=rtrim(btrim(d.landing_url),'/')))
              ORDER BY d.id''',(row['commune_bfs'],document_ids,document_ids,row['source_url'],row['kind'],row['source_url']))
            documents=[dict(d) for d in c.fetchall()]
            if not documents:
                raise ValueError('candidate_document_evidence_changed')
            item=dict(row,previous_review_status='pending',review_status='accepted',documents=documents)
            groups.setdefault(row['commune_bfs'],[]).append(item)
        if not candidates:
            return {'accepted':0,'operation_id':None,'communes':0}
        operation_id=operation_id or str(uuid.uuid5(uuid.NAMESPACE_URL,
            'vd-pdcom:registered-candidates:'+','.join(str(r['id']) for r in candidates)))
        for bfs,items in groups.items():
            evidence={'stage':'registered_source_reconciliation','candidates':items,
                      'coverage_complete':False,'geometry_validation_changed':False,
                      'rule':'Exact registered PDF URL or whitespace/trailing-slash-normalized registered landing URL, with matching document commune membership. No filename-only, query-removal or cross-commune matches.'}
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_discovery_attempts
              (operation_id,commune_bfs,outcome,evidence)
              VALUES(%s,%s,'registered_source_reconciliation',%s)''',
              (operation_id,bfs,Json(evidence)))
        c.execute('''UPDATE bronze_ch.vd_pdcom_source_candidates SET review_status='accepted'
          WHERE id=ANY(%s::uuid[]) AND review_status='pending' ''',([str(r['id']) for r in candidates],))
        if c.rowcount!=len(candidates):
            raise ValueError('candidate_review_write_count_mismatch')
    return {'accepted':len(candidates),'operation_id':operation_id,'communes':len(groups)}
